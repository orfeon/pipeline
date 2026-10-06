# Profile Transform Engine (Design Document)

Status: **Proposal** — the Beam execution of the contract in [profile-dsl.md](profile-dsl.md): one sketch
pass for the whole-dataset profile and the edges, one counting pass for every group and class, one shuffle
per `unique` key, one render step. Nothing here is built; §9 lists what has to be measured or decided before
the design is accepted. The starting point is the code of the existing sink (`util/pipeline/profile/`,
`module/sink/ProfileSink.java`): most of `util/pipeline/profile/` is kept and reshaped, the sink module
itself is deleted (dsl §2 — no alias, no compatibility).

## 1. Layout: pure computation and Beam wiring

| class | role | status |
|---|---|---|
| `ProfileSpec` | the resolved plan for one schema: profiled fields and types, skipped fields, sketch parameters, keys, target, unnest | kept, extended (composite keys, domains, numeric target, unnest paths) |
| `ProfileRow` | one element reduced to its profiled field values, plus the outcome of every row rule (§3.1) | kept, extended |
| `ProfileRules` | the parsed expectations: row rules compiled to the shared filter condition tree and to regex patterns, aggregate rules evaluated against the finished records | new |
| `ProfileAccumulator` | pass 1: per-field counters, moments, KLL / CPC / frequent-items / Theta sketches, the co-moment matrix, the row sample; plus the bounded exact value table (§3) | kept, extended; the per-class `TargetStats` sketches go away |
| `ProfileEdges` | the output of pass 1 that pass 2 reads: per field the cell edges or the value list, as a singleton side input | new |
| `ProfileCells` | pass 2: per (axis, group) the per-field arrays — cell counts, the counts over declared edges where a field has them, null count, min, max, sum, sum of squares, and per cell the target positives or target sum / sum of squares; per key × input a Theta sketch | new |
| `ProfileReport` | the record schemas of dsl §5 and the functions from accumulators to records: notable codes, drift and target statistics from cell counts, changes, checks | new (the statistics move out of `ProfileRenderer`) |
| `ProfileRenderer` | payload and manifest JSON, the HTML template, size degradation | kept, reduced to rendering |
| `ProfileStages` | the graph (§2) and its DoFns | new (extracted from `ProfileSink.expand`) |
| `ProfileTransform` | thin: validation, spec, `ProfileStages`, the outputs | new |
| `ProfileSink` | — | deleted, with `ProfileSinkTest`, `ProfileReportPreviewTest`, the three `examples/bigquery-to-profile*.yaml` (rewritten for the transform) and `module/sink/profile.md` (replaced by `module/transform/profile.md`; the `index.yaml` entry moves) |

`ProfileReport` is pure (accumulators in, records out) and carries the unit tests of every statistic; the
Beam classes hold no arithmetic.

## 2. The graph

```
input ─ Union ─ (Unnest) ─ Extract ─┬─ Combine.globally(ProfileAccumulator) ──────────┬─ Edges (singleton side input)
                                    │                                                 │
                                    ├─ group sizes: Count.perElement ─ SelectGroups ──┤  (side input)
                                    │                                                 ▼
                                    ├─ KeyByAxis ─ KeepSelected ─ Combine.perKey(ProfileCells) ─ as map ─┐
                                    │                                                                    │
                                    └─ per unique key: (input, hash) ─ Count.perKey ─ duplicates ────────┤
                                                                                                         ▼
                              profile (pass 1) ───────────────────────────────────────────────── Finalize ─┬─ fields (default)
                                                                                                           ├─ groups, values, bins, pairs, target, keys
                                                                                                           ├─ changes, checks
                                                                                                           ├─ sketches, sample
                                                                                                           └─ summary  (after the report / payload files are written)
```

- **Extract** is the sink's `ExtractDoFn`: one `ProfileRow` per element, a field whose conversion throws goes
  to the failure output and the row still counts.
- **Unnest** (dsl §7.4) runs before it: one element per child, carrying every field of the parent (nested
  structs and the parent's other arrays included, which are then profiled as they would be without
  `unnest`). A parent whose array is empty or null emits nothing and increments one of two counters that
  reach the `summary` (`parentsEmpty`, `parentsNull`); `parents` is their sum plus the parents that emitted.
- **Pass 1** is the sink's global combine with fan-out.
- **Pass 2** is one `Combine.perKey` over the same `ProfileRow` collection, keyed by axis group
  (`KeyByAxisDoFn` — a row is emitted once per axis), with `ProfileEdges` as a side input. The whole
  dataset and the two target classes are groups of their own axes, so there is one code path for every bin
  count. The runner consumes the extracted rows twice; pass 2 waits on the edges side input, which is what
  makes it a second pass.
- **Finalize** receives the pass 1 accumulator as its element and everything else as side inputs, builds
  the records, writes the report and payload files, and only then emits `summary`.

With `bins.mode: sketch` pass 2 is replaced by the sink's per-group `ProfileAccumulator` combine and the
cell counts are sketch queries at the same edges.

The time axis has two independent uses of this graph. Its group-size count (the `Count.perElement` branch)
gives the missing buckets (§3.2) and is always built when `time` is declared; its entry in `KeyByAxis` —
the per-bucket cells — is built only with `time.groups: true`.

## 3. Pass 1: the whole dataset

Unchanged from the sink, with these differences:

- **Exact value table.** Per string, bool and numeric field, a map value → count. It is mergeable and exact,
  and it is what `distinctExact`, the `values` output of a low-cardinality field and the categorical bins
  read. Values are held whole. The table is dropped for good — the field falls back to the frequent-items
  sketch alone, and a merge with a dropped table is dropped — when it exceeds 1,000 entries or when a value
  longer than 256 characters arrives: a table that shortened its keys would merge distinct values and report
  the result as exact. The bound on a field's table is therefore 1,000 × 256 characters. Shortening to 256
  characters happens only where a value is written out (`values.value`, `fields.top`, the report).
- **Frequent items.** `fiMaxMapSize` follows `accuracy` (512 / 1024 / 4096) instead of being fixed at 512,
  and an empty no-false-positive result falls back to the no-false-negative rows (dsl §5.4).
- **Null sentinels and blanks** are two more counters per string field.
- **No per-class sketches.** The target split of every field is pass 2's `target` axis.
- **Edges.** Per numeric-like field, the quantiles at 1/100 … 99/100 of the pass 1 KLL sketch, deduplicated
  (ties collapse cells), and next to them the declared `bins.edges` of the fields that have them. Per
  categorical field, the value table, or the top
  values of the frequent-items result plus `(other)`. Per declared pair, the same quantiles of the two
  fields' sketches merged.
- **`sum`** is accumulated on its own as a compensated (Kahan) sum rather than derived as mean × count, so
  that a reconciliation of two inputs does not inherit the rounding of the running mean.

### 3.1 Row rules

A `rows` condition may read any field of the input, including fields excluded from profiling and fields the
profile reads as something else (an array as its length). So a row rule is evaluated in **Extract**, on the
element, with the condition tree the `filter` parameter of other modules compiles to; `ProfileRow` carries
the result per rule as two bit sets (violated, not evaluated). `matches` rules are evaluated there too.

Pass 1 then only counts: per rule, violations and not-evaluated rows, and up to five example rows (the row
JSON the sample already renders), merged by keeping the first five. Nothing about a rule is approximate.

### 3.2 Freshness and missing buckets

`latest` is the pass 1 maximum of the time field. Missing buckets come from the group-size count that
already runs ahead of the group selection: it sees every bucket of the time axis before the most recent 60
are kept, so the gap list is computed there over the whole observed range and travels to Finalize with the
selected groups. Neither needs pass 2: with `time.groups: false` the bucket counts are still taken and no
bucket is profiled.

## 4. Pass 2: counting

`ProfileCells` for one group holds, per field, a `long[cells]` and five scalars, and with a target one or
two more arrays per field. Adding a value is a binary search over at most 99 edges (a hash lookup for a
categorical field). At 650 fields and 100 cells a group's accumulator is about 0.5 MB without a target and
1–1.5 MB with one; a run with 30 groups gathers 15–45 MB on the finalize worker — smaller than the
per-group sketch sets it replaces, which is why the group sketches are not kept alongside.

Everything dsl §6 emits is a function of these arrays:

| output | from |
|---|---|
| `bins` | cells merged into `bins.count` equal-frequency bins; for a field with declared edges, its second array as it is |
| `groups.ks` | max cumulative difference over the cells, group against baseline |
| `groups.psi`, `target.iv` | the same bins as the field's `bins` records, 0.5-per-bin smoothing |
| `groups.tvd` | the categorical value counts |
| `groups.p50` | linear interpolation inside the cell that contains the median |
| `groups.min` / `max` / `mean` / `stddev` | the scalars |
| `rate` / `targetMean` and their intervals | the per-cell target arrays |

A "rest of the axis" baseline (dsl §5.3) is the whole-dataset cells minus the group's — no extra
accumulator.

A field with declared edges has two arrays per group: the quantile cells, which every field has and which
KS and the median read, and the counts over its declared edges. A value is located in each by its own
binary search; the cost is one more search and one more short array for the declared fields only.

A declared pair adds two cell arrays (one per field, over the pair's pooled edges) to the whole-dataset
group only.

Pass 2 is not built at all when nothing reads it: no profiled axis (`segments`, `inputs`, `time` with
`groups: true`), no target, no pair and no `bins` output. That is the shape of a monitor run (dsl §9.5): the
input is read once, and with `associations: none` the per-row cost is linear in the number of fields. The
bucket count of §3.2 and the key shuffle of §5 are separate small branches, not a second pass over the
fields.

## 5. Keys

- **Set sketches** stay in pass 1 (Theta per key; a composite key is the hash of its field tuple). The
  overlap between the groups of an `inputs` axis needs one Theta sketch per key × input, held in that
  group's pass 2 accumulator.
- **`unique: true`** is a separate branch: (input index, 128-bit hash of the key) → `Count.perKey` → a
  combine per input of (keys with count > 1, Σ(count − 1)). The input index is part of the shuffle key only
  under `mode: compare`, where uniqueness is a property of each input (dsl §7.1) — counting over the union
  would report every key two inputs share as a duplicate; under `mode: union` it is constant. One shuffle of
  one small element per row; combiner lifting keeps the shuffled volume near the number of distinct keys per
  bundle. A hash collision would count as a duplicate; at 128 bits that is not a practical concern.
- **Null keys** never reach a sketch or the shuffle: a row whose key has a null field increments `nullKeys`
  in pass 1 and is skipped by both. (Hashing a null would make every such row one key, and all of them
  duplicates of each other.)
- **Against a previous run**: the key's Theta sketch is written into the payload, and intersected with the
  previous payload's in Finalize (dsl §7.3).
- **`keyness`** is capped at 1 and its bounds come from the Theta bounds; with `unique: true` the exact
  duplicate count is the statement and `keyness` is derived from it.

## 6. Finalize, files and degradation

One DoFn: copy the accumulators (querying a sketch mutates its lazily sorted state — a rule the sink's
code already follows), build the records through `ProfileReport`, render and write `output.report` / `output.payload`, emit.
`generatedAt` is taken once here and written to every record.

`previous` as a file is opened twice. At assembly its format is checked — a file without a format-version-2
payload is rejected there, not after the input has been scanned — and, with `bins.edges: previous`, its
whole-dataset edges are taken into the spec (the one thing pass 2 needs from it, which is why that option is
restricted to the file forms). In Finalize it is read in full for the changes and the key overlaps.
`previous: {input}` arrives as a side input holding the latest run's field records.

With `previous.window` the side input holds the field records of the last N runs of the dataset instead of
one: N × fields records (28 × 650 ≈ 18,000), reduced in Finalize to a median and a MAD per field and
quantity. Selecting those runs (same `dataset`, before this run's `partition` / `generatedAt`, optionally
the same weekday) is done by the transform from the collection it is given, so the source may simply read
the recent part of the history table.

Aggregate rules are evaluated last, against the finished records, by `ProfileRules`; a rule over an estimate
passes only when both ends of its interval do.

The size ladder of dsl §10.3 applies to the report only. The record outputs are emitted in full; the
`bins` output is `fields × groups × bins.count` records (650 × 30 × 10 ≈ 200,000) and is the one to leave
out of `outputs` when it is not wanted.

## 7. Why bins are counted, not queried

The sink derives every comparison from sketch queries: the PMF of a per-group KLL sketch over 64 equal-width
bins, rounded to counts. A KLL sketch keeps a sample whose items carry power-of-two weights, so the mass it
assigns to a narrow bin is a multiple of 128, 256, … rows; a bin can be empty on one side only, and PSI
turns that into a large log term. Measured with the library version in use (datasketches-java 6.2.0), two
independent samples of one normal distribution, 20 trials, sketches built from 40 merged bundles:

| sizes, k | exact, 64 equal-width | sink (sketch, 64 equal-width) | sketch, 10 equal-frequency | sketch, 20 equal-frequency |
|---|---|---|---|---|
| 450k vs 450k, k=200 | 0.0003 | mean 0.117, max 0.190 | 0.0009 / 0.0019 | 0.0030 / 0.0057 |
| 450k vs 450k, k=800 | 0.0003 | 0.021 / 0.039 | 0.0001 / 0.0002 | 0.0002 / 0.0005 |
| 900k vs 303, k=200 | 0.236 | 0.344 / 0.604 | 0.030 / 0.066 | 0.059 / 0.097 |

Log-normal and integer-valued distributions behave the same. Two readings:

- Equal-frequency bins alone remove the artefact (two orders of magnitude at the default accuracy); this is
  what `bins.mode: sketch` gives.
- The last row is not sketch error: 9/303 = 0.030 is the sampling noise of a 10-bin PSI at that size, and
  the exact 64-bin value is 0.24. This is the `noisePsi` column.

The counting pass goes one step further for a reason the table does not show: the outputs are stored in
tables and compared across runs, and a stored number is read as a fact. A KLL sketch draws from a static
random source with no seed parameter, and bundle boundaries and merge order differ between runs, so no
sketch-derived count can be made reproducible. A count over fixed edges can (dsl §6.2).

## 8. Tests

- `ProfileReportTest` (pure): every statistic against a brute-force computation on small data; notable
  codes; null sentinels; change kinds and thresholds; expectation rules including the interval rule for
  approximate values.
- `ProfileRulesTest` (pure): row conditions against hand-counted violations, the not-evaluated count on
  null operands, `matches`, every aggregate rule kind, the robust z-score of the window including MAD = 0.
- `ProfileCellsTest` (pure): merge associativity; cells against exact counts; the rest-of-axis subtraction;
  ties collapsing edges; declared edges.
- `ProfileTransformTest` (config-driven e2e, as `SelectTransformTest`): each output's records with
  `PAssert`; the report file read back from `target/`; a same-distribution two-input run asserting
  `psi < 0.01` — the regression test of §7; a two-input `mode: compare`
  run over the same keys asserting no duplicate and full overlap, and null keys counted apart; a declared
  edge list leaving `ks` unchanged; a value table with two long values sharing a 256-character prefix
  reported as not exact.
- `ProfileSinkTest` and `ProfileReportPreviewTest` are deleted with the sink; the cases that still apply
  (type coercion, nested structs, `values: hide`, the size ladder, failure routing) move to the tests above.

## 9. To measure or decide before acceptance

1. **Cost of the second pass.** Expected 1.5–2× the sink's wall time on a wide input; unmeasured. The
   reference is the sink's measured 13 minutes for 916k rows × 649 fields on Dataflow. If the extracted rows are
   recomputed rather than reused, the source is read twice; whether to force a materialization (reshuffle)
   between Extract and the two passes is a measurement, not a guess.
2. **Edges as a side input at 650 fields** (650 × 99 doubles ≈ 0.5 MB): fine on Dataflow; to confirm on
   prism and DirectRunner with `enforceImmutability`.
3. **`previous: {input}`**: whether it is one of the module's `inputs` singled out by name (it must then be
   excluded from the union) or a `sideInputs` entry. The second is cleaner if the common `sideInputs` field
   delivers a collection to a transform the way the `query` transform's `sideinput` lookup source uses it.
4. **Optional outputs.** dsl §3 makes the heavy outputs explicit (`outputs`). If a module can know at
   assembly which of its outputs are consumed, the parameter becomes unnecessary; that is a property of the
   assembly loop, not of this module.
5. **Group distinct counts.** The sink reports a per-group distinct estimate for string fields (a CPC sketch
   per group × field). The proposal drops it — it is the only group statistic that would still need a sketch
   per group × field. Add it back only if a use for it is named.
6. **Size of the key sketches in the payload.** A compact Theta sketch at the default accuracy is tens of
   kilobytes; a handful of keys is negligible, `accuracy: high` with many keys is not measured. If it
   matters, the payload carries them at the default size whatever `accuracy` says.
7. **Null operands in a row condition.** dsl §9.2 counts a row whose condition reads a null field as
   not evaluated. Whether the shared filter condition tree can report "unknown" apart from "false" has to be
   checked; if it cannot, the tree's evaluation needs a three-valued variant used by this module only.
8. **`run.partition` as a date.** Freshness from the end of the slice and `same: weekday` need the label
   parsed; which formats are accepted (a date, a timestamp, `yyyyMMdd`) and what an unparsable label does
   (the two features are skipped, with a note in the summary) is to be fixed with the user-facing reference.

## 10. Not part of this design

The failure of the `bigquery` source on nested STRUCT results of a `query` read (the declared Avro schema
names nested records `root.<field>.<Field>`, the coder resolves unions by record full name, and the table
read path already avoids this by taking the read session's schema) blocks the zero-configuration use of the
profile on nested tables. It is a source fix with its own regression test, tracked separately.
