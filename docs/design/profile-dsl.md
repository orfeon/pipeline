# Profile Transform DSL (Design Document)

Status: **Stage 1 implemented** — the contract of the `profile` *transform* that replaced the `profile`
sink: the same observations, emitted as record outputs any sink can store, with the HTML report as an
optional file the transform writes. `module: profile` accepts today what §13 lists under stage 1; the
parameters of stages 2 and 3 are rejected at assembly with a "not implemented yet" message rather than
ignored. §14 is what is deliberately left out. The execution side is [profile-engine.md](profile-engine.md);
the user-facing reference is `src/main/resources/server/docs/module/transform/profile.md`.

## 1. Purpose and position

The `profile` transform observes a dataset and reports **what it looks like**: per-field statistics, how
the fields differ between groups (segments, time buckets, inputs), how they changed since a previous run,
and which declared expectations do not hold. It is the unsupervised member next to the learner-free trio
feature → screen → evaluation, and it is not specific to machine learning: the same run serves the
acceptance of a rebuilt table, a migration check (before against after), a month-over-month comparison and
the inventory of a training matrix.

It replaces the `profile` sink for three reasons the sink cannot answer:

- **The result is not readable by a program.** A report over several hundred fields embeds tens of
  megabytes of JSON in HTML; a consumer (a script, an agent, a later step) has to extract and filter it.
- **Nothing accumulates.** "Since which version is this field entirely null" needs one row per field per
  run in a table, not one file per run.
- **Nothing can follow it.** The sink emits one summary record, so no later step can act on "a field
  became entirely null".

### 1.1 Design principles

- **Observe, never gate.** The transform counts and reports. It never fails a pipeline on data content;
  expectations (§9) are counted and emitted, and whoever consumes the output decides what to do.
- **Zero configuration is useful.** With no parameters the default output is one record per field. Every
  declaration (`keys`, `segments`, `time`, `target`, `previous`, `expectations`) adds one output or one set
  of columns.
- **Exact where it is cheap, bounded where it is not, and the record says which.** Counts, null counts,
  minima, maxima, moments and bin counts are exact. Quantiles, distinct counts and frequent values of
  high-cardinality fields are sketch estimates and carry `_lo` / `_hi` columns. No estimate is emitted
  without its bounds.
- **A comparison statistic is computed from exact counts.** PSI, KS, total variation and the information
  value are functions of bin counts; the counts come from a counting pass over fixed edges, not from sketch
  queries (§6). This is the change that motivated the redesign: sketch-derived bin counts gave a PSI of
  0.1–0.24 between two samples of one distribution (engine doc §7).
- **Outputs are a contract.** Every record carries the run identity and `formatVersion`; a column is added,
  never renamed or repurposed, within a format version.
- **Domain-neutral.** The transform knows fields, keys, groups, a time field and optionally one outcome
  field. It has no notion of a model, a baseline prediction, a residual or leakage (§14).

### 1.2 Relation to the screen and evaluation transforms

The line is whether a prediction is the subject. Screen and evaluation ask about the relation between an
outcome, a baseline and candidate or prediction columns; the profile asks what the columns themselves look
like.

| | profile | evaluation |
|---|---|---|
| question | what does this data look like, and how does it differ from a previous run or another population | does this prediction add information over the baseline |
| required declarations | none | `label`, `predictions`, `splits` |
| columns | every field on equal terms | roles: outcome, baseline, prediction sets |
| unit | the row | the unit (a group, or an independent row) |
| statistics | null rates, quantiles, distinct counts, KS / PSI, changes, expectation results | excess log score, hit rate, Brier, bootstrap intervals, calibration |
| rows it cannot use | counted and kept (a value it cannot read is a field error) | a unit with an invalid prediction or baseline leaves every comparison |
| time | the look of each bucket, freshness, missing buckets | splits with a `selection` / `report` role |

Three places where the two could be mistaken for one another:

- **Rate per bin.** `target` + `bins` on a prediction column against the evaluation transform's
  `calibration`: see §8. Calibration belongs to evaluation.
- **Cuts of the data.** `segments` gives the distribution of every field within each group; the evaluation
  transform's `slices` and slice discovery give a metric of the prediction within each group. Both stay:
  "the data is different in this segment" and "the prediction is wrong in this segment" are different
  findings, and the first is often the explanation of the second.
- **Comparison with a previous run and the HTML report.** Each transform compares and reports its own
  outputs; what they share is the record identity and the `checks` shape (§4).

Used together: the profile runs on the training matrix before training (fields that are entirely null,
constant, or shifted between the time windows), evaluation verifies the predictions after it, and in
operation the profile runs on the inputs and on the prediction columns on every load (`preset: monitor`) —
it needs no outcome, so it reports a shift before the outcomes that evaluation needs exist. When an
evaluation metric degrades, the profile's history of the same slices is where the search for the cause
starts.

## 2. Module and migration

```yaml
transforms:
  - name: itemsProfile
    module: profile
    inputs: [items]
    parameters:
      output:
        report: gs://mybucket/reports/items.html     # optional: also write the HTML report

sinks:
  - name: itemsProfileFields
    module: bigquery
    inputs: [itemsProfile]                            # the default output: one record per field
    parameters:
      table: myproject.quality.profile_fields
      writeDisposition: WRITE_APPEND
```

A config with only `sources` and the `profile` transform is valid: when `output.report` is set the report is
written and nothing else needs to consume the transform.

**No compatibility with the sink.** The `profile` sink is removed in the change that adds the transform;
there is no alias and no deprecation period. A sink config is rewritten by hand: the module moves to
`transforms`, `output` becomes `output.report` (there is no default report location — no `output`, no
file), `compareWith` becomes `previous`, and the sink's one output record becomes the `summary` output.
Reports and sketch files written by the sink are not read (§9.1). Parameters described below as "as in the
sink" keep their meaning; nothing else of the sink's behaviour is promised.

## 3. Parameters

All optional. Parameters marked *kept* have the meaning they have in the sink.

| parameter | type | description |
|---|---|---|
| `output` | String or Object | Files the transform writes. A string is the report uri. Object form: `report` (HTML), `payload` (the report's JSON payload and manifest as one JSON file, §10.2). Omitted: no file is written. |
| `outputs` | Array<String\> | The optional record outputs to compute: any of `bins`, `values`, `sketches`, `sample` (§5). The others are always produced. Default `[bins, values]`. |
| `run` | Object | `{id, dataset, partition}` — the identity written to every record (§4). Defaults: the job name, the module name, none. |
| `preset` | Enum | `full` (default) or `monitor` — the defaults of a frequent run (§9.5). |
| `fields` | Object | *kept* — `{include: [...]}` / `{exclude: [...]}`, dot paths. |
| `values` | Enum | *kept* — `show` / `hide`. `hide` removes raw values from every output and file (§11). |
| `unnest` | String | An array-of-struct field to expand into rows before profiling (§7.4). |
| `keys` | Array | Identifier declarations (§7). |
| `segments` | Array | *kept* — fields to compare by, `[category]` or `[{field, topK}]`. |
| `time` | String or Object | *kept* — `{field, granularity}`, plus `timezone` (an IANA zone id, default `UTC`) for the bucket boundaries and `groups` (default true; false keeps freshness and the bucket row counts of §9.3 without profiling each bucket). |
| `mode` / `baseline` | Enum / String | *kept* — `mode: compare` makes each input a comparison group; `baseline` names the reference input. |
| `compare` | Array<Array<String\>\> | *kept* — declared comparable numeric field pairs. |
| `target` | String or Object | The outcome field every other field is related to (§8). |
| `bins` | Object | The counting pass (§6): `{mode: exact \| sketch, count: 10, edges: {<field>: [...]}}`; `edges: previous` reuses the edges of the `previous` run (§6.2). |
| `drift` | Object | `{axis, exclude: [...]}` — the axis whose groups feed the `drift` columns of the default output (§6.3), and fields left out of the drift ranking (their statistics are still emitted). |
| `previous` | Object | The run to diff against (§9.1): `{payload: uri}`, `{report: uri}`, or `{input: <name>}`; with `window`, the recent runs to measure against (§9.4). |
| `expectations` | Array<Object\> | Declared expectations (§9.2). |
| `accuracy` | Enum | *kept* — `low` / `default` / `high`; now also sizes the frequent-items map (§5.4). |
| `associations` | Object | *kept* — `{numeric: all \| none}`. |
| `sample` | Object | *kept* — `{enabled, k}`. |
| `report` | Object | *kept* — `{title}`. |
| `fanout` | Integer | *kept*. |

Batch (bounded) inputs in the global window only.

## 4. Record identity

Every record of every output starts with the same columns:

| column | type | |
|---|---|---|
| `runId` | STRING | `run.id`, default the job name |
| `dataset` | STRING | `run.dataset`, default the module name — *what* was profiled; one history table holds many datasets |
| `partition` | STRING | `run.partition` — *which slice* of the dataset (a logical date, a version label); null when not given |
| `generatedAt` | TIMESTAMP | one instant per run, shared by all outputs |
| `formatVersion` | INT64 | the version of this contract; starts at `2` so that it cannot be taken for the sink's payload (`1`), which is not read |

Appending the outputs of successive runs to one table per output gives the history: the question "in which
run did this field become entirely null" is one query over the default output.

`partition` is separate from `generatedAt` because the two differ whenever a slice is backfilled or re-run:
the history is ordered by `partition` when it is given, by `generatedAt` otherwise. A re-run of the same
slice appends a second set of records; the natural key of an output is (`dataset`, `partition`, the
output's own key columns), and of two record sets with that key the later `generatedAt` is the current
one. A consumer that wants one record set per slice upserts on that key or reads the latest.

Estimates are named `<stat>` with `<stat>_lo` / `<stat>_hi` — the naming of the evaluation transform's
intervals; here the bounds are the sketch's two-standard-deviation bounds. A statistic without those columns
is exact, with two stated exceptions: the quantile columns of §5.1 (`p01` … `p99`, `lengthP50`), whose error
is the record's `rankError`, and the statistics read off the cell grid of §6.1 (`ks`, `groups.p50`), which
are exact functions of exact counts but resolved to one cell.

The identity columns and the shape of a `checks` record (§9.2) are meant to be shared with the evaluation
transform, not owned by this one: a history table keyed by (`dataset`, `partition`) and a notification
driven by `checks` should serve a data profile and a prediction evaluation of the same slice alike.
Adopting them in the evaluation transform's `summary` and its designed-but-unbuilt reporting layer
(evaluation-dsl.md §11) is a proposal for that document, outside this one.

## 5. Outputs

Referenced as `<name>` (the default) and `<name>.<output>`.

### 5.1 Fields (the default output)

One record per profiled field.

`field`, `type` (`numeric` / `string` / `bool` / `timestamp` / `array`), `sourceType`, `rows`, `count`
(non-null, readable), `nulls`, `nullRate`, `errors`, `distinct` / `_lo` / `_hi`, `distinctExact` (true when
the value table of §5.4 is complete, in which case the bounds equal the estimate);
numeric-like: `min`, `max`, `sum`, `mean`, `stddev`, `skewness`, `zeros`, `nans`, `infs`, `p01`, `p05`, `p25`,
`p50`, `p75`, `p95`, `p99`, `rankError` (the quantile sketch's normalized rank error);
string: `empties`, `blanks` (whitespace only, full-width included), `lengthMin`, `lengthMax`, `lengthP50`,
`top` (the most frequent value; null with `values: hide`), `topShare`;
bool: `trues`, `falses`;
`nullLike`, `nullLikeRate` — rows whose value is a null sentinel (§5.2), and the null rate if they were
counted as null;
`notable` (ARRAY<STRING\>, §5.2);
with a target: `association` and its kind (§8); with a drift axis (§6.3): `drift`, `driftKind`, `driftVs`,
`nullShift` (§6.3).

### 5.2 Notable codes

Ordered by severity; consumers filter the default output on this column.

| code | meaning |
|---|---|
| `all_null` | no non-null value |
| `null_like_only` | every non-null value is a null sentinel |
| `constant` | one distinct value (the value is in `top` for strings, `min` for numerics) |
| `high_null` | null rate above 0.5 |
| `null_like_value` | a null sentinel among the frequent values |
| `dominant_value` | one value holds more than 90% of the rows |
| `skewed` | absolute skewness above 2 |
| `unique_like` | distinct ≈ rows, for string and integer fields only (a continuous field is expected to be) |

Null sentinels: the strings `null`, `NULL`, `None`, `NaN`, `N/A`, `NA`, `-`, the empty string and
whitespace-only values (compared after trimming, case-insensitively for the words). They are reported, never
converted: the profile still counts such a value as non-null.

### 5.3 Groups (`<name>.groups`)

Present with `segments`, `time` or `mode: compare`. One record per axis × group × field.

`axis` (`segments:<field>` / `time:<field>` / `inputs`), `group`, `baseline` (true for the reference group
of the axis), `field`, `groupRows`, `count`, `nulls`, `nullRate`, `min`, `max`, `sum`, `mean`, `stddev`,
`p50` (interpolated from the cells of §6.1, exact to one cell), and against the reference of the axis
(null on the baseline record of the `inputs` axis): `ks` (numeric-like), `tvd` (string / bool), `psi`,
`nullShift`, `noiseKs`, `noisePsi` (§6.3).

The baseline of the `inputs` axis is `baseline`; `segments` and `time` axes take the rows outside the group
as the reference (each group against the rest), so the drift columns are populated on every axis.

Groups are bounded: the largest `topK` per segment field, the most recent 60 time buckets, every input.
With `time: {groups: false}` the time axis has no records here (§9.3).

`sum` is exact and is there for reconciliation: with `mode: compare`, the row counts and the sums of two
inputs (a source and its copy, a table before and after a migration) are compared as they are, not through
a mean.

### 5.4 Values (`<name>.values`)

One record per field × value, for string, bool and **low-cardinality numeric** fields.

`field`, `value` (STRING; `#<rank>` with `values: hide`), `rank`, `count`, `count_lo`, `count_hi`, `share`,
`exact`.

A field with at most `1000` distinct values, none longer than 256 characters, is tabulated exactly
(`exact: true`, bounds equal the count; the table holds the values whole — a value is never shortened to
make it fit, since two long values sharing a prefix would then count as one) —
this is what makes a discrete numeric field readable (a publication lag concentrated on three values is
three records, not a histogram bar). Above that the records are the frequent-items estimate; the map size
follows `accuracy` (512 / 1024 / 4096). When the no-false-positive query returns nothing — values spread
evenly over hundreds of distinct values — the records are the no-false-negative candidates with their
bounds, so the output says "no value is frequent enough to be certain of, the largest is between a and b"
instead of being empty.

### 5.5 Bins (`<name>.bins`)

One record per field × group × bin; `axis` and `group` are null for the whole dataset, and the two target
classes appear as `axis: target`.

`field`, `axis`, `group`, `bin`, `lower`, `upper` (numeric-like) or `value` (categorical; `(other)` and
`(null)` are bins of their own), `count`, `share`, and with a target: `positives`, `rate`, `rate_lo`,
`rate_hi` (binary) or `targetMean`, `targetSe` (numeric), `edgesKind` (`quantile` / `declared` /
`values`), `exact` (false only under `bins.mode: sketch`).

### 5.6 Target (`<name>.target`)

Present with `target`. One record per field other than the target (§8).

### 5.7 Keys (`<name>.keys`)

Present with `keys` (§7). Records of `kind: key` (one per declared key, per input under `mode: compare`) and `kind: overlap` (one per
comparison of §7.2 and §7.3).

### 5.8 Changes (`<name>.changes`)

Present with `previous` (§9.1). One record per field × change kind.

### 5.9 Checks (`<name>.checks`)

Present with `expectations` (§9.2). One record per rule × matched field.

### 5.10 Summary (`<name>.summary`)

One record per run, complete enough that a later step can act without reading the other outputs: `rows`, `errorRows`, `fields`, `report` (the uri, null when none was written), `reportBytes`,
`degradations` (ARRAY<STRING\>, §10.3), one count per notable code (`allNull`, `constant`, …), one count
per change kind, `checks`, `checksFailed`, `checksFailedWarn`, `checksFailedError` (§9.2), and with `time`:
`latest` (the largest value of the time field), `freshnessSeconds` (`generatedAt` − `latest`),
`freshnessFromPartitionSeconds`, `missingBuckets` and `missingBucketLabels` (§9.3), and with `unnest`:
`grain`, `parents`, `parentsEmpty`, `parentsNull` (§7.4). A step that should run after the report is written waits on this output.

### 5.11 Sketches and sample (`<name>.sketches`, `<name>.sample`)

Opt-in through `outputs`. `sketches`: one record per field × sketch kind — `field`, `kind` (`kll` / `cpc` /
`frequentItems` / `theta`), `parameters`, `binary` (BYTES); the whole dataset only. `sample`: one record per
sampled row, `row` (JSON). Both are absent under `values: hide` where they would carry raw values
(`frequentItems`, `sample`).

The sample is no longer part of the sketch artifact: in the sink it was the reason a sketch file over 649
fields weighed 336 MB and the reason sketch binaries dropped out of most reports.

### 5.12 Pairs (`<name>.pairs`)

Present with `compare`. One record per declared field pair: `a`, `b`, `countA`, `countB`, `ks`, `psi`,
`noiseKs`, `noisePsi`. The two fields are counted in the second pass over one set of edges — the
equal-frequency cells of the two fields' values pooled (§6.1) — so the statistics are exact like those of a
group against its baseline. The report's Q-Q plot of a pair stays a sketch query (quantiles of each field at
fixed ranks).

## 6. Bins and comparison statistics

### 6.1 The counting pass

The transform reads its input twice. The first pass builds the whole-dataset profile (the statistics of
§5.1) and, from its quantile sketch, the **edges** of every numeric-like field: the 100 equal-frequency
cells of the whole dataset (fewer where ties collapse edges). The second pass counts, exactly, the rows of
every group and target class in every cell, and for categorical fields in every value of the whole-dataset
value list: the most frequent 50 values — of the exact table when the field has one, of the frequent items
otherwise — plus `(other)`. (The `values` output still carries the complete exact table; the cap applies to
what is counted per group, where a 1,000-value field would cost a 1,000-cell array per group.)

Everything a comparison needs is a function of those counts:

- `bins` are the cells merged into `bins.count` (default 10) equal-frequency bins.
- KS is the largest cumulative difference over the cells (a 100-point grid).
- PSI and the information value are computed over the merged bins, with the 0.5-per-bin smoothing the sink
  already uses for the information value. No epsilon floor is needed: an equal-frequency bin is never empty
  on the pooled side.
- The group median is interpolated within its cell.

`bins.edges` declares the edges of a field's `bins` records (`edgesKind: declared`): this is how a sparse
region is resolved on purpose — the price bands above the 99th percentile hold a fraction of a percent of
the rows and equal-frequency bins fold them into one — and how bins are made to follow a convention the
data does not know (price bands, age bands). The declared edges are counted *next to* the quantile cells,
not instead of them: `bins`, PSI and the information value of that field use the declared edges, while KS
and the group median keep the 100-cell grid, so declaring three price bands does not reduce a KS to three
points.

A declared field pair (§5.12) is counted the same way, each of its two fields over the cells of the two
pooled. The second pass is skipped altogether when no output needs it (§9.5).

`bins.mode: sketch` skips the second pass: group sketches are queried at the same edges, `exact` is false
and the comparison statistics carry the sketch error. It exists for inputs that are expensive to read twice.

### 6.2 What is reproducible

Counts are exact *for the edges on the record*. The edges come from a sketch and can move by its rank error
between two runs over the same data, so two runs agree exactly when the edges are the same: declared edges,
or `bins.edges: previous`, which reuses the edges of the `previous` run and makes a history of bin counts
comparable bin by bin. With quantile edges the counts of two runs differ by the rows between the two edge
positions, and the derived statistics differ in the third decimal (engine doc §7).

### 6.3 Drift columns

A group is compared with its baseline (§5.3) on three separate things, because they fail separately:

- **Distribution of the non-null values**: `ks` for numeric-like fields, `tvd` (total variation distance)
  for string and bool fields; `psi` for both.
- **Missingness**: `nullShift` = the group's null rate minus the baseline's. A field whose values are
  unchanged and whose null rate went from 0 to 8% has `ks` ≈ 0 and is the finding.
- **Presence**: a field that exists on one side only is a `changes` record (§9.1), not a drift value.

`drift` on the default output is the largest `ks` / `tvd` over the groups of **one** axis (`driftKind` says
which statistic, `driftVs` the group, `nullShift` the largest null-rate difference on the same axis). The
axis is `drift.axis` when given (`inputs`, `segments:<field>` or `time:<field>`), otherwise the `inputs`
axis when `mode: compare` declares one, otherwise none: with only `segments` or `time` declared the default
output has no drift columns and the per-group values are read from `groups`. A segment or a time bucket
differing from the rest is usually what the axis was declared to show, not a finding about the field, so it
is not promoted to the field record unless asked for. `drift.exclude` removes fields that differ by
construction (the time field the inputs were split on).

`noiseKs` = 1.36·√(1/n₁ + 1/n₂) and `noisePsi` = (bins − 1)(1/n₁ + 1/n₂) are the sizes the statistic reaches
between two random samples of one distribution. They are reference columns and take part in no ranking: when
the two sides are samples (one day against a year) a value below them is not a difference; when the two
sides are complete populations a difference is a difference whatever its size.

## 7. Keys

```yaml
keys:
  - item_id                                    # shorthand
  - {fields: [order_id, line_no], unique: true}
  - {fields: [user_id], domain: user}
  - {fields: [seller_id], domain: user}
```

### 7.1 Key record

`kind: key`, `key` (the fields joined by `+`), `axis`, `group`, `distinct` / `_lo` / `_hi`, `keyness`
(distinct / rows, capped at 1), `nullKeys`, and with `unique: true`: `duplicateKeys` and `duplicateRows` —
exact, counted by a shuffle on the key hash (a set sketch cannot tell 146 duplicated rows in a million from
none). A composite key is the tuple of its fields.

- **Uniqueness is a property of one input.** With `mode: compare` there is one key record per input
  (`axis: inputs`, `group` the input) and duplicates are counted within each input; a key present in two
  inputs is not a duplicate — that is the overlap of §7.2, and in a migration check it is the expected
  result. With `mode: union` the inputs are one dataset and there is one record (`axis`, `group` null).
- **A null key is not a key.** A row whose key is null — any field of a composite key — is counted in
  `nullKeys` and takes no part in `distinct`, `duplicateKeys`, `duplicateRows` or an overlap. Whether null
  keys are acceptable is an expectation on `nullKeys` or on the fields' `nullRate`.

### 7.2 Overlap record

`kind: overlap`, `key`, `otherKey`, `axis`, `group`, `otherGroup`, `intersection` / `_lo` / `_hi`,
`containment` (the share of `key` found in `otherKey`).

Overlaps are emitted only where they mean something:

- between two keys of the same declared `domain` (`seller_id` in `user_id`: referential containment), and
- for one key between the groups of an `inputs` axis (the share of this period's users seen in the
  baseline period).

Two keys without a common domain are not compared: two unrelated ten-digit identifiers overlap by accident.

### 7.3 Against a previous run

With `previous` as a file, a key gets `kind: overlap` records against the previous run (`group: previous`):
the retained share (previous keys still present) and the new share (current keys not seen before). The set
sketch of every declared key is part of the payload (§10.2) for exactly this purpose, so a payload or report
file is always enough. A set sketch holds hashes, not values, and is included under `values: hide` too.

With `previous: {input}` the previous run is field records, which carry no sketch: the comparison is not
made, and a `changes` record of kind `key_overlap_unavailable` says so for each key.

### 7.4 Nested rows

`unnest: lines` expands one array-of-struct field into rows before profiling: the child's fields become
`lines.<field>` and every parent field is repeated on each child row. `keys`, `segments`, `time` and
`target` may then name child fields. The row unit of every output is the child — parent fields are counted
once per child, which is stated in the `summary` (`grain: lines`); to profile the parents at their own
grain, run without `unnest`.

A parent whose array is empty or null yields no row: a row of null child fields would be a child that does
not exist, and would raise the null rate of every child field. So that such parents do not vanish
unnoticed, the `summary` counts them: `parents`, `parentsEmpty`, `parentsNull`. `rows` is the number of
child rows.

Without `unnest` an array field is profiled as its element count.

## 8. Target

`target` relates every other field to one outcome field. It is an optional view, not the purpose of the
module.

| form | outcome | per bin (`bins`) | per field (`target` output) |
|---|---|---|---|
| `sold_flag`, `{field, positive}` | binary | `positives`, `rate` with a Wilson interval | `ks` or `tvd`, `iv`, `pointBiserial`, `rateWhenNull`, `nullRows` |
| `{field, type: numeric}` | numeric | `targetMean`, `targetSe` | `correlation`, `eta2` (share of the target variance between the bins), `meanWhenNull`, `nullRows` |

`association` on the default output is `ks` / `tvd` for a binary target and `|correlation|` or `eta2` for a
numeric one (`associationKind` says which). The information value is reported as one statistic among these
and ranks nothing by default.

The transform does not flag a field as a leak. A strong association is a number; whether it is suspicious
depends on what else is known about the field, and that judgement is an expectation (§9.2) or the
business of the screen transform.

**A prediction column is not a special case.** Pointed at a probability column, `target` and `bins` yield a
table that looks like a reliability diagram: the realised rate per bin of the prediction. It is the
relation of one numeric field to the outcome and nothing more — it has no baseline next to it, no split, no
unit and no rule for rows a prediction is invalid on. Calibration of a prediction is the evaluation
transform's `calibration` (evaluation-dsl.md §7), which bins by the prediction, by its divergence from the
baseline or by declared bands, per split and per prediction set (§1.2).

## 9. Changes and expectations

### 9.1 Changes against a previous run

`previous` names the run to diff against: the `payload` or `report` file of that run (written by this
transform — a file of the sink, format version 1, is rejected when it is read), or `{input: <name>}` — a collection holding the default output of earlier runs, of which the
latest run of the same `dataset` before this one is taken, in the order of §4 (`partition` when given,
`generatedAt` otherwise; typically a `bigquery` source over the history table).

`changes` records: `field`, `change`, `previous`, `current`, `detail`.

| change | condition |
|---|---|
| `added` / `removed` | the field exists on one side only |
| `type_changed` | profile type differs |
| `became_all_null` / `recovered_from_all_null` | `all_null` on one side only |
| `became_constant` / `no_longer_constant` | `constant` on one side only |
| `null_shift` | null rate moved by more than 0.05 (absolute) |
| `null_like_shift` | null-sentinel rate moved by more than 0.05 |
| `distinct_shift` | distinct count outside the other side's bounds by more than a factor of 2 |
| `distribution_shift` | `ks` / `tvd` against the previous run above 0.1 and above its noise size |
| `row_count` | one record for the dataset: rows before and after |
| `deviation` | with `previous.window` only: a quantity outside its recent range (§9.4) |
| `key_overlap_unavailable` | one record per key when the previous run carries no set sketch (§7.3) |

The thresholds are defaults of the *listing*, not judgements — they decide what is worth a record — and
are parameters (`previous.thresholds`). The first seven kinds need only the previous run's field records;
`distribution_shift` needs its bins or payload and is omitted, with a `detail` saying why, when they are
not available.

This output is the first thing to read after rebuilding a table: which fields disappeared, went null, went
constant or lost values.

### 9.2 Expectations

```yaml
expectations:
  - {fields: ["*"],          nullRate: {max: 0.2}}
  - {fields: [list_price],   notConstant: true}
  - {fields: [list_price],   range: {min: 0}}
  - {fields: [category],     distinct: {max: 50}}
  - {key: item_id,           unique: true}
  - {pair: [list_price, sold_price], ks: {max: 0.1}}
  - {changes: [removed, became_all_null], count: {max: 0}}
  # row rules: counted per row
  - {rows: "sold_at >= listed_at", name: sold_after_listed}
  - {rows: "status IN ('listed', 'sold', 'cancelled')", violations: {maxRate: 0.001}}
  - {fields: [seller_email], matches: "^[^@\\s]+@[^@\\s]+$"}
  # freshness and volume (§9.3)
  - {freshness: {max: 2h}, severity: error, owner: marketplace-data}
  - {rowCount: {min: 100000}}
  - {buckets: {missing: {max: 0}}}
```

Three kinds of rule:

- **Aggregate rules** bound a statistic of the default output (`nullRate`, `nullLikeRate`, `distinct`,
  `range`, `notConstant`), of a key (`unique`), of a pair (`ks`, `psi`), of the changes (`count` per kind)
  or of the run (`freshness`, `rowCount`, `buckets`). `fields` takes names and globs.
- **Row rules** state a condition every row should satisfy. `rows` is a filter condition in the shared
  SQL-like syntax (`module/common/filter.md`: comparisons between fields, arithmetic, `IN`, `LIKE`,
  `BETWEEN`, `IS NULL`); `matches` is a regular expression a string field's non-null values should match.
  The rows that violate the condition are counted exactly in the first pass. A row on which the condition
  cannot be evaluated because a field it reads is null is counted separately (`notEvaluated`) and is not a
  violation: missingness is `nullRate`'s business. The default bound is no violation
  (`violations: {max: 0}`); `max` and `maxRate` relax it.
- **Row rules keep examples.** Up to five violating rows per rule are kept (the first seen per bundle, so
  not a random sample) as JSON in `examples`; none under `values: hide`.

Every rule may carry `name`, `severity` (`warn`, the default, or `error`), `owner`, `tags` and
`description`. The transform does nothing with them except copy them to the records, so that whatever reads
`checks` can route and word a notification without a second configuration.

Each rule yields one `checks` record per matched field (or key, pair, change kind, or one for the dataset):
`rule` (its index), `name`, `expectation`, `field`, `observed`, `expected`, `passed`, `approximate` (true
when `observed` is an estimate, in which case the rule passes only if the whole interval does), `severity`,
`owner`, `tags`, `description`, and for row rules `violations`, `violationRate`, `notEvaluated`, `examples`.

Nothing fails. `summary.checksFailed` (and its `Warn` / `Error` split) and the `checks` output are what a
later step reads: a sink that stores them, an action triggered per failed record, a job wrapper that
inspects the summary.

### 9.3 Freshness and volume

Declaring `time` is enough for both, and neither needs the buckets to be profiled: freshness is a maximum
of the first pass and the bucket row counts are one small count shuffle. `time: {field, groups: false}`
keeps exactly these two and drops the per-bucket records of `groups` — and with them the reason for a
second pass (§9.5).

- **Freshness.** `summary.latest` is the largest value of the time field and `summary.freshnessSeconds` its
  distance from `generatedAt`. With `run.partition` given as a date or timestamp the distance is also
  reported from the end of that slice (`freshnessFromPartitionSeconds`), which is the meaningful number for
  a backfill.
- **Volume per bucket.** The `groups` output has `groupRows` per time bucket (when the buckets are
  profiled). What it cannot show is a bucket with no row at all, so the run reports the buckets missing
  between the first and the last observed one: `summary.missingBuckets` (the count) and `summary.missingBucketLabels` (the first 100).
  Calendar gaps that are expected (no rows on weekends) are a matter of the expectation's bound, not of the
  count.

### 9.4 Measuring against recent runs

One previous run cannot tell a change from the ordinary variation of a quantity that moves every day — a
daily row count above all. `previous: {input: <name>, window: 28}` takes, instead of the latest run alone,
the latest 28 runs of the same `dataset` before this one (ordered as in §4), and `same: weekday` restricts
them to the runs whose `partition` (or `generatedAt`) falls on the same day of the week.

For the dataset's row count and for each field's `nullRate`, `distinct`, `mean` and `sum`, the run then
reports where the current value stands among those runs: `changes` records of kind `deviation` with
`historyRuns`, `historyMedian`, `historyMad` and `deviation` = (current − median) / (1.4826 · MAD), listed
when |deviation| exceeds 3 (a parameter, `previous.thresholds.deviation`). A quantity that did not vary in
the window (MAD = 0) is listed when it differs from the median at all. The kinds of §9.1 are still computed
against the latest run.

This is a robust z-score and nothing more. Trend and seasonality models are not in scope (§14); the
`same: weekday` selection is the one concession to the most common periodicity.

### 9.5 The monitor preset

A profile that runs on every load is paid for every time. `preset: monitor` changes the defaults to what a
recurring check needs: `associations: {numeric: none}` (the correlation co-moments are quadratic in the
number of numeric fields and dominate the per-row cost of a wide input), `sample: {enabled: false}` and
`outputs: []`, and `time.groups: false`. Every parameter set explicitly overrides the preset.

Independently of the preset, the second pass runs only when something needs it — a profiled group axis
(`segments`, `mode: compare`, `time` with `groups: true`), a target, a declared pair, or the `bins` output.
A monitor run with expectations on the default output, row rules, freshness and missing buckets reads its
input once: the first pass, plus the bucket count shuffle when `time` is declared and one key shuffle per
`unique` key.

## 10. The report

### 10.1 HTML

`output.report` writes a single-file HTML report in the form of the sink's, rendered from the same
aggregates as the record outputs — the two cannot disagree. Its content follows from the above:
drift ranked by KS / TVD with the null shift next to it, notable fields in severity order with a count per
code ahead of the list, bins from the counting pass, the changes and checks as tabs, and no leak badge.

Two things the report keeps and one that changes shape:

- The whole-dataset field cards (256-bin histogram, CDF, quantiles, frequent values) are pass 1 results and
  are unchanged.
- The "Next steps" tab — declarations the run could have used (`keys`, `segments`, `time`, `target`
  candidates inferred from names and cardinalities) as copy-pasteable snippets — is kept, and gains
  `expectations` candidates read off the run (a field with no null today suggests `nullRate: {max: 0}`).
  The suggestions are part of the payload, not of the record outputs.
- **Group overlays.** The sink overlays each group's histogram over 64 equal-width bins. A group's
  distribution is now its counts over the equal-frequency cells, so the overlay is drawn as a density
  (count ÷ cell width) on the same value axis. The body of a distribution is resolved more finely than
  before and each tail is one cell wide: an overlay no longer shows structure inside the outer 1% unless
  `bins.edges` declares edges there. The per-bin target chart changes the same way (10 bins by default,
  each holding a tenth of the rows, instead of 64 bins most of which held almost none).

### 10.2 Payload file

`output.payload` writes the report's embedded payload and manifest as one JSON file. It is the input of
`previous: {payload: ...}` and of scripts that want the whole run in one object; the structure is specified
with `formatVersion` in the user-facing reference.

The payload — embedded or as a file — always carries what a later run needs from this one and cannot
recompute: the edges and counts of the whole-dataset bins (`bins.edges: previous`, `distribution_shift`)
and the set sketch of every declared key (§7.3). Neither is subject to the size ladder of §10.3.

### 10.3 Size

The report keeps a size limit and a fixed degradation order, recorded in `summary.degradations` and in the
manifest. The per-field sketch binaries are not embedded (they are the `sketches` output; the key set
sketches of §10.2 are the exception); the order is sample rows
→ correlation matrix reduced to the strongest pairs → histogram resolution → comparison groups → comparison
resolution. The record outputs never degrade.

## 11. Raw values

`values: hide` applies to every output and file: `values.value`, categorical `bins.value` and
segment group labels become ranks (`#1`, `group #1`), `fields.top` is null, and the `sample` output and the value-bearing sketch
binaries are not produced. Time bucket labels and input names are not values. The report and the outputs
are otherwise as sensitive as the source data.

## 12. Constraints and diagnostics

Rejected at assembly: an unbounded or windowed input; a `keys`, `segments`, `time`, `target`, `unnest`,
`bins.edges` or `expectations` field missing from the (unnested) schema; a `time` field that is not a
timestamp; a non-monotonic `bins.edges` list; `mode: compare` with fewer than two inputs; `previous.input`
that is not an input of the module; `bins.edges: previous` without a `previous` file (`payload` / `report`;
the edges are needed at assembly, engine doc §6); an `outputs` entry that is
not one of §3; a `rows` condition that does not parse or reads a field missing from the schema; a `matches`
pattern that does not compile or names a non-string field; `freshness` or `buckets` without `time`;
`previous.window` or a `deviation` threshold without `previous.input`; an unknown `severity` or `preset`;
an expectation that could never produce a meaningful record — a `key` rule naming a key that `keys` does
not declare (or `unique` on a key declared without `unique: true`), a `pair` rule naming a pair that
`compare` does not declare, a `changes` rule without `previous` (or naming `deviation` without
`previous.window`, or an unknown change kind); a `drift.axis` that is not a declared axis; a `previous`
file that is not of this transform's format (format version 1, or no payload), which also rules it out as
a source of `bins.edges: previous`.

Reported, never fatal: a target with one class only (`summary` and the report carry the warning); a
`previous` run that cannot supply what a comparison needs; a field skipped for its type or
depth (listed in the manifest).

## 13. Stages

1. **The transform and the counting pass — implemented.** `fields`, `groups`, `values`, `bins`, `pairs`,
   `target` (binary), `summary`; the full record identity of §4 (`dataset`, `partition`); §5.2 notable codes
   and null sentinels; `sum`; §6 with declared edges and `drift.axis` / `drift.exclude`; the report and the
   payload file written by the transform; the sink, its examples and its reference page removed. Also in
   this stage, carried over from the sink: `keys` as a list of field names with the `kind: key` records
   (`distinct` with bounds, capped `keyness`, `nullKeys`) — no `unique`, composite keys, domains or overlaps.
2. **Runs and declarations.** `previous` and `changes`; `expectations` and `checks` with all three rule
   kinds, severity and metadata (§9.2); freshness and missing buckets (§9.3) with `time.groups`; keys with
   `unique`, composite keys, `domain` and input overlaps, and the key set sketches in the payload (§10.2);
   the `sketches` and `sample` outputs; the numeric target.
3. **Recurring runs and shapes.** `previous.window` and `deviation` (§9.4); `preset: monitor` (§9.5);
   `unnest`; `time.timezone`; `bins.edges: previous`; `bins.mode: sketch` (moved here from stage 1: it needs
   the per-group and per-class sketch sets the counting pass replaced, a second code path worth building
   only once an input that cannot be read twice asks for it).

The identity columns and the `checks` columns are in the first two stages on purpose: they are the part of
the contract a history table and a notification depend on, and the part that is expensive to change later.

## 14. Out of scope

- **Anything conditioned on a prediction** (§1.2). Relating a field to what a baseline misses is the
  screen transform; the excess of a prediction over a baseline, calibration tables, and slices of a metric
  are the evaluation transform. A profile of a prediction column is an ordinary numeric profile.
- **Leak detection.** See §8.
- **Structure of repeated keys** — between-key and within-key variance, persistence of a quantity along a
  key's history, the distribution of repetitions per key. It needs a different input declaration (an entity,
  an order, a quantity) and a per-key pass; it is a candidate for a module of its own, not an output here.
- **A multi-class target** and **string format inference** (date-like, numeric-like, fixed-width): natural
  extensions with no design yet.
- **Notifying and stopping.** The transform emits `checks` and `summary`; an action, a sink or the job
  wrapper acts on them (§1.1).
- **Learned anomaly detection, trend and seasonality models.** §9.4 stops at a robust z-score over a window
  of runs: a judgement that cannot be explained in one line is not trusted in monitoring, and it is beyond
  counting.
- **Dashboards.** The history tables are read with a BI tool; the report describes one run.

Deferred, with the constraint each one carries:

- **Windowed (streaming) profiles.** With record outputs the natural form is one record set per window, and
  the report is not written. A window cannot be read twice, so the counting pass needs edges known in
  advance — declared, or taken from a previous run — or `bins.mode: sketch`.
- **Incremental profiles.** Profiling only the new slice and merging its sketches with stored ones gives the
  cumulative statistics that are sketch estimates or sums (quantiles, distinct counts, counts, moments), but
  no cumulative bin count over edges that have since moved. It needs the `sketches` output read back as an
  input.
