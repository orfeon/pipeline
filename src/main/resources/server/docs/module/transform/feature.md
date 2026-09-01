---
type: Transform Module
title: Feature Transform Module
description: Declarative feature generation for machine learning with built-in leakage checking. Features are declared in four scopes (row, context, sequence, population) against a sources contract that states when each field becomes known (availableAt) and when it reaches the system (ingestionLag); the module derives the availability of every output column, rejects columns that would use information unavailable at prediction time, and shifts history windows so late-arriving outcomes are only used once they would really be present. Batch mode; sequence/population features use per-key time-ordered history.
tags: [transform, feature, machine-learning, leakage, batch]
timestamp: 2026-08-23T00:00:00Z
---

# Feature Transform Module

Transform Module that generates machine-learning features from a relation using a declarative
specification. The specification separates **facts** about the input data (the *sources contract*:
field types, when a value becomes known, how late it reaches the system, whether rows get corrected)
from the **intent** (which features to compute, in which scope). From the two, the module derives for
every output column the time at which it is available and checks it against the prediction time, so a
configuration that would leak future information fails at pipeline assembly instead of producing a
model that cannot be served.

Supports:

- **row** scope — expressions, calendar decomposition (optionally cyclical sin/cos), fixed-edge binning,
  categorical crosses, per-value indicators, field equality, residuals against a named baseline.
- **context** scope — statistics relative to the rows that co-occur in the same group (rank, z-score,
  share of total, gap to best, percentile, median difference, group size, value counts / ratios, entropy).
- **sequence** scope — per-entity strictly-past history: lag, delta, trend, EWMA (decay by events or
  time), run length, events/days since a predicate last held, count of matching rows, windowed aggregates
  (count / mean / min / max / sum / std / first / last). Windows combine `maxEvents`, `maxAge` and a
  `filter` that can reference the current row through `$self.<field>`.
- **population** scope — expanding-fit encoding: conditional statistics of a target per key set
  (count / share / mean / rate / std / distribution), optionally windowed and offset by a baseline, with
  **shrinkage** along a generalization lattice (key → parent keys → global, `additive` main effects for
  crosses): fixed or variance-components pseudo-counts, leave-node-out, identity / logit / log scale,
  composed values, per-level deviations and effective sample size. **factorization** machines (fm / fwfm,
  ALS) over categorical fields: pair interaction scores, embeddings, linear predictor.
- **Leakage checking** — every column carries a derived availability time and lineage (source, kind,
  evidence). Columns available after `predictAt` are rejected unless they are only consumed as
  intermediates; history windows over late-arriving fields are shifted automatically.
- **Grouped output** — `output.groupBy` re-aggregates rows into one record per context with a child array.

The compiled plan (expanded columns, availability status, stages, diagnostics) is logged at assembly
time; warnings and hints from the compiler are part of that report.

## Transform module common parameters

| parameter  | optional | type                              | description                                                    |
|------------|----------|-----------------------------------|----------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.              |
| module     | required | String                            | Specified `feature`                                            |
| inputs     | required | Array<String\>                    | Input step names (flattened into one relation).                |
| waits      | optional | Array<String\>                    | Step names to wait for before processing.                      |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy (batch, global window recommended). Row / context features follow it; a static fit (and its artifact) is always computed over the whole input in the global window. |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                    |

## Feature transform module parameters

| parameter  | optional | type                           | description |
|------------|----------|--------------------------------|-------------|
| sources    | required | String or Object               | Sources contract (see below): inline object, or a URI / local path / `data:` reference to a YAML or JSON document. File content is rendered with FreeMarker using the step `args`. |
| lineage    | required | Array<Object\>                 | Maps input fields to their source: `{fields: [...], from: <source name>, eventTime: <field>}`. Every field used by a feature must be declared here. |
| time       | required | Object                         | `field`: the event-time field of the input (must equal the sources' `eventTime`). The transform re-timestamps every element from this field, so it is the time axis of all history windows regardless of the source's `timestampAttribute`; rows whose value is null go to the failure output. `orderTieBreak`: fields declaring a total order for rows that share a timestamp. |
| predictAt  | required | String                         | When the features are used: `event_time - PT10M`, `event_time` (the literal event time), ... Every emitted column must be available at or before this time. |
| entities   | optional | Array<Object\>                 | Subjects of sequence features: `{name, keys: [...], minInterval: <ISO8601>}`. |
| contexts   | optional | Array<Object\>                 | Co-occurrence groups for context features: `{name, keys: [...]}`. |
| baselines  | optional | Array<Object\>                 | Named baselines: `{name, expr, context}`. `expr` may wrap a numeric expression in a context op, e.g. `share(1 / price)`. Referenced by `type: residual` (`baseline:`) and encoding `offset:`. |
| features   | required | Array<Object\> or String       | Feature blocks (see scopes below). A string is a URI / path to a document whose `features` list is used. |
| fit        | optional | Object                         | Defaults for population features (overridable per block with `fit:`): `orderBy` (= time.field), `mode` (`expanding` \| `static` \| `fold`), `groupBy` (entity name: the fold unit), `folds` (number of folds for `fold`, default 5), `artifact` (`{uri, refit, id}` or the URI string — see *Static fits and artifacts* and *Out-of-fold fits*). `minHistory` is accepted but not implemented yet (warning). |
| engine     | optional | Object                         | Runtime knobs that do not change the plan. `parallelWaves` (default `true`): evaluate the independent stages of each wave in parallel and merge them by row id (see *Performance and sizing*); `false` runs the stages as one linear chain. `rowId`: input fields identifying a row (a natural key) for that merge; without it every row gets a random id pinned by one extra Reshuffle before the first fan-out. `spill`: the per-key sort of the keyed stages — `memoryMB` (in-memory buffer per key before sorted chunks are spilled to worker-local disk; default derived from the worker heap: a quarter of the heap shared by the cores, clamped to 16-256 MB; the `--featureSpillMemoryMB` pipeline option sets it for every feature step), `directory` (spill directory on the worker, default `java.io.tmpdir`), `compress` (deflate the chunk files, default false). See *Performance and sizing*. |
| output     | optional | Object                         | `prefix` (output name prefix), `nullPolicy` (`keep` \| `fillZero` — missing numeric feature values become 0 \| `indicator` — adds `<name>_isnull` flags for sequence / population / validFor columns), `exclude` (name globs such as `block.*` or lineage selectors `derivedFrom:market`, `evidence:declared`, `scope:population`, `block:<name>`), `groupBy` (context name), `parentFields` (input fields placed on the parent record), `childName` (field name of the child array, default `rows` — rename it when it collides with a reserved word downstream), `passThrough` (`all` (default) \| `keys` \| `none`: which input fields are copied to the output; input fields are not availability-checked, so `keys` — time.field, entity / context keys, tie-break and parentFields — makes the table safe to consume with `SELECT *`). |

### Sources contract

```yaml
sources:
  - name: listings
    eventTime: session_time        # event-time field of the rows
    availability: atEventTime      # table default: known once the event exists (pre-event information)
    mutability: appendOnly         # appendOnly | corrections
    ingestionLag: PT0S             # delay between a value becoming known and reaching the input
    settlementLag: PT30M           # concretizes after(event): event_time + settlementLag
    snapshotOf: {source: listings_snapshot, at: "event_time - PT6H"}   # corrections: training values come from a point-in-time snapshot
    keys: [session_id, seller_id]
    fields:
      - {name: start_price, type: float64, kind: attribute}
      - {name: current_bid, type: float64, availableAt: "event_time - PT10M", observedAtField: snapshot_time, kind: market, validFor: PT15M}
      - {name: sold, type: int32, availableAt: after(event), kind: outcome}
```

| field attribute   | description |
|-------------------|-------------|
| `availableAt`     | `atEventTime` (pre-event), `event_time ± <duration>`, `after(event)` (= event_time + settlementLag), `atRowCreation`. Defaults to the table `availability`. |
| `ingestionLag`    | Upper bound of the delay until the value is present in the input relation. The checks use `availableAt + ingestionLag`. |
| `observedAtField` | Field holding the time the value was actually observed. **Required** for pre-event claims (`event_time - δ`, `atRowCreation`) unless `evidence: declared` is stated explicitly. |
| `evidence`        | `measured` (default with observedAtField) or `declared` (not auditable). `kind: market` with `declared` is an error unless `allowDeclared: true` with a `justification`. |
| `kind`            | Free lineage tag (`market`, `outcome`, `attribute`, ...), propagated to every derived column as `derivedFrom`. |
| `validFor`        | How long the value stays meaningful (freshness). |

### Feature scopes

```yaml
features:
  - {name: price_per_unit, scope: row, expr: "start_price / quantity"}
  - {name: time_parts, scope: row, type: datetime, input: session_time, derive: [month, dayOfWeek], cyclical: true}
  - {name: price_bin, scope: row, type: bin, input: start_price, edges: [10, 50, 100]}
  - {name: cat_grade, scope: row, type: cross, inputs: [category, condition_grade]}
  - {name: grade_is, scope: row, type: indicator, input: condition_grade, values: [good, fair]}   # grade_is_good, grade_is_fair (0/1)
  - {name: kept_grade, scope: row, type: equals, inputs: [condition_grade, recent_all_condition_grade_lag1]}  # 1/0, null if either side is null
  - {name: vs_market, scope: row, type: residual, input: share, baseline: market, on: identity}

  - name: relative                  # context: inputs × ops, or ops with their own fields
    scope: context
    context: session
    inputs: [start_price, current_bid]
    ops: [rank, zscore, shareOfTotal]
    excludeSelf: false
  - name: composition
    scope: context
    context: session
    ops:
      - {type: countByValue, fields: [condition_grade]}                      # map<value, count> column
      - {type: countByValue, fields: [condition_grade], values: [good, fair]}  # one INT64 column per value (sink / model friendly)
      - {type: entropy, fields: [condition_grade]}
      - {type: groupSize}

  - name: recent                    # sequence: strictly-past rows of the entity
    scope: sequence
    entity: seller
    windows:
      - {maxEvents: 5}
      - {maxAge: P365D, filter: "category = $self.category"}
    ops:
      - {type: lag, fields: [sold, start_price], k: 2}
      - {type: delta, field: start_price, k: 1}
      - {type: trend, field: start_price, k: 5}
      - {type: ewma, expr: "sold >= 1", halflife: [3, 10], decayBy: events}
      - {type: runLength, field: condition_grade, value: good}
      - {type: sinceEvent, predicate: "sold = 1", unit: [events, days]}
      - {type: countMatch, predicate: "sold = 1"}
      - {type: countMatch, predicate: "sold = 0", as: losses}   # as: names the column (two predicates of one op type need it)
      - {type: ewma, expr: "start_price / quantity", halflife: [3], as: unit_price}  # as: replaces the anonymous __e{n} segment
      - {type: aggregate, field: start_price, funcs: [count, mean, max]}
      - {type: aggregate, funcs: [count]}        # COUNT(1): every visible past row, nulls included

  - name: enc                       # population: expanding encoding, keySets × windows × targets × stats
    scope: population
    type: encoding
    keySets:
      - keys: [seller_id]
      - keys: [category]
        windows: [{maxAge: P365D}]
    targets:
      - {stats: [count]}
      - {field: sold, stats: [mean]}
      - {expr: "final_price / start_price", stats: [mean, std]}
    naming: "{block}__{keys}__{window}__{target}__{stat}"
    maxFeatures: 100
```

### Static fits and artifacts (fit.mode static)

```yaml
  fit:
    mode: static                                  # statistics fitted on the whole input, applied by lookup
    artifact: {uri: "gs://bucket/features", refit: false}
```

`expanding` (default) recomputes the statistics at every row from strictly-past, already-available
contributions and is the leak-safe choice for training backfill. `static` fits each lattice level once
over the whole input (windows are ignored) and applies it to every row as a pure map — rows then see
their own outcome, so use it for serving / offline analysis, not for training. With `artifact.uri` the
fitted statistics are written to `<uri>/<planHash>/<block>.avro` (+ `<block>.manifest.json`); the plan
hash covers the spec and the sources contract (everything except `fit.artifact` itself), so any change
produces a new directory. The manifest also records the `varianceComponents` pseudo-counts (`lambdas`,
per level) derived from the persisted statistics, so a run's shrinkage can be audited. `artifact.id` pins an explicit version directory instead of the hash. When an artifact
for the current plan hash already exists it is loaded at worker setup instead of re-fitting (`refit: true`
forces a new fit) — this is the serving path: the same config, run on request data, applies the fitted
statistics without any history. Streaming runs require an existing artifact. Paths use the Beam
filesystems (`gs://`, `s3://`, relative local paths).

### Out-of-fold fits (fit.mode fold)

```yaml
  fit:
    mode: fold
    folds: 5                                      # default 5
    groupBy: seller                               # fold unit = this entity's keys (optional)
    artifact: {uri: "gs://bucket/features"}       # optional: the whole-input statistics, for a static serving run
```

`fold` fits the same lattice statistics as `static` over the whole input but applies to every row the
statistics **without the row's own fold** (cross-fitting): the row's fold is a deterministic hash of
its fold unit — the `groupBy` entity's keys, or, without `groupBy`, the row identity
(`time.field` + `time.orderTieBreak`; `time.field` alone when no tie-break is declared, with a warning —
rows sharing a timestamp then share a fold).
Rows whose fold-unit fields are null get the full statistics. Unlike `expanding`, the other folds contain
rows *after* the current one, so this is the classic target-encoding cross-fit for i.i.d. training data,
not a time-ordered backfill; when a key set's key derives from a past outcome, `fit.groupBy` is
required so that an entity's own rows never leak across folds. Windows are ignored as in `static`.
With `artifact.uri` the whole-input (not out-of-fold) statistics are persisted exactly as `static`
would; pin the version with `artifact.id` so a serving config with `mode: static` (a different plan
hash) loads them. A fold run itself always re-fits (it needs the per-fold tags, which an artifact does
not hold).

### Factorization (population, type: factorization)

```yaml
  - name: fm
    scope: population
    type: factorization
    variant: fwfm                                 # fm | fwfm (field-pair weights r_fg; bayesian is v2)
    fields: [seller_id, category, condition_grade] # categorical fields (≥ 2)
    latentDim: 8
    task: {target: sold, offset: market}          # target: <field> (alias field) or expr: <numeric expr>, optional baseline offset
    fit: {artifact: {uri: "gs://bucket/features"}} # always fit.mode static; cadence / window / warmStart are not implemented yet
    als: {epochs: 10, reg: 0.01, seed: 0}
    outputs:
      - {pair: [seller_id, category], as: fm_seller_category}   # r_fg · ⟨v_f[x_f], v_g[x_g]⟩
      - {embedding: category, as: cat_emb, dims: 4}            # cat_emb_0 .. cat_emb_3
      - {sum: true, as: fm_linear}                             # w0 + Σ w + Σ pairs (without the offset)
```

`ŷ = w0 + Σ_f w_f[x_f] + Σ_{f<g} r_fg ⟨v_f[x_f], v_g[x_g]⟩`, fitted by alternating least squares (closed-form
ridge update per parameter, deterministic for a seed). The whole training set is gathered on one worker
for the fit, so it must fit in memory. Unknown field values yield null outputs. The artifact
(`<planHash>/<block>.fm.avro` + manifest with the fwfm `pairWeights` ranking) is written and reused like
the static encoding artifacts.

### Shrinkage and key lattices (population)

```yaml
  - name: enc
    scope: population
    type: encoding
    keySets:
      - keys: [seller_id]                         # flat: seller → global
      - keys: [seller_id]
        structure: hierarchy                      # seller → parent (from a field of the row) → global
        parentRef: seller_group
      - keys: [seller_id]
        hierarchy: [[category], []]               # explicit lattice: seller → category → global
      - keys: [category]                          # main effect, required by the cross below
      - keys: [condition_grade]
      - keys: [category, condition_grade]
        structure: cross                          # cell → additive(main effects) → global (sequential estimator)
        shrinkage: {priorWeight: 50}              # keySet-level override
    targets:
      - {field: sold, stats: [mean]}
    shrinkage:
      weights: varianceComponents                 # fixed: w = n / (n + priorWeight); varianceComponents: λ = σ²/τ² per level (batch method of moments)
      priorWeight: 20                             # fixed pseudo-count, and the fallback when a level has too few keys
      scale: logit                                # identity | logit | log; required when a lattice uses additive
      leaveNodeOut: true                          # subtract the leaf's own statistics from every ancestor
      output: [composed, deviations, effectiveN]  # composed (default) | deviations (dev0, dev1, ...) | effectiveN (<stat>__neff)
```

`smoothing: {type: bayesian, priorWeight: N}` is accepted as the legacy spelling of fixed-weight
shrinkage toward the global mean. Every lattice level is evaluated as its own keyed stage over the same
window and target, and the composition is a per-row formula: `est(level) = est(parent) + w · (t(mean) −
est(parent))` from the global level down to the key, on the declared scale. `share` is
`n_key / n_global` over strictly-past rows.

A feature must not reuse the name of an input field (in-place overwrite is rejected). Generated column
names: row `<name>` (datetime `<name>_<derive>[_sin|_cos]`), context
`<name>_<field>_<op>`, sequence `<name>_<window>_<field>_<op><param>` (window token: `n5`, `365d`,
`365d_n20`, `all`), encoding per `naming` (empty segments collapse). `output.prefix` is prepended; columns
that are only usable offline get a leading `_` and are not emitted.

Inline `expr` in sequence ops and encoding targets is evaluated per past row (no `$self`); expressions are
numeric (Lucene expression syntax), predicates and window filters use the SQL-like
[Filter](../common/filter.md) syntax.

### Naming, conditions and placement notes

- `as:` on a sequence / context op names the output column segment: for `sinceEvent` / `countMatch` it replaces
  the op suffix (`<block>_<window>_<as>[_<unit>]`), otherwise the field segment — which is how an inline
  `expr` avoids the anonymous `<block>__e{n}` name. On an encoding target `as:` replaces the target name
  (`<block>__<keys>__<as>__<stat>`).
- `countByValue` / `ratioByValue` produce a `map` column by default; with `values: [...]` they produce one
  numeric column per value (`<block>_<field>_countByValue_<value>`, absent value = 0 / null ratio). Prefer
  `values` when the output goes to a sink such as BigQuery or straight into a model.
- Window `filter` and op `predicate` texts are parsed at compile time. A column whose name is a keyword of
  the condition grammar (`rank`, `order`, ...) is quoted automatically with backticks (reported as
  `predicate.quoted` / `filter.quoted`); you can also write `` `rank` <= 3 `` yourself. A condition that
  does not parse is a compile error (`predicate.parse` / `filter.parse`), not a worker failure.
- With `output.groupBy`, group-constant context columns (`countByValue`, `ratioByValue`, `entropy`,
  `groupSize` without `excludeSelf`) are placed on the **parent** record, not in the child array — look for
  them next to the group keys (`placement: parent` in the plan's column list).
- Hints such as `sequence.aggregate.encoding` are reported once per block.

### Availability check

For each column the module derives `availableAt` from its inputs (max over inputs; sequence / population
columns add the constraint that a past row at t' contributes only when
`availableAt(t') + ingestionLag ≤ predictAt(t)`):

- **staticSafe** — provable at assembly.
- **windowShift** — the history window's near edge is moved back by `δ' − (predictAt − event_time)`; e.g.
  an outcome settled 30 minutes after the event and ingested within 6 days is only visible 6 days 30 minutes
  (+ the predictAt offset) later. This is what makes training features reproducible at serving time.
- **violation** — an emitted column would use information available after `predictAt`: assembly fails.
  Such a column may still exist as an intermediate consumed by a sequence feature (its past values are fine).

## Validating without running (validate --expand)

The same compiler is exposed without a pipeline run:

- REST: `POST /api/feature` with either `{parameters: {...}, inputSchema?: {fields: [...]}, args?: {...}}` or
  a whole pipeline config (the first `module: feature` step, or the one named by `name`). The response
  holds `ok`, `plan` (columns with availability / status / lineage, stages, diagnostics), `engineErrors`
  and a human-readable `describe` report.
- MCP tool `validate-feature` (arguments `config` or `parameters`, optional `name`, `inputSchema`, `args`,
  `streaming`, `format: text`).
- Pipeline Builder agent tool `validateFeature`.
- MCP tool `run-pipeline` with `dryRun: true`: assembles the whole config in the server and returns, besides
  every step's resolved schema, `featurePlans` — this report compiled against the real input schemas.
  From there `launch-pipeline` submits the config to Dataflow / a Cloud Run Job and `get-job` /
  `get-job-logs` / `list-job-errors` follow it, so the validate → dry run → launch → inspect loop closes over MCP
  (the Pipeline Builder agent has the same tools: `run`, `launchPipeline`, `getJob`, `getJobLogs`).
- CLI: `--dryRun=true` together with the usual `--config=...` loads the config and assembles the whole
  pipeline (every module's validation, schema resolution and the feature plan compilation against the real
  input schema) without running it. The feature plan report is printed to stdout; an invalid spec exits
  with the compile errors. Works with any runner build (e.g. the `direct` image in
  [Run Pipeline locally](../../exec/README.md#run-pipeline-locally-directrunner)).

The report (the `describe` text and the `plan.audit` array) also contains **hot-key audit queries**: for every
distinct key set of the keyed stages (context / sequence / population / groupBy) an SQL that lists the top
keys by row count, and a plain row count for a global (single key) level:

```sql
SELECT seller_id, COUNT(1) AS row_count FROM {input} WHERE seller_id IS NOT NULL
GROUP BY seller_id ORDER BY row_count DESC LIMIT 20
```

Replace `{input}` with the relation that feeds the transform and run it on your warehouse before a large
backfill: the top `row_count` is the number of rows one worker gathers in memory for that stage (see
[Performance and sizing](#performance-and-sizing)). Keys that are intermediate columns (derived by an earlier
stage) are flagged in the query's `note` — evaluate those on the relation as it stands before that stage.

## Performance and sizing

- **Stages are scheduled by key, not by config order.** Every keyed column goes to the earliest stage that
  evaluates its kind under the same key and comes after the stages its dependencies are computed in, so
  two blocks keyed by the same entity share one shuffle even when a block with another key sits between
  them, and sequence and population columns of one key share the same keyed replay (the stage is reported
  as `population` when it holds any population column). Row columns are evaluated as late as possible —
  in the stage of their first consumer, or in the last stage when only the output reads them — so their
  values are not carried through shuffles that do not need them. The hidden statistics of a `fit.mode:
  static` / `fold` block always share one fit stage. The plan report lists the resulting stages
  (`#n kind key=[...] blocks=[...] deps=[...] wave=w`) and the shuffle count (one per keyed stage).
  `deps` are the stages whose keyed / fit columns a stage needs (row columns are followed through to
  their own inputs — a branch would just recompute them) and `wave` is its depth in that dependency DAG:
  the stages of one wave are mutually independent.
- **Waves run in parallel.** The engine evaluates the stages of a wave as parallel branches of the same
  input: each branch emits only its own columns keyed by a row id, and the branches are merged back into
  full rows — inside the next stage's GroupByKey when that stage is a single context stage (or the
  `output.groupBy` finalize), otherwise by one row-id GroupByKey per wave. A job therefore pays one
  shuffle barrier per wave (plus the merges) instead of one per keyed stage, and a wave takes as long as
  its slowest branch: `waves=` and `dagShuffles` in the plan report are the depth and the shuffle count of
  this execution (`shuffles` is the linear count). Sizing: the branches of a wave run concurrently, so give
  the job enough workers for their combined work (`numWorkers` at least the number of heavy branches)
  and size `diskSizeGb` for the keys that spill at the same time; a wave of one stage costs nothing extra.
  `engine.parallelWaves: false` restores the linear chain (for an A/B run — the outputs are identical);
  `engine.rowId` names a natural key and drops the Reshuffle that pins random row ids. A row column that
  reads an earlier stage and is only consumed by the output is evaluated in the last keyed stage, which
  then depends on that earlier stage — a wave of its own; put such columns in the block that consumes them
  when that matters. Streaming runs the linear chain. A fused stage keeps one
  history per key, **trimmed per field**: each projected field stays only as far back as the longest window
  of the columns reading it, so fusing blocks does not extend any field's retention — a column that reads
  the whole history of its key (a scan-path window without `maxAge`, reported by the
  `sequence.window.unbounded` hint) keeps only its own inputs for every row, not the other columns' fields.
  The retained **row count** per key is still the longest window among the fused columns: with an unbounded
  column every past row of the key keeps a small entry skeleton (~40 bytes) even after all other fields are
  removed, so size hot keys by row count × the unbounded column's fields (the hint lists them), plus the
  skeleton.
- Keyed statistics (sequence `aggregate`, population encodings) are evaluated incrementally (O(n) per
  key). A window `filter` of the form `f = $self.f` over a pre-event field is automatically evaluated as
  an **additional partition key**, so hot entities split across workers; rows whose `f` is null bypass
  the stage (their columns are null). Other filters are evaluated per row over the window.
- The rows of each key are sorted by event time inside the stage (in memory up to the spill budget —
  `engine.spill.memoryMB`, by default 16-256 MB derived from the worker heap — and beyond it as sorted chunks
  on the worker's local disk that are merged on read) and replayed as a stream, so a hot key — including a
  shrinkage encoding's global level, which is a single key holding **every** row — is never materialised in
  memory. Keys that fit the budget never touch the disk (only a small sample is encoded to size the buffer).
  What stays in memory per key is the running statistics plus the *projected* history (only the fields the
  windows read) behind the longest window: a `maxAge` window, or any incremental statistic, lets rows be
  dropped once they leave every window, and without `maxAge` the operators that read a fixed tail (`lag` /
  `delta` / `trend` by their `k`, unfiltered `maxEvents` windows) keep only that tail. Only `ewma`,
  `runLength` / `sinceEvent` / `countMatch`, and any window with a `filter` but no `maxAge` read the key's
  full history, and they keep only the fields they read for it (the history is trimmed per field, so the
  other columns' fields still leave with their own windows, though each retained row keeps a ~40-byte entry
  skeleton); the stage logs which columns do at startup — give such windows a `maxAge` to bound them. Local disk of the workers must have room for the keys being sorted concurrently: the chunk
  files of a key are deleted as soon as its replay ends (and the stage's directory when the worker tears the
  stage down), so the disk holds at most one key per concurrent bundle, each up to the key's encoded size
  (`compress: true` trades CPU for a smaller footprint). The spill budget is per key being processed (each
  concurrent bundle owns one), which is why the default divides the heap by the core count. The hot-key
  audit queries in the validate / dry-run report (above) give the per-key row counts to size that against.
  Every spilled key logs `keyed spill sorter Stage<N>_<kind> key=<key>: <chunks> chunk(s) / <MB> MB on disk +
  <rows> rows in memory; live spill on this worker <MB> MB (peak <MB> MB)` — the peak over a job is the
  worker disk the keyed stages need. Columns that read the whole history of a key are reported at compile
  time by the `sequence.window.unbounded` hint (with the fields they keep).

## Limitations (current engine)

- Batch only for sequence / population features (per-key time-ordered replay). Row / context features also
  run in streaming within the configured window, as a linear chain (the parallel-wave merge is a batch
  GroupByKey).
- Key set `structure: sequence`, nested encoding targets, the `quantile` stat (and
  `distribution` in static mode), and population types other than `encoding` / `factorization` are parsed
  but rejected. Factorization: `variant: bayesian`, `fit.cadence / window / warmStart`, and non-static fits. In `shrinkage`,
  `estimator: joint`, `weights: heldOut` and an `offset` on a logit / log scale are rejected;
  `parentStatistic: type` falls back to token with a warning. `weights: varianceComponents` estimates the
  per-level pseudo-count from the whole batch (a hyper-parameter, not time-expanding); a level whose
  between-key variance truncates to zero is fully shrunk to its parent (logged at run time).
- `atRowCreation` / `event_date THH:MM` availability needs per-row filtering that is not implemented yet.
- Rows whose key fields contain null bypass keyed evaluation (their keyed features are null).

## Example

```yaml
sources:
  - name: input
    module: bigquery
    timestampAttribute: session_time
    parameters:
      query: "SELECT * FROM `project.dataset.auction_rows`"
transforms:
  - name: features
    module: feature
    inputs: [input]
    parameters:
      sources: gs://my-bucket/feature/sources.yaml
      lineage:
        - {fields: [session_id, seller_id, category, start_price], from: listings}
        - {fields: [sold, final_price], from: auction_results}
      time: {field: session_time, orderTieBreak: [session_id]}
      predictAt: "event_time - PT10M"
      entities:
        - {name: seller, keys: [seller_id]}
      contexts:
        - {name: session, keys: [session_id]}
      features:
        - {name: relative, scope: context, context: session, inputs: [start_price], ops: [rank, zscore]}
        - name: recent
          scope: sequence
          entity: seller
          windows: [{maxEvents: 10}]
          ops:
            - {type: lag, fields: [sold], k: 3}
            - {type: aggregate, field: start_price, funcs: [mean]}
        - name: enc
          scope: population
          type: encoding
          keySets: [{keys: [seller_id]}, {keys: [category]}]
          targets: [{field: sold, stats: [mean]}]
      output:
        prefix: f_
sinks:
  - name: output
    module: bigquery
    inputs: [features]
    parameters:
      table: project.dataset.features
```
