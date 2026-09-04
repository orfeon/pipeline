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
  (count / share / mean / rate / std / distribution / quantile), optionally windowed and offset by a baseline, with
  **shrinkage** along a generalization lattice (key → parent keys → global, `additive` main effects for
  crosses): fixed or variance-components pseudo-counts, leave-node-out, identity / logit / log scale,
  composed values, per-level deviations and effective sample size. **factorization** machines (fm / fwfm,
  ALS) over categorical fields: pair interaction scores, embeddings, linear predictor. **discretize**: bin
  edges fitted on the input (quantile method), a fitted categorical column to key an encoding on.
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
| baselines  | optional | Array<Object\>                 | Named baselines: `{name, expr, context, emit}`. `expr` may wrap a numeric expression in a context op, e.g. `share(1 / price)`. Referenced by `type: residual` (`baseline:`), encoding / factorization `offset:` and the `softmax` op. Baselines are intermediate columns; `emit: <name>` also writes the value as an output column (the same number the softmax offset reads), which a `baseline` role can name. |
| features   | required | Array<Object\> or String       | Feature blocks (see scopes below). A string is a URI / path to a document whose `features` list is used. |
| fit        | optional | Object                         | Defaults for population features (overridable per block with `fit:`): `orderBy` (= time.field), `mode` (`expanding` \| `static` \| `fold` \| `forward`), `groupBy` (entity name: the fold unit), `folds` (number of folds for `fold`, default 5), `blocks` (`{bucket: year \| quarter \| month \| week \| day}` or `{size: <ISO-8601>}`, default `P90D`) and `minBlocks` (default 1) for `forward`, `artifact` (`{uri, refit, id}` or the URI string — see *Static fits and artifacts*, *Out-of-fold fits* and *Forward block fits*). `minHistory` is accepted but not implemented yet (warning). |
| engine     | optional | Object                         | Runtime knobs that do not change the plan. `parallelWaves` (default `true`): evaluate the independent stages of each wave in parallel and merge them by row id (see *Performance and sizing*); `false` runs the stages as one linear chain. `rowId`: input fields identifying a row (a natural key) for that merge; without it every row gets a random id pinned by one extra Reshuffle before the first fan-out. `spill`: the per-key sort of the keyed stages — `memoryMB` (in-memory buffer per key before sorted chunks are spilled to worker-local disk; default derived from the worker heap: a quarter of the heap shared by the cores, clamped to 16-256 MB; the `--featureSpillMemoryMB` pipeline option sets it for every feature step), `directory` (spill directory on the worker, default `java.io.tmpdir`), `compress` (deflate the chunk files, default false). See *Performance and sizing*. |
| output     | optional | Object                         | `prefix` (output name prefix), `nullPolicy` (`keep` \| `fillZero` — missing numeric feature values become 0 \| `indicator` — adds `<name>_isnull` flags for sequence / population / validFor columns), `exclude` (name globs such as `block.*` or lineage selectors `derivedFrom:market`, `evidence:declared`, `scope:population`, `block:<name>`), `groupBy` (context name), `parentFields` (input fields placed on the parent record), `childName` (field name of the child array, default `rows` — rename it when it collides with a reserved word downstream), `passThrough` (`all` (default) \| `keys` \| `none`: which input fields are copied to the output; input fields are not availability-checked, so `keys` — time.field, entity / context keys, tie-break and parentFields — makes the table safe to consume with `SELECT *`), `roles` (the data contract: `group` / `time` / `entity` / `label` / `baseline` / `weight` → an input field, a context / entity name or a baseline name; role fields always pass through and are recorded in the manifest so consumers never treat them as features), `include` (the output projection: a list of column names, or a URI / path to a JSON array / `{columns: [...]}` / one-name-per-line file such as a screening step's pass list; when declared it replaces `exclude`), `manifest` (URI of the assembly-time manifest, see [Output contract](#output-contract-roles-include-manifest)). |
| audit      | optional | Object                         | `observedAt` (`count` (default) \| `fail` \| `off`): what the [observedAt audit](#observedat-audit-declaration-vs-data) does with a row whose observation time is after the declared availability — count it (metrics + run manifest), route it to the failure output, or skip the audit. |

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
  - {name: placebo_noise, scope: row, type: noise, distribution: normal, seed: 20260717}   # information-free column (see Placebos)

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
  - name: prob                      # group softmax: a model score into a probability, on top of a baseline
    scope: context
    context: session
    ops:
      - {type: softmax, field: model_score, offset: market, temperature: 1.3, as: pWin}
  - name: placebo                   # the field's values permuted within the group (see Placebos)
    scope: context
    context: session
    ops:
      - {type: shuffle, fields: [start_price], seed: 20260717}

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
      - {field: final_price, stats: [quantile, q25, quantile90]}   # median, 25th and 90th percentile of the past values
    naming: "{block}__{keys}__{window}__{target}__{stat}"
    maxFeatures: 100
```

Encoding stats: `count` (rows), `share` (leaf count / global count), `mean` / `rate` (shrinkable), `std`,
`distribution` (map of value shares), and `quantile` — the median — or `quantile<NN>` / `q<NN>` for the NN-th
percentile (0..100), linearly interpolated between the past values (R type 7 / numpy default). `std`,
`distribution` and the quantiles are read from the key's own past values (no shrinkage: a quantile of an
interpolated distribution is not the interpolated quantile), so they are available in the expanding fit
only — `fit.mode: static` / `fold` keep (n, Σy, Σy²) per key and reject them. A NaN target (or baseline)
value counts as missing for every numeric encoding stat, like null.

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

### Forward block fits (fit.mode forward)

```yaml
  fit:
    mode: forward
    blocks: {size: P90D}                          # or {bucket: year | quarter | month | week | day}; default P90D
    minBlocks: 1                                  # rows with fewer preceding blocks (with data for the key) read nothing
    artifact: {uri: "gs://bucket/features"}       # optional: the whole-input totals, for a static serving run
```

`forward` is the time-series counterpart of `fold`: every row reads the statistics of the **complete time
blocks whose targets are known at predictAt**, and nothing from its own block — a stepwise `expanding`
(the statistics move at block boundaries) that is computed as a parallel Combine per (key, block) plus a
per-key prefix over blocks instead of a time-ordered replay per key. The single-threaded global-level
stage of an expanding lattice disappears (`encoding.globalKey`), and unlike `fold` no later row leaks
into the statistics. Per level, a block is usable when its end is at or before `predictAt(row) − lag`,
`lag` being the target's availability delay after its event (settlement + ingestion; an attribute-only
level has none), so a fresh outcome never enters a block early. `minBlocks` makes rows with a short
history read nothing (`count` reads 0, the other statistics null). Windows: `maxAge` is rounded up to
whole blocks (`fit.mode.forward.window`), `maxEvents` / `filter` are ignored (`fit.mode.forward.windowIgnored`).
Sufficient statistics only (count / sum / mean / rate / std; `quantile` / `distribution` are expanding
only, `encoding.stat.static`). With `weights: varianceComponents` the pseudo-count λ is estimated per
block from the keys' statistics up to that block, and recorded per block in the artifact manifest
(`lambdasByBlock`). Block size trades staleness against stability: yearly blocks leave the first year
empty and miss within-year drift, `P90D` is a good default; `blocks.bucket` gives calendar alignment
(UTC). The `blocks` / `minBlocks` settings are part of the plan hash. Batch only.

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

### Discretize (population, type: discretize)

```yaml
  - name: price_bin
    scope: population
    type: discretize
    input: start_price                 # numeric field or feature
    method: quantile                   # quantile (tree / optimal, the supervised methods, are not implemented yet)
    bins: 8                            # and / or minSamplesPerBin: N — B = min(bins (default 10), n / N)
    fit: {artifact: {uri: "gs://bucket/features"}}   # always fit.mode static
  - name: by_price
    scope: population
    type: encoding
    keySets: [{keys: [price_bin]}]     # the fitted bins key an encoding (the fit stage runs first)
    targets: [{field: sold, stats: [mean]}]
```

Unlike the row `type: bin` (hand-written `edges`), the edges are learned from the whole input in a static
fit and applied by lookup: the interior edges are the `i / B` quantiles of the non-null, non-NaN values
(type 7; `B = min(bins, n / minSamplesPerBin)` with `bins` defaulting to 10; ties and edges at the extremes
are dropped, so discrete data can yield fewer bins than requested). The INT64 output is `-1` for a missing
value (null / NaN), `0` below the fitted minimum, `1..B` for the fitted bins (`edge <= v < next`) and
`B + 1` above the fitted maximum — the out-of-range and missing bins are categories of their own, so a
serving value the fit never saw shows up as a drift signal instead of hiding in an edge bin. (The row `bin`
numbers its bins `0..` from the count of edges below the value and has no dedicated bins; the two are not
interchangeable.) An input without a single value still fits (n = 0): every non-missing value maps to
bin 1 and the artifact is written. The values are gathered on one worker for the fit (8 bytes per row).
The artifact `<planHash>/<block>.bins.json` (edges, min, max, n) is written and reused like the other static
artifacts; `fit.cadence / window / warmStart` are accepted but ignored. For a cross key, keep the bins
coarse (4–8): the cardinality multiplies.

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

### Group softmax (context op `softmax`)

Turns a per-row score (a model output from an `onnx` step, say) into a probability that sums to 1 within
the context, on top of an optional baseline — the serving-side counterpart of a model trained with a
group softmax and `init_score = log(baseline)`:

```
p_i = w_i · exp(f_i / T) / Σ_j w_j · exp(f_j / T)      w = offset value (1 without offset), T = temperature
```

| parameter | meaning |
|---|---|
| `field` | the score column (numeric) |
| `offset` | a `baselines[].name` or a numeric column, read **in probability space** (a `share(...)` baseline is one). `offsetScale: log` takes `exp` first (−∞ / NaN → null) |
| `temperature` | constant > 0 (default 1); `temperatureFrom: <uri>` reads it from a calibration document at assembly (a bare number, or JSON with `temperature` / `T`). The document is outside the plan hash (no fit depends on it); the resolved value and the document hash are in the manifest (`externals`) and the output hash |
| `scoreNull` | `zero` (default: a null score falls back to 0, i.e. to the offset's probability; with `nullPolicy: indicator` a `<name>_scoreNull` flag says so) \| `null` (the row's output is null) |

Null handling, matched to a training-side normalisation that drops NaN from the sum: a **null offset**
makes the row null and removes it from the denominator (the other rows still sum to 1; `nullPolicy:
indicator` adds `<name>_isnull`); an **offset of 0** gives p = 0 and stays in the denominator as 0. The
column inherits its availability from the score and the offset, and the offset's `validFor` (a market
price expires; so does the probability). With f = 0 and T = 1 the output equals the renormalised offset.
`excludeSelf` has no effect. Row / context only, so the op works in streaming (an `onnx` → `feature`
→ sink serving chain).

### Placebos (`type: noise`, context op `shuffle`)

Information-free columns that go through the same selection / training path as the candidates, to
calibrate a selection threshold (a null column still shows a small positive gain) or to measure
permutation importance without leaving the pipeline:

- **`noise`** (row): `distribution: normal | uniform`, `seed` (required). The draw is a pure function of
  `seed` and the row identity — `time.field` + `orderTieBreak`, the fold rule — so re-runs, workers and
  engine modes agree. Rows sharing the identity share the draw (`row.noise.identity` warns when no
  `orderTieBreak` is declared). Availability: pre-event.
- **`shuffle`** (context): `fields`, `seed` (required). Within each group the field's values are
  reassigned by a permutation drawn from `seed` and the group key, applied to the rows ordered by
  `time.field`, `orderTieBreak`, then the remaining input fields — a pure function of the group's
  content, whatever order the runner delivers the rows. The multiset per group is preserved, the output
  keeps the field's type and **availability** (a shuffled outcome is still an outcome: emitting it is
  the usual violation; as an intermediate consumed by a sequence feature it is fine).

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

### Output contract (roles, include, manifest)

The output table is the shared input of a training job, a screening step and an evaluation step. Three
`output` parameters make its contract explicit instead of leaving it to each consumer:

```yaml
output:
  prefix: f_
  passThrough: keys
  roles:
    group: session          # a context name (its keys are recorded) or an input field
    time: session_time      # ordering for time-series splits
    entity: seller_id       # join key of predictions
    label: sold             # the outcome the consumer derives its target from
    baseline: market        # a baseline name or an output column
  include: gs://bucket/screen/${args.version}/passed.json   # optional projection
  manifest: gs://bucket/features/${args.version}/manifest.json
```

- **roles** name what a consumer must not treat as a feature. Every role must resolve (an input field,
  a context / entity for `group` / `entity`, a baseline for `baseline`; `output.roles.unresolved`
  otherwise). An input field with a role is passed through whatever `passThrough` says. A `baseline`
  role naming a baseline that is not emitted is reported (`output.roles.baseline.notEmitted`): baselines
  are intermediate columns today, so derive the value as a feature (`shareOfTotal`) and name that column.
- **include** is the projection: only the listed columns (canonical or output names; a `<name>_isnull`
  entry keeps its base column) are emitted, plus the pass-through fields. Names matching no column are a
  warning (`output.include.unknown`) — the list may come from another plan version. An empty list is an error
  (`output.include.empty`): the table would carry no feature column (a screening step that passed nothing). `include` and
  `exclude` are not combined: when `include` is declared, `exclude` is ignored (`output.include.exclude`).
  A URI is read at assembly and its content hash recorded, so a file that changes later is still traceable.
- **manifest** writes `manifest.json` at assembly (a dry run writes it too): `planHash`, **`outputHash`**
  (plan hash + emitted names + roles + include content — the identity of the output table, since a
  projection does not change the plan hash), `roles` (with the resolved column / keys), `include` (source,
  hash, listed and unknown names), `fields` (pass-through input fields with source / kind / availability
  and their role), `columns` (every emitted column: type, `categorical`, scope, block, operator,
  availableAt / computeAt / status, placement, lineage — `derivedFrom`, `sources`, `evidence`, inputs),
  `artifacts` (fitted blocks → artifact path) and the full `plan` report. A batch run also writes
  `manifest.run.json` next to it at finalize with what only execution knows: the output row count and the
  observedAt audit results.

`include` and `manifest` are outside the plan hash (artifacts stay valid across projections);
`roles` and the projection are inside `outputHash`.

### observedAt audit (declaration vs. data)

`availableAt` is a declaration the data may violate: a "t-10 price" column whose rows were actually
observed after `event_time - PT10M` leaks whatever happened in between, and no static check can see
it. For every input field whose contract names an `observedAtField`, the engine compares that column
with the declared availability on every row:

- **late** — `observedAt > event_time + availableAt` (the declaration is wrong for this row); for a
  dynamic declaration (`atRowCreation`) the deadline is `predictAt`.
- **afterPredictAt** — `observedAt > predictAt`: the value would not have existed when the prediction
  was made. This is the leak.
- **missing** — the field has a value but no observation time.

Counts are Beam metrics (`feature/observedAt_<field>_late|afterPredictAt|missing`, visible in the job
UI), the plan report lists the audited fields (`-- observedAt audit`), and with `output.manifest` the
run manifest holds per field the counts plus the deciles of `predictAt − observedAt` in seconds
(`leadSecondsDeciles`: `[min, p10, …, p90, max]`; negative = observed after predictAt). A declared
`observedAtField` that is not in the input relation is a warning (`sources.observedAt.missingInput`) —
pass the observation-time column through from upstream to make the claim auditable. `audit.observedAt:
fail` routes late rows to the failure output (fatal under `failFast`), which turns the audit into a
guard for a serving pipeline.

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
  input (row columns the wave's stages share — an outcome expression read by several blocks, say — are
  evaluated on that input first): each branch emits only its own columns keyed by a row id, and the branches are merged back into
  full rows — inside the next stage's GroupByKey when that stage is a single context stage (or the
  `output.groupBy` finalize), otherwise by one row-id GroupByKey per wave. A job therefore pays one
  shuffle barrier per wave (plus the merges) instead of one per keyed stage, and a wave takes as long as
  its slowest branch: `waves=` and `dagShuffles` in the plan report are the depth and the shuffle count of
  this execution (`shuffles` is the linear count). Sizing: the branches of a wave run concurrently, so give
  the job enough workers for their combined work (`numWorkers` at least the number of heavy branches)
  and size `diskSizeGb` for the keys that spill at the same time; a wave of one stage costs nothing extra.
  `engine.parallelWaves: false` restores the linear chain (for an A/B run — the outputs are identical:
  a row that fails in one branch under `failFast: false` is dropped from the output and routed to the
  failure sink, exactly as the linear chain drops it at the failing stage);
  `engine.rowId` names a natural key and drops the Reshuffle that pins random row ids (null key components
  become a deterministic token; rows sharing a row id are rejected as failures, all of them). The field
  names `__rowId` and `__partial` are reserved by the merge — an input field with either name is rejected
  at validation. A row column that
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
- **Fix the worker pool for a parallel batch run.** Dataflow's default autoscaler sees the fan-out as one
  fused stage and scales the job *down* right as the branches start, so raising `numWorkers` alone does
  nothing: pass `options.dataflow.autoscalingAlgorithm: NONE` with `numWorkers` = `maxNumWorkers` sized
  for wave 1's combined work (see the [Dataflow options](../../options/dataflow.md) page for the
  autoscaler mechanism; on a measured production plan, fixing the pool alone nearly halved the job time
  with identical output).
- **Global and very coarse keys are the remaining critical path.** A keyed stage with no key (a shrinkage
  lattice's global level — the `[]` entry of a `hierarchy` — or the global denominator a `share`
  statistic needs) processes every row
  under one key — one worker thread, however many workers the job has — and once the waves run in parallel
  it is what the job waits for (the `encoding.globalKey` hint marks such stages). Where the statistics are
  encoding sufficient statistics, `fit.mode: forward` / `static` / `fold` computes them as a parallel Combine
  instead — but this is a modeling change, not a drop-in: `forward` reads complete earlier blocks only (leak-free,
  stepwise; the closest to `expanding`), `fold` is out-of-fold over the whole batch (other folds
  include later events), `static` freezes a training period's statistics (and matches how a serving path
  would consume them). The values change, so treat it as a feature-design decision and validate by model
  metrics, not by output diffing.
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
- Key set `structure: sequence`, nested encoding targets, the `quantile` / `distribution` stats in
  `fit.mode: static` / `fold` (expanding only), and population types other than `encoding` /
  `factorization` / `discretize` are parsed but rejected. Factorization: `variant: bayesian`, `fit.cadence / window / warmStart`,
  and non-static fits. Discretize: `method: tree` / `optimal` (supervised) and non-static fits. In `shrinkage`,
  `estimator: joint`, `weights: heldOut` and an `offset` on a logit / log scale are rejected;
  `parentStatistic: type` falls back to token with a warning. `weights: varianceComponents` estimates the
  per-level pseudo-count from the whole batch (a hyper-parameter, not time-expanding); a level whose
  between-key variance truncates to zero is fully shrunk to its parent (logged at run time).
- `atRowCreation` / `event_date THH:MM` availability needs per-row filtering that is not implemented yet.
- Rows whose key fields contain null bypass keyed evaluation (their keyed features are null).
- DirectRunner (the `direct` image) is unsuited to keyed stages over coarse or global keys — a
  shrinkage lattice's global level, a `share` denominator: its GroupByKey copies each key's buffered
  state per bundle, slowing such stages by orders of magnitude as rows grow. Run those pipelines on
  Dataflow, or locally / on Cloud Run with the `prism` image (Beam's portable local runner), which
  executes them at proper speed.

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
