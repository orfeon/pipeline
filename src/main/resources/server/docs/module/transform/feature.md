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
  categorical crosses, per-value indicators, field equality, residuals against a named baseline, readouts
  of a numeric array field (`type: vector`: slice / diff / normalize, then mean / slope / argmax / polyfit …).
- **context** scope — statistics relative to the rows that co-occur in the same group (rank, z-score,
  share of total, gap to best, percentile, median difference, group size, value counts / ratios, entropy).
- **sequence** scope — per-entity strictly-past history: lag, delta, trend, EWMA (decay by events or
  time), run length, events/days since a predicate last held, count of matching rows, windowed aggregates
  (count / mean / min / max / sum / std / first / last, the shape and series summaries skew / kurt / zeroCross /
  peaks / acf / pacf / ar), and path summaries (the general form `lift` + `summarize`: exponential / Laguerre,
  Fourier and Legendre projections of the window). Windows combine `maxEvents`, `maxAge` and a
  `filter` that can reference the current row through `$self.<field>`.
- **population** scope — expanding-fit encoding: conditional statistics of a target per key set
  (count / share / mean / rate / std / distribution / quantile), optionally windowed and offset by a baseline, with
  **shrinkage** along a generalization lattice (key → parent keys → global, `additive` main effects for
  crosses): fixed or variance-components pseudo-counts, leave-node-out, identity / logit / log scale,
  composed values, per-level deviations and effective sample size. **factorization** machines (fm / fwfm,
  ALS) over categorical fields: pair interaction scores, embeddings, linear predictor. **discretize**: bin
  edges fitted on the input (quantile method), a fitted categorical column to key an encoding on.
  **quantileTransform**: a value's position in the fitted distribution (rank normalisation, optionally as a
  normal score). **svd**: PCA / truncated-SVD scores of a numeric vector (several fields or an array).
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
| time       | required | Object                         | `field`: the event-time field of the input (must equal the sources' `eventTime`). The transform re-timestamps every element from this field, so it is the time axis of all history windows regardless of the source's `timestampAttribute`; rows whose value is null go to the failure output. `orderTieBreak`: fields declaring a total order for rows that share a timestamp. History is strictly past **by timestamp**: rows sharing a timestamp never see each other, and a row later the same day does see that day's earlier rows — so a feature over "earlier events of the same day" needs the event's actual time here (a date-granularity field makes every same-day row invisible to the others). |
| predictAt  | required | String                         | When the features are used: `event_time - PT10M`, `event_time` (the literal event time), ... Every emitted column must be available at or before this time. |
| entities   | optional | Array<Object\>                 | Subjects of sequence features: `{name, keys: [...], minInterval: <ISO8601>}`. |
| contexts   | optional | Array<Object\>                 | Co-occurrence groups for context features: `{name, keys: [...]}`. |
| baselines  | optional | Array<Object\>                 | Named baselines: `{name, expr, context, emit}`. `expr` may wrap a numeric expression in a context op, e.g. `share(1 / price)`. Referenced by `type: residual` (`baseline:`), encoding / factorization `offset:` and the `softmax` op. Baselines are intermediate columns; `emit: <name>` also writes the value as an output column (the same number the softmax offset reads), which a `baseline` role can name. |
| features   | required | Array<Object\> or String       | Feature blocks (see scopes below). A string is a URI / path to a document whose `features` list is used. |
| fit        | optional | Object                         | Defaults for population features (overridable per block with `fit:`): `orderBy` (= time.field), `mode` (`expanding` \| `static` \| `fold` \| `forward`), `groupBy` (entity name: the fold unit), `folds` (number of folds for `fold`, default 5), `blocks` (`{bucket: year \| quarter \| month \| week \| day}` or `{size: <ISO-8601>}`, default `P90D`) `minBlocks` (default 1), `minHistory` (the same as a duration, rounded up to whole blocks; `minBlocks` wins) and `window` (the range of blocks a row reads) for `forward`, `artifact` (`{uri, refit, id}` or the URI string — see *Static fits and artifacts*, *Out-of-fold fits* and *Forward block fits*). |
| engine     | optional | Object                         | Runtime knobs that do not change the plan. `parallelWaves` (default `true`): evaluate the independent stages of each wave in parallel and merge them by row id (see *Performance and sizing*); `false` runs the stages as one linear chain. `rowId`: input fields identifying a row (a natural key) for that merge; without it every row gets a random id pinned by one extra Reshuffle before the first fan-out. `spill`: the per-key sort of the keyed stages — `memoryMB` (in-memory buffer per key before sorted chunks are spilled to worker-local disk; default derived from the worker heap: a quarter of the heap shared by the cores, clamped to 16-256 MB; the `--featureSpillMemoryMB` pipeline option sets it for every feature step), `directory` (spill directory on the worker, default `java.io.tmpdir`), `compress` (deflate the chunk files, default false). See *Performance and sizing*. |
| output     | optional | Object                         | `prefix` (output name prefix), `nullPolicy` (`keep` \| `fillZero` — missing numeric feature values become 0 \| `indicator` — adds `<name>_isnull` flags for sequence / population / validFor columns), `exclude` (a list of `<block>.*` (the whole block), exact canonical column names, block names, or lineage selectors `derivedFrom:market`, `evidence:declared`, `scope:population`, `block:<name>` — not globs or regular expressions; a pattern matching nothing is a warning `output.exclude.unmatched`), `groupBy` (context name), `parentFields` (input fields placed on the parent record), `childName` (field name of the child array, default `rows` — rename it when it collides with a reserved word downstream), `passThrough` (`all` (default) \| `keys` \| `none`: which input fields are copied to the output; input fields are not availability-checked, so `keys` — time.field, entity / context keys, tie-break and parentFields — makes the table safe to consume with `SELECT *`), `roles` (the data contract: `group` / `time` / `entity` / `label` / `baseline` / `weight` → an input field, a context / entity name or a baseline name; role fields always pass through and are recorded in the manifest so consumers never treat them as features), `include` (the output projection: a list of column names, or a URI / path to a JSON array / `{columns: [...]}` / one-name-per-line file such as a screening step's pass list; when declared it replaces `exclude`), `manifest` (URI of the assembly-time manifest, see [Output contract](#output-contract-roles-include-manifest)). |
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
| `type`            | A scalar type name (`float64`, `int64`, `string`, `timestamp`, `date`, ...) or `array<element type>` (e.g. `array<float64>` — the vector input of `type: svd`; nested arrays are not accepted). No feature op produces an array, so an array field always comes from the input. |
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
  - {name: bids, scope: row, type: vector, input: bid_path, funcs: [mean, slope, argmax]}   # readouts of an array<float64> field (see Array readouts)

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
      - {type: regression, field: final_price, against: start_price, funcs: [beta, corr]}   # two series (see Two-series and fractional-difference ops)
      - {type: fracdiff, field: start_price, d: 0.4, k: 20}
      - {type: ewma, expr: "sold >= 1", halflife: [3, 10], decayBy: events}
      - {type: runLength, field: condition_grade, value: good}
      - {type: sinceEvent, predicate: "sold = 1", unit: [events, days]}
      - {type: countMatch, predicate: "sold = 1"}
      - {type: countMatch, predicate: "sold = 0", as: losses}   # as: names the column (two predicates of one op type need it)
      - {type: ewma, expr: "start_price / quantity", halflife: [3], as: unit_price}  # as: replaces the anonymous __e{n} segment
      - {type: aggregate, field: start_price, funcs: [count, mean, max]}
      - {type: aggregate, funcs: [count]}        # COUNT(1): every visible past row, nulls included
      # with a field, a null / NaN / ±Infinity value is missing: it counts for no statistic, count included
      - {type: aggregate, field: sold, funcs: [count, mean], weightBy: "exp(-abs(start_price - $self.start_price) / 50)", as: near}  # similarity-weighted (see Weighted aggregates)

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
percentile (0..100), linearly interpolated between the past values (R type 7 / numpy default). `std`
and the quantiles are read from the key's own past values (no shrinkage: a quantile of an interpolated
distribution is not the interpolated quantile); `distribution` shrinks along the lattice when the block
declares `shrinkage` (Dirichlet-Multinomial per-category pseudo-counts, see *Shrinkage*). All three need
the key's value distribution, so they are available in the expanding fit only — `fit.mode: static` /
`fold` / `forward` keep (n, Σy, Σy²) per key and reject them. A NaN target (or baseline) value counts as
missing for every numeric encoding stat, like null. `distribution` is a map column by default; with
`values: [...]` on the target (`- {field: condition_grade, stats: [distribution], values: [good, fair]}`)
it is emitted as one FLOAT64 column per listed category instead (`<column>_<value>`: the category's share,
0 when it has no mass, null when the key has no history) — the same rule as `countByValue`, for a sink
such as BigQuery or a model that takes flat numeric columns. Categories are matched by their string form
(an INT64 category `1` is `values: [1]`); unlisted categories are not emitted, and `values` on a target
without `distribution` is an error (`encoding.target.values`).

**Baseline offset (`offset: <baseline>`).** A block may subtract a named baseline from its target
(`offset: market` with `baselines: [{name: market, ...}]`; the block then computes at `predictAt`,
`encoding.offset.computeAt`). The offset is an additive term on the shrinkage scale:

- `scale: identity` (default) — every statistic is taken over `target − baseline`: `mean` / `rate` are the
  key's mean residual (shrunk toward the parent's), `std` the residual spread. A past row whose baseline is
  missing (or NaN) has no residual: it is left out of every statistic of the block, `count` included, in the
  expanding replay and in the static / fold / forward fits alike.
- `scale: logit` / `log` — each level's own term is `t(observed) − t(mean baseline)` over the level's rows
  (the observed-over-expected **log-odds ratio** on logit, the Poisson-offset MLE `log(Σy / Σb)` on log),
  the leaf shrinks that term toward the parent's term, and the **composed value is the term itself** — a
  residual on the scale, *not* a probability or rate — with `deviations` on the same scale (info
  `encoding.offset.additive`). The levels keep a hidden `Σ baseline` (`<level>__sumoff`) next to
  `Σ(y − b)`, in the expanding replay and in the static / fold / forward artifacts alike; `std` and the
  quantiles stay statistics of the identity residual. A level whose mean baseline is outside the scale
  (`Σ baseline ≤ 0`, or `≥ n` on logit — e.g. a baseline that is 0 for every row of a cold-start key) has
  no term of its own and falls back to its parent, as an unseen level does. `estimator: joint` fits the
  same per-cell terms (such a cell is skipped). A keySet with its own identity-scale or disabled `shrinkage`
  stays on the residual statistics above.

### Two-series and fractional-difference ops (sequence `regression`, `fracdiff`)

```yaml
- name: vs_index
  scope: sequence
  entity: seller
  windows: [{maxAge: P90D}]
  ops:
    - {type: regression, field: final_price, against: start_price, funcs: [beta, corr]}   # vs_index_90d_final_price_vs_start_price_beta / _corr
    - {type: regression, field: final_price, against: start_price, lag: 1, funcs: [corr]} # ..._vs_start_price_lag1_corr
    - {type: fracdiff, field: start_price, d: 0.4, k: 20}                                 # vs_index_90d_start_price_fracdiff0p4
```

- **`regression`** reads two fields of the entity's past events — `field` (y) regressed against `against` (x):
  `cov` (population covariance), `corr`, `beta` (= cov / var x, the slope of y on x), `intercept`, `r2`
  (default `[beta, corr]`). An event contributes when both values are present (not null / NaN / ±Infinity); every func needs two
  contributing events; `corr` / `r2` are null when either series is constant, `beta` / `intercept` when x is.
  The other series is an ordinary field of the row (a market or group series joined onto each row upstream).
  The key is `against`, not `on` — YAML 1.1 reads a bare `on` as a boolean.
- **`lag: k`** (lead-lag) pairs `field` of each event with `against` **k events earlier** inside the window —
  "does x lead y by k events" (swap the two fields for the other direction; `lag` ≥ 0). Columns are named
  `..._lag<k>_<func>`.
- **`fracdiff`** is the fractional difference `(1 − B)^d` of the field, truncated to its first `k`
  coefficients (`w0 = 1`, `wj = −w(j−1) · (d − j + 1) / j`; default `k: 20`) and applied to the last `k` past
  events: `0 < d < 1` makes a level series stationary while keeping memory, `d: 1` is the plain first
  difference. It needs `k` past events without a missing value among them (null otherwise — a fixed-width
  filter over fewer terms would be a different series). Like every sequence op it is strictly past: the value
  as of the latest past event, not including the current row.
- **Cost / retention.** A same-event `regression` runs incrementally (running cross moments, evicted under
  `maxAge`) — O(1) per row like a plain aggregate. A lagged `regression` is evaluated by scanning its window
  per row: bound it with `maxAge` or `maxEvents` (otherwise `sequence.window.unbounded`). `fracdiff` keeps
  only its last `k` events.
- Diagnostics: `sequence.regression.against` (missing / non-numeric), `sequence.regression.func`,
  `sequence.regression.lag`, `sequence.fracdiff.d` (required, in (0, 2]), `sequence.fracdiff.k` (≥ 2).

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
per level; a fully shrunk level's infinite λ appears as the string `"Infinity"`) derived from the persisted
statistics, so a run's shrinkage can be audited. `artifact.id` pins an explicit version directory instead of the hash. When an artifact
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
    minHistory: P180D                             # alternative to minBlocks: the minimum history, rounded up to blocks
    window: P2Y                                   # optional: a row reads the blocks within this range only (rounded up to blocks)
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
history read nothing (`count` reads 0, the other statistics null); `minHistory` says the same as a duration
(rounded up to blocks; an explicit `minBlocks` wins). Windows: a keySet's `maxAge` is rounded up to
whole blocks (`fit.mode.forward.window`), `maxEvents` / `filter` are ignored (`fit.mode.forward.windowIgnored`);
`fit.window` is the block-level default range for keySets that declare no `maxAge` (a rolling fit —
the usual choice under drift, where `expanding` statistics go stale) and the range of a forward `svd`.
Sufficient statistics only (count / sum / mean / rate / std; `quantile` / `distribution` are expanding
only, `encoding.stat.static`). With `weights: varianceComponents` the pseudo-count λ is estimated per
block from the keys' statistics up to that block, and recorded per block in the artifact manifest
(`lambdasByBlock`). Block size trades staleness against stability: yearly blocks leave the first year
empty and miss within-year drift, `P90D` is a good default; `blocks.bucket` gives calendar alignment
(UTC). The `blocks` / `minBlocks` / `minHistory` / `window` settings are part of the plan hash. Batch only.
A `type: svd` or `type: quantileTransform` block inherits this `mode` unless it declares its own (see *SVD / PCA*
and *Quantile transform*); the other population types (factorization / discretize) are always static and are
unaffected.

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

**Time folds (`fold: {by: time}`).** Hash folds mix every period into every fold, so a row's out-of-fold statistics
still contain its neighbours in time — the rows whose labels describe the same days. With `by: time` every time block
is a fold:

```yaml
  fit:
    mode: fold
    blocks: {bucket: month}                       # the folds (as fit.mode forward: bucket or size, default P90D)
    fold: {by: time, purge: P20D, embargo: P7D}
```

- A row in block `b` reads the statistics of the whole input minus the blocks `[b − purge, b + purge + embargo]`: its
  own block, the **purge** on both sides of it (the rows whose label window overlaps the row's — a label window
  overlaps its neighbours before *and* after it) and the **embargo**, an extra buffer after the purge (e.g. for
  serially correlated features). Both are rounded up to whole blocks (`purge: P20D` with 7-day blocks leaves 3 blocks
  out on each side); a calendar bucket counts its shortest length (28 days a month, 90 a quarter, 365 a year), so the
  range never falls short. Declaring an `embargo` only widens the range — it never replaces the purge.
- `purge` defaults to the horizon of the label the target reads — a `direction: future` column, directly or through a
  row expression (info `fit.fold.purge`); other targets default to no purge. `embargo` defaults to none.
- A row leaves out `2 × purge + embargo + 1` blocks. When that is more than half of the input's blocks the
  out-of-fold statistics read a minority of the data: the engine logs a warning and counts the rows in the counter
  `feature/timeFold_<level>_excludedOverHalf` (the input's block span is only known at run time) — use smaller blocks
  or a shorter purge / embargo.
- `folds` and `groupBy` do not apply (every block is a fold); `purge` / `embargo` without `by: time` are ignored with
  a warning (`fit.fold.ignored`), `by` is `row | time` (`fit.fold.by`), a negative `purge` / `embargo` is an error
  (`fit.fold.negative`). `estimator: joint` solves hash folds only
  (`fit.fold.time.joint`). A keySet key derived from a past target stays an error (`fit.groupBy.required`) — the
  entity's rows in the other blocks carry this row's outcome in their key, and `groupBy` does not help here: use
  `by: row` with `groupBy`.
- Like every fold the result is a cross-fit (later blocks are read), batch only; the per-block statistics are one
  parallel Combine per (key, block), as in `fit.mode: forward`, and an `artifact` holds the whole-input totals.

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

### Quantile transform (population, type: quantileTransform)

```yaml
  - name: price_q
    scope: population
    type: quantileTransform
    input: start_price                 # numeric field or feature
    bins: 100                          # quantile intervals of the fitted CDF (default 100)
    distribution: uniform              # uniform (default): F(v) in [0, 1] | normal: the normal score Φ⁻¹(F(v))
    # clip: 0.001                      # normal only: clamp F(v) to [clip, 1 − clip] before Φ⁻¹ (default 1e-6)
    fit: {artifact: {uri: "gs://bucket/features"}}   # fit.mode static, forward (below), or inherited from the top-level fit
```

Rank-based normalisation: the whole input's distribution is summarised by `bins + 1` knots (the type-7
quantiles at `0, 1/B, …, 1`) and a value maps to its position in it, interpolated linearly between knots —
monotone, scale-free and robust to outliers, and the same map at training and serving time. A value below
the fitted minimum reads 0, above the maximum 1 (`normal`: clamped at `±Φ⁻¹(1 − clip)`, never infinite); a
value equal to a run of tied knots (a mass point, also one sitting at the minimum or the maximum — a
zero-inflated count's zeros) reads the middle of the run's probability range; missing (null / NaN) reads
null. Like discretize the values are gathered on one worker for the fit
(8 bytes per row) and an input without a single value still fits (n = 0: every value reads null — a serving
run that loads such an artifact logs a warning). The artifact is `<planHash>/<block>.quantiles.json` (knots,
bins, n, distribution, clip for `normal`).

`clip` (normal only, `0 < clip < 0.5`, default `1e-6`): with the default the fitted minimum and maximum read
`±4.75` whatever n, and the first / last quantile interval is interpolated in *value*, not rank, so the extreme
rows become outliers of a downstream linear combination (`expr`) or `svd`. `clip: 0.001` caps the score at
`±3.09` (`0.01` → `±2.33`) and leaves every position inside `[clip, 1 − clip]` unchanged; it changes the
fitted transform, so the plan hash and the artifact directory. The clip is applied by the config that runs, not
by the artifact: a serving config that pins an artifact (`fit.artifact.id`) or loads one fitted before `clip`
existed still clamps at its own `clip`. With `distribution: uniform` it has no effect on the output but still
participates in the plan hash — the warning `quantileTransform.clip` asks you to remove it.

**Forward fit (`fit: {mode: forward, blocks, window, minBlocks | minHistory}`).** A static quantile transform
ranks every row against a distribution that includes the test period — no label leaks, but a drifting field (a
price level, a volume) is placed where it could not have been placed at the time. Under `forward` the values are
gathered per time block and the knots are fitted for every block window a row may read — the complete blocks within
`window` (all preceding blocks when absent) whose input is known at predictAt, the row's own block excluded (`fit.mode.forward`
info; an outcome input delays the readable blocks by its settlement + ingestion lag) — so training and serving see
the same walk-forward ranks. The fit is **exact**: the knots are the order statistics of exactly those values, identical
to a static fit run on them. Rows with fewer than `minBlocks` (or `minHistory`) preceding blocks read null, as does a
window holding no value. A block that declares no `fit.mode` of its own inherits a top-level `fit: {mode: forward}`
(geometry included); `fit: {mode: static}` on the block opts it out and says so (`quantileTransform.fit.mode.static`
info). The artifact still holds the whole-input knots, for a static serving run, and a forward fit is re-fitted every
run. Cost: the values still meet on one worker (8 bytes per row, as in the static fit); the knots are re-fitted once per
change point (every observed block, plus one per block leaving a `window`), each a sort of the readable values — tens
of blocks over a few million rows is seconds.

### SVD / PCA (population, type: svd)

```yaml
  - name: hist_pc
    scope: population
    type: svd
    inputs: [recent_n5_start_price_lag1, recent_n5_start_price_lag2, recent_n5_start_price_lag3]   # the vector
    # input: embedding                 # or one input field declared `type: array<float64>` in the sources contract
    rank: 2                            # score columns hist_pc_0, hist_pc_1 (default min(d, 8); required for an array input)
    center: true                       # subtract the fitted means (default true)
    standardize: false                 # divide by the fitted standard deviations (PCA of the correlation matrix; the RMS when center: false)
    outputs: [scores]                  # scores (default) | residual | residualNorm — see "Residuals" below
    fit: {artifact: {uri: "gs://bucket/features"}}   # fit.mode static, forward (below), or inherited from the top-level fit
```

The "Compress" step of the sequence frame: the vector is centred (and optionally standardised) with the
whole-input moments and projected onto the leading `rank` right singular vectors, giving decorrelated scores
ordered by explained variance (`<name>_0` carries the most). The fit needs only (n, Σx, Σxxᵀ), accumulated
relative to the first vector so a large offset (epoch times, ids) does not cancel the covariance away — one
Combine over the rows, no row leaves the workers — and diagonalises the d × d covariance on the driver (d =
the vector length, tens to a few hundred). Components are oriented so the largest loading is positive (a
re-fit reproduces the scores). A vector with a missing component (null / NaN) takes no part in the fit and
reads null scores. An array input must have one length: vectors of another length are skipped (and read
null) and the run logs a warning — the fitted length is whichever the fit saw first, so normalise the array
length upstream; when `rank` exceeds an array's length the fit caps the components at the length, the
surplus score columns read null and a warning names the cap (for `inputs` the compiler rejects the rank). An
array input is always an input field (declared `type: array<float64>` — or another numeric element type —
in the sources contract and listed in `lineage`): no row / context / sequence op produces an array, so a
vector assembled from features uses `inputs`. A fit with fewer than two vectors has no components and reads
null everywhere (a serving run that loads such an artifact logs a warning). The artifact is
`<planHash>/<block>.svd.json` (mean, scale, components, per-component variances, total variance, n) — the
explained-variance ratio is `variances[k] / totalVariance`.

**Forward fit (`fit: {mode: forward, blocks, window, minBlocks | minHistory}`).** A static svd places every row in
a distribution that includes the test period (no label leak, but a drifting field is placed where it could not
have been placed at the time). Under `forward` the moments are accumulated per time block (one `Combine` per
block) and the components are re-solved for every block window a row may read — the complete blocks within
`window` (all preceding blocks when absent) whose inputs are known at predictAt, the row's own block excluded
(`fit.mode.forward` info) — so training and serving see the same walk-forward components; rows with fewer than
`minBlocks` (or `minHistory`) preceding blocks read null, as does a window whose blocks hold fewer than two
vectors. A block that declares no `fit.mode` of its own inherits a top-level `fit: {mode: forward}` (geometry
included), so the whole spec walks forward together; `fit: {mode: static}` on the block opts it out and says so
(`svd.fit.mode.static` info). The artifact still holds the whole-input components, for a static serving run.

**Residuals (`outputs: [scores, residual, residualNorm]`).** The scores say where a vector sits on the leading
`rank` components; the residual is what those components do not explain — `x − mean − scale · Σ score_k ·
component_k`, per input and **in the units of the input** (the idiosyncratic part of each series once the common
factors are taken out). `residual` emits one column per input, `<name>_resid_<input>`, and needs named `inputs`
(an array has no named dimensions: `svd.outputs`); `residualNorm` emits `<name>_residnorm`, the Euclidean length
of the residual vector, and works for an array input too. With every component kept (`rank` = the vector length)
the residual is 0. The columns share the block's fit (static or forward) and read null wherever the scores do.

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
      estimator: sequential                       # backoff (chains, default) | sequential (additive / cross, default) | joint (fit.mode static / fold / forward only)
      weights: varianceComponents                 # fixed: w = n / (n + priorWeight); varianceComponents: λ = σ²/τ² per level (batch method of moments)
      priorWeight: 20                             # fixed pseudo-count, and the fallback when a level has too few keys
      family: gaussian                            # gaussian | betaBinomial | gammaPoisson | dirichletMultinomial; default derived from the stat
      scale: logit                                # identity | logit | log; required when a lattice uses additive
      leaveNodeOut: true                          # subtract the leaf's own statistics from every ancestor
      output: [composed, deviations, effectiveN]  # composed (default) | deviations (dev0, dev1, ...) | effectiveN (<stat>__neff)
```

`smoothing: {type: bayesian, priorWeight: N}` is accepted as the legacy spelling of fixed-weight
shrinkage toward the global mean. Every lattice level is evaluated as its own keyed stage over the same
window and target, and the composition is a per-row formula: `est(level) = est(parent) + w · (t(mean) −
est(parent))` from the global level down to the key, on the declared scale. `share` is
`n_key / n_global` over strictly-past rows.

**Estimators.** `backoff` is the top-down pass above, one level at a time; `sequential` (the default of a
lattice with `additive` / `cross`) shrinks a cell toward the sum of the shrunk main effects, so it absorbs
part of an interaction into whichever main effect comes first when the keys are confounded. `joint` fits
every level of the lattice **simultaneously** as one ridge / BLUP system over the lattice's cells (the
indicator basis of every level's contexts, `t(y) = μ + Σ e_level + ε`): confounded contexts are separated
without an order-dependent bias, and `deviations` are the orthogonalised per-level effects (`dev0` = the
leaf, then each coarser level, on the transform scale) while `effectiveN` is the leaf's `n + λ`. The solve
needs the whole cell table, so `joint` requires `fit.mode: static | fold | forward` (one solve; one per fold
on the other folds' cells; under `forward` one per block window — a row reads the solution over exactly the
blocks `(usable − windowBlocks, usable]`, like the per-level statistics, and gets null when nothing lies in
its window) and is rejected under `expanding`. The cells are aggregated in parallel and solved on one worker
(block Gauss–Seidel until convergence); λ per level is the same moment estimator as `varianceComponents`
(over the level's contexts) or `priorWeight` under `fixed`, a pseudo-count in rows on every scale (on
`logit` / `log` the ridge is rescaled by the context's mean delta-method weight, so a leaf with `n` rows
keeps weight `n / (n + λ)` as under the other estimators), and a level whose between-context variance
truncates to zero is fixed at 0. A row whose leaf key is null has no estimate; a null key on a coarser
level leaves the row in the intercept and the levels it has (no effect for that level, as at apply time).
The artifact is `<block>__<keys>__<window>__<target>.joint.avro` per keySet × target, holding the
whole-input solution; `share` and unshrunk stats of a joint block still use the per-level statistics.

**Families.** Shrinkage adds pseudo sufficient statistics inherited from the parent, so on the identity
scale the Gaussian, Beta-Binomial (`rate` of a 0/1 target) and Gamma-Poisson (`mean` of a count target)
posterior means coincide — `parent + n / (n + λ) · (ȳ − parent)` — and the one-way moment estimator of λ is
also the Beta-Binomial (Kleinman) moment estimator, so `family` changes neither the value nor λ of a
scalar statistic; it is derived from the stat (`mean` → gaussian, `rate` → betaBinomial, `distribution` →
dirichletMultinomial) and recorded in the lineage, and an explicit family must fit the stat
(`encoding.shrinkage.family.stat`). The conjugate closed forms need `scale: identity`: on `logit` / `log`
the derived family of `mean` / `rate` is gaussian (Gaussian shrinkage of the transformed statistics with a
delta-method weight, the spec's rule 7 — the lineage records `gaussian`), a *declared* conjugate family
there is an error (`encoding.shrinkage.family.scale`), and a `distribution` is emitted unshrunk with a
warning of the same code. `distribution` under shrinkage is the Dirichlet-Multinomial case:
each level's per-category counts shrink toward the (leave-node-out) parent distribution,
`p(level) = (counts + λ · p(parent)) / (n + λ)`, and the composed column is a map of probabilities over the
categories seen at any level — chain lattices only (`encoding.shrinkage.family.lattice`), `backoff` only,
expanding only, with `priorWeight` as λ (`varianceComponents` has no scalar target to estimate from,
`encoding.shrinkage.weights.distribution`) and no `deviations`.

A feature must not reuse the name of an input field (in-place overwrite is rejected). Generated column
names: row `<name>` (datetime `<name>_<derive>[_sin|_cos]`), context
`<name>_<field>_<op>`, sequence `<name>_<window>_<field>_<op><param>` (window token: `n5`, `365d`,
`365d_n20`, `all`), encoding per `naming` (empty segments collapse). `output.prefix` is prepended; columns
that are only usable offline get a leading `_` and are not emitted.

Inline `expr` in sequence ops and encoding targets is evaluated per past row (no `$self`); expressions are
numeric (Lucene expression syntax), predicates and window filters use the SQL-like
[Filter](../common/filter.md) syntax.

### Weighted aggregates (sequence `aggregate` with `weightBy`)

A window `filter` selects past events 0 / 1 (`category = $self.category`); on sparse histories — a new
seller, a listing unlike the earlier ones — an equality filter leaves nothing. `weightBy` keeps every
event and weighs it by how similar it is to the current row instead:

```yaml
- name: similar
  scope: sequence
  entity: seller
  windows: [{maxAge: P365D}]
  ops:
    - type: aggregate
      field: sold
      funcs: [count, mean]
      weightBy: "exp(-abs(start_price - $self.start_price) / 50)"   # the event's fields by name, the current row's as $self.<field>
      as: near                                                      # similar_365d_near_count / _mean
```

- The expression is numeric (the row `expr` syntax; operands numeric / bool). A name reads the **past
  event**, `$self.<field>` reads the **current row**; a weight without `$self` is a plain per-event weight.
- An event contributes when its value is present (not null / NaN / ±Infinity) and its weight is a positive
  finite number. A null operand on either side, a NaN and a weight ≤ 0 contribute nothing — so a current row whose `$self` field
  is null gets `count` 0 and null for the rest.
- Funcs: `count` = Σw (the *effective count*, **float64** — 0 when nothing contributes), `sum` = Σw·x,
  `mean` / `avg` / `rate` = Σw·x / Σw, `std` = the weighted population deviation (two contributing events at
  least). With every weight 1 these are the plain aggregates. `min` / `max` / `first` / `last` have no
  weighted form (`sequence.weightBy.func`). Without a `field`, `count` weighs every visible row.
- `weightBy` composes with the window (`maxAge`, `maxEvents`, `filter` — the window selects first, then the
  weights apply) and is only defined on `aggregate` (`sequence.weightBy.op`).
- **Availability**: the event side follows the sequence rule (an outcome read from past events shifts the
  window like any other past input); the `$self` side must be known at `predictAt` — `$self.<outcome>` is
  an `availability.violation`.
- **Cost**: the weights differ for every (row, event) pair, so no running statistic can serve them: the
  aggregate scans its window for every row (info `sequence.weightBy.scan`) instead of the O(1) incremental
  update of a plain aggregate. Give the window a `maxAge` or `maxEvents`; without either the column keeps
  the key's whole history and is listed by the `sequence.window.unbounded` hint.
- A plain and a weighted aggregate of one field in one block would share a column name: set `as:` on one.
- Diagnostics: `sequence.weightBy.op`, `sequence.weightBy.func`, `sequence.weightBy.type` (a non-numeric
  operand), `sequence.weightBy.parse`.

### Naming, conditions and placement notes

- `as:` on a sequence / context op names the output column segment: for `sinceEvent` / `countMatch` it replaces
  the op suffix (`<block>_<window>_<as>[_<unit>]`), otherwise the field segment — which is how an inline
  `expr` avoids the anonymous `<block>__e{n}` name. That `n` counts expressions across the whole spec, so adding
  or removing an earlier one renames the columns: name an op `expr` with `as:` (`sequence.expr.anonymous` lists the
  unnamed ones). On an encoding target `as:` replaces the target name
  (`<block>__<keys>__<as>__<stat>`).
- `countByValue` / `ratioByValue` produce a `map` column by default; with `values: [...]` they produce one
  numeric column per value (`<block>_<field>_countByValue_<value>`, absent value = 0 / null ratio). Prefer
  `values` when the output goes to a sink such as BigQuery or straight into a model. An encoding target's
  `distribution` stat follows the same rule (`targets[].values`, see *Encoding stats*).
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

### Array readouts (row, `type: vector`)

A numeric array field (`type: array<float64>` in the sources contract — the within-event series a row
carries: the bids observed before a session, the split times of a run, the levels of an order book)
becomes scalar columns: optional vector → vector **steps**, then one column per **readout**.

```yaml
- name: bid_step
  scope: row
  type: vector
  input: bid_path            # array<float64> (any numeric element type)
  slice: {from: -3}          # 1. elements [from, to); a negative index counts from the end; bounds are clamped
  diff: 1                    # 2. differences of adjacent elements, applied <diff> times
  normalize: mean            # 3. sum | mean | l2 | zscore — rescaled by the vector's own statistic
  position: unit             # slope / polyfit positions: index (default: 0, 1, 2 …) | unit (index / (n − 1), in [0, 1])
  funcs: [mean, slope, polyfit]
  degree: 2                  # polyfit degree, 1..5 (default 2)
```

The steps always run in the order slice → diff → normalize; all three are optional. Readouts:

| func | output | value |
|---|---|---|
| `length` | `<name>_length` int64 | number of elements after the steps (0 for an empty vector) |
| `sum` / `mean` / `min` / `max` / `first` / `last` | `<name>_<func>` float64 | the usual meaning |
| `std` | `<name>_std` float64 | population standard deviation (needs 2 elements — the convention of the sequence / encoding `std`) |
| `argmin` / `argmax` | `<name>_<func>` int64 | index of the first minimum / maximum **within the vector the steps produced** (a slice re-bases it to 0) |
| `norm` | `<name>_norm` float64 | Euclidean length |
| `slope` | `<name>_slope` float64 | least-squares slope over the positions (needs 2 elements; with `position: index` the value of the sequence `trend`) |
| `polyfit` | `<name>_poly0` … `<name>_poly<degree>` float64 | least-squares polynomial coefficients in ascending order, `c0 + c1·p + …` (needs `degree + 1` elements) |

- **Positions.** `index` measures `slope` / `polyfit` per element; `unit` spreads the elements over [0, 1],
  which makes the coefficients comparable between rows whose arrays differ in length.
- **Nulls.** A null array, or an array holding a null / NaN / infinite element, reads null for every
  readout (`length` included): a vector with a hole has no defined readout. A readout that is undefined on
  the vector at hand — too few elements, a `normalize` whose denominator is 0, a non-finite result — is
  null, never NaN. An empty vector (an empty array, or a slice beyond it) has `length` 0 and no other
  readout. A `repeated` input field without a value arrives as the empty array, not as null.
- **Availability and lineage** are the array field's own: the readouts are ordinary row columns, so an
  `expr` composes them (`bids_last / bids_mean`), a context / sequence / encoding block consumes them, and
  an array that is an outcome is rejected like any other outcome field (`availability.violation`).
- Several views of one array are several blocks (the whole path and its last three elements, say); relate
  them with an `expr`.
- Diagnostics: `row.vector.input` (not an array of numbers), `row.vector.funcs` (missing / unknown /
  listed twice), `row.vector.slice`, `row.vector.diff`, `row.vector.normalize`, `row.vector.position`,
  `row.vector.degree` (out of 1..5; a warning when `degree` is set without `polyfit`).

Every parameter is part of the plan hash. The array field itself still passes through to the output
unless `output.passThrough` / `include` drops it, and `type: svd` takes the same kind of field as its vector.

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

### Shape and series summaries (sequence `aggregate` funcs)

Besides the moments and the extremes, `aggregate` reads scalar summaries of *how* the past values are
distributed and ordered — the descriptive statistics of a short series:

```yaml
- name: price_shape
  scope: sequence
  entity: seller
  windows: [{maxEvents: 30}]
  ops:
    - {type: aggregate, field: start_price, funcs: [skew, kurt, peaks, acf1, pacf2, ar2_1]}
    - {type: aggregate, expr: "start_price - 100", funcs: [zeroCross], as: vs100}   # crossings of a level: subtract it first
```

| func | output | value |
|---|---|---|
| `skew` | float64 | m₃ / m₂^1.5 of the window's values (population moments, the `std` convention); three values at least |
| `kurt` | float64 | excess kurtosis m₄ / m₂² − 3; four values at least |
| `zeroCross` | int64 | sign changes between consecutive non-zero values (a zero has no sign) |
| `peaks` | int64 | strict local maxima — values above both neighbours; the two ends never count |
| `acf<j>` | float64 | sample autocorrelation at lag j (in events), the biased estimator Σ(x_t − x̄)(x_{t−j} − x̄) / Σ(x_t − x̄)²; more than j values |
| `pacf<j>` | float64 | partial autocorrelation at lag j (the last coefficient of the Yule–Walker AR(j) fit) |
| `ar<p>_<i>` | float64 | i-th coefficient (1 ≤ i ≤ p) of the AR(p) model solved from the Yule–Walker equations (Levinson–Durbin) |

- j and p run 1..20. Missing values (null / NaN / ±Infinity) are dropped first: the series is the window's
  present values in time order.
- A window without spread (a constant series, up to rounding) has no `skew` / `kurt` / `acf` / `pacf` / `ar`
  (null, never NaN); `zeroCross` / `peaks` are null only when the window holds no value at all.
- **Cost.** `skew` / `kurt` are sums of per-event contributions (power sums up to order four): they fold
  incrementally and evict under `maxAge` like `mean` / `std`. The series readouts read *neighbouring* values, so
  they are evaluated by scanning the window for every row — give the window `maxEvents` or `maxAge` (a window
  with neither keeps the key's whole history: `sequence.window.unbounded`).
- An unknown func — or a lag / order outside 1..20, an `ar` index outside 1..p — is `sequence.aggregate.func`;
  the message lists what is available.

### Path summaries (sequence general form: `lift` + `summarize`)

Instead of `ops`, a sequence block can summarise its window as a **path**: `lift` names the channels read from
every past event, `summarize.dynamics` the linear recurrence folded over them. The result is a fixed-length
vector per channel, emitted as one FLOAT64 column per component.

```yaml
- name: price_path
  scope: sequence
  entity: seller
  windows: [{maxAge: P365D}]
  lift:
    fields: [start_price, final_price]      # numeric (or boolean) fields / block columns
    exprs:                                  # expression channels (desugared like an op's expr)
      - {expr: "final_price / start_price", as: ratio}   # as: names the channel (a bare string gets the anonymous <block>__e{n})
    timeAugment: true                       # adds the constant channel `time`
  summarize:
    dynamics: {family: lti, measure: exponential, order: 2, halflife: [7, 30], decayBy: time}
# price_path_365d_start_price_exp7_0 .. _2, price_path_365d_start_price_exp30_0 .. _2, ..., price_path_365d_ratio_exp7_0 .. _2,
# price_path_365d_time_exp7_1 .. _2
```

Each component is a weighted mean over the window's present values, `Σ w_i · x_i · b_j(age_i) / Σ w_i` — a
projection of the path onto a basis `b_j` under the measure `w`:

| `measure` | weight w | basis b_j (component j) | parameters | columns per channel |
|---|---|---|---|---|
| `exponential` | `2^(−age / halflife)` | Laguerre polynomial `L_j(ln 2 · age / halflife)` — component 0 is exactly `ewma` | `halflife` (required, a list: one state each), `order` 0..16 (default 0) | order + 1: `_exp<h>_<j>` |
| `fourier` | 1, or `2^(−age / halflife)` with a `halflife` | the constant (`c0`), then `cos` / `sin(2π k · age / period)` for k = 1..order | `period` (required), `order` 1..16 (default 1), `halflife` (optional) | 2·order + 1: `_fourier<P>_c0`, `_c<k>`, `_s<k>` (`_fourier<P>h<h>_…` when damped) |
| `legendre` | 1 | shifted Legendre polynomial `P_j(2u − 1)`, u = position over the window's own span (first event → 0, now → 1) | `order` 0..8 (default 3) | order + 1: `_leg_<j>` |

- **The clock** (`decayBy`): `events` (default) measures age in events — the newest past event is 0, as `ewma`
  counts — and `time` in days. On `time`, `fourier` and `legendre` measure age from the current row's time
  (`legendre`: u = (event − first event) / (row − first event)), while `exponential` measures it from the newest
  past event: its higher components would otherwise grow with the entity's inactivity (≈ (gap / halflife)^j), so the
  gap is a feature of its own (`sinceEvent` with `unit: [days]`); component 0 (`ewma`) is the same either way. A missing value (null / NaN / ±Infinity) is still an event on the `events` clock; it adds no
  weight.
- **Reading the components.** Component 0 is the (decay-weighted) mean. The higher Laguerre components weigh
  recent and older events with opposite signs (`L_1 = 1 − u`): a trend of the value against its age. The Fourier
  components pick up periodicity at `period`, `period / 2`, …; the Legendre ones the shape of the path over the
  window (level, slope, curvature, …). The `time` channel's components describe *when* the events happened
  (its component 0 is always 1 and is not emitted). It reads no field, but it summarises the same events as the
  block's value channels: when a channel is an outcome whose window is shifted, the `time` channel takes the
  latest channel's shift too (`sequence.lift.align` when the channels differ).
- **Cost.** Every measure is a running state: `exponential` and `fourier` are exact under any spacing and evict
  under `maxAge` in O(1) per row; `legendre` rescales with the window's span, so it runs on a running state without
  `maxAge` and re-reads the window under one. None of them keeps the key's history without a window (no
  `sequence.window.unbounded` hint, `ewma` included) — except under a `filter` without `maxAge`, as for any op.
- Availability, windows (`maxEvents` / `maxAge` / `filter`), the window shift of an outcome channel and the
  naming prefix `{block}_{window}_{channel}` are those of the ops.
- **Diagnostics**: a block uses either `ops` or `lift` + `summarize` (`sequence.form`); `summarize` needs
  `dynamics` (`sequence.summarize`) with `family: lti` (`sequence.dynamics.family`: `bilinear` log-signatures are
  not implemented yet) and a `measure` (`sequence.dynamics.measure`); `sequence.dynamics.order` /
  `.halflife` / `.period` / `.decayBy` / `.parameter` check the parameters; channels must be numeric
  (`sequence.lift.type`); a block emitting more than 64 component columns (windows × halflifes × channels ×
  components) is `sequence.dynamics.size`; `compress` is not implemented (`sequence.compress` — feed the
  component columns to a population `svd` block).

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
- **label** — post-event by construction (a `direction: future` block) or by declaration (`output.roles.label`):
  emitted as a label, never a feature (see "Labels over the future" below).

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
  entry keeps its base column) are emitted, plus the pass-through fields and the role columns. Names matching no column are a
  warning (`output.include.unknown`) — the list may come from another plan version. An empty list is an error
  (`output.include.empty`): the table would carry no feature column (a screening step that passed nothing). `include` and
  `exclude` are not combined: when `include` is declared, `exclude` is ignored (`output.include.exclude`).
  An `exclude` pattern that selects no emitted column is a warning (`output.exclude.unmatched`) — the
  patterns are exact names, `<block>.*` or selectors, so `dm.*__distribution` matches nothing while
  `dm.*` or the full column name does.
  A URI is read at assembly and its content hash recorded, so a file that changes later is still traceable.
  A column a role names (a baseline's `emit` copy, a label derived as a column) is emitted whether or not
  the list contains it — a pass list never names role columns, they were never candidates — and the plan
  report says which were kept (`output.include.role`). `exclude` follows the same rule (`output.exclude.role`:
  a market baseline's `emit` copy survives `derivedFrom:market`). A column kept only as a role gets no
  `<name>_isnull` indicator under `nullPolicy: indicator` — the flag would be a feature column the
  projection never admitted.
- **manifest** writes `manifest.json` at assembly (a dry run writes it too): `planHash`, **`outputHash`**
  (plan hash + emitted names + roles + include content — the identity of the output table, since a
  projection does not change the plan hash), `roles` (with the resolved column / keys), `include` (source,
  hash, listed and unknown names), `fields` (pass-through input fields with `scope: input`, source / kind /
  `derivedFrom` / availability / evidence and their role), `columns` (every emitted column: type, `categorical`, scope, block, operator,
  availableAt / computeAt / status, placement, lineage — `derivedFrom`, `sources`, `evidence`, inputs),
  `artifacts` (fitted blocks → artifact path) and the full `plan` report. A batch run also writes
  `manifest.run.json` next to it at finalize with what only execution knows: the output row count and the
  observedAt audit results.

`include` and `manifest` are outside the plan hash (artifacts stay valid across projections);
`roles` and the projection are inside `outputHash`.

The output schema carries the same lineage as field options, so a direct downstream (the `screen`
transform, a `select`) can read it without the manifest: every emitted column its `feature.scope` /
`feature.block` / `feature.derivedFrom` / `feature.evidence` / …, every pass-through input field
`feature.scope = input`, `feature.kind`, `feature.derivedFrom` (its kind, plus the lineage the field
already carried when it came from another feature transform), `feature.sources`, `feature.availableAt`,
`feature.evidence`, and a role's field or column `feature.role`. A consumer's `derivedFrom:market` or
`scope:input` selector therefore drops a passed-through market input the same way it drops a column
derived from one, and a `screen` directly downstream takes its `group` / `label` / `baseline` / `weight` /
`time.field` defaults from `feature.role` as it would from the manifest's `roles`. When several fields carry one
role (the columns of a `direction: future` block are all labels, below), the schema cannot tell which one is
declared: the role is then taken from the manifest, or the consumer names it itself.

### Labels over the future (`direction: future`)

A sequence block with `direction: future` reads, for every row, the entity's **strictly-future** window
`(t, t + maxAge]` instead of its past — the forward-looking labels of a time-series task, computed by the same
engine that computes the features, with the same keys and the same row identity:

```yaml
- name: next
  scope: sequence
  entity: seller
  direction: future
  windows: [{maxAge: P20D}]                               # the label horizon (required)
  ops:
    - {type: aggregate, field: final_price, funcs: [count, mean, last]}
    - {type: lag, field: start_price, k: 1}                 # next_20d_start_price_lead1: the next event's value
    - {type: barrier, field: start_price, up: 0.1, down: -0.05}   # first barrier touched
    - {type: sinceEvent, predicate: "sold = 1", unit: [days]}      # next_20d_until_days
- name: ret
  scope: row
  expr: "next_20d_start_price_lead1 / start_price - 1"
output:
  roles: {label: ret}
```

- **They are labels, never features.** Every column of the block gets the status `label` (the role `label` stays
  with the one column `output.roles.label` names, so a downstream screen / evaluation defaults to it):
  it is emitted (whatever `include` / `exclude` say), has no `_isnull` companion under `nullPolicy: indicator`,
  and its `availableAt` is the horizon plus the availability of what it reads (`final_price`, known 6 days after
  its own event, gives `event_time + P20D + P6DT30M`) — so any *feature* referencing it (a row expression, a
  context op, another sequence block) is an `availability.violation`. A column derived from labels is a label only
  when `output.roles.label` names it (`ret` above); `output.roles.label` names the one label consumers default to.
  An encoding whose *target* is a label is fine: the past labels become visible once their horizon has passed.
- **Window.** `maxAge` is required (`sequence.direction.maxAge`) and may combine with `maxEvents` (the next n
  events) and `filter`. Rows sharing the row's timestamp are not in its future (as they are not in its past); the
  window is never shifted.
- **Ops read from the row outwards** (`sequence.direction.op` lists them): `aggregate` (every func; `first` is the
  nearest event, `last` the furthest), `lag` — named `lead<k>`, the k-th next event —, `ewma` (weighted by the
  distance ahead), `sinceEvent` — named `until_<unit>`: events / days until the predicate first holds —,
  `countMatch`, `runLength` (the run starting with the next event), `regression` without `lag`, and **`barrier`**:
  `1` when the path first moves up by `up` (relative to the current row's own value of the field), `-1` when it first
  moves down by `down`, `0` when it touches neither within the window, null without a future value or a current
  value (`up` > 0 and / or `down` < 0, `sequence.barrier.levels`; future windows only, `sequence.barrier.direction`).
  `delta`, `trend`, `fracdiff` and a lagged `regression` read the window in one direction and are rejected, as is the
  general form (`lift` + `summarize`), which reads the past window only.
- **Engine.** The block runs in a keyed stage of its own (`future` in the plan report), replaying each key's rows
  latest first: one more GroupByKey, in the same wave as the past stages it does not depend on.
- **Overlapping labels and uniqueness weights.** Labels of neighbouring rows describe overlapping periods, so they
  are not independent samples. The number of the entity's rows whose `h`-window overlaps a row's is a past and a
  future `COUNT(1)` over `maxAge: h`; its inverse is a training weight, declared as `output.roles.weight` — a weight
  derived from labels is post-event too, and is emitted like a label (status `label`, never a feature):

  ```yaml
  - {name: before, scope: sequence, entity: seller, windows: [{maxAge: P20D}], ops: [{type: aggregate, funcs: [count]}]}
  - {name: after, scope: sequence, entity: seller, direction: future, windows: [{maxAge: P20D}], ops: [{type: aggregate, funcs: [count]}]}
  - {name: uniqueness, scope: row, expr: "1 / (1 + before_20d_count + after_20d_count)"}
  output:
    roles: {label: ret, weight: uniqueness}
  ```

  Rows sharing the row's timestamp are counted by neither window. The matching cross-validation leaves out the same
  neighbours: a time fold with `purge` = the horizon (above).

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
  `delta` / `trend` by their `k`, unfiltered `maxEvents` windows) keep only that tail (`ewma` and the path
  summaries are running states and keep none). Only the operators that re-read their window — `runLength` /
  `sinceEvent` / `countMatch`, the series readouts and `first` / `last` of `aggregate`, a lagged `regression`,
  `weightBy` — without `maxAge` or `maxEvents`, and any window with a `filter` but no `maxAge`, read the key's
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
  `factorization` / `discretize` / `quantileTransform` / `svd` (`spectralEmbedding`, `transitionStats`) are parsed
  but rejected. Factorization: `variant: bayesian`, `fit.cadence / window / warmStart`,
  and non-static fits. Discretize: non-static fits (`fit.cadence / window /
  warmStart` are accepted and ignored); quantileTransform and svd: `fold` (`static` and `forward` are implemented, `fit.cadence /
  warmStart` ignored). Discretize: `method: tree` / `optimal` (supervised). In `shrinkage`,
  `estimator: joint` needs `fit.mode: static` / `fold` / `forward` (rejected under `expanding`), a conjugate
  `family` needs `scale: identity`, a shrunk `distribution` needs a chain lattice and `backoff`, and
  `weights: heldOut` is rejected;
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
