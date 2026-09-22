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
  **smooth**: the curve of a target over a numeric key (penalised B-splines, strength by REML) and the
  target's residual from it. **transitionStats** / **spectralEmbedding**: over the values an entity takes one
  after another — the shrunk distribution of the next value given the previous one(s), and coordinates of a
  categorical state from its co-occurrences (PPMI + spectral factorisation).
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
| entities   | optional | Array<Object\>                 | Subjects of sequence features: `{name, keys: [...], minInterval: <ISO8601>}`. `minInterval` is a declaration the plan relies on and the run audits (see [minInterval audit](#mininterval-audit-declaration-vs-data)). |
| contexts   | optional | Array<Object\>                 | Co-occurrence groups for context features: `{name, keys: [...]}`. |
| baselines  | optional | Array<Object\>                 | Named baselines: `{name, expr, context, emit}`. `expr` may wrap a numeric expression in a context op that reads one value per row of the group — `share` / `shareOfTotal`, `rank`, `zscore`, `gapToBest`, `percentile`, `median_diff`, `entropy`, `groupSize` — e.g. `share(1 / price)`, and the baseline then needs the `context` it is computed over. The ops that take parameters of their own (`softmax`, `residualize`, `harville`, `shuffle`) are declared as ops of a context block instead (`baselines.expr.op`). Referenced by `type: residual` (`baseline:`), encoding / factorization `offset:` and the `softmax` op. Baselines are intermediate columns; `emit: <name>` also writes the value as an output column (the same number the softmax offset reads), which a `baseline` role can name. |
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

**Calendar clocks (`clocks:`).** Next to `sources`, the document may declare calendars — business days, trading
sessions — on which windows, decay and fit blocks are counted instead of wall time:

```yaml
sources: [...]
clocks:
  - {name: business, type: calendar, dates: [2025-01-06, 2025-01-07, ...]}   # UTC dates, any order
  - {name: trading, type: calendar, uri: "gs://bucket/calendars/trading_days.csv"}   # one date per line / first CSV column, or a JSON array
```

- A row's **position** on a calendar is the ordinal of the last tick on or before its (UTC) date: a Sunday sits on
  the Friday before it. A date before the first tick sits before tick 0 and a date after the last tick on the last
  one, so a calendar must cover the data's range (plus the longest window).
- `window: {maxAge: 20, clock: business}` keeps the past rows whose position is at least the row's minus 20 (the
  row's own tick included, as `maxAge` on wall time includes `t − maxAge`); the window token is `20business`. Also on a
  keySet window. `maxEvents` / `filter` combine as usual; a `direction: future` window stays on wall time
  (`clock.direction`).
- `decayBy: business` (`ewma`, `summarize.dynamics`) measures ages in ticks: a halflife of 5 is five business days,
  whatever falls in between.
- `fit.blocks: {size: 20, clock: business}` makes blocks of 20 ticks (`fit.mode: forward`, time folds); a keySet
  window on the same clock is then counted in those ticks (`clock.fit` when the blocks are on another clock), and
  `fit.window` / `minHistory` / `purge` durations round with the calendar's mean tick spacing.
- **Availability stays on wall time**: `availableAt`, `ingestionLag` and the window shift they cause are durations —
  a clock measures windows, not knowledge.
- A `uri` is read at assembly (like `output.include`); the **dates** are part of the plan hash, so a new holiday is a
  new plan while a comment, a header or a reformatting of the file that leaves the ticks alone is not.
- A clock's `name` rides into generated column names (the window token `20business`), so it is an identifier
  (letters / digits / `_`, not starting with a digit) other than `time` and `events`; a duplicate name is rejected
  (`sources.clocks.name`) rather than silently overriding the earlier declaration. `clock.unknown` names an
  undeclared clock and lists each declared one with its tick count and coverage; `window.clock`, `fit.blocks.clock`
  and `sources.clocks.*` report malformed declarations.

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
      - {maxAge: P365D, filter: "category = $self.category"}   # as: <name> names the window segment (a filter has no token of its own)
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
`encoding.offset.computeAt`). The baseline is read from the **past rows** next to their outcome, never from
the current row (the value is the residual term alone), so its availability counts on the past side like the
target's: a baseline over pre-event market fields costs nothing, while a baseline over an outcome field (a
settled price) shifts the window near edge by that outcome's lag — `windowShift`, exactly as a target of
that kind does — and delays the blocks a `fit.mode: forward` fit may read (a `fit.mode: static` / `fold`
fit is unchanged: its statistics are an artifact of the fit boundary). A target-less level (the row counts
behind `count` / `share`) reads no baseline and takes no shift from it. Such a baseline is a valid offset
even though the current row cannot see its own value yet — so `baselines[].emit` of it stays an
`availability.violation`, unless the emitted copy is the evaluation baseline (`output.roles.baseline`),
which is post-event by declaration like a label: status `label`, never a feature, read by the evaluation
after the fact. A baseline's `context` is one event: its rows share the event time, so the value a past
row contributes is known at that row's own lag — a context whose rows spread over hours would read
outcomes settled later than the row's lag says. The offset is an additive term on the
shrinkage scale:

- `scale: identity` (default) — every statistic is taken over `target − baseline`: `mean` / `rate` are the
  key's mean residual (shrunk toward the parent's), `std` the residual spread. A past row whose baseline is
  missing (or NaN) has no residual: it is left out of every statistic of the target, the target's `count`
  included, in the expanding replay and in the static / fold / forward fits alike (the target-less row count
  and `share` count every row).
- `scale: logit` / `log` — each level's own term is the **score-type** estimate of the key's log-odds
  (log-rate) ratio against its baseline: `S / V` with `S = Σ(y − b)` over the level's rows and `V` their
  information at the baseline, `Σ b(1 − b)` on logit and `Σb` on log — one scoring step from the baseline,
  exact to first order, and finite for every key: a key with no success in n rows reads `−Σb / V`, which
  grows with n toward `−1 / (1 − b̄)` instead of diverging (the transformed mean `logit(ȳ) − logit(b̄)` is
  undefined there and a clamp would leak its constant into the value). The leaf shrinks that term toward
  the parent's term **by information**, `V / (V + λ′)`, so a key of rare events is trusted less than a key
  of the same row count at even odds; `λ′` is `priorWeight` rows of the average information of the lattice's
  coarsest (root) level (a declared `priorWeight` keeps its meaning of "rows of average information"), or under
  `weights: varianceComponents` `1 / τ²` with the between-key variance τ² estimated on the score scale. The
  **composed value is the term itself** — a residual on the scale, *not* a probability or rate — with
  `deviations` on the same scale and `effectiveN` in rows (info `encoding.offset.additive`). The levels
  keep a hidden `Σ baseline` (`<level>__sumoff`) and, on logit, `Σ b(1 − b)` (`<level>__suminfo`) next to
  `Σ(y − b)`, in the expanding replay and in the static / fold / forward artifacts alike; `std` and the
  quantiles stay statistics of the identity residual. A level whose rows carry no information (every
  baseline at 0 or 1 — e.g. a baseline that is 0 for every row of a cold-start key) has no term of its own
  and falls back to its parent, as an unseen level does. `estimator: joint` fits the same per-cell terms
  weighted by their information (a cell without information is skipped). A keySet with its own
  identity-scale or disabled `shrinkage` stays on the residual statistics above. The plan hash of a spec with an
  offset on logit / log names this estimator, so an artifact fitted by the earlier transformed-mean estimator is
  not addressed by it (the block is fitted again); an artifact pinned by `fit.artifact.id` that predates the
  `suminfo` statistic is refused with a refit advice.

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

### Ratings from contests (sequence `rating`)

```yaml
contexts:
  - {name: session, keys: [session_id]}
features:
  - name: skill
    scope: sequence
    entity: seller                     # the rated player
    ops:
      - {type: rating, field: final_price, context: session, order: descending}             # skill_all_final_price_rating_mu / _sigma
      - {type: rating, field: final_price, context: session, order: descending, method: elo, as: elo, funcs: [mu, count, delta]}  # skill_all_elo_mu ...
      - {type: rating, field: final_price, context: session, order: descending, tau: 2, tauPer: P30D, as: rested}                 # the uncertainty reopens with the time away
      - {type: rating, field: final_price, context: session, order: descending, method: bradleyTerry, pairs: mean, as: bt}       # large fields: a contest weighs like one game
  - name: field                        # the rating against the others of the same session: an ordinary context block
    scope: context
    context: session
    inputs: [skill_all_final_price_rating_mu]
    ops: [zscore, gapToBest]
```

A **rating** is the entity's strength as its past *contests* tell it: every contest moves all its players at
once, by how the result compares with what their ratings expected — so beating strong opponents counts for
more than beating weak ones, which no per-entity aggregate of the outcome can express.

- **`context`** names the contest (a `contexts[].name`): the rows of one group **sharing an event time** are
  one contest; **`field`** is its numeric outcome and **`order`** says which end is better — `ascending`
  (default: a rank or finishing position, smaller is better) or `descending` (a score, larger is better).
  Equal outcomes are ties. A row without a contest key, an entity key or a finite outcome takes no part, and a
  contest needs two distinct players. A player with several rows in one contest takes part once per row and
  receives the sum of their changes (its own rows are not compared with each other in `elo` / `bradleyTerry`).
- **`method`**: `plackettLuce` (default) and `bradleyTerry` are the closed-form Bayesian updates of Weng & Lin
  (2011) over a Gaussian strength `(mu, sigma)` — the ranking likelihood, and all pairs of the contest;
  `elo` is the pairwise logistic update with `kFactor` shared over the opponents (no uncertainty).
  Parameters: `mu` (prior, default 25; elo 1500), `sigma` (default `mu / 3`), `beta` (performance noise,
  default `sigma / 2`), `tau` (added to every participant's variance before a contest — strengths drift —
  default `sigma / 100`), `tauPer` (a duration: `tau` becomes the drift per that much time away, see *Drift in
  time*), `pairs` (`bradleyTerry` only: `all` (default) | `adjacent` | `mean`, see *Field size*); elo: `kFactor`
  (32), `scale` (400).
- **Drift in time (`tauPer`).** By default `tau²` is added once per contest the player takes part in, so ten
  months away and a contest a week ago leave the same uncertainty — where contests are irregular, the absence
  is the very thing that makes a strength uncertain. With `tauPer: P30D` the variance grows by `tau² · Δt /
  tauPer` over the time `Δt` since the player's **previous contest** (as the ratings know it: a contest whose
  outcome is not yet available has not happened), and nothing on a first contest — the prior is the whole
  uncertainty already. The `sigma` a row reads carries the drift **up to the row**, so a returning player reads
  wide before the contest that will narrow it again; `mu` does not move. `tau` must then be declared (the
  default is sized for one contest): choose it from how far a strength wanders — with the default prior
  (`sigma` 8.33), `tau: 2, tauPer: P30D` takes a settled player (`sigma` 3) to 7 after ten months away.
  Size `tauPer` well above the outcome's availability lag (`settlementLag` + `ingestionLag` + the `predictAt`
  offset — the window shift of the column): the newest contest the ratings may know is always that far back, so
  that lag is a floor under `Δt` that every row carries, and a `tauPer` near it inflates every `sigma` by a
  constant instead of telling absences apart. `bradleyTerry` / `plackettLuce` only; the period is wall time.
- **`funcs`** (default `[mu, sigma]`; elo `[mu]`): `mu`, `sigma`, `count` (contests rated so far) and `delta`
  (the rating's change in its last contest, null before the first). An entity never rated reads the prior
  (`count` 0), a row without the entity key reads null. Columns are `{block}_{window}_{field}_rating_{func}`,
  or `{block}_{window}_{as}_{func}` with `as` — needed when one field is rated by two methods.
- **Strictly past, and only what is known.** The contests sharing the row's time are never visible, and the
  window is shifted by the outcome's availability like any sequence column: a contest enters the ratings once
  its outcome is available at the row's `computeAt`. The entity's `minInterval` does not absorb that shift —
  the other players' contests fall inside it.
- **One replay per pool.** An update reads the ratings the earlier contests left, so the contests must be
  folded in time order by one replay: the columns run under the **global key** (hint
  `sequence.rating.globalKey`; a single worker thread, memory = one rating per player **plus** the pool's rows
  still inside the window shift — the replay only folds a contest once its outcome is available, so a long
  availability lag times the whole pool's row rate is what sizes the worker). When the contests fall
  into independent pools, split them with `windows: [{filter: "category = $self.category"}]` on a pre-event
  field — it becomes the partition key and each pool is replayed on its own. To keep the rating over everything
  next to the pooled one in the same block, name the pooled window: `windows: [{}, {filter: "category =
  $self.category", as: byCategory}]` (unnamed, both are `all`). Nothing else is a window here: an
  update cannot be taken back, so `maxAge` / `maxEvents` / any other filter are rejected
  (`sequence.rating.window`); `tau` is what ages an old rating. The result never depends on the row order: within
  a contest the changes are computed from the pre-contest ratings, and the contests held at one event time are
  applied in the order of their context key.
- **Field size decides what the uncertainty is worth.** The default parameters are the customary ones of these
  models, which come from games of a handful of players; in a large field both Bayesian updates still follow
  the paper to the letter, but behave differently. What one contest of `k` fresh players does to the winner's
  `mu` and to `sigma` (8.33 before) — `bradleyTerry` moves every player's `sigma` alike, `plackettLuce` by the
  place, so there the winner's and the last player's are given:

  | `k` | `bradleyTerry` winner / `sigma` | `plackettLuce` winner / `sigma` (winner – last) |
  |---|---|---|
  | 2 | +2.6 / 8.07 | +2.6 / 8.07 |
  | 4 | +7.9 / 7.50 | +2.8 / 8.26 – 8.08 |
  | 8 | +18.4 / 6.22 | +2.3 / 8.32 – 8.18 |
  | 16 | +39.5 / 1.89 | +1.7 / 8.33 – 8.25 |

  `bradleyTerry` adds up the `k − 1` pairs of every player, so in a large field one contest moves `mu` by
  several prior standard deviations and collapses `sigma`; with the small default `tau` it never reopens, and
  **the first contest decides the rating** for good. A larger `beta` softens both without curing either
  (`beta: 2 · sigma` at `k = 16`: `sigma` 7.8 and a move of +19.8, still 2.4 prior standard deviations).
  Above some eight players choose the pairing instead: **`pairs: mean`** divides the sums by the number of
  opponents — a contest weighs like one game whatever the field (at `k = 16`: winner +2.6, every `sigma` 8.07,
  the two-player values), the normalisation `elo` applies to `kFactor` — and **`pairs: adjacent`** is the
  paper's partial-pair update: a player meets its rank neighbours only (the opponents sharing its outcome and
  those at the nearest better and the nearest worse one), so fresh equals in the middle of the field do not
  move in their first contest (ends ±2.6 / 8.07, middle 0 / 7.79) and the ratings separate over the following
  ones. Both keep `sigma` a usable "how well do we know this player": it narrows by a few percent per contest
  instead of collapsing in one. `plackettLuce` and `elo` are the other way out of a large field — `elo`'s
  `kFactor` is shared over the opponents and does not grow with it either. `plackettLuce` has the opposite
  property: its normaliser `c² = Σ(σ² + β²)` grows with the field, so a contest barely shrinks `sigma` — at
  `k = 16` by 1.0% for the last player and 0.03% for the winner, and no `beta` takes the last player past 1.5%
  (the smaller the `beta` the more it shrinks: 8.22 as `beta` → 0). `mu` is a sound rating and its step decays
  only as slowly as `sigma` does, but **`sigma` is little more than a function of the contest count**: read
  `count` for "how well do we know this player", and treat `sigma` as a feature in small contests only.
- **Warm-up.** Every player starts from the prior, so over the first stretch of the input the ratings of a
  pool are close together and spread out only as contests accumulate — the distribution of `mu` (and of any
  gap between ratings) drifts until the pool has warmed up, which a model reads as a trend in time. Keep that
  stretch out of the training window, or read the rating relative to its contest with a **scale-free** context
  op over it (`zscore`, `rank`): a `gapToBest` is in `mu` units, so it drifts with the spread exactly like `mu`
  itself, and a contest whose players are all still at the prior has no spread at all (`zscore` reads null
  there). `count` tells how warm a player is; a pool split by a `$self` filter warms up per pool.
- **Teams (`with`).** A rating gives the whole result of a row to the one entity it rates, so an entity that
  always appears in company — an agent selling for sellers, a driver in a car — is rated for the company it
  keeps: its rating is mostly theirs. `with` rates the row as a **team** instead, the block's entity together
  with other entities of the same row, each with a rating of its own:

  ```yaml
  entities:
    - {name: seller, keys: [seller_id]}
    - {name: agent, keys: [agent_id]}
  features:
    - name: skill
      scope: sequence
      entity: seller                                  # the rated player, as before
      ops:
        - type: rating
          field: final_price
          context: session
          order: descending
          as: duo                                     # required with a team
          with: [{entity: agent, mu: 0, sigma: 4}]    # or just [agent]: the op's prior and drift
          funcs: [mu, sigma, count]                   # read for every member
          team: [mu, sigma]                           # the row's whole strength (optional)
  # skill_all_duo_mu / _sigma / _count              the seller — the names of a rating without a team
  # skill_all_duo_agent_mu / _sigma / _count        the agent
  # skill_all_duo_team_mu / _sigma                  seller + agent
  ```

  The team's strength is the sum of its members' (`mu = Σ mu_j`, `sigma² = Σ sigma_j²`, the noise `beta` once per
  team); the contest is rated between the teams exactly as between players, and a team's change is **shared among
  its members by their part of its variance** — the well-known member hardly moves, the uncertain one takes the
  update (a settled seller of `sigma` 2 next to a new agent of `sigma` 8.33 keeps 5% of it). Because an agent works
  with many sellers, what it adds beyond them separates out over the contests. Under `tauPer` every member drifts
  on its own clock, so the member that stayed away takes the larger part.
  - **A member's prior is a statement.** `mu`, `sigma` and `tau` of a member default to the op's. For a member
    that is an *effect on top of* the rated player, declare `mu: 0` and a `sigma` the size of that effect: `sigma`
    is what decides the shares (above: the seller takes `8.33² / (8.33² + 4²)` = 81% of every change while both
    are new).
  - **Read a member relative to its contest, or read the team.** Only the sum is identified — every seller up
    and every agent down by the same amount changes no expectation — so the members' levels can shift against
    each other over a long replay. `team: [mu, sigma]` is what the contests pin down ("this seller with this
    agent"); a member's `mu` is comparable among the members of its entity at one time: feed it to a context
    block (`zscore`, `gapToBest`).
  - A row without one of the members' keys joins no contest and reads null for that member and for the team;
    its other members still read. A member never rated reads its prior, so a known seller with a new agent
    reads a team. A member of several teams of one contest (one agent, two listings) receives the sum of its
    shares. The rows of one and the same team are not compared with each other.
  - `plackettLuce` / `bradleyTerry` only: `elo` keeps no variance to share by. The state, the stage key (global,
    or the `$self` pool) and the cost are those of the rating without a team, plus one rating per member.
- Diagnostics: `sequence.rating.with` (a member that is no `entities[].name`, the block's own entity or named
  twice, an entity called `team`, a member's unknown key or invalid prior, `elo`, no `as`, `team` without `with`
  or with an unknown readout; as an info it describes the team), `sequence.rating.context`, `sequence.rating.method`, `sequence.rating.order`,
  `sequence.rating.func` (unknown, or `sigma` under elo), `sequence.rating.parameter` (a parameter of the other
  method family, a non-positive `sigma` / `beta` / `kFactor` / `scale`, a negative `tau`, `pairs` outside
  `bradleyTerry` or unknown, a `tauPer` that is not positive or comes without `tau`),
  `sequence.rating.window`, `sequence.rating.as` (two rating ops of one block resolve to the same column
  segment with different parameters — they would share one running state; name them apart with `as`).

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
    minRows: 200                                  # smooth / svd / quantileTransform / spectralEmbedding: a fit over fewer rows is not solved
    align: procrustes                             # svd / spectralEmbedding: procrustes (default) | sign | none — see "Alignment of forward fits"
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

`minBlocks` / `minHistory` count blocks, not rows — and blocks are cut from the epoch, so the input's first block is
usually a fraction of one, and on a sparse key (a field that is mostly null) even a full block may hold a handful of
rows. **`minRows`** is the floor in rows for the lookup fits (`smooth`, `svd`, `quantileTransform`,
`spectralEmbedding`; a block's own `fit.minRows` wins over the top-level one): a fit — one window's under `forward`,
the whole input's under `static` — that fewer rows contributed to is not solved, and the rows that would read it
read null. It counts the rows that entered the fit (a key and a target for a curve, a complete vector for svd, a
non-null value for a quantile transform, a value with at least one previous value for an embedding). Default: a
`smooth` needs one row more than it has coefficients (`segments + degree + 1` — with fewer the penalty alone decides
the curve, and with exactly as many the unpenalised fit interpolates the rows, leaving no residual degree of freedom
to estimate the noise from); the other types have no floor. `minRows: 0` switches it off. The run log counts the change points it
emptied (`forward fit over … change point(s), n of them with fewer than fit.minRows …`). Encodings ignore it: a thin
level is shrunk towards its parent instead (`encoding.fit.minRows` warning on a block that declares one). Part of the
plan hash when declared. A fit the floor empties writes no artifact, so a later run with enough rows still fits.

**`align`** decides how the consecutive fits of an `svd` / `spectralEmbedding` block are brought into one coordinate
system (`procrustes` by default) — see *Alignment of forward fits* under *SVD / PCA*; the other fits have no such
freedom and ignore it (`<type>.fit.align` warning on a block that declares one).

A `type: svd`, `type: quantileTransform`, `type: smooth` or `type: spectralEmbedding` block inherits this `mode` unless it declares its own (see *SVD / PCA*,
*Quantile transform* and *Smooth curve*); the other population types (factorization / discretize) are always static and are
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
  out on each side); a calendar bucket counts its shortest length (28 days a month, 90 a quarter, 365 a year) and
  blocks on a calendar clock (`blocks: {size: <ticks>, clock: <name>}`) the clock's shortest tick spacing, so the
  range never falls short. Declaring an `embargo` only widens the range — it never replaces the purge.
- `purge` defaults to the horizon of the label the target reads — a `direction: future` column, directly or through a
  row expression (info `fit.fold.purge`); other targets default to no purge. `embargo` defaults to none.
- A row leaves out `2 × purge + embargo + 1` blocks. When that is more than half of the input's blocks the
  out-of-fold statistics read a minority of the data: the engine logs a warning and counts the rows in the counter
  `feature/timeFold_<level>_excludedOverHalf` (the input's block span is only known at run time) — use smaller blocks
  or a shorter purge / embargo.
- **`until: <instant | date>`** (`2025-06-30`, `2025-06-30T00:00:00Z`; UTC) ends the training period: the cross-fit
  runs within the blocks up to the block of `until` (a training row reads the other training blocks minus its purge /
  embargo range, never a later block), and a row of a later block reads **forward** — the blocks before its own whose
  targets were known at predictAt (the block that ends before `event + predictAt offset − the target's lag`, as
  `fit.mode: forward` reads; no window, no `minBlocks`). One batch thus yields the out-of-fold values of the training
  rows and the walk-forward values of the evaluation rows, without the evaluation rows reading later outcomes; λ
  under `weights: varianceComponents` is estimated on the training period. The artifact keeps the whole-input totals.
  Choose the blocks with the purge in mind: both round up to whole blocks, so a 7-day purge on month blocks leaves
  whole months out — make the blocks about as long as the purge (weekly blocks for a 7-day purge).
- `folds` and `groupBy` do not apply (every block is a fold); `purge` / `embargo` / `until` without `by: time` are
  ignored with a warning (`fit.fold.ignored`), `by` is `row | time` (`fit.fold.by`), a negative `purge` / `embargo`
  is an error (`fit.fold.negative`), a malformed `until` is `fit.fold.until`. `estimator: joint` solves hash folds only
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
ordered by explained variance (`<name>_0` carries the most) — both hold for a static fit and for a forward
fit with `fit.align: none`; under the default forward alignment the columns are a rotated basis of the same
subspace, so they are neither uncorrelated nor ordered (see *Alignment of forward fits*). The fit needs only (n, Σx, Σxxᵀ), accumulated
relative to the first vector so a large offset (epoch times, ids) does not cancel the covariance away — one
Combine over the rows, no row leaves the workers — and solves the d × d covariance on the driver for its `rank`
leading components (d = the vector length, tens to a few hundred; beyond 128 dimensions by the library
decomposition `spectralEmbedding` uses). Components of a static fit are oriented so the largest loading is positive (a
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

**Alignment of forward fits (`fit.align: procrustes | sign | none`, svd and spectralEmbedding).** An eigendecomposition
fixes its coordinates only up to what the matrix cannot see: the sign of every eigenvector, and any rotation inside a
group of (nearly) equal eigenvalues. A static fit is solved once, so a fixed rule (the largest loading positive) makes
it reproducible. A forward fit is solved again at every change point, and there that rule is *discontinuous*: the
eigenvectors move smoothly with the data while the loading that happens to be the largest changes hands, and close
eigenvalues swap order — so, left alone, a column flips and mixes from block to block although the fitted subspace
hardly moved, and a model that splits on it across time reads noise. Under `forward` every fit is therefore brought
into the coordinates of the fit before it:

| `fit.align` | what happens to the fit of a change point | use |
|---|---|---|
| `procrustes` (default) | the orthogonal map `R` minimising `‖A R − B‖` — `A` this fit, `B` the previous (already aligned) one, paired over what they share: the loadings' dimensions for svd, the values both fits embed for an embedding | columns that continue across blocks: what a downstream model needs |
| `sign` | every column is flipped to correlate positively with its own predecessor | when each column must stay *the k-th eigenvector*; close eigenvalues still mix and swap |
| `none` | the static rule per fit | reproducing the columns of an earlier version |

`procrustes` contains `sign` (for well-separated eigenvalues `R` is a diagonal of ±1) and also undoes what signs
cannot: a rotation inside a near-degenerate eigenspace and a swap of order are orthogonal maps too. What a fit
*explains* does not change — distances and inner products between coordinates, an svd's residual and total variance
are those of the unrotated fit — but a column is then **a stable coordinate of the fitted subspace, not its k-th
eigenvector**: an svd's `variances` are the data's variance along the rotated directions (unsorted), and an embedding's
`eigenvalues` are the spectrum of the fitted components rather than one value per column (the artifact says
`alignment: procrustes`). Two properties of an unaligned svd therefore go: the score columns are **no longer
uncorrelated** with one another (their covariance is `Rᵀ Λ R`, diagonal only when `R` is a permutation of signs), and
`<name>_0` no longer carries the most variance. Both matter only to a consumer that relies on them (an unregularised
linear fit on the scores, "the first component"); a tree model or a ridge reads the stable columns and prefers them.
`fit: {align: none}` keeps the unaligned pair. The chain runs **forward in time only** — a fit aligned to a later one would carry a trace
of rows it may not read — and skips the change points that have no fit (`minRows`, an empty window). The whole-input
model, which a static serving run loads in place of the forward fits a training run read, is aligned last, to the end
of the chain, so serving continues the columns the consumer's model was trained on. An artifact that already exists
is kept, as always: one this chain wrote in an earlier run is a point of the same chain (the fits of the earlier
blocks do not depend on what follows them), but one written **before `fit.align` existed** holds the old orientation
— the run warns about exactly that case, and `fit.artifact.refit: true`, once, rewrites it.

An alignment can anchor only as many columns as the two fits have in common. A component the previous fit did
not have (a vocabulary that was still smaller than `rank`) takes the direction left over, oriented by the static
rule — a fit growing. When two fits **share less than they have columns** — consecutive windows of an embedding
with a value or two in common, the usual cause being a `fit.window` of few, small blocks over a field with many rare
values — the shared part anchors what it can and the other columns are that fit's own leading components: as
determinate as a static fit, but they continue nothing. The run counts the change points where that happened and
warns (`… shared too little with the fit before them to anchor every column`); a longer `fit.window`, larger
blocks or a smaller `rank` give consecutive fits more in common. Part of the plan hash when declared; a static fit ignores it (`<type>.fit.align` warning), as do the fits
without such a freedom (curves, quantile knots, level statistics).

**Residuals (`outputs: [scores, residual, residualNorm]`).** The scores say where a vector sits on the leading
`rank` components; the residual is what those components do not explain — `x − mean − scale · Σ score_k ·
component_k`, per input and **in the units of the input** (the idiosyncratic part of each series once the common
factors are taken out). `residual` emits one column per input, `<name>_resid_<input>`, and needs named `inputs`
(an array has no named dimensions: `svd.outputs`); `residualNorm` emits `<name>_residnorm`, the Euclidean length
of the residual vector, and works for an array input too. With every component kept (`rank` = the vector length)
the residual is 0. The columns share the block's fit (static or forward) and read null wherever the scores do.

### Smooth curve over a numeric key (population, type: smooth)

```yaml
  - name: price_curve
    scope: population
    type: smooth
    input: start_price                 # the numeric key x
    target: sold                       # the numeric / boolean target y (an input field or a column)
    range: [0, 500]                    # [lo, hi] of the key — required (see "Knots"); keys beyond it are clamped to it
    segments: 10                       # equal intervals of the range (default 10)
    degree: 3                          # B-spline degree 0..5 (default 3 = cubic; 0 = smoothed steps)
    penalty: {order: 2, lambda: reml}  # difference penalty: order 1 | 2 | 3 (default 2), lambda reml (default) | a positive number
    outputs: [curve]                   # curve (default) -> price_curve | residual -> price_curve_resid
    fit: {mode: forward, blocks: {size: P90D}}   # static (default), forward, or inherited from a top-level forward fit
```

An encoding keyed on bins of a numeric field loses what happens inside a bin and at its edges; `smooth` fits the
conditional mean of the target as a **curve** of the key instead: `f(x) = Σ_j β_j B_j(x)` over `segments + degree`
uniform B-splines, with a penalty `λ‖Δ^order β‖²` on the differences of neighbouring coefficients (a P-spline). The
penalty, not the knot count, sets the smoothness — leave `segments` generous. `order` names what the curve
shrinks towards as `λ` grows: a constant (1), a straight line (2, the default), a parabola (3).

- **The strength `λ`.** `lambda: reml` (default) chooses it by restricted maximum likelihood through the mixed-model
  reading of the penalty — the smooth counterpart of `weights: varianceComponents` for key lattices: a noisy target
  is shrunk to the line, a clear non-linear signal keeps its shape. A declared number fixes it (`λ = σ²/τ²`, so the
  same number smooths a small sample more than a large one). The chosen value, the effective degrees of freedom
  (`edf`: `order` = fully shrunk, `segments + degree` = unpenalised) and the residual variance are in the artifact
  and the run log. When the data shows no curvature (or the key takes a few values only) the criterion flattens
  into a plateau towards `λ → ∞` and has no minimum to locate: a best strength REML cannot tell from an end of its
  search (16 decades around `tr(XᵀX) / tr(P)`) is reported **as that end**, marked `limit: polynomial` (or
  `unpenalised` at the other end) in the artifact and the log — the curve is the penalty's polynomial either way,
  and the strength is a fixed number rather than wherever on the plateau rounding puts a minimiser. When you
  reproduce a curve elsewhere, compare curves, not strengths: away from the limits REML locates `log10 λ` to about
  1e-4, which moves the curve by about 1e-6.
- **Execution.** A row contributes the vector `[B(x), y]` to the same (n, Σz, Σzzᵀ) accumulator an `svd` block uses,
  which holds `XᵀX`, `Xᵀy` and `yᵀy` — everything the solve *and* the REML criterion need. No row leaves the
  workers, the block shares the fit stage's one Combine with the svd blocks, and the small penalised system
  (tens of coefficients) is solved on one worker. The target is centred before the solve, so a large level costs
  no precision.
- **Knots.** They are laid over `range` before the single pass over the rows, which is why the range is declared
  and not read off the data; a key outside it is clamped (the curve is constant beyond its ends), in the fit and in
  the apply alike. For knots **at the data's quantiles**, feed a uniform `quantileTransform` column: its range is
  `[0, 1]` and `range` may then be omitted (`smooth.range` info) — the transform gets a fit stage of its own ahead
  of the curve's.
- **Missing values.** A row without a key or a target takes no part in the fit; a row without a key reads null. A
  fit with no more rows than `penalty.order` has no curve and reads null everywhere.
- **The curve is a feature, the residual is a target.** `price_curve` reads the *other* rows' targets through the
  fit and only the row's own key, so it is as available as the key (lineage `derivedFrom: outcome` for an outcome
  target). `outputs: [residual]` adds `<name>_resid` = target − curve, which reads the row's **own** target and is
  as available as that target: consumed by another block — typically `targets: [{field: price_curve_resid, ...}]`
  of an encoding, which then estimates an entity's effect *net of* the key's curve — it is kept as an intermediate
  (`availability.intermediate`); declared as `output.roles.label` it is a label; otherwise it is an
  `availability.violation`.
- **Fit modes.** Under `static` every training row's own target shapes the curve it reads (the static-fit caveat
  of any target-consuming fit: harmless for serving from an artifact, optimistic inside the training set). Under
  `forward` the moments are kept per time block and the curve is re-solved for every block window a row may read —
  the complete blocks within `fit.window` whose **targets are known at predictAt** (the target's settlement and
  ingestion lag delays the readable blocks, as for a forward encoding), the row's own block excluded; `minBlocks` /
  `minHistory` / `minRows` as for svd (a curve's default `minRows` is one more than its coefficients — see *Forward
  block fits*), and `λ` is re-chosen per window under `reml`. **Without `fit.window` the curve is fitted on the
  whole history**: once years of blocks have accumulated one more block hardly moves it, so it is close to a fixed
  non-linear transform of the key. To follow a relation that drifts, declare a rolling `fit.window` (`P730D`,
  say). `fold` / `expanding` are rejected
  (`smooth.fit.mode`). The artifact `<planHash>/<block>.smooth.json` holds the whole-input curve (range, segments,
  degree, penalty order, λ, edf, σ², n, coefficients) for a static serving run.
- Several keys are several blocks; chain them through the residual (`target: <previous>_resid`) for an additive
  fit by hand — each link is a fit stage of its own, and a row `expr` summing the curves (`by_price + by_quantity`)
  is the additive prediction, evaluated in the last of those stages. `method: isotonic` / `rff`, several inputs in
  one block (additive / tensor smooths) and category-varying curves are not implemented (`smooth.method`).
- **A curve per category, by hand.** A row without a key takes no part in the fit and reads null, so a key masked
  to one category fits that category's curve, and a row `expr` picks the row's own (both curves share a fit stage):

  ```yaml
  - {name: price_bulk,   scope: row, expr: "quantity > 1 ? start_price : null"}
  - {name: price_single, scope: row, expr: "quantity > 1 ? null : start_price"}
  - {name: curve_bulk,   scope: population, type: smooth, input: price_bulk,   target: sold, range: [0, 500]}
  - {name: curve_single, scope: population, type: smooth, input: price_single, target: sold, range: [0, 500]}
  - {name: price_curve_by_kind, scope: row, expr: "quantity > 1 ? curve_bulk : curve_single"}
  ```
- **The residual alone.** `outputs: [residual]` emits no curve column. It is the form to use when the *key* is
  known only after the event (a closing price): its curve would be an `availability.violation` as an output, while
  the residual — the target net of that key's curve — is consumed as an encoding target like any other.

### Sequences of values (population, types: transitionStats, spectralEmbedding)

Two population types read the **values an entity takes one after another** — a seller's grades, a machine's states,
a customer's plan changes: `sequenceOf: {entity, field}` names the entity (`entities[].name`) and a categorical field
— a string, a boolean or an integer code, which includes the INT64 column of a `type: bin` row block or of a
`discretize` block (a continuous numeric field must be binned first). Both are built on the entity's previous values, an ordinary `lag` that the block
expands under its own name as intermediate columns (`<name>_all_prev_lag<i>`, most recent first), so they need no
keyed pass of their own and schedule like the blocks they stand for.

```yaml
  - name: grade_next
    scope: population
    type: transitionStats
    sequenceOf: {entity: seller, field: condition_grade}
    order: 1                                   # previous values that make the state (1..4, default 1)
    emit: [{toValueProb: good}, distribution]  # grade_next_to_good (FLOAT64) and the map grade_next_to
                                               # also: ownValueProb | surprisal | entropy | expected (one FLOAT64 each)
    blend: {perEntity: true, priorWeight: 20}  # optional: the entity's own transitions, shrunk toward everyone's
```

**transitionStats** is "what comes next, given where the entity is": the distribution of the field's value
conditional on the previous `order` value(s). It is a desugaring, not an estimator of its own
(`transitionStats.expansion` info spells it out): an **expanding** `encoding` with `stats: [distribution]` keyed on the
state and shrunk along the chain `(entity, state) → (state) → shorter states → marginal` — the Dirichlet-Multinomial
case of *Shrinkage*, `p(level) = (counts + λ · p(parent)) / (n + λ)` with `λ = blend.priorWeight` (default 20) — so it
is strictly past, leak-checked and windowless like any expanding encoding, and a row reads exactly what the explicit
`lag` + `encoding` blocks would read. Without `blend` (or with `perEntity: false`) the transitions are pooled over
entities: `(state) → … → marginal`. `{toValueProb: v}` emits the probability of one next value (0 when it has no
mass, null when nothing is known yet) — `v` is written as the field holds it, a number for an integer code
(`{toValueProb: 0}` → `<name>_to_0`); `distribution` emits the whole map. Four **readouts** of the distribution
take one column each, `<name>_<readout>`: `ownValueProb` is the probability the state gave to *the row's own
value* — how usual this step was for the entity, without listing every value — and `surprisal` its `−ln`
(null when the value has no mass); `entropy` is `−Σ p ln p` of the map (how undecided the state is); `expected`
is `Σ v · p`, the probability-weighted mean of an integer code (an ordered band, a bin index: the field must be
numeric, `transitionStats.emit`). `ownValueProb` and `surprisal` read the row's own value, so they are as
available as the field: on an **outcome** field they are availability violations — usable as an intermediate
target or a label, not as a feature (`transitionStats.emit.own` hint) — while `entropy`, `expected` and
`toValueProb` read the distribution only. All are null when nothing is known yet. An entity's first event has no previous
value, so its state levels are empty and it reads the marginal; a state never seen before reads its parent. It is
always expanding, whatever the top-level `fit.mode` (a value distribution has no static form). When the field is an
outcome the usual window shift applies to the lag and to the counted transitions alike.

```yaml
  - name: grade_embed
    scope: population
    type: spectralEmbedding
    sequenceOf: {entity: seller, field: condition_grade}
    cooccur: {window: 2, weighting: ppmi}      # steps back that count as co-occurring (1..8, default 2); ppmi only
    rank: 8                                    # coordinates grade_embed_0 .. grade_embed_7 (default 8)
    of: current                                # embed the row's own value (default) | previous: the value it comes from | [current, previous]: both, from the one fit
    maxValues: 256                             # vocabulary cap, by co-occurrence mass (2..1024, default 256)
    fit: {artifact: {uri: "gs://bucket/features"}}   # static (default), forward, or inherited from a top-level forward fit
```

**spectralEmbedding** gives a categorical state numeric coordinates from the company it keeps: every (value, a value
at most `window` steps earlier in the same entity's sequence) is one co-occurrence, the counts become a positive
pointwise mutual information matrix `max(0, ln(C_ab · T / (r_a · r_b)))`, and its eigenvectors of largest
|eigenvalue|, scaled by `sqrt(|eigenvalue|)`, are the coordinates (the symmetric factorisation; oriented so the
largest loading is positive — a re-fit reproduces the columns; under `fit.mode: forward` every fit is rotated into
the coordinates of the one before it instead, see *Alignment of forward fits*). Values that follow and precede the same values land
close together, which lets a model generalise across a high-cardinality state without a target: no label is read, so
the columns are as available as the embedded value. `of: [current, previous]` reads the one fit twice — the row's
value as `<name>_<k>`, the value it comes from as `<name>_prev_<k>` — which is how to get both: two blocks that
differ only in `of` count the same pairs and solve the same eigenproblem twice (and, under `forward`, are aligned
as two separate chains). `of: previous` embeds the state the entity comes from — the form
to use when the field itself is an outcome (the row's own value would be an `availability.violation`). A value that
is missing, unseen in the fit or beyond `maxValues` reads null, as do the surplus columns when there are fewer
values than `rank`. A value the cap keeps but whose every co-occurrence partner it dropped reads null too: with no
co-occurrence row it has no position, rather than the origin of the fitted space. The fit state is the pair counts — a sum of row contributions, so one Combine (per time block
under `fit.mode: forward`, where a row reads the complete blocks before it and the usual `window` / `minBlocks` apply)
— and the eigenproblem is solved on one worker: up to 128 values by the same Jacobi sweep every earlier fit used,
beyond that by a tridiagonal decomposition (Householder, then QR) that takes 0.15 s for 700 values and 0.5 s for
1024 where the sweep took 7–15 s — so a forward fit's solve, once per change point, stops being the stage that sets
the run time. The matrix itself is dense in the distinct values, hence the cap. The cap is
applied before the pairs are counted (one extra pass over the fit input ranks the values by co-occurrence mass), so
the Combine state is bounded by `maxValues` rather than by the field's cardinality; under `fit.mode: forward` those
values are chosen over the whole input while the counts stay per block — when the field has more values than
`maxValues`, *which* values are embedded therefore depends on the whole input, the one thing in a forward embedding
that is not walk-forward (the `fit.mode.forward` info states it). Artifact
`<planHash>/<block>.spectral.json` (values with their coordinates, eigenvalues, pair count). A PPMI matrix is not
definite: some of the components of largest |eigenvalue| may have a **negative** eigenvalue, and the artifact's
`eigenvalues` (one per coordinate, in column order) carry the sign. Distances between coordinates are meaningful
for every component; a dot product reproduces the PPMI only over the components of positive eigenvalue.

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
      - keys: [grade_all_condition_grade_lag1, grade_all_condition_grade_lag2]
        structure: sequence                       # a path, most recent first: (lag1, lag2) → (lag1) → global
        as: gradePath                             # the {keys} segment of the emitted names (default: the keys joined by _)
    targets:
      - {field: sold, stats: [mean]}
    shrinkage:
      estimator: sequential                       # backoff (chains, default) | sequential (additive / cross, default) | joint (fit.mode static / fold / forward only)
      weights: varianceComponents                 # fixed: w = n / (n + priorWeight); varianceComponents: λ = σ²/τ² per level (batch method of moments)
      priorWeight: 20                             # fixed pseudo-count, and the fallback when a level has too few keys
      family: gaussian                            # gaussian | betaBinomial | gammaPoisson | dirichletMultinomial; default derived from the stat
      scale: logit                                # identity | logit | log; required when a lattice uses additive
      leaveNodeOut: true                          # subtract the leaf's own statistics from every ancestor (in a chain, the leaf = the deepest level that has rows)
      output: [composed, deviations, effectiveN]  # composed (default) | deviations (dev0, dev1, ...) | effectiveN (<stat>__neff)
```

`smoothing: {type: bayesian, priorWeight: N}` is accepted as the legacy spelling of fixed-weight
shrinkage toward the global mean. Every lattice level is evaluated as its own keyed stage over the same
window and target, and the composition is a per-row formula: `est(level) = est(parent) + w · (t(mean) −
est(parent))` from the global level down to the key, on the declared scale. `share` is
`n_key / n_global` over strictly-past rows. With `leaveNodeOut` the rows of the leaf are taken out of every
ancestor before it is shrunk toward them (an ancestor contains them, so it would otherwise pull the leaf toward
itself). In a chain lattice the leaf is the **deepest level that has rows**: a row whose declared leaf is
empty — a key never seen, or a null key component — backs off to a coarser level, and it is that level's rows
that leave the ancestors, so the row reads exactly what the lattice declared from that level reads. A lattice
with `additive` (`structure: cross`) keeps the declared cell instead: the main-effect chains take out the cell
they generalise, and an empty cell has nothing to take out.

**Paths (`structure: sequence`).** The keys are the steps of a path **declared most recent first** —
typically the `lag` columns of a categorical field (`- {type: lag, field: condition_grade, k: 2}` →
`..._lag1`, `..._lag2`), optionally led by a field of the current row — and the lattice is the chain of the
path's suffixes: `(k1, k2, k3) → (k1, k2) → (k1) → global`, i.e. the explicit `hierarchy: [[k1, k2], [k1],
[]]`. The statistic of a long path is shrunk toward what the shorter, better-observed path says, and a row
whose older steps are null (an entity with a short history) or whose path was never seen reads its **longest
known suffix** — the value a path declared with only those steps reads, leave-node-out included — so young
entities are not dropped the way a cross of lag columns drops them. At least two
keys (`encoding.keySet.sequence`); the info of the same code lists the derived levels, and the same code
warns when the block declares no `shrinkage` at all — the chain is only composed for a shrunk statistic, so
without it the column is the raw full-path value and an unseen path reads null. A chain composes top-down
(`backoff`) whatever `estimator` the block declares (`sequential` is the estimator of an `additive` / `cross`
lattice). Every level is a keyed stage of its own, so a path of `k` steps costs `k` shuffles; keys that
derive from an outcome (the lags of `sold`) need `fit.groupBy` under `fit.mode: fold`, like any such key.

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
- `as:` on a **window** names the window segment, which is otherwise derived from its bounds (`365d`, `n20`,
  `365d_n20`, `20trading`, `all`). A `filter` has no token of its own, so a filter-only window is `all` — the
  name of the unconditional window — and the two are a `column.duplicate` in one block until the filtered one
  is named: `windows: [{}, {filter: "category = $self.category", as: byCategory}]` gives `<block>_all_…` next
  to `<block>_byCategory_…` (the statistic over everything and the one per pool, whose gap is the usual
  feature — for a `rating` too). On a keySet's window the name is the `{window}` segment.
- `as:` on an encoding **keySet** replaces the `{keys}` segment (the keys joined by `_`):
  `- {keys: [grade_all_condition_grade_lag1, grade_all_condition_grade_lag2], structure: sequence, as: gradePath}`
  emits `<block>__gradePath__<target>__<stat>` instead of a name that repeats every lag column — and it lets
  one block declare the same keys twice (a raw statistic next to its shrunk lattice). Only the emitted names
  change; the hidden level statistics keep their key-derived names (the keySets of a block share them) — and so
  does the `estimator: joint` fit, so the same keys twice under `joint` read one solve: with the same lattice
  and shrinkage that is the point, and when they differ it is an error (`encoding.shrinkage.joint`; declare
  them in separate blocks).
- Both are names of letters, digits and `_` starting with a letter (`window.as`, `encoding.keySet.as`). They
  are part of the spec, so renaming changes the plan hash like any other rename of an output. One window name
  is one window: two windows of a block named the same must select the same rows (same `maxAge` / `maxEvents` /
  `clock` / `filter`), since the statistics behind the name are shared (`window.as`).
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

### Group solvers (context ops `residualize`, `harville`)

Two context ops fit a small model over the rows of the group and hand each row its part of the solution:

```yaml
- name: neutral                     # what is left of the score once the group's dependence on the market is taken out
  scope: context
  context: session
  ops:
    - {type: residualize, field: model_score, against: [log_market, quantity]}   # neutral_model_score_residualize
- name: placed                      # from win probabilities to "within the first k"
  scope: context
  context: session
  ops:
    - {type: harville, field: prob_pWin_softmax, as: p, top: [2, 3], discount: [0.81, 0.65]}   # placed_p_harville_top2 / _top3
```

- **`residualize`** regresses `field` on the `against` fields (one name or a list; input fields or columns),
  with an intercept, **over the rows of the group**, and returns each row's residual — the *neutralised*
  value: uncorrelated with every regressor within the group, mean 0. A row takes part when the field and every
  regressor are present (null otherwise); a group needs at least `p + 2` such rows (`p` regressors) or every
  row reads null — with fewer the fit passes through the points. A regressor that is constant in the group, or a
  combination of the others, is left out (the residual is the same). Against a single constant the residual is
  the deviation from the group mean. With the block's `excludeSelf: true` every row is fitted on the **other**
  rows (leave-one-out: a prediction error rather than an in-sample residual; one more row is needed) — read
  off the one fit of the group, so it costs no more than the in-sample residual. A row the fit runs exactly
  through (it alone decides its own fitted value) reads null.
  The key is `against`, not `on` (a YAML 1.1 boolean). `against` may name a column an **earlier op of the same
  block** produces (the ops of a block run in the order they are declared) or a column of another block; a name
  declared later in the same block is not yet a column and is reported as an unknown regressor.
- **`harville`** reads `field` as win probabilities (any non-negative strengths: they are normalised over the
  group, so implied probabilities that sum past 1 are fine) and returns, for each `k` in `top` (1..3, default
  `[2, 3]`), the probability of finishing **within the first k places** by the Harville forward computation —
  the winner is drawn by `p`, the next place among the rest in proportion to their strengths, and so on.
  `discount: [λ2, λ3]` raises the probabilities to `λ` when the 2nd / 3rd place is drawn (default 1 = plain
  Harville; values below 1 flatten the later places, which plain Harville gives too readily to the
  favourites). A null or negative value takes no part (null out), a 0 can only lose while a row with strength
  is still running; once the remaining rows are all 0 they share the place in equal parts, so every place is
  taken by exactly one row and in a group with at most `k` rows everyone is within the first k. Columns:
  `{block}_{field}_harville_top{k}` (`as` replaces the field segment). `excludeSelf` has no effect.
- **A residual does not remove its regressors from the model.** The residual is `field − a − b · against`
  within the group, so a model that is given the residual **and** `field` (or anything monotone in it within the
  group, such as its `zscore`) can rebuild `against`'s position in the group from the two. That is harmless when
  `against` is a feature anyway — and it defeats the purpose when `against` is kept out of the features **on
  purpose**: a market or baseline that enters the model as an offset / initial score (the setup the `softmax`
  op and `output.roles.baseline` serve) comes back in through the residual, and the gain it shows is the
  baseline's, not the score's. There, check the residual against a control that holds `against` itself: if
  the control does as well, the residual carries the baseline. Emit the residual **without** the raw field and
  the model has nothing to rebuild `against` from: `output.exclude` when the field is a column this transform
  computes, `output.passThrough` (`keys` / `none`) when it is an input field — `exclude` matches emitted
  columns only and never drops a pass-through input (it reports `output.exclude.unmatched` instead).
- **Choosing `discount`.** The stronger a row, the more plain Harville overstates its probability of finishing
  within the first three. `[0.81, 0.65]` (the example above) is a measured starting point for fields of eight
  to eighteen: on one such dataset it had the best log loss of the within-first-three probabilities among the
  variants tried, where plain Harville put the strongest rows ten points above their realised rate. The best
  exponents depend on the field size and on the domain: fit the two numbers outside the pipeline against
  realised placings when they matter.
- **Cost.** The group is solved in memory on one worker: `residualize` is linear in the group size (with or
  without `excludeSelf`), `harville` quadratic for the 2nd place and cubic for the 3rd — the places asked for
  in one `top` are one pass, not one per place. `maxGroupSize` (default 64) is read by `harville` only: a group
  with more valid rows reads null (info `context.op.groupSolver`; on `residualize` the key is ignored with a
  warning, since it reads every row of the group whatever its size). The result does not depend on the order
  the rows of a group arrive in.
- Both are row / context only (streaming-capable) and inherit the availability of every field they read.
  Under `nullPolicy: indicator` both get an `_isnull` companion column (a group can read null as a whole).
- Diagnostics: `context.residualize.against` (missing, non-numeric, repeated, or the field itself),
  `context.harville.top` (distinct integers in 1..3), `context.harville.discount` (at most two positive
  exponents), `context.op.maxGroupSize` (≥ 2; a warning on `residualize`).

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
  gap is a feature of its own (`sinceEvent` with `unit: [days]`); component 0 (`ewma`) is the same either way.
- **What an event is.** An event of a channel is a past row **with a value** for it. A row whose value is missing
  (null / NaN / ±Infinity) is no event of that channel: it adds no weight, it does not count on the `events`
  clock (ages count the channel's valued events), and on `time` it does not move the read position — the newest
  past event is the newest *valued* one, so a run of missing rows after the last value never enters a component
  (a cancelled entry with no result leaves the entity's components where its last result left them). Under
  `legendre` the same rule sets the span's origin: it is the oldest row of the window **with a value**, so leading
  missing rows do not stretch u. The channels
  of one block are folded from the same rows, so a channel with a value on a row moves while one without does not —
  the `timeAugment` channel is the constant 1, which every row has, so that one channel still counts every row of
  the window (its ages and its read position are its own, not the value channels').
  The `events` clock of the other summaries is counted the same way but over their own events: the log-signature's
  time channel is the ordinal among the **complete points** (every channel present), and `trend` orders the present
  values among its last `k` rows.
- **Reading the components.** Component 0 is the (decay-weighted) mean. The higher Laguerre components weigh
  recent and older events with opposite signs (`L_1 = 1 − u`): a trend of the value against its age. The Fourier
  components pick up periodicity at `period`, `period / 2`, …; the Legendre ones the shape of the path over the
  window (level, slope, curvature, …). The `time` channel's components describe *when* the events happened
  (its component 0 is always 1 and is not emitted). It reads no field, but it summarises the same window as the
  block's value channels: when a channel is an outcome whose window is shifted, the `time` channel takes the
  latest channel's shift too (`sequence.lift.align` when the channels differ). Inside that window it counts every
  row (see *What an event is*), where a value channel counts only the rows carrying its value.
- **Cost.** Every measure is a running state: `exponential` and `fourier` are exact under any spacing and evict
  under `maxAge` in O(1) per row; `legendre` rescales with the window's span, so it runs on a running state without
  `maxAge` and re-reads the window under one. None of them keeps the key's history without a window (no
  `sequence.window.unbounded` hint, `ewma` included) — except under a `filter` without `maxAge`, as for any op.
- Availability, windows (`maxEvents` / `maxAge` / `filter`), the window shift of an outcome channel and the
  naming prefix `{block}_{window}_{channel}` are those of the ops.
- **Log-signatures (`family: bilinear`).** Instead of one summary per channel, `bilinear` summarises the *joint*
  path the events trace through all the channels:

  ```yaml
  summarize:
    dynamics: {family: bilinear, type: logsignature, depth: 3, decayBy: time}   # depth 1..4 (default 2)
  ```

  The path is piecewise linear through the events' points (an event contributes when every channel is present;
  `timeAugment` adds the event's time — days on `time`, its ordinal among the complete points on `events`, ticks on
  a calendar — as the last channel; on `events` the time channel's own increment is the number of points minus one
  and the areas with it order the other channels' increments by point count, so use `decayBy: time` when the
  timing of the events matters). Its truncated log-signature is emitted in the Lyndon basis, one column per word:
  `{block}_{window}_logsig_{word}`, the channels lettered `a, b, c, …` in lift order (info
  `sequence.dynamics.logsignature` prints the legend). Words of one letter are the channels' total increments over the
  window, `ab` the Lévy area between channels a and b (signed: which moved first), longer words the higher-order
  interactions. Fewer than two points read null. The state is folded once per event on an unbounded window; a
  `maxAge` / `maxEvents` window re-reads its events (the oldest point cannot be removed from a signature without the
  one after it). One channel alone is only its increment (`sequence.dynamics.channels`), at most 26 channels.
- **Compress (`compress: {svd: {...}}`).** The component columns of the block (every window) feed an svd block
  `{block}_svd` — `rank`, `center`, `standardize`, `outputs` and its own `fit` (static by default, `mode: forward` to
  walk forward) as for `type: svd`, and no other key — whose scores `{block}_svd_<k>` are emitted instead of the
  components; `keep: true` emits the components too.
- **Diagnostics**: a block uses either `ops` or `lift` + `summarize` (`sequence.form`); `summarize` needs
  `dynamics` (`sequence.summarize`) with `family: lti | bilinear` (`sequence.dynamics.family`) — lti a `measure`
  (`sequence.dynamics.measure`), bilinear `type: logsignature` (`sequence.dynamics.type`) and a `depth`
  (`sequence.dynamics.depth`); `sequence.dynamics.order` / `.halflife` / `.period` / `.parameter` (a parameter of
  the other family included) check the parameters; channels must be numeric (`sequence.lift.type`); a block emitting
  more than 64 component columns is `sequence.dynamics.size`; a malformed `compress` is `sequence.compress`.
- `trend` (an op) is the same arithmetic as `regression`: the beta of the present values among the last `k` rows on
  their order (a missing row shortens the fit; it neither counts as a position nor extends the tail).

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
  role naming a baseline that is not emitted is reported (`output.roles.baseline.notEmitted`): give it
  `baselines[].emit` (a baseline over an outcome is then post-event by declaration — status `label`, never a
  feature — like a label or the training weight).
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

### minInterval audit (declaration vs. data)

`entities[].minInterval` says that two events of an entity are never closer than that. The compiler trusts it:
when the interval is at least the shift an outcome would otherwise impose on a window (its settlement +
ingestion lag past predictAt), the window reads without the shift and its columns are `staticSafe` — with the
declaration as the only guarantee that the outcome of the entity's previous event is known by the time the
next one is predicted. An event that follows its predecessor sooner than declared may read an outcome that
was not yet known: a leak the availability check cannot see, because it never sees the data.

So a declaration that absorbs a shift is reported, not assumed: the plan says which columns rest on it and
the largest shift it absorbed (`-- minInterval audit`, the `entity.minInterval` info, `plan.minIntervalAudit`),
the audit list gains a query over the input that counts the events contradicting it (BigQuery form: the gap
to the entity's previous event by `LAG … OVER (PARTITION BY <keys> ORDER BY <time>)`, counted where it is
positive and below the interval), and the keyed stage that replays the entity counts the same events at run
time as **`feature/minInterval_<entity>_below`** (a Beam counter, in the Dataflow UI with the other
`feature/*` counters). A gap of zero — rows sharing a timestamp, which never see each other — is not a
violation; where several rows share the later timestamp of a short gap the query counts one of them and the
counter counts each. One stage counts each entity, the one keyed by the entity itself where a column of it
rests on the declaration (a window reduced by a filter field is replayed under a finer key, and the counter
then sees the gaps of that sub-key). A declaration shorter than every shift absorbs nothing, changes
nothing, and is not audited.

Any count above zero means the declaration is wrong for this data: declare the interval the data has
(the query's `min_gap_seconds` says what it is) and let the affected windows take their shift, or accept that
those rows read a few outcomes early. The counted rows are not corrected — a row-level fallback to the shift
would make the columns' availability depend on the neighbourhood of each row, which the plan cannot express.

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
- Nested encoding targets, the `quantile` / `distribution` stats in
  `fit.mode: static` / `fold` (expanding only) are parsed but rejected. transitionStats is always expanding;
  spectralEmbedding: `fold`, and `cooccur.weighting` other than `ppmi`. Factorization: `variant: bayesian`, `fit.cadence / window / warmStart`,
  and non-static fits. Discretize: non-static fits (`fit.cadence / window /
  warmStart` are accepted and ignored); quantileTransform, svd and smooth: `fold` (`static` and `forward` are implemented, `fit.cadence /
  warmStart` ignored). Discretize: `method: tree` / `optimal` (supervised). Smooth: `method: isotonic` / `rff`,
  several inputs in one block. In `shrinkage`,
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
