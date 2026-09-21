# Config cheat sheet

Every key the `feature` transform accepts, with its values. The module documentation
(`module/transform/feature.md`) is the authoritative reference; this is the compact version to write
and review a spec quickly.

## Module step

```yaml
- name: features
  module: feature
  inputs: [records]            # exactly one input relation (join upstream)
  parameters: {...}            # below
  failFast: true               # a row that fails evaluation fails the pipeline (false → failure sink)
```

## `parameters`

| key | required | values / notes |
|---|---|---|
| `sources` | yes | URI / path / `data:` of a YAML or JSON sources document, or the object inline. Rendered with `${args.*}` |
| `lineage` | yes | list of `{fields: [...], from: <source>}` (`eventTime: <field>` per entry only when several event times are mixed). Every field a feature reads must appear |
| `time.field` | yes | the event-time field (timestamp / datetime / date); must equal the sources' `eventTime`. Rows re-timestamped from it; null → failure |
| `time.orderTieBreak` | recommended | fields ordering rows that share a timestamp |
| `predictAt` | yes | `event_time - PT10M`, `event_time`, `event_time + PT1H` |
| `entities` | for sequence | `{name, keys: [...], minInterval: <ISO-8601>}` |
| `contexts` | for context | `{name, keys: [...]}` |
| `baselines` | optional | `{name, expr, context, emit}`; `expr` may wrap a numeric expression in a context op (`share(1 / price)`); referenced by `residual.baseline`, encoding / factorization `offset` and the `softmax` op. `emit: <name>` also outputs the value as a column (nameable by the `baseline` role) |
| `features` | yes | list of blocks (below), or a URI / path of a document with a `features` list |
| `fit` | optional | `orderBy` (= time.field), `mode: expanding \| static \| fold \| forward`, `groupBy: <entity>`, `folds` (default 5), `fold: {by: row \| time, purge, embargo}` (time: every block is a fold, a row reads all blocks but its own, the purge on both sides and the embargo after the purge — purge defaults to the target label's horizon; more than half the blocks left out → run-time warning + counter `timeFold_<level>_excludedOverHalf`), `blocks: {bucket: year \| quarter \| month \| week \| day} \| {size: P90D}` + `minBlocks` \| `minHistory` (forward: minimum preceding blocks, as a count or a duration) + `window` (forward: the range of blocks a row reads, the default for keySets without `maxAge` and the range of a forward svd), `artifact: {uri, refit, id}` or the URI string |
| `engine` | optional | `parallelWaves` (default true), `rowId: [input fields]`, `spill: {memoryMB, directory, compress}`. Outside the plan hash — never changes values |
| `output` | optional | `prefix`, `nullPolicy: keep \| fillZero \| indicator`, `exclude: [globs / selectors]`, `groupBy: <context>`, `parentFields: [...]`, `childName` (default `rows`), `passThrough: all \| keys \| none`, `roles: {group, time, entity, label, baseline, weight}`, `include: [names] \| <uri>` (projection; replaces `exclude`), `manifest: <uri>` |
| `audit` | optional | `observedAt: count \| fail \| off` — rows observed after their declared availability are counted (default), routed to the failure output, or not audited |

`output.exclude` selectors: name globs (`block.*`), `derivedFrom:<kind>`, `evidence:declared`,
`scope:<scope>`, `block:<name>`. `output.include` accepts canonical or output names (a `<name>_isnull`
entry keeps its base column); a URI may point to a JSON array, `{columns | fields | passed | include: [...]}`
(objects with `name` allowed) or one name per line. Role columns (a baseline's `emit` copy, a label
derived as a column) stay emitted whether or not the list names them (`output.include.role`) or an
`exclude` pattern matches them (`output.exclude.role`); a column kept only as a role gets no `_isnull`
indicator. The output schema carries the lineage as `feature.*` field options — pass-through inputs as
`scope: input` with their `kind` / source / evidence, and `feature.role` on a role's field or column — and
a `screen` directly downstream reads both the selectors and the role defaults from it. `output.manifest` writes `manifest.json` at
assembly (`planHash`, `outputHash`, `roles`, `include`, `fields`, `columns` with lineage, `artifacts`,
`plan`) and, in batch, `manifest.run.json` at finalize (`rows`, `observedAtAudit.<field>` with `rows` /
`nullValue` / `missing` / `late` / `afterPredictAt` / `measured` / `leadSecondsDeciles`).

## Sources document

```yaml
version: 1
sources:
  - name: <source>
    description: "..."
    eventTime: <field>                     # required
    availability: atEventTime              # table default for fields without availableAt
    settlementLag: PT30M                   # after(event) = event_time + settlementLag (default PT0S)
    ingestionLag: P6D                      # upper bound, relative to availableAt (default PT0S)
    mutability: appendOnly | corrections
    snapshotOf: {source: <archive source>, at: "<time expression>"}   # corrections only
    lateness: PT2H
    keys: [...]
    fields:
      - name: <field>                      # required
        type: string | int32 | int64 | float32 | float64 | bool | timestamp | date | array<float64> | ... # required (array<elem>: the svd vector input)
        description: "..."
        kind: attribute | market | outcome | <free tag>
        availableAt: atEventTime | "event_time ± <duration>" | after(event) | atRowCreation
        ingestionLag: PT0S                 # per-field override
        observedAtField: <field>           # required for pre-event relative claims unless evidence: declared
        evidence: measured | declared
        allowDeclared: true                # with justification, per field
        justification: "..."
        validFor: PT15M
```

## Feature blocks — common keys

`name` (no `.`, no leading `_`, not an input field name), `scope`, `computeAt` (`event_time ± duration`,
≤ predictAt), `validFor`, `maxFeatures` (expansion guard), `combine: product | zip`.

## `scope: row`

| type | keys | output |
|---|---|---|
| (expr) | `expr: "<numeric expression>"` | `<name>` float64. Operands numeric / bool; no `$self` |
| `datetime` | `input`, `derive: [year, month, day, dayOfWeek, dayOfYear, weekOfYear, hour, minute]`, `cyclical: true` | `<name>_<derive>` int64, or `<name>_<derive>_sin` / `_cos` float64 |
| `bin` | `input`, `edges: [...]` | `<name>` int64 (bins numbered `0..` by edges below the value) |
| `cross` | `inputs: [a, b, ...]` (≥ 2) | `<name>` string (`a\|b`) |
| `indicator` | `input`, `values: [v1, v2]` | `<name>_<value>` int64 0/1 |
| `equals` | `inputs: [a, b]` | `<name>` int64 0/1, null if either is null |
| `residual` | `input`, `baseline: <baselines[].name>`, `on: identity \| logit \| log` | `<name>` float64 |
| `noise` | `distribution: normal \| uniform`, `seed` (required) | `<name>` float64 placebo: a pure function of `seed` and the row identity (`time.field` + `orderTieBreak`); pre-event |
| `vector` | `input` (an `array<float64>` field), `funcs: [length, sum, mean, std, min, max, argmin, argmax, first, last, slope, norm, polyfit]`; optional steps applied in this order: `slice: {from, to}` (negative = from the end, clamped), `diff: <order>`, `normalize: sum \| mean \| l2 \| zscore`; `position: index \| unit` (slope / polyfit), `degree: 1..5` (polyfit, default 2) | `<name>_<func>` (float64; `length` / `argmin` / `argmax` int64), `polyfit` → `<name>_poly0..poly<degree>`. Inherits the array field's availability. Null array or a null / NaN element → every readout null; an undefined readout (too few elements, zero denominator) → null; an empty vector has `length` 0 only. A `repeated` field without a value arrives as the empty array |

## `scope: context`

```yaml
- name: relative
  scope: context
  context: <contexts[].name>
  inputs: [f1, f2]              # sugar: every op gets fields: inputs
  ops: [rank, zscore]           # strings, or objects {type, fields: [...], values: [...], as}
  excludeSelf: false
```

| op | input | output |
|---|---|---|
| `rank` | numeric | int64 (1 = largest) |
| `zscore`, `gapToBest`, `shareOfTotal` (`share`), `percentile`, `median_diff` | numeric | float64 |
| `groupSize` | none | int64 |
| `softmax` | numeric `field` (score); `offset: <baselines[].name or column>` in probability space (`offsetScale: log` takes exp first), `temperature` (> 0, default 1) or `temperatureFrom: <uri>` (number or JSON `{temperature}` / `{T}`; outside the plan hash, in the manifest `externals`), `scoreNull: zero \| null` | float64: `w·exp(f/T)` normalised over the group; null offset → null row out of the denominator, offset 0 → 0; inherits the offset's `validFor`; `nullPolicy: indicator` adds `_isnull` and `_scoreNull` |
| `residualize` | numeric `field`, `against: <field>` or `[a, b]` (numeric fields / columns — not `on`, a YAML 1.1 boolean) | float64 `<name>_<field>_residualize`: the residual of the field regressed, with an intercept, on the `against` fields over the rows of the group (neutralised: uncorrelated with each regressor within the group, mean 0). Needs `p + 2` complete rows in the group, else null; a constant / redundant regressor is left out. Block-level `excludeSelf: true` = leave-one-out (fitted on the other rows) |
| `harville` | numeric `field` read as win probabilities (normalised over the group), `top: [2, 3]` (places 1..3, default `[2, 3]`), `discount: [λ2, λ3]` (exponents applied when the 2nd / 3rd place is drawn; default 1, below 1 flattens the later places), `maxGroupSize` (default 64) | float64 `<name>_<field or as>_harville_top<k>`: the probability of finishing within the first k places (Harville forward computation). Null / negative → null, 0 can only lose; cubic in the group size for `top: 3` — a larger group than `maxGroupSize` reads null |
| `shuffle` | any field, `seed` (required) | the field's type: values permuted within the group (seed + group key, rows ordered by identity then input values); availability of the field |
| `countByValue` | categorical | `map<string,int64>`; with `values: [...]` one int64 per value |
| `ratioByValue` | categorical | `map<string,float64>`; with `values: [...]` one float64 per value |
| `entropy` | categorical | float64 |

Column names: `<name>_<field>_<op>` (`<name>_<op>` for `groupSize`, `<name>_<field>_<op>_<value>` per
value, `as:` replaces the field segment). Under `output.groupBy`, `countByValue` / `ratioByValue` /
`entropy` / `groupSize` without `excludeSelf` land on the parent record; the same ops **with
`excludeSelf: true`** (and every per-row op such as `rank` / `zscore`) vary per row and stay in the child
array — the way to get "the composition of the others" as a per-row feature.

## `scope: sequence`

```yaml
- name: recent
  scope: sequence
  entity: <entities[].name>
  windows:                                   # or window: {...}; default = the whole past ("all")
    - {maxEvents: 5}
    - {maxAge: P365D, filter: "category = $self.category"}
  ops: [...]
```

Windows are strictly past (`t' < t`); the near edge is derived from `ingestionLag`, so `window` keys
other than `maxEvents` / `maxAge` / `filter` / `clock` / `as` are rejected. `clock: <calendar>` (declared in the sources'
`clocks:`) counts `maxAge` in ticks (`{maxAge: 20, clock: business}` → token `20business`); `decayBy: <calendar>`
and `fit.blocks: {size: <ticks>, clock: <calendar>}` count on it too; availability stays on wall time. Window token in names: `n5`, `365d`,
`365d_n5`, `all` — or the window's `as:`. A `filter` has no token: a filter-only window is `all`, so next to
the unconditional window it must be named (`windows: [{}, {filter: "category = $self.category", as: byCategory}]`
→ `<block>_all_…` and `<block>_byCategory_…`; unnamed = `column.duplicate`). One name is one window: two windows
of a block may share an `as:` only if they select the same rows (`window.as`).

| op | keys | output name / type |
|---|---|---|
| `lag` | `field(s)` or `expr`, `k` (default 1) | `<name>_<w>_<field>_lag1 .. lagk`, input type |
| `delta` | `field`, `k` | `..._delta<k>` float64 (lag k − lag k+1) |
| `trend` | `field`, `k` (default 5) | `..._trend<k>` float64 (regression slope) |
| `regression` | `field` (y), `against` (x — not `on`, a YAML 1.1 boolean), `funcs: [cov, corr, beta, intercept, r2]` (default `[beta, corr]`), optional `lag: k` (pairs `field` with `against` k events earlier: lead-lag) | `..._<field>_vs_<against>_[lag<k>_]<func>` float64; two contributing events at least; `corr` / `r2` null when a series is constant, `beta` / `intercept` when x is. Same-event form is incremental; with `lag` it scans the window per row (bound it with `maxAge` / `maxEvents`) |
| `fracdiff` | `field`, `d` (required, 0 < d ≤ 2), `k` (terms, default 20) | `..._fracdiff<d>` float64: `(1 − B)^d` truncated to `k` coefficients over the last `k` past events; null when fewer than `k` events or one of them is missing; `d: 1` = first difference |
| `ewma` | `field` / `expr`, `halflife: [h1, h2]` (> 0; events, or days under `decayBy: time`), `decayBy: events \| time` | `..._ewma<h>` float64; a running state (no history kept) — the order-0 `exponential` path summary below |
| `runLength` | `field`, `value` | `..._runlength` int64 |
| `aggregate` shape / series funcs | in `funcs`: `skew`, `kurt` (excess; population moments), `zeroCross` (sign changes, zeros ignored), `peaks` (strict local maxima), `acf<j>`, `pacf<j>`, `ar<p>_<i>` (Yule–Walker; j, p in 1..20, i in 1..p) | `..._<func>`: float64 (`zeroCross` / `peaks` int64). `skew` / `kurt` run incrementally; the others scan the window per row — bound it with `maxEvents` / `maxAge`. Null on too few values or a constant series; for a level other than 0 use `expr: "x - level"` with `zeroCross` |
| `sinceEvent` | `predicate`, `unit: [events, days]` | `<name>_<w>_since_events` int64 / `_since_days` float64 |
| `countMatch` | `predicate` | `<name>_<w>_countmatch` int64 |
| `rating` | `field` (the contest's numeric outcome), `context` (a `contexts[].name`: the rows of one group at one event time are a contest), `order: ascending` (default — a rank, smaller is better) or `descending` (a score), `method: plackettLuce` (default), `bradleyTerry` or `elo`, `funcs: [mu, sigma, count, delta]` (default `[mu, sigma]`; elo `[mu]`, no `sigma`), priors `mu` (25; elo 1500) / `sigma` (`mu / 3`) / `beta` (`sigma / 2`) / `tau` (`sigma / 100`, the drift per contest), elo `kFactor` (32) / `scale` (400) | `<name>_all_<field>_rating_<func>` (or `<name>_all_<as>_<func>`) float64, `count` int64. The block's `entity` is the rated player; every contest moves all its players at once (opponent-adjusted strength). Never rated = the prior with `count` 0. Runs as ONE replay under the global key (hint `sequence.rating.globalKey`); no `maxAge` / `maxEvents`, and the only filter is `"f = $self.f"` on a pre-event field, which splits the contests into independently replayed pools. Shifted by the outcome's availability; `minInterval` does not apply. Feed it to a context block (`zscore`, `gapToBest`) for the strength relative to the field |
| `aggregate` | `field` / `expr`, `funcs: [count, mean, avg, sum, std, min, max, first, last, rate]`; no field + `funcs: [count]` = COUNT(1). Optional `weightBy: "<numeric expr>"` — the past event's fields by name, the current row's as `$self.<field>` (a similarity kernel, e.g. `exp(-abs(start_price - $self.start_price) / 50)`): `count` = Σw (float64), `sum` = Σw·x, `mean` = Σw·x / Σw, `std` weighted; no `min / max / first / last`; null / NaN / ≤ 0 weights contribute nothing; `$self` fields must be known at `predictAt`; always scanned per row, so bound the window (`maxAge` / `maxEvents`); use `as:` when the block also has the plain aggregate of the field | `..._<func>`; count int64 (float64 under `weightBy`), mean / std / sum / rate float64, min / max / first / last input type |

**General form (path summaries)** — instead of `ops` (never both in one block):

```yaml
- name: price_path
  scope: sequence
  entity: <entities[].name>
  windows: [{maxAge: P365D}]
  lift: {fields: [start_price], exprs: [{expr: "final_price / start_price", as: ratio}], timeAugment: true}
  summarize:
    dynamics: {family: lti, measure: exponential, order: 2, halflife: [7, 30], decayBy: time}
```

| `measure` | parameters | columns per channel (float64) | component j |
|---|---|---|---|
| `exponential` | `halflife` list (required), `order` 0..16 (default 0) | `<name>_<w>_<channel>_exp<h>_<j>`, j = 0..order | weighted mean of x · Laguerre `L_j(ln2 · age / h)`, weights `2^(−age/h)`; j = 0 is `ewma` |
| `fourier` | `period` (required), `order` 1..16 (default 1), optional `halflife` | `..._fourier<P>[h<h>]_c0`, `_c<k>`, `_s<k>` | mean of x · cos / sin(2πk · age / P) (damped by a halflife) |
| `legendre` | `order` 0..8 (default 3) | `..._leg_<j>` | mean of x · P_j(2u − 1), u = position over the window's own span |

`decayBy` = the clock (`events` default: newest past event = age 0; `time`: days — to the current row for
fourier / legendre, to the newest past event for exponential, whose higher components would otherwise grow with
the gap; add `sinceEvent` `unit: [days]` for the gap). `lift.exprs` entries are strings or `{expr, as}`; name them
(`as`), since an unnamed one is `<name>__e{n}`, numbered across the whole spec. `timeAugment` adds the constant
channel `time` (components 1.. only), shifted like the latest of the block's channels. Every measure keeps no history
without a window; `legendre` re-reads its window under `maxAge`. At most 64 component columns per block
(windows × halflifes × channels × components). Channels must be numeric / bool.

`dynamics: {family: bilinear, type: logsignature, depth: 1..4 (default 2), decayBy}` summarises the joint path
through all channels (+ `timeAugment` = the event's clock position as the last channel): one column per Lyndon word
`<name>_<w>_logsig_<word>`, channels lettered a, b, c… in lift order (`a` = total increment of channel a, `ab` = the
Lévy area of a and b). Null with fewer than two complete points; bounded windows re-read their events.
`compress: {svd: {rank, center, standardize, outputs, fit}, keep: false}` fits an svd over the block's component
columns: scores `<name>_svd_<k>`, the components become intermediate unless `keep: true`.

`as:` on an op names the field segment (or replaces the op suffix for `sinceEvent` / `countMatch`).
Op `expr` and `predicate` see past rows only (`$self` only inside `window.filter`). Predicates and
filters use the Filter grammar (`module/common/filter.md`); expressions are numeric.

**Labels (`direction: future`)** — the block reads the strictly-future window `(t, t + maxAge]` (`maxAge`
required). Every column is a label (status `label`, emitted whatever the projection, no `_isnull`; the role `label` stays with `output.roles.label`); a
feature referencing one is `availability.violation`; a row expression over labels is a label only when
`output.roles.label` names it. Ops: `aggregate` (`first` = nearest, `last` = furthest), `lag` → `..._lead<k>`,
`ewma`, `sinceEvent` → `..._until_<unit>`, `countMatch`, `runLength`, `regression` (no `lag`), and
`barrier: {field, up: 0.1, down: -0.05}` → `..._barrier` int64: 1 / -1 when the path first moves up / down by
that fraction of the current row's value, 0 when neither, null without a future value.

## `scope: population`

### `type: encoding`

```yaml
- name: enc
  scope: population
  type: encoding
  keySets:
    - keys: [k1, k2]
      windows: [{maxAge: P365D}]                 # optional; ignored in static / fold, rounded to blocks in forward
      structure: flat | hierarchy | cross | sequence   # hierarchy needs parentRef (+ maxDepth); sequence: keys = a path, most recent first (lag columns), shrunk along its suffixes (k1,k2,k3) -> (k1,k2) -> (k1) -> global; null / unseen older steps read the longest known suffix
      parentRef: <field>
      hierarchy: [[coarser keys], additive, []]  # explicit lattice, fine → coarse
      shrinkage: {...}                           # per-keySet override
      as: <name>                                 # replaces the {keys} segment of the emitted names (default: the keys joined by _) — short names for lag-column paths, and the same keys twice in one block
  targets:
    - {stats: [count, share]}                    # no target
    - {field: <f>, stats: [mean, rate, std, distribution, quantile, q25, quantile90], as: <alias>}
    - {expr: "<numeric expr>", stats: [mean]}
  offset: <baselines[].name>                     # target minus baseline; on scale logit / log the composed value is the log-odds / log-rate ratio against the baseline (info encoding.offset.additive)
  combine: product | zip
  naming: "{block}__{keys}__{window}__{target}__{stat}"   # default; empty segments collapse
  shrinkage:
    estimator: backoff | sequential | joint      # derived from the lattice; joint = one simultaneous ridge / BLUP fit, fit.mode static | fold | forward only
    weights: fixed | varianceComponents          # heldOut not implemented
    priorWeight: 20
    family: gaussian | betaBinomial | gammaPoisson | dirichletMultinomial   # derived from the stat; conjugate families need scale identity
    scale: identity | logit | log                # required with additive
    leaveNodeOut: true
    output: [composed, deviations, effectiveN]   # extra columns dev0.., <stat>__neff
  smoothing: {type: bayesian, priorWeight: N}    # legacy sugar for fixed weights
  fit: {mode: expanding | static | fold | forward, groupBy: <entity>, folds: 5, fold: {by: time, purge: P20D, embargo: P7D}, blocks: {size: P90D}, minBlocks: 1 | minHistory: P180D, window: P2Y, artifact: {...}}
  maxFeatures: 200
```

Stats: `count` int64, `share` float64 (key count / global count), `mean` / `rate` float64
(shrinkable), `std` float64, `distribution` map (shrinkable along a chain lattice: Dirichlet-Multinomial
per-category pseudo-counts, `priorWeight` as λ, no deviations), `quantile` (median) / `quantile<NN>` / `q<NN>`
float64. `std`, `distribution` and quantiles are expanding-only. `family` changes no scalar value (the
conjugate posterior means and the moment λ coincide with the Gaussian ones); `estimator: joint` fits every
level at once (`dev<i>` = the level effects, `<stat>__neff` = leaf n + λ) and needs a lookup fit mode.

### `type: factorization` (always static)

`variant: fm | fwfm`, `fields: [categorical ≥ 2]`, `latentDim` (default 8), `task: {target | expr,
offset}`, `als: {epochs, reg, seed}`, `outputs: [{pair: [a, b], as}, {embedding: <field>, as, dims},
{sum: true, as}]`, `fit: {artifact}`. Unknown values → null.

### `type: discretize` (always static)

`input` (numeric), `method: quantile` (`tree` / `optimal` not implemented), `bins` (default 10),
`minSamplesPerBin`, `fit: {artifact}`. Output int64: `-1` missing, `0` below, `1..B`, `B+1` above.
Typically the key of a following encoding.

### `type: quantileTransform` (static, or forward per time block)

`input` (numeric), `bins` (default 100), `distribution: uniform | normal`, `clip` (normal only, default
`1e-6`: F(v) is clamped to `[clip, 1 − clip]` before Φ⁻¹), `fit: {artifact}` and, for the walk-forward fit,
`fit: {mode: forward, blocks, window, minBlocks | minHistory}` (exact: the knots of the values in the complete
preceding blocks; a block with no `fit.mode` of its own inherits a top-level `fit: {mode: forward}`; rows without
enough preceding blocks read null). Output float64 `<name>`: the
value's position in the fitted distribution (0..1, interpolated between the quantile knots; ties read the
middle of their range, also a tied run at the minimum or maximum such as a zero-inflated count's zeros; out
of range clamps to 0 / 1) or its normal score. Missing → null. With the default clip the fitted minimum /
maximum read ±4.75 whatever n; set `clip: 0.001` (±3.09) when the score feeds an `expr` or an `svd`, so the
extreme rows do not become outliers. `clip` changes the plan hash (the artifact directory).

### `type: svd` (static, or forward per time block)

`inputs: [numeric fields]` (the vector) or `input: <array field>` (an input field declared
`type: array<float64>` in the sources contract — no feature op produces an array; then `rank` is required), `rank`
(default min(d, 8)), `center` (default true), `standardize` (default false),
`fit: {artifact}` and, for the walk-forward fit, `fit: {mode: forward, blocks, window, minBlocks | minHistory}` —
a block with no `fit.mode` of its own inherits a top-level `fit: {mode: forward}`. Output
float64 `<name>_0 .. <name>_{rank−1}`: PCA scores ordered by explained variance. A vector with a missing
component → null scores. An array input must have one length (other lengths are skipped, read null and are
warned about at run time; `rank` above the array length is capped with a warning and the surplus columns read
null). Fitted from (n, Σx, Σxxᵀ): no row leaves the workers.
`outputs: [scores, residual, residualNorm]` (default `[scores]`): `residual` = what the kept components do not
explain, one float64 `<name>_resid_<input>` per input **in input units** (`x − mean − scale · Σ score · component`;
needs named `inputs`, not an array — `svd.outputs`); `residualNorm` = `<name>_residnorm`, its Euclidean length
(arrays too). Null wherever the scores are null; 0 when `rank` equals the vector length.

### `type: smooth` (static, or forward per time block)

`input` (numeric key), `target` (numeric / boolean field or column), `range: [lo, hi]` (required — the knots are
placed before the rows are read; keys beyond it are clamped, the curve is constant there; may be omitted when the
input is a uniform `quantileTransform` column: `[0, 1]`, i.e. knots at the quantiles), `segments` (default 10),
`degree` (0..5, default 3), `penalty: {order: 1 | 2 | 3, lambda: reml | <positive number>}` (default order 2 —
the curve shrinks towards a line — and REML), `outputs: [curve, residual]` (default `[curve]`), `fit` as for svd
(`static | forward`, inherited from a top-level forward fit). Output float64 `<name>` = the fitted conditional mean
of the target at the row's key (a feature: only the key is read from the row itself), and `<name>_resid` = target −
curve (reads the row's own target: an intermediate when another block consumes it, a label under
`output.roles.label`, otherwise `availability.violation`). A penalised B-spline regression solved from sufficient
statistics (it shares the svd blocks' Combine; no row leaves the workers); `segments + degree` ≤ 64. Under
`forward` the readable blocks are delayed by the target's settlement + ingestion lag and λ is re-chosen per window.
Missing key → null; a fit with ≤ `penalty.order` rows → null everywhere. Artifact `<block>.smooth.json` (λ, edf,
σ², coefficients).

### `type: transitionStats` (always expanding)

`sequenceOf: {entity, field}` (an `entities[].name` and a categorical field), `order` (previous values that make
the state, 1..4, default 1), `emit: [{toValueProb: <value>}, distribution]` (required), `blend: {perEntity: true,
priorWeight: 20}` (optional; absent or `perEntity: false` = transitions pooled over entities). Output float64
`<name>_to_<value>` = P(next value = value | the entity's previous value(s)), and the map `<name>_to` for
`distribution`. Sugar for a `lag` of the entity plus an expanding `encoding` with `stats: [distribution]` keyed on
the state and shrunk along `(entity, state) → (state) → shorter states → marginal` with `priorWeight` as the
pseudo-count — same values, same leak checks (`transitionStats.expansion` info shows the expansion). 0 = the value
has no mass, null = nothing known yet; a first event reads the marginal. Intermediate columns
`<name>_all_prev_lag<i>` hold the state.

### `type: spectralEmbedding` (static, or forward per time block)

`sequenceOf: {entity, field}`, `cooccur: {window: 2, weighting: ppmi}` (steps back that co-occur, 1..8), `rank`
(default 8), `of: current | previous` (which value of the row is embedded; `previous` for an outcome field),
`maxValues` (vocabulary cap by co-occurrence mass, 2..1024, default 256), `fit` as for svd. Output float64
`<name>_0 .. <name>_{rank−1}`: the value's coordinates from the PPMI matrix of the co-occurrence counts
(eigenvectors of largest |eigenvalue| × sqrt(|eigenvalue|), largest loading positive). No target is read. Null for
a missing / unseen / capped value — including one the cap keeps but whose every partner it dropped (no co-occurrence
row, so no position rather than the origin) — and for surplus columns when there are fewer values than `rank`. The
cap is applied before the counts are accumulated (an extra pass ranks the values by co-occurrence mass; under
`fit.mode: forward` over the whole input, the counts per block), so a high-cardinality field costs a pass, not the
worker's memory. Artifact `<block>.spectral.json`.

## Availability expressions

| expression | meaning |
|---|---|
| `atEventTime` | pre-event: known once the event exists |
| `event_time - PT10M` / `event_time + P1D` | relative to the event |
| `after(event)` | `event_time + settlementLag` |
| `atRowCreation` | the row's creation time (not statically checkable → rejected by the engine today) |
| `event_date T08:00` | absolute time of the event's day (`snapshotOf.at`) |

Durations are ISO-8601 (`PT30M`, `P6D`, `P1Y`); window tokens abbreviate them (`365d`).

## Column status in the plan

| status | meaning |
|---|---|
| `staticSafe` | provably available at `computeAt` |
| `windowShift` | history near edge moved back by the past inputs' settlement + ingestion lag (+ the predictAt offset) |
| `runtimeFilter` | not decidable statically (`atRowCreation`, `event_date THH:MM`) — rejected by the engine today |
| `violation` | needs post-event information: an error when emitted, an `_` intermediate when only consumed by a sequence / encoding |
| `label` | post-event by construction (`direction: future`) or by declaration (`output.roles.label`): emitted as a label, never a feature |

## Generated names (summary)

`output.prefix` + canonical name; `_` prefix for intermediates (not emitted). Row `<name>`; context
`<name>_<field>_<op>`; sequence `<name>_<window>_<field>_<op><param>`; encoding per `naming`;
`<column>_isnull` companions under `nullPolicy: indicator`; `<stat>__neff` / `dev<level>` with
shrinkage outputs; hidden lattice levels `<block>__<keys>__<window>__<target>__n` never appear in the
output.
