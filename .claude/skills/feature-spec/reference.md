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
| `fit` | optional | `orderBy` (= time.field), `mode: expanding \| static \| fold \| forward`, `groupBy: <entity>`, `folds` (default 5), `blocks: {bucket: year \| quarter \| month \| week \| day} \| {size: P90D}` + `minBlocks` (forward), `artifact: {uri, refit, id}` or the URI string. `minHistory` accepted, ignored |
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
        type: string | int32 | int64 | float32 | float64 | bool | timestamp | date | ... # required
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
other than `maxEvents` / `maxAge` / `filter` are rejected. Window token in names: `n5`, `365d`,
`365d_n5`, `all`.

| op | keys | output name / type |
|---|---|---|
| `lag` | `field(s)` or `expr`, `k` (default 1) | `<name>_<w>_<field>_lag1 .. lagk`, input type |
| `delta` | `field`, `k` | `..._delta<k>` float64 (lag k − lag k+1) |
| `trend` | `field`, `k` (default 5) | `..._trend<k>` float64 (regression slope) |
| `ewma` | `field` / `expr`, `halflife: [h1, h2]`, `decayBy: events \| time` | `..._ewma<h>` float64 |
| `runLength` | `field`, `value` | `..._runlength` int64 |
| `sinceEvent` | `predicate`, `unit: [events, days]` | `<name>_<w>_since_events` int64 / `_since_days` float64 |
| `countMatch` | `predicate` | `<name>_<w>_countmatch` int64 |
| `aggregate` | `field` / `expr`, `funcs: [count, mean, avg, sum, std, min, max, first, last, rate]`; no field + `funcs: [count]` = COUNT(1) | `..._<func>`; count int64, mean / std / sum / rate float64, min / max / first / last input type |

`as:` on an op names the field segment (or replaces the op suffix for `sinceEvent` / `countMatch`).
Op `expr` and `predicate` see past rows only (`$self` only inside `window.filter`). Predicates and
filters use the Filter grammar (`module/common/filter.md`); expressions are numeric.

## `scope: population`

### `type: encoding`

```yaml
- name: enc
  scope: population
  type: encoding
  keySets:
    - keys: [k1, k2]
      windows: [{maxAge: P365D}]                 # optional; ignored in static / fold, rounded to blocks in forward
      structure: flat | hierarchy | cross        # hierarchy needs parentRef (+ maxDepth); sequence not implemented
      parentRef: <field>
      hierarchy: [[coarser keys], additive, []]  # explicit lattice, fine → coarse
      shrinkage: {...}                           # per-keySet override
  targets:
    - {stats: [count, share]}                    # no target
    - {field: <f>, stats: [mean, rate, std, distribution, quantile, q25, quantile90], as: <alias>}
    - {expr: "<numeric expr>", stats: [mean]}
  offset: <baselines[].name>                     # target minus baseline (identity scale only)
  combine: product | zip
  naming: "{block}__{keys}__{window}__{target}__{stat}"   # default; empty segments collapse
  shrinkage:
    estimator: backoff | sequential              # derived from the lattice; joint not implemented
    weights: fixed | varianceComponents          # heldOut not implemented
    priorWeight: 20
    scale: identity | logit | log                # required with additive
    leaveNodeOut: true
    output: [composed, deviations, effectiveN]   # extra columns dev0.., <stat>__neff
  smoothing: {type: bayesian, priorWeight: N}    # legacy sugar for fixed weights
  fit: {mode: expanding | static | fold | forward, groupBy: <entity>, folds: 5, blocks: {size: P90D}, minBlocks: 1, artifact: {...}}
  maxFeatures: 200
```

Stats: `count` int64, `share` float64 (key count / global count), `mean` / `rate` float64
(shrinkable), `std` float64, `distribution` map, `quantile` (median) / `quantile<NN>` / `q<NN>` float64.
`std`, `distribution` and quantiles are expanding-only.

### `type: factorization` (always static)

`variant: fm | fwfm`, `fields: [categorical ≥ 2]`, `latentDim` (default 8), `task: {target | expr,
offset}`, `als: {epochs, reg, seed}`, `outputs: [{pair: [a, b], as}, {embedding: <field>, as, dims},
{sum: true, as}]`, `fit: {artifact}`. Unknown values → null.

### `type: discretize` (always static)

`input` (numeric), `method: quantile` (`tree` / `optimal` not implemented), `bins` (default 10),
`minSamplesPerBin`, `fit: {artifact}`. Output int64: `-1` missing, `0` below, `1..B`, `B+1` above.
Typically the key of a following encoding.

### `type: quantileTransform` (always static)

`input` (numeric), `bins` (default 100), `distribution: uniform | normal`, `fit: {artifact}`. Output
float64 `<name>`: the value's position in the fitted distribution (0..1, interpolated between the quantile
knots; ties read the middle of their range, also a tied run at the minimum or maximum such as a zero-inflated
count's zeros; out of range clamps to 0 / 1) or its normal score. Missing → null.

### `type: svd` (always static)

`inputs: [numeric fields]` (the vector) or `input: <array<numeric> field>` (then `rank` is required), `rank`
(default min(d, 8)), `center` (default true), `standardize` (default false), `fit: {artifact}`. Output
float64 `<name>_0 .. <name>_{rank−1}`: PCA scores ordered by explained variance. A vector with a missing
component → null scores. An array input must have one length (other lengths are skipped, read null and are
warned about at run time; `rank` above the array length is capped with a warning and the surplus columns read
null). Fitted from (n, Σx, Σxxᵀ): no row leaves the workers.

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

## Generated names (summary)

`output.prefix` + canonical name; `_` prefix for intermediates (not emitted). Row `<name>`; context
`<name>_<field>_<op>`; sequence `<name>_<window>_<field>_<op><param>`; encoding per `naming`;
`<column>_isnull` companions under `nullPolicy: indicator`; `<stat>__neff` / `dev<level>` with
shrinkage outputs; hidden lattice levels `<block>__<keys>__<window>__<target>__n` never appear in the
output.
