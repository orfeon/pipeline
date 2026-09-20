# Feature Transform Engine (Design Document)

Status: **Implemented — describes the engine as it is (§9.2 lists the deferred items); the streaming keyed stages and merge of §9.4.6, the prefix-scan of §9.4.4 and the planned items of §9.6.8 are design notes, not code.**

How the DSL of [feature-dsl.md](feature-dsl.md) (below: "the spec") is implemented as `module:
feature` on Apache Beam: what is reused from the framework, what is new, where the spec and the
execution constraints collide, and the current implementation status. Section numbers are referenced
from the code ("engine doc §x.y" in javadoc and comments refers to this document). The developer
workflow (recipes for adding operators, test harnesses, the measurement loop) lives in the
`feature-engine` skill (`.claude/skills/feature-engine/`).

Measurements quoted below come from a reference production plan of ≈0.9 M input rows, 127 emitted
columns (183 with intermediates) and about twenty keyed stages, run on Dataflow.

---

## 1. Module layout and responsibilities

### 1.1 Class layout

```
module/transform/FeatureTransform.java   — @Transform.Module(name="feature"), a thin adapter
util/pipeline/feature/
  FeaturePlanService                     — shared entry: document resolution + compile + engine constraints
                                           (module, REST /api/feature, MCP validate-feature, agent validateFeature)
  SourceContract, FeatureSpec, AvailableAt, Durations, OperatorCatalog, Diagnostics
  FeaturePlanCompiler                    — spec + sources → FeaturePlan | Diagnostics (pure function)
  FeaturePlan, OutputColumn              — the compiled plan and its columns (Serializable)
  RowEvaluator, ContextEvaluator, SequenceEvaluator, PopulationEvaluator, FeatureValues
                                         — Beam-free evaluators, one instance per stage DoFn
  Summary                                — typed mergeable accumulators (moments / shape / extrema / counts / order /
                                           regression): the state of the incremental path and of the fit stage's
                                           Combine, declared per statistic in OperatorCatalog (§9.6.1)
  BlockSeries, ForwardBlocks             — a fit as a window over time blocks: one Summary state per block, merged per
                                           range, one model per change point (§9.6.2)
  VectorOps, SeriesStats                 — pure readouts over a double[] (a row's array field, a sequence window) (§9.6.4)
  Shrinkage, VarianceComponents, Discretization, QuantileTransform, Svd, Factorization, OrderStatistics, FitArtifact
                                         — pure models shared by both layers
  FeatureStages                          — FeaturePlan → Beam transforms (stages, waves, fits, finalize)
  KeyedSpillSorter                       — per-key external sort of the keyed stages (§9.3)
```

Rationale: as for `select` / `aggregation`, the logic lives under `util/pipeline/` and the module class
is thin (the "thin module, logic in util" split). The module class must sit in
`com.mercari.solution.module.transform` (package scanning).

### 1.2 Two layers: compile and execution

The core requirements of the spec (DAG topological sort, product expansion, the availability algebra,
`validate --expand`) are all **pure computations completed at pipeline-assembly time on the driver**
and do not depend on Beam. They are isolated in `FeaturePlanCompiler`:

```
FeaturePlanCompiler.compile(sourcesJson, parametersJson, inputFields)
  → success: FeaturePlan (stages + output schema + lineage metadata + diagnostics)
  → failure: Diagnostics (accumulated structured errors / warnings / hints)
```

- `FeatureTransform.expand()` calls compile; errors become `IllegalModuleException(errorMessages)`
  (the accumulating convention of `Parameters.validate`). **A leak-check violation surfaces as an
  assembly failure.**
- `validate --expand` (spec §7) calls the same compile and returns `FeaturePlan.describe()` (every
  expanded column with type, derived availability, static-safe / filter-required / violation status,
  stages and their evaluation order). The server (REST validation API / MCP tool), the agent's
  self-correction loop and the unit tests share one function. Being pure, it needs no execution
  infrastructure.
- The contract points of spec §8 (operator catalog, canonical hash, shared validator) live in this layer.

The execution layer reads nothing but the plan: every evaluator rebuilds its per-column plan from
`OutputColumn.coordinates` (a `Map<String, String>` written by the compiler), never from the spec.

---

## 2. Parameters and document loading

```yaml
transforms:
  - name: featureGen
    module: feature
    inputs: [records]
    parameters:
      sources: gs://bucket/feature/sources.yaml   # URI or inline object
      lineage: [...]
      time: {field: session_time, orderTieBreak: [session_id]}
      predictAt: "event_time - PT10M"
      entities: [...]
      contexts: [...]
      features: [...]                             # inline, or a URI for large specs
      fit: {...}
      engine: {...}                               # runtime knobs (§9.2), outside the plan hash
      output: {prefix: f_, nullPolicy: keep}
```

- `sources` / `features` accept **a string (URI) or a structure (inline)**. URI resolution reuses
  `Config.readContent(uri)` + `Config.convertConfigJson(text, Format.unknown)` (the `QueryTransform.loadQuery`
  pattern: `gs://`, Parameter Manager, `data:` and local paths work automatically; the Windows
  `Paths.get` guard is inside `readContent`).
- After loading, `TemplateUtil.executeStrictTemplate(raw, getTemplateArgs())` renders `${args.*}` inside
  the sources / features documents (as for query's SQL files).
- Fields that are "string or structure" are received as `JsonElement` and dispatched in the compile
  layer (the precedent is select's `filter`). `FeatureSpec` rides inside DoFns and therefore keeps
  nested blocks as JSON strings, not as Gson objects.

### 2.1 Internal model of the sources document

The availability language (spec §2.3) is four closed forms, parsed by regular expressions + ISO-8601
durations into the algebraic `AvailableAt`:

- `atEventTime()` is the **pre-event sentinel** (a lower bound "sufficiently before event time"), not
  `event_time + PT0S` — otherwise every pre-event attribute would violate `predictAt = event_time − δ`.
  `after(event)` × `settlementLag PT0S` is "exactly event time".
- `eventRelative(offset)` is a static offset; `dynamic(reason)` (`atRowCreation`, `event_date THH:MM`)
  cannot be compared statically and yields status `runtimeFilter`.
- The propagation rule (spec §6.1) is a symbolic max (`AvailableAt.max`); `plus(ingestionLag)` is
  relative to the availability and is not added to the pre-event sentinel.
- The derived availability and the origin source are stored in `Schema.Field.withOptions(...)` of the
  output schema (`feature.*` options) — the existing schema mechanism is the lineage's persistence; the
  `_` prefix lint (spec §6.4) is applied by the compiler when generating output names. Pass-through
  input fields get the same treatment from their source contract (`FeaturePlan.passThroughOptions`, the
  one source of the schema options and the manifest's `fields` entries: `feature.scope = input`,
  `feature.kind`, `feature.derivedFrom` = the kind plus the lineage the field already carried from an
  upstream feature transform, `feature.sources`, `feature.availableAt`, `feature.evidence`; the other
  `feature.*` options of an upstream column are dropped), and a role's field / column carries
  `feature.role` (`OutputColumn.role`, stamped once by the compiler), so a downstream lineage selector
  (`derivedFrom:market`, `scope:input`) and the screen's role defaults work without the manifest.

---

## 3. Execution plan: stages and DAG

### 3.1 Stage model

The feature DAG is topologically sorted and fused into **stages = bundles of operations evaluable under
one key**. In Beam a key change is a shuffle, so the stage count ≈ the shuffle count is the main cost
variable.

| stage kind | key | Beam implementation | scopes |
|---|---|---|---|
| `row` | none | stateless `ParDo` (`RowStageDoFn`) | row |
| `context` | context keys | `KeyDoFn` → `GroupByKey` → in-group evaluation (`ContextStageDoFn`) | context |
| `sequence` / `population` | entity keys / keySet keys | `SortKeyDoFn` → `GroupByKey` → per-key time-ordered replay (`KeyedHistoryDoFn` over `KeyedSpillSorter`) | sequence, expanding encoding |
| `fit` | (global) | `Combine` → artifact / side input → apply `ParDo` (`FitApplyDoFn`) | population static / fold (encoding levels, factorization, discretize, quantileTransform, svd) |
| `groupBy` | context keys | the finalize (`Finalize_Group` + `GroupedFinalizeDoFn`) | `output.groupBy` |

- Stages form a **linear chain** in the baseline design (each stage receives the row with every field
  so far, appends its columns and passes it on): "a later stage reads an earlier stage's column as an
  ordinary field", which makes DAG references trivial. Parallel *waves* over that chain are §9.4.
- Fusion rules (`FeaturePlanCompiler.StageScheduler`, S2): columns are walked in expansion order and
  **each column goes to the earliest stage of the same kind and key after the stages of its
  dependencies** (key-affinity scheduling; blocks of one key fuse into one stage even when a block of
  another key sits between them in the config).
  (a) Row columns are placed **as late as possible**: in their first consumer's stage (the stage before
  it when the consumer reads them before its DoFn), pulled earlier together with the row columns they
  read when an earlier consumer appears, and in the last stage when only the output reads them — so a
  row value never rides a shuffle that does not need it.
  (b) Sequence and population (expanding encoding) columns are evaluated by the same
  `KeyedHistoryDoFn`, so **one key = one stage** (reported as `population` when it holds any
  population column).
  (c) Dependencies read *inside* the DoFn (the row, the history) may share the stage; dependencies read
  *before* the DoFn — stage keys, the fit's Combine, the fields a variance-components estimate reads
  (level keys / target / offset) — require a **strictly earlier** stage. Inside a stage the column order
  is the expansion order (dependencies first).
  (d) The hidden levels of a static / fold fit block and the row columns reading them (compose / share /
  deviations) share **one fit stage per block** (the artifact and the λ side input exist only there).
  (e) The history of a fused stage is **trimmed per field** (S6): each column's retention watermark
  (`SequenceEvaluator.retainInto`) is folded into a per-field minimum (`Watermarks`), `History.trim`
  removes fields from old entries independently and drops an entry once every field is gone. A
  scan-path column without `maxAge` (the `sequence.window.unbounded` hint) keeps only its own past
  inputs for the whole history; the other columns' fields leave with their own windows. Entries whose
  fields are all gone keep a ~40-byte skeleton, so the retained *row count* of a key is the longest
  window among the fused columns (all rows with an unbounded column).
- `getShuffleCount` = one per keyed stage. The stage DAG (`Stage.dependsOn`, `getWaves`,
  `getDagShuffleEstimate`) is described in §9.4.
- Schema propagation between stages is append-only (`Schema.builder(inputSchema).withField(...)`, the
  `OnnxTransform.mergeSchema` precedent); the last stage removes `output.exclude` and `_` columns.

### 3.2 Key constraints

Group keys are String concatenations (`FeatureValues.key`, the `SchemaUtil.createGroupKeysFunction`
convention). Therefore:

- Rows with a null key component **do not contribute to the stage's state and get null for its
  features** (the same rule as query's buffer source): `KeyedHistoryDoFn` evaluates them under
  `NULL_KEY` without sorting.
- Separator collisions in key values are a known limitation shared with the rest of the framework.
- Row ids of the wave merge (§9.4) use `FeatureValues.keyWithNullTokens` instead (null components map
  to a deterministic token; a declared `engine.rowId` must never be null or random).

---

## 4. Per-scope implementation

### 4.1 row — reuse and constraints

- `expr` delegates to the framework's Lucene expression engine (`ExpressionUtil`). **Constraint:
  variables and results are doubles only.** The spec's general expressions (`nullif` and string
  operations) are not available; v0 is "expr = numeric expression, typed operations are `type:` ops".
  Compiled expressions are not Serializable → transient + `@Setup` rebuild (framework convention).
- `datetime` (with cyclical sin / cos), `bin` (manual edges), `cross`, `indicator`, `equals`,
  `residual` are implemented in `RowEvaluator`; the lattice readers (`share`, `fitStat`, `compose`,
  `deviation`, `effectiveN`) are row operators too, as is `mapValue` — one listed category's share read
  from a `distribution` map column that `targets[].values` turned into an intermediate (the flat
  per-category FLOAT64 columns a sink or a model consumes, the `countByValue` / `values` rule).
- `vector` reads scalar columns out of a numeric array field (`VectorOps`, pure functions over `double[]`):
  the steps slice → diff → normalize, then one column per readout (`polyfit`: one per coefficient). Vector
  outputs are always expanded into scalar columns — the svd scores' convention — because Avro round-trips
  `array<double>` at float precision on this classpath and the consumers are tabular models. The readout
  catalog is `OperatorCatalog.VECTOR_FUNCS`; a vector with a missing component has no readout (the rule
  `SvdSpec.vector` applies to the same kind of field, now the shared `VectorOps.toVector`).
- The spec's "expression AST shared with an optimisation transform" has no counterpart here; the
  shared component is `ExpressionUtil` (+ `Filter` for predicates and window filters, which are parsed and
  reserved-word-quoted at compile time by `conditionText`).

### 4.2 context — new implementation

- The framework's navigation select DoFn was an empty stub; rank / zscore / gapToBest / shareOfTotal /
  percentile / median_diff / countByValue / ratioByValue / entropy / groupSize are implemented in
  `ContextEvaluator`: `GroupByKey` → the group in memory → evaluate → emit **per row**.
- Co-occurrence groups (a session's listings) are assumed small, so GBK + in-memory evaluation suffices.
  A `Combine` (aggregate statistics then re-join) does not fit per-row outputs like rank / percentile.
- `excludeSelf` removes the row inside the evaluation (no extra shuffle); group-constant ops are
  evaluated once per group; `values:` emits one numeric column per listed value instead of a map column
  (map columns are awkward for warehouse sinks).
- In streaming the group is closed by the module's window / trigger; v0 is batch-first and the streaming
  behaviour (GBK inside `getStrategy()`'s window) is documented as best effort.

### 4.3 sequence — per-key time-ordered replay

The original plan was to reuse the framework's stateful select pattern (`OrderedListState` +
`@RequiresTimeSortedInput`). The implemented design is **GroupByKey + in-DoFn time-ordered replay**
instead, for a reason found during implementation: a context stage's GroupByKey fires at the end of
the global window and re-emits rows at their original event time, which a downstream
`@RequiresTimeSortedInput` DoFn **silently drops as late data**. For a batch-first v0, collecting the
rows of a key and evaluating them in time order is simpler and expresses the same-timestamp exclusion
naturally. A stateful variant is the streaming follow-up (§6, §9.4.6).

- Rows enter the keyed stage as `(key, (event millis, row))`; `KeyedSpillSorter` (§9.3) sorts each key
  in memory up to a budget and on worker-local disk beyond, and the DoFn streams the sorted rows.
- **Strictly past (`t' < t`)**: rows sharing a timestamp are held in a `pending` list and join the
  history only once the timestamp advances, so they are never visible to each other; `orderTieBreak`
  therefore only needs to be declared, not enforced. The DoFn re-emits at the original timestamp
  (`getAllowedTimestampSkew` = max).
- **Strictly future (`direction: future`, label columns)**: the same replay run backwards. The compiler gives the
  block's columns the stage kind `future` (a slot of its own: never fused with a past stage of the same key, since the
  sort differs); `SortKeyDoFn` keys the rows by `~millis` (the bitwise complement: descending, no overflow) and
  `KeyedHistoryDoFn` hands the evaluators the mirrored clock `−t` for the row and for every history entry. On that
  clock the strictly-past machinery reads `(t, t + maxAge]` unchanged — the `maxAge` far edge, `decayBy: time`
  distances, the `pending` exclusion of same-timestamp rows, eviction and trimming — and the output keeps the real
  event time. What reads the window in one direction is decided up front: `aggregate first / last` swap in
  `SequenceEvaluator.func` (the replay's newest event is the nearest one; the coordinates keep the declared func), `lag` is named `lead`, `sinceEvent` `until`, and the
  ops that would need the real order (`delta`, `trend`, `fracdiff`, lagged `regression`) are rejected. `barrier`
  (the first-touch label) scans the window from its newest (nearest) end against the current row's value. The
  columns carry `Status.label` and no role — the role `label` stays with the declared `output.roles.label` (`classifyFuture`: `availableAt` = the horizon plus the read
  fields' own availability, no window shift); `finalizeColumns` exempts labels — and a column declared as
  `output.roles.label` — from the violation check and from `_isnull` indicators, so the only way a label reaches a
  feature is through `availableAt`, where it is an ordinary `availability.violation`.
- **Availability filter (spec §6.2 tier 3)** is not implemented: `runtimeFilter` columns are rejected
  by `engineConstraints`. Tiers 1 (near-edge shift, `windowShift` / `shiftMillis`) and 2 (`minInterval`)
  are implemented in the compiler and evaluator.
- Operators: `lag` / `delta` / `trend` (`slope`) / `ewma` (halflife list, `decayBy: events | time`) /
  `runLength` / `sinceEvent` / `countMatch` / `aggregate` (count / mean / min / max / sum / std / rate /
  first / last), and the general form's `dynamics` columns (below). `window.maxEvents` / `maxAge` select the window by binary search over the history;
  `filter` is evaluated per row unless it is a same-field pre-event `$self` equality, which the compiler
  reduces to an **additional partition key** (`stageKeys`; hot entities split across workers, rows with a
  null filter value bypass the stage) — outcome-like fields stay filters because keying on them would leak.
- **Incremental evaluation** (O(n) per key): `aggregate` statistics and encoding statistics keep one
  `Summary` state per column (per `$self` value under an equality filter) advanced by monotone fold /
  evict pointers (`advance` / `contribution` / `readStatistic`). A `Summary` is a typed, mergeable
  accumulator — `create` / `update(±1)` / `merge` / `read` — and the family a statistic runs on is
  declared once in `OperatorCatalog.summary` (moments for count / sum / mean / std, extrema for max /
  min, value counts for distribution, exact order statistics for quantiles, the cross moments of a pair for
  the `regression` op's cov / corr / beta / intercept / r2). Its algebra decides the
  path: every family is a monoid (summaries of disjoint row sets merge), an *invertible* family is a
  group (a window can evict), so max / min run incrementally over an unbounded past but take the scan
  path under `maxAge`. Operators without a family (lag / trend / predicates) and windows with
  `maxEvents` or a general filter take the scan path over a sublist view. So does an aggregate under
  `weightBy`: its weight reads the current row (`$self`), a different number for every (row, event) pair,
  so nothing folded once per event can serve it — *self-dependent* statistics have no family by
  construction, which `OperatorCatalog.summary(stat, weighted)` declares (`weightedAggregate`: Σw, Σw·x and
  a second pass for the deviation; the compiled expression and its `$self` / event variable split are
  resolved once in `setup()`). A weighted aggregate is bounded by `maxAge` or `maxEvents` like any scan
  column and unbounded without them. Both paths read a past value through one rule
  (`SequenceEvaluator.finite`): null, non-numeric, NaN and ±∞ are missing and contribute to no statistic,
  `count` included — a non-finite value folded into a running sum would stay there (NaN for good; ∞ − ∞ = NaN
  once evicted) while the scan recovers when it leaves the window. `SequenceIncrementalTest`
  checks the two paths agree on random histories; `SummaryTest` checks the monoid / group laws.
- **Scalar summaries of the window** (`aggregate` funcs). `skew` / `kurt` are sums of per-event contributions —
  `Summary.Shape`, (n, Σx..Σx⁴) about an anchor, invertible — so they take the incremental path like the moments;
  the family is separate from `Moments` so the state every encoding level carries stays three numbers.
  `zeroCross` / `peaks` / `acf<j>` / `pacf<j>` / `ar<p>_<i>` (`SeriesStats`, tokens parsed once into the column
  plan) read *neighbouring* values: as summaries they would be ordered monoids carrying boundary values, whose
  eviction needs the successors of the evicted element — something `update(state, contribution, −1)` cannot
  express — so they have no family and scan the window (they would scan under every rolling window anyway; an
  ordered family only pays off for the streaming state).
- **Retention**: a column's history watermark is its evict pointer (incremental), the `maxAge` far edge
  (scan), or the near edge minus a bounded tail (`lag` / `trend` / `fracdiff` = k, `delta` = k + 1, unfiltered
  `maxEvents`); `runLength` / `sinceEvent` / `countMatch`, the other scan-only readouts and filtered windows without
  `maxAge` are unbounded and reported by the `sequence.window.unbounded` hint (§3.1 (e)).
- **Ratings** (`rating`, `Rating`): the block's entity is the rated player, `context` the contest (the rows of one
  group at one event time), `field` its outcome; `elo` and the Weng–Lin closed-form updates (`bradleyTerry`,
  `plackettLuce`) over (mu, sigma) with a per-contest drift `tau`. An update reads the ratings the earlier contests
  left, so the state is **not a `Summary`**: no merge (nothing to combine per block, no prefix-scan form) and no
  inverse (no window — the compiler rejects `maxAge` / `maxEvents` / general filters, `sequence.rating.window`). It
  is a running state all the same: the fold pointer advances one event time at a time and hands the run of rows
  sharing it — they joined the history together — to `Rating.fold`, which splits it into contests by the context
  keys; the readout columns of one op share the state (`stateKey`), and the watermark is the fold pointer, so the
  history holds only the contests the window shift still keeps back. The columns are *pooled*: their stage key is
  not the entity but the reduced filter field alone (`stageKeys`, empty = the global key — hint
  `sequence.rating.globalKey` instead of `encoding.globalKey`, whose advice does not apply), and the entity's
  `minInterval` never absorbs the shift (other players' contests fall inside it). Within a contest every change is
  computed from the pre-contest ratings over the entries sorted by player, so the row order inside a timestamp —
  which the sorter does not fix — cannot reach the output. The scan path (`Rating.replay` over the visible window)
  is the reference `RatingTest` compares the running state with, bit for bit, over trimmed and untrimmed histories.
- **Two series** (`regression`): the contribution of an event is the pair (x, y) = (`against`, `field`), folded
  into `Summary.Regression` — (n, Σx, Σy, Σx², Σy², Σxy) taken relative to an anchor (the first pair, re-anchored
  on merge) so a price level does not cancel the covariance away; invertible, so it evicts under `maxAge` like
  the moments. The lead-lag form (`lag: k`, `field` of event i with `against` of event i − k) is *not* a
  per-event contribution — evicting the far edge would need the rows before it — so `summaryOf` gives it no
  family and it folds the same family over the scanned window. `fracdiff` is a fixed FIR over the last k events
  (`fracdiffWeights`, resolved once into the column plan): a bounded tail, no state.
- **The general form** (`lift` + `summarize.dynamics`, family `lti`): each (channel, halflife) of a block is one
  `Dynamics` state — a vector, one entry per component — shared by the component columns through the `stateKey`
  coordinate (`ColumnPlan.stateKey` keys the `KeyState`, so the second component column finds its state already
  advanced). The contribution of an event is `Dynamics.Event(millis, value)` — a missing value is still an event
  (it advances the events clock); `Summary.readAt(state, readout, now)` reads the state moved to the current row's
  time (fourier / legendre; exponential reads at the newest event — §9.6.6 read position). Measures (§9.6.6): `exponential` (Laguerre basis under `e^(−θ·age)`, `ewma` = order 0 — the `ewma` op is
  sugar: its columns carry `measure: exponential, order: 0` and run on the same state, so it is no longer an
  unbounded scan), `fourier` (rotation per harmonic, optionally damped) — both groups — and `legendre` (power
  sums about the first event, rescaled to the window's span at read; a monoid, so a `maxAge` window re-reads).
  `Dynamics.project` is the direct projection of a window: the scan path (`maxEvents`, general filters) and the
  reference of `DynamicsTest` / `SequenceIncrementalTest`. The `bilinear` family (log-signatures, `Signature`) and
  `compress` are in §9.6.6.

### 4.4 population (encoding) — expanding fits map onto the time-ordered replay

**Central design decision**: an expanding fit (ordered target statistics) is "key by the keySet and
accumulate sufficient statistics in time order while encoding each row", i.e. **the same mechanism as
sequence**. Backfill completes in one pass without persisting artifacts.

```
keyed stage (keySet K):
  key by K → sort each key by event time → replay:
    state: running sufficient statistics per K value (n, Σy, Σy² / value counts / order statistics)
    row t:  1. fold every past row whose timestamp < t into the statistics (same-timestamp rows are pending)
            2. compute stats (count / share / mean / rate / std / distribution / quantile) → emit
            3. the row's own target contribution joins the history (visible to later timestamps only)
```

- Step 1 guarantees **structurally** that rows of one event (targets `after(event)`) never enter each
  other's encoding — the encoding version of the availability filter.
- `targets × stats` are several output columns of one stage (one accumulator per target).
- **Lattice shrinkage** is decomposed into *per-level stages* and *row-local composition*: every level
  (leaf / intermediate / global) is a hidden population column (`{block}__{keys|global}__{window}__{target}__n`
  / `__sum` / `__sumsq`) of the level's keyed stage; the visible columns are row operators (`compose` /
  `deviation` / `effectiveN` / `share` / `fitStat`) that compose top-down (back-off, leave-node-out =
  parent − child, scale transforms) inside the row. The Beam counterpart of spec §5.3.1 "per-level
  aggregation + one top-down pass". The global level of a lattice is **one key holding every row**;
  §9.3 and §9.5 are about that key.
- `weights: fixed` (`w = n/(n+λ)`) is row-local. `weights: varianceComponents` (τ² across siblings) is
  not compatible with expanding semantics in the strict sense; the implementation **estimates the
  per-level pseudo-counts over the whole batch** (`VarianceComponents.estimate`: a Combine → side input
  `Map<levelNColumn, λ>`), documented as the structural-leak class of spec §6.3 (a hyper-parameter, not
  time-expanding). τ² ≤ 0 → complete shrinkage (logged); fewer than two keys → `priorWeight`.
- `fit.mode: static` / `fold` go through the fit stage (§4.5).
- Statistics: `count`, `share` (leaf n / global n), `mean`, `rate`, `std` from (n, Σy, Σy²);
  `distribution` from value counts; `quantile` / `q<NN>` from an `OrderStatistics` multiset (Fenwick tree
  over sorted blocks, insert / delete for `maxAge` eviction, R type-7 interpolation). Quantile and
  distribution need the per-key value distribution, so they are **expanding only** (`encoding.stat.static`
  in static / fold, which keep sufficient statistics only) and receive no shrinkage (the quantile of an
  interpolated distribution is not the interpolation of quantiles). NaN counts as missing for every
  numeric statistic.

### 4.5 population (static / fold fits) — the fit stage

The pattern established by the framework's attribution transform: (a) `Combine` (globally / perKey) to
sufficient statistics, (b) gather on one worker where a matrix computation is needed, (c) `View.asMap`
/ `asList` side inputs into an apply `ParDo`.

- **Encoding levels** (`fit.mode: static | fold`): `VarianceComponents.perKeyStats` computes (n, Σy,
  Σy²) per level and key over the **whole input re-windowed into the global window** (`_FitGlobal`:
  a static fit means "the whole input" whatever the module's windowing strategy, so the values are
  identical under fixed windows and the artifact writer and the windowed main input both map onto the
  side inputs). `FitApplyDoFn` fills the hidden level columns per row by lookup; composition is the
  same row operators as in the expanding case. `fold`: every contribution is also emitted under a
  fold-tagged entry (`foldOf(unitKey) = floorMod(hashCode, folds)`, the unit being the `fit.groupBy`
  entity's keys or the row identity), and apply subtracts the row's own fold from the totals (n ≤ 0 →
  "no statistics"). λ comes from the totals.
- **Static-fit blocks** (`StaticFitBlock<M>`: `FmSpec` for factorization, `DiscretizeSpec` for
  discretize, `QuantileTransformSpec` for quantileTransform, `SvdSpec` for svd, `JointSpec` for
  `estimator: joint`): rebuilt from the output columns' coordinates; `fit(fitInput)` = extract → `Combine.globally`
  (gather on one worker; the gather has a default accumulator so an empty input still fits) → fit DoFn
  (writes the artifact) → `View.asList`; or `readArtifact` at `@Setup` when the artifact exists.
  `apply(model, values)` fills the block's columns. Adding a population type = one model class + one
  `StaticFitBlock` record + one compiler expansion (see the skill's `add-operator.md`). Two gather shapes:
  discretize and quantileTransform gather the raw values (`Doubles` / `QuantileTransform.Values`, 8 bytes per
  row) because quantiles need the order statistics — quantileTransform per time block, as a `Summary` family
  that is a monoid without inverse (merge = concatenation), so a `BlockSeries` serves `static` (one block),
  `forward` (a prefix) and a `window` (a range, merged rather than differenced) and the knots are re-fitted per
  change point, exactly the static fit on the readable values (`QuantileModel`, the twin of `SvdModel`); svd gathers only the sufficient statistics (`Svd.Moments`: n, Σx, Σxxᵀ taken
  relative to the first accepted vector so a large offset does not cancel the covariance away; merging
  re-anchors exactly — one `Combine`, no row leaves the workers) and diagonalises the d × d matrix on the
  driver (cyclic Jacobi, convergence judged relative to the Frobenius norm).
- **Joint estimator** (`JointSpec` → `JointFit`, spec §5.5 rule 1): one model per keySet × window × target of
  an encoding block with `estimator: joint`. The extract emits `(cell, y)` with the cell = the cross of every
  level's key fields (`FeatureValues.key`, decodable by `keyComponents`), `Combine.perKey` aggregates
  (n, Σy, Σy²), the gather brings the cells to one worker and `JointFit.solve` runs block Gauss–Seidel on the
  ridge / BLUP normal equations over the indicator basis of every level (weights `n · v(ȳ)` with the
  delta-method factor of the scale; λ per level from `Shrinkage.lambdaFromMoments` over the level's contexts
  or `priorWeight`; a level with τ² ≤ 0 fixed at 0; levels swept coarse → fine so a rank-deficient system
  resolves to the hierarchical convention). No hidden level columns are registered for a joint lattice — the
  `joint` columns (`kind` composed / deviation / effectiveN) are filled by lookup of the row's context keys
  (unseen context → effect 0, null leaf key → null). `fold` tags each cell part with `foldOf(unit)` and
  solves the totals minus each fold; `forward` keys the cells by block and solves once per window change
  point (every observed block, plus `block + windowBlocks` where a block leaves the window) over the blocks in
  `(U − windowBlocks, U]`, so the floor entry of a row's usable block `U` is exactly the encoding path's
  window (`minBlocks` counts observed blocks, not change points); both re-fit every run, the artifact
  (`<id>.joint.avro` + manifest with μ, λ, contexts, iterations) holds the whole-input solution.
- Rejected at construction: a fit target / offset / input produced by the same fit stage (it would
  read null — the compiler's strict-dependency rule keeps them apart, and the engine double-checks),
  and a fit without an existing artifact in streaming.

---

## 5. Fit artifacts and train/serve

- **Format**: Avro for the encoding levels (`FitArtifact`: level / key / n / sum / sumSq +
  `<block>.manifest.json` with λ), `<block>.fm.avro` for factorization (latent vectors stored as
  big-endian `bytes` — Avro on this classpath round-trips `array<double>` at float precision),
  `<block>.bins.json` for discretize, `<block>.quantiles.json` for quantileTransform (knots, and the
  probability `clip` of a normal score, recorded for the record — the clip is apply-time, so the loading
  config's `clip` replaces the artifact's (`QuantileTransformSpec.readArtifact` → `withClip`), which keeps a
  pinned `fit.artifact.id` or a pre-clip artifact honest),
  `<block>.svd.json` for svd (mean / scale / components / variances — JSON keeps double precision, and the
  matrix is rank × d), `<block>__<keys>__<window>__<target>.joint.avro` (kind / level / key /
  value records: μ, per-level λ, effects, leaf n) for the joint estimator. Reads and writes go through
  `ResourceUtil` (`gs://`, `s3://` and local paths with one code path).
- **Content addressing**: the artifact root is `{artifactUri}/{planHash}/`, where `planHash` =
  SHA-256 (first 16 hex digits) of the canonical (key-sorted) sources document + parameters **minus
  `engine` and every `fit.artifact`** (`FeaturePlanCompiler.hash` / `withoutArtifact`): re-fitting or
  relocating artifacts must not change the identity of what was fitted, and runtime knobs must not
  invalidate artifacts. `fit.artifact.id` pins a hash for a serving config. The plan hash is also the
  candidate identity of spec §8.
- **Loading**: at assembly `ResourceUtil.exists` decides fit vs. load per block (`refit: true` forces a
  fit; fold levels are always re-fitted but respect `refit: false` for the totals artifact); workers load
  at `@Setup` into JVM-wide caches keyed by path (paths are content-addressed, so a path never changes
  meaning).
- **Serving path**: the framework's HTTP serve mode (`request` source + `POST /run`) is the online
  path — a feature step with `fit.mode: static` + artifact URIs is a pure map and works per request.
  Sequence scope needs history, so serving would read logged / precomputed features through a lookup
  (`query`); this is designed together with feature logging (v1).

---

## 6. Streaming

| scope | batch | streaming |
|---|---|---|
| row | yes | yes (stateless) |
| context | yes (event-key GBK) | partial: GBK inside the strategy's window; group completeness depends on the trigger |
| sequence | yes (keyed replay) | **rejected** (the stateful DoFn + timers variant is not implemented) |
| population expanding | yes | **rejected** |
| population static | yes | apply only, with an existing artifact (fit rejected) |
| population fold | yes | **rejected** (out-of-fold statistics need the whole input) |

Rejections are `engineConstraints` errors at assembly. Waves (§9.4) run linearly in streaming (the
fan-out merge is a batch GroupByKey; the stateful merge is §9.4.6). `mutability: corrections` /
`lateness` freezing guarantees in streaming wait for feature logging (v1).

Design notes for the streaming work: (a) GBK + backward re-emission (`getAllowedTimestampSkew`)
makes downstream late-data drops, so keyed stages must become stateful DoFns with event-time timers;
(b) the linear chain accumulates watermark waits per stage, so the wave DAG matters even more (§9.4.6);
(c) the single global key (prior = global / share denominators) is a throughput ceiling and would be
replaced by a periodic-snapshot side-input approximation. Low-latency serving is `fit.mode: static` +
artifacts + `request` source (no keyed stages); a streaming chain is for feature-logging-style continuous
computation with generous latency.

---

## 7. Output, naming, lineage

- Output names are generated deterministically in the compile layer: row `{name}` (`datetime`:
  `{name}_{derive}[_sin|_cos]`), context `{name}_{field}_{op}`, sequence
  `{name}_{window}_{field}_{op}{param}` (the window token is always present; the default window is
  `all`), encoding by the `naming` template (default `{block}__{keys}__{window}__{target}__{stat}`,
  empty segments collapsed), anonymous expressions `{block}__e{n}`, baselines `__baseline_{name}`,
  deviations `dev{level}` (`@` is not Avro-legal). Output name = `_` (intermediate) + `output.prefix` +
  canonical name. `maxFeatures` overflow is a compile error.
- Every output `Schema.Field` carries the lineage in its options (`feature.block` / `scope` /
  `operator` / `canonical` / `availableAt` / `status` / `windowShift` / `derivedFrom` / `sources` /
  `evidence` / `fit` / `placement` / `validFor` / `computeAt` / `coord.*`); `describe()` and the runtime
  schema come from one source.
- Rows flow through every stage as `DataType.ELEMENT` maps keyed by canonical names and are converted
  to the output schema (default AVRO) in the finalize. `nullPolicy: indicator` adds `bool` companion
  columns (fixed at compile time); `fillZero` zeroes missing numeric features; `passThrough: all | keys |
  none` selects the input fields copied to the output (`keys` makes the table safe for `SELECT *`).
- Failure handling follows the framework: per-element try / catch → `Module.processError` →
  `failureTag` → `errorHandler.addError`. One row's evaluation failure is one failure record (`failFast`
  defaults to true in batch); a keyed stage whose sort / spill fails routes the whole key row by row.

---

## 8. Tests and documentation

- Config-driven e2e tests (`create` source + `PAssert`) on a domain-neutral online-auction dataset,
  one class for the module (`FeatureTransformTest`) plus targeted ones (spill, merge); leak checks are
  asserted with data (targets of two rows of one event never enter each other's encoding; a row below
  the availability threshold is excluded from the window). The compile layer has Beam-free unit tests
  (`FeaturePlanCompilerTest`: availability algebra, DAG, product expansion, scheduling, waves, error
  codes); the evaluators have a randomized incremental-vs-scan equivalence test
  (`SequenceIncrementalTest`). The full map is in the skill's `testing.md`.
- Docs: `src/main/resources/server/docs/module/transform/feature.md` (front-matter `title:`, parameter
  table, per-scope reference, YAML examples, performance and sizing, limitations) + the entry in
  `module/index.yaml` (required for the UI).

---

## 9. Implementation status

### 9.1 Phase mapping

Spec §9 lists the phases. Implemented: all of **v0** except the run-time availability filter
(`runtimeFilter` columns are rejected) and the `asOf` / nested-encoding family; **v0 additions**
entirely (shrinkage block, `structure: hierarchy | cross`, generalised `hierarchy` with `additive`, keySet
`windows`, per-keySet shrinkage, `window.filter`, baselines / offsets, `scale`, `estimator: sequential`,
`fit.groupBy`); from **v1 / v1 additions**: `fit.mode: static` and `fold` with artifacts,
`weights: varianceComponents`, `output: deviations | effectiveN`, `type: factorization` (fm / fwfm,
ALS, `pair` / `embedding` / `sum` outputs, r-matrix lineage), `type: discretize` (`method: quantile`),
`type: quantileTransform` (uniform / normal), `type: svd` (PCA scores of a field vector or an array),
the `quantile` stats, `output.groupBy`, hot-key audit queries, `--dryRun` and the server exposure of
`validate --expand`, `estimator: joint` (static / fold / forward), the conjugate families (`family`,
with the Dirichlet-Multinomial shrinkage of `distribution`), the general sequence form with `lti` dynamics. Everything else is parsed and rejected with a
diagnostic (§9.2 "deferred").

### 9.2 Implementation status and decisions

**Compile layer.** Reference resolution is an assembly loop (a block expands once every reference it
makes — expression identifiers, fields, keys, baseline / offset — resolves to an input field or an
expanded column; leftovers are reported together as `reference.unresolved` / `reference.cycle`; block
order is irrelevant). Violating columns (`availableAt > computeAt`) become `_` intermediates when
consumed and `availability.violation` errors when terminal. Secondary failures are demoted to
caused-by info, availability verdicts are deferred while blocks are unresolved, and repetitive hints
are emitted once per block. Diagnostic codes are `scope.field.reason` and part of the interface (tests
assert them, the docs cite them, the agent loop acts on them).

**Execution layer.** Rows are converted to `ELEMENT` maps and **re-timestamped from `time.field`**
at the entry (`ToElementDoFn`), so Avro inputs and sources without a timestamp attribute drive the
keyed stages correctly; a null time field is a failure record. Keyed stages are GBK + time-ordered
replay (§4.3), not Beam state.

**Output contract and audit.** `output.roles` / `include` / `manifest` (DSL spec §7) are compile-layer
facts: roles are validated against input fields / contexts / entities / baselines, `include` is applied
in `finalizeColumns` as the projection (it replaces `exclude`; a URI is resolved by
`FeaturePlanService.resolve` before compile and its content hash kept; roles are resolved once before the
projection — `resolveRoleColumns` stamps `OutputColumn.role`, read by `applyInclude`, the `exclude` pass,
`toOptions`, the manifest and `FeaturePlan.getRoleColumns` — so a role column is kept whether or not the
list names it or an exclude pattern matches it, `output.include.role` / `output.exclude.role`, and a
column kept only as a role gets no `_isnull` indicator), and `FeaturePlan.toManifest`
builds the manifest the transform writes at assembly. `include`, `includeSource`, `includeHash` and
`manifest` are stripped from the plan hash (`withoutArtifact`), and `FeaturePlan.getOutputHash` =
plan hash + emitted names + roles + include hash identifies the output table. The observedAt audit
(`FeaturePlan.ObservedAtAudit`, one entry per input field with an `observedAtField`) runs inside
`ToElementDoFn`: counters `feature/observedAt_<field>_late|afterPredictAt|missing`, `audit.observedAt:
fail` throws into the failure path, and — batch only, when a manifest URI is declared — samples of
`predictAt − observedAt` ride a side output into `ApproximateQuantiles` / `Count` per key, reduced with
the finalize row count into one `View.asList` and written as `manifest.run.json` by one worker
(`WriteRunManifestDoFn`, the artifact writer pattern). The output PCollection itself is never consumed
inside the engine (the module sets its coder once more), hence the count side output from the finalize
DoFns.

**Softmax, baseline emit and placebos.** `softmax` is the first context op with two per-row inputs
(score + offset): `configureContextOp` resolves the offset (a baseline → its `__baseline_*` column),
inherits its `validFor`, and puts temperature / scales into the coordinates; `ContextEvaluator.softmax`
evaluates the group in probability space with a max-shift. `temperatureFrom` is resolved by
`FeaturePlanService.resolveTemperatureFrom` into `{source, hash, value}`, stripped from the plan hash and
listed in `FeatureSpec.resolvedExternals` (output hash + manifest `externals`). `baselines[].emit`
adds a row `copy` column of the baseline. `noise` (row) hashes (seed, `time.field` + `orderTieBreak`)
with murmur3 into a `SplittableRandom`; `shuffle` (context) draws a Fisher–Yates permutation from
(seed, group key) and applies it to the rows ordered by identity then the remaining input fields
(`tieBreak` coordinate), so the result is a pure function of the group in every engine mode — the
parallel-vs-linear equality test covers both.

**Forward fits.** `fit.mode: forward` (spec §4.4 fit metadata): `ForwardBlocks` (fixed-size blocks from the
epoch or UTC calendar buckets; `usableBlock(event, predictOffset, lag) = indexOf(event + predictOffset −
lag) − 1`), `VarianceComponents.forwardSeries` (extract → `Combine.perKey` per (level, key, block) → GBK per
(level, key) → sorted prefix `ForwardBlocks.Series`) as a `View.asList` that `FitApplyDoFn` indexes once per
DoFn instance into an in-memory map, and `FitApplyDoFn.forwardStats` (floor lookup of the row's usable block,
`windowBlocks` prefix difference, `minBlocks`). The λ per block (moments over the keys' cumulative
statistics up to each block) is computed in the pipeline — `VarianceComponents.lambdasByBlockView`: one
`Combine.perKey` per level whose accumulator is the level-wide moments as a step function over the keys'
blocks (`BlockMomentsFn`, merges union the blocks), gathered into a levels × blocks list side input — and
swapped into the row evaluator per row; the forward artifacts' totals (`<block>.avro`) and their manifest
`lambdasByBlock` come from the same PCollections through the shared `writeArtifacts` path (entries grouped
under their block, one write per block, an empty marker so an empty input still writes). Only a lattice
column with hidden `levels` requests the λ estimate (`RowEvaluator.needsVarianceComponents`); a joint
column declares the same weights but resolves its pseudo-counts inside the solve.

Why no map side input on this path: on a portable runner (Dataflow Runner v2, prism) a `View.asMap` is
served through the state API — `get` is one fetch per (row, level) and iterating the entries is one fetch
per entry — so the per-row probes of a multi-level forward block and the artifact writer's scan stalled a
production run for tens of minutes with no output (consumer feedback on PR #117). The list view is one
sequential read per DoFn instance; the whole forward series of a stage must fit a worker's memory (a
series holds four arrays over the blocks a key touches), which the artifact writer already assumed.

**Baseline offset on a logit / log scale** (spec §3 rule 5). An offset block whose shrinkage scale is not the
identity registers one more hidden statistic per lattice level, `<level>__sumoff` = Σ baseline over the rows the
level's `sum` counts (`PopulationEvaluator.SUM_OFFSET`, served incrementally by the same moments summary as
`sum`, with the baseline as its contribution); the
gate is per lattice (`FeaturePlanCompiler.offsetTerm`: offset block, shrinkage enabled, scale not identity —
passed into `levelStats`), so an identity-scale or unshrunk lattice of the same block, and every identity-scale
plan, is unchanged. `Shrinkage.Level` carries the column (`offColumn`, a fourth token in the `levels`
coordinate) and the static `Shrinkage.own` computes the level's term `t((Σ(y − b) + Σb) / n) − t(Σb / n)` with
leave-node-out over all three sums — null (no estimate at that level, like `n = 0`) when the mean baseline is
outside the transform's open domain (`Shrinkage.baselineDefined`: `Σb ≤ 0` on log / logit, `Σb ≥ n` on logit),
where the term is undefined and the transform's clamp would otherwise leak ±13.8 / −27.6 into it; `compose`
returns the shrunk term through `Shrinkage.output` (no inverse transform under an offset term). The rows a
level counts and sums are the same on every engine: `FeatureValues.offsetTarget` (target present and, under an
offset, baseline present — NaN counts as missing) is the one rule behind the replay's `count` / `sum` / `sumoff`
and the fit-side extract DoFns. On the fit side the extracted value is that pair `(y − b, b)`
(`VarianceComponents.valueCoder`), `KeyStats` / `ForwardBlocks.Series` / `JointFit.Cell` carry `sumOff`, the
artifact schema gains a defaulted `sumOff` field (older artifacts read 0), `FitApplyDoFn` fills the hidden
column from it, and `JointFit.solve(offset = true)` uses the same `Shrinkage.own` per cell as `z_c` with the
observed statistic's delta-method weight (a cell with an undefined baseline is skipped like an empty one),
`estimate` returning η itself. The diagnostic is the info `encoding.offset.additive` (the former
`encoding.offset.scale` rejection is gone).

**Joint estimator and conjugate families.** `estimator: joint` is a fit-stage estimator: the lattice's
statistics-carrying levels (an `additive` entry expands to the main-effect key lists) become the effect
levels of one mixed model `t(y) = μ + Σ e_level + ε` solved as ridge / BLUP over the aggregated cells
(§4.5, `JointFit`); it needs the whole cell table, so `expanding` rejects it (`encoding.shrinkage.estimator`).
`deviations` are the per-level effects, `effectiveN` the leaf's `n + λ`. λ stays a row pseudo-count on every
scale: a context's ridge is `λ · v̄` with `v̄ = Σw / Σn` its mean delta-method weight, so its shrink weight is
`n / (n + λ)` like the other estimators (a plain ridge on the identity scale). Cells are keyed with
`FeatureValues.keyWithNulls`: the leaf key must be present, a null coarser key gives the cell no indicator on
that level (`ctx = −1`) but keeps it in the intercept and the other levels — the per-level rule of the
encoding path, and what `JointFit.effect` does at apply time. The `family` declaration is a
check plus one new statistic rather than new arithmetic: with pseudo-count `m = λ` the posterior mean of every
conjugate family on the identity scale is the existing recursion, and the one-way moment estimator of λ is
Kleinman's Beta-Binomial moment estimator (`ShrinkageTest` asserts the identity), so `gaussian` /
`betaBinomial` / `gammaPoisson` produce identical `mean` / `rate` columns and differ only in the lineage;
a *declared* conjugate family requires `scale: identity` (`encoding.shrinkage.family.scale`), while a derived one
falls back to gaussian on logit / log (`Shrinkage.resolveFamily`, rule 7 keeps transformed scales Gaussian; a
derived `distribution` there is emitted unshrunk with a warning). `dirichletMultinomial` is the `distribution`
statistic shrunk along a chain lattice:
hidden `__n` + `__dist` (per-category shares) columns per level, `Shrinkage.composeDistribution` =
`(counts + λ · p(parent)) / (n + λ)` with leave-node-out over the union of categories, a map-valued
composed column; expanding only (the stat is not sufficient), `priorWeight` as λ — the compose column is
stamped `weights: fixed` so it never reads the stage's λ map, which its shared hidden `__n` columns would
otherwise resolve to a sibling scalar statistic's estimate — and no `deviations`. Fit manifests write an
infinite λ (a fully shrunk level) as the string `"Infinity"` (`FitArtifact.lambdaJson`): Gson's lenient
writer would emit a bare token that strict JSON readers reject.

**Fits.** Static: per-level sufficient statistics from the whole input (global window), lookup per
row, `fitStat` for count / mean / rate / std, artifact write / load, `refit`; the hidden columns'
availability is the fit boundary (`computeAt`), and an info diagnostic states that training rows contain
their own outcome (training = expanding, serving / offline = static). Fold: fold-tagged per-key
statistics, total minus own fold, artifact = totals, an info diagnostic states the cross-fit nature
(other folds contain later events). A block-level `fit.groupBy` must name a declared entity (an unknown
name would silently fall back to a row-level fold — a leak).

**Production feedback folded into the design** (from the first backfills): `output.childName`,
`passThrough`, `values:` per-value columns, `as:` aliases for ops and targets, `COUNT(1)` (field-less
aggregate), row `indicator` / `equals`, `${args.*}` defaults passed to module template arguments, the
`sequence.aggregate.encoding` hint once per block, compile-time parsing and reserved-word quoting of
predicates and filters.

**Performance work items** (all measured on the reference plan; the timeline is 52 → 31.8 → 25.3 →
15.0 → 9.3 → 9.0 minutes end to end):

- **S1 — spill sorter (§9.3)**: Beam's `SortValues` (which keeps spill files until the JVM exits, writes
  twice, encodes every key) replaced by `KeyedSpillSorter`. Disk 500 GB → 30 GB, 52 → 31.8 min.
- **S2 — key-affinity scheduling (§3.1)**: `buildStages` replaced by the scheduler; blocks of one key
  fuse across config order, sequence + population fuse. Stages 26 → 20.
- **S3 — wave DAG (§9.4)**: independent stages branch and merge by row id; the merge folds into the
  next context stage or the grouped finalize. 25.3 → 15.0 min, and with a fixed worker pool 9.3 min.
- **S4 — coarse keys to static / fold**: a configuration-side lever, surfaced by the `encoding.globalKey`
  hint (key-less replay stages; the values change, so it is a modeling decision and validated by model
  metrics, not by output diffs).
- **S5 — unbounded windows**: the `sequence.window.unbounded` compile-time hint (with the fields kept).
- **S6 — per-field history trimming (§3.1 (e))**: 31.8 → 25.3 min, longest population stage 212 → 136 s,
  worker memory 42.4 → 39.9 GB, identical output.

**Operational findings recorded as design facts**: Dataflow's default autoscaler shrinks the pool at
the wave fan-out (one fused stage with no visible backlog), so a parallel batch run needs
`options.dataflow.autoscalingAlgorithm: NONE` with a fixed `numWorkers` (the option itself was broken —
inner-class name and enum-typed setter — and fixed in the same arc); `engine.spill.compress: true` is
2–3× slower on spill stages and stays off by default; the DirectRunner is unsuited to coarse keys
(§9.5) and the prism image is the local / Cloud Run subset tier (in-memory, no spill, container memory
roughly linear in the input).

**Deferred (parsed, rejected with a diagnostic)**: `weights: heldOut`,
`estimator: joint` under `fit.mode: expanding` (row-local replay cannot hold the cell table), a variance-components
λ for a shrunk `distribution`; `structure: sequence`; nested encoding targets;
`quantile` / `distribution` under static / fold; discretize `tree` / `optimal` (the two-stage target
consumption is not modelled); `spectralEmbedding` / `transitionStats` (the sequence-of-values population
types: they need the per-entity value sequence, i.e. a keyed pass before the fit); the general sequence
form's `probabilistic` dynamics (`sequence.dynamics.family`); factorization `variant: bayesian` and `fit.cadence / warmStart`; sketch-backed (approximate,
bounded-size) per-key quantile / distribution stats in static / fold — quantileTransform, static and forward,
keeps the exact values (decision 11); the run-time availability
filter (`atRowCreation`, `event_date THH:MM`); streaming keyed stages and the stateful merge (§9.4.6);
sequence / population stages as fold-in merge targets (composite sorter key, §9.4.3); the prefix-scan
decomposition of the global-key stage (§9.4.4); observedAt / ingestedAt / confounding audit queries
(spec §7 — whether sources should carry physical table references is undecided).

**Block series and the forward svd** (§9.6.2). `BlockSeries<S>` holds one `Summary`
state per observed time block of a fit and merges the blocks a row may read on demand — `(usable − windowBlocks,
usable]`, every block up to `usable` without a window — so a statistic written once as a `Summary` family is
served under `static` (one block), `forward` (a prefix) and a `window` (a range) by the monoid law alone (a
non-invertible family is merged over the range instead of a prefix difference). A model that has to be solved
from the state is fitted once per *change point* (every observed block and, under a window, the index at which a
block leaves — the rule `JointFit` already used for its per-block solutions) and read by floor lookup with
`minBlocks`. The first user is `type: svd` under `fit.mode: forward`: `Svd.SUMMARY` wraps the anchored moments,
`SvdSpec.fit` runs one `Combine.perKey` over the blocks (`SummaryFn`, a `Summary` as a Beam `CombineFn`),
gathers the parts and solves the components per change point (`SvdModel`); the artifact keeps the whole-input
components for a static serving run and a forward fit is re-fitted every run, like the joint estimator. The fit
block gains `window` (the range of blocks; for encodings the default of keySets without `maxAge`) and `minHistory`
(`minBlocks` as a duration), both in the plan hash. The encoding levels keep their own `ForwardBlocks.Series`
(prefix arrays + the per-block λ) for now; moving them onto `BlockSeries<Moments>` is the next step of this
line, after which one Combine per summary family serves every block kind of a fit stage (the fan-out item above).

**Row vector op** (§9.6.4, the row supply). `type: vector` turns an
`array<float64>` input into scalar readout columns through `VectorOps` (§4.1); until then an array field could
only feed `type: svd`. It is a plain row op — no stage, no state, availability inherited from the field — and
the first of the three supplies of the vector operators; the sequence-window and context-group supplies will
call the same `VectorOps` readouts for the statistics that have no `Summary` family (scan).

**Fit-stage fan-out (performance, not correctness)**: every static-fit block used to be its own
`Extract → Combine → Gather → Fit → View` chain, so a fit stage with 13 quantileTransform / svd blocks expanded
into ~34 Dataflow steps and the stage time grew with the block count (a consumer measured 13 → 20 min on 148k
rows against the previous config). Blocks whose fit state is a `Summary` family now declare only what a row
contributes and how the model is solved (`SummaryFitBlock`: `contribution(row)` → (time block, value),
`solve(parts, planHash)`), and the stage fits them together (`fitSummaryBlocks`): per family ONE extraction pass
over the rows (the row map is built once for every block, not once per block), ONE `Combine.perKey` keyed by
(block, time block), a regrouping by block and one solve per block — the blocks solve in parallel, each on the
worker its group lands on — and the models of every family reach `FitApplyDoFn` as ONE list side input of
(block, model), indexed once per DoFn instance (a list, not a map view: a map side input is a state fetch per
lookup on a portable runner). A block that fits an empty input too (quantileTransform: n = 0 is still an
artifact) adds an empty marker part so its group exists; svd, which has no model without vectors, does not. On
the test graph with eight blocks the stage's top-level transforms go 40 → 14 (289 → 38 nodes including composite
internals) and no longer grow with the block count. svd (moments) and quantileTransform (values) are on it;
discretize (the same gathered values) and the fm / joint fits keep their own chains. The Dataflow wall-clock
measurement on the consumer's 13-block config is still to be recorded here.

### 9.3 S1: the keyed stages' own external sort (`KeyedSpillSorter`)

#### 9.3.1 The problem

The first spill implementation used Beam's `SortValues` (`BufferedExternalSorter`, native sorter)
between the GroupByKey and the replay DoFn. It stopped OOMs with one line but showed four problems on
the reference plan:

| # | problem | cause inside the Beam sorter |
|---|---|---|
| P1 | spill files stay until the JVM exits → disk exhaustion | the temp dir is only `deleteOnExit`; a PTransform cannot observe "key done" |
| P2 | disk ≈ 2× the input | every record is written to a data file first, then sorted chunks are written separately |
| P3 | small keys pay encode / decode too | values are converted to `KV<byte[], byte[]>` before sorting, for every key |
| P4 | a fixed 100 MB budget, multiplied by concurrent bundles | `withMemoryMB` is fixed at graph construction and not parameterised |

Side issue: the spill directory had to be chosen on the launcher by runner detection, because the
sorter options require a path at graph-construction time.

#### 9.3.2 Alternatives

| option | summary | verdict |
|---|---|---|
| A. `@RequiresTimeSortedInput` stateful DoFn | let the runner deliver each key in time order | per-row state reads, slower than GBK + replay; limited batch support on some runners. Re-evaluated with the streaming work, not for S1 |
| B. spill projected columns only + row-id re-join | sort only the history fields, re-join the output | less disk but one more shuffle, worsening the serial fixed cost. Rejected (revisited with §9.4) |
| C. **own `KeyedSpillSorter` (adopted)** | chunk-sort the GBK iterable inside the DoFn, k-way merge, small keys in memory | solves P1–P4 and the change stays inside `FeatureStages` |

#### 9.3.3 Design

Borrowed from Beam's sorter: the buffer-then-spill hybrid, chunk sort + priority-queue merge, and the
sign-flipped big-endian long as the byte-ordered secondary key. Not borrowed: pre-encoding to bytes, the
unsorted data file, `deleteOnExit`. The `beam-sdks-java-extensions-sorter` dependency was removed.

1. *Tier 1 (in memory)*: rows are collected as objects up to a **row limit** `budgetBytes /
   estimatedRowBytes`, where the estimate encodes the first 64 rows with `ElementCoder` and multiplies by
   a heap factor of 3 (`MElement` + map overhead), re-sampled per key (later stages carry more columns).
   A key that fits is sorted in memory (stable) with **zero encode / decode / disk I/O** (P3).
2. *Tier 2 (spill)*: at the limit the buffer is sorted and written as one chunk file (`count, (long key,
   ElementCoder value)*`, 64 KB buffered, optional Deflate level 1); the buffer is cleared. **The last
   buffer stays in memory** and joins the merge (disk ≤ input − last chunk, P2).
3. *Merge*: chunk readers (one row read ahead) plus the in-memory chunk in a priority queue keyed by
   `(sortKey, chunkIndex, seq)` — equal keys keep GBK arrival order.
4. *Deletion*: the returned `Sorted` iterable is `AutoCloseable`; the replay closes it in `finally`
   and the chunk files are deleted **per key** (P1). `@Teardown` deletes the DoFn instance's directory,
   `@Setup` sweeps directories of dead pids on the same host.
5. *Temp dir*: `@Setup` creates `feature-spill-<pid>-*` under the worker's `java.io.tmpdir` (Dataflow
   `/tmp`, local runners the launching machine); the launcher-side runner branch is gone.
6. *Fan-in*: chunks = ⌈key bytes / budget⌉; more than 1024 chunks (a key larger than 1024 × budget) is an
   error rather than a multi-pass merge — such keys are visible beforehand in the hot-key audit.

**Budget (P4)**: `engine.spill.memoryMB` > `--featureSpillMemoryMB` (pipeline option) > the default
`clamp(maxHeap / (cores × 4), 16 MB, 256 MB)` computed on the worker (batch runners process one bundle
per core, so concurrent sorts = cores). `engine.spill.directory` and `engine.spill.compress` (default
false) complete the block. The spill configuration is part of `FeaturePlan.toJson()` / `describe()`,
and the audit-query note says that the top row count is the spill bound.

**Edges**: `NULL_KEY` rows bypass the sorter; an empty iterable returns empty; an `IOException` fails
the key (row by row, `failFast`) and the files are deleted in `finally`.

**Logging**: one `budget` line per JVM (INFO once, DEBUG after), one line per spilled key —
`keyed spill sorter Stage<N>_<kind> key=<key>: <chunks> chunk(s) / <MB> MB on disk + <rows> rows in
memory; live spill on this worker <MB> MB (peak <MB> MB)` — whose peak is the worker disk the keyed
stages need, and the unbounded-columns line at stage setup.

#### 9.3.4 Result

On the reference plan: disk 500 GB → 30 GB, 52 → 31.8 minutes, sequence stages 44 → 30 s, the
coarse-key population stages 1.5–2.5× faster, worker memory 50.5 → 42.4 GB, identical output. With
`compress: true` the spill stages were 2–3× slower (Deflate CPU on the critical path) for 10 % less
memory, so the default stays off (a disk-pressure escape hatch).

#### 9.3.5 Relation to S2 / S3

Owning the sorter inside the DoFn makes "one GBK for several stages of one key" (S2) free, and the
wave DAG (S3) passes the same `Iterable` — the sorter is unchanged by either. A streaming stateful
variant does not need it, but the batch path remains.

### 9.4 S3: parallelising the existing structure (the wave DAG)

After S1 / S2 / S6 the remaining fixed cost was ≈ 20 stages × ~40 s of **barriers**: a batch
GroupByKey starts the downstream stage only after every upstream work item has finished writing the
shuffle (scheduling, shuffle-read start-up and the tail of the slowest key together cost ~40 s). In the
linear chain independent stages line up serially, so barrier count = stage count. The quantity to
shrink is the **number of barriers on the critical path**, not the processing inside a stage.

#### 9.4.1 Why SDF / State API are not the answer

| candidate | effect on barriers | verdict |
|---|---|---|
| Splittable DoFn | **none** — it parallelises one element's processing over restrictions (large files, hot keys); GBK barriers stay. Also (a) a GBK iterable is sequential, so offset restrictions re-read from the start, (b) time-ordered replay makes row i depend on all rows < i, so splits cannot be evaluated independently without a prefix-scan decomposition (§9.4.4) | not an S3 substitute; as a hot-key remedy S4 comes first |
| stateful DoFn (State API) + `@RequiresTimeSortedInput` | **none** — in batch the runner inserts a key redistribution (= barrier) per stateful step plus a sort for time-ordered delivery | the no-State-API policy stands; independent of parallelisation |
| fewer stages (S2 / S6 done, S4) | direct | continues |
| **DAG (S3)** | serial 20 → critical-path depth D (waves) | this section |
| runner side (fixed workers) | shortens one barrier | fan-out makes backlog visible to the autoscaler, but a fixed pool stays recommended |

#### 9.4.2 S3′: wave DAG with row-id merge

**Idea**: leave the inside of each stage untouched and change only how the stages are arranged. The
compile layer derives from the inter-stage dependencies (a stage's columns reading another stage's
columns through `inputs` / `pastInputs` / strict reads) the **waves** = sets of mutually independent
stages evaluable from the same input; the stages of a wave branch from one PCollection and their
outputs are merged by row id into the next wave's input.

```
        wave 1                            wave 2                       wave 3
input ─┬─ Stage1 (entity A)      ─┐   ┌─ Stage5 (entity C)    ─┐   ┌─ Stage9 (row: compose)
       ├─ Stage2 (entity B)      ─┼ M ┼─ Stage6 (attribute)   ─┼ M ┤
       ├─ Stage3 (global level)  ─┤   └─ Stage7 (fit)         ─┘   └─ Finalize
       └─ Stage4 (attribute key) ─┘
   M = merge by row id: base row + each branch's partial row → one row
```

- **Compile layer (implemented first, no side effects)**: `StageScheduler.place` records every
  column's dependencies (`depsOf`) and `build` maps them to `Stage.dependsOn`. **Row columns are not
  nodes**: they are followed through to their own dependencies, because the linear chain places a row
  column in its first consumer's stage and carries the value forward while a branch evaluating the same
  columns from the stage input would simply recompute it — a "transport edge" such as "the global stage
  depends on the seller stage because `enc__e3` was placed there" is not a DAG edge. Conservatively, the
  dependencies of row columns a stage hosts (compose etc.) are included. The groupBy stage depends on
  everything. `FeaturePlan.getWaves()` groups stages by dependency depth; `getDagShuffleEstimate()`
  mirrors the engine loop; `describe()` prints `waves=D (dag shuffles~N)` and `deps=[...] wave=w` per
  stage, `toJson()` the same — so a dry run shows D for any config before deciding.
- **Row id**: `ToElementDoFn` assigns `__rowId` = the declared `engine.rowId` (a natural key, made
  deterministic with `keyWithNullTokens`) or a UUID. A random id must be **pinned** by a GroupByKey or a
  `Reshuffle` (`RowId_Pin`) before any fan-out reads it (a retry must not recompute what a branch already
  saw); the pin goes right before the first fan-out that no GBK has preceded (fit / row stages are no
  barrier); a declared id needs no pin.
- **Branches emit partial rows**: each branch runs its stage on the wave input and `PartialDoFn` keeps
  only `__rowId`, `__partial`, the stage's own columns and the carry keys of the merge target (the
  `ELEMENT` coder is a map coder, so a partial row transports only the keys it holds — no schema change).
- **Prelude (`Wave{n}_Rows`)**: before branching, the row columns hosted by the wave's stages that are
  computable from the wave input (input fields, earlier waves, such row columns — in expansion order) are
  evaluated on the base by `applyRows`. This was the bug found by the first production run: the linear
  chain carries a row column placed in one branch's stage, but the other branches read the wave input and
  saw null (43 of 107 columns). A DAG rule "row columns are transparent" must be matched by the engine
  actually recomputing them. Row columns reading keyed columns (compose, isnull) stay in their stage and
  travel in partial rows. Variance-components lambdas of prelude columns are wired as a side input.
- **Merge, three paths** (`getFoldTarget` / `keysAvailable` in `FeaturePlan`, shared with the estimate):
  (a) the next wave is a **single context stage** whose keys are on the base row (input fields, earlier
  waves, prelude columns) → base + partials are flattened into **that stage's GroupByKey** and
  `ContextStageDoFn` coalesces by row id inside the group (S3″); a variance-components estimate of that
  stage is computed over the wave input, not over the flattened pieces (same row set as the linear chain);
  (b) the last wave with `output.groupBy` → folded into `Finalize_Group`; (c) otherwise `Wave{n}_Merge`:
  key by row id → Flatten → GroupByKey → `MergeDoFn`. Folding into a sequence / population stage
  (composite sorter key) is not implemented and falls to (c).
- **`coalesce` semantics = the linear chain**: the number of partials must equal the number of
  branches (a row that failed in one branch is dropped and routed to the failure output, exactly as the
  chain drops it at the failing stage — no false zeros under `fillZero`); duplicate row ids reject the
  whole group; orphan partials are rejected; rows without an id (linear mode) pass through. All rejections
  become `BadRecord`s through one `rejectionRecords`. Hot paths read per field instead of copying maps;
  a context / finalize that is not a merge target skips reassembly (`fanInBranches = 0`).
- **Reserved names**: input fields `__rowId` / `__partial` are rejected by the compiler (`input.reserved`).
- **Modes**: `engine.parallelWaves: false` restores the linear chain (an A/B control with identical
  output); streaming always runs linearly.
- **Tests**: `FeatureTransformTest.testParallelWaves*` run each config in both modes on one pipeline
  and assert identical outputs while fingerprinting the merge path by transform names (`RowId_Pin`,
  `Wave1_FanIn`, `Wave1_Merge`, `_context_Vc`); `FeatureStagesMergeTest` covers `coalesce`;
  `FeaturePlanCompilerTest` covers reserved names, waves and the fold-aware estimate.

#### 9.4.3 S3″: folding the merge into the next wave's GroupByKey

A merge-only GroupByKey (D barriers) is avoidable when the next wave's consumer is a keyed stage: the
partial rows and the base enter **that stage's** GroupByKey under its key (partials carry the key
columns), and the reassembly rides the group. For context stages this is an in-memory join per group
(implemented, path (a) above). For sequence / population stages it would make the sorter key the
composite `(millis, rowId)` and coalesce consecutive records of one `(millis, rowId)` before
`evaluate` — a merge join for free on the sort; only the chunk format and the merge comparator change.
Not implemented: the reference plan never needed it (its wave-1 outputs all flow into one context stage
under the same key as the final groupBy), so the general path (c) covers the rest.

#### 9.4.4 Another lever: parallelising a hot-key stage (prefix-scan decomposition)

The one place where SDF thinking applies: cut one key's time-ordered replay into time chunks, compute
each chunk's summary (count / sum / sumsq / pending contributions / the lag-k tail) in parallel, and
replay each chunk from the prefix-combined initial state. It requires mergeable operator state
(aggregates and expanding encodings yes; rank / percentile / full-window scans no, or sketch
approximations), costs two shuffles per stage and changes the evaluators' state model — a separate
large item. Coarse keys can be moved to the parallel Combine of static / fold instead (S4), so v0 does
not take it. After S3 the single-threaded global-level stage is the remaining critical path, so this is
the next engine-side lever if it becomes necessary.

#### 9.4.5 Rollout and measurements

0. Compile-layer waves / estimate first (no side effects). The reference plan reported `stages=21
   shuffles=20 waves=3 (dag shuffles~5)`: wave 1 = 19 mutually independent keyed stages (contexts,
   entity histories, the lattice levels including the global level, a fold fit), wave 2 = one context
   stage hosting 84 columns (compose, row derivations, context z-scores), wave 3 = the groupBy. Stage
   times of the linear run put wave 1 at 24–170 s per stage (Σ ≈ 943 s) — a desk estimate of ≈ 8 min for
   the feature section instead of 19 → **go**.
1. S3′ with both merge paths from the start (the reference plan only exercises the fold-in path, but the
   general path is needed for other shapes), `rowId` declaration → no Reshuffle.
2. Measurement (fixed workers so the concurrent branches have capacity; concurrent spills add up, so
   `diskSizeGb` is sized for the keys spilling at the same time).
3. S3″ where needed.

Results (reference plan, six then twelve `n1-highmem-8` workers, 100 GB disk): total **25 m 17 s →
15 m 03 s**, feature section ≈ 19 → ≈ 8 min, output identical in every numeric column; the prototype
without the prelude was faster still but corrupted 43 columns, and `parallelWaves: false` matched the
previous run exactly (the A/B is a useful bisection). Wave 1's 19 branches ran concurrently at 61–191 s
each, **throughput-bound** on the shared threads (the global-key branch was not dominant); the fold
chain looked like the critical path but was contention. With the autoscaler off (`NONE`) and twelve fixed
workers: **9 m 16 s** total, feature section ≈ 2.6 min, branches 7–73 s, and the single-threaded
global-key stage became the critical path (73 s) — the domain of §9.4.4. The default autoscaler had been
shrinking the pool 6 → 1 / 12 → 1 right at the fan-out. Folding the variance-components estimate onto the
wave input (path (a) for context stages hosting compose columns) removed the last explicit
`Wave1_Merge`: **8 m 58 s**, barrier −1 as predicted. S4 (global level to static / fold) and S3‴-b (fit
apply at the merge point) lost priority once the fit chain shrank to under a minute.

**The reference plan hit a data-quality issue with `engine.rowId`**: the natural key was not unique
(292 fully duplicated rows) and the merge fail-fasted with `Fan-out merge: engine.rowId is not unique`
— the uniqueness check working as designed; the Reshuffle saving (≈ 30 s) is unmeasured.

#### 9.4.6 Streaming feasibility

The wave skeleton is mode-independent; only the merge component changes, exactly as the keyed stage's
inside changes from GBK + replay (batch) to a stateful DoFn + event-time timers (streaming).

- **Wave splitting is a compile-layer notion** (inter-stage dependencies → independent sets),
  runner- and mode-independent. In streaming the motivation is stronger: a linear chain sums the
  per-stage watermark waits, a DAG takes the maximum over depth D.
- **Row ids**: streaming commits bundle output and state atomically (exactly-once), so branches cannot
  disagree on an id and no Reshuffle is needed; a Pub/Sub message id or a Kafka offset is a deterministic id.
- **The merge is a stateful merge DoFn, not a CoGroupByKey**: a streaming `CoGroupByKey(rowId)` needs
  windows / triggers, and a global window with count triggers never garbage-collects per-row-id trigger
  state. A stateful DoFn keyed by row id buffers base + partials and coalesces when B + 1 pieces are
  present (a standard streaming join). This is the one place the State API is required, consistent with
  the streaming keyed stages being stateful anyway.
- **The watermark hold is the crux**: on the first piece, set an event-time timer with
  `withOutputTimestamp(t)` so the output watermark is held at t; the branch DoFns hold t too, so all
  pieces arrive before the merge's input watermark passes t and the merged row (time t) is not late for
  the next wave's stateful stage, which then evaluates it in time order (strictly past). Forgetting the
  hold makes a late piece's timer fire immediately and mixes future rows into histories (the same trap as
  the late-data drop of §4.3). Merge state lives only for the watermark skew between branches (all
  branches wait for the same t): in-flight rows × B.
- **S3″ is more natural in streaming**: the next stateful stage already buffers rows until its timer,
  so coalescing by row id inside the buffer needs no sorter change. An explicit merge is needed only
  before stages without state (row / Finalize), as in batch.
- **Unchanged constraints**: population fit / expanding stay rejected in streaming, the single global
  key is a throughput ceiling (the periodic-snapshot side input is separate), runner constraints (Flink
  supports stateful + timers; Spark's support is limited).

Implementation shape: keyed stage = {batch: GBK + replay, streaming: stateful}, merge = {batch:
row-id GBK, streaming: stateful merge with timer hold}, switched by mode; the wave loop of
`FeatureStages.apply` and the compile-layer wave computation are shared.

### 9.5 Slow coarse-key stages on the DirectRunner — root cause

Running the module on the `direct` image (Cloud Run Jobs), a global-level population stage took 12–40
minutes where Dataflow took 8–11 s. A local reproduction (create source + a lattice with / without a
global level × N rows) shows ≈ 17 ms per row, linear in rows with a huge constant, and unaffected by
`enforceImmutability` / `enforceEncodability`.

**Root cause (from stack sampling)**: the DirectRunner's GroupByKey keeps per-key state in
`CopyOnAccessInMemoryStateInternals` and **clones a key's whole accumulated bag with the coder every
time a bundle touches it** (`CoderUtils.clone ← InMemoryBag.copy ← CopyOnBindBinderFactory.bindBag ←
ReduceFnContextFactory`). Total cloning ≈ (bundles reaching the GBK) × (rows of the key): harmless for
fine keys, catastrophic for a global key (thousands of upstream bundles × every row = hundreds of
millions of cloned rows per stage). It is the DirectRunner's rollback-capable state design, not something
the engine's GBK usage can avoid (stateful DoFns and `Combine.perKey` ride the same state).

**Consequences**: the DirectRunner is structurally unsuited to keyed stages over coarse keys — full and
subset measurements go to Dataflow, and the **prism** runner (Beam's portable local runner, which does
not use copy-on-access state) is the local / Cloud Run path: identical output to Dataflow and minutes
instead of hours on subsets, but in-memory without spill, so container memory grows roughly linearly with
the input and the full reference plan does not fit on a single 32 GiB node. Tiering: Cloud Run × prism =
subset verification and reproduction; full generation = Dataflow with fixed workers and waves. The
prism image needed three launch fixes (baked-in credential env var removed, `waitUntilFinish` for
non-blocking runners with a non-zero exit on failure, the `jamm` runtime dependency of the portable
harness), a bundled prism binary in the image, and `prism/cloudRunJob` launch targets; all recorded in
`options/prism.md` and the deploy docs. Two upstream Beam issues were noted (the `PrismLocator` OS-name
URL and a local zip not being unpacked).

### 9.6 One summary, four places: the abstractions behind the statistics

The requests that followed the first production uses — walk-forward fits for every fitted type, rolling
two-series statistics, distribution-shape summaries, array inputs, similarity-weighted histories, cheaper fit
stages — looked like forty separate features. They are mostly one question asked in different places: *which
statistics can be maintained as a state, and what can that state do?* This section records the abstractions the
engine settled on, so that a new statistic is a registration rather than a new code path. §4.3 / §4.5 describe
the pieces where they are used; this is the map.

#### 9.6.1 `Summary<S>` — the typed accumulator

A `Summary` summarises a stream of *contributions* (what one event adds: a value, a pair, a vector): `create`,
`update(state, contribution, ±1)`, `merge`, `read(state, Readout)`, `count`, and one declared property,
`invertible()`. The evaluator owns the *extraction* (which value of a row contributes, the null convention of
its scope); the family owns the arithmetic. Two algebraic facts decide everything else:

| property | meaning | what it buys |
|---|---|---|
| monoid (every family) | `merge` is associative, `create()` is the identity | states of disjoint row sets combine: a Beam `Combine`, time blocks, partitions, folds |
| group (`invertible()`) | a contribution can be removed again | a `maxAge` window evicts in O(1) per row; a block range can be a prefix difference (the encoding levels' `ForwardBlocks.Series`) |

The same family is therefore usable in four places, and a statistic is implemented once:

1. **the keyed replay's incremental path** (§4.3): fold a contribution in when its row becomes visible, remove it
   when it leaves the window;
2. **the fit stage's per-block `Combine`** (§9.6.2, §9.6.3);
3. **a prefix-scan over a hot key** (§9.4.4, not implemented): the "mergeable state" that decomposition needs is
   exactly a monoid, so the catalog already says which columns qualify;
4. **the state of a streaming keyed stage** (§9.4.6, not implemented): state = `S`, eviction = `invertible()`.

Families (`Summary.Summaries`, plus the two that live with their model class):

| family | state | invertible | readouts | used by |
|---|---|---|---|---|
| `MOMENTS` | n, Σx, Σx² | yes | count / sum / mean / avg / rate / std | sequence `aggregate`, every encoding stat derived from (n, Σy, Σy²) |
| `SHAPE` | n, Σx..Σx⁴ about an anchor | yes | skew / kurt | sequence `aggregate` |
| `EXTREMA` | max, min | **no** | max / min | sequence `aggregate` (scan under `maxAge`) |
| `COUNTS` | value → count | yes | distribution | encoding `distribution` |
| `ORDER` | Fenwick multiset (`OrderStatistics`) | yes | quantile(p) | encoding quantile stats |
| `REGRESSION` | n, Σx, Σy, Σx², Σy², Σxy about an anchor | yes | cov / corr / beta / intercept / r2 | sequence `regression` |
| `Svd.SUMMARY` | n, Σx, Σxxᵀ about an anchor | **no** (the anchored sums are not subtracted vector by vector) | — (solved, not read) | `type: svd` fits |
| `QuantileTransform.VALUES` | the values themselves | **no** (merge = concatenation) | count | `type: quantileTransform` fits |

Conventions every family follows: population moments (the `std` convention) and null — never NaN — when a
readout is undefined (too few contributions, no spread); power sums of order ≥ 2 over values that may carry a
level (prices, epoch times) are taken **about an anchor** (the first contribution; `merge` re-anchors by the
binomial shift) because a central moment formed from raw sums cancels away; a state emptied by eviction resets
exactly, so rounding residue does not outlive a window.

**The path rule.** `OperatorCatalog.summary(stat)` maps a statistic token to `(family, Readout)` and is the only
place that decides what runs incrementally. `SequenceEvaluator.plan()` then reads:

```
incremental ⇔ the statistic has a family
            ∧ (no maxAge ∨ the family is invertible)
            ∧ no maxEvents                          (a count-bounded window is not a time-ordered fold / evict)
            ∧ (no filter ∨ the filter is `f = $self.f`)   (one state per filter value)
            ∧ the statistic is not self-dependent
scan        ⇔ otherwise — bounded by maxAge, or, without a filter, by maxEvents or the op's own tail (lag / trend /
              fracdiff = k, delta = k + 1); anything else — any filter without maxAge, `f = $self.f` included — keeps the
              key's whole history and is reported (sequence.window.unbounded)
```

Three kinds of statistic have **no family by construction**, and the reason is part of the design:

- **self-dependent** (`weightBy`): the weight reads the current row, a different number for every (row, event)
  pair — nothing folded once per event can serve it. Declared as `OperatorCatalog.summary(stat, weighted)`.
- **pairs of events** (the lagged `regression`, `lag: k`): the contribution belongs to two events; evicting the far
  edge would need the rows before it.
- **order-dependent readouts** (`SeriesStats`: zeroCross / peaks / acf / pacf / ar): as summaries they are *ordered*
  monoids — `merge` is a concatenation that carries boundary values, contributions must arrive in order (so they
  cannot be a Beam `CombineFn`), and removing the oldest element needs its *successors*, which
  `update(state, contribution, −1)` cannot express. They would scan under every rolling window anyway; an ordered
  family (an `ordered()` property and a successor-aware removal) only pays off for the streaming state and is to
  be designed with it.

All three fold the scan over the window; where a family exists for the same arithmetic (`regression`, `skew`), the
scan path folds that very family so both paths share one arithmetic and one null rule.

#### 9.6.2 `BlockSeries<S>` — a fit as a window over time blocks

Every non-expanding fit mode is a way of choosing *which time blocks a row may read*; the model is whatever the
merged state of those blocks solves to. `BlockSeries<S>` holds one `Summary` state per observed block
(`ForwardBlocks`: `blocks.bucket` | `blocks.size`) and merges a range on demand:

| fit | blocks a row reads | how |
|---|---|---|
| `static` | all (a single block, index 0) | the total |
| `forward` | `(−∞, usable]` — the complete blocks whose inputs are known at predictAt, the row's own block excluded | merge of the prefix |
| `forward` + `window` | `(usable − windowBlocks, usable]` | merge of the range |
| `minBlocks` / `minHistory` | — | fewer observed blocks at or before `usable` → the row reads null |
| `fold` by time | every block but `[b − purgeBlocks, b + purgeBlocks + embargoBlocks]` around the row's block `b` | the total minus the range |

Only the monoid law is used (a range is *merged*, not differenced), which is what lets a non-invertible family
— the gathered values of a quantile transform — walk forward exactly. A model that has to be *solved* from the
state (an eigendecomposition, quantile knots) is fitted once per **change point** — every observed block and,
under a window, the index at which a block leaves (`changePoints`) — and a row reads the floor entry of its
usable block (`lookup`), the rule `JointFit` already used for its per-block solutions. The artifact keeps the
whole-input model (what a static serving run loads); a forward fit is re-fitted every run.

`type: svd` and `type: quantileTransform` are on it. The encoding levels still carry their own
`ForwardBlocks.Series` (prefix arrays of `KeyStats` + the per-block λ); moving them onto
`BlockSeries<Moments>` is the remaining step, after which a warm start is "merge the new block into the stored parts".
A **time fold** (`fit.mode: fold` + `fold.by: time`) already reads those series: `TimeFold.of` reads the fold
coordinates (`foldBy`, `purgeBlocks`, `embargoBlocks` — the compiler's `timeFoldCoordinates`, with the purge defaulting
to the horizon of a `direction: future` column the target reads, `labelHorizon`) into `FitLevel.timeFold` — a record of
its own, apart from `Forward`, which keeps only the row-relative geometry of a forward level (usable block, lag, window,
`minBlocks`). The level's series is fitted with the forward levels' (one `_Forward` Combine, one series side input),
and `FitApplyDoFn.timeFoldStats` returns the totals minus one prefix difference — the encoding levels' series are
invertible, so the range is differenced. λ is the whole input's, as for a hash fold: `lambdasFromKeyStats` over the
time-fold levels' totals (`_TimeFoldTotals` → `_TimeFoldVc`, a map side input merged into the evaluator's λ with the
static ones), never the per-block step function of `lambdasByBlockView`, which only the forward levels enter
(`_ForwardOnly` splits the series when both kinds share a fit stage). Its value is the last step of that function
(`VarianceComponentsTest.testWholeInputLambdaIsLastStep`). A time-fold artifact is written like a hash fold's: the
totals, no `lambdasByBlock` in the manifest. The purge is two-sided (a label window overlaps its neighbours in both directions) and the embargo
extends the range after it. Only the engine knows the input's block span (the first / last block over the level's
series), so the leave-out-more-than-half check is a run-time one: `auditTimeFold` counts the rows in
`feature/timeFold_<level>_excludedOverHalf` and logs one warning per level and DoFn instance.
`estimator: joint` keeps hash folds only (`fit.fold.time.joint`).

#### 9.6.3 `SummaryFitBlock` — what a fitted block declares

A `StaticFitBlock` whose fit state is a `Summary` family declares only `contribution(row)` → (time block, value)
and `solve(parts, planHash)`. The stage fits all such blocks together (`fitSummaryBlocks`): per family one
extraction pass over the rows, **one** `Combine.perKey` keyed by (block, time block), a regrouping by block, one
solve per block (in parallel across blocks), and one list side input of (block, model) for all of them, indexed
once per `FitApplyDoFn` instance. The step count of a fit stage no longer grows with the number of blocks (eight
blocks: 40 → 14 top-level transforms; the Dataflow wall-clock on the consumer's 13-block config is still to be
recorded in §9.2). A block that fits an empty input too (quantileTransform: n = 0 is still an artifact) adds an
empty marker part so its group exists. fm, discretize and the joint estimator keep their own chains: fm and joint
have no summary state, discretize gathers the same values as the quantile transform and can join its family.

#### 9.6.4 Vector operators and their three supplies

A scan readout is a pure function over a `double[]`. Three places produce such a vector, and they are meant to
share one operator set rather than grow three:

| supply | producer | positions | status |
|---|---|---|---|
| a row's array field (`array<float64>`) | `RowEvaluator`, `type: vector` | index (or `unit`: index / (n − 1)) | implemented — `VectorOps`: slice → diff → normalize, then readouts incl. `polyfit` |
| a sequence window's values in time order | `SequenceEvaluator`, the scan path | event order | implemented for `SeriesStats` (acf / pacf / ar / zeroCross / peaks), `trend`, `fracdiff` |
| a context group's values | `ContextEvaluator.evaluateColumn` (`values`, `self`, `excludeSelf`) | position in the group | the existing ops (rank / zscore / …) already have this shape; group solvers (neutralisation residuals, within-group probability solvers) are the planned users |

A vector *output* is always expanded into scalar columns (`<name>_<readout>`, `<name>_poly<k>`, svd `<name>_<k>`
/ `<name>_resid_<input>`): Avro round-trips `array<double>` at float precision on this classpath, and the
consumers are tabular models. `VectorOps` and `SeriesStats` are separate classes today only because they arrived
on independent branches; they are one catalogue.

#### 9.6.5 The row-granularity principle (what stays outside)

The transform maps **one input row to one output row plus columns**, and the availability algebra stands on
that. A request that changes the row set belongs upstream:

| request | why outside | where |
|---|---|---|
| bars from ticks, volume / dollar clocks that *emit* rows | the row set changes | an upstream resampling step (`aggregation`, or a dedicated transform) |
| pairwise rows ((event, a) → (event, a, b)) and their reduction | row expansion and its inverse | upstream expansion → feature (entity = pair) → a second feature step (context reduce) |
| as-of joins across sources | a join, and the DSL (spec §2.7) gives joins to the enrichment layer | the `query` transform's lookups |

#### 9.6.6 Dynamics — summaries over a path (`lti` implemented)

The Summarize stage of spec §1.4 as a `Summary` family, `Dynamics`, parameterised per column (measure, order,
halflife, period, clock) rather than a singleton. An event is an impulse `x` at a clock position; component j of the
state is `Σ x_i K_j(age_i)` and a readout divides by the measure's mass `Σ w_i` over the present values, so every
component is a weighted mean:

| measure | kernel `K_j(a)` | propagation by Δ | algebra |
|---|---|---|---|
| `exponential` | `e^(−θa) L_j(θa)`, θ = ln 2 / halflife (HiPPO-LagT) | `Φ(Δ) = e^(−θΔ) T(θΔ)`, `T` lower-triangular Toeplitz of `φ_m = L_m − L_{m−1}` (= `e^(−uN)`, N strictly-lower all-ones) — exact for any spacing | group |
| `fourier` | `e^(−θa) (1, cos kωa, sin kωa)`, ω = 2π / period, θ = 0 without halflife | a rotation per harmonic × the decay | group |
| `legendre` | `P_j(2u − 1)`, u = position over the window's span (HiPPO-LegS, uniform) | none: power sums `Σ x s^k` about the first event, re-expressed through the shifted-Legendre coefficients at read | monoid |

- **Read position.** The events clock reads at the newest event (age 0). On the time clock fourier reads at the
  current row's time — the state (kept at its newest event) is rotated by Δ without the decay factor, which cancels
  in the ratio, so the newest event never underflows — and legendre measures its span to the row; both stay bounded.
  Exponential reads at the newest event on the time clock too: moved by `T(θΔ)`, the weighted mean of `L_j` over
  events all at least Δ old grows like `(θΔ)^j / j!` with the entity's inactivity (order 16 reaches ~1e20 after
  half a year at a 7-day halflife), so the gap is left to its own feature (`sinceEvent`); order 0 — `ewma` — is
  independent of the read position either way.
- **The time channel** (`timeAugment`) reads no field, so on its own its window would never be shifted. It
  describes the events the value channels see, so the compiler classifies it with the latest availability among
  the block's channels (`classifyPast`'s `alignWith`: aligned, not a past input — no lineage, no projection).
- **Channel names.** A `lift.exprs` entry `{expr, as}` names its channel segment; an unnamed one keeps the
  anonymous `{block}__e{n}` (a spec-wide counter — `sequence.lift.anonymous`), and two channels of one block with
  one name are `sequence.lift.name`.
- **Eviction** subtracts `x K(age)`; the state resets exactly when its window holds no present value. **No periodic
  re-fold** (the earlier design note): `Φ` is contractive (exponential) or a rotation (fourier), so the rounding an
  eviction leaves decays with the state or stays at the scale of the values folded in — `DynamicsTest` runs 20 000
  events through a sliding window against the direct projection at 1e-8. The Legendre power sums would need a
  re-anchoring once the window slides far from its first event, so the family declares itself a monoid instead and
  a `maxAge` window re-reads — the path rule (§9.6.1) does the rest.
- **Merge** (the fit-stage / prefix-scan use): the older state is moved to the newer's position and added; on the
  events clock `other` holds the later events (the order of blocks and partitions).
- **Limits.** Order ≤ 16 (exponential / fourier) and ≤ 8 (legendre: the monomial readout loses digits like its
  coefficients `C(j,k) C(j+k,k)` grow); a block emits at most 64 component columns (`sequence.dynamics.size`).
- **Sugar.** `ewma` columns carry `measure: exponential, order: 0, component: 0`; the old formula
  (`0.5^(steps / halflife)` from the current row) agrees to 1e-12 (`DynamicsTest.testEwmaMatchesTheFormerFormula`).
  A NaN / ±∞ value is now missing for `ewma` as for every aggregate. `trend` stays a scan over its last k events but
  folds them into the `REGRESSION` family (the beta of the values on their order) — one arithmetic with the
  `regression` op, equal to the former slope (`SequenceIncrementalTest.testTrendIsTheRegressionBeta`).
- **`bilinear` — log-signatures** (`Signature`, a `Summary`): the path through the lifted channels (every channel
  present; `timeAugment` appends the event's clock position) in the truncated tensor algebra, `S = exp(Δ₁) ⊗ exp(Δ₂) ⊗
  …`, read as the coefficients of `log S` at the Lyndon words (a basis of the free Lie algebra; the coordinate map is
  triangular, so the columns determine the log-signature). One state per window for all channels, a column per word.
  Chen's identity is the monoid: `merge` joins two paths by the increment between them, `S(X) ⊗ exp(y₀ − x_n) ⊗
  S(Y)`; the reversed path is the inverse (`SignatureTest`). Evicting the oldest point would remove the increment to
  the *next* point, which a per-event contribution cannot carry, so the family is not invertible: an unbounded window
  folds once per event, a bounded one re-reads (the legendre rule). Depth ≤ 4, at most 64 columns per block.
- **`compress: {svd}`**: the compiler expands a population svd block `{block}_svd` over the block's component columns
  (every window; `expandCompress` → `expandSvd` with a synthetic definition) and marks the components intermediate
  unless `keep: true` — the "Compress" stage is an ordinary svd fit, static or forward.

#### 9.6.7 Clock — windows, decay and blocks on a calendar

A calendar clock (`Clock`: a name and its tick dates as sorted epoch days, parsed from the sources document's
`clocks:`; a `uri` is read by `FeaturePlanService.resolveClocks` before compile, so the dates are in the plan hash)
answers one question — the **position** of an instant, the ordinal of the last tick on or before its UTC date — and
everything else is built on it:

| use | wall time | calendar |
|---|---|---|
| window far edge | `now − maxAge` | the start of the tick `ordinal(now) − maxAgeTicks` (`Clock.farEdgeMillis`): a millisecond bound again, monotone in `now`, so the scan's binary search, the incremental evict pointer and the trim watermark (`SequenceEvaluator.farEdge`) are unchanged |
| decay / dynamics age | `(a − b) / day` | `ordinal(a) − ordinal(b)` (`Dynamics.distance`) |
| fit block | `floor(millis / size)` or a calendar bucket | `floor(ordinal / ticks)` (`ForwardBlocks.ofClock`); rounding a duration (`fit.window`, `minHistory`, `purge`) to blocks uses the mean tick spacing |

The calendar is the one piece of the engine contract that is data rather than a string: the coordinates name the clock
(`windowClock` + `maxAgeTicks`, `decayBy`, `blockClock` + `blockTicks`) and the `Clock` instance rides with the column
(`OutputColumn.clocks`, one shared instance per clock, so a serialized stage carries each calendar once);
`Forward.of(column)` / `Dynamics.spec(coordinates, clocks)` / `SequenceEvaluator.plan` read it there. Availability never
uses a clock (the window shift stays in millis). A future window stays on wall time (`clock.direction`: its mirrored
replay would need the mirrored calendar), and a keySet window on a calendar under `fit.mode: forward` needs blocks on
the same clock (`clock.fit`), where it is counted in blocks.

#### 9.6.8 Planned on the same line (design positions, not implemented)

- **Ratings**: a sequence op under the global key whose state is a map entity → (μ, σ), updated when a group of
  same-timestamp rows closes (the `pending` flush). Order-dependent, hence replay-only — declared non-mergeable.
- **Sketches** (KLL / t-digest) as a monoid family: per-key quantile / distribution stats under static / fold and
  drift audits (PSI) against the fitted summaries. The quantile transform keeps its exact `VALUES` state
  (decision 11).

---

## 10. Decisions and open questions

**Decisions**

1. Leak checks, expansion and lineage are isolated in the compile layer (pure functions) shared by the
   module, the server APIs and external tooling.
2. Stages form an append-only linear chain; parallelism comes from waves over that chain (§9.4), not
   from a general join graph.
3. Expanding encodings map onto the time-ordered keyed replay (no artifact needed for backfill).
4. Lattice shrinkage decomposes into per-level stages + row-local top-down composition.
5. Keyed stages are GBK + in-DoFn replay with an owned spill sorter; no Beam State API in batch;
   same-timestamp rows are mutually excluded by construction, so `orderTieBreak` is a declaration check.
6. v0 is batch-first; streaming supports row / context only, population fits are rejected explicitly.
7. The plan hash excludes artifact locations and `engine` knobs; artifacts are content-addressed.
8. `feature.Durations` and `outbound.Durations` stay separate (different grammars).
9. The DirectRunner is not a benchmark target for keyed stages; prism is the local tier.
10. A statistic is a `Summary` family plus a catalog line; its algebra (monoid / group), not its call site,
    decides where it runs — incremental replay, per-block Combine, and later prefix-scan and streaming state
    (§9.6.1). Statistics without a family say why (self-dependent, pairs of events, order-dependent).
11. Every non-expanding fit is a range of time blocks merged by the monoid law, solved per change point
    (§9.6.2); exact states (the values of a quantile transform) are preferred over sketches for fitted transforms,
    sketches are reserved for per-key statistics where the state must stay bounded.
12. One input row in, one output row out: requests that change the row set (resampling, pairwise expansion,
    as-of joins) are upstream steps, not feature scopes (§9.6.5).
13. Vector outputs are expanded into scalar columns; parameter names avoid the YAML 1.1 booleans
    (`on` / `off` / `yes` / `no`) even though configs are parsed as YAML 1.2, because specs travel through other tools.

**Open questions**

- Expression typing: the Lucene engine is double-only. Admitting the spec's general expressions
  (string comparisons, `nullif`) means delegating to the per-element Calcite engine (`Query2`) or
  extending the expression engine; v0 stays numeric.
- `varianceComponents` as a whole-batch estimate (the structural-leak class) vs. `fixed` weights only.
- Pending-contribution state for very long settlement lags on high-frequency keys (a `maxPending`
  guard?) — moot for the batch replay, relevant for the streaming stateful stage.
- Reusing one sources document across several pipelines: whether a `version:` identifier check is
  needed before full dependency hashing (v1).
- Whether the sources document should carry physical table references so the audit queries (spec §7)
  can be generated fully instead of with a `{input}` placeholder.
