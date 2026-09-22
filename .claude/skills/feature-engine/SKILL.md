---
name: feature-engine
description: Developing and maintaining the feature transform (util/pipeline/feature + module/transform/FeatureTransform) — the declarative feature-engineering DSL with availability-time leak checking, its pure compile layer (FeaturePlanCompiler / OperatorCatalog / FeaturePlan) and its Beam engine (FeatureStages — keyed replay, waves, static fits, KeyedSpillSorter). Use when adding or changing a row / context / sequence op, an encoding stat, a population type (encoding, factorization, discretize, quantileTransform, svd, smooth, transitionStats, spectralEmbedding, key-set structures flat / hierarchy / cross / sequence, and the backlog nested encoding), the shrinkage estimators (backoff / sequential / joint) and families, touching the stage scheduler, waves, the fan-out merge, FitApplyDoFn / artifacts, spill / history trimming, or the plan report (describe / toJson / audit); when a diagnostic code (encoding.globalKey, sequence.window.unbounded, population.unsupported, encoding.stat.static, input.reserved, availability.violation, reference.unresolved ...) or an engine message ("keyed spill sorter", "Fan-out merge", "RowId_Pin", "Wave1_Merge", "fit.mode static ... requires an existing artifact", "feature stage scheduling") needs explaining; or when measuring a feature-engine change on Dataflow / prism.
---

# Feature transform engine

The `feature` transform compiles a YAML/JSON feature spec against a *sources contract* into a
`FeaturePlan` (every output column with its availability time, status, lineage and stage) and
executes that plan as a chain of Beam stages. The DSL spec is `docs/design/feature-dsl.md`
(canonical: sources contract, scopes, encoding, availability algebra, §-numbers cited from the
code) and the engine design is `docs/design/feature-engine.md` (§3.1 scheduling, §9.2
implementation status and deferred items, §9.3 spill sorter, §9.4 waves, §9.5 DirectRunner
finding). This skill is the map plus the invariants and gotchas that are only recorded there or in
past PR reviews. User-facing reference: `src/main/resources/server/docs/module/transform/feature.md`.

Recipes: [add-operator.md](add-operator.md) (adding ops / stats / population types, with PR #100
as the worked example) and [testing.md](testing.md) (test conventions, the parallel-vs-linear
equality harness, and the production measurement loop).

## Architecture map

Two layers, deliberately separated (engine doc §1.2): a **pure compile layer** shared by the
module, the REST API, MCP and the Pipeline Builder agent, and a **Beam execution layer** that only
reads what the compile layer wrote into each column's `coordinates`.

### Entry points

- `module/transform/FeatureTransform.java` (small): `FeaturePlanService.resolve` (sources /
  features documents from inline, URI, path, `data:`; FreeMarker with the step args) →
  `FeaturePlanCompiler.compile(sources, parameters, inputSchema.getFields())` →
  errors = `plan.getDiagnostics().getErrorMessages()` + `FeatureStages.engineConstraints(plan, streaming)`
  (both fail assembly with `IllegalModuleException`) → `LOG.info(plan.describe())` — or, under
  `--dryRun=true`, printed to stdout *instead* (once: the log shares the console) → `FeatureStages.createOutputSchema` → `Union.flatten` → `FeatureStages.apply`.
- `util/pipeline/feature/FeaturePlanService.java`: the one shared entry (`resolve` / `compile` /
  `validate(rawRequest)`). REST `POST /api/feature` (`server/api/FeatureService`), MCP
  `validate-feature` (`server/mcp/tool/ValidateFeatureTool`), agent `validateFeature`
  (`server/agent/tool/FeatureValidator`) and the `run-pipeline dryRun` response's `featurePlans`
  all go through it. Server-side tests need `-Pserver` (plain `mvn test` skips `server/**`).

### Compile layer (`util/pipeline/feature/`, no Beam imports)

- `SourceContract` — the sources document: per-source `eventTime`, `availability`, `settlementLag`,
  `ingestionLag`, `mutability`, `snapshotOf`, per-field `availableAt` / `observedAtField` / `kind`
  (attribute / market / outcome) / `validFor`. `SourceContract.Json` is the shared lenient JSON accessor.
- `Clock` — a calendar declared in the sources document's `clocks:` (tick dates; a `uri` is read by
  `FeaturePlanService.resolveClocks`, so the dates are in the plan hash): `ordinal(millis)`, `farEdgeMillis(now,
  ticks)` (a window's far edge as millis — the scan / evict / trim machinery is unchanged), `distance` (decay ages
  in ticks). Coordinates name the clock (`windowClock` + `maxAgeTicks`, `decayBy`, `blockClock` + `blockTicks`); the
  instance rides with the column (`OutputColumn.getClocks()` — the one non-string part of the engine contract, shared per
  clock) and is read by `SequenceEvaluator.plan`, `Dynamics.spec(coordinates, clocks)` and `Forward.of(column)`
  (`ForwardBlocks.ofClock`). Availability never uses a clock; future windows stay on wall time (engine doc §9.6.7).
- `FeatureSpec` — the parsed spec (`FeatureDef`, `Op`, `Window`, `KeySet`, `Target`, `FitSpec`,
  `OutputSpec`, `EngineSpec`). It rides inside DoFns, so it holds **no Gson objects**: nested
  blocks are kept as JSON strings (`fitJson`, `shrinkageJson`, `hierarchyJson`) and re-parsed by the
  compiler. New parameters are plain public fields + one line in the parse method.
- `AvailableAt` — the availability algebra: `atEventTime()` is a *pre-event sentinel* (anything
  before the event), `eventRelative(offset)` a static offset, `dynamic(reason)` non-static
  (`atRowCreation`, `event_date THH:MM` → status `runtimeFilter`). `max`, `plus(lag)`,
  `isStaticallyAtOrBefore`, `isProvablyAfter`. `effectiveAvailableAt = availableAt + ingestionLag`
  (relative to availableAt, not to event time; not added to pre-event).
- `OperatorCatalog` — **single source of truth** for what the DSL accepts: `register(scope, name,
  InputKind, outputType|null=same as input, fit, description)`, `stat(name)` for encoding stats
  (`Stat.sufficient` = derivable from (n, Σy, Σy²) = allowed in static / fold), `quantileProbability`,
  `aggregateOutput(func)`, `IMPLEMENTED_POPULATION_TYPES` (the others parse but fail with
  `population.unsupported`), `isNumeric` / `isCategorical`.
- `FeaturePlanCompiler` — `run()` = `resolveLineage` → `resolveDefinitions` (entities / contexts /
  baselines) → `expandAll` (an **assembly loop** like `MPipeline`: a block expands once every
  reference it makes resolves to an input field or an expanded column; leftovers are reported once
  as `reference.unresolved` / `reference.cycle`; config order is irrelevant) → `finalizeColumns`
  (intermediate / exclude / violation classification, `_` lint) → `buildSchema` → `buildStages`
  (`StageScheduler`) → `hintGlobalKeyStages`. Per scope: `expandRow`, `expandContext`,
  `expandSequence` (+ `desugarExpression`, `reducibleFilterField`, `classifyPast`),
  `expandPopulation` → `expandEncoding` (lattice levels, `populationColumn`, `composeCoordinates`,
  `levelStats`) / `expandFactorization` / `expandDiscretize` (+ `parseStaticOnlyFit`,
  `finishStaticFitted`). `hash()` = SHA-256 of the canonical (key-sorted) sources + parameters
  **minus `engine` and every `fit.artifact`** (`withoutArtifact`) — the plan hash names the artifact
  directory, so it must change when the fitted thing changes and must not change when only where it
  is stored or how it is executed changes.
- `OutputColumn` — one expanded column. `canonicalName` (what other blocks reference) vs
  `outputName` (`_` for intermediates + `output.prefix` + canonical); `block` / `scope` / `operator`
  / `fieldType`; **`coordinates` (a `Map<String,String>`) is the whole contract with the engine** —
  the evaluators rebuild their plans from it (`SequenceEvaluator.plan`, the static-fit specs enumerated by
  `staticFitBlocks` — `fmSpecs` / `discretizeSpecs` / `quantileTransformSpecs` / `svdSpecs` / `smoothSpecs` / `spectralSpecs` / `jointSpecs` — and
  `fitLevels`) and it is exported as `feature.coord.*` schema options;
  `inputs` (read from the row itself) vs `pastInputs` (read from past rows — what the keyed stage
  projects into the history); `availableAt` / `computeAt` / `status` (`staticSafe` /
  `windowShift` / `runtimeFilter` / `violation`) / `windowShift`; `intermediate` (not emitted),
  `anonymous` (desugared expr), `fitted`, `placement` (child / parent under `output.groupBy`).
- `Diagnostics` — `error` (fails the compile) / `warning` / `hint` / `info`, each with a **code**
  (`scope.field.reason`: `encoding.stat.static`, `discretize.bins`, `sequence.window.unbounded`),
  a location (`features.<block>`) and a message. Tests assert codes (`hasCode`), docs cite them, the
  agent loop reads them — keep codes stable, and say what is available in the message
  (`"(available: " + OperatorCatalog.AVAILABLE_STATS + ")"`). One hint per block for repetitive
  advice (`hintedBlocks`); secondary failures are demoted to caused-by info.
- Output contract + audit (PR after #102): `OutputSpec.roles / include / includeSource / includeHash /
  manifest` and `AuditSpec.observedAt` in `FeatureSpec`; `FeaturePlanService.resolveInclude` reads an
  include URI before compile (list + content hash); the compiler validates roles (`resolveRoles`), applies
  `include` as the projection in `finalizeColumns` (`applyInclude`, replaces `exclude`; roles are resolved
  once before it — `resolveRoleColumns` stamps `OutputColumn.role`, which the projection, `exclude`,
  `toOptions`, the manifest and `FeaturePlan.getRoleColumns` all read — so a role column survives either
  projection, `output.include.role` / `output.exclude.role`, and gets no `_isnull` indicator when kept only
  as a role) and builds one
  `FeaturePlan.ObservedAtAudit` per input field with an `observedAtField` (`resolveObservedAtAudits`;
  `present` = the observation column is in the input schema). `FeaturePlan.toManifest` is the manifest the
  transform writes at assembly; `FeatureStages.artifactPaths` fills its `artifacts`.
- `FeaturePlan` — the result: columns, `Stage` records (`index`, `kind`, `keys`, `blocks`,
  `columnNames`, `dependsOn`; predicates `isKeyed` / `isReplay` / `runsUnderSingleKey`),
  `getShuffleCount` (linear), `getWaves` / `getDagShuffleEstimate` (DAG), the **engine geometry**
  (`getEngineWaves`, `getPreludeColumns`, `getWaveInputFields`, `keysAvailable`, `getFoldTarget`)
  that `FeatureStages` and the estimate both read, `getAuditQueries` (hot-key SQL), `describe()`
  (the text report: header `columns=a/b stages=n shuffles=n waves=d (dag shuffles~n)`, `-- stages`,
  `-- columns`, `-- audit`, `-- diagnostics`) and `toJson()`.
- Pure models used by both layers: `Shrinkage` (lattice parse + row-local top-down composition,
  `lambdaFromMoments`, `Family` — derived from the stat, a check plus the Dirichlet-Multinomial
  `composeDistribution` for a shrunk `distribution`; scalar families share the arithmetic and the λ
  estimator, see the class javadoc), `JointFit` (`estimator: joint`: cell table → ridge / BLUP by block
  Gauss–Seidel swept coarse → fine, fold / forward variants, `<id>.joint.avro`),
  `Discretization` (quantile edges + `<block>.bins.json`), `QuantileTransform` (CDF knots
  + probit with the `clip` probability clamp, `<block>.quantiles.json`), `Svd` (`Moments` (n, Σx, Σxxᵀ) → the leading eigenpairs, `<block>.svd.json`), `SymmetricEigen` (the leading `k` eigenpairs of a dense symmetric matrix, by magnitude or — PSD — by value: `Svd.jacobi` up to 128 rows, so every earlier artifact is reproduced bit for bit, and ojalgo's symmetric decomposition beyond — 0.15 s at 700 rows against the sweep's 7–15 s. A hand-rolled restarted block Krylov iteration with a warm start from the previous change point was built, measured against it on 300–1024-row matrices, and removed: it won only on fast-decaying spectra (0.16 s vs 0.18 s) and lost 2–8x on the slowly decaying ones co-occurrence data has. Do not reintroduce an iterative path without that measurement), `Smooth` (`type: smooth`: a P-spline of a
  target over a numeric key — uniform B-splines on a declared range, difference penalty, λ by REML — solved from the
  `Svd.Moments` of `[B(x), y]`, read through their centred form with the target centred; `<block>.smooth.json`; a
  *reused* family, engine doc §9.6.3), `Spectral` (`type: spectralEmbedding`: unordered pair counts of a value with its lag
  values — the `PairCounts` summary, sorted maps so nothing depends on arrival order — → PPMI → `Svd.jacobi` →
  coordinates `sqrt(|λ|) · v` by decreasing |λ|, vocabulary capped by co-occurrence mass; `<block>.spectral.json`), `Factorization`
  (fm / fwfm ALS + `<block>.fm.avro`), `OrderStatistics` (Fenwick-tree block multiset for
  quantiles with eviction), `FitArtifact` (`<uri>/<planHash>/<block>.avro` + manifest for encoding
  levels), `Durations` (ISO-8601 + calendar periods + column tokens; **kept separate** from
  `outbound.Durations` by decision), `FeatureValues` (value coercion, keys, `keyWithNullTokens`),
  `VectorOps` (pure `double[]` functions: `toVector` — shared with `SvdSpec`, a hole = no vector —,
  `slice` / `diff` / `normalize`, `read` / `polyfit`; the home of scan readouts over a vector, fed today by a
  row's array field and meant to be fed by a sequence window and a context group too, engine doc §9.6.4),
  `SeriesStats` (order-dependent readouts of a sequence window's values: zeroCross / peaks / acf / pacf /
  ar — tokens parsed once into the column plan; scan-only by construction, §9.6.1), `BlockSeries<S>` (one
  `Summary` state per time block, merged per readable range, one model per change point: what makes
  `static` / `forward` / `window` one implementation, §9.6.2), `Rating` (the sequence `rating` op: elo and the
  Weng–Lin `bradleyTerry` / `plackettLuce` updates over a pool of players — a running state that is *not* a
  `Summary`, because a contest reads the ratings the earlier ones left: no merge, no inverse. `fold(state, run)` takes
  the rows of ONE event time and splits them into contests by the context keys; `SequenceEvaluator.advanceRating`
  feeds it run by run from the fold pointer, `replay` is the scan reference. With `tauPer` the drift runs on the time since the
  player last competed (`Player.lastMillis` = the run event time) and `read(state, player, func, nowMillis)` adds the drift up to
  the row: the op has ONE row-time-dependent readout (`sigma`), so both paths must pass `nowMillis`. `Pairs` (`all` / `adjacent` /
  `mean`) is bradleyTerry only; `adjacent` is defined on outcomes, never on entry positions (ties stay order-free). Teams
  (`withTeam`; DSL `with:` / `team:` → coordinates `teamPool` / `teamMembers` and per column `readout` / `memberIndex`, validated in
  `validateRatingTeam` under the one code `sequence.rating.with`; `SequenceEvaluator.readRating` picks member / team. INVARIANT: the
  columns of one `stateKey` share ONE fold pointer, so they need one availability contract - the same self AND past inputs on every
  readout column, whatever it reads; a column classified apart (a violation has no window shift) would advance the shared state past
  its siblings' near edge, a silent leak. `SequenceEvaluator.checkSharedStates` refuses such a stage at setup): an `Entry` holds the state keys of its members (`teamOf(row)`, pool-prefixed), the rules
  run on the summed `(m, v)` unchanged and return `Ω` / raw `Δ`, and `update` shares them by `v_j / v` (clamp AFTER the share).
  A rating without a team must stay bit-identical — `testPlayerArithmeticIsUnchangedByTeams` compares it bit for bit with
  `PlayersOnly`, a FROZEN copy of the pre-team update inside `RatingTest` (an oracle in the same JVM, not recorded numbers:
  `Math.exp` is 1-ulp-specified, a constant would be platform-bound); if it fails, the change moved a player's numbers (sum
  start, share, clamp position, accumulation order). `withTeam` declares the whole team at once and rejects what would
  mis-key the state (no key fields, an empty / repeated call, a separator inside a pool); state keys are
  `FeatureValues.key` texts (`<len>:<value>\u0001` per component) — never build one by hand, use `memberKey` / `teamOf`. Its columns are *pooled*
  (`finishSequence(..., pooled = true)`: `stageKeys` = the reduced filter field alone, empty = global key; no
  `minInterval`), and the row order inside a timestamp never reaches the output: a contest is evaluated over
  entries sorted by player, and the contests of one event time are applied in context-key order. On the scan path
  (`forceScan` only — production runs the fold pointer) it reads every contest of its window from the start, so
  `unbounded()` calls it unbounded whatever `tailSize()` would say and the key's history stays pinned; a window that
  evicts is implemented on neither path — no inverse for the running state, and truncation would split a contest for
  `replay` — so `SequenceEvaluator.checkWindowContract` fails a rating column carrying `maxAge` / `maxEvents` / a
  filter at `setup()` rather than replaying it; the check is deliberately not in `plan()`, which the compile layer
  calls through `unboundedReason()`).

### Evaluators (`Serializable`, Beam-free, one instance per stage DoFn)

- `RowEvaluator.evaluateColumn` — `switch (c.operator)`: `expr` / `baseline` (Lucene expression
  engine, **doubles only**), `datetime`, `bin`, `cross`, `indicator`, `equals`, `residual`,
  `isnull`, `copy` (baselines[].emit), `noise` (murmur3 of seed + row identity → `SplittableRandom`),
  `vector` (readouts of a numeric array field: a `VectorOps.Plan` per column resolved from the coordinates in
  `setup()` — steps slice → diff → normalize, then a readout; the readout names live in
  `OperatorCatalog.VECTOR_FUNCS`, `polyfit` expands to one column per coefficient),
  and the hidden-level readers of a lattice: `share`, `fitStat`, `compose` (a scalar, or a map when the
  `family` coordinate is `dirichletMultinomial`; `targets[].values` turns that map into an intermediate read
  by one `mapValue` row column per listed category — `expandDistributionValues`), `deviation`, `effectiveN` (λ from `setLambdas`, the
  variance-components side input). `joint` columns are population-scope lookup columns filled by
  `JointSpec.apply` in the fit stage, not row columns.
- `ContextEvaluator.evaluateColumn` — one group at a time, driven by a per-column `Plan` built once in
  `setup()` (`plan(c)`: every coordinate parsed there, never per group — `against` / `discount` / `order`
  splits, `seed`, `top`, `maxGroupSize`); `apply(op, values, self, excludeSelf)`; group-constant ops are
  evaluated once per group; `values:` lists become per-value columns (`valueKey` normalises integral numbers).
  `softmax` and `shuffle` bypass `apply`: they read two per-row inputs / need the group order (`softmax` in
  probability space with a max-shift; `shuffle` = Fisher–Yates from (seed, group key) over rows sorted by
  `order` + `tieBreak` — the tie-break over all input fields is what makes it engine-mode independent). The
  group solvers `residualize` / `harville` go through `solve(name, plan, rows)` → `GroupOps` (pure
  `double[][] channels → double[]`, NaN = missing): several fields of the group at once, one value back per row.
  Work shared by several columns of one group is cached on the group's list identity (`harvillePlaces`: every
  place of a `top` is one pass — a new group is a new list, so the cache cannot outlive it).
  A new solver is a `GroupOps` function + a branch in `solve` + a record in `Plan`; it must take its sums in an
  order the *values* decide (`GroupOps.sort`, a primitive merge sort — no boxed index per row) — the rows of a
  group arrive in runner order, and the parallel / linear equality is compared bit for bit — and declare its
  cost (`context.op.groupSolver`, `maxGroupSize`) when it is more than linear in the group size. Prefer a
  closed form over a per-row refit: the leave-one-out residuals come from the one fit and its leverages
  (`e_i / (1 - h_i)`), not from m fits.
- `FeaturePlanCompiler.expandContext` — the op's own parameters are validated **once per op** in
  `validateContextOp` (which returns the `ContextOpParams` every column of the op shares: coordinates, extra
  inputs, an inherited `validFor`, the resolved regressors, the places), and the columns one field produces come
  from `contextVariants` (one per listed `value`, one per `top`, one otherwise). Put a new op's validation and
  fan-out there rather than in a branch of its own, or an op over three fields reports the same error three
  times and the rules of the shared path stop applying to it.
- `Summary<S>` — the typed, mergeable accumulator behind every statistic the engine serves without
  re-reading rows: `create` / `update(state, contribution, ±1)` / `merge` / `read(state, Readout)`,
  with `invertible()` saying whether a contribution can be removed again (a group: windows can evict)
  or only added (a monoid: extrema). Built-in families in `Summary.Summaries`: `MOMENTS` (n, Σ, Σ²:
  count / sum / mean / std), `EXTREMA` (max / min, not invertible), `COUNTS` (value → count:
  distribution), `ORDER` (`OrderStatistics`: quantiles), `SHAPE` (anchored power sums to order four: skew /
  kurt), `REGRESSION` (anchored cross moments of a pair
  `double[]{x, y}`: cov / corr / beta / intercept / r2 — the sequence `regression` op; its lagged form pairs two
  events and is therefore scan-only). `Dynamics` (engine doc §9.6.6) is the family of the sequence general form
  (`lift` + `summarize.dynamics`, `lti`: exponential = Laguerre, fourier, legendre) and of `ewma` (order-0
  exponential sugar): parameterised per column from the coordinates (`Dynamics.spec`), a vector state shared by a
  channel's component columns through the `stateKey` coordinate (= `ColumnPlan.stateKey`, the `KeyState` key), a
  contribution `Event(millis, value)` that is never null (a missing value advances the events clock), read with
  `Summary.readAt(state, readout, now)` (the only position-dependent read), and `project` as the direct
  projection the scan path and the tests use. exponential / fourier are groups, legendre a monoid (it rescales
  with its span: re-read under `maxAge`). `Signature` is the `bilinear` family (coordinate `family: bilinear`):
  the log-signature of the joint path through all channels in the truncated tensor algebra (`exp` / `multiply` /
  `inverse` / `log`, Lyndon-word readouts named by channel letters), contribution `Event(millis, values)` (null when
  a channel is missing), a monoid whose `merge` is Chen's identity — bounded windows re-read. `compress: {svd}` is
  compile-time only: `expandCompress` builds a synthetic svd block over the component columns. `trend` folds the
  `REGRESSION` family over its tail. `OperatorCatalog.summary(stat)` maps a
  statistic token to `(family, Readout)` and is **the** rule for what runs incrementally; the same
  families are the per-block Combine state of the fit stage (`Svd.SUMMARY`, `QuantileTransform.VALUES` —
  both monoids without inverse) and are meant to become the prefix-scan state and the streaming
  state, so a new statistic is one family + one catalog line (engine doc §9.6.1 has the family table, the
  path rule and the three kinds of statistic that have no family by construction; recipe G in
  [add-operator.md](add-operator.md)).
- `SequenceEvaluator` — the keyed replay logic. `ColumnPlan` from coordinates (shift, `maxAge`,
  `maxEvents`, filter → `EqualityFilter` when `f = $self.f`, `stat` token, `summary` spec, `empty`
  state); two paths per column: **incremental** (fold / evict pointers over the history + one
  `Summary` state per filter value; `contribution(plan, past)` extracts what a row contributes,
  `readStatistic` applies the scope's null / cast convention) when `summaryOf(c)` is non-null, no
  `maxEvents`, no general filter, and either no `maxAge` or the family is invertible; else **scan**
  (`select` = binary-searched sublist view, `evaluateScan` switch). An aggregate with `weightBy` (a
  numeric expression over the event and, through `$self.f` → `__self_f`, the current row) is scan-only by
  declaration — `OperatorCatalog.summary(stat, weighted)` returns no family, because a self-dependent
  weight differs per (row, event) pair — and runs `weightedAggregate` (`Weight` = the compiled expression
  + its variable split, built once in `setup()`). `History` (absolute indices,
  trimmable prefix), `Watermarks` (per-field trim floors), `retainInto` / `tailSize` /
  `unboundedColumns` / `unboundedReason` (the compile-time twin used by the
  `sequence.window.unbounded` hint). `bufferedFields()` = union of `pastInputs` = what the stage
  projects per past row.
- `PopulationEvaluator extends SequenceEvaluator` — encoding statistics: overrides `statToken` /
  `summaryOf` (encoding stats through the catalog, plus the hidden `sumoff` on the moments family),
  `contribution` (target minus baseline offset — the baseline itself for `sumoff` — a bare 0 for the
  target-less row counts and for the counted rows, the category of a distribution; the rows a level
  counts and sums are the one rule `FeatureValues.offsetTarget`), `readStatistic` (a hidden `sum` /
  `sumoff` reads 0 when empty) and the scan path; `isSupported(stat)` = "has a summary family" and is
  what `engineConstraints` checks; NaN counts as missing.
- `VarianceComponents` — per-level (n, Σy, Σy²) per key as a Beam `Combine`, λ = σ²/τ² by the method
  of moments (side input `Map<levelNColumn, λ>`), fold-tagged entries for `fit.mode: fold`,
  `lambdasInMemory` for loaded artifacts; `forwardSeries` (a `View.asList` the apply DoFn indexes once
  per instance) / `lambdasByBlockView` (λ per (level, block) as one `Combine.perKey` per level —
  `BlockMomentsFn`, the level-wide moments as a step function over blocks — gathered into a small list
  view; `lambdasByBlock(Map)` is the in-memory reference the test compares it with) for
  `fit.mode: forward` (`ForwardBlocks` = block arithmetic + the cumulative `Series`; coordinates
  `blockBucket` | `blockSizeMillis`, `minBlocks`, `forwardLagMillis` (target availability delay), `windowBlocks`,
  `blockField` / `blockFieldType` written by `FeaturePlanCompiler.forwardCoordinates`; the engine side is
  `FitLevel.forward` + `FitApplyDoFn.forwardStats`, which also swaps the row's per-block λ into the evaluator).
  A time fold (`fit.mode: fold` + `fold.by: time`) rides the same series but has its own record: `TimeFold.of` reads
  the coordinates `foldBy` / `purgeBlocks` / `embargoBlocks` (`FeaturePlanCompiler.timeFoldCoordinates`, purge
  defaulting to the target label's horizon via `labelHorizon`) into `FitLevel.timeFold` (`isTimeFold()`; `Forward` /
  `isForward()` are forward-only), and `FitApplyDoFn.timeFoldStats` reads totals minus the blocks
  `[b − purge, b + purge + embargo]` (two-sided purge) with the whole-input λ (`_TimeFoldTotals` → `_TimeFoldVc`, not
  the per-block `_ForwardVc`; `_ForwardOnly` splits the series when both kinds share a stage); `auditTimeFold` counts rows leaving
  out more than half of the input's blocks (`feature/timeFold_<level>_excludedOverHalf`, run-time only).

### Beam engine (`FeatureStages`)

`apply(input, inputSchema, plan, outputSchema, loggings, failFast)`:

1. `ToElementDoFn`: every row → `DataType.ELEMENT` map keyed by canonical names, **re-timestamped
   from `time.field`**; also runs the observedAt audit (`plan.getRunnableObservedAtAudits()`: counters
   `feature/observedAt_<field>_*`, `audit.observedAt: fail` throws into the failure path, and with a
   manifest URI in batch a `KV<String, Double>` side output of `predictAt − observedAt` samples).
   The run manifest (`writeRunManifest`: `ApproximateQuantiles.perKey(11)` + `Count.perKey` in the
   global window + the finalize DoFns' `#rows` count side output → `View.asList` →
   `WriteRunManifestDoFn`) is written next to the manifest; never consume the output PCollection inside
   the engine — the module calls `setCoder` on it once more, which fails after a use. **Re-timestamped
   from `time.field`** (null → failure output); assigns `__rowId` (declared `engine.rowId` via
   `keyWithNullTokens`, else a UUID) only when the run is parallel.
2. Stage loop — linear (`engine.parallelWaves: false`, or streaming, or no wave with ≥ 2 stages) or
   the **wave loop** over `plan.getEngineWaves()`. `Wiring.applyStage` is the single place a stage
   becomes transforms, named `Stage{n}_{kind}` (+ `_Key`, `_Group`, `_Vc`, `_Fit`, `_StatsView`,
   `_Bins_<block>_*`, `_WriteStatic` / `_WriteForward` + `_Select` / `_Marker` / `_Group`):
   - `row` → `RowStageDoFn`.
   - `context` → `KeyDoFn` → `GroupByKey` → `ContextStageDoFn` (group in memory; also the
     fan-in merge point, `fanInBranches > 0` → `coalesce`).
   - `sequence` / `population` → `SortKeyDoFn` (key, (event millis, row)) → `GroupByKey` →
     `KeyedHistoryDoFn`: `KeyedSpillSorter.sort` (in-memory up to the budget, sorted chunks on
     worker-local disk beyond, deleted when the key's replay closes) → replay in time order with
     the **same-timestamp rows held in `pending`** until the timestamp advances (strictly-past
     semantics without tie-break dependence) → `evaluateKeyed` → `history.trim(watermarks)` →
     `outputWithTimestamp` (needs `getAllowedTimestampSkew` = max). Rows with a null key
     (`NULL_KEY`) bypass evaluation (keyed columns null).
   - `future` (a `direction: future` block — label columns) → the same chain with `SortKeyDoFn(keys, true)`
     (`~millis`, latest first) and `KeyedHistoryDoFn(..., mirrored = true)`: the evaluators see the clock `−t`
     for the row and the history, so the strictly-past `SequenceEvaluator` reads `(t, t + maxAge]` unchanged; the
     order-dependent readings are fixed up front (`first` / `last` swapped by `SequenceEvaluator.func` while the
     coordinates keep the declared func, `lag` named `lead`, `sinceEvent` `until`; `delta` / `trend` /
     `fracdiff` / lagged `regression` rejected — `OperatorCatalog.FUTURE_OPS`). The columns are `Status.label`
     without a role (`classifyFuture`; the role `label` is the declared `output.roles.label` only), exempt from
     the violation check and from `_isnull`; a feature reading one is a violation through `availableAt`.
     `FeatureLineage.labels` collects every status / role `label` column; a screen excludes them all from its
     candidates.
   - `fit` → `applyFit`: encoding levels (`fitLevels` → `VarianceComponents.perKeyStats` over the
     stage input re-windowed into `GlobalWindows` → `View.asMap`; artifact load via `FitArtifact`,
     artifact write through `writeArtifacts` = the entries grouped under their block + an empty marker →
     one `WriteArtifactDoFn` call per block — never a scan of a map side input, which is one state fetch
     per entry on a portable runner), plus `StaticFitBlock`s (`FmSpec`, `DiscretizeSpec`, `QuantileTransformSpec`,
     `SvdSpec`, `JointSpec` — one per keySet × window × target with `estimator: joint`, cells
     aggregated per key then solved on one worker by `JointFit`: `fit(fitInput)` → one side-input
     model, or `readArtifact` at `@Setup`; fold / forward joint models always re-fit; the blocks whose fit
     state is a `Summary` family — `SvdSpec`, `QuantileTransformSpec`, `SmoothSpec` (the svd family again), `SpectralSpec` (pair counts) — are `SummaryFitBlock`s (all four `ForwardFitBlock`s: one shared fit geometry, one shared `FitArtifact.Json`, and one floor — `fitAbove` gives a state fewer rows than the `minRows` coordinate contributed to the family's empty-state model, for the whole-input fit and every change point alike; `rowsOf(state)` is the family's own count; after the change points are solved, `solve` chains them in TIME ORDER through `alignTo(previous, current)` — svd and spectralEmbedding rotate each fit into the coordinates of the last fitted one before it (`Alignment`: orthogonal Procrustes / signs, the `align` coordinate), never backwards (a fit aligned to a later one reads rows it may not), and the whole-input model last, because a static serving run loads it in place of the forward fits) fitted
     together by `fitSummaryBlocks`: one `_Fit<Family>_Extract` pass + one `_Fit<Family>_Combine` per family
     keyed by (block, time block), `_Group` by block, `_Solve`, and ONE `_FitModelsView` list side input for
     all of them, so the stage's step count does not grow with the block count) → `FitApplyDoFn`
     fills the hidden columns
     and applies the blocks, then evaluates the stage's row columns. Rejects at construction: a fit
     input produced by the same stage (would read null), and a fit without artifact in streaming.
3. Wave fan-out (batch only): `RowId_Pin` (`Reshuffle`) before the first fan-out when ids are
   random and no GBK pinned them yet; `Wave{n}_Rows` (`applyRows` — the wave's row columns that
   are computable from the wave input are evaluated on the base **before** branching); each
   branch = `applyStage` + `PartialDoFn` (`__rowId`, `__partial`, own columns, carry keys); merge =
   (a) fold into the next wave's single context stage (`getFoldTarget`; Vc estimated over the wave
   input), (b) fold into the `output.groupBy` finalize, else (c) `Wave{n}_Merge` (row-id GBK +
   `MergeDoFn`). `coalesce` requires partials == branches (a branch failure drops the row, like the
   linear chain), rejects duplicate row ids as a whole group, `rejectionRecords` → `BadRecord`.
4. `Finalize` / `Finalize_Key` + `Finalize_Group` + `GroupedFinalize` (`output.groupBy`: parent
   record + child array `output.childName`, `parentFields`, `passThrough`, `nullPolicy`,
   `Finalizer` builds the output map; `__rowId` / `__partial` are dropped here).

`engineConstraints(plan, streaming)` adds the engine's own rejections (keyed stages / fold in
streaming, `runtimeFilter` columns, stats the population evaluator cannot serve). `spillOptions`
resolves `engine.spill` + `--featureSpillMemoryMB`. `createOutputSchema` / `passThroughInputs`
shape the output schema (lineage in field options: `OutputColumn.toOptions` for emitted columns,
`FeaturePlan.passThroughOptions` for inputs — `feature.scope = input`, `feature.kind`,
`feature.derivedFrom` = the kind plus any upstream lineage the field arrived with, `feature.sources`,
`feature.evidence`; other upstream `feature.*` options are dropped — and `feature.role` on both; the same
map feeds the manifest's `fields` entries, and `FeatureLineage.fromSchema` (`util/pipeline/feature/`, read by
the `screen` and `evaluation` transforms) reads the selectors and the roles from it,
`FeatureLineage.fromManifest` the `fields` and `columns` entries).

## Invariants — what a change must keep true

1. **Compile layer stays Beam-free and deterministic.** `FeaturePlanCompilerTest` runs without a
   pipeline; the same spec must produce the same plan (and hash) whatever the block order
   (`testHashIsOrderIndependent`). Anything the engine needs at runtime goes into `coordinates`
   as strings; the engine never re-reads the spec for a column.
2. **The plan report is the contract, and the engine mirrors it.** `waves` / `dagShuffles` /
   `deps` in `describe()` come from the same `FeaturePlan` methods the wave loop uses
   (`getEngineWaves`, `getFoldTarget`, `getPreludeColumns`, `keysAvailable`). Do not re-implement
   a scheduling rule in `FeatureStages`; add it to `FeaturePlan` so estimate and wiring cannot
   drift (PR #92 review). Transform names (`Wave1_FanIn`, `Wave1_Merge`, `RowId_Pin`,
   `_context_Vc`) are asserted by tests as the merge-path fingerprint.
3. **Parallel == linear.** Every wave / merge change is validated by running the same config with
   `engine.parallelWaves: false` on the same pipeline and comparing outputs row by row
   (`assertParallelMatchesLinear`). A DAG-level rule such as "row columns are transparent" must be
   matched by the engine recomputing them on the branch input (`Wave{n}_Rows`); the linear chain
   carries values that branches never see — that was the 43-column null bug (83f25296).
4. **Scheduling rules** (`StageScheduler`): a keyed column goes to the earliest same-kind /
   same-key slot after its dependencies; inputs read *inside* the DoFn (row, history) may share the
   stage, inputs read *before* it (stage keys, fit stats, Vc fields) need an earlier stage
   (`strictInputs`); row columns are placed as late as possible (first consumer, or the last stage) —
   except a row column over fitted columns, which is pulled back to the earliest stage its own inputs allow
   (`rowEarliest`): the fit stage when the fit is all it reads (lambdas / artifact live there), and the latest
   of them when it reads several fit stages or a fitted plus a keyed column (`placeRow`; an earlier stage
   precedes part of what it reads, which the scheduler's own consistency check rejects);
   a reader of estimated pseudo-counts (`weights: varianceComponents` + `levels`) must stay IN its levels' fit
   stage — another stage would estimate its own λ over its input, silently — and `placeRow` throws otherwise;
   a static-fit block = exactly one fit stage; sequence + population under one key fuse (reported
   as `population`). A column reading a later stage is a scheduler bug and throws.
5. **Keyed evaluation is O(n) per key and history is trimmed per field.** New sequence /
   population logic must either be incremental (`contribute` / `readStatistic` with eviction) or
   declare a bounded tail (`tailSize`); anything else is *unbounded* and must surface through
   `unboundedReason` → the `sequence.window.unbounded` hint. Never hold a key's rows as a list in a
   DoFn: `KeyedHistoryDoFn` streams the sorted iterable, and `History.trim` runs **before** the
   output so a trim failure cannot double-route a row.
6. **Strictly-past semantics.** Rows sharing a timestamp never see each other (the `pending`
   buffer); `orderTieBreak` is a declaration check only. Every keyed statistic reads `pastInputs`
   from the projected history, never from the current row, except through `$self` equality filters
   (reduced to partition keys at compile time when the field is pre-event; outcome-like fields stay
   filters because keying on them would leak).
7. **Availability is decided in one finish function per family.** `finishRow` (self inputs only),
   `finishContext`, `classifyPast` (sequence / expanding encoding: past side must be static →
   `staticSafe` / `windowShift` / `runtimeFilter`, `minInterval` can absorb the shift),
   `finishStaticFitted` (lookup fits: the artifact is available at `computeAt` by declaration, only
   the row side decides). A `minInterval` that absorbs a shift is recorded per entity (`FeaturePlan.MinIntervalAudit`,
   the `minIntervalEntity` coordinate): audit SQL + `entity.minInterval` info at compile time, and `KeyedHistoryDoFn.auditInterval`
   counts the rows below it as `feature/minInterval_<entity>_below` at run time (per row, against the key's previous distinct
   event time; future stages excluded). Reported, never repaired per row. A violation that is consumed becomes a `_` intermediate; a terminal
   violation is `availability.violation`.
8. **Plan hash / artifacts.** `withoutArtifact` strips `engine`, `fit.artifact`, the output
   projection (`output.include` / `includeSource` / `includeHash` / `manifest`) and `ops[].temperatureFrom`
   (resolved by `FeaturePlanService.resolveTemperatureFrom` into `{source, hash, value}`); the projection,
   roles, include content and `FeatureSpec.resolvedExternals` go into `FeaturePlan.getOutputHash`
   instead (the output-table identity). A new
   *runtime-only* knob goes under `engine` (or is stripped explicitly); a new *semantic* parameter
   must stay in the hash. Artifacts are content-addressed by that hash under `<uri>/<planHash>/`
   and cached per JVM (`ARTIFACT_CACHE` / `MODEL_CACHE`), so a path must never be reused for
   different content. `fit.artifact.id` pins a hash for serving configs.
9. **Static fits read the whole input in the global window** (`_FitGlobal` re-windowing) whatever
   the module's windowing strategy; fold fits always re-fit (an artifact holds totals only).
10. **Failure routing.** Every DoFn catches `Throwable` per element and emits
    `Module.processError(...)` under `failFast`; keyed stages fail a whole key row by row
    (`failKey`) if the sort / spill fails. New DoFns follow the `StageDoFn` pattern.
11. **Reserved names.** `__rowId`, `__partial` (input fields rejected with `input.reserved`),
    `__baseline_*`, `{block}__e{n}`, hidden level columns `*__n` / `*__sum` / `*__sumsq`; deviations
    are `dev{level}` because `@` is not Avro-legal.
12. **Docs and tests are domain-neutral** (online-auction dataset: sessions / sellers / listings /
    `sold` / `final_price`). Production configs and `perf.txt` carry horse-racing names — never copy
    them into tests, docs or commits.

## Runtime facts that shape decisions

- Spill sorter (§9.3): budget per key being sorted (default = heap/4 shared by the cores, clamped
  16–256 MB), chunk files deleted per key, `compress: true` is 2–3× slower on spill stages (keep
  the default false). Log lines: one `budget` line per JVM, `keyed spill sorter Stage<N>_<kind>
  key=<key>: <chunks> chunk(s) / <MB> MB on disk + <rows> rows in memory; live spill on this worker
  <MB> MB (peak <MB> MB)` per spilled key, and the unbounded-columns line at stage setup.
- Waves (§9.4): one barrier per wave instead of one per keyed stage; production arc 52 → 9 min.
  Wave 1 is *throughput-bound*: Dataflow's autoscaler shrinks the pool at the fan-out, so a parallel
  batch run needs `options.dataflow.autoscalingAlgorithm: NONE` + fixed `numWorkers` (the
  `autoscalingAlgorithm` option itself was broken until PR #93). The remaining critical path is the
  single-key global-level stage (`encoding.globalKey` hint → `fit.mode: forward` / `static` / `fold` is a
  modeling change, not a drop-in; `forward` = per-(key, block) Combine + per-key prefix, the coarse
  prefix-scan of §9.4.4 restricted to sufficient statistics); the full §9.4.4 prefix-scan is unimplemented.
- DirectRunner (§9.5): its GroupByKey clones a key's whole buffered bag per touching bundle
  (`CopyOnAccessInMemoryStateInternals`), so a global key costs (upstream bundles × all rows)
  coder clones — orders of magnitude slower on coarse keys, not fixable from our side. **Never
  benchmark keyed stages on direct**; use Dataflow (full runs) or the prism image (subsets — in-memory
  runner, container memory ~linear in input, OOM-kills at ~916k rows on 32 GiB).
- Streaming: row / context only, linear chain, static fits need an existing artifact; the stateful
  keyed stage and stateful merge (§9.4.6) are not implemented.

## Gotchas (each one cost a debugging session)

- Avro on this classpath round-trips `array<double>` at **float precision** — store vectors as
  `bytes` (big-endian doubles), as `Factorization` does.
- Beam `FileSystems` treats a Windows drive letter as a URI scheme: artifact / spill paths in tests
  must be **relative** (`target/feature-artifacts/<uuid>`) or `gs://`.
- Configs are parsed as **YAML 1.2** (`YamlUtil`, SnakeYAML Engine, core schema, 64 MB limit): **duplicate keys are
  an error** — a test that composes a config by `String.replace` and ends up with a second `engine:` / `fit:` key
  fails loudly instead of silently keeping the last one — and `on` / `off` / `yes` / `no` are plain strings. Parameter
  names still avoid them (`regression` takes `against`, not `on`): specs travel through YAML 1.1 tools outside this
  repository, where a bare `on:` key arrives as `true` and is silently ignored. Very large generated test configs
  stay JSON (`FeatureSpillTest`).
- Text-block YAML in `FeatureTransformTest` has a *runtime* indentation of 6/8/10 spaces (the
  text block strips the common prefix); configs are composed with `String.replace` on exact
  lines, so match the runtime indentation, and `withEncoding` in the compiler test strips 4 more.
- DirectRunner's `enforceImmutability` catches an in-place `Arrays.sort` on a DoFn input — copy first
  (`FitDiscretizeDoFn`).
- A GroupByKey that re-emits rows at their original event time makes any downstream
  `@RequiresTimeSortedInput` drop them silently as late data — keyed stages set
  `getAllowedTimestampSkew()` to max and never use time-sorted-input DoFns.
- Two key functions with different null rules: `FeatureValues.key` returns null when any component
  is null (the row bypasses the keyed stage, its keyed columns are null), while
  `keyWithNullTokens` maps nulls to a deterministic token (row ids must never be null or random
  when declared). Use the right one for the purpose.
- The Bash tool unescapes `\n` inside heredocs and breaks on apostrophes in Python strings: edit
  Java lines containing escape sequences with the Edit tool; `sed -i` strips CRLF on the few CRLF
  files (the skills / CLAUDE.md are LF).
- `gh pr create` fails for this fork (SAML-protected parent); open PRs with `gh api
  repos/orfeon/pipeline/pulls` and push over https.

## Backlog (design position recorded, not implemented)

Listed in engine doc §9.2 "Deferred" and enforced as compile errors so nothing fails at runtime:

- `weights: heldOut` (`encoding.shrinkage.weights`), `estimator: joint` under `fit.mode: expanding` (the
  row-local replay has no cell table), a moment-estimated λ for a shrunk `distribution` — extend `Shrinkage` +
  `expandEncoding`. (An `offset` on a logit / log scale is implemented: hidden `__sumoff` per level,
  `Shrinkage.Level.offColumn`, `KeyStats.sumOff`, info `encoding.offset.additive`.)
- nested encoding targets (`targets[].field.ref`) —
  `encoding.nested`; ordering of fits is the open question. (`structure: sequence` is implemented: the keys are a
  path declared most recent first and `expandEncoding` derives the suffix chain `(k1..kn) → (k1..kn−1) → … → (k1)` as
  lattice levels — the same thing an explicit chain `hierarchy` declares, so the engine has no code of its own for it.)
- `quantile` / `distribution` in static / fold (`encoding.stat.static`): a static fit keeps only
  (n, Σy, Σy²) per key; would need a per-key sketch artifact.
- discretize `tree` / `optimal` (`discretize.method`): supervised, consumes a target — the spec ties
  the fit rule to the encoding keyed on the bins (two-stage target consumption) which the compiler
  does not model.
- (`spectralEmbedding` / `transitionStats` are implemented: both stand on `FeaturePlanCompiler.sequencePath`, a
  synthetic `lag` block expanded under the population block's name. `transitionStats` is a compile-time desugaring
  into an expanding distribution encoding — no run-time operator; `spectralEmbedding` is a `SummaryFitBlock` over
  `Spectral.SUMMARY` pair counts. A new sequence-of-values type starts from the same path columns.)
- factorization `variant: bayesian`, `fit.cadence / window / warmStart` (fit boundaries).
- `runtimeFilter` columns (`atRowCreation`, `event_date THH:MM`): per-row availability filtering.
- Streaming keyed stages (stateful DoFn + timers) and the stateful wave merge (§9.4.6).
- Sequence / population stages as fold-in merge targets (composite sorter key), §9.4.4 prefix-scan
  for the global-key stage, S3‴-b (fit apply at the merge point), observedAt / ingestedAt /
  confounding audit queries (§7 of the spec).
