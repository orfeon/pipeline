---
name: feature-engine
description: Developing and maintaining the feature transform (util/pipeline/feature + module/transform/FeatureTransform) — the declarative feature-engineering DSL with availability-time leak checking, its pure compile layer (FeaturePlanCompiler / OperatorCatalog / FeaturePlan) and its Beam engine (FeatureStages — keyed replay, waves, static fits, KeyedSpillSorter). Use when adding or changing a row / context / sequence op, an encoding stat, a population type (encoding, factorization, discretize, and the backlog quantileTransform / svd / estimator joint / structure sequence / nested encoding), touching the stage scheduler, waves, the fan-out merge, FitApplyDoFn / artifacts, spill / history trimming, or the plan report (describe / toJson / audit); when a diagnostic code (encoding.globalKey, sequence.window.unbounded, population.unsupported, encoding.stat.static, input.reserved, availability.violation, reference.unresolved ...) or an engine message ("keyed spill sorter", "Fan-out merge", "RowId_Pin", "Wave1_Merge", "fit.mode static ... requires an existing artifact", "feature stage scheduling") needs explaining; or when measuring a feature-engine change on Dataflow / prism.
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
  (both fail assembly with `IllegalModuleException`) → `LOG.info(plan.describe())`, printed to stdout
  under `--dryRun=true` → `FeatureStages.createOutputSchema` → `Union.flatten` → `FeatureStages.apply`.
- `util/pipeline/feature/FeaturePlanService.java`: the one shared entry (`resolve` / `compile` /
  `validate(rawRequest)`). REST `POST /api/feature` (`server/api/FeatureService`), MCP
  `validate-feature` (`server/mcp/tool/ValidateFeatureTool`), agent `validateFeature`
  (`server/agent/tool/FeatureValidator`) and the `run-pipeline dryRun` response's `featurePlans`
  all go through it. Server-side tests need `-Pserver` (plain `mvn test` skips `server/**`).

### Compile layer (`util/pipeline/feature/`, no Beam imports)

- `SourceContract` — the sources document: per-source `eventTime`, `availability`, `settlementLag`,
  `ingestionLag`, `mutability`, `snapshotOf`, per-field `availableAt` / `observedAtField` / `kind`
  (attribute / market / outcome) / `validFor`. `SourceContract.Json` is the shared lenient JSON accessor.
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
  the evaluators rebuild their plans from it (`SequenceEvaluator.plan`, `fmSpecs`,
  `discretizeSpecs`, `fitLevels`) and it is exported as `feature.coord.*` schema options;
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
  `include` as the projection in `finalizeColumns` (`applyInclude`, replaces `exclude`; the columns that
  roles resolve to — `roleColumnsByCanonical`, mirror of `FeaturePlan.getRoleColumns` — survive the
  projection, `output.include.role`) and builds one
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
  `lambdaFromMoments`), `Discretization` (quantile edges + `<block>.bins.json`), `Factorization`
  (fm / fwfm ALS + `<block>.fm.avro`), `OrderStatistics` (Fenwick-tree block multiset for
  quantiles with eviction), `FitArtifact` (`<uri>/<planHash>/<block>.avro` + manifest for encoding
  levels), `Durations` (ISO-8601 + calendar periods + column tokens; **kept separate** from
  `outbound.Durations` by decision), `FeatureValues` (value coercion, keys, `keyWithNullTokens`).

### Evaluators (`Serializable`, Beam-free, one instance per stage DoFn)

- `RowEvaluator.evaluateColumn` — `switch (c.operator)`: `expr` / `baseline` (Lucene expression
  engine, **doubles only**), `datetime`, `bin`, `cross`, `indicator`, `equals`, `residual`,
  `isnull`, `copy` (baselines[].emit), `noise` (murmur3 of seed + row identity → `SplittableRandom`),
  and the hidden-level readers of a lattice: `share`, `fitStat`, `compose`, `deviation`,
  `effectiveN` (λ from `setLambdas`, the variance-components side input).
- `ContextEvaluator.evaluateColumn` — one group at a time; `apply(op, values, self, excludeSelf)`;
  group-constant ops are evaluated once per group; `values:` lists become per-value columns
  (`valueKey` normalises integral numbers). `softmax` and `shuffle` bypass `apply`: they read two
  per-row inputs / need the group order (`softmax(c, rows)` in probability space with a max-shift;
  `shuffle(c, rows)` = Fisher–Yates from (seed, group key) over rows sorted by `order` + `tieBreak`
  coordinates — the tie-break over all input fields is what makes it engine-mode independent). Op
  parameters that are not a single field go through `FeaturePlanCompiler.configureContextOp`.
- `SequenceEvaluator` — the keyed replay logic. `ColumnPlan` from coordinates (shift, `maxAge`,
  `maxEvents`, filter → `EqualityFilter` when `f = $self.f`, `stat`, `quantile`); two paths per
  column: **incremental** (fold / evict pointers over the history + `Accumulator` — n, Σ, Σ², max,
  min, value counts, order statistics; `contribute` / `readStatistic`) when `incrementalStat` is
  non-null, no `maxEvents`, no general filter, and not max/min with eviction; else **scan**
  (`select` = binary-searched sublist view, `evaluateScan` switch). `History` (absolute indices,
  trimmable prefix), `Watermarks` (per-field trim floors), `retainInto` / `tailSize` /
  `unboundedColumns` / `unboundedReason` (the compile-time twin used by the
  `sequence.window.unbounded` hint). `bufferedFields()` = union of `pastInputs` = what the stage
  projects per past row.
- `PopulationEvaluator extends SequenceEvaluator` — encoding statistics: overrides `contribute` /
  `readStatistic` / scan for `count` / `sum` / `mean` / `rate` / `std` / `distribution` /
  quantiles; `isSupported(stat)` is what `engineConstraints` checks; NaN counts as missing.
- `VarianceComponents` — per-level (n, Σy, Σy²) per key as a Beam `Combine`, λ = σ²/τ² by the method
  of moments (side input `Map<levelNColumn, λ>`), fold-tagged entries for `fit.mode: fold`,
  `lambdasInMemory` for loaded artifacts; `forwardSeries` / `forwardTotals` / `lambdasByBlock` for
  `fit.mode: forward` (`ForwardBlocks` = block arithmetic + the cumulative `Series`; coordinates
  `blockBucket` | `blockSizeMillis`, `minBlocks`, `forwardLagMillis` (target availability delay), `windowBlocks`,
  `blockField` / `blockFieldType` written by `FeaturePlanCompiler.forwardCoordinates`; the engine side is
  `FitLevel.forward` + `FitApplyDoFn.forwardStats`, which also swaps the row's per-block λ into the evaluator).

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
   `_Bins_<block>_*`, `_Write_<block>`):
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
   - `fit` → `applyFit`: encoding levels (`fitLevels` → `VarianceComponents.perKeyStats` over the
     stage input re-windowed into `GlobalWindows` → `View.asMap`; artifact load / write per block
     via `FitArtifact`), plus `StaticFitBlock`s (`FmSpec`, `DiscretizeSpec`: `fit(fitInput)` → one
     side-input model, or `readArtifact` at `@Setup`) → `FitApplyDoFn` fills the hidden columns
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
`passThroughField` for inputs — `feature.scope = input`, `feature.kind`, `feature.derivedFrom` = the
kind, `feature.sources`, `feature.evidence` — and `feature.role` on both; the `screen` transform's
`Lineage.fromSchema` reads them).

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
   (`strictInputs`); row columns are placed as late as possible (first consumer, or the last stage);
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
   the row side decides). A violation that is consumed becomes a `_` intermediate; a terminal
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
- SnakeYAML caps a document at ~3 MB: generated large test configs must be JSON (`FeatureSpillTest`).
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

- `estimator: joint`, conjugate families, `weights: heldOut`, logit / log scale with `offset`
  (`encoding.shrinkage.estimator` / `encoding.offset.scale`) — extend `Shrinkage` + `expandEncoding`.
- `structure: sequence` key sets, nested encoding targets (`targets[].field.ref`) —
  `encoding.keySet.structure` / `encoding.nested`; ordering of fits is the open question.
- `quantile` / `distribution` in static / fold (`encoding.stat.static`): a static fit keeps only
  (n, Σy, Σy²) per key; would need a per-key sketch artifact.
- discretize `tree` / `optimal` (`discretize.method`): supervised, consumes a target — the spec ties
  the fit rule to the encoding keyed on the bins (two-stage target consumption) which the compiler
  does not model.
- `quantileTransform` / `svd` / `spectralEmbedding` / `transitionStats` (`population.unsupported`):
  follow the static-fit block recipe in [add-operator.md](add-operator.md).
- factorization `variant: bayesian`, `fit.cadence / window / warmStart` (fit boundaries).
- `runtimeFilter` columns (`atRowCreation`, `event_date THH:MM`): per-row availability filtering.
- Streaming keyed stages (stateful DoFn + timers) and the stateful wave merge (§9.4.6).
- Sequence / population stages as fold-in merge targets (composite sorter key), §9.4.4 prefix-scan
  for the global-key stage, S3‴-b (fit apply at the merge point), observedAt / ingestedAt /
  confounding audit queries (§7 of the spec).
