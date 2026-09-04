# Adding an operator, an encoding stat or a population type

Every addition follows the same spine: **catalog → spec → compiler expansion (coordinates +
diagnostics + availability) → evaluator / engine → docs → tests**. Pick the recipe for the
scope, then run the common checklist at the end. PR #100 (`096956a8` + review fixes `c4ecc431`:
the `quantile` stat and `type: discretize`) is the reference diff — `git show 096956a8 --stat`
lists the 13 files such a change touches.

## Recipe A — row op (`scope: row`, `type: <op>`)

Row ops read only the current row; availability is the max of their inputs.

1. `OperatorCatalog`: `register(Scope.row, "<op>", InputKind.<numeric|categorical|any>, <output type
   or null>, false, "<description>")`.
2. `FeatureSpec.FeatureDef`: add the op's parameters as public fields; parse them in the feature
   parse method (`Json.string / integer / strings`).
3. `FeaturePlanCompiler.expandRow`: a new `case "<op>"`. Validate with `diagnostics.error("row.<op>.<field>", loc, ...)`;
   create the column with `newColumn(def.name, Scope.row, "<op>", <output name>, <type>, computeAt)`,
   put every runtime parameter into `c.coordinates` (strings), `addSelfInput(c, field)` for each
   input, then `finishRow(c, def)`. Multi-column ops emit one column per variant (`datetime`,
   `indicator`).
4. `RowEvaluator.evaluateColumn`: a new `case "<op>"` reading `c.inputs` / `c.coordinates`.
   Numeric conversion through `FeatureValues.toDouble`; return null on a null input.
5. Docs: the row section of `feature.md` (naming: `{name}` or `{name}_{variant}`).
6. Tests: `FeaturePlanCompilerTest` (column type / coordinates / error codes),
   `FeatureTransformTest` if the runtime logic is more than a one-liner.

## Recipe B — context op (`scope: context`, `ops: [<op>]`)

`expandContext` is generic: it only needs the catalog entry (`InputKind.none` → one group-level
column named `{block}_{op}`; otherwise one column per field named `{block}_{field}_{op}`, type
`operator.outputFor(fieldType)`; `values:` → per-value columns for map-valued ops).

1. Catalog entry.
2. `ContextEvaluator.apply`: a new `case`; numeric ops go through `numeric(op, values, self, excludeSelf)`.
   If the result is the same for every row of the group, add the op to the `groupConstant` list in
   `evaluateColumn` (evaluated once per group) and to `PARENT_CONTEXT_OPS` in the compiler if it
   should land on the parent record under `output.groupBy`.
3. Docs + a compiler test; `ContextEvaluator.apply` is static, so unit-test it directly
   (`testReviewRegressions` does).

## Recipe C — sequence op (`scope: sequence`, `ops: [{type: <op>, ...}]`)

Sequence ops read **past rows only** (`$self` is rejected in ops; window filters may use it).

1. Catalog entry (`InputKind.predicate` for predicate ops, `any` / `numeric` otherwise).
2. `FeatureSpec.Op`: parameters + parse.
3. `FeaturePlanCompiler.expandSequence`: a new `case` in the per-field switch. Naming
   `base + "<op><param>"` where `base = {block}_{window}_{field}_`; coordinates `field`
   (canonical) + parameters; `addPastInput(c, field)` (this is what the keyed stage projects into
   the history); `finishSequence(c, def, entity, window, filterRefs, reducedKey, op)` sets the
   window coordinates and runs `classifyPast`. Predicate ops go through the predicate branch
   (`conditionText` parses and quotes reserved identifiers at compile time).
4. `SequenceEvaluator`: decide the evaluation path.
   - **Incremental** (preferred): extend `incrementalStat` to return the op's stat token, add
     accumulator fields to `Accumulator`, update `contribute(plan, acc, past, sign)` (sign −1 is
     eviction under `maxAge` — if the statistic cannot be evicted, exclude it like max / min in
     `plan()`), and `readStatistic`. The `EqualityFilter` sub-key dispatch comes for free.
   - **Scan**: a new `case` in `evaluateScan` over the `window` sublist. Then declare the
     retention: a bounded tail in `tailSize` (`lag` / `trend` = k, `delta` = k+1, `maxEvents`), else
     the column is *unbounded* — `unboundedReason` must describe it and the
     `sequence.window.unbounded` hint will list it. Do not add scan ops that walk the whole
     history per row without a bound unless the spec really needs it.
5. Tests: `SequenceIncrementalTest` (add the op to `SPEC`: randomized equivalence of incremental
   vs scan and trimmed vs untrimmed), `FeaturePlanCompilerTest`, `FeatureTransformTest` for a
   hand-checked value on the auction rows.
6. Docs: the sequence op table in `feature.md`, including the retention rule of the op.

## Recipe D — encoding stat (`targets: [{stats: [<stat>]}]`)

1. `OperatorCatalog.STATS` + `stat(name)`: `Stat(name, requiresTarget, outputType, sufficient)`.
   `sufficient = true` means derivable from (n, Σy, Σy²) — then it works in **expanding, static and
   fold**; `false` means expanding only (the compiler rejects static / fold with
   `encoding.stat.static` from `s.sufficient()`, nothing else to do).
2. Sufficient stat: `RowEvaluator` `case "fitStat"` (static / fold read the leaf level's hidden
   columns) and `PopulationEvaluator.readStatistic` (expanding). `AVAILABLE_STATS` and the
   `engineConstraints` check derive from the catalog.
3. Non-sufficient stat (the `quantile` pattern): a state field on `Accumulator` (must support
   removal for `maxAge` eviction — `OrderStatistics` exists for order-based stats), `contribute`
   (sign ±1), `readStatistic`, and the scan-path branch in `PopulationEvaluator.evaluateScan`.
   Resolve any per-row parsing at plan time into `ColumnPlan` (`plan.quantile`), never per row.
4. If the stat is shrinkable (mean / rate are), wire it through `Shrinkage` / `composeCoordinates`;
   raw statistics (quantile, distribution) bypass composition — say so in the docs.
5. Tests: `FeaturePlanCompilerTest.testQuantileStat` pattern (coordinates, `fit=expanding`, the
   rejection codes), `SequenceIncrementalTest` `SPEC` (equivalence + eviction), e2e
   `FeatureTransformTest.testQuantileStat` with hand-computed expanding values.

## Recipe E — population type with a static fit (worked example: `discretize`)

The pattern for `quantileTransform`, `svd`, `spectralEmbedding`, `transitionStats`: fitted once
over the whole input (or loaded from an artifact), applied per row by lookup.

1. **Model class** (pure Java, `Serializable`, like `Discretization`): `fit...(...)`, `apply` /
   `bin` / `transform`, `toJson` / `fromJson` (or Avro — doubles as `bytes`), `artifactPath(uri,
   planHash, block)` = `<uri>/<planHash>/<block>.<ext>`, `exists` / `read` / `write` via
   `ResourceUtil` / Beam `FileSystems`. Unit-test it in isolation (`DiscretizationTest`).
2. `FeatureSpec.FeatureDef`: the parameters (`method`, `bins`, ...) + parse.
3. `OperatorCatalog`: the type is already registered as `fit=true`; add it to
   `IMPLEMENTED_POPULATION_TYPES`.
4. `FeaturePlanCompiler.expandPopulation`: dispatch to a new `expand<Type>(def, computeAt)`:
   - resolve and type-check the input(s) (`discretize.input` style codes), validate parameters;
   - `parseStaticOnlyFit(def, "<type>", "the ... is fitted", "<why static>")` — inherits the top-level
     artifact settings, requires `fit.mode static`, warns on `cadence / window / warmStart`;
   - `diagnostics.info("fit.mode.static", loc, ...)` including `artifactPhrase(fitSpec)` and the
     outcome-like caveat when the input is an outcome;
   - `newColumn(...)`, `c.fitted = true`, coordinates `fit=static`, the parameters, `field`,
     `artifactUri` / `refit` when set; `addSelfInput` + `addPastInput` for the input;
     `finishStaticFitted(c, def)`; `register(c)`.
   The scheduler then puts the column in the block's single fit stage and any encoding keyed on
   it in a later keyed stage (assert this in the compiler test).
5. `FeatureStages`: a `record <Type>Spec(block, column, <params>, artifactUri, refit) implements
   StaticFitBlock<Model>` rebuilt from coordinates by `<type>Specs(stageColumns)`; `fit(fitInput,
   label, planHash)` = extract → `Combine.globally(<gather fn with a default accumulator>)` →
   fit DoFn (writes the artifact when `artifactUri != null`) → `View.asList`; `fitInputs()` lists
   the fields read from the stage input (same-stage producers are rejected); `apply(model,
   values)` fills the column (`model == null` → null). Register it in `applyFit`
   (`blocks.addAll(<type>Specs(stageColumns))`) — `FitApplyDoFn` needs no change. Copy before
   sorting (DirectRunner immutability). The whole training set lands on one worker: state the
   memory cost in the docs (discretize: 8 bytes per row).
6. Docs: a `### <Type> (population, type: <type>)` section in `feature.md` (example, fit semantics,
   artifact file, out-of-range behaviour), and remove the type from the *Limitations* list.
7. Tests: `FeaturePlanCompilerTest.test<Type>Expansion` (coordinates, `fit` stage before the keyed
   stage that consumes the column, every error code), `FeatureTransformTest.test<Type>` (values on
   the auction rows + the artifact on disk under a relative `target/...` path, read back and
   checked), the model unit test.

## Recipe F — population type with expanding statistics

Anything evaluated per key in time order (a new `estimator`, `structure: sequence`, nested
targets) goes through `expandEncoding` → `populationColumn` (hidden level columns named
`{block}__{keys|global}__{window}__{target}__n/__sum`) and the row-local composition in
`Shrinkage` (`composeCoordinates`, `levels` coordinate, `compose` / `deviation` / `effectiveN` row
ops). Read engine doc §4.4 and spec §5 first; the invariants that bite are strictly-past
semantics (invariant 6), the one-fit-stage rule for static levels, and the wave prelude (compose
columns are row columns hosted by a keyed stage — they are *not* recomputed on branches, they
travel in partial rows).

## Common checklist

- [ ] Catalog is updated (`OperatorCatalog`) and the message of every "unknown / unsupported"
      diagnostic derives its "available: ..." list from it.
- [ ] Diagnostic codes follow `scope.field.reason`, are asserted with `hasCode` in a test, and are
      mentioned in `feature.md` when a user can act on them.
- [ ] Coordinates are strings, resolved into a per-column plan at `setup()` (never parsed per row).
- [ ] Availability goes through the family's finish function; a new family documents its rule in
      the spec's §6.1 terms.
- [ ] Runtime-only knobs live under `engine` (outside the plan hash); semantic parameters do not.
- [ ] Keyed logic is incremental or declares a bounded tail; otherwise `unboundedReason` covers it.
- [ ] `feature.md` updated (section + Limitations); `index.yaml` only if the module description changes.
- [ ] Tests: compiler (pure), runtime (e2e on the auction dataset), and — for keyed / wave changes —
      `SequenceIncrementalTest` and the parallel-vs-linear equality (see [testing.md](testing.md)).
- [ ] Engine doc §9.2 gets an entry if the working docs are still being maintained.
