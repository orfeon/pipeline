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
   - **Incremental** (preferred): give the statistic a `Summary` family. Either an existing one
     serves it (add a readout `case` to the family's `read` and a line to
     `OperatorCatalog.summary`), or write a new family: a `State` class + `create` / `update(state,
     contribution, ±1)` / `merge` / `read` / `count`, declaring `invertible()` honestly (sign −1 is
     eviction under `maxAge`; a family that cannot remove — extrema — returns false and `plan()`
     sends windowed columns to the scan path automatically). What a past row *contributes* is the
     evaluator's job (`contribution(plan, past)`, null = skip); the scope's null / cast convention is
     `readStatistic`. The `EqualityFilter` sub-key dispatch and the fold / evict pointers come for
     free. `SummaryTest` has the monoid / group harness (`assertMonoid`, `assertInvertible`) —
     add the family there. `Summary.Shape` (skew / kurt: anchored power sums, a catalog line, a `case` in
     the scan `aggregate`) is the smallest worked example; a statistic that reads *neighbouring* values
     (`SeriesStats`: acf, peaks) is not a sum of contributions and stays on the scan path.
   - **Scan**: a new `case` in `evaluateScan` over the `window` sublist. Then declare the
     retention: a bounded tail in `tailSize` (`lag` / `trend` / `fracdiff` = k, `delta` = k+1, `maxEvents`), else
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
3. Non-sufficient stat (the `quantile` pattern): a `Summary` family whose state holds what the
   statistic needs (`ORDER` wraps `OrderStatistics` so it can remove for `maxAge` eviction), a
   `Readout` carrying the parameter (`Readout.of("quantile", p)` — resolved once in
   `OperatorCatalog.summary`, never per row), `PopulationEvaluator.contribution` if the extraction
   differs from "target minus offset", and the scan-path branch in `PopulationEvaluator.evaluateScan`.
4. If the stat is shrinkable (mean / rate are), wire it through `Shrinkage` / `composeCoordinates`;
   raw statistics (quantile, distribution) bypass composition — say so in the docs.
5. Tests: `FeaturePlanCompilerTest.testQuantileStat` pattern (coordinates, `fit=expanding`, the
   rejection codes), `SequenceIncrementalTest` `SPEC` (equivalence + eviction), e2e
   `FeatureTransformTest.testQuantileStat` with hand-computed expanding values.

## Recipe E — population type with a static fit (worked example: `discretize`)

The pattern `quantileTransform`, `svd`, `smooth` and `spectralEmbedding` follow: fitted once
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
     artifact settings, requires `fit.mode static`, warns on `cadence / window / warmStart`. A type whose
     fit state is a `Summary` family (svd's moments) can use `parseLookupFit(..., forwardAllowed = true)`
     instead: `fit.mode: forward` is then accepted (and inherited from a top-level `forward` fit when the
     block declares no mode of its own), `blocks` / `minBlocks` / `minHistory` / `window` are
     read into the spec, and the columns get the forward coordinates via `forwardCoordinates(c, null,
     inputs, def, fitSpec)` plus `predictOffsetMillis` — the engine side is a `BlockSeries<S>` fitted per
     change point (`SvdSpec` as a `SummaryFitBlock` — `contribution` / `solve` — is the template);
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
   values)` fills the column (`model == null` → null). **When the fit state is a `Summary` family**
   (svd's moments, quantileTransform's values) implement `SummaryFitBlock<T, S, M>` instead of `fit`:
   `family()` / `familyName()` / `stateClass()` / `contributionCoder()`, `contribution(row)` → (time
   block — 0 under static —, value) or null, `solve(parts, planHash)` (merge the per-time-block states
   through a `BlockSeries`, fit, write the artifact) and `fitsEmptyInput()`. The stage then fits every
   such block together — one extraction pass and one `Combine.perKey` per family, one side input for
   all models (`fitSummaryBlocks`) — so a dozen blocks do not become a dozen chains. Register it once in `staticFitBlocks`
   (`blocks.addAll(<type>Specs(columns))`), which feeds both `applyFit` and the manifest's
   `artifactPaths` — `FitApplyDoFn` needs no change. Copy before
   sorting (DirectRunner immutability). The whole training set lands on one worker: state the
   memory cost in the docs (discretize: 8 bytes per row). Before writing a family, check whether the fit is a
   function of one that exists: `SmoothSpec` needs (XᵀX, Xᵀy, yᵀy), which are the second moments of the vector
   `[basis(x), y]`, so it contributes that vector to `Svd.SUMMARY` under the family name `Moments` and shares the
   svd blocks' Combine — no new state, coder or merge law to test. A target-consuming block lists the target in
   `fitInputs()` and passes it to `forwardCoordinates` (its availability is the forward lag).
   **When the type is a composition of blocks that exist**, write no engine code at all: build the synthetic
   `FeatureDef`s and call their expanders (`expandCompress` → `expandSvd`; `sequencePath` → `expandSequence` for the
   lag path; `expandTransitionStats` → `expandEncoding` with a `naming` template that gives the columns the type's
   own names). Pin the equivalence in an e2e test — the sugar and the explicit blocks side by side, value for value.
6. Docs: a `### <Type> (population, type: <type>)` section in `feature.md` (example, fit semantics,
   artifact file, out-of-range behaviour), and remove the type from the *Limitations* list.
7. Tests: `FeaturePlanCompilerTest.test<Type>Expansion` (coordinates, `fit` stage before the keyed
   stage that consumes the column, every error code), `FeatureTransformTest.test<Type>` (values on
   the auction rows + the artifact on disk under a relative `target/...` path, read back and
   checked), the model unit test.

## Recipe F — population type with expanding statistics

Anything evaluated per key in time order (a new `estimator`, a new key-set `structure` — `sequence`
derives its suffix chain there — nested targets) goes through `expandEncoding` → `populationColumn` (hidden level columns named
`{block}__{keys|global}__{window}__{target}__n/__sum`) and the row-local composition in
`Shrinkage` (`composeCoordinates`, `levels` coordinate, `compose` / `deviation` / `effectiveN` row
ops). Read engine doc §4.4 and spec §5 first; the invariants that bite are strictly-past
semantics (invariant 6), the one-fit-stage rule for static levels, and the wave prelude (compose
columns are row columns hosted by a keyed stage — they are *not* recomputed on branches, they
travel in partial rows).

## Recipe G — a `Summary` family (a statistic that is maintained, not re-read)

Read engine doc §9.6.1 first. Decide with its table before writing code — the answer is a property of the
statistic, not a preference:

| the statistic … | then |
|---|---|
| is a sum of per-event contributions | a family: it runs incrementally, and as a fit state per time block |
| … and a contribution can be subtracted again | `invertible()` = true: it also evicts under `maxAge` |
| reads neighbouring events (acf, peaks), pairs two events (a lagged pairing), or reads the current row (`weightBy`) | **no family**: a scan readout (`VectorOps` / `SeriesStats`, a `case` in `evaluateScan`) — say why in the javadoc, and make sure the window is bounded or `unboundedReason` names it |

Writing the family (worked examples: `Summary.Shape` — the smallest; `Summary.Regression` — a pair contribution;
`QuantileTransform.VALUES` — a monoid without inverse):

1. A `State` class (`Serializable`, public fields) and the family as a nested class of `Summary` (or next to its
   model when only a fit uses it), a singleton constant in `Summary.Summaries`.
2. `update(state, contribution, ±1)`: the contribution type is the family's contract (`Number`, `double[]{x, y}`,
   a category) — document it; a non-invertible family throws `UnsupportedOperationException` on `−1`.
   - Sums of order ≥ 2 over values that may carry a level: subtract an **anchor** (the first contribution), make
     `merge` re-anchor `other` onto `into` (binomial shift), and **reset exactly when `n` returns to 0** so rounding
     residue does not outlive a window and the next value re-anchors.
   - `merge(into, other)` must not modify `other` (DirectRunner immutability, and `BlockSeries` re-merges parts).
3. `read(state, Readout)`: population moments, **null — never NaN —** below the minimum count or without spread
   (guard the variance against the rounding floor of the sums it is formed from, not against `== 0`); unknown
   readout → `IllegalArgumentException`.
4. `OperatorCatalog.summary(stat)`: one `case` mapping the token(s) to `(family, Readout)`. Nothing else decides
   the path: `SequenceEvaluator.plan()` reads the family and `invertible()`.
5. The evaluator side: `contribution(plan, past)` if the extraction is not "the field's number" (a pair reads two
   fields; return null to skip a row — read each value through `SequenceEvaluator.finite`, so null, NaN and ±∞
   are missing on both paths), and the **scan twin** — fold the same family over the window rather than
   re-deriving the arithmetic, so both paths share one null rule (`aggregate` `skew`, `regression`).
6. Output type / validation: `OperatorCatalog.aggregateOutput` (or the op's own func list + `AVAILABLE_*` message).

Tests (all three, they catch different bugs):

- `SummaryTest`: hand values; degenerate inputs (too few, constant — including a constant *left behind by eviction*);
  a large offset (the anchor); `assertMonoid(family, values, SummaryTest::assertClose, readouts)` — both merge orders
  and the identity, with the comparator because re-anchoring agrees up to rounding; `assertInvertible` (random add /
  remove vs a fresh fold); the exact reset.
- `SequenceIncrementalTest`: add the func to the shared `SPEC` under a `maxAge` window, an equality-filter window
  and a `maxEvents` window — incremental == scan and trimmed == untrimmed come for free.
- Compiler (`unboundedReason` is null without a window for a family-backed func, non-null for a scan readout) and an
  e2e value on the auction rows.

**As a fit state** (a fitted block whose model is solved from the summary): implement `SummaryFitBlock` (recipe E,
step 5) — `contribution(row)` → (time block, value), `solve(parts, planHash)` through a `BlockSeries`. `static`,
`forward`, `window` and `minBlocks` then need no code of their own, and the block shares the stage's one
`Combine.perKey` per family. Let the compiler accept the mode with `parseLookupFit(..., forwardAllowed = true)` +
`forwardCoordinates`, and test that a change-point model **equals the static fit on the readable blocks**
(`QuantileTransformTest.testValuesFamilyAndBlockSeries`, `SvdTest`).

**Parallel branches.** Several statistics are usually added on independent branches. They all register in the same
few places (`OperatorCatalog.summary`, `FeatureSpec.Op`, `blockReferences`, `ColumnPlan` / `plan()`, the test
fixtures), so put each addition next to a *different* neighbouring line and trial-merge the open branches onto a
throwaway branch before opening the PR — two insertions at one spot are a conflict even when the code is unrelated.

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
