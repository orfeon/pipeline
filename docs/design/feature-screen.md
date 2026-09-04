# Screen Transform (Design Document)

Status: **Implemented — marginal score test with placebo calibration, periods, time window and leak flags;
conditioning (the partial test against an existing feature set, §2 `ConditioningScorer` / `FitState`, §3);
families `groupedMultinomial` / `binomial` / `gaussian` / `poisson`; `output.selection` (the pass list the feature
transform's `output.include` reads). The independent-row `rank` / `absdev` via a quantile sketch remains a design
position (§4).** User-facing reference: `src/main/resources/server/docs/module/transform/screen.md`.

## 1. Position

`screen` is the supervised twin of the `profile` sink and the downstream of the `feature` transform:
feature (generation) → screen (ranking and cut-off before training) → training job. It is a *filter*
method: learner-free, one bounded Combine per statistic, so the pass count never depends on the data. Wrapper
methods (RFE / SFS), embedded importances, SHAP / MDA and causal selection are out of scope — they need a
learner or predictions (an evaluation step) or a data-dependent loop.

The one statistic is the Rao score test of an offset GLM at β = 0: `chi2 = S² / H`, `est_gain = chi2 / (2N)`
(the one-step Newton improvement of the average log-likelihood per unit, the unit of an excess log score).
Everything else — placebo threshold, q-values, period agreement, leak flag — is closed-form over the
accumulator set. The same machinery reads as a *residual explanation* tool (which columns explain what the
current model misses): candidates = the model's own features gives a drift / staleness signal per period.

## 2. Code map

`util/pipeline/screen/` (Beam wiring only in `ScreenStages`):

- `ScreenSpec` — parse (`parse(JsonObject)`: every error collected) and resolve (`resolve(schema, lineage)`:
  role defaults from the feature manifest, candidate selection = numeric fields × include globs / selectors −
  exclude − role fields). `Lineage` is read from the input schema's `feature.*` field options (feature
  directly upstream) or from `candidates.manifest` (`FeaturePlan.toManifest`: `roles`, `timeField`,
  `columns[].scope / block / lineage.derivedFrom / lineage.evidence`). Selector grammar is the feature
  transform's `output.exclude` grammar (`derivedFrom:` / `scope:` / `block:` / `evidence:`).
- `ScreenMath` — erfc (series + continued fraction), χ²(1) tail and quantile (Acklam inverse normal + one
  Halley step), type-7 quantile, Benjamini–Hochberg, `seededRandom` (murmur3 of seed + key →
  `SplittableRandom`, the feature transform's noise derivation), calendar buckets, name globs, coercions.
- `ScreenRow` — the prepared sample (unit key, identity, time, period, label, baseline, weight, `x[]`) with a
  compact coder. `x` = candidates + the shuffle reference column.
- `GroupScorer` — pure per-unit scoring: sort rows by (time, identity), baseline → p (prob / logProb /
  inverseShare; uniform prior for the grouped family), label normalisation, placebo columns (noise from
  `seededRandom(seed, unitKey + "noise")` drawn in row order; shuffles = Fisher–Yates from
  `seededRandom(seed, unitKey + "shuffle" + j)`), transforms (raw / percentile rank with mean ties / absdev
  from the median), then the contribution per (column, transform) into a `Map<Integer, ScoreAccumulator>`.
  Grouped: centre by the p-weighted mean over observed rows, `S = Σ x̃ (ỹ − p)`, `H = Σ p x̃² − (Σ p x̃)²`,
  scaled by the unit weight, one period per unit. Binomial: per-row moment sums (c1..c5, per row period) that
  `ScreenReport.stats` centres at the end — offset mode `S = c1 − x̄ c2`, `H = c3 − c4² / c5` with
  `c = Σ w x r, Σ w r, Σ w v x², Σ w v x, Σ w v` (`r = y − p`, `v = p(1 − p)`); prior mode (no baseline)
  `H = ȳ(1 − ȳ)(c3 − c4² / c5)` from raw moments. Both modes profile the intercept out.
- `ScoreAccumulator` — 8 slots (`S`, `H`, `N_OBS`, `C1..C5`) for the window plus a `TreeMap` of the same per
  period, min / max time; key `BOOKKEEPING_KEY = -1` reuses the slots for run counts (`ROWS_IN`,
  `ROWS_TIME_FILTERED`, `ROWS_INVALID`, `UNITS_SCORED`, `UNITS_SKIPPED`, `ROWS_SCORED`). Custom coder;
  `Fn` is the Combine (input = accumulator = output).
- `ConditioningScorer` — pure per-unit computations of the conditioning passes: `moments` (standardisation sums
  of F), `design` (F̃, missing → 0, intercept column for the binomial family), `fitted` (softmax of log p + F̃θ
  within the group / σ(logit p + F̃θ)), `evaluate` (`[units, ll, g, G]` at θ), `partial` (`[s, b, a]` per column x
  transform at p̂: s = x̃'(ỹ − p̂), b = x̃'Wx̃, a = F̃'Wx̃, W the Fisher metric — block diagonal diag(p̂) − p̂p̂' with x̃
  centred by p̂ within the group, or diag(p̂(1 − p̂)) with the intercept doing the centring).
- `FitState` — the Newton controller state (proposal, best point with its ll / g / G, direction, step size,
  convergence, history) and `advance(eval, l2, tol)`: accept when the penalised average objective did not
  decrease and propose a full Newton step `(G/n + l2 I) d = g/n − l2 θ` (`MatrixOps.solveGram`), else halve the
  step from the best point; converged when the direction or the improvement is below tolerance, or the step
  size fell under 1e-3.
- `ScreenReport` — `stats` per slot array, `partial` (γ = (G + l2·N·I)⁻¹a, S⊥ = s − γ'g, H⊥ = b − 2γ'a + γ'Gγ,
  r²_F = 1 − H⊥/b; fully explained columns are degenerate with r²_F = 1), `build` (records + summary maps; with
  conditioning `passed` / threshold / q-values follow the partial test), output schemas, `describe`.
- `ScreenStages` — `Prepare` (time window, validity, identity; bookkeeping side output) → units (`Group` =
  `GroupByKey` on the unit key, or one row per unit) → `ScoreUnits` (bundle-local accumulator map per window
  flushed at `@FinishBundle` = a partial combine, so the shuffle carries keys × bundles elements, not rows ×
  columns) → `Combine.perKey` → `Gather` (`Combine.globally` of the few combined accumulators) → `Finalize`
  (records to the default output, one summary to `summary`). With conditioning: `ConditioningMoments` (one
  global Combine → singleton view), `ConditioningInit` (`Create` of the initial state → view), then for
  `it = 1..maxIter` `ConditioningFit<it>` (a ParDo over the units with the moments and the previous state as
  side inputs, bundle-local sum, `Combine.globally` with defaults so a skipped pass yields an empty vector) →
  `ConditioningFit<it>_Advance` (the controller on a copy of the previous state) → view; finally
  `ConditioningPartial` (bundle-local `[s, b, a]` per key → `Combine.perKey` → map view) and the
  fit view join `Finalize` as side inputs. `engineConstraints` rejects conditioning outside the global window
  (the Combines carry defaults).

`module/transform/ScreenTransform` is thin: streaming rejected, parse → lineage → resolve, `describe` to the
log, outputs `MCollectionTuple.of(records).and("summary", ...)`.

## 3. Decisions

- **Key per (column, transform), not one big accumulator.** The proposal's single accumulator of
  (candidates × transforms + placebos) × (1 + periods) × slots is a few MB per partial; keying by column keeps
  each accumulator at periods × 8 doubles and lets the combiner distribute. Bundle-local pre-aggregation in the
  score DoFn keeps the element count at keys × bundles.
- **Centring is algebraic for the binomial family.** The p-weighted mean of x is not known in one pass; the
  moment sums are centred in `stats`, so the binomial path is one pass like the grouped one (the grouped
  family centres inside the group where all rows are in hand).
- **Placebos go through every transform** and one pooled quantile is the threshold (the multiplicity the
  threshold must absorb is candidates × transforms). `thresholdTheoretical` (χ²(1) quantile / 2N) is always
  reported next to it as the calibration check, and is the threshold when no placebo column is configured.
- **Determinism.** Rows are sorted by (time, identity) before any draw; identity = declared `rowId` fields or
  every field value in name order. Re-runs and engine differences (bundle boundaries, worker count) cannot
  change a placebo column.
- **Roles and lineage come from the feature manifest**, no new mechanism on the feature side: the manifest
  already carries `roles` (group / label / baseline / weight), `timeField` and per-column lineage. Direct
  upstream: the schema's `feature.*` options.
- **Independent rows support `raw` only** (`rank` / `absdev` over the window need a quantile sketch — PR 3,
  `KllDoublesSketch` as in the profile sink). `inverseShare` and `shuffle` need a group.
- **Weight**: per row (binomial), unit mean (grouped) — the conditional logit has one likelihood term per group.
- **Units skipped, not rows**: a group without a positive label or with an invalid baseline (for its form) is
  skipped whole and counted (`nUnitsSkipped`); a row with a null label / group / weight is invalid
  (`nRowsInvalid`); a null time is a failure (routed to the error handler).
- **Batch only, windowing respected.** Every stage is a Combine in the module's windowing strategy, so a
  fixed window yields one record set per window (the streaming case is rejected at assembly: the summary and
  q-values need the whole window).
- **Conditioning is `maxIter + 2` passes, one Combine each.** Beam cannot iterate, so the Newton fit is
  unrolled at graph construction; the controller (`FitState.advance`) runs after every pass on the pass's
  `[n, ll, g, G]` and decides what the next pass evaluates. Backtracking is folded into the same pass kind:
  a rejected step halves the step from the best point and costs one more pass, never a second kind of pass
  (the proposal's separate line-search pass is unnecessary). Converged iterations are skipped inside the
  DoFn (empty vector out) — the pass still exists in the graph but reads nothing. The orthogonalisation and
  the partial test collapse into one pass because both are bilinear in x: `[s, b, a]` at p̂ plus the fit's
  (g, G) give γ, S⊥, H⊥ and r²_F in closed form, so the proposal's two extra passes are one. Penalty and
  tolerance are on the *average* log-likelihood (per weighted unit, the unit count carrying the same weight as the sums), so `l2` and `tol` are size-free and weight-scale-free; F is
  standardised so `l2` is scale-free; the binomial family always carries an intercept in F̃ (a calibration
  shift beyond the baseline, the prior rate without one), which also centres the partial statistics.
- **The pass list is one JSON document with `columns` first.** `ScreenReport.selection` writes what
  `FeaturePlanService.parseIncludeList` reads (`{columns: [...]}`), plus the provenance a consumer needs to
  trust it: the effective test (`partial` / `marginal`), thresholds, the upstream manifest's `planHash` /
  `outputHash` (from `candidates.manifest`), `screenHash` (SHA-256 of the canonical parameters without the
  file locations, sharing `FeaturePlanCompiler.canonical` / `sha256` — the same 16-hex width — with the feature
  plan hash) and the passing
  records' statistics; NaN statistics are written as null. Written from the finalize step (global window only)
  through `ResourceUtil.writeString`; a write failure fails the step. `ScreenSelectionIncludeTest` pins the round trip.
- **Row families share one moment-sum path.** `binomial`, `gaussian` and `poisson` differ only in the Fisher
  weight at the baseline mean (μ(1 − μ), 1, μ — `ScreenSpec.fisherWeight`, the one definition the marginal
  moments, the conditioning fit and the report's prior mode share — the report applies it at ȳ in prior mode, and
  the label variance for gaussian); gaussian carries one extra slot (`C6 = Σ w r²`) for the residual
  variance, so its statistic is scale-free without a second pass. The conditioning fit uses the matching link
  (logit / identity / log) and likelihood; without a baseline the intercept starts at the link of the weighted label
  mean (`ConditioningScorer.initialTheta`, from two extra slots of the moments pass) so the first pass sits at the
  prior-mean model — at θ = 0 the poisson mean is 1 and the first step on the intercept is log ȳ in one jump,
  which overshoots for count labels with a large mean and wastes the pass budget on step halvings. The gaussian
  fit is least squares at σ² = 1 (one Newton step); the partial test and `conditioningGain` divide by the residual
  variance at the fit (`SIGMA_KEY` sums from the partial pass), and an exact fit (σ² = 0) falls back to the
  marginal test with a note.
- **Hardening after the four PRs** (review follow-ups): `MatrixOps.checkMatrix` rejects non-finite input
  (ojalgo's SVD fallback never terminates on NaN — the screen guards its own inputs, the shared solver now
  guards every caller); the report orthogonalises every column with one multi-right-hand-side `solveGram`
  (`ScreenReport.gammas`: one Cholesky of the fit's Gram matrix instead of one per column x transform); the
  Newton passes read a projection of the units (`ProjectDoFn`: label, baseline, weight, F — `ConditioningScorer`
  takes the F offset, 0 for projected rows) so `maxIter` passes never re-read the candidate columns; the
  prepare step accumulates the run counts per bundle and window (one element per bundle on the bookkeeping
  key, not one per row) and ships the row identity as a 128-bit murmur3 hash instead of the whole record;
  `ScreenMath` delegates its randomness, quantile and coercions to `FeatureValues` / `OrderStatistics`, so the
  screen accepts exactly the time formats the feature transform accepts; the feature transform rejects an
  empty `output.include` (`output.include.empty`) instead of emitting a table without feature columns.
- Field names follow the proposal (`est_gain`, `n_groups`, `period_z`, `leakSuspect` …) with additions that
  cost nothing now and keep later extensions schema-compatible: `df` (block tests), `pValue` / `qValue`,
  `degenerate`, `family`, and the summary's `passedColumns` (the selection list of PR 4).

## 3.1 DirectRunner note

The unrolled conditioning graph is large (each pass = ParDo + global Combine + controller + view) and the
DirectRunner processes a GroupByKey output as one bundle per key. Two DirectRunner mechanisms then dominate
the run time, independently of the data size: its immutability enforcement traverses the whole pipeline
graph once per bundle (`ImmutabilityEnforcementFactory.isReadTransform`, CPU-bound with 16 workers for a
1,200-row test), and its watermark manager updates every downstream transform per completed bundle. The e2e
tests therefore disable `enforceImmutability` (as the Spanner / Datastore ITs do) and use small datasets. The
state chain uses singleton views over default-carrying `Combine.globally` (every pass yields exactly one
element); a variant with list views and `withoutDefaults` Combines ran an order of magnitude slower on the
DirectRunner (its quiescence driver spun on pushed-back bundles). None of this applies to Dataflow, where bundles are large and side inputs are
materialised once — measure conditioning there or on the prism image, never on direct (engine doc §9.5 has
the same finding for keyed feature stages).

## 4. Deferred (design position)

- **Independent-row `rank` / `absdev`** need a window-wide quantile sketch (one KLL pass, `KllDoublesSketch` as
  in the profile sink) before the score pass; the grouped transforms stay exact.
- Block tests (`df > 1`) for categorical / vector candidates (one-hot blocks, embeddings): `S` vector, `H`
  matrix, χ²(k); the record schema already has `df`.
- Windowed / streaming marginal screen (sliding-window drift monitoring): the marginal path is one Combine
  and could run under a trigger; conditioning stays batch.
- Declared interaction probes (`cross:<field>` transform variants) — bounded by declaration only.
