# Screen transform — baseline-conditioned feature screening

Status: PR 1 implemented (marginal score test, placebo calibration, periods, time window, leak flags;
families `groupedMultinomial` / `binomial`). PR 2 (conditioning) / PR 3 (gaussian, poisson, independent-row
rank) / PR 4 (`output.selection`) are design positions only. User-facing reference:
`src/main/resources/server/docs/module/transform/screen.md`.

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
- `ScreenReport` — `stats` per slot array, `build` (records + summary maps), output schemas, `describe`.
- `ScreenStages` — `Prepare` (time window, validity, identity; bookkeeping side output) → `Group`
  (`GroupByKey` on the unit key; skipped for independent rows) → `ScoreGroups` / `ScoreRows` (bundle-local
  accumulator map flushed at `@FinishBundle` = a partial combine, so the shuffle carries keys × bundles
  elements, not rows × columns) → `Combine.perKey` → `Gather` (`Combine.globally` of the few combined
  accumulators) → `Finalize` (records to the default output, one summary to `summary`).

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
- Field names follow the proposal (`est_gain`, `n_groups`, `period_z`, `leakSuspect` …) with additions that
  cost nothing now and keep later extensions schema-compatible: `df` (block tests), `pValue` / `qValue`,
  `degenerate`, `family`, and the summary's `passedColumns` (the selection list of PR 4).

## 4. Deferred (design position)

- **PR 2 — conditioning (partial test).** Beam cannot iterate: the Newton fit of the conditional logit on the
  conditioning set F is unrolled to `maxIter` passes at graph construction (no early stop — converged
  iterations become no-op passes), and the backtracking line search becomes "evaluate the log-likelihood at
  several step sizes in the same pass" (two passes per iteration). Recommended default `maxIter` 8–10. Then one
  pass for `F'Wx` per candidate (k-vector per column), the Cholesky solve (`util/domain/math/MatrixOps.solveGram`,
  ojalgo) on the driver side of the DoFn, and one more marginal pass on the orthogonalised x⊥ →
  `partial_gain`, `r2_F`. Placebos take the same route.
- **PR 3 — gaussian / poisson** (`σ²` from `Σ(y − μ)²` in the same accumulator: one pass) and the
  independent-row `rank` / `absdev` via a KLL pass.
- **PR 4 — `output.selection`** (writes `{"columns": [...], "threshold": ..., "planHash": ...}` for the feature
  transform's `output.include`; `passedColumns` in the summary already carries the list).
- Block tests (`df > 1`) for categorical / vector candidates (one-hot blocks, embeddings): `S` vector, `H`
  matrix, χ²(k); the record schema already has `df`.
- Windowed / streaming marginal screen (sliding-window drift monitoring): the marginal path is one Combine
  and could run under a trigger; conditioning stays batch.
- Declared interaction probes (`cross:<field>` transform variants) — bounded by declaration only.
