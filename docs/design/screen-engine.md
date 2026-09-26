# Screen Transform Engine (Design Document)

Status: **Implemented** — the Beam execution of the contract in [screen-dsl.md](screen-dsl.md): one bounded
Combine per statistic, the unrolled Newton passes of the conditioning fit, the pass-list write, and the
runner findings behind the test setup. §8 lists what is deferred. Code: `util/pipeline/screen/` and
`module/transform/ScreenTransform.java`; the tests are the `Screen*Test` / `*ScorerTest` classes named in §7.

## 1. Layout: pure computation and Beam wiring

The package separates what a statistician can read and test without Beam from the Beam graph, as the
feature transform does (engine doc §1.2):

| class | role | Beam |
|---|---|---|
| `ScreenSpec` | parse (`parse(JsonObject)`, every error collected) and resolve (`resolve(schema, lineage)`: manifest role defaults, candidate / conditioning column selection, `parametersHash`); the family's `fisherWeight` / `link` delegate to the shared `Family` | no |
| `FeatureLineage` (`util/pipeline/feature/`) | the feature transform's lineage as a downstream transform reads it: `fromSchema` (the `feature.*` field options), `fromManifest`, the role defaults, the selector matching (`derivedFrom:` / `scope:` / `block:` / `evidence:` / `kind:`) and the numeric-column rule; shared with the evaluation transform | no |
| `glm.Family` / `glm.Baselines` / `glm.GlmFit` (`util/pipeline/glm/`) | the vocabulary shared by the supervised transforms: the families with their baseline forms, Fisher weight and link; the baseline → mean-per-row conversion and the grouped label normalisation (`Baselines.means` / `normalizeLabels`, with the `Skip` reasons); the offset GLM's fitted means and Newton pass evaluation `[n, ll, g, G]` (`GlmFit.fitted` / `evaluate`) | no |
| `glm.StatMath` | erfc (series + continued fraction), χ²(1) tail and quantile (Acklam inverse normal + one Halley step), Benjamini–Hochberg, calendar buckets, name globs; the sample quantile delegates to `OrderStatistics`, and the scorers / Prepare use the feature transform's `FeatureValues` directly for randomness and coercions | no |
| `glm.SpecJson` | the lenient parameter readers (`string` / `number` / `strings` / `parseInstant`, …) the specs share | no |
| `glm.SketchAccumulator` | one mergeable KLL quantile sketch (k = 400) with `rank` (mid-rank fraction), `quantile`, `median` (type-7 below k values), `edges`, read lock-free from a sorted view built once (a side input is read by several bundles); coder + `Fn`; shared with the evaluation transform's calibration tables | coder + CombineFn |
| `WindowQuantiles` | the window's sketches, one per candidate (`rank` / `median` / `quantile` / `edges` by column): the rank / absdev reference of independent rows and the value-bin edges of DSL §12.1; coder + `Fn` (input = accumulator = output) | coder + CombineFn |
| `ScreenRow` | the prepared sample (unit key, identity, time, period, the heterogeneity modifier's level, label, baseline, weight, `x[]` = candidates, the shuffle reference, the conditioning columns) with a compact coder; `conditioningOnly` = the projection the fit passes read | coder only |
| `GroupScorer` | per-unit marginal scoring: `prepare` (sort, the rows `baseline.invalid: dropRow` rejects removed and counted, baseline → mean, labels, weights), `columns` (candidates + placebos), transforms (within the unit, or against the window sketches), the family's contribution into `ScoreAccumulator`s; the binned block's bin assignment (`bins`: value edges cached per column from the sketches, exact normal quantiles for a noise placebo, position bins from the within-unit rank) and its per-bin sums (`binnedRowContribution` / `binnedGroupedContribution`); the candidates' joint sums under `JOINT_KEY` (`addJoint`, in place into the accumulator's vector laid out by `JointLayout`: S, the packed Fisher block and the packed pHd block over the joint columns, DSL §9.5); the pair grids' edges by x column (`gridEdges`, the conditioning columns' sketches) | no |
| `ScoreAccumulator` | 9 slots (`S`, `H`, `N_OBS`, `C1..C6`) for the window plus the same per period — and per heterogeneity level, kept in the period map under `LEVEL_PREFIX` and fed by `addSlice` (no total), so merge and coder are unchanged — min / max time, and a variable-length `extra` vector for the window (the binned block's per-bin sums, DSL §6.1: row families `[Σ w, Σ w r, Σ w v]` per bin + the totals, grouped `[S_b, P_b, (P P')_bb']`); the bookkeeping key reuses the slots for run counts; custom coder; `Fn` (input = accumulator = output) | coder + CombineFn |
| `ConditioningScorer` | per-unit conditioning computations: `moments`, `initialTheta`, `design`, `fitted` and `evaluate` (`[n, ll, g, G]`, both delegating to `GlmFit`), `partial` (`[s, b, a]` per column, per period too, plus the gaussian variance sums and the fit's `[n, g, G]` per period under `FIT_PERIOD_KEY`; for the binned block `[s (B), H (B² / B), A (B × k)]` without period slices; for a declared pair the product of two standardised design columns — a placebo pair the first member times a noise column — as one more `[s, b, a]` column under `pairKey`, no slices; for a real pair its 2-D grid at the fitted means under `pairGridKey` (`pairGrid`: the members' raw values binned by their sketch edges, the K = k² cells as a one-hot block)) | no |
| `PartialAccumulator` | one variable-length vector for the window plus the same per period and per heterogeneity level (`addSlice` under `LEVEL_PREFIX`, as the score accumulator) — the partial pass's shape; custom coder; `Fn` | coder + CombineFn |
| `glm.FitState` | the Newton controller (proposal, best point, direction, step size, convergence, history); `advance(eval, l2, tol)` | Serializable |
| `glm.VectorAccumulator` | element-wise sum of fixed-length vectors (the conditioning passes), empty = identity; coder + `Fn` | coder + CombineFn |
| `ScreenReport` | `stats` per slot array, `binnedStats` / `blockChi2` (the block test: active bins, one reference dropped, Cholesky on the reduced system, χ²(df)), `gammas` + `partial` (the orthogonalisation) and `blockPartial` (the block's Γ, S⊥, H⊥, r²_F = 1 − tr H⊥ / tr H), `heterogeneity` (the level slices' Σ S_l² / H_l − (Σ S_l)² / Σ H_l, marginal from the score slices and partial from the partial slices), `suggestions` (the one-candidate recipes from the binned sums: `contrastChi2` along a bin-constant contrast, `shapes`, `isotonic`, the discovery / confirmation halves of the doubled `extra` vector, per-kind placebo cuts; `Bins` = the representatives / edges callbacks the finalize step builds from the sketches), `joint` (the several-candidate suggestions from the joint sums: the centred S / H / M, the pHd eigenpairs through `SymmetricEigen`, the redundancy clusters, the forward selection and the composite), `interactions` (a real pair's shape from its 2-D grid: the best depth-2 tree of split gains, its share of the grid's block χ², the sides' asymmetry, DSL §8.7), `build` (records + summary + suggestions, the placebo cut per statistic kind — df1 / binned / het / pair; the pair records from the pair keys' partial sums through the same `gammas` / `partial`), `selection` (the pass list, `passedPairs` apart from `columns`), the output schemas, `describe` | no |
| `ScreenStages` | the graph (§2–§4) and its DoFns | yes |
| `ScreenTransform` | thin: streaming rejected, parse → lineage → resolve → `engineConstraints`, `describe` to the log, three outputs (records, `summary`, `suggestions`) | module |

Invariant: nothing Beam-specific reaches the pure classes, and the pure classes are what the hand-computed
tests pin (§7). The `glm` package and `FeatureLineage` are the parts the evaluation transform shares: the
screen classes own only the screening statistic (score test, placebos, transforms, partial test, pass list).

## 2. The marginal graph

```
input ─ Prepare ─┬─ rows KV<unitKey, ScreenRow> ─ Group (GBK) or Units (one row each) ─ ScoreUnits ─┐
                 └─ bookkeeping KV<-1, ScoreAccumulator> (one per bundle and window) ───────────────┴─ Flatten
                     ─ Combine.perKey(ScoreAccumulator.Fn) ─ Gather (Combine.globally, list) ─ Finalize ─┬─ records
                                                                                                          └─ summary
```

- **Prepare** reads the element into a `ScreenRow`: the time (from `time.field`, with its schema type; else
  the element timestamp, the bounded-source sentinel kept out of the time range), the window filter, the
  label (field or expression), the group, the weight, the baseline, the numeric columns (NaN = missing), the
  period bucket, and the identity — a 128-bit murmur3 hash of the `rowId` fields (else of every field value in
  name order; bytes Base64-encoded). Invalid rows and time-filtered rows are counted, not emitted; a null time
  is a failure routed to the error handler. The run counts are accumulated per bundle and window and emitted
  once per bundle on the bookkeeping key, so the shuffle carries bundles, not rows, on that key.
- **Units** are the GroupByKey output for a grouped run, or one row each otherwise (`SingletonUnitDoFn`), the
  same `KV<String, Iterable<ScreenRow>>` type for every pass.
- **WindowQuantiles** (independent rows with `rank` / `absdev`, value bins of the binned test, a pair's
  2-D grid): one pre-pass over the rows — `QuantilesDoFn` feeds the `sketchedColumns()` of x (the candidates
  and the shuffle reference when their sketches are read, the pair members' conditioning columns when a pair
  shape is asked for; a sketch index is the x column, the others stay empty) of the rows
  that will be scored (a row whose baseline is invalid for its form is skipped) into per-bundle sketches, flushed at `@FinishBundle` per window, then
  `Combine.globally(...).asSingletonView()` (a default-carrying singleton per window, so a fixed-window run
  gets one reference per window). `ScoreUnits` and, under conditioning, `ConditioningPartial` read the view
  and hand it to the scorers (`withWindowQuantiles`); the transform dispatch is `GroupScorer.transform(spec,
  quantiles, column, transform, values)` — within the unit when the sketches are null or the run is grouped
  (a grouped run carries them for the value bins / pair grids only), else the sketch rank /
  window median for a candidate and the exact normal cdf / |x| for a noise placebo. One more read of the
  input; nothing else in the graph changes.
- **ScoreUnits** calls `GroupScorer.score` per unit into a bundle-local `Map<Integer, ScoreAccumulator>` per
  window, flushed at `@FinishBundle`: a partial combine, so the shuffle into `Combine.perKey` carries keys ×
  bundles elements, not units × columns. Keys are `column × transforms + transform` (columns = candidates,
  noise placebos, shuffle placebos); the bookkeeping key is −1.
- **Combine.perKey** merges the accumulators (slot sums, period maps, time range). Keying per (column,
  transform) instead of one large accumulator keeps each accumulator at periods × 9 doubles and lets the
  combiner distribute.
- **Gather** collects the few combined accumulators into one list (`Combine.globally`; in the global window
  the default empty list still fires, so the summary is emitted on an empty input) and **Finalize** runs
  `ScreenReport.build` once, emitting every scoring record to the default output, one summary to the
  `summary` output and, under `suggestions: true`, the one-candidate suggestions to the `suggestions` output
  (the finalize step reads the window sketches view for the bins' representatives and edges), then writes
  the pass list when `output.selection` is set (`ResourceUtil.writeString`; a failure fails the step).

### 2.1 What the scorer computes

`prepare` sorts the unit's rows by (time, identity), derives the baseline mean per row from the form
(shares normalised within the group for the grouped family, probabilities clamped for binomial, rates
positive for poisson), normalises the grouped labels, and takes the weights. `columns` builds the candidate
matrix and the placebo columns: noise from `seededRandom(seed, unitKey + "noise")` drawn in row order,
shuffles by Fisher–Yates from `seededRandom(seed, unitKey + "shuffle" + j)` over the reference column.
For each column × transform the family's contribution is added: the grouped family centres by the p-weighted
mean over the observed rows and adds `w·S_g`, `w·H_g` for the unit's period; the row families add the raw
moment sums `c1..c6` per row period (`ScreenReport.stats` centres them and applies the prior-mode weight).
The binned block (`transforms: [binned]`) assigns every row a bin — value bins from the window sketches'
edges (cached per column while the sketches are set; a noise placebo's edges are the exact normal quantiles,
a shuffle placebo's its reference column's), position bins from the within-unit rank, the missing bin for a
non-finite value — and adds the block's sums to the key's `extra` vector (no period slices; `N_OBS` alone
goes to the slots), which `ScreenReport.binnedStats` turns into S_b / H_bb and the χ²(df) statistic.

## 3. Windowing and constraints

Every stage is a Combine in the module's windowing strategy: a fixed window yields one record set per
window; the score DoFn keeps one accumulator map per window. `engineConstraints` rejects a triggered input
(each Combine would fire once per pane, several partial summaries, and the conditioning singleton views
break), a non-global window with conditioning or `output.selection`, and a merging (session) window when
independent rows use `rank` / `absdev` (the WindowQuantiles view is a side input, which a merging WindowFn
cannot map); streaming is rejected by the module.

## 4. The conditioning graph

```
rows ─ ConditioningMoments (per-bundle sums) ─ Combine.globally ─ singleton view ──────────────┐
Create(k) ─ ConditioningInit (initial θ from the moments) ─ singleton view = state₀              │
units ─ ConditioningProject (label, baseline, weight, F) = fitUnits                              │
for it in 1..maxIter:                                                                            │
  fitUnits ─ ConditioningFit<it> [side: moments, state_{it-1}] ─ Combine.globally ─ Advance<it> [side: state_{it-1}] ─ view = state_it
units ─ ConditioningPartial [side: moments, state_max] ─ Combine.perKey ─ map view ─┐
Gather ─ Finalize [side: state_max, partial map] ─ records / summary / selection ◄─┘
```

- **Moments** (one pass over the rows): `[n, Σ, Σ²]` per conditioning column over finite values, then
  `[Σ w y, Σ w]` — the standardisation of F and the starting intercept (the link of the weighted label mean;
  θ = 0 for the grouped family and in offset mode).
- **Newton, unrolled.** Beam cannot iterate, so the fit is `maxIter` passes at graph construction. Each pass
  evaluates `[n, ll, g, G]` at the state's proposal (per-bundle sums, then `Combine.globally`), and the
  controller (`FitState.advance` on a copy of the previous state) decides what the next pass evaluates:
  accept when the penalised average objective did not decrease and propose a full Newton step
  `(G/n + l2·I) d = g/n − l2·θ` (`MatrixOps.solveGram`); otherwise halve the step from the best point — so a
  rejected step costs one more pass, never a second kind of pass. Converged when the direction or the
  improvement falls below tolerance or the step size below 1e-3; a non-finite evaluation at the start ends
  the fit with no best point, later ones are rejected, and no non-finite matrix reaches the solver. Converged
  iterations evaluate nothing (an empty vector out) — the pass still exists in the graph but reads nothing.
- **Singleton views over default-carrying Combines.** Every pass yields exactly one element (an empty vector
  when nothing was evaluated), so the state chain never has an unready or empty view. A variant with list
  views and `withoutDefaults` Combines ran an order of magnitude slower on the DirectRunner (§6).
- **Projection.** The fit passes read `ScreenRow.conditioningOnly` (label, baseline, weight, F; the identity
  kept as the sort tie-break, so the per-unit sums are order-stable): `maxIter` passes over the conditioning
  columns only; `ConditioningScorer` takes the F offset (0
  for projected rows, `spec.conditioningOffset()` for full rows). The moments and partial passes read the
  full rows.
- **Partial pass** (one pass): at the fitted p̂, `[s, b, a]` per column × transform into a bundle-local map of
  `PartialAccumulator`s — the window total and, with `periods`, the unit's (grouped) or each row's (row
  families, bucketed once per unit) period — plus the gaussian variance sums under `SIGMA_KEY` and the fitted
  model's `[n, g, G]` per period under `FIT_PERIOD_KEY` (one `GlmFit.evaluate` per grouped unit, the per-row
  sums for the row families; the Gram is carried up to k = 100, `ConditioningScorer.PERIOD_GRAM_MAX_K`), then
  `Combine.perKey` and a map view. The orthogonalisation and the partial test collapse into this one pass
  because both are bilinear in x: with the fit's (g, G), `ScreenReport.gammas` solves γ for every column at
  once (one Cholesky of G, a multi-right-hand-side `solveGram`; a column with no information or a non-finite
  right-hand side stays out and is reported degenerate), `partial` reads S⊥, H⊥ and r²_F in closed form and
  `partialPeriod` the same per bucket with the window's γ (DSL doc §8.2). A binned block's key carries
  `[s (B), H, A (B × k)]` instead (`ConditioningScorer.binnedPartial`, no period slices), and
  `ScreenReport.blockPartial` solves its Γ (k × B) by the same multi-right-hand-side `solveGram`, forms S⊥ /
  H⊥ and takes χ² = S⊥' H⊥⁺ S⊥ over the bins the marginal block kept (DSL doc §6.1). With a heterogeneity
  modifier the pass also keeps the `[s, b, a]` sums and the fit's `[n, g, G]` per modifier level (a slice
  under `LEVEL_PREFIX`, the grouped family's per unit; the row families bucket a unit's rows once into
  (period, level) cells and add each cell's sums to its period and its level), so the partial
  heterogeneity test reads the level slices exactly as the period decomposition does (DSL doc §7.1). A
  declared pair is one more `[s, b, a]` column of this pass (`pairColumn`: the product of the two members'
  standardised design columns, or of the first member and a noise column for a placebo pair), keyed after
  every column × transform key, no slices; the report treats its key like any column's in `gammas` /
  `partial` (DSL doc §8.6).

Total: `maxIter + 2` passes at most, each a global Combine, independent of the data. The gaussian fit is
least squares at σ² = 1 (one Newton step); the report divides the partial statistics and the gain by the
residual variance at the fit, and falls back to the marginal test when that variance is zero.

## 5. Determinism and failure routing

Every random draw derives from the seed and the unit key (dsl doc §5); rows are sorted before any draw, so
bundle boundaries and worker counts cannot change a placebo column. The exception is the WindowQuantiles
sketch (§2): KLL compaction draws from the library's unseeded generator and the merge follows the bundles,
so beyond k values a candidate's window `rank` / `absdev` can move within the rank error between runs.
Every DoFn catches per-element errors into the failure output (`Module.processError`) under `failFast`; the
finalize step's pass-list write is the one deliberate hard failure (the list is a primary deliverable).

## 6. Runner findings

**DirectRunner.** The unrolled conditioning graph is large (each pass = ParDo + global Combine + controller +
view) and the DirectRunner processes a GroupByKey output as one bundle per key. Two of its mechanisms then
dominate the run time independently of the data size: the immutability enforcement traverses the whole
pipeline graph once per bundle (`ImmutabilityEnforcementFactory.isReadTransform`; a 1,200-row test was
CPU-bound on 16 workers for minutes), and the watermark manager updates every downstream transform per
completed bundle. The e2e tests therefore disable `enforceImmutability` (as the Spanner / Datastore ITs do)
and use small datasets; the conditioning e2e went from 160 s to 17 s. None of this applies to Dataflow, where
bundles are large and side inputs are materialised once — measure conditioning there or on the prism image,
never on direct (the feature engine doc §9.5 records the same finding for keyed stages).

**Dataflow.** The marginal path is one shuffle (the GroupByKey) plus small Combines; conditioning re-reads
the materialised units `maxIter + 1` times through the projection. Accumulator sizes: per (column,
transform) key periods × 9 doubles; per Newton pass `2 + k + k²` doubles (k ≤ 500 enforced); per partial key
`(2 + k) × (1 + periods)`, plus one `FIT_PERIOD_KEY` entry of `(1 + k + k²) × (1 + periods)` doubles up to
k = 100 (`(1 + k) × (1 + periods)` beyond); a binned key adds `3B + 3` (row families) or `2B + B²` (grouped)
doubles to its marginal accumulator and `B (1 + B + k)` / `B (2 + k)` to its partial one, B = bins + 1; a
pair costs `2 + k` doubles per partial key, times `1 + pairs.placebo` keys per declared pair, under
`pairs.maxPairs`; the joint sums are one key of `4 + 2m + m(m + 1)` doubles (m = joint columns + noise, under
`joint.maxColumns` = 200: ≈ 40k doubles) with O(m²) work per row — the one opt-in whose per-row cost grows
with the candidate count; the window quantile view is m sketches of a few KB each
(k = 400: about 3 KB per column, so 500 candidates ≈ 1.5 MB, materialised once per worker). Nothing is
data-dependent in size except the number of period buckets.

## 7. Tests

- Pure, hand-computed: `StatMathTest` (`util/pipeline/glm`; tails, quantiles, BH, buckets, globs),
  `GroupScorerTest` (grouped and binomial S / H / chi2 from small groups, scale-shift invariance, baseline
  forms and skips, transforms, placebo determinism, the report's threshold / flags / q-values, spec
  validation, manifest roles and lineage selectors), `FamilyScorerTest` (gaussian and poisson prior / offset
  statistics, form validation, the conditioning links, the gaussian partial with the residual variance, the
  seeded start), `ConditioningScorerTest` (the grouped ll / g / G at θ = 0, the controller's accept / reject /
  halve / skip, convergence with L2, `r2_F = 1` for the conditioned column through `gammas`, weight-scale
  invariance, the non-finite start), `ScreenSelectionIncludeTest` (the pass list round-trips through the
  feature transform's include parser; hash order-independence and location-independence; an empty run
  writes nulls).
- End to end (`ScreenTransformTest`, `enforceImmutability` off): a synthetic online-auction dataset — sessions
  of listings where one sells, the winner drawn from softmax(1.5·f_known + 1.0·f_extra), the baseline the
  *exact* conditional probability given f_known (Monte Carlo over the unobserved f_extra), a continuous and a
  count label driven by f_extra. Asserted: the baseline's own feature is conditioned out and the extra
  information passes; independent rows with an expression label, a time window and quarterly periods;
  conditioning on the baseline's own feature removes it (`r2_F ≈ 1`); the pass list written and read back;
  gaussian / poisson; assembly rejections (unresolvable spec, windowed input with conditioning, triggers).
- Lessons kept in the tests: a baseline that omits a real effect shows attenuation (exact conditioning needs
  the true conditional probability); label expression variables are reserved, so family labels are fields;
  few placebo columns make the pass flag of an unrelated column flip between seeds — assert on |z|.

## 8. Status and deferred

Implemented: everything in the DSL document's §1–§11. Deferred, with the design position recorded in the DSL
document §12: block tests for declared column groups (`df > 1`; the binned block test is built and is the
machinery: `ScreenReport.blockChi2` / `blockPartial` take any one-hot or vector block), `passRule: fdr`,
precision weights, a windowed marginal screen under a trigger, declared interaction probes, the rest of the
in-screen expansion (categorical score tests, a built-in baseline-bin modifier for the heterogeneity test —
the test itself is built over the period buckets and a declared field — a pre-selection of pairs beyond a
declared set (the declared pairs on the conditioning fit's p̂ are built, and the pHd directions over the
joint sums name the members to declare)), pruning between passes against the `pass.minGain` floor (the floor itself is
built: `ScreenSpec.gainCut`, one comparison in the report), the ratio / difference and two-dimensional
interaction-shape suggestions (the one-candidate ones — shape / cut / missing / monotone with the discovery /
confirmation split — and the several-candidate ones — pHd, redundancy clusters, forward selection, composite
over the joint sums — are built as the `suggestions` output) — in the step order of DSL §12.4. Engine-side
refactors judged larger than their value so far: a `Family` enum in place of the string switches, σ² carried
in `FitState` instead of the partial map, a typed summary record instead of the map the selection reads.
Outside the repository: the numerical acceptance against the proposer's reference implementation and the
Dataflow measurement of conditioning on production-sized data.
