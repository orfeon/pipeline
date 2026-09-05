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
| `ScreenSpec` | parse (`parse(JsonObject)`, every error collected) and resolve (`resolve(schema, lineage)`: manifest role defaults, candidate / conditioning column selection, `parametersHash`); `Lineage` (schema options or manifest); the family's `fisherWeight` / `link` — the single definition the scorers and the report share | no |
| `ScreenMath` | erfc (series + continued fraction), χ²(1) tail and quantile (Acklam inverse normal + one Halley step), Benjamini–Hochberg, calendar buckets, name globs; randomness, quantile and coercions delegate to `FeatureValues` / `OrderStatistics` | no |
| `ScreenRow` | the prepared sample (unit key, identity, time, period, label, baseline, weight, `x[]` = candidates, the shuffle reference, the conditioning columns) with a compact coder; `conditioningOnly` = the projection the fit passes read | coder only |
| `GroupScorer` | per-unit marginal scoring: `prepare` (sort, baseline → mean, labels, weights), `columns` (candidates + placebos), transforms, the family's contribution into `ScoreAccumulator`s | no |
| `ScoreAccumulator` | 9 slots (`S`, `H`, `N_OBS`, `C1..C6`) for the window plus the same per period, min / max time; the bookkeeping key reuses the slots for run counts; custom coder; `Fn` (input = accumulator = output) | coder + CombineFn |
| `ConditioningScorer` | per-unit conditioning computations: `moments`, `initialTheta`, `design`, `fitted`, `evaluate` (`[n, ll, g, G]`), `partial` (`[s, b, a]`, plus the gaussian variance sums) | no |
| `FitState` | the Newton controller (proposal, best point, direction, step size, convergence, history); `advance(eval, l2, tol)` | Serializable |
| `VectorAccumulator` | element-wise sum of fixed-length vectors (the conditioning passes), empty = identity; coder + `Fn` | coder + CombineFn |
| `ScreenReport` | `stats` per slot array, `gammas` + `partial` (the orthogonalisation), `build` (records + summary), `selection` (the pass list), the output schemas, `describe` | no |
| `ScreenStages` | the graph (§2–§4) and its DoFns | yes |
| `ScreenTransform` | thin: streaming rejected, parse → lineage → resolve → `engineConstraints`, `describe` to the log, two outputs | module |

Invariant: nothing Beam-specific reaches the pure classes, and the pure classes are what the hand-computed
tests pin (§7).

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
- **ScoreUnits** calls `GroupScorer.score` per unit into a bundle-local `Map<Integer, ScoreAccumulator>` per
  window, flushed at `@FinishBundle`: a partial combine, so the shuffle into `Combine.perKey` carries keys ×
  bundles elements, not units × columns. Keys are `column × transforms + transform` (columns = candidates,
  noise placebos, shuffle placebos); the bookkeeping key is −1.
- **Combine.perKey** merges the accumulators (slot sums, period maps, time range). Keying per (column,
  transform) instead of one large accumulator keeps each accumulator at periods × 9 doubles and lets the
  combiner distribute.
- **Gather** collects the few combined accumulators into one list (`Combine.globally`; in the global window
  the default empty list still fires, so the summary is emitted on an empty input) and **Finalize** runs
  `ScreenReport.build` once, emitting every scoring record to the default output and one summary to the
  `summary` output, then writes the pass list when `output.selection` is set (`ResourceUtil.writeString`; a
  failure fails the step).

### 2.1 What the scorer computes

`prepare` sorts the unit's rows by (time, identity), derives the baseline mean per row from the form
(shares normalised within the group for the grouped family, probabilities clamped for binomial, rates
positive for poisson), normalises the grouped labels, and takes the weights. `columns` builds the candidate
matrix and the placebo columns: noise from `seededRandom(seed, unitKey + "noise")` drawn in row order,
shuffles by Fisher–Yates from `seededRandom(seed, unitKey + "shuffle" + j)` over the reference column.
For each column × transform the family's contribution is added: the grouped family centres by the p-weighted
mean over the observed rows and adds `w·S_g`, `w·H_g` for the unit's period; the row families add the raw
moment sums `c1..c6` per row period (`ScreenReport.stats` centres them and applies the prior-mode weight).

## 3. Windowing and constraints

Every stage is a Combine in the module's windowing strategy: a fixed window yields one record set per
window; the score DoFn keeps one accumulator map per window. `engineConstraints` rejects a triggered input
(each Combine would fire once per pane, several partial summaries, and the conditioning singleton views
break) and a non-global window with conditioning or `output.selection`; streaming is rejected by the module.

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
- **Projection.** The fit passes read `ScreenRow.conditioningOnly` (label, baseline, weight, F, identity
  cleared): `maxIter` passes over the conditioning columns only; `ConditioningScorer` takes the F offset (0
  for projected rows, `spec.conditioningOffset()` for full rows). The moments and partial passes read the
  full rows.
- **Partial pass** (one pass): at the fitted p̂, `[s, b, a]` per column × transform into a bundle-local map
  (plus the gaussian variance sums under `SIGMA_KEY`), then `Combine.perKey` and a map view. The
  orthogonalisation and the partial test collapse into this one pass because both are bilinear in x: with the
  fit's (g, G), `ScreenReport.gammas` solves γ for every column at once (one Cholesky of G, a
  multi-right-hand-side `solveGram`; a column with no information or a non-finite right-hand side stays out
  and is reported degenerate) and `partial` reads S⊥, H⊥ and r²_F in closed form.

Total: `maxIter + 2` passes at most, each a global Combine, independent of the data. The gaussian fit is
least squares at σ² = 1 (one Newton step); the report divides the partial statistics and the gain by the
residual variance at the fit, and falls back to the marginal test when that variance is zero.

## 5. Determinism and failure routing

Every random draw derives from the seed and the unit key (dsl doc §5); rows are sorted before any draw, so
bundle boundaries and worker counts cannot change a placebo column. Every DoFn catches per-element errors
into the failure output (`Module.processError`) under `failFast`; the finalize step's pass-list write is the
one deliberate hard failure (the list is a primary deliverable).

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
`2 + k`. Nothing is data-dependent in size except the number of period buckets.

## 7. Tests

- Pure, hand-computed: `ScreenMathTest` (tails, quantiles, BH, buckets, globs, the delegated coercions),
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
document §12: independent-row `rank` / `absdev` (a KLL pass), block tests (`df > 1`), `passRule: fdr`,
precision weights, a windowed marginal screen under a trigger, declared interaction probes. Engine-side
refactors judged larger than their value so far: a `Family` enum in place of the string switches, σ² carried
in `FitState` instead of the partial map, a typed summary record instead of the map the selection reads.
Outside the repository: the numerical acceptance against the proposer's reference implementation and the
Dataflow measurement of conditioning on production-sized data.
