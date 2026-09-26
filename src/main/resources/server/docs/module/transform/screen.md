---
type: Transform Module
title: Screen Transform Module
description: Baseline-conditioned feature screening before training. Scores every numeric candidate column against the label with a Rao score test of an offset GLM (one closed-form Combine, no learner), so the score is the one-step log-likelihood improvement over an existing prediction. Placebo-calibrated pass threshold (noise and within-group shuffle columns), transform variants (raw / rank / absdev), per-period sign agreement (of the partial test too, and pass.minPeriodsAgree can require it), a practical gain floor (pass.minGain), a time window that fences off the test period, leak-suspect flags, Benjamini–Hochberg q-values. Optional conditioning (partial test) fits an existing feature set by unrolled Newton passes and scores what each candidate adds beyond it (r2_F, partial gain). Families groupedMultinomial (conditional logit within a group), binomial, gaussian and poisson. output.selection writes the pass list the feature transform's output.include reads (closed loop). Batch only.
tags: [transform, screen, feature-selection, machine-learning, statistics, placebo, batch]
timestamp: 2026-09-04T00:00:00Z
---

# Screen Transform Module

Transform module that **ranks candidate feature columns before training**, conditioned on an existing
baseline prediction. It answers one question per column: *does this column explain what the baseline
misses?* — without training a model. It is the supervised counterpart of the `profile` sink (unsupervised
column statistics) and the natural downstream of the [`feature`](feature.md) transform: feature
(generation) → screen (ranking and cut-off before training) → training job.

The statistic is a **Rao score test** of an offset GLM: with the linear predictor η = offset + β·x and the
offset fixed at the baseline, the score S and Fisher information H at β = 0 give the one-step Newton
improvement of the log-likelihood, `est_gain = chi2 / (2N)` with `chi2 = S² / H`. `est_gain` is the *average
log-likelihood improvement per unit* (group or row) — the same unit as an excess log score / logloss
improvement of a model comparison. Every statistic is a single pass over the data (one bounded Combine per
column × transform), so the cost does not depend on the number of candidates in passes, only in work per row.

**It is a ranking and cut-off device, not an acceptance test.** The probe is linear and univariate: a feature
that only works through interactions is invisible (false negative), and a high linear gain can still add
nothing to a tree model (false positive). See [Limits](#limits).

## What it computes

| family | score S | information H | notes |
|---|---|---|---|
| `groupedMultinomial` | Σ x̃ (ỹ − p) | Σ_g [ Σ p x̃² − (Σ p x̃)² ] | conditional logit within a group (the candidates of a search query, the listings of an auction session, the bids of a lot); `p` is the baseline share within the group (sums to 1); `ỹ` is the label normalised to sum 1 within the group |
| `binomial` | Σ x̃ (y − p) | Σ p (1 − p) x̃² | independent rows; `p` is the baseline probability. Without a baseline the prior rate is used and the intercept is profiled out |
| `gaussian` | Σ x̃ (y − μ) / σ² | Σ x̃² / σ² | independent rows, identity link; `μ` is the baseline value (`form: value`), σ² the residual variance around it (the label variance without a baseline). The statistic is invariant to the scale of the label |
| `poisson` | Σ x̃ (y − μ) | Σ μ x̃² | independent rows, log link; `μ` is the baseline rate (`form: rate`, or `logRate` on the log scale). Without a baseline the prior rate is used |

- `x̃` is the candidate centred by the p-weighted mean over the observed rows (within the group for
  `groupedMultinomial`, over the window for `binomial`); a missing value after centring is 0 (no information).
  The statistic is invariant to the scale of x and to a constant shift within the group.
- Output per column × transform: `S`, `H`, `beta = S/H`, `chi2 = S²/H` (χ²(1) under the null), `z = sign(S)·√chi2`,
  `est_gain = chi2 / (2N)`, `pValue` (χ²(1) upper tail), `qValue` (Benjamini–Hochberg over the candidate records).
- Degenerate columns (fewer than two observed rows, a constant within every group — exact for
  `groupedMultinomial` whatever the magnitude, since the group's values are shifted by its first value before
  centring — or, for the row families, whose centring is a difference of moment sums, `Σ x̃²` below 1e-12 of the
  raw second moment: a window-constant column or one whose spread is below 1e-6 of its magnitude; centre such a
  column upstream) get `est_gain = 0` and `degenerate = true`; under conditioning they have no partial test
  either and never pass.
- Weights (`weight`): per row for `binomial`; the row mean of the unit for `groupedMultinomial`.

### Placebo calibration

`est_gain` is a squared statistic: it is positive under the null too (χ²(1) / 2N scale), so **no absolute
threshold applies**. The transform adds placebo columns to the same pipeline — `placebo.noise` standard-normal
columns (`__noise_<i>`) and `placebo.shuffle.n` within-group permutations of a reference column
(`__shuffle_<i>`, marginal distribution kept, alignment broken) — and takes the `placebo.quantile` quantile
of their `est_gain` (pooled over the transform variants of the same statistic kind: `raw` / `rank` / `absdev`
share one cut, the [binned block test](#binned-block-test) has its own) as the pass threshold. The default q99
(not q95) accounts for the candidate × transform multiplicity. Without placebo columns (`noise: 0`, no shuffle)
the theoretical χ²(df) quantile / 2N is used; it is always reported as `thresholdTheoretical` in the summary —
the two agree when the statistic is well calibrated.

The placebo random numbers are derived from `placebo.seed` and the unit key (the group key, or the row identity
for independent rows), so a re-run reproduces the same columns.

### Transform variants

| transform | definition | catches |
|---|---|---|
| `raw` | the column as is | direct linear effect |
| `rank` | percentile rank within the group over the observed (finite) values: `(number of smaller values + half the number of other values tied with it) / (observed count − 1)`, in [0, 1] (an untied minimum reads 0, an untied maximum 1); 0.5 when only one value is observed. With `r` the average 1-based rank and `m` the observed count it is `(r − 1) / (m − 1)` — in pandas `(s.rank() − 1) / (s.count() − 1)`, not `s.rank(pct=True)` (`r / m`: the numerator and the denominator both differ) | monotone non-linear effects, outlier robustness |
| `absdev` | \|x − median of the group\| | symmetric "extremeness" effects |

Records are keyed by (`candidate`, `transform`). `rank` and `absdev` are within-group statistics; with
independent rows (no `group`) the reference is the whole window instead: one extra pass sketches every
candidate (a KLL quantile sketch per column, rank error about 0.8 %), `rank` is the value's mid-rank among
the window's observed values as a fraction of their count — `(values below + half the values equal, itself
included) / n`, in (0, 1) — and `absdev` is `|x − window median|`. Noise placebos are standard normal by
construction and take the exact normal cdf / `|x|`; the summary's `notes` says the sketch was used. The
sketch's compaction is randomised, so beyond 400 values per candidate a re-run can move a candidate's
`rank` / `absdev` z slightly (within the rank error; the placebo columns and every grouped transform stay
exactly reproducible). Session windows cannot carry the window reference (use the global, fixed, sliding or
calendar window). The default without `group` stays `raw` (the pre-pass reads the input once more): list the
transforms to get `rank` / `absdev`.

### Binned block test

`transforms: [raw, binned]` with `bins: {k: 10, edges: value}` tests a candidate as a one-hot block of `k`
bins plus a missing bin (a missing value is a bin of its own, so informative missingness scores), which
catches any univariate shape at bin resolution — a band, a threshold, a U — where the linear probe of `raw`
sees nothing. The record is one χ²(df) statistic without a sign: `chi2`, `df` (active bins − 1), `pValue`,
`est_gain = chi2 / (2N)` on the same scale as the other transforms, and `bin_stats` (per bin: score `S`,
information `H`, weight mass `n` — the shape of the effect across the bins); `S`, `H`, `beta`, `z`, the
period fields and the leak flag do not apply (null / false). Under conditioning the block gets its own
partial test (`partial_chi2`, `partial_df`, `partial_gain`, `partial_pValue`, `r2_F` = the share of the
block's information F explains).

- `edges: value` bins by the window's value quantiles (the same sketch pre-pass as the independent-row
  `rank`, run for grouped input too when the block test asks for it); bin i holds `(edge_{i−1}, edge_i]`.
  `edges: rank` (needs `group`) bins by the row's rank within its group — a *position* bin ("does the
  standing within the group matter"), a different question from the value bins ("does the level matter").
- **Its own threshold.** A df = k − 1 gain is not comparable with a df = 1 gain, so the placebo cut is taken
  per statistic kind: `raw` / `rank` / `absdev` share one, `binned` has its own; each record's `threshold` is
  its kind's cut and the summary / pass list carry the `thresholds` map (`df1`, `binned`), the scalar
  `threshold` staying the df = 1 cut. `pass.minGain` applies to both; `pass.minPeriodsAgree` is a df = 1 rule
  and does not bar the block.
- **Power.** The block spends k − 1 degrees of freedom on what `raw` tests with one: a linear effect passes
  `raw` first; keep `binned` for the shapes `raw` and `rank` miss, and keep `k` small (10 is plenty).
- **Closing the loop.** The record carries `bin_edges` (value bins), and a passing block goes into the pass
  list as a recipe: `passedBlocks` (written with the `binned` transform only; and the `bins` member of its
  `passed` entry) with `k`, `edges` / `rankCuts`, `missingBin` and the fragment
  `{scope: row, type: bin, input: x, edges: [...]}` — the row `bin` op the next feature run adds; `columns`
  keeps the raw column's name. The row op's bins are `[edge_{i−1}, edge_i)` where the screen's are
  `(edge_{i−1}, edge_i]`, so the fragment's edges are the next doubles above `edges` (`10.000000000000002` for
  `10`), written in full: copy them as they are, since a rounded edge moves the rows at it to the other bin.

### Heterogeneity across a modifier

`heterogeneity: periods` or `heterogeneity: {field: segment}` asks, per `raw` / `rank` / `absdev` record,
whether the candidate's effect *differs* across the levels of a modifier — the period buckets, or a
declared field's values. From the levels' own score tests the total splits into the common effect and the
heterogeneity `Σ S_l² / H_l − (Σ S_l)² / Σ H_l` (χ² with levels − 1 degrees of freedom), which catches an
effect that flips sign across segments or periods — invisible to the window statistic, whose sum cancels.

- Record: `het_chi2`, `het_df`, `het_pValue`, `het_gain` (on the `est_gain` scale), `het_levels`, and for a
  field modifier `level_z` (per level: z, S, H, n; for `periods` read `period_z`). With conditioning the same
  decomposition runs on the partial slices (`partial_het_*`) and decides, as for the main statistic.
- **Its own flag.** `het_passed` compares the effective heterogeneity gain with its own placebo cut
  (`thresholds.het`, lifted to `pass.minGain`). It is **never folded into `passed`** — a candidate passes on
  its main effect; the summary and the pass list list the flagged columns apart (`nHetPassed`,
  `hetPassedColumns`). The reading is "cross this candidate with the modifier upstream" (a `cross` op in the
  feature transform), not "select it as is".
- `periods` costs nothing (the period slices are already there); a field modifier keeps one more slice per
  level in every accumulator. For the grouped family a field modifier is a group-level attribute (the
  value of the group's first row). "Does the effect depend on the predicted level" is the same test on a
  field that bins the baseline upstream.

### Suggestions

`suggestions: true` (with `binned` in `transforms`) reads the binned sums as estimates and writes, per
scorable candidate, recipes in the feature transform's vocabulary to the `<name>.suggestions` output:

| kind | what it says | fields |
|---|---|---|
| `shape` | which univariate shape captures the effect: `linear`, `log`, `sqrt`, `rank`, `step` / `hinge` / `abs` at a cut — scored by the share of the block's χ² the shape's contrast captures (in [0, 1]) | `name`, `cut`, `direction`, `share`, `fragment` (e.g. `{scope: row, expr: "abs(x - 20)"}`) |
| `cut` | the best single split (a boosting round's first split) | `cut`, `direction`, `fragment` (a row `bin` with that edge) |
| `missing` | the missing values' own effect against the rest, and the fill value whose bin behaves like them (only when values are missing) | `direction`, `fill`, `fragment` (an `x == null` indicator, or the fill) |
| `monotone` | whether the effect is monotone (the isotonic fit's share) and in which direction, with the sign consistency of the bin effects | `name` (increasing / decreasing), `consistency`, `share` |

- **Honest gain.** Every choice is made on a discovery half of the units (a seeded hash, as the placebo
  columns) and reported on the other half: `share` / `chi2` are the discovery values, `confirmation_chi2` /
  `confirmation_share` / `confirmation_gain` / `confirmation_pValue` the chosen recipe's on the confirmation
  half — the numbers to trust.
- **Calibrated.** Placebo columns go through the same search; each kind's `threshold` is the placebo quantile
  of their confirmation gains (lifted to `pass.minGain`), and `passed` compares the confirmation gain with it.
- **Basis.** Without `conditioning` the recipes are read on the marginal binned sums (what the baseline
  misses). With it they are read on the partial block — both halves orthogonalised against the conditioning
  set — so a recipe says what the conditioning set does not already carry, not a re-encoding of it; `basis`
  names which (`marginal` / `partial`) and, on the partial basis, `r2_F` is the share of the block's
  information the conditioning set carries.
- **Hypotheses.** A suggestion goes into a feature spec and is checked by the next screen or the `evaluation`
  transform; nothing is applied automatically.

### Several candidates (joint)

`joint: true` accumulates the candidates' joint sums — the score vector, the m × m Fisher matrix and the pHd
matrix over the joint columns (every candidate, or `joint.include`; plus a few noise columns for the null
scale) — and writes, to the same `suggestions` output, what a univariate ranking cannot say:

| kind | what it says | fields |
|---|---|---|
| `phd` | the principal Hessian directions: the directions of residual curvature (quadratic effects and interactions in bulk), their loadings naming the candidates involved — a diagnostic, never a pass flag; a real direction loads on candidates, not on the noise columns | `name` direction i, `candidate` the top loading (loadings are scale-free: a column's units do not decide its rank), `chi2` the eigenvalue, `share`, `consistency` the largest noise loading (null without a noise column), `fragment` the top candidates' coefficients in their own units — the recipe is the projection and its square; and the members to declare as `pairs` |
| `redundant` | near-duplicate candidates (\|correlation\| ≥ `joint.redundancy` in the Fisher metric): keep one, or average / project them | `candidate` the strongest member, `fragment` the others, `share` the cluster's smallest \|correlation\| |
| `select` | a forward selection: the candidate that adds most given the already selected set, step by step, while it clears the df = 1 cut — a set that works together | `candidate`, `name` step k, `chi2`, `share` = `confirmation_gain` = the gain given the set (in-sample), `threshold` the cut it cleared, `fragment` "given [...]" |
| `composite` | the best linear combination of the selected set to add to the baseline | `fragment` the row expression, `chi2` / `share` the joint statistic and gain |
| `difference` | a pair whose joint statistic clearly exceeds the better single one (`joint.excess`, default 1.5×, and the other member's gain given the better one clears the df = 1 cut) with equal and opposite standardised coefficients — the label follows `a − b`; at most `joint.pairs` (default 10) pairs | `name` `a - b`, `fragment` `{scope: row, expr: "a - r*b"}`, `chi2` the joint statistic, `share` the excess factor, `consistency` how equal the magnitudes are |
| `ratio` | the same pair when both columns are positive over the window: the difference's log-scale reading (approximate) | `fragment` `{scope: row, expr: "a / b"}` |

These are one-step, in-sample estimates at the null point — hypotheses for a feature spec, checked by the
next screen. With `conditioning` the joint sums are taken at the fitted model and orthogonalised against the
conditioning set (`basis: partial`): the pHd directions, the selection, the composite and the differences /
ratios say what the conditioning set does not already carry, and `select` / `difference` / `ratio` records
carry the named candidate's `r2_F`; the redundancy clusters keep the plain correlation (near-duplicates are
near-duplicates whatever the conditioning set carries). The joint sums cost O(m²) per row and `m(m + 1)` doubles of state: keep `joint.include` to the
candidates worth combining (at most `maxColumns`, default 200). A missing joint value follows the marginal
test's rule — no information: the grouped family centres each column by the unit's p-weighted mean over its
observed rows (a missing value is 0 after centring), a row family shifts each column by its window mean from
the sketch pre-pass (a missing value is 0 after the shift) — so a unit with a missing value stays in the joint
sums. The summary counts the joint's row set: `nJointUnits` (units — rows for a row family — in the sums),
`nJointFilled` (of them, those with a missing joint value filled; a note reports a share above 10%) and
`nJointDropped` (rows a row family had to leave out for want of a window mean: only under a merging window,
which carries no sketch view).

### Categorical candidates

`categorical: {include: [category, condition_grade]}` tests string fields natively — a level → (score,
information) block instead of a one-hot or target encoding upstream. The sketch pre-pass counts every level
exactly (a column past 20,000 distinct levels fails the step: not a categorical candidate), keeps the
`maxLevels` (default 32) most frequent as named levels and folds the rest into `(other)` (a null value is
its own level `(null)`); a screen of categorical candidates alone needs no numeric candidate. The column's
record (`transform: levels`) is the same block test as the
[binned block](#binned-block-test): `chi2`, `df` (active levels − 1), `pValue`, `est_gain`, no sign, the
partial block under conditioning, and `level_z` with each level's contrast against the rest (its signed z,
S, H, n — which levels carry the effect). It has its own placebo kind (`thresholds.levels`): `placebo`
(default 5) columns per candidate whose levels are redrawn from the window frequencies (the marginal
distribution kept, the alignment with the label broken). A passing column goes into the pass list by name
(the feature transform encodes it); for a passing column only, the `suggestions` output adds its `grouping`
(the levels sorted by effect and cut once at the best split — a level grouping, the two groups in the
fragment) and an `onehot` record for every level whose own contrast is strong (|z| ≥ 3), with the feature
transform's row op in the fragment (`{type: indicator, input, values: [level]}`; `== null` for the `(null)`
level; none for the folded `(other)`).

### Periods, time window and leak flags

- `periods` computes S and z per calendar bucket of a time field; `periods_agree / n_periods` counts the buckets
  whose sign matches the overall sign, and `period_z` lists them — the material for reading a decaying effect.
  With [conditioning](#conditioning-partial-test) the partial test is sliced the same way
  (`partial_period_z`, `partial_periods_agree / partial_n_periods`): the marginal slices and the partial slices
  can disagree — a suppressor (marginal ≈ 0, partial strong) has noise for marginal period signs and a
  consistent partial one — so read the agreement of the test that decided `passed`.
- `pass.minPeriodsAgree` makes the period agreement part of the cut: `passed` then requires the effective
  test's `periods_agree ≥ minPeriodsAgree × n_periods` (a share up to 1) or `≥ minPeriodsAgree` (a count above
  1) on top of the placebo threshold; a candidate without a usable period never passes. It is a stability
  filter on top of the calibrated cut, not calibrated by the placebo columns itself; the rule as applied is
  reported as `passRule` in the summary and the pass list.
- `pass.minGain` is a practical floor on the gain: `passed` then requires the effective test's gain above
  `max(threshold, minGain)`. The placebo threshold answers "is it distinguishable from noise" and shrinks with
  the data (the χ²(1) quantile over 2N: ≈ 6.6 / 2N ≈ 3.3 / N on the gain scale at q99); on a large window it lets
  through columns whose gain is real but too small to matter for training. `minGain` is in the unit of
  `est_gain` — the average log-likelihood improvement per unit, the scale a trained model's excess log score is
  reported on — so the same value means the same thing whatever N. With `weight` the gain is weight-scaled (the
  weights multiply S and H, the gain divides by the unit count), so the floor is compared with the mean weight
  times the per-unit gain: normalise the weights to mean 1, or scale `minGain` by the mean weight. The record's
  `threshold` stays the placebo cut (the calibration check); `passRule` names the floor
  (`est_gain > max(threshold, 1.0E-5)`).
- `time.to` (and `time.from`) fence the window: rows outside are not screened (`nRowsTimeFiltered` in the
  summary). Screening the test period is the classic way to leak the evaluation into the selection.
- `flags.leakZ` marks a candidate with |z| above the value as `leakSuspect` (a known leak typically stands out by
  a factor of several over the healthy top). **It is a flag only, never a rejection.** A bare number reads the
  marginal z; `{z, on: partial}` reads the partial z under [conditioning](#conditioning-partial-test) — a leak
  is not explained by the conditioning set, so its partial z stays outsized, while a legitimate but strong
  candidate that overlaps F (a rating that summarises the same history the model already uses) has a large
  marginal z and a modest partial one. Prefer `on: partial` when the marginal top is legitimately far above
  the placebo scale; without a partial test (the fit accepted no point, or a `gaussian` fit left no residual
  variance) the flag falls back to the marginal z and `notes` says so. The partial flag assumes F itself does
  not leak: a candidate F explains (r2_F near 1 — including a conditioning column that also matches
  `candidates`) has a partial z near 0 and is never flagged, so vet F with the marginal flag first.

### Conditioning (partial test)

`conditioning.fields` names an existing feature set F. The transform fits the conditioning model
η = offset + F̃·θ (the conditional logit within the group for `groupedMultinomial`; for the row families a GLM with an
intercept — logistic for `binomial`, least squares for `gaussian` (one Newton step; the residual variance at the fit
scales the partial test), log-linear for `poisson`; F̃ = F standardised, a missing value → the window mean, or under
`conditioning.missing: groupMean` (`groupedMultinomial` only) the unit's baseline-weighted mean of its observed values —
under the baseline the fill at which the missing row adds nothing to the unit's centred design, the rule the candidate
columns already follow; the later Newton passes centre by the fitted probabilities, where it is the closest fixed
value) by Newton's method with an L2 penalty on the
*average* log-likelihood, then orthogonalises every candidate against F in the Fisher metric W of the fitted
model and reads the score test of what is left:

- `r2_F = 1 − x⊥'Wx⊥ / x'Wx` — how much of the candidate F already explains (1 = fully redundant);
- `partial_S`, `partial_H`, `partial_chi2`, `partial_z`, `partial_gain`, `partial_pValue` — the score test of x⊥;
- with `periods`, `partial_period_z` / `partial_periods_agree` / `partial_n_periods` — the same test sliced by
  period with the window's orthogonalisation (the slices add up to the window's partial S and H; a period the
  marginal test cannot score — no observed or within-unit variation of the candidate — has no partial slice either; the per-period
  information is exact up to 100 conditioning columns and, beyond, the window's Gram term shared out by the
  period's unit mass — a note says so; the per-period score and sign are always exact).

With conditioning, `passed`, `threshold` and `qValue` refer to the **partial** test (the placebo columns take
the same route, so the threshold is calibrated for it); the marginal statistics stay in the record. Reading the
two together classifies a candidate: marginal high × partial high = new information; marginal high × partial ≈ 0
= redundant with F (high `r2_F`); marginal ≈ 0 × partial high = a suppressor effect.

Cost: one pass for the column moments, one pass per Newton iteration (at most `maxIter`; a rejected step halves
the step size and costs one more pass; converged iterations are skipped) and one pass for the partial sums —
`maxIter + 2` passes over the data at most, each a global Combine. The summary reports `conditioningIterations`,
`conditioningRejectedSteps`, `conditioningConverged` and `conditioningGain` (the in-sample average
log-likelihood improvement of F over the baseline — or, without one, over the prior-mean intercept the fit starts
from — on the per-unit scale of `est_gain`; for `gaussian` it is divided by the residual variance at the fit, so it
is invariant to the label scale — a sanity check that the conditioning set is informative). A `gaussian` fit whose
residual variance is 0 (an exact fit) cannot scale the partial statistics: they are null, `passed` follows the
marginal test and `notes` says so.
Conditioning needs the global window (no `strategy` window) and, like every screen run, the default trigger.

### Pairs

`pairs: {fields: [[f_price, f_recent_bids]]}` (or `among: [f_*]` for every pair of a set) tests the product of
two conditioning columns — an interaction — beyond what the model of those columns explains. Both members
must be in `conditioning.fields`: a product is meaningful only at the fitted means of a model that holds
its main effects (at the baseline alone, an unmodelled main effect leaves curvature the product would pick
up as a spurious interaction). The pair record (`candidate: a*b`, `transform: product`) carries the partial
statistics only (`partial_z`, `partial_gain`, `r2_F`, …; the marginal fields are null), has its own placebo
kind — each pair brings `pairs.placebo` placebo pairs, its first member times a noise column (pairs sharing a
member take different noise columns, so no placebo repeats), whose gains give
`thresholds.pair` — and `passed` compares its partial gain with that cut (lifted to `pass.minGain`). A row
missing either member is missing for the product (as the fragment `a * b` would be null there), not the
product of the conditioning fill. A
passing pair is a recipe, never a column of the pass list: the summary and the pass list carry `passedPairs`
apart (counted in `nPairsPassed`, not `nPassed`), each with the fragment `{scope: row, expr: "a * b"}` to build upstream. Each pair costs `2 + k`
doubles per partial key (times `1 + placebo`), and with `shape` each real pair's grid `2 K` more (`2 K + K²`
for `groupedMultinomial`, K = `shape`²); `maxPairs` bounds a run. The members of a pure interaction
have no marginal effect, so do not pre-select pairs by the marginal ranking: declare the set you suspect (the
pHd directions of [`joint`](#several-candidates-joint) name the members).

**The interaction shape.** The pair test says whether the product adds information; `pairs.shape` (default
4 bins per member, at least 2; `false` / 0 = off) says what shape it has. Each declared pair also keeps a
2-D grid of its members' value bins at the fitted means (their value quantiles come from the sketch pre-pass:
one more read of the input, over the pair members' columns only), and the `suggestions` output gets one
`interaction` record per pair: the best
depth-2 tree over the grid (a first cut on one member, then the other member's best cut on each side) with
`share` (the tree's gain over the grid's block χ²), `cut` / `direction` (the first member's cut and the side
where the other member matters), `fill` (the other member's cut on that side), `consistency` (the two sides'
second-level gains, smaller over larger: near 0 the other member matters on one side only — "b matters only
when a > c" — near 1 on both; null, with no `direction` / `fill`, when no cut of the other member adds
anything on either side) and `fragment` (the two row `bin` ops crossed, or the conditional expression
`a > c ? b : 0` when the shape is one-sided). In-sample, a diagnostic: read it for the pairs that passed.

## Input contract

| role | description |
|---|---|
| `group` (optional) | mutually exclusive samples of one unit (a query's candidates, a session's listings, a lot's bids). Required for `groupedMultinomial`. Omitted: every row is independent. |
| `label` | the label field, or an expression over numeric fields (`{expr: "rank == 1 ? 1 : 0"}`). Several positives in a group are normalised (`normalizeTies: true`). |
| `baseline` (optional) | the reference prediction. `groupedMultinomial` / `binomial`: `form: prob` (a probability; normalised within the group for `groupedMultinomial`), `logProb`, `inverseShare` (1/x made a share within the group — odds, prices). `gaussian`: `form: value` (the predicted value). `poisson`: `form: rate` or `logRate`. `invalid`: what an invalid value does to its unit — `skipUnit` (default: the unit is skipped whole) or `dropRow` (the row leaves the unit and the shares are taken over the rest; for a withdrawn candidate whose row should not exist). `prob` accepts 0, `inverseShare` / `rate` reject a null, 0 or negative value. Omitted: the prior (uniform share / prior rate / label mean). |
| `time` (recommended) | the time field (`timestamp` / `date` / ISO string); `to` / `from` fence the window. Omitted: the element timestamp is used (set the source's `timestampAttribute`; bounded sources otherwise carry the minimum timestamp, so `to` / `from` require `field`). |
| `weight` (optional) | a sample-weight field. |
| candidates | numeric input fields (`int32` / `int64` / `float32` / `float64` / `bool`) selected by name globs or lineage selectors; role fields are never candidates. |

The transform takes the `feature` transform's row form (one row = entity × context). The `output.groupBy`
parent/child form is not accepted (unnest upstream).

**Defaults from the feature transform.** The roles the feature transform declared (`output.roles`) fill `group` /
`label` / `baseline` / `weight` and its time field fills `time.field` when they are not set — the data contract
declared once on the feature side is reused here. They are read from the input schema when the feature transform is
the direct upstream (`feature.role` field options), or from the `roles` / `timeField` of the manifest that
`candidates.manifest` points at (the upstream's `output.manifest`) when the table comes back through a sink / source.

**Lineage selectors.** `candidates.include` / `exclude` accept, next to name globs (`f_*`, `odds*`), the
lineage selectors of the feature transform's `output.exclude`: `derivedFrom:<kind>` (a source field's `kind`, e.g.
`market` / `outcome`, propagated to every column derived from it), `scope:<input|row|context|sequence|population>`,
`block:<name>`, `evidence:<declared|measured>` — and, accepted by the screen only, `kind:<kind>` (the origin tag of a
pass-through input field itself — a derived column has no kind, only `derivedFrom`). Lineage is read from the input schema when the feature transform is
the direct upstream (its field options travel with the schema — the pass-through input fields carry `scope: input`
and their `kind` as `derivedFrom`, so `derivedFrom:market` or `scope:input` drops a passed-through market column
as well as the columns derived from it), or from `candidates.manifest` when the table comes back through a sink /
source (its `columns` and `fields` entries carry the same lineage). Using a selector without any lineage available
is an assembly error.

## Transform module common parameters

| parameter  | optional | type                              | description                                                           |
|------------|----------|-----------------------------------|-----------------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.                     |
| module     | required | String                            | Specified `screen`                                                    |
| inputs     | required | Array<String\>                    | Input step names (several inputs are flattened).                      |
| waits      | optional | Array<String\>                    | Steps to wait for before processing.                                  |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy. With a window, one set of records per window.     |
| parameters | required | Map<String,Object\>               | Screen parameters below.                                              |

## Screen transform module parameters

| parameter | optional | type | description |
|---|---|---|---|
| family | optional | String | `groupedMultinomial` (default), `binomial`, `gaussian` or `poisson`. The row families (`binomial` / `gaussian` / `poisson`) accept `group` for the within-group transforms and shuffles; `groupedMultinomial` requires it. A negative label is an invalid row for `poisson`. |
| group | optional | String | Group key field. Required for `groupedMultinomial`. |
| label | required | String or Object | Field name, or `{field}` / `{expr, normalizeTies}`. `expr` is a [Lucene expression](https://lucene.apache.org/core/10_5_0/expressions/org/apache/lucene/expressions/js/package-summary.html) over numeric fields; `normalizeTies` (default true) normalises the labels of a group to sum 1. |
| baseline | optional | String or Object | Field name (the family's default form), or `{field, form, invalid}`: `prob` / `logProb` / `inverseShare` (groupedMultinomial, binomial), `value` (gaussian), `rate` / `logRate` (poisson); `invalid`: `skipUnit` (default) / `dropRow`. |
| time | optional | String or Object | Field name, or `{field, to, from}` with ISO-8601 instants. Rows after `to` / before `from` are not screened. |
| weight | optional | String or Object | Weight field (`{field}` accepted). |
| rowId | optional | Array<String\> | Fields that identify a row (the placebo noise seed and the tie-break of rows sharing a time; the unit key for independent rows). Default: every field value. The identity travels as a 128-bit hash. |
| candidates | optional | Object or Array | `{include: [globs / selectors], exclude: [globs / selectors], manifest: <uri>}`, or a list of include globs. Default include `["*"]`. |
| transforms | optional | Array<String\> | Any of `raw`, `rank`, `absdev`, `binned`. Default: the first three with `group`, `raw` without (independent rows take `rank` / `absdev` against a window quantile sketch when listed — one extra pass over the input). `binned` (the block test, see [Binned block test](#binned-block-test)) is never in the default list. |
| bins | optional | Object or Integer | The binned block test's bins: `{k, edges}` or the number of bins. `k` (default 10, at most 100) value / position bins plus a missing bin; `edges`: `value` (default: the window's value quantiles, from the sketch pre-pass) or `rank` (the within-unit rank, needs `group`). Needs `binned` in `transforms`. |
| heterogeneity | optional | String or Object | The heterogeneity test's modifier (see [Heterogeneity across a modifier](#heterogeneity-across-a-modifier)): `periods` (the period buckets; needs `periods`), a field name, or `{by: periods \| field, field}`. A field modifier is read per row (per group, its first row's value, for `groupedMultinomial`); a null value is its own level; the field is never a candidate. |
| suggestions | optional | Boolean | `true` emits the one-candidate derivation suggestions (see [Suggestions](#suggestions)) to the `<name>.suggestions` output; needs `binned` in `transforms`. Default false. |
| pairs | optional | Object | Products of two conditioning columns tested at the fitted means (see [Pairs](#pairs)): `fields: [[a, b], ...]` and / or `among: [names / globs]` (every pair of the matched conditioning fields), `maxPairs` (default 200), `placebo` (placebo pairs per pair: the first member × a noise column, default 5; at most `placebo.noise`), `shape` (value bins per member of the pair's 2-D grid for the interaction shape, default 4; `false` / 0 = off). Needs `conditioning` holding both members of every pair. |
| joint | optional | Boolean or Object | The candidates' joint sums for the several-candidate suggestions (see [Several candidates](#several-candidates-joint)): `true`, or `{include: [globs / selectors] (default every candidate), maxColumns (default 200), noise (noise columns carried for the null scale, default 10), directions (pHd directions, default 3), redundancy (|correlation| of a cluster, default 0.95), select (forward-selection steps, default 10), pairs (difference / ratio suggestions at most, default 10), excess (a pair's joint statistic over the better single one, default 1.5)}`. O(m²) per row: opt-in, bounded. |
| categorical | optional | Object or Array | String fields tested natively as candidates (see [Categorical candidates](#categorical-candidates)): `{include: [globs / selectors], maxLevels (named levels kept, default 32), placebo (placebo columns per candidate, default 5)}` or a list of include globs. Role fields are never candidates. |
| periods | optional | Object or String | `{field, bucket}` or a bucket name; bucket `year` / `quarter` / `month` / `week` / `day` (UTC). `field` defaults to `time.field`. |
| placebo | optional | Object | `noise` (standard-normal columns, default 100), `shuffle: {field, n}` (within-group permutations of `field`, default n 100; needs `group`), `quantile` (default 0.99), `seed` (default 0). `noise: 0` without shuffle falls back to the theoretical threshold. |
| flags | optional | Object | `leakZ`: flag candidates with \|z\| above it as `leakSuspect` — a number (the marginal z) or `{z, on: marginal \| partial}` (`partial` needs `conditioning`). Default: no flag. |
| pass | optional | Object | `minPeriodsAgree`: the usable period buckets of the effective test (partial with conditioning, else marginal) whose sign must agree with its overall sign for `passed` — a share in (0, 1] of `n_periods` or a count above 1; needs `periods`. `minGain`: a positive floor on the effective test's gain (`est_gain` / `partial_gain`, the average log-likelihood improvement per unit): `passed` needs the gain above `max(threshold, minGain)`. Default: the placebo threshold alone. |
| conditioning | optional | Object or Array | `{fields: [names / globs], l2, maxIter, tol, missing}` or a list of fields: the partial test against an existing feature set (see [Conditioning](#conditioning-partial-test)). `l2` (default 1e-4) penalises the average log-likelihood; `maxIter` (default 10, at most 100) is the number of Newton passes over the data; `tol` (default 1e-8) the objective improvement that ends the fit; `missing` (`mean` (default) \| `groupMean`) how a missing conditioning value enters the fit — the window mean, or the unit's baseline-weighted mean of its observed values (`groupedMultinomial` only). Needs the global window. |
| output | optional | Object | `selection`: URI / path of the pass-list file written at the end of the run (see [Closing the loop](#closing-the-loop-outputselection)). Needs the global window. |

## Outputs

The default output (`<name>`) holds one scoring record per column × transform, placebo columns included.
`<name>.summary` holds one record per run (per window under a windowing strategy). `<name>.suggestions` holds
the derivation suggestions under `suggestions: true` (see [Suggestions record](#suggestions-record-namesuggestions-with-suggestions-true)).

### Scoring record

| field | type | description |
|---|---|---|
| candidate | STRING | column name (`__noise_<i>` / `__shuffle_<i>` for placebo columns) |
| transform | STRING | `raw` / `rank` / `absdev` |
| method | STRING | `scoreTest` |
| family | STRING | the family |
| S, H, beta, chi2, z, est_gain | FLOAT64 | the statistics above (`beta` null when degenerate) |
| df | INT64 | degrees of freedom: 1, or the binned block's active bins − 1 |
| pValue, qValue | FLOAT64 | χ²(df) upper tail (χ²(1), or the binned block's df); Benjamini–Hochberg q-value over the candidate records (null for placebo) |
| n_groups | INT64 | scored units (groups, or rows when independent) — the N of `est_gain` |
| n_obs | INT64 | rows whose transformed value is finite |
| periods_agree, n_periods | INT64 | buckets agreeing with the overall sign / non-degenerate buckets |
| period_z | ARRAY<STRUCT<period STRING, z FLOAT64, S FLOAT64, H FLOAT64, n INT64\>\> | per bucket |
| bin_stats | ARRAY<STRUCT<bin INT64, S FLOAT64, H FLOAT64, n FLOAT64\>\> | the binned block test only: per bin (the last index is the missing bin) the score, the information and the weight mass; null for the other transforms |
| bin_edges | ARRAY<FLOAT64\> | the binned block test with `edges: value`: the k − 1 window quantile edges (bin i = `(edge_{i−1}, edge_i]`); null for position bins, a column without a sketch value and the other transforms |
| het_chi2, het_df, het_pValue, het_gain, het_levels | FLOAT64 / INT64 | the heterogeneity test across the modifier's levels (`heterogeneity`; null without one, and for the block test); `partial_het_*` the same on the partial slices under conditioning |
| level_z | ARRAY<STRUCT<level STRING, z FLOAT64, S FLOAT64, H FLOAT64, n INT64\>\> | a field modifier: the score test per level; null for `periods` (read `period_z`) |
| het_passed | BOOL | the effective heterogeneity gain above `max(thresholds.het, minGain)`; candidate records only, never part of `passed` |
| r2_F | FLOAT64 | conditioning only: redundancy of the candidate with F (1 = fully explained) |
| partial_S, partial_H, partial_chi2, partial_z, partial_gain, partial_pValue | FLOAT64 | conditioning only: the score test of the candidate orthogonalised against F |
| partial_df | INT64 | conditioning + the binned block test: the partial block's active bins − 1 (null for the other transforms) |
| partial_periods_agree, partial_n_periods | INT64 | conditioning + periods: buckets whose partial sign agrees with the overall partial sign / non-degenerate buckets (null without conditioning) |
| partial_period_z | ARRAY<STRUCT<period STRING, z FLOAT64, S FLOAT64, H FLOAT64, n INT64\>\> | conditioning + periods: the partial test per bucket (S⊥, H⊥ with the window's orthogonalisation; they sum to `partial_S` / `partial_H`) |
| threshold | FLOAT64 | the placebo quantile (or theoretical) threshold — of the partial gain with conditioning |
| passed | BOOL | `est_gain > threshold` (`partial_gain` with conditioning; `max(threshold, minGain)` under `pass.minGain`), and the period agreement under `pass.minPeriodsAgree`; candidate columns only |
| leakSuspect | BOOL | \|z\| > `flags.leakZ` (\|partial_z\| under `flags.leakZ.on: partial`) |
| placebo | BOOL | placebo column |
| degenerate | BOOL | no usable information (constant / too few rows) |

### Summary record

`family`, `method`, `group`, `label`, `baseline`, `baselineForm`, `weight`, `passRule` (the rule behind `passed` as
applied, e.g. `partial_gain > threshold and partial_periods_agree >= 0.66 * partial_n_periods`), `minPeriodsAgree`, `minGain` (null unless declared), `threshold`, `thresholdTheoretical` (the df = 1 cut), `thresholds` / `thresholdsTheoretical` (the cut per statistic kind: `df1`, `binned` with the block test, `het` with a heterogeneity modifier), `bins` (`edges/k` of the block test, else null), `heterogeneity` (the modifier: `periods` or `field:<name>`, else null), `nHetPassed` / `hetPassedColumns` (the heterogeneity flag's count and columns, best gain first; null without a modifier), `nPairs` / `nPairsPassed` / `passedPairs` (the declared pairs and the passing ones, `a*b`; null without `pairs`), `nSuggestions` (null without `suggestions` / `joint` / `pairs` / `categorical`), `nJointColumns` / `nJointUnits` / `nJointFilled` / `nJointDropped` (the joint columns and the joint sums' row set, see [Several candidates](#several-candidates-joint); null without `joint`), `nCategoricals` (null without `categorical`),
`quantile`, `seed`, `nRows`, `nRowsTimeFiltered`, `nRowsInvalid` (null label / group / weight), `nRowsScored`,
`nUnits`, `nUnitsSkipped` (in the same unit as `nUnits`: groups without a positive label or with an invalid baseline; for `binomial` with a `group`, the rows of a group holding an invalid baseline), `nUnitsSkippedInvalidBaseline` (the invalid-baseline part of it), `nRowsDropped` (rows `baseline.invalid: dropRow` removed), `nCandidates`,
`nTransforms`, `nScored`, `nPassed`, `nPlacebo`, `nLeakSuspect`, `leakOn` (the z the flag read: `marginal` / `partial`; null without a flag), `timeField`, `timeFrom`, `timeTo`, `minTime`,
`maxTime` (TIMESTAMP, of the scored rows), `periodsBucket`, `transforms`, `candidates`, `test` (the statistic
that decided `passed` / `threshold`: `partial` when a conditioning fit accepted a point, else `marginal`), `passedColumns` (candidate
names with a passing transform, best gain first — the list to feed back into the feature transform's
`output.include`), `conditioningFields`, `conditioningK`, `conditioningIterations`, `conditioningRejectedSteps`,
`conditioningConverged`, `conditioningGain`, `conditioningL2` (null without conditioning), `notes` (role defaults
applied, columns excluded by lineage, a skipped share of units above 1% with its reasons, dropped rows).

### Suggestions record (`<name>.suggestions`, with `suggestions: true`)

One record per candidate × kind (`shape` / `cut` / `missing` / `monotone`, see [Suggestions](#suggestions)):
`candidate`, `kind`, `name`, `cut`, `direction` (`+` / `-`, the sign of the label's response along the recipe),
`fill`, `consistency`, `share` and `chi2` (discovery half), `confirmation_chi2`, `confirmation_share`,
`confirmation_gain`, `confirmation_pValue` (confirmation half), `threshold` (the kind's placebo cut),
`passed`, `placebo`, `fragment` (the recipe in the feature transform's row vocabulary, or a description when it
has no row op — a monotone constraint, a within-unit rank), `basis` (`marginal` / `partial`: the sums the
record was read on, see [Suggestions](#suggestions) and [Several candidates](#several-candidates-joint)),
`r2_F` (on the partial basis: the share of the candidate's — or the block's — information the conditioning set
carries; null otherwise). Placebo columns get records too (`placebo: true`, never `passed`); the summary counts
the candidates' records (`nSuggestions`, placebo records excluded).

## Examples

### Example 1: sessions of listings, conditioned on the current model

The baseline is the production model's probability per listing; only information the model does not have
scores. The 2025 H2 window is kept out of the screen.

```yaml
sources:
  - name: rows
    module: bigquery
    parameters:
      query: "SELECT * FROM `project.dataset.auction_features`"
transforms:
  - name: screen
    module: screen
    inputs: [rows]
    parameters:
      family: groupedMultinomial
      group: session_id
      label: sold
      baseline: {field: p_current_model, form: prob}
      time: {field: session_time, to: "2025-06-30T23:59:59Z"}
      candidates:
        include: ["f_*"]
        exclude: ["f_final_*"]
      transforms: [raw, rank, absdev]
      periods: {bucket: quarter}
      placebo:
        noise: 100
        shuffle: {field: f_start_price, n: 100}
        quantile: 0.99
        seed: 20260101
      flags: {leakZ: 20}
sinks:
  - name: scores
    module: bigquery
    inputs: [screen]
    parameters: {table: project.dataset.feature_screen}
  - name: summary
    module: bigquery
    inputs: [screen.summary]
    parameters: {table: project.dataset.feature_screen_summary}
```

### Example 2: downstream of the feature transform, roles and lineage from its manifest

Candidates derived from market or outcome fields are excluded by lineage; group / label / baseline / time come
from the roles the feature transform declared.

```yaml
transforms:
  - name: screen
    module: screen
    inputs: [features]
    parameters:
      candidates:
        manifest: gs://bucket/feature/${args.version}/manifest.json
        exclude: ["derivedFrom:outcome", "derivedFrom:market", "scope:row"]
      placebo: {noise: 100, shuffle: {field: start_price, n: 100}}
```

### Example 3: independent rows, expression label, prior baseline

```yaml
transforms:
  - name: screen
    module: screen
    inputs: [rows]
    parameters:
      family: binomial
      label: {expr: "final_price > start_price ? 1 : 0"}
      time: {field: session_time, to: "2025-12-31T23:59:59Z"}
      candidates: {include: ["*"], exclude: ["final_price", "start_price"]}
      periods: {bucket: month}
      placebo: {noise: 100, quantile: 0.99}
```

### Example 4: partial test against the current feature set

Candidates are scored for what they add beyond the features the current model uses (and beyond the market
baseline); a candidate with `r2_F` near 1 is a re-encoding of something the model already has.

```yaml
transforms:
  - name: screen
    module: screen
    inputs: [rows]
    parameters:
      family: groupedMultinomial
      group: session_id
      label: sold
      baseline: {field: p_market, form: inverseShare}
      time: {field: session_time, to: "2025-06-30T23:59:59Z"}
      candidates: {include: ["cand_*"]}
      conditioning:
        fields: ["model_*"]
        l2: 1.0e-4
        maxIter: 10
      placebo: {noise: 100, shuffle: {field: cand_start_price, n: 100}}
```

### Example 5: closing the loop with the feature transform

The screen writes its pass list; the next feature run projects its output to those columns. The two
configs share the version argument, so the manifest the screen read (`candidates.manifest`) and the include the
feature reads (`output.include`) are tied to one plan.

```yaml
# screen run
transforms:
  - name: screen
    module: screen
    inputs: [features]
    parameters:
      candidates:
        manifest: gs://bucket/feature/${args.version}/manifest.json
        exclude: ["derivedFrom:outcome", "scope:row"]
      placebo: {noise: 100, shuffle: {field: start_price, n: 100}}
      output:
        selection: gs://bucket/screen/${args.version}/passed.json

# next feature run
transforms:
  - name: features
    module: feature
    inputs: [rows]
    parameters:
      sources: ...
      features: ...
      output:
        include: gs://bucket/screen/${args.version}/passed.json
        manifest: gs://bucket/feature/${args.nextVersion}/manifest.json
```

## Closing the loop (output.selection)

`output.selection` writes one JSON document at the end of the run:

```json
{
  "version": 1,
  "columns": ["f_extra", "f_recent_bids"],
  "test": "partial",
  "passRule": "partial_gain > threshold and partial_periods_agree >= 0.66 * partial_n_periods", "minPeriodsAgree": 0.66, "minGain": null,
  "leakZ": 20.0, "leakOn": "partial",
  "family": "groupedMultinomial", "method": "scoreTest",
  "threshold": 0.000063, "thresholdTheoretical": 0.000067, "thresholds": {"df1": 0.000063}, "bins": null,
  "heterogeneity": null, "quantile": 0.99,
  "nCandidates": 27, "nPassed": 2, "nUnits": 49839,
  "timeFrom": null, "timeTo": "2025-06-30T23:59:59Z",
  "screenHash": "…", "planHash": "…", "outputHash": "…", "manifest": "gs://…/manifest.json",
  "conditioningFields": ["model_a", "model_b"],
  "createdAt": "2026-09-05T10:00:00Z",
  "passed": [{"candidate": "f_extra", "transform": "rank", "est_gain": 0.00077, "z": 8.95, "partial_gain": 0.00051, "partial_z": 7.1, "r2_F": 0.035, "periods_agree": 3, "n_periods": 3, "leakSuspect": false}]
}
```

- `columns` is what the feature transform's `output.include` reads (`{columns: [...]}` is one of its accepted
  shapes); the other members record how the list was produced.
- `test` says which statistic the cut-off used (`partial` when the conditioning fit accepted a point — the same
  rule as the summary's `test` — else `marginal`); `passRule` spells the rule out, and with `periods` each passing
  record carries its `periods_agree / n_periods` of that test.
- `leakZ` / `leakOn` are the flag the passing records' `leakSuspect` was read with (the threshold and the z it
  read, as the summary's `leakOn`; null without `flags.leakZ`).
- `planHash` / `outputHash` are the upstream feature manifest's identities when `candidates.manifest` was given
  (null otherwise); `screenHash` is the SHA-256 (16 hex characters, the width of the feature transform's hashes) of
  this step's canonical parameters without the file locations (`output`, `candidates.manifest`), so it is the same
  across runs that only move the pass list or the manifest.
  Together they make the pass list traceable to the plan that produced the candidates and the configuration that
  screened them.
- `threshold` / `thresholdTheoretical` are null when no unit was scored (no `NaN` in the file).
- An empty `columns` (nothing passed) is still written and logged as a warning: a feature run reading it as
  `output.include` fails at assembly (`output.include.empty`, the table would carry no feature column), so check
  `nPassed` before closing the loop.
- The file is written once per run from the finalize step (global window only); a failed write fails the step.
  Keeping a ledger of runs is a matter of versioned paths (`${args.version}`) or an `action/storage` copy.

## Reading the output

- Rank candidates by `est_gain` (or `z`); `passed` is the placebo-calibrated cut-off, `qValue` the
  false-discovery view over the candidate set.
- A candidate with a high `raw` score is a direct linear effect; one that only scores under `absdev` is an
  "extremeness" effect; `rank` catches monotone non-linear effects and is robust to outliers.
- `periods_agree` far below `n_periods` means an unstable effect: look at `period_z` for a decay over time. With
  conditioning read `partial_periods_agree` / `partial_period_z` (the test that decided `passed`); an operating
  rule such as "passes and agrees in two thirds of the years" is `pass: {minPeriodsAgree: 0.66}` (not 0.67: the
  share is compared as `agree ≥ share × n_periods`, and 2 of 3 is 0.667 < 0.67), so the pass list
  applies it too.
- On a large window the placebo cut alone admits columns whose gain is real but negligible (the cut shrinks
  as 1 / N). Put a practical floor under it with `pass: {minGain: 1e-5}` — in the unit of `est_gain`, so
  compare it with the gain a model comparison would have to show to be worth a retrain; the pass list
  records the floor (`minGain`) and the rule (`passRule`).
- `leakSuspect` candidates deserve a look at their lineage before they are used: an outsized z is the typical
  signature of a column computed after the outcome. When strong legitimate candidates trip the flag, read it on
  the partial z (`flags: {leakZ: {z: 20, on: partial}}`): a leak survives the conditioning, a re-summary of what
  the model knows does not.
- `output.selection` writes the pass list in the format the feature transform's `output.include` reads, so
  the next feature run emits only the screened columns (see [Closing the loop](#closing-the-loop-outputselection));
  `passedColumns` in the summary is the same list.

### Scoring against the settlement reference

Where the return is settled at a price fixed *after* the decision — the closing price of a market, the hammer
price of an auction, the odds at post time — the reference the decision is made against (the market at decision
time) and the reference the return is paid at (the settlement market) differ, and the market closes part of the
gap on its own. Of a candidate's gain over the decision-time market, the part the settlement market absorbs by
itself earns nothing; what survives against the settlement market is what a probability model can turn into
return. So screen the same candidates against both references and read the ratio of the two gains (the
retained share). Two screens over one input, one per reference, and a join downstream:

```yaml
transforms:
  - name: screen_bet                       # the decision-time market
    module: screen
    inputs: [features]
    parameters:
      group: session
      label: won
      baseline: {field: price_bet, form: inverseShare, invalid: dropRow}
      candidates: {exclude: [price_final]}   # the settlement price is not observable at decision time: a yardstick, never a candidate
      conditioning: {fields: ["base_*"], missing: groupMean}
      periods: year
      output: {selection: gs://bucket/screen/${args.version}/passed_bet.json}
  - name: screen_final                     # the settlement market: same parameters, the other reference
    module: screen
    inputs: [features]
    parameters:
      group: session
      label: won
      baseline: {field: price_final, form: inverseShare, invalid: dropRow}
      candidates: {exclude: [price_bet]}
      conditioning: {fields: ["base_*"], missing: groupMean}
      periods: year
      output: {selection: gs://bucket/screen/${args.version}/passed_final.json}
  - name: retained                         # one row per candidate x transform, both gains and their ratio
    module: beamsql
    inputs: [screen_bet, screen_final]
    parameters:
      sql: |
        SELECT b.candidate, b.transform,
               b.partial_gain AS gain_bet, f.partial_gain AS gain_final,
               SIGN(b.partial_z * f.partial_z) * f.partial_gain / NULLIF(b.partial_gain, 0) AS retained,
               b.passed AS passed_bet, f.passed AS passed_final
        FROM (SELECT candidate, transform, partial_z, partial_gain, passed FROM `screen_bet` WHERE NOT placebo) b
        JOIN (SELECT candidate, transform, partial_z, partial_gain, passed FROM `screen_final` WHERE NOT placebo) f
          ON b.candidate = f.candidate AND b.transform = f.transform
```

- Project the columns before the join (the subqueries above): a scoring record carries the nested `period_z`
  array, and a Beam SQL join of the whole records fails at assembly (`Types not equal`).
- `invalid: dropRow` on both: a withdrawn entry has no settlement price (null or 0), and without it the whole
  group would be skipped under `inverseShare` (`nUnitsSkippedInvalidBaseline`, and a note above 1%). It still
  has its decision-time price, so the decision-time screen keeps it (`nRowsDropped` differs between the two
  summaries); when the two gains must cover the same rows, filter such entries out upstream of both screens.
- Both screens keep the same `conditioning`, `periods`, `placebo` and `time` window, so the two thresholds are
  calibrated the same way and the gains are comparable; with `conditioning` compare the partial gains (as
  above), without it `est_gain` / `z`. `periods` (and a `time.to` / `from` fence) read the feature transform's
  time field when it is the direct upstream, as here; otherwise declare `time: {field, to}` in both.
- **Reading the retained share.** Near 1: information the market never learns (a feature for the probability
  model). Near 0: information the market absorbs before settlement, or a re-encoding of the market level —
  useful to execution (when to place the order, what the settlement price will be), not to the probability
  model. A `rank` / `absdev` variant of a market-level column can still pass against the settlement reference
  (the skew of the extreme values survives in the settlement market): the retained share of its `raw` variant
  tells it apart. Read it only where the decision-time gain clears the threshold (`passed_bet`): a ratio of two
  one-step gains is unstable near zero, so a candidate without a decision-time gain (a conditioning column, a
  noise-level candidate) reads an arbitrary value, far above 1 included.
- **The cut.** Use the settlement reference as a marker, not as the cut: take the union of the two pass lists as
  the candidate set and carry the retained share next to it — a candidate that passes only at decision time
  with a low retained share is execution information, not noise. The next feature run's `output.include` reads
  one list (one URI, or the names inline), not several files: write the union as one list — the `candidate`s
  of the `retained` rows with `passed_bet OR passed_final` — and point `output.include` at it (see
  [Closing the loop](#closing-the-loop-outputselection)).
- The settlement price column must stay out of `candidates` in the decision-time screen (as above): it is
  numeric, so it would be screened, and a post-decision price is the textbook leak — flagged as `leakSuspect`
  only when `flags.leakZ` is set (no flag by default).

## Limits

- Univariate, linear probe: pure interactions are invisible (false negatives); a high linear gain can still
  add nothing to a tree model (false positives). A ranking device, not an acceptance test.
- A non-linear re-encoding of a variable the baseline already uses (its rank or absolute deviation) can still
  score: the baseline is a fixed offset, not a conditioning on the variable's transforms. The same holds for
  `conditioning`: it is linear in F, so a non-linear re-encoding of F overstates its novelty — trust low `r2_F`
  candidates more.
- Multiple comparisons: the placebo quantile calibrates *per candidate*; `qValue` gives the FDR view, but
  neither controls the family-wise error of a large candidate set.
- Blind to time dynamics: the statistic is a window average; read `period_z` for decay.
- Batch only (every statistic is a global Combine); under a windowing strategy the records are per window.
- Independent rows (`group` omitted) take `rank` / `absdev` against a window quantile sketch: one extra pass,
  approximate (rank error about 0.8 %) and not bit-reproducible beyond 400 values per candidate; not under
  session windows.
- Conditioning needs the global window and costs `maxIter + 2` passes; keep the conditioning set to a few
  hundred columns (the Newton Gram matrix is k × k).
