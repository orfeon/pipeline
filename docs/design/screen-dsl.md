# Screen Transform DSL (Design Document)

Status: **Implemented** — the contract described here is what `module: screen` accepts today (families
`groupedMultinomial` / `binomial` / `gaussian` / `poisson`, placebo calibration, transform variants, periods,
time window, leak flags, the partial test against an existing feature set, the pass list for the feature
transform). §12 lists the extension positions that are designed but not built. The execution side is
[screen-engine.md](screen-engine.md); the user-facing reference is
`src/main/resources/server/docs/module/transform/screen.md`.

## 1. Purpose and position

The `screen` transform ranks candidate feature columns **before training**, conditioned on an existing
baseline prediction. It answers one question per column — *does this column explain what the baseline
misses?* — without training a model, so the answer costs one pass over the data whatever the number of
candidates.

It sits between feature generation and the training job: feature (generation) → screen (ranking and cut-off)
→ training. It is the supervised counterpart of the `profile` sink (unsupervised column statistics). In the
taxonomy of feature selection it is a *filter* method: learner-free, one bounded Combine per statistic, the
pass count independent of the data. Wrapper methods (recursive elimination, forward selection), embedded
importances (tree gain, L1 paths), SHAP / permutation importance and causal selection are out of scope: they
need a learner, predictions of a fitted model (an evaluation step) or a data-dependent loop.

The same machinery reads as a *residual explanation* tool: with the current model's own features as the
candidates, a column that scores means the model no longer absorbs it (drift, staleness, miscalibration), and
the per-period statistics say since when.

### 1.1 Design principles

- **One statistic, closed form.** Everything the transform reports derives from sums that a Beam Combine can
  accumulate: the score test itself, the placebo threshold, the q-values, the period agreement, the partial
  test. No pass depends on the data (the Newton fit of the conditioning model is unrolled to a fixed number
  of passes, §8).
- **Calibrated by construction, not by an absolute threshold.** A squared statistic is positive under the
  null; the pass threshold is read off placebo columns that went through the same pipeline (§5).
- **Deterministic.** Every random draw derives from the declared seed and a row / unit identity, so a rerun
  on any runner reproduces the placebo columns and therefore the pass list.
- **Same vocabulary as the feature transform.** Roles, lineage selectors, the manifest and the pass list are
  the feature transform's contract; nothing had to be added on the feature side to close the loop.
- **A ranking device, not an acceptance test.** The probe is linear and univariate (§11); the output is an
  ordering and a cut-off, and the limits are part of the contract.

## 2. Input contract

The input is the feature transform's row form (one row = entity × context) or any table with numeric
candidate columns. The `output.groupBy` parent / child form is not accepted (unnest upstream).

| role | parameter | meaning |
|---|---|---|
| unit | `group` | mutually exclusive samples of one unit (the candidates of a search query, the listings of an auction session, the bids of a lot). Required for `groupedMultinomial`; optional for the row families, where it only scopes the within-group transforms and shuffles. Omitted: every row is independent. |
| label | `label` | a field, or `{field}` / `{expr, normalizeTies}`. `expr` is a Lucene expression over numeric fields; its variables are reserved (never candidates). `normalizeTies` (default true) normalises a group's labels to sum 1. |
| baseline | `baseline` | the reference prediction: a field (the family's default form) or `{field, form}` (§4). Omitted: the prior. |
| time | `time` | a field (`timestamp` / `date` / ISO string), or `{field, to, from}`; `to` / `from` fence the window (rows outside are counted, not screened). Omitted: the element timestamp; `to` / `from` then need `time.field`. |
| weight | `weight` | a sample-weight field; per row for the row families, the unit mean for the grouped family (§3.4). |
| identity | `rowId` | fields identifying a row (sort tie-break, placebo seed, the unit key of independent rows). Default: every field value. |

**Candidates** are the numeric input fields (`int32` / `int64` / `float32` / `float64` / `bool`) matching
`candidates.include` (globs, default `*`) minus `candidates.exclude`, minus every role field, minus the
label expression's variables, minus the shuffle reference and the conditioning columns' role fields.
`include` / `exclude` also accept the feature transform's lineage selectors — `derivedFrom:<kind>` (the
origin kind a source field declares, propagated to every derived column), `scope:<input|row|context|sequence|population>`,
`block:<name>`, `evidence:<declared|measured>` — plus, screen-only, `kind:<kind>` (the origin tag of a
pass-through input field itself; a derived column carries its kinds in `derivedFrom` only) — resolved
against the lineage the feature transform attached to its output schema (`feature.*` field options, when
it is the direct upstream; its pass-through input fields carry `scope: input` and their kind, so a
passed-through market column is excluded by the same selector as the columns derived from it) or against
its manifest (`candidates.manifest`, when the table came back through a sink; its `fields` entries give the
pass-through inputs the same lineage). A selector with no lineage available is an assembly error.

**Defaults from the feature transform.** The roles the feature transform declared fill `group` /
`label` / `baseline` / `weight` and `time.field` when they are not set — from the `feature.role` field
options of the direct upstream's schema, or from the manifest's `roles`. When `candidates.manifest` is given, its `roles` fill `group` /
`label` / `baseline` / `weight` and its `timeField` fills `time.field` unless set; its `planHash` /
`outputHash` are carried into the pass list (§9.3). The data contract is declared once, on the feature side.

## 3. Families and the score test

The statistic is the Rao score test of an offset generalised linear model. With the linear predictor
η = offset + β·x and the offset fixed at the baseline, the score S and the Fisher information H at β = 0 give
the one-step Newton improvement of the log-likelihood:

```
chi2 = S² / H          (χ²(1) under the null)
z    = sign(S) · √chi2
est_gain = chi2 / (2N) (N = scored units: groups, or rows when independent)
```

`est_gain` is the *average log-likelihood improvement per unit* — the unit of an excess log score / logloss
improvement of a model comparison, so a screened column's gain is comparable with a trained model's gain.

| family | unit | S | H | notes |
|---|---|---|---|---|
| `groupedMultinomial` | group | Σ x̃ (ỹ − p) | Σ_g [ Σ p x̃² − (Σ p x̃)² ] | conditional logit within the group; `p` the baseline share (sums to 1 in the group), `ỹ` the label normalised to sum 1 |
| `binomial` | row | Σ x̃ (y − p) | Σ p (1 − p) x̃² | `p` the baseline probability |
| `gaussian` | row | Σ x̃ (y − μ) / σ² | Σ x̃² / σ² | identity link; `μ` the baseline value, σ² the residual variance around it |
| `poisson` | row | Σ x̃ (y − μ) | Σ μ x̃² | log link; `μ` the baseline rate |

### 3.1 Centring

`x̃` is the candidate centred by the Fisher-weighted mean over the observed rows — within the group for the
grouped family, over the window for the row families — and a missing value after centring is 0 (no
information). Centring profiles the intercept out: the statistic is invariant to the scale of x and to a
constant shift (within the group for the grouped family). The row families centre algebraically at the end
(the moment sums are accumulated raw, the report centres them), so every family is one pass.

### 3.2 The row families share one path

`binomial`, `gaussian` and `poisson` differ only in the Fisher weight at the baseline mean — μ(1 − μ), 1, μ —
and in what the report applies in prior mode (ȳ(1 − ȳ), the label variance, ȳ). Gaussian carries one more
sum (Σ w r²) for the residual variance, so its statistic is free of the label's scale without a second pass.

### 3.3 Prior mode

Without a baseline the offset is the prior: the uniform share within the group (grouped), the prior rate /
the label mean (row families), with the intercept profiled out by the centring. The result is the classical
univariate test of the column against the label.

### 3.4 Weights

`weight` multiplies every sum. For the row families it is a per-row weight; for the grouped family the
conditional logit has one likelihood term per group, so the group's weight is the row mean. Weights enter
as frequency weights (H is the model-based information), which is the natural reading for duplicated or
importance-sampled rows; a precision-weight reading is an extension position (§12).

### 3.5 Degenerate columns

A column with fewer than two observed rows, a constant within every group, or (gaussian) no residual
variance is *degenerate*: `est_gain = 0`, `z = 0`, `beta` null, `degenerate = true`. Degenerate placebo
columns enter the placebo quantile as 0. Under conditioning a degenerate column has no partial test either
(§8): its partial fields are the degenerate values and it never passes. The constant test is exact for the
grouped family: the unit's values are shifted by its first observed value before the p-weighted centring
(the statistic is shift-invariant within the group, and the shifted values are exact for the spreads a
double can carry), so a within-group constant gives H = 0 whatever its magnitude and a large-valued column
with a small spread keeps every digit of that spread. The row families centre by a difference of moment
sums (Σ w v x² − (Σ w v x)² / Σ w v), which loses the digits of the magnitude: below Σ x̃² = 1e-12 × Σ w v x²
the statistic has fewer than four significant digits and the column is degenerate — a window-constant
column, or one whose spread is below 1e-6 of its magnitude; centre such a column upstream.

## 4. Baseline forms

| family | forms | meaning |
|---|---|---|
| `groupedMultinomial`, `binomial` | `prob` (default), `logProb`, `inverseShare` | a probability (normalised within the group for the grouped family), its log, or 1 / x made a share within the group (odds, prices; needs `group`) |
| `gaussian` | `value` | the predicted value |
| `poisson` | `rate` (default), `logRate` | the predicted rate, or its log |

A bare field name takes the family's default form; a form not valid for the family is an assembly error.
Binomial probabilities are clamped to [ε, 1 − ε]. A unit whose baseline is invalid for its form (a negative
probability, a non-positive rate or `inverseShare` value — `prob` accepts 0, `inverseShare` / `rate` reject a
null, 0 or negative value — a share that does not sum) is skipped whole and counted (`nUnitsSkipped`,
`nUnitsSkippedInvalidBaseline`), never partially scored. `baseline.invalid: dropRow` drops the invalid rows
instead and scores the unit on the rest (`nRowsDropped`): for a withdrawal whose row should not exist, not for
a value missing by accident. A skipped share above 1% is noted with the reasons.

## 5. Placebo calibration

`est_gain` is a squared statistic, positive under the null on the χ²(1) / 2N scale, so no absolute threshold
applies. The transform adds placebo columns that go through exactly the same pipeline:

- `placebo.noise` standard-normal columns (`__noise_<i>`), drawn per row;
- `placebo.shuffle.n` within-group permutations of a reference column (`__shuffle_<i>`): the marginal
  distribution is kept, the alignment with the label is broken (needs `group`).

The pass threshold is the `placebo.quantile` quantile (default 0.99) of the placebo columns' `est_gain`, **pooled
over every transform variant**: the multiplicity the threshold must absorb is candidates × transforms, and the
default q99 (not q95) reflects it. Without any placebo column the theoretical χ²(1) quantile / 2N is the
threshold; it is always reported as `thresholdTheoretical` next to the empirical one, and the two agreeing is
the calibration check (on a 50k-group dataset the proposal measured 0.000063 against 0.000067).

**Determinism.** Every draw comes from `seededRandom(seed, unitKey + tag)` — a murmur3 hash of the seed and
the key feeding a `SplittableRandom`, the feature transform's own noise derivation. The unit key is the group
key, or the row identity for independent rows; within a unit the rows are sorted by (time, identity) before
any draw. The identity is a 128-bit hash of the declared `rowId` fields (else of every field value in name
order). Re-runs, runners, bundle boundaries and worker counts cannot change a placebo column.

## 6. Transform variants

| transform | definition | catches |
|---|---|---|
| `raw` | the column as is | direct linear effect |
| `rank` | percentile rank within the group over the observed values: `(smaller + ties / 2) / (observed − 1)` with `ties` the other values equal to it, in [0, 1] (an untied minimum 0, an untied maximum 1) — `(r − 1) / (m − 1)` for the average 1-based rank `r` of `m` observed values, i.e. pandas `(rank() − 1) / (count() − 1)`, not `rank(pct=True)` = `r / m` (numerator and denominator both differ), 0.5 for a single observed value | monotone non-linear effects, outlier robustness |
| `absdev` | \|x − median of the group's observed values\| | symmetric "extremeness" effects |

Records are keyed by (`candidate`, `transform`). `rank` and `absdev` are within-group statistics: with
independent rows only `raw` is available (a window-wide quantile sketch is the extension position, §12).
Default: all three with `group`, `raw` without; an explicit list is never widened.

## 7. Periods, time window, flags, q-values

- **Periods.** `periods: {field, bucket}` (year / quarter / month / week / day, UTC; `field` defaults to
  `time.field`, with its own type) accumulates the same sums per bucket. The record reports each bucket's S,
  H, z and observed rows (`period_z`), the buckets with usable information (`n_periods`) and how many agree
  with the overall sign (`periods_agree`) — the material for reading a decaying effect. Under conditioning the
  partial test is sliced the same way (§8.2: `partial_period_z`, `partial_periods_agree`, `partial_n_periods`),
  since the marginal and the partial period signs can disagree (a suppressor).
- **Pass rule.** `pass: {minPeriodsAgree}` adds the period agreement of the effective test to the cut: a share
  (≤ 1) of its usable periods or a count (> 1); no usable period never passes. `pass: {minGain}` puts a
  practical floor under the cut: `passed` needs the effective gain above `max(threshold, minGain)`. The placebo
  threshold is a significance cut — roughly constant on the χ² scale (≈ 6.6, the χ²(1) quantile at q99) and so
  ≈ 6.6 / (2N) ≈ 3.3 / N on the gain scale — and on a large window it admits columns whose gain is real but too
  small to matter; the floor is in the unit of `est_gain` (average log-likelihood improvement per unit), the
  scale a trained model's excess log score is reported on, so it means the same thing whatever N. With
  `weight` (§3.4) S and H carry the weights while the gain divides by the unit count, so the floor reads the
  mean weight times the per-unit gain (the placebo threshold scales the same way and is unaffected). Both rules tighten
  the placebo cut and are not themselves placebo-calibrated; the record's `threshold` stays the placebo cut,
  and the summary and the pass list report the rule as applied (`passRule`, `minPeriodsAgree`, `minGain`).
- **Time window.** Rows after `time.to` or before `time.from` are not screened and are counted
  (`nRowsTimeFiltered`). Screening the evaluation period leaks the evaluation into the selection.
- **Leak flag.** `flags.leakZ` marks a candidate with |z| above it as `leakSuspect` — a flag, never a
  rejection: a leak's signature is a z several times the healthy top, and only lineage can tell. The flag
  reads the marginal z (a number) or, as `{z, on: partial}` under conditioning, the partial z: a leak is not
  explained by F, so it keeps its outsized partial z, while a strong legitimate candidate that overlaps F
  loses most of its z to the orthogonalisation. The partial flag assumes F does not leak: a candidate F explains
  (r²_F ≈ 1, including a conditioning column that also matches `candidates`) has a partial z near 0 and is never
  flagged. A relative threshold (a ratio to the conditioning set's own marginal z) was considered and declined:
  F's columns are scored only when they also match `candidates`, so the ratio would need a marginal pass of
  its own over F, and the bound would move with whatever F holds. Without a partial test (no accepted fit, or
  a gaussian fit without residual variance) the flag reads the marginal z and a note says so.
- **q-values.** Benjamini–Hochberg over the candidate records' p-values (of the effective test, §8.5) gives
  the false-discovery view; `passed` itself is the placebo cut (`est_gain > threshold`, tightened by `pass`). Making `passed`
  follow the q-value is an extension position (§12).

## 8. Conditioning: the partial test

`conditioning.fields` names an existing feature set F (globs; the role fields and the baseline cannot be
conditioned on). The transform then measures what each candidate adds *beyond F*.

### 8.1 The conditioning model

η = offset + F̃·θ with F̃ = F standardised (a missing value → the window mean, or under `conditioning.missing:
groupMean`, grouped family only, the unit's baseline-weighted mean of its observed values — under the baseline p
the fill at which the missing row contributes nothing to the unit's p-centred design, the rule the candidate
columns follow in §3.2, so the first Newton pass is exact; the later passes centre by the fitted p̂, where the
fill is no longer exactly neutral; a unit with no observed value falls back to the window mean) — the conditional logit within the group for
the grouped family; for the row families a GLM with an intercept column (a calibration shift beyond the
baseline, the prior rate without one): logistic for `binomial`, least squares for `gaussian` (σ² = 1 in the
fit, the residual variance enters afterwards), log-linear for `poisson`. θ maximises the L2-penalised
*average* log-likelihood, `ll / n − l2 / 2 ‖θ‖²` with n the weight mass of the units, so `l2` (default 1e-4)
and `tol` (default 1e-8) are free of the data size, the weight scale and the columns' scales. Without a
baseline the intercept starts at the link of the weighted label mean (log ȳ, logit ȳ, ȳ) so the first pass
sits at the prior-mean model.

### 8.2 Orthogonalisation and the partial statistic

At the fitted p̂ the Fisher metric W is block diagonal diag(p̂) − p̂p̂' (grouped, with x̃ centred by p̂ within
the group) or diag(v̂) with v̂ the family's Fisher weight (row families, the intercept doing the centring).
For each candidate the sums s = x̃'(ỹ − p̂), b = x̃'Wx̃, a = F̃'Wx̃ and the fit's gradient g and Gram matrix G
give, in closed form:

```
γ    = (G + l2·n·I)⁻¹ a              (the same ridge as the fit's Newton system)
S⊥   = s − γ'g                       (the score of x orthogonalised against F)
H⊥   = b − 2 γ'a + γ'Gγ
r2_F = 1 − H⊥ / b                    (how much of x F already explains)
```

`partial_chi2 = S⊥² / H⊥`, `partial_z`, `partial_gain = partial_chi2 / (2N)`, `partial_pValue`. Gaussian
divides S⊥ and H⊥ by the residual variance at the fitted model. A column with H⊥ ≈ 0 is fully explained by
F: degenerate with `r2_F = 1`.

With `periods` the same sums are kept per bucket (s_p, b_p, a_p per column; the fit's own n_p, g_p, G_p per
bucket under one key) and sliced with the window's γ:

```
S⊥_p = s_p − γ'g_p
H⊥_p = b_p − 2 γ'a_p + γ'G_pγ        (Σ_p S⊥_p = S⊥, Σ_p H⊥_p = H⊥)
```

so `partial_period_z` decomposes the partial statistic by period and `partial_periods_agree` counts the
buckets whose S⊥_p has the sign of S⊥. As for the window (a column the marginal test cannot score has no
partial), a bucket whose marginal slice is degenerate — no observed or within-unit variation of x — has no
partial slice: its S⊥_p / H⊥_p would be the fit's own −γ'g_p / γ'G_pγ, not the candidate's. The per-period Gram costs periods × k² doubles in one accumulator, so
it is carried up to k = 100; beyond, γ'G_pγ is taken as the window's γ'Gγ times the bucket's share of the
unit mass (S⊥_p and the sign stay exact, H⊥_p is approximate, the sums still match) and a note says so.

### 8.3 Reading marginal and partial together

marginal high × partial high = new information; marginal high × partial ≈ 0 (high `r2_F`) = redundant with
F; marginal ≈ 0 × partial high = a suppressor effect. Placebo columns take the same route, so the threshold
is calibrated for the partial test.

### 8.4 Cost

One pass for the column moments, one pass per Newton iteration (at most `maxIter`, default 10; a rejected
step halves the step size and costs one more pass; converged iterations evaluate nothing) and one pass for
the partial sums — `maxIter + 2` passes at most, each a global Combine (engine doc §4).

### 8.5 The effective test

With a conditioning fit that accepted a point, `passed`, `threshold` and `qValue` refer to the partial test
and the summary / pass list say `test: partial`; the marginal statistics stay in the record. When no unit
could be scored, or a gaussian fit has no residual variance, the report falls back to the marginal test with
a note (`test: marginal`, `conditioningConverged: false`). `conditioningGain` is the in-sample average
log-likelihood improvement of F over the starting point (divided by the residual variance for gaussian) — a
sanity check that the conditioning set is informative.

## 9. Outputs

### 9.1 Scoring records (the default output)

One record per column × transform, placebo columns included: `candidate`, `transform`, `method`
(`scoreTest`), `family`, `S`, `H`, `beta`, `chi2`, `z`, `est_gain`, `df` (1; block tests will use it),
`pValue`, `qValue` (null for placebo), `n_groups` (N), `n_obs`, `periods_agree`, `n_periods`, `period_z`
(array of {period, z, S, H, n}), `r2_F`, `partial_S / H / chi2 / z / gain / pValue`,
`partial_periods_agree`, `partial_n_periods`, `partial_period_z` (null without conditioning), `threshold`,
`passed`, `leakSuspect`, `placebo`, `degenerate`. Field names follow the
proposal that introduced the transform so its reference implementation compares directly.

### 9.2 Summary (`<name>.summary`)

One record per run (per window under a windowing strategy): the spec's roles, `test`, `passRule` /
`minPeriodsAgree` / `minGain`, the thresholds and the quantile, the seed, the row and unit counts (in, time-filtered, invalid, scored, skipped), the candidate /
transform / scored / passed / placebo / leak-suspect counts, the z the leak flag read (`leakOn`), the time field and window, the scored rows' time
range, the period bucket, `transforms`, `candidates`, `passedColumns` (candidate names with a passing
transform, best gain first), the conditioning fields / size / iterations / rejected steps / convergence /
gain / l2, and `notes` (role defaults applied, columns excluded by lineage, fallbacks).

### 9.3 The pass list (`output.selection`)

One JSON document written at the end of the run, in the shape the feature transform's `output.include`
reads (`{columns: [...]}` first) plus the provenance a consumer needs to trust it: `test`, `passRule` (with
`minPeriodsAgree` / `minGain`), the leak flag (`leakZ` / `leakOn`), family /
method, thresholds, quantile, counts, the time window, `planHash` / `outputHash` of the upstream feature manifest
(when `candidates.manifest` was given), `screenHash` (the SHA-256 of the canonical parameters without the
file locations — the same canonicalisation and width as the feature plan hash), the conditioning fields,
`createdAt`, and the passing records' statistics. Non-finite thresholds are written as null; an empty pass
list is written (and logged as a warning) — the feature transform rejects an empty include
(`output.include.empty`) rather than emit a table without feature columns. Global window only; a write
failure fails the step.

The closed loop is two configs sharing a version argument: the screen reads the feature manifest
(`candidates.manifest`) and writes the pass list; the next feature run reads it (`output.include`) and writes
its manifest.

## 10. Constraints and diagnostics

Assembly errors (every message names the parameter and what is available): an unknown family or a form not
valid for the family; `groupedMultinomial` without `group`; `rank` / `absdev` / `shuffle` / `inverseShare`
without `group`; a role or candidate field missing from the input schema, or a non-numeric shuffle
reference; a lineage selector without lineage; no candidate left; a conditioning pattern matching nothing
or naming a role / the baseline, or more than 500 columns; `time.from` / `time.to` without `time.field`; an
empty `conditioning`; `pass.minPeriodsAgree` without `periods`, not positive, or a non-integer above 1; a `pass.minGain` that is
not a positive finite number; a triggered input (every Combine would fire per pane); a non-global window with
conditioning or `output.selection`; an unreadable or malformed manifest; streaming input.

Row validity: a null / non-finite label, a null group, a negative poisson label, a null / non-finite /
negative weight → `nRowsInvalid`; a null time → the failure output. Unit skips: no positive label
(grouped), an invalid baseline → `nUnitsSkipped` (in the family's unit; the invalid-baseline part in
`nUnitsSkippedInvalidBaseline`); rows `baseline.invalid: dropRow` removed → `nRowsDropped`.

## 11. Limits

- Univariate, linear probe: pure interactions are invisible (false negatives); a high linear gain can add
  nothing to a tree model (false positives). A ranking device, not an acceptance test.
- The baseline is a fixed offset, and the conditioning is linear in F: a non-linear re-encoding of a
  variable the baseline or F already uses (its rank, its absolute deviation) can still score. Trust low
  `r2_F` candidates more.
- A baseline that omits a real effect shows attenuation: its own feature gets a non-zero z (the baseline
  overstates the effect it does model). Exact conditioning needs the true conditional probability; the e2e
  tests build one by Monte Carlo.
- The placebo quantile calibrates per test; `qValue` gives the false-discovery view; neither controls the
  family-wise error of a large candidate set. Few placebo columns make the threshold noisy — keep the default
  100.
- Blind to time dynamics: the statistic is a window average; read `period_z` for decay.
- Batch only; conditioning and the pass list need the global window.
- One reference per run. Where the return is settled at a price fixed after the decision (a closing price, a
  hammer price), the gain over the decision-time reference overstates what a model can earn: the user doc's
  "Scoring against the settlement reference" runs two screens over one input (one per reference) and joins
  the records downstream for the retained share. A multi-baseline run (`baselines: [...]` with a retained-share
  column and a union pass rule) was considered and declined: it adds a second dimension to every accumulator
  key, the placebo calibration and the conditioning fit to save one input scan, and the retained share — a
  ratio of two one-step gains — is unstable near zero, so it belongs to a downstream join, not to the record.

## 12. Extension positions (designed, not built)

- **Block tests** (`df > 1`) for categorical / vector candidates (one-hot blocks, embeddings from the `onnx`
  transform or a factorization): S a vector, H a matrix, χ²(k), gain per degree of freedom; the record schema
  already carries `df`, and `candidates` will accept `{name, fields: [...]}` blocks.
- **`passRule`**: `placebo` (the current cut) or `fdr` (a q-value cut) — the BH q-value is already computed.
- **Weights as precision weights** (a separate `precisionWeight`), if a consumer needs H to scale with them.
- **Independent-row `rank` / `absdev`**: one KLL quantile-sketch pass over the window before the score pass
  (the profile sink's `KllDoublesSketch`); the grouped transforms stay exact.
- **Windowed marginal screen** for sliding-window drift monitoring: the marginal path is one Combine and
  could run under a trigger; conditioning stays batch.
- **Declared interaction probes** (`cross:<field>` transform variants), bounded by declaration only; the
  stratified form in §12.1 generalises the single product column.
- **In-screen expansion** of non-linear bases and interactions (§12.1).
- **Pruning between passes** of candidates that cannot reach the practical floor (§12.2).
- **Derivation suggestions**: which transform or combination of the candidates to build next (§12.3).

§12.4 orders these positions into steps. The three sections below are the design positions as reviewed:
what the proposal keeps, and where the review changed it (marked *review*).

### 12.1 In-screen expansion

Every statistic is a function of sums, so a basis expansion can live inside the accumulator instead of
being materialised upstream: the cost moves from rows × expanded columns through the feature stages, storage
and shuffles (each expanded column then a separate df = 1 candidate) to the accumulator's state. The case it
pays for is discovery — far more expansions than survivors: screen the expansion in place, and only the
survivors are materialised upstream through the pass list. The positions below are ordered by value per
cost.

**Binned score test** (any univariate shape, at bin resolution). Per candidate and bin, accumulate S_b (the
residual sum) and H_b (the Fisher weight sum). With the intercept profiled out as in §3.1, the row families
give

```
chi2 = Σ S_b² / H_b − (Σ S_b)² / Σ H_b      (χ²(k − 1))
```

in O(k) state; the grouped family needs the k × k matrix H = Σ_g [diag(P_g) − P_g P_g'] (P_g,b the baseline
share of bin b in unit g, the one-hot rows summing to 1 within the unit, so H has rank k − 1 and the test uses
its pseudo-inverse). Missing is a bin of its own (informative missingness), not a zero.

- *Edges — two kinds, declared* (*review*). Value bins need edges over the window: the KLL pre-pass above,
  one pass shared by every candidate, for the grouped family too. The grouped family can also bin by the
  within-unit `rank` (exact, no pre-pass), but that is a *position* bin — the unit's ordinal position, k
  capped by the unit size (a unit of 5 rows fills 5 bins) — not a value bin, and the two answer different
  questions (does the column's level matter / does its standing within the unit matter). `bins: {edges:
  value | rank, k}` names which; the pass list records the edges (value) or the rank cut points (position) so
  the feature transform reproduces the survivor.
- *Categorical candidates* read natively: a level → (S, H) map instead of one-hot or target encoding upstream.
  Exact up to a `maxLevels` cap, beyond it a deterministic seeded hash into buckets (collisions dilute, the
  result stays reproducible); a top-K cut needs a prior counting pass. (*review*: the candidates are numeric
  today — `ScreenRow` carries `double[]`, and the lineage's numeric-column rule selects them — so this is a
  separate step after the binned test: Prepare, the coder and the candidate rule change, the statistic does
  not.)
- *Report-time shapes* from the same (S_b, H_b), no further pass: a trend on the bin index (≈ `rank`), the
  best single cut point (a max-type statistic), the full k − 1 test. It subsumes `rank` / `absdev` and the
  one-hot case of the block tests.

**Heterogeneity across a modifier** (candidate × declared field). Per candidate and modifier level l,
accumulate S_l and H_l (centred within the level). The total Σ S_l² / H_l (df k) splits into the common
effect (Σ S_l)² / Σ H_l (df 1) and the heterogeneity Σ S_l² / H_l − (Σ S_l)² / Σ H_l (df k − 1); the latter
catches a candidate whose effect flips sign across levels, invisible to the marginal test. State
O(m · k). Modifiers: a declared categorical field, the `periods` buckets (their per-bucket S and H are
already accumulated, so the time heterogeneity test is nearly free — and under conditioning the partial
slices S⊥_p / H⊥_p of §8.2 are too, so the partial heterogeneity comes at the same price; *review*), or bins
of the baseline itself (does the effect depend on the predicted level).

**Pairwise products** among m candidates. The product z = x̃_i x̃_j is tested with the main effects as
nuisance: the efficient score S_z − H_zm H_mm⁻¹ S_m with variance H_zz − H_zm H_mm⁻¹ H_mz, per pair from
Σ r x_i x_j, Σ v x_i² x_j², Σ v x_i² x_j, Σ v x_i x_j² and the shared Σ v x_i x_j. Upstream materialisation of
m(m − 1)/2 columns is not realistic, so this is the only practical route. *Where the sums are taken*
(*review*): not at the baseline offset with the main effects fixed at 0 — that is the score test at a point
that is the restricted maximum only when the baseline already absorbs both main effects, and an unmodelled
main effect leaves curvature that the product picks up (the attenuation of §11 in a second form). The pair
sums are taken at the fitted p̂ of a conditioning fit whose F contains the members (in practice the candidate
set itself, k ≤ 500 — the fit exists, §8, and its cost does not depend on the number of pairs), and the
orthogonalisation is against the two members only (the cross-terms with the other members of F are
dropped: an approximation that keeps the state at ≈ 6 doubles per pair; the exact partial test of z against
all of F would cost |F| per pair). State: m = 200 is ≈ 1.2e5 doubles, m = 2000 ≈ 1.2e7 — beyond an
accumulator, so a `maxPairs` bound needs a pre-selection (a declared list, or a ranking pass). A pre-selection
by the marginal ranking misses exactly the pure interactions whose members have no marginal effect; the pHd
loadings below are the better ranking. Fourth-order raw moments lose digits faster than §3.5's second-order
ones: the pair path standardises from a moments pass first (or runs on `rank`).

**Principal Hessian directions** (pHd, Li 1992), a diagnostic. From M = Σ w r x̃ x̃' and the candidates'
covariance Σ (the same pass, O(m²) state), the eigenvectors of Σ^(−1/2) M Σ^(−1/2) with large |eigenvalue| are
the directions of residual curvature — quadratic effects and interactions in bulk, with the loadings naming
the candidates involved (the eigensolver is the feature transform's `SymmetricEigen`). Reported, never a pass
flag; placebo noise columns included in x give the null scale of the eigenvalues and should load ≈ 0. Li's
condition (an elliptically distributed x) does not hold for binary or heavily skewed candidates, whose
loadings are then biased — one more reason it stays a diagnostic.

Shared requirements:

- **Calibration** (*review*). Placebo columns go through the same expansion (a pair's placebo is candidate ×
  noise, which keeps x_i's marginal). The §5 pooling over transform variants assumes one df = 1 statistic;
  with df > 1 or max-type statistics the threshold is per statistic kind — pooled, the high-df kinds would
  lift the cut for the rest. The record's `threshold` is already per record, so each record carries its
  kind's threshold; the summary and the pass list carry a `thresholds` map by kind next to the current
  scalar (which stays the df = 1 cut). Alternatively the comparison moves to the p-value scale; the placebo
  quantile per kind is the direct extension of §5 and keeps `est_gain` comparable within a kind.
- **Conditioning.** The partial test generalises to S⊥' H⊥⁺ S⊥ with a = F̃'Wφ per basis column: state
  m · k · |F| for the binned test.
- **Closing the loop.** A survivor enters the pass list only in a form the feature transform can reproduce (a
  row `bin` with the reported edges, a `cross`, a level grouping); what has no such form (pHd directions, a
  position bin without a `rank` op upstream) stays a diagnostic.
- **Power.** A k-bin test spends k − 1 degrees of freedom on a linear effect that `raw` tests with one; the
  expansions sit next to `raw`, not in its place.
- **Still out of reach**: three-way and higher interactions, and arbitrary two-variable shapes beyond what the
  pHd directions point at.

### 12.2 Pruning between passes

The expensive paths — grouped `rank` / `absdev` (a per-unit sort per candidate), the binned and categorical
maps, the partial sums at m · k · |F|, the pairwise products at O(m²) — spend per-row work on candidates that
are clearly hopeless. Pruning them is a question of where the decision can be taken and against what
threshold.

- **Between passes only.** A Combine has no global view mid-pass, and pruning on a bundle's partial sums
  biases the result (a column weak in one bundle can be strong globally). A pass publishes an *active set*
  as a singleton view and the next pass evaluates only its members: the graph and the pass count stay fixed,
  only the work inside a pass depends on the data — the mechanism by which converged Newton passes evaluate
  nothing (engine doc §4). A pruning pass costs one more read of the input, so it pays only where the
  per-row, per-candidate arithmetic dominates the read; for `raw` marginals alone it does not. Whether it
  does for the expensive paths is measured on Dataflow before the pass is built (*review*).
- **Not against the significance threshold.** The placebo threshold is roughly constant on the χ² scale
  (≈ 6.6 at q99 — pooling the variants does not move the quantile of their χ²(1) draws, §5) whatever N: a
  candidate that just passes has a non-centrality near 6.6, so a 10 % sample sees about 0.7 —
  indistinguishable from the null. Sampling cannot prune near a significance cut.
- **Against the practical floor.** With `pass.minGain` (g_min, §7) the floor's non-centrality is 2N · g_min,
  growing with N. Example: N = 1e7 rows, g_min = 1e-5 gives λ = 200 on the full window and 20 on a 10 %
  sample; pruning at a sample χ² below 4.6 loses a candidate sitting exactly at the floor with probability
  ≈ 1 % (√20 − 2.33 = 2.14, 2.14² = 4.6) and prunes ≈ 97 % of the null ones (P(χ²(1) < 4.6) ≈ 0.968), so the
  heavy work falls to ≈ 13 % plus the survivors. Pruning pays most on the large data where the cost matters.
  The floor is useful on its own and is built (§7); the pruning pass needs it.

Positions:

- **Exact pruning** (no error, near-free): degenerate columns, known after the moments pass; exact duplicate
  columns, found by an order-independent fingerprint Σ hash(row identity, value) accumulated in O(m) and
  computed once per duplicate set. The marginal statistic cannot prune the partial test: a suppressor has
  marginal ≈ 0 and a high partial statistic (§8.3), and no bound links the two.
- **Nested hash samples** (the main position). The sample is a seeded hash of the unit key (grouped) or the row
  identity (independent rows), the placebo derivation, so it is reproducible. Stages f₁ < f₂ < 1 are nested:
  a later stage adds only the increment rows to the earlier sums (successive halving without rework). A
  candidate is pruned when its sample χ² is below the lower ε quantile of χ²(df, 2fN · g_min); ε is declared
  and reported. Placebo columns are never pruned — they are the calibration. A pruned candidate keeps its
  record with `pruned: true` and its sample statistics; it does not silently disappear. Its `qValue` is null
  and it is not in the BH input, but the BH denominator counts every candidate, pruned ones included, so the
  q-values of the survivors stay conservative (*review*: a sample p-value is not comparable with a full-window
  one, so it cannot enter the ranking). Under conditioning the Newton passes do not depend on the candidates,
  so a sample partial pass goes right before the full partial pass (+1 pass). A configuration shape:
  `prune: {sample: [0.1], epsilon: 0.01}` with the floor read from `pass.minGain` (a `prune` without a floor is
  an assembly error).
- **Pairs through a sketch.** A marginal pre-selection misses the pure interactions (§12.1). A randomized range
  finder instead accumulates M Ω = Σ r x̃ (x̃'Ω) with a random Ω (m × d) in O(m · d) per row; the candidates
  loading on M's dominant directions form the set whose pairs the next pass computes in O(m'²). M holds
  r · x_i x_j itself, so members without a marginal effect survive; an interaction spread thinly over many
  candidates is what the approximation loses.

### 12.3 Derivation suggestions

Beyond ranking, the same sums say which transform or combination of the candidates to build next. The
binned sums of §12.1 are the gradient / Hessian histograms of a boosting round on the baseline (the split gain
G_L² / H_L + G_R² / H_R − G² / H is the binned score test restricted to one cut), so the screen already
computes where the next tree over the baseline would cut. Read as estimates rather than a ranking, the sums
give recipes in the feature transform's vocabulary.

**One candidate — how to transform it** (from the binned sums, no further pass):

| information | read from | suggestion |
|---|---|---|
| effect shape | S_b / H_b per bin, the one-step partial-residual curve; each shape (linear, log, sqrt, rank, step at c, hinge max(0, x − c), \|x − c\|) scored by the share of the full binned χ² it captures (the binned test bounds every bin-constant contrast, so the share lies in [0, 1] for the step / bin shapes; a smooth shape is evaluated at the bin representatives and its share is approximate) | "log(x) captures 95 %", "no effect above c → clip at c" |
| cut points | the best split gain (a boosting round's first split) | a row `bin` with the edges, an indicator x > c |
| missingness | the missing bin's S / H against the other bins | an `_isnull` indicator, or the fill value whose bin matches the missing effect |
| monotonicity | sign consistency of the bin effects, the isotonic fit's share | a monotone constraint for a boosted model, with its direction |
| categorical grouping | levels sorted by S_l / H_l and cut optimally (the boosted-tree categorical split) | a level grouping; top-level one-hot for a few strong levels, a shrunk encoding (the feature transform's backoff) for many sparse ones |

**Several candidates — how to combine them:**

| information | needs | suggestion |
|---|---|---|
| redundancy clusters | the candidates' Fisher matrix H (m × m) | near-duplicate sets: keep one, or average / project them |
| complementary set | the same H and S | a report-time forward selection: the score test of candidate j given a selected set is closed-form at β = 0 (a one-step approximation of the partial test, not the fitted one), so no pass re-reads the data — a set that works together, which a univariate ranking cannot give |
| linear composite | the same | β = H⁻¹S, the best linear combination to add to the baseline |
| ratios and differences | the two-dimensional Newton direction of a pair, on log-transformed candidates | coefficients ≈ (+1, −1) → x_i / x_j; on the raw scale ≈ equal and opposite → x_i − x_j; suggested only when the pair's joint χ² clearly exceeds the better single one |
| interaction shape | a two-dimensional histogram of a selected pair (O(k²), a depth-2 tree) | "x_j matters only when x_i > c" → a conditional feature or crossed bins |
| segment / time dependence | the heterogeneity test (§12.1) | a cross with the modifier; an effect decaying over periods → a shorter window |
| curvature directions | pHd (§12.1) | the projection v'x and its square |

**Parameter families.** When the feature transform emits a family (a window of 7 / 30 / 90 days), gain
against the parameter gives the best value and the point where the gain saturates. The lineage today
(scope / block / derivedFrom / evidence / kind — `FeatureLineage.Entry`) and the manifest carry no op or
arguments, so this needs the feature transform to expose them first; it is the last step of §12.4.

Guards:

- **Honest gain.** A shape chosen and scored on the same data is optimistic. A seeded hash of the unit splits
  the window into a discovery and a confirmation half, each with its own sums (twice the state): the shape is
  chosen on the first, its gain reported on the second.
- **Calibration.** Placebo columns go through the same search, maxima included; thresholds are per suggestion
  kind (§12.1).
- **Output.** A `<name>.suggestions` output: kind, inputs, parameters (edges, cut, coefficients), a feature
  DSL fragment, the captured share, the gain over the best single candidate, the confirmation-half gain.
- **Hypotheses, not decisions.** The score test is local to β = 0; a composite with a large effect is
  approximate. Suggestions are never applied automatically: they go into a feature spec and are checked by
  the next screen or by the `evaluation` transform.
- **Out of reach.** Features from information absent from the input; temporal aggregations beyond the
  families the candidates already span.

Scope (*review*): the one-candidate table is near-free once the binned sums exist and every row of it is a
feature op that exists today (row `bin` with hand-written edges, `cross`, an expression); the
several-candidate table needs the m × m H and is a larger surface for a less certain use — it follows, with
the redundancy clusters first.

### 12.4 Steps

In value-per-cost order, each a PR on its own; the floor is built (§7).

1. **KLL pre-pass** (the extension position above): independent-row `rank` / `absdev`, and the value-bin
   edges of §12.1 for every family. One pass shared by every candidate, before the score pass.
2. **Binned score test** with per-kind thresholds, the missing bin, `bins: {edges, k}`, and the
   heterogeneity test (`by: periods | <field> | baselineBins`, marginal and partial).
3. **One-candidate suggestions** with the discovery / confirmation split and the `<name>.suggestions` output.
4. **Pruning** (nested hash samples, the active-set view) — after a Dataflow measurement shows the
   per-row arithmetic of steps 1–2 dominating the read.
5. **Pairs** on the conditioning fit's p̂, `maxPairs` with a declared list or the sketch; **pHd**; the
   several-candidate suggestions.
6. **Categorical candidates** read natively.
7. **Parameter families**, once the feature lineage carries op and arguments.
