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
`block:<name>`, `evidence:<declared|measured>` — resolved
against the lineage the feature transform attached to its output schema (`feature.*` field options, when
it is the direct upstream; its pass-through input fields carry `scope: input` and their kind, so a
passed-through market column is excluded by the same selector as the columns derived from it) or against
its manifest (`candidates.manifest`, when the table came back through a sink). A selector with no lineage
available is an assembly error.

**Defaults from the feature manifest.** When `candidates.manifest` is given, its `roles` fill `group` /
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

A column with H ≤ 0, fewer than two observed rows, a constant within every group, or (gaussian) no residual
variance is *degenerate*: `est_gain = 0`, `z = 0`, `beta` null, `degenerate = true`. Degenerate placebo
columns enter the placebo quantile as 0.

## 4. Baseline forms

| family | forms | meaning |
|---|---|---|
| `groupedMultinomial`, `binomial` | `prob` (default), `logProb`, `inverseShare` | a probability (normalised within the group for the grouped family), its log, or 1 / x made a share within the group (odds, prices; needs `group`) |
| `gaussian` | `value` | the predicted value |
| `poisson` | `rate` (default), `logRate` | the predicted rate, or its log |

A bare field name takes the family's default form; a form not valid for the family is an assembly error.
Binomial probabilities are clamped to [ε, 1 − ε]. A unit whose baseline is invalid for its form (a negative
probability, a non-positive rate, a share that does not sum) is skipped whole and counted (`nUnitsSkipped`),
never partially scored.

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
| `rank` | percentile rank within the group, in [0, 1], ties sharing the mean rank, 0.5 for a single observed value | monotone non-linear effects, outlier robustness |
| `absdev` | \|x − median of the group's observed values\| | symmetric "extremeness" effects |

Records are keyed by (`candidate`, `transform`). `rank` and `absdev` are within-group statistics: with
independent rows only `raw` is available (a window-wide quantile sketch is the extension position, §12).
Default: all three with `group`, `raw` without; an explicit list is never widened.

## 7. Periods, time window, flags, q-values

- **Periods.** `periods: {field, bucket}` (year / quarter / month / week / day, UTC; `field` defaults to
  `time.field`, with its own type) accumulates the same sums per bucket. The record reports each bucket's S,
  H, z and observed rows (`period_z`), the buckets with usable information (`n_periods`) and how many agree
  with the overall sign (`periods_agree`) — the material for reading a decaying effect.
- **Time window.** Rows after `time.to` or before `time.from` are not screened and are counted
  (`nRowsTimeFiltered`). Screening the evaluation period leaks the evaluation into the selection.
- **Leak flag.** `flags.leakZ` marks a candidate with |z| above it as `leakSuspect` — a flag, never a
  rejection: a leak's signature is a z several times the healthy top, and only lineage can tell.
- **q-values.** Benjamini–Hochberg over the candidate records' p-values (of the effective test, §8.5) gives
  the false-discovery view; `passed` itself is the placebo cut (`est_gain > threshold`). Making `passed`
  follow the q-value is an extension position (§12).

## 8. Conditioning: the partial test

`conditioning.fields` names an existing feature set F (globs; the role fields and the baseline cannot be
conditioned on). The transform then measures what each candidate adds *beyond F*.

### 8.1 The conditioning model

η = offset + F̃·θ with F̃ = F standardised (missing → the mean) — the conditional logit within the group for
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
(array of {period, z, S, H, n}), `r2_F`, `partial_S / H / chi2 / z / gain / pValue` (null without
conditioning), `threshold`, `passed`, `leakSuspect`, `placebo`, `degenerate`. Field names follow the
proposal that introduced the transform so its reference implementation compares directly.

### 9.2 Summary (`<name>.summary`)

One record per run (per window under a windowing strategy): the spec's roles, `test`, the thresholds and the
quantile, the seed, the row and unit counts (in, time-filtered, invalid, scored, skipped), the candidate /
transform / scored / passed / placebo / leak-suspect counts, the time field and window, the scored rows' time
range, the period bucket, `transforms`, `candidates`, `passedColumns` (candidate names with a passing
transform, best gain first), the conditioning fields / size / iterations / rejected steps / convergence /
gain / l2, and `notes` (role defaults applied, columns excluded by lineage, fallbacks).

### 9.3 The pass list (`output.selection`)

One JSON document written at the end of the run, in the shape the feature transform's `output.include`
reads (`{columns: [...]}` first) plus the provenance a consumer needs to trust it: `test`, family / method,
thresholds, quantile, counts, the time window, `planHash` / `outputHash` of the upstream feature manifest
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
empty `conditioning`; a triggered input (every Combine would fire per pane); a non-global window with
conditioning or `output.selection`; an unreadable or malformed manifest; streaming input.

Row validity: a null / non-finite label, a null group, a negative poisson label, a null / non-finite /
negative weight → `nRowsInvalid`; a null time → the failure output. Unit skips: no positive label
(grouped), an invalid baseline → `nUnitsSkipped` (in the family's unit).

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
- **Declared interaction probes** (`cross:<field>` transform variants), bounded by declaration only.
