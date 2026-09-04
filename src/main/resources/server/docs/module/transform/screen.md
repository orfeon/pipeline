---
type: Transform Module
title: Screen Transform Module
description: Baseline-conditioned feature screening before training. Scores every numeric candidate column against the label with a Rao score test of an offset GLM (one closed-form Combine, no learner), so the score is the one-step log-likelihood improvement over an existing prediction. Placebo-calibrated pass threshold (noise and within-group shuffle columns), transform variants (raw / rank / absdev), per-period sign agreement, a time window that fences off the test period, leak-suspect flags, Benjamini–Hochberg q-values. Optional conditioning (partial test) fits an existing feature set by unrolled Newton passes and scores what each candidate adds beyond it (r2_F, partial gain). Families groupedMultinomial (conditional logit within a group) and binomial. output.selection writes the pass list the feature transform's output.include reads (closed loop). Batch only.
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

- `x̃` is the candidate centred by the p-weighted mean over the observed rows (within the group for
  `groupedMultinomial`, over the window for `binomial`); a missing value after centring is 0 (no information).
  The statistic is invariant to the scale of x and to a constant shift within the group.
- Output per column × transform: `S`, `H`, `beta = S/H`, `chi2 = S²/H` (χ²(1) under the null), `z = sign(S)·√chi2`,
  `est_gain = chi2 / (2N)`, `pValue` (χ²(1) upper tail), `qValue` (Benjamini–Hochberg over the candidate records).
- Degenerate columns (`H ≤ 0`, fewer than two observed rows, a constant within every group) get `est_gain = 0`
  and `degenerate = true`.
- Weights (`weight`): per row for `binomial`; the row mean of the unit for `groupedMultinomial`.

### Placebo calibration

`est_gain` is a squared statistic: it is positive under the null too (χ²(1) / 2N scale), so **no absolute
threshold applies**. The transform adds placebo columns to the same pipeline — `placebo.noise` standard-normal
columns (`__noise_<i>`) and `placebo.shuffle.n` within-group permutations of a reference column
(`__shuffle_<i>`, marginal distribution kept, alignment broken) — and takes the `placebo.quantile` quantile
of their `est_gain` (over every transform variant) as the pass threshold. The default q99 (not q95) accounts for
the candidate × transform multiplicity. Without placebo columns (`noise: 0`, no shuffle) the theoretical χ²(1)
quantile / 2N is used; it is always reported as `thresholdTheoretical` in the summary — the two agree when the
statistic is well calibrated.

The placebo random numbers are derived from `placebo.seed` and the unit key (the group key, or the row identity
for independent rows), so a re-run reproduces the same columns.

### Transform variants

| transform | definition | catches |
|---|---|---|
| `raw` | the column as is | direct linear effect |
| `rank` | percentile rank within the group, in [0, 1] (ties share the mean rank) | monotone non-linear effects, outlier robustness |
| `absdev` | \|x − median of the group\| | symmetric "extremeness" effects |

Records are keyed by (`candidate`, `transform`). `rank` and `absdev` need `group` (the within-group
statistics); independent rows support `raw` only in this version.

### Periods, time window and leak flags

- `periods` computes S and z per calendar bucket of a time field; `periods_agree / n_periods` counts the buckets
  whose sign matches the overall sign, and `period_z` lists them — the material for reading a decaying effect.
- `time.to` (and `time.from`) fence the window: rows outside are not screened (`nRowsTimeFiltered` in the
  summary). Screening the test period is the classic way to leak the evaluation into the selection.
- `flags.leakZ` marks a candidate with |z| above the value as `leakSuspect` (a known leak typically stands out by
  a factor of several over the healthy top). **It is a flag only, never a rejection.**

### Conditioning (partial test)

`conditioning.fields` names an existing feature set F. The transform fits the conditioning model
η = offset + F̃·θ (the conditional logit within the group for `groupedMultinomial`; the logistic model with an
intercept for `binomial`; F̃ = F standardised, missing → mean) by Newton's method with an L2 penalty on the
*average* log-likelihood, then orthogonalises every candidate against F in the Fisher metric W of the fitted
model and reads the score test of what is left:

- `r2_F = 1 − x⊥'Wx⊥ / x'Wx` — how much of the candidate F already explains (1 = fully redundant);
- `partial_S`, `partial_H`, `partial_chi2`, `partial_z`, `partial_gain`, `partial_pValue` — the score test of x⊥.

With conditioning, `passed`, `threshold` and `qValue` refer to the **partial** test (the placebo columns take
the same route, so the threshold is calibrated for it); the marginal statistics stay in the record. Reading the
two together classifies a candidate: marginal high × partial high = new information; marginal high × partial ≈ 0
= redundant with F (high `r2_F`); marginal ≈ 0 × partial high = a suppressor effect.

Cost: one pass for the column moments, one pass per Newton iteration (at most `maxIter`; a rejected step halves
the step size and costs one more pass; converged iterations are skipped) and one pass for the partial sums —
`maxIter + 2` passes over the data at most, each a global Combine. The summary reports `conditioningIterations`,
`conditioningRejectedSteps`, `conditioningConverged` and `conditioningGain` (the in-sample average
log-likelihood improvement of F over the baseline — a sanity check that the conditioning set is informative).
Conditioning needs the global window (no `strategy` window) and, like every screen run, the default trigger.

## Input contract

| role | description |
|---|---|
| `group` (optional) | mutually exclusive samples of one unit (a query's candidates, a session's listings, a lot's bids). Required for `groupedMultinomial`. Omitted: every row is independent. |
| `label` | the label field, or an expression over numeric fields (`{expr: "rank == 1 ? 1 : 0"}`). Several positives in a group are normalised (`normalizeTies: true`). |
| `baseline` (optional) | the reference prediction: `form: prob` (a probability; normalised within the group for `groupedMultinomial`), `logProb`, `inverseShare` (1/x made a share within the group — odds, prices). Omitted: the prior (uniform share / prior rate). |
| `time` (recommended) | the time field (`timestamp` / `date` / ISO string); `to` / `from` fence the window. Omitted: the element timestamp is used (set the source's `timestampAttribute`; bounded sources otherwise carry the minimum timestamp, so `to` / `from` require `field`). |
| `weight` (optional) | a sample-weight field. |
| candidates | numeric input fields (`int32` / `int64` / `float32` / `float64` / `bool`) selected by name globs or lineage selectors; role fields are never candidates. |

The transform takes the `feature` transform's row form (one row = entity × context). The `output.groupBy`
parent/child form is not accepted (unnest upstream).

**Defaults from the feature manifest.** When `candidates.manifest` points at the upstream feature transform's
`output.manifest`, its `roles` fill `group` / `label` / `baseline` / `weight` and its `timeField` fills `time.field`
when they are not set — the data contract declared once on the feature side is reused here.

**Lineage selectors.** `candidates.include` / `exclude` accept, next to name globs (`f_*`, `odds*`), the
lineage selectors of the feature transform's `output.exclude`: `derivedFrom:<field>`, `scope:<row|context|sequence|population>`,
`block:<name>`, `evidence:<declared|measured>`. Lineage is read from the input schema when the feature transform is
the direct upstream (its column options travel with the schema), or from `candidates.manifest` when the table
comes back through a sink / source. Using a selector without any lineage available is an assembly error.

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
| family | optional | String | `groupedMultinomial` (default) or `binomial`. `gaussian` / `poisson` are planned and rejected with a message. |
| group | optional | String | Group key field. Required for `groupedMultinomial`. |
| label | required | String or Object | Field name, or `{field}` / `{expr, normalizeTies}`. `expr` is a [Lucene expression](https://lucene.apache.org/core/10_5_0/expressions/org/apache/lucene/expressions/js/package-summary.html) over numeric fields; `normalizeTies` (default true) normalises the labels of a group to sum 1. |
| baseline | optional | String or Object | Field name (form `prob`), or `{field, form}` with form `prob` / `logProb` / `inverseShare`. |
| time | optional | String or Object | Field name, or `{field, to, from}` with ISO-8601 instants. Rows after `to` / before `from` are not screened. |
| weight | optional | String or Object | Weight field (`{field}` accepted). |
| rowId | optional | Array<String\> | Fields that identify a row (the placebo noise seed and the tie-break of rows sharing a time). Default: every field value. |
| candidates | optional | Object or Array | `{include: [globs / selectors], exclude: [globs / selectors], manifest: <uri>}`, or a list of include globs. Default include `["*"]`. |
| transforms | optional | Array<String\> | Any of `raw`, `rank`, `absdev`. Default: all three with `group`, `raw` without. |
| periods | optional | Object or String | `{field, bucket}` or a bucket name; bucket `year` / `quarter` / `month` / `week` / `day` (UTC). `field` defaults to `time.field`. |
| placebo | optional | Object | `noise` (standard-normal columns, default 100), `shuffle: {field, n}` (within-group permutations of `field`, default n 100; needs `group`), `quantile` (default 0.99), `seed` (default 0). `noise: 0` without shuffle falls back to the theoretical threshold. |
| flags | optional | Object | `leakZ`: flag candidates with \|z\| above it as `leakSuspect`. Default: no flag. |
| conditioning | optional | Object or Array | `{fields: [names / globs], l2, maxIter, tol}` or a list of fields: the partial test against an existing feature set (see [Conditioning](#conditioning-partial-test)). `l2` (default 1e-4) penalises the average log-likelihood; `maxIter` (default 10, at most 100) is the number of Newton passes over the data; `tol` (default 1e-8) the objective improvement that ends the fit. Needs the global window. |
| output | optional | Object | `selection`: URI / path of the pass-list file written at the end of the run (see [Closing the loop](#closing-the-loop-outputselection)). Needs the global window. |

## Outputs

The default output (`<name>`) holds one scoring record per column × transform, placebo columns included.
`<name>.summary` holds one record per run (per window under a windowing strategy).

### Scoring record

| field | type | description |
|---|---|---|
| candidate | STRING | column name (`__noise_<i>` / `__shuffle_<i>` for placebo columns) |
| transform | STRING | `raw` / `rank` / `absdev` |
| method | STRING | `scoreTest` |
| family | STRING | the family |
| S, H, beta, chi2, z, est_gain | FLOAT64 | the statistics above (`beta` null when degenerate) |
| df | INT64 | degrees of freedom (1) |
| pValue, qValue | FLOAT64 | χ²(1) upper tail; Benjamini–Hochberg q-value over the candidate records (null for placebo) |
| n_groups | INT64 | scored units (groups, or rows when independent) — the N of `est_gain` |
| n_obs | INT64 | rows whose transformed value is finite |
| periods_agree, n_periods | INT64 | buckets agreeing with the overall sign / non-degenerate buckets |
| period_z | ARRAY<STRUCT<period STRING, z FLOAT64, S FLOAT64, H FLOAT64, n INT64\>\> | per bucket |
| r2_F | FLOAT64 | conditioning only: redundancy of the candidate with F (1 = fully explained) |
| partial_S, partial_H, partial_chi2, partial_z, partial_gain, partial_pValue | FLOAT64 | conditioning only: the score test of the candidate orthogonalised against F |
| threshold | FLOAT64 | the placebo quantile (or theoretical) threshold — of the partial gain with conditioning |
| passed | BOOL | `est_gain > threshold` (`partial_gain` with conditioning), candidate columns only |
| leakSuspect | BOOL | \|z\| > `flags.leakZ` |
| placebo | BOOL | placebo column |
| degenerate | BOOL | no usable information (constant / too few rows) |

### Summary record

`family`, `method`, `group`, `label`, `baseline`, `baselineForm`, `weight`, `threshold`, `thresholdTheoretical`,
`quantile`, `seed`, `nRows`, `nRowsTimeFiltered`, `nRowsInvalid` (null label / group / weight), `nRowsScored`,
`nUnits`, `nUnitsSkipped` (in the same unit as `nUnits`: groups without a positive label or with an invalid baseline; for `binomial` with a `group`, the rows of a group holding an invalid baseline), `nCandidates`,
`nTransforms`, `nScored`, `nPassed`, `nPlacebo`, `nLeakSuspect`, `timeField`, `timeFrom`, `timeTo`, `minTime`,
`maxTime` (TIMESTAMP, of the scored rows), `periodsBucket`, `transforms`, `candidates`, `passedColumns` (candidate
names with a passing transform, best gain first — the list to feed back into the feature transform's
`output.include`), `conditioningFields`, `conditioningK`, `conditioningIterations`, `conditioningRejectedSteps`,
`conditioningConverged`, `conditioningGain`, `conditioningL2` (null without conditioning), `notes` (role defaults
applied, columns excluded by lineage).

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
        exclude: ["derivedFrom:final_price", "derivedFrom:current_bid", "scope:row"]
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
        exclude: ["derivedFrom:final_price", "scope:row"]
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
  "family": "groupedMultinomial", "method": "scoreTest",
  "threshold": 0.000063, "thresholdTheoretical": 0.000067, "quantile": 0.99,
  "nCandidates": 27, "nPassed": 2, "nUnits": 49839,
  "timeFrom": null, "timeTo": "2025-06-30T23:59:59Z",
  "screenHash": "…", "planHash": "…", "outputHash": "…", "manifest": "gs://…/manifest.json",
  "conditioningFields": ["model_a", "model_b"],
  "createdAt": "2026-09-05T10:00:00Z",
  "passed": [{"candidate": "f_extra", "transform": "rank", "est_gain": 0.00077, "z": 8.95, "partial_gain": 0.00051, "partial_z": 7.1, "r2_F": 0.035, "leakSuspect": false}]
}
```

- `columns` is what the feature transform's `output.include` reads (`{columns: [...]}` is one of its accepted
  shapes); the other members record how the list was produced.
- `test` says which statistic the cut-off used (`partial` with a converged conditioning fit, else `marginal`).
- `planHash` / `outputHash` are the upstream feature manifest's identities when `candidates.manifest` was given
  (null otherwise); `screenHash` is the SHA-256 of this step's canonical parameters. Together they make the pass
  list traceable to the plan that produced the candidates and the configuration that screened them.
- The file is written once per run from the finalize step (global window only); a failed write fails the step.
  Keeping a ledger of runs is a matter of versioned paths (`${args.version}`) or an `action/storage` copy.

## Reading the output

- Rank candidates by `est_gain` (or `z`); `passed` is the placebo-calibrated cut-off, `qValue` the
  false-discovery view over the candidate set.
- A candidate with a high `raw` score is a direct linear effect; one that only scores under `absdev` is an
  "extremeness" effect; `rank` catches monotone non-linear effects and is robust to outliers.
- `periods_agree` far below `n_periods` means an unstable effect: look at `period_z` for a decay over time.
- `leakSuspect` candidates deserve a look at their lineage before they are used: an outsized z is the typical
  signature of a column computed after the outcome.
- `output.selection` writes the pass list in the format the feature transform's `output.include` reads, so
  the next feature run emits only the screened columns (see below); `passedColumns` in the summary is the same list.

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
- Independent rows (`group` omitted) support `raw` only; `rank` / `absdev` over the whole window need a
  quantile sketch (planned).
- Conditioning needs the global window and costs `maxIter + 2` passes; keep the conditioning set to a few
  hundred columns (the Newton Gram matrix is k × k).
- Planned: `gaussian` / `poisson` families.
