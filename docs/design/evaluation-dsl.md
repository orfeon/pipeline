# Evaluation Transform DSL (Design Document)

Status: **Implemented (stages 1–2)** — the contract described here is what `module: evaluation` accepts today:
families `groupedMultinomial` / `binomial`, several prediction sets against one baseline, time splits with
a `selection` / `report` role, the excess log score with a Poisson bootstrap CI (paired between prediction
sets), the calibration tables, the calibration fits (temperature / blend, estimated on a selection split and
compared as derived prediction sets), declared slices and period buckets, the per-unit output. §11 lists the
stages that are designed but not built (slice discovery, the gaussian / ranking families, the HTML report). The user-facing reference is `src/main/resources/server/docs/module/transform/evaluation.md`; the
execution side is [evaluation-engine.md](evaluation-engine.md).

## 1. Purpose and position

The `evaluation` transform verifies predictions **after training**: it matches prediction columns against
the outcome and reports, as its first-class statistic, the **excess information over a baseline
prediction** — the forecast-verification habit (skill against climatology, reliability diagrams) applied to
grouped data on Beam. The model itself is never read: the predictions may come from an ML model, a rule, a
human or the system being replaced, and the computation is the same.

It closes the learner-free trio: feature (generation) → screen (ranking before training) → evaluation
(verification after training), the supervised counterpart of the `profile` sink's data-quality report. Its
motivation is one measurement: on a problem with a strong baseline (a market, an incumbent system), the
model that wins on logloss / accuracy is the one that imitates the baseline, and only the excess log score
tells whether it adds anything. The transform makes that discipline a configuration rule: selection happens
on a `selection` split, reporting on a `report` split, and every calibration fit (§11) is allowed on the
former only.

### 1.1 Design principles

- **One intermediate representation.** Every unit (a group, or an independent row) is reduced to its loss
  decomposition once: log score under each prediction set and under the baseline, the hit, the Brier term.
  Every metric, CI, slice and pair difference is an aggregate of those records, and the records themselves
  are an output (§8.4) so a consumer can re-aggregate or feed them to the `attribution` transform.
- **One bounded Combine per table, pass count independent of the data** — the screen transform's rule.
  The metrics, the bootstrap, the slices and the fixed-edge calibration tables share one Combine; the
  quantile-binned tables add one sketch pass.
- **Common unit set.** A unit missing any prediction set or the baseline is excluded from every prediction
  set: comparisons are always on the same units (dropping only one side's missing units biases Δ).
- **Deterministic.** The bootstrap weights derive from the declared seed and the resampling unit's key, so a
  rerun on any runner reproduces every CI.
- **Same vocabulary as screen and feature.** Families, baseline forms, `group` / `label` / `rowId` / `weight`
  / `time`, the role defaults from the feature transform's lineage and its manifest.

## 2. Input contract

One row = one sample (an entity in a group, or an independent row) carrying the outcome, the baseline and
the prediction columns. The `output.groupBy` parent / child form of the feature transform is not accepted.

| role | parameter | meaning |
|---|---|---|
| family | `family` | `groupedMultinomial` (mutually exclusive samples within a group: the candidates of a query, the listings of a session) or `binomial` (independent binary rows). Default `groupedMultinomial`. |
| unit | `group` | the group key; required for `groupedMultinomial`, optional for `binomial` (then only the default bootstrap unit). |
| outcome | `label` | a field, or `{field}` / `{expr, normalizeTies}`; `expr` is a Lucene expression over numeric fields. Grouped labels are normalised to sum 1 (`normalizeTies`, default true: a tie shares the group's one likelihood term). |
| baseline | `baseline` | a field or `{field, form}` with the family's forms (`prob` default, `logProb`, `inverseShare` — 1 / x made a share within the group, for odds and prices). Omitted: the prior (§4.4). |
| predictions | `predictions[]` | the prediction sets (§3). At least one. |
| time | `time` | a field (`timestamp` / `date` / ISO string) or `{field}`; the splits' ranges and the period buckets read it. Defaults to the feature transform's time role. Required when a split declares a time range. |
| splits | `splits` | the time splits with their roles (§5). Required. |
| weight | `weight` | a sample-weight field: per row for `binomial`, the unit mean for the grouped family. |
| identity | `rowId` | fields identifying a row (the sort tie-break within a unit, the bootstrap unit of independent rows). Default: every field value. |
| utility | `utility` | `{field}`: the realised value of a positive row (a payoff); the calibration tables report the flat return per bin (§7). |
| manifest | `manifest` | the upstream feature manifest URI, for the role defaults and the lineage selectors when the table came back through a sink. |

**Defaults from the feature transform.** As for screen: `group` / `label` / `baseline` / `weight` and
`time.field` fall back to the roles the feature transform declared (the `feature.role` field options of the
direct upstream, or the manifest's `roles` / `timeField`).

## 3. Prediction sets

```yaml
predictions:
  - {name: candidate, prob: p_candidate}                                   # a probability column
  - {name: raw, field: s_raw, form: logProb}                               # any baseline form
  - {name: scored, score: f, offset: log_m, offsetScale: log, temperature: 1.0}   # grouped softmax
```

- `name` (required, unique; `baseline` is reserved — the baseline is reported under that name).
- `prob: <field>` — a probability (`form: prob`); `field` + `form` accepts every form of the family
  (`prob` / `logProb` / `inverseShare`); a grouped set is normalised within the group like the baseline.
- `score` + optional `offset` + `temperature` (grouped family only): p ∝ w · exp(score / T) within the group,
  with w the offset value (`offsetScale: prob`, default) or exp(offset) (`offsetScale: log`). This is the
  feature transform's `softmax` context op; a model that outputs a raw score to be combined with the
  baseline's log share reads as `score: f, offset: log_m, offsetScale: log`.

A unit with a null / non-finite / invalid value in any set (or in the baseline) is skipped whole
(`nUnitsSkipped`), never partially compared (the common unit set).

## 4. Metrics

### 4.1 Per unit

For a grouped unit with normalised labels ỹ (Σ ỹ = 1), baseline shares p and a prediction set's shares q:

```
logScore        = Σ_i ỹ_i log q_i                   (the mean over tied positives)
logScoreBaseline = Σ_i ỹ_i log p_i
excessLogScore  = logScore − logScoreBaseline        (Δ)
hitAt1          = ỹ at argmax q                       (ties at the maximum share the credit)
brier           = Σ_i (q_i − ỹ_i)²
```

For a `binomial` row with label y ∈ {0, 1} and probability q: `logScore = y log q + (1 − y) log(1 − q)`,
the same under the baseline, `brier = (q − y)²`; `hitAt1` is not defined (null).

### 4.2 Aggregates

Over the units of a key (split × prediction set × slice value), weighted by the unit weight w:

```
value(m) = Σ w_g m_g / Σ w_g          for m in logScore, logScoreBaseline, excessLogScore, hitAt1, brier
logloss  = −logScore
```

**Δ > 0 is the only sign that the prediction carries information the baseline lacks.** `logloss`, `hitAt1`
and `brier` are reported next to it and are not selection criteria: on a problem with a strong baseline the
logloss-best model is the one imitating the baseline (the measurement behind this transform).

### 4.3 Pair differences

For every ordered pair of prediction sets (A, B) with A before B in the declaration, a record with
`prediction: A, pair: B` carries the differences `A − B` of every metric over the same units, with the paired
bootstrap CI (§6): the weights are drawn per unit, so Σ w (m_A − m_B) = Σ w m_A − Σ w m_B and the pair CI
costs no accumulator of its own.

### 4.4 Prior mode

Without `baseline` the offset is the prior: the uniform share 1 / n within the group, or the split's label
mean for `binomial`. Δ then reads as the skill over the prior. The binomial prior is a function of the
split's positives and weight mass alone (Σ w [y log ȳ + (1 − y) log(1 − ȳ)] = W [ȳ log ȳ + (1 − ȳ) log(1 −
ȳ)]), so it is derived at the end from sums the bootstrap vectors already carry; the per-unit records carry
`logScoreBaseline` null in that case.

## 5. Splits

```yaml
time: {field: event_time}
splits:
  valid: {from: "2026-01-01", to: "2026-03-31", role: selection}
  test:  {from: "2026-04-01", to: "2026-06-30", role: report}
```

- **By time range.** Every entry is `{from, to, role}` on `time.field`; `from` is inclusive, `to` inclusive
  (an ISO instant, or a date read as the start / end of that UTC day). Ranges must not overlap, and every
  `selection` split must end before every `report` split starts (assembly errors: the selection must never see
  the report period). A row outside every range is counted (`nRowsUnassigned`) and dropped.
- **By column.** `splits: {field: fold, roles: {valid: selection, test: report}}` reads the split name from a
  column written by the training job. The ordering cannot be checked statically, so the summary reports each
  split's observed time range and adds a note when a `selection` range overlaps a `report` range (a random
  split shows as overlapping ranges).
- **Roles.** `selection` (model choice, calibration fits) / `report` (the reported numbers). At least one
  `report` split is required. Stage 1 computes the same tables on every split; the role matters for the
  fits (§11) and for the reader.

A grouped unit is keyed by (split, group): a group whose rows fall in two splits is two units.

## 6. Bootstrap confidence intervals

```yaml
bootstrap: {samples: 1000, seed: 7, unit: event_date}
```

Units are resampled by the Poisson bootstrap in one pass: for resampling unit u and replicate b the weight
w_{u,b} ~ Poisson(1) is drawn from `seededRandom(seed, unitKey + b)`; every accumulator carries, per
replicate, Σ w_{u,b} w_g m_g and Σ w_{u,b} w_g, and the 2.5 / 97.5 percentiles of the B replicate means are
the 95% CI (`<metric>_lo` / `<metric>_hi`). The same weights serve Δ (the difference of two sums) and the pair
differences (§4.3).

- `samples` (default 1000, 0 disables; at most 10000) is the accumulator width: every key holds
  (metrics + 2) × samples doubles.
- `unit` (default: the group, or the row identity for independent rows) names a field whose value is the
  resampling unit: `unit: event_date` makes a cluster bootstrap by day, the answer to correlated units
  (same-day, same-venue groups) that the group bootstrap ignores.
- The CI assumes the resampling units are independent; a unit whose `unit` field is null falls back to the
  group / identity.

## 7. Calibration tables

```yaml
calibration:
  - {type: reliability, by: prediction, bins: 10}
  - {type: reliability, by: divergence, bins: 10}
  - {type: reliability, by: field, field: price, edges: [1, 2, 3, 5, 10, 20, 50, 100]}
  - {type: edge, thresholds: [1.0, 1.1, 1.25, 1.5, 2.0]}
```

Row-level, per split × prediction set. Each bin reports `n`, `positives` (Σ ỹ), `p_model`, `p_baseline`
(means), `rate` (positives / n) with its Wilson 95% interval, and `utility` — the flat return Σ u ỹ / n when
`utility.field` is set (the mean realised value of buying every row of the bin at unit stake).

| type / by | bins | reading |
|---|---|---|
| `reliability` / `prediction` | quantiles of the prediction (KLL sketch over the split's rows) | the reliability diagram |
| `reliability` / `divergence` | quantiles of f = logit q − logit p (grouped: the logit of the shares, as in the reference implementation) | if the rate stays at `p_baseline` where f is large, the divergence is noise (over-confidence); if it follows `p_model`, information |
| `reliability` / `field` | fixed `edges` on a declared numeric field (a price band, an odds band) | rare-event bands |
| `edge` | one group per threshold: the rows with q > threshold × p | if the rate exceeds `p_baseline` systematically, the divergence is information |

Quantile bins are `bins` equal-rank intervals of the sketch (`k = 400`, an approximation of the boundaries
within the sketch's rank error; the counts per bin are exact for the boundaries used); `edges` bins are
exact. Every bin record carries `lower` / `upper`; edge records carry the threshold in `lower`.

### 7.1 Calibration fits

```yaml
calibration:
  - {type: temperature, fitOn: valid, of: [candidate], grid: [0.5, 3.0, 51]}
  - {type: blend, fitOn: valid, of: [scored], l2: 1e-4, maxIter: 10, tol: 1e-8}
output:
  calibration: gs://bucket/eval/${args.version}/calibration.json
```

A fit is a small model, so the contract binds it: `fitOn` must name a `selection` split (a `report` split is
an assembly error), and the fitted set enters the run as a **derived prediction set** — `<name>@T` /
`<name>@blend` — compared on every split like a declared set (metrics, intervals, pairs, slices, calibration
tables, units). `of` names the base sets (default: every declared set; one fit of each type per set).

Every base set has two fit inputs per row: f — a score set's score (over its declared temperature), a
probability set's log share (grouped) / logit (binomial) — and o — the score set's own offset on the log
scale, else the baseline's log share / logit (a blend without either is an assembly error).
A row the base set gives mass exactly 0 (a zero prob-scale offset, a zero share) enters the fit inputs at the
log floor (log 1e-12) and keeps mass 0 in the derived set. The weights are frequency weights: the fits'
standard errors scale with the magnitude of the `weight` column.

| type | model | estimation | record |
|---|---|---|---|
| `temperature` | η = o + f / T (o only for a score set with its own offset: a probability set's log share is the whole predictor, so q ∝ q^(1/T)) | the grid value maximising the weighted log score over the selection split's units: one pass with `grid` accumulators (`[min, max, count]`, linear; default 0.25 … 4 in 76 steps) | `temperature`, `logScore` at it, `logScoreAtIdentity` and `gainPerUnit` when the grid holds 1, `converged` false with a note when the optimum sits on the grid boundary |
| `blend` | η = a·f + b·o (+ an intercept for `binomial`): the conditional logit / logistic MLE of the two columns | the shared Newton controller (`GlmFit` / `FitState`, L2 on the average log likelihood, `maxIter` unrolled passes, a rejected step halves the step), starting at the set as declared — (a, b) = (1, 1) for a score set with its own offset, (1, 0) when o is the baseline (a probability set's log share / logit, or a score set without an offset, is the whole declared predictor) | `a`, `b`, `intercept`, their standard errors (the inverse Fisher information at the fit; NaN when it is not positive definite), `z_a`, `logScore`, `logScoreAtIdentity` (at the start = the declared set), `gainPerUnit`, `iterations`, `rejectedSteps`, `converged` (false with a note when the chain stalled: every step from the best point rejected) |

Reading a blend: a ≈ 1 and b at its start says the declared set is calibrated; a < 1 says the score needs
shrinking; a's z-value tests whether the set carries information orthogonal to its offset (the Benter
regression). The records are the summary's `fits` and, with `output.calibration`, a JSON document
(`{version, family, group, baseline, baselineForm, parametersHash, planHash, outputHash, createdAt, fits}`).
Isotonic / Platt recalibration is out of scope: it breaks the within-group sum.

## 8. Outputs

### 8.1 Metrics (the default output)

One record per split × prediction set (the baseline included under `prediction: baseline`, Δ = 0) × slice
value (the overall record has `slice` and `value` null), plus the pair records (§4.3):
`split`, `role`, `prediction`, `pair`, `slice`, `value`, `n_units`, `n_rows`, `positives`, `weight` (Σ w), and
for each of `logScore`, `excessLogScore`, `hitAt1`, `brier`: the value and `_lo` / `_hi` (null without
bootstrap); `logloss` (= −logScore). Slices come from `slices[]`: `{field}` (one record per distinct value)
or `{field, bucket: year | quarter | month | week | day}` (the period buckets of a time field; `field`
defaults to `time.field`). For `groupedMultinomial` a slice field is a group-level attribute (the same value
on every row of the group); a unit takes the slice values of its earliest row. Slices are meant for
low-cardinality dimensions — every distinct value is a set of accumulators gathered on one worker (see evaluation-engine.md, Metrics).

### 8.2 Calibration (`<name>.calibration`)

One record per split × prediction set × table × bin: `split`, `prediction`, `type`, `by`, `field`, `table`
(the index in `calibration[]`), `bin`, `lower`, `upper`, `n`, `positives`, `p_model`, `p_baseline`, `rate`,
`rate_lo`, `rate_hi`, `utility`.

### 8.3 Summary (`<name>.summary`)

One record per run: the roles, `predictions`, the splits (name, role, declared range, observed range, units,
rows), the row / unit counts (in, invalid, unassigned, scored, skipped), the bootstrap parameters, the
calibration table count, `fits` (§7.1), `parametersHash` and `notes` (role defaults applied, overlapping
split ranges, prior mode, a fit that produced no estimate).

### 8.4 Units (`<name>.units`)

The intermediate representation: one record per unit × prediction set (the baseline included): `split`,
`unit`, `time`, `prediction`, `n_rows`, `weight`, `logScore`, `logScoreBaseline`, `excessLogScore`,
`hitAt1`, `brier` and `slices` (the unit's slice values as `{field, value}` records). Re-aggregate it in a
warehouse, or feed it to the `attribution` transform to ask which slices Δ's total comes from.

## 9. Constraints and diagnostics

Assembly errors (every message names the parameter): an unknown family or form; `groupedMultinomial`
without `group`; `score` / `inverseShare` on a family other than `groupedMultinomial`; `prob` together with `field` /
`form` on one set; a `score` set with `temperature` ≤ 0 or an unknown
`offsetScale`; a duplicate or reserved prediction name; no prediction set; no split, no `report` split, a
split without a role, an unknown role; a time-range split without `time.field`; overlapping ranges or a
selection range after a report range; a role or column field missing from the input schema; a calibration
table with an unknown `type` / `by`, `bins` < 2, unsorted `edges`, `by: field` without `field`; a slice
without a field or with an unknown bucket; `bootstrap.samples` outside [0, 10000]; streaming input; a
non-global window or a triggered input (the calibration edges are a side input and the tables are one
Combine each); a fit whose `fitOn` is missing, unknown or a `report` split, an `of` naming no declared set,
two fits of one type on one set, a blend without an offset, a grid outside `[min > 0, max ≥ min, 2 ≤ count ≤
10000]`, `maxIter` outside [1, 100].

Row validity: a null / non-finite label, a null group, a null / negative weight, a row not in any split →
counted, not scored; a null time with a time-range split → the failure output. Unit skips: no positive
label (grouped), an invalid baseline or prediction value → `nUnitsSkipped` (in the family's unit).

## 10. Limits

- Δ is relative to the baseline: swapping the baseline (an odds snapshot at another time) changes its
  meaning; the summary records which column and form the baseline was.
- The bootstrap CI assumes independent resampling units; correlated units need `bootstrap.unit`.
- Quantile bin boundaries are sketch approximations; `edges` are exact.
- Batch, global window only.

## 11. Stages designed, not built

- **Slice discovery** (`sliceDiscovery`): candidate slices from low-cardinality dimensions (numeric ones
  quantile-binned) up to `maxDepth`, scored on `discoverOn` and confirmed on `confirmOn` in the same Combine
  (candidates × 2 splits × 3 sums); the null is the random-subset (exchangeability) distribution of a
  slice's mean Δ, the maximum over candidates calibrated by Monte Carlo at finalize; a baseline-drawn
  parametric null as an opt-in under a candidate cap.
- **`family: gaussian`** (Δ as the squared-error skill score) and **`ranking`** (NDCG@k).
- **`contributions`** (the aggregation of contribution columns), **`compareWith`** (a previous summary
  JSON) and the HTML report, together (the reporting layer).
