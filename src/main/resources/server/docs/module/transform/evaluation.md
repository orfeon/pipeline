---
type: Transform Module
title: Evaluation Transform Module
description: Prediction verification after training, against a baseline. Matches one or more prediction sets (probability columns, or a raw score softmaxed within the group on top of an offset) with the outcome on time splits that carry a selection / report role, and reports the excess log score over the baseline as the first-class metric, with a Poisson bootstrap confidence interval (paired between prediction sets, cluster-able by a declared unit), next to logloss, hit@1 and Brier; per declared slice and calendar bucket; calibration tables (reliability by prediction quantile / divergence / declared field bands, edge groups above a ratio to the baseline, Wilson intervals, flat return per bin from a utility column); the per-unit loss decomposition as an output. Families groupedMultinomial (mutually exclusive samples within a group) and binomial; role defaults and lineage from the feature transform manifest. Batch only.
tags: [transform, evaluation, machine-learning, statistics, calibration, bootstrap, batch]
timestamp: 2026-09-13T00:00:00Z
---

# Evaluation Transform Module

Transform module that **verifies predictions after training**: it matches prediction columns against the
outcome and reports, as its first-class metric, the **excess information over a baseline prediction** — the
forecast-verification habit (skill against climatology, reliability diagrams) applied to grouped data. The
model itself is never read: the predictions may come from an ML model, a rule, a human or the system being
replaced, and the computation is the same.

It closes the learner-free trio: [`feature`](feature.md) (generation) → [`screen`](screen.md) (ranking
before training) → evaluation (verification after training), the supervised counterpart of the `profile`
sink's data-quality report.

**Why the excess log score.** On a problem with a strong baseline (a market, an incumbent system) the model
that wins on logloss or accuracy is the one that imitates the baseline; only the excess log score
Δ = mean log p_model(y) − log p_baseline(y) tells whether it adds anything. Δ > 0 is the only sign that the
prediction carries information the baseline lacks. `logloss`, `hitAt1` and `brier` are reported next to it and
are not selection criteria.

**Discipline by configuration.** Splits carry a role: selection (`selection`) or reporting (`report`); the
selection split must end before the report split starts, and a random split shows up as overlapping observed
time ranges in the summary.

## What it computes

For a grouped unit (the candidates of a query, the listings of a session) with labels ỹ normalised to sum 1,
baseline shares p and a prediction set's shares q:

| metric | per unit | notes |
|---|---|---|
| `logScore` | Σ ỹ log q | the mean over tied positives |
| `excessLogScore` (Δ) | Σ ỹ log q − Σ ỹ log p | the first-class metric; the baseline's own record has Δ = 0 |
| `hitAt1` | ỹ at argmax q | ties at the maximum share the credit; null for `binomial` |
| `brier` | Σ (q − ỹ)² | |
| `logloss` | −logScore | derived, no interval of its own |

For a `binomial` row: the Bernoulli log score, the same under the baseline, `brier = (q − y)²`. Aggregates are
weighted means over the units of a key (split × prediction set × slice value); `weight` is per row for
`binomial` and the row mean of the unit for `groupedMultinomial`.

**Common unit set.** A unit with a null / invalid value in any prediction set or in the baseline is skipped
whole (`nUnitsSkipped`): every prediction set is compared on the same units.

**Prior mode.** Without `baseline` the reference is the prior: the uniform share within the group, or the
split's label mean for `binomial`. Δ then reads as the skill over the prior.

### Bootstrap intervals

Units are resampled by the Poisson bootstrap in one pass: per resampling unit and replicate a Poisson(1) weight
derived from `bootstrap.seed` and the unit's key, so a rerun on any runner reproduces every interval. The
2.5 / 97.5 percentiles of the replicate means are `<metric>_lo` / `<metric>_hi`. The weights are per unit, so
the **pair records** (`prediction: A, pair: B`: the differences A − B of every metric over the same units)
carry a paired interval at no extra cost. `bootstrap.unit` names a field whose value is the resampling unit —
`unit: event_date` makes a cluster bootstrap by day, the answer to correlated units (same-day, same-venue
groups) that the group bootstrap ignores.

### Prediction sets

```yaml
predictions:
  - {name: candidate, prob: p_candidate}                                   # a probability column
  - {name: raw, field: s_raw, form: logProb}                               # any form of the family
  - {name: scored, score: f, offset: log_m, offsetScale: log, temperature: 1.0}   # grouped softmax
```

A score set (grouped family only) is q ∝ w · exp(score / T) within the group, w the offset value
(`offsetScale: prob`, default) or exp(offset) (`offsetScale: log`), 1 without an offset — a model that
outputs a raw score to be combined with the baseline's log share reads as `score: f, offset: log_m,
offsetScale: log`. `baseline` is a reserved name (the baseline's own metrics are reported under it).

### Splits

```yaml
time: {field: event_time}
splits:
  valid: {from: "2026-01-01", to: "2026-03-31", role: selection}
  test:  {from: "2026-04-01", to: "2026-06-30", role: report}
```

- By time range: `from` / `to` are inclusive ISO instants or dates (a date reads as the start / end of that UTC
  day). Ranges must not overlap, and every `selection` range must end before every `report` range starts.
  Rows outside every range are counted (`nRowsUnassigned`) and dropped.
- By column: `splits: {field: fold, roles: {valid: selection, test: report}}` reads the split name from a
  column written by the training job; the summary reports each split's observed time range and notes when a
  selection range overlaps a report range.
- At least one `report` split is required. A grouped unit is keyed by (split, group).

### Calibration tables

```yaml
calibration:
  - {type: reliability, by: prediction, bins: 10}
  - {type: reliability, by: divergence, bins: 10}
  - {type: reliability, by: field, field: price, edges: [1, 2, 3, 5, 10, 20, 50, 100]}
  - {type: edge, thresholds: [1.0, 1.1, 1.25, 1.5, 2.0]}
```

Row-level, per split × prediction set; each bin reports `n`, `positives` (Σ ỹ), the means `p_model` and
`p_baseline`, `rate` with its Wilson 95% interval and `utility` (Σ utility · ỹ / n when `utility.field` is set:
the flat return of taking every row of the bin at unit stake).

| type / by | bins | reading |
|---|---|---|
| `reliability` / `prediction` | quantiles of the prediction (a KLL sketch over the split's rows; one extra pass) | the reliability diagram |
| `reliability` / `divergence` | quantiles of f = logit q − logit p | if the rate stays at `p_baseline` where f is large, the divergence is noise; if it follows `p_model`, information |
| `reliability` / `field` | fixed `edges` on a declared numeric field | rare-event bands (a price band, an odds band) |
| `edge` | one group per threshold: the rows with q > threshold × p | if the rate exceeds `p_baseline` systematically, the divergence is information |

Quantile boundaries are sketch approximations (rank error under 1%); `edges` are exact.

## Input contract

| role | description |
|---|---|
| `family` | `groupedMultinomial` (default; mutually exclusive samples within a group) or `binomial` (independent binary rows). |
| `group` | the group key; required for `groupedMultinomial`. |
| `label` | the outcome: a field, or `{field}` / `{expr, normalizeTies}`. Grouped labels are normalised to sum 1 (a tie shares the group's one likelihood term). |
| `baseline` | the reference prediction: a field or `{field, form}` (`prob` default, `logProb`, `inverseShare` = 1 / x made a share within the group, for odds and prices). Omitted: the prior. |
| `predictions` | the prediction sets (at least one). |
| `time` | the time field the split ranges and period buckets read. Defaults to the feature transform's time role. |
| `splits` | the time splits with their roles (required). |
| `weight` | a sample-weight field. |
| `rowId` | fields identifying a row (the sort tie-break within a unit; the bootstrap unit of independent rows). Default: every field value. |
| `utility` | `{field}`: the realised value of a positive row. |
| `manifest` | the upstream feature manifest URI: role defaults when the table came back through a sink. |

**Defaults from the feature transform.** `group` / `label` / `baseline` / `weight` and `time.field` fall back
to the roles the feature transform declared (the `feature.role` field options of the direct upstream, or the
manifest's `roles` / `timeField`).

## Transform module common parameters

| parameter  | optional | type                              | description                                                           |
|------------|----------|-----------------------------------|-----------------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.                     |
| module     | required | String                            | Specified `evaluation`                                                |
| inputs     | required | Array<String\>                    | Input step names (several inputs are flattened).                      |
| waits      | optional | Array<String\>                    | Steps to wait for before processing.                                  |
| parameters | required | Map<String,Object\>               | Evaluation parameters below.                                          |

The transform needs the global window and the default trigger (no `strategy` window / trigger): the splits are
the time partition.

## Evaluation transform module parameters

| parameter | optional | type | description |
|---|---|---|---|
| family | optional | String | `groupedMultinomial` (default) or `binomial`. |
| group | optional | String | Group key field. Required for `groupedMultinomial`. |
| label | required | String or Object | Field name, or `{field}` / `{expr, normalizeTies}`. `expr` is a [Lucene expression](https://lucene.apache.org/core/10_5_0/expressions/org/apache/lucene/expressions/js/package-summary.html) over numeric fields; `normalizeTies` (default true). |
| baseline | optional | String or Object | Field name (form `prob`), or `{field, form}` with `prob` / `logProb` / `inverseShare`. Omitted: the prior. |
| predictions | required | Array<Object\> | `{name, prob}`, `{name, field, form}` or `{name, score, offset, offsetScale, temperature}` (see [Prediction sets](#prediction-sets)). Names are unique; `baseline` is reserved. |
| splits | required | Object | `{<name>: {from, to, role}}` on `time.field`, or `{field, roles: {<value>: role}}`; `role` is `selection` or `report` (at least one `report`). |
| time | optional | String or Object | Field name or `{field}`. Required with time-range splits (or a feature time role). |
| weight | optional | String or Object | Weight field. |
| rowId | optional | Array<String\> | Fields identifying a row. Default: every field value (a 128-bit hash travels). |
| utility | optional | String or Object | The realised value of a positive row; `utility` in the calibration records. |
| bootstrap | optional | Object or false | `samples` (default 1000, 0 or `false` disables, at most 10000), `seed` (default 0), `unit` (a field whose value is the resampling unit; default the group / the row identity). Every accumulator holds 6 × samples doubles. |
| calibration | optional | Array<Object\> | The tables (see [Calibration tables](#calibration-tables)): `{type: reliability, by: prediction \| divergence, bins}` (default by `prediction`, 10 bins), `{type: reliability, by: field, field, edges}`, `{type: edge, thresholds}`. |
| slices | optional | Array | `{field}` (one record per distinct value) or `{field, bucket}` with bucket `year` / `quarter` / `month` / `week` / `day` (UTC) on a timestamp / date field (`field` defaults to `time.field`). A plain string is a field. For `groupedMultinomial` a slice field is a group-level attribute (the same value on every row of the group): a unit takes the slice values of its earliest row. Meant for low-cardinality dimensions (see Limits). |
| manifest | optional | String | The upstream feature manifest URI (role defaults). |

## Outputs

| output | content |
|---|---|
| `<name>` | the metrics: one record per split × prediction set (the baseline under `prediction: baseline`) × slice value (`slice` / `value` null for the overall record), plus one pair record per ordered pair of prediction sets (`pair` = the other set, values = differences) |
| `<name>.calibration` | one record per split × prediction set × table × bin |
| `<name>.units` | the per-unit loss decomposition: one record per unit × prediction set (the baseline included) |
| `<name>.summary` | one record per run |

### Metrics record

| field | type | description |
|---|---|---|
| split, role | STRING | the split and its role |
| prediction | STRING | the prediction set (`baseline` for the baseline's own record) |
| pair | STRING | pair records: the subtracted set (null otherwise) |
| slice, value | STRING | the slice (`<field>` or `<field>/<bucket>`) and its value; null for the overall record |
| n_units, n_rows | INT64 | scored units (groups, or rows) and rows |
| positives | FLOAT64 | Σ w ỹ (grouped: the weight mass of the units) |
| weight | FLOAT64 | Σ w |
| logScore, excessLogScore, hitAt1, brier | FLOAT64 | the metrics (differences for a pair record) |
| `<metric>_lo`, `<metric>_hi` | FLOAT64 | the bootstrap 95% interval (null without bootstrap, and for the baseline's excess) |
| logloss | FLOAT64 | −logScore |

### Calibration record

`split`, `prediction`, `type`, `by`, `field`, `table` (the index in `calibration[]`), `bin`, `lower`, `upper`
(the bin bounds: the sketch's minimum / maximum for the outer quantile bins, the declared edges, null
outside the edges; the threshold in `lower` for `edge`), `n`, `positives`, `p_model`, `p_baseline` (null in
binomial prior mode), `rate`, `rate_lo`, `rate_hi` (Wilson), `utility`.

### Units record

`split`, `unit` (the group key, or the row identity), `time`, `prediction`, `n_rows`, `weight`, `logScore`,
`logScoreBaseline` (null in binomial prior mode), `excessLogScore`, `hitAt1`, `brier`,
`slices` (ARRAY<STRUCT<field STRING, value STRING\>\>). Re-aggregate it in a warehouse, or feed it to the
[`attribution`](attribution.md) transform to ask which slices Δ's total comes from.

### Summary record

`family`, `group`, `label`, `baseline`, `baselineForm`, `weight`, `timeField`, `splitField`, `predictions`,
`splits` (ARRAY<STRUCT<name, role, from, to, minTime, maxTime, nUnits, nUnitsSkipped, nRows\>\> — the declared
and observed range of each split), `nRows`, `nRowsInvalid` (null label / group / weight), `nRowsUnassigned`
(in no split), `nUnits`, `nUnitsSkipped` (no positive label, an invalid baseline or prediction value),
`bootstrapSamples`, `bootstrapSeed`, `bootstrapUnit`, `nCalibrationTables`, `slices`, `parametersHash` (the
SHA-256, 16 hex characters, of the canonical parameters without `manifest`), `planHash` / `outputHash` (of
the feature manifest when given), `notes` (role defaults applied, prior mode, overlapping split ranges).

## Examples

### Example 1: sessions of listings, several models against the current one

```yaml
sources:
  - name: predictions
    module: storage
    parameters:
      input: gs://bucket/jobs/${args.job}/predictions.parquet
      format: parquet
transforms:
  - name: eval
    module: evaluation
    inputs: [predictions]
    parameters:
      family: groupedMultinomial
      group: session_id
      label: sold
      baseline: {field: p_current, form: prob}
      time: session_time
      predictions:
        - {name: candidate, prob: p_candidate}
        - {name: scored, score: f_candidate, offset: p_current, offsetScale: prob}
      splits:
        valid: {from: "2026-01-01", to: "2026-03-31", role: selection}
        test:  {from: "2026-04-01", to: "2026-06-30", role: report}
      bootstrap: {samples: 1000, seed: 7, unit: session_date}
      calibration:
        - {type: reliability, by: prediction, bins: 10}
        - {type: reliability, by: divergence, bins: 10}
        - {type: edge, thresholds: [1.0, 1.25, 1.5, 2.0]}
      slices:
        - {field: session_time, bucket: month}
        - {field: category}
sinks:
  - name: metrics
    module: bigquery
    inputs: [eval]
    parameters:
      table: project.dataset.model_evaluation
      createDisposition: CREATE_IF_NEEDED
      writeDisposition: WRITE_APPEND
  - name: calibration
    module: bigquery
    inputs: [eval.calibration]
    parameters:
      table: project.dataset.model_calibration
  - name: units
    module: storage
    inputs: [eval.units]
    parameters:
      output: gs://bucket/eval/${args.job}/units
      format: parquet
```

The report split's `candidate` record answers the question: `excessLogScore` with `excessLogScore_lo` above 0
means the candidate carries information the current model lacks; the pair record `candidate` − `scored`
compares the two ways of using the candidate score on the same sessions.

### Example 2: replacing an incumbent scorer (independent rows)

```yaml
transforms:
  - name: eval
    module: evaluation
    inputs: [scored_events]
    parameters:
      family: binomial
      label: {field: converted}
      baseline: {field: p_current, form: prob}
      time: event_time
      rowId: [event_id]
      predictions:
        - {name: candidate, prob: p_candidate}
      splits:
        valid: {from: "2026-01-01", to: "2026-03-31", role: selection}
        test:  {from: "2026-04-01", to: "2026-06-30", role: report}
      bootstrap: {samples: 1000, seed: 1, unit: event_date}
      calibration:
        - {type: reliability, by: prediction, bins: 10}
      slices: [{field: event_time, bucket: month}, {field: country}, {field: device}]
```

### Example 3: the training job's own folds, no baseline

```yaml
parameters:
  family: groupedMultinomial
  group: query_id
  label: clicked
  time: query_time
  predictions:
    - {name: ranker, prob: p_click}
  splits: {field: fold, roles: {valid: selection, test: report}}
```

Without a baseline, Δ is the skill over the uniform share within the query. The summary's `splits` carry the
observed time range of each fold and `notes` says when the selection fold overlaps the report fold in time.

### Example 4: downstream of the feature transform

```yaml
parameters:
  manifest: gs://bucket/features/${args.version}/manifest.json   # group / label / baseline / time from its roles
  predictions:
    - {name: model, prob: p_model}
  splits:
    valid: {from: "2026-01-01", to: "2026-03-31", role: selection}
    test:  {from: "2026-04-01", to: "2026-06-30", role: report}
```

## Reading the output

- The report split's `excessLogScore` (with its interval) is the number to decide on; the selection split's
  is the one that chose the model. The two agreeing is the sanity check.
- `hitAt1`, `logloss` and `brier` describe the prediction; they do not rank models against a strong baseline.
- The pair records compare prediction sets on the same units with a paired interval: two sets whose
  individual intervals overlap can still differ significantly.
- Reliability by `divergence`: where the model and the baseline disagree most, does the realised rate follow
  the model (information) or the baseline (over-confidence)? The `edge` groups ask the same per ratio
  threshold.
- Slices with a negative Δ where the overall is positive are where the model loses to the baseline; the
  `units` output feeds the `attribution` transform for the total decomposition.

## Limits

- Δ is relative to the baseline: swapping the baseline (an odds snapshot at another time) changes its meaning;
  the summary records which column and form the baseline was.
- The bootstrap interval assumes independent resampling units; correlated units need `bootstrap.unit`.
- Quantile bin boundaries are sketch approximations; `edges` are exact.
- Slices are for low-cardinality dimensions: every distinct value costs splits × (1 + prediction sets)
  accumulators of 6 × `bootstrap.samples` doubles (about 48 KB each at the default 1000), all gathered on one
  worker for the final report. Keep distinct values in the hundreds (or lower `bootstrap.samples`); a
  high-cardinality field (an id) belongs in a coarser bucket, not in `slices`.
- Batch, global window only. Calibration fits (temperature / blend), slice discovery, the gaussian / ranking
  families and the HTML report are the next stages (see `docs/design/evaluation-dsl.md` §11).
