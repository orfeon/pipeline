---
type: Transform Module
title: Attribution Transform Module
description: Explains the difference between two multi-dimensional aggregates (baseline vs target) by automatically localizing it to a concise set of dimension-value slices (root causes). Implements the RiskLoc and Adtributor localization algorithms with derived (ratio) measure allocation, four baseline strategies (two-input external, label column, time shift, synthetic marginal), numeric binning, and cardinality/support guards. Batch only.
tags: [transform, attribution, rootcause, rca, anomaly, analysis, batch]
timestamp: 2026-07-12T00:00:00Z
---

# Attribution Transform Module

Transform module that answers "**why did this metric change?**". Given two multi-dimensional
aggregates — a *baseline* (expected/previous/forecast values) and a *target* (actual values) —
it automatically localizes the difference to a concise set of dimension-value combinations
(slices) that best explain the change, replacing manual pivot drill-down in BI tools with a
single declarative step.

Where aggregation modules answer "what happened", this module answers "**where and why it
changed**". It is designed to be placed directly downstream of a `aggregation` (or any
group-by style) step: the input rows are expected to be **already leaf-aggregated**
(dimension columns + numeric measure columns). Rows with identical dimension values are summed,
so multiple time buckets or partial aggregates per dimension tuple are fine.

Supports:

- **RiskLoc algorithm** (default) — multi-dimensional root cause localization by weighted risk
  (Kalander, arXiv:2205.10004): finds culprit slices at any cuboid depth, e.g. `region=a AND category=x`.
- **Adtributor algorithm** — the classic single-dimension attribution (Bhagwan et al., NSDI 2014)
  using explanatory power + Jensen–Shannon surprise. Also `exhaustive` as a brute-force baseline.
- **Derived measures** — ratio/expression measures such as `cvr = orders / sessions` declared as
  exp4j expressions, allocated to their components by `gre` (generalized ripple effect, default),
  `partialDerivative`, or `shapley`.
- **Four reference (baseline) strategies** — two-input `external`, single-input `external` with a
  label column, `timeShift` (period-over-period), `split` (by a row attribute), and
  `synthetic` marginal (interaction discovery against an independence model).
- **Guards** — `maxCardinality` / `minSupport` bucketing of tail values into `other`, and
  `maxLayer` bounding the search depth, to control cost and spurious findings.
- **Binned dimensions** — numeric columns bucketed by quantile or equal width before search.

Batch only. Streaming mode, the distributed (large) execution profile, and the parameter values
marked **reserved** below are planned for future versions; reserved values are accepted by the
schema but rejected at validation time with a "not implemented" error.

## Transform module common parameters

| parameter  | optional | type                | description                                                        |
|------------|----------|---------------------|--------------------------------------------------------------------|
| name       | required | String              | Step name. Specified to be unique in config file.                  |
| module     | required | String              | Specified `attribution`                                            |
| inputs     | required | Array<String\>      | Input step names. With the two-input `external` reference the order is **[target, baseline]**; all other strategies take exactly one input. |
| waits      | optional | Array<String\>      | Step names to wait for before processing.                          |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                        |

The parameters follow five concept blocks — *what changed* (`measures`), *compared with what*
(`comparison`), *in which vocabulary* (`vocabulary`), *with which semantics* (`semantics`),
*for whom* (`output`) — plus the cross-cutting `engine` block. Minimal usage requires only
`measures` and `vocabulary.dimensions`; everything else defaults to
external reference + contribution + riskloc + top-3 report.

## measures parameters (required)

Array of measures to explain. Each measure is analyzed independently.

| parameter  | optional | type   | description                                                                                                     |
|------------|----------|--------|-----------------------------------------------------------------------------------------------------------------|
| name       | required | String | Measure name. For `fundamental`, the numeric input field to sum. For `derived`, the output name of the expression. |
| type       | optional | Enum   | `fundamental` (default) or `derived`. (`distribution`, `sketch` are **reserved**.)                               |
| expression | required for derived | String | Arithmetic expression (exp4j) over numeric input fields, e.g. `"orders / sessions"`. All variables must exist as numeric input fields. |

Fundamental measures and derived-expression variables must be **sum-additive** (counts, amounts).
Declare ratios as `derived` with their additive components as variables — do not feed
pre-computed ratio columns as fundamental measures.

## comparison parameters

| parameter | optional | type      | description                                             |
|-----------|----------|-----------|---------------------------------------------------------|
| mode      | optional | Enum      | `pair` (default). (`series`, `cohort` are **reserved**.) |
| reference | optional | Reference | How the baseline is obtained. See below.                |

### reference parameters

| parameter     | optional | type   | description                                                                                                                                             |
|---------------|----------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| strategy      | optional | Enum   | `external` (default), `timeShift`, `split`, or `synthetic`.                                                                                              |
| labelField    | optional | String | `external` single-input form: the column that labels each row as baseline or target.                                                                     |
| targetLabel   | required with labelField | String | `labelField` value marking target rows. Rows matching neither label are dropped.                                                        |
| baselineLabel | required with labelField | String | `labelField` value marking baseline rows.                                                                                                |
| timeShift     | required for timeShift | TimeShift | `{ offset, timeField }`. `offset` is an ISO-8601 duration/period in days or weeks (e.g. `P7D`, `P2W`, `PT6H`); calendar units (`P1M`) are rejected. `timeField` is an optional timestamp column (defaults to the element event time). |
| split         | required for split | Split | `{ by: { field, baseline, target } }`: rows whose `field` equals `target` become the target set, `baseline` the baseline set; other rows are dropped. Values are compared as strings, so booleans and numbers work. |
| synthetic     | optional | Synthetic | `{ method: marginal }`. (`forecast` is **reserved**.)                                                                                                |

Strategy semantics:

- **external (2 inputs)** — `inputs: [target, baseline]`. Typical: plan vs actual, A/B groups,
  before/after a release from two queries.
- **external + labelField (1 input)** — long-format input where a column such as `window_type`
  distinguishes the two sets.
- **timeShift (1 input)** — period-over-period comparison. Windows are anchored at the maximum
  time `tmax` found in the data: target = `(tmax - offset, tmax]`, baseline =
  `(tmax - 2*offset, tmax - offset]`, other rows dropped. Feed exactly two periods of data
  (e.g. 14 daily rows with `offset: P7D`) for a well-balanced comparison; the anchoring makes
  runs deterministic and reproducible with no extra configuration.
- **split (1 input)** — baseline and target are two subsets of the same input, split by a row
  attribute (e.g. `is_error`, `variant`). Used for error-slice discovery / model debugging.
- **synthetic marginal (1 input)** — the baseline is synthesized from the target itself as the
  independence model over the dimension marginals. Slices deviating from it are evidence of
  **interaction structure** between dimensions (candidate cross features). Requires nonnegative
  measures. Since the synthetic baseline preserves totals, explanatory power is computed on
  absolute-delta shares for this strategy.

## vocabulary parameters (required)

| parameter      | optional | type              | description                                                       |
|----------------|----------|-------------------|-------------------------------------------------------------------|
| unit           | optional | Enum              | `slice` (default). (`metric` is **reserved**.)                    |
| dimensions     | required | Array<Dimension\> | Dimensions (input columns) forming the slice vocabulary. Max 31.  |
| expressiveness | optional | Enum              | `slice` (default). (`predicate`, `ruleList` are **reserved**.)    |

### dimension parameters

| parameter | optional | type    | description                                                              |
|-----------|----------|---------|--------------------------------------------------------------------------|
| name      | required | String  | Input column name. Missing values become the slice value `(null)`.      |
| type      | optional | Enum    | `flat` (default) or `binned`. (`hierarchy`, `embedding` are **reserved**.) |
| binning   | required for binned | Binning | `{ method: quantile | width, bins }` — numeric values are bucketed into interval labels like `[0,50)`; unparseable values go to `other`. |

## semantics parameters

| parameter         | optional | type | description                                                                                      |
|-------------------|----------|------|----------------------------------------------------------------------------------------------------|
| basis             | optional | Enum | `contribution` (default). (`mixRate`, `causalAdjusted` are **reserved**.)                          |
| derivedAllocation | optional | Enum | Allocation of derived measures to components: `gre` (default), `partialDerivative`, or `shapley` (exact, up to 10 variables). |
| epBasis           | optional | Enum | Basis of explanatory power: `auto` (default), `netDelta`, or `absoluteDelta`. See below.          |

### epBasis

The two bases answer **different questions** and every output row records which one was used
(`epBasis` field) — do not compare their values across bases:

- `netDelta` — share of the *net* change: `(v - f) / (V - F)`. "This slice explains X% of the
  drop." Undefined when the totals barely moved.
- `absoluteDelta` — share of the *total churn*: `|v - f| / Σ|v - f|`. "This slice accounts for
  X% of everything that moved", including increases and decreases that cancel out in the totals
  (mix shifts, marginal baselines).
- `auto` (default) — `netDelta`, automatically falling back to `absoluteDelta` when the net
  change is less than 5% of the total churn. The `synthetic` marginal reference always resolves
  to `absoluteDelta` this way (its net delta is zero by construction), and `epBasis: netDelta`
  is rejected for it at validation time.

## engine parameters

| parameter  | optional | type       | description                                                                    |
|------------|----------|------------|--------------------------------------------------------------------------------|
| algorithm  | optional | Enum       | `riskloc` (default), `adtributor`, `exhaustive`. (`squeeze` is **reserved**.)  |
| riskloc    | optional | RiskLoc    | `{ riskThreshold: 0.5, pepThreshold: 0.02, pruningLayers: 1 }`. `riskThreshold` is the minimum risk score for a slice to qualify; `pepThreshold` stops the iteration once the remaining unexplained share drops below it; `pruningLayers` only speeds up search (never changes results, `0` disables). |
| adtributor | optional | Adtributor | `{ teep: 0.1, tep: 0.67 }` — per-value and cumulative explanatory power thresholds (NSDI 2014). |
| guards     | optional | Guards     | See below.                                                                     |

### guards parameters

Algorithm-independent sanity constraints against spurious findings and cost explosion.

| parameter      | optional | type    | description                                                                                              |
|----------------|----------|---------|------------------------------------------------------------------------------------------------------------|
| minSupport     | optional | Double  | Dimension values whose volume share is below this in every measure are bucketed into `other`. Default `0.005`. |
| maxLayer       | optional | Integer | Maximum cuboid depth (number of dimensions combined in one slice). Default `3`. Search cost grows exponentially with this. |
| maxCardinality | optional | Integer | Maximum distinct values per dimension; the tail (by volume + delta) is bucketed into `other`. Default `200`. |
| fdrControl     | optional | Enum    | `none` (default). (`bh` is **reserved**.)                                                                  |

## output parameters

| parameter     | optional | type    | description                                                                     |
|---------------|----------|---------|-----------------------------------------------------------------------------------|
| mode          | optional | Enum    | `report` (default). (`featureSpec`, `interventionSpec` are **reserved**.)         |
| topK          | optional | Integer | Maximum findings per measure. Default `3`.                                        |
| emitNoFinding | optional | Boolean | If `true` (default), emits an explicit `noFinding: true` row for a measure with no significant attribution — useful for downstream agents to branch on. Note that an entirely empty input produces no output at all. |

## Output schema (report mode)

One row per finding per measure (plus one `noFinding` row per measure when applicable):

| field             | type                                          | description                                                       |
|-------------------|-----------------------------------------------|--------------------------------------------------------------------|
| measure           | String                                        | Measure name                                                       |
| algorithm         | String                                        | `riskloc` / `adtributor` / `exhaustive`                            |
| epBasis           | String                                        | Explanatory-power basis actually used: `netDelta` or `absoluteDelta` (see [epBasis](#epbasis)) |
| rank              | Long                                          | 1-based rank within the measure (0 on noFinding rows)              |
| elements          | Array<Struct{dimension: String, value: String}\> | The slice conjunction (Adtributor: the selected values of the culprit dimension) |
| layer             | Long                                          | Number of dimensions combined in the slice                         |
| riskScore         | Double (nullable)                             | RiskLoc risk score (null for other algorithms)                     |
| explanatoryPower  | Double                                        | Share of the total change explained by the slice                   |
| surprise          | Double (nullable)                             | Jensen–Shannon divergence based distribution-change score          |
| baseline / target / delta | Double                                | Slice sums (derived measures: the expression over slice component sums) |
| deltaRatio        | Double (nullable)                             | `delta / baseline` (null when baseline is 0)                       |
| totalBaseline / totalTarget | Double                              | Measure totals for context                                         |
| leafCount         | Long                                          | Number of leaves covered by the slice                              |
| noFinding         | Boolean                                       | `true` only on explicit no-finding rows                            |

## Example: KPI change analysis (week over week)

"GMV dropped 6% versus last week — which category × region × client is responsible?"

```yaml
sources:
  - name: dailyKpi
    module: bigquery
    parameters:
      query: |
        SELECT category, region, client, event_date AS ts, SUM(gmv) AS gmv
        FROM `myproject.mart.daily_kpi`
        WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AND CURRENT_DATE()
        GROUP BY category, region, client, event_date
transforms:
  - name: gmvDropAnalysis
    module: attribution
    inputs: [dailyKpi]
    parameters:
      measures:
        - name: gmv
      comparison:
        reference:
          strategy: timeShift
          timeShift:
            offset: P7D
            timeField: ts
      vocabulary:
        dimensions:
          - name: category
          - name: region
          - name: client
sinks:
  - name: report
    module: bigquery
    inputs: [gmvDropAnalysis]
    parameters:
      table: myproject.mart.gmv_attribution
```

## Example: plan vs actual (two inputs) with a derived measure

```yaml
transforms:
  - name: budgetVariance
    module: attribution
    inputs: [actual, budget]     # [target, baseline]
    parameters:
      measures:
        - name: cost
        - name: cpa
          type: derived
          expression: "cost / conversions"
      comparison:
        reference:
          strategy: external
      vocabulary:
        dimensions:
          - name: project
          - name: service
          - name: sku
      semantics:
        derivedAllocation: gre
      output:
        topK: 5
```

## Example: error-slice discovery (model debugging)

"On which slices does the model systematically fail?" — split one evaluation table by
the misclassification flag and compare the two distributions with Adtributor.

```yaml
transforms:
  - name: errorSlices
    module: attribution
    inputs: [evaluations]
    parameters:
      measures:
        - name: example_count
      comparison:
        reference:
          strategy: split
          split:
            by: {field: is_misclassified, baseline: false, target: true}
      vocabulary:
        dimensions:
          - name: device
          - name: user_segment
          - name: price
            type: binned
            binning: {method: quantile, bins: 16}
      engine:
        algorithm: adtributor
```

## Example: interaction discovery (synthetic marginal baseline)

Slices whose volume deviates from the dimension-independence model indicate interaction
effects — candidates for cross features or targeted investigation.

```yaml
transforms:
  - name: interactions
    module: attribution
    inputs: [featureStats]
    parameters:
      measures:
        - name: row_count
      comparison:
        reference:
          strategy: synthetic
          synthetic: {method: marginal}
      vocabulary:
        dimensions:
          - name: query_category
          - name: item_category
          - name: price_band
      engine:
        guards: {maxLayer: 2}
      output:
        topK: 10
```

## Execution profile and scale guidance

This version runs the *small* execution profile: after an internal re-aggregation by dimension
tuple, all leaves are gathered into a single group and the localization runs on one worker.
This is the right shape for attribution — inputs are aggregates, not raw events — but means the
leaf count (distinct dimension tuples) should stay roughly below a few million rows. Control it
with `guards.maxCardinality` / `minSupport` and by limiting `vocabulary.dimensions`.
Search cost is dominated by (number of cuboids) × (leaf count); the cuboid count grows
exponentially with `guards.maxLayer`, so raise it with care.

## Known limitations

- **Deepest-layer single-element root causes sit on the detection boundary.** A root cause
  covering exactly one leaf (all dimensions fully specified) has a risk score that cannot
  exceed 0.5 by construction (`r1 ≤ w/(w+1) ≤ 0.5`), which is exactly the default
  `riskThreshold` (the comparison is `>=`, matching the reference implementation, so extreme
  single-leaf changes are still detected). If such causes matter in your data, lower
  `riskThreshold` slightly — and check they are not being bucketed into `other` by
  `guards.minSupport` first. Do not expect sub-boundary single-leaf causes at the default
  threshold.
- **Degenerate cutoff guard.** With very few distinct deviation values (noise-free or heavily
  bucketed data), the reference cutoff logic can classify nothing as anomalous. In that case
  the module falls back to a zero cutoff toward the deviation side carrying more mass and logs
  a warning. This guard is a production default on top of the reference algorithm; it activates
  only when the reference behavior would have returned nothing.
- Misspelled enum values deserialize as null and silently fall back to their defaults
  (Gson behavior common to all modules); validation rejects reserved values only when
  spelled exactly.
- The module always re-aggregates duplicate dimension tuples by summing before analysis.
- Streaming inputs are rejected at validation time in this version.
