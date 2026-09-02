---
type: Transform Module
title: Attribution Transform Module
description: Explains why a metric changed between a baseline and a target period along two orthogonal axes. Slice attribution (vocabulary.unit slice, the default) localizes the change to a concise set of dimension-value slices (where) with the RiskLoc, Squeeze and Adtributor algorithms, derived (ratio) measure allocation, distribution measures (KLL sketches), distinct-count measures (Theta sketches), pre-serialized sketch input, four baseline strategies and cardinality/support guards. Metric-tree attribution (vocabulary.unit metric) decomposes the change along a declared driver/KPI tree (why) — sum, product (volume x rate), sum-of-products and weighted-average nodes with per-dimension breakdowns — with exact additivity at every level and an optional causal adjustment for declared rate-to-volume dependencies (ordered allocation with a baseline-fitted regression). Batch only.
tags: [transform, attribution, rootcause, rca, anomaly, analysis, batch, datasketches, metrictree, kpi, decomposition, causal]
timestamp: 2026-08-22T00:00:00Z
---

# Attribution Transform Module

Transform module that answers "**why did this metric change?**". Given two multi-dimensional
aggregates — a *baseline* (expected/previous/forecast values) and a *target* (actual values) —
it automatically localizes the difference to a concise set of dimension-value combinations
(slices) that best explain the change, replacing manual pivot drill-down in BI tools with a
single declarative step.

Where aggregation modules answer "what happened", this module answers "**where and why it
changed**" along two orthogonal axes, selected by `vocabulary.unit`:

- **`slice`** (default) — *where*: which dimension-value slices (`country=JP AND channel=app`)
  explain the change. Everything in this document applies to it unless noted.
- **`metric`** — *why*: which driver metrics in a declared **metric tree** (`revenue = units ×
  AUP`, `AUP` by deal status, …) explain the change. See [Metric-tree attribution](#metric-tree-attribution-vocabularyunit-metric);
  the `measures`, `comparison` (all baseline strategies) and `output` blocks are shared.
 Input rows can be **raw events or pre-aggregated leaves** (dimension columns +
numeric measure columns): leaf aggregation by dimension tuple runs inside the module,
distributed across workers, and rows with identical dimension values are always merged — so
multiple time buckets, partial aggregates or event-level rows per tuple are all fine. For very
large raw datasets, pre-aggregating at the source (e.g. `GROUP BY` in the BigQuery query)
remains worthwhile purely to cut read I/O — but never aggregate **coarser** than the declared
dimensions, or culprits in the aggregated-away dimensions become unlocalizable.

> **Scope note**: "attribution" here means multi-dimensional KPI-change / root-cause
> attribution (the Adtributor / RiskLoc family). It is **not** marketing multi-touch
> attribution — there is no notion of user journeys or per-touchpoint conversion credit
> (the `shapley` option allocates a derived measure across its component variables, not
> across channels) — and not ML feature attribution (SHAP-style model explanation).

Supports:

- **RiskLoc algorithm** (default) — multi-dimensional root cause localization by weighted risk
  (Kalander, arXiv:2205.10004): finds culprit slices at any cuboid depth, e.g. `region=a AND category=x`.
- **Adtributor algorithm** — the classic single-dimension attribution (Bhagwan et al., NSDI 2014)
  using explanatory power + Jensen–Shannon surprise. Also `exhaustive` as a brute-force baseline.
- **Squeeze algorithm** — deviation-magnitude clustering + potential-score search
  (Li et al., ISSRE 2019), ported from the reference implementation; a second opinion with
  different failure modes than RiskLoc.
- **Derived measures** — ratio/expression measures such as `cvr = orders / sessions` declared as
  [Lucene expressions](https://lucene.apache.org/core/10_5_0/expressions/org/apache/lucene/expressions/js/package-summary.html)
  (JavaScript-like syntax), allocated to their components by `gre` (generalized ripple effect, default),
  `partialDerivative`, or `shapley`.
- **Distribution measures** — "the p99 latency got worse — which slice?" Per-leaf value
  distributions are held as mergeable [Apache DataSketches](https://datasketches.apache.org/)
  KLL sketches and quantile shifts are localized per configured quantile. See
  [Distribution measures](#distribution-measures).
- **Distinct-count measures** — "DAU dropped — which slice?" Per-leaf identity sets are held as
  mergeable Theta sketches; distinct counts are not sum-additive, and the sketch union is what
  makes slice evaluation possible. See [Distinct-count measures](#distinct-count-measures).
- **Sketch measures** — pre-serialized DataSketches bytes as input: aggregate at the source
  (e.g. BigQuery DataSketches UDFs) and ship only sketch bytes. See
  [Sketch measures](#sketch-measures-pre-aggregated-sketch-input).
- **Four reference (baseline) strategies** — two-input `external`, single-input `external` with a
  label column, `timeShift` (period-over-period), `split` (by a row attribute), and
  `synthetic` marginal (interaction discovery against an independence model).
- **Guards** — `maxCardinality` / `minSupport` bucketing of tail values into `other`, and
  `maxLayer` bounding the search depth, to control cost and spurious findings.
- **Binned dimensions** — numeric columns bucketed by quantile or equal width before search.
- **Metric-tree attribution** (`vocabulary.unit: metric`) — metric-tree change decomposition
  (MTCD: Zhou, Janzing, Tsang, Blöbaum, Visentini Scarzanella, "A Unified Approach to
  Interpretable Causal Root Cause Attribution", KDD '26 TSMO workshop): a declared KPI/driver tree
  with sum / product / sum-of-products / weighted-average nodes and per-dimension breakdowns,
  exact additivity at every level, and an optional **causal adjustment** of product nodes along
  declared rate → volume edges. See [Metric-tree attribution](#metric-tree-attribution-vocabularyunit-metric).

Batch only. Streaming mode and the parameter values marked **reserved** below are planned for
future versions; reserved values are accepted by the schema but rejected at validation time
with a "not implemented" error.

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
| name       | required | String | Measure name. For `fundamental`, the numeric input field to sum. For `derived`, the output name of the expression. For `distribution`, the numeric input field whose per-row values form the distribution. For `distinct`, the identity field (any scalar type) whose distinct count is analyzed. For `sketch`, the field carrying serialized sketch bytes. |
| type       | optional | Enum   | `fundamental` (default), `derived`, `distribution`, `distinct` or `sketch`.                                      |
| expression | required for derived | String | Arithmetic expression (Lucene expressions, JavaScript-like syntax) over numeric input fields, e.g. `"orders / sessions"`. All variables must exist as numeric input fields. |
| quantiles  | optional (distribution / sketch kll) | Array<Double\> | Quantiles in (0, 1) to analyze, e.g. `[0.5, 0.99]`. Each quantile is localized independently and produces its own result rows. Default `[0.5]`. |
| format     | required for sketch | Enum | `kll` (quantiles sketch — analyzed like a [distribution measure](#distribution-measures)) or `theta` (identity set sketch — analyzed like a [distinct-count measure](#distinct-count-measures)). |

Fundamental measures and derived-expression variables must be **sum-additive** (counts, amounts).
Declare ratios as `derived` with their additive components as variables — do not feed
pre-computed ratio columns as fundamental measures.

### Distribution measures

A `distribution` measure localizes a **shift in the value distribution** (e.g. a latency tail
regression) instead of a change in a sum. Unlike the other types, the input rows are expected to
be **event-level**: each row contributes its value of the `name` field as one sample to its
dimension tuple's distribution. Per-leaf distributions are held as mergeable
[Apache DataSketches](https://datasketches.apache.org/) KLL sketches, which is what makes slice
evaluation possible — a slice's distribution is the union (merge) of its leaves' sketches.

Semantics per configured quantile `q`:

- Per-leaf baseline/target values are the leaf sketches' `q`-quantiles; the localization
  algorithms then run unchanged on those values.
- Explanatory power is the **mass-weighted absolute quantile shift** share
  `(n_baseline + n_target) · |Δq|`, normalized to sum 1. Quantiles are not additive, so a
  net-change share is undefined: distribution measures always report `epBasis: absoluteDelta`
  (requesting `netDelta` is a validation error).
- Reported `baseline` / `target` values (per finding and totals) are quantiles of the merged
  sketches, **not sums**.
- Not combinable with the `synthetic` reference (no independence model is defined for
  distributions) or with `derivedAllocation: shapley`.

Sketches with up to 200 values per leaf and role are exact; beyond that, quantile estimates
carry the KLL error bounds (~1.65% rank error) and sketch compaction introduces slight
run-to-run variation. Best suited to positive-valued metrics (latency, size, cost).

### Distinct-count measures

A `distinct` measure localizes a **change in a distinct count** (e.g. "DAU dropped — which
slice?"). Distinct counts are not sum-additive — the same identity can appear in many leaves —
so they cannot be declared as `fundamental` measures over pre-computed `COUNT(DISTINCT ...)`
columns. Instead, feed **event-level rows**: each row contributes its value of the `name` field
(any scalar type — string ids, numeric ids) as one identity to its dimension tuple's set,
held as a mergeable Theta sketch. A slice's distinct count is the estimate of the **union** of
its leaves' sketches, never a sum.

Semantics:

- Per-leaf baseline/target values are the leaf sketches' distinct estimates; the localization
  algorithms then run unchanged on those values.
- Explanatory power is the **absolute estimate shift** share `|Δestimate|`, normalized to sum 1.
  Because identities can span leaves, leaf-level deltas do not decompose the union delta
  exactly; distinct measures therefore always report `epBasis: absoluteDelta` (requesting
  `netDelta` is a validation error).
- Reported `baseline` / `target` values (per finding and totals) are union estimates.
- Not combinable with the `synthetic` reference; `quantiles` is a distribution-only parameter.

Up to 4096 distinct identities per leaf and role the estimate is exact (and deterministic);
beyond that, Theta error bounds apply (~1.6% RSE) and the union of many estimating sketches
compounds slightly.

### Sketch measures (pre-aggregated sketch input)

A `sketch` measure accepts **pre-serialized Apache DataSketches bytes** instead of raw
samples/identities: the heavy per-event aggregation runs at the source (e.g. a BigQuery
`GROUP BY` over billions of events) and only the compact sketch bytes cross the wire — the
best of source-side aggregation (minimal read I/O) and sketch semantics. Rows carry the
sketch in the `name` field as a `bytes` column, or a `string` column holding base64.

After merging, the measure behaves exactly like its raw-fed counterpart — same analysis, same
output, same constraints (always `epBasis: absoluteDelta`, no `synthetic` reference):

- `format: kll` — a KLL **doubles** quantiles sketch, analyzed like a
  [distribution measure](#distribution-measures) at the configured `quantiles`.
- `format: theta` — a Theta identity-set sketch, analyzed like a
  [distinct-count measure](#distinct-count-measures).

Partial aggregates are fine: multiple sketch rows per dimension tuple (and even a mix of
sketch rows and raw event rows feeding *different* measures) merge correctly. Corrupt sketch
bytes are routed to the failure output (`outputFailure` / `failureSinks`).

**Serialization compatibility** — the bytes must be the Apache DataSketches serial format:

- `theta` interoperates with the
  [Apache DataSketches BigQuery UDFs](https://github.com/apache/datasketches-bigquery)
  (`bqutil.datasketches.theta_sketch_agg_string(...)` etc.) and any DataSketches
  Java/C++/Python producer.
- `kll` expects a **`kll_sketch<double>`** (Java `KllDoublesSketch`). Note the BigQuery UDFs
  currently serialize **float** KLL sketches (`kll_sketch_float_build`), which are a
  *different, incompatible* format — KLL sketch input therefore currently requires a
  doubles-sketch producer (DataSketches Java/C++/Python, another pipeline stage, …).
  BigQuery's native `KLL_QUANTILES.*` functions are also not compatible.

```yaml
sources:
  - name: dailySketches
    module: bigquery
    parameters:
      query: |
        SELECT region, category, window,
               bqutil.datasketches.theta_sketch_agg_string(user_id, STRUCT(12)) AS users_sk
        FROM `myproject.logs.events`
        GROUP BY region, category, window
transforms:
  - name: dauAttribution
    module: attribution
    inputs: [dailySketches]
    parameters:
      measures:
        - name: users_sk
          type: sketch
          format: theta
      comparison:
        reference:
          strategy: external
          labelField: window
          baselineLabel: last_week
          targetLabel: this_week
      vocabulary:
        dimensions:
          - name: region
          - name: category
```

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
| unit           | optional | Enum              | `slice` (default) or `metric` (metric tree, see [below](#metric-tree-attribution-vocabularyunit-metric)). |
| dimensions     | required for slice | Array<Dimension\> | Dimensions (input columns) forming the slice vocabulary. Max 31. For `unit: metric` they are the columns available to `breakdowns` (optional). |
| tree           | required for metric | Tree | The metric tree declaration (`unit: metric` only). See [tree parameters](#tree-parameters). |
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
| basis             | optional | Enum | `contribution` (default) or `causalAdjusted` (`unit: metric` only — apply the causal edges declared in `causal`). |
| derivedAllocation | optional | Enum | Allocation of derived measures to components: `gre` (default), `partialDerivative`, or `shapley` (exact, up to 10 variables). Slice mode only. |
| epBasis           | optional | Enum | Basis of explanatory power: `auto` (default), `netDelta`, or `absoluteDelta`. See below. Slice mode only. |
| causal            | required for causalAdjusted | Causal | Causal edges and estimation settings for the metric tree. See [causal parameters](#causal-parameters). |

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
  is rejected for it at validation time. [Distribution measures](#distribution-measures) always
  use `absoluteDelta` regardless of this setting (quantiles are not additive).

## engine parameters

| parameter  | optional | type       | description                                                                    |
|------------|----------|------------|--------------------------------------------------------------------------------|
| algorithm  | optional | Enum       | `riskloc` (default), `adtributor`, `squeeze`, `exhaustive`.                    |
| riskloc    | optional | RiskLoc    | `{ riskThreshold: 0.5, pepThreshold: 0.02, pruningLayers: 1 }`. `riskThreshold` is the minimum risk score for a slice to qualify; `pepThreshold` stops the iteration once the remaining unexplained share drops below it; `pruningLayers` only speeds up search (never changes results, `0` disables). |
| adtributor | optional | Adtributor | `{ teep: 0.1, tep: 0.67 }` — per-value and cumulative explanatory power thresholds (NSDI 2014). |
| squeeze    | optional | Squeeze    | `{ psUpperBound: 0.9, maxNumElementsSingleCluster: 12, maxNormalDeviation: 0.2, enableFilter: true }` — reference-implementation defaults (ISSRE 2019): potential-score early-exit bound, per-cuboid element cap, minimum mean deviation for a cluster to count as anomalous, and the knee-point amplitude filter. |
| guards     | optional | Guards     | See below.                                                                     |
| metricTree | optional | MetricTree | `{ minParentDeltaRatio: 0.01 }` — `unit: metric` degeneracy guard, see [Metric-tree attribution](#degenerate-parents). |

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
| topK          | optional | Integer | Maximum findings per measure. Default `3` (`10` for `unit: metric`, where it bounds the ranked non-root nodes). |
| emitNoFinding | optional | Boolean | If `true` (default), emits an explicit `noFinding: true` row for a measure with no significant attribution — useful for downstream agents to branch on. Note that an entirely empty input produces no output at all. |
| drilldown     | optional | Drilldown | `unit: metric` only: slice-localize the top tree nodes onto a second output `<name>.drilldown`. `{ topK: 3, dimensions: [...], minExplanatoryPower: 0 }`, see [Drilldown](#drilldown-why--where). |

## Output schema (report mode, unit: slice)

One row per finding per measure (plus one `noFinding` row per measure when applicable):

| field             | type                                          | description                                                       |
|-------------------|-----------------------------------------------|--------------------------------------------------------------------|
| measure           | String                                        | Measure name                                                       |
| quantile          | Double (nullable)                             | The analyzed quantile — set only for distribution measures (one row group per quantile) |
| algorithm         | String                                        | `riskloc` / `adtributor` / `exhaustive`                            |
| epBasis           | String                                        | Explanatory-power basis actually used: `netDelta` or `absoluteDelta` (see [epBasis](#epbasis)) |
| rank              | Long                                          | 1-based rank within the measure (0 on noFinding rows)              |
| elements          | Array<Struct{dimension: String, value: String}\> | The slice conjunction (Adtributor: the selected values of the culprit dimension) |
| layer             | Long                                          | Number of dimensions combined in the slice                         |
| riskScore         | Double (nullable)                             | Algorithm confidence score: RiskLoc's risk score, or Squeeze's generalized potential score (null for other algorithms) |
| explanatoryPower  | Double                                        | Share of the total change explained by the slice                   |
| unexplainedShare  | Double                                        | Share of the measure's change (on its `epBasis`) that the reported findings do **not** explain, clamped to [0, 1]. Same value on every row of a measure. See below. |
| externalCandidate | Boolean                                       | `true` when the measure's change looks caused by an **external root cause** — real, but not localizable in the declared dimensions. Same value on every row of a measure. See below. |
| surprise          | Double (nullable)                             | Jensen–Shannon divergence based distribution-change score          |
| baseline / target / delta | Double                                | Slice sums (derived measures: the expression over slice component sums; distribution measures: quantiles of the merged slice sketches; distinct measures: union distinct estimates) |
| deltaRatio        | Double (nullable)                             | `delta / baseline` (null when baseline is 0)                       |
| totalBaseline / totalTarget | Double                              | Measure totals for context                                         |
| leafCount         | Long                                          | Number of leaves covered by the slice                              |
| noFinding         | Boolean                                       | `true` only on explicit no-finding rows                            |

### External root cause detection (externalCandidate / unexplainedShare)

`externalCandidate: true` means the measure's change looks caused by an **external root
cause**: it is real, but not localizable in the declared dimensions — the culprit lives in a
dimension you did not declare, or affects everything at once (a global shift). The right
reaction is to add candidate dimensions or investigate outside this dataset — not to trust
the reported slices as the full story. Downstream agents can branch directly on the flag.

The judgment (adapted from PSqueeze's external root cause determination) requires the measure
to have actually moved (relative net change ≥ 5%), and then flags it when:

- **no findings were reported** — a real change that nothing localized; or
- **`algorithm: squeeze`**: the weakest finding's potential score (`riskScore`) is below 0.8
  (the reference implementation's criterion; its per-dataset threshold calibration does not
  apply to a single run, so the reference's fallback value is used); or
- **other algorithms**: `unexplainedShare` exceeds 0.35 (adapted from the reference's
  "explains less than 65%" criterion) — skipped when the findings were truncated at
  `output.topK`, where the unexplained share is inflated by design.

`unexplainedShare` itself stays available as the raw quantity behind the judgment (with no
findings it is 1 by definition — read it together with the delta). Measures whose totals
cancel by construction (`synthetic` marginal) never qualify as "moved" and are never flagged.

## Metric-tree attribution (vocabulary.unit: metric)

Slice attribution asks *where* a metric changed. Metric-tree attribution asks *why*: given a
declared tree of driver metrics — the KPI tree every analytics team keeps in a spreadsheet —
it decomposes the root's change into the exact contribution of every node, at every level, so
"revenue fell" becomes "AUP held, units fell; of that, repeat-customer units in the no-deal
segment". It is the metric-tree change decomposition (MTCD) framework of Zhou et al. (KDD '26
TSMO workshop), optionally with their causal correction for dependent siblings.

Both axes compose: `output.drilldown` runs a slice attribution on the top tree nodes and emits
the culprit slices on a second output, `<name>.drilldown` (see [Drilldown](#drilldown-why--where)).

### Input and node values

Input rows are the same as in slice mode — raw or pre-aggregated rows carrying a period label
(or two inputs, a time column, …; every `comparison.reference` strategy works) plus the numeric
columns the tree refers to and the dimension columns used by breakdowns. Every node's baseline
and target values are computed from **period-level sums** of input columns:

| node value   | meaning                                                                                  |
|--------------|------------------------------------------------------------------------------------------|
| `field`      | Sum of an additive input column (units, revenue, sessions, …).                           |
| `expression` | A [Lucene expression](https://lucene.apache.org/core/10_5_0/expressions/org/apache/lucene/expressions/js/package-summary.html) over period-level sums of input columns — the way to declare a **rate**: `revenue / units`, `orders / sessions`. Never feed a pre-averaged rate column. |
| (none)       | Implied by the static children: `sum` → Σ components, `product` → volume × rate.         |

### tree parameters

`vocabulary.tree.nodes` is a flat list of named nodes; structure comes from references.

| parameter     | optional | type   | description |
|---------------|----------|--------|-------------|
| name          | required | String | Node name. `measures[].name` must name a node (the root of that analysis; several measures = several roots). |
| field         | optional | String | Additive input column summed per period. |
| expression    | optional | String | Rate expression over input columns (mutually exclusive with `field`). |
| decomposition | optional | Enum   | Static children: `sum` (Type 1, `components`) or `product` (Type 2, `volume` + `rate`). |
| components    | required for sum | Array<String\> | Child node names whose values sum to this node. |
| volume        | required for product | String | The additive (count) child node `n` in `y = n · X̄`. |
| rate          | required for product | String | The rate child node `X̄` in `y = n · X̄`. |
| breakdowns    | optional | Array<Breakdown\> | Dynamic children per value of a dimension column (below). Each breakdown is an independent, complete partition of this node's change. |

A node may be referenced by several parents (the static children must only be acyclic).

#### breakdown parameters

| parameter     | optional | type   | description |
|---------------|----------|--------|-------------|
| by            | required | String | Dimension column (must be declared in `vocabulary.dimensions`). One child per value. |
| decomposition | optional | Enum   | `sum` (default, Type 1 — node is additive), `sumOfProducts` (Type 3 — node is an additive `field`; `volume` required), or `weightedAverage` (Type 4 — node is a rate `expression`; `weight` required). |
| volume        | required for sumOfProducts | String | Additive input column `n_k`; the per-group rate is `field / volume`. |
| weight        | required for weightedAverage | String | Additive input column giving the share `p_k = weight_k / Σ weight`. For an exact decomposition use the rate's **denominator** (e.g. `sessions` for `orders / sessions`). |
| breakdowns    | optional | Array<Breakdown\> | Nested breakdowns applied to each group's **rate** child (`sum`: the group itself), on the rows of that group only. |

The local change decompositions (Table 1 of the paper) are:

| decomposition     | metric                     | contributions (sum exactly to Δy)                                   | child rows per value |
|-------------------|----------------------------|----------------------------------------------------------------------|----------------------|
| `sum`             | `y = Σ y_k`                | `Δy_k`                                                               | one (`effect: delta`) |
| `product`         | `y = n · X̄`               | volume `Δn · X̄₁`, rate `ΔX̄ · n₀` (ordered: the rate moves first)  | `volume`, `rate` (static) |
| `sumOfProducts`   | `y = Σ n_k · X̄_k`         | volume `Δn_k · X̄_k,1`, rate `ΔX̄_k · n_k,0`                         | `volume`, `rate`     |
| `weightedAverage` | `ȳ = Σ p_k · ȳ_k`         | share `Δp_k · (ȳ_k,0 − ȳ₀)`, rate `Δȳ_k · p_k,1`                   | `share`, `rate`      |

Contributions propagate down the tree (Algorithm 1): the root's contribution is its own change
`Δy_r`; a child's contribution is its local contribution rescaled by the parent's importance,
`C_c = contrib_c · C_v / Δy_v`. Hence every group of siblings sums exactly to their parent's
contribution, and every node's `explanatoryPower = C / Δy_root`.

#### Degenerate parents

When a parent's own change is (nearly) zero because its children cancel, the rescaling
`C_v / Δy_v` is undefined or explosive. A parent whose `|Δy_v| < minParentDeltaRatio · Σ|contrib_c|`
(`engine.metricTree.minParentDeltaRatio`, default `0.01`; `0` only guards the exact zero) is
flagged `degenerate: true`; its descendants get `contribution = 0` but keep their
`localContribution`, so the cancelling movement remains visible in the report.

A `residual` value on a node means its local decomposition did not add up to its change (beyond
rounding) — typically a `weightedAverage` whose `weight` is not the rate's denominator, or a
`product` node that also carries its own `field`. The contributions are still reported as
computed; fix the declaration for an exact decomposition.

### causal parameters (`semantics.basis: causalAdjusted`)

MTCD treats siblings as independent. When a rate causally drives its volume sibling (AUP →
units sold: a price change moves volume), the plain split misattributes the induced volume
change to the volume node. Declaring the edge applies the paper's unified estimator: a function
`f̂₀` from the rate to the volume is fitted on **baseline-period granules** (e.g. days), the
volume node is credited only with its own mechanism change conditional on the new-period rate,
and the rate node receives the rest (direct + indirect effect) — ordered allocation along the
causal path, which is the aligned estimand when the causal order is unique (Shapley averaging
over impossible orders is *not* used).

| parameter           | optional | type   | description |
|---------------------|----------|--------|-------------|
| granularity.field   | required unless every edge has `elasticity` | String | Column identifying the regression granule (day, week, store, …). Any type; not a declared dimension. |
| edges               | required | Array<Edge\> | Declared dependencies (below). |
| slopeStabilityAlpha | optional | Double | Significance level of the slope-stability test used by `estimator: auto`. Default `0.05`. |
| minGranules         | optional | Integer | Minimum baseline granules to fit `f̂₀`; below it the edge is not applied (`estimator: fallback` + `warning`). Default `14`. |

| edge parameter | optional | type    | description |
|----------------|----------|---------|-------------|
| from           | required | String  | The cause: the **rate** child of a `product` node. |
| to             | required | String  | The effect: the **volume** child of the same `product` node (v1 supports sibling edges only; one edge per target). |
| model          | optional | Enum    | `linear` (default) or `quadratic` polynomial `f̂₀`. |
| robust         | optional | Boolean | Huber-weighted fit (outlier resistant). Default `false`. |
| estimator      | optional | Enum    | `auto` (default), `simplified` (Eq. 6: `(n₁ − Σⱼ f̂₀(X̄₁ⱼ)) · X̄₁`, needs only period aggregates of `n`), or `full` (Eq. 5: `Σⱼ (n₁ⱼ − f̂₀(X̄₁ⱼ)) · X̄₁ⱼ`). `auto` tests the simplified estimator's assumption (only the intercept of the relation changed, F-test on the pooled interaction model) and switches to `full` when it is rejected. |
| elasticity     | optional | Double  | Known slope `dn/dX̄` instead of fitting: `n₀* = n₀ + elasticity · ΔX̄`. Lets the edge work without granular data. |

> **The edges are your assumption.** Only the declared edges need to be causally right (the
> rest of the tree stays model-free), but a wrong direction makes both the plain and the
> adjusted attribution wrong. Use domain knowledge, temporal ordering and falsification tests
> before declaring one; the output's `diagnostics` (fit coefficients, `r2`, granule counts,
> `interactionPValue`) help judge the fit.

### Output schema (unit: metric)

One row per tree node (the root plus the `topK` highest-|contribution| non-root nodes).

| field             | type              | description |
|-------------------|-------------------|-------------|
| measure           | String            | Root node name |
| algorithm         | String            | `mtcd` |
| node              | String            | Node name (breakdown children: the node, `<node>_rate`, `<node>_share`, or the volume column) |
| parent            | String (nullable) | Parent path |
| path              | String            | `revenue/aup/deal=deal/rate` — static children by name, breakdown children as `<by>=<value>[/<effect>]` |
| depth             | Long              | 0 for the root |
| dimension / value | String (nullable) | Set on breakdown children |
| decomposition     | String (nullable) | The rule that produced this child from its parent |
| effect            | String            | `root`, `delta`, `volume`, `rate`, `share` |
| rank              | Long              | 1-based rank of non-root nodes by \|contribution\| (0 for the root) |
| baseline / target / delta / deltaRatio | Double | The node's own values |
| localContribution | Double            | Contribution to the **parent's** change (kept even under a degenerate parent) |
| contribution      | Double            | Contribution to the **root's** change (`C_c`) |
| explanatoryPower  | Double            | `contribution / Δroot` |
| rootBaseline / rootTarget | Double    | Root values for context |
| degenerate        | Boolean           | This node's children were zeroed (see above) |
| residual          | Double (nullable) | Non-additive local decomposition (see above) |
| causalAdjusted    | Boolean           | A causal edge was applied to this node |
| estimator         | String (nullable) | `simplified`, `full`, `elasticity`, or `fallback` |
| diagnostics       | Struct (nullable) | `beta[]`, `r2`, `baselineGranules`, `targetGranules`, `interactionPValue` |
| warning           | String (nullable) | e.g. why an edge was not applied |
| noFinding         | Boolean           | `true` on the single row emitted when the root did not change at all |

### Drilldown (why × where)

`output.drilldown` closes the loop between the two axes inside one step: for the
`drilldown.topK` highest-ranked non-root nodes, the module runs the **slice attribution** of
this document (same `engine.*` algorithm and guards) with

- the node's value as the measure — its `field`, its `expression`, or the composition of its
  static children (`(units_new) + (units_repeat)`, `(units) * (revenue / units)`), as a
  fundamental or derived measure; a `share` child localizes its `weight` column;
- the node's own rows as the input — a breakdown child only sees the rows of its group;
- `drilldown.dimensions` (default: every declared dimension) **minus** the dimensions already
  fixed on the node's path as the vocabulary.

| parameter           | optional | type          | description |
|---------------------|----------|---------------|-------------|
| topK                | optional | Integer       | Number of top-ranked nodes to drill into. Default `3`. |
| dimensions          | optional | Array<String\> | Subset of `vocabulary.dimensions` to localize on. Default: all. |
| minExplanatoryPower | optional | Double        | Skip nodes whose \|explanatoryPower\| is below this (e.g. nodes zeroed by a degenerate parent). Default `0`. |

The findings go to the secondary output **`<name>.drilldown`** with the
[slice report schema](#output-schema-report-mode-unit-slice) (`elements`, `explanatoryPower`,
`riskScore`, `externalCandidate`, `noFinding`, …) prefixed by the node being explained:

| field                | type    | description |
|----------------------|---------|-------------|
| node / path / nodeEffect | String | The tree node (as in the primary output) |
| nodeRank             | Long    | Its rank in the tree |
| nodeContribution / nodeExplanatoryPower | Double | Its contribution to the root change |
| nodeExpression       | String  | The measure expression that was localized |
| measure              | String  | The tree root name; `algorithm` is the slice algorithm (`riskloc`, …) |

Reading it together: the primary output says *"units (volume) explains 80% of the revenue
drop"*; the drilldown row for `revenue/units` says *"…and that is `region=east AND
channel=app`"*. A `noFinding` drilldown row (or `externalCandidate: true`) means the driver moved
uniformly across the declared dimensions — a global cause rather than a segment.

### Example: revenue driver tree with a causal edge

```yaml
sources:
  - name: daily
    module: bigquery
    parameters:
      query: |
        SELECT period, day, deal_status, account_type,
               SUM(units) AS units, SUM(units_new) AS units_new, SUM(revenue) AS revenue
        FROM `myproject.kpi.daily_sales`
        WHERE period IN ('2025-07', '2025-08')
        GROUP BY period, day, deal_status, account_type
transforms:
  - name: whyRevenue
    module: attribution
    inputs: [daily]
    parameters:
      measures:
        - name: revenue
      comparison:
        reference:
          strategy: external
          labelField: period
          baselineLabel: "2025-07"
          targetLabel: "2025-08"
      vocabulary:
        unit: metric
        dimensions:
          - name: deal_status
          - name: account_type
        tree:
          nodes:
            - name: revenue
              decomposition: product
              volume: units
              rate: aup
            - name: units
              field: units
              decomposition: sum
              components: [units_new, units_repeat]
            - name: units_new
              field: units_new
            - name: units_repeat
              expression: "units - units_new"
            - name: aup
              expression: "revenue / units"
              breakdowns:
                - by: deal_status
                  decomposition: weightedAverage
                  weight: units
                  breakdowns:
                    - by: account_type
                      decomposition: weightedAverage
                      weight: units
      semantics:
        basis: causalAdjusted
        causal:
          granularity: { field: day }
          edges:
            - from: aup
              to: units
      output:
        topK: 10
        drilldown:
          topK: 3
          dimensions: [deal_status, account_type]
sinks:
  - name: treeReport
    module: bigquery
    inputs: [whyRevenue]
    parameters: { table: "myproject:kpi.revenue_tree" }
  - name: sliceReport
    module: bigquery
    inputs: [whyRevenue.drilldown]
    parameters: { table: "myproject:kpi.revenue_tree_slices" }
```

Reading the output: the row `revenue/units` (effect `volume`, `causalAdjusted: true`) is the
part of the revenue change caused by the units mechanism itself; `revenue/aup` carries AUP's
direct effect plus the volume it induced; `revenue/aup/deal_status=deal/rate` and its nested
`account_type` rows say which segment's price moved. Rank orders every node by absolute
contribution, and `localContribution` lets you read each level on its own scale. The
`whyRevenue.drilldown` rows then name the segment behind each of the top three nodes.

## Choosing an algorithm

| | `riskloc` (default) | `adtributor` | `squeeze` | `exhaustive` |
|---|---|---|---|---|
| Root causes it finds | Cross-dimension slices, multiple independent causes | Elements within a single dimension | Cross-dimension element sets, clustered per deviation magnitude | Everything (exact EP ranking) |
| Cost | Medium | Light | Medium | Heavy (combinatorial) |
| Output shape | Minimal culprit slice set | Per-dimension value ranking | One finding per anomaly cluster (its element set as slices) | Full slice ranking |
| Typical use | Incident investigation, KPI deep-dive | Recurring reports, screening dashboards | Second opinion, forecast baselines, simultaneous multi-cause | Calibration, small-data exact answers |

- **`riskloc`** — start here and keep it unless you have a reason not to. It is the only
  implemented algorithm that finds cross-dimension culprits (`region=a AND category=x`) and
  separates multiple independent causes iteratively. The one practical tuning knob is
  `riskThreshold`: lower it to `0.3`–`0.4` for recall-oriented investigation (including
  single-leaf causes, which sit on the 0.5 boundary — see
  [Known limitations](#known-limitations)), raise it to `0.6`–`0.7` to suppress noise in
  recurring monitoring.
- **`adtributor`** — for routine per-dimension reporting where the "one culprit dimension"
  assumption holds ("which country?", "which client version?"). Lighter, stable, and its
  ranking output is easy to explain to non-analysts, but it structurally misses
  cross-dimension (mix/interaction) culprits. A practical two-stage pattern: run `adtributor`
  on schedule, and when a change it cannot explain appears, re-run the same config with
  `riskloc` for the deep dive.
- **`exhaustive`** — not for production jobs. On small data (up to roughly thousands of leaves
  with `maxLayer` ≤ 2) it produces the exact EP ranking, which makes it the tool for
  calibrating `riskThreshold` when onboarding a new data domain, and for verifying that a
  `riskloc` result is not a search artifact.
- **`squeeze`** — the other major published localization algorithm (Li et al., ISSRE 2019),
  ported from the reference implementation. It first clusters leaves by deviation magnitude
  and then searches each cluster independently, so **simultaneously active causes with
  different magnitudes** land in separate findings in one pass (riskloc separates them by
  iterative removal instead), and one finding can carry a multi-element slice set. It has no
  risk-threshold knob (selection is potential-score driven) and pairs naturally with
  forecast-style baselines. On the public benchmarks riskloc scores higher overall — use
  squeeze as a **second opinion**: agreement between the two raises confidence, disagreement
  flags a case for manual review.

### Calibrating riskThreshold with the exhaustive oracle

When onboarding a new data domain, use `exhaustive` once to calibrate `riskloc` instead of
guessing thresholds:

1. Take a representative sample of the comparison (restrict to the vocabulary dimensions,
   `guards: {maxLayer: 2}`, and keep the leaf count small — up to roughly thousands of
   distinct tuples — so the full enumeration stays cheap).
2. Run the config twice, changing only `engine.algorithm`: `exhaustive` produces the exact
   explanatory-power ranking (the ground truth), `riskloc` the production candidate.
3. Compare the top slices. If `riskloc` misses culprits that sit high in the exhaustive
   ranking, lower `riskThreshold` in steps of ~0.05 and re-run; single-leaf culprits in
   particular sit on the 0.5 boundary (see [Known limitations](#known-limitations)). If
   `riskloc` reports slices the exhaustive ranking assigns negligible explanatory power,
   raise the threshold instead.
4. Fix the calibrated threshold in the production config and drop `exhaustive`. Re-calibrate
   when the data regime changes (new dimensions, cardinality growth, different noise level).

The same two-run comparison also distinguishes "the algorithm missed it" from "the signal is
not in the data" when a production run returns an unexpected no-finding.

For derived measures, keep `derivedAllocation: gre` (default) for ordinary ratio KPIs.
`partialDerivative` is a cheap linearization that is adequate while changes are small relative
to the totals — a large disagreement with `gre` is itself a signal that the change is too big
for linear attribution. Choose `shapley` (≤ 10 variables) when the allocation itself is the
deliverable and must be order-independent and axiomatically fair, e.g. financial variance
reporting. For the explanatory-power basis, the `auto` default rarely needs overriding — see
[epBasis](#epbasis).

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

## Example: latency tail regression (distribution measure)

"The p99 latency regressed after the release — which endpoint × region × version is
responsible?" Feed event-level rows; the module builds per-slice latency distributions
(KLL sketches) and localizes the quantile shift.

```yaml
sources:
  - name: requests
    module: bigquery
    parameters:
      query: |
        SELECT endpoint, region, app_version, deployed, latency_ms
        FROM `myproject.logs.requests`
        WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
transforms:
  - name: latencyRegression
    module: attribution
    inputs: [requests]
    parameters:
      measures:
        - name: latency_ms
          type: distribution
          quantiles: [0.5, 0.99]
      comparison:
        reference:
          strategy: split
          split:
            by: {field: deployed, baseline: false, target: true}
      vocabulary:
        dimensions:
          - name: endpoint
          - name: region
          - name: app_version
```

## Execution profile and scale guidance

The pipeline runs in two stages:

1. **Distributed leaf aggregation** — rows are keyed by (role, dimension tuple) and combined
   with combiner lifting: measure sums, KLL / Theta sketches and row counts accumulate on the
   mappers, so shuffle volume is proportional to the number of **distinct leaf tuples**, not
   input rows. Event-level input at scale is handled here.
2. **Single-worker localization** — only the leaf aggregates are gathered to one worker where
   the search runs. Measured on the seeded synthetic benchmark
   (`AttributionEngineBenchmarkTest`, single thread, default engine config):

| leaves (3 dims) | riskloc localization |
|---|---|
| 10,000 | ~40 ms |
| 100,000 | ~150 ms |
| 1,000,000 | ~1.4 s |

   Runtime is near-linear in leaf count and stays around one second at a million leaves even
   at 6 dimensions / 41 cuboids (element pruning keeps the cuboid-count effect sublinear), so
   the single-worker step is not the bottleneck for realistic vocabularies. `squeeze` is
   heavier — ~150 ms at 10k and ~9 s at one million leaves (its KDE amplitude filter is
   O(leaves × 1000)) — but still single-worker viable. Memory is the practical limit: keep
   distinct leaf tuples roughly below a few million with `guards.maxCardinality` /
   `minSupport` and by limiting `vocabulary.dimensions`. The cuboid count grows exponentially
   with `guards.maxLayer`, so raise it with care.

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
- **Distribution measures are approximate beyond 200 samples per leaf and role.** Up to that
  size the KLL sketch stores all values and quantiles are exact and deterministic; beyond it,
  estimates carry the KLL rank error bounds (~1.65%) and sketch compaction is randomized, so
  repeated runs can differ slightly in quantile values (rarely in the localized slices). Small
  quantile shifts below the error bound are not reliably attributable.
- **Distinct measures are approximate beyond 4096 identities per leaf and role** (Theta error
  bounds, ~1.6% RSE). Leaf-level distinct deltas do not decompose the union delta exactly when
  identities span leaves — explanatory power ranks leaves by their own estimate shifts, which
  can over- or under-state a slice's contribution to the overall distinct change. The reported
  slice values (union estimates) are always the consistent quantity.
- Misspelled enum values deserialize as null and silently fall back to their defaults
  (Gson behavior common to all modules); validation rejects reserved values only when
  spelled exactly.
- The module always re-aggregates duplicate dimension tuples by summing before analysis.
- Metric-tree causal edges are limited to the rate → volume siblings of a `product` node (the
  paper's validated case); dependent shares in `weightedAverage` breakdowns and edges across
  subtrees are not adjusted. The adjustment is unbiased when the causal order is unique and the
  declared direction is right; it is as outlier-sensitive as any regression (`robust: true`).
- Streaming inputs are rejected at validation time in this version.
