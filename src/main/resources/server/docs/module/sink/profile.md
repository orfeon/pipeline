---
type: Sink Module
title: Profile Sink Module
description: Generates a single self-contained interactive HTML data profiling report from the input dataset using Apache DataSketches (quantiles, distinct counts, frequent items, set sketches, sampling, correlations). All estimates carry mathematical error bounds and the mergeable sketch binaries are embedded for cross-run comparison (CPC / Theta / frequent-items sketches also readable by BigQuery `bqutil.datasketches`).
tags: [sink, profile, batch, report, html, statistics, datasketches, observation, quality]
timestamp: 2026-07-29T00:00:00Z
---

# Profile Sink Module

Sink Module that observes the input dataset and writes a **single self-contained HTML report** for
data exploration. Aggregates are extracted in one pass with [Apache DataSketches](https://datasketches.apache.org/)
(mergeable, mathematically error-bounded sketches) and embedded into the report as JSON, which the
report renders interactively in the browser.

The report is descriptive, not judgmental: it shows *what the data looks like* (distributions,
cardinalities, frequent values, correlations, key properties) and never gates or fails a pipeline
based on data content. It works with **no required parameters** — pointing it at an input already
produces a useful report — and each additional declared parameter adds a corresponding report section.

The observations per field type:

| profile type | applied to | collected statistics |
|---|---|---|
| numeric | int8/16/32/64, float8/16/32/64, decimal | null rate, min/max/mean/stddev/skewness, quantiles + histogram + CDF (KLL), distinct count (CPC), zero/NaN/Inf counts |
| string | string, json, enumeration | null rate, distinct count (CPC), top-K frequent values (Frequent Items), length distribution, empty count |
| boolean | bool | true/false counts, null rate |
| timestamp | timestamp, datetime, date | min/max, time-bucket histogram, null rate |
| array | array | element-count distribution |

`struct (element)` fields are flattened to dot paths up to depth 3. `map`, `bytes` and other
unsupported types are skipped and listed in the report appendix. Numeric×numeric Pearson
correlations are computed for all pairs by default. Rows are sampled with VarOpt sampling for
representative-value display and scatter plots.

## Output artifact

A single HTML file that is both the report and the sketch store. It embeds three versioned JSON blocks:

- `profile-payload` — view data (statistics, 256-bin histograms, CDFs, top-K, correlation matrix, suggestions)
- `profile-manifest` — run metadata (expanded parameters, actual sketch parameters, schema snapshot, degradations)
- `profile-sketches` — base64 sketch binaries (mergeable across runs; the CPC / Theta / frequent-items sketches are readable by BigQuery `bqutil.datasketches` UDFs, the KLL quantile sketches are the doubles variant those UDFs do not read)

When the embedded total exceeds the size limit the report degrades automatically in a fixed order
(sketch binaries → sample rows → histogram resolution) and records what was dropped in the manifest.
Charts are rendered with ECharts loaded from CDN (version-pinned with SRI); without network access
the numeric statistics remain readable and chart areas show a fallback message.

The report may contain raw data values (top-K labels, sample rows). **Treat the report with the
same sensitivity as the source data**, or set `values: hide`.

## Sink module common parameters

| parameter | optional | type                | description                                        |
|-----------|----------|---------------------|----------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.  |
| module    | required | String              | Specified `profile`                                |
| inputs    | required | Array<String\>      | Specify the names of the step to be used as input. Multiple inputs are unioned. |
| parameters | optional | Map<String,Object\> | Specify the following individual parameters (all optional) |

## Profile sink module parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| output | optional | String or Object | Report destination as a string (`gs://...`, any other Beam FileSystems uri such as `s3://...`, or a local path), or object form `{report: ..., sketches: ...}` where `sketches` additionally writes the sketch binaries JSON to a separate location. When omitted, falls back to `{workDir}/{name}/report.html` (DirectRunner server) or `{tempLocation}/profile/{jobName}/{name}/report.html`. A local path is written on the worker that renders the report, so on a distributed runner (Dataflow, Flink, Spark) use a storage uri; the sink warns at construction otherwise. |
| fields | optional | Object | Field filter: `{include: [...]}` or `{exclude: [...]}` with dot paths for nested fields. |
| values | optional | Enum | `show` (default) or `hide`. With `hide`, raw values are removed from the report (top-K labels, sample rows, value-bearing sketch binaries); ranks, frequencies, distributions and statistics remain. |
| keys | optional | Array<String\> | Fields to treat as identifiers. Adds Theta set sketches per key: distinct counts with bounds, keyness (distinct/rows) and a pairwise containment matrix, shown in a dedicated Keys tab. |
| target | optional | String or Object | The binary outcome to relate every other field to. Shorthand `sold_flag` for a bool field (positive = `true`), or longhand `{field: is_sold, positive: 1}` / `{field: status, positive: sold}` for numeric and string fields (`positive` is required there; any other non-null value is negative, null target rows are excluded). Adds a Target tab — fields ranked by information value (IV), KS / total-variation separation, point-biserial correlation, positive rate among null rows, and a positive-rate-per-bin / per-category chart for each field — plus positive/negative chips in the compare bar that overlay the two classes on every field card, and the positive rate per group in the Segments / Time / Compare tables. When no row (or every row) matches `positive`, the report shows a warning banner and the sink logs it, since the per-field statistics need both classes. Continuous (regression) targets are not supported. |
| segments | optional | Array<String or Object\> | Fields to compare the dataset by. Shorthand `[category]` or longhand `[{field: category, topK: 30}]` (`topK`: max groups kept, default 20, largest first). Adds per-segment sub-profiles, a Segments tab and chips in the global compare bar — selecting chips overlays per-group distributions on every field card. Only the kept groups are profiled: group sizes are counted first and the sub-profiles are computed for the top `topK` alone, so a high-cardinality field (user ids, free text) costs one small count shuffle rather than one sketch set per distinct value; the report shows how many groups were left out. |
| time | optional | String or Object | Timestamp field for time evolution. Shorthand `created_at` or longhand `{field: created_at, granularity: day}` (`granularity`: `hour`/`day`/`week`/`month`/`year`, default `month`; UTC buckets, most recent 60 kept — like `segments`, only those 60 are profiled). Adds per-bucket sub-profiles, a Time tab and compare-bar chips like `segments`. |
| mode | optional | Enum | `union` (default) merges multiple inputs into one profile. `compare` (requires 2+ inputs) additionally treats each input as a comparison group: per-input sub-profiles, compare-bar chips, and PSI/KS drift metrics against the baseline shown per field and in the Compare tab. |
| baseline | optional | String | With `mode: compare`, the input used as the drift reference (default: the first input). The baseline chip is always included in overlays as the fixed reference. |
| compare | optional | Array<Array<String\>\> | Declared comparable numeric field pairs, e.g. `[[list_price, sold_price]]`. Each pair gets an overlaid distribution, a Q-Q plot and PSI/KS statistics in the Relations tab. |
| compareWith | optional | String | Path (`gs://...` or local) of a **past report generated by this module**, checked for existence at pipeline construction. Its embedded payload is compared field-by-field (PSI/KS from stored CDFs, quantile/null-rate/distinct shifts, added/removed/type-changed alignment); when both reports embed sketch binaries, key fields additionally get Theta-sketch overlap (retained/new key shares). Results appear in the Compare tab. Sketch-parameter mismatches between runs are reported as warnings. |
| accuracy | optional | Enum | `low`, `default`, `high`. Preset controlling sketch size/accuracy trade-off (KLL k = 100/200/800, CPC lgK = 10/12/14). Actual values are recorded in the report appendix. |
| associations | optional | Object | `{numeric: all}` (default) computes Pearson correlation for all numeric pairs; `{numeric: none}` disables. |
| sample | optional | Object | `{enabled: true, k: 10000}`. VarOpt row sampling for sample values and scatter plots. Disabled automatically when `values: hide`. |
| report | optional | Object | `{title: ...}`. Report title (defaults to the sink name). |
| fanout | optional | Integer | Combine fan-out for hot-path distribution. Default: `16`. |

Batch (bounded) inputs only, in the global window: streaming inputs and a non-global `strategy.window`
(or a windowed input) are rejected at pipeline construction, since every window would render to the
same output path.

### Output and failure handling

The sink emits one record per run: `output` (report path), `rows`, `errorRows`, `fields`, `bytes`.

Each input record is reduced to its profiled fields once, before the combine. A value the profile
type cannot interpret (for example a decimal column arriving as raw bytes with an unknown scale) is
counted as a field error in the report and does not affect the other fields. A field whose
conversion throws (an unsupported value type in the input) is also counted as a field error, the
row is counted in `errorRows`, and the record is routed to the module's failure handling like any
other sink: with `failFast: true` the job fails naming the field, otherwise the record goes to
`failureSinks` / `outputFailure` (the first failures are also logged with the field and cause).
`decimal` fields use the schema scale (default 9, BigQuery NUMERIC); nested `struct` fields are
navigated whether the source delivers them as nested records or, like Spanner STRUCT columns, as a
JSON string.

Data values are embedded in the report as JSON with `<` escaped, so a value containing
`</script>` cannot break out of the embedded blocks; non-finite numbers (NaN/Infinity) are
reported in the field counters and stored as null in sample rows.

The report's "Next steps" tab shows analyses the module could have run but did not (key/segment/time/target
candidates inferred from the data) as copy-pasteable parameter snippets — automatic inference is
deliberately limited to cheap and harmless statistics, while semantic declarations are left to you.

## Examples

### Example 1: No configuration

Profile all fields of a BigQuery query result.

```yaml
sources:
  - name: items
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.items`"

sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items
```

### Example 2: Output location, field filter and title

```yaml
sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items
    parameters:
      output: gs://mybucket/reports/items.html
      fields:
        exclude:
          - description
          - image_urls
      report:
        title: Items dataset observation
```

### Example 3: Declaring keys and accuracy

Declaring identifier fields adds key diagnostics (distinct counts, keyness, containment between keys).

```yaml
sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items
    parameters:
      output: gs://mybucket/reports/items.html
      keys:
        - user_id
        - item_id
      accuracy: high
```

### Example 4: Segment and time comparison

Each declaration adds a report section: per-category sub-profiles with a compare bar that overlays
group distributions on every field card, and a monthly time-evolution view.

```yaml
sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items
    parameters:
      output: gs://mybucket/reports/items.html
      segments:
        - category
      time:
        field: created_at
        granularity: month
```

### Example 5: Relating every field to a binary outcome

Declaring the target adds per-class sketches for every other field: the Target tab ranks fields
by how differently the positive and negative rows are distributed over them (information value,
`> 0.5` is flagged as a possible leak), shows the positive rate per bin or category, and the
compare bar overlays the two classes on every field card. With `segments`, each group's positive
rate is shown as well.

```yaml
sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items
    parameters:
      output: gs://mybucket/reports/items.html
      target: sold_flag                 # bool field; for INT64 flags: {field: is_sold, positive: 1}
      segments:
        - category
```

### Example 6: Hiding raw values

For reports that will be shared beyond people with source-data access.

```yaml
sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items
    parameters:
      output: gs://mybucket/reports/items.html
      values: hide
```

### Example 7: Comparing two datasets and the previous year's report

The two inputs become comparison groups with drift metrics against the baseline, and the previous
run's artifact is compared field-by-field without re-scanning last year's data.

```yaml
sources:
  - name: items2025
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.items_2025`"
  - name: items2026
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.items_2026`"

sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items2025
      - items2026
    parameters:
      output: gs://mybucket/reports/items-2026.html
      mode: compare
      baseline: items2025
      keys:
        - item_id
      compare:
        - [list_price, sold_price]
      compareWith: gs://mybucket/reports/items-2025.html
```

### Example 8: Keeping sketch binaries for later comparison or BigQuery

The sketches JSON contains base64 compact sketches per field, mergeable with sketches from other
runs. The CPC (distinct), Theta (key sets) and frequent-items sketches are readable by BigQuery
`bqutil.datasketches` functions; the KLL quantile sketches are `KllDoublesSketch` binaries, which the
`kll_sketch_float_*` functions cannot read.

```yaml
sinks:
  - name: itemsProfile
    module: profile
    inputs:
      - items
    parameters:
      output:
        report: gs://mybucket/reports/items.html
        sketches: gs://mybucket/reports/items_sketches.json
```
