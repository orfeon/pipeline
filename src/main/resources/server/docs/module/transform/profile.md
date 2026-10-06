---
type: Transform Module
title: Profile Transform Module
description: Observes a dataset and emits what it looks like as records any sink can store - one record per field (null rate, distinct count, quantiles, moments, notable codes), per group and field (segments, time buckets, inputs with KS / TVD / PSI and null-rate shift against a reference), per value, per bin, per declared field pair, per field against a binary target, per key, and one summary - plus an optional single-file interactive HTML report. Statistics that compare groups come from exact counts of a second pass over fixed edges; sketch estimates always carry their bounds.
tags: [transform, profile, batch, statistics, quality, drift, monitoring, report, html, datasketches]
timestamp: 2026-10-06T00:00:00Z
---

# Profile Transform Module

Transform Module that observes the input dataset and reports **what it looks like**: per-field
statistics, how the fields differ between groups (segments, time buckets, inputs), and how they relate
to a binary outcome. The results are **records** — append them to a table and you have a history of the
dataset — and, optionally, a **single self-contained HTML report** for reading one run.

It works with **no parameters**: the default output is one record per field. Each declaration
(`keys`, `segments`, `time`, `mode: compare`, `target`, `compare`) adds an output or a set of columns.
The transform observes and never gates: it does not fail a pipeline on data content.

It is not specific to machine learning. The same run serves the acceptance of a rebuilt table, a
migration check (before against after), a month-over-month comparison, or the inventory of a training
matrix. For questions about a *prediction* — does it add information over a baseline, is it calibrated —
use the `evaluation` transform; to rank candidate features against a baseline, the `screen` transform.

## How it counts

| kind of statistic | how | exactness |
|---|---|---|
| counts, null counts, min, max, sum, mean, stddev | one pass | exact |
| values of a field with at most 1,000 distinct values | one pass (value table) | exact (`exact: true`, `distinctExact: true`) |
| quantiles | KLL sketch | within the record's `rankError` |
| distinct counts, frequent values of high-cardinality fields, key sets | CPC / Frequent Items / Theta sketches | estimates with `_lo` / `_hi` bounds |
| bin counts, and everything computed from them: KS, TVD, PSI, information value, rate per bin | **a second pass over fixed edges** | exact for the edges on the record |

The second pass exists because a comparison computed from sketch queries is dominated by sketch error
(two samples of one distribution can show a PSI above 0.1). The first pass takes the edges — the 100
equal-frequency cells of the whole dataset for a numeric field, the most frequent 50 values plus
`(other)` for a string field — and the second pass counts every group's rows over them. The second pass
is skipped when nothing needs it: no `segments` / `time` / `mode: compare`, no `target`, no `compare`
and `bins` not in `outputs`.

Edges come from a sketch, so two runs over the same data can place them slightly differently; the
counts are exact for the edges written on each record. Declare `bins.edges` for a field to fix them.

The observations per field type:

| profile type | applied to | statistics |
|---|---|---|
| numeric | int8/16/32/64, float8/16/32/64, decimal | null rate, min / max / sum / mean / stddev / skewness, quantiles, distinct count, zero / NaN / Inf counts, the exact value table when there are at most 1,000 values |
| string | string, json, enumeration | null rate, distinct count, frequent values, length distribution, empty / blank / null-sentinel counts |
| bool | bool | true / false counts, null rate |
| timestamp | timestamp, datetime, date | min / max, null rate |
| array | array | element-count distribution |

`struct (element)` fields are flattened to dot paths up to depth 3. `map`, `bytes` and other
unsupported types are skipped and listed in the report appendix.

## Transform module common parameters

| parameter | optional | type                | description                                        |
|-----------|----------|---------------------|----------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.  |
| module    | required | String              | Specified `profile`                                |
| inputs    | required | Array<String\>      | Specify the names of the step to be used as input. Multiple inputs are unioned (or compared, with `mode: compare`). |
| parameters | optional | Map<String,Object\> | Specify the following individual parameters (all optional) |

## Profile transform module parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| output | optional | String or Object | Files the transform writes. A string is the HTML report destination (`gs://...`, any other Beam FileSystems uri such as `s3://...`, or a local path). Object form `{report: ..., payload: ...}`: `payload` writes the report's JSON payload and manifest as one JSON file. Omitted: no file is written (there is no default location). A local path is written on the worker that finalizes the run, so on a distributed runner use a storage uri. |
| outputs | optional | Array<String\> | The optional record outputs to compute: `bins`, `values`. Default `[bins, values]`. `[]` computes neither (and skips the second pass when nothing else needs it). The other outputs are always produced. |
| run | optional | Object | `{id, dataset, partition}` — the identity written to every record. `id` defaults to the job name, `dataset` (what was profiled) to the module name, `partition` (which slice: a logical date, a version label) to null. |
| fields | optional | Object | Field filter: `{include: [...]}` or `{exclude: [...]}` with dot paths for nested fields. |
| values | optional | Enum | `show` (default) or `hide`. With `hide`, raw values are removed from every output and file: value records and categorical bins carry ranks (`#1`), segment groups become `group #1` (in `groups`, `bins` and `driftVs` alike), `top` is null, and the report has no sample rows and no value table of discrete numeric fields. Statistics (min, max, quantiles, histograms) are not values and stay. |
| keys | optional | Array<String\> | Fields to treat as identifiers. Adds the `keys` output: distinct count with bounds (Theta sketch), keyness (distinct / rows, capped at 1) and the number of null keys. |
| segments | optional | Array<String or Object\> | Fields to compare the dataset by. Shorthand `[category]` or longhand `[{field: category, topK: 30}]` (`topK`: max groups kept, at least 1, default 20, largest first). A field can be listed once. Each group is compared with the rows outside it. |
| time | optional | String or Object | Timestamp field for time buckets. Shorthand `created_at` or longhand `{field: created_at, granularity: day}` (`granularity`: `hour` / `day` / `week` / `month` / `year`, default `month`; UTC buckets, the most recent 60 kept). Each bucket is compared with the rows outside it. |
| mode | optional | Enum | `union` (default) treats multiple inputs as one dataset. `compare` (requires 2+ inputs) makes each input a group of the `inputs` axis, compared with the baseline input. |
| baseline | optional | String | With `mode: compare`, the reference input (default: the first input). |
| compare | optional | Array<Array<String\>\> | Declared comparable numeric field pairs, e.g. `[[list_price, sold_price]]`. Adds the `pairs` output: KS and PSI between the two fields, counted over the cells of the two pooled. |
| target | optional | String or Object | The binary outcome every other field is related to. Shorthand `sold_flag` for a bool field (positive = `true`), or longhand `{field: is_sold, positive: 1}` / `{field: status, positive: sold}` for numeric and string fields (`positive` is required there; any other non-null value is negative, null target rows are left out). Adds the `target` output and the `positives` / `rate` columns of `bins`. |
| bins | optional | Object | `{count: 10, edges: {<field>: [...]}}`. `count`: the number of equal-frequency bins the cells are merged into (2–100, default 10). `edges`: split points declared for a numeric field instead of the equal-frequency ones — bins are right-closed, the first and the last are open-ended. Declared edges are counted next to the quantile cells: `bins`, PSI and the information value of that field use them, KS and the group median keep the 100-cell grid. |
| drift | optional | Object | `{axis, exclude: [...]}`. `axis`: the axis whose groups feed the `drift` columns of the default output — `inputs`, `segments:<field>` or `time:<field>`; default the `inputs` axis when `mode: compare` declares one, otherwise none. `exclude`: fields left out of those columns (their statistics are still emitted), e.g. the time field the inputs were split on. The field of the drift axis itself is always left out: it differs from the rows outside its own group by construction. |
| accuracy | optional | Enum | `low`, `default`, `high`. Sketch sizes: KLL k = 100 / 200 / 800, CPC and Theta lgK = 10 / 12 / 14, frequent-items map 512 / 1024 / 4096. |
| associations | optional | Object | `{numeric: all}` (default) computes the Pearson correlation of all numeric pairs for the report; `{numeric: none}` disables it. The cost is quadratic in the number of numeric fields. |
| sample | optional | Object | `{enabled: true, k: 10000}`. VarOpt row sampling for the report's sample values and scatter plots. Disabled automatically with `values: hide`. |
| report | optional | Object | `{title: ...}`. Report title (defaults to the step name). |
| fanout | optional | Integer | Combine fan-out for hot-key distribution. Default: `16`. |

Batch (bounded) inputs in the global window only.

Not implemented yet, and rejected rather than ignored: `previous` (changes against a previous run),
`expectations`, `unnest`, `preset`, `bins.mode: sketch`, `bins.edges: previous`, `time.timezone`,
`time.groups`, a numeric `target`, and the `sketches` / `sample` outputs.

## Outputs

Referenced as `<name>` (the default) and `<name>.<output>`. Every record starts with the identity
columns `runId`, `dataset`, `partition`, `generatedAt` (one instant per run) and `formatVersion` (`2`).
The natural key of an output is (`dataset`, `partition`, the output's own key columns); of two record
sets with that key the later `generatedAt` is the current one.

A column `<stat>` with `<stat>_lo` / `<stat>_hi` next to it is an estimate with two-standard-deviation
bounds. A column without them is exact, with two exceptions: the quantile columns (their error is
`rankError`) and the statistics read off the cell grid (`ks`, `groups.p50`), which are exact functions
of exact counts resolved to one cell.

### Fields (`<name>`, the default output)

One record per profiled field.

| column | type | description |
|---|---|---|
| field, type, sourceType | STRING | dot path, profile type (`numeric` / `string` / `bool` / `timestamp` / `array`), schema type |
| rows, count, nulls, errors | INT64 | rows of the dataset; non-null readable values; nulls; values that could not be read |
| nullRate | FLOAT64 | nulls over the observed values |
| distinct, distinct_lo, distinct_hi | FLOAT64 | distinct count and its bounds |
| distinctExact | BOOLEAN | true when the field's value table is complete (the bounds equal the count) |
| min, max, sum, mean, stddev, skewness | FLOAT64 | numeric and array fields |
| minTime, maxTime | TIMESTAMP | timestamp fields |
| zeros, nans, infs | INT64 | numeric fields |
| p01, p05, p25, p50, p75, p95, p99, rankError | FLOAT64 | quantiles and the sketch's normalized rank error |
| empties, blanks | INT64 | string fields: empty strings; whitespace-only values (full-width space included) |
| lengthMin, lengthMax, lengthP50 | FLOAT64 | string length |
| top, topShare | STRING, FLOAT64 | the most frequent value (null with `values: hide`) and its share |
| trues, falses | INT64 | bool fields |
| nullLike, nullLikeRate | INT64, FLOAT64 | string fields: rows whose value is a null sentinel, and the null rate if they were counted as null |
| notable | ARRAY<STRING\> | notable codes, see below |
| association, associationKind | FLOAT64, STRING | with `target`: KS (numeric) or TVD (string / bool) between the two classes |
| drift, driftKind, driftVs | FLOAT64, STRING, STRING | with a drift axis: the largest KS / TVD over its groups, and the group |
| nullShift | FLOAT64 | with a drift axis: the largest null-rate difference over its groups |

Notable codes, in severity order:

| code | meaning |
|---|---|
| `all_null` | no non-null value |
| `null_like_only` | every non-null value is a null sentinel |
| `constant` | one distinct value |
| `high_null` | null rate above 0.5 |
| `null_like_value` | a null sentinel among the frequent values |
| `dominant_value` | one value holds more than 90% of the rows |
| `skewed` | absolute skewness above 2 |
| `unique_like` | distinct ≈ rows, for string and integer fields |

Null sentinels are the strings `null`, `None`, `NaN`, `N/A`, `NA`, `-` (case-insensitive, trimmed), the
empty string and whitespace-only values. They are reported, never converted: such a value still counts
as non-null.

### Groups (`<name>.groups`)

With `segments`, `time` or `mode: compare`. One record per axis × group × field.

`axis` (`segments:<field>` / `time:<field>` / `inputs`), `group`, `baseline` (true for the reference
input), `field`, `groupRows`, `count`, `nulls`, `nullRate`, `min`, `max`, `sum`, `mean`, `stddev`, `p50`,
and against the reference of the axis: `ks` (numeric fields), `tvd` (string / bool fields), `psi`,
`nullShift`, `noiseKs`, `noisePsi`.

The reference is the baseline input on the `inputs` axis (its own record has no drift columns) and the
rows outside the group on the `segments` and `time` axes. Three things are reported apart because they
fail apart: the distribution of the non-null values (`ks` / `tvd`, `psi`), the missingness (`nullShift`
= the group's null rate minus the reference's), and presence. `noiseKs` = 1.36·√(1/n₁ + 1/n₂) and
`noisePsi` = (bins − 1)(1/n₁ + 1/n₂) are the sizes the statistics reach between two random samples of
one distribution: reference columns for comparing samples, used in no ranking.

`sum` is exact: with `mode: compare`, the row counts and sums of two inputs reconcile as they are.

### Values (`<name>.values`)

One record per field × value, for string, bool and low-cardinality numeric fields: `field`, `value`
(`#<rank>` with `values: hide`), `rank`, `count`, `count_lo`, `count_hi`, `share`, `exact`.

A field with at most 1,000 distinct values, none longer than 256 characters, is tabulated exactly.
Above that the records are the frequent-items estimate (at most 50); when no value is frequent enough
to be certain of, they are the candidates with their bounds rather than nothing.

### Bins (`<name>.bins`)

One record per field × group × bin. `axis` and `group` are null for the whole dataset; the two target
classes appear as `axis: target`.

`field`, `axis`, `group`, `bin`, `lower`, `upper` (numeric fields; null at an open end of declared
edges), `value` (categorical bins; `(other)` and `(null)` are bins of their own), `count`, `share`,
`positives`, `rate`, `rate_lo`, `rate_hi` (whole-dataset bins with `target`: positives among the rows
with a non-null target, Wilson 95% interval), `edgesKind` (`quantile` / `declared` / `values`), `exact`.

### Target (`<name>.target`)

With `target`. One record per field other than the target: `field`, `type`, `count`, `ks`, `tvd`, `iv`
(information value over the field's bins, smoothed), `pointBiserial`, `meanPositive`, `meanNegative`,
`rateWhenNull` (positive rate among the field's null rows), `nullRows`.

A strong association is a number, not a verdict; the transform does not flag leaks.

### Pairs (`<name>.pairs`)

With `compare`. One record per declared pair: `a`, `b`, `countA`, `countB`, `ks`, `psi`, `noiseKs`,
`noisePsi`.

### Keys (`<name>.keys`)

With `keys`. One record per key: `kind` (`key`), `key`, `distinct`, `distinct_lo`, `distinct_hi`,
`keyness`, `nullKeys`. The distinct count is an estimate: it cannot tell a handful of duplicated rows
from none.

### Summary (`<name>.summary`)

One record per run: `rows`, `errorRows`, `fields`, `report` (the uri, null when none was written),
`reportBytes`, `payload`, `degradations`, one count per notable code (`allNull`, `nullLikeOnly`,
`constant`, `highNull`, `nullLikeValue`, `dominantValue`, `skewed`, `uniqueLike`), `targetRate`,
`targetWarning`. It is emitted after the files are written: a step that should run once the report
exists waits on this output.

## The report

`output.report` writes one HTML file rendered from the same aggregates as the records. It embeds two
JSON blocks, `profile-payload` (what is drawn) and `profile-manifest` (run metadata, expanded parameters,
sketch parameters, degradations); `output.payload` writes both as one JSON document
(`{formatVersion, payload, manifest}`). Charts use ECharts from a CDN (version-pinned with SRI); without
network access the numbers remain readable.

When the payload exceeds the size limit the report sheds, in this order, sample rows → the correlation
matrix (down to its strongest pairs) → histogram resolution → comparison groups → comparison
resolution, and records each step in `summary.degradations`. The record outputs never degrade.

The report may contain raw data values. **Treat it with the same sensitivity as the source data**, or
set `values: hide`.

### Failure handling

Each input record is reduced to its profiled fields once. A value the profile type cannot interpret is
counted in the field's `errors`. A field whose conversion throws also counts the row in `errorRows` and
routes the record to the module's failure handling: with `failFast: true` the job fails naming the
field, otherwise the record goes to `failureSinks` / `outputFailure`.

## Examples

### Example 1: A history table of field statistics

```yaml
sources:
  - name: items
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.items`"

transforms:
  - name: itemsProfile
    module: profile
    inputs: [items]
    parameters:
      run:
        dataset: items
        partition: ${args.date}

sinks:
  - name: itemsProfileFields
    module: bigquery
    inputs: [itemsProfile]
    parameters:
      table: myproject.quality.profile_fields
      writeDisposition: WRITE_APPEND
```

"Since which partition is this field entirely null" is then one query over the table:

```sql
SELECT field, MIN(`partition`) AS since
FROM `myproject.quality.profile_fields`
WHERE dataset = 'items' AND 'all_null' IN UNNEST(notable)
GROUP BY field
```

### Example 2: The report alone

A config with only `sources` and the transform is valid: the report is written and nothing needs to
consume the records.

```yaml
transforms:
  - name: itemsProfile
    module: profile
    inputs: [items]
    parameters:
      output: gs://mybucket/reports/items.html
      keys: [item_id]
      segments: [category]
      time:
        field: created_at
        granularity: month
      report:
        title: Items dataset observation
```

### Example 3: Before against after

Two inputs as comparison groups. `sum` and `count` reconcile the two; `ks` / `tvd`, `psi` and
`nullShift` say what moved.

```yaml
transforms:
  - name: migrationCheck
    module: profile
    inputs: [itemsBefore, itemsAfter]
    parameters:
      mode: compare
      baseline: itemsBefore
      output: gs://mybucket/reports/items-migration.html

sinks:
  - name: migrationGroups
    module: bigquery
    inputs: [migrationCheck.groups]
    parameters:
      table: myproject.quality.profile_groups
      writeDisposition: WRITE_APPEND
```

### Example 4: Relating every field to a binary outcome, with declared price bands

```yaml
transforms:
  - name: itemsProfile
    module: profile
    inputs: [items]
    parameters:
      output: gs://mybucket/reports/items.html
      target: sold_flag                 # bool field; for INT64 flags: {field: is_sold, positive: 1}
      compare:
        - [list_price, sold_price]
      bins:
        edges:
          list_price: [1000, 3000, 10000, 30000]
```

### Example 5: Hiding raw values, statistics only

```yaml
transforms:
  - name: itemsProfile
    module: profile
    inputs: [items]
    parameters:
      output: gs://mybucket/reports/items.html
      values: hide
      outputs: []
      associations:
        numeric: none
```
