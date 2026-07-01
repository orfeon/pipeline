---
type: Transform Module
title: Aggregation Transform Module
description: Performs aggregation processing on multiple inputs with grouping, filtering, and field selection. Supports 12 aggregation functions (count, sum, avg, max, min, last, first, arg_max, arg_min, std, simple_regression, array_agg) with per-record conditions, expression-based fields, weighted calculations, and configurable output limiting. Works consistently in both batch and streaming modes.
tags: [transform, aggregation, batch, streaming, groupby]
timestamp: 2026-06-23T00:00:00Z
---

# Aggregation Transform Module

Transform Module for performing aggregation processing on multiple inputs. Records are grouped by specified fields and aggregated using one or more aggregation functions.

The same aggregate processing works consistently in both batch and streaming modes. Compared to the `beamsql` module, the `aggregation` module specializes in aggregate processing with simpler configuration but better performance. It also provides functions not available in `beamsql`, such as `last`, `first`, `arg_max`, `arg_min`, `std`, and `simple_regression`.

Supports:

- **Multiple inputs** - Aggregate data from multiple input sources in a single step.
- **Grouping** - Group records by one or more fields before aggregation.
- **12 aggregation functions** - count, sum, avg, max, min, last, first, arg_max, arg_min, std, simple_regression, array_agg.
- **Per-record conditions** - Each aggregation field can specify a filter condition to include/exclude records.
- **Expression-based fields** - Use mathematical expressions (exp4j) instead of direct field references.
- **Weighted calculations** - avg, std, and simple_regression support weighted computations.
- **Post-aggregation processing** - Apply filter and select to aggregation results.
- **Output limiting** - Limit the number of output records per group key.
- **Streaming support** - Works with windowing strategies and triggers for streaming pipelines.

## Transform module common parameters

| parameter  | optional | type                                        | description                                                           |
|------------|----------|---------------------------------------------|-----------------------------------------------------------------------|
| name       | required | String                                      | Step name. specified to be unique in config file.                     |
| module     | required | String                                      | Specified `aggregation`                                               |
| inputs     | required | Array<String\>                              | Specify the names of the steps to be used as input for aggregation.   |
| wait       | optional | Array<String\>                              | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md)           | Windowing strategy for streaming execution.                           |
| parameters | required | Map<String,Object\>                         | Specify the following individual parameters                          |

## Aggregation transform module parameters

| parameter      | optional | type                                                    | description                                                                                                                                                                                 |
|----------------|----------|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| aggregations   | required | Array<[Aggregation](#aggregation-parameters)\>          | Aggregation definitions for each input. Each definition specifies one input source and the aggregation fields to compute.                                                                   |
| groupFields    | optional | Array<String\>                                          | Field names used to group the data before aggregation. All inputs must contain these fields. If not specified, all records are aggregated as a single group.                                 |
| filter         | optional | [Filter](../common/filter.md)                           | Filter condition applied to the aggregation results. Records that do not match are discarded.                                                                                               |
| select         | optional | Array<[Select](../common/select.md)\>                   | Field selection and transformation applied to the aggregation results. When specified, only the selected fields are included in the output schema.                                          |
| flattenField   | optional | String                                                  | Array field in the aggregation results to flatten.                                                                                                                                          |
| limit          | optional | [Limit](#limit-parameters)                              | Limit the number of output records per group key.                                                                                                                                           |
| fanout         | optional | Integer                                                 | Number of intermediate combine nodes for [hot key fanout](https://beam.apache.org/documentation/transforms/java/aggregation/#combineperkey). Reduces load on the final global combine step. |
| outputEmpty    | optional | Boolean                                                 | If `true`, outputs a record even when no data existed for a group within the window. Default: `false`.                                                                                      |
| outputPaneInfo | optional | Boolean                                                 | If `true`, adds pane information fields to the output schema for streaming processing with triggers. Default: `false`.                                                                      |

## Aggregation parameters

Each element in the `aggregations` array defines aggregation processing for one input source.

| parameter | optional | type                                                  | description                                                                                     |
|-----------|----------|-------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| input     | required | String                                                | Name of the input step to aggregate. Must match one of the step names specified in `inputs`.    |
| fields    | required | Array<[AggregationField](#aggregation-field-parameters)\> | Aggregation field definitions. Each field produces one output field (or nested fields).      |

## Aggregation field parameters

### Common parameters

All aggregation functions share these common parameters.

| parameter | optional | type    | description                                                                                                                                                                                |
|-----------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String  | Output field name for the aggregation result. Must be unique within the aggregation definition.                                                                                            |
| op        | required | Enum    | Aggregation function. Values: `count`, `sum`, `avg`, `max`, `min`, `last`, `first`, `arg_max`, `arg_min`, `std`, `simple_regression`, `array_agg`. Can also be specified as `func`.        |
| field     | selective | String | Input field name to aggregate. Required for most functions (except `count`). Mutually exclusive with `expression` for functions that support expressions.                                  |
| expression| optional | String  | Mathematical expression (exp4j syntax) to compute the value to aggregate. Available for `sum`, `avg`, `max`, `min`, `std`, `simple_regression`. Uses input field names as variables.       |
| condition | optional | Filter  | Filter condition applied per record. Only records matching the condition are included in the aggregation. Uses the same [Filter](../common/filter.md) syntax.                              |
| ignore    | optional | Boolean | If `true`, this aggregation field is computed internally but excluded from the output schema. Default: `false`.                                                                            |

### Function-specific parameters

Each aggregation function may accept additional parameters beyond the common ones.

#### count

Counts the number of records in each group. Does not require `field` or `expression`.

| output type | description                |
|-------------|----------------------------|
| INT64       | Number of records counted. |

#### sum

Computes the sum of a numeric field or expression.

| parameter  | optional | type   | description                                               |
|------------|----------|--------|-----------------------------------------------------------|
| field      | selective | String | Input field name. Mutually exclusive with `expression`.  |
| expression | selective | String | Mathematical expression (exp4j syntax).                  |

| output type | description                                                              |
|-------------|--------------------------------------------------------------------------|
| Same as input field type, or FLOAT64 for expressions | Sum of values. |

#### avg

Computes the (optionally weighted) average of a numeric field or expression.

| parameter        | optional | type   | description                                                                  |
|------------------|----------|--------|------------------------------------------------------------------------------|
| field            | selective | String | Input field name. Mutually exclusive with `expression`.                     |
| expression       | selective | String | Mathematical expression (exp4j syntax).                                     |
| weightField      | optional | String | Field name for weights. When specified, computes weighted average.           |
| weightExpression | optional | String | Expression for weights. Mutually exclusive with `weightField`.               |

| output type | description                    |
|-------------|--------------------------------|
| FLOAT64     | Average (or weighted average). |

#### max / min

Computes the maximum or minimum value of a numeric field or expression.

| parameter  | optional | type   | description                                               |
|------------|----------|--------|-----------------------------------------------------------|
| field      | selective | String | Input field name. Mutually exclusive with `expression`.  |
| expression | selective | String | Mathematical expression (exp4j syntax).                  |

| output type | description                                                            |
|-------------|------------------------------------------------------------------------|
| Same as input field type, or FLOAT64 for expressions | Maximum or minimum value. |

#### last / first

Returns the value of the last (most recent) or first (earliest) record in each group based on event timestamp.

| parameter | optional   | type           | description                                                                                                              |
|-----------|------------|----------------|--------------------------------------------------------------------------------------------------------------------------|
| field     | selective  | String         | Single input field name to retrieve. Mutually exclusive with `fields`. Output type matches the field type.               |
| fields    | selective  | Array<String\> | Multiple input field names to retrieve. Output is a nested element containing all specified fields.                       |

| output type | description                                                                        |
|-------------|------------------------------------------------------------------------------------|
| Same as input field type (single field) or Element (multiple fields) | Last or first value. |

#### arg_max / arg_min

Returns the field value(s) from the record that has the maximum or minimum value of a comparing field or expression.

| parameter           | optional   | type           | description                                                                                                                 |
|---------------------|------------|----------------|-----------------------------------------------------------------------------------------------------------------------------|
| field               | selective  | String         | Single input field name to retrieve. Mutually exclusive with `fields`. Output type matches the field type.                  |
| fields              | selective  | Array<String\> | Multiple input field names to retrieve. Output is a nested element containing all specified fields.                          |
| comparingField      | selective  | String         | Field name used for comparison (max/min). Mutually exclusive with `comparingExpression`. Either this or `comparingExpression` is required. |
| comparingExpression | selective  | String         | Expression used for comparison (max/min). Mutually exclusive with `comparingField`.                                          |

| output type | description                                                                        |
|-------------|------------------------------------------------------------------------------------|
| Same as input field type (single field) or Element (multiple fields) | Value(s) from the record with the max/min comparing value. |

#### std

Computes the standard deviation (or variance) of a numeric field or expression. Supports weighted computation.

| parameter        | optional | type    | description                                                                                                     |
|------------------|----------|---------|-----------------------------------------------------------------------------------------------------------------|
| field            | selective | String | Input field name. Mutually exclusive with `expression`.                                                        |
| expression       | selective | String | Mathematical expression (exp4j syntax).                                                                        |
| ddof             | optional | Integer | Delta Degrees of Freedom. `0` for population std, `1` for sample std. Default: `1`.                            |
| outputVar        | optional | Boolean | If `true`, outputs variance instead of standard deviation. Default: `false`.                                    |
| weightField      | optional | String  | Field name for weights. When specified, computes weighted standard deviation.                                   |
| weightExpression | optional | String  | Expression for weights. Mutually exclusive with `weightField`.                                                  |

| output type | description                                   |
|-------------|-----------------------------------------------|
| FLOAT64     | Standard deviation (or variance if outputVar). |

#### simple_regression

Performs simple linear regression (y = a + bx) and outputs regression statistics.

| parameter        | optional | type    | description                                                                                             |
|------------------|----------|---------|---------------------------------------------------------------------------------------------------------|
| field            | selective | String | Input field name for the dependent variable (y). Mutually exclusive with `expression`.                 |
| expression       | selective | String | Expression for the dependent variable (y).                                                              |
| xField           | optional | String  | Input field name for the independent variable (x). If not specified, a sequential counter is used.      |
| hasIntercept     | optional | Boolean | If `true`, the regression model includes an intercept term. Default: `true`.                            |
| weightField      | optional | String  | Field name for weights.                                                                                 |
| weightExpression | optional | String  | Expression for weights. Mutually exclusive with `weightField`.                                          |

| output type | description                                                                              |
|-------------|------------------------------------------------------------------------------------------|
| Element     | Nested element with fields: `Slope` (FLOAT64), `Intercept` (FLOAT64), `RMSE` (FLOAT64), `N` (INT64). |

#### array_agg

Collects all values of a field into an array. Requires `field` or `fields`.

| parameter | optional   | type           | description                                                                                                                          |
|-----------|------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------|
| field     | selective  | String         | Single input field name to collect. Output is an array of the field type.                                                            |
| fields    | selective  | Array<String\> | Multiple input field names to collect. Output is an array of nested elements containing all specified fields.                         |
| order     | optional   | Enum           | Sort order for the collected array. Values: `ascending`, `descending`, `none`. Default: `none`.                                      |

| output type | description                                                                   |
|-------------|-------------------------------------------------------------------------------|
| Array       | Array of field values (single field) or array of elements (multiple fields).  |

## Limit parameters

Limit configuration restricts the number of output records per group key.

| parameter    | optional | type      | description                                                                                                                  |
|--------------|----------|-----------|------------------------------------------------------------------------------------------------------------------------------|
| count        | optional | Integer   | Maximum number of records to output per group key.                                                                           |
| outputStartAt| optional | Timestamp | Start outputting records only after this timestamp. Records before this time are discarded. Format: ISO-8601 timestamp.       |

## Output schema

The output schema is constructed as follows:

1. **Group fields** - Fields specified in `groupFields` are included first, preserving their original types from the input schema.
2. **Aggregation result fields** - Each aggregation field (not marked with `ignore: true`) is added with the name specified in `name` and the type determined by the aggregation function.
3. **Timestamp** - An implicit timestamp field representing the event time of the aggregation result.

If the `select` parameter is specified, only the selected/transformed fields are included in the output instead.

If `outputPaneInfo` is `true`, additional pane information fields are added to the output schema.

## Examples

### Example 1: Basic count and sum

Count records and sum a numeric field, grouped by a category field.

```yaml
sources:
  - name: orders
    module: bigquery
    parameters:
      query: "SELECT user_id, category, amount FROM `myproject.mydataset.orders`"

transforms:
  - name: summary
    module: aggregation
    inputs:
      - orders
    parameters:
      groupFields:
        - category
      aggregations:
        - input: orders
          fields:
            - name: order_count
              op: count
            - name: total_amount
              op: sum
              field: amount

sinks:
  - name: output
    module: bigquery
    inputs:
      - summary
    parameters:
      table: "myproject.mydataset.order_summary"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
```

### Example 2: Average and standard deviation

Compute average and standard deviation of scores grouped by subject.

```yaml
transforms:
  - name: stats
    module: aggregation
    inputs:
      - scores
    parameters:
      groupFields:
        - subject
      aggregations:
        - input: scores
          fields:
            - name: avg_score
              op: avg
              field: score
            - name: std_score
              op: std
              field: score
              ddof: 1
            - name: student_count
              op: count
```

### Example 3: Max, min, and last

Find the maximum, minimum, and most recent values grouped by sensor ID.

```yaml
transforms:
  - name: sensor_stats
    module: aggregation
    inputs:
      - sensor_data
    parameters:
      groupFields:
        - sensor_id
      aggregations:
        - input: sensor_data
          fields:
            - name: max_temperature
              op: max
              field: temperature
            - name: min_temperature
              op: min
              field: temperature
            - name: latest_reading
              op: last
              field: temperature
```

### Example 4: arg_max to get the record with the highest value

Retrieve the user name from the record with the highest score in each group.

```yaml
transforms:
  - name: top_users
    module: aggregation
    inputs:
      - user_scores
    parameters:
      groupFields:
        - department
      aggregations:
        - input: user_scores
          fields:
            - name: top_user
              op: arg_max
              fields:
                - user_name
                - email
              comparingField: score
```

### Example 5: Weighted average

Compute a weighted average of prices weighted by quantity.

```yaml
transforms:
  - name: weighted_price
    module: aggregation
    inputs:
      - transactions
    parameters:
      groupFields:
        - product_id
      aggregations:
        - input: transactions
          fields:
            - name: weighted_avg_price
              op: avg
              field: price
              weightField: quantity
            - name: total_quantity
              op: sum
              field: quantity
```

### Example 6: Conditional aggregation

Count only records matching a condition.

```yaml
transforms:
  - name: conditional_counts
    module: aggregation
    inputs:
      - events
    parameters:
      groupFields:
        - user_id
      aggregations:
        - input: events
          fields:
            - name: total_events
              op: count
            - name: purchase_count
              op: count
              condition:
                key: event_type
                op: "="
                value: "purchase"
            - name: purchase_amount
              op: sum
              field: amount
              condition:
                key: event_type
                op: "="
                value: "purchase"
```

### Example 7: Expression-based aggregation

Use mathematical expressions instead of direct field references.

```yaml
transforms:
  - name: calculated
    module: aggregation
    inputs:
      - sales
    parameters:
      groupFields:
        - region
      aggregations:
        - input: sales
          fields:
            - name: total_revenue
              op: sum
              expression: "price * quantity"
            - name: avg_unit_price
              op: avg
              expression: "price / quantity"
```

### Example 8: Simple linear regression

Perform simple linear regression to analyze the relationship between variables.

```yaml
transforms:
  - name: regression_analysis
    module: aggregation
    inputs:
      - measurements
    parameters:
      groupFields:
        - category
      aggregations:
        - input: measurements
          fields:
            - name: trend
              op: simple_regression
              field: y_value
              xField: x_value
              hasIntercept: true
```

The output `trend` field is a nested element with sub-fields: `Slope`, `Intercept`, `RMSE`, `N`.

### Example 9: array_agg to collect values into an array

Collect all tags for each group into an array.

```yaml
transforms:
  - name: collected
    module: aggregation
    inputs:
      - items
    parameters:
      groupFields:
        - category
      aggregations:
        - input: items
          fields:
            - name: all_tags
              op: array_agg
              field: tag
              order: ascending
            - name: item_count
              op: count
```

### Example 10: Multiple inputs aggregation

Aggregate data from multiple input sources.

```yaml
sources:
  - name: online_orders
    module: bigquery
    parameters:
      query: "SELECT user_id, amount FROM `myproject.mydataset.online_orders`"

  - name: store_orders
    module: bigquery
    parameters:
      query: "SELECT user_id, amount FROM `myproject.mydataset.store_orders`"

transforms:
  - name: combined_stats
    module: aggregation
    inputs:
      - online_orders
      - store_orders
    parameters:
      groupFields:
        - user_id
      aggregations:
        - input: online_orders
          fields:
            - name: online_total
              op: sum
              field: amount
            - name: online_count
              op: count
        - input: store_orders
          fields:
            - name: store_total
              op: sum
              field: amount
            - name: store_count
              op: count
```

### Example 11: Aggregation with post-processing filter and select

Apply filter and field selection to aggregation results.

```yaml
transforms:
  - name: filtered_stats
    module: aggregation
    inputs:
      - orders
    parameters:
      groupFields:
        - category
      aggregations:
        - input: orders
          fields:
            - name: order_count
              op: count
            - name: total_amount
              op: sum
              field: amount
            - name: avg_amount
              op: avg
              field: amount
      filter:
        key: order_count
        op: ">="
        value: 10
      select:
        - name: category
          field: category
        - name: order_count
          field: order_count
        - name: total_amount
          field: total_amount
        - name: avg_amount
          field: avg_amount
          type: double
```

### Example 12: Streaming aggregation with windowing

Aggregate streaming data using a fixed window.

```yaml
sources:
  - name: events
    module: pubsub
    schema:
      fields:
        - name: user_id
          type: string
        - name: event_type
          type: string
        - name: amount
          type: double
        - name: event_time
          type: timestamp
    parameters:
      subscription: "projects/myproject/subscriptions/events-sub"
      format: json
    timestampAttribute: event_time

transforms:
  - name: windowed_stats
    module: aggregation
    inputs:
      - events
    strategy:
      window:
        type: fixed
        size: 60
        unit: minute
      trigger:
        type: afterWatermark
        earlyFiringPeriod: 60
      accumulationMode: accumulating
    parameters:
      groupFields:
        - user_id
      aggregations:
        - input: events
          fields:
            - name: event_count
              op: count
            - name: total_amount
              op: sum
              field: amount
            - name: last_event_type
              op: last
              field: event_type
      outputPaneInfo: true

sinks:
  - name: output
    module: bigquery
    inputs:
      - windowed_stats
    parameters:
      table: "myproject.mydataset.hourly_user_stats"
      method: STORAGE_WRITE_API
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
      triggeringFrequencySecond: 60
```

### Example 13: Aggregation with limit and fanout

Limit output records and use fanout for hot key distribution.

```yaml
transforms:
  - name: top_products
    module: aggregation
    inputs:
      - sales
    parameters:
      groupFields:
        - region
      aggregations:
        - input: sales
          fields:
            - name: total_sales
              op: sum
              field: amount
            - name: sale_count
              op: count
      limit:
        count: 100
      fanout: 128
```
