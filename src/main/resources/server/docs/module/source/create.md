---
type: Source Module
title: Create Source Module
description: Generates data directly from configuration without external data sources. Supports listing explicit elements or generating sequences from a range specification.
tags: [source, create, batch, streaming, generate, test, dummy]
timestamp: 2026-06-21T14:30:00Z
---

# Create Source Module

Source Module for generating data directly from configuration. You can either list explicit elements or generate a sequence of values from a range (`from`/`to`). Useful for testing, prototyping, and providing static lookup data.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                  |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                            |
| module             | required | String              | Specified `create`                                                                                                           |
| schema             | optional | [Schema](../common/schema.md) | Schema of the data to be read. Required when `type` is `element`.                                                            |
| timestampAttribute | optional | String              | If you want to use the value of a field as the event time, specify the name of the field. (The field must be Timestamp type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                  |

## Create source module parameters

`create` module can either specify data directly in `elements`, or generate a sequence of data by specifying a range with `from` and `to`.

You can also process the generated data by specifying a `select` parameter.

| parameter    | optional           | type                                       | description                                                                                                                                                                                                                                            |
|--------------|--------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type         | required           | Enum                                       | Type of data in `elements` or data to be generated from the range. Supported values: `string`, `int`(`int32`), `long`(`int64`), `float`(`float32`), `double`(`float64`), `decimal`, `date`, `time`, `timestamp`, `element`. See [Type values](#type-values) for details. |
| elements     | selective required | Array<?\>                                  | List of data to output. Either `elements` or `from` must be specified.                                                                                                                                                                                  |
| from         | selective required | String                                     | Start value of the range. Either `elements` or `from` must be specified. Supports [template expressions](#template-support).                                                                                                                             |
| to           | optional           | String                                     | End value of the range (inclusive). Required when `rate` is not specified. Supports [template expressions](#template-support).                                                                                                                            |
| interval     | optional           | Integer                                    | Step size for generating values in the range. Default: `1`.                                                                                                                                                                                              |
| intervalUnit | optional           | Enum                                       | Unit for `interval` when `type` is `date`, `time`, or `timestamp`. Values: `second`, `minute`, `hour`, `day`, `week`, `month`, `year`. Default: `day` for `date`, `minute` for `time`/`timestamp`.                                                      |
| rate         | optional           | Long                                       | Number of records to generate per time unit in streaming mode. Must be greater than 0. When set, the module runs as a streaming source using `GenerateSequence`.                                                                                         |
| rateUnit     | optional           | Enum                                       | Unit of time for `rate`. Values: `second`, `minute`, `hour`, `day`, `week`, `month`, `year`. Default: `second`.                                                                                                                                          |
| select       | optional           | Array<[SelectField](../common/select.md)\> | Field definitions for refining, renaming, or processing the generated data.                                                                                                                                                                              |
| flatten      | optional           | String                                     | Field name of an array-typed field to unnest/flatten into multiple rows.                                                                                                                                                                                 |
| splitSize    | optional           | Integer                                    | Number of partitions for parallel processing in batch mode. Default: `10`.                                                                                                                                                                               |

### Type values

| value                       | description                                                                            |
|-----------------------------|----------------------------------------------------------------------------------------|
| `string`, `char`            | String values. For range mode, numeric strings are generated.                          |
| `int`, `int32`, `integer`   | 32-bit integer values.                                                                 |
| `long`, `int64`             | 64-bit integer values.                                                                 |
| `float`, `float32`          | 32-bit floating point values.                                                          |
| `double`, `float64`         | 64-bit floating point values.                                                          |
| `decimal`, `numeric`        | BigDecimal values.                                                                     |
| `date`                      | Date values. `from`/`to` accept date strings (e.g. `2024-01-01`). Default `intervalUnit` is `day`.    |
| `time`                      | Time values. `from`/`to` accept time strings (e.g. `09:00:00`). Default `intervalUnit` is `minute`.   |
| `timestamp`                 | Timestamp values. `from`/`to` accept ISO-8601 strings (e.g. `2024-01-01T00:00:00Z`). Default `intervalUnit` is `minute`. |
| `element`, `row`, `struct`, `record` | Structured data with schema. Requires `schema` to be defined at the source level. Each element in `elements` is a JSON object matching the schema. |

### Output schema

When `type` is **not** `element` and no `select` parameter is specified, the output has the following schema:

| field     | type                  | description                          |
|-----------|-----------------------|--------------------------------------|
| sequence  | INT64                 | 0-based sequence number of the row.  |
| value     | (specified by `type`) | The generated or listed value.       |
| timestamp | TIMESTAMP             | Processing timestamp.                |

When `type` is `element`, the output schema matches the `schema` defined at the source level.

If `select` is specified, the output schema is determined by the select field definitions.

If `flatten` is specified, the specified array field is unnested and the schema is adjusted accordingly.

### Template support

The `from` and `to` parameters support FreeMarker template expressions, allowing dynamic value generation at runtime. Template functions available through `TemplateUtil.setFunctions` can be used.

Example: `from: "${utils.datetime.today()}"`

## Execution modes

### Batch mode (default)

When `rate` is not specified, the module generates all elements at once using a Splittable DoFn. The work is divided into `splitSize` partitions for parallel processing.

- If `elements` is specified: generates exactly those elements.
- If `from`/`to` is specified: generates the sequence from `from` to `to` (inclusive) with the given `interval`.

### Streaming mode

When `rate` is specified (value > 0), the module generates data continuously at the specified rate using `GenerateSequence`.

- If `to` is specified: stops after generating the sequence up to `to`.
- If `to` is not specified: generates data indefinitely.

## Examples

### Example 1: Generate a range of integers

Generate integer values from 1 to 100.

```yaml
sources:
  - name: numbers
    module: create
    parameters:
      type: int64
      from: "1"
      to: "100"
```

Output: 100 rows, each with `sequence`, `value` (1 to 100), and `timestamp` fields.

### Example 2: List explicit integer elements

```yaml
sources:
  - name: ids
    module: create
    parameters:
      type: int64
      elements:
        - 1
        - 2
        - 3
```

### Example 3: Generate a date range

Generate daily dates for January 2024.

```yaml
sources:
  - name: dates
    module: create
    parameters:
      type: date
      from: "2024-01-01"
      to: "2024-01-31"
      interval: 1
      intervalUnit: day
```

### Example 4: Generate timestamps at hourly intervals

```yaml
sources:
  - name: hourly
    module: create
    parameters:
      type: timestamp
      from: "2024-01-01T00:00:00Z"
      to: "2024-01-01T23:00:00Z"
      interval: 1
      intervalUnit: hour
```

### Example 5: List structured elements with schema

Specify `type: element` with a `schema` to create structured rows.

```yaml
sources:
  - name: users
    module: create
    parameters:
      type: element
      elements:
        - user_id: "u001"
          name: "Alice"
          age: 30
          active: true
          created_at: "2024-01-15T10:00:00Z"
        - user_id: "u002"
          name: "Bob"
          age: 25
          active: false
          created_at: "2024-02-20T14:30:00Z"
        - user_id: "u003"
          name: "Charlie"
          age: 35
          active: true
          created_at: "2024-03-10T09:00:00Z"
    schema:
      fields:
        - name: user_id
          type: string
        - name: name
          type: string
        - name: age
          type: int32
        - name: active
          type: boolean
        - name: created_at
          type: timestamp
    timestampAttribute: created_at
```

### Example 6: Range with select for field transformation

Generate integers and use `select` to compute derived fields.

```yaml
sources:
  - name: computed
    module: create
    parameters:
      type: int64
      from: "1"
      to: "100"
      select:
        - name: id
          field: value
        - name: group
          expression: "value % 10"
          type: int64
        - name: hash_value
          func: hash
          field: group
```

### Example 7: Streaming mode with rate

Generate data continuously at 10 records per second.

```yaml
sources:
  - name: stream
    module: create
    parameters:
      type: int64
      from: "0"
      rate: 10
      rateUnit: second
```

### Example 8: Streaming mode with rate and upper bound

Generate data at 5 records per second, stopping after 1000 records.

```yaml
sources:
  - name: bounded_stream
    module: create
    parameters:
      type: int64
      from: "0"
      to: "999"
      rate: 5
      rateUnit: second
```
