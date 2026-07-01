---
type: Sink Module
title: Debug Sink Module
description: Outputs input data to logs for debugging and inspection. Each record is logged as JSON with window and pane metadata. In DirectRunner mode, results are written as Avro files to a work directory.
tags: [sink, debug, batch, streaming, log, test]
timestamp: 2026-06-21T14:30:00Z
---

# Debug Sink Module

Sink Module for outputting input data to logs for debugging and inspection purposes. Each input record is converted to JSON and logged at the specified level along with Apache Beam window and pane metadata.

When running with DirectRunner (local mode), the module instead writes results as Avro files to a work directory, which can be read back by the Pipeline Server UI.

## Sink module common parameters

| parameter | optional | type                | description                                                           |
|-----------|----------|---------------------|-----------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                     |
| module    | required | String              | Specified `debug`                                                     |
| inputs    | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Debug sink module parameters

| parameter   | optional | type   | description                                                                                                                                                         |
|-------------|----------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| logLevel    | optional | Enum   | Log level for output. Values: `trace`, `debug`, `info`, `warn`, `error`. Default: `info`.                                                                           |
| logTemplate | optional | String | FreeMarker template string for custom log formatting. If not specified, the full JSON output including `data`, `pane`, and `window` metadata is logged. See [Log template](#log-template) for available variables. |

### Log output format

When `logTemplate` is **not** specified, each record is logged as a JSON object with the following structure:

```json
{
  "data": { ... },
  "pane": {
    "timing": "ON_TIME",
    "isFirst": true,
    "isLast": true,
    "isUnknown": false,
    "nonSpeculativeIndex": 0,
    "index": -1
  },
  "window": {
    "type": "global"
  }
}
```

- `data` - The input record converted to JSON.
- `pane` - Apache Beam pane information for the element (useful for streaming windowing analysis).
- `window` - Window information. For `GlobalWindow`, only `type` is present. For `IntervalWindow`, `start`, `end`, and `maxTimestamp` are also included.

### Log template

When `logTemplate` is specified, the output is formatted using the FreeMarker template engine. The following variables are available in the template:

| variable           | type   | description                                                  |
|--------------------|--------|--------------------------------------------------------------|
| data               | Object | The input record as a JSON object.                           |
| timestamp          | String | The element's event timestamp.                               |
| paneTiming         | String | Pane timing (`EARLY`, `ON_TIME`, `LATE`, `UNKNOWN`).         |
| paneIsFirst        | String | Whether this is the first pane (`true`/`false`).             |
| paneIsLast         | String | Whether this is the last pane (`true`/`false`).              |
| paneIndex          | Long   | Pane index. `-1` if both first and last.                     |
| windowMaxTimestamp | String | Maximum timestamp of the window.                             |
| windowStart        | String | Start of the interval window (empty for global window).      |
| windowEnd          | String | End of the interval window (empty for global window).        |

## Execution modes

### Non-DirectRunner (Dataflow, Flink, Spark, etc.)

Each input record is:
1. Converted to JSON via `ElementToJsonConverter`.
2. Formatted with pane/window metadata (or via `logTemplate`).
3. Printed to stdout via `System.out.println`.
4. Logged at the specified `logLevel`.
5. Input schema information (field definitions, Avro schema, Row schema) is also logged at startup.

### DirectRunner (local mode)

When running locally with DirectRunner, the module writes output to Avro files instead of logging. The files are written to `{workDir}/outputs/{sinkName}/` and can be read back by the Pipeline Server for display. A schema file (`schema.avro`) is also written alongside the data.

## Examples

### Example 1: Basic debug output

Log all input records at `info` level.

```yaml
sources:
  - name: input
    module: create
    parameters:
      type: element
      elements:
        - stringField: stringValue1
          longField: 100
        - stringField: stringValue2
          longField: 200
    schema:
      fields:
        - name: stringField
          type: string
        - name: longField
          type: int64

sinks:
  - name: debug
    module: debug
    inputs:
      - input
    parameters:
      logLevel: info
```

### Example 2: Debug with warn level

Use `warn` level to make debug output stand out in noisy logs.

```yaml
sinks:
  - name: debug_output
    module: debug
    inputs:
      - my_transform
    parameters:
      logLevel: warn
```

### Example 3: Custom log template

Use a FreeMarker template to format log output with only the fields you care about.

```yaml
sinks:
  - name: debug_output
    module: debug
    inputs:
      - my_transform
    parameters:
      logLevel: info
      logTemplate: "[${timestamp}] data=${data}"
```

### Example 4: Template with window information

Include window timing details for streaming pipeline debugging.

```yaml
sinks:
  - name: debug_output
    module: debug
    inputs:
      - my_transform
    parameters:
      logLevel: debug
      logTemplate: "window=[${windowStart} - ${windowEnd}] pane=${paneTiming} index=${paneIndex} data=${data}"
```

### Example 5: Multiple inputs

The debug sink can accept multiple inputs and will log each one separately with the input tag name.

```yaml
sinks:
  - name: debug_all
    module: debug
    inputs:
      - source_a
      - source_b
    parameters:
      logLevel: info
```

Each record is logged with the format `{sinkName}.{inputTag}: {json}`, allowing you to distinguish which input produced each log line.
