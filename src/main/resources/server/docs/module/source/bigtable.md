---
type: Source Module
title: Bigtable Source Module
description: Reads data from Google Cloud Bigtable. Supports batch table scans with row filters, key ranges, and per-column-family/qualifier schema mapping (row or cell output), plus change streams for CDC in streaming pipelines.
tags: [source, bigtable, batch, streaming, gcp, cdc]
timestamp: 2026-07-05T00:00:00Z
---

# Bigtable Source Module

Source Module for reading data from [Google Cloud Bigtable](https://cloud.google.com/bigtable/docs). Supports two execution modes:

- **Batch mode** (default): Scan a table with optional row filter and key ranges. Cells are mapped to output fields via `columns` definitions, with two output shapes:
  - `outputType: row` — one output element per Bigtable row, with fields defined by `columns` (plus optional row key / timestamp fields).
  - `outputType: cell` — one output element per cell with a fixed schema (`rowKey`, `family`, `qualifier`, `value`, `timestamp`).
- **Change stream mode** (`mode: changeDataCapture`): Read change data capture (CDC) events from a Bigtable [change stream](https://cloud.google.com/bigtable/docs/change-streams). The pipeline must run in streaming mode.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                                              |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                                        |
| module             | required | String              | Specified `bigtable`                                                                                                                                     |
| mode               | optional | Enum                | Execution mode. Values: `batch` (default), `changeDataCapture`. Other modes are not supported by this module. For change streams, set `changeDataCapture` explicitly. |
| timestampAttribute | optional | String              | Field name whose value is used as the event time (only for `outputType: row`). The field value must be an epoch-microsecond timestamp and must exist in the output values. |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                                              |

## Bigtable source module parameters

### Common parameters

| parameter             | optional | type    | description                                                                                                                                                     |
|-----------------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId             | required | String  | GCP Project ID of the Bigtable instance.                                                                                                                        |
| instanceId            | required | String  | Bigtable instance ID.                                                                                                                                            |
| tableId               | required | String  | Bigtable table ID to read from.                                                                                                                                  |
| appProfileId          | optional | String  | Bigtable [app profile](https://cloud.google.com/bigtable/docs/app-profiles) ID to use for reads.                                                                 |
| emulatorHost          | optional | String  | Bigtable emulator host in `host:port` format. If not set, the `BIGTABLE_EMULATOR_HOST` environment variable (or system property) is used when present.           |

### Batch read parameters

| parameter             | optional | type                                          | description                                                                                                                                                                       |
|-----------------------|----------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| columns               | optional | Array<[Column](#column-parameters)\>          | Column family/qualifier definitions that map Bigtable cells to output fields (used for `outputType: row`). If omitted, the row schema contains only the additional fields below.  |
| filter                | optional | JSON                                          | Row filter condition. See [Filter format](#filter-format).                                                                                                                        |
| keyRange              | optional | JSON                                          | Row key range(s) to read. See [KeyRange format](#keyrange-format). If not specified, all keys are read.                                                                           |
| format                | optional | Enum                                          | Default deserialization format for cell values. Values: `bytes`, `avro`, `text`, `hadoop`, `avromap`. Default: `bytes`. Can be overridden per column family or qualifier.         |
| cellType              | optional | Enum                                          | Which cell versions of a column to output. Values: `last` (latest cell), `first` (oldest cell), `all` (all cells as an array). Default: `last`. Can be overridden per column family. |
| outputType            | optional | Enum                                          | Output element shape. Values: `row` (one element per row), `cell` (one element per cell). Default: `row`.                                                                          |
| withRowKey            | optional | Boolean                                       | If `true`, adds the row key (as STRING) to the output schema. Default: `false`.                                                                                                    |
| rowKeyField           | optional | String                                        | Field name for the row key when `withRowKey` is `true`. Default: `row_key`.                                                                                                        |
| withFirstTimestamp    | optional | Boolean                                       | If `true`, adds the earliest cell timestamp of the row (as TIMESTAMP) to the output schema. Default: `false`.                                                                      |
| firstTimestampField   | optional | String                                        | Field name for the first timestamp. Default: `__firstTimestamp`.                                                                                                                   |
| withLastTimestamp     | optional | Boolean                                       | If `true`, adds the latest cell timestamp of the row (as TIMESTAMP) to the output schema. Default: `false`.                                                                        |
| lastTimestampField    | optional | String                                        | Field name for the last timestamp. Default: `__lastTimestamp`.                                                                                                                     |
| maxBufferElementCount | optional | Integer                                       | Enables Bigtable read throttling by buffering at most this number of elements per read request.                                                                                    |

#### Column parameters

Each entry of `columns` maps one column family to output fields.

| parameter  | optional | type                                          | description                                                                                                   |
|------------|----------|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| family     | required | String                                        | Column family name.                                                                                           |
| qualifiers | required | Array<[Qualifier](#qualifier-parameters)\>    | Column qualifiers to read from this family. Cells of qualifiers not listed here are not included in the output. |
| format     | optional | Enum                                          | Deserialization format for this family. Default: top-level `format`.                                          |
| cellType   | optional | Enum                                          | Cell version selection for this family (`last`, `first`, `all`). Default: top-level `cellType`.               |

#### Qualifier parameters

| parameter | optional | type   | description                                                                                                               |
|-----------|----------|--------|-------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String | Column qualifier name in Bigtable.                                                                                        |
| field     | optional | String | Output field name. Default: same as `name`.                                                                               |
| format    | optional | Enum   | Deserialization format for this qualifier's cell values. Default: the family's `format`.                                  |
| type      | optional | String | Schema field type used to decode the cell value (e.g. `string`, `long`, `double`, `boolean`, `timestamp`, `bytes`).       |
| mode      | optional | String | Schema field mode of the output field (`nullable`, `required`, `repeated`).                                               |

#### KeyRange format

The `keyRange` parameter accepts a JSON object, or an array of such objects for multiple ranges:

| field  | description                                                                                    |
|--------|------------------------------------------------------------------------------------------------|
| prefix | Reads all row keys starting with this prefix. If set, `start`/`end` are ignored.               |
| start  | Start row key (inclusive). If omitted, reading starts from the beginning of the table.         |
| end    | End row key (exclusive). If omitted, reading continues to the end of the table.                |

#### Filter format

The `filter` parameter accepts a JSON object with a `type` field plus type-specific fields. A JSON array of filter objects is treated as a filter chain (all conditions applied in sequence).

| type                                                              | additional fields                                                              | description                                                                     |
|-------------------------------------------------------------------|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `row_key_regex`, `family_name_regex`, `column_qualifier_regex`, `value_regex` | `regex` (required)                                                | Regex match on row key / family name / column qualifier / cell value.           |
| `sample`                                                          | `rate` (required)                                                              | Row sampling filter.                                                            |
| `limit_cells_per_row`, `limit_cells_per_column`                   | `limit` (required)                                                             | Limit number of cells per row / per column.                                     |
| `offset_cells_per_row`                                            | `offset` (required)                                                            | Skip the first N cells of each row.                                             |
| `column_range`                                                    | `family`, `startClosed` or `startOpen`, `endClosed` or `endOpen`               | Column qualifier range within a family.                                         |
| `value_range`                                                     | `startClosed` or `startOpen`, `endClosed` or `endOpen`                         | Cell value range.                                                               |
| `timestamp_range`                                                 | `start`, `end`                                                                 | Cell timestamp range (datetime strings).                                        |
| `block`, `pass`, `sink`, `strip`                                  | `flag` (optional, default `true`)                                              | Block all / pass all / sink / strip cell values.                                |
| `label`                                                           | `label` (required)                                                             | Apply a label transformer.                                                      |
| `chain`, `interleave`                                             | `children` (array of filter objects)                                           | Combine child filters as a chain (AND) or interleave (union).                   |

### Cell output schema

With `outputType: cell`, every cell becomes one output element with the following fixed schema, and the element's event time is set to the cell timestamp:

| field     | type      | description                    |
|-----------|-----------|--------------------------------|
| rowKey    | String    | Row key (UTF-8).               |
| family    | String    | Column family name.            |
| qualifier | String    | Column qualifier (UTF-8).      |
| value     | Bytes     | Raw cell value.                |
| timestamp | Timestamp | Cell timestamp.                |

### Change stream parameters

Read CDC events from a Bigtable change stream (`mode: changeDataCapture`). The output schema is a fixed change-record schema including fields such as `rowKey`, `commitTimestamp`, `tieBreaker`, `sourceCluster`, and the mutation contents.

| parameter                              | optional | type    | description                                                                                                                                       |
|----------------------------------------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| changeStream.changeStreamName          | required | String  | Name of the change stream. Used to identify the reading pipeline (partition metadata is tracked per name).                                        |
| changeStream.metadataProjectId         | optional | String  | Project ID for the change stream metadata table. Default: same as `projectId`.                                                                    |
| changeStream.metadataInstanceId        | optional | String  | Instance ID for the change stream metadata table. Default: same as `instanceId`.                                                                  |
| changeStream.metadataTableId           | optional | String  | Table ID for the change stream metadata table. Default: same as `tableId`.                                                                        |
| changeStream.startTime                 | optional | String  | Timestamp to start reading change events from (datetime string, e.g. `2024-01-15T00:00:00Z`). If not set, reading starts from the current time.   |
| changeStream.createOrUpdateMetadataTable | optional | Boolean | Whether to create or update the metadata table before reading.                                                                                  |
| changeStream.existingPipelineOptions   | optional | Enum    | Behavior when a pipeline with the same `changeStreamName` already exists. Values: `RESUME_OR_NEW`, `RESUME_OR_FAIL`, `FAIL_IF_EXISTS`, `SKIP_CLEANUP`. |
| changeStream.metadataTableAppProfileId | optional | String  | App profile ID to use for metadata table operations.                                                                                              |

The top-level `appProfileId` is also applied to the change stream read when specified. For change streams, the app profile must use single-cluster routing with single-row transactions enabled.

## Examples

### Example 1: Read rows with column mapping

Read a table, decoding cells of the `stats` family as text and including the row key.

```yaml
sources:
  - name: bigtable_source
    module: bigtable
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: users
      format: text
      withRowKey: true
      rowKeyField: user_id
      columns:
        - family: stats
          qualifiers:
            - name: n
              field: name
              type: string
            - name: a
              field: age
              type: long
```

### Example 2: Read with key range and row filter

Read only rows whose key starts with `user#2024`, keeping just the latest cell per column.

```yaml
sources:
  - name: bigtable_filtered
    module: bigtable
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: users
      keyRange:
        prefix: "user#2024"
      filter:
        type: limit_cells_per_column
        limit: 1
      columns:
        - family: profile
          format: text
          qualifiers:
            - name: email
            - name: country
```

### Example 3: Output one element per cell

Emit every cell as a separate element with the fixed cell schema.

```yaml
sources:
  - name: bigtable_cells
    module: bigtable
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: events
      outputType: cell
```

### Example 4: Change stream for CDC

Read change data capture events in a streaming pipeline.

```yaml
sources:
  - name: bigtable_cdc
    module: bigtable
    mode: changeDataCapture
    parameters:
      projectId: myproject
      instanceId: myinstance
      tableId: users
      appProfileId: cdc-profile
      changeStream:
        changeStreamName: my-cdc-pipeline
        metadataTableId: users_cdc_metadata
        startTime: "2024-01-15T00:00:00Z"
        createOrUpdateMetadataTable: true
```
