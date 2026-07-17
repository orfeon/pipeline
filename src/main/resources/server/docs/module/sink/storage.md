---
type: Sink Module
title: Storage Sink Module
description: Writes input data as files to Cloud Storage (GCS), S3, or local file system in Avro, Parquet, Arrow IPC, JSON, or CSV format. Supports dynamic output paths using FreeMarker templates, sharding, compression, and codec configuration.
tags: [sink, storage, batch, streaming, gcs, s3, avro, parquet, arrow, csv, json]
timestamp: 2026-06-22T00:00:00Z
---

# Storage Sink Module

Sink Module for writing input data as structured files to Cloud Storage (GCS), S3, or the local file system. The entire PCollection is written as a batch of files in a specified format (Avro, Parquet, Arrow IPC, JSON, or CSV).

This module differs from the [Files Sink Module](files.md): the Files module writes **one file per input record** with per-record dynamic paths, while the Storage module writes the **entire dataset** as sharded files in a structured format.

## Sink module common parameters

| parameter | optional | type                | description                                                           |
|-----------|----------|---------------------|-----------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                     |
| module    | required | String              | Specified `storage`                                                   |
| inputs    | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| schema    | optional | [Schema](../common/schema.md) | Output schema. If not specified, the input schema is used.            |
| waits     | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy  | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Storage sink module parameters

| parameter     | optional | type    | description                                                                                                                                                                                                        |
|---------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| output        | required | String  | Output path. Supports GCS (`gs://`), S3 (`s3://`), or local paths. Supports [FreeMarker template expressions](#dynamic-output-paths) referencing input field values for dynamic partitioning (e.g. `gs://bucket/path/${field_name}/data`). |
| format        | required | Enum    | Output file format. Values: `avro`, `parquet`, `arrow`, `json`, `csv`.                                                                                                                                             |
| suffix        | optional | String  | File name suffix (e.g. `.avro`, `.parquet`). Supports [FreeMarker template expressions](#suffix-template-variables) for streaming windowed output. Default: `""` (empty).                                           |
| numShards     | optional | Integer | Number of output file shards. If not specified or less than 1, auto-sharding is used. If set to `1`, a single output file is produced (suffix is appended directly without shard index).                            |
| compression   | optional | Enum    | File-level compression. Values: `ZIP`, `GZIP`, `BZIP2`, `ZSTD`, `LZO`, `LZOP`, `DEFLATE`, `UNCOMPRESSED`, `AUTO`. Default: `AUTO`.                                                                               |
| codec         | optional | Enum    | Internal codec for Avro/Parquet formats. See [Codec values](#codec-values). Default: `SNAPPY`.                                                                                                                      |
| tempDirectory | optional | String  | Temporary directory path for intermediate files. If not specified, the runner creates one automatically (may require bucket creation permission).                                                                    |
| noSpilling    | optional | Boolean | If `true`, disables file spilling. Default: `false`.                                                                                                                                                                |
| header        | optional | Boolean | (CSV only) If `true`, writes a CSV header row. Default: `false`.                                                                                                                                                    |
| bom           | optional | Boolean | (CSV/JSON only) If `true`, writes a UTF-8 BOM at the beginning of the file. Default: `false`.                                                                                                                       |
| outputEmpty   | optional | Boolean | If `true`, writes an empty file even when there are no input records. Default: `false`.                                                                                                                              |
| batchSize     | optional | Integer | (Arrow only) Number of rows per Arrow record batch. Bounds each writer's off-heap (direct) memory to roughly `batchSize` × row width; consumers stream the file batch by batch. Default: `10000`.                    |

### Format details

| format   | description                                                                                                  |
|----------|--------------------------------------------------------------------------------------------------------------|
| `avro`   | Apache Avro binary format. Schema is embedded in the file. Supports `codec` parameter.                       |
| `parquet`| Apache Parquet columnar format. Supports `codec` parameter.                                                  |
| `arrow`  | Apache Arrow IPC File format (Feather V2). Readable directly by pandas (`pyarrow.ipc.open_file`), Polars (`read_ipc`), and DuckDB without conversion. Recommended suffix: `.arrow`. Supports `batchSize` and `codec` (`ZSTD` / `LZ4` buffer compression only; other codec values including the default `SNAPPY` write uncompressed buffers). |
| `json`   | Each record as a JSON object, one per line (JSON Lines format).                                              |
| `csv`    | Comma-separated values. Use `header` to include column names. Use `bom` for Excel-compatible UTF-8 output.   |

### Codec values

| codec          | avro | parquet | arrow | description               |
|----------------|------|---------|-------|---------------------------|
| `SNAPPY`       | yes  | yes     | no    | Fast compression (default) |
| `ZSTD`         | yes  | yes     | yes   | Zstandard compression      |
| `UNCOMPRESSED` | yes  | yes     | yes   | No compression             |
| `BZIP2`        | yes  | no      | no    | BZip2 compression          |
| `DEFLATE`      | yes  | no      | no    | Deflate compression        |
| `XZ`           | yes  | no      | no    | XZ compression             |
| `LZO`          | no   | yes     | no    | LZO compression            |
| `LZ4`          | no   | yes     | yes   | LZ4 compression (LZ4_FRAME for arrow) |
| `LZ4_RAW`      | no   | yes     | no    | Raw LZ4 compression        |
| `BROTLI`       | no   | yes     | no    | Brotli compression         |
| `GZIP`         | no   | yes     | no    | GZIP compression           |

For `arrow`, codec values other than `ZSTD` / `LZ4` (including the default `SNAPPY`) are treated as
uncompressed: Arrow IPC buffer compression only defines `LZ4_FRAME` and `ZSTD`.

### Arrow format JVM requirement

Arrow's off-heap memory requires the JVM flag `--add-opens=java.base/java.nio=ALL-UNNAMED`
(JDK 16+); without it the first arrow write fails with `Failed to initialize MemoryUtil`.
The template container image already sets this flag for its entrypoint JVM (DirectRunner /
template launcher). **On Dataflow the worker JVM is started by the Dataflow service, not the
container entrypoint**, so launch pipelines that use `format: arrow` with the pipeline option
`--jdkAddOpenModules=java.base/java.nio=ALL-UNNAMED`.

## Dynamic output paths

The `output` parameter supports FreeMarker template expressions using `${fieldName}` syntax. When template expressions are detected, the module uses dynamic writing: records are partitioned by the evaluated path, and each partition is written to a separate directory/file.

Additionally, the built-in variable `${__timestamp}` is available, representing the element's event timestamp as a `java.time.Instant`.

Example: `gs://my-bucket/${category}/${region}/data` will produce separate output directories for each unique combination of `category` and `region` field values.

### Suffix template variables

When `suffix` contains FreeMarker template expressions, the following variables are available for constructing dynamic file names (useful for windowed streaming output):

| variable          | type    | description                                        |
|-------------------|---------|----------------------------------------------------|
| windowStart       | Instant | Start of the interval window.                      |
| windowEnd         | Instant | End of the interval window.                        |
| paneIndex         | Long    | Pane index.                                        |
| paneIsFirst       | Boolean | Whether this is the first pane.                    |
| paneIsLast        | Boolean | Whether this is the last pane.                     |
| paneTiming        | String  | Pane timing name (EARLY, ON_TIME, LATE, UNKNOWN).  |
| paneIsOnlyFiring  | Boolean | Whether this is the only firing (first and last).  |
| numShards         | Integer | Total number of shards.                            |
| shardIndex        | Integer | Current shard index.                               |
| suggestedSuffix   | String  | Compression-suggested suffix.                      |

## Output schema

After writing, the module produces output records with the following schema (currently not connected to downstream as the module returns `PDone`):

| field     | type      | description                          |
|-----------|-----------|--------------------------------------|
| sink      | STRING    | The sink step name.                  |
| path      | STRING    | The output file path that was written. |
| timestamp | TIMESTAMP | The timestamp when writing occurred. |

## Examples

### Example 1: Write Avro files to GCS

```yaml
sources:
  - name: spanner_source
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: mytable

sinks:
  - name: avro_output
    module: storage
    inputs:
      - spanner_source
    parameters:
      output: "gs://my-bucket/exports/mytable"
      format: avro
```

### Example 2: Write Parquet with ZSTD codec

```yaml
sinks:
  - name: parquet_output
    module: storage
    inputs:
      - source
    parameters:
      output: "gs://my-bucket/output/data"
      format: parquet
      codec: ZSTD
      suffix: ".parquet"
```

### Example 3: Write Arrow IPC files for pandas / Polars / DuckDB

Write Arrow IPC (Feather V2) files that Python tools can read directly without conversion
(`pyarrow.ipc.open_file()`, `polars.read_ipc()`, DuckDB IPC reader).

```yaml
sinks:
  - name: arrow_output
    module: storage
    inputs:
      - source
    parameters:
      output: "gs://my-bucket/exports/data"
      format: arrow
      suffix: ".arrow"
      codec: ZSTD       # optional: ZSTD or LZ4 buffer compression (omit for uncompressed)
      batchSize: 10000  # optional: rows per record batch
```

### Example 4: Write CSV with header and BOM

Write CSV files with a header row and UTF-8 BOM for Excel compatibility.

```yaml
sinks:
  - name: csv_output
    module: storage
    inputs:
      - source
    parameters:
      output: "gs://my-bucket/exports/report"
      format: csv
      suffix: ".csv"
      header: true
      bom: true
```

### Example 5: Dynamic output path partitioning

Partition output files by the value of a field. Each unique value produces a separate output directory.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT region, category, amount FROM `myproject.mydataset.sales`"

sinks:
  - name: partitioned_output
    module: storage
    inputs:
      - bigquery_source
    parameters:
      output: "gs://my-bucket/sales/${region}/${category}/data"
      format: avro
```

### Example 6: Single file output

Write all records into a single file without sharding.

```yaml
sinks:
  - name: single_file
    module: storage
    inputs:
      - source
    parameters:
      output: "gs://my-bucket/exports/all_data"
      format: avro
      numShards: 1
      suffix: ".avro"
```

### Example 7: Write JSON to GCS with compression

```yaml
sinks:
  - name: json_output
    module: storage
    inputs:
      - source
    parameters:
      output: "gs://my-bucket/exports/data"
      format: json
      compression: GZIP
      suffix: ".json"
```

### Example 8: Write to AWS S3

```yaml
sinks:
  - name: s3_output
    module: storage
    inputs:
      - source
    parameters:
      output: "s3://my-s3-bucket/exports/data"
      format: avro
```

### Example 9: Streaming windowed output with dynamic suffix

Write windowed streaming data with window timestamps in the file name.

```yaml
sinks:
  - name: windowed_output
    module: storage
    inputs:
      - windowed_source
    strategy:
      windowType: fixed
      windowSize: 60
      windowUnit: second
    parameters:
      output: "gs://my-bucket/streaming/events"
      format: json
      suffix: "-${windowStart?string('yyyyMMddHHmmss')}-${windowEnd?string('yyyyMMddHHmmss')}.json"
```
