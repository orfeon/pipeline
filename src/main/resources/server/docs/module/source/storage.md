---
type: Source Module
title: Storage Source Module
description: Reads and parses file contents from Google Cloud Storage (GCS), AWS S3, or local file systems. Supports Avro, Parquet, CSV, and JSON formats with schema auto-inference for binary formats. Includes column projection for Parquet, header skipping and line filtering for CSV, and compression support for text formats.
tags: [source, storage, batch, gcs, s3, avro, parquet, csv, json]
timestamp: 2026-06-23T00:00:00Z
---

# Storage Source Module

Source Module for reading and parsing file contents from [Google Cloud Storage](https://cloud.google.com/storage/docs) (GCS), AWS S3, or local file systems. Supports four data formats:

- **Avro** - Reads Apache Avro files. Schema is automatically inferred from the file; no `schema` parameter is needed. Supports column projection via `schema` (declare a subset of the fields) or `fields`.
- **Parquet** - Reads Apache Parquet files. Schema is automatically inferred from the file. Supports column projection via `fields` to read only specific columns.
- **CSV** - Reads CSV (comma-separated values) text files. When `schema` is provided, each line is parsed into typed fields. When `schema` is not provided, each line is output as raw text.
- **JSON** - Reads JSON Lines (newline-delimited JSON) text files. When `schema` is provided, each line is parsed into typed fields. When `schema` is not provided, each line is output as raw text.

This module differs from the [Files Source Module](files.md): the Files module outputs file *metadata* (and optionally raw bytes), while the Storage module reads and parses file *contents* into structured records.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                  |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                            |
| module             | required | String              | Specified `storage`                                                                                                          |
| schema             | optional | [Schema](../common/schema.md) | Schema of the data to be read. Not required for `avro` or `parquet` formats (auto-inferred). Required for `csv` and `json` formats if you want structured output. |
| timestampAttribute | optional | String              | If you want to use the value of a field as the event time, specify the name of the field. (The field must be Timestamp or Date type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                  |

## Storage source module parameters

### Common parameters

| parameter | optional | type           | description                                                                                                                                                                                     |
|-----------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| input     | selective required | String         | File path or glob pattern to read. Supports GCS (`gs://bucket/path/*.avro`), S3 (`s3://bucket/path/*`), and local paths. Either `input` or `inputs` must be specified.                          |
| inputs    | selective required | Array<String\> | List of file paths or glob patterns to read. Results from all paths are merged. Either `input` or `inputs` must be specified.                                                                    |
| format    | required | Enum           | Data format of the files to read. Values: `avro`, `parquet`, `csv`, `json`.                                                                                                                      |

### Parquet-specific parameters

| parameter | optional | type           | description                                                                                                                                                                                     |
|-----------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fields    | optional | Array<String\> | Field names to read (column projection) for `avro` and `parquet` formats. Only the specified columns are read, reducing I/O and memory usage. Every name must exist in the input schema (a missing name is an assembly-time error). If not specified, all columns are read. |

### CSV/JSON-specific parameters

| parameter       | optional | type    | description                                                                                                                                                           |
|-----------------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| compression     | optional | Enum    | Compression format of the file. Values: `ZIP`, `GZIP`, `BZIP2`, `ZSTD`, `LZO`, `LZOP`, `DEFLATE`. If not specified, auto-detected or assumed uncompressed.           |
| filterPrefix    | optional | String  | Lines starting with this prefix are excluded. Useful for skipping CSV headers or comment lines (e.g. `"#"` or the first field of the header row).                     |
| skipHeaderLines | optional | Integer | Number of header lines to skip at the beginning of each file. For example, set to `1` to skip a single header row in CSV files.                                       |
| delimiter       | optional | String  | Custom record delimiter. Default is newline (`\n`). Use this when records are separated by a character or string other than newline.                                   |

### S3 parameters

When reading from AWS S3, provide AWS credentials via the `s3` parameter block, or configure credentials at the pipeline settings level.

| parameter      | optional | type   | description                                     |
|----------------|----------|--------|-------------------------------------------------|
| s3.accessKey   | required | String | AWS access key ID for S3 authentication.        |
| s3.secretKey   | required | String | AWS secret access key for S3 authentication.    |
| s3.region      | required | String | AWS region of the S3 bucket (e.g. `us-west-2`). |

## Schema behavior

### Avro and Parquet formats

Schema is automatically inferred from the file metadata. No `schema` parameter is needed. The module reads the first matching file's embedded schema and uses it for all files.

If a `schema` parameter is explicitly provided, it takes priority over the auto-inferred schema.
For Avro it acts as the [reader schema](https://avro.apache.org/docs/current/specification/#schema-resolution):
declaring a subset of the file's fields projects the output to that subset (unlisted columns are
skipped during decode), and Avro schema resolution rules (default filling, type promotion) apply.
Declaring an explicit schema also makes the pipeline robust against files whose schema evolves,
and skips the sampling step entirely (which also enables reading from local file systems).

For Avro with `fields` specified, the output contains only the selected columns.
For Parquet with `fields` specified, only the selected columns are read (column projection), and
unselected columns are set to null in the output (all columns remain in the output schema).
In both cases every name in `fields` must exist in the input schema — an unknown name is an
assembly-time error rather than a silent drop.

### CSV and JSON formats with schema

When a `schema` is provided, each line is parsed according to the schema field definitions:

- **CSV**: Fields are extracted by position according to the schema field order.
- **JSON**: Each line is parsed as a JSON object and fields are mapped by name according to the schema.

### CSV and JSON formats without schema

When no `schema` is provided, each line is output as a raw text record with the following fixed fields:

| field     | type      | description                                            |
|-----------|-----------|--------------------------------------------------------|
| text      | STRING    | The raw text content of the line.                      |
| name      | STRING    | The source step name.                                  |
| timestamp | TIMESTAMP | The timestamp when the record was processed.           |

## Examples

### Example 1: Read Avro files from GCS

Read all Avro files matching a glob pattern. Schema is auto-inferred.

```yaml
sources:
  - name: avro_source
    module: storage
    parameters:
      input: "gs://my-bucket/data/*.avro"
      format: avro
```

### Example 2: Read Parquet files with column projection

Read only specific columns from Parquet files to reduce I/O.

```yaml
sources:
  - name: parquet_source
    module: storage
    parameters:
      input: "gs://my-bucket/data/*.parquet"
      format: parquet
      fields:
        - user_id
        - name
        - email
```

### Example 3: Read CSV files with schema

Parse CSV files using a defined schema.

```yaml
sources:
  - name: csv_source
    module: storage
    schema:
      fields:
        - name: ID
          type: string
          mode: required
        - name: NumberField
          type: double
          mode: nullable
        - name: DateField
          type: date
          mode: nullable
    parameters:
      input: "gs://my-bucket/data/*.csv"
      format: csv
      skipHeaderLines: 1
```

### Example 4: Read CSV with header filter

Skip CSV header lines using `filterPrefix`.

```yaml
sources:
  - name: csv_source
    module: storage
    schema:
      fields:
        - name: ID
          type: string
        - name: Name
          type: string
        - name: Amount
          type: double
    parameters:
      input: "gs://my-bucket/data/*.csv"
      format: csv
      filterPrefix: "\"ID\""
```

### Example 5: Read JSON Lines without schema

Read JSON Lines files as raw text when no schema is provided.

```yaml
sources:
  - name: json_raw
    module: storage
    parameters:
      input: "gs://my-bucket/logs/*.jsonl"
      format: json
```

This outputs records with `text`, `name`, and `timestamp` fields.

### Example 6: Read compressed JSON files with schema

Read gzip-compressed JSON Lines files with a defined schema.

```yaml
sources:
  - name: json_source
    module: storage
    schema:
      fields:
        - name: event_id
          type: string
        - name: user_id
          type: string
        - name: event_type
          type: string
        - name: event_time
          type: timestamp
    parameters:
      input: "gs://my-bucket/events/*.jsonl.gz"
      format: json
      compression: GZIP
    timestampAttribute: event_time
```

### Example 7: Read from multiple input paths

Merge data from multiple GCS paths.

```yaml
sources:
  - name: multi_source
    module: storage
    parameters:
      inputs:
        - "gs://my-bucket/data/2024/**/*.avro"
        - "gs://my-bucket/data/2025/**/*.avro"
      format: avro
```

### Example 8: Read Avro files from AWS S3

Read Avro files from an S3 bucket with explicit credentials.

```yaml
sources:
  - name: s3_source
    module: storage
    parameters:
      input: "s3://my-bucket/data/*.avro"
      format: avro
      s3:
        accessKey: "ACCESS_KEY"
        secretKey: "SECRET_KEY"
        region: "us-west-2"
```

### Example 9: Avro to Spanner pipeline

Read Avro files from GCS and write to Cloud Spanner.

```yaml
sources:
  - name: avro_source
    module: storage
    parameters:
      input: "gs://my-bucket/export/*.avro"
      format: avro

sinks:
  - name: spanner_output
    module: spanner
    inputs:
      - avro_source
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      table: users
```

### Example 10: CSV to BigQuery pipeline

Read CSV files and load into BigQuery.

```yaml
sources:
  - name: csv_source
    module: storage
    schema:
      fields:
        - name: order_id
          type: string
        - name: customer_id
          type: string
        - name: amount
          type: double
        - name: order_date
          type: date
    parameters:
      input: "gs://my-bucket/orders/*.csv"
      format: csv
      skipHeaderLines: 1

sinks:
  - name: bigquery_output
    module: bigquery
    inputs:
      - csv_source
    parameters:
      table: "myproject.mydataset.orders"
```
