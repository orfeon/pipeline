---
type: Source Module
title: Storage Source Module
description: Reads and parses file contents from Google Cloud Storage (GCS), AWS S3, or local file systems. Supports Avro, Parquet, CSV, JSON, and fixed-width (fwf) formats with schema auto-inference for binary formats. Includes column projection, header skipping and line filtering, compression support for text formats, fixed-width records in any charset (e.g. Shift_JIS / windows-31j), files packed in zip / tar archives (entries selected by glob), and source file / entry / line number fields.
tags: [source, storage, batch, gcs, s3, avro, parquet, csv, json, fwf, fixed-width, zip, tar, archive]
timestamp: 2026-06-23T00:00:00Z
---

# Storage Source Module

Source Module for reading and parsing file contents from [Google Cloud Storage](https://cloud.google.com/storage/docs) (GCS), AWS S3, or local file systems. Supports five data formats:

- **Avro** - Reads Apache Avro files. Schema is automatically inferred from the file; no `schema` parameter is needed. Supports column projection via `schema` (declare a subset of the fields) or `fields`.
- **Parquet** - Reads Apache Parquet files. Schema is automatically inferred from the file. Supports column projection via `fields` to read only specific columns.
- **CSV** - Reads CSV (comma-separated values) text files. When `schema` is provided, each line is parsed into typed fields. When `schema` is not provided, each line is output as raw text.
- **JSON** - Reads JSON Lines (newline-delimited JSON) text files. When `schema` is provided, each line is parsed into typed fields. When `schema` is not provided, each line is output as raw text.
- **fwf** - Reads fixed-width text files: each field is a fixed byte (or character) range of the record, declared by a layout in `schema` — see [Fixed-Width Format (fwf)](../common/fwf.md). Records are read as bytes, so any charset works (e.g. `windows-31j` for Shift_JIS data).

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
| format    | selective required | Enum           | Data format of the files to read. Values: `avro`, `parquet`, `csv`, `json`, `fwf`. May be omitted when `schema.encoding.format` declares it (`fwf`); when both are written they must agree. |

### Projection parameters

| parameter | optional | type           | description                                                                                                                                                                                     |
|-----------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fields    | optional | Array<String\> | Field names to read (column projection) for `avro`, `parquet` and `fwf` formats. Only the specified columns are read, reducing I/O and memory usage. Every name must exist in the input schema (a missing name is an assembly-time error). If not specified, all columns are read. For `fwf` the listed top-level layout fields are output in the listed order, and the other ranges are not decoded at all. |

### CSV/JSON/fwf parameters

| parameter       | optional | type    | description                                                                                                                                                           |
|-----------------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| compression     | optional | Enum    | Compression format of the file. Values: `ZIP`, `GZIP`, `BZIP2`, `ZSTD`, `LZO`, `LZOP`, `DEFLATE`. If not specified, auto-detected or assumed uncompressed. `ZIP` is the legacy way to read a zip: its entries are concatenated into one stream (`skipHeaderLines` skips only the first entry's header). To read the entries of a zip or tar as separate files, use `archive` instead. |
| filterPrefix    | optional | String  | Lines starting with this prefix are excluded. Useful for skipping CSV headers or comment lines (e.g. `"#"` or the first field of the header row).                     |
| skipHeaderLines | optional | Integer | Number of header lines to skip at the beginning of each file. For example, set to `1` to skip a single header row in CSV files.                                       |
| delimiter       | optional | String  | Custom record delimiter. Default is newline (`\n`). Use this when records are separated by a character or string other than newline.                                   |
| recordSplit     | optional | Enum    | `fwf` only. How a file is cut into records. `line` (default): at line feeds — a carriage return before it is removed, so CRLF and LF files read the same — or at `delimiter` when given. `length`: the file has no separators and is cut every `recordLength` (of the layout) bytes — so it requires `schema.encoding.unit: byte` (the default) unless the charset is a single-byte one. |

For `fwf`, `filterPrefix` and `delimiter` are compared with the record bytes (the text encoded in `schema.encoding.charset`; a charset that writes a byte order mark, such as `UTF-16` without an explicit byte order, is an assembly-time error — use `UTF-16LE` / `UTF-16BE`), and blank lines are skipped. A byte order mark at the head of a UTF-8 file is skipped.

### Archive parameters

Reads the matched files as zip / tar archives (`csv`, `json` and `fwf` formats): every selected entry is read as a file of its own, so `skipHeaderLines` and line numbers restart per entry. See [Archive (zip / tar)](../common/archive.md) for the entry semantics and how archives are read.

| parameter           | optional | type           | description |
|---------------------|----------|----------------|-------------|
| archive.type        | optional | Enum           | `zip` or `tar`. Omitted: told from the file name (`.zip`, `.tar`, `.tar.gz`, `.tgz`, ...). |
| archive.entries     | optional | Array<String\> | Globs selecting the entries to read, matched against the entry path (`*` within a path segment, `**` across segments). Omitted: every file entry. |
| archive.exclude     | optional | Array<String\> | Globs of entries to leave out, applied after `entries`. |
| archive.nameCharset | optional | String         | Charset of the entry names. Default `UTF-8` (e.g. `windows-31j` for a zip made on Japanese Windows). |

`archive: {}` reads every file entry. For a zip on GCS / S3 / a local disk only the selected entries are read from storage. `compression: ZIP` can not be combined with `archive`.

### Additional fields parameters

Adds where each record came from as output fields (`csv`, `json` and `fwf` formats). Each parameter is the output field name; the fields are appended after the data fields. A name that is already a data field is an assembly-time error.

| parameter                     | optional | type   | description |
|-------------------------------|----------|--------|-------------|
| additionalFields.resource     | optional | String | Output field name for the file path (e.g. `gs://bucket/dir/file.txt`). STRING. |
| additionalFields.line         | optional | String | Output field name for the line number in the file (1-based, header and blank lines counted; the record number with `recordSplit: length`). INT64. |
| additionalFields.lastModified | optional | String | Output field name for the file's last modification time. TIMESTAMP. Not available on GCS (the epoch), as in the [Files Source Module](files.md). |
| additionalFields.entry        | optional | String | Output field name for the entry path inside an archive (see `archive`). STRING; null for a plain file. |

With `additionalFields` or `archive`, `csv` and `json` files are read through the same byte-record reader as `fwf` (UTF-8, `schema` required): blank lines are skipped, and a line that can not be parsed as a record goes to the failure output. As with `fwf`, a file is not split: one file is read by one worker, so prefer many files over one very large file when using `additionalFields`.

### Reading from AWS S3

When reading from AWS S3 (`s3://...`), configure credentials and region at the pipeline settings
level (`options.aws` — see the aws options reference). Both schema sampling and the actual read
use the same credential source. The former per-module `s3` parameter block
(`s3.accessKey` / `s3.secretKey` / `s3.region`) has been removed.

## Schema behavior

### Avro and Parquet formats

Schema is automatically inferred from the file metadata (the Avro header or the Parquet footer —
files are never downloaded in full). Sampling resolves paths and glob patterns the same way as the
actual read, works for `gs://`, `s3://`, and local paths, skips zero-length placeholder files
(e.g. `_SUCCESS`), and uses the first readable file's embedded schema for all files.

If a `schema` parameter is explicitly provided, it takes priority over the auto-inferred schema.
For Avro it acts as the [reader schema](https://avro.apache.org/docs/current/specification/#schema-resolution):
declaring a subset of the file's fields projects the output to that subset (unlisted columns are
skipped during decode), and Avro schema resolution rules (default filling, type promotion) apply.
Declaring an explicit schema also makes the pipeline robust against files whose schema evolves,
and skips the sampling step entirely.

For Avro with `fields` specified, the output contains only the selected columns.
For Parquet with `fields` specified, only the selected columns are read (column projection), and
unselected columns are set to null in the output (all columns remain in the output schema).
In both cases every name in `fields` must exist in the input schema — an unknown name is an
assembly-time error rather than a silent drop.

### CSV and JSON formats with schema

When a `schema` is provided, each line is parsed according to the schema field definitions:

- **CSV**: Fields are extracted by position according to the schema field order.
- **JSON**: Each line is parsed as a JSON object and fields are mapped by name according to the schema.

### fwf format

`schema` is required: `encoding.format: fwf` with the layout document in `reference` (`uri` or `inline`), as described in [Fixed-Width Format (fwf)](../common/fwf.md). The output fields are the layout fields (nested records and arrays included), narrowed by a declared `schema.fields` and then by `fields`.

A record whose length differs from the layout `recordLength`, or with a value that can not be converted, is a failure: the job fails with `failFast: true` (the batch default), and with `failFast: false` the record goes to the failure sinks (`system.failure`) while the rest is read. `schema.encoding.onLengthMismatch: pad` and `onParseError: null` relax this per schema.

Files are not split: one file is read by one worker (a compressed file can not be split anyway).

An I/O error in the middle of a file (e.g. a truncated gzip file) is not a record error: the rest of that file can not be read. With `failFast: true` the job fails. With `failFast: false` the records read so far stay in the output and one failure record reports the file and the last record number read, so a file listed in the failures has been read only partially.

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

### Example 11: Read fixed-width (fwf) files

Read gzip-compressed Shift_JIS fixed-width files. The layout is kept in a file next to the data, the
format is taken from `schema.encoding.format`, and each record carries its file and line number.

```yaml
sources:
  - name: orders
    module: storage
    parameters:
      input: "gs://my-bucket/orders/**/*.txt.gz"
      schema:
        encoding:
          format: fwf
          charset: windows-31j
        reference:
          uri: gs://my-bucket/layouts/orders.fwf.json
      fields: [shopCode, shopName, total, orderDate]
      additionalFields:
        resource: source_file
        line: source_line
```

`orders.fwf.json`:

```json
{
  "recordLength": 40,
  "fields": [
    { "name": "shopCode",  "type": "string",  "pos": 1,  "len": 4 },
    { "name": "shopName",  "type": "string",  "pos": 5,  "len": 20 },
    { "name": "total",     "type": "decimal", "pos": 25, "len": 8, "scale": 2 },
    { "name": "orderDate", "type": "date",    "pos": 33, "len": 8, "pattern": "yyyyMMdd", "nullIf": ["00000000"] }
  ]
}
```

### Example 12: Read one kind of file out of zip archives

Each zip packs several kinds of files; read only the order files, as fixed-width records, and keep
the entry each record came from.

```yaml
sources:
  - name: orders
    module: storage
    parameters:
      input: "gs://my-bucket/packs/PACK*.zip"
      archive:
        entries: ["ORD*.txt"]
      schema:
        encoding:
          format: fwf
          charset: windows-31j
        reference:
          uri: gs://my-bucket/layouts/orders.fwf.json
      additionalFields:
        resource: source_file
        entry: source_entry
```

