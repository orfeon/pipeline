---
type: Source Module
title: Files Source Module
description: Lists file metadata by matching glob patterns on Cloud Storage, S3, or local file systems. Optionally reads file content as bytes. Useful for file inventory, metadata-driven pipelines, and file content ingestion.
tags: [source, files, batch, gcs, s3, metadata, glob]
timestamp: 2026-06-21T14:30:00Z
---

# Files Source Module

Source Module for listing file metadata by matching glob patterns on Cloud Storage (GCS), S3, or local file systems. Each matched file is output as a record containing metadata such as filename, size, last modified timestamp, and checksum.

When `withContent` is set to `true`, the file content is also read and included as a `content` bytes field.

This module differs from the [Storage Source Module](storage.md): the Storage module reads and parses file *contents* (CSV, JSON, Avro, Parquet), while the Files module outputs file *metadata* (and optionally raw bytes).

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                  |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                            |
| module             | required | String              | Specified `files`                                                                                                            |
| schema             | -        | -                   | Not required. The output schema is fixed (see [Output schema](#output-schema)).                                              |
| timestampAttribute | optional | String              | If you want to use the value of a field as the event time, specify the name of the field. (The field must be Timestamp type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                  |

## Files source module parameters

| parameter           | optional           | type           | description                                                                                                                                                                                                         |
|---------------------|--------------------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| pattern             | selective required | String         | Glob pattern to match files. Supports GCS (`gs://bucket/path/**/*.csv`), S3 (`s3://bucket/path/*`), and local paths. Either `pattern` or `patterns` must be specified.                                              |
| patterns            | selective required | Array<String\> | List of glob patterns to match files. Results from all patterns are merged. Either `pattern` or `patterns` must be specified.                                                                                        |
| emptyMatchTreatment | optional           | Enum           | Behavior when no files match the pattern. Values: `ALLOW` (return empty, no error), `ALLOW_IF_WILDCARD` (allow empty only if pattern has wildcards), `DISALLOW` (throw error on no match). Default: `ALLOW`.         |
| filter              | optional           | Filter         | Filter condition to apply to matched file metadata. Uses the same [Filter](../common/filter.md) syntax as other modules. Filter fields correspond to the output schema fields (e.g. `filename`, `sizeBytes`, etc.). |
| withContent         | optional | Boolean | If `true`, reads and includes file content as bytes in the `content` field, and adds the `compression` field. Default: `false`.                                                                                                    |

## Output schema

Each matched file produces a record with the following fields:

| field               | type      | description                                                                                         |
|---------------------|-----------|-----------------------------------------------------------------------------------------------------|
| filename            | STRING    | The file name (without directory path).                                                             |
| directory           | STRING    | The parent directory name.                                                                          |
| resource            | STRING    | The full resource path (e.g. `gs://bucket/path/to/file.csv`).                                      |
| sizeBytes           | INT64     | File size in bytes.                                                                                 |
| isDirectory         | BOOLEAN   | Whether the resource is a directory.                                                                |
| lastModified        | TIMESTAMP | Last modification timestamp.                                                                        |
| schema              | STRING    | The URI scheme of the resource (e.g. `gs`, `s3`, `file`).                                           |
| isReadSeekEfficient | BOOLEAN   | Whether the file supports efficient random access reads.                                            |
| checksum            | STRING    | File checksum (format depends on the file system).                                                  |
| content             | BYTES     | File content as raw bytes. Only populated when `withContent` is `true`, otherwise `null`.           |
| compression         | STRING    | Compression type of the file (e.g. `UNCOMPRESSED`, `GZIP`). Only populated when `withContent` is `true`, otherwise `null`. |

## Examples

### Example 1: List CSV files on GCS

List all CSV files under a GCS path.

```yaml
sources:
  - name: csv_files
    module: files
    parameters:
      pattern: "gs://my-bucket/data/**/*.csv"
```

### Example 2: Multiple patterns

List files matching multiple patterns from different paths.

```yaml
sources:
  - name: data_files
    module: files
    parameters:
      patterns:
        - "gs://my-bucket/input/2024/**/*.parquet"
        - "gs://my-bucket/input/2025/**/*.parquet"
```

### Example 3: Filter by file size

List only files larger than 1 MB.

```yaml
sources:
  - name: large_files
    module: files
    parameters:
      pattern: "gs://my-bucket/data/*"
      filter:
        key: sizeBytes
        op: ">"
        value: 1048576
```

### Example 4: Read file content

Read file metadata together with file content as bytes.

```yaml
sources:
  - name: file_contents
    module: files
    parameters:
      pattern: "gs://my-bucket/templates/*.html"
      withContent: true
```

### Example 5: Error on no matching files

Fail the pipeline if no files match the pattern.

```yaml
sources:
  - name: required_files
    module: files
    parameters:
      pattern: "gs://my-bucket/critical-data/*.avro"
      emptyMatchTreatment: DISALLOW
```

### Example 6: List local files

List files from the local file system.

```yaml
sources:
  - name: local_files
    module: files
    parameters:
      pattern: "/data/input/*.json"
```

### Example 7: File listing with downstream processing

List files and use a subsequent transform to process them.

```yaml
sources:
  - name: files
    module: files
    parameters:
      pattern: "gs://my-bucket/logs/**/*.log"
      filter:
        key: filename
        op: contains
        value: "error"

transforms:
  - name: file_info
    module: select
    inputs:
      - files
    parameters:
      fields:
        - name: path
          field: resource
        - name: size_mb
          expression: "sizeBytes / 1048576.0"
          type: double
        - name: modified
          field: lastModified

sinks:
  - name: output
    module: bigquery
    inputs:
      - file_info
    parameters:
      table: "myproject.mydataset.file_inventory"
```
