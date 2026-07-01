---
type: Sink Module
title: Files Sink Module
description: Writes each input record as an individual file to Cloud Storage (GCS) or local file system. The output path and content are dynamically determined per record using FreeMarker template expressions.
tags: [sink, files, batch, streaming, gcs, storage, write, export]
timestamp: 2026-06-21T14:30:00Z
---

# Files Sink Module

Sink Module for writing each input record as an individual file to Cloud Storage (GCS) or the local file system. The output file path is constructed per record using FreeMarker template expressions that reference input field values, allowing dynamic file naming.

This module differs from the [Storage Sink Module](storage.md): the Storage module writes the entire PCollection as a batch of files in a structured format (Avro, Parquet, CSV, JSON), while the Files module writes **one file per input record** with per-record dynamic paths and content.

This module also produces output records with the written file path, so downstream steps can use the results.

## Sink module common parameters

| parameter | optional | type                | description                                                           |
|-----------|----------|---------------------|-----------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                     |
| module    | required | String              | Specified `files`                                                     |
| inputs    | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| wait      | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy  | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Files sink module parameters

| parameter  | optional | type                | description                                                                                                                                                                                                                          |
|------------|----------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| output     | required | String              | Output file path template. Supports FreeMarker expressions referencing input field values (e.g. `gs://bucket/path/${fieldName}.json`). Paths starting with `gs://` are written to Cloud Storage; otherwise written to the local file system. |
| content    | required | [Content](#content-parameters) | Specifies what to write as the file content. See [Content parameters](#content-parameters).                                                                                                                                |
| attributes | optional | Map<String,String\> | Metadata attributes to set on the output file (GCS only). Values support FreeMarker template expressions. Common attributes: `contentType`, `cacheControl`, `contentDisposition`, etc.                                               |
| reshuffle  | optional | Boolean             | If `true`, applies a reshuffle before writing to redistribute work across workers. Default: `false`.                                                                                                                                  |

### Content parameters

The `content` parameter defines how the file body is generated for each record. Specify **one of** `field`, `text`, or neither (which serializes the entire record).

| parameter | optional | type   | description                                                                                                                                                                                                    |
|-----------|----------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| format    | optional | Enum   | Output format. Values: `csv`, `json`, `avro`, `text`, `bytes`. Default is auto-detected: `text` if `text` is set, inferred from the field type if `field` is set (e.g. bytes→`bytes`, json/element→`json`), otherwise `json`. |
| field     | optional | String | Input field name whose value is written as the file content. Supports nested fields using dot notation. Cannot be used together with `text`.                                                                    |
| text      | optional | String | FreeMarker template string to generate the file content. Input field values are available as template variables. Cannot be used together with `field`.                                                           |

When neither `field` nor `text` is specified, the entire input record is serialized using the specified `format`:
- `csv` - Serializes the record as a CSV line.
- `json` - Serializes the record as a JSON object.
- `avro` - Serializes the record in Avro binary format.

When `field` is specified, the content behavior depends on the field type and `format`:
- `text` / `csv` - Calls `toString()` on the field value.
- `json` - Converts element/map fields to JSON; string/json fields pass through; other types use `toString()`.
- `avro` - Only supported for `element` typed fields.
- `bytes` - Only supported for `bytes` typed fields (written as raw bytes).

### Content-Type handling (GCS)

When writing to GCS, the Content-Type is set automatically based on the format if not explicitly provided via `attributes`:

| format | default Content-Type    |
|--------|------------------------|
| csv    | `text/csv`             |
| text   | `text/plain`           |
| json   | `application/json`     |
| avro   | `avro/binary`          |
| bytes  | `application/octet-stream` |

You can override it by specifying `contentType` in the `attributes` parameter.

## Output schema

After writing, each record is output with the following schema, allowing downstream steps to process the results.

| field     | type      | description                          |
|-----------|-----------|--------------------------------------|
| output    | STRING    | The file path that was written to.   |
| timestamp | TIMESTAMP | The timestamp when the file was written. |

## FreeMarker template expressions

The `output`, `attributes` values, and `content.text` parameters support FreeMarker template expressions. Input field values are available as variables using `${fieldName}` syntax. Nested fields can be accessed with dot notation (e.g. `${parent.child}`).

## Examples

### Example 1: Write JSON files to GCS per record

Serialize each record as JSON and write to GCS with a dynamic path.

```yaml
sources:
  - name: users
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email FROM `myproject.mydataset.users`"

sinks:
  - name: export_files
    module: files
    inputs:
      - users
    parameters:
      output: "gs://my-bucket/exports/users/${user_id}.json"
      content:
        format: json
```

### Example 2: Write a specific field as file content

Write the value of a specific field (e.g. HTML content) as the file body.

```yaml
sinks:
  - name: html_files
    module: files
    inputs:
      - pages
    parameters:
      output: "gs://my-bucket/pages/${page_id}.html"
      content:
        format: text
        field: html_content
      attributes:
        contentType: "text/html"
```

### Example 3: Write CSV from Google Sheets via Drive source

Export Google Sheets data to individual CSV files on GCS.

```yaml
sources:
  - name: sheets_source
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      files:
        - id: spreadsheet-id
          ranges:
            - Sheet1!A3:G
      content: true
      flatten: spreadsheet.sheets

sinks:
  - name: csv_export
    module: files
    inputs:
      - sheets_source
    parameters:
      output: "gs://my-bucket/drive/${id}/${spreadsheet.sheets.sheetId}.csv"
      content:
        format: csv
        field: spreadsheet.sheets.content
      attributes:
        contentType: "text/csv"
```

### Example 4: Write bytes field as binary files

Write binary content (e.g. images, PDFs) stored in a bytes field.

```yaml
sinks:
  - name: binary_export
    module: files
    inputs:
      - documents
    parameters:
      output: "gs://my-bucket/documents/${doc_id}.pdf"
      content:
        format: bytes
        field: file_content
      attributes:
        contentType: "application/pdf"
```

### Example 5: Generate file content with a FreeMarker template

Use a template to construct custom text content.

```yaml
sinks:
  - name: reports
    module: files
    inputs:
      - report_data
    parameters:
      output: "gs://my-bucket/reports/${report_id}.txt"
      content:
        format: text
        text: |
          Report: ${title}
          Date: ${report_date}
          Summary: ${summary}
          Total: ${total_amount}
```

### Example 6: Write to local file system

Write files to the local file system instead of GCS.

```yaml
sinks:
  - name: local_export
    module: files
    inputs:
      - data
    parameters:
      output: "/tmp/output/${category}/${id}.json"
      content:
        format: json
```

### Example 7: Chain with downstream steps using output

Use the output schema (file path and timestamp) in a downstream step.

```yaml
sinks:
  - name: write_files
    module: files
    inputs:
      - data
    parameters:
      output: "gs://my-bucket/data/${id}.json"
      content:
        format: json

transforms:
  - name: log_written
    module: beamsql
    inputs:
      - write_files
    parameters:
      sql: >
        SELECT output AS file_path, `timestamp` AS written_at
        FROM `write_files`
```
