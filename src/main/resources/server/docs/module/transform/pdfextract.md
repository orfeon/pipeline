---
type: Transform Module
title: PDFExtract Transform Module
description: Extracts text and document metadata (title, author, page count, dates, etc.) from PDF content using Apache PDFBox. The PDF is read from a byte-array field or from a GCS path given in a string field. Extracted fields are appended to the input record, with an optional field-name prefix and optional select post-processing. Parse failures are reported in-record via Failed/ErrorMessage fields, and EPUB content is handled as a fallback.
tags: [transform, pdfextract, pdf, text, extraction, pdfbox, epub]
timestamp: 2026-08-06T00:00:00Z
---

# PDFExtract Transform Module

Transform Module that extracts text content and document metadata from PDF files using [Apache PDFBox](https://pdfbox.apache.org/). The extracted fields are appended to each input record, so downstream modules see the original fields plus the PDF fields.

Supports:

- **Two content sources** - The `field` parameter names either a **bytes field** holding the PDF file content directly, or a **string field** holding the GCS path of the PDF file (`gs://bucket/path/file.pdf`; App Engine style `/gs/bucket/...` paths are also accepted and converted). Other string values (including `http(s)://` URLs) are not downloaded and produce a failed record.
- **In-record error reporting** - When PDF parsing fails, the record is still emitted with `Failed: true`, empty `Content`, and the error in `ErrorMessage`; per-page extraction errors are counted in `ErrorPageCount`. Unexpected per-record errors (e.g. GCS read failure) are routed as module failures according to `failFast` / `outputFailure`.
- **EPUB fallback** - If the content is not a valid PDF but is a ZIP archive, the module attempts to read it as an EPUB and fills `Content`, `Page`, and `FileByteSize`.
- **Optional select post-processing** - A [Select](../common/select.md) definition can refine, rename, or compute fields from the combined record; when specified, the output schema contains only the selected fields.
- In batch mode the input is reshuffled before extraction to spread the (potentially heavy) PDF parsing across workers.

## Transform module common parameters

| parameter  | optional | type                              | description                                                    |
|------------|----------|-----------------------------------|----------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.              |
| module     | required | String                            | Specified `pdfextract`                                         |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be used as input.            |
| waits      | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.  |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                    |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                    |

## PDFExtract transform module parameters

| parameter | optional | type                                  | description                                                                                                                                                                        |
|-----------|----------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| field     | required | String                                | Name of the input field containing the PDF. If the field type is `bytes`, its value is the PDF file content; if the field type is `string`, its value is the GCS path of the PDF file. |
| prefix    | optional | String                                | Prefix added to the names of the extracted PDF fields listed below (e.g. `prefix: pdf_` produces `pdf_Content`, `pdf_Page`, ...). Default: `""` (no prefix).                        |
| select    | optional | Array<[Select](../common/select.md)\> | Select functions applied to the combined record (input fields + extracted PDF fields). When specified, the output schema contains only the fields produced by the select functions.  |

## Fields extracted from the PDF file

Without `select`, the output schema is these fields (each prefixed with `prefix`) plus all input fields.

| field            | type      | description                                                                    |
|------------------|-----------|--------------------------------------------------------------------------------|
| Content          | String    | Text extracted from the PDF file (all pages joined).                           |
| FileByteSize     | Long      | Byte size of the PDF file.                                                     |
| Page             | Long      | Number of pages in the PDF file.                                               |
| Version          | String    | PDF file version.                                                              |
| Encrypted        | Boolean   | Whether the PDF file is encrypted.                                             |
| Title            | String    | Title of the document.                                                         |
| Author           | String    | Name of the person who created the document.                                   |
| Subject          | String    | Subject of the document.                                                       |
| Keywords         | String    | Keywords of the document.                                                      |
| Creator          | String    | The original creation tool if converted from a format other than PDF.          |
| Producer         | String    | The conversion tool if converted from a format other than PDF.                 |
| CreationDate     | Timestamp | The datetime when the document was created.                                    |
| ModificationDate | Timestamp | The last datetime the document was updated.                                    |
| Trapped          | String    | Whether the document has been modified to include trapping information.        |
| Failed           | Boolean   | `true` if parsing the PDF (and EPUB fallback) failed entirely.                 |
| ErrorPageCount   | Long      | Number of pages that failed text extraction.                                   |
| ErrorMessage     | String    | Error messages from PDF parsing, if any.                                       |

## Example: extract text from PDFs stored in GCS

Reads records whose `pdf_path` field holds a GCS path to a PDF file, extracts the text and metadata, and writes selected fields to BigQuery.

```yaml
sources:
  - name: documents
    module: bigquery
    parameters:
      query: "SELECT doc_id, pdf_path FROM `myproject.mydataset.documents`"

transforms:
  - name: extract
    module: pdfextract
    inputs:
      - documents
    parameters:
      field: pdf_path
      select:
        - name: doc_id
        - name: content
          field: Content
        - name: page_count
          field: Page
        - name: title
          field: Title

sinks:
  - name: output
    module: bigquery
    inputs:
      - extract
    parameters:
      table: "myproject.mydataset.document_texts"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
```

## Example: extract from a bytes field with a prefix

Reads PDF files as binary content with the `storage` source and appends the extracted fields with a `pdf_` prefix.

```yaml
sources:
  - name: pdf_files
    module: storage
    parameters:
      input: "gs://my-bucket/pdfs/*.pdf"
      format: binary

transforms:
  - name: extract
    module: pdfextract
    inputs:
      - pdf_files
    parameters:
      field: content
      prefix: pdf_

sinks:
  - name: output
    module: storage
    inputs:
      - extract
    parameters:
      output: "gs://my-bucket/extracted/"
      format: json
```
