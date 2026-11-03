---
type: Transform Module
title: PDF Transform Module
description: Reads PDF documents with Apache PDFBox. The extract operation outputs text and metadata per document or per page, with optional word / line boxes, form field values, outline, links and signatures, or one record per embedded image or attached file. The render operation turns pages into PNG / JPEG images (optionally only the pages without text, for OCR or multimodal models), and split cuts a document into smaller PDFs so that the pages of a large document are processed in parallel. The PDF is read from a bytes field or from a path (gs://, s3://, local, http(s)://) in a string field.
tags: [transform, pdf, text, extraction, image, render, ocr, split, form, attachment, pdfbox]
timestamp: 2026-11-04T00:00:00Z
---

# PDF Transform Module

Transform Module that reads PDF documents using [Apache PDFBox](https://pdfbox.apache.org/). One module handles several operations, chosen by `operation`:

| operation | what it does | records per input PDF |
|-----------|--------------|------------------------|
| `extract` (default) | Text, metadata and document structures | depends on `unit`: one per document, page, embedded image or attached file |
| `render` | Renders pages to PNG / JPEG images | one per selected page |
| `split` | Cuts the document into PDFs of `split.pagesPerChunk` pages | one per chunk |

Each output record holds the input fields followed by the fields of the operation (see [Output fields](#output-fields)). When the PDF comes from a **bytes** field, that field is left out of the output (`keepSource: true` keeps it), so a document expanded into many pages is not copied into every page record.

The PDF is read from the field named by `field`:

- a **bytes** field holds the PDF file itself (e.g. the `content` of the [files source](../source/files.md) with `withContent: true`);
- a **string** field holds its location: `gs://...`, `s3://...`, a local path, or an `http(s)://` URL (downloaded with retries; set `auth` and `http.allowedHosts` for endpoints that need credentials).

Errors are routed as module failures (`failFast` / `outputFailure`): a file that cannot be read or parsed fails its input record. With `unit: page`, `unit: image` and `render`, a page that fails is reported on its own and the other pages are still output. With `unit: document`, a failing page fails the whole document.

In batch mode the input is reshuffled before processing, and the output of `split` is reshuffled again, so that a following `render` or `extract` spreads the pages of a large document over the workers.

## Transform module common parameters

| parameter  | optional | type                              | description                                                    |
|------------|----------|-----------------------------------|----------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.              |
| module     | required | String                            | Specified `pdf`                                                |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be used as input.            |
| waits      | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.  |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                    |
| failFast   | optional | Boolean                           | Stop the pipeline on the first failure (default `true` in batch, `false` in streaming). |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                    |

## PDF transform module parameters

| parameter  | optional | type                                  | description |
|------------|----------|---------------------------------------|-------------|
| operation  | optional | Enum                                  | `extract` (default), `render` or `split`. |
| field      | required | String                                | Input field holding the PDF: `bytes` (file content) or `string` (gs:// / s3:// / local path / http(s):// URL). |
| unit       | optional | Enum                                  | For `extract`: `document` (default), `page`, `image` or `attachment`. |
| include    | optional | Array<Enum\>                          | What to add to `document` / `page` records and to `render` records: `text`, `metadata`, `lines`, `words`, `links`, and for `unit: document` only `forms`, `outline`, `signatures`. Default: `[text, metadata]` for `unit: document`, `[text]` for `unit: page`, none for `render`. |
| pages      | optional | String                                | Pages to process, 1-based: `"1-3,5,10-"` (`N-` runs to the last page). Pages beyond the document are ignored. Default: all pages. |
| password   | optional | String                                | Password of an encrypted PDF. Use `${utils.secrets.get(...)}` rather than writing it in the config. |
| prefix     | optional | String                                | Prefix added to the names of the output fields of the operation (e.g. `pdf_` gives `pdf_text`). Required when they collide with input field names — the module refuses to build otherwise (e.g. the `content` of `split`, `unit: image` and `unit: attachment` collides with the `content` field of the [files source](../source/files.md), and the `pageCount` of `split` with that of a following `render`). Default: `""`. |
| keepSource | optional | Boolean                               | Keep the bytes `field` in the output. Default: `false`. A string `field` (a path) is always kept. |
| text       | optional | [Text](#text-parameters)              | Text extraction options. |
| render     | optional | [Render](#render-parameters)          | Options of `operation: render`. |
| images     | optional | [Images](#images-parameters)          | Options of `unit: image`. |
| split      | optional | [Split](#split-parameters)            | Options of `operation: split`. |
| auth       | optional | Auth                                  | Credentials for `http(s)://` downloads, as the `auth` of the [http source](../source/http.md) (`basic`, `bearer`, `apiKey`, `oauth2`, `gcpOidc`, `gcpOauth`). Requires `http.allowedHosts`. |
| http       | optional | Map                                   | HTTP client options for downloads: `allowedHosts` (hosts a download may go to), `proxy` (`host:port`), `version` (`HTTP_2` default / `HTTP_1_1`), `followRedirects` (`normal` default / `never` / `always`). |
| timeout    | optional | Map                                   | Download timeouts: `connect` (default `10s`) and `request` (default `60s`). |
| select     | optional | Array<[Select](../common/select.md)\> | Select functions applied to each output record (input fields + operation fields). When given, the output schema holds only the selected fields. |

### Text parameters

| parameter      | optional | type    | description |
|----------------|----------|---------|-------------|
| sortByPosition | optional | Boolean | Order the text by its position on the page rather than by the order in the content stream. Default: `true`. |
| lineSeparator  | optional | String  | Written between lines. Default: `"\n"`. |
| pageSeparator  | optional | String  | Written between pages in `unit: document`. Default: `"\n"`. |

### Render parameters

| parameter     | optional | type    | description |
|---------------|----------|---------|-------------|
| dpi           | optional | Float   | Resolution. Default: `150`. |
| format        | optional | Enum    | `png` (default) or `jpeg`. |
| color         | optional | Enum    | `rgb` (default), `gray` or `binary`. |
| quality       | optional | Float   | JPEG quality, 0 to 1. Default: `0.9`. |
| maxPixels     | optional | Integer | Largest image (width × height). A larger page is rendered at a lower dpi; the dpi used is in the `dpi` field. Default: `25000000`. |
| when          | optional | Enum    | `always` (default) renders every selected page. `noText` renders only the pages whose text is shorter than `minTextLength` — scanned pages — and outputs the others with `image: null`, so that they can be routed apart (OCR for the scans, the text for the rest). |
| minTextLength | optional | Integer | For `when: noText`: a page with fewer characters (after trimming) has no text. Default: `1`. |

### Images parameters

| parameter | optional | type    | description |
|-----------|----------|---------|-------------|
| minWidth  | optional | Integer | Skip images narrower than this (pixels), e.g. icons and rules. Default: `1`. |
| minHeight | optional | Integer | Skip images lower than this (pixels). Default: `1`. |
| format    | optional | Enum    | `original` (default): JPEG (gray / RGB) and JPEG 2000 images are output as stored in the PDF, the others as PNG. `png`: every image as PNG. |

### Split parameters

| parameter     | optional | type    | description |
|---------------|----------|---------|-------------|
| pagesPerChunk | optional | Integer | Pages per output PDF. Default: `1`. |

## Output fields

Names are given without `prefix`. Coordinates (`x0`, `y0`, `x1`, `y1`) are in points (1/72 inch) with the origin at the **top-left** corner of the page's crop box; divide by the page `width` / `height` to normalize.

### extract, unit: document

| field     | type  | description |
|-----------|-------|-------------|
| pageCount | Int64 | Number of pages of the document. |
| + include fields | | See [Include fields](#include-fields). Text and lines / words cover the selected `pages`. |

### extract, unit: page

| field     | type    | description |
|-----------|---------|-------------|
| page      | Int64   | Page number (1-based). |
| pageCount | Int64   | Number of pages of the document. |
| width     | Float64 | Page width (points, crop box). |
| height    | Float64 | Page height (points, crop box). |
| rotation  | Int64   | Page rotation in degrees (0 / 90 / 180 / 270). |
| + include fields | | See [Include fields](#include-fields) (`forms`, `outline`, `signatures` are not available). |

### extract, unit: image

One record per image drawn on a selected page (also images inside form XObjects, and inline images). An image drawn several times on a page is output once per page.

| field            | type    | description |
|------------------|---------|-------------|
| page             | Int64   | Page number. |
| pageCount        | Int64   | Number of pages of the document. |
| index            | Int64   | Order of the image on its page, from 0. |
| format           | String  | `jpeg`, `jp2` or `png`: the encoding of `content`. |
| width / height   | Int64   | Image size in pixels. |
| bitsPerComponent | Int64   | Bits per color component. |
| colorSpace       | String  | Color space name, e.g. `DeviceRGB`, `DeviceGray`, `ICCBased`. |
| x0, y0, x1, y1   | Float64 | Box the image is drawn in on the page. |
| content          | Bytes   | The image file. |

### extract, unit: attachment

One record per embedded file: files of the document (`page` is null) and file attachment annotations on the selected pages. Useful for e-invoices that carry their data as an attached XML (ZUGFeRD / Factur-X).

| field            | type      | description |
|------------------|-----------|-------------|
| page             | Int64     | Page of the annotation, null for files of the document. |
| pageCount        | Int64     | Number of pages of the document. |
| name             | String    | File name. |
| description      | String    | File description. |
| mimeType         | String    | MIME type declared in the PDF. |
| size             | Int64     | Size in bytes. |
| creationDate     | Timestamp | Creation date declared in the PDF. |
| modificationDate | Timestamp | Modification date declared in the PDF. |
| content          | Bytes     | The file. |

### render

The fields of `unit: page`, plus:

| field       | type    | description |
|-------------|---------|-------------|
| hasText     | Boolean | Whether the page has at least `minTextLength` characters of text. |
| image       | Bytes   | The rendered page (PNG / JPEG); null when `when: noText` skipped the page. |
| imageWidth  | Int64   | Image width in pixels. |
| imageHeight | Int64   | Image height in pixels. |
| dpi         | Float64 | Resolution used (lower than `render.dpi` when `maxPixels` capped it). |
| + include fields | | As for `unit: page`. |

### split

| field     | type  | description |
|-----------|-------|-------------|
| content   | Bytes | The chunk as a PDF file. |
| startPage | Int64 | First page of the chunk, in the source document. |
| endPage   | Int64 | Last page of the chunk, in the source document. |
| pageCount | Int64 | Number of pages of the source document. |

### Include fields

| include    | fields |
|------------|--------|
| text       | `text` (String): the text of the page(s). |
| metadata   | `fileSize` (Int64), `version` (String), `encrypted` (Boolean), `title`, `author`, `subject`, `keywords`, `creator`, `producer`, `trapped` (String), `creationDate`, `modificationDate` (Timestamp). |
| lines      | `lines`: Array<{`page`, `text`, `x0`, `y0`, `x1`, `y1`, `fontSize`}> — one per text line, in reading order. |
| words      | `words`: Array<{`page`, `text`, `x0`, `y0`, `x1`, `y1`, `fontName`, `fontSize`}> — the runs of text PDFBox separates with a space. `fontName` drops the subset tag (`ABCDEF+`). |
| links      | `links`: Array<{`page`, `uri`, `targetPage`, `x0`, `y0`, `x1`, `y1`}> — link annotations: `uri` for web links, `targetPage` for links inside the document. |
| forms      | `forms`: Array<{`name`, `type`, `value`}> — AcroForm fields: fully qualified name, type (`Tx` text, `Btn` button / checkbox, `Ch` choice, `Sig` signature) and value (null for `Sig` fields: see `signatures`). |
| outline    | `outline`: Array<{`level`, `title`, `page`}> — bookmarks in document order, `level` from 1. |
| signatures | `signatures`: Array<{`name`, `signDate`, `reason`, `location`, `contactInfo`, `subFilter`}> — signature dictionaries (the signatures are not verified). |

## Notes

- Scanned PDFs often store pages as JBIG2 or CCITT images; both are decoded. JPEG 2000 images can be extracted (`format: original`) but not rendered or re-encoded.
- Fonts that a PDF uses without embedding them are replaced by a fallback font when rendering; text extraction is not affected.
- Text is only what the PDF holds as text. For scanned pages, use `render` with `when: noText` and read the images with an OCR model ([onnx transform](onnx.md)) or a multimodal model.

## Example: extract the text of every page

Lists PDF files with the [files source](../source/files.md) and writes one row per page to BigQuery. The PDF is read by the transform from the `resource` path.

```yaml
sources:
  - name: files
    module: files
    parameters:
      pattern: "gs://my-bucket/pdfs/**/*.pdf"

transforms:
  - name: pages
    module: pdf
    inputs: [files]
    parameters:
      field: resource
      unit: page
      include: [text, lines]
      select:
        - name: resource
        - name: page
        - name: text
        - name: lines

sinks:
  - name: output
    module: bigquery
    inputs: [pages]
    parameters:
      table: "myproject.mydataset.pdf_pages"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
```

## Example: document metadata, form values and outline

```yaml
transforms:
  - name: documents
    module: pdf
    inputs: [files]
    parameters:
      field: resource
      include: [metadata, forms, outline]
      prefix: pdf_
```

## Example: render only the scanned pages of large documents

`split` cuts each document into single pages and reshuffles them, so that `render` runs page-parallel. Pages that have text keep it in `text`; scanned pages get an `image` to read with an OCR model. The `prefix` of `split` keeps its `content` and `pageCount` apart from the `content` of the files source and the `pageCount` of `render`.

```yaml
transforms:
  - name: chunks
    module: pdf
    inputs: [files]
    parameters:
      operation: split
      field: resource
      prefix: chunk_
      split:
        pagesPerChunk: 1
  - name: rendered
    module: pdf
    inputs: [chunks]
    parameters:
      operation: render
      field: chunk_content
      include: [text]
      render:
        dpi: 200
        color: gray
        when: noText
  - name: routed
    module: partition
    inputs: [rendered]
    parameters:
      partitions:
        - name: scanned
          filter:
            key: hasText
            op: "="
            value: false
        - name: digital
          filter:
            key: hasText
            op: "="
            value: true
```

The `chunk_startPage` of a chunk is the page number in the source document; the `page` of the rendered record is the page within the chunk (1 for single pages).

## Example: save the images and attachments of the documents

The files source has a `content` field of its own, so the outputs are given a `prefix`.

```yaml
transforms:
  - name: images
    module: pdf
    inputs: [files]
    parameters:
      field: resource
      unit: image
      prefix: image_
      images:
        minWidth: 64
        minHeight: 64
  - name: attachments
    module: pdf
    inputs: [files]
    parameters:
      field: resource
      unit: attachment
      prefix: attachment_

sinks:
  - name: image_files
    module: files
    inputs: [images]
    parameters:
      output: "gs://my-bucket/images/${filename}/${image_page}_${image_index}.${image_format}"
      content:
        field: image_content
        format: bytes
  - name: attachment_files
    module: files
    inputs: [attachments]
    parameters:
      output: "gs://my-bucket/attachments/${filename}/${attachment_name}"
      content:
        field: attachment_content
        format: bytes
```

## Example: download from URLs

A string field with an `http(s)://` URL is downloaded. With credentials, list the hosts they may be sent to.

```yaml
transforms:
  - name: reports
    module: pdf
    inputs: [report_urls]
    parameters:
      field: url
      unit: page
      auth:
        type: bearer
        token: "${utils.secrets.get('projects/my-project/secrets/report-token/versions/latest')}"
      http:
        allowedHosts: [reports.example.com]
```
