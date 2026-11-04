---
type: Transform Module
title: PDF Transform Module
description: Reads and writes PDF documents with Apache PDFBox. The extract operation outputs text and metadata per document or per page, with optional word / line boxes, form field values, outline, links and signatures, or one record per embedded image or attached file. The render operation turns pages into PNG / JPEG images (optionally only the pages without text, for OCR or multimodal models), and split cuts a document into smaller PDFs so that the pages of a large document are processed in parallel. The generate operation fills a PDF form template with the values of each record or makes a PDF from images, and edit changes PDFs by steps - merge, select / reorder and rotate pages, overlay text and images (page numbers, watermarks, stamps), fill forms, set metadata, encrypt and decrypt. The PDF is read from a bytes field or from a path (gs://, s3://, local, http(s)://) in a string field.
tags: [transform, pdf, text, extraction, image, render, ocr, split, form, attachment, generate, merge, watermark, encrypt, pdfbox]
timestamp: 2026-11-04T00:00:00Z
---

# PDF Transform Module

Transform Module that reads and writes PDF documents using [Apache PDFBox](https://pdfbox.apache.org/). One module handles several operations, chosen by `operation`:

| operation | what it does | records per input PDF |
|-----------|--------------|------------------------|
| `extract` (default) | Text, metadata and document structures | depends on `unit`: one per document, page, embedded image or attached file |
| `render` | Renders pages to PNG / JPEG images | one per selected page |
| `split` | Cuts the document into PDFs of `split.pagesPerChunk` pages | one per chunk |
| `generate` | Makes a PDF from each record: fills a form template, or puts images on pages; then applies `steps` | one per input record |
| `edit` | Changes the PDF by `steps`: merge, pages, rotate, overlay, form, metadata, encrypt, decrypt | one per input record |

Each output record holds the input fields followed by the fields of the operation (see [Output fields](#output-fields)). When the PDF comes from a **bytes** field, that field is left out of the output (`keepSource: true` keeps it), so a document expanded into many pages is not copied into every page record.

The PDF is read from the field named by `field`:

- a **bytes** field holds the PDF file itself (e.g. the `content` of the [files source](../source/files.md) with `withContent: true`);
- a **string** field holds its location: `gs://...`, `s3://...`, a local path, or an `http(s)://` URL (downloaded with retries; set `auth` and `http.allowedHosts` for endpoints that need credentials).

For `edit` the field may also be an **array** of them (e.g. collected with `array_agg` of the [aggregation transform](aggregation.md)): the PDFs are merged in that order. For `generate` with `type: images`, the field holds the images (bytes, paths, or an array of either), and `type: form` reads no field.

Errors are routed as module failures (`failFast` / `outputFailure`): a file that cannot be read or parsed fails its input record. With `unit: page`, `unit: image` and `render`, a page that fails is reported on its own and the other pages are still output. With `unit: document`, a failing page fails the whole document. A failure record carries the whole input record so that it can be reprocessed; with a bytes `field` that includes the PDF itself, once per failing page. For large files, prefer a string path `field` to keep failure records small.

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
| operation  | optional | Enum                                  | `extract` (default), `render`, `split`, `generate` or `edit`. |
| field      | selective | String                               | Input field holding the PDF: `bytes` (file content) or `string` (gs:// / s3:// / local path / http(s):// URL); for `edit` also an array of them. For `generate`: the images of `type: images`; not used by `type: form`. |
| unit       | optional | Enum                                  | For `extract`: `document` (default), `page`, `image` or `attachment`. |
| include    | optional | Array<Enum\>                          | What to add to `document` / `page` records and to `render` records: `text`, `metadata`, `lines`, `words`, `links`, and for `unit: document` only `forms`, `outline`, `signatures`. Default: `[text, metadata]` for `unit: document`, `[text]` for `unit: page`, none for `render`. |
| pages      | optional | String                                | Pages to process, 1-based: `"1-3,5,10-"` (`N-` runs to the last page). Pages beyond the document are ignored. Default: all pages. Not for `generate` / `edit`, whose steps take their own `pages`. |
| password   | optional | String                                | Password of an encrypted PDF (also of the PDFs an `edit` reads). Use `${utils.secrets.get(...)}` rather than writing it in the config. |
| prefix     | optional | String                                | Prefix added to the names of the output fields of the operation (e.g. `pdf_` gives `pdf_text`). Required when they collide with input field names — the module refuses to build otherwise (e.g. the `content` of `split`, `unit: image` and `unit: attachment` collides with the `content` field of the [files source](../source/files.md), and the `pageCount` of `split` with that of a following `render`). Default: `""`. |
| keepSource | optional | Boolean                               | Keep the bytes `field` in the output. Default: `false`. A string `field` (a path) is always kept. |
| text       | optional | [Text](#text-parameters)              | Text extraction options. |
| render     | optional | [Render](#render-parameters)          | Options of `operation: render`. |
| images     | optional | [Images](#images-parameters)          | Options of `unit: image`. |
| split      | optional | [Split](#split-parameters)            | Options of `operation: split`. |
| generate   | selective | [Generate](#generate-parameters)     | Required for `operation: generate`: what to make the PDF from. |
| steps      | selective | Array<[Step](#steps)\>               | For `generate` (optional) and `edit` (required): changes applied to the document in order. |
| fonts      | optional | Array<[Font](#fonts)\>                | TrueType fonts for the text that `generate` / `edit` write. Without them only the Standard 14 fonts are available, which cover Western European (WinAnsi) characters only. |
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

### Generate parameters

| parameter   | optional | type    | description |
|-------------|----------|---------|-------------|
| type        | required | Enum    | `form`: fill the form fields (AcroForm) of `template` with each record. `images`: one page per image of `field`. |
| template    | selective | String | For `form`: path of the template PDF (gs:// / s3:// / local), read once per worker. |
| values      | optional | Map<String,String\> | For `form`: form field name (fully qualified, e.g. `applicant.name`) → value, a [FreeMarker template](#templates) over the record. Without `values`, every form field whose name is an input field gets that field's value. A name that is not in the template fails the pipeline at startup. |
| flatten     | optional | Boolean | For `form`: turn the fields into page content, so that the values can no longer be changed. Default: `false`. |
| font        | optional | String  | For `form`: font for the values, a Standard 14 name or a [`fonts`](#fonts) name. Default: each field's own font. |
| pageSize    | optional | Enum    | For `images`: `image` (default: the page has the size of the image at `dpi`), `A3`, `A4`, `A5`, `letter` or `legal` (the image is scaled to fit inside the margins, keeping its aspect ratio, and centered). |
| orientation | optional | Enum    | For `images` with a fixed `pageSize`: `auto` (default: landscape for images wider than high), `portrait` or `landscape`. |
| dpi         | optional | Float   | For `images` with `pageSize: image`: resolution of the images. Default: `96`. |
| margin      | optional | Float   | For `images`: margin around the image, in points. Default: `0`. |

Images: JPEG is embedded as is (no re-encoding); PNG, GIF, BMP and TIFF are embedded losslessly (a multi-page TIFF gives its first page only).

### Steps

Each step has a `type` and the parameters of that type. `pages` (as the `pages` parameter: `"1-3,5,10-"`) selects the pages a step works on; default: all pages.

| type     | parameters | description |
|----------|------------|-------------|
| form     | `values`, `flatten`, `font` | Fills form fields, as `generate.type: form` (without `values`: the input fields named like the form fields). |
| merge    | `field`, `password` | Appends the PDFs of `field` (bytes / path, or an array of them) after the document. Null values are skipped. `password` defaults to the module's `password`. |
| pages    | `pages` (required) | Keeps the selected pages in the order written: `"3,1-2"` gives pages 3, 1, 2 and drops the others. A page selected twice is an error. Bookmarks and form fields of dropped pages remain in the document. |
| rotate   | `angle` (required), `pages` | Rotates pages clockwise by `angle` (a multiple of 90, negative turns counterclockwise), added to their current rotation. |
| overlay  | `items` (required), `pages`, `layer` | Draws [items](#overlay-items) on each page: page numbers, headers / footers, watermarks, stamps, logos. `layer`: `foreground` (default, over the page content) or `background` (under it; hidden by opaque page content such as scans). |
| metadata | `title`, `author`, `subject`, `keywords`, `creator`, `producer` | Sets the document information. Each value is a [template](#templates). |
| encrypt  | `ownerPassword`, `userPassword`, `keyLength`, `permissions` | Encrypts the saved PDF. `userPassword` is needed to open it (default: none, it opens freely but `permissions` apply); `ownerPassword` lifts the permissions (default: a random one nobody knows). Both are [templates](#templates), e.g. a per-record password. `keyLength`: `256` (default, AES-256), `128` or `40`. `permissions` (all `true` by default): `print`, `printHighQuality`, `modify`, `copy`, `annotate`, `fillForms`, `extractForAccessibility`, `assemble`. |
| decrypt  | | Saves the PDF without encryption (open it with `password`). |

An encrypted PDF (opened with `password`) can only be saved again with a `decrypt` or an `encrypt` step; without one, its record fails.

#### Overlay items

Each item draws exactly one of `text`, `image` or `imageField`. Positions are in points on the page **as displayed**: the crop box with its rotation applied, so `bottom` stays at the bottom of a page rotated by `rotate`.

| parameter  | optional | type    | description |
|------------|----------|---------|-------------|
| text       | selective | String | Text, a [template](#templates) with the record's fields and `page` / `pageCount` (e.g. `"${page} / ${pageCount}"`). `\n` starts a new line. |
| image      | selective | String | Path of an image (PNG / JPEG / ...), read once per worker. |
| imageField | selective | String | Bytes field of the record holding an image (e.g. a per-record stamp or signature image). |
| position   | optional | Enum    | Where the item's box goes: `topLeft` (default), `top`, `topRight`, `left`, `center`, `right`, `bottomLeft`, `bottom`, `bottomRight`. Lines of a text are aligned the same way (left / center / right). |
| margin     | optional | Float   | Distance of the box from the page edges at `position`. Default: `0`. |
| x, y       | optional | Float   | Offset of the box after positioning, in points (`x` to the right, `y` downward). Default: `0`. |
| font       | optional | String  | Standard 14 font name (`Helvetica` (default), `Helvetica-Bold`, `Times-Roman`, `Courier`, ...) or a [`fonts`](#fonts) name. |
| fontSize   | optional | Float   | Default: `12`. |
| lineHeight | optional | Float   | Line spacing, times `fontSize`. Default: `1.2`. |
| color      | optional | String  | Text color, `#RRGGBB` or `#RGB`. Default: `#000000`. |
| opacity    | optional | Float   | 0 (invisible) to 1 (default). |
| rotate     | optional | Float   | Degrees counterclockwise, around the center of the box (e.g. `45` for a diagonal watermark at `center`). |
| width, height | optional | Float | Size of an image in points. With one of them the other follows the aspect ratio. Default: the image's pixel size. |

### Fonts

| parameter | optional | type   | description |
|-----------|----------|--------|-------------|
| name      | required | String | Name to use in `font` (not a Standard 14 font name). |
| path      | required | String | TrueType font file (`.ttf`) or collection (`.ttc`), gs:// / s3:// / local. Read once per worker; each document embeds only the characters it uses. OpenType fonts with CFF outlines (most `.otf` files) cannot be embedded. |
| fontName  | optional | String | For a `.ttc`: PostScript name of the font in the collection. Default: the first font. |

For Japanese and other scripts outside WinAnsi, give a font that covers them, e.g. [Noto Sans JP](https://fonts.google.com/noto/specimen/Noto+Sans+JP) or IPAex Gothic (`.ttf`). A text that a font cannot draw fails its record. A form field that is not flattened embeds the whole font (several MB for a CJK font), so that a viewer can redraw a changed value; fonts from a `.ttc` can only be embedded as a subset, so they need `flatten: true` in a form.

### Templates

The texts of `values`, overlay `text`, `metadata` and the `encrypt` passwords are [FreeMarker](https://freemarker.apache.org/) templates when they contain `${...}`: `${name}` inserts the input field `name`, and the [template functions](../common/template.md) are available (e.g. `${utils.datetime.formatDate(issueDate, "yyyy/MM/dd")}`). Overlay texts can also use `${page}` and `${pageCount}`.

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

### generate, edit

| field     | type  | description |
|-----------|-------|-------------|
| content   | Bytes | The PDF file. |
| pageCount | Int64 | Number of pages of the PDF. |

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
- `generate` and `edit` output one PDF per record and hold no state: to make one PDF from many records, collect them first with the [aggregation transform](aggregation.md) (`array_agg`) and merge the array.
- Filling a form: dynamic XFA forms are removed so that viewers show the AcroForm values; check boxes are checked by `true`, `yes`, `on`, `1` or their export value.
- Write the `content` to files with the [files sink](../sink/files.md) (`content.field: content`, `content.format: bytes`).

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

## Example: fill a form for every row

Fills an application form template with each row from BigQuery, flattens it, and writes one PDF per row. With `values`, only the listed form fields are filled; without it, the columns named like the form fields would be (here the date needs formatting, so all are listed).

```yaml
sources:
  - name: applicants
    module: bigquery
    parameters:
      query: "SELECT id, name, address, agree, applied_date FROM `myproject.mydataset.applicants`"

transforms:
  - name: forms
    module: pdf
    inputs: [applicants]
    parameters:
      operation: generate
      fonts:
        - name: gothic
          path: "gs://my-bucket/fonts/NotoSansJP-Regular.ttf"
      generate:
        type: form
        template: "gs://my-bucket/templates/application.pdf"
        values:
          name: "${name}"
          address: "${address}"
          agree: "${agree?c}"
          date: "${utils.datetime.formatDate(applied_date, 'yyyy/MM/dd')}"
        flatten: true
        font: gothic
      steps:
        - type: metadata
          title: "Application ${id}"

sinks:
  - name: files
    module: files
    inputs: [forms]
    parameters:
      output: "gs://my-bucket/applications/${id}.pdf"
      content:
        field: content
        format: bytes
```

## Example: page numbers, a watermark and a password

Adds a footer with page numbers and a diagonal watermark to uploaded PDFs, and protects them with a password. The files source has a `content` field of its own, so the output is given a `prefix`.

```yaml
sources:
  - name: files
    module: files
    parameters:
      pattern: "gs://my-bucket/uploads/*.pdf"

transforms:
  - name: stamped
    module: pdf
    inputs: [files]
    parameters:
      operation: edit
      field: resource
      prefix: pdf_
      steps:
        - type: overlay
          items:
            - text: "${filename} - ${page} / ${pageCount}"
              position: bottom
              margin: 20
              fontSize: 9
              color: "#555555"
            - text: "CONFIDENTIAL"
              position: center
              fontSize: 72
              rotate: 45
              opacity: 0.15
              color: "#ff0000"
        - type: encrypt
          userPassword: "${utils.secrets.get('projects/my-project/secrets/pdf-password/versions/latest')}"
          permissions:
            modify: false
            copy: false

sinks:
  - name: output
    module: files
    inputs: [stamped]
    parameters:
      output: "gs://my-bucket/stamped/${filename}"
      content:
        field: pdf_content
        format: bytes
```

A template password is evaluated per record, so `utils.secrets.get` here reads the secret for every PDF. To read it once, define it in `system.args` (`pdfPassword: "${utils.secrets.get('...')}"`) and write `userPassword: "${args.pdfPassword}"`, which is resolved when the config is loaded.

## Example: merge the PDFs of each folder

Collects the paths of the PDFs per folder with the [aggregation transform](aggregation.md) and merges them, in path order, into one PDF per folder.

```yaml
sources:
  - name: files
    module: files
    parameters:
      pattern: "gs://my-bucket/invoices/**/*.pdf"

transforms:
  - name: folders
    module: aggregation
    inputs: [files]
    parameters:
      groupFields: [directory]
      aggregations:
        - input: files
          fields:
            - name: paths
              op: array_agg
              field: resource
              order: ascending
  - name: merged
    module: pdf
    inputs: [folders]
    parameters:
      operation: edit
      field: paths
      steps:
        - type: overlay
          items:
            - text: "${page}"
              position: bottomRight
              margin: 20

sinks:
  - name: output
    module: files
    inputs: [merged]
    parameters:
      output: "gs://my-bucket/merged/${directory}.pdf"
      content:
        field: content
        format: bytes
```

## Example: images to a PDF

Puts the scanned images of each record (an array of paths) on A4 pages, one image per page.

```yaml
transforms:
  - name: scans
    module: pdf
    inputs: [scan_sets]
    parameters:
      operation: generate
      field: image_paths
      generate:
        type: images
        pageSize: A4
        margin: 18
```
