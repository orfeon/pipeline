---
type: Source Module
title: HTTP Source Module
description: Fetches records from HTTP/REST APIs. A request is described with the same target / body / response blocks as the http sink — FreeMarker templates for url / params / headers / body, authentication providers (basic, bearer, apiKey, OAuth2 client credentials / JWT bearer with token caching, GCP OIDC / OAuth), declarative success / retry classification with Retry-After aware backoff — plus source-specific parts, typed output records from JSON responses (response.schema) with array fan-out (response.itemsPath) or from HTML pages scraped with CSS selectors (format html, itemsSelector, fields), template-valued extra fields (loop variables, parent fields, __url), pagination loops (page numbers, cursors from body or headers), request chaining (one request per parent record, or per item of a parent's response with foreach, filtered with when, executed in parallel), per-request rate limiting and periodic polling in streaming mode. Untyped requests emit raw response records (status, headers, body, payload).
tags: [source, http, rest, api, html, scrape, pagination, polling, oauth2, batch, streaming]
timestamp: 2026-08-22T00:00:00Z
---

# HTTP Source Module

Source module that fetches records from HTTP APIs. A request definition produces records — typed rows parsed from the JSON response, or raw response records — and can paginate, chain onto another request's records, and (in streaming) repeat periodically.

A request is written with the **same blocks as the [http sink](../sink/http.md)**: `target` (url / method / params / headers / auth), `body` and `response` (format / schema / success / retry). A single request is written directly under `parameters`; several requests go under `requests`, each adding `name`, `input` / `foreach` and `loop`.

Typical uses:

- Pull a paginated REST collection into the pipeline as typed records (`itemsPath` + `schema` + `loop`).
- List → detail: fetch a list, then one detail request per item in parallel (`input`, or `foreach` over the raw parent response).
- Scrape a paginated HTML listing into typed records (`format: html` + `itemsSelector` + `fields`) and download the files it links to (`input` + `when` + `format: bytes`, written by the [files sink](../sink/files.md)).
- Poll an API every N minutes in a streaming pipeline (`polling`).
- Archive raw API pages (untyped records with `body` / `payload`) to storage.

## Source module common parameters

| parameter    | optional | type | description |
|--------------|----------|------|-------------|
| name         | required | String | Step name. The first request's records are this step's output; every request is also available as `<name>.<request name>`. |
| module       | required | String | Specified `http` |
| failFast     | optional | Boolean | Fail the pipeline on the first failed request (default true in batch, false in streaming). With `false`, failed requests are routed to `failureSinks` (the seed or the parent record that triggered them). |
| failureSinks | optional | Array<String\> | Steps that receive failed records. |
| timestampAttribute | optional | String | Output field whose value becomes the records' event time (timestamp or date type; a date is midnight UTC). Applies to every request whose output has the field (scraped, JSON or a `value` template field); other requests keep the [default event time](#event-time). A record with a null value keeps the default too. |
| parameters   | required | Map<String,Object\> | Specify the following individual parameters |

## HTTP source module parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| requests  | optional | Array<[Request](#request-parameters)\> | Request definitions (several requests). Exclusive with the single-request form below. |
| target    | optional | [Target](../sink/http.md#target-parameters) | Single-request form: the request's target (with `body` / `response` / `loop` / `rate` at the same level). |
| body      | optional | [Body](../sink/http.md#body-parameters) | Single-request form. |
| response  | optional | [Response](#response-parameters) | Single-request form. |
| loop      | optional | [Loop](#loop-parameters) | Single-request form. |
| rate      | optional | [Rate](#rate-parameters) | Single-request form. |
| auth      | optional | [Auth](../sink/http.md#auth-parameters) | Default authentication for every request (`target.auth` of a request overrides it). |
| timeout   | optional | [Timeout](../sink/http.md#timeout-parameters) | Connect / per-attempt request timeouts. |
| http      | optional | [Http](../sink/http.md#http-parameters) | HTTP client options. Requests with auth, or with an `Authorization` / `Proxy-Authorization` / `Cookie` / `X-Api-Key` header in `target.headers`, are pinned to the hosts of those requests' configured urls; set `allowedHosts` when a url host is a template. Requests without auth or such a header (`target.auth: { type: none }`, e.g. a signed download URL on another host) are not pinned, unless `allowedHosts` is set explicitly, which applies to every request. |
| polling   | optional | [Polling](#polling-parameters) | Streaming only: repeat the root requests periodically. |

### Request parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| name      | optional | String | Request name — required with several requests (used by `input` and as the output tag `<step>.<name>`). Defaults to the step name. |
| target    | required | [Target](../sink/http.md#target-parameters) | `url` (template), `method` (default `GET`), `params`, `headers`, `auth`, `omitEmptyParams` (default `true` in the source: a param that renders empty, such as `pagination_key: "${pagination_key}"` on the first page, is left out). Root requests see `utils.*` and `__timestamp`; chained requests see the parent record's fields (`${id}`), `foreach` requests the item's fields. Loop variables are available everywhere (`${page}`). |
| body      | optional | [Body](../sink/http.md#body-parameters) | Request body. `template` (FreeMarker), or for chained requests `json` / `ndjson` / `form` / `multipart` of the parent record. Default: no body. |
| response  | optional | [Response](#response-parameters) | How the response becomes records; success / retry classification. |
| loop      | optional | [Loop](#loop-parameters) | Pagination. |
| input     | optional | String | Name of the parent request. This request runs once **per record** of the parent's output, in parallel across workers. |
| foreach   | optional | String | With `input`: JSON pointer into the parent's **raw** response (`/data/items`); this request runs once per element of that array, with the element's fields as template variables (`__item` holds the element). Lets the parent stay an untyped archive while children fan out over its items. |
| when      | optional | [Filter](../common/filter.md) | With `input`: run this request only for the parent records (with `foreach`: the items) that match, e.g. `"file IS NOT NULL"`. Evaluated on the parent record's fields (plus the item's fields and `__item` with `foreach`); skipped records produce no output and no failure. Without `foreach`, the fields it refers to must be output fields of the parent (checked at assembly). |
| rate      | optional | [Rate](#rate-parameters) | Per-worker rate limit for this request (useful for `input` / `foreach` fan-out). |

### Response parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| format    | optional | Enum | `json` (default), `text`, `bytes`, `none`, `html`. `html` emits typed records scraped from the page (requires `fields`). |
| schema    | optional | [Schema](../common/schema.md) | Emit **typed records**: the JSON object (or each element of `itemsPath`) is converted to this schema. Without `schema` (and without `format: html`), [raw records](#raw-output-schema) are emitted. |
| itemsPath | optional | String | JSON pointer to an array in the response (`/items`, `/data/results`, `/` for a top-level array); each element becomes one record. Without it, the whole response is one record. |
| itemsSelector | optional | String | `format: html`: CSS selector of the item elements (`table#list tr`); each match becomes one record. Without it, the whole page is one record. |
| baseUri   | optional | String | `format: html`: base URI for resolving relative links (`attribute: "abs:href"`). Default: the request URL. |
| fields    | optional | Array<[Field](#response-fields)\> | Output fields scraped from each html item (`selector`), and/or template fields (`value`) added to any record type (typed json, html or raw). |
| success   | optional | [Success](../sink/http.md#success--retry-conditions) | Success status codes / condition (default: 2xx). |
| retry     | optional | [Retry](../sink/http.md#success--retry-conditions) | Retry policy (default: 408/425/429/5xx and connection errors, 5 attempts, Retry-After honored). A request that still fails is routed to `failureSinks` / fails the pipeline. |

### Response fields

Each entry of `response.fields` has a `name` and exactly one of `selector` or `value`.

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| name      | required | String | Output field name. It must not collide with the `schema` fields or, for raw records, the [raw output fields](#raw-output-schema). |
| selector  | selective | String | `format: html` only. CSS selector evaluated **under each item element** (`td:nth-of-type(2)`, `td.title a[href]`). The remaining parameters are those of the select [scrape](../transform/select.md#scrape) function: `type` (default `string`), `attribute` (e.g. `href`, or `abs:href` for an absolute URL), `pattern` / `group`, `trim`, `mode: repeated` (every match; a match whose `pattern` does not match is dropped) and nested `fields` (a struct). An item without a match yields `null`. |
| value     | selective | String | FreeMarker template rendered **per output record**, converted to `type`. It sees the request's template values — the parent record's fields, the `foreach` item, `utils.*` — plus the current loop variables (`${page}`) and `__url` (the rendered request URL), overlaid with the record's own fields (scraped, `schema` or raw fields) and the `value` fields declared before it. Record fields are standard values: `date` / `time` / `timestamp` render as ISO-8601 (`2026-10-16`, `20:00`, …) and can be formatted with `utils.datetime` (`${utils.datetime.formatTime(time, 'HH:mm:ss')}`); a null field must be guarded (`${time!''}`, `<#if time??>…</#if>`). Use it to keep the request context (`page`, a date argument, a parent key) on the records, or to combine fields (a date and a time into a `timestamp` for `timestampAttribute`). |
| type      | optional | String | Output type (default `string`): `string`, `int64`, `float64`, `bool`, `date`, `time`, `timestamp`, … For `value`, an empty result becomes `null` except for `string`. |

The html body is decoded with the charset of the `Content-Type` header, else the page's `<meta charset>` / BOM (default UTF-8). The loop's `until` / `next` still see `body` as UTF-8 text.

### Loop parameters

The request is repeated until `until` holds. After each response the `next` templates are rendered against that response and update the loop variables, which are available in the `target` / `body` templates.

| parameter     | optional | type | description |
|---------------|----------|------|-------------|
| vars          | optional | Map<String,Object\> | Initial variables (`{ page: 1 }`, `{ cursor: "" }`). |
| next          | optional | Map<String,String\> | Templates producing the next values (`page: "${page + 1}"`, `cursor: "${payload.next_cursor!''}"`, `cursor: "${headers['x-next-cursor']!''}"`). Numeric results become numbers. |
| until         | required | [Filter](../common/filter.md) | Stop condition evaluated on the response: `statusCode`, `headers.<name>` (first value; lower-case keys available), `body`, `payload.<path>` (parsed JSON), plus the current loop variables. A list of conditions is AND-ed. Same shape as `poll.until` of the [http action](../action/http.md). |
| maxIterations | optional | Integer | Safety cap (default `10000`). |

### Rate parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| count     | required | Double | Permits per `unit` per worker. |
| unit      | optional | Enum | `second` (default) or `minute`. |

### Polling parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| interval  | required | String | In streaming mode, run the root requests every interval (`1m`, `PT30S`). Ignored in batch mode (requests run once). Chained requests follow each poll's records. |

## Outputs

- `<step>` — records of the **first** request.
- `<step>.<request name>` — records of each request. Raw outputs of several requests share one schema and can be listed together in a downstream `inputs` (e.g. to archive every resource with one storage sink).

### Typed output

With `response.schema`, each record has the schema's fields, converted from the JSON object (nested objects → structs, arrays → repeated fields, per the [schema](../common/schema.md) conventions), followed by the `value` fields of `response.fields`.

With `format: html`, each record has exactly the `response.fields`, in order.

Raw records get the `value` fields of `response.fields` appended. Raw outputs with the same extra fields still share one schema.

### Event time

- Batch: records are stamped with the execution time (the time the source starts).
- Streaming with `polling`: the time of the poll tick.
- Chained requests (`input`) inherit the event time of their parent record.
- `timestampAttribute` overrides it with a field of the record (e.g. a publication time scraped from the page). The value may be earlier than the parent's event time; in streaming such records can be late for downstream windows.

### Raw output schema

Without `response.schema`:

| field      | type | description |
|------------|------|-------------|
| name       | STRING | Request name. |
| url        | STRING | Rendered URL. |
| method     | STRING | |
| statusCode | INT32  | |
| headers    | MAP<STRING, ARRAY<STRING>> | Response headers. |
| body       | STRING | Body text (`format` text / json). |
| blob       | BYTES  | Body bytes (`format: bytes`). |
| payload    | JSON   | Parsed JSON text — the whole response, or one `itemsPath` element per record. |
| attempts   | INT32  | Attempts made. |
| durationMs | INT64  | Duration of the last attempt. |
| timestamp  | TIMESTAMP | Time the response was received. |

## Examples

### Single request, typed records

```yaml
sources:
  - name: flags
    module: http
    parameters:
      target:
        url: https://config.example.com/flags
        auth: { type: gcpOidc }
      response:
        itemsPath: /flags
        schema:
          fields:
            - { name: key, type: string }
            - { name: enabled, type: boolean }
```

### Paginated list → typed records, then a detail call per item

```yaml
sources:
  - name: products
    module: http
    parameters:
      auth:
        type: oauth2
        tokenUrl: https://auth.example.com/oauth/token
        clientId: ${utils.secrets.get("projects/p/secrets/shop-client-id/versions/latest")}
        clientSecret: ${utils.secrets.get("projects/p/secrets/shop-client-secret/versions/latest")}
      requests:
        - name: list
          target:
            url: https://api.example.com/v1/products
            params: { page: "${page}", per_page: "100" }
          loop:
            vars: { page: 1 }
            next: { page: "${page + 1}" }
            until: { key: payload.has_more, op: "=", value: false }
          response:
            itemsPath: /items
            schema:
              fields:
                - { name: id, type: string }
                - { name: name, type: string }
                - { name: updated_at, type: timestamp }
        - name: detail
          input: list
          target:
            url: https://api.example.com/v1/products/${id}
          rate: { count: 20 }
          response:
            schema:
              fields:
                - { name: id, type: string }
                - { name: description, type: string }
                - { name: variants, type: string, mode: repeated }
sinks:
  - name: out
    module: bigquery
    inputs: [products.detail]
    parameters: { table: "proj:ds.products" }
```

### Archive raw pages and fan out over their items (`foreach`)

```yaml
sources:
  - name: lms
    module: http
    parameters:
      auth:
        type: oauth2
        grant: jwtBearer
        tokenUrl: https://lms.example.com/oauth2/token
        issuer: ${utils.secrets.get("projects/p/secrets/lms-client-id/versions/latest")}
        subject: ${utils.secrets.get("projects/p/secrets/lms-user/versions/latest")}
        audience: lms.example.com
        privateKey: ${utils.secrets.get("projects/p/secrets/lms-private-key/versions/latest")}
      requests:
        - name: courses
          target:
            url: https://lms.example.com/course/v1/courses
            params: { page: "${page}", page_size: "200", status: published }
          loop:
            vars: { page: 1 }
            next: { page: "${page + 1}" }
            until: { key: payload.data.has_more_data, op: "=", value: false }
        - name: enrollments
          input: courses
          foreach: /data/items            # each course object of every page; ${id} is the course id
          target:
            url: https://lms.example.com/course/v1/courses/${id}/enrollments
            params: { page: "${page}", page_size: "200" }
          loop:
            vars: { page: 1 }
            next: { page: "${page + 1}" }
            until: { key: payload.data.has_more_data, op: "=", value: false }
sinks:
  - name: archive
    module: storage
    inputs: [lms.courses, lms.enrollments]    # same raw schema
    parameters:
      output: "gs://bucket/api/resource=${name}/date=${utils.datetime.currentDate()}/${statusCode}"
      format: avro
```

### Scrape a paginated HTML listing and download the linked files

The listing page `list_<page>.html` shows a "next" link (`class="next"`) on every page but the last.
Each table row becomes a typed record. The relative links are downloaded only where they exist
(`when`), and the files sink writes each download as one file.

```yaml
system:
  args:
    date: "${utils.datetime.formatDate(utils.datetime.currentDate('Asia/Tokyo', -1), 'yyyy-MM-dd')}"
sources:
  - name: notices
    module: http
    timestampAttribute: publishedAt                # event time of the list records (and their downloads)
    parameters:
      requests:
        - name: list
          target:
            url: "https://www.example.com/notices/${args.date}/list_${page?c?left_pad(3, '0')}.html"
          loop:
            vars: { page: 1 }
            next: { page: "${page + 1}" }
            until: { key: body, op: "not match", value: 'class="next"' }
          response:
            format: html
            itemsSelector: "#list tr:has(td)"              # data rows only (a header row would yield a null time)
            fields:
              - { name: date, type: date, value: "${args.date}" }
              - { name: time, type: time, selector: "td:nth-of-type(1)" }
              - { name: code, type: string, selector: "td:nth-of-type(2)" }
              - { name: title, type: string, selector: "td:nth-of-type(3)", trim: true }
              - { name: pdfFile, selector: "td:nth-of-type(3) a[href]", attribute: href }
              - { name: attachmentFile, selector: "td:nth-of-type(4) a[href]", attribute: href }
              - { name: publishedAt, type: timestamp, value: "${date}T${utils.datetime.formatTime(time, 'HH:mm:ss')}+09:00" }
        - name: pdf
          input: list
          when: "pdfFile IS NOT NULL"
          target: { url: "https://www.example.com/notices/files/${pdfFile}" }
          rate: { count: 5 }
          response:
            format: bytes
            fields:
              - { name: file, value: "${pdfFile}" }
        - name: attachment
          input: list
          when: "attachmentFile IS NOT NULL"
          target: { url: "https://www.example.com/notices/files/${attachmentFile}" }
          rate: { count: 5 }
          response:
            format: bytes
            fields:
              - { name: file, value: "${attachmentFile}" }
sinks:
  - name: table
    module: bigquery
    inputs: [notices.list]
    parameters: { table: "proj:ds.notices", writeDisposition: WRITE_APPEND, createDisposition: CREATE_IF_NEEDED }
  - name: files
    module: files
    inputs: [notices.pdf, notices.attachment]      # same raw schema (+ file)
    parameters:
      output: "gs://bucket/notices/${args.date}/${file}"
      content: { field: blob }
```

### Cursor pagination from a response header

```yaml
parameters:
  target:
    url: https://api.example.com/events
    params: { cursor: "${cursor}" }
  loop:
    vars: { cursor: "" }
    next: { cursor: "${headers['x-next-cursor']!''}" }
    until: { key: headers.x-next-cursor, op: "=", value: null }
  response: { itemsPath: /, schema: { fields: [...] } }
```

### Periodic polling (streaming)

```yaml
sources:
  - name: status
    module: http
    parameters:
      polling: { interval: 1m }
      target: { url: https://api.example.com/status, auth: { type: gcpOidc } }
      response:
        schema:
          fields:
            - { name: state, type: string }
            - { name: updated_at, type: timestamp }
```

## Notes

- Templates referencing neither record fields nor loop variables (e.g. secret headers) are rendered once per worker; `foreach` requests render every template per item.
- Beam may re-run a bundle: requests are re-sent. Only read (idempotent) endpoints belong in a source.
- Chained requests run in parallel; pagination within one request is sequential by nature.
- Loop variables are numbers: format them with FreeMarker built-ins in templates (`${page?c?left_pad(3, '0')}` → `001`).
- An html `until` on `body` should match the *next* link specifically: a pager usually links to the previous page too, so a pattern that only looks for any page link never stops (the loop then runs into the first missing page, which fails the request).
- Tokens obtained in `system.args` at config load time (e.g. via `utils.oauth.*`) are fixed for the whole run; prefer `auth` so tokens are refreshed before expiry and once after a 401.
