---
type: Source Module
title: HTTP Source Module
description: Fetches records from HTTP/REST APIs. A request is described with the same target / body / response blocks as the http sink — FreeMarker templates for url / params / headers / body, authentication providers (basic, bearer, apiKey, OAuth2 client credentials / JWT bearer with token caching, GCP OIDC / OAuth), declarative success / retry classification with Retry-After aware backoff — plus source-specific parts, typed output records from JSON responses (response.schema) with array fan-out (response.itemsPath), pagination loops (page numbers, cursors from body or headers), request chaining (one request per parent record, or per item of a parent's response with foreach, executed in parallel), per-request rate limiting and periodic polling in streaming mode. Untyped requests emit raw response records (status, headers, body, payload).
tags: [source, http, rest, api, pagination, polling, oauth2, batch, streaming]
timestamp: 2026-08-22T00:00:00Z
---

# HTTP Source Module

Source module that fetches records from HTTP APIs. A request definition produces records — typed rows parsed from the JSON response, or raw response records — and can paginate, chain onto another request's records, and (in streaming) repeat periodically.

A request is written with the **same blocks as the [http sink](../sink/http.md)**: `target` (url / method / params / headers / auth), `body` and `response` (format / schema / success / retry). A single request is written directly under `parameters`; several requests go under `requests`, each adding `name`, `input` / `foreach` and `loop`.

Typical uses:

- Pull a paginated REST collection into the pipeline as typed records (`itemsPath` + `schema` + `loop`).
- List → detail: fetch a list, then one detail request per item in parallel (`input`, or `foreach` over the raw parent response).
- Poll an API every N minutes in a streaming pipeline (`polling`).
- Archive raw API pages (untyped records with `body` / `payload`) to storage.

## Source module common parameters

| parameter    | optional | type | description |
|--------------|----------|------|-------------|
| name         | required | String | Step name. The first request's records are this step's output; every request is also available as `<name>.<request name>`. |
| module       | required | String | Specified `http` |
| failFast     | optional | Boolean | Fail the pipeline on the first failed request (default true in batch, false in streaming). With `false`, failed requests are routed to `failureSinks` (the seed or the parent record that triggered them). |
| failureSinks | optional | Array<String\> | Steps that receive failed records. |
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
| http      | optional | [Http](../sink/http.md#http-parameters) | HTTP client options. With auth, requests are pinned to the hosts of the configured urls; set `allowedHosts` when a url host is a template. |
| polling   | optional | [Polling](#polling-parameters) | Streaming only: repeat the root requests periodically. |

### Request parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| name      | optional | String | Request name — required with several requests (used by `input` and as the output tag `<step>.<name>`). Defaults to the step name. |
| target    | required | [Target](../sink/http.md#target-parameters) | `url` (template), `method` (default `GET`), `params`, `headers`, `auth`. Root requests see `utils.*` and `__timestamp`; chained requests see the parent record's fields (`${id}`), `foreach` requests the item's fields. Loop variables are available everywhere (`${page}`). |
| body      | optional | [Body](../sink/http.md#body-parameters) | Request body. `template` (FreeMarker), or for chained requests `json` / `ndjson` / `form` / `multipart` of the parent record. Default: no body. |
| response  | optional | [Response](#response-parameters) | How the response becomes records; success / retry classification. |
| loop      | optional | [Loop](#loop-parameters) | Pagination. |
| input     | optional | String | Name of the parent request. This request runs once **per record** of the parent's output, in parallel across workers. |
| foreach   | optional | String | With `input`: JSON pointer into the parent's **raw** response (`/data/items`); this request runs once per element of that array, with the element's fields as template variables (`__item` holds the element). Lets the parent stay an untyped archive while children fan out over its items. |
| rate      | optional | [Rate](#rate-parameters) | Per-worker rate limit for this request (useful for `input` / `foreach` fan-out). |

### Response parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| format    | optional | Enum | `json` (default), `text`, `bytes`, `none`. |
| schema    | optional | [Schema](../common/schema.md) | Emit **typed records**: the JSON object (or each element of `itemsPath`) is converted to this schema. Without `schema`, [raw records](#raw-output-schema) are emitted. |
| itemsPath | optional | String | JSON pointer to an array in the response (`/items`, `/data/results`, `/` for a top-level array); each element becomes one record. Without it, the whole response is one record. |
| success   | optional | [Success](../sink/http.md#success--retry-conditions) | Success status codes / condition (default: 2xx). |
| retry     | optional | [Retry](../sink/http.md#success--retry-conditions) | Retry policy (default: 408/425/429/5xx and connection errors, 5 attempts, Retry-After honored). A request that still fails is routed to `failureSinks` / fails the pipeline. |

### Loop parameters

The request is repeated until `until` holds. After each response the `next` templates are rendered against that response and update the loop variables, which are available in the `target` / `body` templates.

| parameter     | optional | type | description |
|---------------|----------|------|-------------|
| vars          | optional | Map<String,Object\> | Initial variables (`{ page: 1 }`, `{ cursor: "" }`). |
| next          | optional | Map<String,String\> | Templates producing the next values (`page: "${page + 1}"`, `cursor: "${payload.next_cursor!''}"`, `cursor: "${headers['x-next-cursor']!''}"`). Numeric results become numbers. |
| until         | required | [Filter](../common/filter.md) | Stop condition evaluated on the response: `statusCode`, `headers.<name>` (first value; lower-case keys available), `body`, `payload.<path>` (parsed JSON), plus the current loop variables. A list of conditions is AND-ed. Same shape as `poll.until` of [action.http](../action/http.md). |
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

With `response.schema`, each record has exactly the schema's fields, converted from the JSON object (nested objects → structs, arrays → repeated fields, per the [schema](../common/schema.md) conventions).

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
| timestamp  | TIMESTAMP | Time of the response. |

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
- Tokens obtained in `system.args` at config load time (e.g. via `utils.oauth.*`) are fixed for the whole run; prefer `auth` so tokens are refreshed before expiry and once after a 401.
