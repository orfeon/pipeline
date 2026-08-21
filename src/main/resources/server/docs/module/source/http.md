---
type: Source Module
title: HTTP Source Module
description: Fetches records from HTTP/REST APIs. Each request definition supports FreeMarker templates for url/params/headers/body, authentication providers (basic, bearer, apiKey, OAuth2 client credentials / JWT bearer with token caching, GCP OIDC / OAuth), typed output records from JSON responses (response.schema) with array fan-out (response.rowsFrom), pagination loops (page numbers, cursors from body or headers), request chaining (one request per record of a parent request, executed in parallel), declarative success/retry classification with Retry-After aware backoff, and periodic polling in streaming mode. Untyped requests emit raw response records (status, headers, body, payload).
tags: [source, http, rest, api, pagination, polling, oauth2, batch, streaming]
timestamp: 2026-08-22T00:00:00Z
---

# HTTP Source Module

Source module that fetches records from HTTP APIs. A config lists one or more **request definitions**; each produces records — typed rows parsed from the JSON response, or raw response records — and can paginate, chain onto another request's records, and (in streaming) repeat periodically.

Built on the same outbound core as the [http sink](../sink/http.md) and [action.http](../action/http.md): `auth`, `response.success` / `retry`, `timeout` and `http` have the same meaning there.

Typical uses:

- Pull a paginated REST collection into the pipeline as typed records (`rowsFrom` + `schema` + `loop`).
- List → detail: fetch a list, then one detail request per item in parallel (`input`).
- Poll an API every N minutes in a streaming pipeline (`polling`).
- Seed a pipeline from any API response (raw record with `body` / `payload`), e.g. feature flags, a manifest, a token.

## Source module common parameters

| parameter  | optional | type                | description |
|------------|----------|---------------------|-------------|
| name       | required | String              | Step name. The first request's records are this step's output; every request is also available as `<name>.<request name>`. |
| module     | required | String              | Specified `http` |
| failFast   | optional | Boolean             | Fail the pipeline on the first failed request (default true in batch, false in streaming). With `false`, failed requests are routed to `failureSinks` (the record that triggered them — the seed or the parent record). |
| failureSinks | optional | Array<String\>    | Steps that receive failed records. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## HTTP source module parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| requests  | required | Array<[Request](#request-parameters)\> | Request definitions. |
| auth      | optional | [Auth](../sink/http.md#auth-parameters) | Default authentication for every request (a request's own `auth` overrides it). |
| timeout   | optional | [Timeout](../sink/http.md#timeout-parameters) | Connect / per-attempt request timeouts. |
| http      | optional | [Http](../sink/http.md#http-parameters) | HTTP client options. With `auth`, requests are pinned to the hosts of the configured urls; set `allowedHosts` when a url host is a template. |
| polling   | optional | [Polling](#polling-parameters) | Streaming only: repeat the root requests periodically. |

### Request parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| name      | optional | String | Request name — required when several requests are defined (used by `input` and as the output tag `<step>.<name>`). Defaults to the step name. |
| url       | required | String | URL template. Root requests see `utils.*` and `__timestamp`; chained requests see the parent record's fields (`${id}`). `endpoint` is accepted as an alias. |
| method    | optional | String | Default `GET`. |
| params    | optional | Map<String,String\> | Query parameters (values are templates, URL-encoded). Loop variables are available (`${page}`). |
| headers   | optional | Map<String,String\> | Request headers (values are templates). |
| body      | optional | String \| JSON | Request body template: a string, or a JSON object/array whose text is rendered as a template (`Content-Type: application/json` unless a header says otherwise). |
| auth      | optional | [Auth](../sink/http.md#auth-parameters) | Per-request authentication (overrides the module-level `auth`). |
| response  | optional | [Response](#response-parameters) | How the response becomes records, and success / retry classification. (`format` directly under the request is accepted as an alias of `response.format`.) |
| loop      | optional | [Loop](#loop-parameters) | Pagination. |
| input     | optional | String | Name of the parent request. This request runs once **per record** of the parent's output (in parallel across workers), with the parent record's fields as template variables. |

### Response parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| format    | optional | Enum | `json` (default), `text`, `bytes`, `none`. |
| schema    | optional | [Schema](../common/schema.md) | Emit **typed records**: the JSON object (or each element of `rowsFrom`) is converted to this schema. Without `schema`, [raw records](#raw-output-schema) are emitted. |
| rowsFrom  | optional | String | JSON pointer to an array in the response (`/items`, `/data/results`, `/` for a top-level array); each element becomes one record. Without it, the whole response is one record. |
| success   | optional | [Success](../sink/http.md#success--retry-conditions) | Success status codes / condition (default: 2xx). |
| retry     | optional | [Retry](../sink/http.md#success--retry-conditions) | Retry policy (default: 408/425/429/5xx and connection errors, 5 attempts, Retry-After honored). A request that still fails is routed to `failureSinks` / fails the pipeline. |

### Loop parameters

The request is repeated while `condition` holds. Before each repetition the `feeds` templates are rendered against the **previous response** and update the loop variables; the variables are available in `url` / `params` / `headers` / `body` templates.

| parameter     | optional | type | description |
|---------------|----------|------|-------------|
| vars          | optional | Map<String,Object\> | Initial variables (`{ page: 1 }`, `{ cursor: "" }`). |
| feeds         | optional | Map<String,String\> | Templates producing the next values (`page: "${page + 1}"`, `cursor: "${payload.next_cursor!''}"`, `cursor: "${headers['x-next-cursor']!''}"`). Numeric results become numbers. |
| condition     | required (with feeds) | [Filter](../common/filter.md) | Continue condition evaluated on the response: `statusCode`, `headers.<name>` (first value, lower-case keys available), `body`, `payload.<path>` (parsed JSON; `response` is an alias), plus the current loop variables. A list of conditions is AND-ed. |
| maxIterations | optional | Integer | Safety cap (default `10000`). |

### Polling parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| interval  | required | String | In streaming mode, run the root requests every interval (`1m`, `PT30S`). Ignored in batch mode (requests run once). Chained requests follow each poll's records. |

## Outputs

- `<step>` — records of the **first** request.
- `<step>.<request name>` — records of each request (use these as `inputs` of downstream steps when several requests are defined).

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
| payload    | JSON   | Parsed JSON text — the whole response, or one `rowsFrom` element per record. |
| attempts   | INT32  | Attempts made. |
| durationMs | INT64  | Duration of the last attempt. |
| timestamp  | TIMESTAMP | Time of the response. |

## Examples

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
          url: https://api.example.com/v1/products
          params: { page: "${page}", per_page: "100" }
          loop:
            vars: { page: 1 }
            feeds: { page: "${page + 1}" }
            condition: { key: payload.has_more, op: "=", value: true }
          response:
            rowsFrom: /items
            schema:
              fields:
                - { name: id, type: string }
                - { name: name, type: string }
                - { name: updated_at, type: timestamp }
        - name: detail
          input: list
          url: https://api.example.com/v1/products/${id}
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

### Cursor pagination from a response header

```yaml
requests:
  - url: https://api.example.com/events
    params: { cursor: "${cursor}" }
    loop:
      vars: { cursor: "" }
      feeds: { cursor: "${headers['x-next-cursor']!''}" }
      condition: { key: headers.x-next-cursor, op: "!=", value: null }
    response: { rowsFrom: /, schema: { fields: [...] } }
```

### Periodic polling (streaming)

```yaml
sources:
  - name: status
    module: http
    parameters:
      polling: { interval: 1m }
      requests:
        - url: https://api.example.com/status
          auth: { type: gcpOidc }
          response:
            schema:
              fields:
                - { name: state, type: string }
                - { name: updated_at, type: timestamp }
```

### Raw seed record

```yaml
requests:
  - url: https://config.example.com/flags.json
# downstream: payload holds the JSON text; use select json_path or a query over it
```

## Notes

- Templates referencing neither record fields nor loop variables (e.g. secret headers) are rendered once per worker.
- Beam may re-run a bundle: requests are re-sent. Only read (idempotent) endpoints belong in a source.
- Chained requests run in parallel; pagination within one request is sequential by nature.
- Rate limiting: use the parent's `retry` with `Retry-After` for 429s; for strict per-worker limits prefer a downstream [http sink](../sink/http.md) / `query` rest lookup.
- Changed from the previous version: `input` is a request name (the old `input.filter/select/flatten` and `preprocessors` are gone — use downstream `select` / `filter`), retries and auth are real, raw output uses `url` instead of `endpoint`, `acceptableStatusCodes` became `response.success.statusCodes`, `format` moved under `response`.
