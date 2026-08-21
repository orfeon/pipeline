---
type: Sink Module
title: HTTP Sink Module
description: Sends each input record (or a micro-batch of records) as an HTTP request to an endpoint and tracks the outcome. Supports per-record FreeMarker templates for URL / query params / headers, JSON / NDJSON / form / bytes / Avro / Protobuf / template bodies with gzip and a size guard, authentication providers (basic, bearer, apiKey, OAuth2 client credentials with token caching, GCP OIDC ID tokens and OAuth access tokens) with a one-time refresh on 401, declarative response classification (success / retry / failed by status code and payload conditions, Retry-After aware exponential backoff), per-item partial failure handling for bulk APIs (e.g. Elasticsearch _bulk) with selective item retry, keyed micro-batching (GroupIntoBatches, batch and streaming), per-worker rate limiting and bounded in-flight concurrency. Emits one control record per request (SUCCEEDED / PARTIAL / FAILED) with status, headers, body and optionally schema-typed payload; failed records go to failureSinks.
tags: [sink, http, rest, api, webhook, bulk, elasticsearch, solr, oauth2, oidc, batch, streaming]
timestamp: 2026-08-21T00:00:00Z
---

# HTTP Sink Module

Sink Module that delivers input records to an HTTP endpoint — one request per record by default, or one request per micro-batch of records with `batch` — and emits the outcome of every request as a record.

Typical use cases:

- Per-record REST calls: upsert into a CRM / SaaS API, send webhooks, invalidate caches.
- Bulk APIs: Elasticsearch `_bulk`, Solr `/update`, Algolia, Mixpanel import — with per-item partial failure handling.
- Streaming forwarding: Pub/Sub → filter → external API, with rate limiting, bounded concurrency and `Retry-After` aware retries.
- Calling private Cloud Run / IAP services or Google APIs with the pipeline's own identity (`gcpOidc` / `gcpOauth`), including fanning out into this pipeline's own [serve mode](../../deploy/cloud-run-service.md) `POST /run`.
- Tracking responses: created ids to BigQuery, failed records to a dead-letter sink, downstream steps `waits` on the sink.

### Choosing between http, tasks and query lookups

| Need | Module |
|---|---|
| Strict, cluster-wide rate limit of the target / delayed execution / decouple target outages from the pipeline's lifetime | [tasks](tasks.md) sink (the Cloud Tasks queue does it) |
| Synchronous call, the response matters, no GCP dependency, low latency, bodies over 100KB | **http** sink |
| Read from an API to add columns to records | [query](../transform/query.md) transform `rest` lookup source |

The `target` / `body` / `batch` parameters are the same as the tasks sink's: moving between the two is a matter of adding or removing `queue`.

## Sink module common parameters

| parameter    | optional | type                | description                                                           |
|--------------|----------|---------------------|-----------------------------------------------------------------------|
| name         | required | String              | Step name. specified to be unique in config file.                     |
| module       | required | String              | Specified `http`                                                      |
| inputs       | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits        | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy     | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.             |
| failFast     | optional | Boolean             | Fail the pipeline on the first error (default true in batch, false in streaming). With `false`, failed records go to `failureSinks` and a `FAILED` / `PARTIAL` output record is emitted. |
| failureSinks | optional | Array<String\>      | Steps that receive failed records.                                    |
| schema       | optional | [Schema](../common/schema.md) | Schema used to serialize `avro` / `protobuf` bodies (default: input schema). |
| parameters   | required | Map<String,Object\> | Specify the following individual parameters                          |

## HTTP sink module parameters

| parameter   | optional | type                              | description |
|-------------|----------|-----------------------------------|-------------|
| target      | required | [Target](#target-parameters)      | Endpoint, method, query params, headers and authentication. |
| body        | optional | [Body](#body-parameters)          | How records are serialized into the request body. Default: one JSON object per record. |
| response    | optional | [Response](#response-parameters)  | Response parsing and classification (success / retry / failed / partial failure). |
| batch       | optional | [Batch](#batch-parameters)        | Send one request per micro-batch of records instead of per record. |
| concurrency | optional | Integer                           | Max requests in flight per worker bundle (default `1`). Requests are issued asynchronously; the bundle commits only once every request resolved. |
| rate        | optional | [Rate](#rate-parameters)          | Per-worker rate limit (token bucket). The cluster-wide rate is roughly `rate × workers`; use the tasks sink when the target needs a strict global limit. |
| timeout     | optional | [Timeout](#timeout-parameters)    | Connection / request timeouts. |
| http        | optional | [Http](#http-parameters)          | HTTP client options (version, redirects, proxy, allowed hosts). |

### Target parameters

| parameter | optional | type                | description |
|-----------|----------|---------------------|-------------|
| url       | required | String              | Endpoint URL. Supports templates on record fields (`https://api.example.com/users/${id?url}`). |
| method    | optional | String              | HTTP method (`POST`, `PUT`, `PATCH`, `DELETE`, `GET`, …). Default `POST`. |
| params    | optional | Map<String,String\> | Query parameters appended to the URL (values are templates, URL-encoded). |
| headers   | optional | Map<String,String\> | Request headers (values are templates). `Content-Type` defaults from `body.format`. A header referencing `__body` is rendered after the body is serialized (signature headers, see [Templates](#templates)). |
| auth      | optional | [Auth](#auth-parameters) | Authentication provider. Default `none`. |

### Auth parameters

Credentials are resolved **once per worker** and cached; token-bearing providers refresh before expiry and re-fetch once after a `401`. Values are static templates — use `${utils.secrets.get("projects/p/secrets/name/versions/latest")}` (Secret Manager) rather than plain text; record fields are not available.

| type       | parameters | description |
|------------|------------|-------------|
| `none`     | —          | Default. |
| `basic`    | `username`, `password` | `Authorization: Basic …` |
| `bearer`   | `token`    | `Authorization: Bearer …` (static token). |
| `apiKey`   | `name`, `value`, `in` (`header` \| `query`, default `header`) | API key as an arbitrary header or query parameter. Prefixes are written in `value` (e.g. `value: "ApiKey xxx"`). |
| `oauth2`   | `grant` (`clientCredentials` default \| `jwtBearer`), `tokenUrl`, `scope`, `audience`, `refreshBeforeSeconds` (default 60); clientCredentials: `clientId`, `clientSecret`; jwtBearer: `issuer`, `subject` (default issuer), `privateKey` (PKCS#8 PEM, RS256), `keyId`, `jwtLifetimeMinutes` (default 60) | OAuth2 token endpoint exchange. `clientCredentials` posts client id/secret (HTTP basic); `jwtBearer` (RFC 7523 — Salesforce JWT flow, Box, Adobe, custom IdPs) signs a JWT (`iss`/`sub`/`aud` = `audience` or `tokenUrl`/`scope`/`iat`/`exp`) and posts it as the assertion. The access token is cached until `expires_in` − `refreshBeforeSeconds`. |
| `gcpOauth` | `scope` (default cloud-platform), `serviceAccount` | Google OAuth access token from the pipeline's credentials (ADC; `MERCARI_PIPELINE_GCP_CREDENTIALS` is honored). `serviceAccount` impersonates that account. For Google APIs. |
| `gcpOidc`  | `audience` (default: scheme + host of `url`), `serviceAccount` | Google-signed OIDC ID token. For private Cloud Run / Cloud Functions / IAP. Requires service-account or compute credentials (or `serviceAccount` impersonation). |

Safety: when `auth` is set, auth headers are only sent to the host of `target.url`. If the host part of `url` is itself a template, `http.allowedHosts` is required. Redirects to other origins do not carry the `Authorization` header. Auth headers are never written to output records, logs or failure records.

For schemes the providers do not cover (HMAC request signing, custom header sets) use `headers` templates with `__body` and `utils.string.hmacSha256(text, secret)`.

### Body parameters

| parameter   | optional | type           | description |
|-------------|----------|----------------|-------------|
| format      | optional | Enum           | `json` (default; one object per record, a JSON array per batch), `ndjson` (one JSON line per record; for bulk APIs), `template` (FreeMarker; default when `template` is set), `form` (`application/x-www-form-urlencoded` of the record fields), `multipart` (`multipart/form-data` built from `parts`), `bytes` (raw bytes of `field`), `avro` (Avro binary per record, Object Container File per batch), `protobuf` (message per record, length-delimited per batch; requires `schema.protobuf`), `none` (no body). |
| template    | optional | String         | FreeMarker template. With `format: template` it renders the whole body (per record, or per batch with `elements` / `size` / `key`). With `format: ndjson` it renders **one line per record** (multi-line templates are allowed — e.g. the Elasticsearch action + document pair). |
| fields      | optional | Array<String\> | Restrict `json` / `ndjson` / `form` / `__doc` to these record fields. |
| wrapper     | optional | String         | For `json` batches: wrap the array, e.g. `'{"records": ${body}}'` (`${body}` is replaced by the array). |
| omitNulls   | optional | Boolean        | Drop null fields from JSON bodies (default `false`). |
| maxBytes    | optional | String         | Size guard after serialization / compression (`1MB`, `100KB`, `1024`). A batch over the limit is split in halves recursively; a single record over it is routed to `failureSinks`. |
| compression | optional | Enum           | `none` (default) or `gzip` (`Content-Encoding: gzip`). |
| field       | optional | String         | Record field holding the body for `format: bytes` (BYTES or STRING). |
| contentType | optional | String         | Override the `Content-Type` derived from `format`. |
| parts       | optional | Array<[Part](#multipart-parts)\> | Parts of a `multipart` body. |

#### Multipart parts

| parameter   | optional | type   | description |
|-------------|----------|--------|-------------|
| name        | required | String | Form field name. |
| field       | optional | String | Record field (BYTES or STRING) sent as the part content. A null value skips the part. Exactly one of `field` / `template`. |
| template    | optional | String | FreeMarker template rendered as the part content (e.g. a JSON metadata part). |
| filename    | optional | String | `filename` of the `Content-Disposition` (template). |
| contentType | optional | String | Part `Content-Type` (default `application/octet-stream` for `field`, `text/plain; charset=utf-8` for `template`). |

### Response parameters

| parameter      | optional | type                                   | description |
|----------------|----------|----------------------------------------|-------------|
| format         | optional | Enum                                   | `json` (default), `text`, `bytes`, `none`. Determines how the response body is read into the output record (`body` / `blob` / `payload`). |
| schema         | optional | [Schema](../common/schema.md)          | With `json`: parse the response object into a typed `payload` struct. |
| success        | optional | [Success](#success--retry-conditions)  | What counts as success. Default: any 2xx. |
| retry          | optional | [Retry](#success--retry-conditions)    | What is retried and how. |
| partialFailure | optional | [PartialFailure](#partialfailure-parameters) | Per-item outcome for bulk responses (`batch` only). |

#### Success / retry conditions

| parameter                | optional | type                          | description |
|--------------------------|----------|-------------------------------|-------------|
| success.statusCodes      | optional | Array<Integer\>               | Status codes that are successful (default: all 2xx). |
| success.condition        | optional | [Filter](../common/filter.md) | Additional condition on the parsed response (variables: `statusCode`, `body`, `payload.<path>`). A successful status that fails the condition is `FAILED` (or `RETRY` when `retry.condition` matches). |
| retry.statusCodes        | optional | Array<Integer\>               | Retried status codes. Default `408, 425, 429, 500, 502, 503, 504`. Connection errors and timeouts are always retried. |
| retry.condition          | optional | [Filter](../common/filter.md) | Retry when this condition on the parsed response matches (non-success responses, or success responses failing `success.condition`). |
| retry.respectRetryAfter  | optional | Boolean                       | Honor the `Retry-After` header (seconds or HTTP date), capped by `maxBackoff`. Default `true`. |
| retry.maxAttempts        | optional | Integer                       | Total attempts including the first (default `5`). |
| retry.initialBackoff     | optional | String                        | First backoff (default `1s`); doubles each attempt with jitter. |
| retry.maxBackoff         | optional | String                        | Backoff cap (default `30s`). |
| retry.totalTimeout       | optional | String                        | Give up when the next attempt would start after this much time since the first (default `5m`). |

Anything that is neither success nor retry (e.g. `400`, `404`, `409`) is `FAILED` immediately: the record(s) go to `failureSinks` without retry.

#### PartialFailure parameters

Bulk APIs answer `200` and report per-item results in the body. `partialFailure` maps them back to the records of the batch (items must be in request order and one per record; otherwise the whole batch follows the top-level verdict).

| parameter      | optional | type                          | description |
|----------------|----------|-------------------------------|-------------|
| itemsPath      | required | String                        | JSON pointer to the items array (e.g. `/items`). |
| errorCondition | required | [Filter](../common/filter.md) | Evaluated on each item; matching items are failed (their records go to `failureSinks`). |
| retryCondition | optional | [Filter](../common/filter.md) | Evaluated before `errorCondition`; matching items are re-sent as a new (smaller) batch after the retry backoff (e.g. Elasticsearch per-item `429`). Bounded by `retry.maxAttempts` / `totalTimeout`. |

### Batch parameters

| parameter           | optional | type    | description |
|---------------------|----------|---------|-------------|
| maxSize             | optional | Integer | Max records per request (`maxSize` and/or `maxBytes` required). |
| maxBytes            | optional | String  | Max approximate input bytes per batch (`5MB`). Use `body.maxBytes` to guard the serialized size. |
| maxBufferingDuration| optional | String  | Streaming: flush a partial batch after this time (`2s`). |
| key                 | optional | String  | Template; only records with the same rendered key share a request. When set, `target.url` / `params` / `headers` templates may only reference fields used in `key`. Default: random shards. |
| shards              | optional | Integer | Number of random shards when `key` is omitted (default `8`). |

### Rate parameters

| parameter | optional | type    | description |
|-----------|----------|---------|-------------|
| count     | required | Double  | Permits per `unit` per worker. Applied to first attempts (retries are already spaced by backoff). |
| unit      | optional | Enum    | `second` (default) or `minute`. |

### Timeout parameters

| parameter | optional | type   | description |
|-----------|----------|--------|-------------|
| connect   | optional | String | Connect timeout (default `10s`). |
| request   | optional | String | Per-attempt request timeout (default `60s`). |

### Http parameters

| parameter       | optional | type           | description |
|-----------------|----------|----------------|-------------|
| version         | optional | String         | `HTTP_2` (default) or `HTTP_1_1`. |
| followRedirects | optional | String         | `normal` (default; never https → http), `never`, `always`. |
| proxy           | optional | String         | `host:port`. |
| allowedHosts    | optional | Array<String\> | Hosts requests may be sent to. Required when `auth` is set and the host part of `url` is a template; otherwise defaults to the host of `url` when `auth` is set. |

## Templates

`target.url`, `params`, `headers`, `body.template` and `batch.key` are [FreeMarker templates](../common/template.md). Variables:

| variable      | scope              | description |
|---------------|--------------------|-------------|
| `${field}`    | per record         | Record fields (strict: missing values fail the record; use `${f!""}`). |
| `__element`   | per record         | Map of all fields (`${utils.json.toJson(__element)}`). |
| `__doc`       | per record         | Map of `body.fields` (all fields when omitted). Useful in `ndjson` templates. |
| `__timestamp` | per record         | Event time (`java.time.Instant`). |
| `__source`    | per record         | Name of the input step the record came from. |
| `__body`      | headers only       | The serialized request body as text, **before** `compression` (for signature headers). |
| `elements`, `size`, `key` | batch  | List of record maps, batch size, rendered `batch.key`. |
| `utils.*`     | everywhere         | Built-in functions, e.g. `utils.secrets.get`, `utils.string.sha256`, `utils.string.hmacSha256`, `utils.datetime.*`. |

Templates that reference no record field (secrets, static tokens) are rendered once at worker startup, not per record.

## Output schema

One record per request (after all retries):

| field        | type                          | description |
|--------------|-------------------------------|-------------|
| url          | STRING                        | Rendered URL (with query params). |
| method       | STRING                        | |
| state        | STRING                        | `SUCCEEDED`, `PARTIAL` (some items of a batch failed), `FAILED`. |
| statusCode   | INT32                         | Status of the last attempt (null on connection errors). |
| headers      | MAP<STRING, ARRAY<STRING>>    | Response headers. |
| body         | STRING                        | Response body text (`format` text / json). |
| blob         | BYTES                         | Response body (`format: bytes`). |
| payload      | STRUCT / JSON                 | Parsed response: a struct when `response.schema` is set, otherwise the JSON text. |
| attempts     | INT32                         | Attempts made. |
| durationMs   | INT64                         | Duration of the last attempt. |
| elementCount | INT64                         | Records in the request. |
| failedCount  | INT64                         | Records that failed. |
| bytes        | INT64                         | Request body size. |
| error        | STRING                        | Error message for FAILED / PARTIAL. |
| timestamp    | TIMESTAMP                     | |

`FAILED` / `PARTIAL` records are emitted only with `failFast: false`; the failed input records are also routed to `failureSinks`. With `failFast: true` the first failure fails the pipeline.

## Idempotency and ordering

Beam delivers at least once: a retried bundle re-sends its records. Make calls idempotent on the target side — `PUT` with a natural id, an `Idempotency-Key` header derived from the record (`${utils.string.sha256(order_id)}`), Elasticsearch `_id` + `version_type: external`, Solr `_version_`. Records of one key are not guaranteed to arrive in order when `concurrency > 1` or across batch shards; use external versions, or `concurrency: 1` with `batch.key` on the id.

## Examples

### Per-record upsert with OAuth2 and a typed response

```yaml
sinks:
  - name: crm_upsert
    module: http
    inputs: [customers]
    failFast: false
    failureSinks: [dead_letter]
    parameters:
      target:
        url: https://api.example.com/v1/customers/${customer_id?url}
        method: PUT
        headers:
          Idempotency-Key: ${utils.string.sha256(customer_id + "-" + updated_at?string)}
        auth:
          type: oauth2
          tokenUrl: https://auth.example.com/oauth/token
          clientId: ${utils.secrets.get("projects/p/secrets/crm-client-id/versions/latest")}
          clientSecret: ${utils.secrets.get("projects/p/secrets/crm-client-secret/versions/latest")}
          scope: customers.write
      body:
        format: json
        fields: [customer_id, email, name]
        omitNulls: true
      response:
        schema:
          fields:
            - { name: id, type: string }
            - { name: status, type: string }
      concurrency: 8
      rate: { count: 50, unit: second }
```

### Elasticsearch `_bulk` (streaming, add/delete, per-item retry)

```yaml
sinks:
  - name: es_index
    module: http
    inputs: [events]
    failFast: false
    failureSinks: [dead_letter]
    parameters:
      target:
        url: https://es.example.com/_bulk
        auth:
          type: apiKey
          name: Authorization
          value: "ApiKey ${utils.secrets.get('projects/p/secrets/es-api-key/versions/latest')}"
      body:
        format: ndjson
        fields: [id, title, price, updated_at]
        template: |
          <#if op == "delete">
          {"delete":{"_index":"items","_id":"${id}","version":${version},"version_type":"external"}}
          <#else>
          {"index":{"_index":"items","_id":"${id}","version":${version},"version_type":"external"}}
          ${utils.json.toJson(__doc)}
          </#if>
      batch: { maxSize: 1000, maxBytes: 5MB, maxBufferingDuration: 2s, shards: 8 }
      concurrency: 4
      response:
        retry: { statusCodes: [429, 502, 503, 504], maxAttempts: 8, maxBackoff: 60s }
        partialFailure:
          itemsPath: /items
          errorCondition: "index.error != null or delete.error != null"
          retryCondition: "index.status = 429 or delete.status = 429"
```

### Webhook with HMAC signature

```yaml
parameters:
  target:
    url: https://hooks.example.com/events
    headers:
      X-Signature: ${utils.string.hmacSha256(__body, utils.secrets.get("projects/p/secrets/webhook-secret/versions/latest"))}
  body: { format: json }
```

### Calling this pipeline's own Cloud Run serve mode per record

```yaml
parameters:
  target:
    url: https://pipeline-xxxx.a.run.app/run?args.table=${table_name}
    auth: { type: gcpOidc }
  body: { format: none }
  concurrency: 4
  timeout: { request: 30m }
```

## Notes

- In `ndjson` templates `${utils.json.toJson(__doc)}` emits the record (restricted to `body.fields`) as one JSON object; without a template each record is emitted as one JSON line.
- Auth providers cover the common schemes; AWS SigV4 and mTLS are planned.
- `privateKey` for `jwtBearer` is a multi-line PEM: load it from Secret Manager (`${utils.secrets.get(...)}`) rather than inlining it in the config.
- The module does not deduplicate requests; see [Idempotency and ordering](#idempotency-and-ordering).
