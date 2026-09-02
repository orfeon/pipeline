---
type: Sink Module
title: gRPC Sink Module
description: Sends each input record (or a micro-batch of records) as a gRPC request to any service described by a protoc descriptor set — no generated stubs. Records are mapped to the request message by field name (JSON mapping) or with a FreeMarker protobuf-JSON template; unary and client-streaming methods are supported, batches go into a repeated field or onto the client stream. Shares the http sink's authentication providers (sent as call metadata), declarative success / retry classification on gRPC status codes with backoff, keyed micro-batching, bounded in-flight concurrency and per-worker rate limiting, and emits one SUCCEEDED / FAILED control record per call with the response as JSON payload; failed records go to failureSinks.
tags: [sink, grpc, protobuf, api, batch, streaming]
timestamp: 2026-08-22T00:00:00Z
---

# gRPC Sink Module

Sink module that delivers input records to a gRPC service — one call per record by default, or one call per micro-batch with `batch` — and emits the outcome of every call as a record. It is the gRPC counterpart of the [http sink](http.md) and shares its `auth`, retry, batching, concurrency and output conventions.

The service contract is a **protoc descriptor set** (`protoc --include_imports --descriptor_set_out=service.desc your.proto`), the same mechanism as the [query transform's grpc lookup source](../transform/query.md#grpc-source): requests and responses are built dynamically, so no service-specific code is needed. The descriptor file only has to be readable on the launcher; its bytes are shipped to the workers.

Typical uses: upsert records into an internal microservice, push events to a gRPC ingestion endpoint (client streaming), bulk-load through a "repeated items" request.

## Sink module common parameters

| parameter    | optional | type                | description |
|--------------|----------|---------------------|-------------|
| name         | required | String              | Step name. |
| module       | required | String              | Specified `grpc` |
| inputs       | required | Array<String\>      | Input step names. |
| waits        | optional | Array<String\>      | Steps to wait for. |
| strategy     | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming. |
| failFast     | optional | Boolean             | Fail the pipeline on the first error (default true in batch, false in streaming). With `false`, failed records go to `failureSinks` and a `FAILED` output record is emitted. |
| failureSinks | optional | Array<String\>      | Steps that receive failed records. |
| parameters   | required | Map<String,Object\> | Specify the following individual parameters |

## gRPC sink module parameters

| parameter             | optional | type | description |
|-----------------------|----------|------|-------------|
| target                | required | String | gRPC target: `host:port` or any name-resolver URI. |
| descriptorSetPath     | required | String | Path to the protoc descriptor set (`--include_imports` required). Read on the launcher. |
| method                | required | String | Fully-qualified method `package.Service/Method`. Unary or client-streaming (server-streaming methods are rejected). Resolved at assembly time. |
| plaintext             | optional | Boolean | Use plaintext instead of TLS (default `false`). |
| metadata              | optional | Map<String,String\> | Call metadata (headers). Values are templates on record fields; templates referencing no field are rendered once per worker. In batch mode they may only reference `batch.key` fields. |
| auth                  | optional | [Auth](http.md#auth-parameters) | Authentication provider shared with the http modules (`basic`, `bearer`, `apiKey`, `oauth2`, `gcpOidc`, `gcpOauth`); its headers are sent as call metadata (`authorization`). Tokens are cached per worker, refreshed before expiry and once after `UNAUTHENTICATED`. |
| request               | optional | [Request](#request-parameters) | How records become the request message. |
| response              | optional | [Response](#response-parameters) | Success condition and retry policy. |
| batch                 | optional | [Batch](#batch-parameters) | One call per micro-batch of records. |
| deadline              | optional | String | Per-attempt deadline (default `60s`). |
| concurrency           | optional | Integer | Max calls in flight per worker bundle (default `1`); the bundle commits only once every call resolved. |
| rate                  | optional | [Rate](http.md#rate-parameters) | Per-worker rate limit. |
| maxInboundMessageBytes| optional | Integer | Max response message size. |

### Request parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| mapping   | optional | Enum | `fields` (default): the record is converted to JSON and parsed into the request message — fields match by name, unknown fields are ignored, nested records / repeated fields / `google.protobuf.Timestamp` follow protobuf JSON rules. `template`: a FreeMarker template rendering the request as protobuf JSON (default when `template` is set). |
| fields    | optional | Array<String\> | Restrict the `fields` mapping to these record fields. |
| template  | optional | String | Protobuf-JSON template. Per record: the record's fields, `__element`, `__timestamp`, `__source`, `utils.*`. Per batch (unary method without `repeatedField`): `elements`, `size`, `key` — renders the whole batch request. |
| omitNulls | optional | Boolean | Drop null record fields before mapping (default `true`). |

### Response parameters

| parameter        | optional | type | description |
|------------------|----------|------|-------------|
| successCondition | optional | [Filter](../common/filter.md) | Condition on the response (`payload.<field>`, `status`) for an `OK` call to count as success; otherwise the call is `FAILED` (e.g. `{ key: payload.ok, op: "=", value: true }`). |
| retry.statuses   | optional | Array<String\> | gRPC status codes to retry (default `UNAVAILABLE, RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED, ABORTED`). `UNAUTHENTICATED` triggers one credential refresh instead. |
| retry.maxAttempts | optional | Integer | Total attempts (default `5`). |
| retry.initialBackoff / maxBackoff / totalTimeout | optional | String | Exponential backoff with jitter (defaults `1s` / `30s` / `5m`). |

Any other non-OK status (`INVALID_ARGUMENT`, `NOT_FOUND`, `PERMISSION_DENIED`, …) fails the record(s) immediately.

### Batch parameters

| parameter            | optional | type | description |
|----------------------|----------|------|-------------|
| maxSize / maxBytes   | optional | Integer / String | Batch limits (at least one required). |
| maxBufferingDuration | optional | String | Streaming: flush a partial batch after this time. |
| key                  | optional | String | Template; only records with the same rendered key share a call. Default: random shards. |
| shards               | optional | Integer | Random shards when `key` is omitted (default `8`). |
| repeatedField        | optional | String | Unary method: the repeated message field of the request that receives one message per record (`items`). Not needed for client-streaming methods (one message per record on the stream) or `request.template` (the template renders the whole batch). |

## Output schema

One record per call (after retries):

| field         | type | description |
|---------------|------|-------------|
| target        | STRING | |
| method        | STRING | |
| state         | STRING | `SUCCEEDED` / `FAILED` |
| status        | STRING | gRPC status code of the last attempt (`OK`, `UNAVAILABLE`, …). |
| statusMessage | STRING | Status description (null when OK). |
| payload       | JSON   | Response message as protobuf JSON. |
| attempts      | INT32  | |
| durationMs    | INT64  | Duration of the last attempt. |
| elementCount  | INT64  | Records in the call. |
| bytes         | INT64  | Serialized request size. |
| error         | STRING | Error message for FAILED. |
| timestamp     | TIMESTAMP | |

`FAILED` records are emitted only with `failFast: false`; the failed input records are also routed to `failureSinks`.

## Examples

### Per-record upsert with metadata and retry

```yaml
sinks:
  - name: upsert
    module: grpc
    inputs: [items]
    failFast: false
    failureSinks: [dead_letter]
    parameters:
      target: items.internal.example.com:443
      descriptorSetPath: gs://bucket/proto/items.desc
      method: shop.v1.ItemService/UpsertItem
      metadata: { x-tenant: "${tenant}" }
      auth: { type: gcpOidc, audience: https://items.internal.example.com }
      request: { fields: [id, name, price, updated_at] }
      response:
        retry: { statuses: [UNAVAILABLE, RESOURCE_EXHAUSTED], maxAttempts: 5 }
      concurrency: 8
      rate: { count: 200 }
```

### Bulk upsert through a repeated field, keyed by tenant

```yaml
parameters:
  target: items.internal.example.com:443
  descriptorSetPath: gs://bucket/proto/items.desc
  method: shop.v1.ItemService/BulkUpsert        # rpc BulkUpsert(BulkRequest{repeated Item items; string tenant})
  request:
    template: '{"tenant": "${key}", "items": [<#list elements as e>{"id": "${e.id}", "name": "${e.name?json_string}"}<#sep>, </#list>]}'
  batch: { maxSize: 500, maxBufferingDuration: 2s, key: "${tenant}" }
  response:
    successCondition: { key: payload.ok, op: "=", value: true }
```
(With `request.mapping: fields` instead, set `batch.repeatedField: items` and each record is mapped to one `Item`.)

### Client streaming

```yaml
parameters:
  target: ingest.internal.example.com:443
  descriptorSetPath: gs://bucket/proto/ingest.desc
  method: ingest.v1.Ingest/Push                 # rpc Push(stream Event) returns (Ack)
  batch: { maxSize: 1000, maxBufferingDuration: 1s }
```

## Notes

- Beam delivers at least once: a retried bundle re-sends its records. Prefer idempotent RPCs (upsert by id).
- The request message is built from JSON; for strict typing (int64 precision, bytes) follow protobuf JSON conventions (int64 as string is accepted, bytes as base64).
- `float32` request fields accept the pipeline's float64 values; enums accept names or numbers.
