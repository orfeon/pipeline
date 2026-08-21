---
type: Sink Module
title: Tasks Sink Module
description: Enqueues input records as Google Cloud Tasks HTTP tasks (one task per record, or micro-batches of records per task). The queue provides rate limiting, retries, delayed/scheduled execution (scheduleTime/delay), name-based deduplication (idempotent task.id) and OIDC/OAuth authentication to the target (gcpOidc / gcpOauth). Supports per-record FreeMarker templates for URL, headers, task id and schedule, JSON / Avro / Protobuf / template bodies, a body size guard with automatic batch splitting, keyed micro-batching (GroupIntoBatches), queue-level HTTP routing (tasks:buffer) and emits one control record per task (CREATED / ALREADY_EXISTS / FAILED).
tags: [sink, tasks, cloudtasks, gcp, http, webhook, scheduling, batch, streaming]
timestamp: 2026-08-20T00:00:00Z
---

# Tasks Sink Module

Sink Module that creates [Google Cloud Tasks](https://cloud.google.com/tasks/docs) HTTP tasks from input records — one task per record by default, or one task per micro-batch of records with `batch`.

Use it when a pipeline must call an HTTP endpoint for every record **and** you want the call to be governed by a queue rather than by the pipeline:

| Queue capability | What it gives the pipeline |
|---|---|
| `maxDispatchesPerSecond` / `maxConcurrentDispatches` | Cluster-wide protection of the target. No per-worker rate limiting needed. |
| `retryConfig` | The pipeline finishes as soon as tasks are enqueued; target outages are retried by the queue. |
| `scheduleTime` | Delayed / scheduled execution per record (reminders, TTL expiry, "check again in 10 minutes"), up to 30 days ahead. |
| Named tasks | Deduplication: a task name is accepted once. Used for idempotent enqueueing (see [Idempotency](#idempotency)). |
| `oidcToken` / `oauthToken` | Authenticated calls to private Cloud Run / Cloud Functions / Google APIs without the pipeline holding the target's credentials. |

Typical use cases: per-record webhooks or third-party API calls (e.g. measurement / CRM S2S events), delayed notifications, fanning out a batch job into many sub-runs of this pipeline's own [Cloud Run serve mode](../../deploy/cloud-run-service.md) (`POST /run?args.*`), triggering Cloud Workflows / Cloud Run Jobs, and streaming event → task conversion with deduplication.

If you only need fire-and-forget fan-out without scheduling, rate limiting or named-task dedup, prefer the [pubsub](pubsub.md) sink (higher throughput, cheaper). If you need the response of the call (ids, per-item results), a synchronous call, or no GCP dependency, use the [http](http.md) sink; its `target` / `body` / `batch` parameters are the same.

## Sink module common parameters

| parameter    | optional | type                | description                                                           |
|--------------|----------|---------------------|-----------------------------------------------------------------------|
| name         | required | String              | Step name. specified to be unique in config file.                     |
| module       | required | String              | Specified `tasks`                                                     |
| inputs       | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits        | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy     | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.             |
| failFast     | optional | Boolean             | Fail the pipeline on the first error (default true in batch, false in streaming). With `false`, failed records go to `failureSinks` and a `FAILED` output record is emitted. |
| failureSinks | optional | Array<String\>      | Steps that receive failed records.                                    |
| parameters   | required | Map<String,Object\> | Specify the following individual parameters                          |

## Tasks sink module parameters

| parameter       | optional | type   | description |
|-----------------|----------|--------|-------------|
| queue           | required | String | Queue resource name: `projects/{project}/locations/{location}/queues/{queue}`. Supports FreeMarker templates on record fields (e.g. per-tenant or sharded queues). |
| target          | optional | Target | HTTP target of each task (see below). **If omitted, tasks are sent with `tasks:buffer`** to the queue's own [HTTP target](https://cloud.google.com/tasks/docs/creating-http-target-tasks#queue-level) — URL, headers and auth are then managed on the queue and only the body is taken from the record. |
| body            | optional | Body   | Task payload (see below). Default: the record as JSON. |
| task            | optional | Task   | Task name, schedule and deadline (see below). |
| batch           | optional | Batch  | Pack several records into one task (see [Batching](#batching)). Default: one record = one task. |
| retry           | optional | Retry  | Retry policy of the `createTask` RPC itself (not of the target call — that is the queue's `retryConfig`). |
| onAlreadyExists | optional | Enum   | What to do when a named task already exists: `success` (default; output record has `state: ALREADY_EXISTS`) or `fail`. |
| concurrency     | optional | Integer | Number of `createTask` calls in flight per worker bundle (default 1 = sequential). Raises enqueue throughput for large batch loads; the bundle still completes only after every call has returned. Does not apply to `tasks:buffer`. |
| endpoint        | optional | String | Custom gRPC endpoint (`host:port`, plaintext, no credentials) for the [Cloud Tasks emulator](https://github.com/aertje/cloud-tasks-emulator). `tasks:buffer` is not available against an emulator. |

### target

| parameter | optional | type               | description |
|-----------|----------|--------------------|-------------|
| url       | required | String             | Target URL. Template on record fields, e.g. `https://api.example.com/users/${user_id}` (use `${field?url}` to percent-encode). |
| method    | optional | Enum               | `POST` (default), `GET`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`. The body is not sent for `GET`/`HEAD`. |
| params    | optional | Map<String,String\> | Query parameters appended to the URL (values are templates, URL-encoded). |
| headers   | optional | Map<String,String\> | Request headers. Values are templates. Headers that reference no record field (e.g. an API key via `${utils.secrets.get("projects/p/secrets/s/versions/latest")}`) are rendered once per worker. A header referencing `__body` is rendered after the body (signature headers). |
| auth      | optional | Auth               | Token attached by Cloud Tasks when dispatching. |

### target.auth

| parameter      | optional | type   | description |
|----------------|----------|--------|-------------|
| type           | optional | Enum   | `none` (default), `gcpOidc` (ID token — Cloud Run, Cloud Functions, your own services), `gcpOauth` (access token — Google APIs). Same names as the [http sink](http.md#auth-parameters); the other http-sink auth types are not available because Cloud Tasks attaches the token itself. |
| serviceAccount | optional | String | Service account email whose token is attached. Defaults to the default service account of the launching environment (metadata server); required when not launching from GCP. The account creating the tasks needs `roles/cloudtasks.enqueuer` and `iam.serviceAccounts.actAs` on this account. |
| audience       | optional | String | `gcpOidc` only. Defaults to `url` without its query string. |
| scope          | optional | String | `gcpOauth` only. Default `https://www.googleapis.com/auth/cloud-platform`. |

### body

| parameter | optional | type    | description |
|-----------|----------|---------|-------------|
| format    | optional | Enum    | Same as the [http sink](http.md#body-parameters): `json` (default; the record as a JSON object — a JSON array in batch mode, `wrapper` / `fields` apply), `ndjson`, `form`, `bytes`, `avro` (Avro binary of the record — an Avro Object Container File in batch mode; the Avro schema is derived from the input schema or taken from the module `schema`), `protobuf` (serialized message — length-delimited messages in batch mode; requires `schema.protobuf.descriptorFile` / `messageName`), `template` (FreeMarker text), `none` (no body). `template` is implied when `template` is set. |
| template  | optional | String  | FreeMarker template for `format: template`. Variables: record fields, `__timestamp` (event time), `__source` (input step name), `utils.*`; in batch mode additionally `elements` (list of records), `size` and `key`. Use `?json_string` for escaping. |
| omitNulls | optional | Boolean | `json` only. Drop `null` fields (recursively). Default `false`. Useful for APIs with many optional fields. |
| maxBytes  | optional | String  | Size such as `100KB` / `1MB` / `1024`. Reject records whose serialized body exceeds this size: no task is created, the record goes to `failureSinks` and a `FAILED` output record is emitted. In batch mode an oversized batch is first split in halves until each task fits (a single record that still exceeds the limit is rejected). Set it below the target's limit so Cloud Tasks never retries a request that can never succeed. |

### task

| parameter        | optional | type    | description |
|------------------|----------|---------|-------------|
| id               | optional | String  | Template for the task name (must reference record fields). When set, the task is **named** and Cloud Tasks rejects duplicates (`ALREADY_EXISTS`), which makes enqueueing idempotent across Beam bundle retries and at-least-once upstreams. When omitted, Cloud Tasks assigns a random name and enqueueing is at-least-once. |
| hashId           | optional | Boolean | Hash the rendered `id` with SHA-256 (hex). Default `true` — Cloud Tasks recommends non-sequential ids (sequential ids like timestamps increase latency and error rates), and the hash also guarantees the `[A-Za-z0-9_-]{1,500}` format. The resulting name is available in the output record. |
| scheduleTime     | optional | String  | When to dispatch. Either a field reference (`${remind_at}` — TIMESTAMP, epoch seconds/millis/micros or RFC 3339 string field) or a template producing an RFC 3339 string. Past times dispatch immediately; max 30 days ahead. |
| delay            | optional | String  | Dispatch after a relative delay. ISO-8601 (`PT10M`) or short form (`30s`, `10m`, `2h`, `1d`). Template allowed. Exclusive with `scheduleTime`. |
| dispatchDeadline | optional | String  | Per-attempt deadline for the target call. `15s`–`30m` (Cloud Tasks default `10m`). Keep it short for non-idempotent targets (see below). |

### batch

| parameter           | optional | type    | description |
|---------------------|----------|---------|-------------|
| maxSize             | optional | Integer | Maximum records per task. At least one of `maxSize` / `maxBytes` is required. |
| maxBytes            | optional | String  | Maximum (approximate, pre-serialization) bytes per task (`1MB`) as seen by Beam's `GroupIntoBatches`. Use `body.maxBytes` for the exact limit on the serialized body. |
| maxBufferingDuration | optional | String | Streaming only: flush an incomplete batch after this duration (`10s`, `PT1M`). |
| key                 | optional | String  | Template on record fields; only records with the same rendered key share a task (e.g. `${tenant}`). When omitted, records are spread over `shards` random groups. |
| shards              | optional | Integer | Number of random groups when `key` is omitted (parallelism of batch formation). Default 8. |

### retry

| parameter                | optional | type    | description |
|--------------------------|----------|---------|-------------|
| maxAttempts              | optional | Integer | Default 5. Retries `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `INTERNAL` of `createTask`. |
| initialRetryDelaySeconds | optional | Double  | Default 0.5 (exponential, x2). |
| maxRetryDelaySeconds     | optional | Double  | Default 10. |
| totalTimeoutSeconds      | optional | Double  | Default 60. |

## Output

The sink emits one **control record** per input record (consumable by `waits` of any step and by action modules; see [action README](../action/README.md) for the data/control two-plane rule):

| field        | type      | description |
|--------------|-----------|-------------|
| queue        | String    | Rendered queue name. |
| taskName     | String    | `projects/.../queues/.../tasks/{id}` returned by the API. |
| url          | String    | Rendered target URL (`null` when `target` is omitted). |
| state        | String    | `CREATED`, `ALREADY_EXISTS` or `FAILED` (only with `failFast: false`). |
| scheduleTime | Timestamp | Schedule time returned by the API. |
| createTime   | Timestamp | Create time returned by the API. |
| elementCount | Long      | Number of records in the task (1 without `batch`). |
| bytes        | Long      | Body size. |
| error        | String    | Error message for `FAILED`. |
| timestamp    | Timestamp | Processing time. |

## Idempotency

- **Enqueue side**: Beam may re-run a bundle, which re-issues `createTask` for the same records. With `task.id` the second call gets `ALREADY_EXISTS` and is reported as success; without it, duplicate tasks may be created. Always set `task.id` when the target is not idempotent.
- Task names stay reserved for up to **24 hours after the task is deleted or executed** (9 days for queues created with `queue.yaml`). Include a run date or run id in `task.id` if the same logical record can legitimately be enqueued again on a later run.
- **Dispatch side**: Cloud Tasks delivers at-least-once — if the target times out after processing, the queue retries. Use a short `dispatchDeadline` and a conservative queue `retryConfig` for non-idempotent targets.
- Cloud Tasks retries **every non-2xx response**, including `4xx` validation errors that can never succeed. Validate on the pipeline side where possible (`body.maxBytes`, upstream `filter`) and bound `retryConfig.maxAttempts` on the queue.

## Notes

- Queue management (create with rate limits, pause/resume, purge, waiting until the queue has drained before a next step) is the job of the [tasks action](../action/tasks.md) (`action.tasks`).

- `createTask` throughput is limited per queue (on the order of several hundred requests per second). For very large batch loads shard across queues with a template in `queue`.
- Headers (including API keys) are stored in the task and visible to anyone with `cloudtasks.tasks.fullView`. To keep secrets out of tasks, configure the API key as a header override on a queue-level HTTP target and omit `target` in the sink.
- Task payload size is limited by Cloud Tasks (see the [quotas](https://cloud.google.com/tasks/docs/quotas) page); guard with `body.maxBytes`.
- Queues are regional; the `location` in `queue` must be the queue's region.

## Batching

With `batch`, records are grouped with Beam's `GroupIntoBatches` (works in batch and streaming pipelines) and each group becomes **one task** whose body holds all records (`json` → array, `avro` → container file, `protobuf` → delimited messages, `template` → rendered with `elements`/`size`/`key`). Cloud Tasks has no bulk create API, so batching reduces calls to the *target*, not `createTask` RPCs.

Because every record in a task shares one URL, header set, task name and schedule, the templates of `queue`, `target.url`, `target.headers`, `task.id`, `task.scheduleTime` and `task.delay` may only reference fields that also appear in `batch.key` (records in a batch are equal on those fields). Referencing any other field is an assembly-time error. The body template is free to use every field through `elements`.

```yaml
sinks:
  - name: bulk
    module: tasks
    inputs: [events]
    parameters:
      queue: projects/myproject/locations/asia-northeast1/queues/${tenant}
      target:
        url: https://api.example.com/${tenant}/events:batch
        headers:
          Content-Type: application/json
          X-Batch-Size: ${size}
        auth: { type: gcpOidc }
      body:
        template: '{"events": [<#list elements as e>{"id": "${e.id}", "ts": "${e.event_time}"}<#sep>,</#list>]}'
        maxBytes: 65536
      task:
        id: "${tenant}-${key}-${utils.string.uuid()}"   # per-task unique; use a deterministic key if the target dedups
      batch:
        maxSize: 200
        maxBufferingDuration: 5s
        key: "${tenant}"
```

## Examples

### Per-record webhook with OIDC, schedule from a field

```yaml
sources:
  - name: reminders
    module: bigquery
    parameters:
      query: SELECT user_id, remind_at, message FROM `myproject.app.reminders` WHERE dt = "${args.dt}"
sinks:
  - name: enqueue
    module: tasks
    inputs: [reminders]
    parameters:
      queue: projects/myproject/locations/asia-northeast1/queues/reminders
      target:
        url: https://notifier-xxxx.a.run.app/notify/${user_id}
        headers:
          Content-Type: application/json
        auth:
          type: gcpOidc
          serviceAccount: notifier-invoker@myproject.iam.gserviceaccount.com
      body:
        format: json
        omitNulls: true
      task:
        id: "${user_id}-${remind_at}-${args.dt}"
        scheduleTime: "${remind_at}"
        dispatchDeadline: 30s
```

### External API with API key and a custom body (one event per request)

```yaml
sinks:
  - name: s2s
    module: tasks
    inputs: [events]
    failFast: false
    failureSinks: [rejected]
    parameters:
      queue: projects/myproject/locations/asia-northeast1/queues/s2s-events
      target:
        url: https://api.example.com/inappevent/${app_id?url}
        headers:
          Content-Type: application/json
          authentication: ${utils.secrets.get("projects/myproject/secrets/s2s-api-key/versions/latest")}
      body:
        maxBytes: 1024
        template: |
          {
            "appsflyer_id": "${appsflyer_id}",
            <#if customer_user_id??>"customer_user_id": "${customer_user_id?json_string}",</#if>
            "eventName": "${event_name?json_string}",
            "eventValue": "${event_value?json_string}",
            "eventTime": "${utils.datetime.formatTimestamp(event_time, "yyyy-MM-dd HH:mm:ss.SSS", "UTC")}"
          }
      task:
        id: "${appsflyer_id}-${event_name}-${event_time}"
```

### Fan-out into sub-runs of this pipeline (Cloud Run serve mode)

```yaml
sinks:
  - name: fanout
    module: tasks
    inputs: [tables]
    parameters:
      queue: projects/myproject/locations/asia-northeast1/queues/subruns   # maxConcurrentDispatches controls parallelism
      target:
        url: https://pipeline-xxxx.a.run.app/run?args.table=${table_name}
        auth:
          type: gcpOidc
          serviceAccount: pipeline@myproject.iam.gserviceaccount.com
      body:
        format: none
      task:
        id: "${run_id}-${table_name}"
        dispatchDeadline: 30m
```

### Queue-level HTTP target (URL and credentials managed on the queue)

```yaml
sinks:
  - name: enqueue
    module: tasks
    inputs: [events]
    parameters:
      queue: projects/myproject/locations/asia-northeast1/queues/webhook   # queue has an HTTP target configured
      body:
        format: json
```

### Delayed task, 10 minutes after the event

```yaml
sinks:
  - name: recheck
    module: tasks
    inputs: [orders]
    parameters:
      queue: projects/myproject/locations/asia-northeast1/queues/recheck
      target:
        url: https://orders-xxxx.a.run.app/recheck
        auth: { type: gcpOidc }
      task:
        id: "${order_id}"
        delay: 10m
```
