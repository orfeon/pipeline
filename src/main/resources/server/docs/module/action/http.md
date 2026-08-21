---
type: Action Module
title: HTTP Action Module
description: Performs one HTTP request per trigger firing as a workflow step — the control-plane counterpart of the http sink. Notify (Slack / webhook) after upstream steps complete, trigger downstream jobs (Cloud Run Jobs, Airflow, Workflows), run maintenance calls around a bulk load (create index, disable refresh, commit, alias swap), or start an asynchronous API job and poll its status endpoint until a terminal condition holds (poll with until / failWhen / interval / timeout). Shares target / body / response / auth (basic, bearer, apiKey, OAuth2, GCP OIDC / OAuth) with the http sink; retries per the response policy and emits the common action envelope with the final response as payload.
tags: [action, http, rest, api, webhook, notify, trigger, poll, workflow]
timestamp: 2026-08-21T00:00:00Z
---

# HTTP Action Module

Action module (`action.http`) that sends **one HTTP request per firing**. It is the control-plane counterpart of the [http sink](../sink/http.md): the sink delivers data records; this action calls endpoints *about* the pipeline run — notifications, job triggers, maintenance calls, asynchronous job start + status polling. Placeable in sources/transforms/sinks; see [action modules](README.md) for placement, trigger semantics and the output envelope.

Typical workflows:

```
bigquery sink  →  action.http once (POST Slack webhook "load finished")
action.http once (PUT /items_v2, refresh_interval=-1)  →  http sink (_bulk)  →  action.http once (POST /items_v2/_refresh, waits)  →  action.http once (POST /_aliases)
action.http once (POST /jobs, poll until state=DONE)  →  next step
```

## Triggers and templates

| trigger      | elements delivered | template context |
|--------------|--------------------|------------------|
| `once` (default) | none — inputs and `waits` are completion signals | `utils.*`, `__timestamp` (firing time) |
| `perElement` | one record per firing | `${field}` of the record, `__element`, `__doc`, `__timestamp`, `__source` |
| `collect`    | all records in one firing | `elements` (list of field maps), `size` |

`target.url` / `params` / `headers` / `body.template` are [FreeMarker templates](../common/template.md) with the same variables as the http sink (`__body` in headers for signatures).

## Parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| trigger   | optional | Enum | `once`, `perElement`, `collect`. |
| target    | required | [Target](../sink/http.md#target-parameters) | URL, method (default `POST`), query params, headers, `auth` — identical to the http sink. |
| body      | optional | [Body](../sink/http.md#body-parameters) | Body serialization — identical to the http sink. With `once` only `template` / `none` produce a body (there is no record to serialize). With `collect`, `json` sends the array of records (`wrapper` applies), `ndjson` one line per record, `template` sees `elements` / `size`. |
| response  | optional | [Response](../sink/http.md#response-parameters) | `format` / `schema` / `success` / `retry` — identical to the http sink. Retries happen inside the firing (blocking). `partialFailure` is not supported here (use the sink). |
| poll      | optional | [Poll](#poll-parameters) | After a successful request, poll a status endpoint until done. |
| timeout   | optional | [Timeout](../sink/http.md#timeout-parameters) | Connect / per-attempt request timeout. |
| http      | optional | [Http](../sink/http.md#http-parameters) | HTTP client options. When `auth` is set, requests (including polls) are pinned to the hosts of `target.url` and `poll.url`; set `allowedHosts` when a poll URL comes from the response and points elsewhere. |

### Poll parameters

Polling starts after the initial request succeeded (per `response.success`). The poll URL and headers are templates over the trigger's context **plus** the initial response: `statusCode`, `headers` (first value per header, also lower-cased keys — `${headers.location}`), `body` (text) and `payload` (parsed JSON — `${payload.statusUrl}`, `${payload.name}`).

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| url       | required | String | Status endpoint template. |
| method    | optional | String | Default `GET`. |
| headers   | optional | Map<String,String\> | Extra headers (templates). `target.auth` applies to polls too. |
| until     | required | [Filter](../common/filter.md) | Terminal condition on the poll response (`statusCode`, `body`, `payload.<path>`). When true, the action completes successfully (unless `failWhen` matched first). |
| failWhen  | optional | [Filter](../common/filter.md) | Failure condition, evaluated before `until`; when true the firing fails (routed to `failureSinks` / fails the pipeline per `failFast`). |
| interval  | optional | String | Time between polls (default `10s`). |
| timeout   | optional | String | Give up (fail) after this long (default `1h`). |

Each poll response is classified by `response.success` / `retry` like the initial request (non-2xx polls are retried with backoff, then fail).

## Output envelope

| field | value |
|-------|-------|
| service | `http` |
| op | HTTP method of the request |
| jobId | Request URL (or the poll URL when `poll` is set) |
| state | `SUCCEEDED` (failures are thrown, not emitted) |
| payload | JSON `{"statusCode": …, "attempts": …, "body": <json body or text>}` of the final response (the last poll when polling) |

## Idempotency

A retried bundle re-sends the request (and re-polls). Use idempotent endpoints where possible (`PUT`, an `Idempotency-Key` header from `${utils.string.sha256(...)}`, job APIs that accept a client-supplied id). Notifications may be delivered twice in rare retry cases.

## Examples

### Slack notification after a load completes

```yaml
sinks:
  - name: load
    module: bigquery
    inputs: [rows]
    parameters: { table: "proj:ds.table" }
  - name: notify
    module: action.http
    inputs: [load]            # once: fires after the sink completed
    parameters:
      target:
        url: ${utils.secrets.get("projects/p/secrets/slack-webhook/versions/latest")}
      body:
        format: template
        template: '{"text": "load finished at ${__timestamp}"}'
```

### Elasticsearch reindex: prepare → bulk → refresh → alias swap

```yaml
sinks:
  - name: prepare
    module: action.http
    parameters:
      target:
        url: https://es.example.com/items_v2/_settings
        method: PUT
        auth: { type: basic, username: elastic, password: "${utils.secrets.get('projects/p/secrets/es-pw/versions/latest')}" }
      body: { format: template, template: '{"index": {"refresh_interval": "-1"}}' }
  - name: bulk
    module: http
    inputs: [items]
    waits: [prepare]
    parameters: { target: { url: https://es.example.com/_bulk, auth: {...} }, body: { format: ndjson, template: "..." }, batch: { maxSize: 1000 } }
  - name: refresh
    module: action.http
    inputs: [bulk]
    parameters:
      target: { url: https://es.example.com/items_v2/_refresh, auth: {...} }
      body: { format: none }
  - name: swap
    module: action.http
    inputs: [refresh]
    parameters:
      target: { url: https://es.example.com/_aliases, auth: {...} }
      body:
        format: template
        template: '{"actions": [{"remove": {"index": "items_v1", "alias": "items"}}, {"add": {"index": "items_v2", "alias": "items"}}]}'
```

### Start an asynchronous job and wait for it

```yaml
sinks:
  - name: export
    module: action.http
    inputs: [load]
    parameters:
      target:
        url: https://api.example.com/v1/exports
        auth: { type: oauth2, tokenUrl: https://auth.example.com/token, clientId: "${utils.secrets.get('...')}", clientSecret: "${utils.secrets.get('...')}" }
      body: { format: template, template: '{"dataset": "daily", "date": "${utils.datetime.currentDate()}"}' }
      poll:
        url: ${payload.statusUrl}            # or ${headers.location}
        until: { key: payload.state, op: in, value: [DONE, FAILED] }
        failWhen: { key: payload.state, op: "=", value: FAILED }
        interval: 30s
        timeout: 2h
```

### Per-record trigger of a Cloud Run Job with the pipeline's identity

```yaml
sinks:
  - name: run_job
    module: action.http
    inputs: [tenants]
    parameters:
      trigger: perElement
      target:
        url: https://run.googleapis.com/v2/projects/p/locations/asia-northeast1/jobs/process-${tenant}:run
        auth: { type: gcpOauth }
      body: { format: none }
```
