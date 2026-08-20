---
type: Action Module
title: Tasks Action Module
description: Cloud Tasks queue operations as workflow steps — the control-plane counterpart of the tasks sink. Creates a queue with rate limits / retry config (idempotent, adopts an existing queue), updates, pauses, resumes, purges or deletes it, waits until the queue has drained (waitForEmpty, so a later step can rely on every enqueued task having run), runs or deletes a single task, or reads the queue state into the envelope payload.
tags: [action, tasks, cloudtasks, queue, pause, resume, purge, wait, trigger, workflow, gcp]
timestamp: 2026-08-20T00:00:00Z
---

# Tasks Action Module

Action module (`action.tasks`) for [Google Cloud Tasks](https://cloud.google.com/tasks/docs) **queue** operations. It is the control-plane counterpart of the [tasks sink](../sink/tasks.md): the sink enqueues data records as tasks; this action manages the queue around them. Placeable in sources/transforms/sinks; see [action modules](README.md) for placement, trigger semantics and the output envelope.

Typical workflow:

```
action.tasks (create / resume)  →  tasks sink (enqueue records)  →  action.tasks (waitForEmpty)  →  next step
```

## Operations

| op            | effect | state in envelope | idempotent on bundle retry |
|---------------|--------|-------------------|----------------------------|
| `create`      | Create the queue with `rateLimits` / `retryConfig`. | `DONE`, or `EXISTS` when the queue already existed (it is adopted, not modified — use `update` to change settings). | yes |
| `update`      | Update `rateLimits` and/or `retryConfig` (only the blocks given are touched). | `DONE` | yes |
| `delete`      | Delete the queue. | `DONE`, or `NOT_FOUND` | yes |
| `pause`       | Stop dispatching (tasks keep accumulating). | `DONE` | yes |
| `resume`      | Resume dispatching. | `DONE` | yes |
| `purge`       | Delete all tasks in the queue. | `DONE` | yes |
| `get`         | Read the queue (payload = queue JSON: state, rate limits, retry config). | `DONE` | yes |
| `waitForEmpty`| Poll `listTasks` every `pollIntervalSeconds` until no task is left (completed **and** exhausted-retry tasks disappear; tasks that are still retrying keep the wait alive). Throws on `timeoutSeconds`. | `DONE` (payload: `polls`, `waitedSeconds`) | yes |
| `runTask`     | Force-dispatch one task now (`task`). | `DONE` (payload = task JSON) | **no** — a retried bundle may dispatch twice |
| `deleteTask`  | Delete one task (`task`). | `DONE`, or `NOT_FOUND` | yes |

`jobId` in the envelope is the queue name (or the task name for `runTask` / `deleteTask`).

## Parameters

| parameter           | optional | type       | description |
|---------------------|----------|------------|-------------|
| trigger             | optional | Enum       | `once` (default), `perElement`, `collect` — see [action README](README.md). `queue` and `task` are FreeMarker templates over the trigger's data (`${field}` for perElement; `elements`/`size` for collect). |
| op                  | required | Enum       | One of the operations above. |
| queue               | required | String     | `projects/{project}/locations/{location}/queues/{queue}`. Template allowed. |
| task                | optional | String     | `runTask` / `deleteTask`: task id (or full task name). Template allowed, e.g. `${taskName}` from a tasks sink record. |
| rateLimits          | optional | RateLimits | `create` / `update`: `maxDispatchesPerSecond` (Double), `maxConcurrentDispatches` (Integer). |
| retryConfig         | optional | RetryConfig| `create` / `update`: `maxAttempts` (Integer, -1 = unlimited), `maxRetryDuration`, `minBackoff`, `maxBackoff` (durations: `PT1M`, `30s`, `2h`), `maxDoublings` (Integer). |
| timeoutSeconds      | optional | Long       | `waitForEmpty` deadline. Default 86400. |
| pollIntervalSeconds | optional | Long       | `waitForEmpty` poll interval. Default 10. |
| endpoint            | optional | String     | Custom gRPC endpoint (`host:port`, plaintext) for the Cloud Tasks emulator. |

The executing service account needs `roles/cloudtasks.queueAdmin` for queue operations (`cloudtasks.enqueuer` is not enough) and `cloudtasks.tasks.*` for `runTask` / `deleteTask` / `waitForEmpty` (`listTasks`).

## Notes

- Queue names are reserved for about 7 days after deletion; prefer `pause`/`purge` over `delete`+`create` for recurring jobs.
- Cloud Tasks applies new rate limits gradually; an `update` right before a burst does not take effect instantly.
- `waitForEmpty` counts tasks via `listTasks` (the v2 API exposes no queue statistics), scanning at most 1000 per poll — it is a readiness gate, not a metric.
- Pipelines have no try/finally: a trailing `resume`/`delete` step does not run if an earlier step fails.

## Examples

### Create the queue, enqueue, wait for completion, then load results

```yaml
sources:
  - name: queue
    module: action.tasks
    parameters:
      op: create
      queue: projects/myproject/locations/asia-northeast1/queues/subruns
      rateLimits:
        maxConcurrentDispatches: 4
      retryConfig:
        maxAttempts: 3
        minBackoff: 10s
  - name: tables
    module: bigquery
    parameters:
      query: SELECT table_name FROM `myproject.app.INFORMATION_SCHEMA.TABLES`
sinks:
  - name: fanout
    module: tasks
    inputs: [tables]
    waits: [queue]
    parameters:
      queue: projects/myproject/locations/asia-northeast1/queues/subruns
      target:
        url: https://pipeline-xxxx.a.run.app/run?args.table=${table_name}
        auth: { type: oidc, serviceAccount: pipeline@myproject.iam.gserviceaccount.com }
      body: { format: none }
      task: { id: "${table_name}-${args.run_id}", dispatchDeadline: 30m }
  - name: drained
    module: action.tasks
    inputs: [fanout]                    # fires once, after every task has been enqueued
    parameters:
      op: waitForEmpty
      queue: projects/myproject/locations/asia-northeast1/queues/subruns
      pollIntervalSeconds: 30
      timeoutSeconds: 7200
  - name: load
    module: action.bigquery
    waits: [drained]                    # all sub-runs have finished
    parameters:
      query: CALL `myproject.app.merge_subrun_results`()
```

### Pause during a backfill, resume afterwards

```yaml
sources:
  - name: pause
    module: action.tasks
    parameters: { op: pause, queue: projects/myproject/locations/asia-northeast1/queues/notify }
transforms:
  - name: backfill
    module: tasks
    inputs: [events]
    waits: [pause]
    parameters: { queue: projects/myproject/locations/asia-northeast1/queues/notify, target: { url: https://notifier/notify } }
sinks:
  - name: resume
    module: action.tasks
    inputs: [backfill]
    parameters: { op: resume, queue: projects/myproject/locations/asia-northeast1/queues/notify }
```

### Delete tasks named in input records (perElement)

```yaml
sinks:
  - name: cancel
    module: action.tasks
    inputs: [cancellations]
    parameters:
      trigger: perElement
      op: deleteTask
      queue: projects/myproject/locations/asia-northeast1/queues/reminders
      task: "${reminder_id}"
```
