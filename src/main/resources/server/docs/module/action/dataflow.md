---
type: Action Module
title: Dataflow Action Module
description: Cloud Dataflow job operations as workflow steps (Dataflow REST API v1b3). Launches a Flex Template job with config/args and waits for it (terminal for batch, running for streaming; idempotent via a deterministic jobName that adopts an already-active job), reads or lists jobs into the payload as a guard (failWhen / skipWhen — is the streaming consumer running, do not launch a duplicate), waits for jobs launched elsewhere (jobIdField with collect), cancels / drains / rescales a job (jobs.update requestedState / runtimeUpdatableParams) and reads error messages for diagnosis. Runs from any runner, so a lightweight Direct pipeline on Cloud Run can orchestrate heavy Dataflow batches.
tags: [action, dataflow, job, flextemplate, launch, wait, cancel, drain, scale, guard, orchestration, trigger, workflow, gcp]
timestamp: 2026-08-28T00:00:00Z
---

# Dataflow Action Module

Action module (`actions` section, `module: dataflow`) for [Cloud Dataflow](https://cloud.google.com/dataflow/docs/reference/rest) **job** operations. It lets a pipeline launch other pipelines as Dataflow jobs and wait for them, gate on the state of a job, or cancel / drain / rescale one. See [action modules](README.md) for the `actions` section, trigger semantics and the output envelope.

Typical uses:

- **Lightweight orchestrator → heavy batch**: a Direct pipeline on Cloud Run (serve mode or Cloud Run Job) launches a Dataflow batch job on request and, once it is done, continues with a [bigquery](bigquery.md) MERGE or an [http](http.md) notification — Dataflow is used only when the heavy work is needed.
- **Fan-out**: one child job per table / partition (`trigger: perElement`, `wait: false`), then a single `jobs.wait` step (`trigger: collect`, `jobIdField`) before the merge.
- **Guard**: `jobs.list` (`filter: ACTIVE`, `jobName`) + `skipWhen` / `failWhen` — do not launch a streaming job twice, or do not publish when the consumer is not running.
- **Safe streaming redeploy**: `jobs.update` `requestedState: JOB_STATE_DRAINED` (waits until drained) → `flexTemplates.launch`; or an in-place update with `update: true`.

## Operations

`operation` is a module-level field (next to `name` / `module`), named after the REST API `resource.method`.

| operation | effect | state in envelope | idempotent on bundle retry |
|---|---|---|---|
| `flexTemplates.launch` | Launch a Flex Template job ([`LaunchFlexTemplateParameter`](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch)) and, by default, wait for it. | The job's `currentState` (`JOB_STATE_DONE`, `JOB_STATE_RUNNING`, …), or `EXISTS` when an active job with the same name was adopted. | yes with a deterministic `jobName` (see below) |
| `jobs.get` | Read one job (`jobId`, or the latest job named `jobName`). | `currentState` | yes |
| `jobs.list` | List jobs (`filter`, optionally narrowed to `jobName`). Payload: `jobs[]`, `count`, `firstJob`. | `DONE` | yes |
| `jobs.wait` | Wait for jobs launched elsewhere: `jobId` / `jobName`, or with `trigger: collect` every id in `jobIdField` (one poll loop for all). Payload: the `Job`, or `jobs[]` / `count` / `firstJob` for several. | `currentState`, or `DONE` for several | yes |
| `jobs.update` | [`jobs.update`](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/update): `requestedState` (`JOB_STATE_CANCELLED` / `JOB_STATE_DRAINED`, waits until reached) and/or `runtimeUpdatableParams` (`minNumWorkers` / `maxNumWorkers` / `workerUtilizationHint`, streaming). | `currentState` | yes |
| `jobs.messages.list` | Job messages of `minimumImportance` and above (default `JOB_MESSAGE_ERROR`). Payload: `messages[]`, `count`, `currentState`. | `DONE` | yes |

`jobId` in the envelope is the Dataflow job id (comma-joined for a multi-job `jobs.wait`).

### Idempotency and failure handling

Dataflow rejects a second **active** job with the same name in a project / region. A deterministic `jobName` (e.g. `backfill-${table}-${args.run_id}`) therefore makes `flexTemplates.launch` safe on a retried bundle: ALREADY_EXISTS adopts the running job (state `EXISTS`) and waits for it like a fresh launch. The name of a *finished* job can be reused, so a retry after completion launches again. Without `jobName` the step launches as `<step name>-<yyyyMMddHHmmss>` (a WARN is logged; not idempotent).

| situation | behaviour |
|---|---|
| Job ends `JOB_STATE_FAILED` | Non-retryable failure; the error messages (`jobs.messages.list`, ERROR) are attached to the failure description. |
| Job is `JOB_STATE_CANCELLED` by someone else while waited for | Non-retryable failure. A `JOB_STATE_DRAINED` job counts as completed. |
| `timeoutSeconds` exceeded | Non-retryable failure; the job is cancelled first when `cancelOnTimeout` is true (default for jobs this step launched). |
| API errors on launch / poll (UNAVAILABLE, DEADLINE_EXCEEDED, …) | Retryable — use the module-level `retry`. |

Waiting happens inside the action's DoFn (the same model as the [bigquery](bigquery.md) action), so a `perElement` launch with `wait: true` occupies one worker thread per element and elements of the same bundle wait one after another. For fan-out prefer `wait: false` + a `collect` `jobs.wait` (example below): launches are instant and one step polls every job. Concurrency is then bounded by Dataflow's concurrent-job quota (jobs beyond it wait in `JOB_STATE_QUEUED`).

## Parameters

### Common

| parameter | optional | type | description |
|---|---|---|---|
| projectId | optional | String | Project of the job. Default: the pipeline's project. Template allowed. |
| region | optional | String | Regional endpoint (`asia-northeast1`). Default: the pipeline's Dataflow region when running on Dataflow; required on other runners. Template allowed. |
| jobId | conditionally required | String | Target job for `jobs.get` / `jobs.wait` / `jobs.update` / `jobs.messages.list`. Template allowed, e.g. `${jobId}` from a launch envelope. |
| jobName | conditionally required | String | Alternative to `jobId`: the **latest** job with this name. For `flexTemplates.launch` the name of the new job (`[a-z]([-a-z0-9]{0,1022}[a-z0-9])?`). Template allowed. |
| jobIdField | optional | String | `jobs.wait` with `trigger: collect`: field of the collected elements holding job ids (`jobId` of launch envelopes). |
| wait | optional | Boolean | `flexTemplates.launch` / `jobs.update` with `requestedState`: wait for the job. Default `true`. |
| waitUntil | optional | Enum | `terminal` (DONE / FAILED / CANCELLED / DRAINED / UPDATED), `running` (RUNNING, or terminal), `none`. Default for a launch: `terminal` for a batch job, `running` for a streaming job (a streaming job never ends by itself); `terminal` for `jobs.wait`. |
| timeoutSeconds | optional | Long | Wait deadline; exceeding it fails the step. Default `86400`. |
| cancelOnTimeout | optional | Boolean | Cancel the job when the deadline passes. Default `true` for `flexTemplates.launch`, `false` otherwise (a job launched elsewhere is not ours to cancel). |
| view | optional | Enum | `JobView` of the payload for `jobs.get`: `JOB_VIEW_SUMMARY` (default), `JOB_VIEW_ALL` (includes steps and environment — large), `JOB_VIEW_DESCRIPTION`. |
| endpoint | optional | String | Custom gRPC endpoint (`host:port`, plaintext) for tests. |

### flexTemplates.launch

The parameters mirror [`LaunchFlexTemplateParameter`](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#LaunchFlexTemplateParameter); `config` and `args` are conveniences that fill the Mercari Pipeline template parameters.

| parameter | optional | type | description |
|---|---|---|---|
| containerSpecGcsPath | required | String | Flex Template spec (`gs://…/dataflow.json`). Template allowed. |
| config | optional | String | The child pipeline's config: a `gs://…` / `ar://…` / Parameter Manager reference, or the config text itself. Sets the template parameter `config`. Template allowed. Inline text is also expanded by the *parent* config's FreeMarker pass — wrap it in `<#noparse>…</#noparse>` or, preferably, reference a file. |
| args | optional | Map<String,String\> | Child pipeline `args` (`${args.x}` in the child config); each becomes the template parameter `args.<key>`. Values are templates, e.g. `table: ${table_name}` with `trigger: perElement`. |
| parameters | optional | Map<String,String\> | Any other template parameters (values template-able). `config` / `args` win on conflict. |
| launchOptions | optional | Map<String,String\> | `launchOptions` of the request. |
| environment | optional | [FlexTemplateRuntimeEnvironment](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#FlexTemplateRuntimeEnvironment) | REST field names: `maxWorkers`, `numWorkers`, `machineType`, `serviceAccountEmail`, `subnetwork`, `network`, `tempLocation`, `stagingLocation`, `additionalUserLabels`, `additionalExperiments`, `enableStreamingEngine`, `workerRegion`, `workerZone`, `ipConfiguration`, `kmsKeyName`, … When running on Dataflow, `serviceAccountEmail`, `subnetwork`, `tempLocation` and `stagingLocation` default to the parent job's values. |
| update | optional | Boolean | In-place update of the running streaming job named `jobName` (`transformNameMappings` optional). |
| transformNameMappings | optional | Map<String,String\> | Transform name mapping for `update`. |

### jobs.update

| parameter | optional | type | description |
|---|---|---|---|
| requestedState | optional | String | `JOB_STATE_CANCELLED` or `JOB_STATE_DRAINED` (the `JOB_STATE_` prefix may be omitted). With `wait` (default) the step returns once the job reached it. |
| runtimeUpdatableParams | optional | Object | `maxNumWorkers`, `minNumWorkers` (Integer), `workerUtilizationHint` (Double) — applied with the matching `updateMask`; streaming jobs only. |

### jobs.list / jobs.messages.list

| parameter | optional | type | description |
|---|---|---|---|
| filter | optional | Enum | `ALL` (default), `ACTIVE`, `TERMINATED`. |
| limit | optional | Integer | Max jobs / messages returned. Default `100`. |
| minimumImportance | optional | Enum | `jobs.messages.list`: `JOB_MESSAGE_DEBUG` … `JOB_MESSAGE_ERROR` (default). |

The executing service account needs `roles/dataflow.developer` (launch, get, list, update, messages) and, to launch, `iam.serviceAccounts.actAs` on the child job's worker service account plus read access to the template spec and staging buckets.

## Payload

The payload is the [`Job`](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs#Job) resource as returned by the API (`JOB_VIEW_SUMMARY` unless `view` says otherwise): `id`, `name`, `type` (`JOB_TYPE_BATCH` / `JOB_TYPE_STREAMING`), `currentState`, `currentStateTime`, `createTime`, `startTime`, `labels`, `jobMetadata.sdkVersion`, `runtimeUpdatableParams`, … — so the API reference is the dictionary for `failWhen` / `skipWhen` paths, e.g. `failWhen: payload.currentState <> 'JOB_STATE_RUNNING'`. `jobs.list` and a multi-job `jobs.wait` wrap the jobs as `jobs[]` / `count` / `firstJob`; `jobs.messages.list` returns `messages[]` (`time`, `messageText`, `messageImportance`) / `count` / `currentState`.

## Notes

- A pipeline has no try/finally: a trailing `jobs.update` (scale back down, drain) does not run when an earlier step fails.
- Launching the parent's own template + config from within it creates an endless chain of jobs; nothing detects this.
- `jobs.get` / `jobs.list` with `trigger: perElement` on a streaming input polls the API per element — mind the `jobs.get` quota.

## Examples

### Orchestrate a Dataflow batch from a Direct pipeline on Cloud Run, then merge

```yaml
actions:
  - name: backfill
    module: dataflow
    operation: flexTemplates.launch
    parameters:
      projectId: myproject
      region: asia-northeast1
      jobName: backfill-${args.run_id}
      containerSpecGcsPath: gs://mybucket/templates/dataflow.json
      config: gs://mybucket/configs/backfill.yaml
      args:
        date: ${args.date}
      environment:
        maxWorkers: 20
        serviceAccountEmail: pipeline@myproject.iam.gserviceaccount.com
      timeoutSeconds: 7200
  - name: merge
    module: bigquery
    operation: jobs.query
    waits: [backfill]
    parameters:
      query: CALL `myproject.app.merge_backfill`('${args.run_id}')
```

### Fan out one job per table, wait for all, then continue

```yaml
sources:
  - name: tables
    module: bigquery
    parameters:
      query: SELECT table_name FROM `myproject.app.INFORMATION_SCHEMA.TABLES` WHERE table_name LIKE 'events_%'
actions:
  - name: launch
    module: dataflow
    operation: flexTemplates.launch
    trigger: perElement
    inputs: [tables]
    parameters:
      region: asia-northeast1
      jobName: backfill-${table_name}-${args.run_id}   # deterministic: a retried bundle adopts the running job
      containerSpecGcsPath: gs://mybucket/templates/dataflow.json
      config: gs://mybucket/configs/backfill.yaml
      args: { table: "${table_name}" }
      wait: false                                      # launch only; one step below waits for all
  - name: wait
    module: dataflow
    operation: jobs.wait
    trigger: collect
    inputs: [launch]
    parameters:
      region: asia-northeast1
      jobIdField: jobId
      timeoutSeconds: 10800
  - name: merge
    module: bigquery
    operation: jobs.query
    waits: [wait]
    parameters:
      query: CALL `myproject.app.merge_backfill`('${args.run_id}')
```

### Guard: publish only when the streaming consumer is running

```yaml
sinks:
  - name: publish
    module: pubsub
    inputs: [events]
    waits: [consumer_alive]
    parameters: { topic: projects/myproject/topics/events }
actions:
  - name: consumer_alive
    module: dataflow
    operation: jobs.list
    failWhen: payload.`count` = 0
    parameters:
      region: asia-northeast1
      filter: ACTIVE
      jobName: events-consumer
```

### Drain the running streaming job, then launch the new version

```yaml
actions:
  - name: drain
    module: dataflow
    operation: jobs.update
    parameters:
      region: asia-northeast1
      jobName: events-consumer
      requestedState: JOB_STATE_DRAINED     # returns once the job is drained
  - name: relaunch
    module: dataflow
    operation: flexTemplates.launch
    waits: [drain]
    parameters:
      region: asia-northeast1
      jobName: events-consumer
      containerSpecGcsPath: gs://mybucket/templates/dataflow.json
      config: gs://mybucket/configs/consumer.yaml
      waitUntil: running
```

### Scale a streaming job up around a backfill

```yaml
actions:
  - name: scale_up
    module: dataflow
    operation: jobs.update
    parameters:
      region: asia-northeast1
      jobName: events-consumer
      runtimeUpdatableParams: { maxNumWorkers: 50 }
  - name: scale_down
    module: dataflow
    operation: jobs.update
    waits: [publish]                       # the backfill sink
    parameters:
      region: asia-northeast1
      jobName: events-consumer
      runtimeUpdatableParams: { maxNumWorkers: 5 }
```
