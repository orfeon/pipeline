---
type: Action Module
title: Vertex AI Action Module
description: Vertex AI generative AI (Gemini) operations as workflow steps (Vertex AI REST API v1). Submits a Gemini batch prediction job (batchPredictionJobs.create — JSONL on GCS or a BigQuery table with a request column in, predictions out) and waits for it, idempotent via a deterministic displayName that adopts a queued / running / succeeded job; reads or lists jobs as a guard (failWhen / skipWhen), waits for jobs submitted elsewhere (jobId, filter, or jobIdField with collect), cancels a job; and runs a single generateContent call from the control plane (models.generateContent with prompt / system / responseSchema shorthands — summarize or triage the triggering control records into structured JSON for a notification or a gate). Per-record inference over data records belongs in the select transform, not here.
tags: [action, vertexai, gemini, llm, batch, prediction, generate, generateContent, job, wait, cancel, guard, trigger, workflow, gcp]
timestamp: 2026-08-28T00:00:00Z
---

# Vertex AI Action Module

Action module (`actions` section, `module: vertexai`) for [Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest) generative AI operations: Gemini **batch prediction jobs** and a single **generateContent** call. See [action modules](README.md) for the `actions` section, trigger semantics and the output envelope.

Typical uses:

- **Batch inference pipeline**: a pipeline builds one `request` (a `GenerateContentRequest` JSON) per record with the [select](../transform/select.md) transform, writes them to BigQuery or as JSONL to GCS, then `batchPredictionJobs.create` runs the whole set through Gemini at batch pricing. A later pipeline (or the same one, after `wait`) reads the output table / files: `response` holds the `GenerateContentResponse`, the other input columns are passed through, so no join is needed. Classification, extraction, summarization, translation, PII detection at scale.
- **Triage → notify**: `collect` the failure records or action envelopes of a run, ask Gemini for a structured verdict (`models.generateContent` with `responseSchema`), then `failWhen` on it or forward the envelope (`${payload}` is the response JSON text) to an [http](http.md) notification.
- **Guard / wait**: `batchPredictionJobs.list` + `failWhen` (do not submit while too many jobs run), `batchPredictionJobs.wait` by `filter` for a job submitted by another system.
- **Fan-out**: one job per table / date (`trigger: perElement`, `wait: false`, deterministic `displayName`), then a single `batchPredictionJobs.wait` step (`trigger: collect`, `jobIdField`).

Per-record inference over **data** records (label every row synchronously) is not an action's job: it is the data plane — use the `select` transform's HTTP-based functions or the batch job above. `models.generateContent` with `trigger: perElement` is allowed for control records (one summary per written file, …) and logs a warning at assembly.

## Operations

`operation` is a module-level field (next to `name` / `module`), named after the REST API `resource.method`.

| operation | effect | state in envelope | idempotent on bundle retry |
|---|---|---|---|
| `batchPredictionJobs.create` | Submit a [`BatchPredictionJob`](https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.batchPredictionJobs) for a Gemini model and, by default, wait for it. | The job's `state` after the wait (`JOB_STATE_SUCCEEDED`, `JOB_STATE_PARTIALLY_SUCCEEDED`, …); with `wait: false` the state right after submission, or `EXISTS` when a job with the same `displayName` was adopted (`payload.adopted = true` in both cases). | yes with a deterministic `displayName` (see below) |
| `batchPredictionJobs.get` | Read one job (`jobId`). | `state` | yes |
| `batchPredictionJobs.list` | List jobs (`filter`). Payload: `jobs[]`, `count`, `firstJob` (newest first). | `DONE` | yes |
| `batchPredictionJobs.wait` | Wait for jobs submitted elsewhere: `jobId` / `filter` (payload: the job, state = its `state`), or with `trigger: collect` every id in `jobIdField` in one poll loop (payload: `jobs[]` / `count` / `firstJob`, state `DONE`). | see left | yes |
| `batchPredictionJobs.cancel` | Cancel a job and (by default) wait until `JOB_STATE_CANCELLED`. | `state` | yes |
| `models.generateContent` | One [`generateContent`](https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/inference) call: the request fields (`contents`, `systemInstruction`, `generationConfig`, `tools`, `toolConfig`, `safetySettings`, `cachedContent`, `labels`) or the `prompt` / `system` / `responseSchema` shorthands, templated with the triggering elements. | `candidates[0].finishReason` (`STOP`, `MAX_TOKENS`, `SAFETY`, …), `BLOCKED` when the prompt was blocked (`promptFeedback.blockReason`), `EMPTY` when no candidate came back. | no (a retry re-generates; billed twice) |

`jobId` in the envelope is the numeric batch prediction job id (comma-joined for a multi-job `wait`), or the `responseId` of a generateContent call.

### Idempotency and failure handling

A batch prediction job has no client-supplied id, so a retried bundle would submit a second job. With `reuseExisting` (default) and a deterministic `displayName` (e.g. `classify-${args.run_id}`), `batchPredictionJobs.create` first lists jobs with that display name and adopts the newest when it is queued / pending / running / succeeded (`payload.adopted = true`); a newest job that failed, expired or was cancelled means the work has to run again. The check is best-effort (a race between list and create can still submit twice). Without `displayName` the step submits as `<step name>-<yyyyMMddHHmmss>` and logs a WARN.

| situation | behaviour |
|---|---|
| Job ends `JOB_STATE_FAILED` / `JOB_STATE_EXPIRED` | Non-retryable failure; `error.message` and `completionStats` are attached. |
| Job ends `JOB_STATE_PARTIALLY_SUCCEEDED` | Normal completion by default (some rows failed — `payload.completionStats.failedCount`, and the `status` column of the output rows); `failOnPartial: true` makes it a failure. |
| Job is `JOB_STATE_CANCELLED` by someone else while waited for | Non-retryable failure. |
| `timeoutSeconds` exceeded | Non-retryable failure; the job is cancelled first when `cancelOnTimeout` is true (default for jobs this step submitted). |
| Rejected request (INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, FAILED_PRECONDITION, …) | Non-retryable — re-execution cannot fix it. |
| Transient API errors (429 RESOURCE_EXHAUSTED, 5xx, I/O) on submit / generateContent | Retryable — use the module-level `retry`. During a wait they are retried inside the poll loop. |
| generateContent returns `SAFETY` / `RECITATION` / `BLOCKED` | Not an error: the envelope carries the state; gate with `failWhen: state = 'BLOCKED'` if needed. |

Waiting happens inside the action's DoFn (as in the [bigquery](bigquery.md) / [dataflow](dataflow.md) actions), and a batch prediction job can take hours (target turnaround is 24h). Prefer splitting **submit** (`wait: false`, in a Dataflow pipeline) from **wait + post-processing** (a lightweight Direct pipeline on Cloud Run, `batchPredictionJobs.wait` with `filter: display_name="…"`), or run the submitting pipeline itself on a lightweight runner and hand heavy post-processing to the [dataflow](dataflow.md) action.

## Parameters

### Common

| parameter | optional | type | description |
|---|---|---|---|
| projectId | optional | String | Project of the job / model. Default: the pipeline's project. Template allowed. |
| location | conditionally required | String | Vertex AI location. For the `batchPredictionJobs.*` operations a **regional** location is required (`us-central1`, `asia-northeast1`, …; batch jobs and a BigQuery input table must be in the same region). For `models.generateContent` default `global` (the [global endpoint](https://cloud.google.com/vertex-ai/generative-ai/docs/learn/locations)); set a region for data residency or Provisioned Throughput. Template allowed. |
| model | conditionally required | String | `batchPredictionJobs.create` / `models.generateContent`: a Gemini model id (`gemini-2.5-flash` → `publishers/google/models/gemini-2.5-flash`), a `publishers/…` resource, or a tuned model endpoint `projects/…/locations/…/endpoints/…`. Template allowed. |
| jobId | conditionally required | String | Target job for `get` / `wait` / `cancel` (the numeric id or the full resource name). Template allowed, e.g. `${jobId}` from a create envelope. |
| jobIdField | optional | String | `wait` with `trigger: collect`: field of the collected elements holding job ids (`jobId` of create envelopes). |
| filter | optional | String | `list`: the [list filter](https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.batchPredictionJobs/list) (`display_name="…"`, `state="JOB_STATE_RUNNING"`, `create_time>"2026-08-28T00:00:00Z"`, `labels.run="r1"`, combined with `AND`). `wait`: polls the list until a job matches, then waits for the newest one. Template allowed. |
| pageSize | optional | Integer | `list`: max jobs to return. Default `100`. |
| wait | optional | Boolean | `create` / `cancel`: wait for the job. Default `true`. |
| waitUntil | optional | Enum | `terminal` (default) / `running` (return as soon as the job leaves the queue) / `none` (`wait`: report the current state without polling). |
| timeoutSeconds | optional | Long | Max seconds to wait. Default `86400`. |
| cancelOnTimeout | optional | Boolean | Cancel the pending job(s) on timeout. Default `true` for `create`, `false` otherwise (a job submitted elsewhere is not ours to cancel). |
| endpoint | optional | String | API endpoint override (tests / private endpoints). Default: derived from `location`. |

### batchPredictionJobs.create

The [`BatchPredictionJob`](https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.batchPredictionJobs#BatchPredictionJob) fields are written at the top level of `parameters`; every string value is a template.

| parameter | optional | type | description |
|---|---|---|---|
| displayName | optional (recommended) | String | Display name of the job. Make it deterministic per unit of work (`classify-${args.run_id}`) — it is the idempotency key (see above). |
| inputConfig | required | Object | `gcsSource: { uris: [gs://…/*.jsonl] }` (a single string is accepted) or `bigquerySource: { inputUri: "bq://project.dataset.table" }`. `instancesFormat` defaults to `jsonl` / `bigquery` accordingly. |
| outputConfig | required | Object | `gcsDestination: { outputUriPrefix: "gs://…/" }` or `bigqueryDestination: { outputUri: "bq://project.dataset.table" }` (a dataset-only `bq://project.dataset` lets the API create a `predictions_<timestamp>` table — prefer naming the table so the next step knows where to read). `predictionsFormat` defaults accordingly. |
| reuseExisting | optional | Boolean | Adopt an existing job with the same `displayName` instead of submitting again. Default `true`. |
| failOnPartial | optional | Boolean | Treat `JOB_STATE_PARTIALLY_SUCCEEDED` as a failure. Default `false`. |
| labels, serviceAccount, encryptionSpec, instanceConfig, modelParameters, dedicatedResources | optional | — | Passed through to the job resource. |

**Input format.** Each JSONL line / BigQuery row is `{"request": <GenerateContentRequest>, ...}`: the `request` field (a JSON / STRING column in BigQuery) holds `contents`, and optionally `systemInstruction`, `generationConfig` (with `responseSchema` for structured output), `tools`, `safetySettings`. Other fields / columns (keys, metadata) are passed through to the output; BigQuery columns of type ARRAY / STRUCT / RANGE / DATETIME / GEOGRAPHY are not allowed, and `response` / `status` are reserved names.

**Output format.** One line / row per input with `request`, `response` (the `GenerateContentResponse` JSON — the answer is `candidates[0].content.parts[0].text`), `status` (empty on success, the error otherwise) and `processed_time`, plus the passed-through fields. The payload's `outputInfo.gcsOutputDirectory` / `outputInfo.bigqueryOutputTable` tells the next step where the results went.

### models.generateContent

| parameter | optional | type | description |
|---|---|---|---|
| prompt | conditionally required | String | Shorthand for `contents: [{ role: user, parts: [{ text: prompt }] }]`. Template: `perElement` sees the element's fields, `once` / `collect` see `elements` (list of maps) and `size`, e.g. `<#list elements as e>${e.jobId}: ${e.state}\n</#list>`. Exclusive with `contents`. |
| system | optional | String | Shorthand for `systemInstruction: { parts: [{ text: system }] }`. Exclusive with `systemInstruction`. |
| responseSchema | optional | Object | Shorthand for `generationConfig.responseMimeType: application/json` + `generationConfig.responseSchema` (an OpenAPI-style [Schema](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/control-generated-output)). The parsed answer lands in `payload.json`. Exclusive with `generationConfig.responseSchema` / `responseJsonSchema`. |
| contents, systemInstruction, generationConfig, tools, toolConfig, safetySettings, cachedContent, labels | optional | — | The [`GenerateContentRequest`](https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/inference#request) fields, passed through (string leaves templated). `tools` are passed as-is — the action runs no function-calling loop. |

## Payload

- `batchPredictionJobs.*`: the `BatchPredictionJob` resource as returned by the API (`name`, `displayName`, `model`, `state`, `createTime`, `startTime`, `endTime`, `error`, `completionStats` (`successfulCount` / `failedCount` / `incompleteCount`), `outputInfo`, `labels`, …). int64 fields are strings, as in the REST JSON. `create` adds `adopted: true` for an adopted job; `list` / collected `wait` wrap the jobs as `jobs[]`, `count`, `firstJob`.
- `models.generateContent`: the `GenerateContentResponse` (`candidates[]`, `promptFeedback`, `usageMetadata`, `modelVersion`, `responseId`) plus `text` (the first candidate's text parts, thoughts excluded) and, when `text` is a JSON object / array, `json` (parsed) — ``failWhen: payload.`json`.`severity` = 'critical'``. Downstream templates see the envelope's `payload` as JSON text (`${payload}`), conditions see it as a map.

## Examples

### Batch classification: build requests, submit, wait, read back

```yaml
sources:
  - name: reviews
    module: bigquery
    parameters:
      query: SELECT review_id, body FROM `myproject.app.reviews` WHERE _PARTITIONDATE = '${args.date}'
transforms:
  - name: requests
    module: select
    inputs: [reviews]
    parameters:
      fields:
        - name: review_id
        - name: request                       # one GenerateContentRequest per row
          func: json
          fields:
            - name: contents
              func: json
              mode: repeated
              fields:
                - { name: role, func: constant, value: user }
                - name: parts
                  func: json
                  mode: repeated
                  fields:
                    - { name: text, func: expression, expression: "'Classify this review: ' || body" }
            - name: generationConfig
              func: json
              fields:
                - { name: responseMimeType, func: constant, value: application/json }
                - name: responseSchema
                  func: constant
                  value: { type: OBJECT, properties: { sentiment: { type: STRING, enum: [positive, neutral, negative] } } }
sinks:
  - name: stage
    module: bigquery
    inputs: [requests]
    parameters:
      table: myproject.llm.review_requests_${args.date}
      createDisposition: CREATE_IF_NEEDED
      writeDisposition: WRITE_TRUNCATE
actions:
  - name: classify
    module: vertexai
    operation: batchPredictionJobs.create
    waits: [stage]
    failWhen: payload.`completionStats`.`failedCount` != '0'
    parameters:
      location: us-central1
      displayName: classify-reviews-${args.date}          # deterministic → safe on retry
      model: gemini-2.5-flash
      inputConfig:
        bigquerySource: { inputUri: "bq://myproject.llm.review_requests_${args.date}" }
      outputConfig:
        bigqueryDestination: { outputUri: "bq://myproject.llm.review_responses_${args.date}" }
      timeoutSeconds: 43200
  - name: merge                              # read the responses back once the job is done
    module: bigquery
    operation: jobs.query
    waits: [classify]
    parameters:
      query: |
        MERGE `myproject.app.review_labels` t
        USING (
          SELECT review_id,
                 JSON_VALUE(response, '$.candidates[0].content.parts[0].text') AS label_json
          FROM `myproject.llm.review_responses_${args.date}` WHERE status = ''
        ) s ON t.review_id = s.review_id
        WHEN MATCHED THEN UPDATE SET label_json = s.label_json
        WHEN NOT MATCHED THEN INSERT (review_id, label_json) VALUES (s.review_id, s.label_json)
```

### Fan out one job per date, wait for all

```yaml
actions:
  - name: submit
    module: vertexai
    operation: batchPredictionJobs.create
    trigger: perElement
    inputs: [dates]
    parameters:
      location: us-central1
      displayName: "classify-${date}"
      model: gemini-2.5-flash
      inputConfig:  { gcsSource: { uris: "gs://my-bucket/requests/${date}/*.jsonl" } }
      outputConfig: { gcsDestination: { outputUriPrefix: "gs://my-bucket/responses/${date}/" } }
      wait: false
  - name: submitted
    module: vertexai
    operation: batchPredictionJobs.wait
    trigger: collect
    inputs: [submit]
    parameters:
      location: us-central1
      jobIdField: jobId
```

### Triage failures with a structured verdict, then notify

```yaml
actions:
  - name: triage
    module: vertexai
    operation: models.generateContent
    trigger: collect
    inputs: [load_failures]                   # a failure sink's records, or action envelopes
    failWhen: payload.`json`.`severity` = 'critical'
    parameters:
      model: gemini-2.5-flash                 # location defaults to global for models.*
      system: "You are the on-call data engineer. Answer in one short paragraph."
      prompt: |
        ${size} records failed to load. Summarize the likely cause and the action to take:
        <#list elements as e>- ${e.payload}
        </#list>
      responseSchema:
        type: OBJECT
        properties:
          severity: { type: STRING, enum: [low, high, critical] }
          summary:  { type: STRING }
      generationConfig: { temperature: 0 }
  - name: notify
    module: http
    inputs: [triage]
    parameters:
      target: { url: "${args.slack_webhook}", method: POST }
      body: { text: "Load failures triage: ${payload}" }   # payload = the response JSON text (text / json / usageMetadata)
```

### Wait for a job submitted by another system

```yaml
actions:
  - name: wait_nightly
    module: vertexai
    operation: batchPredictionJobs.wait
    parameters:
      location: us-central1
      filter: display_name="nightly-embeddings" AND create_time>"${args.date}T00:00:00Z"
      timeoutSeconds: 21600
```
