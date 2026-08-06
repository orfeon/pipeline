---
type: Sink Module
title: Action Sink Module
description: Executes an operation against an external Google Cloud service at a point in the pipeline. Supported services are BigQuery (run a query job or load job), Vertex AI Gemini (launch a batch prediction job), and Dataflow (template launch, currently a placeholder). The sink can run with no inputs, using `waits` to trigger the action after other steps complete.
tags: [sink, action, bigquery, dataflow, vertexai, gemini, job, trigger, batch]
timestamp: 2026-08-06T00:00:00Z
---

# Action Sink Module

Sink Module for executing an operation (an "action") against an external Google Cloud service from inside the pipeline. Typical use is to trigger a job — such as a BigQuery query/load job or a Vertex AI Gemini batch prediction job — after upstream steps have finished.

The action is executed **once per input element**. The module can also be used **without any `inputs`**: in that case it generates a single dummy seed element internally and applies `waits`, so the action fires exactly once after the steps listed in `waits` have completed. This makes it useful as a post-processing trigger (e.g. "after the storage sink finishes, run a BigQuery load job over the written files").

If the action throws an error, the element is routed to failure handling as a `BadRecord`, honoring the module's `failFast` setting.

## Sink module common parameters

| parameter  | optional | type                | description                                                                                  |
|------------|----------|---------------------|----------------------------------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                                            |
| module     | required | String              | Specified `action`                                                                           |
| inputs     | optional | Array<String\>      | Names of steps whose records trigger the action (one execution per record). May be omitted; then a single dummy element is generated and the action runs once. |
| waits      | optional | Array<String\>      | Names of steps to wait for before executing the action. Commonly used together with empty `inputs` to run the action after other sinks complete. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                                                  |

## Action sink module parameters

| parameter | optional | type                                    | description                                                                                                       |
|-----------|----------|-----------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| service   | required | Enum                                    | Service to execute the action against. Values: `bigquery`, `vertexai_gemini`, `dataflow`.                          |
| bigquery  | conditionally required | [BigQuery parameters](#bigquery-action-parameters) | Required when `service` is `bigquery`.                                                          |
| gemini    | conditionally required | [Gemini parameters](#vertex-ai-gemini-action-parameters) | Required when `service` is `vertexai_gemini`.                                             |
| dataflow  | conditionally required | [Dataflow parameters](#dataflow-action-parameters) | Required when `service` is `dataflow`.                                                          |
| labels    | optional | Map<String,String\>                     | Labels attached to launched jobs. Currently only referenced by the `dataflow` service.                             |

### BigQuery action parameters

Runs a BigQuery job via the BigQuery Jobs API.

| parameter         | optional | type           | description                                                                                                             |
|-------------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------|
| op                | required | Enum           | Job type. Implemented values: `query`, `load`. (`extract` and `copy` are declared but not implemented yet and fail at runtime.) |
| projectId         | optional | String         | GCP project ID to run the job in. Defaults to the pipeline's project.                                                    |
| query             | conditionally required | String | SQL to execute. Required when `op` is `query`.                                                                  |
| useLegacySql      | optional | Boolean        | Whether the query uses legacy SQL. Default: `false` (standard SQL). (`op: query` only)                                   |
| priority          | optional | Enum           | Query priority: `INTERACTIVE` or `BATCH`. Default: `INTERACTIVE`. (`op: query` only)                                     |
| sourceUris        | conditionally required | Array<String\> | GCS URIs of files to load. Required when `op` is `load`.                                                  |
| destinationTable  | conditionally required | String | Destination table (e.g. `project.dataset.table`). Required when `op` is `load`; optional for `op: query` (writes query results to the table). |
| writeDisposition  | optional | Enum           | Write disposition for the destination table: `WRITE_TRUNCATE`, `WRITE_APPEND`, `WRITE_EMPTY`.                            |
| createDisposition | optional | Enum           | Create disposition for the destination table: `CREATE_IF_NEEDED`, `CREATE_NEVER`.                                        |
| wait              | optional | Boolean        | Whether to block until the BigQuery job reaches `DONE` state (polled every 10 seconds). Default: `true`.                 |
| quotaUser         | optional | String         | Arbitrary string used as the quota user on the Jobs API request.                                                         |

### Vertex AI Gemini action parameters

Launches a Vertex AI batch prediction job (`batchPredictionJobs` REST API) for a Gemini model.

| parameter                 | optional | type                                              | description                                                              |
|---------------------------|----------|---------------------------------------------------|--------------------------------------------------------------------------|
| op                        | required | Enum                                              | Operation. Value: `batchPrediction`.                                     |
| project                   | optional | String                                            | GCP project ID. Defaults to the pipeline's default project.              |
| region                    | required | String                                            | Vertex AI region (e.g. `us-central1`).                                   |
| batchPredictionJobsRequest | required | [BatchPredictionJobsRequest](#batchpredictionjobsrequest-parameters) | The batch prediction job request body.                |
| wait                      | optional | Boolean                                           | Declared for job waiting (default `true`), but the current implementation submits the job without polling for completion. |

#### BatchPredictionJobsRequest parameters

| parameter    | optional | type                         | description                                                                                    |
|--------------|----------|------------------------------|------------------------------------------------------------------------------------------------|
| displayName  | optional | String                       | Display name of the batch prediction job.                                                      |
| model        | optional | String                       | Model resource name or Gemini model ID to run the prediction with.                             |
| inputConfig  | required | [InputConfig](#inputconfig)  | Where the prediction instances are read from.                                                  |
| outputConfig | required | [OutputConfig](#outputconfig) | Where the prediction results are written to.                                                  |

##### InputConfig

Specify one of `gcsSource` or `bigquerySource`.

| parameter       | optional | type   | description                                                                                            |
|-----------------|----------|--------|--------------------------------------------------------------------------------------------------------|
| instancesFormat | optional | String | Format of input instances. Default: `jsonl` when `gcsSource` is set, `bigquery` when `bigquerySource` is set. |
| gcsSource       | optional | Object | GCS input. Field: `inputUris` (String) — GCS URI(s) of the input file(s).                              |
| bigquerySource  | optional | Object | BigQuery input. Field: `inputUri` (String) — BigQuery table URI (e.g. `bq://project.dataset.table`).   |

##### OutputConfig

Specify one of `gcsDestination` or `bigqueryDestination`.

| parameter           | optional | type   | description                                                                                             |
|---------------------|----------|--------|---------------------------------------------------------------------------------------------------------|
| predictionsFormat   | optional | String | Format of prediction output. Default: `jsonl` when `gcsDestination` is set, `bigquery` when `bigqueryDestination` is set. |
| gcsDestination      | optional | Object | GCS output. Field: `outputUriPrefix` (String) — GCS URI prefix for output files.                        |
| bigqueryDestination | optional | Object | BigQuery output. Field: `outputUri` (String) — BigQuery output table URI.                               |

### Dataflow action parameters

> **Note**: the Dataflow service is currently a placeholder. `launchTemplate` throws `NotImplementedException` and `launchFlexTemplate` performs no operation. Do not rely on this service yet.

| parameter | optional | type   | description                                                        |
|-----------|----------|--------|--------------------------------------------------------------------|
| op        | required | Enum   | Operation: `launchTemplate` or `launchFlexTemplate`.               |
| options   | optional | Object | Dataflow launch options (defaults are copied from the running pipeline's options). |

## Output

The action sink returns an output collection mainly so other steps can `waits` on it. Only some actions produce meaningful output records:

- `dataflow` / `launchFlexTemplate`: schema `{ jobId: STRING (nullable) }`
- `vertexai_gemini`: schema `{ body: STRING }`
- `bigquery`: no output schema; use the step only as a `waits` target.

## Examples

### Example 1: Run a BigQuery load job after files are written

The action sink has no `inputs`; it waits for the `storage` sink to finish, then triggers a single BigQuery load job over the written Avro files.

```yaml
sources:
  - name: input
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.mytable`"

sinks:
  - name: store
    module: storage
    inputs:
      - input
    parameters:
      output: gs://my-bucket/export/data
      format: avro
  - name: load_to_bq
    module: action
    waits:
      - store
    parameters:
      service: bigquery
      bigquery:
        op: load
        projectId: myproject
        sourceUris:
          - gs://my-bucket/export/data*.avro
        destinationTable: myproject.mydataset.loaded_table
        writeDisposition: WRITE_TRUNCATE
        createDisposition: CREATE_IF_NEEDED
```

### Example 2: Run a BigQuery query after another step

Execute a summary query and write its result to a destination table, waiting for job completion.

```yaml
sinks:
  - name: summarize
    module: action
    waits:
      - some_previous_sink
    parameters:
      service: bigquery
      bigquery:
        op: query
        query: "SELECT category, COUNT(*) AS cnt FROM `myproject.mydataset.events` GROUP BY category"
        destinationTable: myproject.mydataset.event_summary
        writeDisposition: WRITE_TRUNCATE
        wait: true
```

### Example 3: Launch a Vertex AI Gemini batch prediction job

Submit a batch prediction job reading JSONL instances from GCS and writing predictions back to GCS.

```yaml
sinks:
  - name: gemini_batch
    module: action
    waits:
      - write_instances
    parameters:
      service: vertexai_gemini
      gemini:
        op: batchPrediction
        region: us-central1
        batchPredictionJobsRequest:
          displayName: my-batch-prediction
          model: publishers/google/models/gemini-2.0-flash-001
          inputConfig:
            gcsSource:
              inputUris: gs://my-bucket/instances/input.jsonl
          outputConfig:
            gcsDestination:
              outputUriPrefix: gs://my-bucket/predictions/
```
