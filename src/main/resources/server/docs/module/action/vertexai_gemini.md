---
type: Action Module
title: Vertex AI Gemini Action Module
description: Launches a Vertex AI batch prediction job (batchPredictionJobs REST API) for a Gemini model from inside the pipeline and waits for its completion by polling the job state. Submission is not idempotent (the API has no client-supplied job id), so prefer trigger once.
tags: [action, vertexai, gemini, batch, prediction, job, trigger, workflow]
timestamp: 2026-08-19T00:00:00Z
---

# Vertex AI Gemini Action Module

Action module (`action.vertexai_gemini`) that launches a Vertex AI batch prediction job (the `batchPredictionJobs` REST API) for a Gemini model and, by default, waits for its completion. Placeable in sources/transforms/sinks; see [action modules](README.md) for placement, trigger semantics and the output envelope.

Submission is **not idempotent** — the API has no client-supplied job id, so a retried Beam bundle may submit a duplicate job. Prefer `trigger: once` unless duplicates are acceptable. `${field}` parameter templating is not supported by this service (elements only control firing).

## Parameters

| parameter                 | optional | type                                              | description                                                              |
|---------------------------|----------|---------------------------------------------------|--------------------------------------------------------------------------|
| trigger                   | optional | Enum                                              | `once` (default), `perElement`, `collect`. See [action modules](README.md#trigger). |
| op                        | required | Enum                                              | Operation. Value: `batchPrediction`.                                     |
| project                   | optional | String                                            | GCP project ID. Defaults to the pipeline's project.                      |
| region                    | required | String                                            | Vertex AI region (e.g. `us-central1`).                                   |
| batchPredictionJobsRequest | required | [BatchPredictionJobsRequest](#batchpredictionjobsrequest-parameters) | The batch prediction job request body.                |
| wait                      | optional | Boolean                                           | Whether to poll the job until a terminal state. `JOB_STATE_FAILED` / `CANCELLED` / `EXPIRED` raise an error. Default: `true`. |
| timeoutSeconds            | optional | Integer                                           | Max seconds to wait for job completion. Default: `86400` (24h).          |

### BatchPredictionJobsRequest parameters

| parameter    | optional | type                         | description                                                                                    |
|--------------|----------|------------------------------|------------------------------------------------------------------------------------------------|
| displayName  | optional | String                       | Display name of the batch prediction job.                                                      |
| model        | optional | String                       | Model resource name or Gemini model ID to run the prediction with.                             |
| inputConfig  | required | [InputConfig](#inputconfig)  | Where the prediction instances are read from.                                                  |
| outputConfig | required | [OutputConfig](#outputconfig) | Where the prediction results are written to.                                                  |

#### InputConfig

Specify one of `gcsSource` or `bigquerySource`.

| parameter       | optional | type   | description                                                                                            |
|-----------------|----------|--------|--------------------------------------------------------------------------------------------------------|
| instancesFormat | optional | String | Format of input instances. Default: `jsonl` when `gcsSource` is set, `bigquery` when `bigquerySource` is set. |
| gcsSource       | optional | Object | GCS input. Field: `inputUris` (String) — GCS URI(s) of the input file(s).                              |
| bigquerySource  | optional | Object | BigQuery input. Field: `inputUri` (String) — BigQuery table URI (e.g. `bq://project.dataset.table`).   |

#### OutputConfig

Specify one of `gcsDestination` or `bigqueryDestination`.

| parameter           | optional | type   | description                                                                                             |
|---------------------|----------|--------|---------------------------------------------------------------------------------------------------------|
| predictionsFormat   | optional | String | Format of prediction output. Default: `jsonl` when `gcsDestination` is set, `bigquery` when `bigqueryDestination` is set. |
| gcsDestination      | optional | Object | GCS output. Field: `outputUriPrefix` (String) — GCS URI prefix for output files.                        |
| bigqueryDestination | optional | Object | BigQuery output. Field: `outputUri` (String) — BigQuery output table URI.                               |

## Output

One envelope record per execution (see [action modules](README.md#output-envelope)); `jobId` is the batch prediction job resource name and `payload` the job resource JSON.

## Example

Submit a batch prediction job reading JSONL instances from GCS after they are written, waiting for job completion.

```yaml
sinks:
  - name: gemini_batch
    module: action.vertexai_gemini
    waits:
      - write_instances
    parameters:
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
