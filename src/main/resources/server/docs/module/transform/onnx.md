---
type: Transform Module
title: Onnx Transform Module
description: Runs inference on input records with an ONNX model using ONNX Runtime. Loads the model file from Google Cloud Storage or a local path, maps record fields to model inputs and model outputs to record fields, and appends the inference results to each record. Supports optional filter conditions, select-based pre/post-processing, micro-batching via bufferSize, and chained multi-step mappings. Works in both batch and streaming pipelines.
tags: [transform, onnx, ml, inference, machinelearning, batch, streaming]
timestamp: 2026-10-30T00:00:00Z
---

# Onnx Transform Module

Transform Module that performs inference on input records with a specified [ONNX](https://onnx.ai/) model using [ONNX Runtime](https://onnxruntime.ai/). The inference results are merged into each input record, so downstream modules see the original fields plus the model output fields.

Supports:

- **Model loading from GCS or local path** - A single `.onnx` file (`gs://bucket/model.onnx`), or a directory (`gs://bucket/models/xxx/` or a local directory) whose files are downloaded with their layout kept, so that external weight files (`*.onnx_data`) are found next to the model; `model.file` picks the model file inside it. The loaded session is shared by all DoFn instances (and all onnx modules using the same model and session options) in the same worker process (JVM), so one model copy serves multiple threads.
- **Field mapping** - `mappings` connects record field names to ONNX model input names, and ONNX model output names to output field names. Multiple mapping steps run sequentially against the same model, and a later step can consume the outputs of an earlier one.
- **Pre/post-processing** - Optional [Select](../common/select.md) function lists applied before (`preprocesses`) and after (`postprocesses`) inference.
- **Filtering** - Optional [Filter](../common/filter.md) condition; records that do not match are dropped before inference.
- **Micro-batching** - `bufferSize` buffers records and runs inference on the batch in one session call. dim0 of every model output is the batch: each record receives its own row of the outputs.
- **Failure handling** - Errors are routed as failures according to the module common `failFast` / `outputFailure` / `failureSinks` settings. A failing pre/post-processing fails its record only; a failing inference fails every record of that batch.

The same processing works in batch and streaming. Large ONNX files require large worker memory at startup; multi-core workers with large memory are recommended because the model is shared in memory across threads on a worker.

## Transform module common parameters

| parameter  | optional | type                              | description                                                    |
|------------|----------|-----------------------------------|----------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.              |
| module     | required | String                            | Specified `onnx`                                               |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be used as input.            |
| waits      | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.  |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                    |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                    |

When multiple `inputs` are specified they are unioned into a single collection before inference (their schemas are merged).

## Onnx transform module parameters

| parameter     | optional | type                                     | description                                                                                                                                                                          |
|---------------|----------|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| model         | required | [Model](#model-parameters)               | Settings for the ONNX model used for inference.                                                                                                                                      |
| mappings      | selective required | Array<[Mapping](#mapping-parameters)\>   | Mapping steps between record fields and the model's input/output names. Steps run in order; each step's outputs are merged into the record before the next step runs.                |
| mapping       | selective required | [Mapping](#mapping-parameters)   | A single mapping step: the same as `mappings` with one element. Specify either `mapping` or `mappings`.                |
| filter        | optional | [Filter](../common/filter.md)            | Filter condition applied before inference. Records that do not match are discarded (they do not appear in the output).                                                               |
| preprocesses  | optional | Array<[Select](../common/select.md)\>    | Select functions applied to each record before inference. The computed fields are added to the record and can be referenced by `mappings[].inputs`.                                  |
| postprocesses | optional | Array<[Select](../common/select.md)\>    | Select functions applied to each record after inference. When specified, the output schema becomes the schema produced by these select functions (only the selected fields remain).  |
| bufferSize    | optional | Integer                                  | Number of records to buffer before running inference as one batch. Must be 1 or more. Default: `1`. Records left in a partially filled buffer are inferred when the bundle finishes; each record keeps its own timestamp and window. With `bufferSize` greater than 1, array inputs of a dynamic length (token ids, ...) must have the same length in every record of a batch (padding is not supported yet). |

## Model parameters

| parameter     | optional | type   | description                                                                                                                                                                                                                                     |
|---------------|----------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path          | required | String | Path of the ONNX model. A GCS object path (`gs://bucket/model.onnx`), a GCS directory path ending with `/` (all files under the directory are downloaded to the worker, keeping the sub directories), a local file path, or a local directory path.                                                      |
| file          | optional | String | For a directory `path`: the model file relative to the directory (e.g. `onnx/model_q4.onnx`). If omitted, the directory must contain exactly one `.onnx` file.                                                      |
| intraOpThreads | optional | Integer | Number of threads one inference uses. Default: `1`: the worker threads share the session and run inferences in parallel, which scales better than giving every inference all the cores. Tune parallelism with the worker thread count (e.g. Dataflow `numberOfWorkerHarnessThreads`). |
| interOpThreads | optional | Integer | Number of threads that run independent graph operators in parallel (`executionMode: PARALLEL` only). Default: the ONNX Runtime default. |
| optLevel      | optional | Enum   | ONNX Runtime graph [optimization](https://onnxruntime.ai/docs/performance/model-optimizations/graph-optimizations.html) level. One of `NO_OPT`, `BASIC_OPT`, `EXTENDED_OPT`, `ALL_OPT`. Default: `BASIC_OPT`.                                   |
| executionMode | optional | Enum   | ONNX Runtime execution mode for running graph operators. One of `SEQUENTIAL`, `PARALLEL`. Default: `SEQUENTIAL`.                                                                                                                                |

## Mapping parameters

Each element of `mappings` defines one inference step against the model.

| parameter | optional | type                | description                                                                                                                                                                     |
|-----------|----------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| inputs    | required | Map<String,String\> | Maps record fields to model inputs. The map **key is the record field name** (input fields or fields computed by `preprocesses`), and the **value is the ONNX model input name**. |
| outputs   | optional | Map<String,String\> | Maps model outputs to record fields. The map **key is the ONNX model output name**, and the **value is the output field name** added to the record. Only the listed model outputs are requested from the session. If omitted, every model output is added with its own name. |

Every model input must be given a record field by `inputs`.

## Output schema

The output schema is: input schema fields + fields produced by `preprocesses` + the mapped model output fields (named by `mappings[].outputs`). If `postprocesses` is specified, the output schema is instead the schema produced by the postprocess select functions.

dim0 of a model output is the batch, and the type of an output field is decided by the rest of the shape:

| model output shape | record field type | example |
|---|---|---|
| `[B]` | scalar | a score per record |
| `[B, n]` | array of n values | an embedding `[B, 768]`; `[B, 1]` is an array of one value |
| `[B, d1, ..., dk]` | matrix of shape `[d1, ..., dk]` (row-major) | `[B, 2, 3]` |
| `[]` (scalar) | scalar, the same value for every record | |

Element types: float / float16 / bfloat16 -> `float32`, double -> `float64`, int64 -> `int64`, int8 / uint8 / int16 / int32 -> `int32`, bool -> `boolean`, string -> `string`. An output whose dim0 is not the batch size fails the batch.

## Example: text embedding with pre/post-processing

Reads records from BigQuery, computes an embedding for a text field with an ONNX model, keeps only the id and a renamed embedding field, and writes the results to storage.

```yaml
sources:
  - name: items
    module: bigquery
    parameters:
      query: "SELECT id, title, description FROM `myproject.mydataset.items`"

transforms:
  - name: embedding
    module: onnx
    inputs:
      - items
    parameters:
      model:
        path: gs://my-bucket/models/text-embedding.onnx
        optLevel: BASIC_OPT
      preprocesses:
        - name: text
          func: concat
          fields: [title, description]
          delimiter: " "
      mappings:
        - inputs:
            text: input_text
          outputs:
            embedding_output: embedding
      postprocesses:
        - name: id
        - name: embedding

sinks:
  - name: output
    module: storage
    inputs:
      - embedding
    parameters:
      output: "gs://my-bucket/embeddings/"
      format: avro
```

## Example: filtered inference with chained mappings

Runs two inference steps against the same model: the second mapping consumes the output field of the first. Only records matching the filter are processed.

```yaml
transforms:
  - name: predict
    module: onnx
    inputs:
      - events
    parameters:
      model:
        path: gs://my-bucket/models/model_dir/
        executionMode: PARALLEL
      filter:
        key: status
        op: "="
        value: "active"
      mappings:
        - inputs:
            feature_a: input_a
            feature_b: input_b
          outputs:
            score: raw_score
        - inputs:
            raw_score: input_score
          outputs:
            label: predicted_label
```
