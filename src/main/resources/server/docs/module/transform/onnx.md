---
type: Transform Module
title: Onnx Transform Module
description: Runs inference on input records with an ONNX model using ONNX Runtime. Loads the model from Google Cloud Storage or a local path, builds the model inputs and the output fields with select lists (padding, constant inputs, name variables for many inputs), and adds the results to each record. Supports optional filter conditions, micro-batching via bufferSize and per-record retry of a failing batch. Works in both batch and streaming pipelines.
tags: [transform, onnx, ml, inference, machinelearning, batch, streaming]
timestamp: 2026-10-30T00:00:00Z
---

# Onnx Transform Module

Transform Module that performs inference on input records with a specified [ONNX](https://onnx.ai/) model using [ONNX Runtime](https://onnxruntime.ai/). One module runs one model, and one input record gives one output record: the input fields plus the fields of `outputs`. What is specific to a model is written in the select lists `inputs` and `outputs`, not in the module.

Supports:

- **Model loading from GCS or local path** - A single `.onnx` file (`gs://bucket/model.onnx`), or a directory (`gs://bucket/models/xxx/` or a local directory) whose files are downloaded with their layout kept, so that external weight files (`*.onnx_data`) are found next to the model; `model.file` picks the model file inside it. The loaded session is shared by all DoFn instances (and all onnx modules using the same model and session options) in the same worker process (JVM), so one model copy serves multiple threads. Pipeline assembly only reads the graph of the `.onnx` file: no session is created and no weights are read, unless the graph leaves out the type or the shape of an input or an output, which only a session can infer.
- **Select lists** - [`inputs`](#inputs) builds the model inputs with select functions, and [`outputs`](#outputs) builds the output fields from the model outputs.
- **Padding and constant inputs** - `pad` pads the records of a batch to a common length; `empty` and `fill` give inputs that do not come from the record; `%d` / `%s` in a name match many model inputs.
- **Filtering** - Optional [Filter](../common/filter.md) condition; records that do not match are dropped before inference.
- **Micro-batching** - `bufferSize` buffers records and runs inference on the batch in one session call.
- **Failure handling** - Errors are routed as failures according to the module common `failFast` / `outputFailure` / `failureSinks` settings. A failing select function fails its record only. A failing batch is retried record by record, so that only the records that fail by themselves are failures.

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

| parameter  | optional | type                                  | description |
|------------|----------|---------------------------------------|-------------|
| model      | required | [Model](#model-parameters)            | The ONNX model to run. |
| inputs     | required | Array<[InputField](#inputs)\>         | Select list that builds the model inputs. |
| outputs    | required | Array<[Select](../common/select.md)\> | Select list that builds the output fields. |
| filter     | optional | [Filter](../common/filter.md)         | Filter condition applied before inference. Records that do not match are discarded (they do not appear in the output). |
| bufferSize | optional | Integer                               | Number of records to run inference on as one batch. Default: `1`. Records left in a partially filled buffer are inferred when the bundle finishes; each record keeps its own timestamp and window. With overlapping (sliding) windows inference runs once per window. |

## Model parameters

| parameter     | optional | type   | description |
|---------------|----------|--------|-------------|
| path          | required | String | Path of the ONNX model. A GCS object path (`gs://bucket/model.onnx`), a GCS directory path ending with `/` (all files under the directory are downloaded to the worker, keeping the sub directories), a local file path, or a local directory path. |
| file          | optional | String | For a directory `path`: the model file relative to the directory (e.g. `onnx/model_q4.onnx`). If omitted, the directory must contain exactly one `.onnx` file. |
| intraOpThreads | optional | Integer | Number of threads one inference uses. Default: `1`: the worker threads share the session and run inferences in parallel, which scales better than giving every inference all the cores. Tune parallelism with the worker thread count (e.g. Dataflow `numberOfWorkerHarnessThreads`). |
| interOpThreads | optional | Integer | Number of threads that run independent graph operators in parallel (`executionMode: PARALLEL` only). Default: the ONNX Runtime default. |
| optLevel      | optional | Enum   | ONNX Runtime graph optimization level. One of `NO_OPT`, `BASIC_OPT`, `EXTENDED_OPT`, `ALL_OPT`. Default: `BASIC_OPT`. |
| executionMode | optional | Enum   | ONNX Runtime execution mode. One of `SEQUENTIAL`, `PARALLEL`. Default: `SEQUENTIAL`. |

## inputs

`inputs` is a [select](../common/select.md) list evaluated per record from top to bottom; an item can refer to the fields of the record and to the items above it.

- **An item whose `name` is a model input name feeds that model input.** The other items are intermediate values, which are not output.
- Every model input must be given. A model input that is not given is a configuration error.

An item can have these keys besides the select function:

| key | description |
|-----|-------------|
| `pad: 0` | Pads the records of a batch to the longest of each dimension with the value, at the tail. Without `pad`, records of different lengths can not form a batch: they are retried one by one, which works but gives up batching. |
| `pad: { value: 0, side: left }` | Pads at the head. |
| `fill: 0` | A tensor of the value, instead of a select function. Its shape comes from the model input: a dynamic dim0 is the batch size, other dynamic dimensions are 0. `shape: [2, 3]` specifies the shape explicitly. |
| `empty: [0, 512]` | An empty tensor of the shape, instead of a select function, for an input the model takes but this use does not fill. |

### Name variables

A `name` with `%d` (digits) or `%s` (letters, digits and `_`) applies to every model input it matches. A pattern that matches no model input is a configuration error, which catches settings copied from another model.

```yaml
- { name: "past_conv.%d", fill: 0 }            # past_conv.0, past_conv.1, ...
- { name: "past_key_values.%d.%s", fill: 0 }   # past_key_values.0.key, past_key_values.0.value, ...
```

### Rank of a value

The rank of a value (from its field type) decides how it becomes the model input:

| rank of the value | model input | use |
|---|---|---|
| one less than the model input | The values of the records are stacked on a new dim0: each record is a row of the batch. | `input_ids int64[B, L]` from `array<int64>`; `x float[B]` from a scalar |
| the same as the model input | The value is the input as it is. `bufferSize` must be 1, and the whole model output is the value of the record. | a table `x float[R, C]` from a matrix |

Other ranks are a configuration error. A `matrix` field is flat in the record and takes its shape from the schema; a shape with one dynamic dimension is restored from the number of values. Values are converted to the element type of the model input.

## outputs

`outputs` is a select list evaluated per record after inference. An item can refer to the fields of the record, the items of `inputs`, the model outputs and the items above it. If a name is in several of them, the model output comes first. A model output name with dots (`present.0.key`) is matched as a whole before it is read as a nested field.

Only the model outputs mentioned in `outputs` are requested from the session, and at least one must be.

The output record is the input record plus the fields of `outputs`; a field with the name of an input field replaces it. Use a later `select` transform to narrow the fields.

dim0 of a model output is the batch (unless no value was stacked), and the type of a model output is decided by the rest of the shape:

| model output shape | field type | example |
|---|---|---|
| `[B]` | scalar | a score per record |
| `[B, n]` | array of n values | an embedding `[B, 768]`; `[B, 1]` is an array of one value |
| `[B, d1, ..., dk]` | matrix of shape `[d1, ..., dk]` (row-major) | `[B, 2, 3]` |
| `[]` (scalar) | scalar, the same value for every record | |

Element types: float / float16 / bfloat16 -> `float32`, double -> `float64`, int64 -> `int64`, int8 / uint8 / int16 / int32 -> `int32`, bool -> `boolean`, string -> `string`. An output whose dim0 is not the batch size fails the batch.

Select functions for model outputs: [`vector_normalize`, `squeeze`, `softmax`, `sigmoid`, `argmax`, `topk`, `labels`](select.md#vector_normalize).

## Deprecated parameters

| parameter | how it is read |
|-----------|----------------|
| preprocesses | The head of `inputs`. Its fields stay in the output record. |
| mapping | `inputs: { field: modelInput }` is `{ name: modelInput, field: field }` in `inputs`; `outputs: { modelOutput: field }` is `{ name: field, field: modelOutput }` in `outputs`. Without its `outputs`, every model output is added with its own name. A name that one entry reads and another writes is a configuration error. Can not be used with `inputs` / `outputs`. |
| mappings | The same as `mapping`, and must have one element. |
| postprocesses | A select list on the output record. Only its fields are output. |

## Example: text embedding

Tokenizes a text field, runs an embedding model on batches of 16 records and truncates the embedding to 256 dimensions.

```yaml
sources:
  - name: items
    module: bigquery
    parameters:
      query: "SELECT id, title, description FROM `myproject.mydataset.items`"

transforms:
  - name: embedding
    module: onnx
    inputs: [items]
    parameters:
      model:
        path: gs://my-bucket/models/embeddinggemma/
        file: onnx/model_q4.onnx
      inputs:
        - { name: doc, text: "title: ${title} | text: ${description}" }
        - name: tok
          func: tokenize_encode
          tokenizer: huggingface
          path: gs://my-bucket/models/embeddinggemma/tokenizer.json
          field: doc
          maxLength: 2048
        - { name: input_ids, field: tok.ids, pad: 0 }
        - { name: attention_mask, field: tok.attentionMask, pad: 0 }
      outputs:
        - { name: embedding, func: vector_normalize, field: sentence_embedding, size: 256 }
      bufferSize: 16

sinks:
  - name: output
    module: storage
    inputs: [embedding]
    parameters:
      output: "gs://my-bucket/embeddings/"
      format: avro
```

## Example: classification

Runs a classifier on the numeric features of a record, and adds the probabilities and the most probable label.

```yaml
transforms:
  - name: classify
    module: onnx
    inputs: [features]
    parameters:
      model:
        path: gs://my-bucket/models/classifier.onnx
      filter:
        key: status
        op: "="
        value: "active"
      inputs:
        - { name: input, field: values }
      outputs:
        - { name: probabilities, func: softmax, field: logits }
        - { name: index, func: argmax, field: probabilities }
        - { name: label, func: labels, field: index, labels: [negative, neutral, positive] }
      bufferSize: 32
```
