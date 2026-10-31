---
type: Transform Module
title: Onnx Transform Module
description: Runs inference on input records with an ONNX model using ONNX Runtime. Loads the model from Google Cloud Storage or a local path, builds the model inputs and the output fields with select lists (padding, constant inputs, name variables for many inputs), and adds the results to each record. A loop runs the model again on what it gave, for text generation with a KV cache. Image functions decode, resize and crop images, for reading text from images (OCR). Supports optional filter conditions, micro-batching via bufferSize and per-record retry of a failing batch. Works in both batch and streaming pipelines.
tags: [transform, onnx, ml, inference, machinelearning, llm, generation, image, ocr, batch, streaming]
timestamp: 2026-10-30T00:00:00Z
---

# Onnx Transform Module

Transform Module that performs inference on input records with a specified [ONNX](https://onnx.ai/) model using [ONNX Runtime](https://onnxruntime.ai/). One module runs one model, and one input record gives one output record: the input fields plus the fields of `outputs`. What is specific to a model is written in the select lists `inputs` and `outputs`, not in the module.

Supports:

- **Model loading from GCS or local path** - A single `.onnx` file (`gs://bucket/model.onnx`), or a directory (`gs://bucket/models/xxx/` or a local directory) whose files are downloaded with their layout kept, so that external weight files (`*.onnx_data`) are found next to the model; `model.file` picks the model file inside it. The loaded session is shared by all DoFn instances (and all onnx modules using the same model and session options) in the same worker process (JVM), so one model copy serves multiple threads. Pipeline assembly only reads the graph of the `.onnx` file: no session is created and no weights are read, unless the graph leaves out the type or the shape of an input or an output, which only a session can infer.
- **Select lists** - [`inputs`](#inputs) builds the model inputs with select functions, and [`outputs`](#outputs) builds the output fields from the model outputs.
- **Padding and constant inputs** - `pad` pads the records of a batch to a common length; `empty` and `fill` give inputs that do not come from the record; `%d` / `%s` in a name match many model inputs.
- **Loop** - [`loop`](#loop) runs the model again on what the last run gave, for text generation with a KV cache and other iterative models.
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
| loop       | optional | [Loop](#loop)                         | Runs the model again on what the last run gave. `bufferSize` must be 1. |
| filter     | optional | [Filter](../common/filter.md)         | Filter condition applied before inference. Records that do not match are discarded (they do not appear in the output). |
| bufferSize | optional | Integer                               | Number of records to run inference on as one batch. Default: `1`. Records left in a partially filled buffer are inferred when the bundle finishes; each record keeps its own timestamp and window. With overlapping (sliding) windows inference runs once per window. |

## Model parameters

| parameter     | optional | type   | description |
|---------------|----------|--------|-------------|
| path          | required | String | Path of the ONNX model. A GCS object path (`gs://bucket/model.onnx`), a GCS directory path ending with `/` (all files under the directory are downloaded to the worker, keeping the sub directories), a local file path, or a local directory path. |
| file          | optional | String | For a directory `path`: the model file relative to the directory (e.g. `onnx/model_q4.onnx`). If omitted, the directory must contain exactly one `.onnx` file. |
| intraOpThreads | optional | Integer | Number of threads one inference uses. Default: `1`: the worker threads share the session and run inferences in parallel, which scales better than giving every inference all the cores. Tune parallelism with the worker thread count (e.g. Dataflow `numberOfWorkerHarnessThreads`). |
| interOpThreads | optional | Integer | Number of threads that run independent graph operators in parallel (`executionMode: PARALLEL` only). Default: the ONNX Runtime default. |
| optLevel      | optional | Enum   | ONNX Runtime graph optimization level. One of `NO_OPT`, `BASIC_OPT`, `EXTENDED_OPT`, `ALL_OPT`. Default: `ALL_OPT`, as in ONNX Runtime. A lower level can give wrong values without an error: a model quantized with float16 weights (`model_q4f16.onnx`) gave wrong logits on a CPU with `BASIC_OPT` and `NO_OPT`. |
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
| `pad: { value: NaN, side: left, multiple: 16 }` | `value` can be `NaN` for a float input (`.nan` in YAML works as well), where a model takes NaN as a missing value. `multiple` rounds the length up to a multiple, also for one record: for a model that only takes such lengths. It applies to the dimensions of the row that are dynamic in the model input; a model input without one is a configuration error. |
| `fill: 0` | A tensor of the value (`NaN` for a float input only), instead of a select function. Its shape comes from the model input: a dynamic dim0 is the batch size, other dynamic dimensions are 0. `shape: [2, 3]` specifies the shape explicitly. A model input of rank 0 gets a scalar (`num_logits_to_keep int64[]` with `fill: 1`), and a float16 input a float16 tensor. |
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
| the same as the model input | The value is the input as it is. `bufferSize` must be 1. | a table `x float[R, C]` from a matrix |

Other ranks are a configuration error. A `matrix` field is flat in the record and takes its shape from the schema; a shape with one dynamic dimension is restored from the number of values. Values are converted to the element type of the model input.

What dim0 of the model outputs means follows from these:

| values | model outputs |
|---|---|
| all stacked | dim0 is the batch: each record gets its row. |
| all as they are | The whole output is the value of the record. |
| both | dim0 is still the batch, which has one record: tokens `input_ids int64[B, L]` stacked next to `image_features float[N, 512]` as it is give `sentence_embedding [1, D]`, an array of D values. An output whose dim0 is not 1 fails the record. To take a whole output, give every value at the rank of its model input. |

## outputs

`outputs` is a select list evaluated per record after inference. An item can refer to the fields of the record, the items of `inputs`, the model outputs and the items above it. If a name is in several of them, the model output comes first. A model output name with dots (`present.0.key`) is matched as a whole before it is read as a nested field.

Only the model outputs mentioned in `outputs` are requested from the session, and at least one must be.

The output record is the input record plus the fields of `outputs`; a field with the name of an input field replaces it. Use a later `select` transform to narrow the fields.

dim0 of a model output is the batch if a value was stacked ([Rank of a value](#rank-of-a-value)), and the type of a model output is decided by the rest of the shape:

| model output shape | field type | example |
|---|---|---|
| `[B]` | scalar | a score per record |
| `[B, n]` | array of n values | an embedding `[B, 768]`; `[B, 1]` is an array of one value |
| `[B, d1, ..., dk]` | matrix of shape `[d1, ..., dk]` (row-major) | `[B, 2, 3]` |
| `[]` (scalar) | scalar, the same value for every record | |

Element types: float / float16 / bfloat16 -> `float32`, double -> `float64`, int64 -> `int64`, int8 / uint8 / int16 / int32 -> `int32`, bool -> `boolean`, string -> `string`. An output whose dim0 is not the batch size fails the batch.

Select functions for model outputs: [`vector_normalize`, `squeeze`, `softmax`, `sigmoid`, `argmax`, `topk`, `labels`](select.md#vector_normalize), [`group_softmax`](select.md#group_softmax), [`slice`](select.md#slice), [`sample`](select.md#sample) and [`timeseries_quantiles`](select.md#timeseries_quantiles). For the model inputs: [`array`, `array_append`](select.md#array), [`range`](select.md#range), [`timeseries_grid`](select.md#timeseries_grid), and [`token_ids`](select.md#token_ids) for what `group_softmax` takes. For a tabular model that learns in context: [`tabular_encode` and `tabular_decode`](select.md#tabular_encode). For images: [`image_decode` and the other image functions](select.md#image_decode), [`connected_components`](select.md#connected_components), [`text_line_boxes`](select.md#text_line_boxes) and [`ctc_decode`](select.md#ctc_decode).

## loop

`loop` runs the model on one record again and again, with what the last run gave. The first run takes its inputs from `inputs`.

| parameter     | optional | type | description |
|---------------|----------|------|-------------|
| next          | required | Map<String, String or [Select](../common/select.md)\> | What the next run takes, evaluated in order after each run. A string is the name of a model output, which the next run takes as it is. An object is a select function, whose key is its `name`. |
| until         | required | [Filter](../common/filter.md) | Checked after `next`: the loop stops when it holds. It compares single values: a field that is an array or a matrix is a configuration error, as is a filter without a condition. `false` runs every record up to `maxIterations`. |
| maxIterations | required | Integer | The largest number of runs. Reaching it is not a failure: the record gets the values of the last run. |
| vars          | optional | Map<String, [Select](../common/select.md)\> | Variables of the loop, set before the first run. `{ value: [], type: array<int64> }` is an empty array. An item of `next` of the same name updates one. |

```
inputs, vars -> run -> next -> until or maxIterations? -> outputs
                 ^                 | no
                 +-----------------+
```

- A model input that is not in `next` keeps its value, as an encoder output or a constant does.
- `next`, `until` and `outputs` can refer to the fields of the record, the items of `inputs`, the variables, the items of `next` above them and the model outputs of the last run. A variable or an item of `next` must not have the name of a model output, or `_iteration`.
- `_iteration` is the number of the last run, from 0.
- A string in `next` keeps the tensor of the model output, without a copy or a conversion: the type and the rank of the model input and output must be the same. Name variables correspond in the order they appear.
- The model outputs that `next` and `until` read are copied once per run, and those that only `outputs` reads once, from the last run. The logits of a model that gives every position of a long prompt are copied whole, once: for a large vocabulary, use a model that only gives the last position.
- All the runs of a record are held by one worker thread, and the transform has no time limit: choose `maxIterations` accordingly.
- A failure in any run fails the record, without its earlier results.

The `until` of the [http source](../source/http.md) is checked before its `next`. It is the opposite here, as what `until` checks is typically what `next` chose from the outputs.

### Example: text generation

```yaml
- name: generate
  module: onnx
  inputs: [prompts]
  parameters:
    model:
      path: gs://bucket/models/llm/
      file: onnx/model_q4.onnx
    inputs:
      - { name: tok, func: tokenize_encode, tokenizer: huggingface, path: gs://bucket/models/llm/tokenizer.json, field: prompt }
      - { name: input_ids, field: tok.ids }
      - { name: attention_mask, field: tok.attentionMask }
      - { name: position_ids, func: range, field: tok.ids }
      - { name: "past_key_values.%d.%s", fill: 0 }
    loop:
      vars:
        generated: { value: [], type: array<int64> }
      next:
        "past_key_values.%d.%s": "present.%d.%s"
        token: { func: argmax, field: logits, position: last }
        generated: { func: array_append, field: generated, valueField: token }
        input_ids: { func: array, fields: [token] }
        attention_mask: { func: array_append, field: attention_mask, value: 1 }
        position_ids: { func: range, field: attention_mask, last: 1 }
      until: { key: token, op: in, value: [1, 7] }
      maxIterations: 512
    outputs:
      - { name: text, func: tokenize_decode, tokenizer: huggingface, path: gs://bucket/models/llm/tokenizer.json, field: generated, skipSpecialTokens: true }
```

- `generated` also holds the token that stopped the loop, which `skipSpecialTokens` leaves out of `text`.
- To sample instead, use `token: { func: sample, field: logits, position: last, temperature: 0.7, topP: 0.9, seed: 0 }`.
- The inputs differ from model to model: leave out what the model does not take (`position_ids`), and add what it needs (`{ name: "past_conv.%d", fill: 0 }`, `{ name: num_logits_to_keep, fill: 1 }`). The ids that end a text (`until`) differ as well.
- A model split into several files must first be merged into one.

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

## Example: choosing among options

Asks a text generation model a question and compares the logits of the tokens that start each option. A model that returns the logits of the last position only, `logits float[B, 1, V]`, gives `[1, V]` for a record. A model split into several `.onnx` files must be merged into one before it is used here: one module runs one model.

```yaml
transforms:
  - name: decide
    module: onnx
    inputs: [inquiries]
    parameters:
      model:
        path: gs://my-bucket/models/classifier/
        file: onnx/model_q4.onnx
      inputs:
        - name: prompt
          text: "<|im_start|>user\n${body}\n\nWhich is it? refund / account / other<|im_end|>\n<|im_start|>assistant\n"
        - { name: tok, func: tokenize_encode, tokenizer: huggingface, path: gs://my-bucket/models/classifier/tokenizer.json, field: prompt, addSpecialTokens: false }
        - { name: input_ids, field: tok.ids }
        - { name: attention_mask, field: tok.attentionMask }
        - { name: "past_conv.%d", fill: 0 }
        - { name: "past_key_values.%d.%s", fill: 0 }
        - name: options
          func: token_ids
          tokenizer: huggingface
          path: gs://my-bucket/models/classifier/tokenizer.json
          texts:
            - [" refund", "refund"]
            - [" account", "account"]
            - [" other", "other"]
      outputs:
        - { name: lastLogits, func: squeeze, field: logits, axis: 0 }
        - { name: probabilities, func: group_softmax, field: lastLogits, groups: options }
        - { name: index, func: argmax, field: probabilities }
        - { name: category, func: labels, field: index, labels: [refund, account, other] }
```

## Example: time series forecast

One record is one series: `aggregation` collects its points, `timeseries_grid` aligns them, and the model gives a quantile forecast of `forecasts float[B, 9, 64]`. The model takes NaN as a missing value and lengths of a multiple of 16.

```yaml
transforms:
  - name: series
    module: aggregation
    inputs: [sales]
    parameters:
      groupFields: [item_id]
      aggregations:
        - input: sales
          fields:
            - { name: points, op: array_agg, fields: [timestamp, value] }
  - name: forecast
    module: onnx
    inputs: [series]
    parameters:
      model:
        path: gs://my-bucket/models/forecaster/model.onnx
      inputs:
        - { name: grid, func: timeseries_grid, field: points, timestampField: timestamp, valueField: value, interval: 1h, length: 512 }
        - { name: context, field: grid.values, pad: { value: NaN, side: left, multiple: 16 } }
      outputs:
        - name: forecast
          func: timeseries_quantiles
          field: forecasts
          levels: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
          start: grid.lastTimestamp
          interval: 1h
          select: [0.1, 0.5, 0.9]
      bufferSize: 32
```

A NaN or an infinity fails the record if the model input is an integer or a bool: a grid with gaps needs a float model input.

`forecast` is an array of 64 elements with `timestamp`, `q10`, `q50` and `q90`. Use `flattenField: forecast` of a later `select` transform to get a record per step: that transform also requires a `select` list, of the fields to keep, which has to name `forecast`.

## Example: predicting rows from the other rows of a group

[Kumo Tabular](https://huggingface.co/nvidia/Kumo-Tabular) predicts rows from labeled rows given along with them, without training: `onnx-community/kumo-tabular-small-classifier-ONNX` and `onnx-community/kumo-tabular-small-regressor-ONNX`. One group of rows is one inference.

```yaml
transforms:
  - name: groups
    module: aggregation
    inputs: [listings]
    parameters:
      groupFields: [category_id]
      aggregations:
        - input: listings
          fields:
            - { name: rows, op: array_agg, fields: [item_id, price, brand, condition, label] }
  - name: kumo
    module: onnx
    inputs: [groups]
    parameters:
      model:
        path: gs://my-bucket/models/kumo-tabular-small-classifier-ONNX/
        file: onnx/model.onnx
      inputs:
        - name: tab
          func: tabular_encode
          preset: kumo
          task: classification
          field: rows
          key: item_id
          target: label
          numerical: [price]
          categorical: [brand, condition]
          maxContextRows: 2000
        - { name: x, field: tab.x }
        - { name: y_context, field: tab.yContext }
        - { name: categorical_mask, field: tab.categoricalMask }
      outputs:
        - { name: predictions, func: tabular_decode, preset: kumo, field: logits, encoded: tab }
```

- A row with a `label` is context, and a row without one is predicted.
- `predictions` is an array with `key`, `prediction` and `probabilities`. For a record per row, use a later `select` transform with `flattenField: predictions` and a `select` list that names `predictions`.
- A group without a row to predict gives an empty `predictions` without running the model, and null for an output of the model, whatever its context is. A group with a row to predict that has no context row, more than 10 classes, or no column left after the constant ones are removed fails by itself.
- The time and the memory grow quickly with the number of rows of a group: `maxContextRows` bounds the context, but all the rows to predict go to the model. Split a group with too many of them before this transform.
- For a regression, use the regressor model with `task: regression`, and `quantiles` in `tabular_decode`.

## Example: reading the text of an image

[indra-ocr](https://huggingface.co/datalab-to/indra-ocr) finds the lines of a page with one model, chooses the script of each line with another, and reads a line with the model of its script. Each is a transform: a page gives a record per line, and only the texts are shuffled, to choose the best reading of a line. The license of the weights limits their use: read it before you use them.

```yaml
transforms:
  - name: detect
    module: onnx
    inputs: [pages]
    parameters:
      model: { path: gs://my-bucket/models/indra-ocr/, file: det.onnx }
      inputs:
        - { name: decoded, func: image_decode, field: content }
        - { name: page, func: image_gray, field: decoded }
        - { name: small, func: image_resize, field: page, longSide: 1024 }
        - { name: padded, func: image_pad, field: small, multiple: 32, value: 255 }
        - { name: x, func: image_tensor, field: padded, invert: true }
      outputs:
        - { name: boxes, func: text_line_boxes, field: out, imageField: page, longSide: 1024 }
        - { name: bands, func: image_crop, field: page, boxes: boxes, height: 32, padX: 0.3, padY: 0.2, invertDark: true }
        - { name: windows, func: image_crop, field: page, boxes: boxes, height: 96, padX: 0.3, padY: 1.6, invertDark: true }
        - { name: numbers, func: range, field: boxes }
  - name: lines
    module: select
    inputs: [detect]
    parameters:
      select:
        - { name: id, field: id }
        - { name: line, func: array_zip, fields: [{ name: number, field: numbers }, { name: band, field: bands }, { name: tall, field: windows }] }
      flattenField: line
  - name: script
    module: onnx
    inputs: [lines]
    parameters:
      model: { path: gs://my-bucket/models/indra-ocr/, file: script.onnx }
      bufferSize: 4
      inputs:
        - { name: decoded, func: image_decode, field: line.band }
        - { name: gray, func: image_gray, field: decoded }
        - { name: size, func: image_size, field: gray }
        - { name: x, func: image_tensor, field: gray, invert: true, pad: { value: 0, multiple: 8 } }
        - { name: widths, field: size.width }
      outputs:
        - { name: best, func: topk, field: p, k: 1 }
        - { name: first, func: slice, field: best, index: 0 }
        - { name: confidence, field: first.value }
        - { name: index, field: first.index }
        - { name: group, func: labels, field: index, labels: [base, cjk, rtl, world] }
  - name: route
    module: partition
    inputs: [script]
    parameters:
      exclusive: false
      partitions:
        - name: base
          filter: { or: [{ key: group, op: "=", value: base }, { key: confidence, op: "<=", value: 0.95 }] }
        - name: cjk
          filter: { key: group, op: "=", value: cjk }
  - name: rec_base
    module: onnx
    inputs: [route.base]
    parameters:
      model: { path: gs://my-bucket/models/indra-ocr/, file: rec.onnx }
      bufferSize: 4
      inputs:
        - { name: decoded, func: image_decode, field: line.tall }
        - { name: gray, func: image_gray, field: decoded }
        - { name: size, func: image_size, field: gray }
        - { name: x, func: image_tensor, field: gray, invert: true, pad: { value: 0, multiple: 8 } }
        - { name: widths, field: size.width }
      outputs:
        - { name: read, func: ctc_decode, field: ids, confField: conf, lengthField: lengths, path: gs://my-bucket/models/indra-ocr/meta.json, key: vocab, normalize: NFC, minConf: 0.5 }
        - { name: text, field: read.text }
        - { name: confidence, field: read.conf }
  # rec_cjk is the same with file: rec_cjk.onnx, field: line.band and key: vocab_cjk; rtl and world as well
  - name: read
    module: select
    inputs: [rec_base, rec_cjk]
    parameters:
      select:
        - { name: id, field: id }
        - { name: number, field: line.number }
        - { name: text, field: text }
        - { name: confidence, field: confidence }
  - name: best
    module: aggregation
    inputs: [read]
    parameters:
      groupFields: [id, number]
      aggregations:
        - input: read
          fields:
            - { name: text, op: arg_max, field: text, comparingField: confidence }
```

- A line the script model is not sure of also goes to `rec_base`, and `best` keeps the reading with the highest confidence.
- `rec.onnx` reads a tall window, the line with the page above and below it, at 96 pixels. The other recognizers read the line at 32 pixels.
- `numbers` is the order the lines are found in, which is not the reading order of a page of several columns.
- This does not do what the package of indra-ocr does besides the models above: a second detection for small text, turning and straightening a page, the language model for lines of low confidence, the order of the text of right-to-left scripts, and the reading order.
