---
type: Transform Module
title: Onnx Gen Transform Module
description: Experimental transform that runs text generation for each input record with an ONNX Runtime GenAI model (e.g. Phi-family models). Loads the generative model files from a Google Cloud Storage directory or local path, renders a FreeMarker prompt template with each record's field values, and generates a response with configurable search options. Currently the generated text is only written to worker logs; the module produces no downstream output collection.
tags: [transform, onnx, genai, llm, generation, experimental]
timestamp: 2026-08-06T00:00:00Z
---

# Onnx Gen Transform Module (Experimental)

Transform Module that runs text generation for each input record using an [ONNX Runtime GenAI](https://onnxruntime.ai/docs/genai/) model (for example Phi-family generative models exported for onnxruntime-genai).

For each input record, the `prompt` template is rendered with the record's field values (FreeMarker syntax, e.g. `${fieldName}`), the rendered prompt is passed to the generative model, and a response is generated.

**Experimental limitations:**

- The generated response is currently only written to the worker logs (together with the generation duration). The module does **not** emit an output collection, so no downstream module can consume its results yet.
- The model directory is downloaded to worker local disk at startup; the loaded model is shared by all threads in the same worker process. Generative models are large — use workers with sufficient memory and disk.

Per-record generation errors are routed as failures according to the module common `failFast` setting.

## Transform module common parameters

| parameter  | optional | type                              | description                                                    |
|------------|----------|-----------------------------------|----------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.              |
| module     | required | String                            | Specified `onnx_gen`                                           |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be used as input.            |
| waits      | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.  |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                    |
| logs       | optional | Array<[Logging](../common/logging.md)\> | Logging config; supports logging each `input` element.  |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                    |

## Onnx gen transform module parameters

| parameter     | optional | type                 | description                                                                                                                                                                                                          |
|---------------|----------|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| model         | required | String               | Path to the ONNX Runtime GenAI model directory. A GCS directory path (`gs://bucket/models/phi-3/` — all files in the directory are downloaded to the worker) or a local directory path. A trailing `/` is appended automatically. |
| prompt        | required | String               | Prompt template rendered per record with [FreeMarker template](../common/template.md) syntax. Record field values are referenced as `${fieldName}`.                                                                  |
| searchOptions | optional | Map<String,Double\>  | Numeric generation search options passed to the GenAI generator params, e.g. `max_length`, `temperature`, `top_p`, `top_k`, `repetition_penalty`. Default: empty.                                                    |
| searchFlags   | optional | Map<String,Boolean\> | Boolean generation search options passed to the GenAI generator params, e.g. `do_sample`, `early_stopping`. Default: empty.                                                                                          |

## Example: generate a summary per record

Renders a prompt from each record's fields and generates a response with a Phi model. The responses appear in the worker logs.

```yaml
sources:
  - name: reviews
    module: bigquery
    parameters:
      query: "SELECT id, product_name, review_text FROM `myproject.mydataset.reviews`"

transforms:
  - name: summarize
    module: onnx_gen
    inputs:
      - reviews
    parameters:
      model: gs://my-bucket/models/phi-3-mini-4k-instruct-onnx/
      prompt: |
        <|user|>
        Summarize the following review of ${product_name} in one sentence:
        ${review_text}
        <|end|>
        <|assistant|>
      searchOptions:
        max_length: 256
        temperature: 0.2
      searchFlags:
        do_sample: true
```
