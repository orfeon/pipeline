---
type: Transform Module
title: Reshuffle Transform Module
description: Redistributes elements across workers using Beam's Reshuffle to prevent fusion and enable checkpointing. Takes no parameters. Passes data through unchanged with the same schema. Works in both batch and streaming modes.
tags: [transform, reshuffle, fusion, checkpoint, parallelism, batch, streaming]
timestamp: 2026-07-05T00:00:00Z
---

# Reshuffle Transform Module

Transform Module that redistributes input elements across workers by applying Beam's `Reshuffle.viaRandomKey()`. Records pass through unchanged — the data, schema, and output name are identical to the input.

Reshuffle is useful because Beam runners fuse adjacent steps into a single stage for efficiency. Fusion can hurt a pipeline when an early step produces few elements that fan out into expensive downstream work, since all the work stays on the workers that produced the elements. Inserting a reshuffle:

- **Breaks fusion** - Downstream steps run in a separate stage, so elements are rebalanced across all workers instead of staying where they were produced.
- **Redistributes elements** - Each element is assigned a random key and shuffled, spreading skewed or low-parallelism output evenly.
- **Acts as a checkpoint** - The shuffle materializes the data, so upstream steps (e.g. calls to external services) are not re-executed when downstream steps retry.

Typical placements: after a source or transform that generates many records from few inputs (fan-out), before an expensive per-element transform (e.g. `http`, `onnx`), or after a step with non-idempotent side effects that should not be retried.

When the module has multiple inputs, each input is reshuffled independently and its output is referenced as `{transformName}.{inputName}`. With a single input, the output is referenced simply as `{transformName}`.

## Transform module common parameters

| parameter  | optional | type                              | description                                                           |
|------------|----------|-----------------------------------|-----------------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.                     |
| module     | required | String                            | Specified `reshuffle`                                                 |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be used as input.                   |
| waits      | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                           |
| parameters | optional | Map<String,Object\>               | This module takes no individual parameters.                           |

## Reshuffle transform module parameters

This module has no individual parameters. The `parameters` block can be omitted or left empty.

## Examples

### Example 1: Break fusion before an expensive transform

A BigQuery query returns a list of documents; reshuffling before the per-record ONNX inference spreads the work across all workers instead of the few that read the query results.

```yaml
sources:
  - name: documents
    module: bigquery
    parameters:
      query: "SELECT id, body FROM `myproject.mydataset.documents`"

transforms:
  - name: rebalance
    module: reshuffle
    inputs:
      - documents

  - name: inference
    module: onnx
    inputs:
      - rebalance
    parameters:
      model:
        path: "gs://my-bucket/models/embedding.onnx"

sinks:
  - name: results
    module: storage
    inputs:
      - inference
    parameters:
      output: "gs://my-bucket/embeddings/"
      format: json
```
