---
type: Action Module
title: Storage Action Module
description: Writes a small file from the triggering records — the control-plane counterpart of the storage sink. Use it to keep execution result histories (e.g. the file list a storage sink emitted) as a JSONL object, write templated summary reports, or create marker files (e.g. an empty _SUCCESS object with trigger once). Supports GCS/S3/local paths via the Beam filesystems.
tags: [action, storage, gcs, file, history, marker, report, trigger, workflow]
timestamp: 2026-08-19T00:00:00Z
---

# Storage Action Module

Action module (`actions` section, `module: storage`) that writes a small file from the triggering records. It is the control-plane counterpart of the [storage sink](../sink/storage.md): the sink writes datasets (with formats, schemas and sharding), while this action writes execution artifacts — result histories, summary reports, marker files. See [action modules](README.md) for the `actions` section, trigger semantics and the output envelope.

Each execution creates (or overwrites) the object at `output` in full. With `trigger: perElement`, include element fields in the `output` template so firings do not overwrite each other. Paths use the Beam filesystems (GCS `gs://…`, S3 `s3://…`, or local).

## Content

- With `content` set: the rendered template becomes the file body. Template variables: the element's fields for `perElement`; `elements` (list of field maps) and `size` for `collect` (and `once`, where the list is empty).
- Without `content`: the elements are written as JSON Lines — one JSON object per element (an empty file for `trigger: once`, useful as a marker/`_SUCCESS` object).

## Parameters

| parameter | optional | type   | description                                                                                       |
|-----------|----------|--------|-----------------------------------------------------------------------------------------------------|
| output    | required | String | Destination path (template-able), e.g. `gs://bucket/history/latest.jsonl`.                           |
| content   | optional | String | Template for the file body. Omit to write the elements as JSON Lines.                                |

## Output

One envelope record per execution (see [action modules](README.md#output-envelope)); `jobId` is the written path.

## Examples

### Example 1: Keep the written-file list of a storage sink as a history object

```yaml
sinks:
  - name: store
    module: storage
    inputs: [input]
    parameters:
      output: gs://my-bucket/export/data
      format: avro
actions:
  - name: history
    module: storage
    trigger: collect
    inputs: [store]                # storage sink emits {sink, path, timestamp} records
    parameters:
      output: gs://my-bucket/history/latest.jsonl
```

### Example 2: Templated summary report

```yaml
actions:
  - name: report
    module: storage
    trigger: collect
    inputs: [store]
    parameters:
      output: gs://my-bucket/reports/summary.txt
      content: |
        ${size} files written:
        <#list elements as e>
        - ${e.path}
        </#list>
```

### Example 3: Marker file after other steps complete

```yaml
actions:
  - name: success_marker
    module: storage
    waits: [store, load]
    parameters:
      output: gs://my-bucket/export/_SUCCESS
      content: ""
```
