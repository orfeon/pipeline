---
type: Sink Module
title: Tasks Sink Module
description: Planned sink for enqueueing input records as Google Cloud Tasks HTTP tasks. NOT YET IMPLEMENTED - the module is registered but its execution unconditionally fails with NotImplementedException, so it cannot be used in pipelines yet. Parameter definitions (queue, format, attributes, batching) exist as a design placeholder.
tags: [sink, tasks, cloudtasks, gcp, unimplemented]
timestamp: 2026-08-06T00:00:00Z
---

# Tasks Sink Module

Sink Module intended to create tasks in a Google Cloud Tasks queue from input records.

> **WARNING: This module is not implemented.** Although the module name `tasks` is registered and discoverable, its `expand` method unconditionally throws `NotImplementedException("Not Implemented tasks sink module")`. Any pipeline configuration that references this module will fail at pipeline assembly time. Do not use it. This page documents the currently declared (planned) parameters for reference only.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `tasks`                                                     |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Tasks sink module parameters (planned)

These parameters are declared and validated in the code, but the module never executes past validation because of the unconditional `NotImplementedException`.

| parameter         | optional | type           | description                                                                     |
|-------------------|----------|----------------|---------------------------------------------------------------------------------|
| queue             | required | String         | Cloud Tasks queue to create tasks in.                                           |
| format            | required | Enum           | Serialization format for the task payload. Values: `avro`, `json`, `protobuf`.  |
| attributes        | optional | Array<String\> | Input field names to attach as task attributes. Default: empty list.            |
| maxBatchSize      | optional | Integer        | Maximum number of records per request batch.                                    |
| maxBatchBytesSize | optional | Integer        | Maximum total payload bytes per request batch.                                  |

## Status

- Registered module name: `tasks` (`@Sink.Module(name="tasks")` on `TasksSink`).
- Execution: always fails with `NotImplementedException` before doing any work.
- The internal Cloud Tasks REST caller (`cloudtasks.googleapis.com/v2` task creation with metadata-server authentication) exists only as skeleton code and is not functional.
- No configuration example is provided because no configuration can currently run.

If you need to trigger external work from a pipeline today, consider the `action` sink (BigQuery / Vertex AI Gemini actions) or the `pubsub` sink instead.
