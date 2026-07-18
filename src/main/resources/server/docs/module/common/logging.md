---
type: Common
title: Logging
description: The per-module `logs` field — record-level logging at named logging points (input / output / not_matched, ...) for debugging pipelines.
tags: [common, logging, logs, debug]
timestamp: 2026-07-17T00:00:00Z
---

# Logging

Every module accepts an optional `logs` field (at the module level, next to `parameters`) that
turns on record-level logging at named **logging points** inside the module. Use it to observe
what a module actually receives and emits without changing the pipeline — the records appear in
the worker logs (Cloud Logging on Dataflow, the console on Direct runner).

```yaml
transforms:
  - name: filtered
    module: select
    inputs: [input]
    logs:
      - { name: input, level: debug }
      - { name: not_matched, level: warn }
    parameters:
      filter:
        - { key: amount, op: ">", value: 0 }
```

## Log entry parameters

| parameter | optional | type   | description                                                                                    |
|-----------|----------|--------|--------------------------------------------------------------------------------------------------|
| name      | required | String | The logging point to enable. Available names depend on the module (see below).                |
| level     | optional | Enum   | Log level: one of `trace`, `debug`, `info`, `warn`, `error`. The default is `info`.           |

Each enabled point logs a message of the form `{moduleName}.{pointName}: {record as JSON}`.
Logging points that are not listed in `logs` stay silent — the field is opt-in per point.

## Common logging points

The exact set depends on the module, but most modules follow this convention:

| name          | logged records                                                                  |
|---------------|-----------------------------------------------------------------------------------|
| `input`       | Every record the module receives.                                               |
| `output`      | Every record the module emits.                                                  |
| `not_matched` | Records dropped by a filter condition (modules with `filter` support: select, http, drive, files, ...). |
| `matched`     | Records that passed a filter condition.                                          |
| `response`    | Raw responses (http source).                                                      |
| `system`      | Module-internal status messages.                                                  |

The `logs` field is also available on failure sink definitions (`failureSinks`).

## Notes

- Logging happens per record. On high-volume or streaming pipelines enable it selectively
  (e.g. only `not_matched` at `warn`) and remove it after debugging — logging every element of a
  large input has a real performance and log-volume cost.
- For inspecting outputs during development, the [debug sink module](../sink/debug.md) is often
  the better tool; `logs` is most useful for seeing what happens *inside* a module (e.g. why a
  record was filtered out).
