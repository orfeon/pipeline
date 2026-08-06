---
type: Sink Module
title: Iceberg Sink Module
description: Writes input records to an Apache Iceberg table through a configurable Iceberg catalog. The destination table is specified as a catalog table identifier, catalog connection settings are passed as catalog/config property maps (config type defaults to hadoop), and streaming writes can set a triggering frequency. Experimental - the Beam IcebergIO integration is still under development and the module currently performs no write.
tags: [sink, iceberg, batch, streaming, lakehouse]
timestamp: 2026-08-06T00:00:00Z
---

# Iceberg Sink Module

Sink Module for writing input records to an [Apache Iceberg](https://iceberg.apache.org/) table.
The destination table is specified as a catalog table identifier (for example `mydb.mytable`), and
the Iceberg catalog is configured with a catalog name plus two property maps
(`catalogProperties` for Iceberg catalog properties such as `type` and `warehouse`,
`configProperties` for the underlying Hadoop configuration). Multiple inputs are flattened
(union) into a single collection before writing.

> **Status: experimental.** The underlying Apache Beam IcebergIO integration in this module is
> still under development. Parameters are validated as documented below, but the actual table
> write is not yet wired up (the module currently performs no write) — do not use this module in
> production pipelines yet.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `iceberg`                                                   |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                           |

## Iceberg sink module parameters

| parameter                   | optional | type                | description                                                                                                                                          |
|-----------------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| output                      | required | String              | Iceberg table identifier to write to, in catalog table identifier form (e.g. `mydb.mytable`).                                                          |
| catalogName                 | required | String              | Name of the Iceberg catalog used to resolve the destination table.                                                                                     |
| catalogProperties           | optional | Map<String,String\> | Iceberg catalog properties (e.g. `type`, `warehouse`, `uri`). Default: empty map.                                                                      |
| configProperties            | optional | Map<String,String\> | Properties for the underlying Hadoop `Configuration` used by the catalog. Default: empty map with `type` set to `hadoop` when not specified.           |
| triggeringFrequencySeconds  | optional | Long                | Triggering frequency in seconds for streaming writes (how often data files are committed to the table). Only meaningful in streaming mode.             |

## Examples

### Example 1: Write BigQuery query results to an Iceberg table

```yaml
sources:
  - name: bigquery_input
    module: bigquery
    parameters:
      query: "SELECT * FROM `myproject.mydataset.orders`"

sinks:
  - name: iceberg_output
    module: iceberg
    inputs:
      - bigquery_input
    parameters:
      output: "analytics.orders"
      catalogName: "mycatalog"
      catalogProperties:
        type: "hadoop"
        warehouse: "gs://my-bucket/warehouse"
```

### Example 2: Streaming write from Pub/Sub with a triggering frequency

```yaml
sources:
  - name: pubsub_input
    module: pubsub
    schema:
      fields:
        - name: event_id
          type: string
        - name: event_time
          type: timestamp
    parameters:
      subscription: "projects/myproject/subscriptions/mysubscription"
      format: json

sinks:
  - name: iceberg_output
    module: iceberg
    inputs:
      - pubsub_input
    parameters:
      output: "analytics.events"
      catalogName: "mycatalog"
      catalogProperties:
        type: "hadoop"
        warehouse: "gs://my-bucket/warehouse"
      triggeringFrequencySeconds: 60
```
