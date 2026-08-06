---
type: Source Module
title: Iceberg Source Module
description: Reads records from an Apache Iceberg table through a configurable Iceberg catalog. The table is specified as a catalog table identifier, and catalog connection settings are passed as catalog/config property maps. Experimental - the Beam IcebergIO integration is still under development and the module is not yet functional.
tags: [source, iceberg, batch, lakehouse]
timestamp: 2026-08-06T00:00:00Z
---

# Iceberg Source Module

Source Module for reading records from an [Apache Iceberg](https://iceberg.apache.org/) table.
The table to read is specified as a catalog table identifier (for example `mydb.mytable`), and the
Iceberg catalog to resolve it against is configured with a catalog name plus two property maps
(`catalogProperties` for Iceberg catalog properties such as `type` and `warehouse`,
`configProperties` for the underlying Hadoop configuration).

> **Status: experimental.** The underlying Apache Beam IcebergIO integration in this module is
> still under development. Parameters are validated as documented below, but the actual table read
> is not yet wired up — do not use this module in production pipelines yet.

## Source module common parameters

| parameter          | optional | type                | description                                                                        |
|--------------------|----------|---------------------|------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                  |
| module             | required | String              | Specified `iceberg`                                                                |
| schema             | optional | [Schema](../common/schema.md) | Schema of the data to be read.                                           |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                        |

## Iceberg source module parameters

| parameter                   | optional | type                | description                                                                                                                                     |
|-----------------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| input                       | required | String              | Iceberg table identifier to read, in catalog table identifier form (e.g. `mydb.mytable`).                                                        |
| catalogName                 | required | String              | Name of the Iceberg catalog used to resolve the table.                                                                                           |
| catalogProperties           | optional | Map<String,String\> | Iceberg catalog properties (e.g. `type`, `warehouse`, `uri`). Default: empty map.                                                                |
| configProperties            | optional | Map<String,String\> | Properties for the underlying Hadoop `Configuration` used by the catalog (e.g. filesystem settings). Default: empty map.                         |
| triggeringFrequencySeconds  | optional | Long                | Reserved for streaming read configuration. Currently declared but not used by the read path.                                                     |

When the pipeline runs in streaming mode, the read is intended to run as a continuous (streaming)
read of the Iceberg table; in batch mode it is a one-shot scan.

## Examples

### Example 1: Read an Iceberg table from a Hadoop catalog on GCS

```yaml
sources:
  - name: iceberg_input
    module: iceberg
    parameters:
      input: "mydb.events"
      catalogName: "mycatalog"
      catalogProperties:
        type: "hadoop"
        warehouse: "gs://my-bucket/warehouse"

sinks:
  - name: debug
    module: debug
    inputs:
      - iceberg_input
    parameters:
      logLevel: info
```

### Example 2: Read an Iceberg table and write to BigQuery

```yaml
sources:
  - name: iceberg_input
    module: iceberg
    parameters:
      input: "analytics.orders"
      catalogName: "mycatalog"
      catalogProperties:
        type: "hadoop"
        warehouse: "gs://my-bucket/warehouse"
      configProperties:
        fs.gs.project.id: "myproject"

sinks:
  - name: bigquery_output
    module: bigquery
    inputs:
      - iceberg_input
    parameters:
      table: "myproject.mydataset.orders"
```
