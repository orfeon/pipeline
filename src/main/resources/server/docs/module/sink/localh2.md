---
type: Sink Module
title: LocalH2 Sink Module
description: Builds a local H2 database file from input records on the pipeline workers and uploads it as a zip archive to Cloud Storage (GCS). Supports multiple inputs written to separate tables, user-supplied DDL statements, INSERT/UPSERT/DELETE operations, batched JDBC writes, optionally seeding from an existing database zip, and grouping records into separate database files by key fields.
tags: [sink, localH2, h2, database, jdbc, gcs, batch, embedded]
timestamp: 2026-08-06T00:00:00Z
---

# LocalH2 Sink Module

Sink Module for building an embedded [H2](https://www.h2database.com/) database file from pipeline records and uploading it to Cloud Storage as a **zip archive**. The module:

1. Creates (or downloads and extracts, if `input` is set) an H2 database file in a local working directory on the worker.
2. Executes any user-supplied DDL statements (e.g. `CREATE TABLE`, `CREATE INDEX`).
3. Writes every input record into its configured table using batched JDBC prepared statements (`INSERT`, `INSERT_OR_UPDATE`, `INSERT_OR_DONOTHING`, or `DELETE`).
4. Zips the database directory and uploads it to the GCS path given by `output` (single shard per destination).

Multiple pipeline inputs can be written into the same database, each mapped to its own table via the `configs` list. The resulting zip is a portable, self-contained database — useful for distributing lookup data to applications or for use as an embedded read-only store.

The registered module name is `localH2` (use `module: localH2` in configs).

Note: the module emits a placeholder output collection; it is not intended for downstream consumption (use `waits` on this step if you need to sequence later steps after the upload).

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `localH2`                                                   |
| inputs     | required | Array<String\>      | Names of the steps whose records are written into the database. Each input listed here should have a matching entry in `parameters.configs`. |
| waits      | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## LocalH2 sink module parameters

| parameter     | optional | type                                | description                                                                                                                                                  |
|---------------|----------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| output        | required | String                              | GCS path (`gs://...`) to upload the zipped H2 database file to.                                                                                              |
| database      | required | String                              | Database name. Used as the H2 database file name inside the working directory and the archive.                                                               |
| configs       | required | Array<[Config](#config-parameters)\> | Per-input table write settings. At least one entry is required. See [Config parameters](#config-parameters).                                                 |
| input         | optional | String                              | GCS path (`gs://...`) of a zip archive containing an initial H2 database. If it exists, it is downloaded and extracted before writing, so records are applied on top of the existing database. If the object does not exist, a warning is logged and an empty database is created. |
| batchSize     | optional | Integer                             | Number of records to buffer before executing a JDBC batch and committing. Default: `1000`.                                                                   |
| groupFields   | optional | Array<String\>                      | Input field names used to group records into **separate database zip files** (dynamic destinations, one zip per distinct key). Default: empty (all records go into one database file). |
| tempDirectory | optional | String                              | Temporary directory (GCS path) used by the file write for staging.                                                                                            |

### Config parameters

Each entry in `configs` maps one pipeline input to a table in the database.

| parameter | optional | type           | description                                                                                                                                   |
|-----------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| input     | required | String         | Name of the input step whose records this config applies to. Must match one of the module's `inputs`.                                          |
| table     | optional | String         | Destination table name. Default: same as `input`.                                                                                              |
| ddls      | optional | Array<String\> | SQL DDL statements executed when the database is opened (e.g. `CREATE TABLE IF NOT EXISTS ...`, `CREATE INDEX ...`). Default: none — if omitted, the table must already exist (e.g. come from the `input` seed database). |
| keyFields | optional | Array<String\> | Key field names used to build the statement for update/delete operations (e.g. primary key columns for `INSERT_OR_UPDATE` and `DELETE`).       |
| op        | optional | Enum           | Write operation. Values: `INSERT`, `INSERT_OR_UPDATE`, `INSERT_OR_DONOTHING`, `DELETE`. Default: `INSERT`.                                     |

## Behavior notes

- The prepared statement for each table is generated from the input's Avro schema, so input field names/types should match the table columns created by `ddls`.
- Writes are committed per batch (`batchSize` records). A JDBC error aborts the bundle.
- With `groupFields`, the group key (concatenated field values) selects the destination, producing one zip per group; the `output` file naming supports templating of the destination key.
- The upload happens on `flush` (end of bundle/window): the database connection is closed, the whole local database directory is zipped and streamed to `output`.

## Examples

### Example 1: Build a lookup database from BigQuery and upload to GCS

```yaml
sources:
  - name: users
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email FROM `myproject.mydataset.users`"

sinks:
  - name: h2
    module: localH2
    inputs:
      - users
    parameters:
      output: gs://my-bucket/databases/users.zip
      database: userdb
      configs:
        - input: users
          table: users
          ddls:
            - "CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, name VARCHAR, email VARCHAR)"
```

### Example 2: Update an existing database with two inputs (upsert)

Download a previously built database, upsert records from two inputs into their own tables, and upload the updated database.

```yaml
sinks:
  - name: h2_update
    module: localH2
    inputs:
      - users
      - items
    parameters:
      input: gs://my-bucket/databases/master.zip
      output: gs://my-bucket/databases/master.zip
      database: masterdb
      batchSize: 500
      configs:
        - input: users
          table: users
          op: INSERT_OR_UPDATE
          keyFields:
            - user_id
        - input: items
          table: items
          op: INSERT_OR_UPDATE
          keyFields:
            - item_id
          ddls:
            - "CREATE TABLE IF NOT EXISTS items (item_id BIGINT PRIMARY KEY, title VARCHAR, price DOUBLE)"
```
