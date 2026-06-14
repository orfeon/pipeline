# TiDB Sink Module

Sink module to insert input records to a specified TiDB (or MySQL compatible) table.

The write method is selectable through the `mode` parameter, following the analysis of how TiDB Lightning techniques can be applied from Apache Beam:

| mode | method | characteristics |
| --- | --- | --- |
| `jdbc` (default) | JDBC batch INSERT (optionally UPSERT) | lowest latency, safe for online tables, row level conflict control |
| `loaddata` | `LOAD DATA LOCAL INFILE` streaming | higher throughput for medium batches |
| `importinto` | write Parquet to GCS then `IMPORT INTO ... FROM 'gs://...'` | highest bulk throughput (internally uses the Lightning physical import); the target table is taken offline during the import |

Records that fail to be written are emitted as a PCollection of failure records ([MFailure](../../../../src/main/java/com/mercari/solution/module/MFailure.java) schema), which can be wired to a failure sink.

## Sink module common parameters

| parameter  | optional  | type                | description                                       |
|------------|-----------|---------------------|---------------------------------------------------|
| name       | required  | String              | Step name. specified to be unique in config file. |
| module     | required  | String              | Specified `tidb`                                  |
| inputs     | required  | Array<String\>      | Step name whose data you want to write from       |
| parameters | required  | Map<String,Object\> | Specify the following individual parameters.      |

## TiDB sink module parameters

| parameter | optional | type | description |
|-----------|----------|------|-------------|
| url | required | String | JDBC connection url such as `jdbc:mysql://{host}:4000/{database}`. |
| user | required | String | User name to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`. |
| password | optional | String | User password. You can also specify a Secret Manager resource name. |
| table | required | String | Destination table name. May be schema qualified like `mydb.mytable`. |
| mode | optional | String | Write method: `jdbc` (default), `loaddata`, or `importinto`. |
| autoConfigureUrl | optional | Boolean | When true (default), the throughput related JDBC url parameters recommended for the selected `mode` are appended automatically if they are not already present in the `url`. |
| op | optional | String | `INSERT` (default), `INSERT_OR_UPDATE`, or `INSERT_OR_DONOTHING`. For `jdbc` mode it controls the INSERT statement (UPSERT / INSERT IGNORE). For `loaddata` mode it maps to `REPLACE` / `IGNORE`. Ignored for `importinto`. |
| batchSize | optional | Integer | Number of rows per batch (`jdbc`) or per `LOAD DATA` call (`loaddata`). The default is 1000. |
| createTable | optional | Boolean | Specify true to create the table automatically if it does not exist. The default is false. |
| emptyTable | optional | Boolean | Specify true to delete all rows from the table before writing. The default is false. |
| keyFields | optional | Array<String\> | Primary key fields (used when `createTable` is true). |
| tempDirectory | optional | String | (`importinto` only) GCS directory to stage the Parquet files. If not specified, the pipeline `tempLocation` is used. |
| importThread | optional | Integer | (`importinto` only) the `thread` option passed to `IMPORT INTO ... WITH thread=N`. |

### Auto configured url parameters

When `autoConfigureUrl` is true, the following parameters are appended for each mode (only if missing):

| mode | appended parameters |
| --- | --- |
| `jdbc` | `rewriteBatchedStatements=true`, `cachePrepStmts=true`, `useServerPrepStmts=true`, `useConfigs=maxPerformance` |
| `loaddata` | `allowLoadLocalInfile=true` |
| `importinto` | (none) |

## Notes

* **mode importinto** writes Parquet files to `tempDirectory` (or `tempLocation`) on GCS and then runs `IMPORT INTO {table} FROM 'gs://.../*.parquet' FORMAT 'parquet'`. TiDB itself must be able to read that GCS path (configure GCS access on the TiDB nodes). It requires TiDB v7.5+ and takes the target table offline during the import, so it is not suitable for appending to an online table.
* **mode loaddata** requires `allowLoadLocalInfile=true` (added automatically by `autoConfigureUrl`) and uses the MySQL connector `setLocalInfileInputStream` API. NULLs are encoded as `\N`; timestamps are formatted as `yyyy-MM-dd HH:mm:ss.SSSSSS` in UTC.
* Detailed per-record failure handling will be implemented later; currently the failure records are only made available as a PCollection.

## Example config files

```yaml
# mode jdbc (default): streaming / online table safe
sinks:
  - name: tidbOutput
    module: tidb
    inputs:
      - myTransform
    parameters:
      url: jdbc:mysql://localhost:4000/mydatabase
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      table: mydatabase.mytable
      op: INSERT_OR_UPDATE
      batchSize: 1000
```

```yaml
# mode importinto: bulk load via Parquet on GCS
sinks:
  - name: tidbBulk
    module: tidb
    inputs:
      - myTransform
    parameters:
      url: jdbc:mysql://localhost:4000/mydatabase
      user: myuser
      password: mypassword
      table: mydatabase.mytable
      mode: importinto
      tempDirectory: gs://mybucket/tidb-staging/
      importThread: 16
```
