# TiDB Source Module

Source module for loading records from TiDB (or MySQL compatible) databases.

The table is split into chunks and read in parallel by distributed workers, using TiDB specific features inspired by the [dumpling](https://docs.pingcap.com/tidb/stable/dumpling-overview) tool:

* **TiKV region based split (strategy A)**: `TABLESAMPLE REGIONS()` returns the boundary key of each TiKV region (default 96MB units), and each region becomes a chunk. Because chunks follow the physical data distribution, read requests are spread across TiKV nodes and the whole cluster's I/O bandwidth is used. Requires TiDB v5.0+.
* **Numeric MIN/MAX split (strategy B, fallback)**: when region split is unavailable (non-TiDB MySQL, older TiDB) and the split key is an integer column, the key range is divided evenly based on the estimated row count.
* **Whole table (strategy C, fallback)**: when no splittable key exists, the table is read with a single query.
* **Snapshot read**: a consistent TSO is acquired once and every worker sets `tidb_snapshot` to it, so all workers read the same MVCC version without taking any lock (no impact on concurrent writes).
* **`_tidb_rowid`**: used as the split key for tables that have no explicit primary key.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `tidb` |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## TiDB source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| url | required | String | JDBC connection url such as `jdbc:mysql://{host}:4000/{database}`. (TiDB speaks the MySQL protocol) |
| user | required | String | User name to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`. |
| password | optional | String | User password to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`. |
| table | required | String | Table name for reading data. May be schema qualified like `mydb.mytable`. |
| select | optional | String | The text to be inserted into the SELECT clause to specify the columns to be retrieved. The default is `*`. |
| where | optional | String | The condition text to be inserted into the WHERE clause to filter records. |
| splitField | optional | String | The column used to split the table into chunks. The default is a single column primary key, or the implicit `_tidb_rowid` when the table has no usable primary key. |
| splitSize | optional | Integer | The approximate number of records in one chunk for the numeric MIN/MAX split (strategy B). The default is 1000000. (the region split does not use this value) |
| useSnapshot | optional | Boolean | Specify true to read from a consistent `tidb_snapshot`. The default is true. |
| fetchSize | optional | Integer | JDBC fetch size. The default is `Integer.MIN_VALUE` which enables the MySQL connector row-by-row streaming mode to avoid buffering the whole result on the worker. |

## Notes

* `TABLESAMPLE REGIONS()` requires TiDB v5.0 or later. On other databases the module automatically falls back to the numeric MIN/MAX split or a whole table read.
* GC SafePoint registration (which dumpling performs through the PD client) is out of scope of this module because it is not reachable from the JDBC connection. For long running reads, raise the cluster's `tidb_gc_life_time` so that GC does not collect the snapshot version while the read is in progress.

## Example config file

```yaml
sources:
  - name: tidbInput
    module: tidb
    parameters:
      url: jdbc:mysql://localhost:4000/mydatabase
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      table: mydatabase.mytable
      select: "id,name,created_at"
      where: "created_at >= '2024-01-01'"
      useSnapshot: true
```
