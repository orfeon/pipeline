# Postgres Source Module

Source module for loading records from PostgreSQL (or PostgreSQL compatible) databases.

Unlike the `jdbc` source module, this module transfers data in `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` format using PostgreSQL CopyManager API for higher throughput.
The table is automatically split into physical block (`ctid`) ranges, and the ranges are read in parallel by distributed workers.
The number of blocks is obtained from `pg_relation_size` (the physical size of the table) and the block range is split mechanically, so no full scan or `OFFSET` is needed to plan the split.
Each range is read with an efficient TID range scan (`WHERE ctid >= '(start,0)' AND ctid < '(end,0)'`) so that a single query does not become huge.

> Note: TID range scans are supported in PostgreSQL 14 and later. On older versions each range may fall back to a sequential scan.
> Because `ctid` is the physical row location, rows that are inserted, updated (moved to another page), or vacuumed *while the read is running* may be read more than once or missed. Use against a table that is not being modified concurrently (or accept the snapshot skew typical of batch reads).

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `postgres` |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## Postgres source module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| url | required | String | JDBC connection url such as `jdbc:postgresql://{host}:{port}/{database}`. |
| user | conditional required | String | User name to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`. If this parameter is not specified, the worker's service account will be used as the [database user](https://cloud.google.com/sql/docs/postgres/iam-logins). In that case, specify `enableIamAuth=true` as a parameter in the `url`. |
| password | conditional required | String | User password to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`. No need to specify if the service account will be used as `user`. |
| table | required | String | Table name for reading data. |
| select | optional | String | The text to be inserted into the SELECT clause to specify the columns to be retrieved. The default is `*`. |
| where | optional | String | The condition text to be inserted into the WHERE clause to filter records. |
| splitSize | optional | Integer | The approximate number of records in one `ctid` range. The block count per range is derived from this and the estimated row density (`pg_class.reltuples` / block count). The default is 1000000. Run `ANALYZE` on the table beforehand for a more accurate split; when statistics are unavailable a conservative default density is used. |

* url examples
  * PostgreSQL for Cloud SQL
    * `jdbc:postgresql://google/mydatabase?cloudSqlInstance=myproject:us-central1:myinstance&socketFactory=com.google.cloud.sql.postgres.SocketFactory`
  * PostgreSQL for AlloyDB
    * `jdbc:postgresql:///mydatabase?alloydbInstanceName=projects/myproject/locations/us-central1/clusters/mycluster/instances/myinstance-primary&socketFactory=com.google.cloud.alloydb.SocketFactory`

## Supported column types

`boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `numeric`, `text`, `varchar`, `char`, `bytea`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `json`, `jsonb`

## Example config file

```yaml
sources:
  - name: postgresInput
    module: postgres
    parameters:
      url: jdbc:postgresql://localhost:5432/mydatabase
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      table: public.mytable
      select: "id,name,created_at"
      where: "created_at >= '2024-01-01'"
      splitSize: 1000000
```
