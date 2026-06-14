# Postgres Sink Module

Sink module to insert input records to a specified PostgreSQL (or PostgreSQL compatible) table.

Unlike the `jdbc` sink module, this module transfers data in `COPY ... FROM STDIN (FORMAT BINARY)` format using PostgreSQL CopyManager API for higher throughput.

## Sink module common parameters

| parameter  | optional  | type                | description                                       |
|------------|-----------|---------------------|---------------------------------------------------|
| name       | required  | String              | Step name. specified to be unique in config file. |
| module     | required  | String              | Specified `postgres`                              |
| inputs     | required  | Array<String\>      | Step name whose data you want to write from       |
| parameters | required  | Map<String,Object\> | Specify the following individual parameters.      |

## Postgres sink module parameters

| parameter   | optional | type           | description                                                                                                                                            |
|-------------|----------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| url         | required | String         | JDBC connection url such as `jdbc:postgresql://{host}:{port}/{database}`.                                                                              |
| user        | required | String         | User name to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`.     |
| password    | required | String         | User password to access the database. You can also specify a Secret Manager resource name like `projects/{myproj}/secrets/{mysecret}/versions/latest`. |
| table       | required | String         | Destination table name. Input fields are matched to the table columns by name, and columns missing in the input are written as NULL.                   |
| batchSize   | optional | Integer        | The number of records written in one COPY transaction. The default is 100000.                                                                          |
| createTable | optional | Boolean        | Specify true if you want to generate the table automatically if the destination table does not exist.                                                  |
| emptyTable  | optional | Boolean        | Specify true if you want to delete all data from the destination table before inserting data.                                                          |
| keyFields   | optional | Array<String\> | Specify the primary key fields. (used when `createTable` is true)                                                                                      |

* url examples
  * PostgreSQL for Cloud SQL
    * `jdbc:postgresql://google/mydatabase?cloudSqlInstance=myproject:us-central1:myinstance&socketFactory=com.google.cloud.sql.postgres.SocketFactory`
  * PostgreSQL for AlloyDB
    * `jdbc:postgresql:///mydatabase?alloydbInstanceName=projects/myproject/locations/us-central1/clusters/mycluster/instances/myinstance-primary&socketFactory=com.google.cloud.alloydb.SocketFactory`

## Supported column types

`boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`, `numeric`, `text`, `varchar`, `char`, `bytea`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `json`, `jsonb`

Note: COPY BINARY format requires that the type of the value sent matches the type of the destination column.
For example, an Avro `long` input field cannot be written to an `integer` column (the value is converted according to the destination column type, but precision loss or errors may occur for incompatible combinations).

## Example config file

```yaml
sinks:
  - name: postgresOutput
    module: postgres
    inputs:
      - myTransform
    parameters:
      url: jdbc:postgresql://localhost:5432/mydatabase
      user: myuser
      password: projects/myproject/secrets/mysecret/versions/latest
      table: public.mytable
      createTable: true
      keyFields:
        - id
```
