# Bigtable Source Module (Experimental)

Source Module for loading data by specifying filter conditions from Cloud Bigtable.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                                                                   |
|--------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                                                             |
| module             | required | String              | Specified `bigtable`                                                                                                                                                          |
| schema             | -        | [Schema](SCHEMA.md) | Schema of the data to be read. bigtable module does not require specification                                                                                                 |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp or Date type)                                         |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters.                                                                                                                                  |

## Bigtable source module parameters

| parameter    | optional           | type                              | description                                                                                                                                                                                                                                                                                                  |
|--------------|--------------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId    | required           | String                            | Cloud Bigtable's GCP project ID that you want to read                                                                                                                                                                                                                                                        |
| instanceId   | required           | String                            | The instance ID of the Cloud Bigtable you want to read                                                                                                                                                                                                                                                       |
| tableId      | required           | String                            | The table name of the Cloud Bigtable you want to read                                                                                                                                                                                                                                                        |
| keyRange     | selective required | [KeyRange](../common/bigtable.md) | Specify key range conditions for queries from request to Bigtable                                                                                                                                                                                                                                            |
| filter       | selective required | [Filter](../common/bigtable.md)   | Specify filter conditions for query from request to Bigtable                                                                                                                                                                                                                                                 |
| columns      | required           | Array<Column\>                    | Define the output schema by defining the mapping of each column to its type and feed name                                                                                                                                                                                                                    |
| format       | optional           | [Enum](../common/bigtable.md)     | Specify the cell value serialization format.　One of `bytes`, `avro` or `text`. Used as default value if not specified in each `columns` parameter. The default is `bytes`.                                                                                                                                   |
| outputType   | optional           | Enum                              | Specify the output type.　One of `row` or `cell`. `row` output for each row using the schema specified in `columns`. `cell` output for each cell using a fixed common schema. The default is 'row'                                                                                                            |
| cellType     | optional           | Enum                              | Specify cellType that defines how to retrieve the multiple cells associated with a column qualifier. Used as default value if not specified in each `columns` parameter. One of `last`, `first` or `all`. The default is `first`. If `all` is specified, it will be an array of the type specified in `type` |
| appProfileId | optional           | String                            | Specify the app profile id                                                                                                                                                                                                                                                                                   |

### Column parameters

Specify the writing cell settings for each column family.
If the parameters `format`,`cellType` are not specified, the upper-level setting is applied as default.

| parameter  | optional | type              | description                                                                                                                                 |
|------------|----------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| family     | required | String            | Specify the columnFamily name to be assigned to qualifiers                                                                                  |
| qualifiers | required | Array<Qualifier\> | Specify the columnQualifiers settings to be assigned to the columnFamily.                                                                   |
| format     | optional | Enum              | Specify the cell value serialization format. The default is parent `format` value                                                           |
| cellType   | optional | Enum              | Specify cellType that defines how to retrieve the multiple cells associated with a column qualifier. The default is parent `cellType` value |

### Qualifier parameters

Specify the settings for each column qualifier.
If the parameters `format`,`cellType` are not specified, the upper-level setting is applied as default.

| parameter | optional | type   | description                                                                                                                                 |
|-----------|----------|--------|---------------------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String | Specify columnQualifier name to be assigned to the field                                                                                    |
| field     | required | String | Specify the name of the field that holds the retrieved column qualifier cell value                                                          |
| type      | required | String | Specify the field type that holds the retrieved column qualifier cell value                                                                 |
| format    | optional | Enum   | Specify the cell value serialization format. The default is parent `format` value                                                           |
| cellType  | optional | Enum   | Specify cellType that defines how to retrieve the multiple cells associated with a column qualifier. The default is parent `cellType` value |


### Cell schema

If `cell` is specified for `outputType` parameter, each cell will be output using the following common schema.


| field     | type      | description                                                 |
|-----------|-----------|-------------------------------------------------------------|
| rowKey    | String    | The key of the row to which the cell belongs.               |
| family    | String    | The name of the column family to which the cell belongs     |
| qualifier | String    | The name of the column(qualifier) to which the cell belongs |
| value     | Bytes     | The Cell value                                              |
| timestamp | Timestamp | timestamp assigned to the cell                              |

## Example

### Example with row specified for outputType

```json
{
  "sources": [
    {
      "name": "BigtableInput",
      "module": "bigtable",
      "parameters": {
        "projectId": "example-project",
        "instanceId": "example-instance",
        "tableId": "example-table",
        "filter": {
          "type": "timestamp_range",
          "start": "2024-01-01T00:00:00Z",
          "end": "2024-12-31T23:59:59Z"
        },
        "columns": [
          {
            "family": "a",
            "qualifiers": [
              { "name": "iid", "field": "item_id", "type": "string" },
              { "name": "at", "field": "action_type", "type": "string" },
              { "name": "am", "field": "amount", "type": "int64" }
            ],
            "cellType": "last"
          },
          {
            "family": "b",
            "qualifiers": [
              { "name": "in", "field": "item_name" },
              { "name": "iu", "field": "image_url", "cellType": "all" }
            ],
            "cellType": "last"
          }
        ],
        "withRowKey": true,
        "rowKeyField": "row_key"
      }
    }
  ]
}
```

The above configuration retrieves records with the following schema.

```json
{
  "row_key": "1234567890#",
  "item_id": "xxx",
  "action_type": "buy",
  "amount": 3000,
  "item_name": "book",
  "image_url": [
    "https://xxx/yyy/001.png",
    "https://xxx/yyy/002.png",
    "https://xxx/yyy/003.png"
  ]
}
```

### Example with cell specified for outputType

```yaml
options:
  jobName: bigtable-to-bigquery
system:
  args:
    project: ${utils.gcp.project()}
sources:
  - name: bigtable_source
    module: bigtable
    parameters:
      projectId: ${args.project}
      instanceId: myinstance
      tableId: mytable
      appProfileId: myprofile
      outputType: cell
      flowControl: true
      filter:
        - type: family_name_regex
          regex: g # filter only cells in column family is g
        - type: column_qualifier_regex
          regex: column_string_a # filter column name is column_string_a
        - type: limit_cells_per_column
          limit: 1 # get only latest cell value
sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - bigtable_source
    parameters:
      table: "myproject.mydataset.mytable"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
```

The above configuration retrieves records with the following schema.

```json
{
  "rowKey": "1234567890#",
  "family": "g",
  "qualifier": "column_string_a",
  "value": "YQ==",
  "timestamp": "2025-08-01T12:34:56Z"
}
```

## Related example config files
