# Files Sink Module

Sink module for write files.

This module is used when creating one file per record, unlike the storage sink module, which writes multiple records to a single file.

## Sink module common parameters

| parameter  | optional | type                | description                                       |
|------------|----------|---------------------|---------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file. |
| module     | required | String              | Specified `files`                                 |
| inputs     | required | Array<String\>      | Step names whose data you want to write from.     |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.      |

## Files sink module parameters

| parameter  | optional | type                | description                                                                                                                                                                                                                                                        |
|------------|----------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| output     | required | String              | Specify the address of the file to be copied to. Can be assembled from record data using [Apache FreeMarker](https://freemarker.apache.org/). If destinationService is `drive`, specify the fileId that you want to be the parent folder of the file to be copied. |
| attributes | optional | Map<String,String\> | Specify the attribute name and value if you want to give attribute information such as `contentType` to the destination file. The value can be assembled from record data using [Apache FreeMarker](https://freemarker.apache.org/)                                |
| content    | optional | Content             | Specify the content to be written to the file.                                                                                                                                                                                                                     |

## Content parameters

Parameters specifying the content to be written to the file.
If neither `field` nor `text` is specified, the entire record will be written to the file.

| parameter | optional | type   | description                                                                                                                  |
|-----------|----------|--------|------------------------------------------------------------------------------------------------------------------------------|
| type      | optional | Enum   | Specifies the format of file. Supported `text`, `csv`, `json` and `avro`.                                                    |
| field     | optional | String | The value of the field specified here will be written to the file. If fields are nested, you can specify them using periods. |
| text      | optional | String | Specify text as the file content using an Apache FreeMarker template                                                         |

## Related example config files

* [Google Drive Sheets to Cloud Storage Files](../../../../examples/drive-to-files.yaml)
