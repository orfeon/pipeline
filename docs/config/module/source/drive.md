# Google Drive Source Module

Source Module for loading file information by specifying queries or files into Google Drive.

## Source module common parameters

| parameter  | optional | type                | description                                                                     |
|------------|----------|---------------------|---------------------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                               |
| module     | required | String              | Specified `drive`                                                               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                                     |

## Drive source module parameters

Specify queries to retrieve files from Drive, or specify file IDs directly.
(You can specify both.)

| parameter | optional           | type          | description                                                                                                                                                  |
|-----------|--------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| queries   | selective required | Array<Query\> | Specify [query](https://developers.google.com/drive/api/v3/search-files#query_string_examples) list for Google Drive.                                        |
| files     | selective required | Array<File\>  | Specify File ids for Google Drive.                                                                                                                           |
| user      | optional           | String        | Specify service account to access Google Drive. If not specified, the worker's service account will be used.                                                 |
| fields    | optional           | String        | Specify the [fields](https://developers.google.com/drive/api/v3/reference/files) you want to retrieve. Nesting field is not supported, default is as follows |
| export    | optional           | String        | Specify the [export mime type](https://developers.google.com/workspace/drive/api/guides/ref-export-formats) you want to retrieve.                            |
| content   | optional           | Boolean       | Specify true if you want to retrieve the file content. The default is false.                                                                                 |
| flatten   | optional           | String        | Specify the array field name when expanding and un-nesting the values of that array field                                                                    |

* Default fields: `files(id,driveId,name,size,description,version,originalFilename,kind,mimeType,fileExtension,parents,createdTime,modifiedTime)`

## Query parameters

| parameter | optional | type    | description                                                                                                |
|-----------|----------|---------|------------------------------------------------------------------------------------------------------------|
| query     | required | String  | Specify the [Query](https://developers.google.com/drive/api/v3/search-files#query_string_examples) string. |
| folderId  | optional | String  | Specify the folder ID if you want to search under the folder.                                              |
| driveId   | optional | String  | Specify ID of the shared drive to search.                                                                  |
| recursive | optional | Boolean | Specify if you want to get files under the folder recursively. Default is true.                            |

## File parameters

| parameter | optional | type           | description                                                                                                                                                     |
|-----------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id        | required | String         | Specify the file ID of the file to retrieve.                                                                                                                    |
| ranges    | optional | Array<String\> | (For Google Sheets only) Specify the [ranges text](https://developers.google.com/workspace/sheets/api/guides/concepts?hl=ja#cell) to retrieve within the sheet. |

## Preparation

### Enable API

* [Google Drive API](https://console.developers.google.com/apis/library/drive.googleapis.com)
* [IAM Service Account Credentials API](https://console.developers.google.com/apis/library/iamcredentials.googleapis.com)

### Assign role

The drive source module uses [impersonating service accounts](https://cloud.google.com/iam/docs/impersonating-service-accounts).

The following roles must be assigned to dataflow worker service accounts to impersonate a drive user service account

* Service Account User (roles/iam.serviceAccountUser)
* Service Account Token Creator (roles/iam.serviceAccountTokenCreator)

In addition to that, the service account specified in `user` must also be granted read permission to the Drive to be retrieved by the `query`.

## Related example config files

* [Google Drive Sheets to Cloud Storage Files](../../../../examples/drive-to-files.yaml)
