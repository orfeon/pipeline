---
type: Source Module
title: Drive Source Module
description: Retrieves file metadata and content from Google Drive. Supports searching files by query, specifying file IDs directly, reading Google Sheets content, and exporting Google Workspace files to various formats.
tags: [source, drive, batch, google, sheets, docs, workspace]
timestamp: 2026-06-21T14:30:00Z
---

# Google Drive Source Module

Source Module for retrieving file metadata and content from Google Drive using the [Drive API v3](https://developers.google.com/drive/api/v3/reference/files). Supports two retrieval methods: searching files with [query expressions](https://developers.google.com/drive/api/v3/search-files#query_string_examples), or specifying file IDs directly. Both can be used together.

For Google Sheets files, the module can additionally read the spreadsheet content (cell data). For any Google Workspace file, it can export the file to a specified MIME type (e.g. PDF).

## Source module common parameters

| parameter | optional | type                | description                                                           |
|-----------|----------|---------------------|-----------------------------------------------------------------------|
| name      | required | String              | Step name. specified to be unique in config file.                     |
| module    | required | String              | Specified `drive`                                                     |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Drive source module parameters

Specify `queries` to search files on Drive, or `files` to retrieve specific file IDs. You can specify both.

| parameter     | optional           | type                              | description                                                                                                                                                                                                                        |
|---------------|--------------------|------------------------------------|--------------------------------------------------------------------------------------------------------------|
| queries       | selective required | Array<[Query](#query-parameters)\> | List of query conditions to search Google Drive.                                                              |
| files         | selective required | Array<[File](#file-parameters)\>   | List of specific file IDs to retrieve.                                                                        |
| user          | optional           | String                             | Service account email to access Google Drive via impersonation. If not specified, the worker's default service account is used. |
| fields        | optional           | String                             | Drive API [fields](https://developers.google.com/drive/api/v3/reference/files) to retrieve. Must be in `files(field1,field2,...)` format. `*` is not supported. See [Default fields](#default-fields). |
| export        | optional           | String                             | [Export MIME type](https://developers.google.com/workspace/drive/api/guides/ref-export-formats) for Google Workspace files (e.g. `application/pdf`). The exported binary is available in the `export.body` field. |
| content       | optional           | Boolean                            | If `true`, reads the file content. For Google Sheets, reads the spreadsheet data into the `spreadsheet` field. Default: `false`. |
| contentFormat | optional           | Enum                               | Format for the content when `content` is `true`. Values: `none`, `csv`, `json`. Default: `none`.              |
| filter        | optional           | Filter                             | Filter condition to apply to retrieved file metadata. Uses the standard [Filter](../common/filter.md) syntax. |
| flatten       | optional           | String                             | Array field name to unnest/expand into multiple rows (e.g. `spreadsheet.sheets`).                             |

### Default fields

When `fields` is not specified, the following default fields are retrieved:

```
files(id,driveId,name,size,description,version,originalFilename,kind,mimeType,fileExtension,parents,createdTime,modifiedTime)
```

### Available field types

Fields retrieved from Drive are mapped to the following types:

| category  | fields                                                                                                          | type           |
|-----------|-----------------------------------------------------------------------------------------------------------------|----------------|
| STRING    | id, driveId, name, description, originalFilename, kind, mimeType, fileExtension, fullFileExtension, resourceKey, webContentLink, webViewLink, iconLink, thumbnailLink, folderColorRgb, md5Checksum, headRevisionId | STRING |
| BOOLEAN   | starred, trashed, explicitlyTrashed, viewedByMe, shared, ownedByMe, viewerCanCopyContent, writerCanShare, isAppAuthorized, hasThumbnail, modifiedByMe, hasAugmentedPermissions | BOOLEAN |
| INT64     | size, version, quotaBytesUsed, thumbnailVersion                                                                 | INT64          |
| TIMESTAMP | createdTime, modifiedTime, viewedByMeTime, modifiedByMeTime, sharedWithMeTime, trashedTime                     | TIMESTAMP      |
| STRING[]  | parents, spaces, permissionIds                                                                                  | Array<STRING\> |
| User      | trashingUser, sharingUser, lastModifyingUser                                                                    | Element (User) |
| User[]    | owners                                                                                                          | Array<Element (User)\> |

**User schema:**

| field        | type    |
|--------------|---------|
| kind         | STRING  |
| displayName  | STRING  |
| photoLink    | STRING  |
| me           | BOOLEAN |
| permissionId | STRING  |
| emailAddress | STRING  |

## Query parameters

| parameter | optional | type    | description                                                                                                                                |
|-----------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------|
| query     | required | String  | [Query string](https://developers.google.com/drive/api/v3/search-files#query_string_examples) for searching Drive files.                   |
| folderId  | optional | String  | Folder ID to restrict the search scope.                                                                                                    |
| driveId   | optional | String  | Shared drive ID to search within.                                                                                                          |
| recursive | optional | Boolean | Whether to search recursively under the folder. Default: `true`.                                                                           |

## File parameters

| parameter | optional | type           | description                                                                                                                                                                 |
|-----------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id        | required | String         | The file ID of the Google Drive file.                                                                                                                                       |
| ranges    | optional | Array<String\> | (Google Sheets only) [A1 notation ranges](https://developers.google.com/sheets/api/guides/concepts#cell) to retrieve (e.g. `Sheet1!A1:D10`). Relevant only when `content: true`. |

## Output schema

The output schema includes the Drive file metadata fields (determined by `fields`) plus additional content fields:

| field        | type    | description                                                                                                 |
|--------------|---------|-------------------------------------------------------------------------------------------------------------|
| *(file fields)* | *(various)* | File metadata fields as specified by the `fields` parameter. See [Available field types](#available-field-types). |
| export       | Element | Present when `export` is specified. Contains `mimeType` (STRING) and `body` (BYTES).                        |
| spreadsheet  | Element | Present for Google Sheets files when `content: true`. Contains spreadsheet metadata and sheet content.       |
| document     | Element | Present for Google Docs files. Contains document metadata.                                                   |
| presentation | Element | Present for Google Slides files. Contains presentation metadata.                                             |
| form         | Element | Present for Google Forms files. Contains form metadata.                                                      |

### Spreadsheet schema (when content is read)

| field          | type    | description                              |
|----------------|---------|------------------------------------------|
| spreadsheetId  | STRING  | The spreadsheet ID.                      |
| spreadsheetUrl | STRING  | URL to the spreadsheet.                  |
| title          | STRING  | Title of the spreadsheet.                |
| locale         | STRING  | Locale of the spreadsheet.               |
| timeZone       | STRING  | Time zone of the spreadsheet.            |
| sheets         | Array<Sheet\> | Array of sheet data.               |

**Sheet schema:**

| field       | type    | description                                                                     |
|-------------|---------|---------------------------------------------------------------------------------|
| title       | STRING  | Sheet tab name.                                                                 |
| index       | INT32   | Sheet index position.                                                           |
| sheetId     | INT32   | Unique sheet ID.                                                                |
| sheetType   | STRING  | Sheet type (e.g. `GRID`).                                                       |
| hidden      | BOOLEAN | Whether the sheet is hidden.                                                    |
| content     | STRING  | Sheet cell data (format depends on `contentFormat`).                            |
| rowCount    | INT32   | Number of rows in the sheet.                                                    |
| columnCount | INT32   | Number of columns in the sheet.                                                 |

## Preparation

### Enable APIs

- [Google Drive API](https://console.developers.google.com/apis/library/drive.googleapis.com)
- [Google Sheets API](https://console.developers.google.com/apis/library/sheets.googleapis.com) (if reading Sheets content)
- [IAM Service Account Credentials API](https://console.developers.google.com/apis/library/iamcredentials.googleapis.com)

### Assign roles

The Drive source module uses [service account impersonation](https://cloud.google.com/iam/docs/impersonating-service-accounts). The following roles must be assigned to the Dataflow worker service account to impersonate the Drive user service account:

- Service Account User (`roles/iam.serviceAccountUser`)
- Service Account Token Creator (`roles/iam.serviceAccountTokenCreator`)

The service account specified in `user` must also be granted read permission to the Drive files being accessed.

## Examples

### Example 1: Search files by query

Search for all PDF files in a specific folder.

```yaml
sources:
  - name: pdf_files
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      queries:
        - query: "mimeType = 'application/pdf'"
          folderId: "1ABCxyz_folder_id"
          recursive: true
```

### Example 2: Retrieve specific files by ID

```yaml
sources:
  - name: specific_files
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      files:
        - id: "1ABCxyz_file_id_1"
        - id: "2DEFxyz_file_id_2"
```

### Example 3: Read Google Sheets content

Read specific ranges from a Google Sheets file and flatten sheets into individual rows.

```yaml
sources:
  - name: sheets_data
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      files:
        - id: "1ABCxyz_spreadsheet_id"
          ranges:
            - "Sheet1!A1:G"
            - "Sheet2!A1:D"
      content: true
      flatten: spreadsheet.sheets
```

Each sheet produces a separate output row with the sheet's `content`, `title`, `sheetId`, etc.

### Example 4: Export Google Workspace file as PDF

Export a Google Docs document to PDF format.

```yaml
sources:
  - name: exported_docs
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      files:
        - id: "1ABCxyz_document_id"
      export: "application/pdf"
```

The exported PDF binary is available in the `export.body` field.

### Example 5: Sheets to CSV files on GCS

Read Google Sheets content and write each sheet as a CSV file to Cloud Storage.

```yaml
sources:
  - name: sheets_source
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      files:
        - id: spreadsheet-file-id
          ranges:
            - "Sheet1!A3:G"
      content: true
      flatten: spreadsheet.sheets

sinks:
  - name: csv_export
    module: files
    inputs:
      - sheets_source
    parameters:
      output: "gs://my-bucket/drive/${id}/${spreadsheet.sheets.sheetId}.csv"
      content:
        format: csv
        field: spreadsheet.sheets.content
      attributes:
        contentType: "text/csv"
```

### Example 6: Search shared drive with custom fields

```yaml
sources:
  - name: shared_drive_files
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      fields: "files(id,name,mimeType,size,createdTime,modifiedTime,webViewLink,md5Checksum)"
      queries:
        - query: "modifiedTime > '2024-01-01T00:00:00'"
          driveId: "0ABCxyz_shared_drive_id"
```

### Example 7: Combine query and file ID retrieval

```yaml
sources:
  - name: mixed_source
    module: drive
    parameters:
      user: my-sa@developer.gserviceaccount.com
      queries:
        - query: "name contains 'report' and mimeType = 'application/vnd.google-apps.spreadsheet'"
          folderId: "folder_id"
      files:
        - id: "specific_file_id"
          ranges:
            - "Summary!A1:Z100"
      content: true
```
