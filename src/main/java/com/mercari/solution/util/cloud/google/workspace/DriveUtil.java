package com.mercari.solution.util.cloud.google.workspace;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.User;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import com.mercari.solution.util.cloud.google.IAMUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DriveUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DriveUtil.class);

    private static final Pattern PATTERN_FIELDS = Pattern.compile("[a-z]+\\([a-zA-Z,]+\\)");
    private static final Pattern PATTERN_CONDITION = Pattern.compile("(.+?)(s*>=s*|s*<=s*|s*=s*|s*>s*|s*<s*)(.+)");
    private static final Pattern PATTERN_DRIVE_FIELDS = Pattern.compile("files\\(([^)]+)\\)");

    private static final String MIMETYPE_APPS_PREFIX = "application/vnd.google-apps.";
    public static final String MIMETYPE_APPS_FILE = MIMETYPE_APPS_PREFIX + "file";
    public static final String MIMETYPE_APPS_FOLDER = MIMETYPE_APPS_PREFIX + "folder";
    public static final String MIMETYPE_APPS_DOCS = MIMETYPE_APPS_PREFIX + "document";
    public static final String MIMETYPE_APPS_SHEETS = MIMETYPE_APPS_PREFIX + "spreadsheet";
    public static final String MIMETYPE_APPS_FORMS = MIMETYPE_APPS_PREFIX + "form";
    public static final String MIMETYPE_APPS_SLIDES = MIMETYPE_APPS_PREFIX + "presentation";

    public static final String DEFAULT_FIELDS_ = "id,driveId,name,size,description,version,originalFilename,kind,mimeType,fileExtension,parents,createdTime,modifiedTime";
    public static final String DEFAULT_FIELDS = String.format("files(%s),nextPageToken", DEFAULT_FIELDS_);

    private static final Map<String, Drive> drives = new HashMap<>();

    public enum ContentFormat {
        none,
        csv,
        json
    }

    public static GoogleCredentials credentialsWithScopes(String targetPrincipalAccount, String[] args) {
        if(args.length == 0) {
            args = new String[]{DriveScopes.DRIVE_READONLY};
        }

        try {
            GoogleCredentials credentials = GcpCredentialsCache.credentials();
            final String account = IAMUtil.getAccount();
            if (account != null && targetPrincipalAccount != null && !account.equals(targetPrincipalAccount)) {
                credentials = ImpersonatedCredentials.create(
                        credentials,
                        targetPrincipalAccount,
                        null,
                        Arrays.asList(args),
                        3600);
            } else if (IAMUtil.AccountType.MACHINE_MANAGED.equals(IAMUtil.accountType(credentials))) {
                // Because the credentials obtained via `GoogleCredentials.getApplicationDefault()` cannot have the `Drive` scope added later,
                // we have changed the code to always use `Impersonate`.
                credentials = ImpersonatedCredentials.create(
                        credentials,
                        targetPrincipalAccount,
                        null,
                        Arrays.asList(args),
                        3600);
            } else {
                credentials = credentials.createScoped(args);
            }
            return credentials;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Drive drive(final String targetPrincipalAccount, String... args) {
        if(args.length == 0) {
            args = new String[]{DriveScopes.DRIVE_READONLY};
        }

        final GoogleCredentials credentials = credentialsWithScopes(targetPrincipalAccount, args);
        final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                new HttpCredentialsAdapter(credentials),
                // Do not log 404. It clutters the output and is possibly even required by the caller.
                new RetryHttpRequestInitializer(ImmutableList.of(404)));
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        return new Drive.Builder(transport, jsonFactory, initializer)
                .setApplicationName("mercari-pipeline")
                .build();
    }

    public static boolean isFolder(final File file) {
        return MIMETYPE_APPS_FOLDER.equals(file.getMimeType());
    }

    public static boolean isFieldsBound(final String fields) {
        final Matcher matcher = PATTERN_DRIVE_FIELDS.matcher(fields);
        return matcher.find();
    }

    public static String extractFields(final String fields) {
        final Matcher matcher = PATTERN_DRIVE_FIELDS.matcher(fields);
        if(matcher.find()) {
            return matcher.group(1);
        }
        return fields;
    }

    public static File get(final Drive drive, final String fileId, final String fields) throws IOException {
        return drive.files()
                .get(fileId)
                .setFields(fields)
                .setSupportsAllDrives(true)
                .setSupportsTeamDrives(true)
                .execute();
    }

    public static List<File> get(final Drive drive, final Set<String> fileIds, final String fields) throws IOException {
        return get(drive, new ArrayList<>(fileIds), fields);
    }

    public static List<File> get(final Drive drive, final List<String> fileIds, final String fields) throws IOException {
        final List<File> files = new ArrayList<>();
        final List<String> failures = new ArrayList<>();
        final JsonBatchCallback<File> callback = new JsonBatchCallback<>() {
            @Override
            public void onSuccess(File file, HttpHeaders headers) {
                LOG.info("success: {}", file);
                files.add(file);
            }

            @Override
            public void onFailure(GoogleJsonError error, HttpHeaders headers) {
                LOG.error("failed error: {} headers: {}", error, headers);
                failures.add(error.getMessage());
            }
        };

        int batchSize = 100;
        for (int i = 0; i < fileIds.size(); i += batchSize) {
            int end = Math.min(i + batchSize, fileIds.size());
            final List<String> chunk = fileIds.subList(i, end);

            final BatchRequest batchRequest = drive.batch();
            for (final String fileId : chunk) {
                drive.files().get(fileId)
                        .setFields(fields)
                        .setSupportsAllDrives(true)
                        .setSupportsTeamDrives(true)
                        .queue(batchRequest, callback);
            }
            batchRequest.execute();
        }

        return files;
    }

    public static List<File> query(
            final Drive drive,
            final String query,
            final String driveId,
            final String parentFolderId,
            final List<String> path,
            final String fields,
            final boolean recursive) throws IOException {

        final List<File> resultFiles = new ArrayList<>();

        final String q;
        if(parentFolderId == null) {
            q = query;
        } else {
            q = "'" + parentFolderId + "' in parents and ((" + query + ") or mimeType='" + DriveUtil.MIMETYPE_APPS_FOLDER + "')";
        }

        Drive.Files.List request = drive.files().list()
                .setPageSize(1000)
                .setQ(q)
                .setFields(fields);

        if(driveId != null) {
            request = request.setDriveId(driveId);
        }

        String pageToken = null;
        do {
            if (pageToken != null) {
                request.setPageToken(pageToken);
            }

            FileList fileList = request.execute();

            for(final File file : fileList.getFiles()) {
                if(DriveUtil.isFolder(file)) {
                    if(recursive) {
                        final List<String> childPath = new ArrayList<>(path);
                        childPath.add(file.getName());
                        final List<File> results = query(drive, query, driveId, file.getId(), childPath, fields,true);
                        resultFiles.addAll(results);
                    }
                } else {
                    resultFiles.add(file);
                }
            }

            pageToken = fileList.getNextPageToken();
        } while(pageToken != null);

        return resultFiles;
    }

    public static void copy(final Drive drive, final String sourceFileId, final String destinationFileId, final Map<String, Object> attributes) throws IOException {
        final File file;
        if(attributes == null || attributes.isEmpty()) {
            file = drive.files().get(sourceFileId).execute();
            file.setId(null);
        } else {
            file = new File();
            for(final Map.Entry<String, Object> entry : attributes.entrySet()) {
                file.set(entry.getKey(), entry.getValue());
            }
        }
        file.setParents(Arrays.asList(destinationFileId));
        drive.files().copy(sourceFileId, file).execute();
    }

    public static byte[] download(final Drive drive, final String fileId) {
        try (final InputStream is = drive.files().get(fileId).executeMediaAsInputStream()) {
            return is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void downloadTo(final Drive drive, final String fileId, final OutputStream os) {
        try {
            drive.files().get(fileId).executeMediaAndDownloadTo(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] export(final Drive drive, final String fileId, final String mimeType) {
        try (final InputStream is = drive.files().export(fileId, mimeType).executeMediaAsInputStream()) {
            return is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void exportTo(final Drive drive, final String fileId, final String mimeType, final OutputStream os) {
        try {
            drive.files().export(fileId, mimeType).executeMediaAndDownloadTo(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createFile(final Drive drive, final File file, final byte[] bytes) {
        final ByteArrayContent content = new ByteArrayContent(file.getMimeType(), bytes);
        try {
            drive.files().create(file, content).execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createFile(final Drive drive, final File file, final InputStream is) {
        final InputStreamContent content = new InputStreamContent(file.getMimeType(), is);
        try {
            drive.files().create(file, content).execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static com.mercari.solution.module.Schema createFileSchema(final String fields) {
        final com.mercari.solution.module.Schema.Builder builder = com.mercari.solution.module.Schema.builder();

        final String fields_ = fields.trim().replaceAll(" ", "");
        final Matcher matcher = PATTERN_FIELDS.matcher(fields_);

        if(matcher.find()) {
            final String group = matcher.group();
            final int start = group.indexOf("(");
            final int end = group.lastIndexOf(")");
            if(start > 0 && end > 0) {
                final String fields__ = fields_.substring(start + 1, end);
                for(final String field : fields__.split(",")) {
                    builder.withField(field, convertFieldType(field));
                }
            }
        } else {
            throw new IllegalArgumentException("Failed to create schema from fields: " + fields_);
        }

        return builder.build();
    }

    public static Map<String, Object> convertPrimitives(final com.mercari.solution.module.Schema schema, final File file) {
        final Map<String, Object> values = new HashMap<>();
        for(final com.mercari.solution.module.Schema.Field field : schema.getFields()) {
            final Object value = switch (field.getName()) {
                // STRING
                case "id", "driveId", "name", "description", "originalFilename", "kind", "mimeType", "fileExtension",
                     "fullFileExtension", "resourceKey", "webContentLink", "webViewLink", "iconLink", "thumbnailLink",
                     "folderColorRgb", "md5Checksum", "headRevisionId", "nextPageToken",
                     // BOOLEAN
                     "starred", "trashed", "explicitlyTrashed", "viewedByMe", "shared", "ownedByMe", "viewerCanCopyContent",
                     "writerCanShare", "isAppAuthorized", "hasThumbnail", "modifiedByMe", "hasAugmentedPermissions",
                     // INT64
                     "size", "version", "quotaBytesUsed", "thumbnailVersion",
                     // STRING ARRAY
                     "parents", "spaces", "permissionIds" ->  file.get(field.getName());
                // DATETIME
                case "createdTime" -> file.getCreatedTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getCreatedTime().toStringRfc3339());
                case "modifiedTime" -> file.getModifiedTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getModifiedTime().toStringRfc3339());
                case "viewedByMeTime" -> file.getViewedByMeTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getViewedByMeTime().toStringRfc3339());
                case "modifiedByMeTime" -> file.getModifiedByMeTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getModifiedByMeTime().toStringRfc3339());
                case "sharedWithMeTime" -> file.getSharedWithMeTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getSharedWithMeTime().toStringRfc3339());
                case "trashedTime" -> file.getTrashedTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getTrashedTime().toStringRfc3339());
                // User
                case "trashingUser" -> convertDriveUserMap(file.getTrashingUser());
                case "sharingUser" -> convertDriveUserMap(file.getSharingUser());
                case "lastModifyingUser" -> convertDriveUserMap(file.getLastModifyingUser());
                // User ARRAY
                case "owners" -> {
                    if(file.getOwners() == null) {
                        yield null;
                    } else {
                        final List<Map<String, Object>> owners = new ArrayList<>();
                        for(final User owner : file.getOwners()) {
                            if(owner != null) {
                                owners.add(convertDriveUserMap(owner));
                            }
                        }
                        yield owners;
                    }
                }
                default -> throw new IllegalArgumentException();
            };
            values.put(field.getName(), value);
        }
        return values;
    }

    private static com.mercari.solution.module.Schema.FieldType convertFieldType(final String field) {
        return switch (field) {
            case "id", "driveId", "name", "description", "originalFilename", "kind", "mimeType", "fileExtension",
                 "fullFileExtension", "resourceKey", "webContentLink", "webViewLink", "iconLink", "thumbnailLink",
                 "folderColorRgb", "md5Checksum", "headRevisionId", "nextPageToken"
                    -> com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true);
            case "starred", "trashed", "explicitlyTrashed", "viewedByMe",
                 "shared", "ownedByMe", "viewerCanCopyContent", "writerCanShare",
                 "isAppAuthorized", "hasThumbnail", "modifiedByMe", "hasAugmentedPermissions"
                    -> com.mercari.solution.module.Schema.FieldType.BOOLEAN.withNullable(true);
            case "size", "version", "quotaBytesUsed", "thumbnailVersion"
                    -> com.mercari.solution.module.Schema.FieldType.INT64.withNullable(true);
            case "createdTime", "modifiedTime", "viewedByMeTime", "modifiedByMeTime", "sharedWithMeTime", "trashedTime"
                    -> com.mercari.solution.module.Schema.FieldType.TIMESTAMP.withNullable(true);
            case "parents", "spaces", "permissionIds"
                    -> com.mercari.solution.module.Schema.FieldType.array(com.mercari.solution.module.Schema.FieldType.STRING).withNullable(true);
            case "trashingUser", "sharingUser", "lastModifyingUser"
                -> com.mercari.solution.module.Schema.FieldType.element(createDriveUserSchema2()).withNullable(true);
            case "owners" -> com.mercari.solution.module.Schema.FieldType.array(com.mercari.solution.module.Schema.FieldType.element(createDriveUserSchema2())).withNullable(true);
            default -> throw new IllegalStateException("Not supported field: " + field);
        };
    }

    private static Schema createDriveUserSchema() {
        return Schema.builder()
                .addField("kind", Schema.FieldType.STRING.withNullable(true))
                .addField("displayName", Schema.FieldType.STRING.withNullable(true))
                .addField("photoLink", Schema.FieldType.STRING.withNullable(true))
                .addField("me", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("permissionId", Schema.FieldType.STRING.withNullable(true))
                .addField("emailAddress", Schema.FieldType.STRING.withNullable(true))
                .build();
    }

    private static com.mercari.solution.module.Schema createDriveUserSchema2() {
        return com.mercari.solution.module.Schema.builder()
                .withField("kind", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("displayName", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("photoLink", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("me", com.mercari.solution.module.Schema.FieldType.BOOLEAN.withNullable(true))
                .withField("permissionId", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("emailAddress", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .build();
    }


    private static org.apache.avro.Schema createDriveUserAvroSchema() {
        return SchemaBuilder
                .record("user")
                .namespace("")
                .fields()
                .name("kind").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("displayName").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("photoLink").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("me").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("permissionId").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("emailAddress").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .endRecord();
    }

    private static Map<String,Object> convertDriveUserMap(final User user) {
        if(user == null) {
            return null;
        }
        final Map<String, Object> values = new HashMap<>();
        values.put("kind", user.getKind());
        values.put("displayName", user.getDisplayName());
        values.put("photoLink", user.getPhotoLink());
        values.put("me", user.getMe());
        values.put("permissionId", user.getPermissionId());
        values.put("emailAddress", user.getEmailAddress());
        return values;
    }

    private static Row convertDriveUser(final User user) {
        if(user == null) {
            return null;
        }
        return Row.withSchema(createDriveUserSchema())
                .withFieldValue("kind", user.getKind())
                .withFieldValue("displayName", user.getDisplayName())
                .withFieldValue("photoLink", user.getPhotoLink())
                .withFieldValue("me", user.getMe())
                .withFieldValue("permissionId", user.getPermissionId())
                .withFieldValue("emailAddress", user.getEmailAddress())
                .build();
    }

    private static GenericRecord convertDriveUserRecord(final User user) {
        if(user == null) {
            return null;
        }
        return new GenericRecordBuilder(createDriveUserAvroSchema())
                .set("kind", user.getKind())
                .set("displayName", user.getDisplayName())
                .set("photoLink", user.getPhotoLink())
                .set("me", user.getMe())
                .set("permissionId", user.getPermissionId())
                .set("emailAddress", user.getEmailAddress())
                .build();
    }

    public synchronized static Drive getOrCreateDrive(final String name, final String account) {
        return getOrCreateDrive(drives, name, account, List.of(DriveScopes.DRIVE_READONLY));
    }

    public synchronized static Drive getOrCreateDrive(
            final Map<String, Drive> drives,
            final String name,
            final String account) {

        return getOrCreateDrive(drives, name, account, List.of(DriveScopes.DRIVE_READONLY));
    }

    public synchronized static Drive getOrCreateDrive(
            final Map<String, Drive> drives,
            final String name,
            final String account,
            final List<String> scopes) {

        if(drives.containsKey(name)) {
            final Drive drive = drives.get(name);
            if(drive != null) {
                return drive;
            }
        }
        createDrive(drives, name, account, scopes);
        return drives.get(name);
    }

    public synchronized static void createDrive(
            final Map<String, Drive> drives,
            final String name,
            final String account,
            final List<String> scopes) {

        if (drives.containsKey(name)) {
            return;
        }

        final String[] args = scopes.toArray(new String[scopes.size()]);
        final Drive drive = DriveUtil.drive(account, args);
        drives.put(name, drive);
    }

}
