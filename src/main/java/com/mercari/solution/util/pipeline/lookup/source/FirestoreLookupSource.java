package com.mercari.solution.util.pipeline.lookup.source;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldMask;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.GeoPoint;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.cloud.google.FirestoreUtil;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Cloud Firestore-backed {@link LookupSource}: exposes Firestore collections as
 * key-driven lookup tables. Firestore is schemaless, so each table declares its
 * value columns in configuration (like the REST source); the single key column
 * is the document id (always a string). A lookup turns the batch's distinct
 * point keys into one {@code getAll} batch read restricted to the projected
 * fields — never a query, never a scan; missing documents simply yield no row
 * (use a LEFT JOIN for a default).
 *
 * <p>Only point equality on the key column is supported — a Firestore document
 * id is an opaque single key, so range/prefix requests are rejected at runtime
 * with an explanatory error. The collection may be a slash-separated path to a
 * subcollection (e.g. {@code users/u1/orders}).
 */
public class FirestoreLookupSource extends LookupSource {

    /** One exposed table = one Firestore collection. */
    public static class TableConfig implements Serializable {

        private final String name;
        private final String collection;
        private final String keyField;
        private final Schema schema;   // key column first, then declared value columns

        private TableConfig(String name, String collection, String keyField,
                List<Schema.Field> fields) {
            this.name = name;
            this.collection = collection;
            this.keyField = keyField;
            final Schema.Builder builder = Schema.builder();
            builder.withField(keyField, Schema.FieldType.STRING);
            for (final Schema.Field field : fields) {
                if (!field.getName().equals(keyField)) {
                    builder.withField(field);
                }
            }
            this.schema = builder.build();
        }

        public static TableBuilder builder() {
            return new TableBuilder();
        }
    }

    public static class TableBuilder {

        private String name;
        private String collection;
        private String keyField = FirestoreUtil.NAME_FIELD;
        private final List<Schema.Field> fields = new ArrayList<>();

        public TableBuilder withName(String name) {
            this.name = name;
            return this;
        }

        /** Collection path, possibly a subcollection (defaults to the table name). */
        public TableBuilder withCollection(String collection) {
            this.collection = collection;
            return this;
        }

        /** Column name the document id is surfaced as (default {@code __name__}). */
        public TableBuilder withKeyField(String keyField) {
            this.keyField = keyField;
            return this;
        }

        public TableBuilder withField(String name, Schema.FieldType fieldType) {
            this.fields.add(Schema.Field.of(name, fieldType));
            return this;
        }

        public TableBuilder withFields(List<Schema.Field> fields) {
            this.fields.addAll(fields);
            return this;
        }

        public TableConfig build() {
            if (name == null) {
                throw new IllegalArgumentException("firestore table requires name");
            }
            for (final Schema.Field field : fields) {
                checkDecodeSupport(field.getFieldType());
            }
            return new TableConfig(name, collection == null ? name : collection,
                    keyField, List.copyOf(fields));
        }
    }

    public static TableBuilder table() {
        return new TableBuilder();
    }

    private final String projectId;
    private final String databaseId;
    private final String emulatorHost;
    private final Map<String, TableConfig> tables;

    private transient Firestore client;

    private FirestoreLookupSource(Builder builder) {
        super(builder.name);
        this.projectId = Objects.requireNonNull(builder.projectId, "projectId must not be null");
        this.databaseId = builder.databaseId == null
                ? FirestoreUtil.DEFAULT_DATABASE_NAME : builder.databaseId;
        this.emulatorHost = builder.emulatorHost;
        final Map<String, TableConfig> map = new LinkedHashMap<>();
        for (final TableConfig table : builder.tables) {
            map.put(table.name, table);
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException(
                    "firestore lookup source requires at least one table");
        }
        this.tables = map;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private String projectId;
        private String databaseId;
        private String emulatorHost;
        private final List<TableConfig> tables = new ArrayList<>();

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder withDatabaseId(String databaseId) {
            this.databaseId = databaseId;
            return this;
        }

        public Builder withEmulatorHost(String emulatorHost) {
            this.emulatorHost = emulatorHost;
            return this;
        }

        public Builder withTable(TableConfig table) {
            this.tables.add(table);
            return this;
        }

        public FirestoreLookupSource build() {
            return new FirestoreLookupSource(this);
        }
    }

    @Override
    protected void setupInternal() {
        if (client == null) {
            final FirestoreOptions.Builder options = FirestoreOptions.newBuilder()
                    .setProjectId(projectId)
                    .setDatabaseId(databaseId);
            if (emulatorHost != null) {
                // setEmulatorHost switches the channel to plaintext with
                // emulator credentials; no real GCP credentials are required.
                options.setEmulatorHost(emulatorHost);
            } else {
                try {
                    options.setCredentials(GcpCredentialsCache.credentials());
                } catch (IOException e) {
                    throw new IllegalStateException(
                            "failed to resolve credentials for firestore lookup source: "
                                    + getName(), e);
                }
            }
            this.client = options.build().getService();
        }
    }

    @Override
    protected void closeInternal() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new IllegalStateException(
                        "failed to close firestore client of lookup source: " + getName(), e);
            } finally {
                client = null;
            }
        }
    }

    @Override
    public Map<String, Schema> tableSchemas() {
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        for (final TableConfig table : tables.values()) {
            schemas.put(table.name, table.schema);
        }
        return schemas;
    }

    @Override
    public List<LookupKey> keyCandidates(String table) {
        final TableConfig config = tables.get(table);
        return config == null
                ? List.of() : List.of(LookupKey.primaryKey(List.of(config.keyField)));
    }

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        final TableConfig config = tables.get(table);
        if (config == null) {
            throw new IllegalStateException("unknown lookup table: " + getName() + "." + table);
        }
        final Set<String> ids = new LinkedHashSet<>();
        for (final LookupRequest request : batch.requests()) {
            if (request.isRange()) {
                throw new IllegalStateException("Firestore table '" + getName() + "." + table
                        + "' supports point equality on the key column only"
                        + " (no range or prefix conditions)");
            }
            final Object keyValue = request.prefix().get(0);
            if (keyValue == null) {
                continue; // SQL null equality never matches
            }
            ids.add(keyValue.toString());
        }
        if (ids.isEmpty()) {
            return List.of();
        }
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(config.schema.countFields());
        final DocumentReference[] references = ids.stream()
                .map(id -> client.collection(config.collection).document(id))
                .toArray(DocumentReference[]::new);
        final List<DocumentSnapshot> snapshots;
        try {
            final FieldMask fieldMask = fieldMask(config, outCols);
            snapshots = (fieldMask == null
                    ? client.getAll(references)
                    : client.getAll(references, fieldMask)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Firestore lookup of table '" + getName() + "."
                    + table + "' was interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Firestore lookup of table '" + getName() + "."
                    + table + "' (collection: " + config.collection + ") failed: "
                    + e.getCause().getMessage(), e.getCause());
        }
        final List<Object[]> rows = new ArrayList<>();
        for (final DocumentSnapshot snapshot : snapshots) {
            if (snapshot.exists()) {
                rows.add(decodeRow(config, snapshot, outCols));
            }
        }
        return rows;
    }

    /**
     * The read mask restricted to the projected value columns, or {@code null}
     * when only the key column is needed (the document id comes with the
     * reference; an empty mask would read the whole document).
     */
    private static FieldMask fieldMask(TableConfig config, int[] outCols) {
        final List<String> fields = new ArrayList<>();
        for (final int col : outCols) {
            if (col != 0) {
                fields.add(config.schema.getField(col).getName());
            }
        }
        return fields.isEmpty() ? null : FieldMask.of(fields.toArray(String[]::new));
    }

    /**
     * Decodes one document into a row in {@code outCols} order. The key column
     * is filled from the document id (so it joins back exactly); value columns
     * are read from the document data by field name.
     */
    private static Object[] decodeRow(TableConfig config, DocumentSnapshot snapshot,
            int[] outCols) {
        final Object[] row = new Object[outCols.length];
        for (int i = 0; i < outCols.length; i++) {
            final int col = outCols[i];
            if (col == 0) {
                row[i] = snapshot.getId();
            } else {
                final Schema.Field field = config.schema.getField(col);
                row[i] = decodeValue(snapshot.get(field.getName()), field.getFieldType());
            }
        }
        return row;
    }

    private static void checkDecodeSupport(Schema.FieldType fieldType) {
        switch (fieldType.getType()) {
            case string, json, bool, int8, int16, int32, int64, float32, float64,
                 decimal, date, timestamp, bytes -> {
            }
            case array -> checkDecodeSupport(fieldType.getArrayValueType());
            default -> throw new IllegalArgumentException(
                    "Unsupported firestore column type: " + fieldType.getType()
                            + " (use 'json' for nested maps)");
        }
    }

    /** Firestore document value → Calcite-internal value by declared field type. */
    static Object decodeValue(Object value, Schema.FieldType fieldType) {
        if (value == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case string -> switch (value) {
                case String s -> s;
                case Number n -> String.valueOf(n);
                case Boolean b -> String.valueOf(b);
                case Timestamp t -> toInstant(t).toString();
                case DocumentReference r -> r.getId();
                default -> throw unsupported(value, fieldType);
            };
            case json -> toJsonElement(value).toString();
            case bool -> switch (value) {
                case Boolean b -> b;
                default -> throw unsupported(value, fieldType);
            };
            case int8, int16, int32 -> switch (value) {
                case Number n -> n.intValue();
                default -> throw unsupported(value, fieldType);
            };
            case int64 -> switch (value) {
                case Number n -> n.longValue();
                default -> throw unsupported(value, fieldType);
            };
            case float32, float64 -> switch (value) {
                case Number n -> n.doubleValue();
                default -> throw unsupported(value, fieldType);
            };
            case decimal -> switch (value) {
                case String s -> new BigDecimal(s);
                case Long l -> BigDecimal.valueOf(l);
                case Number n -> BigDecimal.valueOf(n.doubleValue());
                default -> throw unsupported(value, fieldType);
            };
            case date -> switch (value) {
                // Firestore has no DATE type: an ISO-8601 string or a timestamp (at UTC).
                case String s -> (int) LocalDate.parse(s).toEpochDay();
                case Timestamp t ->
                        (int) toInstant(t).atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay();
                default -> throw unsupported(value, fieldType);
            };
            case timestamp -> switch (value) {
                case Timestamp t -> toInstant(t).toEpochMilli();
                case Long l -> l; // taken as epoch millis
                case String s -> Instant.parse(s).toEpochMilli();
                default -> throw unsupported(value, fieldType);
            };
            case bytes -> switch (value) {
                case Blob b -> new ByteString(b.toBytes());
                default -> throw unsupported(value, fieldType);
            };
            case array -> {
                if (!(value instanceof List<?> list)) {
                    throw unsupported(value, fieldType);
                }
                final List<Object> out = new ArrayList<>(list.size());
                for (final Object element : list) {
                    out.add(decodeValue(element, fieldType.getArrayValueType()));
                }
                yield out;
            }
            default -> throw new IllegalStateException(
                    "Unsupported firestore column type: " + fieldType.getType());
        };
    }

    /** Firestore document value → JSON, for {@code json}-typed columns. */
    private static JsonElement toJsonElement(Object value) {
        return switch (value) {
            case null -> JsonNull.INSTANCE;
            case String s -> new JsonPrimitive(s);
            case Number n -> new JsonPrimitive(n);
            case Boolean b -> new JsonPrimitive(b);
            case Timestamp t -> new JsonPrimitive(toInstant(t).toString());
            case Blob b -> new JsonPrimitive(
                    Base64.getEncoder().encodeToString(b.toBytes()));
            case GeoPoint p -> {
                final JsonObject point = new JsonObject();
                point.addProperty("latitude", p.getLatitude());
                point.addProperty("longitude", p.getLongitude());
                yield point;
            }
            case DocumentReference r -> new JsonPrimitive(r.getPath());
            case List<?> list -> {
                final JsonArray array = new JsonArray();
                for (final Object element : list) {
                    array.add(toJsonElement(element));
                }
                yield array;
            }
            case Map<?, ?> map -> {
                final JsonObject object = new JsonObject();
                for (final Map.Entry<?, ?> entry : map.entrySet()) {
                    object.add(String.valueOf(entry.getKey()), toJsonElement(entry.getValue()));
                }
                yield object;
            }
            default -> new JsonPrimitive(String.valueOf(value));
        };
    }

    private static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static IllegalStateException unsupported(Object value, Schema.FieldType fieldType) {
        return new IllegalStateException("Cannot decode firestore value of type "
                + value.getClass().getName() + " as " + fieldType.getType());
    }
}
