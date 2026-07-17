package com.mercari.solution.util.pipeline.lookup.source;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.cloud.google.DatastoreUtil;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
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

/**
 * Cloud Datastore-backed {@link LookupSource}: exposes Datastore kinds as
 * key-driven lookup tables. Datastore is schemaless, so each table declares its
 * value columns in configuration (like the REST source); the single key column
 * is the entity key's <em>name</em> (string, default) or numeric <em>id</em>
 * ({@code keyType: int64}). A lookup turns the batch's distinct point keys into
 * one {@code lookup} RPC (batch key get — never a query, never a scan); missing
 * entities simply yield no row (use a LEFT JOIN for a default).
 *
 * <p>Only point equality on the key column is supported — Datastore has no
 * ordered multi-column primary key to prefix-scan, so range/prefix requests are
 * rejected at runtime with an explanatory error.
 */
public class DatastoreLookupSource extends LookupSource {

    /** One exposed table = one Datastore kind. */
    public static class TableConfig implements Serializable {

        private final String name;
        private final String kind;
        private final String keyField;
        private final boolean numericKey;   // key = entity id (int64) instead of name (string)
        private final Schema schema;        // key column first, then declared value columns

        private TableConfig(String name, String kind, String keyField,
                boolean numericKey, List<Schema.Field> fields) {
            this.name = name;
            this.kind = kind;
            this.keyField = keyField;
            this.numericKey = numericKey;
            final Schema.Builder builder = Schema.builder();
            builder.withField(keyField,
                    numericKey ? Schema.FieldType.INT64 : Schema.FieldType.STRING);
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
        private String kind;
        private String keyField = "__name__";
        private boolean numericKey = false;
        private final List<Schema.Field> fields = new ArrayList<>();

        public TableBuilder withName(String name) {
            this.name = name;
            return this;
        }

        /** Datastore kind (defaults to the table name). */
        public TableBuilder withKind(String kind) {
            this.kind = kind;
            return this;
        }

        /** Column name the entity key is surfaced as (default {@code __name__}). */
        public TableBuilder withKeyField(String keyField) {
            this.keyField = keyField;
            return this;
        }

        /** Key is the entity's numeric id (INT64) instead of its name (STRING). */
        public TableBuilder withNumericKey(boolean numericKey) {
            this.numericKey = numericKey;
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
                throw new IllegalArgumentException("datastore table requires name");
            }
            for (final Schema.Field field : fields) {
                checkDecodeSupport(field.getFieldType());
            }
            return new TableConfig(name, kind == null ? name : kind, keyField,
                    numericKey, List.copyOf(fields));
        }
    }

    public static TableBuilder table() {
        return new TableBuilder();
    }

    private final String projectId;
    private final String databaseId;
    private final String namespace;
    private final String emulatorHost;
    private final Map<String, TableConfig> tables;

    private transient Datastore client;

    private DatastoreLookupSource(Builder builder) {
        super(builder.name);
        this.projectId = Objects.requireNonNull(builder.projectId, "projectId must not be null");
        this.databaseId = builder.databaseId;
        this.namespace = builder.namespace;
        this.emulatorHost = builder.emulatorHost;
        final Map<String, TableConfig> map = new LinkedHashMap<>();
        for (final TableConfig table : builder.tables) {
            map.put(table.name, table);
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException(
                    "datastore lookup source requires at least one table");
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
        private String namespace;
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

        public Builder withNamespace(String namespace) {
            this.namespace = namespace;
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

        public DatastoreLookupSource build() {
            return new DatastoreLookupSource(this);
        }
    }

    @Override
    protected void setupInternal() {
        if (client == null) {
            final String host = emulatorHost != null
                    ? emulatorHost : DatastoreUtil.getEmulatorHost();
            if (host != null) {
                this.client = DatastoreFactory.get().create(new DatastoreOptions.Builder()
                        .projectId(projectId)
                        .localHost(host.contains("://")
                                ? host.substring(host.indexOf("://") + 3) : host)
                        .initializer(new RetryHttpRequestInitializer())
                        .build());
            } else {
                try {
                    this.client = DatastoreUtil.getDatastore(
                            projectId, GcpCredentialsCache.credentials());
                } catch (IOException e) {
                    throw new IllegalStateException(
                            "failed to create datastore client for lookup source: "
                                    + getName(), e);
                }
            }
        }
    }

    @Override
    protected void closeInternal() {
        // The Datastore v1 client holds no closeable resources of its own.
        client = null;
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
        final Set<Key> keys = new LinkedHashSet<>();
        for (final LookupRequest request : batch.requests()) {
            if (request.isRange()) {
                throw new IllegalStateException("Datastore table '" + getName() + "." + table
                        + "' supports point equality on the key column only"
                        + " (no range or prefix conditions)");
            }
            final Object keyValue = request.prefix().get(0);
            if (keyValue == null) {
                continue; // SQL null equality never matches
            }
            keys.add(buildKey(config, keyValue));
        }
        if (keys.isEmpty()) {
            return List.of();
        }
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(config.schema.countFields());
        final List<Object[]> rows = new ArrayList<>();
        List<Key> remaining = new ArrayList<>(keys);
        try {
            while (!remaining.isEmpty()) {
                final com.google.datastore.v1.LookupRequest.Builder request =
                        com.google.datastore.v1.LookupRequest.newBuilder()
                                .addAllKeys(remaining);
                if (databaseId != null) {
                    request.setDatabaseId(databaseId);
                }
                final LookupResponse response = client.lookup(request.build());
                for (final EntityResult found : response.getFoundList()) {
                    rows.add(decodeRow(config, found.getEntity(), outCols));
                }
                remaining = response.getDeferredList();
            }
        } catch (DatastoreException e) {
            throw new IllegalStateException("Datastore lookup of table '" + getName() + "."
                    + table + "' (kind: " + config.kind + ") failed: " + e.getMessage(), e);
        }
        return rows;
    }

    private Key buildKey(TableConfig config, Object keyValue) {
        final Key.Builder key = Key.newBuilder();
        if (namespace != null || databaseId != null) {
            final PartitionId.Builder partition = PartitionId.newBuilder()
                    .setProjectId(projectId);
            if (databaseId != null) {
                partition.setDatabaseId(databaseId);
            }
            if (namespace != null) {
                partition.setNamespaceId(namespace);
            }
            key.setPartitionId(partition);
        }
        final Key.PathElement.Builder path = Key.PathElement.newBuilder().setKind(config.kind);
        if (config.numericKey) {
            path.setId(((Number) keyValue).longValue());
        } else {
            path.setName(keyValue.toString());
        }
        return key.addPath(path).build();
    }

    /**
     * Decodes one entity into a row in {@code outCols} order. The key column is
     * filled from the entity key (so it joins back exactly); value columns are
     * read from the entity properties by field name.
     */
    private static Object[] decodeRow(TableConfig config, Entity entity, int[] outCols) {
        final Object[] row = new Object[outCols.length];
        for (int i = 0; i < outCols.length; i++) {
            final int col = outCols[i];
            if (col == 0) {
                final Key.PathElement leaf =
                        entity.getKey().getPath(entity.getKey().getPathCount() - 1);
                row[i] = config.numericKey ? leaf.getId() : leaf.getName();
            } else {
                final Schema.Field field = config.schema.getField(col);
                row[i] = decodeValue(
                        entity.getPropertiesMap().get(field.getName()), field.getFieldType());
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
                    "Unsupported datastore column type: " + fieldType.getType()
                            + " (use 'json' for embedded entities)");
        }
    }

    /** Datastore {@link Value} → Calcite-internal value by declared field type. */
    static Object decodeValue(Value value, Schema.FieldType fieldType) {
        if (value == null || value.getValueTypeCase() == Value.ValueTypeCase.NULL_VALUE
                || value.getValueTypeCase() == Value.ValueTypeCase.VALUETYPE_NOT_SET) {
            return null;
        }
        return switch (fieldType.getType()) {
            case string -> switch (value.getValueTypeCase()) {
                case STRING_VALUE -> value.getStringValue();
                case INTEGER_VALUE -> String.valueOf(value.getIntegerValue());
                case DOUBLE_VALUE -> String.valueOf(value.getDoubleValue());
                case BOOLEAN_VALUE -> String.valueOf(value.getBooleanValue());
                case TIMESTAMP_VALUE -> toInstant(value).toString();
                case KEY_VALUE -> keyToString(value.getKeyValue());
                default -> throw unsupported(value, fieldType);
            };
            case json -> toJsonElement(value).toString();
            case bool -> value.getBooleanValue();
            case int8, int16, int32 -> switch (value.getValueTypeCase()) {
                case INTEGER_VALUE -> (int) value.getIntegerValue();
                case DOUBLE_VALUE -> (int) value.getDoubleValue();
                default -> throw unsupported(value, fieldType);
            };
            case int64 -> switch (value.getValueTypeCase()) {
                case INTEGER_VALUE -> value.getIntegerValue();
                case DOUBLE_VALUE -> (long) value.getDoubleValue();
                default -> throw unsupported(value, fieldType);
            };
            case float32, float64 -> switch (value.getValueTypeCase()) {
                case DOUBLE_VALUE -> value.getDoubleValue();
                case INTEGER_VALUE -> (double) value.getIntegerValue();
                default -> throw unsupported(value, fieldType);
            };
            case decimal -> switch (value.getValueTypeCase()) {
                case STRING_VALUE -> new BigDecimal(value.getStringValue());
                case INTEGER_VALUE -> BigDecimal.valueOf(value.getIntegerValue());
                case DOUBLE_VALUE -> BigDecimal.valueOf(value.getDoubleValue());
                default -> throw unsupported(value, fieldType);
            };
            case date -> switch (value.getValueTypeCase()) {
                // Datastore has no DATE type: an ISO-8601 string or a timestamp (at UTC).
                case STRING_VALUE -> (int) LocalDate.parse(value.getStringValue()).toEpochDay();
                case TIMESTAMP_VALUE ->
                        (int) toInstant(value).atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay();
                default -> throw unsupported(value, fieldType);
            };
            case timestamp -> switch (value.getValueTypeCase()) {
                case TIMESTAMP_VALUE -> toInstant(value).toEpochMilli();
                case INTEGER_VALUE -> value.getIntegerValue(); // taken as epoch millis
                case STRING_VALUE -> Instant.parse(value.getStringValue()).toEpochMilli();
                default -> throw unsupported(value, fieldType);
            };
            case bytes -> new ByteString(value.getBlobValue().toByteArray());
            case array -> {
                if (value.getValueTypeCase() != Value.ValueTypeCase.ARRAY_VALUE) {
                    throw unsupported(value, fieldType);
                }
                final List<Object> list = new ArrayList<>();
                for (final Value element : value.getArrayValue().getValuesList()) {
                    list.add(decodeValue(element, fieldType.getArrayValueType()));
                }
                yield list;
            }
            default -> throw new IllegalStateException(
                    "Unsupported datastore column type: " + fieldType.getType());
        };
    }

    /** Datastore {@link Value} → JSON, for {@code json}-typed columns. */
    private static com.google.gson.JsonElement toJsonElement(Value value) {
        return switch (value.getValueTypeCase()) {
            case NULL_VALUE, VALUETYPE_NOT_SET -> JsonNull.INSTANCE;
            case STRING_VALUE -> new JsonPrimitive(value.getStringValue());
            case INTEGER_VALUE -> new JsonPrimitive(value.getIntegerValue());
            case DOUBLE_VALUE -> new JsonPrimitive(value.getDoubleValue());
            case BOOLEAN_VALUE -> new JsonPrimitive(value.getBooleanValue());
            case TIMESTAMP_VALUE -> new JsonPrimitive(toInstant(value).toString());
            case KEY_VALUE -> new JsonPrimitive(keyToString(value.getKeyValue()));
            case BLOB_VALUE -> new JsonPrimitive(
                    Base64.getEncoder().encodeToString(value.getBlobValue().toByteArray()));
            case GEO_POINT_VALUE -> {
                final JsonObject point = new JsonObject();
                point.addProperty("latitude", value.getGeoPointValue().getLatitude());
                point.addProperty("longitude", value.getGeoPointValue().getLongitude());
                yield point;
            }
            case ARRAY_VALUE -> {
                final JsonArray array = new JsonArray();
                for (final Value element : value.getArrayValue().getValuesList()) {
                    array.add(toJsonElement(element));
                }
                yield array;
            }
            case ENTITY_VALUE -> {
                final JsonObject object = new JsonObject();
                for (final Map.Entry<String, Value> property :
                        value.getEntityValue().getPropertiesMap().entrySet()) {
                    object.add(property.getKey(), toJsonElement(property.getValue()));
                }
                yield object;
            }
        };
    }

    private static Instant toInstant(Value value) {
        final com.google.protobuf.Timestamp timestamp = value.getTimestampValue();
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    /** A key reference as its leaf name or id (ancestors are not modeled). */
    private static String keyToString(Key key) {
        final Key.PathElement leaf = key.getPath(key.getPathCount() - 1);
        return leaf.getName().isEmpty() ? String.valueOf(leaf.getId()) : leaf.getName();
    }

    private static IllegalStateException unsupported(Value value, Schema.FieldType fieldType) {
        return new IllegalStateException("Cannot decode datastore value of type "
                + value.getValueTypeCase() + " as " + fieldType.getType());
    }
}
