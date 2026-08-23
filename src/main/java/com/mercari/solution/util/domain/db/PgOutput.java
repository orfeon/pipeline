package com.mercari.solution.util.domain.db;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decoder for the PostgreSQL {@code pgoutput} logical replication plugin's binary message
 * format (protocol version 1, with the {@code binary} option so tuple values arrive in the
 * same binary representation as COPY BINARY fields and are decoded by
 * {@link PostgresUtil#decodeValue}).
 *
 * <p>The decoder is stateful: {@code Relation} messages register per-table column metadata
 * (with key-column flags), {@code Begin} opens a transaction scope (commit LSN and commit
 * timestamp are known up-front), and each Insert/Update/Delete message yields one
 * {@link ChangeEvent} with key/old/new values rendered as JSON object strings.</p>
 */
public class PgOutput {

    /** Change operation of a {@link ChangeEvent}. */
    public enum Op {
        INSERT,
        UPDATE,
        DELETE,
        TRUNCATE
    }

    /** One decoded row-change event, ready to be emitted into the pipeline. */
    public static class ChangeEvent implements Serializable {

        public final long lsn;
        public final long commitLsn;
        public final long commitTimestampMicros;
        public final long transactionId;
        public final long sequence;
        public final String schema;
        public final String table;
        public final String op;
        public final String keysJson;
        public final String oldValuesJson;
        public final String newValuesJson;
        // relation columns as a JSON array of {name, type, key} (unified cdc type names), or null
        public final String columnsJson;

        ChangeEvent(
                final long lsn,
                final long commitLsn,
                final long commitTimestampMicros,
                final long transactionId,
                final long sequence,
                final String schema,
                final String table,
                final Op op,
                final String keysJson,
                final String oldValuesJson,
                final String newValuesJson,
                final String columnsJson) {

            this.lsn = lsn;
            this.commitLsn = commitLsn;
            this.commitTimestampMicros = commitTimestampMicros;
            this.transactionId = transactionId;
            this.sequence = sequence;
            this.schema = schema;
            this.table = table;
            this.op = op.name();
            this.keysJson = keysJson;
            this.oldValuesJson = oldValuesJson;
            this.newValuesJson = newValuesJson;
            this.columnsJson = columnsJson;
        }
    }

    private static class RelationColumn {

        private final String name;
        private final boolean key;
        private final PostgresUtil.Column column;

        private RelationColumn(final String name, final boolean key, final PostgresUtil.Column column) {
            this.name = name;
            this.key = key;
            this.column = column;
        }
    }

    private static class Relation {

        private final String namespace;
        private final String name;
        private final List<RelationColumn> columns;

        private Relation(final String namespace, final String name, final List<RelationColumn> columns) {
            this.namespace = namespace;
            this.name = name;
            this.columns = columns;
        }
    }

    // pg_type oids of the supported scalar types (reverse of ColumnType.getOid)
    private static final Map<Integer, PostgresUtil.ColumnType> SCALAR_OIDS = createScalarOids();
    // pg_type oids of one-dimensional arrays of the supported scalar types
    private static final Map<Integer, PostgresUtil.ColumnType> ARRAY_OIDS = createArrayOids();

    private static Map<Integer, PostgresUtil.ColumnType> createScalarOids() {
        final Map<Integer, PostgresUtil.ColumnType> map = new HashMap<>();
        for(final PostgresUtil.ColumnType type : PostgresUtil.ColumnType.values()) {
            final int oid = type.getOid();
            if(oid > 0) {
                map.put(oid, type);
            }
        }
        map.put(19, PostgresUtil.ColumnType.TEXT);  // name
        map.put(26, PostgresUtil.ColumnType.INT4);  // oid
        return map;
    }

    private static Map<Integer, PostgresUtil.ColumnType> createArrayOids() {
        final Map<Integer, PostgresUtil.ColumnType> map = new HashMap<>();
        map.put(1000, PostgresUtil.ColumnType.BOOL);
        map.put(1005, PostgresUtil.ColumnType.INT2);
        map.put(1007, PostgresUtil.ColumnType.INT4);
        map.put(1016, PostgresUtil.ColumnType.INT8);
        map.put(1021, PostgresUtil.ColumnType.FLOAT4);
        map.put(1022, PostgresUtil.ColumnType.FLOAT8);
        map.put(1231, PostgresUtil.ColumnType.NUMERIC);
        map.put(1009, PostgresUtil.ColumnType.TEXT);
        map.put(1015, PostgresUtil.ColumnType.VARCHAR);
        map.put(1014, PostgresUtil.ColumnType.BPCHAR);
        map.put(1001, PostgresUtil.ColumnType.BYTEA);
        map.put(1182, PostgresUtil.ColumnType.DATE);
        map.put(1183, PostgresUtil.ColumnType.TIME);
        map.put(1270, PostgresUtil.ColumnType.TIMETZ);
        map.put(1115, PostgresUtil.ColumnType.TIMESTAMP);
        map.put(1185, PostgresUtil.ColumnType.TIMESTAMPTZ);
        map.put(2951, PostgresUtil.ColumnType.UUID);
        map.put(199, PostgresUtil.ColumnType.JSON);
        map.put(3807, PostgresUtil.ColumnType.JSONB);
        map.put(143, PostgresUtil.ColumnType.XML);
        map.put(1041, PostgresUtil.ColumnType.INET);
        map.put(651, PostgresUtil.ColumnType.CIDR);
        map.put(1040, PostgresUtil.ColumnType.MACADDR);
        map.put(775, PostgresUtil.ColumnType.MACADDR8);
        return map;
    }

    /**
     * Stateful message decoder. Not thread-safe: one instance per replication stream.
     *
     * <p>{@code keyColumns} maps {@code schema.table} to its primary key column names
     * (resolved from the catalog at pipeline launch). It pins {@code keysJson} to the primary
     * key even when the table's replica identity is FULL — the protocol's per-column key flags
     * then cover every column. Tables absent from the map (e.g. created after launch under a
     * FOR ALL TABLES publication) fall back to the replica-identity flags.</p>
     */
    public static class Decoder implements Serializable {

        private final Map<String, List<String>> keyColumns;
        private final Map<Integer, Relation> relations = new HashMap<>();

        public Decoder() {
            this(new HashMap<>());
        }

        public Decoder(final Map<String, List<String>> keyColumns) {
            this.keyColumns = keyColumns;
        }

        // current transaction scope (set by Begin, cleared by Commit)
        private long txCommitLsn;
        private long txCommitTimestampMicros;
        private long txId;
        private long txSequence;

        /**
         * Decodes one pgoutput message (the XLogData payload as delivered by the pgjdbc
         * replication stream). Returns the row-change events the message yields: one for
         * Insert/Update/Delete, one per relation for Truncate, none for the bookkeeping
         * messages (Begin/Commit/Relation/Type/Origin).
         */
        public List<ChangeEvent> decode(final ByteBuffer buffer, final long lsn) {
            final List<ChangeEvent> events = new ArrayList<>();
            final char type = (char) buffer.get();
            switch (type) {
                case 'B' -> { // Begin: final(commit) LSN, commit timestamp, xid
                    this.txCommitLsn = buffer.getLong();
                    this.txCommitTimestampMicros = PostgresUtil.toUnixMicros(buffer.getLong());
                    this.txId = Integer.toUnsignedLong(buffer.getInt());
                    this.txSequence = 0L;
                }
                case 'C' -> { // Commit
                }
                case 'R' -> decodeRelation(buffer);
                case 'Y' -> { // Type: custom type metadata; unknown oids fall back to text labels
                }
                case 'O' -> { // Origin
                }
                case 'I' -> {
                    final Relation relation = getRelation(buffer.getInt());
                    expectTupleKind(buffer, 'N');
                    final TupleValue[] newTuple = readTupleData(buffer, relation);
                    events.add(createEvent(lsn, relation, Op.INSERT, null, newTuple));
                }
                case 'U' -> {
                    final Relation relation = getRelation(buffer.getInt());
                    TupleValue[] oldTuple = null;
                    char kind = (char) buffer.get();
                    if(kind == 'K' || kind == 'O') {
                        oldTuple = readTupleData(buffer, relation);
                        kind = (char) buffer.get();
                    }
                    if(kind != 'N') {
                        throw new IllegalStateException("Illegal pgoutput update tuple kind: " + kind);
                    }
                    final TupleValue[] newTuple = readTupleData(buffer, relation);
                    events.add(createEvent(lsn, relation, Op.UPDATE, oldTuple, newTuple));
                }
                case 'D' -> {
                    final Relation relation = getRelation(buffer.getInt());
                    final char kind = (char) buffer.get();
                    if(kind != 'K' && kind != 'O') {
                        throw new IllegalStateException("Illegal pgoutput delete tuple kind: " + kind);
                    }
                    final TupleValue[] oldTuple = readTupleData(buffer, relation);
                    events.add(createEvent(lsn, relation, Op.DELETE, oldTuple, null));
                }
                case 'T' -> {
                    final int count = buffer.getInt();
                    buffer.get(); // options (cascade, restart identity)
                    for(int i = 0; i < count; i++) {
                        final Relation relation = getRelation(buffer.getInt());
                        events.add(createEvent(lsn, relation, Op.TRUNCATE, null, null));
                    }
                }
                case 'M' -> { // logical decoding message (not requested)
                }
                default -> throw new IllegalStateException("Unsupported pgoutput message type: " + type);
            }
            return events;
        }

        private void decodeRelation(final ByteBuffer buffer) {
            final int relationId = buffer.getInt();
            final String namespace = readString(buffer);
            final String name = readString(buffer);
            buffer.get(); // replica identity setting
            final short columnCount = buffer.getShort();
            final List<RelationColumn> columns = new ArrayList<>();
            for(int i = 0; i < columnCount; i++) {
                final boolean key = (buffer.get() & 1) == 1;
                final String columnName = readString(buffer);
                final int typeOid = buffer.getInt();
                buffer.getInt(); // type modifier
                columns.add(new RelationColumn(columnName, key, createColumn(columnName, typeOid)));
            }
            relations.put(relationId, new Relation(namespace.isEmpty() ? "pg_catalog" : namespace, name, columns));
        }

        // Unknown oids (user-defined enum and domain types) are decoded as text labels,
        // matching the COPY BINARY reader's enum handling.
        private static PostgresUtil.Column createColumn(final String name, final int typeOid) {
            final PostgresUtil.ColumnType scalar = SCALAR_OIDS.get(typeOid);
            if(scalar != null) {
                return new PostgresUtil.Column(name, scalar);
            }
            final PostgresUtil.ColumnType element = ARRAY_OIDS.get(typeOid);
            if(element != null) {
                return PostgresUtil.Column.arrayOf(name, element);
            }
            return new PostgresUtil.Column(name, PostgresUtil.ColumnType.ENUM);
        }

        private Relation getRelation(final int relationId) {
            final Relation relation = relations.get(relationId);
            if(relation == null) {
                throw new IllegalStateException("pgoutput relation message was not received for relation id: " + relationId);
            }
            return relation;
        }

        private static void expectTupleKind(final ByteBuffer buffer, final char expected) {
            final char kind = (char) buffer.get();
            if(kind != expected) {
                throw new IllegalStateException("Illegal pgoutput tuple kind: " + kind + ", expected: " + expected);
            }
        }

        // TupleData: per column 'n' (null), 'u' (unchanged toast, value not sent),
        // 'b' (binary) or 't' (text) followed by the value bytes.
        // Returns one entry per relation column: null = SQL NULL, UNCHANGED = not sent.
        private static TupleValue[] readTupleData(final ByteBuffer buffer, final Relation relation) {
            final short columnCount = buffer.getShort();
            if(columnCount != relation.columns.size()) {
                throw new IllegalStateException("Illegal pgoutput tuple column count: " + columnCount
                        + ", relation " + relation.name + " expects: " + relation.columns.size());
            }
            final TupleValue[] values = new TupleValue[columnCount];
            for(int i = 0; i < columnCount; i++) {
                final char kind = (char) buffer.get();
                switch (kind) {
                    case 'n' -> values[i] = null;
                    case 'u' -> values[i] = TupleValue.UNCHANGED;
                    case 'b', 't' -> {
                        final int length = buffer.getInt();
                        final byte[] bytes = new byte[length];
                        buffer.get(bytes);
                        values[i] = new TupleValue(kind == 't', bytes);
                    }
                    default -> throw new IllegalStateException("Illegal pgoutput tuple data kind: " + kind);
                }
            }
            return values;
        }

        private ChangeEvent createEvent(
                final long lsn,
                final Relation relation,
                final Op op,
                final TupleValue[] oldTuple,
                final TupleValue[] newTuple) {

            final TupleValue[] keyTuple = switch (op) {
                case INSERT -> newTuple;
                // the old tuple (replica identity image) is only sent when a key column changed
                case UPDATE -> oldTuple != null ? oldTuple : newTuple;
                case DELETE -> oldTuple;
                case TRUNCATE -> null;
            };
            final List<String> primaryKeyColumns = keyColumns.get(relation.namespace + "." + relation.name);
            final String keysJson = keyTuple == null ? "{}" : toKeysJson(relation, keyTuple, primaryKeyColumns);
            final String oldValuesJson = oldTuple == null ? null : toFullJson(relation, oldTuple);
            final String newValuesJson = newTuple == null ? null : toFullJson(relation, newTuple);
            return new ChangeEvent(
                    lsn, txCommitLsn, txCommitTimestampMicros, txId, txSequence++,
                    relation.namespace, relation.name, op,
                    keysJson, oldValuesJson, newValuesJson,
                    toColumnsJson(relation, primaryKeyColumns));
        }

        // the relation's columns in unified cdc type names, key flags as resolved for keysJson
        private static String toColumnsJson(final Relation relation, final List<String> primaryKeyColumns) {
            final JsonArray columns = new JsonArray();
            for(final RelationColumn column : relation.columns) {
                final boolean key = primaryKeyColumns != null ? primaryKeyColumns.contains(column.name) : column.key;
                final JsonObject json = new JsonObject();
                json.addProperty("name", column.name);
                json.addProperty("type", com.mercari.solution.util.pipeline.cdc.ChangeSchema
                        .fromPostgresType(column.column.type, column.column.elementType));
                json.addProperty("key", key);
                columns.add(json);
            }
            return columns.toString();
        }

        // keys: the launch-resolved primary key columns, or the protocol's replica-identity
        // key flags when the table is not in the map
        private static String toKeysJson(final Relation relation, final TupleValue[] tuple, final List<String> primaryKeyColumns) {
            final JsonObject json = new JsonObject();
            for(int i = 0; i < tuple.length; i++) {
                final RelationColumn column = relation.columns.get(i);
                final boolean key = primaryKeyColumns != null ? primaryKeyColumns.contains(column.name) : column.key;
                if(key) {
                    addJsonValue(json, column, tuple[i]);
                }
            }
            return json.toString();
        }

        private static String toFullJson(final Relation relation, final TupleValue[] tuple) {
            final JsonObject json = new JsonObject();
            for(int i = 0; i < tuple.length; i++) {
                addJsonValue(json, relation.columns.get(i), tuple[i]);
            }
            return json.toString();
        }

        private static void addJsonValue(final JsonObject json, final RelationColumn column, final TupleValue value) {
            if(value == TupleValue.UNCHANGED) {
                // unchanged toasted value: omitted (the field is absent, not null)
                return;
            }
            if(value == null) {
                json.add(column.name, JsonNull.INSTANCE);
                return;
            }
            json.add(column.name, toJsonElement(column.column, value));
        }
    }

    // One tuple column value: binary or text format bytes (null entries represent SQL NULL,
    // the UNCHANGED sentinel a toasted value the server did not resend)
    private record TupleValue(boolean text, byte[] bytes) {
        private static final TupleValue UNCHANGED = new TupleValue(false, new byte[0]);
    }

    private static JsonElement toJsonElement(final PostgresUtil.Column column, final TupleValue value) {
        if(value.text()) {
            return textToJsonElement(column.type, new String(value.bytes(), StandardCharsets.UTF_8));
        }
        final Object decoded = PostgresUtil.decodeValue(column, value.bytes());
        return valueToJsonElement(column.type, column.elementType, decoded);
    }

    private static JsonElement valueToJsonElement(
            final PostgresUtil.ColumnType type,
            final PostgresUtil.ColumnType elementType,
            final Object value) {

        if(value == null) {
            return JsonNull.INSTANCE;
        }
        return switch (type) {
            case BOOL -> new JsonPrimitive((Boolean) value);
            case INT2, INT4, INT8, FLOAT4, FLOAT8 -> new JsonPrimitive((Number) value);
            // avro decimal bytes (scale 9); rendered as a plain decimal string to avoid
            // float64 round-trip loss downstream
            case NUMERIC -> new JsonPrimitive(decimalToString((ByteBuffer) value));
            case TEXT, VARCHAR, BPCHAR, UUID, JSON, JSONB, XML, ENUM,
                 INET, CIDR, MACADDR, MACADDR8 -> new JsonPrimitive((String) value);
            case BYTEA -> new JsonPrimitive(base64((ByteBuffer) value));
            case DATE -> new JsonPrimitive(LocalDate.ofEpochDay(((Number) value).longValue()).toString());
            case TIME, TIMETZ -> new JsonPrimitive(LocalTime.ofNanoOfDay(((Number) value).longValue() * 1000L).toString());
            case TIMESTAMP -> new JsonPrimitive(LocalDateTime.ofInstant(microsToInstant(((Number) value).longValue()), ZoneOffset.UTC).toString());
            case TIMESTAMPTZ -> new JsonPrimitive(microsToInstant(((Number) value).longValue()).toString());
            case ARRAY -> {
                final JsonArray array = new JsonArray();
                for(final Object element : (List<?>) value) {
                    array.add(valueToJsonElement(elementType, null, element));
                }
                yield array;
            }
        };
    }

    // Fallback for text-format values: types without binary send functions, or servers
    // where the binary option is unavailable. Values keep their postgres text output form,
    // converted to the matching JSON primitive kind where the text form differs.
    private static JsonElement textToJsonElement(final PostgresUtil.ColumnType type, final String text) {
        return switch (type) {
            case BOOL -> new JsonPrimitive("t".equals(text) || "true".equals(text));
            case INT2, INT4 -> new JsonPrimitive(Integer.parseInt(text));
            case INT8 -> new JsonPrimitive(Long.parseLong(text));
            case FLOAT4, FLOAT8 -> new JsonPrimitive(Double.parseDouble(text));
            case BYTEA -> new JsonPrimitive(text.startsWith("\\x")
                    ? Base64.getEncoder().encodeToString(hexToBytes(text.substring(2)))
                    : text);
            default -> new JsonPrimitive(text);
        };
    }

    private static String decimalToString(final ByteBuffer buffer) {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.asReadOnlyBuffer().get(bytes);
        return new BigDecimal(new BigInteger(bytes), 9).stripTrailingZeros().toPlainString();
    }

    private static String base64(final ByteBuffer buffer) {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.asReadOnlyBuffer().get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static byte[] hexToBytes(final String hex) {
        final byte[] bytes = new byte[hex.length() / 2];
        for(int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static Instant microsToInstant(final long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
    }

    private static String readString(final ByteBuffer buffer) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte b;
        while((b = buffer.get()) != 0) {
            bos.write(b);
        }
        return bos.toString(StandardCharsets.UTF_8);
    }

}
