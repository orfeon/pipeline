package com.mercari.solution.util.pipeline.cdc;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The unified change data capture record (envelope).
 *
 * <p>Provider specific change records (Spanner change streams, TiCDC, Debezium, ...) are
 * normalized into this single schema by the {@code cdc} transform, and apply-capable sinks
 * (bigquery, spanner, ...) consume it. The envelope is an ordinary schema'd record — it flows
 * through the pipeline as a plain {@link MElement} so that any transform can filter or reshape
 * it and any file sink can archive it.</p>
 *
 * <p>{@code before}/{@code after} may exclude primary key columns (Spanner change streams do) —
 * the full row is always {@code keys ∪ after}. {@code keys} is the primary key of the row
 * <em>after</em> the change (the deleted row's key for DELETE); a primary key change is
 * normalized into a DELETE of the old key followed by an INSERT of the new key
 * ({@link #splitKeyChange}), so consumers may assume an UPDATE never moves a row.</p>
 *
 * <p>Besides row changes the envelope carries table-level <em>control</em> records
 * ({@link Op#isControl()}): {@code TRUNCATE}, {@code SCHEMA} (the row schema changed —
 * {@code schema} holds the new {@link ChangeSchema}, {@code statement} the provider DDL text
 * when available) and the reserved {@code SNAPSHOT_BEGIN}/{@code SNAPSHOT_END}. Control
 * records have null {@code keys}/{@code before}/{@code after} and share the {@code sequence}
 * ordering of the row changes of the same table. Apply-sinks skip control records unless
 * configured otherwise.</p>
 */
public class ChangeRecord implements Serializable {

    public static final String FIELD_TABLE = "table";
    public static final String FIELD_OP = "op";
    public static final String FIELD_KEYS = "keys";
    public static final String FIELD_BEFORE = "before";
    public static final String FIELD_AFTER = "after";
    public static final String FIELD_COMMIT_TIMESTAMP = "commitTimestamp";
    public static final String FIELD_SEQUENCE = "sequence";
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_TRANSACTION = "transaction";
    public static final String FIELD_SCHEMA = "schema";
    public static final String FIELD_STATEMENT = "statement";

    public static final String FIELD_TRANSACTION_ID = "id";
    public static final String FIELD_TRANSACTION_TOTAL_RECORDS = "totalRecords";
    public static final String FIELD_TRANSACTION_INDEX = "index";

    public static final String FIELD_SOURCE_PROVIDER = "provider";
    public static final String FIELD_SOURCE_DATABASE = "database";
    public static final String FIELD_SOURCE_METADATA = "metadata";

    /**
     * Change operation. {@code SNAPSHOT} represents initial-snapshot reads
     * (Debezium op {@code r}); apply-sinks treat it like {@code INSERT}.
     * {@code TRUNCATE}, {@code SCHEMA}, {@code SNAPSHOT_BEGIN} and {@code SNAPSHOT_END} are
     * table-level control records (no row values). The snapshot boundary symbols are reserved
     * for a future source snapshot feature and are not emitted yet.
     */
    public enum Op implements Serializable {

        INSERT(0),
        UPDATE(1),
        DELETE(2),
        SNAPSHOT(3),
        TRUNCATE(4),
        SCHEMA(5),
        SNAPSHOT_BEGIN(6),
        SNAPSHOT_END(7);

        private final int id;

        Op(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        /** Whether this is a table-level control record rather than a row change. */
        public boolean isControl() {
            return switch (this) {
                case INSERT, UPDATE, DELETE, SNAPSHOT -> false;
                case TRUNCATE, SCHEMA, SNAPSHOT_BEGIN, SNAPSHOT_END -> true;
            };
        }

        public static Op of(final int id) {
            for(final Op op : values()) {
                if(op.id == id) {
                    return op;
                }
            }
            throw new IllegalArgumentException("No such enum object for ChangeRecord.Op id: " + id);
        }

        public static List<String> symbols() {
            final List<String> symbols = new ArrayList<>();
            for(final Op op : values()) {
                symbols.add(op.name());
            }
            return symbols;
        }
    }

    // _CHANGE_SEQUENCE_NUMBER compatible: up to 4 sections of 1-16 hex digits separated by '/'
    private static final Pattern SEQUENCE_PATTERN = Pattern
            .compile("^[0-9A-Fa-f]{1,16}(/[0-9A-Fa-f]{1,16}){0,3}$");

    private ChangeRecord() {
    }

    public static Schema schema() {
        return Schema.builder()
                .withField(FIELD_TABLE, Schema.FieldType.STRING.withNullable(false))
                .withField(FIELD_OP, Schema.FieldType.enumeration(Op.symbols()).withNullable(false))
                .withField(FIELD_KEYS, Schema.FieldType.JSON)
                .withField(FIELD_BEFORE, Schema.FieldType.JSON)
                .withField(FIELD_AFTER, Schema.FieldType.JSON)
                .withField(FIELD_COMMIT_TIMESTAMP, Schema.FieldType.TIMESTAMP.withNullable(false))
                .withField(FIELD_SEQUENCE, Schema.FieldType.STRING.withNullable(false))
                .withField(FIELD_SOURCE, Schema.FieldType.element(Schema.builder()
                        .withField(FIELD_SOURCE_PROVIDER, Schema.FieldType.STRING.withNullable(false))
                        .withField(FIELD_SOURCE_DATABASE, Schema.FieldType.STRING)
                        .withField(FIELD_SOURCE_METADATA, Schema.FieldType.JSON)
                        .build()))
                .withField(FIELD_TRANSACTION, Schema.FieldType.element(Schema.builder()
                        .withField(FIELD_TRANSACTION_ID, Schema.FieldType.STRING.withNullable(false))
                        .withField(FIELD_TRANSACTION_TOTAL_RECORDS, Schema.FieldType.INT64)
                        .withField(FIELD_TRANSACTION_INDEX, Schema.FieldType.INT64)
                        .build()).withNullable(true))
                .withField(FIELD_SCHEMA, Schema.FieldType.JSON)
                .withField(FIELD_STATEMENT, Schema.FieldType.STRING)
                .build();
    }

    /** Builds the {@code transaction} sub-record values. */
    public static Map<String, Object> transaction(final String id, final Long totalRecords, final Long index) {
        if(id == null) {
            return null;
        }
        final Map<String, Object> transaction = new HashMap<>();
        transaction.put(FIELD_TRANSACTION_ID, id);
        transaction.put(FIELD_TRANSACTION_TOTAL_RECORDS, totalRecords);
        transaction.put(FIELD_TRANSACTION_INDEX, index);
        return transaction;
    }

    /**
     * Builds a table-level control record (TRUNCATE / SCHEMA / ...): no row values, the
     * given {@code schema} (JSON, see {@link ChangeSchema}) and provider {@code statement}.
     */
    public static Map<String, Object> control(
            final String table,
            final Op op,
            final long commitTimestampMicros,
            final String sequence,
            final Map<String, Object> source,
            final Map<String, Object> transaction,
            final String schemaJson,
            final String statement) {

        if(!op.isControl()) {
            throw new IllegalArgumentException("ChangeRecord control record requires a control op, got: " + op);
        }
        final Map<String, Object> envelope = new HashMap<>();
        envelope.put(FIELD_TABLE, table);
        envelope.put(FIELD_OP, op.getId());
        envelope.put(FIELD_KEYS, null);
        envelope.put(FIELD_BEFORE, null);
        envelope.put(FIELD_AFTER, null);
        envelope.put(FIELD_COMMIT_TIMESTAMP, commitTimestampMicros);
        envelope.put(FIELD_SEQUENCE, sequence);
        envelope.put(FIELD_SOURCE, source);
        envelope.put(FIELD_TRANSACTION, transaction);
        envelope.put(FIELD_SCHEMA, schemaJson);
        envelope.put(FIELD_STATEMENT, statement);
        return envelope;
    }

    /**
     * Normalizes a primary key change: when an UPDATE's {@code before} and {@code after} both
     * carry every key column (the key column names are those of {@code keys}) and the values
     * differ, the envelope is split into a DELETE of the old key and an INSERT of the new key.
     * The two records extend the original {@code sequence} with one more section
     * ({@code .../0} and {@code .../1}) so they keep their position and relative order.
     * Any other envelope is returned as is (in a singleton list).
     */
    public static List<Map<String, Object>> splitKeyChange(final Map<String, Object> envelope) {
        final List<Map<String, Object>> result = new ArrayList<>();
        final Object opValue = envelope.get(FIELD_OP);
        if(opValue == null || !Op.UPDATE.equals(getOp(opValue))) {
            result.add(envelope);
            return result;
        }
        final Object keys = envelope.get(FIELD_KEYS);
        final Object before = envelope.get(FIELD_BEFORE);
        final Object after = envelope.get(FIELD_AFTER);
        if(keys == null || before == null || after == null) {
            result.add(envelope);
            return result;
        }
        final JsonObject keysJson = parseObject(keys.toString());
        final JsonObject beforeJson = parseObject(before.toString());
        final JsonObject afterJson = parseObject(after.toString());
        if(keysJson == null || beforeJson == null || afterJson == null || keysJson.isEmpty()) {
            result.add(envelope);
            return result;
        }
        final JsonObject oldKeys = new JsonObject();
        final JsonObject newKeys = new JsonObject();
        for(final String keyName : keysJson.keySet()) {
            if(!beforeJson.has(keyName) || !afterJson.has(keyName)) {
                result.add(envelope);
                return result;
            }
            oldKeys.add(keyName, beforeJson.get(keyName));
            newKeys.add(keyName, afterJson.get(keyName));
        }
        if(oldKeys.equals(newKeys)) {
            result.add(envelope);
            return result;
        }
        final String sequence = envelope.get(FIELD_SEQUENCE) == null ? null : envelope.get(FIELD_SEQUENCE).toString();
        if(sequence == null || sequence.split("/").length >= 4) {
            throw new IllegalArgumentException(
                    "ChangeRecord primary key change cannot be split: sequence has no room for another section: " + sequence);
        }

        final Map<String, Object> delete = new HashMap<>(envelope);
        delete.put(FIELD_OP, Op.DELETE.getId());
        delete.put(FIELD_KEYS, oldKeys.toString());
        delete.put(FIELD_AFTER, null);
        delete.put(FIELD_SEQUENCE, sequence + "/0");

        final Map<String, Object> insert = new HashMap<>(envelope);
        insert.put(FIELD_OP, Op.INSERT.getId());
        insert.put(FIELD_KEYS, newKeys.toString());
        insert.put(FIELD_BEFORE, null);
        insert.put(FIELD_SEQUENCE, sequence + "/1");

        result.add(delete);
        result.add(insert);
        return result;
    }

    /**
     * The envelope itself as a BigQuery {@link TableRow} — the failure-record representation of
     * the bigquery sink cdc mode (so a failed change is archived as a replayable envelope, not as
     * the merged destination row). {@link #fromJson} is the inverse.
     */
    public static TableRow toEnvelopeTableRow(final MElement element) {
        final Map<String, Object> values = element.asPrimitiveMap();
        final TableRow row = new TableRow();
        // toString() must be JSON: the failure sinks archive the record as its text form
        row.setFactory(GsonFactory.getDefaultInstance());
        row.set(FIELD_TABLE, asString(values.get(FIELD_TABLE)));
        row.set(FIELD_OP, getOp(values.get(FIELD_OP)).name());
        row.set(FIELD_KEYS, asString(values.get(FIELD_KEYS)));
        row.set(FIELD_BEFORE, asString(values.get(FIELD_BEFORE)));
        row.set(FIELD_AFTER, asString(values.get(FIELD_AFTER)));
        row.set(FIELD_COMMIT_TIMESTAMP, asLong(values.get(FIELD_COMMIT_TIMESTAMP)));
        row.set(FIELD_SEQUENCE, asString(values.get(FIELD_SEQUENCE)));
        if(values.get(FIELD_SOURCE) instanceof Map<?, ?> source) {
            final TableRow sourceRow = new TableRow();
            sourceRow.set(FIELD_SOURCE_PROVIDER, asString(source.get(FIELD_SOURCE_PROVIDER)));
            sourceRow.set(FIELD_SOURCE_DATABASE, asString(source.get(FIELD_SOURCE_DATABASE)));
            sourceRow.set(FIELD_SOURCE_METADATA, asString(source.get(FIELD_SOURCE_METADATA)));
            row.set(FIELD_SOURCE, sourceRow);
        }
        if(values.get(FIELD_TRANSACTION) instanceof Map<?, ?> transaction) {
            final TableRow transactionRow = new TableRow();
            transactionRow.set(FIELD_TRANSACTION_ID, asString(transaction.get(FIELD_TRANSACTION_ID)));
            transactionRow.set(FIELD_TRANSACTION_TOTAL_RECORDS, asLong(transaction.get(FIELD_TRANSACTION_TOTAL_RECORDS)));
            transactionRow.set(FIELD_TRANSACTION_INDEX, asLong(transaction.get(FIELD_TRANSACTION_INDEX)));
            row.set(FIELD_TRANSACTION, transactionRow);
        }
        row.set(FIELD_SCHEMA, asString(values.get(FIELD_SCHEMA)));
        row.set(FIELD_STATEMENT, asString(values.get(FIELD_STATEMENT)));
        return row;
    }

    /**
     * Parses an envelope from its JSON text (the {@link #toEnvelopeTableRow} form, or an
     * envelope record serialized as JSON) into envelope primitive values. {@code op} accepts
     * the symbol name or id; {@code commitTimestamp} epoch micros or an ISO-8601 text; nested
     * JSON fields ({@code keys} etc.) may be objects or JSON text.
     */
    public static Map<String, Object> fromJson(final String json) {
        final JsonObject object = parseObject(json);
        if(object == null) {
            throw new IllegalArgumentException("ChangeRecord envelope must be a JSON object: " + json);
        }
        return fromJson(object);
    }

    public static Map<String, Object> fromJson(final JsonObject object) {
        final Map<String, Object> envelope = new HashMap<>();
        final String table = jsonString(object.get(FIELD_TABLE));
        final JsonElement opElement = object.get(FIELD_OP);
        if(table == null || opElement == null || opElement.isJsonNull()) {
            throw new IllegalArgumentException("ChangeRecord envelope requires table and op: " + object);
        }
        final Op op = opElement.isJsonPrimitive() && opElement.getAsJsonPrimitive().isNumber()
                ? Op.of(opElement.getAsInt())
                : Op.valueOf(opElement.getAsString());
        envelope.put(FIELD_TABLE, table);
        envelope.put(FIELD_OP, op.getId());
        envelope.put(FIELD_KEYS, jsonText(object.get(FIELD_KEYS)));
        envelope.put(FIELD_BEFORE, jsonText(object.get(FIELD_BEFORE)));
        envelope.put(FIELD_AFTER, jsonText(object.get(FIELD_AFTER)));
        envelope.put(FIELD_COMMIT_TIMESTAMP, jsonMicros(object.get(FIELD_COMMIT_TIMESTAMP)));
        final String sequence = jsonString(object.get(FIELD_SEQUENCE));
        if(!isValidSequence(sequence)) {
            throw new IllegalArgumentException("ChangeRecord envelope has illegal sequence: " + object);
        }
        envelope.put(FIELD_SEQUENCE, sequence);
        final Map<String, Object> source = new HashMap<>();
        final JsonElement sourceElement = object.get(FIELD_SOURCE);
        if(sourceElement != null && sourceElement.isJsonObject()) {
            final JsonObject s = sourceElement.getAsJsonObject();
            source.put(FIELD_SOURCE_PROVIDER, jsonString(s.get(FIELD_SOURCE_PROVIDER)));
            source.put(FIELD_SOURCE_DATABASE, jsonString(s.get(FIELD_SOURCE_DATABASE)));
            source.put(FIELD_SOURCE_METADATA, jsonText(s.get(FIELD_SOURCE_METADATA)));
        }
        if(source.get(FIELD_SOURCE_PROVIDER) == null) {
            source.put(FIELD_SOURCE_PROVIDER, "envelope");
        }
        envelope.put(FIELD_SOURCE, source);
        final JsonElement transactionElement = object.get(FIELD_TRANSACTION);
        if(transactionElement != null && transactionElement.isJsonObject()) {
            final JsonObject t = transactionElement.getAsJsonObject();
            envelope.put(FIELD_TRANSACTION, transaction(
                    jsonString(t.get(FIELD_TRANSACTION_ID)),
                    jsonLong(t.get(FIELD_TRANSACTION_TOTAL_RECORDS)),
                    jsonLong(t.get(FIELD_TRANSACTION_INDEX))));
        } else {
            envelope.put(FIELD_TRANSACTION, null);
        }
        envelope.put(FIELD_SCHEMA, jsonText(object.get(FIELD_SCHEMA)));
        envelope.put(FIELD_STATEMENT, jsonString(object.get(FIELD_STATEMENT)));
        return envelope;
    }

    /**
     * Re-validates an envelope read back as a schema'd record (archived envelope files): the op
     * is resolved to its id and every envelope field is present.
     */
    public static Map<String, Object> fromElement(final MElement element) {
        final Map<String, Object> values = element.asPrimitiveMap();
        final Map<String, Object> envelope = new HashMap<>();
        final String table = asString(values.get(FIELD_TABLE));
        if(table == null) {
            throw new IllegalArgumentException("ChangeRecord envelope requires table: " + values);
        }
        envelope.put(FIELD_TABLE, table);
        envelope.put(FIELD_OP, getOp(values.get(FIELD_OP)).getId());
        envelope.put(FIELD_KEYS, asString(values.get(FIELD_KEYS)));
        envelope.put(FIELD_BEFORE, asString(values.get(FIELD_BEFORE)));
        envelope.put(FIELD_AFTER, asString(values.get(FIELD_AFTER)));
        final Long commitTimestamp = asLong(values.get(FIELD_COMMIT_TIMESTAMP));
        if(commitTimestamp == null) {
            throw new IllegalArgumentException("ChangeRecord envelope requires commitTimestamp: " + values);
        }
        envelope.put(FIELD_COMMIT_TIMESTAMP, commitTimestamp);
        final String sequence = asString(values.get(FIELD_SEQUENCE));
        if(!isValidSequence(sequence)) {
            throw new IllegalArgumentException("ChangeRecord envelope has illegal sequence: " + values);
        }
        envelope.put(FIELD_SEQUENCE, sequence);
        final Map<String, Object> source = new HashMap<>();
        if(values.get(FIELD_SOURCE) instanceof Map<?, ?> s) {
            source.put(FIELD_SOURCE_PROVIDER, asString(s.get(FIELD_SOURCE_PROVIDER)));
            source.put(FIELD_SOURCE_DATABASE, asString(s.get(FIELD_SOURCE_DATABASE)));
            source.put(FIELD_SOURCE_METADATA, asString(s.get(FIELD_SOURCE_METADATA)));
        }
        if(source.get(FIELD_SOURCE_PROVIDER) == null) {
            source.put(FIELD_SOURCE_PROVIDER, "envelope");
        }
        envelope.put(FIELD_SOURCE, source);
        if(values.get(FIELD_TRANSACTION) instanceof Map<?, ?> t) {
            envelope.put(FIELD_TRANSACTION, transaction(
                    asString(t.get(FIELD_TRANSACTION_ID)),
                    asLong(t.get(FIELD_TRANSACTION_TOTAL_RECORDS)),
                    asLong(t.get(FIELD_TRANSACTION_INDEX))));
        } else {
            envelope.put(FIELD_TRANSACTION, null);
        }
        envelope.put(FIELD_SCHEMA, asString(values.get(FIELD_SCHEMA)));
        envelope.put(FIELD_STATEMENT, asString(values.get(FIELD_STATEMENT)));
        return envelope;
    }

    private static String jsonString(final JsonElement element) {
        if(element == null || element.isJsonNull()) {
            return null;
        }
        return element.isJsonPrimitive() ? element.getAsString() : element.toString();
    }

    // a JSON-typed field: an object/array is kept as its JSON text, a string as is
    private static String jsonText(final JsonElement element) {
        if(element == null || element.isJsonNull()) {
            return null;
        }
        return element.isJsonPrimitive() ? element.getAsString() : element.toString();
    }

    private static Long jsonLong(final JsonElement element) {
        if(element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            return null;
        }
        return element.getAsJsonPrimitive().isNumber() ? element.getAsLong() : Long.parseLong(element.getAsString());
    }

    private static Long jsonMicros(final JsonElement element) {
        if(element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
            throw new IllegalArgumentException("ChangeRecord envelope requires commitTimestamp");
        }
        if(element.getAsJsonPrimitive().isNumber()) {
            return element.getAsLong();
        }
        final String text = element.getAsString();
        try {
            return Long.parseLong(text);
        } catch (final NumberFormatException e) {
            return DateTimeUtil.toEpochMicroSecond(java.time.Instant.parse(text));
        }
    }

    private static String asString(final Object value) {
        return value == null ? null : value.toString();
    }

    private static Long asLong(final Object value) {
        return switch (value) {
            case Number number -> number.longValue();
            case String string -> Long.parseLong(string);
            case null, default -> null;
        };
    }

    private static JsonObject parseObject(final String json) {
        if(json == null || json.isBlank()) {
            return null;
        }
        final JsonElement parsed = JsonParser.parseString(json);
        return parsed.isJsonObject() ? parsed.getAsJsonObject() : null;
    }

    /**
     * Collapses the changes of one (table, keys) to the single latest change by
     * {@code sequence}. Used by the cdc transform's {@code accumulate} mode and by
     * transactional apply-sinks (a commit must not carry two mutations of the same row).
     */
    public static MElement latest(final Iterable<MElement> elements) {
        MElement latest = null;
        String latestSequence = null;
        for(final MElement element : elements) {
            final String sequence = element.getAsString(FIELD_SEQUENCE);
            if(latest == null || compareSequence(sequence, latestSequence) > 0) {
                latest = element;
                latestSequence = sequence;
            }
        }
        return latest;
    }

    /**
     * Composes a change sequence value from ordered numeric sections
     * (e.g. commit timestamp micros, record sequence, mod index).
     * The format is compatible with the BigQuery {@code _CHANGE_SEQUENCE_NUMBER} pseudocolumn:
     * 1 to 4 sections of at most 16 hex digits joined by {@code /}, compared numerically per
     * section in order.
     */
    public static String sequence(final long... sections) {
        if(sections.length == 0 || sections.length > 4) {
            throw new IllegalArgumentException("ChangeRecord sequence requires 1 to 4 sections, got: " + sections.length);
        }
        final StringBuilder sb = new StringBuilder();
        for(final long section : sections) {
            if(section < 0) {
                throw new IllegalArgumentException("ChangeRecord sequence sections must not be negative: " + section);
            }
            if(!sb.isEmpty()) {
                sb.append('/');
            }
            sb.append(Long.toHexString(section));
        }
        return sb.toString();
    }

    public static boolean isValidSequence(final String sequence) {
        return sequence != null && SEQUENCE_PATTERN.matcher(sequence).matches();
    }

    /**
     * Compares two sequence values with the BigQuery {@code _CHANGE_SEQUENCE_NUMBER} semantics:
     * section by section as numbers; a missing section sorts lower than any present one.
     */
    public static int compareSequence(final String sequence1, final String sequence2) {
        final String[] sections1 = sequence1.split("/");
        final String[] sections2 = sequence2.split("/");
        final int max = Math.max(sections1.length, sections2.length);
        for(int i = 0; i < max; i++) {
            if(i >= sections1.length) {
                return -1;
            }
            if(i >= sections2.length) {
                return 1;
            }
            final int c = Long.compareUnsigned(
                    Long.parseUnsignedLong(sections1[i], 16),
                    Long.parseUnsignedLong(sections2[i], 16));
            if(c != 0) {
                return c;
            }
        }
        return 0;
    }

    public static Op getOp(final MElement element) {
        return getOp(element.asPrimitiveMap().get(FIELD_OP));
    }

    /** Resolves the op from its primitive representation (enum index or symbol name). */
    public static Op getOp(final Object primitiveValue) {
        return switch (primitiveValue) {
            case null -> throw new IllegalArgumentException("ChangeRecord op value must not be null");
            case Number number -> Op.of(number.intValue());
            case String symbol -> Op.valueOf(symbol);
            case Enum<?> e -> Op.valueOf(e.name());
            case Object o -> Op.valueOf(o.toString());
        };
    }

    public static RowMutationInformation toRowMutationInformation(final Op op, final String sequence) {
        if(!isValidSequence(sequence)) {
            throw new IllegalArgumentException("Illegal ChangeRecord sequence: " + sequence);
        }
        final RowMutationInformation.MutationType mutationType = switch (op) {
            case INSERT, UPDATE, SNAPSHOT -> RowMutationInformation.MutationType.UPSERT;
            case DELETE -> RowMutationInformation.MutationType.DELETE;
            case TRUNCATE, SCHEMA, SNAPSHOT_BEGIN, SNAPSHOT_END -> throw new IllegalArgumentException(
                    "ChangeRecord control record has no row mutation: " + op);
        };
        return RowMutationInformation.of(mutationType, sequence);
    }

    /**
     * Builds the BigQuery row for an envelope record: {@code keys ∪ after} for upserts,
     * {@code keys} only for deletes (BigQuery CDC ignores non-key values on delete).
     */
    public static TableRow toTableRow(final MElement element) {
        final Map<String, Object> values = element.asPrimitiveMap();
        final Op op = getOp(values.get(FIELD_OP));
        final JsonObject row = new JsonObject();
        final Object keys = values.get(FIELD_KEYS);
        if(keys != null) {
            mergeInto(row, keys.toString());
        }
        if(!Op.DELETE.equals(op)) {
            final Object after = values.get(FIELD_AFTER);
            if(after != null) {
                mergeInto(row, after.toString());
            }
        }
        return toTableRow(row);
    }

    private static void mergeInto(final JsonObject target, final String json) {
        final JsonElement parsed = JsonParser.parseString(json);
        if(!parsed.isJsonObject()) {
            throw new IllegalArgumentException("ChangeRecord keys/after must be a JSON object: " + json);
        }
        for(final Map.Entry<String, JsonElement> entry : parsed.getAsJsonObject().entrySet()) {
            target.add(entry.getKey(), entry.getValue());
        }
    }

    private static TableRow toTableRow(final JsonObject jsonObject) {
        final TableRow tableRow = new TableRow();
        for(final Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            final Object value = toTableRowValue(entry.getValue());
            if(value != null) {
                tableRow.set(entry.getKey(), value);
            }
        }
        return tableRow;
    }

    private static Object toTableRowValue(final JsonElement element) {
        if(element == null || element.isJsonNull()) {
            return null;
        } else if(element.isJsonPrimitive()) {
            if(element.getAsJsonPrimitive().isBoolean()) {
                return element.getAsBoolean();
            }
            // numbers are kept as strings: Storage Write API parses them by the table schema
            // without float64 round-trip loss for INT64/NUMERIC columns
            return element.getAsString();
        } else if(element.isJsonObject()) {
            return toTableRow(element.getAsJsonObject());
        } else if(element.isJsonArray()) {
            final List<Object> values = new ArrayList<>();
            for(final JsonElement e : element.getAsJsonArray()) {
                final Object value = toTableRowValue(e);
                if(value != null) {
                    values.add(value);
                }
            }
            return values;
        }
        return null;
    }

}
