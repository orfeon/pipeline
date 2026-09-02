package com.mercari.solution.util.pipeline.cdc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TiCDC provider support: normalizes canal-json events into the unified {@link ChangeRecord}
 * envelope. Covers both delivery paths — Kafka messages (one event per message payload) and
 * the TiCDC storage sink (newline-delimited canal-json files synced to GCS/S3, read back with
 * the storage/files source).
 *
 * <p>DDL events ({@code isDdl: true}) become control records: {@code TRUNCATE TABLE} /
 * {@code DROP TABLE} statements a {@code TRUNCATE} record, any other DDL a {@code SCHEMA}
 * record carrying the SQL text as {@code statement} (canal-json DDL events have no column
 * types, so {@code schema} is null — the cdc transform synthesizes a second SCHEMA record
 * with the new schema from the {@code mysqlType} of the next row event). Watermark events
 * are skipped.</p>
 */
public class TiCdcChangeCapture {

    public static final String PROVIDER = "ticdc";

    private TiCdcChangeCapture() {
    }

    /**
     * Parses one canal-json event and returns envelope primitive-value maps — one per row in
     * the event's {@code data} array. Returns an empty list for DDL/watermark events.
     */
    public static List<Map<String, Object>> normalize(final String eventJson) {
        return normalize(parse(eventJson));
    }

    public static JsonObject parse(final String eventJson) {
        final JsonElement parsed = JsonParser.parseString(eventJson);
        if(!parsed.isJsonObject()) {
            throw new IllegalArgumentException("ticdc canal-json event must be a JSON object: " + eventJson);
        }
        return parsed.getAsJsonObject();
    }

    /** The table name of the event, or null. */
    public static String table(final JsonObject event) {
        return getAsString(event, "table");
    }

    /** The row schema of a row event ({@code mysqlType} + {@code pkNames}), or null for DDL/watermark events. */
    public static List<ChangeSchema.Column> columns(final JsonObject event) {
        if(!event.has("mysqlType") || !event.get("mysqlType").isJsonObject()) {
            return null;
        }
        return ChangeSchema.fromTiCdcEvent(event.getAsJsonObject("mysqlType"), pkNames(event));
    }

    /** Builds a {@code SCHEMA} control record ordered before the rows of the given event. */
    public static Map<String, Object> schemaChange(final JsonObject event, final String schemaJson) {
        final String table = table(event);
        if(table == null) {
            throw new IllegalArgumentException("ticdc canal-json event misses table: " + event);
        }
        final Timing timing = timing(event);
        return ChangeRecord.control(
                table, ChangeRecord.Op.SCHEMA, timing.commitTimestampMicros,
                ChangeRecord.sequence(timing.orderKey),
                createSource(event, timing.commitTs), ChangeRecord.transaction(Long.toString(timing.orderKey), null, null),
                schemaJson, null);
    }

    private record Timing(long commitTimestampMicros, Long commitTs, long orderKey) { }

    private static Timing timing(final JsonObject event) {
        // es: the event commit time in epoch millis
        final Long es = getAsLong(event, "es");
        final long commitTimestampMicros = (es == null ? 0L : es) * 1000L;
        // _tidb.commitTs: the TiDB TSO, present when the canal-json extension is enabled
        final Long commitTs = event.has("_tidb") && event.get("_tidb").isJsonObject()
                ? getAsLong(event.getAsJsonObject("_tidb"), "commitTs")
                : null;
        return new Timing(commitTimestampMicros, commitTs, commitTs != null ? commitTs : commitTimestampMicros);
    }

    private static List<String> pkNames(final JsonObject event) {
        final List<String> pkNames = new ArrayList<>();
        if(event.has("pkNames") && event.get("pkNames").isJsonArray()) {
            for(final JsonElement pkName : event.getAsJsonArray("pkNames")) {
                if(pkName.isJsonPrimitive()) {
                    pkNames.add(pkName.getAsString());
                }
            }
        }
        return pkNames;
    }

    private static Map<String, Object> createSource(final JsonObject event, final Long commitTs) {
        final Map<String, Object> source = new HashMap<>();
        source.put(ChangeRecord.FIELD_SOURCE_PROVIDER, PROVIDER);
        source.put(ChangeRecord.FIELD_SOURCE_DATABASE, getAsString(event, "database"));
        source.put(ChangeRecord.FIELD_SOURCE_METADATA, createSourceMetadataJson(event, commitTs));
        return source;
    }

    public static List<Map<String, Object>> normalize(final JsonObject event) {

        if(getAsBoolean(event, "isDdl")) {
            return normalizeDdl(event);
        }
        final String type = getAsString(event, "type");
        final ChangeRecord.Op op = switch (type) {
            case "INSERT" -> ChangeRecord.Op.INSERT;
            case "UPDATE" -> ChangeRecord.Op.UPDATE;
            case "DELETE" -> ChangeRecord.Op.DELETE;
            case null, default -> null; // QUERY / TIDB_WATERMARK / unknown
        };
        if(op == null) {
            return new ArrayList<>();
        }

        final String table = getAsString(event, "table");
        if(table == null) {
            throw new IllegalArgumentException("ticdc canal-json event misses table: " + event);
        }
        final Timing timing = timing(event);
        final long commitTimestampMicros = timing.commitTimestampMicros;
        final Long commitTs = timing.commitTs;
        final List<String> pkNames = pkNames(event);

        final JsonArray data = event.has("data") && event.get("data").isJsonArray()
                ? event.getAsJsonArray("data")
                : new JsonArray();
        final JsonArray old = event.has("old") && event.get("old").isJsonArray()
                ? event.getAsJsonArray("old")
                : null;

        final Map<String, Object> source = createSource(event, commitTs);

        final List<Map<String, Object>> envelopes = new ArrayList<>();
        for(int i = 0; i < data.size(); i++) {
            final JsonElement rowElement = data.get(i);
            if(!rowElement.isJsonObject()) {
                throw new IllegalArgumentException("ticdc canal-json event has illegal data row: " + rowElement);
            }
            final JsonObject row = rowElement.getAsJsonObject();
            final JsonObject oldRow = old != null && i < old.size() && old.get(i).isJsonObject()
                    ? old.get(i).getAsJsonObject()
                    : null;

            final Map<String, Object> envelope = new HashMap<>();
            envelope.put(ChangeRecord.FIELD_TABLE, table);
            envelope.put(ChangeRecord.FIELD_OP, op.getId());
            envelope.put(ChangeRecord.FIELD_KEYS, createKeysJson(pkNames, row));
            envelope.put(ChangeRecord.FIELD_COMMIT_TIMESTAMP, commitTimestampMicros);
            envelope.put(ChangeRecord.FIELD_SEQUENCE, commitTs != null
                    ? ChangeRecord.sequence(commitTs, i)
                    : ChangeRecord.sequence(commitTimestampMicros, i));
            switch (op) {
                case DELETE -> {
                    envelope.put(ChangeRecord.FIELD_BEFORE, row.toString());
                    envelope.put(ChangeRecord.FIELD_AFTER, null);
                }
                case UPDATE -> {
                    envelope.put(ChangeRecord.FIELD_BEFORE, oldRow == null ? null : oldRow.toString());
                    envelope.put(ChangeRecord.FIELD_AFTER, row.toString());
                }
                default -> {
                    envelope.put(ChangeRecord.FIELD_BEFORE, null);
                    envelope.put(ChangeRecord.FIELD_AFTER, row.toString());
                }
            }

            envelope.put(ChangeRecord.FIELD_SOURCE, source);
            // canal-json has no transaction id: rows of one commit share the TSO / event time
            envelope.put(ChangeRecord.FIELD_TRANSACTION, ChangeRecord.transaction(Long.toString(timing.orderKey), null, (long) i));
            envelope.put(ChangeRecord.FIELD_SCHEMA, null);
            envelope.put(ChangeRecord.FIELD_STATEMENT, null);

            envelopes.add(envelope);
        }
        return envelopes;
    }

    // DDL event: {"isDdl":true,"type":"QUERY","sql":"ALTER TABLE ...","table":"t","database":"d",...}
    private static List<Map<String, Object>> normalizeDdl(final JsonObject event) {
        final List<Map<String, Object>> envelopes = new ArrayList<>();
        final String table = table(event);
        final String sql = getAsString(event, "sql");
        if(table == null || table.isBlank()) {
            return envelopes; // database-level DDL
        }
        final ChangeRecord.Op op = isTruncateStatement(sql) ? ChangeRecord.Op.TRUNCATE : ChangeRecord.Op.SCHEMA;
        final Timing timing = timing(event);
        envelopes.add(ChangeRecord.control(
                table, op, timing.commitTimestampMicros,
                ChangeRecord.sequence(timing.orderKey, 0L),
                createSource(event, timing.commitTs), ChangeRecord.transaction(Long.toString(timing.orderKey), null, 0L),
                null, sql));
        return envelopes;
    }

    static boolean isTruncateStatement(final String sql) {
        if(sql == null) {
            return false;
        }
        final String normalized = sql.trim().toUpperCase(java.util.Locale.ROOT).replaceAll("\\s+", " ");
        return normalized.startsWith("TRUNCATE") || normalized.startsWith("DROP TABLE");
    }

    private static String createKeysJson(final List<String> pkNames, final JsonObject row) {
        final JsonObject keys = new JsonObject();
        for(final String pkName : pkNames) {
            if(row.has(pkName)) {
                keys.add(pkName, row.get(pkName));
            }
        }
        return keys.toString();
    }

    private static String createSourceMetadataJson(final JsonObject event, final Long commitTs) {
        final JsonObject metadata = new JsonObject();
        if(event.has("ts")) {
            metadata.add("ts", event.get("ts"));
        }
        if(event.has("es")) {
            metadata.add("es", event.get("es"));
        }
        if(commitTs != null) {
            metadata.addProperty("commitTs", commitTs);
        }
        return metadata.toString();
    }

    private static String getAsString(final JsonObject object, final String field) {
        if(!object.has(field) || object.get(field).isJsonNull() || !object.get(field).isJsonPrimitive()) {
            return null;
        }
        return object.get(field).getAsString();
    }

    private static Long getAsLong(final JsonObject object, final String field) {
        if(!object.has(field) || object.get(field).isJsonNull() || !object.get(field).isJsonPrimitive()) {
            return null;
        }
        try {
            return object.get(field).getAsLong();
        } catch (final NumberFormatException e) {
            return null;
        }
    }

    private static boolean getAsBoolean(final JsonObject object, final String field) {
        return object.has(field)
                && object.get(field).isJsonPrimitive()
                && object.get(field).getAsJsonPrimitive().isBoolean()
                && object.get(field).getAsBoolean();
    }

}
