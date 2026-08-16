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
 * <p>DDL events ({@code isDdl: true} / type {@code QUERY}) and watermark events are skipped.</p>
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

        final JsonElement parsed = JsonParser.parseString(eventJson);
        if(!parsed.isJsonObject()) {
            throw new IllegalArgumentException("ticdc canal-json event must be a JSON object: " + eventJson);
        }
        final JsonObject event = parsed.getAsJsonObject();

        if(getAsBoolean(event, "isDdl")) {
            return new ArrayList<>();
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
            throw new IllegalArgumentException("ticdc canal-json event misses table: " + eventJson);
        }
        final String database = getAsString(event, "database");

        // es: the event commit time in epoch millis
        final Long es = getAsLong(event, "es");
        final long commitTimestampMicros = (es == null ? 0L : es) * 1000L;
        // _tidb.commitTs: the TiDB TSO, present when the canal-json extension is enabled
        final Long commitTs = event.has("_tidb") && event.get("_tidb").isJsonObject()
                ? getAsLong(event.getAsJsonObject("_tidb"), "commitTs")
                : null;

        final List<String> pkNames = new ArrayList<>();
        if(event.has("pkNames") && event.get("pkNames").isJsonArray()) {
            for(final JsonElement pkName : event.getAsJsonArray("pkNames")) {
                if(pkName.isJsonPrimitive()) {
                    pkNames.add(pkName.getAsString());
                }
            }
        }

        final JsonArray data = event.has("data") && event.get("data").isJsonArray()
                ? event.getAsJsonArray("data")
                : new JsonArray();
        final JsonArray old = event.has("old") && event.get("old").isJsonArray()
                ? event.getAsJsonArray("old")
                : null;

        final String metadataJson = createSourceMetadataJson(event, commitTs);

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

            final Map<String, Object> source = new HashMap<>();
            source.put(ChangeRecord.FIELD_SOURCE_PROVIDER, PROVIDER);
            source.put(ChangeRecord.FIELD_SOURCE_DATABASE, database);
            source.put(ChangeRecord.FIELD_SOURCE_METADATA, metadataJson);
            envelope.put(ChangeRecord.FIELD_SOURCE, source);

            envelopes.add(envelope);
        }
        return envelopes;
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
