package com.mercari.solution.util.pipeline.cdc;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.converter.JsonToMutationConverter;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Converts change record envelopes into Spanner {@link Mutation}s against a destination table
 * whose schema (column types, primary key order) is known.
 *
 * <ul>
 *   <li>{@code INSERT} / {@code UPDATE} / {@code SNAPSHOT} → {@code insertOrUpdate(keys ∪ after)}:
 *       only the columns present in the JSON are set, so a partial {@code after} (Spanner
 *       {@code OLD_AND_NEW_VALUES}) leaves the other columns untouched, and a re-applied change
 *       is harmless.</li>
 *   <li>{@code DELETE} → {@code delete(key)} with the key parts in the table's primary key order.</li>
 *   <li>Control records have no mutation ({@link #isApplicable}).</li>
 * </ul>
 *
 * <p>Columns of the JSON that do not exist on the destination table are skipped (logged once
 * per table and column), mirroring {@code ignoreUnknownValues} of the bigquery sink.</p>
 */
public class ChangeRecordMutationConverter implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeRecordMutationConverter.class);

    /** The destination table description needed for the conversion. */
    public record TableSchema(String table, Schema schema, List<String> primaryKeyColumns) implements Serializable { }

    private final Set<String> reportedUnknownColumns = new HashSet<>();

    public static boolean isApplicable(final ChangeRecord.Op op) {
        return !op.isControl();
    }

    /** Builds the mutation of a row-change envelope (primitive values), or null for control records. */
    public Mutation convert(final TableSchema tableSchema, final Map<String, Object> envelope) {
        final ChangeRecord.Op op = ChangeRecord.getOp(envelope.get(ChangeRecord.FIELD_OP));
        if(!isApplicable(op)) {
            return null;
        }
        final JsonObject keys = parseObject(envelope.get(ChangeRecord.FIELD_KEYS));
        if(keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("change record requires keys to be applied to spanner table: " + tableSchema.table() + ", envelope: " + envelope);
        }
        return switch (op) {
            case DELETE -> Mutation.delete(tableSchema.table(), toKey(tableSchema, keys));
            default -> {
                final Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableSchema.table());
                final JsonObject row = new JsonObject();
                final JsonObject after = parseObject(envelope.get(ChangeRecord.FIELD_AFTER));
                if(after != null) {
                    for(final Map.Entry<String, JsonElement> entry : after.entrySet()) {
                        row.add(entry.getKey(), entry.getValue());
                    }
                }
                // keys win: the envelope key is authoritative for the row identity
                for(final Map.Entry<String, JsonElement> entry : keys.entrySet()) {
                    row.add(entry.getKey(), entry.getValue());
                }
                for(final Map.Entry<String, JsonElement> entry : row.entrySet()) {
                    final Schema.Field field = findField(tableSchema, entry.getKey());
                    if(field == null) {
                        continue;
                    }
                    final Value value = JsonToMutationConverter.convertValue(field, entry.getValue());
                    builder.set(field.getName()).to(value);
                }
                yield builder.build();
            }
        };
    }

    private Schema.Field findField(final TableSchema tableSchema, final String name) {
        if(tableSchema.schema().hasField(name)) {
            return tableSchema.schema().getField(name);
        }
        if(reportedUnknownColumns.add(tableSchema.table() + "." + name)) {
            LOG.warn("change record column: {} does not exist on spanner table: {}, the value is ignored", name, tableSchema.table());
        }
        return null;
    }

    private Key toKey(final TableSchema tableSchema, final JsonObject keys) {
        final Key.Builder builder = Key.newBuilder();
        final List<String> primaryKeyColumns = tableSchema.primaryKeyColumns();
        if(primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            throw new IllegalStateException("spanner table: " + tableSchema.table() + " has no primary key columns");
        }
        for(final String column : primaryKeyColumns) {
            if(!keys.has(column)) {
                throw new IllegalArgumentException("change record keys miss primary key column: " + column + " of spanner table: " + tableSchema.table() + ", keys: " + keys);
            }
            final Schema.Field field = tableSchema.schema().hasField(column) ? tableSchema.schema().getField(column) : null;
            builder.appendObject(toKeyPart(field, keys.get(column)));
        }
        return builder.build();
    }

    // Key parts must be the raw Java values of the Spanner type (Key.Builder.appendObject)
    static Object toKeyPart(final Schema.Field field, final JsonElement element) {
        if(element == null || element.isJsonNull()) {
            return null;
        }
        if(field == null) {
            return element.isJsonPrimitive() ? element.getAsString() : element.toString();
        }
        return switch (field.getType().getTypeName()) {
            case BOOLEAN -> element.getAsBoolean();
            case STRING -> RowSchemaUtil.hasSpannerType(field.getOptions(), "UUID")
                    ? UUID.fromString(element.getAsString())
                    : element.getAsString();
            case INT64, INT32, INT16, BYTE -> element.getAsLong();
            case FLOAT -> element.getAsFloat();
            case DOUBLE -> element.getAsDouble();
            case DECIMAL -> new BigDecimal(element.getAsString());
            case BYTES -> ByteArray.copyFrom(Base64.getDecoder().decode(element.getAsString()));
            case DATETIME -> Timestamp.parseTimestamp(element.getAsString());
            case LOGICAL_TYPE -> {
                if(RowSchemaUtil.isLogicalTypeDate(field.getType())) {
                    yield Date.parseDate(element.getAsString());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(field.getType())) {
                    yield Timestamp.parseTimestamp(element.getAsString());
                }
                yield element.getAsString();
            }
            default -> element.getAsString();
        };
    }

    /**
     * Collapses the changes of one transaction to at most one mutation per (table, keys) —
     * Spanner rejects several mutations of the same row within one commit. The changes of a
     * row are folded in {@code sequence} order: the {@code after} values of successive upserts
     * are merged (a later partial UPDATE only overrides the columns it carries), and a DELETE
     * discards what came before it. The result is ordered by the sequence at which each row
     * <em>first</em> appeared in the transaction: Spanner checks parent/child (interleave, FK)
     * constraints mutation by mutation within a commit, and the source order of first
     * appearance is known to satisfy them (it did on the source database), whereas ordering by
     * the latest sequence could move a parent's later UPDATE behind its child's INSERT.
     */
    public static List<Map<String, Object>> collapse(final Iterable<Map<String, Object>> envelopes) {
        final List<Map<String, Object>> ordered = new ArrayList<>();
        for(final Map<String, Object> envelope : envelopes) {
            if(isApplicable(ChangeRecord.getOp(envelope.get(ChangeRecord.FIELD_OP)))) {
                ordered.add(envelope);
            }
        }
        ordered.sort((a, b) -> ChangeRecord.compareSequence(
                a.get(ChangeRecord.FIELD_SEQUENCE).toString(), b.get(ChangeRecord.FIELD_SEQUENCE).toString()));

        // insertion order of the map = first appearance of each row
        final Map<String, Map<String, Object>> folded = new java.util.LinkedHashMap<>();
        for(final Map<String, Object> envelope : ordered) {
            final String key = envelope.get(ChangeRecord.FIELD_TABLE) + "#" + envelope.get(ChangeRecord.FIELD_KEYS);
            final Map<String, Object> previous = folded.get(key);
            final ChangeRecord.Op op = ChangeRecord.getOp(envelope.get(ChangeRecord.FIELD_OP));
            if(previous == null || ChangeRecord.Op.DELETE.equals(op)
                    || ChangeRecord.Op.DELETE.equals(ChangeRecord.getOp(previous.get(ChangeRecord.FIELD_OP)))) {
                folded.put(key, envelope);
                continue;
            }
            // upsert after upsert: merge the after values
            final Map<String, Object> merged = new java.util.HashMap<>(envelope);
            final JsonObject after = new JsonObject();
            for(final Object json : new Object[]{ previous.get(ChangeRecord.FIELD_AFTER), envelope.get(ChangeRecord.FIELD_AFTER) }) {
                final JsonObject values = parseObject(json);
                if(values != null) {
                    for(final Map.Entry<String, JsonElement> entry : values.entrySet()) {
                        after.add(entry.getKey(), entry.getValue());
                    }
                }
            }
            merged.put(ChangeRecord.FIELD_AFTER, after.toString());
            folded.put(key, merged);
        }
        return new ArrayList<>(folded.values());
    }

    private static JsonObject parseObject(final Object value) {
        if(value == null) {
            return null;
        }
        final String json = value.toString();
        if(json.isBlank()) {
            return null;
        }
        final JsonElement parsed = JsonParser.parseString(json);
        return parsed.isJsonObject() ? parsed.getAsJsonObject() : null;
    }

}
