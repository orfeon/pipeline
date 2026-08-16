package com.mercari.solution.util.pipeline.cdc;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;

import java.io.Serializable;
import java.util.ArrayList;
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
 * the full row is always {@code keys ∪ after}.</p>
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

    public static final String FIELD_SOURCE_PROVIDER = "provider";
    public static final String FIELD_SOURCE_DATABASE = "database";
    public static final String FIELD_SOURCE_METADATA = "metadata";

    /**
     * Change operation. {@code SNAPSHOT} represents initial-snapshot reads
     * (Debezium op {@code r}); apply-sinks treat it like {@code INSERT}.
     */
    public enum Op implements Serializable {

        INSERT(0),
        UPDATE(1),
        DELETE(2),
        SNAPSHOT(3);

        private final int id;

        Op(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
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
                .withField(FIELD_KEYS, Schema.FieldType.JSON.withNullable(false))
                .withField(FIELD_BEFORE, Schema.FieldType.JSON)
                .withField(FIELD_AFTER, Schema.FieldType.JSON)
                .withField(FIELD_COMMIT_TIMESTAMP, Schema.FieldType.TIMESTAMP.withNullable(false))
                .withField(FIELD_SEQUENCE, Schema.FieldType.STRING.withNullable(false))
                .withField(FIELD_SOURCE, Schema.FieldType.element(Schema.builder()
                        .withField(FIELD_SOURCE_PROVIDER, Schema.FieldType.STRING.withNullable(false))
                        .withField(FIELD_SOURCE_DATABASE, Schema.FieldType.STRING)
                        .withField(FIELD_SOURCE_METADATA, Schema.FieldType.JSON)
                        .build()))
                .build();
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
