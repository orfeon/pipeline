package com.mercari.solution.util.pipeline.lookup;

import com.mercari.solution.util.schema.converter.ElementToRowConverter;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Exposes one {@link LookupSource} table to <b>Beam SQL</b> (the {@code beamsql}
 * module) through Beam's native lookup mechanism: a join against this table is
 * planned as {@code BeamSideInputLookupJoinRel} and each input row triggers one
 * {@link #seekRow} call, answered here by a single-point {@link LookupSource#lookup}.
 *
 * <p>Semantics differ from the {@code query} module's lookup-join: <b>point
 * equi-joins only</b> (no key-prefix or range fetches, no LATERAL), and one
 * backend request per input row (Beam calls {@code seekRow} row by row — there
 * is no batching or key deduplication). The join columns must together form one
 * of the table's candidate keys (matched by name, any order); the join
 * condition must put the main-input column on the <b>left</b> of each equality
 * ({@code input.userId = lk.USER_ID}) — the reverse order fails inside Beam's
 * join planning.
 *
 * <p>Lifecycle mirrors the query module: the source's table metadata is derived
 * once at pipeline construction (build the tables via {@link #tablesOf} between
 * {@code source.setup()} and {@code source.close()}); each worker's join DoFn
 * gets its own deserialized copy and re-opens clients in {@link #setUp}.
 */
public class LookupSeekableTable extends SchemaBaseBeamTable implements BeamSqlSeekableTable {

    private final LookupSource source;
    private final String table;
    private final com.mercari.solution.module.Schema tableSchema;

    private LookupSeekableTable(final LookupSource source, final String table,
            final com.mercari.solution.module.Schema tableSchema) {
        super(beamSchema(tableSchema));
        this.source = source;
        this.table = table;
        this.tableSchema = tableSchema;
    }

    /**
     * The table's Beam schema. DATE / TIME columns are surfaced as ISO-8601
     * STRINGs: Beam's seekable-join output builder re-converts logical-type
     * values it has already converted, so any {@code java.time} logical column
     * in the seek row breaks the join — {@code CAST} in SQL when the temporal
     * type is needed. TIMESTAMP is Beam's primitive DATETIME and passes through.
     */
    private static Schema beamSchema(final com.mercari.solution.module.Schema tableSchema) {
        final Schema.Builder builder = Schema.builder();
        for (final com.mercari.solution.module.Schema.Field field : tableSchema.getFields()) {
            final Schema.FieldType fieldType = switch (field.getFieldType().getType()) {
                case date, time -> Schema.FieldType.STRING.withNullable(true);
                default -> ElementToRowConverter.convertFieldType(field.getFieldType());
            };
            builder.addField(field.getName(), fieldType);
        }
        return builder.build();
    }

    /**
     * One seekable table per table of the source. The source must be
     * {@code setup()} (metadata derived); the returned tables serialize the
     * source and re-open it per worker.
     */
    public static Map<String, BeamSqlTable> tablesOf(final LookupSource source) {
        final Map<String, BeamSqlTable> tables = new LinkedHashMap<>();
        for (final Map.Entry<String, com.mercari.solution.module.Schema> entry
                : source.tableSchemas().entrySet()) {
            tables.put(entry.getKey(),
                    new LookupSeekableTable(source, entry.getKey(), entry.getValue()));
        }
        return tables;
    }

    @Override
    public void setUp(final Schema joinSubsetType) {
        source.setup();
    }

    @Override
    public void tearDown() {
        source.close();
    }

    @Override
    public List<Row> seekRow(final Row keyRow) {
        // The sub-row carries the equi-join columns by NAME; they must together
        // form one of the table's candidate keys (any column order).
        final List<String> seekColumns = keyRow.getSchema().getFieldNames();
        final Set<String> seekColumnSet = new HashSet<>(seekColumns);
        LookupKey matched = null;
        for (final LookupKey candidate : source.keyCandidates(table)) {
            if (candidate.columns().size() == seekColumnSet.size()
                    && seekColumnSet.containsAll(candidate.columns())) {
                matched = candidate;
                break;
            }
        }
        if (matched == null) {
            throw new IllegalStateException("Lookup table '" + source.getName() + "." + table
                    + "' requires the join columns to form one of its keys "
                    + source.keyCandidates(table).stream().map(LookupKey::columns).toList()
                    + ", but the join uses " + seekColumns);
        }

        // Key values in candidate-key order, converted to Calcite-internal values.
        final List<Object> keyValues = new ArrayList<>(matched.columns().size());
        for (final String column : matched.columns()) {
            final Object value = toInternal(fieldType(column), keyRow.getValue(column));
            if (value == null) {
                return List.of(); // a null key component never matches
            }
            keyValues.add(value);
        }

        final Iterable<Object[]> rows = source.lookupWithCache(table, matched.indexName(),
                LookupBatch.of(List.of(LookupRequest.point(keyValues))), null);

        // The lookup contract allows a superset; filter to exact key matches.
        final int[] keyIndexes = new int[matched.columns().size()];
        for (int i = 0; i < keyIndexes.length; i++) {
            keyIndexes[i] = fieldIndex(matched.columns().get(i));
        }
        final List<Row> result = new ArrayList<>();
        for (final Object[] values : rows) {
            boolean exact = true;
            for (int i = 0; i < keyIndexes.length; i++) {
                if (!Objects.equals(values[keyIndexes[i]], keyValues.get(i))) {
                    exact = false;
                    break;
                }
            }
            if (exact) {
                result.add(toBeamRow(values));
            }
        }
        return result;
    }

    @Override
    public PCollection<Row> buildIOReader(final PBegin begin) {
        throw new UnsupportedOperationException("Lookup table '" + source.getName() + "."
                + table + "' can only be used as the lookup side of a join on its key;"
                + " standalone scans are not supported");
    }

    @Override
    public POutput buildIOWriter(final PCollection<Row> input) {
        throw new UnsupportedOperationException(
                "Lookup table '" + source.getName() + "." + table + "' is read-only");
    }

    @Override
    public PCollection.IsBounded isBounded() {
        return PCollection.IsBounded.BOUNDED;
    }

    // ------------------------------------------------------------------
    // Value conversion
    // ------------------------------------------------------------------

    private com.mercari.solution.module.Schema.FieldType fieldType(final String column) {
        final int index = fieldIndex(column);
        return tableSchema.getField(index).getFieldType();
    }

    private int fieldIndex(final String column) {
        for (int i = 0; i < tableSchema.countFields(); i++) {
            if (tableSchema.getField(i).getName().equals(column)) {
                return i;
            }
        }
        throw new IllegalStateException(
                "unknown column '" + column + "' of lookup table " + table);
    }

    /** Beam Row value → Calcite-internal value, by the table's field type. */
    private static Object toInternal(
            final com.mercari.solution.module.Schema.FieldType fieldType, final Object value) {
        if (value == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool, string, json, enumeration -> value;
            case int8, int16 -> ((Number) value).shortValue();
            case int32 -> ((Number) value).intValue();
            case int64 -> ((Number) value).longValue();
            case float32, float64 -> ((Number) value).doubleValue();
            case decimal -> value instanceof BigDecimal d ? d : new BigDecimal(value.toString());
            case date -> switch (value) {
                case LocalDate localDate -> (int) localDate.toEpochDay();
                case String s -> (int) LocalDate.parse(s).toEpochDay();
                case Number number -> number.intValue();
                default -> value;
            };
            case time -> switch (value) {
                case LocalTime localTime -> (int) (localTime.toNanoOfDay() / 1_000_000L);
                case String s -> (int) (LocalTime.parse(s).toNanoOfDay() / 1_000_000L);
                case Number number -> number.intValue();
                default -> value;
            };
            case timestamp, datetime -> switch (value) {
                case org.joda.time.base.AbstractInstant instant -> instant.getMillis();
                case java.time.Instant instant -> instant.toEpochMilli();
                case Number number -> number.longValue();
                default -> value;
            };
            case bytes -> value instanceof byte[] bytes ? new ByteString(bytes) : value;
            default -> value;
        };
    }

    /**
     * A fetched row (Calcite-internal values, full column order) → Beam Row.
     * Values are attached (not re-converted) in their logical-type INPUT form:
     * Beam's seekable-join output builder re-runs the logical-type conversion
     * on this row's values, so pre-converted base values (e.g. epoch-day Long
     * for DATE) would be double-converted and fail.
     */
    private Row toBeamRow(final Object[] values) {
        final List<Object> converted = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++) {
            converted.add(toBeamValue(tableSchema.getField(i).getFieldType(), values[i]));
        }
        return Row.withSchema(getSchema()).attachValues(converted);
    }

    private static Object toBeamValue(
            final com.mercari.solution.module.Schema.FieldType fieldType, final Object value) {
        if (value == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool, string, json, enumeration, decimal -> value;
            case int8 -> ((Number) value).byteValue();
            case int16 -> ((Number) value).shortValue();
            case int32 -> ((Number) value).intValue();
            case int64 -> ((Number) value).longValue();
            case float32 -> ((Number) value).floatValue();
            case float64 -> ((Number) value).doubleValue();
            // date/time surface as ISO-8601 strings (see beamSchema)
            case date -> LocalDate.ofEpochDay(((Number) value).longValue()).toString();
            case time -> LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L).toString();
            case timestamp, datetime ->
                    org.joda.time.Instant.ofEpochMilli(((Number) value).longValue());
            case bytes -> value instanceof ByteString byteString ? byteString.getBytes() : value;
            case array -> {
                final List<Object> converted = new ArrayList<>();
                for (final Object element : (Iterable<?>) value) {
                    converted.add(toBeamValue(fieldType.getArrayValueType(), element));
                }
                yield converted;
            }
            default -> value;
        };
    }
}
