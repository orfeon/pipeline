package com.mercari.solution.util.pipeline.lookup;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Conversions between the project's primitive value representation (what
 * {@link MElement#getPrimitiveValue} holds) and Calcite's <em>internal</em>
 * value representation, which is what flows through generated enumerable code:
 *
 * <ul>
 *   <li>VARCHAR → {@link String}, BOOLEAN → {@link Boolean}</li>
 *   <li>SMALLINT → {@link Short}, INTEGER → {@link Integer}, BIGINT → {@link Long}</li>
 *   <li>FLOAT/DOUBLE → {@link Double} (float32 is widened), DECIMAL → {@link BigDecimal}</li>
 *   <li>DATE → epoch-day {@link Integer}, TIME → millis-of-day {@link Integer},
 *       TIMESTAMP → epoch-millis {@link Long} (the project's epoch-micros primitives
 *       are converted; sub-millisecond precision is not observable inside SQL)</li>
 *   <li>BINARY/VARBINARY → Avatica {@link ByteString}</li>
 *   <li>ARRAY → {@link List}, ROW → {@code Object[]}</li>
 * </ul>
 *
 * <p>Both the in-memory input tables and every {@link LookupSource} produce
 * internal values, so join keys compare equal without further coercion.
 */
public final class CalciteValues {

    private CalciteValues() {
    }

    /** Project primitive value → Calcite-internal value, by schema field type. */
    public static Object toInternal(final Schema.FieldType fieldType, final Object primitive) {
        if (primitive == null) {
            return null;
        }
        return switch (fieldType.getType()) {
            case bool, string, json, enumeration -> primitive;
            case int8, int16 -> ((Number) primitive).shortValue();
            case int32 -> ((Number) primitive).intValue();
            case int64 -> ((Number) primitive).longValue();
            case float32, float64 -> ((Number) primitive).doubleValue();
            case decimal -> primitive instanceof BigDecimal d
                    ? d : new BigDecimal(primitive.toString());
            case date -> ((Number) primitive).intValue();
            case time -> (int) (((Number) primitive).longValue() / 1000L); // micros → millis
            case timestamp, datetime ->
                    Math.floorDiv(((Number) primitive).longValue(), 1000L); // micros → millis
            case bytes -> switch (primitive) {
                case ByteBuffer buffer -> {
                    final byte[] bytes = new byte[buffer.remaining()];
                    buffer.duplicate().get(bytes);
                    yield new ByteString(bytes);
                }
                case byte[] bytes -> new ByteString(bytes);
                case ByteString byteString -> byteString;
                default -> throw new IllegalArgumentException(
                        "Unsupported bytes value: " + primitive.getClass());
            };
            case array, matrix -> {
                // A matrix value is a flat list; its element type accessor differs.
                final Schema.FieldType valueType = Schema.Type.matrix.equals(fieldType.getType())
                        ? fieldType.getMatrixValueType() : fieldType.getArrayValueType();
                final List<Object> values = new ArrayList<>();
                for (final Object v : (Iterable<?>) primitive) {
                    values.add(toInternal(valueType, v));
                }
                yield values;
            }
            case element -> toInternalRow(fieldType.getElementSchema(), primitive);
            case map -> primitive;
            default -> throw new IllegalArgumentException(
                    "Unsupported field type for SQL: " + fieldType.getType());
        };
    }

    /** Nested element (MElement or Map) → Calcite ROW ({@code Object[]}) in schema field order. */
    private static Object[] toInternalRow(final Schema schema, final Object value) {
        final Object[] row = new Object[schema.countFields()];
        for (int i = 0; i < schema.countFields(); i++) {
            final Schema.Field field = schema.getField(i);
            final Object fieldValue = switch (value) {
                case MElement element -> element.getPrimitiveValue(field.getName());
                case Map<?, ?> map -> map.get(field.getName());
                default -> throw new IllegalArgumentException(
                        "Unsupported element value: " + value.getClass());
            };
            row[i] = toInternal(field.getFieldType(), fieldValue);
        }
        return row;
    }

    /** Converts one input element into a Calcite scan row in schema field order. */
    public static Object[] toInternalRow(final Schema schema, final MElement element) {
        final Object[] row = new Object[schema.countFields()];
        if (element == null) {
            return row;
        }
        for (int i = 0; i < schema.countFields(); i++) {
            final Schema.Field field = schema.getField(i);
            row[i] = toInternal(field.getFieldType(), element.getPrimitiveValue(field.getName()));
        }
        return row;
    }

    /**
     * Drains a result set of an executed query into per-row primitive maps
     * (the representation {@link MElement#ofList} accepts). Values rendered by
     * Avatica as local wall-clock {@link Timestamp}/{@link Date}/{@link Time}
     * objects are inverted back to epoch-based primitives.
     */
    public static List<Map<String, Object>> convertResultSet(final ResultSet resultSet) {
        try {
            final List<Map<String, Object>> results = new ArrayList<>();
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                final Map<String, Object> values = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    values.put(metaData.getColumnName(i),
                            toPrimitive(metaData.getColumnType(i), resultSet.getObject(i)));
                }
                results.add(values);
            }
            return results;
        } catch (final SQLException e) {
            throw new IllegalStateException("Failed to convert query result set", e);
        }
    }

    private static Object toPrimitive(final int sqlType, final Object value) {
        if (value == null) {
            return null;
        }
        return switch (sqlType) {
            case Types.BIT, Types.BOOLEAN -> value;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> ((Number) value).intValue();
            case Types.BIGINT -> ((Number) value).longValue();
            case Types.REAL -> ((Number) value).floatValue();
            case Types.FLOAT, Types.DOUBLE -> ((Number) value).doubleValue();
            case Types.DECIMAL, Types.NUMERIC -> value;
            case Types.DATE -> switch (value) {
                case Date date -> (int) date.toLocalDate().toEpochDay();
                case Number number -> number.intValue();
                default -> value;
            };
            case Types.TIME -> switch (value) {
                // millis/micros of day
                case Time time -> time.toLocalTime().toNanoOfDay() / 1000L;
                case Number number -> number.longValue() * 1000L;
                default -> value;
            };
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> switch (value) {
                // Avatica renders the internal epoch-millis as local wall-clock;
                // invert via LocalDateTime at UTC, then millis → micros.
                case Timestamp timestamp ->
                        timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
                case Number number -> number.longValue() * 1000L;
                default -> value;
            };
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> switch (value) {
                case byte[] bytes -> ByteBuffer.wrap(bytes);
                case ByteString byteString -> ByteBuffer.wrap(byteString.getBytes());
                case Blob blob -> {
                    try {
                        yield ByteBuffer.wrap(blob.getBytes(1, (int) blob.length()));
                    } catch (final SQLException e) {
                        throw new IllegalStateException(e);
                    }
                }
                default -> value;
            };
            case Types.ARRAY -> {
                final List<?> list = asList(value);
                if (list == null) {
                    yield value;
                }
                final List<Object> converted = new ArrayList<>(list.size());
                for (final Object element : list) {
                    converted.add(normalizeElement(element));
                }
                yield converted;
            }
            case Types.STRUCT, Types.JAVA_OBJECT -> switch (value) {
                case Object[] row -> List.of(row);
                default -> value;
            };
            default -> value;
        };
    }

    /**
     * An array value in whatever form the engine produced it (a {@link List},
     * or an Avatica {@link java.sql.Array} when it crossed a JDBC result
     * boundary) → a Calcite-internal {@link List}; null when not an array.
     */
    static List<?> asList(final Object value) {
        return switch (value) {
            case List<?> list -> list;
            case java.sql.Array array -> {
                try {
                    final Object values = array.getArray();
                    if (values instanceof Object[] objects) {
                        yield java.util.Arrays.asList(objects);
                    }
                    // A NOT NULL element type materializes as a primitive
                    // array (e.g. boolean[] from ARRAY<BOOLEAN NOT NULL>).
                    final int length = java.lang.reflect.Array.getLength(values);
                    final List<Object> out = new ArrayList<>(length);
                    for (int i = 0; i < length; i++) {
                        out.add(java.lang.reflect.Array.get(values, i));
                    }
                    yield out;
                } catch (final SQLException e) {
                    throw new IllegalStateException("failed to read array value", e);
                }
            }
            case null, default -> null;
        };
    }

    /** {@link #asList} + per-element normalization; passes non-arrays through. */
    static Object toInternalList(final Object value) {
        final List<?> list = asList(value);
        if (list == null) {
            return value;
        }
        final List<Object> converted = new ArrayList<>(list.size());
        for (final Object element : list) {
            converted.add(normalizeElement(element));
        }
        return converted;
    }

    /** Best-effort normalization for array elements (no per-element type metadata). */
    private static Object normalizeElement(final Object element) {
        return switch (element) {
            case null -> null;
            case Timestamp timestamp ->
                    timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
            case Date date -> (int) date.toLocalDate().toEpochDay();
            case Time time -> time.toLocalTime().toNanoOfDay() / 1000L;
            case ByteString byteString -> ByteBuffer.wrap(byteString.getBytes());
            default -> element;
        };
    }

    /** {@code [0, 1, ..., count-1]} — the "all columns" projection. */
    public static int[] allColumns(final int count) {
        final int[] all = new int[count];
        for (int i = 0; i < count; i++) {
            all[i] = i;
        }
        return all;
    }

    /**
     * Reorders {@code tables} into a name-keyed map preserving configuration order.
     */
    public static Map<String, Schema> tableMap(final List<String> names, final List<Schema> schemas) {
        final Map<String, Schema> map = new LinkedHashMap<>();
        for (int i = 0; i < names.size(); i++) {
            map.put(names.get(i), schemas.get(i));
        }
        return map;
    }
}
