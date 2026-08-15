package com.mercari.solution.util.schema;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.StructToJsonConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StructSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(StructSchemaUtil.class);
    private static final Pattern PATTERN_ARRAY_ELEMENT = Pattern.compile("(?<=\\<).*?(?=\\>)");
    private static final Pattern PATTERN_CHANGE_RECORD_TYPE_CODE = Pattern.compile("\"code\":\"([A-Z][A-Z0-9]*)\"");

    public static boolean hasField(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return false;
        }
        return hasField(struct.getType(), fieldName);
    }

    public static boolean hasField(final Type type, final String fieldName) {
        if(type == null || fieldName == null) {
            return false;
        }
        return type.getStructFields().stream()
                .anyMatch(f -> f.getName().equals(fieldName));
    }

    public static Object getValue(final Struct struct, final String fieldName) {
        if(struct.isNull(fieldName)) {
            return null;
        }
        return switch (struct.getColumnType(fieldName).getCode()) {
            case BOOL -> struct.getBoolean(fieldName);
            case BYTES -> struct.getBytes(fieldName).toByteArray();
            case STRING -> struct.getString(fieldName);
            case UUID -> struct.getUuid(fieldName).toString();
            case JSON -> struct.getJson(fieldName);
            case INT64 -> struct.getLong(fieldName);
            case FLOAT32 -> struct.getFloat(fieldName);
            case FLOAT64 -> struct.getDouble(fieldName);
            case NUMERIC -> struct.getBigDecimal(fieldName);
            case PG_JSONB, PG_NUMERIC -> struct.getString(fieldName);
            case DATE -> {
                final Date date = struct.getDate(fieldName);
                yield LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
            }
            case TIMESTAMP -> Instant.ofEpochMilli(Timestamps.toMillis(struct.getTimestamp(fieldName).toProto()));
            case STRUCT -> struct.getStruct(fieldName);
            case ARRAY -> switch (struct.getColumnType(fieldName).getArrayElementType().getCode()) {
                case BOOL -> struct.getBooleanList(fieldName);
                case BYTES -> struct.getBytesList(fieldName).stream().map(ByteArray::toByteArray).toList();
                case STRING -> struct.getStringList(fieldName);
                case UUID -> struct.getUuidList(fieldName).stream().map(java.util.UUID::toString).toList();
                case JSON -> struct.getJsonList(fieldName);
                case INT64 -> struct.getLongList(fieldName);
                case FLOAT32 -> struct.getFloatList(fieldName);
                case FLOAT64 -> struct.getDoubleList(fieldName);
                case NUMERIC -> struct.getBigDecimalList(fieldName);
                case PG_JSONB, PG_NUMERIC -> struct.getStringList(fieldName);
                case DATE -> struct.getDateList(fieldName).stream()
                        .map(date -> LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()))
                        .toList();
                case TIMESTAMP -> struct.getTimestampList(fieldName).stream()
                        .map(t -> Instant.ofEpochMilli(Timestamps.toMillis(t.toProto())))
                        .toList();
                case STRUCT -> struct.getStructList(fieldName);
                default -> throw new IllegalArgumentException();
            };
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(fieldName).getCode().name());
        };
    }

    public static Object getRawValue(final Struct struct, final String fieldName) {
        if(struct == null) {
            return null;
        }
        if(!hasField(struct, fieldName)) {
            return null;
        }
        if(struct.isNull(fieldName)) {
            return null;
        }
        return switch (struct.getColumnType(fieldName).getCode()) {
            case BOOL -> struct.getBoolean(fieldName);
            case BYTES -> struct.getBytes(fieldName);
            case STRING -> struct.getString(fieldName);
            case UUID -> struct.getUuid(fieldName);
            case JSON -> struct.getJson(fieldName);
            case INT64 -> struct.getLong(fieldName);
            case FLOAT32 -> struct.getFloat(fieldName);
            case FLOAT64 -> struct.getDouble(fieldName);
            case NUMERIC -> struct.getBigDecimal(fieldName);
            case PG_JSONB -> struct.getPgJsonb(fieldName);
            case PG_NUMERIC -> struct.getString(fieldName);
            case DATE -> struct.getDate(fieldName);
            case TIMESTAMP -> struct.getTimestamp(fieldName);
            case STRUCT -> struct.getStruct(fieldName);
            case ARRAY -> switch (struct.getColumnType(fieldName).getArrayElementType().getCode()) {
                case BOOL -> struct.getBooleanList(fieldName);
                case BYTES -> struct.getBytesList(fieldName);
                case STRING -> struct.getStringList(fieldName);
                case UUID -> struct.getUuidList(fieldName);
                case JSON -> struct.getJsonList(fieldName);
                case INT64 -> struct.getLongList(fieldName);
                case FLOAT32 -> struct.getFloatList(fieldName);
                case FLOAT64 -> struct.getDoubleList(fieldName);
                case NUMERIC -> struct.getBigDecimalList(fieldName);
                case PG_JSONB -> struct.getPgJsonbList(fieldName);
                case PG_NUMERIC -> struct.getStringList(fieldName);
                case DATE -> struct.getDateList(fieldName);
                case TIMESTAMP -> struct.getTimestampList(fieldName);
                case STRUCT -> struct.getStructList(fieldName);
                case ARRAY -> throw new IllegalArgumentException("Not supported array in array for field: " + fieldName);
                default -> throw new IllegalArgumentException();
            };
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(fieldName).getCode().name());
        };
    }

    public static Instant toInstant(final Object value) {
        final Timestamp timestamp = (Timestamp) value;
        return DateTimeUtil.toJodaInstant(timestamp);
    }

    public static Object getCSVLineValue(final Struct struct, final String field) {
        if(struct == null) {
            return null;
        }
        if(!hasField(struct, field)) {
            return null;
        }
        if(struct.isNull(field)) {
            return null;
        }
        return switch (struct.getColumnType(field).getCode()) {
            case BOOL -> struct.getBoolean(field);
            case BYTES -> struct.getBytes(field).toBase64();
            case STRING -> struct.getString(field);
            case UUID -> struct.getUuid(field).toString();
            case JSON -> struct.getJson(field);
            case INT64 -> struct.getLong(field);
            case FLOAT32 -> struct.getFloat(field);
            case FLOAT64 -> struct.getDouble(field);
            case NUMERIC -> struct.getBigDecimal(field);
            case PG_JSONB, PG_NUMERIC -> struct.getString(field);
            case DATE -> struct.getDate(field).toString();
            case TIMESTAMP -> struct.getTimestamp(field).toString();
            case STRUCT -> struct.getStruct(field);
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        };
    }

    public static Value getStructValue(final Struct struct, final String field) {
        if(struct == null) {
            return null;
        }
        if(!hasField(struct, field)) {
            return null;
        }
        if(struct.isNull(field)) {
            return null;
        }
        return switch (struct.getColumnType(field).getCode()) {
            case BOOL -> Value.bool(struct.getBoolean(field));
            case BYTES -> Value.bytes(struct.getBytes(field));
            case NUMERIC -> Value.numeric(struct.getBigDecimal(field));
            case PG_NUMERIC -> Value.pgNumeric(struct.getString(field));
            case PG_JSONB -> Value.pgJsonb(struct.getString(field));
            case STRING -> Value.string(struct.getString(field));
            case UUID -> Value.uuid(struct.getUuid(field));
            case JSON -> Value.json(struct.getJson(field));
            case INT64 -> Value.int64(struct.getLong(field));
            case FLOAT32 -> Value.float32(struct.getFloat(field));
            case FLOAT64 -> Value.float64(struct.getDouble(field));
            case DATE -> Value.date(struct.getDate(field));
            case TIMESTAMP -> Value.timestamp(struct.getTimestamp(field));
            case STRUCT -> Value.struct(struct.getColumnType(field), struct.getStruct(field));
            case ARRAY ->
                switch (struct.getColumnType(field).getArrayElementType().getCode()) {
                case BOOL -> Value.boolArray(struct.getBooleanArray(field));
                case BYTES -> Value.bytesArray(struct.getBytesList(field));
                case NUMERIC -> Value.numericArray(struct.getBigDecimalList(field));
                case PG_NUMERIC -> Value.pgNumericArray(struct.getStringList(field));
                case PG_JSONB -> Value.pgJsonbArray(struct.getStringList(field));
                case STRING -> Value.stringArray(struct.getStringList(field));
                case UUID -> Value.uuidArray(struct.getUuidList(field));
                case JSON -> Value.jsonArray(struct.getJsonList(field));
                case INT64 -> Value.int64Array(struct.getLongArray(field));
                case FLOAT32 -> Value.float32Array(struct.getFloatArray(field));
                case FLOAT64 -> Value.float64Array(struct.getDoubleArray(field));
                case DATE -> Value.dateArray(struct.getDateList(field));
                case TIMESTAMP -> Value.timestampArray(struct.getTimestampList(field));
                case STRUCT -> Value.structArray(struct.getColumnType(field).getArrayElementType(), struct.getStructList(field));
                case ARRAY, UNRECOGNIZED -> throw new IllegalArgumentException("ARRAY AND UNRECOGNIZED not supported");
                    default -> throw new IllegalArgumentException();
                };
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        };
    }

    public static String getAsString(final Object struct, final String field) {
        if(struct == null) {
            return null;
        }
        return getAsString((Struct) struct, field);
    }

    public static String getAsString(final Struct struct, final String field) {
        if(!hasField(struct, field)) {
            return null;
        }
        if(struct.isNull(field)) {
            return null;
        }
        return switch (struct.getColumnType(field).getCode()) {
            case BOOL -> Boolean.toString(struct.getBoolean(field));
            case BYTES -> struct.getBytes(field).toBase64();
            case STRING, PG_JSONB, PG_NUMERIC -> struct.getString(field);
            case UUID -> struct.getUuid(field).toString();
            case JSON -> struct.getJson(field);
            case INT64 -> Long.toString(struct.getLong(field));
            case FLOAT32 -> Float.toString(struct.getFloat(field));
            case FLOAT64 -> Double.toString(struct.getDouble(field));
            case NUMERIC -> struct.getBigDecimal(field).toString();
            case DATE -> struct.getDate(field).toString();
            case TIMESTAMP -> struct.getTimestamp(field).toString();
            case STRUCT -> StructToJsonConverter.convert(struct.getStruct(field));
            case ARRAY -> "[" + switch (struct.getColumnType(field).getArrayElementType().getCode()) {
                case BOOL -> Arrays.toString(struct.getBooleanArray(field));
                case BYTES -> struct.getBytesList(field).stream().map(ByteArray::toBase64).collect(Collectors.joining(","));
                case STRING, PG_JSONB, PG_NUMERIC -> String.join(",", struct.getStringList(field));
                case UUID -> struct.getUuidList(field).stream().map(java.util.UUID::toString).collect(Collectors.joining(","));
                case JSON -> String.join(",", struct.getJsonList(field));
                case INT64 -> struct.getLongList(field).stream().map(l -> Long.toString(l)).collect(Collectors.joining(","));
                case FLOAT32 -> struct.getFloatList(field).stream().map(l -> Float.toString(l)).collect(Collectors.joining(","));
                case FLOAT64 -> struct.getDoubleList(field).stream().map(l -> Double.toString(l)).collect(Collectors.joining(","));
                case NUMERIC -> struct.getBigDecimalList(field).stream().map(BigDecimal::toString).collect(Collectors.joining(","));
                case DATE -> struct.getDateList(field).stream().map(Date::toString).collect(Collectors.joining(","));
                case TIMESTAMP -> struct.getTimestampList(field).stream().map(Timestamp::toString).collect(Collectors.joining(","));
                case STRUCT -> struct.getStructList(field).stream().map(StructToJsonConverter::convert).collect(Collectors.joining(","));
                default -> throw new IllegalArgumentException("Not supported array column type: " + struct.getColumnType(field).getCode().name());
            } + "]";
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        };
    }

    public static Long getAsLong(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        return switch (struct.getColumnType(field).getCode()) {
            case BOOL -> struct.getBoolean(field) ? 1L: 0L;
            case STRING -> {
                try {
                    yield Long.valueOf(struct.getString(field));
                } catch (Exception e) {
                    yield null;
                }
            }
            case INT64 -> struct.getLong(field);
            case FLOAT32 -> Float.valueOf(struct.getFloat(field)).longValue();
            case FLOAT64 -> Double.valueOf(struct.getDouble(field)).longValue();
            case NUMERIC -> struct.getBigDecimal(field).longValue();
            case PG_NUMERIC -> new BigDecimal(struct.getString(field)).longValue();
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        };
    }

    public static Double getAsDouble(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        return switch (struct.getColumnType(field).getCode()) {
            case BOOL -> struct.getBoolean(field) ? 1D: 0D;
            case STRING -> {
                try {
                    yield Double.valueOf(struct.getString(field));
                } catch (Exception e) {
                    yield null;
                }
            }
            case INT64 -> Long.valueOf(struct.getLong(field)).doubleValue();
            case FLOAT32 -> Float.valueOf(struct.getFloat(field)).doubleValue();
            case FLOAT64 -> struct.getDouble(field);
            case NUMERIC -> struct.getBigDecimal(field).doubleValue();
            case PG_NUMERIC -> new BigDecimal(struct.getString(field)).doubleValue();
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        };
    }

    public static BigDecimal getAsBigDecimal(final Struct struct, final String field) {
        if(struct.isNull(field)) {
            return null;
        }
        return switch (struct.getColumnType(field).getCode()) {
            case BOOL -> BigDecimal.valueOf(struct.getBoolean(field) ? 1D: 0D);
            case STRING -> {
                try {
                    yield BigDecimal.valueOf(Double.valueOf(struct.getString(field)));
                } catch (Exception e) {
                    yield null;
                }
            }
            case INT64 -> BigDecimal.valueOf(struct.getLong(field));
            case FLOAT32 -> BigDecimal.valueOf(struct.getFloat(field));
            case FLOAT64 -> BigDecimal.valueOf(struct.getDouble(field));
            case NUMERIC -> struct.getBigDecimal(field);
            case PG_NUMERIC -> new BigDecimal(struct.getString(field));
            case DATE, TIMESTAMP, BYTES, JSON, STRUCT, ARRAY -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        };
    }

    // for bigtable
    public static ByteString getAsByteString(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return null;
        }
        if(!StructSchemaUtil.hasField(struct, fieldName)) {
            return null;
        }
        final Type.StructField field = struct.getType().getStructFields().stream().filter(f -> f.getName().equals(fieldName)).findAny().get();

        return switch (field.getType().getCode()) {
            case BOOL -> BigtableSchemaUtil.toByteString(struct.getBoolean(fieldName));
            case STRING -> BigtableSchemaUtil.toByteString(struct.getString(fieldName));
            case JSON -> BigtableSchemaUtil.toByteString(struct.getJson(fieldName));
            case BYTES -> BigtableSchemaUtil.toByteString(struct.getBytes(fieldName).toByteArray());
            case INT64 -> BigtableSchemaUtil.toByteString(struct.getLong(fieldName));
            case FLOAT32 -> BigtableSchemaUtil.toByteString(struct.getFloat(fieldName));
            case FLOAT64 -> BigtableSchemaUtil.toByteString(struct.getDouble(fieldName));
            case NUMERIC -> BigtableSchemaUtil.toByteString(struct.getBigDecimal(fieldName));
            case PG_NUMERIC -> BigtableSchemaUtil.toByteString(struct.getString(fieldName));
            case DATE -> BigtableSchemaUtil.toByteString(DateTimeUtil.toEpochDay(struct.getDate(fieldName)));
            case TIMESTAMP -> BigtableSchemaUtil.toByteString(Timestamps.toMicros(struct.getTimestamp(fieldName).toProto()));
            default -> null;
        };
    }

    public static byte[] getBytes(final Struct struct, final String fieldName) {
        final Type.StructField field = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny().orElse(null);
        if(field == null) {
            return null;
        }
        return switch (field.getType().getCode()) {
            case BYTES -> struct.getBytes(field.getName()).toByteArray();
            case STRING -> Base64.getDecoder().decode(struct.getString(fieldName));
            case JSON -> Base64.getDecoder().decode(struct.getJson(fieldName));
            default -> null;
        };
    }

    public static ByteBuffer getAsBytes(final Struct struct, final String fieldName) {
        final Type.StructField field = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny().orElse(null);
        if(field == null) {
            return null;
        }
        final byte[] bytes = switch (field.getType().getCode()) {
            case BYTES -> struct.getBytes(field.getName()).toByteArray();
            case STRING -> Base64.getDecoder().decode(struct.getString(fieldName));
            case JSON -> Base64.getDecoder().decode(struct.getJson(fieldName));
            default -> null;
        };
        return Optional
                .ofNullable(bytes)
                .map(ByteBuffer::wrap)
                .orElse(null);
    }

    public static List<Float> getAsFloatList(final Struct struct, final String fieldName) {
        if(struct == null || fieldName == null) {
            return new ArrayList<>();
        }
        if(!StructSchemaUtil.hasField(struct, fieldName)) {
            return new ArrayList<>();
        }
        final Type.StructField field = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny()
                .get();
        if(!Type.Code.ARRAY.equals(field.getType().getCode())) {
            return new ArrayList<>();
        }

        return switch (field.getType().getArrayElementType().getCode()) {
            case BOOL -> struct.getBooleanList(fieldName).stream()
                    .map(b -> Optional.ofNullable(b).orElse(false) ? 1F : 0F)
                    .collect(Collectors.toList());
            case STRING, PG_NUMERIC -> struct.getStringList(fieldName).stream()
                    .map(s -> Float.valueOf(Optional.ofNullable(s).orElse("0")))
                    .collect(Collectors.toList());
            case INT64 -> struct.getLongList(fieldName).stream()
                    .map(s -> Float.valueOf(Optional.ofNullable(s).orElse(0L)))
                    .collect(Collectors.toList());
            case FLOAT32 -> struct.getFloatList(fieldName).stream()
                    .map(s -> Optional.ofNullable(s).orElse(0F))
                    .collect(Collectors.toList());
            case FLOAT64 -> struct.getDoubleList(fieldName).stream()
                    .map(s -> Optional.ofNullable(s).orElse(0D).floatValue())
                    .collect(Collectors.toList());
            case NUMERIC -> struct.getBigDecimalList(fieldName).stream()
                    .map(s -> Optional.ofNullable(s).orElse(BigDecimal.ZERO).floatValue())
                    .collect(Collectors.toList());
            default -> new ArrayList<>();
        };
    }

    public static long getEpochDay(final Date date) {
        return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()).toEpochDay();
    }

    public static Instant getTimestamp(final Struct struct, final String field, final Instant timestampDefault) {
        if(struct.isNull(field)) {
            return timestampDefault;
        }
        return switch (struct.getColumnType(field).getCode()) {
            case STRING -> {
                final String stringValue = struct.getString(field);
                try {
                    yield DateTimeUtil.toJodaInstant(stringValue);
                } catch (IllegalArgumentException e) {
                    yield timestampDefault;
                }
            }
            case DATE -> {
                final Date date = struct.getDate(field);
                yield new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(),
                        0, 0, DateTimeZone.UTC).toInstant();
            }
            case TIMESTAMP -> Instant.ofEpochMilli(struct.getTimestamp(field).toSqlTimestamp().getTime());
            default -> throw new IllegalArgumentException("Not supported column type: " + struct.getColumnType(field).getCode().name());
        };
    }

    public static Object getAsPrimitive(final Schema.FieldType fieldType, final Object fieldValue) {
        if(fieldValue == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case STRING, INT64, FLOAT, DOUBLE, BOOLEAN -> {
                return fieldValue;
            }
            case INT32 -> {
                return ((Long) fieldValue).intValue();
            }
            case DATETIME -> {
                return DateTimeUtil.toEpochMicroSecond((Timestamp) fieldValue);
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return DateTimeUtil.toEpochDay((Date)fieldValue);
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return DateTimeUtil.toLocalTime((String) fieldValue).toNanoOfDay() / 1000L;
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return fieldValue;
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE, ARRAY -> {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case INT64, FLOAT, DOUBLE, BOOLEAN, STRING -> {
                        return fieldValue;
                    }
                    case INT32 -> {
                        return ((List<Long>) fieldValue).stream()
                                .map(Long::intValue)
                                .collect(Collectors.toList());
                    }
                    case DATETIME -> {
                        return ((List<Timestamp>) fieldValue).stream()
                                .map(DateTimeUtil::toEpochMicroSecond)
                                .collect(Collectors.toList());
                    }
                    case LOGICAL_TYPE -> {
                        return ((List<Object>) fieldValue).stream()
                                .map(o -> {
                                    if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                                        return DateTimeUtil.toEpochDay((Date)o);
                                    } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                                        return DateTimeUtil.toLocalTime((String) o).toNanoOfDay() / 1000L;
                                    } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                                        return o;
                                    } else {
                                        throw new IllegalStateException();
                                    }
                                })
                                .collect(Collectors.toList());
                    }
                    default -> throw new IllegalStateException();
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static Object getAsPrimitive(Object struct, Schema.FieldType fieldType, String field) {
        if(struct == null) {
            return null;
        }
        if(field.contains(".")) {
            final String[] fields = field.split("\\.", 2);
            final String parentField = fields[0];
            final Object child = ((Struct) struct).getStruct(parentField);
            return getAsPrimitive(child, fieldType, fields[1]);
        }

        final Value value = ((Struct) struct).getValue(field);
        if(value == null || value.isNull()) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case BOOLEAN -> {
                return value.getBool();
            }
            case STRING -> {
                return value.getString();
            }
            case INT32 -> {
                return switch (value.getType().getCode()) {
                    case BOOL -> value.getBool() ? 1 : 0;
                    case STRING -> Integer.valueOf(value.getString());
                    case INT64 -> Long.valueOf(value.getInt64()).intValue();
                    case FLOAT32 -> Float.valueOf(value.getFloat32()).intValue();
                    case FLOAT64 -> Double.valueOf(value.getFloat64()).intValue();
                    case DATE -> DateTimeUtil.toEpochDay(value.getDate());
                    case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp()).intValue();
                    case NUMERIC -> value.getNumeric().intValue();
                    default -> throw new IllegalArgumentException();
                };
            }
            case INT64 -> {
                return switch (value.getType().getCode()) {
                    case BOOL -> value.getBool() ? 1L : 0L;
                    case STRING -> Long.valueOf(value.getString());
                    case INT64 -> value.getInt64();
                    case FLOAT32 -> Float.valueOf(value.getFloat32()).longValue();
                    case FLOAT64 -> Double.valueOf(value.getFloat64()).longValue();
                    case DATE -> DateTimeUtil.toEpochDay(value.getDate()).longValue();
                    case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp());
                    case NUMERIC -> value.getNumeric().longValue();
                    default -> throw new IllegalArgumentException();
                };
            }
            case FLOAT -> {
                return switch (value.getType().getCode()) {
                    case BOOL -> value.getBool() ? 1F : 0F;
                    case STRING -> Float.valueOf(value.getString());
                    case INT64 -> Long.valueOf(value.getInt64()).floatValue();
                    case FLOAT32 -> value.getFloat32();
                    case FLOAT64 -> Double.valueOf(value.getFloat64()).floatValue();
                    case DATE -> DateTimeUtil.toEpochDay(value.getDate()).floatValue();
                    case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp()).floatValue();
                    case NUMERIC -> value.getNumeric().floatValue();
                    default -> throw new IllegalArgumentException();
                };
            }
            case DOUBLE -> {
                return switch (value.getType().getCode()) {
                    case BOOL -> value.getBool() ? 1D : 0D;
                    case STRING -> Double.valueOf(value.getString());
                    case INT64 -> Long.valueOf(value.getInt64()).doubleValue();
                    case FLOAT32 -> Float.valueOf(value.getFloat32()).doubleValue();
                    case FLOAT64 -> value.getFloat64();
                    case DATE -> DateTimeUtil.toEpochDay(value.getDate()).doubleValue();
                    case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp()).doubleValue();
                    case NUMERIC -> value.getNumeric().doubleValue();
                    default -> throw new IllegalArgumentException();
                };
            }
            case DATETIME -> {
                return DateTimeUtil.toEpochMicroSecond(value.getTimestamp());
            }
            case LOGICAL_TYPE -> {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return DateTimeUtil.toEpochDay(value.getDate());
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return DateTimeUtil.toLocalTime(value.getString()).toNanoOfDay() / 1000L;
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    return value.getAsString();
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE, ARRAY -> {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case BOOLEAN -> {
                        return value.getBoolArray();
                    }
                    case STRING -> {
                        return value.getStringArray();
                    }
                    case INT32 -> {
                        return value.getInt64Array()
                                .stream()
                                .map(Long::intValue)
                                .collect(Collectors.toList());
                    }
                    case INT64 -> {
                        return value.getInt64Array();
                    }
                    case FLOAT -> {
                        return value.getFloat32Array();
                    }
                    case DOUBLE -> {
                        return value.getFloat64Array();
                    }
                    case DATETIME -> {
                        return value.getTimestampArray().stream()
                                .map(DateTimeUtil::toEpochMicroSecond)
                                .collect(Collectors.toList());
                    }
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            return value.getDateArray()
                                    .stream()
                                    .map(DateTimeUtil::toEpochDay)
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                            return value.getStringArray()
                                    .stream()
                                    .map(DateTimeUtil::toLocalTime)
                                    .map(t -> t.toNanoOfDay() / 1000L)
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                            throw new IllegalStateException();
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                    default -> throw new IllegalStateException();
                }
            }
            default -> throw new IllegalStateException();
        }
    }

    public static Object getAsPrimitive(Value value) {
        if(value == null || value.isNull()) {
            return null;
        }
        return switch (value.getType().getCode()) {
            case STRING -> value.getAsString();
            // the pipeline uuid field type holds its value as a String
            case UUID -> value.getUuid().toString();
            case BOOL -> value.getBool();
            case JSON -> value.getJson();
            case INT64 -> value.getInt64();
            case FLOAT32 -> value.getFloat32();
            case FLOAT64 -> value.getFloat64();
            case DATE -> DateTimeUtil.toEpochDay(value.getDate());
            case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp());
            case BYTES -> value.getBytes().toByteArray();
            case NUMERIC, PG_NUMERIC -> value.getNumeric();
            case PG_JSONB -> value.getPgJsonb();
            case STRUCT -> StructToJsonConverter.convert(value.getStruct());
            case ARRAY ->
                switch (value.getType().getArrayElementType().getCode()) {
                    case STRING -> value.getAsStringList();
                    case UUID -> value.getUuidArray().stream().map(java.util.UUID::toString).collect(Collectors.toList());
                    case BOOL -> value.getBoolArray();
                    case JSON -> value.getJsonArray();
                    case INT64 -> value.getInt64Array();
                    case FLOAT32 -> value.getFloat32Array();
                    case FLOAT64 -> value.getFloat64Array();
                    case DATE -> value.getDateArray().stream().map(DateTimeUtil::toEpochDay).collect(Collectors.toList());
                    case TIMESTAMP -> value.getTimestampArray().stream().map(DateTimeUtil::toEpochMicroSecond).collect(Collectors.toList());
                    case BYTES -> value.getBytesArray().stream().map(ByteArray::toByteArray).collect(Collectors.toList());
                    case NUMERIC, PG_NUMERIC -> value.getNumericArray();
                    case PG_JSONB -> value.getPgJsonbArray();
                    case STRUCT -> value.getStructArray().stream().map(StructToJsonConverter::convert).collect(Collectors.toList());
                    default -> throw new IllegalArgumentException();
                };
            default -> throw new IllegalArgumentException();
        };
    }

    public static Object getAsStandard(final Value value) {
        if(value == null || value.isNull()) {
            return null;
        }
        return switch (value.getType().getCode()) {
            case STRING -> value.getAsString();
            case UUID -> value.getUuid().toString();
            case BOOL -> value.getBool();
            case JSON -> value.getJson();
            case INT64 -> value.getInt64();
            case FLOAT32 -> value.getFloat32();
            case FLOAT64 -> value.getFloat64();
            case DATE -> DateTimeUtil.toLocalDate(value.getDate());
            case TIMESTAMP -> DateTimeUtil.toInstant(value.getTimestamp());
            case BYTES -> ByteBuffer.wrap(value.getBytes().toByteArray());
            case NUMERIC, PG_NUMERIC -> value.getNumeric();
            case PG_JSONB -> value.getPgJsonb();
            case STRUCT -> asStandardMap(value.getStruct(), null);
            case ARRAY ->
                    switch (value.getType().getArrayElementType().getCode()) {
                        case STRING -> value.getAsStringList();
                        case UUID -> value.getUuidArray().stream().map(java.util.UUID::toString).collect(Collectors.toList());
                        case BOOL -> value.getBoolArray();
                        case JSON -> value.getJsonArray();
                        case INT64 -> value.getInt64Array();
                        case FLOAT32 -> value.getFloat32Array();
                        case FLOAT64 -> value.getFloat64Array();
                        case DATE -> value.getDateArray().stream().map(DateTimeUtil::toLocalDate).collect(Collectors.toList());
                        case TIMESTAMP -> value.getTimestampArray().stream().map(DateTimeUtil::toInstant).collect(Collectors.toList());
                        case BYTES -> value.getBytesArray().stream().map(ByteArray::toByteArray).map(ByteBuffer::wrap).collect(Collectors.toList());
                        case NUMERIC, PG_NUMERIC -> value.getNumericArray();
                        case PG_JSONB -> value.getPgJsonbArray();
                        case STRUCT -> value.getStructArray().stream().map(s -> StructSchemaUtil.asStandardMap(s, null)).collect(Collectors.toList());
                        default -> throw new IllegalArgumentException();
                    };
            default -> throw new IllegalArgumentException();
        };
    }

    public static Object convertPrimitive(Schema.FieldType fieldType, Object primitiveValue) {
        if (primitiveValue == null) {
            return null;
        }
        switch (fieldType.getTypeName()) {
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BOOLEAN:
                return primitiveValue;
            case DATETIME: {
                return Timestamp.ofTimeMicroseconds((Long)primitiveValue);
            }
            case LOGICAL_TYPE: {
                if (RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return Date.parseDate(LocalDate.ofEpochDay((Integer) primitiveValue).toString());
                } else if (RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return LocalTime.ofNanoOfDay(1000L * (Long) primitiveValue).toString();
                } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType)) {
                    final int index = (Integer) primitiveValue;
                    return fieldType.getLogicalType(EnumerationType.class).valueOf(index);
                } else {
                    throw new IllegalStateException();
                }
            }
            case ITERABLE:
            case ARRAY: {
                switch (fieldType.getCollectionElementType().getTypeName()) {
                    case INT32, INT64, FLOAT, DOUBLE, STRING, BOOLEAN -> {
                        return primitiveValue;
                    }
                    case DATETIME -> {
                        return ((List<Long>) primitiveValue).stream()
                                .map(Timestamp::ofTimeMicroseconds)
                                .collect(Collectors.toList());
                    }
                    case LOGICAL_TYPE -> {
                        if (RowSchemaUtil.isLogicalTypeDate(fieldType.getCollectionElementType())) {
                            return ((List<Integer>) primitiveValue).stream()
                                    .map(LocalDate::ofEpochDay)
                                    .map(ld -> Date.fromYearMonthDay(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()))
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeTime(fieldType.getCollectionElementType())) {
                            return ((List<Long>) primitiveValue).stream()
                                    .map(us -> LocalTime.ofNanoOfDay(us * 1000L).toString())
                                    .collect(Collectors.toList());
                        } else if (RowSchemaUtil.isLogicalTypeEnum(fieldType.getCollectionElementType())) {
                            return ((List<Integer>) primitiveValue).stream()
                                    .map(index -> fieldType.getLogicalType(EnumerationType.class).valueOf(index))
                                    .collect(Collectors.toList());
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
            default:
                throw new IllegalStateException();
        }
    }

    public static Map<String, Object> asPrimitiveMap(final Struct struct) {
        return asPrimitiveMap(struct, null);
    }

    public static Map<String, Object> asPrimitiveMap(final Struct struct, final List<String> columns) {
        final Map<String, Object> primitiveMap = new HashMap<>();
        if(struct == null) {
            return primitiveMap;
        }
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(columns == null || columns.isEmpty() || columns.contains(field.getName())) {
                final Object value = getAsPrimitive(struct.getValue(field.getName()));
                primitiveMap.put(field.getName(), value);
            }
        }
        return primitiveMap;
    }

    public static Map<String, Object> asStandardMap(final Struct struct, final Collection<String> columns) {
        final Map<String, Object> standartMap = new HashMap<>();
        if(struct == null) {
            return standartMap;
        }
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(columns == null || columns.isEmpty() || (columns.contains(field.getName()) && !struct.isNull(field.getName()))) {
                final Object value = getAsStandard(struct.getValue(field.getName()));
                standartMap.put(field.getName(), value);
            }
        }
        return standartMap;
    }

    public static Type addStructField(final Type type, final List<Type.StructField> fields) {
        final List<Type.StructField> allFields = new ArrayList<>(type.getStructFields());
        allFields.addAll(fields);
        return Type.struct(allFields);
    }

    public static Type flatten(final Type type, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Type.StructField> fields = flattenFields(type, paths, null, addPrefix);
        return Type.struct(fields);
    }

    public static List<Struct> flatten(final Type type, final Struct struct, final String path, final boolean addPrefix) {
        final List<String> paths = Arrays.asList(path.split("\\."));
        final List<Map<String, Value>> values = flattenValues(struct, paths, null, addPrefix);
        return values.stream()
                .map(map -> {
                    final Struct.Builder builder = Struct.newBuilder();
                    map.entrySet().forEach(e -> {
                        if(e.getValue() != null) {
                            builder.set(e.getKey()).to(e.getValue());
                        }
                    });
                    return builder.build();
                })
                .collect(Collectors.toList());
    }

    public static Struct.Builder toBuilder(final Type type, final Struct struct) {
        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : type.getStructFields()) {
            if(!hasField(struct, field.getName())) {
                continue;
            }
            final Value value = getStructValue(struct, field.getName());
            if(value != null) {
                switch (field.getType().getCode()) {
                    case ARRAY -> {
                        if(field.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                            final List<Struct> children = new ArrayList<>();
                            for(final Struct child : struct.getStructList(field.getName())) {
                                if(child == null) {
                                    children.add(null);
                                } else {
                                    children.add(toBuilder(field.getType().getArrayElementType(), child).build());
                                }
                            }
                            builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), children);
                        } else {
                            builder.set(field.getName()).to(value);
                        }
                    }
                    case STRUCT -> {
                        final Struct child = toBuilder(field.getType(), struct.getStruct(field.getName())).build();
                        builder.set(field.getName()).to(child);
                    }
                    default -> builder.set(field.getName()).to(value);
                }
            } else {
                switch (field.getType().getCode()) {
                    case BOOL -> builder.set(field.getName()).to((Boolean)null);
                    case JSON, STRING -> builder.set(field.getName()).to((String)null);
                    case BYTES -> builder.set(field.getName()).to((ByteArray) null);
                    case INT64 -> builder.set(field.getName()).to((Long)null);
                    case FLOAT32 -> builder.set(field.getName()).to((Float)null);
                    case FLOAT64 -> builder.set(field.getName()).to((Double)null);
                    case NUMERIC -> builder.set(field.getName()).to((BigDecimal) null);
                    case PG_NUMERIC -> builder.set(field.getName()).to((String) null);
                    case DATE -> builder.set(field.getName()).to((Date)null);
                    case TIMESTAMP -> builder.set(field.getName()).to((Timestamp)null);
                    case STRUCT -> builder.set(field.getName()).to(field.getType(), null);
                    case ARRAY -> {
                        switch (field.getType().getArrayElementType().getCode()) {
                            case BOOL -> builder.set(field.getName()).toBoolArray((Iterable<Boolean>)null);
                            case BYTES -> builder.set(field.getName()).toBytesArray(null);
                            case STRING -> builder.set(field.getName()).toStringArray(null);
                            case JSON -> builder.set(field.getName()).toJsonArray(null);
                            case INT64 -> builder.set(field.getName()).toInt64Array((Iterable<Long>)null);
                            case FLOAT32 -> builder.set(field.getName()).toFloat32Array((Iterable<Float>)null);
                            case FLOAT64 -> builder.set(field.getName()).toFloat64Array((Iterable<Double>)null);
                            case NUMERIC -> builder.set(field.getName()).toNumericArray(null);
                            case PG_NUMERIC -> builder.set(field.getName()).toPgNumericArray(null);
                            case DATE -> builder.set(field.getName()).toDateArray(null);
                            case TIMESTAMP -> builder.set(field.getName()).toTimestampArray(null);
                            case STRUCT -> builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), null);
                            case ARRAY -> throw new IllegalStateException();
                            default -> throw new IllegalArgumentException();
                        }
                    }
                }
            }
        }
        return builder;
    }

    public static Struct.Builder toBuilder(final Struct struct) {
        return toBuilder(struct, null, null);
    }

    public static Struct.Builder toBuilder(final Struct struct,
                                           final Collection<String> includeFields,
                                           final Collection<String> excludeFields) {

        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : struct.getType().getStructFields()) {
            if(includeFields != null && !includeFields.contains(field.getName())) {
                continue;
            }
            if(excludeFields != null && excludeFields.contains(field.getName())) {
                continue;
            }
            if(struct.isNull(field.getName())) {
                switch (field.getType().getCode()) {
                    case BOOL -> builder.set(field.getName()).to((Boolean) null);
                    case STRING -> builder.set(field.getName()).to((String) null);
                    case JSON -> builder.set(field.getName()).to(Value.json(null));
                    case BYTES -> builder.set(field.getName()).to((ByteArray) null);
                    case INT64 -> builder.set(field.getName()).to((Long) null);
                    case FLOAT32 -> builder.set(field.getName()).to((Float) null);
                    case FLOAT64 -> builder.set(field.getName()).to((Double) null);
                    case NUMERIC -> builder.set(field.getName()).to((BigDecimal) null);
                    case PG_NUMERIC -> builder.set(field.getName()).to(Value.pgNumeric(null));
                    case TIMESTAMP -> builder.set(field.getName()).to((Timestamp) null);
                    case DATE -> builder.set(field.getName()).to((Date) null);
                    case STRUCT -> builder.set(field.getName()).to(field.getType(), null);
                    case ARRAY -> {
                        switch (field.getType().getArrayElementType().getCode()) {
                            case BOOL -> builder.set(field.getName()).toBoolArray((Iterable<Boolean>) null);
                            case STRING -> builder.set(field.getName()).toStringArray(null);
                            case JSON -> builder.set(field.getName()).toJsonArray(null);
                            case BYTES -> builder.set(field.getName()).toBytesArray(null);
                            case INT64 -> builder.set(field.getName()).toInt64Array((Iterable<Long>) null);
                            case FLOAT32 -> builder.set(field.getName()).toFloat32Array((Iterable<Float>) null);
                            case FLOAT64 -> builder.set(field.getName()).toFloat64Array((Iterable<Double>) null);
                            case NUMERIC -> builder.set(field.getName()).toNumericArray(null);
                            case PG_NUMERIC -> builder.set(field.getName()).toPgNumericArray(null);
                            case DATE -> builder.set(field.getName()).toDateArray(null);
                            case TIMESTAMP -> builder.set(field.getName()).toTimestampArray(null);
                            case STRUCT -> builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), null);
                            case ARRAY -> throw new IllegalStateException("Array in Array not supported for spanner struct: " + struct.getType());
                            default -> {}
                        }
                    }
                    default -> {}
                }
                continue;
            }
            switch (field.getType().getCode()) {
                case BOOL -> builder.set(field.getName()).to(struct.getBoolean(field.getName()));
                case STRING -> builder.set(field.getName()).to(struct.getString(field.getName()));
                case JSON -> builder.set(field.getName()).to(Value.json(struct.getJson(field.getName())));
                case BYTES -> builder.set(field.getName()).to(struct.getBytes(field.getName()));
                case INT64 -> builder.set(field.getName()).to(struct.getLong(field.getName()));
                case FLOAT32 -> builder.set(field.getName()).to(struct.getFloat(field.getName()));
                case FLOAT64 -> builder.set(field.getName()).to(struct.getDouble(field.getName()));
                case NUMERIC -> builder.set(field.getName()).to(struct.getBigDecimal(field.getName()));
                case PG_NUMERIC -> builder.set(field.getName()).to(Value.pgNumeric(struct.getString(field.getName())));
                case TIMESTAMP -> builder.set(field.getName()).to(struct.getTimestamp(field.getName()));
                case DATE -> builder.set(field.getName()).to(struct.getDate(field.getName()));
                case STRUCT -> builder.set(field.getName()).to(struct.getStruct(field.getName()));
                case ARRAY -> {
                    switch (field.getType().getArrayElementType().getCode()) {
                        case FLOAT32 -> builder.set(field.getName()).toFloat32Array(struct.getFloatList(field.getName()));
                        case FLOAT64 -> builder.set(field.getName()).toFloat64Array(struct.getDoubleList(field.getName()));
                        case BOOL -> builder.set(field.getName()).toBoolArray(struct.getBooleanList(field.getName()));
                        case INT64 -> builder.set(field.getName()).toInt64Array(struct.getLongList(field.getName()));
                        case STRING -> builder.set(field.getName()).toStringArray(struct.getStringList(field.getName()));
                        case JSON -> builder.set(field.getName()).toJsonArray(struct.getJsonList(field.getName()));
                        case BYTES -> builder.set(field.getName()).toBytesArray(struct.getBytesList(field.getName()));
                        case DATE -> builder.set(field.getName()).toDateArray(struct.getDateList(field.getName()));
                        case TIMESTAMP -> builder.set(field.getName()).toTimestampArray(struct.getTimestampList(field.getName()));
                        case NUMERIC -> builder.set(field.getName()).toNumericArray(struct.getBigDecimalList(field.getName()));
                        case PG_NUMERIC -> builder.set(field.getName()).toPgNumericArray(struct.getStringList(field.getName()));
                        case STRUCT -> builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), struct.getStructList(field.getName()));
                        case ARRAY -> throw new IllegalStateException("Array in Array not supported for spanner struct: " + struct.getType());
                        default -> {}
                    }
                }
                default -> {}
            }
        }
        return builder;
    }

    public static Struct.Builder toBuilder(final Type type,
                                           final Struct struct,
                                           final Map<String, String> renameFields) {

        final Struct.Builder builder = Struct.newBuilder();
        for(final Type.StructField field : type.getStructFields()) {
            final String getFieldName = renameFields.getOrDefault(field.getName(), field.getName());
            final String setFieldName = field.getName();
            if(!hasField(struct, getFieldName)) {
                if(renameFields.containsValue(setFieldName)) {
                    final String getOuterFieldName = renameFields.entrySet().stream()
                            .filter(e -> e.getValue().equals(setFieldName))
                            .map(Map.Entry::getKey)
                            .findAny()
                            .orElse(setFieldName);
                    if(struct.isNull(getOuterFieldName)) {
                        setNullValue(field, builder);
                        continue;
                    }
                    final Type.StructField rowField = struct.getType().getStructFields().stream().filter(f -> f.getName().equals(getOuterFieldName)).findAny().get();
                    if(!field.getType().getCode().equals(rowField.getType().getCode())) {
                        setNullValue(field, builder);
                        continue;
                    }

                    switch (field.getType().getCode()) {
                        case ARRAY: {
                            if(field.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                                final List<Struct> children = new ArrayList<>();
                                for(final Struct child : struct.getStructList(getOuterFieldName)) {
                                    if(child != null) {
                                        children.add(toBuilder(field.getType().getArrayElementType(), child).build());
                                    }
                                }
                                builder.set(setFieldName).toStructArray(field.getType().getArrayElementType(), children);
                            } else {
                                builder.set(setFieldName).to(struct.getValue(getOuterFieldName));
                            }
                            break;
                        }
                        case STRUCT: {
                            final Struct child = toBuilder(field.getType(), struct.getStruct(getOuterFieldName)).build();
                            builder.set(setFieldName).to(field.getType(), child);
                            break;
                        }
                        default: {
                            builder.set(setFieldName).to(struct.getValue(getOuterFieldName));
                            break;
                        }
                    }
                } else {
                    setNullValue(field, builder);
                }
            } else {
                final Value value = getStructValue(struct, getFieldName);
                if (value != null) {
                    switch (field.getType().getCode()) {
                        case ARRAY: {
                            if (field.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                                final List<Struct> children = new ArrayList<>();
                                for (final Struct child : struct.getStructList(getFieldName)) {
                                    if (child == null) {
                                        children.add(null);
                                    } else {
                                        children.add(toBuilder(field.getType().getArrayElementType(), child).build());
                                    }
                                }
                                builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), children);
                            } else {
                                builder.set(field.getName()).to(value);
                            }
                            break;
                        }
                        case STRUCT: {
                            final Struct child = toBuilder(field.getType(), struct.getStruct(getFieldName)).build();
                            builder.set(field.getName()).to(child);
                            break;
                        }
                        default:
                            builder.set(field.getName()).to(value);
                            break;
                    }
                } else {
                    setNullValue(field, builder);
                }
            }
        }

        return builder;
    }

    public static Type renameFields(final Type type, final Map<String, String> renameFields) {
        final List<Type.StructField> structFields = new ArrayList<>();
        for(Type.StructField field : type.getStructFields()) {
            if (renameFields.containsKey(field.getName())) {
                structFields.add(Type.StructField.of(renameFields.get(field.getName()), field.getType()));
            } else {
                structFields.add(field);
            }
        }
        return Type.struct(structFields);
    }

    public static Struct merge(final Type type, final Struct struct, Map<String, ? extends Object> values) {
        final Struct.Builder builder = Struct.newBuilder();
        for(Type.StructField field : type.getStructFields()) {
            if(values.containsKey(field.getName())) {
                builder.set(field.getName()).to(toValue(field.getType(), values.get(field.getName())));
            } else if(hasField(struct, field.getName())) {
                builder.set(field.getName()).to(struct.getValue(field.getName()));
            } else {
                builder.set(field.getName()).to(toValue(field.getType(), null));
            }
        }
        return builder.build();
    }

    public static Struct create(final Type type, Map<String, Object> values) {
        final Struct.Builder builder = Struct.newBuilder();
        for (final Type.StructField field : type.getStructFields()) {
            final Object value = values.get(field.getName());
            switch (field.getType().getCode()) {
                case BOOL -> builder.set(field.getName()).to((Boolean) value);
                case JSON, STRING -> builder.set(field.getName()).to((String) value);
                case BYTES -> builder.set(field.getName()).to((ByteArray) value);
                case INT64 -> {
                    if(value instanceof Integer) {
                        builder.set(field.getName()).to((Integer) value);
                    } else {
                        builder.set(field.getName()).to((Long) value);
                    }
                }
                case FLOAT32 -> {
                    if(value == null) {
                        builder.set(field.getName()).to((Float) null);
                    } else {
                        builder.set(field.getName()).to(((Number) value).floatValue());
                    }
                }
                case FLOAT64 -> {
                    if(value == null) {
                        builder.set(field.getName()).to((Double) null);
                    } else {
                        builder.set(field.getName()).to(((Number) value).doubleValue());
                    }
                }
                case NUMERIC -> builder.set(field.getName()).to((BigDecimal) value);
                case PG_NUMERIC -> builder.set(field.getName()).to((String) value);
                case DATE -> builder.set(field.getName()).to((Date) value);
                case TIMESTAMP -> builder.set(field.getName()).to((Timestamp) value);
                case STRUCT -> builder.set(field.getName()).to((Struct) value);
                case ARRAY -> {}
            }
        }
        return builder.build();
    }

    public static Type selectFields(Type type, final List<String> fields) {
        final List<Type.StructField> structFields = selectFieldsBuilder(type, fields);
        return Type.struct(structFields);
    }

    public static List<Type.StructField> selectFieldsBuilder(Type type, final List<String> fields) {
        final List<Type.StructField> structFields = new ArrayList<>();
        final Map<String, List<String>> childFields = new HashMap<>();
        for(String field : fields) {
            if(field.contains(".")) {
                final String[] strs = field.split("\\.", 2);
                if(childFields.containsKey(strs[0])) {
                    childFields.get(strs[0]).add(strs[1]);
                } else {
                    childFields.put(strs[0], new ArrayList<>(Arrays.asList(strs[1])));
                }
            } else {
                structFields.add(type.getStructFields().get(type.getFieldIndex(field)));
            }
        }

        if(!childFields.isEmpty()) {
            for(var entry : childFields.entrySet()) {
                final Type.StructField childField = type.getStructFields().get(type.getFieldIndex(entry.getKey()));
                switch (childField.getType().getCode()) {
                    case STRUCT: {
                        final Type childType = selectFields(childField.getType(), entry.getValue());
                        structFields.add(Type.StructField.of(entry.getKey(), childType));
                        break;
                    }
                    case ARRAY: {
                        if(!childField.getType().getArrayElementType().getCode().equals(Type.Code.STRUCT)) {
                            throw new IllegalStateException();
                        }
                        final Type childType = selectFields(childField.getType().getArrayElementType(), entry.getValue());
                        structFields.add(Type.StructField.of(entry.getKey(), Type.array(childType)));
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
        }
        return structFields;
    }

    public static Schema convertSchemaFromInformationSchema(final List<Struct> structs, final Collection<String> columnNames) {
        final Schema.Builder builder = Schema.builder();
        for(final Struct struct : structs) {
            if(columnNames != null && !columnNames.contains(struct.getString("COLUMN_NAME"))) {
                LOG.info("skipField: " + struct.getString("COLUMN_NAME"));
                continue;
            }
            builder.addField(Schema.Field.of(
                    struct.getString("COLUMN_NAME"),
                    convertFieldType(struct.getString("SPANNER_TYPE")))
                    .withNullable("YES".equals(struct.getString("IS_NULLABLE"))));
        }
        return builder.build();
    }

    public static Type convertTypeFromInformationSchema(final List<Struct> structs, final Collection<String> columnNames) {
        final List<Type.StructField> fields = new ArrayList<>();
        for(final Struct struct : structs) {
            if(columnNames != null && !columnNames.contains(struct.getString("COLUMN_NAME"))) {
                LOG.info("skipField: " + struct.getString("COLUMN_NAME"));
                continue;
            }
            fields.add(Type.StructField.of(
                    struct.getString("COLUMN_NAME"),
                    convertSchemaField(struct.getString("SPANNER_TYPE"))));
        }
        return Type.struct(fields);
    }

    private static Schema.FieldType convertFieldType(final String t) {
        final String type = t.trim().toUpperCase();
        return switch (type) {
            case "INT64" -> Schema.FieldType.INT64;
            case "FLOAT32" -> Schema.FieldType.FLOAT;
            case "FLOAT64" -> Schema.FieldType.DOUBLE;
            case "NUMERIC" -> Schema.FieldType.DECIMAL;
            case "BOOL" -> Schema.FieldType.BOOLEAN;
            case "JSON" -> Schema.FieldType.STRING;
            case "DATE" -> CalciteUtils.DATE;
            case "TIMESTAMP" -> Schema.FieldType.DATETIME;
            case "BYTES" -> Schema.FieldType.BYTES;
            default -> {
                if (type.startsWith("STRING")) {
                    yield Schema.FieldType.STRING;
                } else if (type.startsWith("BYTES")) {
                    yield Schema.FieldType.BYTES;
                } else if (type.startsWith("ARRAY")) {
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(type);
                    if (m.find()) {
                        yield Schema.FieldType.array(convertFieldType(m.group()).withNullable(true));
                    }
                }
                throw new IllegalStateException("DataType: " + type + " is not supported!");
            }
        };
    }

    public static Mutation adjust(final Type type, final Mutation mutation) {
        switch (mutation.getOperation()) {
            case INSERT, INSERT_OR_UPDATE -> {
                Mutation.WriteBuilder builder = Mutation.Op.INSERT.equals(mutation.getOperation())
                        ? Mutation.newInsertBuilder(mutation.getTable()) : Mutation.newInsertOrUpdateBuilder(mutation.getTable());
                final Map<String, Value> values = mutation.asMap();
                for(final Type.StructField field : type.getStructFields()) {
                    final Value value = values.get(field.getName());
                    if(value == null) { // for default value
                        continue;
                    }
                    if(field.getType().getCode().equals(value.getType().getCode())) {
                        builder = builder.set(field.getName()).to(value);
                    } else {
                        final Value adjustedValue = adjustValue(field.getType().getCode(), value);
                        builder = builder.set(field.getName()).to(adjustedValue);
                    }
                }
                return builder.build();
            }
            case UPDATE, REPLACE -> {
                Mutation.WriteBuilder builder = Mutation.Op.UPDATE.equals(mutation.getOperation())
                        ? Mutation.newUpdateBuilder(mutation.getTable()) : Mutation.newReplaceBuilder(mutation.getTable());

                final Map<String, Value> values = mutation.asMap();
                for(final Map.Entry<String, Value> entry : values.entrySet()) {
                    final Type.Code fieldTypeCode = type.getStructFields().stream()
                            .filter(f -> f.getName().equals(entry.getKey())).findAny()
                            .map(Type.StructField::getType)
                            .map(Type::getCode)
                            .orElse(Type.Code.UNRECOGNIZED);
                    final Value value = values.get(entry.getKey());
                    if(fieldTypeCode.equals(value.getType().getCode())) {
                        builder = builder.set(entry.getKey()).to(value);
                    } else {
                        final Value adjustedValue = adjustValue(fieldTypeCode, value);
                        builder = builder.set(entry.getKey()).to(adjustedValue);
                    }
                }
                return builder.build();
            }
            case DELETE -> {
                // TODO
                for(final Key key : mutation.getKeySet().getKeys()) {

                }
                return Mutation.delete(mutation.getTable(), mutation.getKeySet());
            }
            default -> {
                throw new IllegalStateException();
            }
        }
    }

    public static boolean validate(final Type type, final Mutation mutation) {
        if(type == null || mutation == null) {
            throw new IllegalArgumentException("Both type and mutation must not be null. type: " + type + ", mutation: " + mutation);
        }
        if(!Type.Code.STRUCT.equals(type.getCode())) {
            throw new IllegalArgumentException("Type must be struct. type: " + type + ", mutation: " + mutation);
        }

        switch (mutation.getOperation()) {
            case INSERT, INSERT_OR_UPDATE -> {
                final Map<String, Value> values = mutation.asMap();
                for(final Type.StructField field : type.getStructFields()) {
                    final Value value = values.get(field.getName());
                    if(value == null) {
                        return false;
                    }
                    if(!field.getType().getCode().equals(value.getType().getCode())) {
                        return false;
                    }
                }
                return true;
            }
            case UPDATE, REPLACE -> {
                final Map<String, Value> values = mutation.asMap();
                for(Map.Entry<String, Value> entry : values.entrySet()) {
                    final Type.Code fieldType = type.getStructFields().stream()
                            .filter(f -> f.getName().equals(entry.getKey())).findAny()
                            .map(Type.StructField::getType)
                            .map(Type::getCode)
                            .orElse(Type.Code.UNRECOGNIZED);
                    if(!fieldType.equals(entry.getValue().getType().getCode())) {
                        return false;
                    }
                }
                return true;
            }
            case DELETE -> {
                // TODO
                for(final Key key : mutation.getKeySet().getKeys()) {

                }
                return true;
            }
            default -> {
                throw new IllegalStateException("Not supported Mutation.Op: " + mutation.getOperation());
            }
        }
    }

    private static Value adjustValue(final Type.Code fieldTypeCode, Value value) {
        return switch (fieldTypeCode) {
            case STRING -> {
                if(value.isNull()) {
                    yield Value.string(null);
                }
                final String stringValue = switch (value.getType().getCode()) {
                    case STRING -> value.getString();
                    case JSON -> value.getJson();
                    case BYTES -> Base64.getEncoder().encodeToString(value.getBytes().toByteArray());
                    case BOOL -> Boolean.toString(value.getBool());
                    case INT64 -> Long.toString(value.getInt64());
                    case FLOAT32 -> Float.toString(value.getFloat32());
                    case FLOAT64 -> Double.toString(value.getFloat64());
                    case NUMERIC -> value.getNumeric().toPlainString();
                    case DATE -> value.getDate().toString();
                    case TIMESTAMP -> value.getTimestamp().toString();
                    case PG_JSONB -> value.getPgJsonb();
                    case PG_NUMERIC -> value.getString();
                    case STRUCT -> StructToJsonConverter.convert(value.getStruct());
                    default -> throw new IllegalArgumentException();
                };
                yield Value.string(stringValue);
            }
            case BOOL -> {
                if(value.isNull()) {
                    yield Value.bool(null);
                }
                final Boolean boolValue = switch (value.getType().getCode()) {
                    case BOOL -> value.getBool();
                    case STRING -> Boolean.parseBoolean(value.getString());
                    case INT64 -> value.getInt64() > 0;
                    case FLOAT32 -> value.getFloat32() > 0;
                    case FLOAT64 -> value.getFloat64() > 0;
                    default -> throw new IllegalArgumentException();
                };
                yield Value.bool(boolValue);
            }
            case INT64 -> {
                if(value.isNull()) {
                    yield Value.int64(null);
                }
                final Long longValue = switch (value.getType().getCode()) {
                    case BOOL -> value.getBool() ? 1L : 0L;
                    case STRING -> Long.parseLong(value.getString());
                    case INT64 -> value.getInt64();
                    case FLOAT32 -> Float.valueOf(value.getFloat32()).longValue();
                    case FLOAT64 -> Double.valueOf(value.getFloat64()).longValue();
                    case NUMERIC -> value.getNumeric().longValue();
                    case DATE -> DateTimeUtil.toEpochDay(value.getDate()).longValue();
                    case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp());
                    default -> throw new IllegalArgumentException();
                };
                yield Value.int64(longValue);
            }
            case FLOAT64 -> {
                if(value.isNull()) {
                    yield Value.float64(null);
                }
                final Double doubleValue = switch (value.getType().getCode()) {
                    case BOOL -> value.getBool() ? 1D : 0D;
                    case STRING -> Double.parseDouble(value.getString());
                    case INT64 -> Long.valueOf(value.getInt64()).doubleValue();
                    case FLOAT32 -> Float.valueOf(value.getFloat32()).doubleValue();
                    case FLOAT64 -> value.getFloat64();
                    case NUMERIC -> value.getNumeric().doubleValue();
                    case DATE -> DateTimeUtil.toEpochDay(value.getDate()).doubleValue();
                    case TIMESTAMP -> DateTimeUtil.toEpochMicroSecond(value.getTimestamp()).doubleValue();
                    default -> throw new IllegalArgumentException();
                };
                yield Value.float64(doubleValue);
            }
            case DATE -> {
                if(value.isNull()) {
                    yield Value.date(null);
                }
                final Date dateValue = switch (value.getType().getCode()) {
                    case STRING -> Date.parseDate(value.getString());
                    case INT64 -> {
                        final LocalDate localDate = LocalDate.ofEpochDay(value.getInt64());
                        yield Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                    }
                    case FLOAT32 -> {
                        final LocalDate localDate = LocalDate.ofEpochDay(Double.valueOf(value.getFloat32()).longValue());
                        yield Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                    }
                    case FLOAT64 -> {
                        final LocalDate localDate = LocalDate.ofEpochDay(Double.valueOf(value.getFloat64()).longValue());
                        yield Date.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                    }
                    case DATE -> value.getDate();
                    default -> throw new IllegalArgumentException();
                };
                yield Value.date(dateValue);
            }
            case TIMESTAMP -> {
                if(value.isNull()) {
                    yield Value.timestamp(null);
                }
                final Timestamp timestampValue = switch (value.getType().getCode()) {
                    case STRING -> Timestamp.parseTimestamp(value.getString());
                    case INT64 -> Timestamp.ofTimeMicroseconds(value.getInt64());
                    case FLOAT32 -> Timestamp.ofTimeMicroseconds(Float.valueOf(value.getFloat32()).longValue());
                    case FLOAT64 -> Timestamp.ofTimeMicroseconds(Double.valueOf(value.getFloat64()).longValue());
                    case TIMESTAMP -> value.getTimestamp();
                    default -> throw new IllegalArgumentException();
                };
                yield Value.timestamp(timestampValue);
            }
            default -> throw new IllegalStateException();
        };
    }

    public static String getChangeRecordTableName(final GenericRecord record) {
        final Object value = record.get("tableName");
        if(value == null) {
            throw new IllegalArgumentException();
        }
        return value.toString();
    }

    public static Long getChangeDataCommitTimestampMicros(final MElement unionValue) {
        return switch (unionValue.getType()) {
            case AVRO -> (Long)((GenericRecord) unionValue.getValue()).get("commitTimestamp");
            case ROW -> DateTimeUtil.toEpochMicroSecond(((Row) unionValue.getValue()).getDateTime("commitTimestamp"));
            default -> throw new IllegalArgumentException();
        };
    }

    public static List<KV<Key, GenericRecord>> createChangeRecordKey(
            final org.apache.avro.Schema tableSchema,
            final GenericRecord changeRecord) {

        final List<GenericRecord> mods = (List<GenericRecord>)changeRecord.get("mods");
        if(mods == null || mods.size() == 0) {
            throw new IllegalStateException("mods must not be null or zero size");
        }
        final Map<String, String> rowTypes = ((List<GenericRecord>) (changeRecord.get("rowType")))
                .stream()
                .collect(Collectors.toMap(
                        r -> r.get("name").toString(),
                        r -> r.get("Type").toString()));

        final List<KV<Key, GenericRecord>> keyAndChangeRecords = new ArrayList<>();
        for(final GenericRecord mod : mods) {
            Key.Builder keyBuilder = Key.newBuilder();
            final String keysJson = Optional.ofNullable(mod.get("keysJson")).orElse(new Utf8()).toString();
            final JsonObject keyObject = new Gson().fromJson(keysJson, JsonObject.class);
            for(final Map.Entry<String, JsonElement> entry : keyObject.entrySet()) {
                final String fieldName = entry.getKey();
                final JsonElement fieldValue = entry.getValue();
                final boolean isNull = fieldValue == null || fieldValue.isJsonNull();

                final String rowType = rowTypes.get(fieldName);
                if(rowType != null && !"TYPE_CODE_UNSPECIFIED".equalsIgnoreCase(rowType)) {
                    switch (rowType) {
                        case "BOOL" -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBoolean());
                        case "INT64" -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsLong());
                        case "FLOAT32", "FLOAT64" -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsDouble());
                        case "NUMERIC" -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBigDecimal());
                        case "DATE" -> keyBuilder = keyBuilder.append(isNull ? null : Date.parseDate(fieldValue.getAsString()));
                        case "TIMESTAMP" -> keyBuilder = keyBuilder.append(isNull ? null : Timestamp.parseTimestamp(fieldValue.getAsString()));
                        case "JSON", "STRING" -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                        case "BYTES" -> {
                            if(isNull) {
                                keyBuilder = keyBuilder.append((ByteArray) null);
                            } else {
                                final byte[] bytes = Base64.getDecoder().decode(fieldValue.getAsString());
                                keyBuilder = keyBuilder.append(ByteArray.copyFrom(bytes));
                            }
                        }
                        default -> throw new IllegalArgumentException("Not supported modType: " + rowType + " for field: " + fieldName);
                    }
                } else {
                    final org.apache.avro.Schema.Field field = tableSchema == null ? null : tableSchema.getField(fieldName);
                    if(field == null) {
                        throw new IllegalStateException("");
                    }

                    final org.apache.avro.Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                    switch (fieldSchema.getType()) {
                        case BOOLEAN -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBoolean());
                        case FIXED, BYTES -> {
                            if (AvroSchemaUtil.isLogicalTypeDecimal(fieldSchema)) {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsBigDecimal());
                            } else {
                                if(isNull) {
                                    keyBuilder = keyBuilder.append((ByteArray) null);
                                } else {
                                    final byte[] bytes = Base64.getDecoder().decode(fieldValue.getAsString());
                                    keyBuilder = keyBuilder.append(ByteArray.copyFrom(bytes));
                                }
                            }
                        }
                        case ENUM, STRING -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                        case INT -> {
                            if (LogicalTypes.date().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : Date.parseDate(fieldValue.getAsString()));
                            } else if (LogicalTypes.timeMillis().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                            } else {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsInt());
                            }
                        }
                        case LONG -> {
                            if (LogicalTypes.timestampMillis().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : Timestamp.parseTimestamp(fieldValue.getAsString()));
                            } else if (LogicalTypes.timestampMicros().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : Timestamp.parseTimestamp(fieldValue.getAsString()));
                            } else if (LogicalTypes.timeMicros().equals(fieldSchema.getLogicalType())) {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsString());
                            } else {
                                keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsLong());
                            }
                        }
                        case FLOAT, DOUBLE -> keyBuilder = keyBuilder.append(isNull ? null : fieldValue.getAsDouble());
                        default -> throw new IllegalStateException("Not supported fieldSchema: " + fieldSchema);
                    }
                }
            }

            final Key key = keyBuilder.build();
            final GenericRecord record = AvroSchemaUtil
                    .toBuilder(changeRecord)
                    .set("mods", Arrays.asList(mod))
                    .build();
            keyAndChangeRecords.add(KV.of(key, record));
        }

        return keyAndChangeRecords;
    }

    private static Type convertSchemaField(final String t) {
        final String type = t.trim().toUpperCase();
        return switch (type) {
            case "INT64" -> Type.int64();
            case "FLOAT32" -> Type.float32();
            case "FLOAT64" -> Type.float64();
            case "NUMERIC" -> Type.numeric();
            case "BOOL" -> Type.bool();
            case "JSON" -> Type.json();
            case "DATE" -> Type.date();
            case "TIMESTAMP" -> Type.timestamp();
            default -> {
                if (type.startsWith("STRING")) {
                    yield Type.string();
                } else if (type.startsWith("BYTES")) {
                    yield Type.bytes();
                } else if (type.startsWith("ARRAY")) {
                    final Matcher m = PATTERN_ARRAY_ELEMENT.matcher(type);
                    if (m.find()) {
                        yield Type.array(convertSchemaField(m.group()));
                    }
                }
                throw new IllegalStateException("DataType: " + type + " is not supported!");
            }
        };
    }

    public static Mutation.WriteBuilder createMutationWriteBuilder(final String table, final String mutationOp) {
        if(mutationOp == null) {
            return Mutation.newInsertOrUpdateBuilder(table);
        }
        return switch (mutationOp.trim().toUpperCase()) {
            case "INSERT" -> Mutation.newInsertBuilder(table);
            case "UPDATE" -> Mutation.newUpdateBuilder(table);
            case "INSERT_OR_UPDATE" -> Mutation.newInsertOrUpdateBuilder(table);
            case "REPLACE" -> Mutation.newReplaceBuilder(table);
            case "DELETE" -> throw new IllegalArgumentException("MutationOP(for insert) must not be DELETE!");
            default -> Mutation.newInsertOrUpdateBuilder(table);
        };
    }

    public static Mutation convert(final Type type,
                                   final Mutation mutation,
                                   final String table,
                                   final String mutationOp,
                                   final Iterable<String> keyFields,
                                   final List<String> allowCommitTimestampFields,
                                   final Set<String> excludeFields,
                                   final Set<String> hideFields) {
        return mutation;
    }

    public static List<String> getKeys(final Mutation mutation) {
        final List<String> keys = new ArrayList<>();
        if(Mutation.Op.DELETE.equals(mutation.getOperation()) && mutation.getKeySet() != null) {
            for (final Key key : mutation.getKeySet().getKeys()) {
                keys.add(key.toString());
            }
        }
        return keys;
    }

    public static String convertChangeRecordTypeCode(String code) {
        final Matcher m = PATTERN_CHANGE_RECORD_TYPE_CODE.matcher(code);
        if(m.find()) {
            return m.group(1);
        }
        return code;
    }

    private static List<Type.StructField> flattenFields(final Type type, final List<String> paths, final String prefix, final boolean addPrefix) {
        return type.getStructFields().stream()
                .flatMap(f -> {
                    final String name;
                    if(addPrefix) {
                        name = (prefix == null ? "" : prefix + "_") + f.getName();
                    } else {
                        name = f.getName();
                    }

                    if(paths.isEmpty() || !f.getName().equals(paths.get(0))) {
                        return Stream.of(Type.StructField.of(name, f.getType()));
                    }

                    if(Type.Code.ARRAY.equals(f.getType().getCode())) {
                        final Type elementType = f.getType().getArrayElementType();
                        if(Type.Code.STRUCT.equals(elementType.getCode())) {
                            return flattenFields(
                                    elementType,
                                    paths.subList(1, paths.size()), name, addPrefix)
                                    .stream();
                        } else {
                            return Stream.of(Type.StructField.of(f.getName(), elementType));
                        }
                    } else {
                        return Stream.of(Type.StructField.of(name, f.getType()));
                    }
                })
                .collect(Collectors.toList());
    }

    private static Value toValue(final Type type, final Object value) {
        return switch (type.getCode()) {
            case BOOL -> Value.bool((Boolean) value);
            case STRING -> Value.string((String) value);
            case BYTES -> Value.bytes((ByteArray) value);
            case JSON -> Value.json((String) value);
            case INT64 -> {
                if(value instanceof Integer) {
                    yield Value.int64((Integer) value);
                } else {
                    yield Value.int64((Long) value);
                }
            }
            case FLOAT32 -> {
                if(value instanceof Double) {
                    yield Value.float32(((Double) value).floatValue());
                } else {
                    yield Value.float32((Float) value);
                }
            }
            case FLOAT64 -> {
                if(value instanceof Float) {
                    yield Value.float64((Float) value);
                } else {
                    yield Value.float64((Double) value);
                }
            }
            case NUMERIC -> Value.numeric((BigDecimal) value);
            case DATE -> Value.date((Date) value);
            case TIMESTAMP -> Value.timestamp((Timestamp) value);
            case STRUCT -> {
                if(value == null) {
                    yield Value.struct(type, null);
                }
                final Struct child = (Struct) value;
                final Struct mergedChild = merge(type, child, new HashMap<>());
                yield Value.struct(type, mergedChild);
            }
            case ARRAY -> switch (type.getArrayElementType().getCode()) {
                    case BOOL -> Value.boolArray((Iterable<Boolean>) value);
                    case STRING -> Value.stringArray((Iterable<String>) value);
                    case BYTES -> Value.bytesArray((Iterable<ByteArray>) value);
                    case JSON -> Value.jsonArray((Iterable<String>) value);
                    case INT64 -> Value.int64Array((Iterable<Long>) value);
                    case FLOAT32 -> Value.float32Array((Iterable<Float>) value);
                    case FLOAT64 -> Value.float64Array((Iterable<Double>) value);
                    case NUMERIC -> Value.numericArray((Iterable<BigDecimal>) value);
                    case DATE -> Value.dateArray((Iterable<Date>) value);
                    case TIMESTAMP -> Value.timestampArray((Iterable<Timestamp>) value);
                    case STRUCT -> {
                        final List<Struct> children = new ArrayList<>();
                        for(final Struct child : (Iterable<Struct>) value) {
                            final Struct mergedChild = merge(type.getArrayElementType(), child, new HashMap<>());
                            children.add(mergedChild);
                        }
                        yield Value.structArray(type.getArrayElementType(), children);
                    }
                    case ARRAY -> throw new IllegalArgumentException("Array in Array is not supported");
                    default -> throw new IllegalArgumentException();
                };
            default -> throw new IllegalArgumentException("Not supported struct type: " + type);
        };
    }

    private static void setNullValue(final Type.StructField field, Struct.Builder builder) {
        switch (field.getType().getCode()) {
            case BOOL -> builder.set(field.getName()).to((Boolean)null);
            case JSON, STRING -> builder.set(field.getName()).to((String)null);
            case BYTES -> builder.set(field.getName()).to((ByteArray) null);
            case INT64 -> builder.set(field.getName()).to((Long)null);
            case FLOAT32 -> builder.set(field.getName()).to((Float) null);
            case FLOAT64 -> builder.set(field.getName()).to((Double)null);
            case NUMERIC -> builder.set(field.getName()).to((BigDecimal) null);
            case DATE -> builder.set(field.getName()).to((Date)null);
            case TIMESTAMP -> builder.set(field.getName()).to((Timestamp)null);
            case STRUCT -> builder.set(field.getName()).to(field.getType(), null);
            case ARRAY -> {
                switch (field.getType().getArrayElementType().getCode()) {
                    case BOOL -> builder.set(field.getName()).toBoolArray((Iterable<Boolean>)null);
                    case BYTES -> builder.set(field.getName()).toBytesArray(null);
                    case STRING -> builder.set(field.getName()).toStringArray(null);
                    case JSON -> builder.set(field.getName()).toJsonArray(null);
                    case INT64 -> builder.set(field.getName()).toInt64Array((Iterable<Long>)null);
                    case FLOAT32 -> builder.set(field.getName()).toFloat32Array((Iterable<Float>)null);
                    case FLOAT64 -> builder.set(field.getName()).toFloat64Array((Iterable<Double>)null);
                    case NUMERIC -> builder.set(field.getName()).toNumericArray(null);
                    case DATE -> builder.set(field.getName()).toDateArray(null);
                    case TIMESTAMP -> builder.set(field.getName()).toTimestampArray(null);
                    case STRUCT -> builder.set(field.getName()).toStructArray(field.getType().getArrayElementType(), null);
                    case ARRAY -> throw new IllegalStateException();
                }
            }
        }
    }

    private static List<Map<String, Value>> flattenValues(final Struct struct, final List<String> paths, final String prefix, final boolean addPrefix) {
        final Map<String, Value> properties = new HashMap<>();
        Type.StructField pathField = null;
        String pathName = null;
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String name;
            if(addPrefix) {
                name = (prefix == null ? "" : prefix + "_") + field.getName();
            } else {
                name = field.getName();
            }
            if(paths.isEmpty() || !field.getName().equals(paths.get(0))) {
                properties.put(name, getStructValue(struct, field.getName()));
            } else {
                pathName = name;
                pathField = field;
            }
        }

        if(pathField == null) {
            return Arrays.asList(properties);
        }

        if(getStructValue(struct, pathField.getName()) == null) {
            return Arrays.asList(properties);
        }

        if(Type.Code.ARRAY.equals(pathField.getType().getCode())) {
            final List<Map<String, Value>> arrayValues = new ArrayList<>();
            final Value array = getStructValue(struct, pathField.getName());
            if(Type.Code.STRUCT.equals(pathField.getType().getArrayElementType().getCode())) {
                try {
                    final Method getStructArray = array.getClass().getDeclaredMethod("getStructArray");
                    getStructArray.setAccessible(true);
                    final Object structArray = getStructArray.invoke(array);
                    for (final Struct child : (List<Struct>)structArray) {
                        final List<Map<String, Value>> list = flattenValues(
                                child, paths.subList(1, paths.size()), pathName, addPrefix);
                        for(Map<String, Value> unnestedChildProperties : list) {
                            unnestedChildProperties.putAll(properties);
                            arrayValues.add(unnestedChildProperties);
                        }
                    }
                    return arrayValues;
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            } else {
                properties.put(pathName, getStructValue(struct, pathField.getName()));
            }
        } else {
            properties.put(pathName, getStructValue(struct, pathField.getName()));
        }

        final Struct.Builder builder = Struct.newBuilder();
        properties.entrySet().forEach(e -> builder.set(e.getKey()).to(e.getValue()));
        return Arrays.asList(Collections.singletonMap(pathName, Value.struct(builder.build())));
    }

}
