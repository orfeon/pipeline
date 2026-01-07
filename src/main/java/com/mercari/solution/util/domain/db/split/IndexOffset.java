package com.mercari.solution.util.domain.db.split;

import com.google.gson.JsonObject;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.domain.db.CharCollation;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.commons.codec.binary.Hex;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@DefaultCoder(AvroCoder.class)
public class IndexOffset {

    @Nullable
    private String fieldName;
    @Nullable
    private Schema.Type fieldType;
    @Nullable
    private Boolean ascending;

    @Nullable
    private Boolean booleanValue;
    @Nullable
    private String stringValue;
    @Nullable
    private ByteBuffer bytesValue;
    @Nullable
    private Integer intValue;
    @Nullable
    private Long longValue;
    @Nullable
    private Float floatValue;
    @Nullable
    private Double doubleValue;

    @Nullable
    private String logicalType;

    @Nullable
    private Boolean isCaseSensitive;

    public String getFieldName() {
        return fieldName;
    }

    public Schema.Type getFieldType() {
        return fieldType;
    }

    public Boolean getAscending() {
        return ascending;
    }

    public Boolean getBooleanValue() {
        return booleanValue;
    }

    public String getStringValue() {
        return stringValue;
    }

    public ByteBuffer getBytesValue() {
        return bytesValue;
    }

    public Integer getIntValue() {
        return intValue;
    }

    public Long getLongValue() {
        return longValue;
    }

    public Float getFloatValue() {
        return floatValue;
    }

    public Double getDoubleValue() {
        return doubleValue;
    }

    public String getLogicalType() {
        return logicalType;
    }

    public Boolean getIsCaseSensitive() {
        return isCaseSensitive;
    }

    public String getAsString() {
        return Optional
                .ofNullable(getValue())
                .map(Object::toString)
                .orElse("");
    }

    public Object getValue() {
        return switch (this.fieldType) {
            case BOOLEAN -> this.booleanValue;
            case ENUM, STRING -> this.stringValue;
            case FIXED, BYTES -> this.bytesValue;
            case INT -> this.intValue;
            case LONG -> this.longValue;
            case FLOAT -> this.floatValue;
            case DOUBLE -> this.doubleValue;
            case NULL -> null;
            default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
        };
    }

    public boolean isGreaterThan(final IndexOffset another, CharCollation charCollation) {
        return compareTo(another, charCollation) > 0;
    }

    public boolean isLesserThan(final IndexOffset another, CharCollation charCollation) {
        return compareTo(another, charCollation) < 0;
    }

    public int compareTo(final IndexOffset another, CharCollation charCollation) {
        if(this.getValue() == null && another.getValue() == null) {
            return 0;
        } else if(this.getValue() == null) {
            return -1;
        } else if(another.getValue() == null) {
            return 1;
        }
        return switch (this.fieldType) {
            case BOOLEAN -> this.booleanValue.compareTo(another.getBooleanValue());
            case ENUM, STRING -> {
                if(isCaseSensitive) {
                    yield charCollation.compare(this.fieldName, this.stringValue, another.getStringValue());
                } else {
                    yield charCollation.compare(this.fieldName, this.stringValue, another.getStringValue());
                    //yield this.stringValue.compareToIgnoreCase(another.getStringValue());
                }
            }
            case FIXED, BYTES -> {
                if("decimal".equals(logicalType)) {
                    yield BigDecimal.valueOf(new BigInteger(this.bytesValue.array()).longValue(), 9)
                            .compareTo(BigDecimal.valueOf(new BigInteger(another.bytesValue.array()).longValue(), 9));
                }
                yield new String(Hex.encodeHex(this.bytesValue.array()))
                        .compareTo(new String(Hex.encodeHex(another.bytesValue.array())));
            }
            case INT -> this.intValue.compareTo(another.getIntValue());
            case LONG -> this.longValue.compareTo(another.getLongValue());
            case FLOAT -> this.floatValue.compareTo(another.getFloatValue());
            case DOUBLE -> this.doubleValue.compareTo(another.getDoubleValue());
            case NULL -> 0;
            default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
        };
    }

    public IndexOffset copy() {
        return of(this.fieldName, this.fieldType, this.ascending, this.getValue(), this.logicalType, this.isCaseSensitive);
    }

    @Override
    public String toString() {
        final JsonObject jsonObject = toJsonObject();
        return jsonObject.toString();
    }

    public JsonObject toJsonObject() {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(fieldName, valueToString());
        jsonObject.addProperty("type", fieldType.name());
        jsonObject.addProperty("order", ascending ? "ASC" : "DESC");
        return jsonObject;
    }

    public Map<String, Object> toMap() {
        final Map<String, Object> values = new HashMap<>();
        values.put("field", fieldName);
        values.put("value", valueToString());
        values.put("type", fieldType.name());
        values.put("logicalType", logicalType);
        return values;
    }

    public String valueToString() {
        if(getValue() == null) {
            return "null";
        }
        return switch (this.fieldType) {
            case BOOLEAN -> this.booleanValue.toString();
            case ENUM, STRING -> this.stringValue;
            case FIXED, BYTES -> {
                if ("decimal".equals(logicalType)) {
                    yield BigDecimal.valueOf(new BigInteger(this.bytesValue.array()).longValue(), 9).toString();
                }
                yield new String(Hex.encodeHex(this.bytesValue.array()));
            }
            case INT -> {
                if ("date".equals(logicalType)) {
                    yield LocalDate.ofEpochDay(this.intValue).toString();
                } else if ("time-millis".equals(logicalType)) {
                    yield LocalTime.ofNanoOfDay(1000000L * this.intValue).toString();
                }
                yield this.intValue.toString();
            }
            case LONG -> {
                if ("timestamp-micros".equals(logicalType)) {
                    yield DateTimeUtil.toLocalDateTime(this.longValue).toString();
                } else if ("time-micros".equals(logicalType)) {
                    yield LocalTime.ofNanoOfDay(1000L * this.intValue).toString();
                }
                yield this.longValue.toString();
            }
            case FLOAT -> this.floatValue.toString();
            case DOUBLE -> this.doubleValue.toString();
            case NULL -> null;
            default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
        };
    }

    public static IndexOffset empty(final IndexOffset base, final CharCollation charCollation) {
        final Object value = switch (base.fieldType) {
            case BOOLEAN -> base.ascending;
            case INT -> base.ascending ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            case LONG -> base.ascending ? Long.MAX_VALUE : Long.MIN_VALUE;
            case FLOAT -> base.ascending ? Float.MAX_VALUE : Float.MIN_VALUE;
            case DOUBLE -> base.ascending ? Double.MAX_VALUE : Double.MIN_VALUE;
            //case STRING, ENUM -> base.ascending ? charCollation.lastCollationAscii() : charCollation.firstCollationAscii();
            case STRING, ENUM -> null;
            //case BYTES -> base.ascending;
            default -> null;
        };
        return of(base.fieldName, base.fieldType, base.ascending, value, base.logicalType, base.isCaseSensitive);
    }

    public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value) {
        return of(fieldName, fieldType, ascending, value, null, true);
    }

    public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value, final String logicalType) {
        return of(fieldName, fieldType, ascending, value, logicalType, true);
    }

    public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value, final boolean isCaseSensitive) {
        return of(fieldName, fieldType, ascending, value, null, isCaseSensitive);
    }

    public static IndexOffset of(final String fieldName, final Schema.Type fieldType, final Boolean ascending, final Object value,
                                                final String logicalType, final boolean isCaseSensitive) {
        final IndexOffset indexOffset = new IndexOffset();
        indexOffset.fieldName = fieldName;
        indexOffset.fieldType = fieldType;
        indexOffset.ascending = ascending;
        indexOffset.logicalType = logicalType;
        indexOffset.isCaseSensitive = isCaseSensitive;
        switch (fieldType) {
            case BOOLEAN -> {
                indexOffset.booleanValue = (Boolean) value;
            }
            case ENUM, STRING -> {
                if(value == null) {
                    indexOffset.stringValue = null;
                } else {
                    indexOffset.stringValue = value.toString();
                }
            }
            case FIXED, BYTES -> {
                indexOffset.bytesValue = (ByteBuffer) value;
            }
            case INT -> {
                if(value == null) {
                    indexOffset.intValue = null;
                } else if(value instanceof Long) {
                    indexOffset.intValue = ((Long) value).intValue();
                } else {
                    indexOffset.intValue = (Integer) value;
                }
            }
            case LONG -> {
                indexOffset.longValue = (Long) value;
            }
            case FLOAT -> {
                indexOffset.floatValue = (Float) value;
            }
            case DOUBLE -> {
                indexOffset.doubleValue = (Double) value;
            }
            case NULL, UNION -> {}
            default -> throw new IllegalArgumentException("Not supported range type: " + fieldType);
        }
        return indexOffset;
    }

    public boolean isSame(final IndexOffset another) {
        if(another == null) {
            return false;
        }
        if(getValue() == null && another.getValue() == null) {
            return true;
        } else if(getValue() == null || another.getValue() == null) {
            return false;
        }
        return getValue().equals(another.getValue());
    }

    public static Schema createAvroSchema() {
        return SchemaBuilder.builder()
                .record("IndexPosition")
                .fields()
                .name("field").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("value").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("type").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("logicalType").type(Schema.createUnion(
                        Schema.create(Schema.Type.STRING),
                        Schema.create(Schema.Type.NULL)
                )).noDefault()
                .endRecord();
    }
}