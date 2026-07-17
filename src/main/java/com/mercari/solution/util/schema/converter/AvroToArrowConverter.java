package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Converts Avro schemas / GenericRecord values to Arrow schemas / vectors.
 * The Avro schema produced by {@link com.mercari.solution.module.Schema#getAvroSchema()}
 * (and values produced by {@link ElementToAvroConverter}) is the canonical intermediate,
 * so every MElement backing type is covered by a single conversion path.
 */
public class AvroToArrowConverter {

    public static org.apache.arrow.vector.types.pojo.Schema convertSchema(final org.apache.avro.Schema avroSchema) {
        final List<Field> fields = new ArrayList<>();
        for(final org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            fields.add(convertField(field.name(), field.schema()));
        }
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    private static Field convertField(final String name, final org.apache.avro.Schema schema) {
        final boolean nullable = AvroSchemaUtil.isNullable(schema);
        final org.apache.avro.Schema fieldSchema = AvroSchemaUtil.unnestUnion(schema);
        final LogicalType logicalType = fieldSchema.getLogicalType();
        return switch (fieldSchema.getType()) {
            case BOOLEAN -> field(name, nullable, ArrowType.Bool.INSTANCE);
            case INT -> {
                if(LogicalTypes.date().equals(logicalType)) {
                    yield field(name, nullable, new ArrowType.Date(DateUnit.DAY));
                } else if(LogicalTypes.timeMillis().equals(logicalType)) {
                    yield field(name, nullable, new ArrowType.Time(TimeUnit.MILLISECOND, 32));
                }
                yield field(name, nullable, new ArrowType.Int(32, true));
            }
            case LONG -> {
                if(LogicalTypes.timestampMicros().equals(logicalType)) {
                    yield field(name, nullable, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
                } else if(LogicalTypes.timestampMillis().equals(logicalType)) {
                    yield field(name, nullable, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
                } else if(LogicalTypes.timeMicros().equals(logicalType)) {
                    yield field(name, nullable, new ArrowType.Time(TimeUnit.MICROSECOND, 64));
                }
                yield field(name, nullable, new ArrowType.Int(64, true));
            }
            case FLOAT -> field(name, nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            case DOUBLE -> field(name, nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case STRING, ENUM -> field(name, nullable, ArrowType.Utf8.INSTANCE);
            case BYTES, FIXED -> {
                if(logicalType instanceof LogicalTypes.Decimal decimal) {
                    yield field(name, nullable, new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), 128));
                } else if(org.apache.avro.Schema.Type.FIXED.equals(fieldSchema.getType())) {
                    yield field(name, nullable, new ArrowType.FixedSizeBinary(fieldSchema.getFixedSize()));
                }
                yield field(name, nullable, ArrowType.Binary.INSTANCE);
            }
            case RECORD -> {
                final List<Field> children = new ArrayList<>();
                for(final org.apache.avro.Schema.Field child : fieldSchema.getFields()) {
                    children.add(convertField(child.name(), child.schema()));
                }
                yield new Field(name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null), children);
            }
            case ARRAY -> {
                final Field element = convertField(ListVector.DATA_VECTOR_NAME, fieldSchema.getElementType());
                yield new Field(name, new FieldType(nullable, ArrowType.List.INSTANCE, null), List.of(element));
            }
            case MAP -> {
                final Field key = new Field(MapVector.KEY_NAME,
                        new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
                final Field value = convertField(MapVector.VALUE_NAME, fieldSchema.getValueType());
                final Field entries = new Field(MapVector.DATA_VECTOR_NAME,
                        new FieldType(false, ArrowType.Struct.INSTANCE, null), List.of(key, value));
                yield new Field(name, new FieldType(nullable, new ArrowType.Map(false), null), List.of(entries));
            }
            default -> throw new IllegalArgumentException(
                    "avro type: " + fieldSchema.getType() + " is not supported for arrow field: " + name);
        };
    }

    private static Field field(final String name, final boolean nullable, final ArrowType type) {
        return new Field(name, new FieldType(nullable, type, null), null);
    }

    public static void setRecord(final VectorSchemaRoot root, final int index, final GenericRecord record) {
        for(final FieldVector vector : root.getFieldVectors()) {
            final String fieldName = vector.getField().getName();
            final Object value = record.getSchema().getField(fieldName) == null ? null : record.get(fieldName);
            setValue(vector, index, value);
        }
    }

    public static void setValue(final FieldVector vector, final int index, final Object value) {
        if(value == null) {
            setNull(vector, index);
            return;
        }
        switch (vector) {
            case BitVector v -> v.setSafe(index, Boolean.TRUE.equals(value) ? 1 : 0);
            case DateDayVector v -> v.setSafe(index, ((Number) value).intValue());
            case TimeMilliVector v -> v.setSafe(index, ((Number) value).intValue());
            case TimeMicroVector v -> v.setSafe(index, ((Number) value).longValue());
            case TimeStampVector v -> v.setSafe(index, ((Number) value).longValue());
            case IntVector v -> v.setSafe(index, ((Number) value).intValue());
            case BigIntVector v -> v.setSafe(index, ((Number) value).longValue());
            case Float4Vector v -> v.setSafe(index, ((Number) value).floatValue());
            case Float8Vector v -> v.setSafe(index, ((Number) value).doubleValue());
            case DecimalVector v -> v.setSafe(index, toBigDecimal(value, v.getScale()));
            case VarCharVector v -> v.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
            case FixedSizeBinaryVector v -> v.setSafe(index, toBytes(value));
            case VarBinaryVector v -> v.setSafe(index, toBytes(value));
            case StructVector v -> {
                final GenericRecord record = (GenericRecord) value;
                v.setIndexDefined(index);
                for(final String childName : v.getChildFieldNames()) {
                    final Object childValue = record.getSchema().getField(childName) == null ? null : record.get(childName);
                    setValue(v.getChild(childName), index, childValue);
                }
            }
            case MapVector v -> {
                final Map<?, ?> map = (Map<?, ?>) value;
                final int start = v.startNewValue(index);
                final StructVector entries = (StructVector) v.getDataVector();
                final FieldVector keyVector = entries.getChild(MapVector.KEY_NAME);
                final FieldVector valueVector = entries.getChild(MapVector.VALUE_NAME);
                int offset = start;
                for(final Map.Entry<?, ?> entry : map.entrySet()) {
                    entries.setIndexDefined(offset);
                    setValue(keyVector, offset, entry.getKey());
                    setValue(valueVector, offset, entry.getValue());
                    offset++;
                }
                v.endValue(index, map.size());
            }
            case ListVector v -> {
                final Collection<?> collection = (Collection<?>) value;
                final int start = v.startNewValue(index);
                final FieldVector dataVector = v.getDataVector();
                int offset = start;
                for(final Object element : collection) {
                    setValue(dataVector, offset, element);
                    offset++;
                }
                v.endValue(index, collection.size());
            }
            default -> throw new IllegalArgumentException(
                    "arrow vector type: " + vector.getClass().getName() + " is not supported");
        }
    }

    private static void setNull(final FieldVector vector, final int index) {
        switch (vector) {
            case BaseFixedWidthVector v -> v.setNull(index);
            case BaseVariableWidthVector v -> v.setNull(index);
            case BaseLargeVariableWidthVector v -> v.setNull(index);
            case ListVector v -> v.setNull(index);
            case StructVector v -> v.setNull(index);
            default -> throw new IllegalArgumentException(
                    "arrow vector type: " + vector.getClass().getName() + " is not supported");
        }
    }

    private static BigDecimal toBigDecimal(final Object value, final int scale) {
        return switch (value) {
            case BigDecimal b -> b;
            case ByteBuffer b -> new BigDecimal(new BigInteger(toBytes(b)), scale);
            case GenericFixed f -> new BigDecimal(new BigInteger(f.bytes()), scale);
            case byte[] b -> new BigDecimal(new BigInteger(b), scale);
            default -> throw new IllegalArgumentException("Illegal decimal value: " + value);
        };
    }

    private static byte[] toBytes(final Object value) {
        return switch (value) {
            case ByteBuffer b -> {
                final ByteBuffer duplicated = b.duplicate();
                final byte[] bytes = new byte[duplicated.remaining()];
                duplicated.get(bytes);
                yield bytes;
            }
            case GenericFixed f -> f.bytes();
            case byte[] b -> b;
            default -> throw new IllegalArgumentException("Illegal bytes value: " + value);
        };
    }

}
