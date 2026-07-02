package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class StructToRowConverterTest {

    private static final Type CHILD_TYPE = Type.struct(
            Type.StructField.of("childName", Type.string()),
            Type.StructField.of("childAge", Type.int64()));

    private static Type createTestType() {
        return Type.struct(
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("boolField", Type.bool()),
                Type.StructField.of("int64Field", Type.int64()),
                Type.StructField.of("float32Field", Type.float32()),
                Type.StructField.of("float64Field", Type.float64()),
                Type.StructField.of("numericField", Type.numeric()),
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("childField", CHILD_TYPE),
                Type.StructField.of("stringArrayField", Type.array(Type.string())),
                Type.StructField.of("boolArrayField", Type.array(Type.bool())),
                Type.StructField.of("int64ArrayField", Type.array(Type.int64())),
                Type.StructField.of("float64ArrayField", Type.array(Type.float64())),
                Type.StructField.of("numericArrayField", Type.array(Type.numeric())),
                Type.StructField.of("bytesArrayField", Type.array(Type.bytes())),
                Type.StructField.of("dateArrayField", Type.array(Type.date())),
                Type.StructField.of("timestampArrayField", Type.array(Type.timestamp())),
                Type.StructField.of("childArrayField", Type.array(CHILD_TYPE)));
    }

    private static Struct createChildStruct(final String name, final long age) {
        return Struct.newBuilder()
                .set("childName").to(name)
                .set("childAge").to(age)
                .build();
    }

    private static Struct createTestStruct() {
        return Struct.newBuilder()
                .set("stringField").to("hello")
                .set("boolField").to(true)
                .set("int64Field").to(64L)
                .set("float32Field").to(1.5f)
                .set("float64Field").to(2.5d)
                .set("numericField").to(new BigDecimal("123.45"))
                .set("bytesField").to(ByteArray.copyFrom("bytes"))
                .set("dateField").to(Date.fromYearMonthDay(2024, 3, 1))
                .set("timestampField").to(Timestamp.parseTimestamp("2024-03-01T12:00:00Z"))
                .set("childField").to(createChildStruct("a", 1L))
                .set("stringArrayField").toStringArray(List.of("a", "b"))
                .set("boolArrayField").toBoolArray(List.of(true, false))
                .set("int64ArrayField").toInt64Array(List.of(1L, 2L))
                .set("float64ArrayField").toFloat64Array(List.of(2.5d))
                .set("numericArrayField").toNumericArray(List.of(new BigDecimal("1.5")))
                .set("bytesArrayField").toBytesArray(List.of(ByteArray.copyFrom("b1")))
                .set("dateArrayField").toDateArray(List.of(Date.fromYearMonthDay(2024, 3, 2)))
                .set("timestampArrayField").toTimestampArray(List.of(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")))
                .set("childArrayField").toStructArray(CHILD_TYPE, List.of(createChildStruct("b", 2L)))
                .build();
    }

    @Test
    public void testConvertSchema() {
        final Type type = createTestType();
        final Schema schema = StructToRowConverter.convertSchema(type);

        Assertions.assertEquals(Schema.TypeName.STRING, schema.getField("stringField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.BOOLEAN, schema.getField("boolField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.INT64, schema.getField("int64Field").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.FLOAT, schema.getField("float32Field").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DOUBLE, schema.getField("float64Field").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DECIMAL, schema.getField("numericField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.BYTES, schema.getField("bytesField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.LOGICAL_TYPE, schema.getField("dateField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DATETIME, schema.getField("timestampField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.ROW, schema.getField("childField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.STRING,
                schema.getField("childField").getType().getRowSchema().getField("childName").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.ARRAY, schema.getField("stringArrayField").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.STRING,
                schema.getField("stringArrayField").getType().getCollectionElementType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.ROW,
                schema.getField("childArrayField").getType().getCollectionElementType().getTypeName());
        // all fields are nullable
        Assertions.assertTrue(schema.getField("stringField").getType().getNullable());
    }

    @Test
    public void testConvert() {
        final Type type = createTestType();
        final Schema schema = StructToRowConverter.convertSchema(type);
        final Struct struct = createTestStruct();

        final Row row = StructToRowConverter.convert(schema, struct);

        Assertions.assertEquals("hello", row.getString("stringField"));
        Assertions.assertEquals(Boolean.TRUE, row.getBoolean("boolField"));
        Assertions.assertEquals(64L, row.getInt64("int64Field"));
        Assertions.assertEquals(1.5f, row.getFloat("float32Field"));
        Assertions.assertEquals(2.5d, row.getDouble("float64Field"));
        Assertions.assertEquals(new BigDecimal("123.45"), row.getDecimal("numericField"));
        Assertions.assertArrayEquals("bytes".getBytes(StandardCharsets.UTF_8), row.getBytes("bytesField"));
        Assertions.assertEquals(LocalDate.of(2024, 3, 1), row.getValue("dateField"));
        Assertions.assertEquals(
                Instant.parse("2024-03-01T12:00:00.000Z"),
                row.getDateTime("timestampField").toInstant());

        final Row child = row.getRow("childField");
        Assertions.assertNotNull(child);
        Assertions.assertEquals("a", child.getString("childName"));
        Assertions.assertEquals(1L, child.getInt64("childAge"));

        Assertions.assertEquals(List.of("a", "b"), row.getArray("stringArrayField"));
        Assertions.assertEquals(List.of(true, false), row.getArray("boolArrayField"));
        Assertions.assertEquals(List.of(1L, 2L), row.getArray("int64ArrayField"));
        Assertions.assertEquals(List.of(2.5d), row.getArray("float64ArrayField"));
        Assertions.assertEquals(List.of(new BigDecimal("1.5")), row.getArray("numericArrayField"));
        final List<byte[]> bytesList = new ArrayList<>(row.<byte[]>getArray("bytesArrayField"));
        Assertions.assertEquals(1, bytesList.size());
        Assertions.assertArrayEquals("b1".getBytes(StandardCharsets.UTF_8), bytesList.get(0));
        Assertions.assertEquals(List.of(LocalDate.of(2024, 3, 2)), row.getArray("dateArrayField"));
        Assertions.assertEquals(List.of(Instant.parse("2024-03-01T12:00:00.000Z")), row.getArray("timestampArrayField"));

        final List<Row> children = new ArrayList<>(row.<Row>getArray("childArrayField"));
        Assertions.assertEquals(1, children.size());
        Assertions.assertEquals("b", children.get(0).getString("childName"));
        Assertions.assertEquals(2L, children.get(0).getInt64("childAge"));
    }

    @Test
    public void testConvertNullValues() {
        final Type type = createTestType();
        final Schema schema = StructToRowConverter.convertSchema(type);
        final Struct struct = Struct.newBuilder()
                .set("stringField").to((String) null)
                .set("boolField").to((Boolean) null)
                .set("int64Field").to((Long) null)
                .set("float32Field").to((Float) null)
                .set("float64Field").to((Double) null)
                .set("numericField").to((BigDecimal) null)
                .set("bytesField").to((ByteArray) null)
                .set("dateField").to((Date) null)
                .set("timestampField").to((Timestamp) null)
                .set("childField").to(CHILD_TYPE, null)
                .set("stringArrayField").toStringArray(null)
                .set("boolArrayField").toBoolArray((Iterable<Boolean>) null)
                .set("int64ArrayField").toInt64Array((Iterable<Long>) null)
                .set("float64ArrayField").toFloat64Array((Iterable<Double>) null)
                .set("numericArrayField").toNumericArray(null)
                .set("bytesArrayField").toBytesArray(null)
                .set("dateArrayField").toDateArray(null)
                .set("timestampArrayField").toTimestampArray(null)
                .set("childArrayField").toStructArray(CHILD_TYPE, null)
                .build();

        final Row row = StructToRowConverter.convert(schema, struct);

        Assertions.assertNull(row.getValue("stringField"));
        Assertions.assertNull(row.getValue("boolField"));
        Assertions.assertNull(row.getValue("int64Field"));
        Assertions.assertNull(row.getValue("float32Field"));
        Assertions.assertNull(row.getValue("float64Field"));
        Assertions.assertNull(row.getValue("numericField"));
        Assertions.assertNull(row.getValue("bytesField"));
        Assertions.assertNull(row.getValue("dateField"));
        Assertions.assertNull(row.getValue("timestampField"));
        Assertions.assertNull(row.getValue("childField"));
        // null array columns are converted to null (fields are nullable)
        Assertions.assertNull(row.getValue("stringArrayField"));
        Assertions.assertNull(row.getValue("int64ArrayField"));
        Assertions.assertNull(row.getValue("childArrayField"));
    }

    @Test
    public void testConvertMissingField() {
        final Schema schema = Schema.builder()
                .addNullableField("stringField", Schema.FieldType.STRING)
                .addNullableField("missingField", Schema.FieldType.STRING)
                .build();
        final Struct struct = Struct.newBuilder()
                .set("stringField").to("hello")
                .build();

        final Row row = StructToRowConverter.convert(schema, struct);
        Assertions.assertEquals("hello", row.getString("stringField"));
        Assertions.assertNull(row.getValue("missingField"));
    }

    @Test
    public void testConvertNullStruct() {
        final Schema schema = Schema.builder()
                .addNullableField("stringField", Schema.FieldType.STRING)
                .build();
        Assertions.assertNull(StructToRowConverter.convert(schema, (Struct) null));
    }

    @Test
    public void testConvertUnsupportedFieldType() {
        final Schema schema = Schema.builder()
                .addNullableField("mapField", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING))
                .build();
        final Struct struct = Struct.newBuilder()
                .set("mapField").to("value")
                .build();

        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
                StructToRowConverter.convert(schema, struct));
        Assertions.assertTrue(e.getMessage().contains("Unsupported field type"));
    }

}
