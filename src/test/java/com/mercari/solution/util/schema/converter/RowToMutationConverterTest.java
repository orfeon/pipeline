package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RowToMutationConverterTest {

    private static final EnumerationType ENUM_TYPE = EnumerationType.create("FOO", "BAR");

    private static Schema createTestSchema() {
        return Schema.builder()
                .addField("stringField", Schema.FieldType.STRING)
                .addNullableField("nullableStringField", Schema.FieldType.STRING)
                .addField("boolField", Schema.FieldType.BOOLEAN)
                .addField("bytesField", Schema.FieldType.BYTES)
                .addNullableField("int16Field", Schema.FieldType.INT16)
                .addNullableField("int32Field", Schema.FieldType.INT32)
                .addField("int64Field", Schema.FieldType.INT64)
                .addNullableField("floatField", Schema.FieldType.FLOAT)
                .addField("doubleField", Schema.FieldType.DOUBLE)
                .addField("decimalField", Schema.FieldType.DECIMAL)
                .addField("datetimeField", Schema.FieldType.DATETIME)
                .addField("localDateTimeField", Schema.FieldType.logicalType(SqlTypes.DATETIME))
                .addNullableField("nullableLocalDateTimeField", Schema.FieldType.logicalType(SqlTypes.DATETIME))
                .addField("dateField", CalciteUtils.DATE)
                .addField("timeField", CalciteUtils.TIME)
                .addField("enumField", Schema.FieldType.logicalType(ENUM_TYPE))
                .addNullableField("nullableEnumField", Schema.FieldType.logicalType(ENUM_TYPE))
                .addField("stringArrayField", Schema.FieldType.array(Schema.FieldType.STRING))
                .addNullableField("nullableStringArrayField", Schema.FieldType.array(Schema.FieldType.STRING))
                .addField("boolArrayField", Schema.FieldType.array(Schema.FieldType.BOOLEAN))
                .addField("bytesArrayField", Schema.FieldType.array(Schema.FieldType.BYTES))
                .addField("int64ArrayField", Schema.FieldType.array(Schema.FieldType.INT64))
                .addField("floatArrayField", Schema.FieldType.array(Schema.FieldType.FLOAT))
                .addField("doubleArrayField", Schema.FieldType.array(Schema.FieldType.DOUBLE))
                .addField("decimalArrayField", Schema.FieldType.array(Schema.FieldType.DECIMAL))
                .addField("datetimeArrayField", Schema.FieldType.array(Schema.FieldType.DATETIME))
                .addField("dateArrayField", Schema.FieldType.array(CalciteUtils.DATE))
                .addField("timeArrayField", Schema.FieldType.array(CalciteUtils.TIME))
                .addField("enumArrayField", Schema.FieldType.array(Schema.FieldType.logicalType(ENUM_TYPE)))
                .build();
    }

    private static Row createTestRow(final Schema schema) {
        return Row.withSchema(schema)
                .withFieldValue("stringField", "hello")
                .withFieldValue("nullableStringField", null)
                .withFieldValue("boolField", true)
                .withFieldValue("bytesField", "bytes".getBytes(StandardCharsets.UTF_8))
                .withFieldValue("int16Field", (short) 16)
                .withFieldValue("int32Field", 32)
                .withFieldValue("int64Field", 64L)
                .withFieldValue("floatField", 1.5f)
                .withFieldValue("doubleField", 2.5d)
                .withFieldValue("decimalField", new BigDecimal("123.45"))
                .withFieldValue("datetimeField", Instant.parse("2024-03-01T12:00:00.000Z"))
                .withFieldValue("localDateTimeField", LocalDateTime.of(2024, 3, 1, 12, 0, 0, 123456789))
                .withFieldValue("nullableLocalDateTimeField", null)
                .withFieldValue("dateField", LocalDate.of(2024, 3, 1))
                .withFieldValue("timeField", LocalTime.of(12, 34, 56))
                .withFieldValue("enumField", ENUM_TYPE.valueOf("BAR"))
                .withFieldValue("nullableEnumField", null)
                .withFieldValue("stringArrayField", List.of("a", "b"))
                .withFieldValue("nullableStringArrayField", null)
                .withFieldValue("boolArrayField", List.of(true, false))
                .withFieldValue("bytesArrayField", List.of("b1".getBytes(StandardCharsets.UTF_8)))
                .withFieldValue("int64ArrayField", List.of(1L, 2L))
                .withFieldValue("floatArrayField", List.of(1.5f))
                .withFieldValue("doubleArrayField", List.of(2.5d))
                .withFieldValue("decimalArrayField", List.of(new BigDecimal("1.5")))
                .withFieldValue("datetimeArrayField", List.of(Instant.parse("2024-03-01T12:00:00.000Z")))
                .withFieldValue("dateArrayField", List.of(LocalDate.of(2024, 3, 2)))
                .withFieldValue("timeArrayField", List.of(LocalTime.of(12, 34, 56)))
                .withFieldValue("enumArrayField", List.of(ENUM_TYPE.valueOf("FOO")))
                .build();
    }

    @Test
    public void testConvert() {
        final Schema schema = createTestSchema();
        final Row row = createTestRow(schema);

        final Mutation mutation = RowToMutationConverter.convert(row, "MyTable", null);
        Assertions.assertEquals("MyTable", mutation.getTable());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutation.getOperation());

        final Map<String, Value> map = mutation.asMap();
        Assertions.assertEquals(Value.string("hello"), map.get("stringField"));
        Assertions.assertTrue(map.get("nullableStringField").isNull());
        Assertions.assertEquals(Value.bool(true), map.get("boolField"));
        Assertions.assertEquals(Value.bytes(ByteArray.copyFrom("bytes")), map.get("bytesField"));
        Assertions.assertEquals(Value.int64(16L), map.get("int16Field"));
        Assertions.assertEquals(Value.int64(32L), map.get("int32Field"));
        Assertions.assertEquals(Value.int64(64L), map.get("int64Field"));
        // Beam FLOAT is written as Spanner FLOAT32, matching convertFieldType (Type.float32) and the array path
        Assertions.assertEquals(Value.float32(1.5f), map.get("floatField"));
        Assertions.assertEquals(Value.float64(2.5d), map.get("doubleField"));
        Assertions.assertEquals(Value.numeric(new BigDecimal("123.45")), map.get("decimalField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), map.get("datetimeField"));
        // DATETIME logical type is interpreted as UTC and written as epoch-based timestamp
        Assertions.assertEquals(
                Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00.123456789Z")),
                map.get("localDateTimeField"));
        Assertions.assertTrue(map.get("nullableLocalDateTimeField").isNull());
        Assertions.assertEquals(Type.timestamp(), map.get("nullableLocalDateTimeField").getType());
        Assertions.assertEquals(Value.date(Date.fromYearMonthDay(2024, 3, 1)), map.get("dateField"));
        Assertions.assertEquals(Value.string("12:34:56"), map.get("timeField"));
        Assertions.assertEquals(Value.string("BAR"), map.get("enumField"));
        Assertions.assertTrue(map.get("nullableEnumField").isNull());

        Assertions.assertEquals(Value.stringArray(List.of("a", "b")), map.get("stringArrayField"));
        Assertions.assertTrue(map.get("nullableStringArrayField").isNull());
        Assertions.assertEquals(Value.boolArray(List.of(true, false)), map.get("boolArrayField"));
        Assertions.assertEquals(Value.bytesArray(List.of(ByteArray.copyFrom("b1"))), map.get("bytesArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of(1L, 2L)), map.get("int64ArrayField"));
        Assertions.assertEquals(Value.float32Array(List.of(1.5f)), map.get("floatArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of(2.5d)), map.get("doubleArrayField"));
        Assertions.assertEquals(Value.numericArray(List.of(new BigDecimal("1.5"))), map.get("decimalArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of(Timestamp.parseTimestamp("2024-03-01T12:00:00Z"))), map.get("datetimeArrayField"));
        Assertions.assertEquals(Value.dateArray(List.of(Date.fromYearMonthDay(2024, 3, 2))), map.get("dateArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("12:34:56")), map.get("timeArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("FOO")), map.get("enumArrayField"));
    }

    @Test
    public void testConvertNullScalarValues() {
        final Schema schema = Schema.builder()
                .addNullableField("int16Field", Schema.FieldType.INT16)
                .addNullableField("int32Field", Schema.FieldType.INT32)
                .addNullableField("int64Field", Schema.FieldType.INT64)
                .addNullableField("floatField", Schema.FieldType.FLOAT)
                .addNullableField("doubleField", Schema.FieldType.DOUBLE)
                .addNullableField("boolField", Schema.FieldType.BOOLEAN)
                .addNullableField("bytesField", Schema.FieldType.BYTES)
                .addNullableField("decimalField", Schema.FieldType.DECIMAL)
                .addNullableField("dateField", CalciteUtils.NULLABLE_DATE)
                .addNullableField("timeField", CalciteUtils.NULLABLE_TIME)
                .build();
        final Row row = Row.withSchema(schema)
                .addValues(null, null, null, null, null, null, null, null, null, null)
                .build();

        final Mutation mutation = RowToMutationConverter.convert(row, "MyTable", "INSERT");
        final Map<String, Value> map = mutation.asMap();
        Assertions.assertTrue(map.get("int16Field").isNull());
        Assertions.assertEquals(Type.int64(), map.get("int16Field").getType());
        Assertions.assertTrue(map.get("int32Field").isNull());
        Assertions.assertEquals(Type.int64(), map.get("int32Field").getType());
        Assertions.assertTrue(map.get("int64Field").isNull());
        Assertions.assertTrue(map.get("floatField").isNull());
        Assertions.assertEquals(Type.float32(), map.get("floatField").getType());
        Assertions.assertTrue(map.get("doubleField").isNull());
        Assertions.assertTrue(map.get("boolField").isNull());
        Assertions.assertTrue(map.get("bytesField").isNull());
        Assertions.assertTrue(map.get("decimalField").isNull());
        Assertions.assertTrue(map.get("dateField").isNull());
        Assertions.assertTrue(map.get("timeField").isNull());
    }

    @Test
    public void testConvertMutationOps() {
        final Schema schema = createTestSchema();
        final Row row = createTestRow(schema);

        Assertions.assertEquals(Mutation.Op.INSERT,
                RowToMutationConverter.convert(row, "T", "INSERT").getOperation());
        Assertions.assertEquals(Mutation.Op.UPDATE,
                RowToMutationConverter.convert(row, "T", "UPDATE").getOperation());
        Assertions.assertEquals(Mutation.Op.REPLACE,
                RowToMutationConverter.convert(row, "T", "REPLACE").getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                RowToMutationConverter.convert(row, "T", "INSERT_OR_UPDATE").getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                RowToMutationConverter.convert(row, "T", null).getOperation());
    }

    @Test
    public void testConvertDelete() {
        final Schema schema = createTestSchema();
        final Row row = createTestRow(schema);

        final Mutation mutation = RowToMutationConverter.convert(
                schema, row, "MyTable", "DELETE",
                List.of("stringField", "int64Field", "dateField", "timeField", "datetimeField"),
                null, null, null);

        Assertions.assertEquals(Mutation.Op.DELETE, mutation.getOperation());
        Assertions.assertEquals("MyTable", mutation.getTable());
        Assertions.assertEquals(
                KeySet.singleKey(Key.of(
                        "hello", 64L,
                        Date.fromYearMonthDay(2024, 3, 1),
                        "12:34:56",
                        Timestamp.parseTimestamp("2024-03-01T12:00:00Z"))),
                mutation.getKeySet());

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                RowToMutationConverter.convert(schema, row, "MyTable", "DELETE", null, null, null, null));
    }

    @Test
    public void testConvertExcludeHideAndCommitTimestamp() {
        final Schema schema = createTestSchema();
        final Row row = createTestRow(schema);

        final Mutation mutation = RowToMutationConverter.convert(
                schema, row, "MyTable", "INSERT",
                null,
                List.of("datetimeField", "createdAtField"),
                Set.of("stringField"),
                Set.of("boolField", "int64Field", "nullableEnumField", "stringArrayField"));

        final Map<String, Value> map = mutation.asMap();
        Assertions.assertFalse(map.containsKey("stringField"));

        Assertions.assertEquals(Value.bool(false), map.get("boolField"));
        Assertions.assertEquals(Value.int64(0L), map.get("int64Field"));
        Assertions.assertTrue(map.get("nullableEnumField").isNull());
        Assertions.assertEquals(Value.stringArray(List.of()), map.get("stringArrayField"));

        Assertions.assertEquals(Value.timestamp(Value.COMMIT_TIMESTAMP), map.get("datetimeField"));
        Assertions.assertEquals(Value.timestamp(Value.COMMIT_TIMESTAMP), map.get("createdAtField"));
    }

    @Test
    public void testConvertSchema() {
        final Schema schema = createTestSchema();
        final Type type = RowToMutationConverter.convertSchema(schema);

        Assertions.assertEquals(Type.Code.STRUCT, type.getCode());
        Assertions.assertEquals(Type.string(), fieldType(type, "stringField"));
        Assertions.assertEquals(Type.bool(), fieldType(type, "boolField"));
        Assertions.assertEquals(Type.bytes(), fieldType(type, "bytesField"));
        Assertions.assertEquals(Type.int64(), fieldType(type, "int16Field"));
        Assertions.assertEquals(Type.int64(), fieldType(type, "int32Field"));
        Assertions.assertEquals(Type.int64(), fieldType(type, "int64Field"));
        Assertions.assertEquals(Type.float32(), fieldType(type, "floatField"));
        Assertions.assertEquals(Type.float64(), fieldType(type, "doubleField"));
        Assertions.assertEquals(Type.numeric(), fieldType(type, "decimalField"));
        Assertions.assertEquals(Type.timestamp(), fieldType(type, "datetimeField"));
        Assertions.assertEquals(Type.timestamp(), fieldType(type, "localDateTimeField"));
        Assertions.assertEquals(Type.date(), fieldType(type, "dateField"));
        Assertions.assertEquals(Type.string(), fieldType(type, "timeField"));
        Assertions.assertEquals(Type.string(), fieldType(type, "enumField"));
        Assertions.assertEquals(Type.array(Type.string()), fieldType(type, "stringArrayField"));
        Assertions.assertEquals(Type.array(Type.int64()), fieldType(type, "int64ArrayField"));
        Assertions.assertEquals(Type.array(Type.float32()), fieldType(type, "floatArrayField"));
        Assertions.assertEquals(Type.array(Type.float64()), fieldType(type, "doubleArrayField"));
        Assertions.assertEquals(Type.array(Type.date()), fieldType(type, "dateArrayField"));

        final Schema rowSchema = Schema.builder()
                .addField("child", Schema.FieldType.row(Schema.builder()
                        .addField("name", Schema.FieldType.STRING)
                        .addField("age", Schema.FieldType.INT64)
                        .build()))
                .build();
        final Type rowType = RowToMutationConverter.convertSchema(rowSchema);
        Assertions.assertEquals(
                Type.struct(
                        Type.StructField.of("name", Type.string()),
                        Type.StructField.of("age", Type.int64())),
                fieldType(rowType, "child"));
    }

    @Test
    public void testConvertGroup() {
        final Schema childSchema = Schema.builder()
                .addField("name", Schema.FieldType.STRING)
                .addField("age", Schema.FieldType.INT64)
                .build();
        final Schema parentSchema = Schema.builder()
                .addField("children", Schema.FieldType.array(Schema.FieldType.row(childSchema)))
                .addField("others", Schema.FieldType.array(Schema.FieldType.row(childSchema)))
                .build();

        final Row c1 = Row.withSchema(childSchema).addValues("a", 1L).build();
        final Row c2 = Row.withSchema(childSchema).addValues("b", 2L).build();
        final Row c3 = Row.withSchema(childSchema).addValues("c", 3L).build();
        final Row parent = Row.withSchema(parentSchema)
                .addValues(Arrays.asList(c1, c2), Arrays.asList(c3))
                .build();

        final MutationGroup group = RowToMutationConverter
                .convertGroup(parentSchema, parent, "INSERT", "children");
        Assertions.assertEquals("children", group.primary().getTable());
        Assertions.assertEquals(Value.string("a"), group.primary().asMap().get("name"));
        Assertions.assertEquals(Value.int64(1L), group.primary().asMap().get("age"));
        Assertions.assertEquals(2, group.attached().size());
        Assertions.assertEquals("children", group.attached().get(0).getTable());
        Assertions.assertEquals(Value.string("b"), group.attached().get(0).asMap().get("name"));
        Assertions.assertEquals("others", group.attached().get(1).getTable());
        Assertions.assertEquals(Value.string("c"), group.attached().get(1).asMap().get("name"));
    }

    private static Type fieldType(final Type structType, final String fieldName) {
        return structType.getStructFields().get(structType.getFieldIndex(fieldName)).getType();
    }

}
