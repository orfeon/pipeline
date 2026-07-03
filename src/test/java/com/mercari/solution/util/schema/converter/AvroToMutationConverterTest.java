package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AvroToMutationConverterTest {

    private static final String SCHEMA_JSON = """
            {
              "type": "record",
              "name": "TestRecord",
              "fields": [
                { "name": "stringField", "type": "string" },
                { "name": "jsonField", "type": { "type": "string", "sqlType": "JSON" } },
                { "name": "datetimeField", "type": { "type": "string", "sqlType": "DATETIME" } },
                { "name": "boolField", "type": "boolean" },
                { "name": "intField", "type": "int" },
                { "name": "longField", "type": "long" },
                { "name": "floatField", "type": "float" },
                { "name": "doubleField", "type": "double" },
                { "name": "bytesField", "type": "bytes" },
                { "name": "decimalField", "type": { "type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9 } },
                { "name": "dateField", "type": { "type": "int", "logicalType": "date" } },
                { "name": "timeMillisField", "type": { "type": "int", "logicalType": "time-millis" } },
                { "name": "timeMicrosField", "type": { "type": "long", "logicalType": "time-micros" } },
                { "name": "timestampMillisField", "type": { "type": "long", "logicalType": "timestamp-millis" } },
                { "name": "timestampMicrosField", "type": { "type": "long", "logicalType": "timestamp-micros" } },
                { "name": "enumField", "type": { "type": "enum", "name": "TestEnum", "symbols": ["FOO", "BAR"] } },
                { "name": "nullableStringField", "type": ["null", "string"], "default": null },
                { "name": "nullableLongField", "type": ["null", "long"], "default": null },
                { "name": "stringArrayField", "type": { "type": "array", "items": "string" } },
                { "name": "jsonArrayField", "type": { "type": "array", "items": { "type": "string", "sqlType": "JSON" } } },
                { "name": "boolArrayField", "type": { "type": "array", "items": "boolean" } },
                { "name": "intArrayField", "type": { "type": "array", "items": "int" } },
                { "name": "longArrayField", "type": { "type": "array", "items": "long" } },
                { "name": "floatArrayField", "type": { "type": "array", "items": "float" } },
                { "name": "doubleArrayField", "type": { "type": "array", "items": "double" } },
                { "name": "bytesArrayField", "type": { "type": "array", "items": "bytes" } },
                { "name": "decimalArrayField", "type": { "type": "array", "items": { "type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9 } } },
                { "name": "dateArrayField", "type": { "type": "array", "items": { "type": "int", "logicalType": "date" } } },
                { "name": "timeMillisArrayField", "type": { "type": "array", "items": { "type": "int", "logicalType": "time-millis" } } },
                { "name": "timeMicrosArrayField", "type": { "type": "array", "items": { "type": "long", "logicalType": "time-micros" } } },
                { "name": "timestampMillisArrayField", "type": { "type": "array", "items": { "type": "long", "logicalType": "timestamp-millis" } } },
                { "name": "timestampMicrosArrayField", "type": { "type": "array", "items": { "type": "long", "logicalType": "timestamp-micros" } } }
              ]
            }
            """;

    // 2024-03-01T12:00:00Z
    private static final long EPOCH_MILLIS = 1709294400000L;
    // 12:34:56.789
    private static final int TIME_MILLIS = 45296789;

    private static Schema createTestSchema() {
        return new Schema.Parser().parse(SCHEMA_JSON);
    }

    private static GenericRecord createTestRecord(final Schema schema) {
        return new GenericRecordBuilder(schema)
                .set("stringField", "hello")
                .set("jsonField", "{\"a\":1}")
                .set("datetimeField", "2024-03-01T12:00:00Z")
                .set("boolField", true)
                .set("intField", 42)
                .set("longField", 10L)
                .set("floatField", 1.5f)
                .set("doubleField", 2.5d)
                .set("bytesField", ByteBuffer.wrap("bytes".getBytes(StandardCharsets.UTF_8)))
                .set("decimalField", ByteBuffer.wrap(new BigDecimal("123.456000000").unscaledValue().toByteArray()))
                .set("dateField", (int) LocalDate.of(2024, 3, 1).toEpochDay())
                .set("timeMillisField", TIME_MILLIS)
                .set("timeMicrosField", TIME_MILLIS * 1000L)
                .set("timestampMillisField", EPOCH_MILLIS)
                .set("timestampMicrosField", EPOCH_MILLIS * 1000L)
                .set("enumField", new GenericData.EnumSymbol(schema.getField("enumField").schema(), "BAR"))
                .set("stringArrayField", List.of("a", "b"))
                .set("jsonArrayField", List.of("{\"b\":2}"))
                .set("boolArrayField", List.of(true, false))
                .set("intArrayField", List.of(1, 2))
                .set("longArrayField", List.of(1L, 2L))
                .set("floatArrayField", List.of(1.5f))
                .set("doubleArrayField", List.of(2.5d))
                .set("bytesArrayField", List.of(ByteBuffer.wrap("b1".getBytes(StandardCharsets.UTF_8))))
                .set("decimalArrayField", List.of(ByteBuffer.wrap(new BigDecimal("1.000000000").unscaledValue().toByteArray())))
                .set("dateArrayField", List.of((int) LocalDate.of(2024, 3, 2).toEpochDay()))
                .set("timeMillisArrayField", List.of(TIME_MILLIS))
                .set("timeMicrosArrayField", List.of(TIME_MILLIS * 1000L))
                .set("timestampMillisArrayField", List.of(EPOCH_MILLIS))
                .set("timestampMicrosArrayField", List.of(EPOCH_MILLIS * 1000L))
                .build();
    }

    @Test
    public void testConvert() {
        final Schema schema = createTestSchema();
        final GenericRecord record = createTestRecord(schema);

        final Mutation mutation = AvroToMutationConverter
                .convert(schema, record, "MyTable", null, null, null, null, null);

        Assertions.assertEquals("MyTable", mutation.getTable());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutation.getOperation());

        final Map<String, Value> map = mutation.asMap();
        Assertions.assertEquals(Value.string("hello"), map.get("stringField"));
        // scalar string with sqlType JSON/DATETIME is written as string
        Assertions.assertEquals(Value.string("{\"a\":1}"), map.get("jsonField"));
        Assertions.assertEquals(Value.string("2024-03-01T12:00:00Z"), map.get("datetimeField"));
        Assertions.assertEquals(Value.bool(true), map.get("boolField"));
        Assertions.assertEquals(Value.int64(42L), map.get("intField"));
        Assertions.assertEquals(Value.int64(10L), map.get("longField"));
        Assertions.assertEquals(Value.float32(1.5f), map.get("floatField"));
        Assertions.assertEquals(Value.float64(2.5d), map.get("doubleField"));
        Assertions.assertEquals(Value.bytes(ByteArray.copyFrom("bytes")), map.get("bytesField"));
        Assertions.assertEquals(Value.numeric(new BigDecimal("123.456000000")), map.get("decimalField"));
        Assertions.assertEquals(Value.date(Date.fromYearMonthDay(2024, 3, 1)), map.get("dateField"));
        Assertions.assertEquals(Value.string("12:34:56.789"), map.get("timeMillisField"));
        Assertions.assertEquals(Value.string("12:34:56.789"), map.get("timeMicrosField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), map.get("timestampMillisField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), map.get("timestampMicrosField"));
        Assertions.assertEquals(Value.string("BAR"), map.get("enumField"));

        Assertions.assertTrue(map.get("nullableStringField").isNull());
        Assertions.assertEquals(Type.string(), map.get("nullableStringField").getType());
        Assertions.assertTrue(map.get("nullableLongField").isNull());
        Assertions.assertEquals(Type.int64(), map.get("nullableLongField").getType());

        Assertions.assertEquals(Value.stringArray(List.of("a", "b")), map.get("stringArrayField"));
        Assertions.assertEquals(Value.jsonArray(List.of("{\"b\":2}")), map.get("jsonArrayField"));
        Assertions.assertEquals(Value.boolArray(List.of(true, false)), map.get("boolArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of(1L, 2L)), map.get("intArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of(1L, 2L)), map.get("longArrayField"));
        Assertions.assertEquals(Value.float32Array(List.of(1.5f)), map.get("floatArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of(2.5d)), map.get("doubleArrayField"));
        Assertions.assertEquals(Value.bytesArray(List.of(ByteArray.copyFrom("b1"))), map.get("bytesArrayField"));
        Assertions.assertEquals(Value.numericArray(List.of(new BigDecimal("1.000000000"))), map.get("decimalArrayField"));
        Assertions.assertEquals(Value.dateArray(List.of(Date.fromYearMonthDay(2024, 3, 2))), map.get("dateArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("12:34:56.789")), map.get("timeMillisArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("12:34:56.789")), map.get("timeMicrosArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of(Timestamp.parseTimestamp("2024-03-01T12:00:00Z"))), map.get("timestampMillisArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of(Timestamp.parseTimestamp("2024-03-01T12:00:00Z"))), map.get("timestampMicrosArrayField"));
    }

    @Test
    public void testConvertMutationOps() {
        final Schema schema = createTestSchema();
        final GenericRecord record = createTestRecord(schema);

        Assertions.assertEquals(Mutation.Op.INSERT,
                AvroToMutationConverter.convert(schema, record, "T", "INSERT", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT,
                AvroToMutationConverter.convert(schema, record, "T", "insert", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.UPDATE,
                AvroToMutationConverter.convert(schema, record, "T", "UPDATE", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.REPLACE,
                AvroToMutationConverter.convert(schema, record, "T", "REPLACE", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                AvroToMutationConverter.convert(schema, record, "T", "INSERT_OR_UPDATE", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                AvroToMutationConverter.convert(schema, record, "T", "UNKNOWN_OP", null, null, null, null).getOperation());
    }

    @Test
    public void testConvertDelete() {
        final Schema schema = createTestSchema();
        final GenericRecord record = createTestRecord(schema);

        final Mutation mutation = AvroToMutationConverter.convert(
                schema, record, "MyTable", "DELETE",
                List.of("stringField", "longField", "dateField"), null, null, null);

        Assertions.assertEquals(Mutation.Op.DELETE, mutation.getOperation());
        Assertions.assertEquals("MyTable", mutation.getTable());
        Assertions.assertEquals(
                KeySet.singleKey(Key.of("hello", 10L, Date.fromYearMonthDay(2024, 3, 1))),
                mutation.getKeySet());

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                AvroToMutationConverter.convert(schema, record, "MyTable", "DELETE", null, null, null, null));
    }

    @Test
    public void testConvertExcludeHideAndCommitTimestamp() {
        final Schema schema = createTestSchema();
        final GenericRecord record = createTestRecord(schema);

        final Mutation mutation = AvroToMutationConverter.convert(
                schema, record, "MyTable", "INSERT",
                null,
                List.of("timestampMicrosField", "createdAtField"),
                Set.of("stringField"),
                Set.of("boolField", "intField", "nullableStringField", "stringArrayField"));

        final Map<String, Value> map = mutation.asMap();
        Assertions.assertFalse(map.containsKey("stringField"));

        // hidden fields are masked with default values (or null if nullable)
        Assertions.assertEquals(Value.bool(false), map.get("boolField"));
        Assertions.assertEquals(Value.int64(0L), map.get("intField"));
        Assertions.assertTrue(map.get("nullableStringField").isNull());
        Assertions.assertEquals(Value.stringArray(List.of()), map.get("stringArrayField"));

        // commit timestamp fields
        Assertions.assertEquals(Value.timestamp(Value.COMMIT_TIMESTAMP), map.get("timestampMicrosField"));
        Assertions.assertEquals(Value.timestamp(Value.COMMIT_TIMESTAMP), map.get("createdAtField"));
    }

    @Test
    public void testConvertSchema() {
        final Schema schema = createTestSchema();
        final Type type = AvroToMutationConverter.convertSchema(schema);

        Assertions.assertEquals(Type.Code.STRUCT, type.getCode());
        Assertions.assertEquals(Type.string(), fieldType(type, "stringField"));
        Assertions.assertEquals(Type.json(), fieldType(type, "jsonField"));
        Assertions.assertEquals(Type.string(), fieldType(type, "datetimeField"));
        Assertions.assertEquals(Type.bool(), fieldType(type, "boolField"));
        Assertions.assertEquals(Type.int64(), fieldType(type, "intField"));
        Assertions.assertEquals(Type.int64(), fieldType(type, "longField"));
        Assertions.assertEquals(Type.float32(), fieldType(type, "floatField"));
        Assertions.assertEquals(Type.float64(), fieldType(type, "doubleField"));
        Assertions.assertEquals(Type.bytes(), fieldType(type, "bytesField"));
        Assertions.assertEquals(Type.numeric(), fieldType(type, "decimalField"));
        Assertions.assertEquals(Type.date(), fieldType(type, "dateField"));
        Assertions.assertEquals(Type.string(), fieldType(type, "timeMillisField"));
        Assertions.assertEquals(Type.string(), fieldType(type, "timeMicrosField"));
        Assertions.assertEquals(Type.timestamp(), fieldType(type, "timestampMillisField"));
        Assertions.assertEquals(Type.timestamp(), fieldType(type, "timestampMicrosField"));
        Assertions.assertEquals(Type.string(), fieldType(type, "enumField"));
        Assertions.assertEquals(Type.string(), fieldType(type, "nullableStringField"));
        Assertions.assertEquals(Type.int64(), fieldType(type, "nullableLongField"));
        Assertions.assertEquals(Type.array(Type.string()), fieldType(type, "stringArrayField"));
        Assertions.assertEquals(Type.array(Type.int64()), fieldType(type, "intArrayField"));
        Assertions.assertEquals(Type.array(Type.date()), fieldType(type, "dateArrayField"));
        Assertions.assertEquals(Type.array(Type.timestamp()), fieldType(type, "timestampMicrosArrayField"));
    }

    @Test
    public void testConvertValues() {
        final Schema schema = createTestSchema();
        final GenericRecord record = createTestRecord(schema);

        final Map<String, Value> values = AvroToMutationConverter.convertValues(schema, record);

        Assertions.assertEquals(Value.string("hello"), values.get("stringField"));
        Assertions.assertEquals(Value.json("{\"a\":1}"), values.get("jsonField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), values.get("datetimeField"));
        Assertions.assertEquals(Value.bool(true), values.get("boolField"));
        Assertions.assertEquals(Value.int64(42L), values.get("intField"));
        Assertions.assertEquals(Value.int64(10L), values.get("longField"));
        Assertions.assertEquals(Value.float32(1.5f), values.get("floatField"));
        Assertions.assertEquals(Value.float64(2.5d), values.get("doubleField"));
        Assertions.assertEquals(Value.bytes(ByteArray.copyFrom("bytes")), values.get("bytesField"));
        Assertions.assertEquals(Value.numeric(new BigDecimal("123.456000000")), values.get("decimalField"));
        Assertions.assertEquals(Value.date(Date.fromYearMonthDay(2024, 3, 1)), values.get("dateField"));
        Assertions.assertEquals(Value.string("12:34:56.789"), values.get("timeMillisField"));
        Assertions.assertEquals(Value.string("12:34:56.789"), values.get("timeMicrosField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), values.get("timestampMillisField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), values.get("timestampMicrosField"));
        Assertions.assertEquals(Value.string("BAR"), values.get("enumField"));

        Assertions.assertTrue(values.get("nullableStringField").isNull());
        Assertions.assertTrue(values.get("nullableLongField").isNull());

        Assertions.assertEquals(Value.stringArray(List.of("a", "b")), values.get("stringArrayField"));
        Assertions.assertEquals(Value.jsonArray(List.of("{\"b\":2}")), values.get("jsonArrayField"));
        Assertions.assertEquals(Value.boolArray(List.of(true, false)), values.get("boolArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of(1L, 2L)), values.get("intArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of(1L, 2L)), values.get("longArrayField"));
        Assertions.assertEquals(Value.float32Array(List.of(1.5f)), values.get("floatArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of(2.5d)), values.get("doubleArrayField"));
        Assertions.assertEquals(Value.bytesArray(List.of(ByteArray.copyFrom("b1"))), values.get("bytesArrayField"));
        Assertions.assertEquals(Value.numericArray(List.of(new BigDecimal("1.000000000"))), values.get("decimalArrayField"));
        Assertions.assertEquals(Value.dateArray(List.of(Date.fromYearMonthDay(2024, 3, 2))), values.get("dateArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("12:34:56.789")), values.get("timeMillisArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("12:34:56.789")), values.get("timeMicrosArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of(Timestamp.parseTimestamp("2024-03-01T12:00:00Z"))), values.get("timestampMillisArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of(Timestamp.parseTimestamp("2024-03-01T12:00:00Z"))), values.get("timestampMicrosArrayField"));
    }

    @Test
    public void testConvertGroup() {
        final String childJson = """
                {
                  "type": "record",
                  "name": "Child",
                  "fields": [
                    { "name": "name", "type": "string" },
                    { "name": "age", "type": "long" }
                  ]
                }
                """;
        final Schema childSchema = new Schema.Parser().parse(childJson);
        final Schema parentSchema = org.apache.avro.SchemaBuilder.record("Parent").fields()
                .name("children").type().array().items(childSchema).noDefault()
                .name("others").type().array().items(childSchema).noDefault()
                .endRecord();

        final GenericRecord c1 = new GenericRecordBuilder(childSchema).set("name", "a").set("age", 1L).build();
        final GenericRecord c2 = new GenericRecordBuilder(childSchema).set("name", "b").set("age", 2L).build();
        final GenericRecord c3 = new GenericRecordBuilder(childSchema).set("name", "c").set("age", 3L).build();

        final GenericRecord parent = new GenericRecordBuilder(parentSchema)
                .set("children", List.of(c1, c2))
                .set("others", List.of(c3))
                .build();

        // no primary field matched: first mutation becomes primary
        final MutationGroup group = AvroToMutationConverter.convertGroup(parentSchema, parent, "INSERT", null);
        Assertions.assertEquals("children", group.primary().getTable());
        Assertions.assertEquals(Value.string("a"), group.primary().asMap().get("name"));
        Assertions.assertEquals(Value.int64(1L), group.primary().asMap().get("age"));
        Assertions.assertEquals(2, group.attached().size());
        Assertions.assertEquals("children", group.attached().get(0).getTable());
        Assertions.assertEquals(Value.string("b"), group.attached().get(0).asMap().get("name"));
        Assertions.assertEquals("others", group.attached().get(1).getTable());
        Assertions.assertEquals(Value.string("c"), group.attached().get(1).asMap().get("name"));

        // primary field matched on an array field: the primary mutation must not be duplicated in attached
        final MutationGroup primaryGroup = AvroToMutationConverter.convertGroup(parentSchema, parent, "INSERT", "children");
        Assertions.assertEquals("children", primaryGroup.primary().getTable());
        Assertions.assertEquals(Value.string("a"), primaryGroup.primary().asMap().get("name"));
        Assertions.assertEquals(Value.int64(1L), primaryGroup.primary().asMap().get("age"));
        Assertions.assertEquals(2, primaryGroup.attached().size());
        Assertions.assertEquals("children", primaryGroup.attached().get(0).getTable());
        Assertions.assertEquals(Value.string("b"), primaryGroup.attached().get(0).asMap().get("name"));
        Assertions.assertEquals("others", primaryGroup.attached().get(1).getTable());
        Assertions.assertEquals(Value.string("c"), primaryGroup.attached().get(1).asMap().get("name"));
    }

    private static Type fieldType(final Type structType, final String fieldName) {
        return structType.getStructFields().get(structType.getFieldIndex(fieldName)).getType();
    }

}
