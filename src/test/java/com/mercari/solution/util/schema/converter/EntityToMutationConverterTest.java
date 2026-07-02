package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EntityToMutationConverterTest {

    // 2024-03-01T12:00:00Z
    private static final long EPOCH_SECONDS = 1709294400L;

    private static Type createTestType() {
        return Type.struct(
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("jsonField", Type.json()),
                Type.StructField.of("boolField", Type.bool()),
                Type.StructField.of("int64Field", Type.int64()),
                Type.StructField.of("float64Field", Type.float64()),
                Type.StructField.of("numericField", Type.numeric()),
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("dateIntField", Type.date()),
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("stringArrayField", Type.array(Type.string())),
                Type.StructField.of("boolArrayField", Type.array(Type.bool())),
                Type.StructField.of("int64ArrayField", Type.array(Type.int64())),
                Type.StructField.of("float64ArrayField", Type.array(Type.float64())),
                Type.StructField.of("numericArrayField", Type.array(Type.numeric())),
                Type.StructField.of("bytesArrayField", Type.array(Type.bytes())),
                Type.StructField.of("dateArrayField", Type.array(Type.date())),
                Type.StructField.of("timestampArrayField", Type.array(Type.timestamp())));
    }

    private static com.google.datastore.v1.Value vString(final String value) {
        return com.google.datastore.v1.Value.newBuilder().setStringValue(value).build();
    }

    private static com.google.datastore.v1.Value vBool(final boolean value) {
        return com.google.datastore.v1.Value.newBuilder().setBooleanValue(value).build();
    }

    private static com.google.datastore.v1.Value vInt(final long value) {
        return com.google.datastore.v1.Value.newBuilder().setIntegerValue(value).build();
    }

    private static com.google.datastore.v1.Value vDouble(final double value) {
        return com.google.datastore.v1.Value.newBuilder().setDoubleValue(value).build();
    }

    private static com.google.datastore.v1.Value vBytes(final String value) {
        return com.google.datastore.v1.Value.newBuilder()
                .setBlobValue(ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private static com.google.datastore.v1.Value vTimestamp(final long seconds, final int nanos) {
        return com.google.datastore.v1.Value.newBuilder()
                .setTimestampValue(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(seconds).setNanos(nanos).build())
                .build();
    }

    private static com.google.datastore.v1.Value vNull() {
        return com.google.datastore.v1.Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    }

    private static com.google.datastore.v1.Value vArray(final com.google.datastore.v1.Value... values) {
        return com.google.datastore.v1.Value.newBuilder()
                .setArrayValue(ArrayValue.newBuilder().addAllValues(List.of(values)).build())
                .build();
    }

    private static Entity createTestEntity(final Key key) {
        return Entity.newBuilder()
                .setKey(key)
                .putProperties("stringField", vString("hello"))
                .putProperties("jsonField", vString("{\"a\":1}"))
                .putProperties("boolField", vBool(true))
                .putProperties("int64Field", vInt(64L))
                .putProperties("float64Field", vDouble(2.5d))
                .putProperties("numericField", vDouble(123.456d))
                .putProperties("bytesField", vBytes("bytes"))
                .putProperties("dateField", vString("2024-03-01"))
                .putProperties("dateIntField", vInt(19783L)) // 2024-03-01 = epoch day 19783
                .putProperties("timestampField", vTimestamp(EPOCH_SECONDS, 0))
                .putProperties("stringArrayField", vArray(vString("a"), vString("b")))
                .putProperties("boolArrayField", vArray(vBool(true), vBool(false)))
                .putProperties("int64ArrayField", vArray(vInt(1L), vInt(2L)))
                .putProperties("float64ArrayField", vArray(vDouble(2.5d)))
                .putProperties("numericArrayField", vArray(vDouble(1.5d)))
                .putProperties("bytesArrayField", vArray(vBytes("b1")))
                .putProperties("dateArrayField", vArray(vString("2024-03-02")))
                .putProperties("timestampArrayField", vArray(vTimestamp(EPOCH_SECONDS, 0)))
                .build();
    }

    private static Key nameKey(final String name) {
        return Key.newBuilder()
                .addPath(Key.PathElement.newBuilder().setKind("TestKind").setName(name).build())
                .build();
    }

    private static Key idKey(final long id) {
        return Key.newBuilder()
                .addPath(Key.PathElement.newBuilder().setKind("TestKind").setId(id).build())
                .build();
    }

    @Test
    public void testConvert() {
        final Type type = createTestType();
        final Entity entity = createTestEntity(nameKey("key1"));

        final Mutation mutation = EntityToMutationConverter
                .convert(type, entity, "MyTable", null, null, null, null, null);

        Assertions.assertEquals("MyTable", mutation.getTable());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutation.getOperation());

        final Map<String, Value> map = mutation.asMap();
        Assertions.assertEquals(Value.string("key1"), map.get("__key__"));
        Assertions.assertEquals(Value.string("hello"), map.get("stringField"));
        Assertions.assertEquals(Value.string("{\"a\":1}"), map.get("jsonField"));
        Assertions.assertEquals(Value.bool(true), map.get("boolField"));
        Assertions.assertEquals(Value.int64(64L), map.get("int64Field"));
        Assertions.assertEquals(Value.float64(2.5d), map.get("float64Field"));
        Assertions.assertEquals(Value.numeric(BigDecimal.valueOf(123.456d)), map.get("numericField"));
        Assertions.assertEquals(Value.bytes(ByteArray.copyFrom("bytes")), map.get("bytesField"));
        Assertions.assertEquals(Value.date(Date.fromYearMonthDay(2024, 3, 1)), map.get("dateField"));
        Assertions.assertEquals(Value.date(Date.fromYearMonthDay(2024, 3, 1)), map.get("dateIntField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.ofTimeSecondsAndNanos(EPOCH_SECONDS, 0)), map.get("timestampField"));

        Assertions.assertEquals(Value.stringArray(List.of("a", "b")), map.get("stringArrayField"));
        Assertions.assertEquals(Value.boolArray(List.of(true, false)), map.get("boolArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of(1L, 2L)), map.get("int64ArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of(2.5d)), map.get("float64ArrayField"));
        Assertions.assertEquals(Value.numericArray(List.of(BigDecimal.valueOf(1.5d))), map.get("numericArrayField"));
        Assertions.assertEquals(Value.bytesArray(List.of(ByteArray.copyFrom("b1"))), map.get("bytesArrayField"));
        Assertions.assertEquals(Value.dateArray(List.of(Date.fromYearMonthDay(2024, 3, 2))), map.get("dateArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of(Timestamp.ofTimeSecondsAndNanos(EPOCH_SECONDS, 0))), map.get("timestampArrayField"));
    }

    @Test
    public void testConvertIdKey() {
        final Type type = createTestType();
        final Entity entity = createTestEntity(idKey(123L));

        final Mutation mutation = EntityToMutationConverter
                .convert(type, entity, "MyTable", "INSERT", null, null, null, null);

        Assertions.assertEquals(Value.int64(123L), mutation.asMap().get("__key__"));
    }

    @Test
    public void testConvertNullValues() {
        final Type type = createTestType();
        // some fields explicitly null, all others absent
        final Entity entity = Entity.newBuilder()
                .setKey(nameKey("key1"))
                .putProperties("stringField", vNull())
                .putProperties("stringArrayField", vNull())
                .build();

        final Mutation mutation = EntityToMutationConverter
                .convert(type, entity, "MyTable", "INSERT", null, null, null, null);

        final Map<String, Value> map = mutation.asMap();
        // null or missing scalar fields are simply not set
        Assertions.assertFalse(map.containsKey("stringField"));
        Assertions.assertFalse(map.containsKey("boolField"));
        Assertions.assertFalse(map.containsKey("int64Field"));
        Assertions.assertFalse(map.containsKey("timestampField"));

        // null or missing array fields are set as empty arrays
        Assertions.assertEquals(Value.stringArray(List.of()), map.get("stringArrayField"));
        Assertions.assertEquals(Value.boolArray(List.of()), map.get("boolArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of()), map.get("int64ArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of()), map.get("float64ArrayField"));
        Assertions.assertEquals(Value.numericArray(List.of()), map.get("numericArrayField"));
        Assertions.assertEquals(Value.bytesArray(List.of()), map.get("bytesArrayField"));
        Assertions.assertEquals(Value.dateArray(List.of()), map.get("dateArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of()), map.get("timestampArrayField"));
    }

    @Test
    public void testConvertMutationOps() {
        final Type type = createTestType();
        final Entity entity = createTestEntity(nameKey("key1"));

        Assertions.assertEquals(Mutation.Op.INSERT,
                EntityToMutationConverter.convert(type, entity, "T", "INSERT", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.UPDATE,
                EntityToMutationConverter.convert(type, entity, "T", "UPDATE", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.REPLACE,
                EntityToMutationConverter.convert(type, entity, "T", "REPLACE", null, null, null, null).getOperation());
        Assertions.assertEquals(Mutation.Op.INSERT_OR_UPDATE,
                EntityToMutationConverter.convert(type, entity, "T", "INSERT_OR_UPDATE", null, null, null, null).getOperation());
    }

    @Test
    public void testConvertDelete() {
        final Type type = createTestType();
        final Entity entity = createTestEntity(nameKey("key1"));

        final Mutation mutation = EntityToMutationConverter.convert(
                type, entity, "MyTable", "DELETE",
                List.of("stringField", "int64Field", "boolField", "float64Field", "timestampField"),
                null, null, null);

        Assertions.assertEquals(Mutation.Op.DELETE, mutation.getOperation());
        Assertions.assertEquals("MyTable", mutation.getTable());
        Assertions.assertEquals(
                KeySet.singleKey(com.google.cloud.spanner.Key.of(
                        "hello", 64L, true, 2.5d,
                        Timestamp.ofTimeSecondsAndNanos(EPOCH_SECONDS, 0))),
                mutation.getKeySet());

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                EntityToMutationConverter.convert(type, entity, "MyTable", "DELETE", null, null, null, null));
    }

    @Test
    public void testCreateKey() {
        final Entity entity = Entity.newBuilder()
                .setKey(nameKey("key1"))
                .putProperties("stringField", vString("hello"))
                .putProperties("intField", vInt(1L))
                .putProperties("bytesField", vBytes("bytes"))
                .putProperties("keyField", com.google.datastore.v1.Value.newBuilder()
                        .setKeyValue(Key.newBuilder()
                                .addPath(Key.PathElement.newBuilder().setKind("Parent").setId(5L).build())
                                .addPath(Key.PathElement.newBuilder().setKind("Child").setName("c1").build())
                                .build())
                        .build())
                .build();

        final com.google.cloud.spanner.Key key = EntityToMutationConverter.createKey(
                entity, List.of("stringField", "intField", "bytesField", "keyField", "missingField"));

        final com.google.cloud.spanner.Key expected = com.google.cloud.spanner.Key.newBuilder()
                .append("hello")
                .append(1L)
                .append(ByteArray.copyFrom("bytes"))
                .append(5L)
                .append("c1")
                .appendObject(null)
                .build();
        Assertions.assertEquals(expected, key);
    }

    @Test
    public void testConvertExcludeAndCommitTimestamp() {
        final Type type = createTestType();
        final Entity entity = createTestEntity(nameKey("key1"));

        final Mutation mutation = EntityToMutationConverter.convert(
                type, entity, "MyTable", "INSERT",
                null,
                List.of("timestampField", "createdAtField"),
                Set.of("stringField"),
                null);

        final Map<String, Value> map = mutation.asMap();
        Assertions.assertFalse(map.containsKey("stringField"));
        Assertions.assertEquals(Value.timestamp(Value.COMMIT_TIMESTAMP), map.get("timestampField"));
        Assertions.assertEquals(Value.timestamp(Value.COMMIT_TIMESTAMP), map.get("createdAtField"));
    }

    @Test
    public void testConvertNullArguments() {
        final Type type = createTestType();
        final Entity entity = createTestEntity(nameKey("key1"));

        Assertions.assertThrows(RuntimeException.class, () ->
                EntityToMutationConverter.convert(type, null, "T", "INSERT", null, null, null, null));
        Assertions.assertThrows(RuntimeException.class, () ->
                EntityToMutationConverter.convert(null, entity, "T", "INSERT", null, null, null, null));
    }

}
