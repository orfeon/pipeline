package com.mercari.solution.util.schema;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.type.LatLng;
import com.mercari.solution.TestDatum;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntitySchemaUtilTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testSelectFields() {
        final Row row = TestDatum.generateRow();
        final List<String> fields = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");

        final Schema schema = RowSchemaUtil.selectFields(row.getSchema(), fields);
        final Entity entity = TestDatum.generateEntity();

        // entity test
        final Entity selectedEntity = EntitySchemaUtil.toBuilder(schema, entity).build();
        Assertions.assertEquals(5, selectedEntity.getPropertiesCount());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedEntity.getPropertiesOrThrow("stringField").getStringValue());
        Assertions.assertEquals(TestDatum.getIntFieldValue().longValue(), selectedEntity.getPropertiesOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(TestDatum.getLongFieldValue().longValue(), selectedEntity.getPropertiesOrThrow("longField").getIntegerValue());

        final Entity selectedEntityChild = selectedEntity.getPropertiesOrThrow("recordField").getEntityValue();
        Assertions.assertEquals(5, selectedEntityChild.getPropertiesCount());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedEntityChild.getPropertiesOrThrow("stringField").getStringValue());
        Assertions.assertEquals(TestDatum.getDoubleFieldValue().doubleValue(), selectedEntityChild.getPropertiesOrThrow("doubleField").getDoubleValue(), DELTA);
        Assertions.assertEquals(TestDatum.getBooleanFieldValue(), selectedEntityChild.getPropertiesOrThrow("booleanField").getBooleanValue());

        final Entity selectedEntityGrandchild = selectedEntityChild.getPropertiesOrThrow("recordField").getEntityValue();
        Assertions.assertEquals(2, selectedEntityGrandchild.getPropertiesCount());
        Assertions.assertEquals(TestDatum.getIntFieldValue().longValue(), selectedEntityGrandchild.getPropertiesOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), selectedEntityGrandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);

        Assertions.assertEquals(2, selectedEntity.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
        for(final Value childValue : selectedEntity.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
            final Entity child = childValue.getEntityValue();
            Assertions.assertEquals(4, child.getPropertiesCount());
            Assertions.assertEquals(TestDatum.getStringFieldValue(), child.getPropertiesOrThrow("stringField").getStringValue());
            Assertions.assertEquals(TestDatum.getTimestampFieldValue().getMillis(),
                    Timestamps.toMillis(child.getPropertiesOrThrow("timestampField").getTimestampValue()));

            Assertions.assertEquals(2, child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
            for(final Value grandchilValue : child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
                final Entity grandchild = grandchilValue.getEntityValue();
                Assertions.assertEquals(2, grandchild.getPropertiesCount());
                Assertions.assertEquals(TestDatum.getIntFieldValue().longValue(), grandchild.getPropertiesOrThrow("intField").getIntegerValue());
                Assertions.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), grandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
            }

            final Entity grandchild = child.getPropertiesOrThrow("recordField").getEntityValue();
            Assertions.assertEquals(TestDatum.getIntFieldValue().longValue(), grandchild.getPropertiesOrThrow("intField").getIntegerValue());
            Assertions.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), grandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
        }

        // null fields row test
        final Row rowNull = TestDatum.generateRowNull();
        final List<String> newFields = new ArrayList<>(fields);
        newFields.add("recordFieldNull");
        newFields.add("recordArrayFieldNull");
        final Schema schemaNull = RowSchemaUtil.selectFields(rowNull.getSchema(), newFields);

        final Entity entityNull = TestDatum.generateEntityNull();
        final Entity selectedEntityNull = EntitySchemaUtil.toBuilder(schemaNull, entityNull).build();
        Assertions.assertEquals(7, selectedEntityNull.getPropertiesCount());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("stringField").getNullValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("intField").getNullValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("longField").getNullValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("recordFieldNull").getNullValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityNull.getPropertiesOrThrow("recordArrayFieldNull").getNullValue());

        final Entity selectedEntityChildNull = selectedEntityNull.getPropertiesOrThrow("recordField").getEntityValue();
        Assertions.assertEquals(5, selectedEntityChildNull.getPropertiesCount());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityChildNull.getPropertiesOrThrow("stringField").getNullValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityChildNull.getPropertiesOrThrow("doubleField").getNullValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityChildNull.getPropertiesOrThrow("booleanField").getNullValue());

        final Entity selectedEntityGrandchildNull = selectedEntityChildNull.getPropertiesOrThrow("recordField").getEntityValue();
        Assertions.assertEquals(2, selectedEntityGrandchildNull.getPropertiesCount());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityGrandchildNull.getPropertiesOrThrow("intField").getNullValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selectedEntityGrandchildNull.getPropertiesOrThrow("floatField").getNullValue());

        Assertions.assertEquals(2, selectedEntityNull.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
        for(final Value childValue : selectedEntityNull.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
            final Entity child = childValue.getEntityValue();
            Assertions.assertEquals(4, child.getPropertiesCount());
            Assertions.assertEquals(NullValue.NULL_VALUE, child.getPropertiesOrThrow("stringField").getNullValue());
            Assertions.assertEquals(NullValue.NULL_VALUE, child.getPropertiesOrThrow("timestampField").getNullValue());

            Assertions.assertEquals(2, child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
            for(final Value grandchildValue : child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
                final Entity grandchild = grandchildValue.getEntityValue();
                Assertions.assertEquals(2, grandchild.getPropertiesCount());
                Assertions.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("intField").getNullValue());
                Assertions.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("floatField").getNullValue());
            }

            final Entity grandchild = child.getPropertiesOrThrow("recordField").getEntityValue();
            Assertions.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("intField").getNullValue());
            Assertions.assertEquals(NullValue.NULL_VALUE, grandchild.getPropertiesOrThrow("floatField").getNullValue());
        }

    }

    private static final java.time.Instant TEST_INSTANT = java.time.Instant.parse("2021-03-02T01:02:03Z");
    private static final Timestamp TEST_TIMESTAMP = Timestamp.newBuilder().setSeconds(TEST_INSTANT.getEpochSecond()).build();
    private static final long TEST_TIMESTAMP_MICROS = TEST_INSTANT.getEpochSecond() * 1_000_000L;
    private static final byte[] TEST_BYTES = "byteValue".getBytes(StandardCharsets.UTF_8);
    private static final LocalDate TEST_DATE = LocalDate.of(2021, 3, 2);
    private static final LocalTime TEST_TIME = LocalTime.of(15, 24, 1);

    private static Value stringValue(final String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value integerValue(final long value) {
        return Value.newBuilder().setIntegerValue(value).build();
    }

    private static Value doubleValue(final double value) {
        return Value.newBuilder().setDoubleValue(value).build();
    }

    private static Value booleanValue(final boolean value) {
        return Value.newBuilder().setBooleanValue(value).build();
    }

    private static Value nullValue() {
        return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    }

    private static Entity createTestEntity() {
        final Entity child = Entity.newBuilder()
                .putProperties("childString", stringValue("childValue"))
                .putProperties("childInt", integerValue(7L))
                .build();
        final Key key = Key.newBuilder()
                .addPath(Key.PathElement.newBuilder().setKind("Parent").setId(123L))
                .addPath(Key.PathElement.newBuilder().setKind("Child").setName("c1"))
                .build();
        return Entity.newBuilder()
                .setKey(Key.newBuilder().addPath(Key.PathElement.newBuilder().setKind("MyKind").setName("key1")))
                .putProperties("stringField", stringValue("stringValue"))
                .putProperties("intField", integerValue(42L))
                .putProperties("doubleField", doubleValue(1.5D))
                .putProperties("booleanField", booleanValue(true))
                .putProperties("bytesField", Value.newBuilder().setBlobValue(ByteString.copyFrom(TEST_BYTES)).build())
                .putProperties("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .putProperties("keyField", Value.newBuilder().setKeyValue(key).build())
                .putProperties("geoField", Value.newBuilder().setGeoPointValue(LatLng.newBuilder().setLatitude(35.0D).setLongitude(139.0D)).build())
                .putProperties("entityField", Value.newBuilder().setEntityValue(child).build())
                .putProperties("arrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(integerValue(1L))
                        .addValues(integerValue(2L))
                        .addValues(integerValue(3L))).build())
                .putProperties("nullField", nullValue())
                .build();
    }

    @Test
    public void testGetValueByFieldName() {
        final Entity entity = createTestEntity();

        Assertions.assertEquals("stringValue", EntitySchemaUtil.getValue(entity, "stringField"));
        Assertions.assertEquals(42L, EntitySchemaUtil.getValue(entity, "intField"));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.getValue(entity, "doubleField"));
        Assertions.assertEquals(true, EntitySchemaUtil.getValue(entity, "booleanField"));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) EntitySchemaUtil.getValue(entity, "bytesField"));
        Assertions.assertEquals(org.joda.time.Instant.ofEpochMilli(TEST_INSTANT.toEpochMilli()), EntitySchemaUtil.getValue(entity, "timestampField"));
        Assertions.assertEquals(LatLng.newBuilder().setLatitude(35.0D).setLongitude(139.0D).build(), EntitySchemaUtil.getValue(entity, "geoField"));
        Assertions.assertEquals(Arrays.asList(1L, 2L, 3L), EntitySchemaUtil.getValue(entity, "arrayField"));
        Assertions.assertNull(EntitySchemaUtil.getValue(entity, "nullField"));
        Assertions.assertNull(EntitySchemaUtil.getValue(entity, "notExistsField"));
        Assertions.assertNull(EntitySchemaUtil.getValue((Entity) null, "stringField"));

        final Entity child = (Entity) EntitySchemaUtil.getValue(entity, "entityField");
        Assertions.assertEquals("childValue", EntitySchemaUtil.getValue(child, "childString"));

        // getFieldValue / getFieldValueAsString
        Assertions.assertEquals("stringValue", EntitySchemaUtil.getFieldValue(entity, "stringField"));
        Assertions.assertEquals(42L, EntitySchemaUtil.getFieldValue(entity, "intField"));
        Assertions.assertNull(EntitySchemaUtil.getFieldValue(entity, "notExistsField"));
        Assertions.assertEquals("42", EntitySchemaUtil.getFieldValueAsString(entity, "intField"));
        Assertions.assertEquals("1.5", EntitySchemaUtil.getFieldValueAsString(entity, "doubleField"));
        Assertions.assertNull(EntitySchemaUtil.getFieldValueAsString(entity, "nullField"));
    }

    @Test
    public void testGetValueByValue() {
        Assertions.assertEquals("stringValue", EntitySchemaUtil.getValue(stringValue("stringValue")));
        Assertions.assertEquals(42L, EntitySchemaUtil.getValue(integerValue(42L)));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.getValue(doubleValue(1.5D)));
        Assertions.assertEquals(true, EntitySchemaUtil.getValue(booleanValue(true)));
        Assertions.assertEquals(ByteString.copyFrom(TEST_BYTES),
                EntitySchemaUtil.getValue(Value.newBuilder().setBlobValue(ByteString.copyFrom(TEST_BYTES)).build()));
        Assertions.assertEquals(TEST_TIMESTAMP,
                EntitySchemaUtil.getValue(Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build()));
        Assertions.assertNull(EntitySchemaUtil.getValue(nullValue()));
        Assertions.assertNull(EntitySchemaUtil.getValue((Value) null));

        final Key key = Key.newBuilder().addPath(Key.PathElement.newBuilder().setKind("MyKind").setId(1L)).build();
        Assertions.assertEquals(key, EntitySchemaUtil.getValue(Value.newBuilder().setKeyValue(key).build()));

        final ArrayValue arrayValue = ArrayValue.newBuilder().addValues(integerValue(1L)).build();
        Assertions.assertEquals(arrayValue, EntitySchemaUtil.getValue(Value.newBuilder().setArrayValue(arrayValue).build()));
    }

    @Test
    public void testGetKeyFieldValue() {
        final Entity namedKeyEntity = Entity.newBuilder()
                .setKey(Key.newBuilder().addPath(Key.PathElement.newBuilder().setKind("MyKind").setName("name1")))
                .build();
        Assertions.assertEquals("name1", EntitySchemaUtil.getKeyFieldValue(namedKeyEntity, "any"));

        final Entity idKeyEntity = Entity.newBuilder()
                .setKey(Key.newBuilder().addPath(Key.PathElement.newBuilder().setKind("MyKind").setId(123L)))
                .build();
        Assertions.assertEquals(123L, EntitySchemaUtil.getKeyFieldValue(idKeyEntity, "any"));
    }

    @Test
    public void testGetAsString() {
        final Entity entity = createTestEntity();

        Assertions.assertEquals("stringValue", EntitySchemaUtil.getAsString(entity, "stringField"));
        Assertions.assertEquals("42", EntitySchemaUtil.getAsString(entity, "intField"));
        Assertions.assertEquals("1.5", EntitySchemaUtil.getAsString(entity, "doubleField"));
        Assertions.assertEquals("true", EntitySchemaUtil.getAsString(entity, "booleanField"));
        Assertions.assertEquals(Base64.getEncoder().encodeToString(TEST_BYTES), EntitySchemaUtil.getAsString(entity, "bytesField"));
        Assertions.assertEquals("2021-03-02T01:02:03.000Z", EntitySchemaUtil.getAsString(entity, "timestampField"));
        Assertions.assertEquals("Parent,123,Child,c1", EntitySchemaUtil.getAsString(entity, "keyField"));
        Assertions.assertNull(EntitySchemaUtil.getAsString(entity, "nullField"));
        Assertions.assertNull(EntitySchemaUtil.getAsString(entity, "notExistsField"));
        Assertions.assertNull(EntitySchemaUtil.getAsString((Object) null, "stringField"));
        Assertions.assertEquals("stringValue", EntitySchemaUtil.getAsString((Object) entity, "stringField"));

        // by value
        Assertions.assertEquals("stringValue", EntitySchemaUtil.getAsString(stringValue("stringValue")));
        Assertions.assertEquals("42", EntitySchemaUtil.getAsString(integerValue(42L)));
        Assertions.assertNull(EntitySchemaUtil.getAsString(nullValue()));
    }

    @Test
    public void testGetAsNumbers() {
        final Entity entity = Entity.newBuilder()
                .putProperties("booleanField", booleanValue(true))
                .putProperties("intField", integerValue(42L))
                .putProperties("doubleField", doubleValue(1.5D))
                .putProperties("stringField", stringValue("12"))
                .putProperties("stringDoubleField", stringValue("2.5"))
                .putProperties("illegalField", stringValue("abc"))
                .putProperties("nullField", nullValue())
                .build();

        Assertions.assertEquals(1L, EntitySchemaUtil.getAsLong(entity, "booleanField"));
        Assertions.assertEquals(42L, EntitySchemaUtil.getAsLong(entity, "intField"));
        Assertions.assertEquals(1L, EntitySchemaUtil.getAsLong(entity, "doubleField"));
        Assertions.assertEquals(12L, EntitySchemaUtil.getAsLong(entity, "stringField"));
        Assertions.assertNull(EntitySchemaUtil.getAsLong(entity, "illegalField"));
        Assertions.assertNull(EntitySchemaUtil.getAsLong(entity, "nullField"));
        Assertions.assertNull(EntitySchemaUtil.getAsLong(entity, "notExistsField"));

        Assertions.assertEquals(1D, EntitySchemaUtil.getAsDouble(entity, "booleanField"));
        Assertions.assertEquals(42D, EntitySchemaUtil.getAsDouble(entity, "intField"));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.getAsDouble(entity, "doubleField"));
        Assertions.assertEquals(2.5D, EntitySchemaUtil.getAsDouble(entity, "stringDoubleField"));
        Assertions.assertNull(EntitySchemaUtil.getAsDouble(entity, "illegalField"));
        Assertions.assertNull(EntitySchemaUtil.getAsDouble(entity, "notExistsField"));

        Assertions.assertEquals(BigDecimal.valueOf(1D), EntitySchemaUtil.getAsBigDecimal(entity, "booleanField"));
        Assertions.assertEquals(BigDecimal.valueOf(42L), EntitySchemaUtil.getAsBigDecimal(entity, "intField"));
        Assertions.assertEquals(BigDecimal.valueOf(1.5D), EntitySchemaUtil.getAsBigDecimal(entity, "doubleField"));
        Assertions.assertEquals(BigDecimal.valueOf(2.5D), EntitySchemaUtil.getAsBigDecimal(entity, "stringDoubleField"));
        Assertions.assertNull(EntitySchemaUtil.getAsBigDecimal(entity, "illegalField"));
        Assertions.assertNull(EntitySchemaUtil.getAsBigDecimal(entity, "notExistsField"));
    }

    @Test
    public void testGetBytes() {
        final Entity entity = Entity.newBuilder()
                .putProperties("bytesField", Value.newBuilder().setBlobValue(ByteString.copyFrom(TEST_BYTES)).build())
                .putProperties("stringField", stringValue(Base64.getEncoder().encodeToString(TEST_BYTES)))
                .putProperties("intField", integerValue(42L))
                .build();

        Assertions.assertArrayEquals(TEST_BYTES, EntitySchemaUtil.getBytes(entity, "bytesField"));
        Assertions.assertArrayEquals(TEST_BYTES, EntitySchemaUtil.getBytes(entity, "stringField"));
        Assertions.assertNull(EntitySchemaUtil.getBytes(entity, "intField"));
        Assertions.assertNull(EntitySchemaUtil.getBytes(entity, "notExistsField"));
        Assertions.assertNull(EntitySchemaUtil.getBytes(null, "bytesField"));

        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), EntitySchemaUtil.getAsBytes(entity, "bytesField"));
        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), EntitySchemaUtil.getAsBytes(entity, "stringField"));
        Assertions.assertNull(EntitySchemaUtil.getAsBytes(entity, "intField"));
        Assertions.assertNull(EntitySchemaUtil.getAsBytes(entity, "notExistsField"));
        Assertions.assertNull(EntitySchemaUtil.getAsBytes(null, "bytesField"));
    }

    @Test
    public void testGetAsByteString() {
        final Entity entity = createTestEntity();

        Assertions.assertEquals(BigtableSchemaUtil.toByteString(true), EntitySchemaUtil.getAsByteString(entity, "booleanField"));
        Assertions.assertEquals(BigtableSchemaUtil.toByteString("stringValue"), EntitySchemaUtil.getAsByteString(entity, "stringField"));
        Assertions.assertEquals(BigtableSchemaUtil.toByteString(42L), EntitySchemaUtil.getAsByteString(entity, "intField"));
        Assertions.assertEquals(BigtableSchemaUtil.toByteString(1.5D), EntitySchemaUtil.getAsByteString(entity, "doubleField"));
        Assertions.assertEquals(BigtableSchemaUtil.toByteString(TEST_TIMESTAMP_MICROS), EntitySchemaUtil.getAsByteString(entity, "timestampField"));
        Assertions.assertEquals(ByteString.copyFrom(TEST_BYTES), EntitySchemaUtil.getAsByteString(entity, "bytesField"));
        Assertions.assertNull(EntitySchemaUtil.getAsByteString(entity, "nullField"));
        Assertions.assertNull(EntitySchemaUtil.getAsByteString(entity, "notExistsField"));
        Assertions.assertNull(EntitySchemaUtil.getAsByteString(null, "stringField"));
        Assertions.assertNull(EntitySchemaUtil.getAsByteString(entity, null));
    }

    @Test
    public void testGetAsFloatList() {
        final Entity entity = Entity.newBuilder()
                .putProperties("arrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(doubleValue(1.5D))
                        .addValues(integerValue(2L))
                        .addValues(stringValue("3.5"))
                        .addValues(booleanValue(true))
                        .addValues(nullValue())).build())
                .putProperties("emptyArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()).build())
                .putProperties("stringField", stringValue("a"))
                .build();

        Assertions.assertEquals(Arrays.asList(1.5F, 2F, 3.5F, 1F, null), EntitySchemaUtil.getAsFloatList(entity, "arrayField"));
        Assertions.assertEquals(new ArrayList<>(), EntitySchemaUtil.getAsFloatList(entity, "emptyArrayField"));
        Assertions.assertEquals(new ArrayList<>(), EntitySchemaUtil.getAsFloatList(entity, "stringField"));
        Assertions.assertEquals(new ArrayList<>(), EntitySchemaUtil.getAsFloatList(entity, "notExistsField"));
        Assertions.assertEquals(new ArrayList<>(), EntitySchemaUtil.getAsFloatList(null, "arrayField"));
    }

    @Test
    public void testGetTimestamp() {
        final Entity entity = Entity.newBuilder()
                .putProperties("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .putProperties("stringField", stringValue("2021-03-02T01:02:03Z"))
                .putProperties("integerField", integerValue(TEST_INSTANT.toEpochMilli()))
                .putProperties("illegalField", stringValue("not a timestamp"))
                .putProperties("booleanField", booleanValue(true))
                .build();

        final org.joda.time.Instant expected = org.joda.time.Instant.ofEpochMilli(TEST_INSTANT.toEpochMilli());
        Assertions.assertEquals(expected, EntitySchemaUtil.getTimestamp(entity, "timestampField"));
        Assertions.assertEquals(expected, EntitySchemaUtil.getTimestamp(entity, "stringField"));
        Assertions.assertEquals(expected, EntitySchemaUtil.getTimestamp(entity, "integerField"));
        Assertions.assertEquals(org.joda.time.Instant.ofEpochSecond(0L), EntitySchemaUtil.getTimestamp(entity, "notExistsField"));

        final org.joda.time.Instant defaultInstant = org.joda.time.Instant.ofEpochMilli(1234L);
        Assertions.assertEquals(defaultInstant, EntitySchemaUtil.getTimestamp(entity, "notExistsField", defaultInstant));
        Assertions.assertEquals(defaultInstant, EntitySchemaUtil.getTimestamp(entity, "illegalField", defaultInstant));
        Assertions.assertEquals(defaultInstant, EntitySchemaUtil.getTimestamp(entity, "booleanField", defaultInstant));

        // toProtoTimestamp
        Assertions.assertEquals(TEST_TIMESTAMP, EntitySchemaUtil.toProtoTimestamp(expected));
        Assertions.assertNull(EntitySchemaUtil.toProtoTimestamp(null));
    }

    @Test
    public void testGetAsPrimitiveByFieldTypeAndFieldName() {
        final Entity entity = Entity.newBuilder()
                .putProperties("stringField", stringValue("12"))
                .putProperties("intField", integerValue(42L))
                .putProperties("doubleField", doubleValue(1.5D))
                .putProperties("booleanField", booleanValue(true))
                .putProperties("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .putProperties("timestampStringField", stringValue("2021-03-02T01:02:03Z"))
                .putProperties("dateField", stringValue("2021-03-02"))
                .putProperties("timeField", stringValue("15:24:01"))
                .putProperties("enumField", stringValue("b"))
                .putProperties("arrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(integerValue(1L))
                        .addValues(integerValue(2L))).build())
                .putProperties("nullField", nullValue())
                .build();

        Assertions.assertEquals(12, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.INT32, "stringField"));
        Assertions.assertEquals(42, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.INT32, "intField"));
        Assertions.assertEquals(1, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.INT32, "doubleField"));
        Assertions.assertEquals(1, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.INT32, "booleanField"));
        Assertions.assertEquals(12L, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.INT64, "stringField"));
        Assertions.assertEquals(42L, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.INT64, "intField"));
        Assertions.assertEquals(12F, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.FLOAT, "stringField"));
        Assertions.assertEquals(1.5F, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.FLOAT, "doubleField"));
        Assertions.assertEquals(12D, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.DOUBLE, "stringField"));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.DOUBLE, "doubleField"));
        Assertions.assertEquals(true, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.BOOLEAN, "booleanField"));
        Assertions.assertEquals(true, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.BOOLEAN, "intField"));
        Assertions.assertEquals("12", EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.STRING, "stringField"));
        Assertions.assertEquals("42", EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.STRING, "intField"));
        Assertions.assertEquals("1.5", EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.STRING, "doubleField"));
        Assertions.assertEquals("true", EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.STRING, "booleanField"));

        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.DATETIME, "timestampField"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.DATETIME, "timestampStringField"));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(), EntitySchemaUtil.getAsPrimitive(entity, dateType, "dateField"));
        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        // TIME primitive representation is micros-of-day (Long), consistent with the (fieldType, Object) overload
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, EntitySchemaUtil.getAsPrimitive(entity, timeType, "timeField"));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));
        Assertions.assertEquals("b", EntitySchemaUtil.getAsPrimitive(entity, enumType, "enumField"));

        Assertions.assertEquals(Arrays.asList(1L, 2L), EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.array(Schema.FieldType.INT64), "arrayField"));

        Assertions.assertNull(EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.STRING, "nullField"));
        Assertions.assertNull(EntitySchemaUtil.getAsPrimitive(entity, Schema.FieldType.STRING, "notExistsField"));
        Assertions.assertNull(EntitySchemaUtil.getAsPrimitive(null, Schema.FieldType.STRING, "stringField"));
        Assertions.assertNull(EntitySchemaUtil.getAsPrimitive("notAnEntity", Schema.FieldType.STRING, "stringField"));
    }

    @Test
    public void testGetAsPrimitiveByFieldTypeAndObject() {
        Assertions.assertNull(EntitySchemaUtil.getAsPrimitive(Schema.FieldType.STRING, (Object) null));
        Assertions.assertEquals("a", EntitySchemaUtil.getAsPrimitive(Schema.FieldType.STRING, (Object) "a"));
        Assertions.assertEquals(42L, EntitySchemaUtil.getAsPrimitive(Schema.FieldType.INT64, (Object) 42L));
        Assertions.assertEquals(42, EntitySchemaUtil.getAsPrimitive(Schema.FieldType.INT32, (Object) 42L));
        Assertions.assertEquals(1.5F, EntitySchemaUtil.getAsPrimitive(Schema.FieldType.FLOAT, (Object) 1.5D));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.getAsPrimitive(Schema.FieldType.DOUBLE, (Object) 1.5D));
        Assertions.assertEquals(true, EntitySchemaUtil.getAsPrimitive(Schema.FieldType.BOOLEAN, (Object) true));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, EntitySchemaUtil.getAsPrimitive(Schema.FieldType.DATETIME, (Object) TEST_TIMESTAMP));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        final com.google.cloud.Date date = com.google.cloud.Date.fromYearMonthDay(2021, 3, 2);
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(), EntitySchemaUtil.getAsPrimitive(dateType, (Object) date));
        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, EntitySchemaUtil.getAsPrimitive(timeType, (Object) "15:24:01"));

        // arrays
        Assertions.assertEquals(Arrays.asList(1L, 2L), EntitySchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.INT64), (Object) Arrays.asList(1L, 2L)));
        Assertions.assertEquals(Arrays.asList(1, 2), EntitySchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.INT32), (Object) Arrays.asList(1L, 2L)));
        Assertions.assertEquals(Arrays.asList(1.5F, 2.5F), EntitySchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.FLOAT), (Object) Arrays.asList(1.5D, 2.5D)));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP_MICROS), EntitySchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.DATETIME), (Object) List.of(TEST_TIMESTAMP)));
        Assertions.assertEquals(List.of((int) TEST_DATE.toEpochDay()), EntitySchemaUtil.getAsPrimitive(Schema.FieldType.array(dateType), (Object) List.of(date)));
        Assertions.assertEquals(List.of(TEST_TIME.toNanoOfDay() / 1000L), EntitySchemaUtil.getAsPrimitive(Schema.FieldType.array(timeType), (Object) List.of("15:24:01")));
    }

    @Test
    public void testGetAsPrimitiveByValue() {
        Assertions.assertNull(EntitySchemaUtil.getAsPrimitive((Value) null));
        Assertions.assertNull(EntitySchemaUtil.getAsPrimitive(nullValue()));
        Assertions.assertEquals("stringValue", EntitySchemaUtil.getAsPrimitive(stringValue("stringValue")));
        Assertions.assertEquals(42L, EntitySchemaUtil.getAsPrimitive(integerValue(42L)));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.getAsPrimitive(doubleValue(1.5D)));
        Assertions.assertEquals(true, EntitySchemaUtil.getAsPrimitive(booleanValue(true)));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) EntitySchemaUtil.getAsPrimitive(Value.newBuilder().setBlobValue(ByteString.copyFrom(TEST_BYTES)).build()));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, EntitySchemaUtil.getAsPrimitive(Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build()));

        final Entity child = Entity.newBuilder()
                .putProperties("childString", stringValue("childValue"))
                .putProperties("childInt", integerValue(7L))
                .build();
        final Map<String, Object> childMap = (Map<String, Object>) EntitySchemaUtil.getAsPrimitive(Value.newBuilder().setEntityValue(child).build());
        Assertions.assertEquals("childValue", childMap.get("childString"));
        Assertions.assertEquals(7L, childMap.get("childInt"));

        Assertions.assertEquals(Arrays.asList(1L, 2L), EntitySchemaUtil.getAsPrimitive(Value.newBuilder()
                .setArrayValue(ArrayValue.newBuilder().addValues(integerValue(1L)).addValues(integerValue(2L))).build()));
    }

    @Test
    public void testGetAsStandard() {
        Assertions.assertNull(EntitySchemaUtil.getAsStandard(null));
        Assertions.assertNull(EntitySchemaUtil.getAsStandard(nullValue()));
        Assertions.assertEquals("stringValue", EntitySchemaUtil.getAsStandard(stringValue("stringValue")));
        Assertions.assertEquals(42L, EntitySchemaUtil.getAsStandard(integerValue(42L)));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.getAsStandard(doubleValue(1.5D)));
        Assertions.assertEquals(true, EntitySchemaUtil.getAsStandard(booleanValue(true)));
        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), EntitySchemaUtil.getAsStandard(Value.newBuilder().setBlobValue(ByteString.copyFrom(TEST_BYTES)).build()));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, EntitySchemaUtil.getAsStandard(Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build()));

        final Key key = Key.newBuilder().addPath(Key.PathElement.newBuilder().setKind("MyKind").setId(1L)).build();
        Assertions.assertEquals(key.toString(), EntitySchemaUtil.getAsStandard(Value.newBuilder().setKeyValue(key).build()));

        Assertions.assertEquals(Arrays.asList(1L, 2L), EntitySchemaUtil.getAsStandard(Value.newBuilder()
                .setArrayValue(ArrayValue.newBuilder().addValues(integerValue(1L)).addValues(integerValue(2L))).build()));
    }

    @Test
    public void testConvertPrimitive() {
        Assertions.assertNull(EntitySchemaUtil.convertPrimitive(Schema.FieldType.INT64, null));
        Assertions.assertEquals(42, EntitySchemaUtil.convertPrimitive(Schema.FieldType.INT32, 42));
        Assertions.assertEquals(42L, EntitySchemaUtil.convertPrimitive(Schema.FieldType.INT64, 42L));
        Assertions.assertEquals(1.5F, EntitySchemaUtil.convertPrimitive(Schema.FieldType.FLOAT, 1.5F));
        Assertions.assertEquals(1.5D, EntitySchemaUtil.convertPrimitive(Schema.FieldType.DOUBLE, 1.5D));
        Assertions.assertEquals("a", EntitySchemaUtil.convertPrimitive(Schema.FieldType.STRING, "a"));
        Assertions.assertEquals(true, EntitySchemaUtil.convertPrimitive(Schema.FieldType.BOOLEAN, true));
        Assertions.assertEquals(TEST_TIMESTAMP, EntitySchemaUtil.convertPrimitive(Schema.FieldType.DATETIME, TEST_TIMESTAMP_MICROS));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        Assertions.assertEquals("2021-03-02", EntitySchemaUtil.convertPrimitive(dateType, (int) TEST_DATE.toEpochDay()));
        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        Assertions.assertEquals("15:24:01", EntitySchemaUtil.convertPrimitive(timeType, TEST_TIME.toNanoOfDay() / 1000L));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));
        final EnumerationType.Value enumValue = (EnumerationType.Value) EntitySchemaUtil.convertPrimitive(enumType, 1);
        Assertions.assertEquals(1, enumValue.getValue());

        // arrays
        Assertions.assertEquals(Arrays.asList(1L, 2L), EntitySchemaUtil.convertPrimitive(Schema.FieldType.array(Schema.FieldType.INT64), Arrays.asList(1L, 2L)));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP), EntitySchemaUtil.convertPrimitive(Schema.FieldType.array(Schema.FieldType.DATETIME), List.of(TEST_TIMESTAMP_MICROS)));
        Assertions.assertEquals(List.of("2021-03-02"), EntitySchemaUtil.convertPrimitive(Schema.FieldType.array(dateType), List.of((int) TEST_DATE.toEpochDay())));
        Assertions.assertEquals(List.of("15:24:01"), EntitySchemaUtil.convertPrimitive(Schema.FieldType.array(timeType), List.of(TEST_TIME.toNanoOfDay() / 1000L)));
        final List<EnumerationType.Value> enumValues = (List<EnumerationType.Value>) EntitySchemaUtil.convertPrimitive(Schema.FieldType.array(enumType), List.of(2));
        Assertions.assertEquals(2, enumValues.get(0).getValue());
    }

    @Test
    public void testCreate() {
        final Schema schema = Schema.builder()
                .addField("booleanField", Schema.FieldType.BOOLEAN)
                .addField("stringField", Schema.FieldType.STRING)
                .addField("bytesField", Schema.FieldType.BYTES)
                .addField("intField", Schema.FieldType.INT32)
                .addField("longField", Schema.FieldType.INT64)
                .addField("floatField", Schema.FieldType.FLOAT)
                .addField("doubleField", Schema.FieldType.DOUBLE)
                .addField("decimalField", Schema.FieldType.DECIMAL)
                .addField("timestampField", Schema.FieldType.DATETIME)
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()))
                .addField("timeField", Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()))
                .addField("enumField", Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c")))
                .addField("nullField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Map<String, Object> values = new HashMap<>();
        values.put("booleanField", true);
        values.put("stringField", "stringValue");
        values.put("bytesField", TEST_BYTES);
        values.put("intField", 42);
        values.put("longField", 420L);
        values.put("floatField", 1.5F);
        values.put("doubleField", 2.5D);
        values.put("decimalField", BigDecimal.valueOf(1234, 2));
        values.put("timestampField", TEST_TIMESTAMP);
        values.put("dateField", "2021-03-02");
        values.put("timeField", "15:24:01");
        values.put("enumField", "b");
        values.put("nullField", null);

        final Entity entity = EntitySchemaUtil.create(schema, values);
        Assertions.assertEquals(true, entity.getPropertiesOrThrow("booleanField").getBooleanValue());
        Assertions.assertEquals("stringValue", entity.getPropertiesOrThrow("stringField").getStringValue());
        Assertions.assertEquals(ByteString.copyFrom(TEST_BYTES), entity.getPropertiesOrThrow("bytesField").getBlobValue());
        Assertions.assertEquals(42L, entity.getPropertiesOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(420L, entity.getPropertiesOrThrow("longField").getIntegerValue());
        Assertions.assertEquals(1.5D, entity.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
        Assertions.assertEquals(2.5D, entity.getPropertiesOrThrow("doubleField").getDoubleValue(), DELTA);
        Assertions.assertEquals("12.34", entity.getPropertiesOrThrow("decimalField").getStringValue());
        Assertions.assertEquals(TEST_TIMESTAMP, entity.getPropertiesOrThrow("timestampField").getTimestampValue());
        Assertions.assertEquals("2021-03-02", entity.getPropertiesOrThrow("dateField").getStringValue());
        Assertions.assertEquals("15:24:01", entity.getPropertiesOrThrow("timeField").getStringValue());
        Assertions.assertEquals("b", entity.getPropertiesOrThrow("enumField").getStringValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, entity.getPropertiesOrThrow("nullField").getNullValue());
    }

    @Test
    public void testMerge() {
        final Entity base = Entity.newBuilder()
                .putProperties("existingField", stringValue("existingValue"))
                .build();

        final Schema schema = Schema.builder()
                .addField("booleanField", Schema.FieldType.BOOLEAN)
                .addField("stringField", Schema.FieldType.STRING)
                .addField("bytesField", Schema.FieldType.BYTES)
                .addField("byteField", Schema.FieldType.BYTE)
                .addField("shortField", Schema.FieldType.INT16)
                .addField("intField", Schema.FieldType.INT32)
                .addField("longField", Schema.FieldType.INT64)
                .addField("floatField", Schema.FieldType.FLOAT)
                .addField("doubleField", Schema.FieldType.DOUBLE)
                .addField("timestampField", Schema.FieldType.DATETIME)
                .addField("decimalField", Schema.FieldType.DECIMAL)
                .addField("entityField", Schema.FieldType.row(Schema.builder().addStringField("childString").build()))
                .addField("nullField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Entity child = Entity.newBuilder().putProperties("childString", stringValue("childValue")).build();
        final Map<String, Object> values = new HashMap<>();
        values.put("booleanField", true);
        values.put("stringField", "stringValue");
        values.put("bytesField", TEST_BYTES);
        values.put("byteField", (byte) 3);
        values.put("shortField", (short) 12);
        values.put("intField", 42);
        values.put("longField", 420L);
        values.put("floatField", 1.5F);
        values.put("doubleField", 2.5D);
        values.put("timestampField", TEST_TIMESTAMP_MICROS);
        values.put("decimalField", BigDecimal.valueOf(1234, 2));
        values.put("entityField", child);

        final Entity merged = EntitySchemaUtil.merge(schema, base, values);
        Assertions.assertEquals("existingValue", merged.getPropertiesOrThrow("existingField").getStringValue());
        Assertions.assertEquals(true, merged.getPropertiesOrThrow("booleanField").getBooleanValue());
        Assertions.assertEquals("stringValue", merged.getPropertiesOrThrow("stringField").getStringValue());
        Assertions.assertEquals(ByteString.copyFrom(TEST_BYTES), merged.getPropertiesOrThrow("bytesField").getBlobValue());
        Assertions.assertEquals(3L, merged.getPropertiesOrThrow("byteField").getIntegerValue());
        Assertions.assertEquals(12L, merged.getPropertiesOrThrow("shortField").getIntegerValue());
        Assertions.assertEquals(42L, merged.getPropertiesOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(420L, merged.getPropertiesOrThrow("longField").getIntegerValue());
        Assertions.assertEquals(1.5D, merged.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
        Assertions.assertEquals(2.5D, merged.getPropertiesOrThrow("doubleField").getDoubleValue(), DELTA);
        Assertions.assertEquals(TEST_TIMESTAMP, merged.getPropertiesOrThrow("timestampField").getTimestampValue());
        Assertions.assertEquals("12.34", merged.getPropertiesOrThrow("decimalField").getStringValue());
        Assertions.assertEquals(child, merged.getPropertiesOrThrow("entityField").getEntityValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, merged.getPropertiesOrThrow("nullField").getNullValue());
    }

    @Test
    public void testConvertSchema() {
        final List<Entity> metaEntities = new ArrayList<>();
        metaEntities.add(createMetaEntity("name", "String"));
        metaEntities.add(createMetaEntity("age", "Integer"));
        metaEntities.add(createMetaEntity("score", "Float"));
        metaEntities.add(createMetaEntity("flag", "Boolean"));
        metaEntities.add(createMetaEntity("created", "Date/Time"));
        metaEntities.add(createMetaEntity("data", "Blob"));
        metaEntities.add(createMetaEntity("skipped", "NULL"));
        metaEntities.add(createMetaEntity("child.x", "String"));
        metaEntities.add(createMetaEntity("child.y", "Integer"));

        final Schema schema = EntitySchemaUtil.convertSchema("", metaEntities);
        Assertions.assertEquals(7, schema.getFieldCount());
        Assertions.assertEquals(Schema.TypeName.STRING, schema.getField("name").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.INT64, schema.getField("age").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DOUBLE, schema.getField("score").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.BOOLEAN, schema.getField("flag").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.DATETIME, schema.getField("created").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.BYTES, schema.getField("data").getType().getTypeName());
        Assertions.assertFalse(schema.hasField("skipped"));

        Assertions.assertEquals(Schema.TypeName.ROW, schema.getField("child").getType().getTypeName());
        final Schema childSchema = schema.getField("child").getType().getRowSchema();
        Assertions.assertEquals(2, childSchema.getFieldCount());
        Assertions.assertEquals(Schema.TypeName.STRING, childSchema.getField("x").getType().getTypeName());
        Assertions.assertEquals(Schema.TypeName.INT64, childSchema.getField("y").getType().getTypeName());
    }

    private static Entity createMetaEntity(final String propertyName, final String propertyType) {
        return Entity.newBuilder()
                .putProperties("property_name", stringValue(propertyName))
                .putProperties("property_type", stringValue(propertyType))
                .build();
    }

    @Test
    public void testToBuilderWithRenameFields() {
        final Entity entity = createTestEntity();
        final Schema schema = Schema.builder()
                .addField("renamedField", Schema.FieldType.STRING.withNullable(true))
                .addField("intField", Schema.FieldType.INT64.withNullable(true))
                .addField("missingField", Schema.FieldType.STRING.withNullable(true))
                .build();
        final Map<String, String> renameFields = new HashMap<>();
        renameFields.put("renamedField", "stringField");

        final Entity renamed = EntitySchemaUtil.toBuilder(schema, entity, renameFields).build();
        Assertions.assertEquals(3, renamed.getPropertiesCount());
        Assertions.assertEquals("stringValue", renamed.getPropertiesOrThrow("renamedField").getStringValue());
        Assertions.assertEquals(42L, renamed.getPropertiesOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, renamed.getPropertiesOrThrow("missingField").getNullValue());
    }

    @Test
    public void testToBuilderWithIncludeExcludeFields() {
        final Entity entity = createTestEntity();

        final Entity included = EntitySchemaUtil.toBuilder(entity, Arrays.asList("stringField", "intField"), null).build();
        Assertions.assertEquals(2, included.getPropertiesCount());
        Assertions.assertEquals("stringValue", included.getPropertiesOrThrow("stringField").getStringValue());
        Assertions.assertEquals(42L, included.getPropertiesOrThrow("intField").getIntegerValue());

        final Entity excluded = EntitySchemaUtil.toBuilder(entity, null, Arrays.asList("stringField")).build();
        Assertions.assertEquals(entity.getPropertiesCount() - 1, excluded.getPropertiesCount());
        Assertions.assertFalse(excluded.containsProperties("stringField"));

        // copy
        final Entity copied = EntitySchemaUtil.toBuilder(entity).build();
        Assertions.assertEquals(entity, copied);
    }

    @Test
    public void testConvertBuilder() {
        final Entity entity = createTestEntity();
        final Schema schema = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING)
                .addField("intField", Schema.FieldType.INT64)
                .build();

        // no excludeFromIndexFields: as-is
        final Entity asIs = EntitySchemaUtil.convertBuilder(schema, entity, new ArrayList<>()).build();
        Assertions.assertEquals(entity, asIs);
        final Entity asIsNull = EntitySchemaUtil.convertBuilder(schema, entity, null).build();
        Assertions.assertEquals(entity, asIsNull);

        final Entity converted = EntitySchemaUtil.convertBuilder(schema, entity, Arrays.asList("stringField")).build();
        Assertions.assertEquals(2, converted.getPropertiesCount());
        Assertions.assertTrue(converted.getPropertiesOrThrow("stringField").getExcludeFromIndexes());
        Assertions.assertEquals("stringValue", converted.getPropertiesOrThrow("stringField").getStringValue());
        Assertions.assertFalse(converted.getPropertiesOrThrow("intField").getExcludeFromIndexes());
        Assertions.assertEquals(entity.getKey(), converted.getKey());
    }

    @Test
    public void testAsPrimitiveMapAndStandardMap() {
        final Entity entity = Entity.newBuilder()
                .putProperties("stringField", stringValue("stringValue"))
                .putProperties("intField", integerValue(42L))
                .putProperties("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .build();

        final Map<String, Object> primitiveMap = EntitySchemaUtil.asPrimitiveMap(entity);
        Assertions.assertEquals(3, primitiveMap.size());
        Assertions.assertEquals("stringValue", primitiveMap.get("stringField"));
        Assertions.assertEquals(42L, primitiveMap.get("intField"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, primitiveMap.get("timestampField"));
        Assertions.assertEquals(new HashMap<>(), EntitySchemaUtil.asPrimitiveMap(null));

        final Map<String, Object> standardMap = EntitySchemaUtil.asStandardMap(entity, Arrays.asList("stringField"));
        Assertions.assertEquals(1, standardMap.size());
        Assertions.assertEquals("stringValue", standardMap.get("stringField"));
        final Map<String, Object> standardMapAll = EntitySchemaUtil.asStandardMap(entity, null);
        Assertions.assertEquals(3, standardMapAll.size());
        Assertions.assertEquals(new HashMap<>(), EntitySchemaUtil.asStandardMap(null, null));
    }

    @Test
    public void testConvertDate() {
        Assertions.assertEquals(com.google.cloud.Date.fromYearMonthDay(2021, 3, 2),
                EntitySchemaUtil.convertDate(stringValue("2021-03-02")));
        Assertions.assertEquals(com.google.cloud.Date.fromYearMonthDay(2021, 3, 2),
                EntitySchemaUtil.convertDate(integerValue(TEST_DATE.toEpochDay())));
        Assertions.assertThrows(IllegalArgumentException.class, () -> EntitySchemaUtil.convertDate(booleanValue(true)));
    }

}
