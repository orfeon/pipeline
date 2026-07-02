package com.mercari.solution.util.schema;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
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

public class DocumentSchemaUtilTest {

    private static final double DELTA = 1e-15;

    private static final java.time.Instant TEST_INSTANT = java.time.Instant.parse("2021-03-02T01:02:03Z");
    private static final Timestamp TEST_TIMESTAMP = Timestamp.newBuilder().setSeconds(TEST_INSTANT.getEpochSecond()).build();
    private static final long TEST_TIMESTAMP_MICROS = TEST_INSTANT.getEpochSecond() * 1_000_000L;
    private static final byte[] TEST_BYTES = "byteValue".getBytes(StandardCharsets.UTF_8);
    private static final LocalDate TEST_DATE = LocalDate.of(2021, 3, 2);
    private static final LocalTime TEST_TIME = LocalTime.of(15, 24, 1);
    private static final String TEST_REFERENCE = "projects/p/databases/d/documents/users/user1";

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

    private static MapValue childMapValue() {
        return MapValue.newBuilder()
                .putFields("childString", stringValue("childValue"))
                .putFields("childInt", integerValue(7L))
                .build();
    }

    private static Document createTestDocument() {
        return Document.newBuilder()
                .setName("projects/p/databases/d/documents/tests/doc1")
                .putFields("stringField", stringValue("stringValue"))
                .putFields("intField", integerValue(42L))
                .putFields("doubleField", doubleValue(1.5D))
                .putFields("booleanField", booleanValue(true))
                .putFields("bytesField", Value.newBuilder().setBytesValue(ByteString.copyFrom(TEST_BYTES)).build())
                .putFields("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .putFields("referenceField", Value.newBuilder().setReferenceValue(TEST_REFERENCE).build())
                .putFields("geoField", Value.newBuilder().setGeoPointValue(LatLng.newBuilder().setLatitude(35.0D).setLongitude(139.0D)).build())
                .putFields("mapField", Value.newBuilder().setMapValue(childMapValue()).build())
                .putFields("arrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(integerValue(1L))
                        .addValues(integerValue(2L))
                        .addValues(integerValue(3L))).build())
                .putFields("nullField", nullValue())
                .build();
    }

    @Test
    public void testToBuilderWithSchema() {
        final Document document = createTestDocument();
        final Schema schema = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("intField", Schema.FieldType.INT64.withNullable(true))
                .addField("missingField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Document selected = DocumentSchemaUtil.toBuilder(schema, document).build();
        Assertions.assertEquals(document.getName(), selected.getName());
        Assertions.assertEquals(3, selected.getFieldsCount());
        Assertions.assertEquals("stringValue", selected.getFieldsOrThrow("stringField").getStringValue());
        Assertions.assertEquals(42L, selected.getFieldsOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, selected.getFieldsOrThrow("missingField").getNullValue());

        // from MapValue
        final Schema childSchema = Schema.builder()
                .addField("childString", Schema.FieldType.STRING.withNullable(true))
                .build();
        final Document fromMap = DocumentSchemaUtil.toBuilder(childSchema, childMapValue()).build();
        Assertions.assertEquals(1, fromMap.getFieldsCount());
        Assertions.assertEquals("childValue", fromMap.getFieldsOrThrow("childString").getStringValue());
    }

    @Test
    public void testToBuilderWithRenameFields() {
        final Document child1 = Document.newBuilder().putFields("childString", stringValue("c1")).putFields("childInt", integerValue(1L)).build();
        final Document child2 = Document.newBuilder().putFields("childString", stringValue("c2")).putFields("childInt", integerValue(2L)).build();
        final Document document = Document.newBuilder()
                .setName("projects/p/databases/d/documents/tests/doc1")
                .putFields("stringField", stringValue("stringValue"))
                .putFields("intField", integerValue(42L))
                .putFields("mapField", Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(child1.getFieldsMap())).build())
                .putFields("arrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(child1.getFieldsMap())))
                        .addValues(Value.newBuilder().setMapValue(MapValue.newBuilder().putAllFields(child2.getFieldsMap())))
                        .addValues(nullValue())).build())
                .build();

        final Schema childSchema = Schema.builder()
                .addField("childString", Schema.FieldType.STRING.withNullable(true))
                .build();
        final Schema schema = Schema.builder()
                .addField("renamedField", Schema.FieldType.STRING.withNullable(true))
                .addField("intField", Schema.FieldType.INT64.withNullable(true))
                .addField("mapField", Schema.FieldType.row(childSchema).withNullable(true))
                .addField("arrayField", Schema.FieldType.array(Schema.FieldType.row(childSchema)).withNullable(true))
                .addField("missingField", Schema.FieldType.STRING.withNullable(true))
                .build();
        final Map<String, String> renameFields = new HashMap<>();
        renameFields.put("renamedField", "stringField");

        final Document renamed = DocumentSchemaUtil.toBuilder(schema, document, renameFields).build();
        Assertions.assertEquals(document.getName(), renamed.getName());
        Assertions.assertEquals(5, renamed.getFieldsCount());
        Assertions.assertEquals("stringValue", renamed.getFieldsOrThrow("renamedField").getStringValue());
        Assertions.assertEquals(42L, renamed.getFieldsOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, renamed.getFieldsOrThrow("missingField").getNullValue());

        final MapValue renamedChild = renamed.getFieldsOrThrow("mapField").getMapValue();
        Assertions.assertEquals(1, renamedChild.getFieldsCount());
        Assertions.assertEquals("c1", renamedChild.getFieldsOrThrow("childString").getStringValue());

        final List<Value> children = renamed.getFieldsOrThrow("arrayField").getArrayValue().getValuesList();
        Assertions.assertEquals(3, children.size());
        Assertions.assertEquals("c1", children.get(0).getMapValue().getFieldsOrThrow("childString").getStringValue());
        Assertions.assertEquals(1, children.get(0).getMapValue().getFieldsCount());
        Assertions.assertEquals("c2", children.get(1).getMapValue().getFieldsOrThrow("childString").getStringValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, children.get(2).getNullValue());
    }

    @Test
    public void testGetAsString() {
        final Document document = createTestDocument();

        Assertions.assertEquals("stringValue", DocumentSchemaUtil.getAsString(document, "stringField"));
        Assertions.assertEquals("42", DocumentSchemaUtil.getAsString(document, "intField"));
        Assertions.assertEquals("1.5", DocumentSchemaUtil.getAsString(document, "doubleField"));
        Assertions.assertEquals("true", DocumentSchemaUtil.getAsString(document, "booleanField"));
        Assertions.assertEquals(Base64.getEncoder().encodeToString(TEST_BYTES), DocumentSchemaUtil.getAsString(document, "bytesField"));
        Assertions.assertEquals("2021-03-02T01:02:03.000Z", DocumentSchemaUtil.getAsString(document, "timestampField"));
        Assertions.assertEquals(TEST_REFERENCE, DocumentSchemaUtil.getAsString(document, "referenceField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsString(document, "mapField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsString(document, "arrayField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsString(document, "nullField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsString(document, "notExistsField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsString(document, null));
        Assertions.assertNull(DocumentSchemaUtil.getAsString((Object) null, "stringField"));
        Assertions.assertEquals("stringValue", DocumentSchemaUtil.getAsString((Object) document, "stringField"));

        // by value
        Assertions.assertEquals("stringValue", DocumentSchemaUtil.getAsString(stringValue("stringValue")));
        Assertions.assertEquals("42", DocumentSchemaUtil.getAsString(integerValue(42L)));
        Assertions.assertNull(DocumentSchemaUtil.getAsString(nullValue()));
    }

    @Test
    public void testGetAsNumbers() {
        final Document document = Document.newBuilder()
                .putFields("booleanField", booleanValue(true))
                .putFields("intField", integerValue(42L))
                .putFields("doubleField", doubleValue(1.5D))
                .putFields("stringField", stringValue("12"))
                .putFields("stringDoubleField", stringValue("2.5"))
                .putFields("illegalField", stringValue("abc"))
                .putFields("nullField", nullValue())
                .build();

        Assertions.assertEquals(1L, DocumentSchemaUtil.getAsLong(document, "booleanField"));
        Assertions.assertEquals(42L, DocumentSchemaUtil.getAsLong(document, "intField"));
        Assertions.assertEquals(1L, DocumentSchemaUtil.getAsLong(document, "doubleField"));
        Assertions.assertEquals(12L, DocumentSchemaUtil.getAsLong(document, "stringField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsLong(document, "illegalField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsLong(document, "nullField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsLong(document, "notExistsField"));

        Assertions.assertEquals(1D, DocumentSchemaUtil.getAsDouble(document, "booleanField"));
        Assertions.assertEquals(42D, DocumentSchemaUtil.getAsDouble(document, "intField"));
        Assertions.assertEquals(1.5D, DocumentSchemaUtil.getAsDouble(document, "doubleField"));
        Assertions.assertEquals(2.5D, DocumentSchemaUtil.getAsDouble(document, "stringDoubleField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsDouble(document, "illegalField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsDouble(document, "notExistsField"));

        Assertions.assertEquals(BigDecimal.valueOf(1D), DocumentSchemaUtil.getAsBigDecimal(document, "booleanField"));
        Assertions.assertEquals(BigDecimal.valueOf(42L), DocumentSchemaUtil.getAsBigDecimal(document, "intField"));
        Assertions.assertEquals(BigDecimal.valueOf(1.5D), DocumentSchemaUtil.getAsBigDecimal(document, "doubleField"));
        Assertions.assertEquals(BigDecimal.valueOf(2.5D), DocumentSchemaUtil.getAsBigDecimal(document, "stringDoubleField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsBigDecimal(document, "illegalField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsBigDecimal(document, "notExistsField"));
    }

    @Test
    public void testGetBytes() {
        final Document document = Document.newBuilder()
                .putFields("bytesField", Value.newBuilder().setBytesValue(ByteString.copyFrom(TEST_BYTES)).build())
                .putFields("stringField", stringValue(Base64.getEncoder().encodeToString(TEST_BYTES)))
                .putFields("intField", integerValue(42L))
                .build();

        Assertions.assertArrayEquals(TEST_BYTES, DocumentSchemaUtil.getBytes(document, "bytesField"));
        Assertions.assertArrayEquals(TEST_BYTES, DocumentSchemaUtil.getBytes(document, "stringField"));
        Assertions.assertNull(DocumentSchemaUtil.getBytes(document, "intField"));
        Assertions.assertNull(DocumentSchemaUtil.getBytes(document, "notExistsField"));
        Assertions.assertNull(DocumentSchemaUtil.getBytes(null, "bytesField"));
        Assertions.assertNull(DocumentSchemaUtil.getBytes(document, null));

        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), DocumentSchemaUtil.getAsBytes(document, "bytesField"));
        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), DocumentSchemaUtil.getAsBytes(document, "stringField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsBytes(document, "intField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsBytes(document, "notExistsField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsBytes(null, "bytesField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsBytes(document, null));
    }

    @Test
    public void testGetTimestamp() {
        final Document document = Document.newBuilder()
                .putFields("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .putFields("stringField", stringValue("2021-03-02T01:02:03Z"))
                .putFields("integerField", integerValue(TEST_INSTANT.toEpochMilli()))
                .putFields("illegalField", stringValue("not a timestamp"))
                .putFields("booleanField", booleanValue(true))
                .build();

        final org.joda.time.Instant expected = org.joda.time.Instant.ofEpochMilli(TEST_INSTANT.toEpochMilli());
        Assertions.assertEquals(expected, DocumentSchemaUtil.getTimestamp(document, "timestampField"));
        Assertions.assertEquals(expected, DocumentSchemaUtil.getTimestamp(document, "stringField"));
        Assertions.assertEquals(expected, DocumentSchemaUtil.getTimestamp(document, "integerField"));
        Assertions.assertEquals(org.joda.time.Instant.ofEpochSecond(0L), DocumentSchemaUtil.getTimestamp(document, "notExistsField"));

        final org.joda.time.Instant defaultInstant = org.joda.time.Instant.ofEpochMilli(1234L);
        Assertions.assertEquals(defaultInstant, DocumentSchemaUtil.getTimestamp(document, "notExistsField", defaultInstant));
        Assertions.assertEquals(defaultInstant, DocumentSchemaUtil.getTimestamp(document, "illegalField", defaultInstant));
        Assertions.assertEquals(defaultInstant, DocumentSchemaUtil.getTimestamp(document, "booleanField", defaultInstant));
    }

    @Test
    public void testGetAsFloatList() {
        final Document document = Document.newBuilder()
                .putFields("arrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(doubleValue(1.5D))
                        .addValues(integerValue(2L))
                        .addValues(stringValue("3.5"))
                        .addValues(booleanValue(true))
                        .addValues(nullValue())).build())
                .putFields("stringField", stringValue("a"))
                .build();

        Assertions.assertEquals(Arrays.asList(1.5F, 2F, 3.5F, 1F, null), DocumentSchemaUtil.getAsFloatList(document, "arrayField"));
        Assertions.assertEquals(new ArrayList<>(), DocumentSchemaUtil.getAsFloatList(document, "stringField"));
        Assertions.assertEquals(new ArrayList<>(), DocumentSchemaUtil.getAsFloatList(document, "notExistsField"));
        Assertions.assertEquals(new ArrayList<>(), DocumentSchemaUtil.getAsFloatList(null, "arrayField"));
        Assertions.assertEquals(new ArrayList<>(), DocumentSchemaUtil.getAsFloatList(document, null));
    }

    @Test
    public void testGetValue() {
        final Document document = createTestDocument();

        Assertions.assertEquals("stringValue", DocumentSchemaUtil.getValue(document, "stringField"));
        Assertions.assertEquals(42L, DocumentSchemaUtil.getValue(document, "intField"));
        Assertions.assertEquals(1.5D, DocumentSchemaUtil.getValue(document, "doubleField"));
        Assertions.assertEquals(true, DocumentSchemaUtil.getValue(document, "booleanField"));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) DocumentSchemaUtil.getValue(document, "bytesField"));
        Assertions.assertEquals(org.joda.time.Instant.ofEpochMilli(TEST_INSTANT.toEpochMilli()), DocumentSchemaUtil.getValue(document, "timestampField"));
        Assertions.assertEquals(childMapValue(), DocumentSchemaUtil.getValue(document, "mapField"));
        Assertions.assertEquals(LatLng.newBuilder().setLatitude(35.0D).setLongitude(139.0D).build(), DocumentSchemaUtil.getValue(document, "geoField"));
        Assertions.assertEquals(Arrays.asList(1L, 2L, 3L), DocumentSchemaUtil.getValue(document, "arrayField"));
        Assertions.assertNull(DocumentSchemaUtil.getValue(document, "nullField"));
        Assertions.assertNull(DocumentSchemaUtil.getValue(document, "notExistsField"));
        Assertions.assertNull(DocumentSchemaUtil.getValue(document, null));
        Assertions.assertNull(DocumentSchemaUtil.getValue((Document) null, "stringField"));
    }

    @Test
    public void testGetAsPrimitiveByFieldTypeAndFieldName() {
        final Document document = Document.newBuilder()
                .putFields("stringField", stringValue("12"))
                .putFields("intField", integerValue(42L))
                .putFields("doubleField", doubleValue(1.5D))
                .putFields("booleanField", booleanValue(true))
                .putFields("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .putFields("timestampStringField", stringValue("2021-03-02T01:02:03Z"))
                .putFields("dateField", stringValue("2021-03-02"))
                .putFields("timeField", stringValue("15:24:01"))
                .putFields("enumField", stringValue("b"))
                .putFields("arrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addValues(integerValue(1L))
                        .addValues(integerValue(2L))).build())
                .putFields("nullField", nullValue())
                .build();

        Assertions.assertEquals(12, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.INT32, "stringField"));
        Assertions.assertEquals(42, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.INT32, "intField"));
        Assertions.assertEquals(1, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.INT32, "doubleField"));
        Assertions.assertEquals(1, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.INT32, "booleanField"));
        Assertions.assertEquals(12L, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.INT64, "stringField"));
        Assertions.assertEquals(42L, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.INT64, "intField"));
        Assertions.assertEquals(12F, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.FLOAT, "stringField"));
        Assertions.assertEquals(1.5F, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.FLOAT, "doubleField"));
        Assertions.assertEquals(12D, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.DOUBLE, "stringField"));
        Assertions.assertEquals(1.5D, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.DOUBLE, "doubleField"));
        Assertions.assertEquals(true, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.BOOLEAN, "booleanField"));
        Assertions.assertEquals(true, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.BOOLEAN, "intField"));
        Assertions.assertEquals("12", DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.STRING, "stringField"));
        Assertions.assertEquals("42", DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.STRING, "intField"));
        Assertions.assertEquals("1.5", DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.STRING, "doubleField"));
        Assertions.assertEquals("true", DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.STRING, "booleanField"));

        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.DATETIME, "timestampField"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.DATETIME, "timestampStringField"));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(), DocumentSchemaUtil.getAsPrimitive(document, dateType, "dateField"));
        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        // TIME primitive representation is micros-of-day (Long), consistent with the (fieldType, Object) overload
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, DocumentSchemaUtil.getAsPrimitive(document, timeType, "timeField"));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));
        Assertions.assertEquals("b", DocumentSchemaUtil.getAsPrimitive(document, enumType, "enumField"));

        Assertions.assertEquals(Arrays.asList(1L, 2L), DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.array(Schema.FieldType.INT64), "arrayField"));

        Assertions.assertNull(DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.STRING, "nullField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsPrimitive(document, Schema.FieldType.STRING, "notExistsField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsPrimitive(null, Schema.FieldType.STRING, "stringField"));
        Assertions.assertNull(DocumentSchemaUtil.getAsPrimitive("notADocument", Schema.FieldType.STRING, "stringField"));
    }

    @Test
    public void testGetAsPrimitiveByFieldTypeAndObject() {
        Assertions.assertNull(DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.STRING, (Object) null));
        Assertions.assertEquals("a", DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.STRING, (Object) "a"));
        Assertions.assertEquals(42L, DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.INT64, (Object) 42L));
        Assertions.assertEquals(42, DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.INT32, (Object) 42L));
        Assertions.assertEquals(1.5F, DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.FLOAT, (Object) 1.5D));
        Assertions.assertEquals(1.5D, DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.DOUBLE, (Object) 1.5D));
        Assertions.assertEquals(true, DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.BOOLEAN, (Object) true));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.DATETIME, (Object) TEST_TIMESTAMP));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        final com.google.cloud.Date date = com.google.cloud.Date.fromYearMonthDay(2021, 3, 2);
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(), DocumentSchemaUtil.getAsPrimitive(dateType, (Object) date));
        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, DocumentSchemaUtil.getAsPrimitive(timeType, (Object) "15:24:01"));

        // arrays
        Assertions.assertEquals(Arrays.asList(1L, 2L), DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.INT64), (Object) Arrays.asList(1L, 2L)));
        Assertions.assertEquals(Arrays.asList(1, 2), DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.INT32), (Object) Arrays.asList(1L, 2L)));
        Assertions.assertEquals(Arrays.asList(1.5F, 2.5F), DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.FLOAT), (Object) Arrays.asList(1.5D, 2.5D)));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP_MICROS), DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.array(Schema.FieldType.DATETIME), (Object) List.of(TEST_TIMESTAMP)));
        Assertions.assertEquals(List.of((int) TEST_DATE.toEpochDay()), DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.array(dateType), (Object) List.of(date)));
        Assertions.assertEquals(List.of(TEST_TIME.toNanoOfDay() / 1000L), DocumentSchemaUtil.getAsPrimitive(Schema.FieldType.array(timeType), (Object) List.of("15:24:01")));
    }

    @Test
    public void testGetAsPrimitiveByValue() {
        Assertions.assertNull(DocumentSchemaUtil.getAsPrimitive((Value) null));
        Assertions.assertNull(DocumentSchemaUtil.getAsPrimitive(nullValue()));
        Assertions.assertEquals("stringValue", DocumentSchemaUtil.getAsPrimitive(stringValue("stringValue")));
        Assertions.assertEquals(42L, DocumentSchemaUtil.getAsPrimitive(integerValue(42L)));
        Assertions.assertEquals(1.5D, DocumentSchemaUtil.getAsPrimitive(doubleValue(1.5D)));
        Assertions.assertEquals(true, DocumentSchemaUtil.getAsPrimitive(booleanValue(true)));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) DocumentSchemaUtil.getAsPrimitive(Value.newBuilder().setBytesValue(ByteString.copyFrom(TEST_BYTES)).build()));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, DocumentSchemaUtil.getAsPrimitive(Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build()));
        Assertions.assertEquals(TEST_REFERENCE, DocumentSchemaUtil.getAsPrimitive(Value.newBuilder().setReferenceValue(TEST_REFERENCE).build()));

        final Map<String, Object> childMap = (Map<String, Object>) DocumentSchemaUtil.getAsPrimitive(Value.newBuilder().setMapValue(childMapValue()).build());
        Assertions.assertEquals("childValue", childMap.get("childString"));
        Assertions.assertEquals(7L, childMap.get("childInt"));

        Assertions.assertEquals(Arrays.asList(1L, 2L), DocumentSchemaUtil.getAsPrimitive(Value.newBuilder()
                .setArrayValue(ArrayValue.newBuilder().addValues(integerValue(1L)).addValues(integerValue(2L))).build()));
    }

    @Test
    public void testGetAsStandard() {
        Assertions.assertNull(DocumentSchemaUtil.getAsStandard(null));
        Assertions.assertNull(DocumentSchemaUtil.getAsStandard(nullValue()));
        Assertions.assertEquals("stringValue", DocumentSchemaUtil.getAsStandard(stringValue("stringValue")));
        Assertions.assertEquals(42L, DocumentSchemaUtil.getAsStandard(integerValue(42L)));
        Assertions.assertEquals(1.5D, DocumentSchemaUtil.getAsStandard(doubleValue(1.5D)));
        Assertions.assertEquals(true, DocumentSchemaUtil.getAsStandard(booleanValue(true)));
        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), DocumentSchemaUtil.getAsStandard(Value.newBuilder().setBytesValue(ByteString.copyFrom(TEST_BYTES)).build()));
        Assertions.assertEquals(TEST_INSTANT, DocumentSchemaUtil.getAsStandard(Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build()));
        Assertions.assertEquals(TEST_REFERENCE, DocumentSchemaUtil.getAsStandard(Value.newBuilder().setReferenceValue(TEST_REFERENCE).build()));

        final Map<String, Object> childMap = (Map<String, Object>) DocumentSchemaUtil.getAsStandard(Value.newBuilder().setMapValue(childMapValue()).build());
        Assertions.assertEquals("childValue", childMap.get("childString"));
        Assertions.assertEquals(7L, childMap.get("childInt"));

        Assertions.assertEquals(Arrays.asList(1L, 2L), DocumentSchemaUtil.getAsStandard(Value.newBuilder()
                .setArrayValue(ArrayValue.newBuilder().addValues(integerValue(1L)).addValues(integerValue(2L))).build()));
    }

    @Test
    public void testConvertPrimitive() {
        Assertions.assertNull(DocumentSchemaUtil.convertPrimitive(Schema.FieldType.INT64, null));
        Assertions.assertEquals(42, DocumentSchemaUtil.convertPrimitive(Schema.FieldType.INT32, 42));
        Assertions.assertEquals(42L, DocumentSchemaUtil.convertPrimitive(Schema.FieldType.INT64, 42L));
        Assertions.assertEquals(1.5F, DocumentSchemaUtil.convertPrimitive(Schema.FieldType.FLOAT, 1.5F));
        Assertions.assertEquals(1.5D, DocumentSchemaUtil.convertPrimitive(Schema.FieldType.DOUBLE, 1.5D));
        Assertions.assertEquals("a", DocumentSchemaUtil.convertPrimitive(Schema.FieldType.STRING, "a"));
        Assertions.assertEquals(true, DocumentSchemaUtil.convertPrimitive(Schema.FieldType.BOOLEAN, true));
        Assertions.assertEquals(TEST_TIMESTAMP, DocumentSchemaUtil.convertPrimitive(Schema.FieldType.DATETIME, TEST_TIMESTAMP_MICROS));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        Assertions.assertEquals("2021-03-02", DocumentSchemaUtil.convertPrimitive(dateType, (int) TEST_DATE.toEpochDay()));
        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        Assertions.assertEquals("15:24:01", DocumentSchemaUtil.convertPrimitive(timeType, TEST_TIME.toNanoOfDay() / 1000L));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));
        final EnumerationType.Value enumValue = (EnumerationType.Value) DocumentSchemaUtil.convertPrimitive(enumType, 1);
        Assertions.assertEquals(1, enumValue.getValue());

        // arrays
        Assertions.assertEquals(Arrays.asList(1L, 2L), DocumentSchemaUtil.convertPrimitive(Schema.FieldType.array(Schema.FieldType.INT64), Arrays.asList(1L, 2L)));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP), DocumentSchemaUtil.convertPrimitive(Schema.FieldType.array(Schema.FieldType.DATETIME), List.of(TEST_TIMESTAMP_MICROS)));
        Assertions.assertEquals(List.of("2021-03-02"), DocumentSchemaUtil.convertPrimitive(Schema.FieldType.array(dateType), List.of((int) TEST_DATE.toEpochDay())));
        Assertions.assertEquals(List.of("15:24:01"), DocumentSchemaUtil.convertPrimitive(Schema.FieldType.array(timeType), List.of(TEST_TIME.toNanoOfDay() / 1000L)));
        final List<EnumerationType.Value> enumValues = (List<EnumerationType.Value>) DocumentSchemaUtil.convertPrimitive(Schema.FieldType.array(enumType), List.of(2));
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

        final Document document = DocumentSchemaUtil.create(schema, values);
        Assertions.assertEquals(true, document.getFieldsOrThrow("booleanField").getBooleanValue());
        Assertions.assertEquals("stringValue", document.getFieldsOrThrow("stringField").getStringValue());
        Assertions.assertEquals(ByteString.copyFrom(TEST_BYTES), document.getFieldsOrThrow("bytesField").getBytesValue());
        Assertions.assertEquals(42L, document.getFieldsOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(420L, document.getFieldsOrThrow("longField").getIntegerValue());
        Assertions.assertEquals(1.5D, document.getFieldsOrThrow("floatField").getDoubleValue(), DELTA);
        Assertions.assertEquals(2.5D, document.getFieldsOrThrow("doubleField").getDoubleValue(), DELTA);
        Assertions.assertEquals("12.34", document.getFieldsOrThrow("decimalField").getStringValue());
        Assertions.assertEquals(TEST_TIMESTAMP, document.getFieldsOrThrow("timestampField").getTimestampValue());
        Assertions.assertEquals("2021-03-02", document.getFieldsOrThrow("dateField").getStringValue());
        Assertions.assertEquals("15:24:01", document.getFieldsOrThrow("timeField").getStringValue());
        Assertions.assertEquals("b", document.getFieldsOrThrow("enumField").getStringValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, document.getFieldsOrThrow("nullField").getNullValue());
    }

    @Test
    public void testMerge() {
        final Document base = Document.newBuilder()
                .putFields("existingField", stringValue("existingValue"))
                .build();

        final Schema childSchema = Schema.builder()
                .addField("childString", Schema.FieldType.STRING)
                .addField("childInt", Schema.FieldType.INT64)
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
                .addField("decimalField", Schema.FieldType.DECIMAL)
                .addField("timestampField", Schema.FieldType.DATETIME)
                .addField("timestampLongField", Schema.FieldType.DATETIME)
                .addField("timestampStringField", Schema.FieldType.DATETIME)
                .addField("rowField", Schema.FieldType.row(childSchema))
                .addField("nullField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Map<String, Object> childValues = new HashMap<>();
        childValues.put("childString", "childValue");
        childValues.put("childInt", 7L);

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
        values.put("decimalField", BigDecimal.valueOf(1234, 2));
        values.put("timestampField", TEST_TIMESTAMP);
        values.put("timestampLongField", TEST_TIMESTAMP_MICROS);
        values.put("timestampStringField", "2021-03-02T01:02:03Z");
        values.put("rowField", childValues);

        final Document merged = DocumentSchemaUtil.merge(schema, base, values);
        Assertions.assertEquals("existingValue", merged.getFieldsOrThrow("existingField").getStringValue());
        Assertions.assertEquals(true, merged.getFieldsOrThrow("booleanField").getBooleanValue());
        Assertions.assertEquals("stringValue", merged.getFieldsOrThrow("stringField").getStringValue());
        Assertions.assertEquals(ByteString.copyFrom(TEST_BYTES), merged.getFieldsOrThrow("bytesField").getBytesValue());
        Assertions.assertEquals(3L, merged.getFieldsOrThrow("byteField").getIntegerValue());
        Assertions.assertEquals(12L, merged.getFieldsOrThrow("shortField").getIntegerValue());
        Assertions.assertEquals(42L, merged.getFieldsOrThrow("intField").getIntegerValue());
        Assertions.assertEquals(420L, merged.getFieldsOrThrow("longField").getIntegerValue());
        Assertions.assertEquals(1.5D, merged.getFieldsOrThrow("floatField").getDoubleValue(), DELTA);
        Assertions.assertEquals(2.5D, merged.getFieldsOrThrow("doubleField").getDoubleValue(), DELTA);
        Assertions.assertEquals("12.34", merged.getFieldsOrThrow("decimalField").getStringValue());
        Assertions.assertEquals(TEST_TIMESTAMP, merged.getFieldsOrThrow("timestampField").getTimestampValue());
        Assertions.assertEquals(TEST_TIMESTAMP, merged.getFieldsOrThrow("timestampLongField").getTimestampValue());
        Assertions.assertEquals(TEST_TIMESTAMP, merged.getFieldsOrThrow("timestampStringField").getTimestampValue());
        Assertions.assertEquals(NullValue.NULL_VALUE, merged.getFieldsOrThrow("nullField").getNullValue());

        final MapValue mergedChild = merged.getFieldsOrThrow("rowField").getMapValue();
        Assertions.assertEquals("childValue", mergedChild.getFieldsOrThrow("childString").getStringValue());
        Assertions.assertEquals(7L, mergedChild.getFieldsOrThrow("childInt").getIntegerValue());
    }

    @Test
    public void testAsPrimitiveMapAndStandardMap() {
        final Document document = Document.newBuilder()
                .putFields("stringField", stringValue("stringValue"))
                .putFields("intField", integerValue(42L))
                .putFields("timestampField", Value.newBuilder().setTimestampValue(TEST_TIMESTAMP).build())
                .build();

        final Map<String, Object> primitiveMap = DocumentSchemaUtil.asPrimitiveMap(document);
        Assertions.assertEquals(3, primitiveMap.size());
        Assertions.assertEquals("stringValue", primitiveMap.get("stringField"));
        Assertions.assertEquals(42L, primitiveMap.get("intField"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, primitiveMap.get("timestampField"));
        Assertions.assertEquals(new HashMap<>(), DocumentSchemaUtil.asPrimitiveMap(null));

        final Map<String, Object> standardMap = DocumentSchemaUtil.asStandardMap(document, Arrays.asList("timestampField"));
        Assertions.assertEquals(1, standardMap.size());
        Assertions.assertEquals(TEST_INSTANT, standardMap.get("timestampField"));
        final Map<String, Object> standardMapAll = DocumentSchemaUtil.asStandardMap(document, null);
        Assertions.assertEquals(3, standardMapAll.size());
        Assertions.assertEquals(new HashMap<>(), DocumentSchemaUtil.asStandardMap(null, null));
    }

    @Test
    public void testConvertDate() {
        Assertions.assertEquals(com.google.cloud.Date.fromYearMonthDay(2021, 3, 2),
                DocumentSchemaUtil.convertDate(stringValue("2021-03-02")));
        Assertions.assertEquals(com.google.cloud.Date.fromYearMonthDay(2021, 3, 2),
                DocumentSchemaUtil.convertDate(integerValue(TEST_DATE.toEpochDay())));
        Assertions.assertThrows(IllegalArgumentException.class, () -> DocumentSchemaUtil.convertDate(booleanValue(true)));
    }

}
