package com.mercari.solution.util.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.TestDatum;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class AvroSchemaUtilTest {

    @Test
    public void testToBuilderProjection() {
        // Characterization for docs/developer/schema-redesign.md P3: this is the building block of
        // the storage source's parquet `fields` projection (StorageSource.createParquetRead).
        final Schema schema = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "root",
                  "namespace": "com.example",
                  "fields": [
                    { "name": "keepField", "type": "string" },
                    { "name": "dropField", "type": "long" },
                    { "name": "alsoKeepField", "type": "double" }
                  ]
                }
                """);

        // only listed fields survive; source-schema order is preserved
        final Schema projected = AvroSchemaUtil
                .toBuilder(schema, List.of("alsoKeepField", "keepField"))
                .endRecord();
        Assertions.assertEquals(2, projected.getFields().size());
        Assertions.assertEquals("keepField", projected.getFields().get(0).name());
        Assertions.assertEquals("alsoKeepField", projected.getFields().get(1).name());
        Assertions.assertEquals(schema.getField("keepField").schema(), projected.getField("keepField").schema());

        // names not present in the source schema are silently ignored (no error)
        final Schema ignoredUnknown = AvroSchemaUtil
                .toBuilder(schema, List.of("keepField", "noSuchField"))
                .endRecord();
        Assertions.assertEquals(1, ignoredUnknown.getFields().size());

        // null field list keeps everything
        final Schema all = AvroSchemaUtil.toBuilder(schema).endRecord();
        Assertions.assertEquals(3, all.getFields().size());
    }

    @Test
    public void testSelectFields() {
        final GenericRecord record = TestDatum.generateRecord();
        final List<String> fields = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");
        final Schema schema = AvroSchemaUtil.selectFields(record.getSchema(), fields);

        // schema test
        Assertions.assertEquals(5, schema.getFields().size());
        Assertions.assertNotNull(schema.getField("stringField"));
        Assertions.assertNotNull(schema.getField("intField"));
        Assertions.assertNotNull(schema.getField("longField"));
        Assertions.assertNotNull(schema.getField("recordField"));
        Assertions.assertNotNull(schema.getField("recordArrayField"));

        final Schema schemaChild = AvroSchemaUtil.unnestUnion(schema.getField("recordField").schema());
        Assertions.assertEquals(5, schemaChild.getFields().size());
        Assertions.assertNotNull(schemaChild.getField("stringField"));
        Assertions.assertNotNull(schemaChild.getField("doubleField"));
        Assertions.assertNotNull(schemaChild.getField("booleanField"));
        Assertions.assertNotNull(schemaChild.getField("recordField"));
        Assertions.assertNotNull(schemaChild.getField("recordArrayField"));

        Assertions.assertEquals(Schema.Type.ARRAY, AvroSchemaUtil.unnestUnion(schemaChild.getField("recordArrayField").schema()).getType());
        final Schema schemaChildChildren = AvroSchemaUtil.unnestUnion(AvroSchemaUtil.unnestUnion(schemaChild.getField("recordArrayField").schema()).getElementType());
        Assertions.assertEquals(2, schemaChildChildren.getFields().size());
        Assertions.assertNotNull(schemaChildChildren.getField("intField"));
        Assertions.assertNotNull(schemaChildChildren.getField("floatField"));

        final Schema schemaGrandchild = AvroSchemaUtil.unnestUnion(schemaChild.getField("recordField").schema());
        Assertions.assertEquals(2, schemaGrandchild.getFields().size());
        Assertions.assertNotNull(schemaGrandchild.getField("intField"));
        Assertions.assertNotNull(schemaGrandchild.getField("floatField"));

        Assertions.assertEquals(Schema.Type.ARRAY, AvroSchemaUtil.unnestUnion(schema.getField("recordArrayField").schema()).getType());
        final Schema schemaChildren = AvroSchemaUtil.unnestUnion(AvroSchemaUtil.unnestUnion(schema.getField("recordArrayField").schema()).getElementType());
        Assertions.assertEquals(4, schemaChildren.getFields().size());
        Assertions.assertNotNull(schemaChildren.getField("stringField"));
        Assertions.assertNotNull(schemaChildren.getField("timestampField"));
        Assertions.assertNotNull(schemaChildren.getField("recordField"));
        Assertions.assertNotNull(schemaChildren.getField("recordArrayField"));

        final Schema schemaChildrenChild = AvroSchemaUtil.unnestUnion(schemaChildren.getField("recordField").schema());
        Assertions.assertEquals(2, schemaChildrenChild.getFields().size());
        Assertions.assertNotNull(schemaChildrenChild.getField("intField"));
        Assertions.assertNotNull(schemaChildrenChild.getField("floatField"));

        Assertions.assertEquals(Schema.Type.ARRAY, AvroSchemaUtil.unnestUnion(schemaChildren.getField("recordArrayField").schema()).getType());
        final Schema schemaChildrenChildren = AvroSchemaUtil.unnestUnion(AvroSchemaUtil.unnestUnion(schemaChildren.getField("recordArrayField").schema()).getElementType());
        Assertions.assertEquals(2, schemaChildrenChildren.getFields().size());
        Assertions.assertNotNull(schemaChildrenChildren.getField("intField"));
        Assertions.assertNotNull(schemaChildrenChildren.getField("floatField"));


        // record test
        final GenericRecord selectedRecord = AvroSchemaUtil.toBuilder(schema, record).build();
        Assertions.assertEquals(5, selectedRecord.getSchema().getFields().size());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedRecord.get("stringField"));
        Assertions.assertEquals(TestDatum.getIntFieldValue(), selectedRecord.get("intField"));
        Assertions.assertEquals(TestDatum.getLongFieldValue(), selectedRecord.get("longField"));

        final GenericRecord selectedRecordChild = (GenericRecord) selectedRecord.get("recordField");
        Assertions.assertEquals(5, selectedRecordChild.getSchema().getFields().size());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedRecordChild.get("stringField"));
        Assertions.assertEquals(TestDatum.getDoubleFieldValue(), selectedRecordChild.get("doubleField"));
        Assertions.assertEquals(TestDatum.getBooleanFieldValue(), selectedRecordChild.get("booleanField"));

        final GenericRecord selectedRecordGrandchild = (GenericRecord) selectedRecordChild.get("recordField");
        Assertions.assertEquals(2, selectedRecordGrandchild.getSchema().getFields().size());
        Assertions.assertEquals(TestDatum.getIntFieldValue(), selectedRecordGrandchild.get("intField"));
        Assertions.assertEquals(TestDatum.getFloatFieldValue(), selectedRecordGrandchild.get("floatField"));

        Assertions.assertEquals(2, ((List)selectedRecord.get("recordArrayField")).size());
        for(final GenericRecord child : (List<GenericRecord>)selectedRecord.get("recordArrayField")) {
            Assertions.assertEquals(4, child.getSchema().getFields().size());
            Assertions.assertEquals(TestDatum.getStringFieldValue(), child.get("stringField"));
            Assertions.assertEquals(TestDatum.getTimestampFieldValue(), Instant.ofEpochMilli((Long)child.get("timestampField") / 1000));

            Assertions.assertEquals(2, ((List)child.get("recordArrayField")).size());
            for(final GenericRecord grandchild : (List<GenericRecord>)child.get("recordArrayField")) {
                Assertions.assertEquals(2, grandchild.getSchema().getFields().size());
                Assertions.assertEquals(TestDatum.getIntFieldValue(), grandchild.get("intField"));
                Assertions.assertEquals(TestDatum.getFloatFieldValue(), grandchild.get("floatField"));
            }

            final GenericRecord grandchild = (GenericRecord) child.get("recordField");
            Assertions.assertEquals(TestDatum.getIntFieldValue(), grandchild.get("intField"));
            Assertions.assertEquals(TestDatum.getFloatFieldValue(), grandchild.get("floatField"));
        }


        // null fields record test
        final GenericRecord recordNull = TestDatum.generateRecordNull();
        final List<String> newFields = new ArrayList<>(fields);
        newFields.add("recordFieldNull");
        newFields.add("recordArrayFieldNull");
        final Schema schemaNull = AvroSchemaUtil.selectFields(recordNull.getSchema(), newFields);

        final GenericRecord selectedRecordNull = AvroSchemaUtil.toBuilder(schemaNull, recordNull).build();
        Assertions.assertEquals(7, selectedRecordNull.getSchema().getFields().size());
        Assertions.assertNull(selectedRecordNull.get("stringField"));
        Assertions.assertNull(selectedRecordNull.get("intField"));
        Assertions.assertNull(selectedRecordNull.get("longField"));
        Assertions.assertNull(selectedRecordNull.get("recordFieldNull"));
        Assertions.assertNull(selectedRecordNull.get("recordArrayFieldNull"));

        final GenericRecord selectedRecordChildNull = (GenericRecord) selectedRecordNull.get("recordField");
        Assertions.assertEquals(5, selectedRecordChildNull.getSchema().getFields().size());
        Assertions.assertNull(selectedRecordChildNull.get("stringField"));
        Assertions.assertNull(selectedRecordChildNull.get("doubleField"));
        Assertions.assertNull(selectedRecordChildNull.get("booleanField"));

        final GenericRecord selectedRecordGrandchildNull = (GenericRecord) selectedRecordChildNull.get("recordField");
        Assertions.assertEquals(2, selectedRecordGrandchildNull.getSchema().getFields().size());
        Assertions.assertNull(selectedRecordGrandchildNull.get("intField"));
        Assertions.assertNull(selectedRecordGrandchildNull.get("floatField"));

        Assertions.assertEquals(2, ((List)selectedRecordNull.get("recordArrayField")).size());
        for(final GenericRecord child : (List<GenericRecord>)selectedRecordNull.get("recordArrayField")) {
            Assertions.assertEquals(4, child.getSchema().getFields().size());
            Assertions.assertNull(child.get("stringField"));
            Assertions.assertNull(child.get("timestampField"));

            Assertions.assertEquals(2, ((List)child.get("recordArrayField")).size());
            for(final GenericRecord grandchild : (List<GenericRecord>)child.get("recordArrayField")) {
                Assertions.assertEquals(2, grandchild.getSchema().getFields().size());
                Assertions.assertNull(grandchild.get("intField"));
                Assertions.assertNull(grandchild.get("floatField"));
            }

            final GenericRecord grandchild = (GenericRecord) child.get("recordField");
            Assertions.assertNull(grandchild.get("intField"));
            Assertions.assertNull(grandchild.get("floatField"));
        }
    }

    @Test
    public void testIsValidFieldName() {
        Assertions.assertTrue(AvroSchemaUtil.isValidFieldName("myfield"));
        Assertions.assertTrue(AvroSchemaUtil.isValidFieldName("Field"));
        Assertions.assertTrue(AvroSchemaUtil.isValidFieldName("a1234"));
        Assertions.assertTrue(AvroSchemaUtil.isValidFieldName("_a1234"));
        Assertions.assertTrue(AvroSchemaUtil.isValidFieldName("f"));
        Assertions.assertTrue(AvroSchemaUtil.isValidFieldName("_"));
        Assertions.assertTrue(AvroSchemaUtil.isValidFieldName("_1"));

        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName("@field"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName("1field"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName("1"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName(""));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName(" "));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName(" field"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName("parent.field"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName("parent-field"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName("parent/field"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName("parent@field"));
        Assertions.assertFalse(AvroSchemaUtil.isValidFieldName(null));
    }

    @Test
    public void testPrimitiveEncodeDecode() throws IOException {

        Boolean b = true;
        byte[] bytes = AvroSchemaUtil.encode(b);
        Object o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.BOOLEAN, bytes);
        Assertions.assertEquals(b, o);

        Integer i = 12345;
        bytes = AvroSchemaUtil.encode(i);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.INT32, bytes);
        Assertions.assertEquals(i, o);

        Long l = 1234567890L;
        bytes = AvroSchemaUtil.encode(l);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.INT64, bytes);
        Assertions.assertEquals(l, o);

        Float f = 12345.67890F;
        bytes = AvroSchemaUtil.encode(f);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.FLOAT32, bytes);
        Assertions.assertEquals(f, o);

        Double d = 1234567890.987654D;
        bytes = AvroSchemaUtil.encode(d);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.FLOAT64, bytes);
        Assertions.assertEquals(d, o);

        String s = "あいうえおかきくけこ123456789abcdefg";
        bytes = AvroSchemaUtil.encode(s);
        o = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.STRING, bytes);
        Assertions.assertEquals(s, o);

    }

    @Test
    public void testEncodeDecodeMap() throws IOException {

        final Map<String, Long> values = new HashMap<>();
        values.put("a",  1L);
        values.put("b",  2L);
        values.put("c", -1L);

        final byte[] serialized = AvroSchemaUtil.encode(values);
        final Object deserialized = AvroSchemaUtil.decode(com.mercari.solution.module.Schema.FieldType.map(com.mercari.solution.module.Schema.FieldType.INT64), serialized);

        Assertions.assertTrue(deserialized instanceof Map<?,?>);
        final Map<String, Long> mapFieldValueMap = (Map<String, Long>) deserialized;
        for(final Map.Entry<String, Long> entry : mapFieldValueMap.entrySet()) {
            switch (entry.getKey()) {
                case "a" -> Assertions.assertEquals( 1L, entry.getValue().longValue());
                case "b" -> Assertions.assertEquals( 2L, entry.getValue().longValue());
                case "c" -> Assertions.assertEquals(-1L, entry.getValue().longValue());
            }
        }

    }

    @Test
    public void testEncodeDecodeRecord() throws IOException {

        final String avroSchema = """
                {
                  "name": "root",
                  "type": "record",
                  "fields": [
                    { "name": "booleanField", "type": "boolean" },
                    { "name": "stringField", "type": "string" },
                    { "name": "intField", "type": "int" },
                    { "name": "longField", "type": "long" },
                    { "name": "floatField", "type": "float" },
                    { "name": "doubleField", "type": "double" },
                    { "name": "arrayStringField",
                      "type": {
                        "type": "array",
                        "items": "string"
                      }
                    },
                    { "name": "mapField",
                      "type": {
                        "type": "map",
                        "values": "long"
                      }
                    }
                  ]
                }
                """;
        final Schema schema = AvroSchemaUtil.convertSchema(avroSchema);

        final GenericRecord record = new GenericData.Record(schema);

        record.put("booleanField", true);
        record.put("stringField", "text");
        record.put("intField", 100);
        record.put("longField", 1000000L);
        record.put("floatField", 0.12345F);
        record.put("doubleField", -0.123456789D);
        record.put("arrayStringField", List.of("a", "b", "c", "d", "e"));
        record.put("mapField", Map.of(
                "a",  1L,
                "b",  2L,
                "c", -1L));

        final byte[] serialized = AvroSchemaUtil.encode(record);
        final Object deserialized = AvroSchemaUtil.decode(schema, serialized);

        Assertions.assertTrue(deserialized instanceof GenericData.Record);
        final GenericRecord r = (GenericRecord) deserialized;

        Assertions.assertEquals(true, r.get("booleanField"));
        Assertions.assertEquals("text", r.get("stringField").toString());
        Assertions.assertEquals(100, r.get("intField"));
        Assertions.assertEquals(1000000L, r.get("longField"));
        Assertions.assertEquals(0.12345F, r.get("floatField"));
        Assertions.assertEquals(-0.123456789D, r.get("doubleField"));

        final Object arrayStringFieldValue = r.get("arrayStringField");
        Assertions.assertTrue(arrayStringFieldValue instanceof Collection<?>);
        final List<?> arrayStringFieldCollection = (List<?>) arrayStringFieldValue;
        for(int i=0; i<arrayStringFieldCollection.size(); i++) {
            switch (i) {
                case 0 -> Assertions.assertEquals("a", arrayStringFieldCollection.get(i).toString());
                case 1 -> Assertions.assertEquals("b", arrayStringFieldCollection.get(i).toString());
                case 2 -> Assertions.assertEquals("c", arrayStringFieldCollection.get(i).toString());
                case 3 -> Assertions.assertEquals("d", arrayStringFieldCollection.get(i).toString());
                case 4 -> Assertions.assertEquals("e", arrayStringFieldCollection.get(i).toString());
            }
        }

        final Object mapFieldValue = r.get("mapField");
        Assertions.assertTrue(mapFieldValue instanceof Map<?,?>);
        final Map<Utf8,Long> mapFieldValueMap = (Map<Utf8,Long>) mapFieldValue;
        for(final Map.Entry<Utf8, Long> entry : mapFieldValueMap.entrySet()) {
            switch (entry.getKey().toString()) {
                case "a" -> Assertions.assertEquals(1L, entry.getValue().longValue());
                case "b" -> Assertions.assertEquals(2L, entry.getValue().longValue());
                case "c" -> Assertions.assertEquals(-1L, entry.getValue().longValue());
            }
        }
    }

    private static final String TYPED_SCHEMA_JSON = """
            {
              "name": "typed",
              "type": "record",
              "namespace": "com.mercari.solution.test",
              "fields": [
                { "name": "booleanField", "type": "boolean" },
                { "name": "stringField", "type": "string" },
                { "name": "numericStringField", "type": "string" },
                { "name": "bytesField", "type": "bytes" },
                { "name": "intField", "type": "int" },
                { "name": "longField", "type": "long" },
                { "name": "floatField", "type": "float" },
                { "name": "doubleField", "type": "double" },
                { "name": "enumField", "type": { "type": "enum", "name": "color", "symbols": ["a", "b", "c"] } },
                { "name": "dateField", "type": { "type": "int", "logicalType": "date" } },
                { "name": "timeMillisField", "type": { "type": "int", "logicalType": "time-millis" } },
                { "name": "timeMicrosField", "type": { "type": "long", "logicalType": "time-micros" } },
                { "name": "timestampMillisField", "type": { "type": "long", "logicalType": "timestamp-millis" } },
                { "name": "timestampMicrosField", "type": { "type": "long", "logicalType": "timestamp-micros" } },
                { "name": "decimalField", "type": { "type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9 } },
                { "name": "nullableStringField", "type": ["null", "string"] },
                { "name": "nullableArrayField", "type": ["null", { "type": "array", "items": "float" }] },
                { "name": "intArrayField", "type": { "type": "array", "items": "int" } },
                { "name": "longArrayField", "type": { "type": "array", "items": "long" } },
                { "name": "floatArrayField", "type": { "type": "array", "items": "float" } },
                { "name": "doubleArrayField", "type": { "type": "array", "items": "double" } },
                { "name": "booleanArrayField", "type": { "type": "array", "items": "boolean" } },
                { "name": "stringArrayField", "type": { "type": "array", "items": "string" } },
                { "name": "timestampMillisArrayField", "type": { "type": "array", "items": { "type": "long", "logicalType": "timestamp-millis" } } },
                { "name": "timestampMicrosArrayField", "type": { "type": "array", "items": { "type": "long", "logicalType": "timestamp-micros" } } },
                { "name": "recordField", "type": { "type": "record", "name": "child", "fields": [
                  { "name": "childString", "type": "string" },
                  { "name": "childInt", "type": "int" } ] } }
              ]
            }
            """;

    private static final LocalDate TEST_DATE = LocalDate.of(2021, 3, 2);
    private static final LocalTime TEST_TIME = LocalTime.of(15, 24, 1);
    private static final long TEST_TIMESTAMP_MILLIS = java.time.Instant.parse("2021-03-02T01:02:03Z").toEpochMilli();
    private static final long TEST_TIMESTAMP_MICROS = TEST_TIMESTAMP_MILLIS * 1000L;
    private static final byte[] TEST_BYTES = "Hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final long TEST_DECIMAL_UNSCALED = 1234500000000L; // 1234.5 with scale 9

    private static GenericRecord createTypedRecord() {
        final Schema schema = AvroSchemaUtil.convertSchema(TYPED_SCHEMA_JSON);
        final GenericRecord record = new GenericData.Record(schema);
        record.put("booleanField", true);
        record.put("stringField", "stringValue");
        record.put("numericStringField", "123");
        record.put("bytesField", ByteBuffer.wrap(Arrays.copyOf(TEST_BYTES, TEST_BYTES.length)));
        record.put("intField", 42);
        record.put("longField", 1234567890123L);
        record.put("floatField", 1.5F);
        record.put("doubleField", -2.25D);
        record.put("enumField", new GenericData.EnumSymbol(schema.getField("enumField").schema(), "b"));
        record.put("dateField", (int) TEST_DATE.toEpochDay());
        record.put("timeMillisField", TEST_TIME.toSecondOfDay() * 1000);
        record.put("timeMicrosField", TEST_TIME.toNanoOfDay() / 1000L);
        record.put("timestampMillisField", TEST_TIMESTAMP_MILLIS);
        record.put("timestampMicrosField", TEST_TIMESTAMP_MICROS);
        record.put("decimalField", ByteBuffer.wrap(BigInteger.valueOf(TEST_DECIMAL_UNSCALED).toByteArray()));
        record.put("intArrayField", Arrays.asList(1, 2, 3));
        record.put("longArrayField", Arrays.asList(1L, 2L));
        record.put("floatArrayField", Arrays.asList(1.5F, 2.5F));
        record.put("doubleArrayField", Arrays.asList(1.5D, 2.5D));
        record.put("booleanArrayField", Arrays.asList(true, false));
        record.put("stringArrayField", Arrays.asList("1.5", "2.5"));
        record.put("timestampMillisArrayField", Arrays.asList(TEST_TIMESTAMP_MILLIS));
        record.put("timestampMicrosArrayField", Arrays.asList(TEST_TIMESTAMP_MICROS));
        final Schema childSchema = schema.getField("recordField").schema();
        final GenericRecord child = new GenericData.Record(childSchema);
        child.put("childString", "c");
        child.put("childInt", 7);
        record.put("recordField", child);
        return record;
    }

    @Test
    public void testConvertSchemaFromTableSchema() {
        final TableSchema tableSchema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("stringField").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("intField").setType("INT64").setMode("NULLABLE"),
                new TableFieldSchema().setName("floatField").setType("FLOAT64"),
                new TableFieldSchema().setName("boolField").setType("BOOLEAN").setMode("REQUIRED"),
                new TableFieldSchema().setName("dateField").setType("DATE").setMode("REQUIRED"),
                new TableFieldSchema().setName("timeField").setType("TIME").setMode("REQUIRED"),
                new TableFieldSchema().setName("timestampField").setType("TIMESTAMP").setMode("REQUIRED"),
                new TableFieldSchema().setName("numericField").setType("NUMERIC").setMode("REQUIRED"),
                new TableFieldSchema().setName("arrayField").setType("STRING").setMode("REPEATED"),
                new TableFieldSchema().setName("recordField").setType("RECORD").setMode("REQUIRED")
                        .setFields(Arrays.asList(
                                new TableFieldSchema().setName("childField").setType("STRING").setMode("REQUIRED")))));

        final Schema schema = AvroSchemaUtil.convertSchema(tableSchema);
        Assertions.assertEquals("root", schema.getName());
        Assertions.assertEquals(10, schema.getFields().size());
        Assertions.assertEquals(Schema.Type.STRING, schema.getField("stringField").schema().getType());
        Assertions.assertTrue(AvroSchemaUtil.isNullable(schema.getField("intField").schema()));
        Assertions.assertEquals(Schema.Type.LONG, AvroSchemaUtil.unnestUnion(schema.getField("intField").schema()).getType());
        Assertions.assertTrue(AvroSchemaUtil.isNullable(schema.getField("floatField").schema()));
        Assertions.assertEquals(Schema.Type.DOUBLE, AvroSchemaUtil.unnestUnion(schema.getField("floatField").schema()).getType());
        Assertions.assertEquals(Schema.Type.BOOLEAN, schema.getField("boolField").schema().getType());
        Assertions.assertEquals(LogicalTypes.date(), schema.getField("dateField").schema().getLogicalType());
        Assertions.assertEquals(LogicalTypes.timeMicros(), schema.getField("timeField").schema().getLogicalType());
        Assertions.assertEquals(LogicalTypes.timestampMicros(), schema.getField("timestampField").schema().getLogicalType());
        Assertions.assertTrue(AvroSchemaUtil.isLogicalTypeDecimal(schema.getField("numericField").schema()));
        Assertions.assertEquals(Schema.Type.ARRAY, schema.getField("arrayField").schema().getType());
        Assertions.assertEquals(Schema.Type.STRING, schema.getField("arrayField").schema().getElementType().getType());
        Assertions.assertEquals(Schema.Type.RECORD, schema.getField("recordField").schema().getType());
        Assertions.assertNotNull(schema.getField("recordField").schema().getField("childField"));

        final Schema filtered = AvroSchemaUtil.convertSchema(tableSchema, Arrays.asList("stringField", "intField"), "filtered");
        Assertions.assertEquals("filtered", filtered.getName());
        Assertions.assertEquals(2, filtered.getFields().size());
    }

    @Test
    public void testConvertSchemaFromSObjectDescribeFields() {
        final JsonArray describeFields = new JsonArray();
        describeFields.add(createDescribeField("Id", "id", null));
        describeFields.add(createDescribeField("Name", "string", false));
        describeFields.add(createDescribeField("Amount", "currency", true));
        describeFields.add(createDescribeField("CreatedDate", "datetime", false));
        describeFields.add(createDescribeField("BirthDate", "date", true));
        describeFields.add(createDescribeField("Count", "int", false));
        describeFields.add(createDescribeField("Tags", "multipicklist", true));

        final Schema schema = AvroSchemaUtil.convertSchema(describeFields, null);
        Assertions.assertEquals(7, schema.getFields().size());
        Assertions.assertEquals(Schema.Type.STRING, schema.getField("Id").schema().getType());
        Assertions.assertEquals(Schema.Type.STRING, schema.getField("Name").schema().getType());
        Assertions.assertTrue(AvroSchemaUtil.isNullable(schema.getField("Amount").schema()));
        Assertions.assertEquals(Schema.Type.DOUBLE, AvroSchemaUtil.unnestUnion(schema.getField("Amount").schema()).getType());
        Assertions.assertEquals(LogicalTypes.timestampMicros(), schema.getField("CreatedDate").schema().getLogicalType());
        Assertions.assertTrue(AvroSchemaUtil.isNullable(schema.getField("BirthDate").schema()));
        Assertions.assertEquals(LogicalTypes.date(), AvroSchemaUtil.unnestUnion(schema.getField("BirthDate").schema()).getLogicalType());
        Assertions.assertEquals(Schema.Type.LONG, schema.getField("Count").schema().getType());
        Assertions.assertTrue(AvroSchemaUtil.isNullable(schema.getField("Tags").schema()));
        Assertions.assertEquals(Schema.Type.ARRAY, AvroSchemaUtil.unnestUnion(schema.getField("Tags").schema()).getType());

        final Schema filtered = AvroSchemaUtil.convertSchema(describeFields, Arrays.asList("Id", "Name"));
        Assertions.assertEquals(2, filtered.getFields().size());
    }

    @Test
    public void testToBuilderRename() {
        final GenericRecord record = createTypedRecord();
        final Schema schema = record.getSchema();

        final Schema rebuilt = AvroSchemaUtil.toBuilder(schema).endRecord();
        Assertions.assertEquals(schema.getFields().size(), rebuilt.getFields().size());

        final Schema subset = AvroSchemaUtil.toBuilder(schema, Arrays.asList("stringField", "intField")).endRecord();
        Assertions.assertEquals(2, subset.getFields().size());

        // copy typed record including record and array fields
        final GenericRecord copied = AvroSchemaUtil.toBuilder(schema, record).build();
        Assertions.assertEquals("stringValue", copied.get("stringField").toString());
        Assertions.assertEquals(Arrays.asList(1, 2, 3), copied.get("intArrayField"));
        Assertions.assertEquals(7, ((GenericRecord) copied.get("recordField")).get("childInt"));
        Assertions.assertNull(copied.get("nullableStringField"));

        // rename fields
        final Schema srcSchema = AvroSchemaUtil.convertSchema("""
                {"name":"src","type":"record","fields":[
                  {"name":"a","type":"string"},
                  {"name":"b","type":"int"}]}
                """);
        final GenericRecord src = new GenericData.Record(srcSchema);
        src.put("a", "renamedValue");
        src.put("b", 1);
        final Schema dstSchema = AvroSchemaUtil.convertSchema("""
                {"name":"dst","type":"record","fields":[
                  {"name":"a2","type":"string"},
                  {"name":"b","type":"int"},
                  {"name":"c","type":["null","string"]}]}
                """);
        final GenericRecord dst = AvroSchemaUtil.toBuilder(dstSchema, src, Map.of("a", "a2")).build();
        Assertions.assertEquals("renamedValue", dst.get("a2").toString());
        Assertions.assertEquals(1, dst.get("b"));
        Assertions.assertNull(dst.get("c"));
    }

    @Test
    public void testGetValueTypes() {
        final GenericRecord record = createTypedRecord();
        Assertions.assertEquals(true, AvroSchemaUtil.getValue(record, "booleanField"));
        Assertions.assertEquals("stringValue", AvroSchemaUtil.getValue(record, "stringField"));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) AvroSchemaUtil.getValue(record, "bytesField"));
        Assertions.assertEquals(42, AvroSchemaUtil.getValue(record, "intField"));
        Assertions.assertEquals(1234567890123L, AvroSchemaUtil.getValue(record, "longField"));
        Assertions.assertEquals(1.5F, AvroSchemaUtil.getValue(record, "floatField"));
        Assertions.assertEquals(-2.25D, AvroSchemaUtil.getValue(record, "doubleField"));
        Assertions.assertEquals("b", AvroSchemaUtil.getValue(record, "enumField"));
        Assertions.assertEquals(TEST_DATE, AvroSchemaUtil.getValue(record, "dateField"));
        Assertions.assertEquals(TEST_TIME, AvroSchemaUtil.getValue(record, "timeMillisField"));
        Assertions.assertEquals(TEST_TIME, AvroSchemaUtil.getValue(record, "timeMicrosField"));
        Assertions.assertEquals(Instant.ofEpochMilli(TEST_TIMESTAMP_MILLIS), AvroSchemaUtil.getValue(record, "timestampMillisField"));
        Assertions.assertEquals(Instant.ofEpochMilli(TEST_TIMESTAMP_MILLIS), AvroSchemaUtil.getValue(record, "timestampMicrosField"));
        Assertions.assertEquals(Arrays.asList(1, 2, 3), AvroSchemaUtil.getValue(record, "intArrayField"));
        Assertions.assertEquals(List.of(Instant.ofEpochMilli(TEST_TIMESTAMP_MILLIS)), AvroSchemaUtil.getValue(record, "timestampMicrosArrayField"));
        Assertions.assertNull(AvroSchemaUtil.getValue(record, "nullableStringField"));
        Assertions.assertNull(AvroSchemaUtil.getValue(record, "notExistsField"));
        Assertions.assertNull(AvroSchemaUtil.getValue(null, "stringField"));
        final GenericRecord child = (GenericRecord) AvroSchemaUtil.getValue(record, "recordField");
        Assertions.assertEquals("c", child.get("childString").toString());
    }

    @Test
    public void testGetTimestamp() {
        final GenericRecord record = createTypedRecord();
        final Instant expected = Instant.ofEpochMilli(TEST_TIMESTAMP_MILLIS);
        Assertions.assertEquals(expected, AvroSchemaUtil.getTimestamp(record, "timestampMillisField"));
        Assertions.assertEquals(expected, AvroSchemaUtil.getTimestamp(record, "timestampMicrosField"));
        Assertions.assertEquals(Instant.parse("2021-03-02T00:00:00Z"), AvroSchemaUtil.getTimestamp(record, "dateField"));

        final Instant defaultTimestamp = Instant.parse("2000-01-01T00:00:00Z");
        Assertions.assertEquals(defaultTimestamp, AvroSchemaUtil.getTimestamp(record, "stringField", defaultTimestamp));
        Assertions.assertEquals(defaultTimestamp, AvroSchemaUtil.getTimestamp(record, "longField", defaultTimestamp));
        Assertions.assertEquals(defaultTimestamp, AvroSchemaUtil.getTimestamp(record, "nullableStringField", defaultTimestamp));
        Assertions.assertEquals(defaultTimestamp, AvroSchemaUtil.getTimestamp(record, "notExistsField", defaultTimestamp));
        Assertions.assertEquals(Instant.ofEpochSecond(0L), AvroSchemaUtil.getTimestamp(record, "notExistsField"));

        final Schema tsSchema = AvroSchemaUtil.convertSchema("""
                {"name":"ts","type":"record","fields":[{"name":"stringTimestampField","type":"string"}]}
                """);
        final GenericRecord tsRecord = new GenericData.Record(tsSchema);
        tsRecord.put("stringTimestampField", "2021-03-02T01:02:03Z");
        Assertions.assertEquals(expected, AvroSchemaUtil.getTimestamp(tsRecord, "stringTimestampField"));
    }

    @Test
    public void testGetAsString() {
        final GenericRecord record = createTypedRecord();
        Assertions.assertEquals("true", AvroSchemaUtil.getAsString(record, "booleanField"));
        Assertions.assertEquals("stringValue", AvroSchemaUtil.getAsString(record, "stringField"));
        Assertions.assertEquals("SGVsbG8=", AvroSchemaUtil.getAsString(record, "bytesField"));
        Assertions.assertEquals("42", AvroSchemaUtil.getAsString(record, "intField"));
        Assertions.assertEquals("1234567890123", AvroSchemaUtil.getAsString(record, "longField"));
        Assertions.assertEquals("1.5", AvroSchemaUtil.getAsString(record, "floatField"));
        Assertions.assertEquals("-2.25", AvroSchemaUtil.getAsString(record, "doubleField"));
        Assertions.assertEquals("b", AvroSchemaUtil.getAsString(record, "enumField"));
        Assertions.assertEquals("2021-03-02", AvroSchemaUtil.getAsString(record, "dateField"));
        Assertions.assertEquals("15:24:01", AvroSchemaUtil.getAsString(record, "timeMillisField"));
        Assertions.assertEquals("15:24:01", AvroSchemaUtil.getAsString(record, "timeMicrosField"));
        Assertions.assertEquals("2021-03-02T01:02:03Z", AvroSchemaUtil.getAsString(record, "timestampMillisField"));
        Assertions.assertEquals("2021-03-02T01:02:03Z", AvroSchemaUtil.getAsString(record, "timestampMicrosField"));
        Assertions.assertNull(AvroSchemaUtil.getAsString(record, "nullableStringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsString(record, "notExistsField"));
        Assertions.assertEquals("stringValue", AvroSchemaUtil.getAsString((Object) record, "stringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsString((Object) null, "stringField"));
    }

    @Test
    public void testGetAsLong() {
        final GenericRecord record = createTypedRecord();
        Assertions.assertEquals(1L, AvroSchemaUtil.getAsLong(record, "booleanField"));
        Assertions.assertEquals(1L, AvroSchemaUtil.getAsLong(record, "floatField"));
        Assertions.assertEquals(-2L, AvroSchemaUtil.getAsLong(record, "doubleField"));
        Assertions.assertEquals(123L, AvroSchemaUtil.getAsLong(record, "numericStringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsLong(record, "stringField"));
        Assertions.assertEquals(42L, AvroSchemaUtil.getAsLong(record, "intField"));
        Assertions.assertEquals(1234567890123L, AvroSchemaUtil.getAsLong(record, "longField"));
        Assertions.assertEquals(1234L, AvroSchemaUtil.getAsLong(record, "decimalField"));
        Assertions.assertNull(AvroSchemaUtil.getAsLong(record, "nullableStringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsLong(record, "notExistsField"));
    }

    @Test
    public void testGetAsBigDecimal() {
        final GenericRecord record = createTypedRecord();
        Assertions.assertEquals(0, new BigDecimal("1234.5").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "decimalField")));
        Assertions.assertEquals(0, BigDecimal.ONE.compareTo(AvroSchemaUtil.getAsBigDecimal(record, "booleanField")));
        Assertions.assertEquals(0, new BigDecimal("1.5").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "floatField")));
        Assertions.assertEquals(0, new BigDecimal("-2.25").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "doubleField")));
        Assertions.assertEquals(0, new BigDecimal("123").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "numericStringField")));
        Assertions.assertEquals(0, new BigDecimal("42").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "intField")));
        Assertions.assertEquals(0, new BigDecimal("1234567890123").compareTo(AvroSchemaUtil.getAsBigDecimal(record, "longField")));
        Assertions.assertNull(AvroSchemaUtil.getAsBigDecimal(record, "stringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsBigDecimal(record, "nullableStringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsBigDecimal(record, "notExistsField"));

        final byte[] decimalBytes = BigInteger.valueOf(TEST_DECIMAL_UNSCALED).toByteArray();
        Assertions.assertEquals(0, new BigDecimal("1234.5").compareTo(
                AvroSchemaUtil.getAsBigDecimal(AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE, decimalBytes)));
        Assertions.assertEquals(0, new BigDecimal("1234.5").compareTo(
                AvroSchemaUtil.getAsBigDecimal(AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE, ByteBuffer.wrap(decimalBytes))));
        Assertions.assertNull(AvroSchemaUtil.getAsBigDecimal(Schema.create(Schema.Type.BYTES), decimalBytes));
        Assertions.assertNull(AvroSchemaUtil.getAsBigDecimal(AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE, (byte[]) null));
    }

    @Test
    public void testGetAsStandard() {
        final GenericRecord record = createTypedRecord();
        Assertions.assertEquals(true, AvroSchemaUtil.getAsStandard(record, "booleanField"));
        Assertions.assertEquals("stringValue", AvroSchemaUtil.getAsStandard(record, "stringField"));
        Assertions.assertEquals(42, AvroSchemaUtil.getAsStandard(record, "intField"));
        Assertions.assertEquals(1234567890123L, AvroSchemaUtil.getAsStandard(record, "longField"));
        Assertions.assertEquals("b", AvroSchemaUtil.getAsStandard(record, "enumField"));
        Assertions.assertEquals(TEST_DATE, AvroSchemaUtil.getAsStandard(record, "dateField"));
        Assertions.assertEquals(TEST_TIME, AvroSchemaUtil.getAsStandard(record, "timeMillisField"));
        Assertions.assertEquals(TEST_TIME, AvroSchemaUtil.getAsStandard(record, "timeMicrosField"));
        Assertions.assertEquals(java.time.Instant.ofEpochMilli(TEST_TIMESTAMP_MILLIS), AvroSchemaUtil.getAsStandard(record, "timestampMillisField"));
        Assertions.assertEquals(java.time.Instant.ofEpochMilli(TEST_TIMESTAMP_MILLIS), AvroSchemaUtil.getAsStandard(record, "timestampMicrosField"));
        Assertions.assertEquals(0, new BigDecimal("1234.5").compareTo((BigDecimal) AvroSchemaUtil.getAsStandard(record, "decimalField")));
        Assertions.assertEquals(Arrays.asList(1, 2, 3), AvroSchemaUtil.getAsStandard(record, "intArrayField"));
        Assertions.assertNull(AvroSchemaUtil.getAsStandard(record, "nullableStringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsStandard(record, "notExistsField"));
        Assertions.assertNull(AvroSchemaUtil.getAsStandard((GenericRecord) null, "stringField"));

        final Map<String, Object> childMap = (Map<String, Object>) AvroSchemaUtil.getAsStandard(record, "recordField");
        Assertions.assertEquals("c", childMap.get("childString"));
        Assertions.assertEquals(7, childMap.get("childInt"));

        // direct fieldSchema and value conversions
        Assertions.assertEquals(true, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.BOOLEAN), (Object) "true"));
        Assertions.assertEquals(true, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.BOOLEAN), 1));
        Assertions.assertEquals("utf8", AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.STRING), new Utf8("utf8")));
        Assertions.assertEquals(123, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.INT), (Object) "123"));
        Assertions.assertEquals(1, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.INT), true));
        Assertions.assertEquals(12L, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.LONG), (Object) "12"));
        Assertions.assertEquals(1.5F, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.FLOAT), (Object) "1.5"));
        Assertions.assertEquals(2.5D, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.DOUBLE), (Object) "2.5"));
        Assertions.assertEquals(1D, AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.DOUBLE), true));
        Assertions.assertEquals(TEST_DATE, AvroSchemaUtil.getAsStandard(AvroSchemaUtil.REQUIRED_LOGICAL_DATE_TYPE, (Object) "2021-03-02"));
        Assertions.assertEquals(TEST_TIME, AvroSchemaUtil.getAsStandard(AvroSchemaUtil.REQUIRED_LOGICAL_TIME_MILLI_TYPE, TEST_TIME.toSecondOfDay() * 1000));
        Assertions.assertEquals(java.time.Instant.ofEpochMilli(TEST_TIMESTAMP_MILLIS),
                AvroSchemaUtil.getAsStandard(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MILLI_TYPE, TEST_TIMESTAMP_MILLIS));
        Assertions.assertNull(AvroSchemaUtil.getAsStandard(Schema.create(Schema.Type.STRING), (Object) null));
    }

    @Test
    public void testGetAsPrimitive() {
        final GenericRecord record = createTypedRecord();

        // by record and fieldName
        Assertions.assertEquals("stringValue", AvroSchemaUtil.getAsPrimitive(record, "stringField"));
        Assertions.assertEquals(1, AvroSchemaUtil.getAsPrimitive(record, "enumField"));
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(), AvroSchemaUtil.getAsPrimitive(record, "dateField"));
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, AvroSchemaUtil.getAsPrimitive(record, "timeMillisField"));
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, AvroSchemaUtil.getAsPrimitive(record, "timeMicrosField"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, AvroSchemaUtil.getAsPrimitive(record, "timestampMillisField"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, AvroSchemaUtil.getAsPrimitive(record, "timestampMicrosField"));
        Assertions.assertEquals(7, AvroSchemaUtil.getAsPrimitive(record, "recordField.childInt"));
        Assertions.assertEquals(List.of("1.5", "2.5"), AvroSchemaUtil.getAsPrimitive(record, "stringArrayField"));
        Assertions.assertNull(AvroSchemaUtil.getAsPrimitive(record, "nullableStringField"));
        Assertions.assertNull(AvroSchemaUtil.getAsPrimitive((GenericRecord) null, "stringField"));
        final Map<String, Object> childMap = (Map<String, Object>) AvroSchemaUtil.getAsPrimitive(record, "recordField");
        Assertions.assertEquals("c", childMap.get("childString"));
        Assertions.assertEquals(7, childMap.get("childInt"));

        // by fieldSchema and value
        final Schema enumSchema = record.getSchema().getField("enumField").schema();
        Assertions.assertEquals(2, AvroSchemaUtil.getAsPrimitive(enumSchema, "c"));

        // by record, beam fieldType and fieldName
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS,
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME, "timestampMillisField"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS,
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME, "timestampMicrosField"));
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(),
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()), "dateField"));
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L,
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()), "timeMicrosField"));
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L,
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()), "timeMillisField"));
        Assertions.assertEquals("stringValue",
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.STRING, "stringField"));
        Assertions.assertEquals(1234567890123L,
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.INT64, "longField"));
        Assertions.assertEquals(7,
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.INT32, "recordField.childInt"));
        Assertions.assertEquals(List.of("1.5", "2.5"),
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.STRING), "stringArrayField"));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP_MICROS),
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME), "timestampMillisArrayField"));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP_MICROS),
                AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME), "timestampMicrosArrayField"));
        Assertions.assertNull(AvroSchemaUtil.getAsPrimitive(record, org.apache.beam.sdk.schemas.Schema.FieldType.STRING, "notExistsField"));

        // by beam fieldType and value
        Assertions.assertEquals("x", AvroSchemaUtil.getAsPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.STRING, new Utf8("x")));
        Assertions.assertEquals(5, AvroSchemaUtil.getAsPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.INT32, 5));
        Assertions.assertEquals(List.of("a", "b"),
                AvroSchemaUtil.getAsPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.STRING), Arrays.asList(new Utf8("a"), new Utf8("b"))));
        Assertions.assertNull(AvroSchemaUtil.getAsPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.STRING, (Object) null));
    }

    @Test
    public void testGetAsFloatList() {
        final GenericRecord record = createTypedRecord();
        Assertions.assertEquals(List.of(1.0F, 0.0F), AvroSchemaUtil.getAsFloatList(record, "booleanArrayField"));
        Assertions.assertEquals(List.of(1.5F, 2.5F), AvroSchemaUtil.getAsFloatList(record, "floatArrayField"));
        Assertions.assertEquals(List.of(1.5F, 2.5F), AvroSchemaUtil.getAsFloatList(record, "doubleArrayField"));
        Assertions.assertEquals(List.of(1.5F, 2.5F), AvroSchemaUtil.getAsFloatList(record, "stringArrayField"));
        Assertions.assertEquals(List.of(1.0F, 2.0F, 3.0F), AvroSchemaUtil.getAsFloatList(record, "intArrayField"));
        Assertions.assertEquals(List.of(1.0F, 2.0F), AvroSchemaUtil.getAsFloatList(record, "longArrayField"));
        Assertions.assertTrue(AvroSchemaUtil.getAsFloatList(record, "nullableArrayField").isEmpty());
        Assertions.assertTrue(AvroSchemaUtil.getAsFloatList(record, "notExistsField").isEmpty());
        Assertions.assertThrows(IllegalStateException.class, () -> AvroSchemaUtil.getAsFloatList(record, "stringField"));
    }

    @Test
    public void testConvertPrimitive() {
        final org.apache.beam.sdk.schemas.Schema.FieldType enumType =
                org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));

        Assertions.assertNull(AvroSchemaUtil.convertPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.INT64, null));
        Assertions.assertEquals(42, AvroSchemaUtil.convertPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.INT32, 42));
        Assertions.assertEquals("a", AvroSchemaUtil.convertPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.STRING, "a"));
        Assertions.assertEquals(TEST_TIMESTAMP_MICROS, AvroSchemaUtil.convertPrimitive(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME, TEST_TIMESTAMP_MICROS));
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(), AvroSchemaUtil.convertPrimitive(
                org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()), (int) TEST_DATE.toEpochDay()));
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, AvroSchemaUtil.convertPrimitive(
                org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()), TEST_TIME.toNanoOfDay() / 1000L));

        final EnumerationType.Value enumValue = (EnumerationType.Value) AvroSchemaUtil.convertPrimitive(enumType, 2);
        Assertions.assertEquals(2, enumValue.getValue());

        final List<EnumerationType.Value> enumValues = (List<EnumerationType.Value>) AvroSchemaUtil.convertPrimitive(
                org.apache.beam.sdk.schemas.Schema.FieldType.array(enumType), Arrays.asList(1, 2));
        Assertions.assertEquals(2, enumValues.size());
        Assertions.assertEquals(1, enumValues.get(0).getValue());
        Assertions.assertEquals(2, enumValues.get(1).getValue());

        Assertions.assertEquals(List.of(1L, 2L), AvroSchemaUtil.convertPrimitive(
                org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME), Arrays.asList(1L, 2L)));
        Assertions.assertEquals(List.of("a", "b"), AvroSchemaUtil.convertPrimitive(
                org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.STRING), Arrays.asList("a", "b")));
    }

    private static JsonObject createDescribeField(final String name, final String type, final Boolean nillable) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", name);
        jsonObject.addProperty("type", type);
        if(nillable != null) {
            jsonObject.addProperty("nillable", nillable);
        }
        return jsonObject;
    }

}
