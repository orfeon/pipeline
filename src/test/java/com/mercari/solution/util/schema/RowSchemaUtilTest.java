package com.mercari.solution.util.schema;

import com.google.cloud.ByteArray;
import com.mercari.solution.TestDatum;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class RowSchemaUtilTest {

    @Test
    public void testToBuilder() {

        final Schema schema = Schema.builder()
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("intField", Schema.FieldType.INT32.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("floatField", Schema.FieldType.FLOAT.withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("datetimeField", Schema.FieldType.DATETIME.withNullable(true))
                .addField("enumField", Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c")).withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("timeField", Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()).withNullable(true))
                .addField(Schema.Field.of("booleanArrayField", Schema.FieldType.array(Schema.FieldType.BOOLEAN).withNullable(true)))
                .addField(Schema.Field.of("stringArrayField", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true)))
                .addField(Schema.Field.of("bytesArrayField", Schema.FieldType.array(Schema.FieldType.BYTES).withNullable(true)))
                .addField(Schema.Field.of("intArrayField", Schema.FieldType.array(Schema.FieldType.INT32).withNullable(true)))
                .addField(Schema.Field.of("longArrayField", Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true)))
                .addField(Schema.Field.of("floatArrayField", Schema.FieldType.array(Schema.FieldType.FLOAT).withNullable(true)))
                .addField(Schema.Field.of("doubleArrayField", Schema.FieldType.array(Schema.FieldType.DOUBLE).withNullable(true)))
                .addField(Schema.Field.of("datetimeArrayField", Schema.FieldType.array(Schema.FieldType.DATETIME).withNullable(true)))
                .addField(Schema.Field.of("enumArrayField", Schema.FieldType.array(Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"))).withNullable(true)))
                .addField(Schema.Field.of("dateArrayField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType())).withNullable(true))
                .addField(Schema.Field.of("timeArrayField", Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType())).withNullable(true))
                .build();

        final List<String> fieldNames = schema
                .getFields().stream()
                .map(Schema.Field::getName)
                .collect(Collectors.toList());
        final List<String> fieldNamesNew = RowSchemaUtil.toBuilder(schema).build()
                .getFields().stream()
                .map(Schema.Field::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(fieldNames, fieldNamesNew);

        final List<String> fieldNamesNewFilter = RowSchemaUtil.toBuilder(schema, Arrays.asList("doubleField", "timeArrayField")).build()
                .getFields().stream()
                .map(Schema.Field::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("doubleField", "timeArrayField"), fieldNamesNewFilter);


        final Row row = Row.withSchema(schema)
                .withFieldValue("booleanField", true)
                .withFieldValue("stringField", "stringValue")
                .withFieldValue("bytesField", ByteArray.copyFrom("Hello").toByteArray())
                .withFieldValue("intField", -123)
                .withFieldValue("longField", 123421L)
                .withFieldValue("floatField", 0.12f)
                .withFieldValue("doubleField", 1.55)
                .withFieldValue("datetimeField", Instant.parse("2021-03-02T00:00:00Z"))
                .withFieldValue("dateField", LocalDate.of(1990, 12, 31))
                .withFieldValue("timeField", LocalTime.of(15, 24, 1, 123456789))
                .withFieldValue("enumField", new EnumerationType.Value(0))
                .withFieldValue("booleanArrayField", Arrays.asList(true, false, true))
                .withFieldValue("stringArrayField", Arrays.asList("S", "D", "K"))
                .build();

        final Row.FieldValueBuilder builder = RowSchemaUtil.toBuilder(row);
        final Row newRow = builder.build();
        Assertions.assertEquals(row.getBoolean("booleanField"), newRow.getBoolean("booleanField"));
        Assertions.assertEquals(row.getString("stringField"), newRow.getString("stringField"));
        Assertions.assertEquals(row.getBytes("bytesField"), newRow.getBytes("bytesField"));
        Assertions.assertEquals(row.getInt32("intField"), newRow.getInt32("intField"));
        Assertions.assertEquals(row.getInt64("longField"), newRow.getInt64("longField"));
        Assertions.assertEquals(row.getFloat("floatField"), newRow.getFloat("floatField"));
        Assertions.assertEquals(row.getDouble("doubleField"), newRow.getDouble("doubleField"));
        Assertions.assertEquals(row.getDateTime("datetimeField"), newRow.getDateTime("datetimeField"));
        Assertions.assertEquals(
                row.getLogicalTypeValue("dateField", LocalDate.class),
                newRow.getLogicalTypeValue("dateField", LocalDate.class));
        Assertions.assertEquals(
                row.getLogicalTypeValue("timeField", LocalTime.class),
                newRow.getLogicalTypeValue("timeField", LocalTime.class));
        Assertions.assertEquals(
                row.getLogicalTypeValue("enumField", EnumerationType.Value.class),
                newRow.getLogicalTypeValue("enumField", EnumerationType.Value.class));

        Assertions.assertEquals(row.getArray("booleanArrayField"), newRow.getArray("booleanArrayField"));
        Assertions.assertEquals(row.getArray("stringArrayField"), newRow.getArray("stringArrayField"));
    }

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

        // schema test
        Assertions.assertEquals(5, schema.getFieldCount());
        Assertions.assertTrue(schema.hasField("stringField"));
        Assertions.assertTrue(schema.hasField("intField"));
        Assertions.assertTrue(schema.hasField("longField"));
        Assertions.assertTrue(schema.hasField("recordField"));
        Assertions.assertTrue(schema.hasField("recordArrayField"));

        final Schema schemaChild = schema.getField("recordField").getType().getRowSchema();
        Assertions.assertEquals(5, schemaChild.getFieldCount());
        Assertions.assertTrue(schemaChild.hasField("stringField"));
        Assertions.assertTrue(schemaChild.hasField("doubleField"));
        Assertions.assertTrue(schemaChild.hasField("booleanField"));
        Assertions.assertTrue(schemaChild.hasField("recordField"));
        Assertions.assertTrue(schemaChild.hasField("recordArrayField"));

        Assertions.assertEquals(Schema.TypeName.ARRAY, schemaChild.getField("recordArrayField").getType().getTypeName());
        final Schema schemaChildChildren = schemaChild.getField("recordArrayField").getType().getCollectionElementType().getRowSchema();
        Assertions.assertEquals(2, schemaChildChildren.getFieldCount());
        Assertions.assertTrue(schemaChildChildren.hasField("intField"));
        Assertions.assertTrue(schemaChildChildren.hasField("floatField"));

        final Schema schemaGrandchild = schemaChild.getField("recordField").getType().getRowSchema();
        Assertions.assertEquals(2, schemaGrandchild.getFieldCount());
        Assertions.assertTrue(schemaGrandchild.hasField("intField"));
        Assertions.assertTrue(schemaGrandchild.hasField("floatField"));

        Assertions.assertEquals(Schema.TypeName.ARRAY, schema.getField("recordArrayField").getType().getTypeName());
        final Schema schemaChildren = schema.getField("recordArrayField").getType().getCollectionElementType().getRowSchema();
        Assertions.assertEquals(4, schemaChildren.getFieldCount());
        Assertions.assertTrue(schemaChildren.hasField("stringField"));
        Assertions.assertTrue(schemaChildren.hasField("timestampField"));
        Assertions.assertTrue(schemaChildren.hasField("recordField"));
        Assertions.assertTrue(schemaChildren.hasField("recordArrayField"));

        final Schema schemaChildrenChild = schemaChildren.getField("recordField").getType().getRowSchema();
        Assertions.assertEquals(2, schemaChildrenChild.getFieldCount());
        Assertions.assertTrue(schemaChildrenChild.hasField("intField"));
        Assertions.assertTrue(schemaChildrenChild.hasField("floatField"));

        Assertions.assertEquals(Schema.TypeName.ARRAY, schemaChildren.getField("recordArrayField").getType().getTypeName());
        final Schema schemaChildrenChildren = schemaChildren.getField("recordArrayField").getType().getCollectionElementType().getRowSchema();
        Assertions.assertEquals(2, schemaChildrenChildren.getFieldCount());
        Assertions.assertTrue(schemaChildrenChildren.hasField("intField"));
        Assertions.assertTrue(schemaChildrenChildren.hasField("floatField"));

        // row test
        final Row selectedRow = RowSchemaUtil.toBuilder(schema, row).build();
        Assertions.assertEquals(5, selectedRow.getFieldCount());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedRow.getString("stringField"));
        Assertions.assertEquals(TestDatum.getIntFieldValue(), selectedRow.getInt32("intField"));
        Assertions.assertEquals(TestDatum.getLongFieldValue(), selectedRow.getInt64("longField"));

        final Row selectedRowChild = selectedRow.getRow("recordField");
        Assertions.assertEquals(5, selectedRowChild.getFieldCount());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), selectedRowChild.getString("stringField"));
        Assertions.assertEquals(TestDatum.getDoubleFieldValue(), selectedRowChild.getDouble("doubleField"));
        Assertions.assertEquals(TestDatum.getBooleanFieldValue(), selectedRowChild.getBoolean("booleanField"));

        final Row selectedRowGrandchild = selectedRowChild.getRow("recordField");
        Assertions.assertEquals(2, selectedRowGrandchild.getFieldCount());
        Assertions.assertEquals(TestDatum.getIntFieldValue(), selectedRowGrandchild.getInt32("intField"));
        Assertions.assertEquals(TestDatum.getFloatFieldValue(), selectedRowGrandchild.getFloat("floatField"));

        Assertions.assertEquals(2, selectedRow.getArray("recordArrayField").size());
        for(final Row child : selectedRow.<Row>getArray("recordArrayField")) {
            Assertions.assertEquals(4, child.getFieldCount());
            Assertions.assertEquals(TestDatum.getStringFieldValue(), child.getString("stringField"));
            Assertions.assertEquals(TestDatum.getTimestampFieldValue(), child.getDateTime("timestampField"));

            Assertions.assertEquals(2, child.getArray("recordArrayField").size());
            for(final Row grandchild : child.<Row>getArray("recordArrayField")) {
                Assertions.assertEquals(2, grandchild.getFieldCount());
                Assertions.assertEquals(TestDatum.getIntFieldValue(), grandchild.getInt32("intField"));
                Assertions.assertEquals(TestDatum.getFloatFieldValue(), grandchild.getFloat("floatField"));
            }

            final Row grandchild = child.getRow("recordField");
            Assertions.assertEquals(TestDatum.getIntFieldValue(), grandchild.getInt32("intField"));
            Assertions.assertEquals(TestDatum.getFloatFieldValue(), grandchild.getFloat("floatField"));
        }

        // null fields row test
        final Row rowNull = TestDatum.generateRowNull();
        final List<String> newFields = new ArrayList<>(fields);
        newFields.add("recordFieldNull");
        newFields.add("recordArrayFieldNull");
        final Schema schemaNull = RowSchemaUtil.selectFields(rowNull.getSchema(), newFields);

        final Row selectedRowNull = RowSchemaUtil.toBuilder(schemaNull, rowNull).build();
        Assertions.assertEquals(7, selectedRowNull.getFieldCount());
        Assertions.assertNull(selectedRowNull.getString("stringField"));
        Assertions.assertNull(selectedRowNull.getInt32("intField"));
        Assertions.assertNull(selectedRowNull.getInt64("longField"));
        Assertions.assertNull(selectedRowNull.getInt64("recordFieldNull"));
        Assertions.assertNull(selectedRowNull.getInt64("recordArrayFieldNull"));

        final Row selectedRowChildNull = selectedRowNull.getRow("recordField");
        Assertions.assertEquals(5, selectedRowChildNull.getFieldCount());
        Assertions.assertNull(selectedRowChildNull.getString("stringField"));
        Assertions.assertNull(selectedRowChildNull.getDouble("doubleField"));
        Assertions.assertNull(selectedRowChildNull.getBoolean("booleanField"));

        final Row selectedRowGrandchildNull = selectedRowChildNull.getRow("recordField");
        Assertions.assertEquals(2, selectedRowGrandchildNull.getFieldCount());
        Assertions.assertNull(selectedRowGrandchildNull.getInt32("intField"));
        Assertions.assertNull(selectedRowGrandchildNull.getFloat("floatField"));

        Assertions.assertEquals(2, selectedRowNull.getArray("recordArrayField").size());
        for(final Row child : selectedRowNull.<Row>getArray("recordArrayField")) {
            Assertions.assertEquals(4, child.getFieldCount());
            Assertions.assertNull(child.getString("stringField"));
            Assertions.assertNull(child.getDateTime("timestampField"));

            Assertions.assertEquals(2, child.getArray("recordArrayField").size());
            for(final Row grandchild : child.<Row>getArray("recordArrayField")) {
                Assertions.assertEquals(2, grandchild.getFieldCount());
                Assertions.assertNull(grandchild.getInt32("intField"));
                Assertions.assertNull(grandchild.getFloat("floatField"));
            }

            final Row grandchild = child.getRow("recordField");
            Assertions.assertNull(grandchild.getInt32("intField"));
            Assertions.assertNull(grandchild.getFloat("floatField"));
        }

    }

    @Test
    public void testFlattenFields() {
        final Schema grandchildSchema = Schema.builder()
                .addStringField("gcstringField")
                .build();
        final Schema childSchema = Schema.builder()
                .addStringField("cstringField")
                .addRowField("grandchild", grandchildSchema)
                .addArrayField("grandchildren", Schema.FieldType.row(grandchildSchema))
                .build();
        final Schema schema = Schema.builder()
                .addStringField("stringField")
                .addArrayField("children", Schema.FieldType.row(childSchema))
                .build();

        final Schema resultSchema1 = RowSchemaUtil.flatten(schema, "children", true);
        Set<String> fieldNames1 = resultSchema1.getFields().stream()
                .map(Schema.Field::getName)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildren"),
                fieldNames1);
        resultSchema1.getFields().forEach(f -> {
            if(f.getName().equals("stringField") || f.getName().equals("children_cstringField")) {
                Assertions.assertEquals(
                        Schema.TypeName.STRING,
                        f.getType().getTypeName());
            } else if(f.getName().equals("children_grandchild")) {
                Assertions.assertEquals(
                        Schema.TypeName.ROW,
                        f.getType().getTypeName());
            } else {
                Assertions.assertEquals(
                        Schema.TypeName.ARRAY,
                        f.getType().getTypeName());
                Assertions.assertEquals(
                        Schema.TypeName.ROW,
                        f.getType().getCollectionElementType().getTypeName());
            }
        });

        final Schema resultSchema2 = RowSchemaUtil.flatten(schema, "children.grandchildren", true);
        Set<String> fieldNames2 = resultSchema2.getFields().stream().map(Schema.Field::getName).collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("stringField", "children_cstringField", "children_grandchild", "children_grandchildren_gcstringField"),
                fieldNames2);
    }

    @Test
    public void testFlattenValues() {
        final Schema grandchildSchema = Schema.builder()
                .addStringField("gcstringField")
                .build();
        final Row grandchild = Row.withSchema(grandchildSchema)
                .withFieldValue("gcstringField", "gcstringValue")
                .build();
        final Schema childSchema = Schema.builder()
                .addStringField("cstringField")
                .addRowField("grandchild", grandchildSchema)
                .addArrayField("grandchildren", Schema.FieldType.row(grandchildSchema))
                .addField("grandchildrenNull", Schema.FieldType.array(Schema.FieldType.row(grandchildSchema)).withNullable(true))
                .build();
        final Row child = Row.withSchema(childSchema)
                .withFieldValue("cstringField", "cstringValue")
                .withFieldValue("grandchild", grandchild)
                .withFieldValue("grandchildren", Arrays.asList(grandchild, grandchild))
                .withFieldValue("grandchildrenNull", null)
                .build();
        final Schema schema = Schema.builder()
                .addStringField("stringField")
                .addField("children", Schema.FieldType.array(Schema.FieldType.row(childSchema)).withNullable(true))
                .addField("childrenNull", Schema.FieldType.array(Schema.FieldType.row(childSchema)).withNullable(true))
                .build();
        final Row row = Row.withSchema(schema)
                .withFieldValue("stringField", "stringValue")
                .withFieldValue("children", Arrays.asList(child, child))
                .withFieldValue("childrenNull", null)
                .build();

        // one path
        final Schema resultSchemaChildren1 = RowSchemaUtil.flatten(schema, "children", true);
        final List<Row> resultRowChildren1 = RowSchemaUtil.flatten(resultSchemaChildren1, row, "children", true);
        Assertions.assertEquals(2, resultRowChildren1.size());

        for(final Row childrenRow : resultRowChildren1) {
            Assertions.assertEquals("stringValue", childrenRow.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenRow.getString("children_cstringField"));

            final Row grandchildRow = childrenRow.getRow("children_grandchild");
            Assertions.assertEquals("gcstringValue", grandchildRow.getString("gcstringField"));

            final Collection<Row> grandchildrenRows = childrenRow.getArray("children_grandchildren");
            Assertions.assertEquals(2, grandchildrenRows.size());
            for(final Row grandchildrenRow : grandchildrenRows) {
                Assertions.assertEquals("gcstringValue", grandchildrenRow.getString("gcstringField"));
            }
        }

        // two path
        final Schema resultSchemaChildren2 = RowSchemaUtil.flatten(schema, "children.grandchildren", true);
        final List<Row> resultRowChildren2 = RowSchemaUtil.flatten(resultSchemaChildren2, row, "children.grandchildren", true);
        Assertions.assertEquals(4, resultRowChildren2.size());

        for(final Row childrenRow : resultRowChildren2) {
            Assertions.assertEquals("stringValue", childrenRow.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenRow.getString("children_cstringField"));
            Assertions.assertEquals("gcstringValue", childrenRow.getString("children_grandchildren_gcstringField"));

            final Row grandchildRow = childrenRow.getRow("children_grandchild");
            Assertions.assertEquals("gcstringValue", grandchildRow.getString("gcstringField"));
        }

        // one path without prefix
        final Schema resultSchemaChildren1WP = RowSchemaUtil.flatten(schema, "children", false);
        final List<Row> resultRowChildren1WP = RowSchemaUtil.flatten(resultSchemaChildren1WP, row, "children", false);
        Assertions.assertEquals(2, resultRowChildren1.size());

        for(final Row childrenRow : resultRowChildren1WP) {
            Assertions.assertEquals("stringValue", childrenRow.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenRow.getString("cstringField"));

            final Row grandchildRow = childrenRow.getRow("grandchild");
            Assertions.assertEquals("gcstringValue", grandchildRow.getString("gcstringField"));

            final Collection<Row> grandchildrenRows = childrenRow.getArray("grandchildren");
            Assertions.assertEquals(2, grandchildrenRows.size());
            for(final Row grandchildrenRow : grandchildrenRows) {
                Assertions.assertEquals("gcstringValue", grandchildrenRow.getString("gcstringField"));
            }
        }

        // two path without prefix
        final Schema resultSchemaChildren2WP = RowSchemaUtil.flatten(schema, "children.grandchildren", false);
        final List<Row> resultRowChildren2WP = RowSchemaUtil.flatten(resultSchemaChildren2WP, row, "children.grandchildren", false);
        Assertions.assertEquals(4, resultRowChildren2WP.size());

        for(final Row childrenRow : resultRowChildren2WP) {
            Assertions.assertEquals("stringValue", childrenRow.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenRow.getString("cstringField"));
            Assertions.assertEquals("gcstringValue", childrenRow.getString("gcstringField"));

            final Row grandchildRow = childrenRow.getRow("grandchild");
            Assertions.assertEquals("gcstringValue", grandchildRow.getString("gcstringField"));
        }


        // Null check
        // one path null
        final Schema resultSchemaChildren1Null = RowSchemaUtil.flatten(schema, "childrenNull", true);
        final List<Row> resultRowChildren1Null = RowSchemaUtil.flatten(resultSchemaChildren1Null, row, "childrenNull", true);
        Assertions.assertEquals(1, resultRowChildren1Null.size());

        for(final Row childrenRow : resultRowChildren1Null) {
            Assertions.assertEquals("stringValue", childrenRow.getString("stringField"));

            Assertions.assertNull(childrenRow.getString("childrenNull_cstringField"));
            Assertions.assertNull(childrenRow.getRow("childrenNull_grandchild"));
            Assertions.assertNull(childrenRow.getArray("childrenNull_grandchildren"));
        }

        // two path null
        final Schema resultSchemaChildren2Null = RowSchemaUtil.flatten(schema, "children.grandchildrenNull", true);
        final List<Row> resultRowChildren2Null = RowSchemaUtil.flatten(resultSchemaChildren2Null, row, "children.grandchildrenNull", true);
        Assertions.assertEquals(2, resultRowChildren2Null.size());

        for(final Row childrenRow : resultRowChildren2Null) {
            Assertions.assertEquals("stringValue", childrenRow.getString("stringField"));
            Assertions.assertEquals("cstringValue", childrenRow.getString("children_cstringField"));

            Assertions.assertNull(childrenRow.getString("children_grandchildrenNull_gcstringField"));
        }


    }

    private static final Schema CHILD_SCHEMA = Schema.builder()
            .addStringField("cs")
            .addInt32Field("ci")
            .build();

    private static final Schema TYPED_SCHEMA = Schema.builder()
            .addBooleanField("booleanField")
            .addStringField("stringField")
            .addStringField("numericStringField")
            .addStringField("base64Field")
            .addStringField("timestampStringField")
            .addByteField("byteField")
            .addInt16Field("int16Field")
            .addInt32Field("int32Field")
            .addInt64Field("int64Field")
            .addFloatField("floatField")
            .addDoubleField("doubleField")
            .addDecimalField("decimalField")
            .addField("bytesField", Schema.FieldType.BYTES)
            .addDateTimeField("datetimeField")
            .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()))
            .addField("timeField", Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()))
            .addField("enumField", Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c")))
            .addMapField("mapField", Schema.FieldType.STRING, Schema.FieldType.INT64)
            .addRowField("rowField", CHILD_SCHEMA)
            .addArrayField("intArrayField", Schema.FieldType.INT32)
            .addArrayField("int16ArrayField", Schema.FieldType.INT16)
            .addArrayField("int64ArrayField", Schema.FieldType.INT64)
            .addArrayField("floatArrayField", Schema.FieldType.FLOAT)
            .addArrayField("doubleArrayField", Schema.FieldType.DOUBLE)
            .addArrayField("stringArrayField", Schema.FieldType.STRING)
            .addArrayField("datetimeArrayField", Schema.FieldType.DATETIME)
            .addArrayField("dateArrayField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()))
            .addArrayField("timeArrayField", Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()))
            .addArrayField("enumArrayField", Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c")))
            .addArrayField("rowArrayField", Schema.FieldType.row(CHILD_SCHEMA))
            .addField("nullableStringField", Schema.FieldType.STRING.withNullable(true))
            .addField("nullableArrayField", Schema.FieldType.array(Schema.FieldType.FLOAT).withNullable(true))
            .build();

    private static final LocalDate TEST_DATE = LocalDate.of(2021, 3, 2);
    private static final LocalTime TEST_TIME = LocalTime.of(15, 24, 1);
    private static final Instant TEST_TIMESTAMP = Instant.parse("2021-03-02T01:02:03Z");
    private static final byte[] TEST_BYTES = "Hello".getBytes(StandardCharsets.UTF_8);

    private static Row createChildRow() {
        return Row.withSchema(CHILD_SCHEMA)
                .withFieldValue("cs", "c")
                .withFieldValue("ci", 7)
                .build();
    }

    private static Row createTypedRow() {
        return Row.withSchema(TYPED_SCHEMA)
                .withFieldValue("booleanField", true)
                .withFieldValue("stringField", "stringValue")
                .withFieldValue("numericStringField", "123")
                .withFieldValue("base64Field", "SGVsbG8=")
                .withFieldValue("timestampStringField", "2021-03-02T01:02:03Z")
                .withFieldValue("byteField", (byte) 3)
                .withFieldValue("int16Field", (short) 12)
                .withFieldValue("int32Field", 42)
                .withFieldValue("int64Field", 1234567890123L)
                .withFieldValue("floatField", 1.5F)
                .withFieldValue("doubleField", -2.25D)
                .withFieldValue("decimalField", new BigDecimal("1234.5"))
                .withFieldValue("bytesField", Arrays.copyOf(TEST_BYTES, TEST_BYTES.length))
                .withFieldValue("datetimeField", TEST_TIMESTAMP)
                .withFieldValue("dateField", TEST_DATE)
                .withFieldValue("timeField", TEST_TIME)
                .withFieldValue("enumField", new EnumerationType.Value(1))
                .withFieldValue("mapField", Map.of("a", 1L, "b", 2L))
                .withFieldValue("rowField", createChildRow())
                .withFieldValue("intArrayField", Arrays.asList(1, 2, 3))
                .withFieldValue("int16ArrayField", Arrays.asList((short) 1, (short) 2))
                .withFieldValue("int64ArrayField", Arrays.asList(1L, 2L))
                .withFieldValue("floatArrayField", Arrays.asList(1.5F, 2.5F))
                .withFieldValue("doubleArrayField", Arrays.asList(1.5D, 2.5D))
                .withFieldValue("stringArrayField", Arrays.asList("1.5", "2.5"))
                .withFieldValue("datetimeArrayField", Arrays.asList(TEST_TIMESTAMP))
                .withFieldValue("dateArrayField", Arrays.asList(TEST_DATE))
                .withFieldValue("timeArrayField", Arrays.asList(TEST_TIME))
                .withFieldValue("enumArrayField", Arrays.asList(new EnumerationType.Value(0), new EnumerationType.Value(2)))
                .withFieldValue("rowArrayField", Arrays.asList(createChildRow(), createChildRow()))
                .withFieldValue("nullableStringField", null)
                .withFieldValue("nullableArrayField", null)
                .build();
    }

    @Test
    public void testToBuilderRenameAndFilter() {
        final Schema srcSchema = Schema.builder()
                .addStringField("a")
                .addInt32Field("b")
                .build();
        final Row src = Row.withSchema(srcSchema)
                .withFieldValue("a", "renamedValue")
                .withFieldValue("b", 1)
                .build();

        // filter with exclude
        final Schema excluded = RowSchemaUtil.toBuilder(srcSchema, Arrays.asList("a"), true).build();
        Assertions.assertEquals(1, excluded.getFieldCount());
        Assertions.assertTrue(excluded.hasField("b"));

        // rename fields
        final Schema dstSchema = Schema.builder()
                .addField("a2", Schema.FieldType.STRING.withNullable(true))
                .addField("b", Schema.FieldType.INT32.withNullable(true))
                .addField("c", Schema.FieldType.STRING.withNullable(true))
                .build();
        final Row dst = RowSchemaUtil.toBuilder(dstSchema, src, Map.of("a", "a2")).build();
        Assertions.assertEquals("renamedValue", dst.getString("a2"));
        Assertions.assertEquals(1, dst.getInt32("b"));
        Assertions.assertNull(dst.getString("c"));
    }

    @Test
    public void testRemoveAndRenameFields() {
        final Schema schema = Schema.builder()
                .addStringField("a")
                .addInt32Field("b")
                .addInt64Field("c")
                .build();

        Assertions.assertSame(schema, RowSchemaUtil.removeFields(schema, null));
        Assertions.assertSame(schema, RowSchemaUtil.removeFields(schema, new ArrayList<>()));

        final Schema removed = RowSchemaUtil.removeFields(schema, Arrays.asList("b"));
        Assertions.assertEquals(2, removed.getFieldCount());
        Assertions.assertTrue(removed.hasField("a"));
        Assertions.assertFalse(removed.hasField("b"));
        Assertions.assertTrue(removed.hasField("c"));

        final Schema renamed = RowSchemaUtil.renameFields(schema, Map.of("a", "a2"));
        Assertions.assertEquals(3, renamed.getFieldCount());
        Assertions.assertTrue(renamed.hasField("a2"));
        Assertions.assertFalse(renamed.hasField("a"));
        Assertions.assertEquals(Schema.TypeName.STRING, renamed.getField("a2").getType().getTypeName());
        Assertions.assertTrue(renamed.hasField("b"));
        Assertions.assertTrue(renamed.hasField("c"));
    }

    @Test
    public void testGetValueTypes() {
        final Row row = createTypedRow();
        Assertions.assertEquals(true, RowSchemaUtil.getValue(row, "booleanField"));
        Assertions.assertEquals("stringValue", RowSchemaUtil.getValue(row, "stringField"));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) RowSchemaUtil.getValue(row, "bytesField"));
        Assertions.assertEquals((byte) 3, RowSchemaUtil.getValue(row, "byteField"));
        Assertions.assertEquals((short) 12, RowSchemaUtil.getValue(row, "int16Field"));
        Assertions.assertEquals(42, RowSchemaUtil.getValue(row, "int32Field"));
        Assertions.assertEquals(1234567890123L, RowSchemaUtil.getValue(row, "int64Field"));
        Assertions.assertEquals(1.5F, RowSchemaUtil.getValue(row, "floatField"));
        Assertions.assertEquals(-2.25D, RowSchemaUtil.getValue(row, "doubleField"));
        Assertions.assertEquals(0, new BigDecimal("1234.5").compareTo((BigDecimal) RowSchemaUtil.getValue(row, "decimalField")));
        Assertions.assertEquals(TEST_TIMESTAMP, RowSchemaUtil.getValue(row, "datetimeField"));
        Assertions.assertEquals(TEST_DATE, RowSchemaUtil.getValue(row, "dateField"));
        Assertions.assertEquals(TEST_TIME, RowSchemaUtil.getValue(row, "timeField"));
        Assertions.assertEquals("b", RowSchemaUtil.getValue(row, "enumField"));
        Assertions.assertEquals(Map.of("a", 1L, "b", 2L), RowSchemaUtil.getValue(row, "mapField"));
        Assertions.assertEquals(createChildRow(), RowSchemaUtil.getValue(row, "rowField"));
        Assertions.assertEquals(Arrays.asList(1, 2, 3), RowSchemaUtil.getValue(row, "intArrayField"));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP), RowSchemaUtil.getValue(row, "datetimeArrayField"));
        Assertions.assertNull(RowSchemaUtil.getValue(row, "nullableStringField"));
        Assertions.assertNull(RowSchemaUtil.getValue(row, "notExistsField"));
        Assertions.assertNull(RowSchemaUtil.getValue(null, "stringField"));
    }

    @Test
    public void testGetAsNumbers() {
        final Row row = createTypedRow();

        // getAsLong
        Assertions.assertEquals(1L, RowSchemaUtil.getAsLong(row, "booleanField"));
        Assertions.assertEquals(123L, RowSchemaUtil.getAsLong(row, "numericStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsLong(row, "stringField"));
        Assertions.assertEquals(3L, RowSchemaUtil.getAsLong(row, "byteField"));
        Assertions.assertEquals(12L, RowSchemaUtil.getAsLong(row, "int16Field"));
        Assertions.assertEquals(42L, RowSchemaUtil.getAsLong(row, "int32Field"));
        Assertions.assertEquals(1234567890123L, RowSchemaUtil.getAsLong(row, "int64Field"));
        Assertions.assertEquals(1L, RowSchemaUtil.getAsLong(row, "floatField"));
        Assertions.assertEquals(-2L, RowSchemaUtil.getAsLong(row, "doubleField"));
        Assertions.assertEquals(1234L, RowSchemaUtil.getAsLong(row, "decimalField"));
        Assertions.assertNull(RowSchemaUtil.getAsLong(row, "datetimeField"));
        Assertions.assertNull(RowSchemaUtil.getAsLong(row, "nullableStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsLong(row, "notExistsField"));
        Assertions.assertNull(RowSchemaUtil.getAsLong(null, "int32Field"));

        // getAsDouble
        Assertions.assertEquals(1D, RowSchemaUtil.getAsDouble(row, "booleanField"));
        Assertions.assertEquals(123D, RowSchemaUtil.getAsDouble(row, "numericStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsDouble(row, "stringField"));
        Assertions.assertEquals(3D, RowSchemaUtil.getAsDouble(row, "byteField"));
        Assertions.assertEquals(12D, RowSchemaUtil.getAsDouble(row, "int16Field"));
        Assertions.assertEquals(42D, RowSchemaUtil.getAsDouble(row, "int32Field"));
        Assertions.assertEquals(1.5D, RowSchemaUtil.getAsDouble(row, "floatField"));
        Assertions.assertEquals(-2.25D, RowSchemaUtil.getAsDouble(row, "doubleField"));
        Assertions.assertEquals(1234.5D, RowSchemaUtil.getAsDouble(row, "decimalField"));
        Assertions.assertEquals((double) TEST_TIMESTAMP.getMillis(), RowSchemaUtil.getAsDouble(row, "datetimeField"));
        Assertions.assertNull(RowSchemaUtil.getAsDouble(row, "nullableStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsDouble(row, "notExistsField"));
        Assertions.assertNull(RowSchemaUtil.getAsDouble(null, "int32Field"));

        // getAsBigDecimal
        Assertions.assertEquals(0, BigDecimal.ONE.compareTo(RowSchemaUtil.getAsBigDecimal(row, "booleanField")));
        Assertions.assertEquals(0, new BigDecimal("123").compareTo(RowSchemaUtil.getAsBigDecimal(row, "numericStringField")));
        Assertions.assertNull(RowSchemaUtil.getAsBigDecimal(row, "stringField"));
        Assertions.assertEquals(0, new BigDecimal("3").compareTo(RowSchemaUtil.getAsBigDecimal(row, "byteField")));
        Assertions.assertEquals(0, new BigDecimal("12").compareTo(RowSchemaUtil.getAsBigDecimal(row, "int16Field")));
        Assertions.assertEquals(0, new BigDecimal("42").compareTo(RowSchemaUtil.getAsBigDecimal(row, "int32Field")));
        Assertions.assertEquals(0, new BigDecimal("1234567890123").compareTo(RowSchemaUtil.getAsBigDecimal(row, "int64Field")));
        Assertions.assertEquals(0, new BigDecimal("1.5").compareTo(RowSchemaUtil.getAsBigDecimal(row, "floatField")));
        Assertions.assertEquals(0, new BigDecimal("-2.25").compareTo(RowSchemaUtil.getAsBigDecimal(row, "doubleField")));
        Assertions.assertEquals(0, new BigDecimal("1234.5").compareTo(RowSchemaUtil.getAsBigDecimal(row, "decimalField")));
        Assertions.assertNull(RowSchemaUtil.getAsBigDecimal(row, "datetimeField"));
        Assertions.assertNull(RowSchemaUtil.getAsBigDecimal(row, "nullableStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsBigDecimal(row, "notExistsField"));
        Assertions.assertNull(RowSchemaUtil.getAsBigDecimal(null, "int32Field"));
    }

    @Test
    public void testGetAsBytes() {
        final Row row = createTypedRow();
        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), RowSchemaUtil.getAsBytes(row, "bytesField"));
        Assertions.assertEquals(ByteBuffer.wrap(TEST_BYTES), RowSchemaUtil.getAsBytes(row, "base64Field"));
        Assertions.assertNull(RowSchemaUtil.getAsBytes(row, "int32Field"));
        Assertions.assertNull(RowSchemaUtil.getAsBytes(row, "nullableStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsBytes(row, "notExistsField"));
        Assertions.assertNull(RowSchemaUtil.getAsBytes(null, "bytesField"));
    }

    @Test
    public void testGetTimestamp() {
        final Row row = createTypedRow();
        final Instant defaultTimestamp = Instant.parse("2000-01-01T00:00:00Z");
        Assertions.assertEquals(TEST_TIMESTAMP, RowSchemaUtil.getTimestamp(row, "datetimeField", defaultTimestamp));
        Assertions.assertEquals(TEST_TIMESTAMP, RowSchemaUtil.getTimestamp(row, "timestampStringField", defaultTimestamp));
        Assertions.assertEquals(defaultTimestamp, RowSchemaUtil.getTimestamp(row, "stringField", defaultTimestamp));
        Assertions.assertEquals(
                new org.joda.time.DateTime(1970, 2, 12, 0, 0, org.joda.time.DateTimeZone.UTC).toInstant(),
                RowSchemaUtil.getTimestamp(row, "int32Field", defaultTimestamp));
        Assertions.assertEquals(Instant.ofEpochMilli(1234567890123L), RowSchemaUtil.getTimestamp(row, "int64Field", defaultTimestamp));
        Assertions.assertEquals(Instant.ofEpochMilli(1L), RowSchemaUtil.getTimestamp(row, "floatField", defaultTimestamp));
        Assertions.assertEquals(Instant.ofEpochMilli(-2L), RowSchemaUtil.getTimestamp(row, "doubleField", defaultTimestamp));
        Assertions.assertEquals(defaultTimestamp, RowSchemaUtil.getTimestamp(row, "booleanField", defaultTimestamp));
        Assertions.assertEquals(defaultTimestamp, RowSchemaUtil.getTimestamp(row, "nullableStringField", defaultTimestamp));
        Assertions.assertEquals(Instant.parse("2021-03-02T00:00:00Z"), RowSchemaUtil.getTimestamp(row, "dateField", defaultTimestamp));
        Assertions.assertEquals(defaultTimestamp, RowSchemaUtil.getTimestamp(row, "notExistsField", defaultTimestamp));
        Assertions.assertEquals(TEST_TIMESTAMP, RowSchemaUtil.getAsInstant(row, "datetimeField"));
    }

    @Test
    public void testGetAsPrimitive() {
        final Row row = createTypedRow();

        // by row and fieldName
        Assertions.assertEquals(true, RowSchemaUtil.getAsPrimitive(row, "booleanField"));
        Assertions.assertEquals("stringValue", RowSchemaUtil.getAsPrimitive(row, "stringField"));
        Assertions.assertEquals(12, RowSchemaUtil.getAsPrimitive(row, "int16Field"));
        Assertions.assertEquals(42, RowSchemaUtil.getAsPrimitive(row, "int32Field"));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) RowSchemaUtil.getAsPrimitive(row, "bytesField"));
        Assertions.assertEquals(1234.5D, RowSchemaUtil.getAsPrimitive(row, "decimalField"));
        Assertions.assertEquals(TEST_TIMESTAMP.getMillis() * 1000L, RowSchemaUtil.getAsPrimitive(row, "datetimeField"));
        Assertions.assertEquals((int) TEST_DATE.toEpochDay(), RowSchemaUtil.getAsPrimitive(row, "dateField"));
        Assertions.assertEquals(TEST_TIME.toNanoOfDay() / 1000L, RowSchemaUtil.getAsPrimitive(row, "timeField"));
        Assertions.assertEquals(1, RowSchemaUtil.getAsPrimitive(row, "enumField"));
        Assertions.assertEquals(Map.of("a", 1L, "b", 2L), RowSchemaUtil.getAsPrimitive(row, "mapField"));
        Assertions.assertEquals(7, RowSchemaUtil.getAsPrimitive(row, "rowField.ci"));
        Assertions.assertNull(RowSchemaUtil.getAsPrimitive(row, "nullableStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsPrimitive(row, "notExistsField"));
        Assertions.assertNull(RowSchemaUtil.getAsPrimitive((Row) null, "stringField"));

        final Map<String, Object> childMap = (Map<String, Object>) RowSchemaUtil.getAsPrimitive(row, "rowField");
        Assertions.assertEquals("c", childMap.get("cs"));
        Assertions.assertEquals(7, childMap.get("ci"));

        // arrays
        Assertions.assertEquals(Arrays.asList(1, 2, 3), RowSchemaUtil.getAsPrimitive(row, "intArrayField"));
        Assertions.assertEquals(Arrays.asList(1, 2), RowSchemaUtil.getAsPrimitive(row, "int16ArrayField"));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP.getMillis() * 1000L), RowSchemaUtil.getAsPrimitive(row, "datetimeArrayField"));
        Assertions.assertEquals(List.of(TEST_DATE.toEpochDay()), RowSchemaUtil.getAsPrimitive(row, "dateArrayField"));
        Assertions.assertEquals(List.of(TEST_TIME.toNanoOfDay() / 1000L), RowSchemaUtil.getAsPrimitive(row, "timeArrayField"));
        Assertions.assertEquals(Arrays.asList(0, 2), RowSchemaUtil.getAsPrimitive(row, "enumArrayField"));
        final List<Map<String, Object>> childMaps = (List<Map<String, Object>>) RowSchemaUtil.getAsPrimitive(row, "rowArrayField");
        Assertions.assertEquals(2, childMaps.size());
        Assertions.assertEquals("c", childMaps.get(0).get("cs"));

        // by row, fieldType and fieldName
        Assertions.assertEquals(7, RowSchemaUtil.getAsPrimitive((Object) row, Schema.FieldType.INT32, "rowField.ci"));
        Assertions.assertEquals("stringValue", RowSchemaUtil.getAsPrimitive((Object) row, Schema.FieldType.STRING, "stringField"));

        // by fieldType and value
        Assertions.assertEquals(5, RowSchemaUtil.getAsPrimitive(Schema.FieldType.INT32, 5));
        Assertions.assertNull(RowSchemaUtil.getAsPrimitive(Schema.FieldType.INT32, null));
    }

    @Test
    public void testGetAsStandard() {
        final Row row = createTypedRow();

        // by row and fieldName
        Assertions.assertEquals(true, RowSchemaUtil.getAsStandard(row, "booleanField"));
        Assertions.assertEquals("stringValue", RowSchemaUtil.getAsStandard(row, "stringField"));
        Assertions.assertEquals(42, RowSchemaUtil.getAsStandard(row, "int32Field"));
        Assertions.assertEquals(1234567890123L, RowSchemaUtil.getAsStandard(row, "int64Field"));
        Assertions.assertEquals(TEST_DATE, RowSchemaUtil.getAsStandard(row, "dateField"));
        Assertions.assertEquals(TEST_TIME, RowSchemaUtil.getAsStandard(row, "timeField"));
        Assertions.assertEquals(java.time.Instant.parse("2021-03-02T01:02:03Z"), RowSchemaUtil.getAsStandard(row, "datetimeField"));
        Assertions.assertEquals("b", RowSchemaUtil.getAsStandard(row, "enumField"));
        Assertions.assertEquals(Arrays.asList(1, 2, 3), RowSchemaUtil.getAsStandard(row, "intArrayField"));
        Assertions.assertNull(RowSchemaUtil.getAsStandard(row, "nullableStringField"));
        Assertions.assertNull(RowSchemaUtil.getAsStandard(row, "notExistsField"));
        Assertions.assertNull(RowSchemaUtil.getAsStandard((Row) null, "stringField"));

        final Map<String, Object> childMap = (Map<String, Object>) RowSchemaUtil.getAsStandard(row, "rowField");
        Assertions.assertEquals("c", childMap.get("cs"));
        Assertions.assertEquals(7, childMap.get("ci"));

        // by fieldType and value
        Assertions.assertEquals(true, RowSchemaUtil.getAsStandard(Schema.FieldType.BOOLEAN, (Object) "true"));
        Assertions.assertEquals(true, RowSchemaUtil.getAsStandard(Schema.FieldType.BOOLEAN, 1));
        Assertions.assertEquals(12, RowSchemaUtil.getAsStandard(Schema.FieldType.INT32, (Object) "12"));
        Assertions.assertEquals(1, RowSchemaUtil.getAsStandard(Schema.FieldType.INT32, true));
        Assertions.assertEquals(9L, RowSchemaUtil.getAsStandard(Schema.FieldType.INT64, (Object) "9"));
        Assertions.assertEquals(1.5F, RowSchemaUtil.getAsStandard(Schema.FieldType.FLOAT, (Object) "1.5"));
        Assertions.assertEquals(2.5D, RowSchemaUtil.getAsStandard(Schema.FieldType.DOUBLE, (Object) "2.5"));
        Assertions.assertEquals("abc", RowSchemaUtil.getAsStandard(Schema.FieldType.STRING, "abc".getBytes(StandardCharsets.UTF_8)));
        Assertions.assertEquals("123", RowSchemaUtil.getAsStandard(Schema.FieldType.STRING, 123));
        Assertions.assertEquals(ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8)),
                RowSchemaUtil.getAsStandard(Schema.FieldType.BYTES, (Object) "abc"));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        Assertions.assertEquals(TEST_DATE, RowSchemaUtil.getAsStandard(dateType, (Object) "2021-03-02"));
        Assertions.assertEquals(TEST_DATE, RowSchemaUtil.getAsStandard(dateType, (int) TEST_DATE.toEpochDay()));

        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        Assertions.assertEquals(TEST_TIME, RowSchemaUtil.getAsStandard(timeType, (Object) "15:24:01"));
        Assertions.assertEquals(TEST_TIME, RowSchemaUtil.getAsStandard(timeType, TEST_TIME.toNanoOfDay() / 1000L));

        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));
        Assertions.assertEquals("b", RowSchemaUtil.getAsStandard(enumType, 1));
        Assertions.assertEquals("b", RowSchemaUtil.getAsStandard(enumType, (Object) "b"));

        Assertions.assertEquals(java.time.Instant.parse("2021-03-02T01:02:03Z"),
                RowSchemaUtil.getAsStandard(Schema.FieldType.DATETIME, (Object) "2021-03-02T01:02:03Z"));
        Assertions.assertEquals(java.time.Instant.parse("2021-03-02T01:02:03Z"),
                RowSchemaUtil.getAsStandard(Schema.FieldType.DATETIME, TEST_TIMESTAMP.getMillis() * 1000L));

        Assertions.assertNull(RowSchemaUtil.getAsStandard(Schema.FieldType.STRING, (Object) null));
    }

    @Test
    public void testGetAsFloatList() {
        final Row row = createTypedRow();
        Assertions.assertEquals(List.of(1.5F, 2.5F), RowSchemaUtil.getAsFloatList(row, "stringArrayField"));
        Assertions.assertEquals(List.of(1.0F, 2.0F), RowSchemaUtil.getAsFloatList(row, "int16ArrayField"));
        Assertions.assertEquals(List.of(1.0F, 2.0F, 3.0F), RowSchemaUtil.getAsFloatList(row, "intArrayField"));
        Assertions.assertEquals(List.of(1.0F, 2.0F), RowSchemaUtil.getAsFloatList(row, "int64ArrayField"));
        Assertions.assertEquals(List.of(1.5F, 2.5F), RowSchemaUtil.getAsFloatList(row, "floatArrayField"));
        Assertions.assertEquals(List.of(1.5F, 2.5F), RowSchemaUtil.getAsFloatList(row, "doubleArrayField"));
        Assertions.assertTrue(RowSchemaUtil.getAsFloatList(row, "nullableArrayField").isEmpty());
        Assertions.assertTrue(RowSchemaUtil.getAsFloatList(row, "stringField").isEmpty());
        Assertions.assertTrue(RowSchemaUtil.getAsFloatList(row, "notExistsField").isEmpty());
    }

    @Test
    public void testConvertPrimitive() {
        Assertions.assertNull(RowSchemaUtil.convertPrimitive(Schema.FieldType.INT64, null));
        Assertions.assertEquals((short) 12, RowSchemaUtil.convertPrimitive(Schema.FieldType.INT16, "12"));
        Assertions.assertEquals((short) 12, RowSchemaUtil.convertPrimitive(Schema.FieldType.INT16, 12));
        Assertions.assertEquals(42, RowSchemaUtil.convertPrimitive(Schema.FieldType.INT32, "42"));
        Assertions.assertEquals(42, RowSchemaUtil.convertPrimitive(Schema.FieldType.INT32, 42L));
        Assertions.assertEquals(9L, RowSchemaUtil.convertPrimitive(Schema.FieldType.INT64, "9"));
        Assertions.assertEquals(1.5F, RowSchemaUtil.convertPrimitive(Schema.FieldType.FLOAT, "1.5"));
        Assertions.assertEquals(2.5D, RowSchemaUtil.convertPrimitive(Schema.FieldType.DOUBLE, "2.5"));
        Assertions.assertEquals("123", RowSchemaUtil.convertPrimitive(Schema.FieldType.STRING, 123));
        Assertions.assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8),
                (byte[]) RowSchemaUtil.convertPrimitive(Schema.FieldType.BYTES, "abc"));
        Assertions.assertEquals(true, RowSchemaUtil.convertPrimitive(Schema.FieldType.BOOLEAN, "true"));
        Assertions.assertEquals(true, RowSchemaUtil.convertPrimitive(Schema.FieldType.BOOLEAN, 1));
        Assertions.assertEquals(TEST_TIMESTAMP, RowSchemaUtil.convertPrimitive(Schema.FieldType.DATETIME, TEST_TIMESTAMP));
        Assertions.assertEquals(TEST_TIMESTAMP, RowSchemaUtil.convertPrimitive(Schema.FieldType.DATETIME, TEST_TIMESTAMP.getMillis() * 1000L));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        Assertions.assertEquals(TEST_DATE, RowSchemaUtil.convertPrimitive(dateType, (int) TEST_DATE.toEpochDay()));
        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        Assertions.assertEquals(TEST_TIME, RowSchemaUtil.convertPrimitive(timeType, TEST_TIME.toNanoOfDay() / 1000L));
        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));
        final EnumerationType.Value enumValue = (EnumerationType.Value) RowSchemaUtil.convertPrimitive(enumType, 1);
        Assertions.assertEquals(1, enumValue.getValue());
        Assertions.assertEquals(enumValue, RowSchemaUtil.convertPrimitive(enumType, enumValue));

        // map
        final Map<String, Object> mapValue = (Map<String, Object>) RowSchemaUtil.convertPrimitive(
                Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT64), Map.of("a", "1"));
        Assertions.assertEquals(1L, mapValue.get("a"));

        // row from map (also covers convertPrimitives)
        final Row childRow = (Row) RowSchemaUtil.convertPrimitive(
                Schema.FieldType.row(CHILD_SCHEMA), Map.of("cs", "c", "ci", 7));
        Assertions.assertEquals("c", childRow.getString("cs"));
        Assertions.assertEquals(7, childRow.getInt32("ci"));
        Assertions.assertSame(childRow, RowSchemaUtil.convertPrimitive(Schema.FieldType.row(CHILD_SCHEMA), childRow));

        // arrays
        Assertions.assertEquals(Arrays.asList((short) 1, (short) 2), RowSchemaUtil.convertPrimitive(
                Schema.FieldType.array(Schema.FieldType.INT16), Arrays.asList(1, 2)));
        Assertions.assertEquals(Arrays.asList(1, 2), RowSchemaUtil.convertPrimitive(
                Schema.FieldType.array(Schema.FieldType.INT32), Arrays.asList(1, 2)));
        Assertions.assertEquals(List.of(TEST_TIMESTAMP), RowSchemaUtil.convertPrimitive(
                Schema.FieldType.array(Schema.FieldType.DATETIME), List.of(TEST_TIMESTAMP.getMillis() * 1000L)));
        Assertions.assertEquals(List.of(TEST_DATE), RowSchemaUtil.convertPrimitive(
                Schema.FieldType.array(dateType), List.of((int) TEST_DATE.toEpochDay())));
        Assertions.assertEquals(List.of(TEST_TIME), RowSchemaUtil.convertPrimitive(
                Schema.FieldType.array(timeType), List.of(TEST_TIME.toNanoOfDay() / 1000L)));
        final List<EnumerationType.Value> enumValues = (List<EnumerationType.Value>) RowSchemaUtil.convertPrimitive(
                Schema.FieldType.array(enumType), Arrays.asList(0, 2));
        Assertions.assertEquals(
                Arrays.asList(0, 2),
                enumValues.stream().map(EnumerationType.Value::getValue).collect(Collectors.toList()));
        final List<Row> childRows = (List<Row>) RowSchemaUtil.convertPrimitive(
                Schema.FieldType.array(Schema.FieldType.row(CHILD_SCHEMA)), List.of(Map.of("cs", "c", "ci", 7)));
        Assertions.assertEquals(1, childRows.size());
        Assertions.assertEquals("c", childRows.get(0).getString("cs"));
    }

    @Test
    public void testGetDefaultValue() {
        Assertions.assertNull(RowSchemaUtil.getDefaultValue(Schema.FieldType.STRING, null));
        Assertions.assertNull(RowSchemaUtil.getDefaultValue(Schema.FieldType.STRING, Schema.Options.none()));

        Assertions.assertEquals("abc", RowSchemaUtil.getDefaultValue(
                Schema.FieldType.STRING, RowSchemaUtil.createDefaultValueOptions("abc")));
        Assertions.assertEquals(true, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.BOOLEAN, RowSchemaUtil.createDefaultValueOptions("true")));
        Assertions.assertEquals((byte) 1, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.BYTE, RowSchemaUtil.createDefaultValueOptions("1")));
        Assertions.assertEquals((short) 12, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.INT16, RowSchemaUtil.createDefaultValueOptions("12")));
        Assertions.assertEquals(123, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.INT32, RowSchemaUtil.createDefaultValueOptions("123")));
        Assertions.assertEquals(12345L, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.INT64, RowSchemaUtil.createDefaultValueOptions("12345")));
        Assertions.assertEquals(1.5F, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.FLOAT, RowSchemaUtil.createDefaultValueOptions("1.5")));
        Assertions.assertEquals(2.5D, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.DOUBLE, RowSchemaUtil.createDefaultValueOptions("2.5")));
        Assertions.assertEquals(0, new BigDecimal("1.23").compareTo((BigDecimal) RowSchemaUtil.getDefaultValue(
                Schema.FieldType.DECIMAL, RowSchemaUtil.createDefaultValueOptions("1.23"))));
        Assertions.assertArrayEquals(TEST_BYTES, (byte[]) RowSchemaUtil.getDefaultValue(
                Schema.FieldType.BYTES, RowSchemaUtil.createDefaultValueOptions("SGVsbG8=")));
        Assertions.assertEquals(TEST_TIMESTAMP, RowSchemaUtil.getDefaultValue(
                Schema.FieldType.DATETIME, RowSchemaUtil.createDefaultValueOptions("2021-03-02T01:02:03Z")));

        final Schema.FieldType dateType = Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType());
        Assertions.assertEquals(TEST_DATE, RowSchemaUtil.getDefaultValue(
                dateType, RowSchemaUtil.createDefaultValueOptions("2021-03-02")));
        Assertions.assertEquals(TEST_DATE, RowSchemaUtil.getDefaultValue(
                dateType, RowSchemaUtil.createDefaultValueOptions(Long.toString(TEST_DATE.toEpochDay()))));

        final Schema.FieldType timeType = Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType());
        Assertions.assertEquals(TEST_TIME, RowSchemaUtil.getDefaultValue(
                timeType, RowSchemaUtil.createDefaultValueOptions("\"15:24:01\"")));

        final Schema.FieldType enumType = Schema.FieldType.logicalType(EnumerationType.create("a", "b", "c"));
        final EnumerationType.Value enumValue = (EnumerationType.Value) RowSchemaUtil.getDefaultValue(
                enumType, RowSchemaUtil.createDefaultValueOptions("b"));
        Assertions.assertEquals(1, enumValue.getValue());
    }

}
