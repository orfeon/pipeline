package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class AvroToRowConverterTest {

    @Test
    public void testConvertSchema() {
        final Schema inputSchema = SchemaBuilder
                .record("root")
                .fields()
                .name("stringField").doc("this is string field").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("jsonField").type(AvroSchemaUtil.NULLABLE_JSON).noDefault()
                .name("intField").doc("this is int field").type(AvroSchemaUtil.REQUIRED_INT).withDefault(0)
                .name("longField").doc("this is long field").type(AvroSchemaUtil.REQUIRED_LONG).withDefault(0L)
                .name("booleanField").doc("this is boolean field").type(AvroSchemaUtil.REQUIRED_BOOLEAN).withDefault(false)
                .name("decimalField").doc("this is decimal field").type(AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE).noDefault()
                .name("floatField").doc("this is float field").type(AvroSchemaUtil.NULLABLE_FLOAT).noDefault()
                .name("doubleField").doc("this is double field").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("timeField").doc("this is time field").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MICRO_TYPE).noDefault()
                .name("dateField").doc("this is date field").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timestampField").doc("this is timestamp field").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("bytesField").doc("this is bytes field").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("enumField").doc("this is enum field").orderDescending().type(Schema
                        .createUnion(Schema.create(Schema.Type.NULL), Schema.createEnum("enumField", "", "", Arrays.asList("a","b","c")))).noDefault()
                .name("stringArrayField").doc("this is string array field").type(AvroSchemaUtil.NULLABLE_ARRAY_STRING_TYPE).noDefault()
                .name("jsonArrayField").doc("this is json array field").type(AvroSchemaUtil.NULLABLE_ARRAY_JSON_TYPE).noDefault()
                .name("intArrayField").doc("this is int array field").type(AvroSchemaUtil.NULLABLE_ARRAY_INT_TYPE).noDefault()
                .name("longArrayField").doc("this is long array field").type(AvroSchemaUtil.NULLABLE_ARRAY_LONG_TYPE).noDefault()
                .name("booleanArrayField").doc("this is boolean array field").type(AvroSchemaUtil.NULLABLE_ARRAY_BOOLEAN_TYPE).noDefault()
                .name("decimalArrayField").doc("this is decimal array field").type(AvroSchemaUtil.NULLABLE_ARRAY_DECIMAL_TYPE).noDefault()
                .name("floatArrayField").doc("this is float array field").type(AvroSchemaUtil.NULLABLE_ARRAY_FLOAT_TYPE).noDefault()
                .name("doubleArrayField").doc("this is double array field").type(AvroSchemaUtil.NULLABLE_ARRAY_DOUBLE_TYPE).noDefault()
                .name("timeArrayField").doc("this is time array field").type(AvroSchemaUtil.NULLABLE_ARRAY_TIME_TYPE).noDefault()
                .name("dateArrayField").doc("this is date array field").type(AvroSchemaUtil.NULLABLE_ARRAY_DATE_TYPE).noDefault()
                .name("timestampArrayField").doc("this is timestamp array field").type(AvroSchemaUtil.NULLABLE_ARRAY_TIMESTAMP_TYPE).noDefault()
                .endRecord();
        final org.apache.beam.sdk.schemas.Schema outputSchema = AvroToRowConverter.convertSchema(inputSchema);

        System.out.println(outputSchema);

        // Field
        final org.apache.beam.sdk.schemas.Schema.Field stringField = outputSchema.getField("stringField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true), stringField.getType());
        Assertions.assertEquals("this is string field", stringField.getDescription());
        Assertions.assertEquals(0, (int)stringField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", stringField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field jsonField = outputSchema.getField("jsonField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true), stringField.getType());
        Assertions.assertEquals(1, (int)jsonField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", jsonField.getOptions().getValue("order"));
        Assertions.assertEquals("JSON", jsonField.getOptions().getValue("sqlType"));

        final org.apache.beam.sdk.schemas.Schema.Field intField = outputSchema.getField("intField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.INT32.withNullable(false), intField.getType());
        Assertions.assertEquals("this is int field", intField.getDescription());
        Assertions.assertEquals(2, (int)intField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", intField.getOptions().getValue("order"));
        Assertions.assertEquals(0, (int)intField.getOptions().getValue("defaultVal"));

        final org.apache.beam.sdk.schemas.Schema.Field longField = outputSchema.getField("longField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(false), longField.getType());
        Assertions.assertEquals("this is long field", longField.getDescription());
        Assertions.assertEquals(3, (int)longField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", longField.getOptions().getValue("order"));
        Assertions.assertEquals(0L, (long)longField.getOptions().getValue("defaultVal"));

        final org.apache.beam.sdk.schemas.Schema.Field booleanField = outputSchema.getField("booleanField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN, booleanField.getType());
        Assertions.assertEquals("this is boolean field", booleanField.getDescription());
        Assertions.assertEquals(4, (int)booleanField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", booleanField.getOptions().getValue("order"));
        Assertions.assertEquals(false, (boolean)booleanField.getOptions().getValue("defaultVal"));

        final org.apache.beam.sdk.schemas.Schema.Field decimalField = outputSchema.getField("decimalField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL.withNullable(true), decimalField.getType());
        Assertions.assertEquals(9, (int)decimalField.getOptions().getValue("scale"));
        Assertions.assertEquals(38, (int)decimalField.getOptions().getValue("precision"));
        Assertions.assertEquals("this is decimal field", decimalField.getDescription());
        Assertions.assertEquals(5, (int)decimalField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", decimalField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field floatField = outputSchema.getField("floatField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT.withNullable(true), floatField.getType());
        Assertions.assertEquals("this is float field", floatField.getDescription());
        Assertions.assertEquals(6, (int)floatField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", floatField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field doubleField = outputSchema.getField("doubleField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE.withNullable(true), doubleField.getType());
        Assertions.assertEquals("this is double field", doubleField.getDescription());
        Assertions.assertEquals(7, (int)doubleField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", doubleField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timeField = outputSchema.getField("timeField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType()).withNullable(true), timeField.getType());
        Assertions.assertEquals("this is time field", timeField.getDescription());
        Assertions.assertEquals(8, (int)timeField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", timeField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field dateField = outputSchema.getField("dateField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true), dateField.getType());
        Assertions.assertEquals("this is date field", dateField.getDescription());
        Assertions.assertEquals(9, (int)dateField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", dateField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timestampField = outputSchema.getField("timestampField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true), timestampField.getType());
        Assertions.assertEquals("this is timestamp field", timestampField.getDescription());
        Assertions.assertEquals(10, (int)timestampField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", timestampField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field bytesField = outputSchema.getField("bytesField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.BYTES.withNullable(true), bytesField.getType());
        Assertions.assertEquals("this is bytes field", bytesField.getDescription());
        Assertions.assertEquals(11, (int)bytesField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", bytesField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field enumField = outputSchema.getField("enumField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(EnumerationType.create("a","b","c")).withNullable(true), enumField.getType());
        Assertions.assertEquals("this is enum field", enumField.getDescription());
        Assertions.assertEquals(12, (int)enumField.getOptions().getValue("pos"));
        Assertions.assertEquals("DESCENDING", enumField.getOptions().getValue("order"));

        // Array Field
        final org.apache.beam.sdk.schemas.Schema.Field stringArrayField = outputSchema.getField("stringArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.STRING).withNullable(true), stringArrayField.getType());
        Assertions.assertEquals("this is string array field", stringArrayField.getDescription());
        Assertions.assertEquals(13, (int)stringArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", stringArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field jsonArrayField = outputSchema.getField("jsonArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.STRING).withNullable(true), jsonArrayField.getType());
        Assertions.assertEquals("JSON", jsonArrayField.getOptions().getValue("sqlType"));
        Assertions.assertEquals("this is json array field", jsonArrayField.getDescription());
        Assertions.assertEquals(14, (int)jsonArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", jsonArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field intArrayField = outputSchema.getField("intArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.INT32).withNullable(true), intArrayField.getType());
        Assertions.assertEquals("this is int array field", intArrayField.getDescription());
        Assertions.assertEquals(15, (int)intArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", intArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field longArrayField = outputSchema.getField("longArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.INT64).withNullable(true), longArrayField.getType());
        Assertions.assertEquals("this is long array field", longArrayField.getDescription());
        Assertions.assertEquals(16, (int)longArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", longArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field booleanArrayField = outputSchema.getField("booleanArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN).withNullable(true), booleanArrayField.getType());
        Assertions.assertEquals("this is boolean array field", booleanArrayField.getDescription());
        Assertions.assertEquals(17, (int)booleanArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", booleanArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field decimalArrayField = outputSchema.getField("decimalArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL).withNullable(true), decimalArrayField.getType());
        Assertions.assertEquals(9, (int)decimalArrayField.getOptions().getValue("scale"));
        Assertions.assertEquals(38, (int)decimalArrayField.getOptions().getValue("precision"));
        Assertions.assertEquals("this is decimal array field", decimalArrayField.getDescription());
        Assertions.assertEquals(18, (int)decimalArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", decimalArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field floatArrayField = outputSchema.getField("floatArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT).withNullable(true), floatArrayField.getType());
        Assertions.assertEquals("this is float array field", floatArrayField.getDescription());
        Assertions.assertEquals(19, (int)floatArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", floatArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field doubleArrayField = outputSchema.getField("doubleArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE).withNullable(true), doubleArrayField.getType());
        Assertions.assertEquals("this is double array field", doubleArrayField.getDescription());
        Assertions.assertEquals(20, (int)doubleArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", doubleArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timeArrayField = outputSchema.getField("timeArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.TIME.getLogicalType())).withNullable(true), timeArrayField.getType());
        Assertions.assertEquals("this is time array field", timeArrayField.getDescription());
        Assertions.assertEquals(21, (int)timeArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", timeArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field dateArrayField = outputSchema.getField("dateArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType())).withNullable(true), dateArrayField.getType());
        Assertions.assertEquals("this is date array field", dateArrayField.getDescription());
        Assertions.assertEquals(22, (int)dateArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", dateArrayField.getOptions().getValue("order"));

        final org.apache.beam.sdk.schemas.Schema.Field timestampArrayField = outputSchema.getField("timestampArrayField");
        Assertions.assertEquals(org.apache.beam.sdk.schemas.Schema.FieldType.array(org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME).withNullable(true), timestampArrayField.getType());
        Assertions.assertEquals("this is timestamp array field", timestampArrayField.getDescription());
        Assertions.assertEquals(23, (int)timestampArrayField.getOptions().getValue("pos"));
        Assertions.assertEquals("ASCENDING", timestampArrayField.getOptions().getValue("order"));

    }

}
