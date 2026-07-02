package com.mercari.solution.util.schema.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.TestDatum;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Base64;
import java.util.List;

public class JsonToAvroConverterTest {

    @Test
    public void test() {
        final GenericRecord record = TestDatum.generateRecord();
        final String json = AvroToJsonConverter.convert(record);
        final GenericRecord revertedRecord = JsonToAvroConverter.convert(record.getSchema(), json);
        testFlatField(revertedRecord);
        final GenericRecord revertedRecordChild = (GenericRecord)revertedRecord.get("recordField");
        testFlatField(revertedRecordChild);
        final GenericRecord revertedRecordGrandchild = (GenericRecord)revertedRecordChild.get("recordField");
        testFlatField(revertedRecordGrandchild);

        for(final GenericRecord child : (List<GenericRecord>)revertedRecord.get("recordArrayField")) {
            testFlatField(child);
        }

        for(final GenericRecord child : (List<GenericRecord>)revertedRecordChild.get("recordArrayField")) {
            testFlatField(child);
        }
    }

    @Test
    public void testValidateSchema() {
        final GenericRecord record = TestDatum.generateRecord();
        final String json = AvroToJsonConverter.convert(record);

        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        Assertions.assertTrue(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));

        // Check existing field
        jsonObject.remove("stringField");
        Assertions.assertTrue(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
        jsonObject.remove("stringArrayField");
        Assertions.assertTrue(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));

        // Check additional field
        jsonObject.addProperty("newStringField", "stringValue");
        Assertions.assertFalse(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
        jsonObject.remove("newStringField");
        final JsonArray jsonArray = new JsonArray();
        jsonObject.add("newArrayField", jsonArray);
        Assertions.assertFalse(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
        jsonArray.add("stringValue");
        jsonObject.add("newArrayField", jsonArray);
        Assertions.assertFalse(JsonToAvroConverter.validateSchema(record.getSchema(), jsonObject));
    }

    private void testFlatField(final GenericRecord record) {
        Assertions.assertEquals(TestDatum.getBooleanFieldValue(), record.get("booleanField"));
        Assertions.assertEquals(TestDatum.getStringFieldValue(), record.get("stringField"));
        Assertions.assertEquals(TestDatum.getBytesFieldValue(), new String(Base64.getDecoder().decode(((ByteBuffer)record.get("bytesField")).array()), StandardCharsets.UTF_8));
        Assertions.assertEquals(TestDatum.getIntFieldValue(), record.get("intField"));
        Assertions.assertEquals(TestDatum.getLongFieldValue(), record.get("longField"));
        Assertions.assertEquals(TestDatum.getFloatFieldValue(), record.get("floatField"));
        Assertions.assertEquals(TestDatum.getDoubleFieldValue(), record.get("doubleField"));
        Assertions.assertEquals(TestDatum.getDateFieldValue(), LocalDate.ofEpochDay((int)record.get("dateField")));
        Assertions.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), (long)record.get("timestampField")/1000);
        int scale = AvroSchemaUtil.getLogicalTypeDecimal(record.getSchema().getField("decimalField").schema()).getScale();
        Assertions.assertEquals(TestDatum.getDecimalFieldValue(), BigDecimal.valueOf(new BigInteger(((ByteBuffer)record.get("decimalField")).array()).longValue(), scale));
        // TODO array check
    }

}
