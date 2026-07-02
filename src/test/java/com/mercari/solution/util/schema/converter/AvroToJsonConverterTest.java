package com.mercari.solution.util.schema.converter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.TestDatum;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;

public class AvroToJsonConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void test() {
        final GenericRecord record = TestDatum.generateRecord();
        final JsonObject json = new Gson().fromJson(AvroToJsonConverter.convert(record), JsonObject.class);
        testFlatField(json);
        final JsonObject childJson = json.get("recordField").getAsJsonObject();
        testFlatField(childJson);
        for(JsonElement g : childJson.get("recordArrayField").getAsJsonArray()) {
            testFlatField(g.getAsJsonObject());
        }
        final JsonObject grandchildJson = childJson.get("recordField").getAsJsonObject();
        testFlatField(grandchildJson);

        for(JsonElement c : json.get("recordArrayField").getAsJsonArray()) {
            testFlatField(c.getAsJsonObject());
            final JsonObject gc = c.getAsJsonObject().get("recordField").getAsJsonObject();
            testFlatField(gc);
            for(JsonElement g : c.getAsJsonObject().get("recordArrayField").getAsJsonArray()) {
                testFlatField(g.getAsJsonObject());
            }
        }
    }

    @Test
    public void testNull() {
        final GenericRecord record = TestDatum.generateRecordNull();
        final JsonObject json = new Gson().fromJson(AvroToJsonConverter.convert(record), JsonObject.class);
        testFlatFieldNull(json);
        final JsonObject childJson = json.get("recordField").getAsJsonObject();
        testFlatFieldNull(childJson);
        for(JsonElement g : childJson.get("recordArrayField").getAsJsonArray()) {
            testFlatFieldNull(g.getAsJsonObject());
        }
        final JsonObject grandchildJson = childJson.get("recordField").getAsJsonObject();
        testFlatFieldNull(grandchildJson);

        for(JsonElement c : json.get("recordArrayField").getAsJsonArray()) {
            testFlatFieldNull(c.getAsJsonObject());
            final JsonObject gc = c.getAsJsonObject().get("recordField").getAsJsonObject();
            testFlatFieldNull(gc);
            for(JsonElement g : c.getAsJsonObject().get("recordArrayField").getAsJsonArray()) {
                testFlatFieldNull(g.getAsJsonObject());
            }
        }
    }

    private void testFlatField(final JsonObject jsonObject) {
        Assertions.assertEquals(TestDatum.getBooleanFieldValue(), jsonObject.get("booleanField").getAsBoolean());
        Assertions.assertEquals(TestDatum.getStringFieldValue(), jsonObject.get("stringField").getAsString());
        Assertions.assertEquals(TestDatum.getBytesFieldValue(), new String(
                Base64.getDecoder().decode(jsonObject.get("bytesField").getAsString()), StandardCharsets.UTF_8));
        Assertions.assertEquals(TestDatum.getIntFieldValue().intValue(), jsonObject.get("intField").getAsInt());
        Assertions.assertEquals(TestDatum.getLongFieldValue().longValue(), jsonObject.get("longField").getAsLong());
        Assertions.assertEquals(TestDatum.getFloatFieldValue(), jsonObject.get("floatField").getAsFloat(), DELTA);
        Assertions.assertEquals(TestDatum.getDoubleFieldValue(), jsonObject.get("doubleField").getAsDouble(), DELTA);
        Assertions.assertEquals(TestDatum.getDateFieldValue().toEpochDay(), LocalDate.parse(jsonObject.get("dateField").getAsString()).toEpochDay());
        Assertions.assertEquals(TestDatum.getTimeFieldValue().toSecondOfDay(), LocalTime.parse(jsonObject.get("timeField").getAsString()).toSecondOfDay());
        Assertions.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Instant.parse(jsonObject.get("timestampField").getAsString()).getMillis());
        Assertions.assertEquals(TestDatum.getDecimalFieldValue().toString(), jsonObject.get("decimalField").getAsString());
        // TODO array check
    }

    private void testFlatFieldNull(final JsonObject jsonObject) {
        Assertions.assertTrue(jsonObject.get("booleanField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("bytesField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("stringField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("intField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("longField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("floatField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("doubleField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("dateField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("timeField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("timestampField").isJsonNull());
        Assertions.assertTrue(jsonObject.get("decimalField").isJsonNull());
    }

}
