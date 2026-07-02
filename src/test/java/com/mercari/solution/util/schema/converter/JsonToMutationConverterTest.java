package com.mercari.solution.util.schema.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class JsonToMutationConverterTest {

    private static final EnumerationType ENUM_TYPE = EnumerationType.create("FOO", "BAR");

    private static Schema.Options sqlTypeOption(final String sqlType) {
        return Schema.Options.builder()
                .setOption("sqlType", Schema.FieldType.STRING, sqlType)
                .build();
    }

    private static Schema createTestSchema() {
        return Schema.builder()
                .addField("boolField", Schema.FieldType.BOOLEAN)
                .addField("stringField", Schema.FieldType.STRING)
                .addField(Schema.Field.of("datetimeStringField", Schema.FieldType.STRING)
                        .withOptions(sqlTypeOption("DATETIME")))
                .addField(Schema.Field.of("jsonStringField", Schema.FieldType.STRING)
                        .withOptions(sqlTypeOption("JSON")))
                .addField(Schema.Field.of("geographyStringField", Schema.FieldType.STRING)
                        .withOptions(sqlTypeOption("GEOGRAPHY")))
                .addField("bytesField", Schema.FieldType.BYTES)
                .addField("floatField", Schema.FieldType.FLOAT)
                .addField("doubleField", Schema.FieldType.DOUBLE)
                .addField("decimalField", Schema.FieldType.DECIMAL)
                .addField("byteField", Schema.FieldType.BYTE)
                .addField("int16Field", Schema.FieldType.INT16)
                .addField("int32Field", Schema.FieldType.INT32)
                .addField("int64Field", Schema.FieldType.INT64)
                .addField("datetimeField", Schema.FieldType.DATETIME)
                .addField("dateField", CalciteUtils.DATE)
                .addField("timeField", CalciteUtils.TIME)
                .addField("enumField", Schema.FieldType.logicalType(ENUM_TYPE))
                .addField("boolArrayField", Schema.FieldType.array(Schema.FieldType.BOOLEAN))
                .addField("stringArrayField", Schema.FieldType.array(Schema.FieldType.STRING))
                .addField("bytesArrayField", Schema.FieldType.array(Schema.FieldType.BYTES))
                .addField("floatArrayField", Schema.FieldType.array(Schema.FieldType.FLOAT))
                .addField("doubleArrayField", Schema.FieldType.array(Schema.FieldType.DOUBLE))
                .addField("int64ArrayField", Schema.FieldType.array(Schema.FieldType.INT64))
                .addField("datetimeArrayField", Schema.FieldType.array(Schema.FieldType.DATETIME))
                .addField("dateArrayField", Schema.FieldType.array(CalciteUtils.DATE))
                .addField("timeArrayField", Schema.FieldType.array(CalciteUtils.TIME))
                .addField("enumArrayField", Schema.FieldType.array(Schema.FieldType.logicalType(ENUM_TYPE)))
                .build();
    }

    private static JsonObject createTestJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("boolField", true);
        json.addProperty("stringField", "hello");
        json.addProperty("datetimeStringField", "2024-03-01T12:00:00Z");
        json.addProperty("jsonStringField", "{\"a\":1}");
        json.addProperty("geographyStringField", "POINT(1 1)");
        json.addProperty("bytesField", Base64.getEncoder().encodeToString("bytes".getBytes(StandardCharsets.UTF_8)));
        json.addProperty("floatField", 1.5f);
        json.addProperty("doubleField", 2.5d);
        json.addProperty("decimalField", new BigDecimal("123.45"));
        json.addProperty("byteField", (byte) 3);
        json.addProperty("int16Field", (short) 16);
        json.addProperty("int32Field", 32);
        json.addProperty("int64Field", 64L);
        json.addProperty("datetimeField", "2024-03-01T12:00:00Z");
        json.addProperty("dateField", "2024-03-01");
        json.addProperty("timeField", "12:34:56");
        json.addProperty("enumField", "BAR");

        final JsonArray boolArray = new JsonArray();
        boolArray.add(true);
        boolArray.add(false);
        json.add("boolArrayField", boolArray);

        final JsonArray stringArray = new JsonArray();
        stringArray.add("a");
        stringArray.add(JsonNull.INSTANCE);
        stringArray.add("b");
        json.add("stringArrayField", stringArray);

        final JsonArray bytesArray = new JsonArray();
        bytesArray.add(Base64.getEncoder().encodeToString("b1".getBytes(StandardCharsets.UTF_8)));
        json.add("bytesArrayField", bytesArray);

        final JsonArray floatArray = new JsonArray();
        floatArray.add(1.5f);
        json.add("floatArrayField", floatArray);

        final JsonArray doubleArray = new JsonArray();
        doubleArray.add(2.5d);
        json.add("doubleArrayField", doubleArray);

        final JsonArray int64Array = new JsonArray();
        int64Array.add(1L);
        int64Array.add(2L);
        json.add("int64ArrayField", int64Array);

        final JsonArray datetimeArray = new JsonArray();
        datetimeArray.add("2024-03-01T12:00:00Z");
        datetimeArray.add(1709294400000000L);
        json.add("datetimeArrayField", datetimeArray);

        final JsonArray dateArray = new JsonArray();
        dateArray.add("2024-03-01");
        json.add("dateArrayField", dateArray);

        final JsonArray timeArray = new JsonArray();
        timeArray.add("12:34:56");
        json.add("timeArrayField", timeArray);

        final JsonArray enumArray = new JsonArray();
        enumArray.add("FOO");
        json.add("enumArrayField", enumArray);

        return json;
    }

    @Test
    public void testConvertValues() {
        final Schema schema = createTestSchema();
        final JsonObject json = createTestJson();

        final Map<String, Value> values = JsonToMutationConverter.convertValues(schema, json);

        Assertions.assertEquals(Value.bool(true), values.get("boolField"));
        Assertions.assertEquals(Value.string("hello"), values.get("stringField"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), values.get("datetimeStringField"));
        Assertions.assertEquals(Value.json("{\"a\":1}"), values.get("jsonStringField"));
        Assertions.assertEquals(Value.string("POINT(1 1)"), values.get("geographyStringField"));
        Assertions.assertEquals(Value.bytes(ByteArray.copyFrom("bytes")), values.get("bytesField"));
        Assertions.assertEquals(Value.float64(1.5d), values.get("floatField"));
        Assertions.assertEquals(Value.float64(2.5d), values.get("doubleField"));
        Assertions.assertEquals(Value.numeric(new BigDecimal("123.45")), values.get("decimalField"));
        Assertions.assertEquals(Value.int64(3L), values.get("byteField"));
        Assertions.assertEquals(Value.int64(16L), values.get("int16Field"));
        Assertions.assertEquals(Value.int64(32L), values.get("int32Field"));
        Assertions.assertEquals(Value.int64(64L), values.get("int64Field"));
        Assertions.assertEquals(Value.timestamp(Timestamp.parseTimestamp("2024-03-01T12:00:00Z")), values.get("datetimeField"));
        Assertions.assertEquals(Value.date(Date.fromYearMonthDay(2024, 3, 1)), values.get("dateField"));
        Assertions.assertEquals(Value.string("12:34:56"), values.get("timeField"));
        Assertions.assertEquals(Value.string("BAR"), values.get("enumField"));

        Assertions.assertEquals(Value.boolArray(List.of(true, false)), values.get("boolArrayField"));
        // null elements are skipped
        Assertions.assertEquals(Value.stringArray(List.of("a", "b")), values.get("stringArrayField"));
        Assertions.assertEquals(Value.bytesArray(List.of(ByteArray.copyFrom("b1"))), values.get("bytesArrayField"));
        // FLOAT arrays are written as float64Array, matching the scalar FLOAT case (float64)
        Assertions.assertEquals(Value.float64Array(List.of(1.5d)), values.get("floatArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of(2.5d)), values.get("doubleArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of(1L, 2L)), values.get("int64ArrayField"));
        Assertions.assertEquals(
                Value.timestampArray(List.of(
                        Timestamp.parseTimestamp("2024-03-01T12:00:00Z"),
                        Timestamp.ofTimeMicroseconds(1709294400000000L))),
                values.get("datetimeArrayField"));
        Assertions.assertEquals(Value.dateArray(List.of(Date.fromYearMonthDay(2024, 3, 1))), values.get("dateArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("12:34:56")), values.get("timeArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of("FOO")), values.get("enumArrayField"));
    }

    @Test
    public void testConvertValuesNull() {
        final Schema schema = createTestSchema();

        // explicit JsonNull for some fields, all others absent
        final JsonObject json = new JsonObject();
        json.add("boolField", JsonNull.INSTANCE);
        json.add("stringField", JsonNull.INSTANCE);
        json.add("dateField", JsonNull.INSTANCE);
        json.add("stringArrayField", JsonNull.INSTANCE);

        final Map<String, Value> values = JsonToMutationConverter.convertValues(schema, json);

        Assertions.assertTrue(values.get("boolField").isNull());
        Assertions.assertTrue(values.get("stringField").isNull());
        Assertions.assertTrue(values.get("datetimeStringField").isNull());
        Assertions.assertTrue(values.get("jsonStringField").isNull());
        Assertions.assertTrue(values.get("bytesField").isNull());
        Assertions.assertTrue(values.get("floatField").isNull());
        Assertions.assertTrue(values.get("doubleField").isNull());
        Assertions.assertTrue(values.get("decimalField").isNull());
        Assertions.assertTrue(values.get("byteField").isNull());
        Assertions.assertTrue(values.get("int16Field").isNull());
        Assertions.assertTrue(values.get("int32Field").isNull());
        Assertions.assertTrue(values.get("int64Field").isNull());
        Assertions.assertTrue(values.get("datetimeField").isNull());
        Assertions.assertTrue(values.get("dateField").isNull());
        Assertions.assertTrue(values.get("timeField").isNull());
        Assertions.assertTrue(values.get("enumField").isNull());

        // null arrays are converted to empty arrays
        Assertions.assertEquals(Value.boolArray(List.of()), values.get("boolArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of()), values.get("stringArrayField"));
        Assertions.assertEquals(Value.bytesArray(List.of()), values.get("bytesArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of()), values.get("floatArrayField"));
        Assertions.assertEquals(Value.float64Array(List.of()), values.get("doubleArrayField"));
        Assertions.assertEquals(Value.int64Array(List.of()), values.get("int64ArrayField"));
        Assertions.assertEquals(Value.timestampArray(List.of()), values.get("datetimeArrayField"));
        Assertions.assertEquals(Value.dateArray(List.of()), values.get("dateArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of()), values.get("timeArrayField"));
        Assertions.assertEquals(Value.stringArray(List.of()), values.get("enumArrayField"));
    }

    @Test
    public void testConvertValuesUnsupportedType() {
        final Schema schema = Schema.builder()
                .addField("rowField", Schema.FieldType.row(Schema.builder()
                        .addField("name", Schema.FieldType.STRING)
                        .build()))
                .build();
        final JsonObject json = new JsonObject();
        final JsonObject child = new JsonObject();
        child.addProperty("name", "a");
        json.add("rowField", child);

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                JsonToMutationConverter.convertValues(schema, json));
    }

}
