package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectFunctionTest {

    @Test
    public void test() {

        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("intField", Schema.FieldType.INT32),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("floatField", Schema.FieldType.FLOAT32),
                Schema.Field.of("doubleField", Schema.FieldType.FLOAT64),
                Schema.Field.of("enumField", Schema.FieldType.enumeration(List.of("a","b","c"))),
                Schema.Field.of("timestampField", Schema.FieldType.TIMESTAMP),
                Schema.Field.of("nestedField", Schema.FieldType.element(Schema.builder()
                                .withField("stringField", Schema.FieldType.STRING)
                        .build())),
                Schema.Field.of("arrayNestedField", Schema.FieldType.array(Schema.FieldType.element(Schema.builder()
                        .withField("stringField", Schema.FieldType.STRING)
                        .build())))
        );

        final String config = """
                [
                  { "name": "longField" },
                  { "name": "renameIntField", "field": "intField" },
                  { "name": "constantStringField", "type": "string", "value": "constantStringValue" },
                  { "name": "expressionField", "expression": "doubleField * intField / longField" },
                  { "name": "hashField", "func": "hash", "field": "stringField" },
                  { "name": "hashArrayField", "func": "hash", "fields": ["stringField","intField","longField"], "size": 32 },
                  { "name": "currentTimestampField", "func": "current_timestamp" },
                  { "name": "eventTimestampField", "func": "event_timestamp" },
                  { "name": "concatField", "func": "concat", "delimiter": " ", "fields": ["stringField","intField","longField"] },
                  { "name": "intField", "field": "nestedField.stringField", "type": "int32" },
                  { "name": "stringFieldA", "field": "nestedField.stringField", "type": "string" },
                  { "name": "structField", "func": "struct", "mode": "repeated", "fields": [
                    { "name": "enumField" },
                    { "name": "stringFieldA", "field": "stringField" },
                    { "name": "intFieldA", "field": "intField" },
                    { "name": "textFieldA", "func": "hash", "text": "${stringFieldA}" },
                    { "name": "nestedStructField", "func": "struct", "fields": [
                      { "name": "stringFieldA", "field": "stringField" },
                      { "name": "nestedNestedStructField", "func": "struct", "fields": [
                        { "name": "timestampField" },
                        { "name": "enumFieldA", "field": "enumField" }
                      ] }
                    ] }
                  ] },
                  { "name": "structEachField", "each": "arrayNestedField", "fields": [
                    { "name": "enumField" },
                    { "name": "stringFieldA", "field": "stringField" },
                    { "name": "intFieldA", "field": "intField" },
                    { "name": "textFieldA", "func": "hash", "text": "${stringFieldA}" },
                    { "name": "nestedStringField", "field": "arrayNestedField.stringField" }
                  ] },
                  { "name": "jsonField", "func": "json", "fields": [
                    { "name": "enumField" },
                    { "name": "stringFieldA", "field": "stringField" },
                    { "name": "longFieldA", "field": "longField" },
                    { "name": "nestedStructField", "func": "struct", "fields": [
                      { "name": "enumField" },
                      { "name": "doubleFieldA", "field": "doubleField" },
                      { "name": "timestampField" },
                      { "name": "nestedNestedStructField", "func": "struct", "fields": [
                        { "name": "timestampField" },
                        { "name": "enumFieldA", "field": "enumField" }
                      ] }
                    ] }
                  ] },
                  { "name": "hashField2", "func": "hash", "field": "stringField" },
                  { "name": "bytesEncodedLongField", "func": "bytes_encode", "field": "longField" },
                  { "name": "bytesDecodedLongField", "func": "bytes_decode", "field": "bytesEncodedLongField", "type": "int64" }
                ]
                """;

        final JsonArray array = new Gson().fromJson(config, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, inputFields);

        final Schema outputSchema = SelectFunction.createSchema(selectFunctions, "structEachField");
        Assertions.assertTrue(outputSchema.hasField("longField"));
        Assertions.assertTrue(outputSchema.hasField("renameIntField"));
        Assertions.assertTrue(outputSchema.hasField("constantStringField"));
        Assertions.assertTrue(outputSchema.hasField("expressionField"));
        Assertions.assertTrue(outputSchema.hasField("hashField"));
        Assertions.assertTrue(outputSchema.hasField("hashArrayField"));
        Assertions.assertTrue(outputSchema.hasField("currentTimestampField"));
        Assertions.assertTrue(outputSchema.hasField("eventTimestampField"));
        Assertions.assertTrue(outputSchema.hasField("concatField"));
        Assertions.assertTrue(outputSchema.hasField("structField"));
        Assertions.assertTrue(outputSchema.hasField("jsonField"));
        Assertions.assertTrue(outputSchema.hasField("intField"));
        Assertions.assertTrue(outputSchema.hasField("bytesEncodedLongField"));
        Assertions.assertTrue(outputSchema.hasField("bytesDecodedLongField"));
        Assertions.assertEquals(Schema.Type.int64, outputSchema.getField("longField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int32, outputSchema.getField("renameIntField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, outputSchema.getField("constantStringField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, outputSchema.getField("expressionField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, outputSchema.getField("hashField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, outputSchema.getField("hashArrayField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.timestamp, outputSchema.getField("currentTimestampField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.timestamp, outputSchema.getField("eventTimestampField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, outputSchema.getField("concatField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.array, outputSchema.getField("structField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, outputSchema.getField("jsonField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int32, outputSchema.getField("intField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.bytes, outputSchema.getField("bytesEncodedLongField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, outputSchema.getField("bytesDecodedLongField").getFieldType().getType());

        //
        for(final SelectFunction selectFunction : selectFunctions) {
            selectFunction.setup();
        }

        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", "stringValue");
        values.put("intField", 32);
        values.put("longField", 10L);
        values.put("floatField", -5.5F);
        values.put("doubleField", 10.10D);
        values.put("enumField", 1);
        values.put("timestampField", Instant.parse("2024-08-30T00:00:00Z").getMillis() * 1000L);

        {
            final Map<String, Object> nestedArrayFieldValues = new HashMap<>();
            nestedArrayFieldValues.put("stringField", "Z");
            values.put("arrayNestedField", List.of(nestedArrayFieldValues));
        }

        final Map<String, Object> nestedFieldValues = new HashMap<>();
        nestedFieldValues.put("stringField", "100");
        values.put("nestedField", nestedFieldValues);

        final Instant eventTimestamp = Instant.parse("2024-01-01T00:00:00Z");

        Map<String,Object> results = SelectFunction.apply(selectFunctions, values, eventTimestamp);
        Assertions.assertEquals(10L, results.get("longField"));
        Assertions.assertEquals(32, results.get("renameIntField"));
        Assertions.assertEquals("constantStringValue", results.get("constantStringField"));
        Assertions.assertEquals(32.32, results.get("expressionField"));
        //Assertions.assertEquals("dbcc96aec884f7d5057672df21e7446c1415ca7669fdabac78e49f4d852d5a0a", results.get("hashField"));
        //Assertions.assertEquals("e6c8c04775d64362367b36c61dc00615eabd1c00887fec05ceae4e5ab9cd215b", results.get("hashArrayField"));
        Assertions.assertNotNull(results.get("currentTimestampField"));
        Assertions.assertEquals(eventTimestamp.getMillis() * 1000L, results.get("eventTimestampField"));
        Assertions.assertEquals("stringValue 32 10", results.get("concatField"));
        Assertions.assertEquals(100, results.get("intField"));

        final JsonObject jsonObject = new Gson().fromJson((String)results.get("jsonField"), JsonObject.class);
        Assertions.assertEquals("stringValue", jsonObject.get("stringFieldA").getAsString());
        final JsonObject nestedJsonObject = jsonObject.get("nestedStructField").getAsJsonObject();
        Assertions.assertEquals("2024-08-30T00:00:00Z", nestedJsonObject.get("timestampField").getAsString());
        Assertions.assertEquals("b", nestedJsonObject.get("enumField").getAsString());
        Assertions.assertEquals(10L, results.get("bytesDecodedLongField"));
    }

    private static List<Schema.Field> simpleInputFields() {
        return List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("intField", Schema.FieldType.INT32),
                Schema.Field.of("longField", Schema.FieldType.INT64));
    }

    private static JsonObject json(final String text) {
        return new Gson().fromJson(text, JsonObject.class);
    }

    @Test
    public void testFuncInference() {
        final List<Schema.Field> inputFields = simpleInputFields();

        // name only -> pass
        Assertions.assertInstanceOf(Pass.class,
                SelectFunction.of(json("{ \"name\": \"stringField\" }"), inputFields));
        // field only -> rename
        Assertions.assertInstanceOf(Rename.class,
                SelectFunction.of(json("{ \"name\": \"renamed\", \"field\": \"stringField\" }"), inputFields));
        // field + type -> cast
        Assertions.assertInstanceOf(Cast.class,
                SelectFunction.of(json("{ \"name\": \"casted\", \"field\": \"intField\", \"type\": \"int64\" }"), inputFields));
        // type only (size 2) -> cast
        Assertions.assertInstanceOf(Cast.class,
                SelectFunction.of(json("{ \"name\": \"intField\", \"type\": \"int64\" }"), inputFields));
        // value + type -> constant
        Assertions.assertInstanceOf(Constant.class,
                SelectFunction.of(json("{ \"name\": \"c\", \"value\": \"v\", \"type\": \"string\" }"), inputFields));
        // expression
        Assertions.assertInstanceOf(Expression.class,
                SelectFunction.of(json("{ \"name\": \"e\", \"expression\": \"intField + 1\" }"), inputFields));
        // text
        Assertions.assertInstanceOf(Text.class,
                SelectFunction.of(json("{ \"name\": \"t\", \"text\": \"${stringField}\" }"), inputFields));
        // fields -> struct
        Assertions.assertInstanceOf(Struct.class,
                SelectFunction.of(json("{ \"name\": \"s\", \"fields\": [ { \"name\": \"stringField\" } ] }"), inputFields));
        // op key works like func
        Assertions.assertInstanceOf(Concat.class,
                SelectFunction.of(json("{ \"name\": \"c\", \"op\": \"concat\", \"fields\": [\"stringField\"] }"), inputFields));

        // missing name
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.of(json("{ \"field\": \"stringField\" }"), inputFields));
        // value without type
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.of(json("{ \"name\": \"c\", \"value\": \"v\" }"), inputFields));
        // no func inferable
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.of(json("{ \"name\": \"x\", \"foo\": \"bar\" }"), inputFields));
    }

    @Test
    public void testOfJsonArrayEdgeCases() {
        final List<Schema.Field> inputFields = simpleInputFields();

        // null array
        Assertions.assertTrue(SelectFunction.of((JsonArray) null, inputFields).isEmpty());

        // non-object elements are skipped, ignored functions are excluded
        final JsonArray array = new Gson().fromJson("""
                [
                  "notAnObject",
                  { "name": "stringField" },
                  { "name": "ignored", "field": "stringField", "ignore": true }
                ]
                """, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, inputFields);
        Assertions.assertEquals(1, selectFunctions.size());
        Assertions.assertEquals("stringField", selectFunctions.get(0).getName());
    }

    @Test
    public void testCreateSchemaFlattenErrors() {
        final List<Schema.Field> inputFields = simpleInputFields();
        final JsonArray array = new Gson().fromJson("[ { \"name\": \"longField\" } ]", JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, inputFields);

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.createSchema(selectFunctions, "noSuchField"));
        // flatten field must be array type
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.createSchema(selectFunctions, "longField"));
    }

    @Test
    public void testApplyWithMElement() {
        final List<Schema.Field> inputFields = simpleInputFields();
        final JsonArray array = new Gson().fromJson("""
                [
                  { "name": "renamed", "field": "stringField" },
                  { "name": "concatField", "func": "concat", "delimiter": "-", "fields": ["stringField", "longField"] }
                ]
                """, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(array, inputFields);
        for(final SelectFunction selectFunction : selectFunctions) {
            selectFunction.setup();
        }

        final Instant timestamp = Instant.parse("2024-01-01T00:00:00Z");
        final com.mercari.solution.module.MElement element = com.mercari.solution.module.MElement.builder()
                .withString("stringField", "abc")
                .withInt64("longField", 5L)
                .withEventTime(timestamp)
                .build();

        final Map<String, Object> results = SelectFunction.apply(selectFunctions, element, timestamp);
        Assertions.assertEquals("abc", results.get("renamed"));
        Assertions.assertEquals("abc-5", results.get("concatField"));
    }

    @Test
    public void testIsGroupingAndIsStateful() {
        final List<Schema.Field> inputFields = simpleInputFields();

        Assertions.assertFalse(SelectFunction.isGrouping(null));
        Assertions.assertFalse(SelectFunction.isGrouping(List.of()));
        Assertions.assertFalse(SelectFunction.isStateful(null));
        Assertions.assertFalse(SelectFunction.isStateful(List.of()));

        final JsonArray statelessArray = new Gson().fromJson("[ { \"name\": \"stringField\" } ]", JsonArray.class);
        final List<SelectFunction> stateless = SelectFunction.of(statelessArray, inputFields);
        Assertions.assertFalse(SelectFunction.isGrouping(stateless));
        Assertions.assertFalse(SelectFunction.isStateful(stateless));

        final JsonArray statefulArray = new Gson().fromJson(
                "[ { \"name\": \"sumLongField\", \"func\": \"sum\", \"field\": \"longField\" } ]", JsonArray.class);
        final List<SelectFunction> stateful = SelectFunction.of(statefulArray, inputFields);
        Assertions.assertTrue(SelectFunction.isGrouping(stateful));
        Assertions.assertTrue(SelectFunction.isStateful(stateful));
    }

    @Test
    public void testGetStringParameter() {
        final JsonObject jsonObject = json("{ \"str\": \"value\", \"obj\": { \"a\": 1 } }");
        Assertions.assertEquals("value", SelectFunction.getStringParameter("n", jsonObject, "str", "default"));
        Assertions.assertEquals("default", SelectFunction.getStringParameter("n", jsonObject, "missing", "default"));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.getStringParameter("n", jsonObject, "obj", "default"));
    }

    @Test
    public void testEventTimestamp() {
        final Instant timestamp = Instant.parse("2024-01-01T00:00:00Z");

        // without duration: returns event timestamp as epoch micros
        final SelectFunction plain = SelectFunction.of(
                json("{ \"name\": \"et\", \"func\": \"event_timestamp\" }"), simpleInputFields());
        plain.setup();
        Assertions.assertEquals(timestamp.getMillis() * 1000L, plain.apply(new HashMap<>(), timestamp));
        Assertions.assertEquals(Schema.Type.timestamp, plain.getOutputFieldType().getType());

        // with duration: timestamp is shifted
        final SelectFunction shifted = SelectFunction.of(
                json("{ \"name\": \"et\", \"func\": \"event_timestamp\", \"duration\": 1, \"durationUnit\": \"minute\" }"),
                simpleInputFields());
        shifted.setup();
        Assertions.assertEquals(
                timestamp.plus(org.joda.time.Duration.standardMinutes(1)).getMillis() * 1000L,
                shifted.apply(new HashMap<>(), timestamp));

        // duration without durationUnit is invalid
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.of(json("{ \"name\": \"et\", \"func\": \"event_timestamp\", \"duration\": 1 }"), simpleInputFields()));

        // with cutoff: timestamp is truncated (floored) to the specified time unit
        final SelectFunction cutoff = SelectFunction.of(
                json("{ \"name\": \"et\", \"func\": \"event_timestamp\", \"cutoff\": \"minute\" }"),
                simpleInputFields());
        cutoff.setup();
        Assertions.assertEquals(
                Instant.parse("2024-01-01T00:03:00Z").getMillis() * 1000L,
                cutoff.apply(new HashMap<>(), Instant.parse("2024-01-01T00:03:45.678Z")));

        // cutoff is applied after the duration shift
        final SelectFunction shiftedCutoff = SelectFunction.of(
                json("{ \"name\": \"et\", \"func\": \"event_timestamp\", \"duration\": 1, \"durationUnit\": \"minute\", \"cutoff\": \"hour\" }"),
                simpleInputFields());
        shiftedCutoff.setup();
        Assertions.assertEquals(
                Instant.parse("2024-01-01T01:00:00Z").getMillis() * 1000L,
                shiftedCutoff.apply(new HashMap<>(), Instant.parse("2024-01-01T00:59:30Z")));
    }

    @Test
    public void testUuid() {
        final Instant timestamp = Instant.parse("2024-01-01T00:00:00Z");

        final SelectFunction uuid = SelectFunction.of(
                json("{ \"name\": \"u\", \"func\": \"uuid\" }"), simpleInputFields());
        uuid.setup();
        final Object value1 = uuid.apply(new HashMap<>(), timestamp);
        final Object value2 = uuid.apply(new HashMap<>(), timestamp);
        Assertions.assertEquals(36, ((String) value1).length());
        Assertions.assertNotEquals(value1, value2);

        final SelectFunction sized = SelectFunction.of(
                json("{ \"name\": \"u\", \"func\": \"uuid\", \"size\": 8 }"), simpleInputFields());
        sized.setup();
        Assertions.assertEquals(8, ((String) sized.apply(new HashMap<>(), timestamp)).length());

        final SelectFunction oversized = SelectFunction.of(
                json("{ \"name\": \"u\", \"func\": \"uuid\", \"size\": 100 }"), simpleInputFields());
        oversized.setup();
        Assertions.assertEquals(36, ((String) oversized.apply(new HashMap<>(), timestamp)).length());
    }

    @Test
    public void testCurrentTimestamp() {
        final SelectFunction current = SelectFunction.of(
                json("{ \"name\": \"c\", \"func\": \"current_timestamp\" }"), simpleInputFields());
        current.setup();
        final long before = Instant.now().getMillis() * 1000L;
        final Object value = current.apply(new HashMap<>(), Instant.parse("2024-01-01T00:00:00Z"));
        final long after = Instant.now().getMillis() * 1000L;
        Assertions.assertInstanceOf(Long.class, value);
        Assertions.assertTrue((Long) value >= before && (Long) value <= after);
    }

    @Test
    public void testCastValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Cast.of("c", json("{ \"name\": \"c\", \"field\": \"stringField\" }"), simpleInputFields(), false));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Cast.of("c", json("{ \"name\": \"c\", \"field\": [\"a\"], \"type\": \"string\" }"), simpleInputFields(), false));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.of(json("{ \"name\": \"c\", \"field\": \"noSuchField\", \"type\": \"string\" }"), simpleInputFields()));
    }

}
