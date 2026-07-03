package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonPathTest {

    private static final Instant TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static SelectFunction create(final String json, final List<Schema.Field> inputFields) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        final SelectFunction selectFunction = SelectFunction.of(jsonObject, inputFields);
        selectFunction.setup();
        return selectFunction;
    }

    private static List<Schema.Field> inputFields() {
        return List.of(Schema.Field.of("jsonField", Schema.FieldType.JSON));
    }

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"j\", \"func\": \"json_path\", \"path\": \"$.a\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"j\", \"func\": \"json_path\", \"field\": \"jsonField\" }", inputFields()));
        // type element requires fields parameter
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"j\", \"func\": \"json_path\", \"field\": \"jsonField\", \"path\": \"$.a\", \"type\": \"element\" }", inputFields()));
    }

    @Test
    public void testDefaultStringOutput() {
        final SelectFunction jsonPath = create(
                "{ \"name\": \"j\", \"func\": \"json_path\", \"field\": \"jsonField\", \"path\": \"$.a.b\" }",
                inputFields());

        Assertions.assertEquals(Schema.Type.string, jsonPath.getOutputFieldType().getType());
        Assertions.assertTrue(jsonPath.getOutputFieldType().getNullable());

        final Map<String, Object> input = new HashMap<>();
        input.put("jsonField", "{ \"a\": { \"b\": \"hello\" } }");
        Assertions.assertEquals("hello", jsonPath.apply(input, TIMESTAMP));
    }

    @Test
    public void testNumberOutput() {
        final SelectFunction jsonPath = create(
                "{ \"name\": \"j\", \"func\": \"json_path\", \"field\": \"jsonField\", \"path\": \"$.n\", \"type\": \"int64\" }",
                inputFields());

        final Map<String, Object> input = new HashMap<>();
        input.put("jsonField", "{ \"n\": 42 }");
        final Object output = jsonPath.apply(input, TIMESTAMP);
        Assertions.assertInstanceOf(Number.class, output);
        Assertions.assertEquals(42L, ((Number) output).longValue());
    }

    @Test
    public void testRepeatedMode() {
        final SelectFunction jsonPath = create(
                "{ \"name\": \"j\", \"func\": \"json_path\", \"field\": \"jsonField\", \"path\": \"$.items[*]\", \"type\": \"string\", \"mode\": \"repeated\" }",
                inputFields());

        Assertions.assertEquals(Schema.Type.array, jsonPath.getOutputFieldType().getType());

        final Map<String, Object> input = new HashMap<>();
        input.put("jsonField", "{ \"items\": [\"a\", \"b\", \"c\"] }");
        Assertions.assertEquals(List.of("a", "b", "c"), jsonPath.apply(input, TIMESTAMP));
    }

    @Test
    public void testElementOutput() {
        final SelectFunction jsonPath = create("""
                        { "name": "j", "func": "json_path", "field": "jsonField", "path": "$.obj", "type": "element",
                          "fields": [
                            { "name": "x", "type": "string" },
                            { "name": "y", "type": "int64" }
                          ] }
                        """,
                inputFields());

        Assertions.assertEquals(Schema.Type.element, jsonPath.getOutputFieldType().getType());

        final Map<String, Object> input = new HashMap<>();
        input.put("jsonField", "{ \"obj\": { \"x\": \"v\", \"y\": 2, \"z\": \"ignored\" } }");
        final Object output = jsonPath.apply(input, TIMESTAMP);
        Assertions.assertInstanceOf(Map.class, output);
        final Map<?, ?> map = (Map<?, ?>) output;
        Assertions.assertEquals("v", map.get("x"));
        Assertions.assertEquals(2, ((Number) map.get("y")).intValue());
        Assertions.assertFalse(map.containsKey("z"));
    }

    @Test
    public void testRequiredMode() {
        final SelectFunction jsonPath = create(
                "{ \"name\": \"j\", \"func\": \"json_path\", \"field\": \"jsonField\", \"path\": \"$.a\", \"type\": \"string\", \"mode\": \"required\" }",
                inputFields());
        Assertions.assertFalse(jsonPath.getOutputFieldType().getNullable());
    }

    @Test
    public void testMissingPathReturnsNull() {
        final SelectFunction jsonPath = create(
                "{ \"name\": \"j\", \"func\": \"json_path\", \"field\": \"jsonField\", \"path\": \"$.missing.path\" }",
                inputFields());

        final Map<String, Object> input = new HashMap<>();
        input.put("jsonField", "{ \"a\": 1 }");
        Assertions.assertNull(jsonPath.apply(input, TIMESTAMP));

        // broken json also yields null
        input.put("jsonField", "{ broken");
        Assertions.assertNull(jsonPath.apply(input, TIMESTAMP));
    }

    @Test
    public void testJsonExtract() {
        // NOTE: JsonExtract.of currently returns a JsonPath instance (JsonExtract itself is dead code)
        final JsonObject jsonObject = new Gson().fromJson(
                "{ \"name\": \"j\", \"field\": \"jsonField\", \"path\": \"$.a\" }", JsonObject.class);
        final SelectFunction extract = JsonExtract.of("j", jsonObject, inputFields(), false);
        Assertions.assertInstanceOf(JsonPath.class, extract);
        extract.setup();

        final Map<String, Object> input = new HashMap<>();
        input.put("jsonField", "{ \"a\": \"value\" }");
        Assertions.assertEquals("value", extract.apply(input, TIMESTAMP));

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                JsonExtract.of("j", new Gson().fromJson("{ \"name\": \"j\", \"path\": \"$.a\" }", JsonObject.class), inputFields(), false));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                JsonExtract.of("j", new Gson().fromJson("{ \"name\": \"j\", \"field\": \"jsonField\" }", JsonObject.class), inputFields(), false));
    }

}
