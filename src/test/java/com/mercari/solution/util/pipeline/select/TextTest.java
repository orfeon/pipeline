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

public class TextTest {

    private static final Instant TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static SelectFunction create(final String json, final List<Schema.Field> inputFields) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        final SelectFunction selectFunction = SelectFunction.of(jsonObject, inputFields);
        selectFunction.setup();
        return selectFunction;
    }

    private static List<Schema.Field> inputFields() {
        return List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64));
    }

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"t\", \"func\": \"text\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"t\", \"func\": \"text\", \"text\": [\"a\"] }", inputFields()));
    }

    @Test
    public void testTemplateText() {
        final SelectFunction text = create(
                "{ \"name\": \"t\", \"func\": \"text\", \"text\": \"hello ${stringField}\" }", inputFields());

        Assertions.assertEquals("t", text.getName());
        Assertions.assertEquals(Schema.Type.string, text.getOutputFieldType().getType());
        Assertions.assertEquals(1, text.getInputFields().size());
        Assertions.assertEquals("stringField", text.getInputFields().get(0).getName());

        final Map<String, Object> input = new HashMap<>();
        input.put("stringField", "world");
        Assertions.assertEquals("hello world", text.apply(input, TIMESTAMP));
    }

    @Test
    public void testValueParameter() {
        final SelectFunction text = create(
                "{ \"name\": \"t\", \"func\": \"text\", \"value\": \"plain\" }", inputFields());
        Assertions.assertTrue(text.getInputFields().isEmpty());
        Assertions.assertEquals("plain", text.apply(new HashMap<>(), TIMESTAMP));
    }

    @Test
    public void testTypedOutput() {
        final SelectFunction text = create(
                "{ \"name\": \"t\", \"func\": \"text\", \"text\": \"${longField}\", \"type\": \"int64\" }", inputFields());

        Assertions.assertEquals(Schema.Type.int64, text.getOutputFieldType().getType());

        final Map<String, Object> input = new HashMap<>();
        input.put("longField", 10L);
        Assertions.assertEquals(10L, text.apply(input, TIMESTAMP));
    }

}
