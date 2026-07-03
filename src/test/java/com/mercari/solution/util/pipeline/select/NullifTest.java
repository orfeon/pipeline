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

public class NullifTest {

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
                Schema.Field.of("intField", Schema.FieldType.INT32));
    }

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"n\", \"func\": \"nullif\", \"field\": \"stringField\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"n\", \"func\": \"nullif\", \"field\": [\"stringField\"], \"condition\": { \"key\": \"intField\", \"op\": \"=\", \"value\": 0 } }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"n\", \"func\": \"nullif\", \"field\": \"noSuchField\", \"condition\": { \"key\": \"intField\", \"op\": \"=\", \"value\": 0 } }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"n\", \"func\": \"nullif\", \"field\": \"stringField\", \"condition\": { \"key\": \"noSuchField\", \"op\": \"=\", \"value\": 0 } }", inputFields()));
    }

    @Test
    public void testNullifMatched() {
        final SelectFunction nullif = create(
                "{ \"name\": \"n\", \"func\": \"nullif\", \"field\": \"stringField\", \"condition\": { \"key\": \"intField\", \"op\": \"=\", \"value\": 0 } }",
                inputFields());

        Assertions.assertEquals("n", nullif.getName());
        Assertions.assertEquals(Schema.Type.string, nullif.getOutputFieldType().getType());
        // input fields contain both target field and condition variables
        Assertions.assertEquals(2, nullif.getInputFields().size());

        final Map<String, Object> input = new HashMap<>();
        input.put("stringField", "value");
        input.put("intField", 0);
        Assertions.assertNull(nullif.apply(input, TIMESTAMP));

        input.put("intField", 1);
        Assertions.assertEquals("value", nullif.apply(input, TIMESTAMP));
    }

    @Test
    public void testDefaultFieldIsName() {
        final SelectFunction nullif = create(
                "{ \"name\": \"stringField\", \"func\": \"nullif\", \"condition\": { \"key\": \"intField\", \"op\": \">\", \"value\": 10 } }",
                inputFields());

        final Map<String, Object> input = new HashMap<>();
        input.put("stringField", "value");
        input.put("intField", 11);
        Assertions.assertNull(nullif.apply(input, TIMESTAMP));

        input.put("intField", 10);
        Assertions.assertEquals("value", nullif.apply(input, TIMESTAMP));
    }

}
