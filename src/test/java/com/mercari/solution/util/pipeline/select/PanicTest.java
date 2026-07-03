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

public class PanicTest {

    private static final Instant TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static SelectFunction create(final String json, final List<Schema.Field> inputFields) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        final SelectFunction selectFunction = SelectFunction.of(jsonObject, inputFields);
        selectFunction.setup();
        return selectFunction;
    }

    private static List<Schema.Field> inputFields() {
        return List.of(Schema.Field.of("intField", Schema.FieldType.INT32));
    }

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"p\", \"func\": \"panic\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\", \"rate\": 1.5 }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\", \"rate\": -0.1 }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\", \"rate\": \"abc\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\", \"rate\": [0.5] }", inputFields()));
    }

    @Test
    public void testAlwaysPanicWithoutCondition() {
        final SelectFunction panic = create(
                "{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\" }", inputFields());

        Assertions.assertEquals("p", panic.getName());
        Assertions.assertEquals(Schema.Type.float64, panic.getOutputFieldType().getType());
        Assertions.assertTrue(panic.getInputFields().isEmpty());

        final Map<String, Object> input = new HashMap<>();
        input.put("intField", 1);
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> panic.apply(input, TIMESTAMP));
        Assertions.assertTrue(e.getMessage().contains("boom"));
    }

    @Test
    public void testConditionNotMatched() {
        final SelectFunction panic = create(
                "{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\", \"condition\": { \"key\": \"intField\", \"op\": \"=\", \"value\": 1 } }",
                inputFields());

        final Map<String, Object> input = new HashMap<>();
        input.put("intField", 2);
        Assertions.assertEquals(-1D, panic.apply(input, TIMESTAMP));
    }

    @Test
    public void testConditionMatched() {
        final SelectFunction panic = create(
                "{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\", \"condition\": { \"key\": \"intField\", \"op\": \"=\", \"value\": 1 }, \"rate\": 1.0 }",
                inputFields());

        final Map<String, Object> input = new HashMap<>();
        input.put("intField", 1);
        Assertions.assertThrows(RuntimeException.class, () -> panic.apply(input, TIMESTAMP));
    }

    @Test
    public void testZeroRateAlmostNeverPanics() {
        final SelectFunction panic = create(
                "{ \"name\": \"p\", \"func\": \"panic\", \"message\": \"boom\", \"rate\": 0.0 }", inputFields());

        final Map<String, Object> input = new HashMap<>();
        input.put("intField", 1);
        // nextDouble() is in [0, 1); with rate 0.0 a panic requires exactly 0.0
        for (int i = 0; i < 100; i++) {
            final Object value = panic.apply(input, TIMESTAMP);
            Assertions.assertInstanceOf(Double.class, value);
            Assertions.assertTrue((Double) value > 0D);
        }
    }

}
