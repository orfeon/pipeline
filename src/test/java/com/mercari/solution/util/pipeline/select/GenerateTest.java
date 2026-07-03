package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateTest {

    private static final Instant TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static SelectFunction create(final String json, final List<Schema.Field> inputFields) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        final SelectFunction selectFunction = SelectFunction.of(jsonObject, inputFields);
        selectFunction.setup();
        return selectFunction;
    }

    @Test
    public void testValidation() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("intField", Schema.FieldType.INT32));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"g\", \"func\": \"generate\", \"to\": \"10\", \"type\": \"int64\" }", inputFields));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"0\", \"type\": \"int64\" }", inputFields));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"0\", \"to\": \"10\" }", inputFields));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"0\", \"to\": \"10\", \"type\": \"int64\", \"interval\": \"abc\" }", inputFields));
    }

    @Test
    public void testGenerateInt64() {
        final SelectFunction generate = create(
                "{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"0\", \"to\": \"10\", \"interval\": 2, \"type\": \"int64\" }",
                List.of());

        Assertions.assertEquals("g", generate.getName());
        Assertions.assertEquals(Schema.Type.array, generate.getOutputFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, generate.getOutputFieldType().getArrayValueType().getType());

        final Object output = generate.apply(new HashMap<>(), TIMESTAMP);
        Assertions.assertEquals(List.of(0L, 2L, 4L, 6L, 8L), output);

        Assertions.assertNull(generate.apply(null, TIMESTAMP));
    }

    @Test
    public void testGenerateInt32WithExpression() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("intField", Schema.FieldType.INT32));
        final SelectFunction generate = create(
                "{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"intField\", \"to\": \"intField * 3\", \"type\": \"int32\" }",
                inputFields);

        final Map<String, Object> input = new HashMap<>();
        input.put("intField", 2);
        final Object output = generate.apply(input, TIMESTAMP);
        Assertions.assertEquals(List.of(2, 3, 4, 5), output);
    }

    @Test
    public void testGenerateFloat64() {
        final SelectFunction generate = create(
                "{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"0\", \"to\": \"3\", \"type\": \"float64\" }",
                List.of());
        final Object output = generate.apply(new HashMap<>(), TIMESTAMP);
        Assertions.assertEquals(List.of(0.0D, 1.0D, 2.0D), output);
    }

    @Test
    public void testGenerateTimestamp() {
        final SelectFunction generate = create(
                "{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"2024-01-01T00:00:00Z\", \"to\": \"2024-01-01T00:03:00Z\", \"type\": \"timestamp\" }",
                List.of());

        final Object output = generate.apply(new HashMap<>(), TIMESTAMP);
        final long base = Instant.parse("2024-01-01T00:00:00Z").getMillis() * 1000L;
        Assertions.assertEquals(List.of(base, base + 60_000_000L, base + 120_000_000L), output);
    }

    @Test
    public void testGenerateDateWithTemplate() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("beginDate", Schema.FieldType.STRING),
                Schema.Field.of("endDate", Schema.FieldType.STRING));
        final SelectFunction generate = create(
                "{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"${beginDate}\", \"to\": \"${endDate}\", \"type\": \"date\" }",
                inputFields);

        final Map<String, Object> input = new HashMap<>();
        input.put("beginDate", "2024-01-01");
        input.put("endDate", "2024-01-05");
        final Object output = generate.apply(input, TIMESTAMP);

        final int epochDay = Long.valueOf(LocalDate.of(2024, 1, 1).toEpochDay()).intValue();
        Assertions.assertEquals(List.of(epochDay, epochDay + 1, epochDay + 2, epochDay + 3), output);
    }

    @Test
    public void testGenerateTime() {
        final SelectFunction generate = create(
                "{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"00:00:00\", \"to\": \"01:00:00\", \"interval\": 30, \"type\": \"time\" }",
                List.of());

        final Object output = generate.apply(new HashMap<>(), TIMESTAMP);
        // TIME values are micros-of-day
        Assertions.assertEquals(List.of(0L, 1_800_000_000L), output);
    }

    @Test
    public void testGenerateTimeWithIntervalUnit() {
        final SelectFunction generate = create(
                "{ \"name\": \"g\", \"func\": \"generate\", \"from\": \"00:00:00\", \"to\": \"03:00:00\", \"intervalUnit\": \"hour\", \"type\": \"time\" }",
                List.of());

        final Object output = generate.apply(new HashMap<>(), TIMESTAMP);
        Assertions.assertEquals(List.of(0L, 3_600_000_000L, 7_200_000_000L), output);
    }

}
