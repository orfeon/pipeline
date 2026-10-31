package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class ArrayFunctionsTest {

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("a", Schema.FieldType.INT64),
            Schema.Field.of("b", Schema.FieldType.INT64),
            Schema.Field.of("text", Schema.FieldType.STRING),
            Schema.Field.of("values", Schema.FieldType.array(Schema.FieldType.INT64)),
            Schema.Field.of("reals", Schema.FieldType.array(Schema.FieldType.FLOAT32)),
            Schema.Field.of("m", Schema.FieldType.matrix(Schema.FieldType.FLOAT32, List.of(2, 2))),
            Schema.Field.of("nested", Schema.FieldType.element(List.of(Schema.Field.of("ids", Schema.FieldType.array(Schema.FieldType.INT64))))));

    private static SelectFunction function(final String json) {
        final SelectFunction function = SelectFunction.of(new Gson().fromJson(json, JsonObject.class), FIELDS);
        function.setup();
        return function;
    }

    private static Object apply(final String json, final Map<String, Object> input) {
        return function(json).apply(input, Instant.EPOCH);
    }

    @Test
    public void testArray() {
        final String json = "{\"name\": \"o\", \"func\": \"array\", \"fields\": [\"a\", \"b\"]}";
        final SelectFunction function = function(json);
        Assertions.assertEquals(Schema.Type.array, function.getOutputFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, function.getOutputFieldType().getArrayValueType().getType());
        Assertions.assertEquals(List.of(1L, 2L), function.apply(Map.of("a", 1L, "b", 2L), Instant.EPOCH));
        // a null stays: a model input with it fails its record
        final Map<String, Object> nulls = new HashMap<>();
        nulls.put("a", 1L);
        nulls.put("b", null);
        Assertions.assertEquals(Arrays.asList(1L, null), function.apply(nulls, Instant.EPOCH));
    }

    @Test
    public void testAppend() {
        final List<Long> values = List.of(1L);
        final Map<String, Object> input = new HashMap<>();
        input.put("values", values);
        input.put("a", 2);
        Assertions.assertEquals(List.of(1L, 2L), apply("{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"values\", \"valueField\": \"a\"}", input), "as the values of the array");
        Assertions.assertEquals(List.of(1L, 9L), apply("{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"values\", \"value\": 9}", input));
        Assertions.assertEquals(List.of(1L), values);

        final Map<String, Object> tensor = new HashMap<>();
        tensor.put("reals", Tensor.of(new float[]{1f, 2f}, new long[]{2}));
        tensor.put("a", 3L);
        Assertions.assertEquals(List.of(1f, 2f, 3f), apply("{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"reals\", \"valueField\": \"a\"}", tensor));

        final Map<String, Object> nothing = new HashMap<>();
        nothing.put("values", null);
        Assertions.assertNull(apply("{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"values\", \"value\": 9}", nothing));
    }

    @Test
    public void testRange() {
        final Map<String, Object> input = new HashMap<>();
        input.put("a", 3L);
        input.put("values", List.of(5L, 6L));
        input.put("nested", Map.of("ids", List.of(7L)));
        input.put("text", null);
        Assertions.assertEquals(List.of(0L, 1L, 2L), apply("{\"name\": \"o\", \"func\": \"range\", \"field\": \"a\"}", input));
        Assertions.assertEquals(List.of(0L, 1L), apply("{\"name\": \"o\", \"func\": \"range\", \"field\": \"values\"}", input));
        Assertions.assertEquals(List.of(0L), apply("{\"name\": \"o\", \"func\": \"range\", \"field\": \"nested.ids\"}", input));
        Assertions.assertEquals(List.of(2L), apply("{\"name\": \"o\", \"func\": \"range\", \"field\": \"a\", \"last\": 1}", input));
        Assertions.assertEquals(List.of(0L, 1L, 2L), apply("{\"name\": \"o\", \"func\": \"range\", \"field\": \"a\", \"last\": 9}", input));
        input.put("a", 0L);
        Assertions.assertEquals(List.of(), apply("{\"name\": \"o\", \"func\": \"range\", \"field\": \"a\", \"last\": 1}", input));
    }

    @Test
    public void testConstantArray() {
        final SelectFunction function = function("{\"name\": \"o\", \"value\": [], \"type\": \"array<int64>\"}");
        Assertions.assertEquals(Schema.Type.array, function.getOutputFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, function.getOutputFieldType().getArrayValueType().getType());
        Assertions.assertEquals(List.of(), function.apply(Map.of(), Instant.EPOCH));
        Assertions.assertEquals(List.of(1L, 2L), function("{\"name\": \"o\", \"value\": [1, 2], \"type\": \"array<int64>\"}").apply(Map.of(), Instant.EPOCH));
    }

    @Test
    public void testConfigErrors() {
        for(final String json : List.of(
                "{\"name\": \"o\", \"func\": \"array\"}",
                "{\"name\": \"o\", \"func\": \"array\", \"fields\": []}",
                "{\"name\": \"o\", \"func\": \"array\", \"fields\": [\"a\", \"text\"]}",
                "{\"name\": \"o\", \"func\": \"array_append\", \"value\": 1}",
                "{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"values\"}",
                "{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"values\", \"value\": 1, \"valueField\": \"a\"}",
                "{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"a\", \"value\": 1}",
                "{\"name\": \"o\", \"func\": \"array_append\", \"field\": \"values\", \"valueField\": \"text\"}",
                "{\"name\": \"o\", \"func\": \"range\"}",
                "{\"name\": \"o\", \"func\": \"range\", \"field\": \"text\"}",
                "{\"name\": \"o\", \"func\": \"range\", \"field\": \"a\", \"last\": 0}")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function(json), json);
        }
    }

}
