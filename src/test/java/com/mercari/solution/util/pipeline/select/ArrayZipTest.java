package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class ArrayZipTest {

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("a", Schema.FieldType.array(Schema.FieldType.INT64)),
            Schema.Field.of("b", Schema.FieldType.array(Schema.FieldType.STRING)),
            Schema.Field.of("s", Schema.FieldType.STRING),
            Schema.Field.of("m", Schema.FieldType.matrix(Schema.FieldType.FLOAT32, List.of(2, 2))),
            Schema.Field.of("nested", Schema.FieldType.element(List.of(
                    Schema.Field.of("a", Schema.FieldType.array(Schema.FieldType.FLOAT32))))));

    private static SelectFunction function(final String fields) {
        final String json = "{ \"name\": \"o\", \"func\": \"array_zip\", \"fields\": " + fields + " }";
        final SelectFunction function = SelectFunction.of(new Gson().fromJson(json, JsonObject.class), FIELDS);
        function.setup();
        return function;
    }

    @Test
    public void testZip() {
        final SelectFunction function = function("[\"a\", \"b\"]");
        final Schema.FieldType type = function.getOutputFieldType().getArrayValueType();
        Assertions.assertEquals(List.of("a", "b"), type.getElementSchema().getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(Schema.Type.int64, type.getElementSchema().getField("a").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, type.getElementSchema().getField("b").getFieldType().getType());

        final Map<String, Object> input = new HashMap<>();
        input.put("a", List.of(1L, 2L));
        input.put("b", List.of("x", "y"));
        Assertions.assertEquals(List.of(Map.of("a", 1L, "b", "x"), Map.of("a", 2L, "b", "y")), function.apply(input, Instant.EPOCH));
    }

    // a nested field is named by its last segment, or explicitly
    @Test
    public void testNames() {
        final Map<String, Object> input = new HashMap<>();
        input.put("nested", Map.of("a", List.of(1f)));
        input.put("b", List.of("x"));
        Assertions.assertEquals(List.of(Map.of("a", 1f, "b", "x")), function("[\"nested.a\", \"b\"]").apply(input, Instant.EPOCH));

        input.put("a", List.of(2L));
        final SelectFunction named = function("[\"a\", { \"name\": \"c\", \"field\": \"nested.a\" }]");
        Assertions.assertEquals(List.of(Map.of("a", 2L, "c", 1f)), named.apply(input, Instant.EPOCH));
        Assertions.assertEquals(List.of("a", "nested"), named.getInputFields().stream().map(Schema.Field::getName).toList());

        // fields of one parent read it once
        final SelectFunction sameParent = function("[\"nested.a\", { \"name\": \"c\", \"field\": \"nested.a\" }]");
        Assertions.assertEquals(List.of("nested"), sameParent.getInputFields().stream().map(Schema.Field::getName).toList());
    }

    @Test
    public void testValues() {
        final SelectFunction function = function("[\"a\", \"b\"]");
        final Map<String, Object> input = new HashMap<>();
        input.put("a", List.of(1L));
        // a null array gives null, also when the other has a value
        input.put("b", null);
        Assertions.assertNull(function.apply(input, Instant.EPOCH));
        // an empty array is an empty result
        input.put("a", List.of());
        input.put("b", List.of());
        Assertions.assertEquals(List.of(), function.apply(input, Instant.EPOCH));
        // different lengths fail the record
        input.put("a", List.of(1L));
        final Exception e = Assertions.assertThrows(IllegalArgumentException.class, () -> function.apply(input, Instant.EPOCH));
        Assertions.assertTrue(e.getMessage().contains("same length"), e.getMessage());
    }

    @Test
    public void testConfigErrors() {
        // no field, not an array, a matrix is flat so its rows can not be zipped, the same name twice, a bad item, unknown
        for(final String fields : List.of("[]", "[\"s\"]", "[\"m\"]", "[\"a\", \"nested.a\"]", "[{ \"name\": \"c\" }]", "[\"nothing\"]")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function(fields), fields);
        }
    }

}
