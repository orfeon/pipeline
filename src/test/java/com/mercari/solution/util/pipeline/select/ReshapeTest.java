package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReshapeTest {

    private static final Instant TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static SelectFunction create(final String json, final List<Schema.Field> inputFields) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        final SelectFunction selectFunction = SelectFunction.of(jsonObject, inputFields);
        selectFunction.setup();
        return selectFunction;
    }

    private static List<Schema.Field> flatInputFields() {
        return List.of(
                Schema.Field.of("vec", Schema.FieldType.array(Schema.FieldType.FLOAT64)));
    }

    @Test
    public void testValidation() {
        final List<Schema.Field> inputFields = flatInputFields();
        Assertions.assertThrows(IllegalModuleException.class, () ->
                create("{ \"name\": \"r\", \"func\": \"reshape\", \"shape\": [2,3] }", inputFields));
        Assertions.assertThrows(IllegalModuleException.class, () ->
                create("{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"vec\" }", inputFields));
        Assertions.assertThrows(IllegalModuleException.class, () ->
                create("{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"vec\", \"shape\": 6 }", inputFields));
        // more indices than shape dims
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"vec\", \"shape\": [2,3], \"indices\": [0,0,0] }", inputFields));
    }

    @Test
    public void testNoIndices() {
        final SelectFunction reshape = create(
                "{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"vec\", \"shape\": [2,3] }",
                flatInputFields());

        Assertions.assertEquals("r", reshape.getName());
        Assertions.assertFalse(reshape.ignore());
        final Schema.FieldType outputFieldType = reshape.getOutputFieldType();
        Assertions.assertEquals(Schema.Type.matrix, outputFieldType.getType());
        Assertions.assertEquals(List.of(2, 3), outputFieldType.getShape());
        Assertions.assertEquals(Schema.Type.float64, outputFieldType.getMatrixValueType().getType());

        final Map<String, Object> input = new HashMap<>();
        input.put("vec", List.of(1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D));
        final Object output = reshape.apply(input, TIMESTAMP);
        Assertions.assertEquals(List.of(1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D), output);

        // null input value
        final Map<String, Object> nullInput = new HashMap<>();
        nullInput.put("vec", null);
        Assertions.assertNull(reshape.apply(nullInput, TIMESTAMP));
    }

    @Test
    public void testPartialIndices() {
        final SelectFunction reshape = create(
                "{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"vec\", \"shape\": [2,3], \"indices\": [1] }",
                flatInputFields());

        final Schema.FieldType outputFieldType = reshape.getOutputFieldType();
        Assertions.assertEquals(Schema.Type.matrix, outputFieldType.getType());
        // remaining shape must be the trailing dims [3], not the leading dims [2]
        Assertions.assertEquals(List.of(3), outputFieldType.getShape());

        final Map<String, Object> input = new HashMap<>();
        input.put("vec", List.of(1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D));
        final Object output = reshape.apply(input, TIMESTAMP);
        Assertions.assertEquals(List.of(4.0D, 5.0D, 6.0D), output);
    }

    @Test
    public void testFullIndices() {
        final SelectFunction reshape = create(
                "{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"vec\", \"shape\": [2,3], \"indices\": [1,2] }",
                flatInputFields());

        Assertions.assertEquals(Schema.Type.float64, reshape.getOutputFieldType().getType());

        final Map<String, Object> input = new HashMap<>();
        input.put("vec", List.of(1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D));
        final Object output = reshape.apply(input, TIMESTAMP);
        Assertions.assertEquals(6.0D, output);
    }

    @Test
    public void testNestedField() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("nested", Schema.FieldType.element(Schema.builder()
                        .withField("arr", Schema.FieldType.array(Schema.FieldType.FLOAT64))
                        .build())));

        final SelectFunction reshape = create(
                "{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"nested.arr\", \"shape\": [2,2], \"indices\": [1] }",
                inputFields);

        final Map<String, Object> nested = new HashMap<>();
        nested.put("arr", List.of(1.0D, 2.0D, 3.0D, 4.0D));
        final Map<String, Object> input = new HashMap<>();
        input.put("nested", nested);

        final Object output = reshape.apply(input, TIMESTAMP);
        Assertions.assertEquals(List.of(3.0D, 4.0D), output);
    }

    @Test
    public void testMatrixInputField() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("mat", Schema.FieldType.matrix(Schema.FieldType.FLOAT64, List.of(6))));

        final SelectFunction reshape = create(
                "{ \"name\": \"r\", \"func\": \"reshape\", \"field\": \"mat\", \"shape\": [3,2], \"indices\": [2] }",
                inputFields);

        final Schema.FieldType outputFieldType = reshape.getOutputFieldType();
        Assertions.assertEquals(Schema.Type.matrix, outputFieldType.getType());
        Assertions.assertEquals(List.of(2), outputFieldType.getShape());

        final Map<String, Object> input = new HashMap<>();
        input.put("mat", List.of(1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D));
        final Object output = reshape.apply(input, TIMESTAMP);
        Assertions.assertEquals(List.of(5.0D, 6.0D), output);
    }

}
