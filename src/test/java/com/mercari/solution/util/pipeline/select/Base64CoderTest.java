package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Base64CoderTest {

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
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("bytesField", Schema.FieldType.BYTES));
    }

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"b\", \"func\": \"base64_encode\", \"field\": \"stringField\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"b\", \"func\": \"base64_encode\", \"field\": [\"stringField\"], \"type\": \"string\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"b\", \"func\": \"base64_decode\", \"field\": \"noSuchField\", \"type\": \"string\" }", inputFields()));
    }

    @Test
    public void testEncodeStringToString() {
        final SelectFunction encode = create(
                "{ \"name\": \"b\", \"func\": \"base64_encode\", \"field\": \"stringField\", \"type\": \"string\" }",
                inputFields());

        Assertions.assertEquals(Schema.Type.string, encode.getOutputFieldType().getType());

        final Map<String, Object> input = new HashMap<>();
        input.put("stringField", "hello");
        Assertions.assertEquals("aGVsbG8=", encode.apply(input, TIMESTAMP));

        // null input
        Assertions.assertNull(encode.apply(new HashMap<>(), TIMESTAMP));
    }

    @Test
    public void testEncodeNumberToString() {
        final SelectFunction encode = create(
                "{ \"name\": \"b\", \"func\": \"base64_encode\", \"field\": \"longField\", \"type\": \"string\" }",
                inputFields());
        final Map<String, Object> input = new HashMap<>();
        input.put("longField", 123L);
        Assertions.assertEquals("MTIz", encode.apply(input, TIMESTAMP));
    }

    @Test
    public void testEncodeToBytes() {
        final SelectFunction encode = create(
                "{ \"name\": \"b\", \"func\": \"base64_encode\", \"field\": \"stringField\", \"type\": \"bytes\" }",
                inputFields());
        final Map<String, Object> input = new HashMap<>();
        input.put("stringField", "hello");
        final Object output = encode.apply(input, TIMESTAMP);
        Assertions.assertInstanceOf(ByteBuffer.class, output);
        Assertions.assertEquals("aGVsbG8=", new String(((ByteBuffer) output).array(), StandardCharsets.UTF_8));
    }

    @Test
    public void testDecodeStringToString() {
        final SelectFunction decode = create(
                "{ \"name\": \"b\", \"func\": \"base64_decode\", \"field\": \"stringField\", \"type\": \"string\" }",
                inputFields());
        final Map<String, Object> input = new HashMap<>();
        input.put("stringField", "aGVsbG8=");
        Assertions.assertEquals("hello", decode.apply(input, TIMESTAMP));

        Assertions.assertNull(decode.apply(new HashMap<>(), TIMESTAMP));
    }

    @Test
    public void testDecodeBytesToBytes() {
        final SelectFunction decode = create(
                "{ \"name\": \"b\", \"func\": \"base64_decode\", \"field\": \"bytesField\", \"type\": \"bytes\" }",
                inputFields());
        final Map<String, Object> input = new HashMap<>();
        input.put("bytesField", ByteBuffer.wrap("aGVsbG8=".getBytes(StandardCharsets.UTF_8)));
        final Object output = decode.apply(input, TIMESTAMP);
        Assertions.assertInstanceOf(ByteBuffer.class, output);
        Assertions.assertEquals("hello", new String(((ByteBuffer) output).array(), StandardCharsets.UTF_8));
    }

    @Test
    public void testUnsupportedOutputTypeThrows() {
        final SelectFunction encode = create(
                "{ \"name\": \"b\", \"func\": \"base64_encode\", \"field\": \"stringField\", \"type\": \"int64\" }",
                inputFields());
        final Map<String, Object> input = new HashMap<>();
        input.put("stringField", "hello");
        Assertions.assertThrows(IllegalArgumentException.class, () -> encode.apply(input, TIMESTAMP));

        final SelectFunction decode = create(
                "{ \"name\": \"b\", \"func\": \"base64_decode\", \"field\": \"stringField\", \"type\": \"int64\" }",
                inputFields());
        final Map<String, Object> input2 = new HashMap<>();
        input2.put("stringField", "aGVsbG8=");
        Assertions.assertThrows(IllegalArgumentException.class, () -> decode.apply(input2, TIMESTAMP));
    }

}
