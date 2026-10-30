package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TokenizeTest {

    private static List<Schema.Field> inputFields() {
        return List.of(Schema.Field.of("textField", Schema.FieldType.STRING));
    }

    private static JsonObject json(final String text) {
        return new Gson().fromJson(text, JsonObject.class);
    }

    // ids and the sum of the attention mask of a text of the given number of words (2 special tokens are added by default)
    private static List<Integer> encode(final String options, final int words) {
        final String path = Path.of("src/test/resources/module/transform/onnx/tokenizer.json").toAbsolutePath().toString().replace('\\', '/');
        final Tokenize tokenize = Tokenize.of("t",
                json("{ \"tokenizer\": \"huggingface\", \"field\": \"textField\", \"path\": \"" + path + "\"" + options + " }"),
                inputFields(), true, false);
        tokenize.setup();
        final Map<?, ?> encoded = (Map<?, ?>) tokenize.apply(new HashMap<>(Map.of("textField", "hello ".repeat(words))), Instant.now());
        final int ids = ((List<?>) encoded.get("ids")).size();
        final int attended = ((List<?>) encoded.get("attentionMask")).stream().mapToInt(v -> ((Number) v).intValue()).sum();
        return List.of(ids, attended);
    }

    @Test
    public void testTruncation() {
        // on by default, at 512 tokens
        Assertions.assertEquals(List.of(512, 512), encode("", 600));
        Assertions.assertEquals(List.of(602, 602), encode(", \"truncation\": false", 600));
        Assertions.assertEquals(List.of(8, 8), encode(", \"truncation\": true, \"maxLength\": 8", 600));
        // maxLength is not clamped to the default 512
        Assertions.assertEquals(List.of(602, 602), encode(", \"truncation\": true, \"maxLength\": 1000", 600));
        Assertions.assertEquals(List.of(700, 700), encode(", \"truncation\": true, \"maxLength\": 700", 800));
    }

    @Test
    public void testPadding() {
        // records are encoded one by one: only maxLength gives them a common length
        Assertions.assertEquals(List.of(16, 5), encode(", \"padding\": true, \"truncation\": true, \"maxLength\": 16", 3));
        Assertions.assertEquals(List.of(5, 5), encode(", \"padding\": true", 3));
        Assertions.assertEquals(List.of(5, 5), encode(", \"padding\": false, \"maxLength\": 16", 3));
    }

    @Test
    public void testAddSpecialTokens() {
        Assertions.assertEquals(List.of(3, 3), encode(", \"addSpecialTokens\": false", 3));
    }

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"field\": \"textField\", \"path\": \"/tmp/tokenizer.json\" }"),
                        inputFields(), true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"tokenizer\": \"huggingface\", \"path\": \"/tmp/tokenizer.json\" }"),
                        inputFields(), true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"tokenizer\": \"huggingface\", \"field\": \"textField\" }"),
                        inputFields(), true, false));
        // unknown tokenizer name
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"tokenizer\": \"unknown\", \"field\": \"textField\", \"path\": \"/tmp/tokenizer.json\" }"),
                        inputFields(), true, false));
    }


    private static SelectFunction tokenIds(final String texts) {
        final String path = Path.of("src/test/resources/module/transform/onnx/tokenizer.json").toAbsolutePath().toString().replace('\\', '/');
        return SelectFunction.of(json("{ \"name\": \"o\", \"func\": \"token_ids\", \"tokenizer\": \"huggingface\", \"path\": \"" + path + "\", \"texts\": " + texts + " }"), inputFields());
    }

    // only the texts of one token count, without special tokens, and no id comes twice in a group
    @Test
    public void testTokenIds() {
        final SelectFunction function = tokenIds("[[\" hello\", \"hello\", \"hello world\", \"world\"], [\"onnx\"], \"pipeline\"]");
        Assertions.assertEquals("array", function.getOutputFieldType().getType().name());
        Assertions.assertEquals("array", function.getOutputFieldType().getArrayValueType().getType().name());
        Assertions.assertEquals(List.of(), function.getInputFields());
        // it reads no field, so it must not turn the Tensors before it in an onnx transform into lists
        Assertions.assertTrue(function.acceptsTensor());
        function.setup();
        Assertions.assertEquals(List.of(List.of(3L, 4L), List.of(5L), List.of(6L)), function.apply(new HashMap<>(), Instant.now()));
    }

    @Test
    public void testTokenIdsGroupWithoutOneToken() {
        final SelectFunction function = tokenIds("[[\"hello\"], [\"hello world\", \"onnx pipeline\"]]");
        final Exception e = Assertions.assertThrows(IllegalArgumentException.class, function::setup);
        Assertions.assertTrue(e.getMessage().contains("hello world"), e.getMessage());
    }

    @Test
    public void testTokenIdsValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> tokenIds("[]"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> tokenIds("\"hello\""));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                SelectFunction.of(json("{ \"name\": \"o\", \"func\": \"token_ids\", \"tokenizer\": \"huggingface\", \"texts\": [\"a\"] }"), inputFields()));
    }

}
