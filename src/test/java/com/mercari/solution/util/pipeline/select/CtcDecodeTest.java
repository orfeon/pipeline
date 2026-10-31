package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CtcDecodeTest {

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("ids", Schema.FieldType.array(Schema.FieldType.INT64)),
            Schema.Field.of("conf", Schema.FieldType.array(Schema.FieldType.FLOAT32)),
            Schema.Field.of("length", Schema.FieldType.INT64),
            Schema.Field.of("name", Schema.FieldType.STRING));

    private static CtcDecode decode(final String json) {
        final JsonObject config = JsonParser.parseString(json).getAsJsonObject();
        final CtcDecode function = CtcDecode.of("read", config, FIELDS, false);
        function.setup();
        return function;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> apply(final CtcDecode function, final Map<String, Object> values) {
        return (Map<String, Object>) function.apply(values, Instant.EPOCH);
    }

    private static Map<String, Object> values(final Object ids, final Object conf, final Object length) {
        final Map<String, Object> values = new HashMap<>();
        values.put("ids", ids);
        values.put("conf", conf);
        values.put("length", length);
        return values;
    }

    @Test
    public void testRepeatsAndBlank() {
        final CtcDecode function = decode("{ field: ids, confField: conf, tokens: [a, b, c] }");
        // repeats go, and a blank between two of the same keeps both
        final Map<String, Object> output = apply(function, values(
                List.of(1L, 1L, 0L, 1L, 2L, 2L, 0L, 0L, 3L),
                List.of(0.5f, 9f, 9f, 0.7f, 0.9f, 9f, 9f, 9f, 0.1f),
                null));
        Assertions.assertEquals("aabc", output.get("text"));
        Assertions.assertEquals((0.5f + 0.7f + 0.9f + 0.1f) / 4, (float) output.get("conf"), 1e-6f);
    }

    @Test
    public void testTensorsWithALength() {
        final CtcDecode function = decode("{ field: ids, confField: conf, lengthField: length, tokens: [a, b] }");
        // what is after the length of a batch padded to the longest is not read
        final Map<String, Object> output = apply(function, values(
                Tensor.of(new long[]{1L, 2L, 1L, 1L}, new long[]{4}),
                Tensor.of(new float[]{0.2f, 0.4f, 9f, 9f}, new long[]{4}),
                2L));
        Assertions.assertEquals("ab", output.get("text"));
        Assertions.assertEquals(0.3f, (float) output.get("conf"), 1e-6f);
    }

    @Test
    public void testNothingRead() {
        final Map<String, Object> output = apply(decode("{ field: ids, confField: conf, tokens: [a] }"), values(List.of(0L, 0L), List.of(0.9f, 0.9f), null));
        Assertions.assertEquals("", output.get("text"));
        Assertions.assertEquals(0f, output.get("conf"));
        Assertions.assertNull(decode("{ field: ids, tokens: [a] }").apply(values(null, null, null), Instant.EPOCH));
    }

    @Test
    public void testMinConf() {
        final CtcDecode function = decode("{ field: ids, confField: conf, tokens: [a], minConf: 0.5 }");
        final Map<String, Object> low = apply(function, values(List.of(1L), List.of(0.4f), null));
        Assertions.assertEquals("", low.get("text"), "a low confidence reads nothing");
        Assertions.assertEquals(0.4f, low.get("conf"), "and keeps its confidence");
        Assertions.assertEquals("a", apply(function, values(List.of(1L), List.of(0.5f), null)).get("text"));
    }

    @Test
    public void testNormalize() {
        final String jamo = "{ field: ids, tokens: [\"\\u1112\", \"\\u1161\", \"\\u11ab\"]%s }";
        final List<Long> ids = List.of(1L, 2L, 3L);
        Assertions.assertEquals("\ud55c", apply(decode(jamo.formatted(", normalize: NFC")), values(ids, null, null)).get("text"), "the syllable of three jamo");
        Assertions.assertEquals("\u1112\u1161\u11ab", apply(decode(jamo.formatted("")), values(ids, null, null)).get("text"));
    }

    @Test
    public void testLatexCommands() {
        final CtcDecode function = decode("{ field: ids, tokens: [\"\\\\alpha\", x, \"1\", \"\\\\\\\\\"] }");
        Assertions.assertEquals("\\alpha x", apply(function, values(List.of(1L, 2L), null, null)).get("text"), "it must not read as \\alphax");
        Assertions.assertEquals("\\alpha1", apply(function, values(List.of(1L, 3L), null, null)).get("text"));
        Assertions.assertEquals("x1", apply(function, values(List.of(2L, 3L), null, null)).get("text"));
        Assertions.assertEquals("\\\\x", apply(function, values(List.of(4L, 2L), null, null)).get("text"), "not a command");
    }

    @Test
    public void testAnotherBlank() {
        // the last class of the model, as many CTC models have it
        final CtcDecode function = decode("{ field: ids, tokens: [a, b], blank: 2 }");
        Assertions.assertEquals("ab", apply(function, values(List.of(0L, 2L, 1L), null, null)).get("text"));
    }

    @Test
    public void testVocabularyFile(@TempDir final Path dir) throws IOException {
        final Path meta = dir.resolve("meta.json");
        // a vocabulary as a string in a file of many things, as meta.json of indra-ocr has them
        Files.writeString(meta, "{ \"det_long_side\": 1024, \"vocab\": \"{\\\"blank\\\": 0, \\\"chars\\\": [\\\"a\\\", \\\"b\\\"]}\", \"other\": { \"chars\": [\"c\"], \"blank\": 1 } }", StandardCharsets.UTF_8);
        final String path = meta.toString().replace('\\', '/');
        Assertions.assertEquals("ab", apply(decode("{ field: ids, path: \"%s\", key: vocab }".formatted(path)), values(List.of(1L, 2L), null, null)).get("text"));
        Assertions.assertEquals("c", apply(decode("{ field: ids, path: \"%s\", key: other }".formatted(path)), values(List.of(0L, 1L), null, null)).get("text"), "the blank of the file");

        final Path tokens = dir.resolve("tokens.json");
        Files.writeString(tokens, "[\"x\", \"y\"]", StandardCharsets.UTF_8);
        Assertions.assertEquals("y", apply(decode("{ field: ids, path: \"%s\" }".formatted(tokens.toString().replace('\\', '/'))), values(List.of(2L), null, null)).get("text"));

        final Exception missing = Assertions.assertThrows(IllegalArgumentException.class, () -> decode("{ field: ids, path: \"%s\", key: nothing }".formatted(path)));
        Assertions.assertTrue(missing.getMessage().contains("nothing"), missing.getMessage());
    }

    @Test
    public void testIdOutOfTheVocabulary() {
        final CtcDecode function = decode("{ field: ids, tokens: [a] }");
        final Exception e = Assertions.assertThrows(IllegalArgumentException.class, () -> function.apply(values(List.of(5L), null, null), Instant.EPOCH));
        Assertions.assertTrue(e.getMessage().contains("id: 5"), e.getMessage());
    }

    @Test
    public void testConfigErrors() {
        for(final Map.Entry<String, String> error : Map.of(
                "{ tokens: [a] }", "requires field",
                "{ field: ids }", "either path or tokens",
                "{ field: ids, tokens: [a], path: x }", "either path or tokens",
                "{ field: name, tokens: [a] }", "must be an array",
                "{ field: ids, confField: name, tokens: [a] }", "must be an array",
                "{ field: ids, lengthField: name, tokens: [a] }", "must be a number",
                "{ field: ids, tokens: [a], minConf: 0.5 }", "requires confField",
                "{ field: ids, tokens: [a], normalize: nfc }", "normalize must be one of",
                "{ field: ids, tokens: [] }", "tokens must be an array").entrySet()) {
            final Exception e = Assertions.assertThrows(IllegalArgumentException.class, () -> decode(error.getKey()), error.getKey());
            Assertions.assertTrue(e.getMessage().contains(error.getValue()), e.getMessage());
        }
    }

}
