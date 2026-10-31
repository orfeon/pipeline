package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.ml.onnx.OnnxTestModels;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.*;

/** The {@code loop} parameter on {@link OnnxTestModels#generator}, whose next token is the last one plus 1. */
public class OnnxLoopTest implements Serializable {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final Path BASE_DIR = Path.of("target", "onnx-loop-test");
    private static final String TOKENIZER = OnnxTestModels.configPath(Path.of("src/test/resources/module/transform/onnx/tokenizer.json"));

    private static final String ELEMENTS = """
                        - { id: a, ids: [2, 3] }
                        - { id: b, ids: [5] }
            """;

    private static final String NEXT = """
                          "past.%d": "present.%d"
                          token: { func: argmax, field: logits, position: last }
                          generated: { func: array_append, field: generated, valueField: token }
                          input_ids: { func: array, fields: [token] }
                          attention_mask: { func: array_append, field: attention_mask, value: 1 }
                          position_ids: { func: range, field: attention_mask, last: 1 }
            """;

    private static final String OUTPUTS = """
                      - { name: generated, field: generated }
                      - { name: steps, field: _iteration }
                      - { name: cache, field: present.0 }
                      - { name: mask, field: mask_sum }
                      - { name: lastPosition, field: positions }
            """;

    private static String config(final Path model, final String loop, final String outputs, final int bufferSize) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                %s
                    schema:
                      fields:
                        - { name: id, type: string }
                        - { name: ids, type: int64, mode: repeated }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    failFast: false
                    parameters:
                      model:
                        path: "%s"
                      bufferSize: %d
                      inputs:
                        - { name: input_ids, field: ids }
                        - { name: mask, func: array_append, field: ids, value: 1 }
                        - { name: attention_mask, func: range, field: ids }
                        - { name: position_ids, func: range, field: ids }
                        - { name: "past.%%d", fill: 0 }
                      loop:
                %s
                      outputs:
                %s
                """.formatted(ELEMENTS.indent(8), OnnxTestModels.configPath(model), bufferSize, loop, outputs);
    }

    private static String loop(final String next, final String until, final int maxIterations) {
        return """
                        vars:
                          generated: { value: [], type: array<int64> }
                        next:
                %s
                        until: %s
                        maxIterations: %d
                """.formatted(next, until, maxIterations);
    }

    private static Map<String, MElement> byId(final Iterable<MElement> rows) {
        final Map<String, MElement> byId = new HashMap<>();
        for(final MElement row : rows) {
            byId.put(row.getAsString("id"), row);
        }
        return byId;
    }

    private static List<Long> longs(final Object value) {
        return ((Collection<?>) value).stream().map(v -> ((Number) v).longValue()).toList();
    }

    private static List<Float> floats(final Object value) {
        return ((Collection<?>) value).stream().map(v -> ((Number) v).floatValue()).toList();
    }

    // the cache only grows by what each run takes, so that it shows the tensor went from one run to the next
    @Test
    public void testGeneration() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("generation/model.onnx"), OnnxTestModels.generator(OnnxTestModels.FLOAT));
        final String loop = loop(NEXT, "{ key: token, op: in, value: [7, 9] }", 20);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(model, loop, OUTPUTS, 1)));

        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final Map<String, MElement> byId = byId(rows);
            final MElement a = byId.get("a");
            Assertions.assertEquals(List.of(4L, 5L, 6L, 7L), longs(a.getPrimitiveValue("generated")), "with the token that stopped it");
            Assertions.assertEquals(3L, ((Number) a.getPrimitiveValue("steps")).longValue());
            Assertions.assertEquals(List.of(2f, 3f, 4f, 5f, 6f), floats(a.getPrimitiveValue("cache")));
            Assertions.assertEquals(5L, ((Number) a.getPrimitiveValue("mask")).longValue());
            Assertions.assertEquals(List.of(4L), longs(a.getPrimitiveValue("lastPosition")));

            final MElement b = byId.get("b");
            Assertions.assertEquals(List.of(6L, 7L), longs(b.getPrimitiveValue("generated")), "a record stops by itself");
            Assertions.assertEquals(List.of(5f, 6f), floats(b.getPrimitiveValue("cache")));
            return null;
        });
        pipeline.run();
    }

    // reaching the limit is not a failure, and the record gets the values of the last run
    @Test
    public void testMaxIterations() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("max/model.onnx"), OnnxTestModels.generator(OnnxTestModels.FLOAT));
        final String loop = loop(NEXT, "{ key: token, op: in, value: [15] }", 2);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(model, loop, OUTPUTS, 1)));

        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final MElement a = byId(rows).get("a");
            Assertions.assertEquals(List.of(4L, 5L), longs(a.getPrimitiveValue("generated")));
            Assertions.assertEquals(1L, ((Number) a.getPrimitiveValue("steps")).longValue());
            Assertions.assertEquals(List.of(2f, 3f, 4f), floats(a.getPrimitiveValue("cache")));
            return null;
        });
        pipeline.run();
    }

    // a float16 cache from a fill goes from one run to the next as it is
    @Test
    public void testFloat16Cache() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("float16/model.onnx"), OnnxTestModels.generator(OnnxTestModels.FLOAT16));
        final String loop = loop(NEXT, "{ key: _iteration, op: \">=\", value: 2 }", 20);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(model, loop, OUTPUTS, 1)));

        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final MElement a = byId(rows).get("a");
            Assertions.assertEquals(List.of(2f, 3f, 4f, 5f), floats(a.getPrimitiveValue("cache")));
            Assertions.assertEquals(List.of(4L, 5L, 6L), longs(a.getPrimitiveValue("generated")), "until can read _iteration");
            return null;
        });
        pipeline.run();
    }

    // sample as an item of next, and the tokens of a loop decoded in outputs
    @Test
    public void testSampleAndDecode() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("sample/model.onnx"), OnnxTestModels.generator(OnnxTestModels.FLOAT));
        final String next = NEXT.replace("{ func: argmax, field: logits, position: last }", "{ func: sample, field: logits, position: last, temperature: 0, seed: 1 }");
        final String loop = loop(next, "{ key: token, op: \">=\", value: 6 }", 20);
        final String out = """
                      - { name: text, func: tokenize_decode, tokenizer: huggingface, path: "%s", field: generated, skipSpecialTokens: true }
            """.formatted(TOKENIZER);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(model, loop, out, 1)));

        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            Assertions.assertEquals("world onnx pipeline", byId(rows).get("a").getAsString("text"));
            return null;
        });
        pipeline.run();
    }

    // an item of next that fails only fails its record
    @Test
    public void testFailingRecord() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("failing/model.onnx"), OnnxTestModels.generator(OnnxTestModels.FLOAT));
        final String next = NEXT + """
                          second: { func: slice, field: ids, index: 1 }
            """;
        final String loop = loop(next, "{ key: token, op: in, value: [7] }", 20);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(model, loop, OUTPUTS, 1)));

        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            Assertions.assertEquals(Set.of("a"), byId(rows).keySet());
            return null;
        });
        pipeline.run();
    }

    // a value of rank 0: num_logits_to_keep int64[] with fill
    @Test
    public void testScalarFill() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("scalar/model.onnx"), OnnxTestModels.scalarInput());
        final String config = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: a, x: 1.5 }
                    schema:
                      fields:
                        - { name: id, type: string }
                        - { name: x, type: float32 }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    failFast: true
                    parameters:
                      model:
                        path: "%s"
                      inputs:
                        - { name: x, field: x }
                        - { name: keep, fill: 3 }
                      outputs:
                        - { name: kept, field: kept }
                """.formatted(OnnxTestModels.configPath(model));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));

        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            Assertions.assertEquals(3L, ((Number) rows.iterator().next().getPrimitiveValue("kept")).longValue());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testConfigErrors() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("errors/model.onnx"), OnnxTestModels.generator(OnnxTestModels.FLOAT));
        final Path float16 = OnnxTestModels.write(BASE_DIR.resolve("errors/mixed.onnx"), OnnxTestModels.generator(OnnxTestModels.FLOAT16, OnnxTestModels.FLOAT));
        final String until = "{ key: token, op: in, value: [7] }";
        final Map<String, String> cases = new LinkedHashMap<>();
        cases.put("maxIterations is required", """
                        next:
                %s
                        until: %s
                """.formatted(NEXT, until));
        cases.put("over than zero", loop(NEXT, until, 0));
        cases.put("next must not be empty", """
                        next: {}
                        until: %s
                        maxIterations: 1
                """.formatted(until));
        cases.put("until must not be null", """
                        next:
                %s
                        maxIterations: 1
                """.formatted(NEXT));
        cases.put("does not have: [nothing]", loop(NEXT, "{ key: nothing, op: in, value: [7] }", 1));
        cases.put("has more name variables", loop(NEXT.replace("\"present.%d\"", "\"logits\""), until, 1));
        cases.put("which is not a model output", loop(NEXT.replace("\"present.%d\"", "\"presents.%d\""), until, 1));
        cases.put("can not take the model output", loop(NEXT.replace("\"past.%d\": \"present.%d\"", "\"past.0\": \"mask_sum\""), until, 1));
        cases.put("it is not a model input", loop(NEXT.replace("\"past.%d\"", "\"token2\""), until, 1).replace("\"present.%d\"", "\"present.0\""));
        cases.put("matches no model input", loop(NEXT.replace("\"past.%d\"", "\"pasts.%d\""), until, 1));
        cases.put("has the name of a model output", loop(NEXT + """
                          logits: { func: range, field: ids }
            """, until, 1));
        cases.put("with name variables must be the name of a model output", loop(NEXT + """
                          "other.%d": { func: range, field: ids }
            """, until, 1));
        cases.put("takes a value of rank 1", loop(NEXT.replace("{ func: array, fields: [token] }", "{ field: token }"), until, 1));
        cases.put("is given both", loop(NEXT + """
                          "past.0": { func: range, field: ids }
            """, until, 1));
        cases.put("logits is an array or a matrix", loop(NEXT, "{ key: logits, op: in, value: [7] }", 1));
        cases.put("generated is an array or a matrix", loop(NEXT, "{ key: generated, op: in, value: [7] }", 1));
        cases.put("does not have: [token.value]", loop(NEXT, "{ key: token.value, op: in, value: [7] }", 1));
        cases.put("_iteration is the number of the last run", loop(NEXT + """
                          _iteration: { func: range, field: ids }
            """, until, 1));

        for(final Map.Entry<String, String> entry : cases.entrySet()) {
            assertConfigError(model, entry.getValue(), entry.getKey());
        }
        assertConfigError(model, loop(NEXT, "null", 1), "until must not be null");
        assertConfigError(model, """
                        next:
                %s
                        until: %s
                        maxIterations: null
                """.formatted(NEXT, until), "maxIterations is required");

        final Exception buffer = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(TestPipeline.create().enableAbandonedNodeEnforcement(false), Config.load(config(model, loop(NEXT, until, 1), OUTPUTS, 2))));
        Assertions.assertTrue(rootMessages(buffer).contains("bufferSize must be 1"), rootMessages(buffer));

        // a cache of another type can not go to the next run as it is
        final Exception type = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(TestPipeline.create().enableAbandonedNodeEnforcement(false), Config.load(config(float16, loop(NEXT, until, 1), OUTPUTS, 1))));
        Assertions.assertTrue(rootMessages(type).contains("can not take the model output"), rootMessages(type));
    }

    private static void assertConfigError(final Path model, final String loop, final String message) {
        final Exception e = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(TestPipeline.create().enableAbandonedNodeEnforcement(false), Config.load(config(model, loop, OUTPUTS, 1))), message);
        Assertions.assertTrue(rootMessages(e).contains(message), message + ": " + rootMessages(e));
    }

    private static String rootMessages(Throwable e) {
        final StringBuilder messages = new StringBuilder();
        while(e != null) {
            if(e.getMessage() != null) {
                messages.append(e.getMessage()).append('\n');
            }
            e = e.getCause();
        }
        return messages.toString();
    }

}
