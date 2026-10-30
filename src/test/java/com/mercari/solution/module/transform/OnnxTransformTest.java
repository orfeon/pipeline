package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.ml.onnx.OnnxTestModels;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.*;

public class OnnxTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final Path BASE_DIR = Path.of("target", "onnx-transform-test");

    private static final float[][] XS = {
            {1f, 0f, 0f},
            {0f, 1f, 0f},
            {0f, 0f, 1f},
            {1f, 2f, 3f},
            {-1f, 0.5f, 2f}
    };

    private static String linearConfig(final String modelPath, final String modelFile, final int bufferSize) {
        final StringBuilder elements = new StringBuilder();
        for(int i=0; i<XS.length; i++) {
            elements.append("                        - { id: ").append(i).append(", x: ")
                    .append(Arrays.toString(XS[i])).append(" }\n");
        }
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
                        - { name: id, type: int64 }
                        - { name: x, type: float32, mode: repeated }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    parameters:
                      model:
                        path: "%s"
                %s
                      mappings:
                        - inputs: { x: x }
                          outputs: { y: y, s: x_sum, y3: y3 }
                      bufferSize: %d
                """.formatted(elements, modelPath, modelFile == null ? "" : "        file: \"" + modelFile + "\"", bufferSize);
    }

    @Test
    public void testLinearUnbatched() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("unbatched/model.onnx"), OnnxTestModels.linear());
        assertLinear(linearConfig(OnnxTestModels.configPath(model), null, 1));
    }

    // 5 records with bufferSize 3: every record gets its own row of the batch output, and the records left in a
    // partially filled buffer are emitted when the bundle finishes
    @Test
    public void testLinearBatched() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("batched/model.onnx"), OnnxTestModels.linear());
        assertLinear(linearConfig(OnnxTestModels.configPath(model), null, 3));
    }

    // a model directory: the model file is given relative to it
    @Test
    public void testModelDirectoryWithFile() throws Exception {
        final Path dir = BASE_DIR.resolve("directory");
        OnnxTestModels.write(dir.resolve("onnx/model.onnx"), OnnxTestModels.linear());
        OnnxTestModels.write(dir.resolve("onnx/model_other.onnx"), OnnxTestModels.tokens());
        assertLinear(linearConfig(OnnxTestModels.configPath(dir) + "/", "onnx/model.onnx", 2));
    }

    private void assertLinear(final String configYaml) throws Exception {
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configYaml));
        final MCollection output = outputs.get("onnx");

        Assertions.assertEquals("array", output.getSchema().getField("y").getFieldType().getType().name());
        // s is [batch, 1]: an array of one value per record
        Assertions.assertEquals("array", output.getSchema().getField("x_sum").getFieldType().getType().name());
        Assertions.assertEquals("matrix", output.getSchema().getField("y3").getFieldType().getType().name());

        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Set<Integer> ids = new HashSet<>();
            for(final MElement row : rows) {
                final int id = ((Number) row.getPrimitiveValue("id")).intValue();
                ids.add(id);
                final float[] x = XS[id];
                final float y0 = x[0] + 3 * x[1] + 5 * x[2] + 0.5f;
                final float y1 = 2 * x[0] + 4 * x[1] + 6 * x[2] - 0.5f;
                Assertions.assertEquals(List.of(y0, y1), floats(row.getPrimitiveValue("y")), "y of record " + id);
                Assertions.assertEquals(List.of(x[0] + x[1] + x[2]), floats(row.getPrimitiveValue("x_sum")), "x_sum of record " + id);
                Assertions.assertEquals(List.of(y0, y1), floats(row.getPrimitiveValue("y3")), "y3 of record " + id);
            }
            Assertions.assertEquals(Set.of(0, 1, 2, 3, 4), ids);
            return null;
        });

        pipeline.run();
    }

    // tokenize_encode feeds a model with int64 inputs of a dynamic length; a rank-1 output gives each record a scalar;
    // tokenize_decode turns the ids back to text; the singular `mapping` is accepted
    @Test
    public void testTokenizer() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("tokens/model.onnx"), OnnxTestModels.tokens());
        final String tokenizer = OnnxTestModels.configPath(Path.of("src/test/resources/module/transform/onnx/tokenizer.json"));
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { text: "Hello world" }
                        - { text: "onnx pipeline onnx" }
                        - { text: "hello unknownword" }
                    schema:
                      fields:
                        - { name: text, type: string }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    parameters:
                      model:
                        path: "%s"
                      preprocesses:
                        - { name: tok, func: tokenize_encode, tokenizer: huggingface, path: "%s", field: text }
                      mapping:
                        inputs: { tok.ids: ids, tok.attentionMask: attention_mask }
                        outputs: { total: total, count: count }
                      postprocesses:
                        - { name: text }
                        - { name: total }
                        - { name: count }
                        - { name: decoded, func: tokenize_decode, tokenizer: huggingface, path: "%s", field: tok.ids, skipSpecialTokens: true }
                """.formatted(OnnxTestModels.configPath(model), tokenizer, tokenizer);

        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configYaml));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals("float32", output.getSchema().getField("total").getFieldType().getType().name());
        Assertions.assertEquals("int64", output.getSchema().getField("count").getFieldType().getType().name());

        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, MElement> byText = new HashMap<>();
            for(final MElement row : rows) {
                byText.put(row.getPrimitiveValue("text").toString(), row);
            }
            Assertions.assertEquals(3, byText.size());
            // [CLS] hello world [SEP] = [1, 3, 4, 2]
            assertTokens(byText.get("Hello world"), 10f, 4L, "hello world");
            // [CLS] onnx pipeline onnx [SEP] = [1, 5, 6, 5, 2]
            assertTokens(byText.get("onnx pipeline onnx"), 19f, 5L, "onnx pipeline onnx");
            // [CLS] hello [UNK] [SEP] = [1, 3, 0, 2]; the special [UNK] is skipped by the decoder
            assertTokens(byText.get("hello unknownword"), 6f, 4L, "hello");
            return null;
        });

        pipeline.run();
    }

    private static void assertTokens(final MElement row, final float total, final long count, final String decoded) {
        Assertions.assertEquals(total, ((Number) row.getPrimitiveValue("total")).floatValue(), 1e-6);
        Assertions.assertEquals(count, ((Number) row.getPrimitiveValue("count")).longValue());
        Assertions.assertEquals(decoded, row.getPrimitiveValue("decoded").toString());
    }

    // a record the model cannot take is a failure; the other records go on
    @Test
    public void testFailure() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("failure/model.onnx"), OnnxTestModels.linear());
        assertFailure(model, 1, Set.of(0L, 2L));
    }

    // a batch that fails is routed to the failures as a whole: no record of it is lost or emitted
    @Test
    public void testFailureBatched() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("failure_batched/model.onnx"), OnnxTestModels.linear());
        assertFailure(model, 3, null);
    }

    private void assertFailure(final Path model, final int bufferSize, final Set<Long> expectedIds) throws Exception {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0, x: [1.0, 2.0, 3.0] }
                        - { id: 1, x: [1.0, 2.0] }
                        - { id: 2, x: [3.0, 2.0, 1.0] }
                    schema:
                      fields:
                        - { name: id, type: int64 }
                        - { name: x, type: float32, mode: repeated }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    failFast: false
                    parameters:
                      model:
                        path: "%s"
                      mappings:
                        - inputs: { x: x }
                          outputs: { y: y }
                      bufferSize: %d
                """.formatted(OnnxTestModels.configPath(model), bufferSize);

        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configYaml));

        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement row : rows) {
                ids.add(((Number) row.getPrimitiveValue("id")).longValue());
            }
            Assertions.assertFalse(ids.contains(1L), "the record with a wrong length must not be emitted");
            if(expectedIds != null) {
                Assertions.assertEquals(expectedIds, ids);
            }
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testMappingsRequired() {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0 }
                    schema:
                      fields:
                        - { name: id, type: int64 }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    parameters:
                      model:
                        path: "model.onnx"
                """;
        final Exception e = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(pipeline, Config.load(configYaml)));
        Assertions.assertTrue(rootMessages(e).contains("parameters.mappings must not be empty"), rootMessages(e));
    }

    private static String rootMessages(Throwable e) {
        final StringBuilder messages = new StringBuilder();
        while(e != null) {
            if(e instanceof IllegalModuleException || e.getMessage() != null) {
                messages.append(e.getMessage()).append('\n');
            }
            e = e.getCause();
        }
        return messages.toString();
    }

    private static List<Float> floats(final Object value) {
        final List<Float> floats = new ArrayList<>();
        for(final Object v : (Collection<?>) value) {
            floats.add(((Number) v).floatValue());
        }
        return floats;
    }

}
