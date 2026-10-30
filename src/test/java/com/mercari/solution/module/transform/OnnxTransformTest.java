package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.ml.onnx.OnnxTestModels;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

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

    // a batch that fails is retried record by record: only the record that fails by itself is a failure
    @Test
    public void testFailureBatched() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("failure_batched/model.onnx"), OnnxTestModels.linear());
        assertFailure(model, 3, Set.of(0L, 2L));
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
            Assertions.assertEquals(expectedIds, ids);
            return null;
        });

        pipeline.run();
    }

    // every record of a failing batch reaches the failure output, whatever the bundles are: all the records fail here
    @Test
    public void testFailureRoutesEveryRecordOfBatch(@TempDir final Path tempDir) throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("failure_routing/model.onnx"), OnnxTestModels.linear());
        final Path failures = tempDir.resolve("failures");
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 10, x: [1.0, 2.0] }
                        - { id: 11, x: [3.0, 4.0] }
                        - { id: 12, x: [5.0, 6.0] }
                    schema:
                      fields:
                        - { name: id, type: int64 }
                        - { name: x, type: float32, mode: repeated }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    failFast: false
                    failureSinks:
                      - name: dead_letter
                        module: storage
                        parameters:
                          output: "%s/failed"
                          format: json
                          suffix: ".json"
                          numShards: 1
                    parameters:
                      model:
                        path: "%s"
                      mappings:
                        - inputs: { x: x }
                          outputs: { y: y }
                      bufferSize: 3
                """.formatted(Path.of("").toAbsolutePath().relativize(failures).toString().replace('\\', '/'), OnnxTestModels.configPath(model));

        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configYaml));
        PAssert.that(outputs.get("onnx").getCollection()).empty();
        pipeline.run().waitUntilFinish();

        final List<String> lines = new ArrayList<>();
        try(final Stream<Path> files = Files.walk(failures)) {
            for(final Path file : files.filter(Files::isRegularFile).toList()) {
                lines.addAll(Files.readAllLines(file));
            }
        }
        Assertions.assertEquals(3, lines.size(), lines.toString());
        final String written = String.join("\n", lines);
        for(final String value : List.of("[1.0,2.0]", "[3.0,4.0]", "[5.0,6.0]")) {
            Assertions.assertTrue(written.contains(value), value + " in " + written);
        }
    }

    // an input with an Avro schema (BigQuery, Avro files, ...): the records are written with the added fields, too
    @Test
    public void testInputWithAvroSchema() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("avro_input/model.onnx"), OnnxTestModels.linear());
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0, x: [1.0, 0.0, 0.0] }
                    schema:
                      avro:
                        json: '{"type":"record","name":"root","fields":[{"name":"id","type":"long"},{"name":"x","type":{"type":"array","items":"float"}}]}'
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    parameters:
                      model:
                        path: "%s"
                      mappings:
                        - inputs: { x: x }
                          outputs: { y: y }
                """.formatted(OnnxTestModels.configPath(model));

        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configYaml));
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(List.of(1.5f, 1.5f), floats(row.getPrimitiveValue("y")));
            }
            return null;
        });

        pipeline.run();
    }

    private static String castConfig(final Path model, final String valueType, final String value, final String mappings) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0, x: %s }
                    schema:
                      fields:
                        - { name: id, type: int64 }
                        - { name: x, type: %s }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    failFast: true
                    parameters:
                      model:
                        path: "%s"
                      mappings:
                %s
                """.formatted(value, valueType, OnnxTestModels.configPath(model), mappings);
    }

    // a transform runs its model once per record
    @Test
    public void testMappingsMoreThanOne() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("mappings_two/model.onnx"), OnnxTestModels.cast(OnnxTestModels.FLOAT));
        final Exception e = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(pipeline, Config.load(castConfig(model, "float32", "1.5", """
                        - inputs: { x: x }
                        - inputs: { y: x }
                """))));
        Assertions.assertTrue(rootMessages(e).contains("parameters.mappings must have one element"), rootMessages(e));
    }

    // a model output named as an input field replaces it, also in its type
    @Test
    public void testOutputFieldReplacesInputField() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("replaced_input/model.onnx"), OnnxTestModels.cast(OnnxTestModels.FLOAT));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(castConfig(model, "float32", "1.5", """
                        - inputs: { x: x }
                          outputs: { y: id }
                """)));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals(List.of("id", "x"), output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals("float32", output.getSchema().getField("id").getFieldType().getType().name());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(1.5f, ((Number) row.getPrimitiveValue("id")).floatValue());
            }
            return null;
        });

        pipeline.run();
    }

    private void assertCastInput(final int elemType, final String valueType, final String value, final float expected) throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("input_type_" + elemType + "/model.onnx"), OnnxTestModels.cast(elemType));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(castConfig(model, valueType, value, """
                        - inputs: { x: x }
                """)));
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                Assertions.assertEquals(expected, ((Number) row.getPrimitiveValue("y")).floatValue());
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUint8Input() throws Exception {
        assertCastInput(OnnxTestModels.UINT8, "int32", "200", 200f);
    }

    @Test
    public void testBoolInput() throws Exception {
        assertCastInput(OnnxTestModels.BOOL, "boolean", "true", 1f);
    }

    @Test
    public void testFloat16Input() throws Exception {
        assertCastInput(OnnxTestModels.FLOAT16, "float32", "1.5", 1.5f);
    }

    @Test
    public void testBFloat16Input() throws Exception {
        assertCastInput(OnnxTestModels.BFLOAT16, "float32", "1.5", 1.5f);
    }

    // [] is broadcast to every record of the batch, without the batch dimension
    @Test
    public void testScalarOutput() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("scalar_output/model.onnx"), OnnxTestModels.scalarOutput());
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(castConfig(model, "float32", "1.5", """
                        - inputs: { x: x }
                """)));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals("float32", output.getSchema().getField("total").getFieldType().getType().name());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(1.5f, ((Number) row.getPrimitiveValue("total")).floatValue());
            }
            return null;
        });

        pipeline.run();
    }

    // [batch, dynamic, 2]: a matrix whose dynamic dimension is -1 in the shape; the values stay flat row-major
    @Test
    public void testMatrixOutputWithDynamicDimension() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("dynamic_matrix/model.onnx"), OnnxTestModels.dynamicMatrix());
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0, x: [1.0, 2.0, 3.0, 4.0] }
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
                      mappings:
                        - inputs: { x: x }
                """.formatted(OnnxTestModels.configPath(model));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configYaml));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals(List.of(-1, 2), output.getSchema().getField("y").getFieldType().getShape());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(List.of(1f, 2f, 3f, 4f), floats(row.getPrimitiveValue("y")));
            }
            return null;
        });

        pipeline.run();
    }

    // the nested tokenize_encode field stays in the record when there are no postprocesses
    @Test
    public void testTokenizerWithoutPostprocess() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("tokens_no_post/model.onnx"), OnnxTestModels.tokens());
        final String tokenizer = OnnxTestModels.configPath(Path.of("src/test/resources/module/transform/onnx/tokenizer.json"));
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { text: "Hello world" }
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
                """.formatted(OnnxTestModels.configPath(model), tokenizer);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configYaml));
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(10f, ((Number) row.getPrimitiveValue("total")).floatValue());
                Assertions.assertEquals(List.of(1L, 3L, 4L, 2L), ((Map<?, ?>) row.getPrimitiveValue("tok")).get("ids"));
            }
            return null;
        });

        pipeline.run();
    }

    private static String textsConfig(final Path model, final String inputs, final String outputs, final int bufferSize) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { text: "Hello world" }
                        - { text: "onnx pipeline onnx" }
                        - { text: "hello" }
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
                      inputs:
                %s
                      outputs:
                %s
                      bufferSize: %d
                """.formatted(OnnxTestModels.configPath(model), inputs, outputs, bufferSize);
    }

    private static final String TOKENIZER = OnnxTestModels.configPath(Path.of("src/test/resources/module/transform/onnx/tokenizer.json"));

    private void assertTexts(final String name, final String pad, final int bufferSize) throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve(name + "/model.onnx"), OnnxTestModels.tokens());
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(textsConfig(model, """
                        - { name: tok, func: tokenize_encode, tokenizer: huggingface, path: "%s", field: text }
                        - { name: ids, field: tok.ids%s }
                        - { name: attention_mask, field: tok.attentionMask%s }
                """.formatted(TOKENIZER, pad, pad), """
                        - { name: total_ids, field: total }
                        - { name: n, field: count }
                """, bufferSize)));
        final MCollection output = outputs.get("onnx");
        // the fields of the input and the outputs; inputs are not output
        Assertions.assertEquals(List.of("text", "total_ids", "n"), output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, float[]> byText = new HashMap<>();
            for(final MElement row : rows) {
                byText.put(row.getPrimitiveValue("text").toString(), new float[]{
                        ((Number) row.getPrimitiveValue("total_ids")).floatValue(), ((Number) row.getPrimitiveValue("n")).floatValue()});
            }
            Assertions.assertEquals(3, byText.size());
            Assertions.assertArrayEquals(new float[]{10f, 4f}, byText.get("Hello world"));
            Assertions.assertArrayEquals(new float[]{19f, 5f}, byText.get("onnx pipeline onnx"));
            Assertions.assertArrayEquals(new float[]{6f, 3f}, byText.get("hello"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testInputsOutputs() throws Exception {
        assertTexts("inputs_outputs", "", 1);
    }

    // records of different lengths in a batch: the attention mask gives the same results as one by one
    @Test
    public void testPadRight() throws Exception {
        assertTexts("pad_right", ", pad: 0", 3);
    }

    @Test
    public void testPadLeft() throws Exception {
        assertTexts("pad_left", ", pad: { value: 0, side: left }", 3);
    }

    // without pad a batch of different lengths fails, and the records still go one by one
    @Test
    public void testNoPadFallsBackToOneByOne() throws Exception {
        assertTexts("no_pad", "", 3);
    }

    // the configuration of a text embedding model: a template, the tokenizer, padding, unused inputs given empty, a normalized output
    @Test
    public void testTextEmbedding() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("text_embedding/model.onnx"), OnnxTestModels.textEmbedding());
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(textsConfig(model, """
                        - { name: doc, text: "${text}" }
                        - { name: tok, func: tokenize_encode, tokenizer: huggingface, path: "%s", field: doc, maxLength: 2048 }
                        - { name: input_ids, field: tok.ids, pad: 0 }
                        - { name: attention_mask, field: tok.attentionMask, pad: 0 }
                        - { name: image_features, empty: [0, 4] }
                """.formatted(TOKENIZER), """
                        - { name: embedding, func: vector_normalize, field: sentence_embedding }
                """, 3)));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals(List.of("text", "embedding"), output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            final Map<String, List<Float>> byText = new HashMap<>();
            for(final MElement row : rows) {
                byText.put(row.getPrimitiveValue("text").toString(), floats(row.getPrimitiveValue("embedding")));
            }
            Assertions.assertEquals(3, byText.size());
            assertNormalized(10f, 4f, byText.get("Hello world"));
            assertNormalized(19f, 5f, byText.get("onnx pipeline onnx"));
            assertNormalized(6f, 3f, byText.get("hello"));
            return null;
        });
        pipeline.run();
    }

    private static void assertNormalized(final float a, final float b, final List<Float> embedding) {
        final float norm = (float) Math.sqrt(a * a + b * b);
        Assertions.assertEquals(a / norm, embedding.get(0), 1e-6);
        Assertions.assertEquals(b / norm, embedding.get(1), 1e-6);
    }

    // past.%d is expanded to the model inputs; fill takes the shape of the model input, with dim0 the batch and other dynamic dimensions 0
    @Test
    public void testConstantInputs() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("constants/model.onnx"), OnnxTestModels.constants());
        final String config = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0, x: 1.5 }
                        - { id: 1, x: 2.5 }
                    schema:
                      fields:
                        - { name: id, type: int64 }
                        - { name: x, type: float32 }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    parameters:
                      model:
                        path: "%s"
                      inputs:
                        - { name: x }
                        - { name: "past.%%d", fill: 0 }
                        - { name: filled, fill: 7, shape: [2, 2] }
                        - { name: features, empty: [0, 2] }
                      outputs:
                        - { name: y }
                        - { name: past_size, field: "out.0" }
                      bufferSize: 2
                """.formatted(OnnxTestModels.configPath(model));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement row : rows) {
                final long id = ((Number) row.getPrimitiveValue("id")).longValue();
                ids.add(id);
                Assertions.assertEquals(id == 0 ? 1.5f : 2.5f, ((Number) row.getPrimitiveValue("y")).floatValue());
                Assertions.assertEquals(List.of(), floats(row.getPrimitiveValue("past_size")));
            }
            Assertions.assertEquals(Set.of(0L, 1L), ids);
            return null;
        });
        pipeline.run();
    }

    private static Exception assemblyFailure(final TestPipeline pipeline, final Path model, final String inputs, final String outputs) {
        return Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(pipeline, Config.load(textsConfig(model, inputs, outputs, 1))));
    }

    @Test
    public void testConfigErrors() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("config_errors/model.onnx"), OnnxTestModels.tokens());
        final String ids = """
                        - { name: tok, func: tokenize_encode, tokenizer: huggingface, path: "%s", field: text }
                        - { name: ids, field: tok.ids }
                """.formatted(TOKENIZER);
        final String output = """
                        - { name: t, field: total }
                """;
        // a model input not given
        final Exception missing = assemblyFailure(pipeline, model, ids, output);
        Assertions.assertTrue(rootMessages(missing).contains("onnx model inputs are not given by inputs"), rootMessages(missing));
        // a pattern that matches no model input
        final Exception pattern = assemblyFailure(pipeline, model, ids + """
                        - { name: attention_mask, field: tok.attentionMask }
                        - { name: "past_conv.%d", fill: 0 }
                """, output);
        Assertions.assertTrue(rootMessages(pattern).contains("matches no model input"), rootMessages(pattern));
        // no model output referred to
        final Exception noOutput = assemblyFailure(pipeline, model, ids + """
                        - { name: attention_mask, field: tok.attentionMask }
                """, """
                        - { name: t, field: text }
                """);
        Assertions.assertTrue(rootMessages(noOutput).contains("must refer to at least one of the model outputs"), rootMessages(noOutput));
        // rank 0 for an input of rank 2
        final Exception rank = assemblyFailure(pipeline, model, """
                        - { name: ids, field: text }
                        - { name: attention_mask, field: text }
                """, output);
        Assertions.assertTrue(rootMessages(rank).contains("has rank 0"), rootMessages(rank));
    }

    // a value of the rank of the model input is the input as it is, and the whole output is the value of the record
    private static String wholeConfig(final Path model, final int bufferSize) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0, raw: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0] }
                    schema:
                      fields:
                        - { name: id, type: int64 }
                        - { name: raw, type: float32, mode: repeated }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    parameters:
                      model:
                        path: "%s"
                      inputs:
                        - { name: x, func: reshape, field: raw, shape: [3, 2] }
                      outputs:
                        - { name: y }
                      bufferSize: %d
                """.formatted(OnnxTestModels.configPath(model), bufferSize);
    }

    @Test
    public void testInputWithoutBatchDimension() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("whole/model.onnx"), OnnxTestModels.identity(OnnxTestModels.FLOAT, "rows", 2L));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(wholeConfig(model, 1)));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals(List.of(-1, 2), output.getSchema().getField("y").getFieldType().getShape());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(List.of(1f, 2f, 3f, 4f, 5f, 6f), floats(row.getPrimitiveValue("y")));
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testInputWithoutBatchDimensionTakesOneRecord() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("whole_buffer/model.onnx"), OnnxTestModels.identity(OnnxTestModels.FLOAT, "rows", 2L));
        final Exception e = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(pipeline, Config.load(wholeConfig(model, 2))));
        Assertions.assertTrue(rootMessages(e).contains("parameters.bufferSize must be 1"), rootMessages(e));
    }

    // a matrix field is flat in the record: its shape comes from the schema, and each record is a row of the batch
    @Test
    public void testMatrixInputIsStacked() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("matrix_input/model.onnx"), OnnxTestModels.identity(OnnxTestModels.FLOAT, "batch", 3L, 2L));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(wholeConfig(model, 2)));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals(List.of(3, 2), output.getSchema().getField("y").getFieldType().getShape());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(List.of(1f, 2f, 3f, 4f, 5f, 6f), floats(row.getPrimitiveValue("y")));
            }
            return null;
        });
        pipeline.run();
    }

    // the name of a model output comes before a nested field, and only the outputs that are named are requested
    @Test
    public void testDottedOutputName() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("dotted/model.onnx"), OnnxTestModels.constants());
        final String config = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { x: 1.5 }
                    schema:
                      fields:
                        - { name: x, type: float32 }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    parameters:
                      model:
                        path: "%s"
                      inputs:
                        - { name: x }
                        - { name: "past.%%d", fill: 0 }
                        - { name: filled, fill: 7, shape: [1, 2] }
                        - { name: features, empty: [0, 2] }
                      outputs:
                        - { name: past_size, field: "out.0" }
                        - { name: filled_output, field: "out.filled" }
                """.formatted(OnnxTestModels.configPath(model));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                Assertions.assertEquals(List.of(7f, 7f), floats(row.getPrimitiveValue("filled_output")));
            }
            return null;
        });
        pipeline.run();
    }

    // the vector functions take the model output as a Tensor; other functions find it as a list
    @Test
    public void testVectorFunctionsOnModelOutput() throws Exception {
        final Path model = OnnxTestModels.write(BASE_DIR.resolve("vectors/model.onnx"), OnnxTestModels.linear());
        final String config = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 0, x: [1.0, 0.0, 0.0] }
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
                      inputs:
                        - { name: x }
                      outputs:
                        - { name: normalized, func: vector_normalize, field: y }
                        - { name: probs, func: softmax, field: y }
                        - { name: best, func: argmax, field: y }
                        - { name: top, func: topk, field: y, k: 1 }
                        - { name: scored, func: labels, field: y, labels: [a, b] }
                        - { name: squeezed, func: squeeze, field: y3 }
                        - { name: similarity, func: cosine_similarity, left: y, right: y }
                        - { name: y }
                      bufferSize: 2
                """.formatted(OnnxTestModels.configPath(model));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        final MCollection output = outputs.get("onnx");
        Assertions.assertEquals("array", output.getSchema().getField("squeezed").getFieldType().getType().name());
        PAssert.that(output.getCollection()).satisfies(rows -> {
            for(final MElement row : rows) {
                // y = [1.5, 1.5]
                final float root = (float) (1 / Math.sqrt(2));
                Assertions.assertEquals(List.of(root, root), floats(row.getPrimitiveValue("normalized")));
                Assertions.assertEquals(List.of(0.5f, 0.5f), floats(row.getPrimitiveValue("probs")));
                Assertions.assertEquals(0L, ((Number) row.getPrimitiveValue("best")).longValue());
                Assertions.assertEquals(1, ((Collection<?>) row.getPrimitiveValue("top")).size());
                Assertions.assertEquals(2, ((Collection<?>) row.getPrimitiveValue("scored")).size());
                Assertions.assertEquals(List.of(1.5f, 1.5f), floats(row.getPrimitiveValue("squeezed")));
                Assertions.assertEquals(1d, ((Number) row.getPrimitiveValue("similarity")).doubleValue(), 1e-6);
                Assertions.assertEquals(List.of(1.5f, 1.5f), floats(row.getPrimitiveValue("y")));
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
        Assertions.assertTrue(rootMessages(e).contains("parameters.inputs must not be empty"), rootMessages(e));
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

    private static List<Float> floats(final Object value) {
        final List<Float> floats = new ArrayList<>();
        for(final Object v : (Collection<?>) value) {
            floats.add(((Number) v).floatValue());
        }
        return floats;
    }

}
