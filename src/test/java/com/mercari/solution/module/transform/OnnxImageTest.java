package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.image.ImageTensors;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.domain.ml.onnx.OnnxTestModels;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Images through the onnx transform, with the steps of line detection and recognition. The detector is an identity model on
 * a color image, whose red, green and blue are the logit and the two distances that a model would give.
 */
public class OnnxImageTest implements Serializable {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final Path DIR = Path.of("target", "onnx-image-test");

    // a band in rows 2 and 3 of an output at half the size of the page, with distances that make a line from 2 to 10
    private static String detectorImage() {
        final int height = 6;
        final int width = 16;
        final short[] data = new short[height * width * 3];
        for(int y=2; y<=3; y++) {
            for(int x=2; x<=11; x++) {
                final int p = (y * width + x) * 3;
                final int center = 2 * y + 1;
                data[p] = 100;
                data[p + 1] = (short) (center - 2);
                data[p + 2] = (short) (10 - center);
            }
        }
        return Base64.getEncoder().encodeToString(ImageTensors.encode(Tensor.of(data, new long[]{height, width, 3})));
    }

    private static String config(final Path detector, final Path recognizer) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: a, image: "%s" }
                    schema:
                      fields:
                        - { name: id, type: string }
                        - { name: image, type: string }
                transforms:
                  - name: detect
                    module: onnx
                    inputs: [create]
                    failFast: true
                    parameters:
                      model:
                        path: "%s"
                      inputs:
                        - { name: content, func: base64_decode, field: image, type: bytes }
                        - { name: decoded, func: image_decode, field: content }
                        - { name: page, func: image_gray, field: decoded }
                        - { name: x, func: image_tensor, field: decoded, scale: 1 }
                      outputs:
                        - { name: boxes, func: text_line_boxes, field: y, imageField: page, resizedField: page, stride: 2, xShrink: 0.15 }
                        - { name: components, func: connected_components, field: page, threshold: 10 }
                        - { name: size, func: image_size, field: page }
                        - { name: lines, func: image_crop, field: page, boxes: boxes, height: 8, padX: 0.3, padY: 0.2, invertDark: true }
                        - { name: encoded, func: image_encode, field: page }
                  - name: flatten
                    module: select
                    inputs: [detect]
                    parameters:
                      select:
                        - { name: id, field: id }
                        - { name: line, func: array_zip, fields: [{ name: box, field: boxes }, { name: image, field: lines }] }
                      flattenField: line
                  - name: recognize
                    module: onnx
                    inputs: [flatten]
                    failFast: true
                    parameters:
                      model:
                        path: "%s"
                      inputs:
                        - { name: decoded, func: image_decode, field: line.image }
                        - { name: gray, func: image_gray, field: decoded }
                        - { name: small, func: image_resize, field: gray, height: 4, width: 6 }
                        - { name: padded, func: image_pad, field: small, multiple: 8, value: 255 }
                        - { name: x, func: image_tensor, field: padded, invert: true }
                      outputs:
                        - { name: output, field: y }
                        - { name: size, func: image_size, field: padded }
                """.formatted(detectorImage(), OnnxTestModels.configPath(detector), OnnxTestModels.configPath(recognizer));
    }

    @Test
    public void testDetectAndRecognizeLines() throws Exception {
        final Path detector = OnnxTestModels.write(DIR.resolve("detector.onnx"), OnnxTestModels.identity(OnnxTestModels.FLOAT, "batch", 3L, "height", "width"));
        final Path recognizer = OnnxTestModels.write(DIR.resolve("recognizer.onnx"), OnnxTestModels.identity(OnnxTestModels.FLOAT, "batch", 1L, "height", "width"));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(detector, recognizer)));

        PAssert.that(outputs.get("detect").getCollection()).satisfies(rows -> {
            final MElement row = rows.iterator().next();
            final List<?> boxes = (List<?>) row.getPrimitiveValue("boxes");
            Assertions.assertEquals(1, boxes.size());
            final Map<?, ?> box = (Map<?, ?>) boxes.getFirst();
            Assertions.assertEquals(2f, ((Number) box.get("y0")).floatValue(), 1e-4f);
            Assertions.assertEquals(6f, ((Number) box.get("y1")).floatValue(), "the bottom is clipped to the page");
            Assertions.assertEquals(4f - 0.15f * 8f, ((Number) box.get("x0")).floatValue(), 1e-4f);
            Assertions.assertEquals(16f, ((Number) box.get("x1")).floatValue(), "the right is clipped to the page");

            final List<?> components = (List<?>) row.getPrimitiveValue("components");
            Assertions.assertEquals(1, components.size());
            Assertions.assertEquals(2, ((Map<?, ?>) components.getFirst()).get("x"));
            Assertions.assertEquals(10, ((Map<?, ?>) components.getFirst()).get("width"));

            final Map<?, ?> size = (Map<?, ?>) row.getPrimitiveValue("size");
            Assertions.assertEquals(6L, size.get("height"));
            Assertions.assertEquals(16L, size.get("width"));

            final List<?> lines = (List<?>) row.getPrimitiveValue("lines");
            Assertions.assertEquals(1, lines.size());
            final Tensor line = ImageTensors.decode(((ByteBuffer) lines.getFirst()).array(), 1000);
            Assertions.assertEquals(8L, line.shape()[0], "the height of the line");

            final Tensor encoded = ImageTensors.decode(((ByteBuffer) row.getPrimitiveValue("encoded")).array(), 1000);
            Assertions.assertArrayEquals(new long[]{6, 16, 1}, encoded.shape());
            return null;
        });

        PAssert.that(outputs.get("recognize").getCollection()).satisfies(rows -> {
            final MElement row = rows.iterator().next();
            Assertions.assertEquals("a", row.getAsString("id"));
            final Map<?, ?> size = (Map<?, ?>) row.getPrimitiveValue("size");
            Assertions.assertEquals(8L, size.get("height"));
            Assertions.assertEquals(8L, size.get("width"));
            final List<?> output = (List<?>) row.getPrimitiveValue("output");
            Assertions.assertEquals(64, output.size(), "1 x 8 x 8 values through the model");
            Assertions.assertEquals(0f, ((Number) output.getLast()).floatValue(), "the padding of paper has no ink");
            return null;
        });
        pipeline.run();
    }

    // the vocabulary travels in the function: it is read when the pipeline is built
    @Test
    public void testCtcDecodeOfAVocabularyFileInASelect() throws Exception {
        Files.createDirectories(DIR);
        final Path vocabulary = Files.writeString(DIR.resolve("vocabulary.json"), "{ \"blank\": 2, \"chars\": [\"a\", \"b\"] }");
        final String config = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: a, ids: [0, 2, 1] }
                    schema:
                      fields:
                        - { name: id, type: string }
                        - { name: ids, type: int64, mode: repeated }
                transforms:
                  - name: read
                    module: select
                    inputs: [create]
                    parameters:
                      select:
                        - { name: read, func: ctc_decode, field: ids, path: "%s" }
                        - { name: text, field: read.text }
                """.formatted(vocabulary.toUri());
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config));
        PAssert.that(outputs.get("read").getCollection()).satisfies(rows -> {
            Assertions.assertEquals("ab", rows.iterator().next().getAsString("text"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testOnlyInTheOnnxTransform() {
        final String config = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: a }
                    schema:
                      fields:
                        - { name: id, type: string }
                        - { name: content, type: bytes }
                transforms:
                  - name: select
                    module: select
                    inputs: [create]
                    parameters:
                      select:
                        - { name: decoded, func: image_decode, field: content }
                """;
        final Exception e = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(TestPipeline.create().enableAbandonedNodeEnforcement(false), Config.load(config)));
        final String messages = messages(e);
        Assertions.assertTrue(messages.contains("only works in the inputs and the outputs of the onnx transform"), messages);
    }

    private static String messages(final Throwable throwable) {
        final StringBuilder messages = new StringBuilder();
        for(Throwable t = throwable; t != null; t = t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }
        return messages.toString();
    }

    private static void assertConfigError(final String inputs, final String message) {
        final Path model = OnnxTestModels.write(DIR.resolve("error.onnx"), OnnxTestModels.identity(OnnxTestModels.FLOAT, "batch", 1L, "height", "width"));
        assertConfigError(model, inputs, "- { name: output, field: y }", message);
    }

    private static void assertConfigError(final Path model, final String inputs, final String outputs, final String message) {
        final String config = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: a }
                    schema:
                      fields:
                        - { name: id, type: string }
                        - { name: content, type: bytes }
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
                """.formatted(OnnxTestModels.configPath(model), inputs.indent(8), outputs.indent(8));
        final Exception e = Assertions.assertThrows(Exception.class,
                () -> MPipeline.apply(TestPipeline.create().enableAbandonedNodeEnforcement(false), Config.load(config)), message);
        final String messages = messages(e);
        Assertions.assertTrue(messages.contains(message), message + ": " + messages);
    }

    @Test
    public void testConfigErrors() {
        final String decoded = "- { name: i, func: image_decode, field: content }\n";
        final String gray = decoded + "- { name: g, func: image_gray, field: i }\n";
        final Map<String, String> cases = new LinkedHashMap<>();
        cases.put("- { name: i, func: image_decode, field: id }", "must be bytes");
        cases.put("- { name: i, func: image_decode }", "requires field");
        cases.put("- { name: i, func: image_decode, field: content, maxPixels: 0 }", "maxPixels must be over than zero");
        cases.put("- { name: g, func: image_gray, field: id }", "must be a matrix of rank");
        cases.put(decoded + "- { name: r, func: image_resize, field: i }", "requires one of longSide");
        cases.put(decoded + "- { name: r, func: image_resize, field: i, longSide: 8, shortSide: 8 }", "requires one of longSide");
        cases.put(decoded + "- { name: r, func: image_resize, field: i, width: 8 }", "both of width and height");
        cases.put(decoded + "- { name: r, func: image_resize, field: i, longSide: 0 }", "longSide must be over than zero");
        cases.put(decoded + "- { name: r, func: image_resize, field: i, longSide: 8, interpolation: cubic }", "SelectField: r func: image_resize interpolation must be one of");
        cases.put(decoded + "- { name: p, func: image_pad, field: i }", "requires multiple, width or height");
        cases.put(decoded + "- { name: t, func: image_tensor, field: i, layout: nchw }", "layout must be chw or hwc");
        cases.put(decoded + "- { name: t, func: image_tensor, field: i, mean: [] }", "mean must be a number");
        cases.put(gray + "- { name: t, func: image_tensor, field: g, mean: [0.4, 0.5, 0.6] }", "one value or one per channel");
        cases.put(decoded + "- { name: t, func: image_tensor, field: i, std: [0.2, 0] }", "std must not be zero");
        cases.put(decoded + "- { name: c, func: connected_components, field: i }", "must be a matrix of rank [2]");
        cases.put(gray + "- { name: c, func: connected_components, field: g, connectivity: 6 }", "connectivity must be 4 or 8");
        cases.put(decoded + "- { name: c, func: image_crop, field: i, height: 8 }", "requires boxes");
        cases.put(decoded + "- { name: c, func: image_crop, field: i, boxes: id }", "requires height");
        cases.put(decoded + "- { name: c, func: image_crop, field: i, boxes: id, height: 8 }", "must be an array of x0");
        cases.put(decoded + "- { name: c, func: image_crop, field: i, boxes: id, height: 8, minWidth: 100, maxWidth: 50 }", "must not be over maxWidth");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: i, imageField: g }", "requires imageField and resizedField");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: i, resizedField: g }", "requires imageField and resizedField");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: g, imageField: g, resizedField: g }", "must be a matrix of rank [3]");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: i, imageField: g, resizedField: id }", "must be a matrix of rank");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: i, imageField: g, resizedField: g }", "requires stride and xShrink");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: i, imageField: g, resizedField: g, stride: 2 }", "requires stride and xShrink");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: i, imageField: g, resizedField: g, xShrink: 0.1 }", "requires stride and xShrink");
        cases.put(gray + "- { name: b, func: text_line_boxes, field: i, imageField: g, resizedField: g, stride: 2, xShrink: -1 }", "xShrink must not be negative");
        cases.put("- { name: i, func: image_decode, field: content, channels: 2 }", "channels must be 1 or 3");
        cases.put("- { name: i, func: image_decode, field: content, channels: 1 }\n- { name: t, func: image_tensor, field: i, mean: [1, 2, 3] }", "mean and std must have one value or one per channel");
        for(final Map.Entry<String, String> entry : cases.entrySet()) {
            assertConfigError(entry.getKey(), entry.getValue());
        }
    }

    @Test
    public void testAMaskOfBooleansIsAConfigError() {
        final Path model = OnnxTestModels.write(DIR.resolve("mask.onnx"), OnnxTestModels.identity(OnnxTestModels.BOOL, "batch", "height", "width"));
        assertConfigError(model,
                "- { name: i, func: image_decode, field: content }\n- { name: x, func: image_gray, field: i }\n",
                "- { name: c, func: connected_components, field: y }\n",
                "must be a matrix of numbers");
    }

    @Test
    public void testNotInsideAnotherFunction() {
        final Path model = OnnxTestModels.write(DIR.resolve("struct.onnx"), OnnxTestModels.identity(OnnxTestModels.FLOAT, "batch", 1L, "height", "width"));
        assertConfigError(model,
                "- { name: i, func: image_decode, field: content }\n- { name: g, func: image_gray, field: i }\n- { name: x, func: image_tensor, field: g }\n",
                "- { name: output, field: y }\n- { name: s, fields: [{ name: size, func: image_size, field: g }] }\n",
                "not inside another function");
    }

}
