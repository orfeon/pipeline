package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.ml.onnx.OnnxTestModels;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * The lines of a page read by a set of OCR models. It runs by hand with {@code OCR_MODEL_DIR}, a directory of
 * datalab-to/indra-ocr at v1.1.0 with det.onnx, script.onnx, rec.onnx, rec_cjk.onnx, rec_rtl.onnx, rec_world.onnx and meta.json,
 * and {@code OCR_IMAGE_DIR}, a directory of the images of the repository datalab-to/indra. The license of the weights
 * limits their use: they are not in this repository, nor is what they give.
 */
public class OnnxOcrModelTest implements Serializable {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String ink(final String field) {
        return """
                        - { name: decoded, func: image_decode, field: %s }
                        - { name: gray, func: image_gray, field: decoded }
                        - { name: size, func: image_size, field: gray }
                        - { name: x, func: image_tensor, field: gray, invert: true, pad: { value: 0, multiple: 8 } }
                        - { name: widths, field: size.width }
                """.formatted(field);
    }

    // a recognizer of a group: the base one reads a tall window of the page around the line
    private static String recognizer(final String group, final String dir) {
        final boolean base = "base".equals(group);
        return """
                  - name: rec_%s
                    module: onnx
                    inputs: ["route.%s"]
                    failFast: true
                    parameters:
                      model: { path: "%s", file: %s }
                      bufferSize: 4
                      inputs:
                %s
                      outputs:
                        - { name: read, func: ctc_decode, field: ids, confField: conf, lengthField: lengths, path: "%s", key: %s, normalize: NFC, minConf: 0.5 }
                        - { name: text, field: read.text }
                        - { name: confidence, field: read.conf }
                """.formatted(group, group, dir, base ? "rec.onnx" : "rec_" + group + ".onnx",
                ink(base ? "line.tall" : "line.band"), Path.of(dir, "meta.json").toUri(), base ? "vocab" : "vocab_" + group);
    }

    private static String config(final String dir, final byte[] image) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: page, image: "%s" }
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
                      model: { path: "%s", file: det.onnx }
                      inputs:
                        - { name: content, func: base64_decode, field: image, type: bytes }
                        - { name: decoded, func: image_decode, field: content }
                        - { name: page, func: image_gray, field: decoded }
                        - { name: small, func: image_resize, field: page, longSide: 1024 }
                        - { name: padded, func: image_pad, field: small, multiple: 32, value: 255 }
                        - { name: x, func: image_tensor, field: padded, invert: true }
                      outputs:
                        - { name: boxes, func: text_line_boxes, field: out, imageField: page, resizedField: small, stride: 2, xShrink: 0.15, minArea: 2 }
                        - { name: bands, func: image_crop, field: page, boxes: boxes, height: 32, padX: 0.3, padY: 0.2, invertDark: true }
                        - { name: windows, func: image_crop, field: page, boxes: boxes, height: 96, padX: 0.3, padY: 1.6, invertDark: true }
                        - { name: numbers, func: range, field: boxes }
                  - name: lines
                    module: select
                    inputs: [detect]
                    parameters:
                      select:
                        - { name: id, field: id }
                        - { name: line, func: array_zip, fields: [{ name: number, field: numbers }, { name: box, field: boxes }, { name: band, field: bands }, { name: tall, field: windows }] }
                      flattenField: line
                  - name: script
                    module: onnx
                    inputs: [lines]
                    failFast: true
                    parameters:
                      model: { path: "%s", file: script.onnx }
                      bufferSize: 4
                      inputs:
                %s
                      outputs:
                        - { name: best, func: topk, field: p, k: 1 }
                        - { name: first, func: slice, field: best, index: 0 }
                        - { name: confidence, field: first.value }
                        - { name: index, field: first.index }
                        - { name: group, func: labels, field: index, labels: [base, cjk, rtl, world] }
                  - name: route
                    module: partition
                    inputs: [script]
                    parameters:
                      exclusive: false
                      partitions:
                        - name: base
                          filter: { or: [{ key: group, op: "=", value: base }, { key: confidence, op: "<=", value: 0.95 }] }
                        - name: cjk
                          filter: { key: group, op: "=", value: cjk }
                        - name: rtl
                          filter: { key: group, op: "=", value: rtl }
                        - name: world
                          filter: { key: group, op: "=", value: world }
                %s
                  - name: read
                    module: select
                    inputs: [rec_base, rec_cjk, rec_rtl, rec_world]
                    parameters:
                      select:
                        - { name: id, field: id }
                        - { name: number, field: line.number }
                        - { name: text, field: text }
                        - { name: confidence, field: confidence }
                  - name: best
                    module: aggregation
                    inputs: [read]
                    parameters:
                      groupFields: [id, number]
                      aggregations:
                        - input: read
                          fields:
                            - { name: text, op: arg_max, field: text, comparingField: confidence }
                """.formatted(Base64.getEncoder().encodeToString(image), dir, dir, ink("line.band"),
                recognizer("base", dir) + recognizer("cjk", dir) + recognizer("rtl", dir) + recognizer("world", dir));
    }

    private void read(final String file, final List<String> expected) throws IOException {
        final String models = System.getenv("OCR_MODEL_DIR");
        final String images = System.getenv("OCR_IMAGE_DIR");
        Assumptions.assumeTrue(models != null && images != null, "OCR_MODEL_DIR and OCR_IMAGE_DIR are not set");
        final long start = System.nanoTime();
        final Config config = Config.load(config(OnnxTestModels.configPath(Path.of(models)), Files.readAllBytes(Path.of(images, file))));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("best").getCollection()).satisfies(rows -> {
            final Map<Long, String> lines = new TreeMap<>();
            for(final MElement row : rows) {
                lines.put((Long) row.getPrimitiveValue("number"), row.getAsString("text"));
            }
            final String text = String.join("\n", lines.values());
            System.out.println("ocr " + file + ": " + lines.size() + " lines\n" + text);
            for(final String phrase : expected) {
                Assertions.assertTrue(text.contains(phrase), phrase + " in:\n" + text);
            }
            return null;
        });
        pipeline.run();
        System.out.println("ocr " + file + ": " + (System.nanoTime() - start) / 1_000_000 + " ms");
    }

    @Test
    public void testLatin() throws IOException {
        read("multi_column.png", List.of("Geological setting", "Continental rifting and breakup", "Borborema"));
    }

    // Korean through rec_cjk with its jamo composed, next to the English of the same page through rec
    @Test
    public void testKorean() throws IOException {
        read("scan.png", List.of("DIOCESE OF SEOUL", "주님의 평화", "코로나19", "전화 (02)735-6157 / 팩스 (02)723-2640"));
    }

    @Test
    public void testChinese() throws IOException {
        read("chinese_essay.png", List.of("CVPR", "老板"));
    }

}
