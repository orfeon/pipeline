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

import java.io.Serializable;
import java.nio.file.Path;
import java.util.*;

/**
 * Text generation on a real model. It runs by hand with {@code LLM_MODEL_DIR}, a directory of
 * onnx-community/granite-4.0-350m-ONNX-web with {@code onnx/model_q4f16.onnx}, its data and {@code tokenizer.json}.
 * {@code LLM_MODEL_FILE} takes another file of the model.
 */
public class OnnxLoopModelTest implements Serializable {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String PROMPT = "<|start_of_role|>user<|end_of_role|>What is the capital of Japan? Answer in one short sentence.<|end_of_text|>\\n<|start_of_role|>assistant<|end_of_role|>";
    private static final int MAX_ITERATIONS = 32;

    private static String config(final String dir, final String next) {
        final String tokenizer = OnnxTestModels.configPath(Path.of(dir, "tokenizer.json"));
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { prompt: "%s" }
                    schema:
                      fields:
                        - { name: prompt, type: string }
                transforms:
                  - name: onnx
                    module: onnx
                    inputs: [create]
                    failFast: true
                    parameters:
                      model:
                        path: "%s"
                        file: %s
                      inputs:
                        - { name: tok, func: tokenize_encode, tokenizer: huggingface, path: "%s", field: prompt, addSpecialTokens: false }
                        - { name: input_ids, field: tok.ids }
                        - { name: attention_mask, field: tok.attentionMask }
                        - { name: position_ids, func: range, field: tok.ids }
                        - { name: "past_key_values.%%d.%%s", fill: 0 }
                      loop:
                        vars:
                          generated: { value: [], type: array<int64> }
                        next:
                %s
                        until: { key: token, op: in, value: [100257] }
                        maxIterations: %d
                      outputs:
                        - { name: generated, field: generated }
                        - { name: text, func: tokenize_decode, tokenizer: huggingface, path: "%s", field: generated, skipSpecialTokens: true }
                """.formatted(PROMPT, OnnxTestModels.configPath(Path.of(dir)), System.getenv().getOrDefault("LLM_MODEL_FILE", "onnx/model_q4f16.onnx"), tokenizer, next, MAX_ITERATIONS, tokenizer);
    }

    private static String dir() {
        final String dir = System.getenv("LLM_MODEL_DIR");
        Assumptions.assumeTrue(dir != null, "LLM_MODEL_DIR is not set");
        return dir;
    }

    private static final String TOKEN = """
                          token: { func: argmax, field: logits, position: last }
                          generated: { func: array_append, field: generated, valueField: token }
            """;

    private void run(final String next, final String label) throws Exception {
        final long start = System.nanoTime();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(config(dir(), next)));
        PAssert.that(outputs.get("onnx").getCollection()).satisfies(rows -> {
            final MElement row = rows.iterator().next();
            final String text = row.getAsString("text");
            System.out.println("llm " + label + ": " + row.getPrimitiveValue("generated") + " " + text);
            Assertions.assertTrue(text.contains("Tokyo"), text);
            return null;
        });
        pipeline.run();
        System.out.println("llm " + label + ": " + (System.nanoTime() - start) / 1_000_000 + " ms");
    }

    // the KV cache of float16 goes from one run to the next as it is
    @Test
    public void testWithCache() throws Exception {
        run("""
                          "past_key_values.%d.%s": "present.%d.%s"
            """ + TOKEN + """
                          input_ids: { func: array, fields: [token] }
                          attention_mask: { func: array_append, field: attention_mask, value: 1 }
                          position_ids: { func: range, field: attention_mask, last: 1 }
            """, "cache");
    }

    // the whole sequence every run, with an empty cache: the tokens must be those of the run with a cache
    @Test
    public void testWithoutCache() throws Exception {
        run(TOKEN + """
                          input_ids: { func: array_append, field: input_ids, valueField: token }
                          attention_mask: { func: array_append, field: attention_mask, value: 1 }
                          position_ids: { func: range, field: attention_mask }
            """, "no cache");
    }

}
