package com.mercari.solution.util.domain.ml.onnx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class OnnxModelTest {

    @Test
    public void testResolveModelFile(@TempDir final Path dir) throws Exception {
        final Path single = dir.resolve("single");
        OnnxTestModels.write(single.resolve("onnx/model.onnx"), OnnxTestModels.linear());
        OnnxTestModels.write(single.resolve("tokenizer.json"), new byte[0]);
        // the only .onnx file under the directory, also in a sub directory
        Assertions.assertEquals(single.resolve("onnx/model.onnx"), OnnxModel.resolveModelFile(single, null));

        final Path multiple = dir.resolve("multiple");
        OnnxTestModels.write(multiple.resolve("onnx/model.onnx"), OnnxTestModels.linear());
        OnnxTestModels.write(multiple.resolve("onnx/model_q4.onnx"), OnnxTestModels.linear());
        Assertions.assertEquals(multiple.resolve("onnx/model_q4.onnx"), OnnxModel.resolveModelFile(multiple, "onnx/model_q4.onnx"));
        final IllegalArgumentException ambiguous = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OnnxModel.resolveModelFile(multiple, null));
        Assertions.assertTrue(ambiguous.getMessage().contains("[onnx/model.onnx, onnx/model_q4.onnx]"), ambiguous.getMessage());

        final IllegalArgumentException missing = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OnnxModel.resolveModelFile(multiple, "onnx/missing.onnx"));
        Assertions.assertTrue(missing.getMessage().contains("onnx/missing.onnx"), missing.getMessage());
    }

}
