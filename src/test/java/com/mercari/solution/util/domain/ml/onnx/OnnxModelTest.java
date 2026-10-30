package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.TensorInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class OnnxModelTest {

    // a batch for ids int64[batch, length], with the given number of ids in each record
    private static List<Object> batch(final int... lengths) {
        final List<Object> records = new ArrayList<>();
        for(final int length : lengths) {
            final List<Object> ids = new ArrayList<>();
            for(int i=0; i<length; i++) {
                ids.add((long) i);
            }
            records.add(ids);
        }
        return records;
    }

    @Test
    public void testConvertTensorChecksEveryRecordLength(@TempDir final Path dir) throws Exception {
        final Path path = OnnxTestModels.write(dir.resolve("tokens.onnx"), OnnxTestModels.tokens());
        final OnnxModel model = OnnxModel.create(OnnxModel.Config.create(OnnxTestModels.configPath(path)));
        final TensorInfo ids = (TensorInfo) model.getInputInfo().get("ids").getInfo();
        final OrtEnvironment environment = OrtEnvironment.getEnvironment();

        try(final OnnxTensor tensor = OnnxRuntimeUtil.convertTensor(environment, ids, batch(3, 3, 3))) {
            Assertions.assertArrayEquals(new long[]{3, 3}, tensor.getInfo().getShape());
        }
        // the total is the same as a [3, 3] tensor: comparing only the total would misalign the rows
        final IllegalArgumentException compensating = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OnnxRuntimeUtil.convertTensor(environment, ids, batch(3, 2, 4)));
        Assertions.assertTrue(compensating.getMessage().contains("same length"), compensating.getMessage());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> OnnxRuntimeUtil.convertTensor(environment, ids, batch(3, 2)));
    }

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
