package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.TensorInfo;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OnnxModelTest {

    // the same as the session reports, for what it does not create a session for
    @Test
    public void testGraphInfoMatchesTheSession(@TempDir final Path dir) throws Exception {
        final Map<String, byte[]> models = Map.of(
                "linear", OnnxTestModels.linear(),
                "tokens", OnnxTestModels.tokens(),
                "constants", OnnxTestModels.constants(),
                "bool", OnnxTestModels.cast(OnnxTestModels.BOOL),
                "uint8", OnnxTestModels.cast(OnnxTestModels.UINT8),
                "float16", OnnxTestModels.cast(OnnxTestModels.FLOAT16),
                "bfloat16", OnnxTestModels.cast(OnnxTestModels.BFLOAT16),
                "scalar", OnnxTestModels.scalarOutput(),
                "double", OnnxTestModels.identity(OnnxTestModels.DOUBLE, "n", 2L));
        for(final Map.Entry<String, byte[]> entry : models.entrySet()) {
            final Path path = OnnxTestModels.write(dir.resolve(entry.getKey() + ".onnx"), entry.getValue());
            final OnnxModel model = OnnxModel.create(OnnxModel.Config.create(OnnxTestModels.configPath(path)));
            final OnnxGraphInfo graph = model.readGraphInfo();
            assertSameInfo(entry.getKey(), model.getInputInfo(), graph.inputs());
            assertSameInfo(entry.getKey(), model.getOutputInfo(), graph.outputs());
        }
    }

    private static void assertSameInfo(final String model, final Map<String, NodeInfo> session, final Map<String, OnnxGraphInfo.ValueInfo> read) {
        Assertions.assertEquals(session.keySet(), read.keySet(), model);
        for(final Map.Entry<String, NodeInfo> entry : session.entrySet()) {
            final TensorInfo info = (TensorInfo) entry.getValue().getInfo();
            final OnnxGraphInfo.ValueInfo value = read.get(entry.getKey());
            Assertions.assertEquals(info.type, value.type(), model + "." + entry.getKey());
            Assertions.assertArrayEquals(info.getShape(), value.shape(), model + "." + entry.getKey());
        }
    }

    // the model file of a GCS directory is only found and read: no weights are downloaded
    @Test
    public void testGraphInfoFromGcsDirectory() throws Exception {
        final byte[] bytes = OnnxTestModels.linear();
        final List<String> requested = new ArrayList<>();
        final MockHttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(final String method, final String url) {
                return new MockLowLevelHttpRequest(url) {
                    @Override
                    public MockLowLevelHttpResponse execute() {
                        final MockLowLevelHttpResponse response = new MockLowLevelHttpResponse().setStatusCode(200);
                        if(url.contains("alt=media")) {
                            requested.add(url.replaceAll(".*/o/([^?]*)\\?.*", "$1"));
                            return response.setContent(bytes);
                        }
                        return response
                                .setContentType("application/json; charset=UTF-8")
                                .setContent("{\"items\":[{\"bucket\":\"b\",\"name\":\"m/\"},{\"bucket\":\"b\",\"name\":\"m/onnx/model.onnx\"},{\"bucket\":\"b\",\"name\":\"m/onnx/model.onnx_data\"},{\"bucket\":\"b\",\"name\":\"m/tokenizer.json\"}]}");
                    }
                };
            }
        };
        final Storage storage = new Storage.Builder(transport, GsonFactory.getDefaultInstance(), null).setApplicationName("test").build();

        final OnnxGraphInfo graph = OnnxModel.readGraphInfo(storage, "gs://b/m/", null);
        Assertions.assertEquals(Set.of("x"), graph.inputs().keySet());
        Assertions.assertEquals(List.of("m%2Fonnx%2Fmodel.onnx"), requested);
    }

    @Test
    public void testExpandName() {
        final List<String> names = List.of("x", "past_conv.0", "past_conv.1", "past_key_values.0.key", "past_key_values.0.value", "past_conv.a");
        Assertions.assertEquals(List.of("x"), OnnxInputs.expandName("x", names));
        Assertions.assertEquals(List.of("past_conv.0", "past_conv.1"), OnnxInputs.expandName("past_conv.%d", names));
        Assertions.assertEquals(List.of("past_key_values.0.key", "past_key_values.0.value"), OnnxInputs.expandName("past_key_values.%d.%s", names));
        // the dot is not a wildcard
        Assertions.assertThrows(IllegalArgumentException.class, () -> OnnxInputs.expandName("past_conv_%d", names));
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
