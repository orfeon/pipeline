package com.mercari.solution.util.cloud.google;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class StorageUtilTest {

    // answers the objects list with the given names, and a download with the name of the requested object
    private static Storage storage(final List<String> names) {
        final String items = String.join(",", names.stream()
                .map(name -> "{\"bucket\":\"bucket\",\"name\":\"" + name + "\"}")
                .toList());
        final MockHttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(final String method, final String url) {
                return new MockLowLevelHttpRequest(url) {
                    @Override
                    public MockLowLevelHttpResponse execute() {
                        final MockLowLevelHttpResponse response = new MockLowLevelHttpResponse().setStatusCode(200);
                        if(url.contains("alt=media")) {
                            return response.setContent(url.replaceAll(".*/o/([^?]*)\\?.*", "$1"));
                        }
                        return response
                                .setContentType("application/json; charset=UTF-8")
                                .setContent("{\"items\":[" + items + "]}");
                    }
                };
            }
        };
        return new Storage.Builder(transport, GsonFactory.getDefaultInstance(), null)
                .setApplicationName("test")
                .build();
    }

    @Test
    public void testDownloadDirectoryKeepsLayout(@TempDir final Path dir) throws Exception {
        final Storage storage = storage(List.of(
                "models/m/", "models/m/onnx/", "models/m/model.onnx", "models/m/onnx/model.onnx", "models/m/onnx/model.onnx_data"));
        StorageUtil.downloadDirectory(storage, "gs://bucket/models/m/", dir, name -> true);

        Assertions.assertEquals("models%2Fm%2Fmodel.onnx", Files.readString(dir.resolve("model.onnx")));
        // a file of the same name in a sub directory stays apart
        Assertions.assertEquals("models%2Fm%2Fonnx%2Fmodel.onnx", Files.readString(dir.resolve("onnx/model.onnx")));
        Assertions.assertTrue(Files.isRegularFile(dir.resolve("onnx/model.onnx_data")));
    }

    @Test
    public void testDownloadDirectoryFilter(@TempDir final Path dir) throws Exception {
        final Storage storage = storage(List.of("models/m/tokenizer.json", "models/m/v1/tokenizer.json", "models/m/model.onnx"));
        StorageUtil.downloadDirectory(storage, "gs://bucket/models/m/", dir,
                name -> !name.contains("/") && name.endsWith(".json"));

        Assertions.assertEquals("models%2Fm%2Ftokenizer.json", Files.readString(dir.resolve("tokenizer.json")));
        Assertions.assertFalse(Files.exists(dir.resolve("v1")));
        Assertions.assertFalse(Files.exists(dir.resolve("model.onnx")));
    }

    @Test
    public void testDownloadDirectoryRejectsObjectOutside(@TempDir final Path dir) {
        final Storage storage = storage(List.of("models/m/../../../outside.onnx"));
        final Path localDir = dir.resolve("local");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageUtil.downloadDirectory(storage, "gs://bucket/models/m/", localDir, name -> true));
        Assertions.assertFalse(Files.exists(dir.resolve("outside.onnx")));
    }

}
