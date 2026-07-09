package com.mercari.solution.util.domain.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class ResourceUtilTest {

    @Test
    public void testIsStorageUri() {
        Assertions.assertTrue(ResourceUtil.isStorageUri("gs://bucket/path/file.txt"));
        Assertions.assertTrue(ResourceUtil.isStorageUri("s3://bucket/path/file.txt"));
        Assertions.assertFalse(ResourceUtil.isStorageUri("/local/path/file.txt"));
        Assertions.assertFalse(ResourceUtil.isStorageUri("projects/p/secrets/s/versions/latest"));
        Assertions.assertFalse(ResourceUtil.isStorageUri(null));
    }

    @Test
    public void testWriteReadRoundTrip(@TempDir final Path tempDir) {
        final String path = tempDir.resolve("resource.txt").toString();
        ResourceUtil.writeString(path, "hello resource");
        Assertions.assertEquals("hello resource", ResourceUtil.readString(path));

        final String bytesPath = tempDir.resolve("resource.bin").toString();
        final byte[] content = "binary content".getBytes(StandardCharsets.UTF_8);
        ResourceUtil.writeBytes(bytesPath, content);
        Assertions.assertArrayEquals(content, ResourceUtil.readBytes(bytesPath));
    }

    @Test
    public void testExists(@TempDir final Path tempDir) {
        final String path = tempDir.resolve("checkpoint.txt").toString();
        Assertions.assertFalse(ResourceUtil.exists(path));
        ResourceUtil.writeString(path, "2026-07-08T00:00:00Z");
        Assertions.assertTrue(ResourceUtil.exists(path));
    }

    @Test
    public void testReadMissingFileThrows(@TempDir final Path tempDir) {
        final String path = tempDir.resolve("missing.txt").toString();
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class, () ->
                ResourceUtil.readString(path));
        Assertions.assertTrue(e.getMessage().contains(path));
    }

}
