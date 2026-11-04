package com.mercari.solution.util.domain.pdf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class PdfResourcesTest {

    @Test
    public void testSharedPerOwnerAndReleased() throws IOException {
        final Path dir = Path.of("target/pdf-resources-test");
        Files.createDirectories(dir);
        final String path = dir.resolve("font.bin").toString().replace('\\', '/');
        Files.writeString(Path.of(path), "v1", StandardCharsets.UTF_8);

        // two threads of one module share the bytes
        final PdfResources thread1 = new PdfResources("owner-a");
        final PdfResources thread2 = new PdfResources("owner-a");
        final byte[] bytes = thread1.read(path);
        Assertions.assertSame(bytes, thread2.read(path));
        // reading again in the same thread holds no extra reference
        Assertions.assertSame(bytes, thread1.read(path));

        // another module (or a later pipeline) reads its own copy
        final PdfResources other = new PdfResources("owner-b");
        Assertions.assertNotSame(bytes, other.read(path));
        other.release();

        // the bytes stay while a thread still holds them
        Files.writeString(Path.of(path), "v2", StandardCharsets.UTF_8);
        thread1.release();
        final PdfResources thread3 = new PdfResources("owner-a");
        Assertions.assertSame(bytes, thread3.read(path));

        // released by all: the next read sees the changed file
        thread2.release();
        thread3.release();
        final PdfResources thread4 = new PdfResources("owner-a");
        Assertions.assertEquals("v2", new String(thread4.read(path), StandardCharsets.UTF_8));
        thread4.release();
    }

}
