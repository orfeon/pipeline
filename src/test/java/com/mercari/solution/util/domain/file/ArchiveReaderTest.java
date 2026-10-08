package com.mercari.solution.util.domain.file;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ArchiveReaderTest {

    @TempDir
    Path tempDir;

    private static byte[] zip(final Map<String, byte[]> entries, final Charset nameCharset) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final ZipOutputStream zip = new ZipOutputStream(bytes, nameCharset)) {
            for(final Map.Entry<String, byte[]> entry : entries.entrySet()) {
                zip.putNextEntry(new ZipEntry(entry.getKey()));
                if(entry.getValue() != null) {
                    zip.write(entry.getValue());
                }
                zip.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] tar(final Map<String, byte[]> entries) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final TarArchiveOutputStream tar = new TarArchiveOutputStream(bytes)) {
            for(final Map.Entry<String, byte[]> entry : entries.entrySet()) {
                final TarArchiveEntry tarEntry = new TarArchiveEntry(entry.getKey());
                if(entry.getValue() != null) {
                    tarEntry.setSize(entry.getValue().length);
                }
                tar.putArchiveEntry(tarEntry);
                if(entry.getValue() != null) {
                    tar.write(entry.getValue());
                }
                tar.closeArchiveEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static Map<String, byte[]> entries() {
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("data/", null);
        entries.put("data/a.txt", "alpha".getBytes(StandardCharsets.UTF_8));
        entries.put("data/sub/b.txt", "beta".getBytes(StandardCharsets.UTF_8));
        entries.put("c.csv", "gamma".getBytes(StandardCharsets.UTF_8));
        entries.put("./d.txt", "delta".getBytes(StandardCharsets.UTF_8));
        return entries;
    }

    private static List<String> read(final ArchiveReader reader, final Predicate<String> filter) throws IOException {
        final List<String> contents = new ArrayList<>();
        try(reader) {
            while(reader.next(filter)) {
                contents.add(reader.name() + "=" + new String(reader.stream().readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        return contents;
    }

    private SeekableByteChannel channel(final String name, final byte[] content) throws IOException {
        final Path file = tempDir.resolve(name);
        Files.write(file, content);
        return Files.newByteChannel(file);
    }

    @Test
    public void testZipCentralDirectoryAndStream() throws IOException {
        final byte[] archive = zip(entries(), StandardCharsets.UTF_8);
        // directory entries are skipped; "./" is not part of the name
        final List<String> all = List.of("data/a.txt=alpha", "data/sub/b.txt=beta", "c.csv=gamma", "d.txt=delta");
        Assertions.assertEquals(all, read(ArchiveReader.zip(channel("all.zip", archive), StandardCharsets.UTF_8), null));
        Assertions.assertEquals(all, read(ArchiveReader.zipStream(new ByteArrayInputStream(archive), StandardCharsets.UTF_8), null));

        final Predicate<String> filter = ArchiveReader.filter(List.of("**/*.txt"), List.of("data/sub/**"));
        final List<String> selected = List.of("data/a.txt=alpha", "d.txt=delta");
        Assertions.assertEquals(selected, read(ArchiveReader.zip(channel("selected.zip", archive), StandardCharsets.UTF_8), filter));
        Assertions.assertEquals(selected, read(ArchiveReader.zipStream(new ByteArrayInputStream(archive), StandardCharsets.UTF_8), filter));
    }

    @Test
    public void testTar() throws IOException {
        final byte[] archive = tar(entries());
        Assertions.assertEquals(List.of("data/a.txt=alpha", "data/sub/b.txt=beta", "c.csv=gamma", "d.txt=delta"),
                read(ArchiveReader.tar(new ByteArrayInputStream(archive), StandardCharsets.UTF_8), null));
        Assertions.assertEquals(List.of("c.csv=gamma"),
                read(ArchiveReader.tar(new ByteArrayInputStream(archive), StandardCharsets.UTF_8), ArchiveReader.filter(List.of("*.csv"), null)));
    }

    @Test
    public void testEntryStreamCloseKeepsTheArchive() throws IOException {
        final byte[] archive = tar(entries());
        final List<String> names = new ArrayList<>();
        try(final ArchiveReader reader = ArchiveReader.tar(new ByteArrayInputStream(archive), StandardCharsets.UTF_8)) {
            while(reader.next(null)) {
                names.add(reader.name());
                // a consumer closing the entry (and reading only part of it) must not end the archive
                reader.stream().read();
                reader.stream().close();
            }
        }
        Assertions.assertEquals(List.of("data/a.txt", "data/sub/b.txt", "c.csv", "d.txt"), names);
    }

    @Test
    public void testNameCharset() throws IOException {
        // a zip written on Japanese Windows: names in windows-31j without the UTF-8 flag
        final Charset ms932 = Charset.forName("windows-31j");
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("売上/一月.txt", "x".getBytes(StandardCharsets.UTF_8));
        final byte[] archive = zip(entries, ms932);
        Assertions.assertEquals(List.of("売上/一月.txt=x"), read(ArchiveReader.zip(channel("jp.zip", archive), ms932), null));
        Assertions.assertEquals(List.of("売上/一月.txt=x"), read(ArchiveReader.zipStream(new ByteArrayInputStream(archive), ms932), null));
        Assertions.assertEquals(List.of("売上/一月.txt=x"),
                read(ArchiveReader.zip(channel("jp2.zip", archive), ms932), ArchiveReader.filter(List.of("売上/*.txt"), null)));
    }

    @Test
    public void testCentralDirectoryReadsOnlySelectedEntries() throws IOException {
        // a large incompressible entry in front of a small one: selecting the small one must not read the large one
        final byte[] large = new byte[2_000_000];
        new Random(1).nextBytes(large);
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("large.bin", large);
        entries.put("small.txt", "small".getBytes(StandardCharsets.UTF_8));
        final CountingChannel channel = new CountingChannel(channel("large.zip", zip(entries, StandardCharsets.UTF_8)));

        Assertions.assertEquals(List.of("small.txt=small"),
                read(ArchiveReader.zip(channel, StandardCharsets.UTF_8), ArchiveReader.filter(List.of("small.txt"), null)));
        Assertions.assertTrue(channel.bytesRead < 200_000, "bytes read: " + channel.bytesRead);
        // and the many small reads of the zip reader reach the storage as a few block reads
        Assertions.assertTrue(channel.seeks < 10, "seeks: " + channel.seeks);
    }

    @Test
    public void testZipWithCommentIsLocatedWithFewSeeks() throws IOException {
        // an archive comment makes the reader step backwards byte by byte to find the central
        // directory: on object storage each step must not become a request
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final ZipOutputStream zip = new ZipOutputStream(bytes, StandardCharsets.UTF_8)) {
            zip.setComment("c".repeat(5000));
            for(int i = 0; i < 50; i++) {
                zip.putNextEntry(new ZipEntry("dir/file" + i + ".txt"));
                zip.write(("content" + i).getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
        }
        final CountingChannel channel = new CountingChannel(channel("comment.zip", bytes.toByteArray()));
        final List<String> contents = read(ArchiveReader.zip(channel, StandardCharsets.UTF_8), ArchiveReader.filter(List.of("dir/file4?.txt"), null));
        Assertions.assertEquals(10, contents.size());
        Assertions.assertEquals("dir/file40.txt=content40", contents.get(0));
        Assertions.assertTrue(channel.seeks < 10, "seeks: " + channel.seeks);
    }

    @Test
    public void testFilter() {
        final Predicate<String> all = ArchiveReader.filter(null, null);
        Assertions.assertTrue(all.test("a/b.txt"));

        final Predicate<String> top = ArchiveReader.filter(List.of("*.txt"), null);
        Assertions.assertTrue(top.test("a.txt"));
        // * stays within one path segment; matching is against the whole path and case-sensitive
        Assertions.assertFalse(top.test("dir/a.txt"));
        Assertions.assertFalse(top.test("a.txt.bak"));
        Assertions.assertFalse(top.test("A.TXT"));

        final Predicate<String> deep = ArchiveReader.filter(List.of("**/*.txt"), null);
        Assertions.assertTrue(deep.test("a.txt"));
        Assertions.assertTrue(deep.test("x/y/a.txt"));

        final Predicate<String> prefix = ArchiveReader.filter(List.of("KY?24*.txt", "logs/**"), List.of("**/*.tmp"));
        Assertions.assertTrue(prefix.test("KYI240101.txt"));
        Assertions.assertFalse(prefix.test("KY/240101.txt"));
        Assertions.assertTrue(prefix.test("logs/a/b.log"));
        Assertions.assertFalse(prefix.test("logs/a/b.tmp"));
        // regex characters in a glob are literal
        Assertions.assertTrue(ArchiveReader.filter(List.of("a(1)+[x].txt"), null).test("a(1)+[x].txt"));
    }

    @Test
    public void testDetectType() {
        Assertions.assertEquals(ArchiveReader.Type.zip, ArchiveReader.Type.detect("PACK240101.ZIP"));
        Assertions.assertEquals(ArchiveReader.Type.tar, ArchiveReader.Type.detect("logs.tar"));
        Assertions.assertEquals(ArchiveReader.Type.tar, ArchiveReader.Type.detect("logs.tar.gz"));
        Assertions.assertEquals(ArchiveReader.Type.tar, ArchiveReader.Type.detect("logs.tgz"));
        Assertions.assertNull(ArchiveReader.Type.detect("data.txt.gz"));
        Assertions.assertNull(ArchiveReader.Type.detect(null));
    }

    private static final class CountingChannel implements SeekableByteChannel {

        private final SeekableByteChannel channel;
        private long bytesRead;
        private int seeks;

        CountingChannel(final SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public int read(final ByteBuffer dst) throws IOException {
            final int read = channel.read(dst);
            if(read > 0) {
                bytesRead += read;
            }
            return read;
        }

        @Override
        public int write(final ByteBuffer src) throws IOException {
            return channel.write(src);
        }

        @Override
        public long position() throws IOException {
            return channel.position();
        }

        @Override
        public SeekableByteChannel position(final long newPosition) throws IOException {
            if(newPosition != channel.position()) {
                seeks++;
            }
            channel.position(newPosition);
            return this;
        }

        @Override
        public long size() throws IOException {
            return channel.size();
        }

        @Override
        public SeekableByteChannel truncate(final long size) throws IOException {
            channel.truncate(size);
            return this;
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

    }

}
