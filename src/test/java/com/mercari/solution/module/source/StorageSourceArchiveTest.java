package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * {@code parameters.archive} of the storage source (work_fixedwidth.md §6): the matched files are
 * zip / tar archives and each selected entry is read as a file of its own. Synthetic data.
 */
public class StorageSourceArchiveTest {

    private static final Charset MS932 = Charset.forName("windows-31j");

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @TempDir
    Path tempDir;

    // code (4) + name (10 bytes, full-width) + amount (5, implied scale 1)
    private static final String RECORD_1 = "A001" + "㈱東京　　" + "00123";
    private static final String RECORD_2 = "B002" + "大阪商店　" + "00045";
    private static final String RECORD_3 = "C003" + "名古屋　　" + "00007";

    private static final String SCHEMA = """
                      schema:
                        encoding: { format: fwf, charset: windows-31j }
                        reference:
                          inline:
                            recordLength: 19
                            fields:
                              - { name: code,   type: string,  len: 4 }
                              - { name: name,   type: string,  len: 10 }
                              - { name: amount, type: decimal, len: 5, scale: 1 }
                """;

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
                tarEntry.setSize(entry.getValue().length);
                tar.putArchiveEntry(tarEntry);
                tar.write(entry.getValue());
                tar.closeArchiveEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] gzip(final byte[] content) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(content);
        }
        return bytes.toByteArray();
    }

    private static byte[] ms932(final String text) {
        return text.getBytes(MS932);
    }

    private String write(final String fileName, final byte[] content) throws Exception {
        final Path file = tempDir.resolve(fileName);
        Files.write(file, content);
        return relativize(file);
    }

    // Beam FileSystems misreads a Windows drive letter ("C:/...") as a URI scheme,
    // so local test files are addressed relative to the working directory
    private static String relativize(final Path path) {
        final Path workingDir = Path.of("").toAbsolutePath();
        return workingDir.relativize(path.toAbsolutePath()).toString().replace('\\', '/');
    }

    // code:entry:line of every record
    private static Set<String> rows(final Iterable<MElement> elements) {
        final Set<String> rows = new HashSet<>();
        for(final MElement element : elements) {
            rows.add(element.getAsString("code") + ":" + element.getAsString("_entry") + ":" + element.getAsLong("_line"));
        }
        return rows;
    }

    private static Map<String, byte[]> orders() {
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("docs/", null);
        entries.put("docs/readme.txt", "not a record".getBytes(StandardCharsets.UTF_8));
        entries.put("ORD240101.txt", ms932(RECORD_1 + "\r\n" + RECORD_2 + "\r\n"));
        entries.put("ITEM240101.txt", ms932("a different record type that must not be read\r\n"));
        entries.put("ORD240102.txt", ms932(RECORD_3 + "\r\n"));
        return entries;
    }

    @Test
    public void testZipEntries() throws Exception {
        // one record type out of a pack of several; the archive type comes from the file name
        final String path = write("PACK240101.zip", zip(orders(), StandardCharsets.UTF_8));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      archive:
                        entries: ["ORD*.txt"]
                      additionalFields: { resource: _file, entry: _entry, line: _line }
                %s
                """.formatted(path, SCHEMA)));
        final MCollection output = outputs.get("input");
        Assertions.assertEquals(List.of("code", "name", "amount", "_file", "_entry", "_line"),
                output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        PAssert.that(output.getCollection()).satisfies(elements -> {
            // records are numbered per entry
            Assertions.assertEquals(Set.of("A001:ORD240101.txt:1", "B002:ORD240101.txt:2", "C003:ORD240102.txt:1"), rows(elements));
            for(final MElement element : elements) {
                Assertions.assertTrue(element.getAsString("_file").endsWith("PACK240101.zip"));
                if("A001".equals(element.getAsString("code"))) {
                    Assertions.assertEquals("㈱東京", element.getAsString("name"));
                    Assertions.assertEquals(12.3, element.getAsDouble("amount"), 1e-9);
                }
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testHeaderLinesAreSkippedPerEntry() throws Exception {
        // every entry is a file of its own: its header is skipped (the legacy compression: ZIP
        // concatenates the entries and skips only the first header)
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("a.csv", "code,qty\nA001,1\nB002,2\n".getBytes(StandardCharsets.UTF_8));
        entries.put("sub/b.csv", "code,qty\nC003,3\n".getBytes(StandardCharsets.UTF_8));
        final String path = write("csv.zip", zip(entries, StandardCharsets.UTF_8));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      format: csv
                      skipHeaderLines: 1
                      archive: {}
                      additionalFields: { entry: _entry, line: _line }
                      schema:
                        fields:
                          - { name: code, type: string }
                          - { name: qty, type: int64 }
                """.formatted(path)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:a.csv:2", "B002:a.csv:3", "C003:sub/b.csv:2"), rows(elements));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testTarGzWithCompressedEntryAndExclude() throws Exception {
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        // an entry that is a gzip file itself is decompressed by its name
        entries.put("2024/ORD240101.txt.gz", gzip(ms932(RECORD_1 + "\n" + RECORD_2 + "\n")));
        entries.put("2024/ORD240102.txt", ms932(RECORD_3 + "\n"));
        entries.put("2024/tmp/ORD240103.txt", ms932("half written"));
        final String path = write("orders.tar.gz", gzip(tar(entries)));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      archive:
                        entries: ["**/ORD*"]
                        exclude: ["**/tmp/**"]
                      additionalFields: { entry: _entry, line: _line }
                %s
                """.formatted(path, SCHEMA)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(
                    Set.of("A001:2024/ORD240101.txt.gz:1", "B002:2024/ORD240101.txt.gz:2", "C003:2024/ORD240102.txt:1"),
                    rows(elements));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testCompressedZipIsReadAsAStream() throws Exception {
        // a zip inside a gzip can not be seeked: the type is declared, the gzip comes from the name
        final String path = write("PACK240101.zip.gz", gzip(zip(orders(), StandardCharsets.UTF_8)));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      archive:
                        type: zip
                        entries: ["ORD*.txt"]
                      additionalFields: { entry: _entry, line: _line }
                %s
                """.formatted(path, SCHEMA)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:ORD240101.txt:1", "B002:ORD240101.txt:2", "C003:ORD240102.txt:1"), rows(elements));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEntryNameCharset() throws Exception {
        // a zip written on Japanese Windows: entry names in windows-31j without the UTF-8 flag
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("売上/一月.txt", ms932(RECORD_1 + "\r\n"));
        entries.put("在庫/一月.txt", ms932("another record type\r\n"));
        final String path = write("jp.zip", zip(entries, MS932));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      archive:
                        entries: ["売上/*.txt"]
                        nameCharset: windows-31j
                      additionalFields: { entry: _entry, line: _line }
                %s
                """.formatted(path, SCHEMA)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:売上/一月.txt:1"), rows(elements));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testFailuresNameTheEntry() throws Exception {
        // a bad record inside an entry is a record failure; the other entries are read
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("ORD1.txt", ms932(RECORD_1 + "\r\n" + "SHORT" + "\r\n" + RECORD_2 + "\r\n"));
        entries.put("ORD2.txt", ms932(RECORD_3 + "\r\n"));
        final String path = write("bad.zip", zip(entries, StandardCharsets.UTF_8));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    failFast: false
                    parameters:
                      input: "%s"
                      archive: {}
                      additionalFields: { entry: _entry, line: _line }
                %s
                """.formatted(path, SCHEMA)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:ORD1.txt:1", "B002:ORD1.txt:3", "C003:ORD2.txt:1"), rows(elements));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testUnknownArchiveTypeIsAFileFailure() throws Exception {
        // the name does not say zip or tar and archive.type is not set
        final String path = write("pack.bin", zip(orders(), StandardCharsets.UTF_8));
        MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      archive: {}
                %s
                """.formatted(path, SCHEMA)));
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class, pipeline::run);
        Assertions.assertTrue(String.valueOf(e.getMessage()).contains("set parameters.archive.type"), e.getMessage());
    }

    @Test
    public void testUnsupportedCompressionIsAFileFailure() throws Exception {
        // .tar.xz would be read as a tar of garbage (and may pass as an empty one): rejected by its name
        final String path = write("orders.tar.xz", tar(Map.of("ORD1.txt", ms932(RECORD_1 + "\r\n"))));
        MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      archive: {}
                %s
                """.formatted(path, SCHEMA)));
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class, pipeline::run);
        Assertions.assertTrue(String.valueOf(e.getMessage()).contains("orders.tar.xz is compressed with a format that is not supported (.xz)"), e.getMessage());
    }

    @Test
    public void testFailureRecordsNameTheEntry() throws Exception {
        // the failure output itself, through the storage failure sink
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("ORD1.txt", ms932(RECORD_1 + "\r\n" + "SHORT" + "\r\n"));
        entries.put("ORD2.txt", ms932(RECORD_3 + "\r\n"));
        final String path = write("bad.zip", zip(entries, StandardCharsets.UTF_8));
        final Path failures = tempDir.resolve("failures");
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                system:
                  failure:
                    failFast: false
                    sinks:
                      - name: dead_letter
                        module: storage
                        parameters:
                          output: "%s/failed"
                          format: json
                          suffix: ".json"
                          numShards: 1
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      archive: {}
                      additionalFields: { entry: _entry, line: _line }
                %s
                """.formatted(relativize(failures), path, SCHEMA)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:ORD1.txt:1", "C003:ORD2.txt:1"), rows(elements));
            return null;
        });
        pipeline.run().waitUntilFinish();

        final StringBuilder written = new StringBuilder();
        try(final java.util.stream.Stream<Path> files = Files.walk(failures)) {
            for(final Path file : files.filter(Files::isRegularFile).toList()) {
                written.append(Files.readString(file, StandardCharsets.UTF_8));
            }
        }
        final String failure = written.toString();
        Assertions.assertTrue(failure.contains("ORD1.txt"), failure);
        Assertions.assertTrue(failure.contains("record length 5 does not match"), failure);
        Assertions.assertFalse(failure.contains("ORD2.txt"), failure);
    }

    @Test
    public void testValidation() throws Exception {
        final String path = write("PACK240101.zip", zip(orders(), StandardCharsets.UTF_8));
        assertInvalid("parameters.archive.type must be zip or tar", """
                      input: "%s"
                      archive: { type: rar }
                %s
                """.formatted(path, SCHEMA));
        assertInvalid("parameters.archive.nameCharset: no-such-charset is not a supported charset", """
                      input: "%s"
                      archive: { nameCharset: no-such-charset }
                %s
                """.formatted(path, SCHEMA));
        assertInvalid("parameters.compression: ZIP can not be used with parameters.archive", """
                      input: "%s"
                      compression: ZIP
                      archive: {}
                %s
                """.formatted(path, SCHEMA));
        assertInvalid("parameters.archive.entries must not contain an empty pattern", """
                      input: "%s"
                      archive: { entries: ["ORD*.txt", ""] }
                %s
                """.formatted(path, SCHEMA));
        assertInvalid("parameters.archive is not supported for format avro", """
                      input: "%s"
                      format: avro
                      archive: {}
                """.formatted(path));
    }

    private void assertInvalid(final String expected, final String parameters) {
        final String config = """
                sources:
                  - name: input
                    module: storage
                    parameters:
                """ + parameters;
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(p, Config.load(config)));
        Assertions.assertTrue(String.valueOf(e.getMessage()).contains(expected), "expected '" + expected + "' in: " + e.getMessage());
    }

}
