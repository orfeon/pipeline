package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

/**
 * The storage source's byte-record read path (work_fixedwidth.md §4.6 / §5): {@code format: fwf}
 * and the csv / json options that need it. The data is synthetic windows-31j text.
 */
public class StorageSourceFwfTest {

    private static final Charset MS932 = Charset.forName("windows-31j");

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @TempDir
    Path tempDir;

    // code (4) + name (10 bytes: full-width, padded with ideographic spaces) + amount (5, implied scale 1) + day (8)
    private static final String RECORD_1 = "A001" + "㈱東京　　" + "00123" + "20240131";
    private static final String RECORD_2 = "B002" + "大阪商店　" + "00045" + "20240201";
    private static final String RECORD_3 = "C003" + "名古屋　　" + "     " + "00000000";

    private static final String SCHEMA = """
                      schema:
                        encoding: { format: fwf, charset: windows-31j }
                        reference:
                          inline:
                            recordLength: 27
                            fields:
                              - { name: code,   type: string,  len: 4 }
                              - { name: name,   type: string,  len: 10 }
                              - { name: amount, type: decimal, len: 5, scale: 1 }
                              - { name: day,    type: date,    len: 8, pattern: yyyyMMdd, nullIf: ["00000000"] }
                """;

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

    private static byte[] gzip(final byte[] content) throws Exception {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(content);
        }
        return bytes.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> byCode(final Iterable<MElement> elements) {
        final Map<String, Map<String, Object>> rows = new HashMap<>();
        for(final MElement element : elements) {
            rows.put(element.getAsString("code"), (Map<String, Object>) element.getValue());
        }
        return rows;
    }

    @Test
    public void testFwfLines() throws Exception {
        // CRLF, LF and a blank line in one file; the format comes from schema.encoding.format
        final String path = write("lines.txt",
                (RECORD_1 + "\r\n" + RECORD_2 + "\n" + "\r\n" + RECORD_3 + "\r\n").getBytes(MS932));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                %s
                """.formatted(path, SCHEMA)));

        final MCollection output = outputs.get("input");
        Assertions.assertEquals(List.of("code", "name", "amount", "day"),
                output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(Schema.Type.date, output.getSchema().getField("day").getFieldType().getType());

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Map<String, Object>> rows = byCode(elements);
            Assertions.assertEquals(Set.of("A001", "B002", "C003"), rows.keySet());
            Assertions.assertEquals("㈱東京", rows.get("A001").get("name"));
            Assertions.assertEquals("大阪商店", rows.get("B002").get("name"));
            Assertions.assertEquals((int) LocalDate.of(2024, 1, 31).toEpochDay(), rows.get("A001").get("day"));
            Assertions.assertNull(rows.get("C003").get("amount"));
            Assertions.assertNull(rows.get("C003").get("day"));
            return null;
        });
        PAssert.that(output.getCollection()).satisfies(elements -> {
            for(final MElement element : elements) {
                if("A001".equals(element.getAsString("code"))) {
                    Assertions.assertEquals(12.3, element.getAsDouble("amount"), 1e-9);
                }
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testGzipGlobAndAdditionalFields() throws Exception {
        write("part-a.txt", (RECORD_1 + "\r\n" + RECORD_2 + "\r\n").getBytes(MS932));
        write("part-b.txt.gz", gzip((RECORD_3 + "\r\n").getBytes(MS932)));
        final String pattern = relativize(tempDir) + "/part-*";
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      format: fwf
                      additionalFields: { line: _line, resource: _file }
                %s
                """.formatted(pattern, SCHEMA)));

        final MCollection output = outputs.get("input");
        // appended after the layout fields in a fixed order
        Assertions.assertEquals(List.of("code", "name", "amount", "day", "_file", "_line"),
                output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(Schema.Type.int64, output.getSchema().getField("_line").getFieldType().getType());

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Map<String, Object>> rows = byCode(elements);
            Assertions.assertEquals(3, rows.size());
            Assertions.assertTrue(String.valueOf(rows.get("A001").get("_file")).endsWith("part-a.txt"));
            Assertions.assertEquals(2L, rows.get("B002").get("_line"));
            // the gzip file is detected from its name; record numbers restart per file
            Assertions.assertTrue(String.valueOf(rows.get("C003").get("_file")).endsWith("part-b.txt.gz"));
            Assertions.assertEquals(1L, rows.get("C003").get("_line"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testRecordSplitLengthAndProjection() throws Exception {
        // no separators at all: records are cut every recordLength bytes
        final String path = write("fixed.dat", (RECORD_1 + RECORD_2 + RECORD_3).getBytes(MS932));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      recordSplit: length
                      fields: [name, code]
                %s
                """.formatted(path, SCHEMA)));

        final MCollection output = outputs.get("input");
        Assertions.assertEquals(List.of("name", "code"),
                output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Map<String, Object>> rows = byCode(elements);
            Assertions.assertEquals(Set.of("A001", "B002", "C003"), rows.keySet());
            Assertions.assertEquals("名古屋", rows.get("C003").get("name"));
            Assertions.assertFalse(rows.get("A001").containsKey("amount"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testSkipHeaderLinesAndFilterPrefix() throws Exception {
        final String header = "HEADER" + " ".repeat(21);
        final String path = write("header.txt",
                (header + "\r\n" + RECORD_1 + "\r\n" + RECORD_2 + "\r\n" + RECORD_3 + "\r\n").getBytes(MS932));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      skipHeaderLines: 1
                      filterPrefix: "B0"
                %s
                """.formatted(path, SCHEMA)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001", "C003"), byCode(elements).keySet());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testFailureRouting() throws Exception {
        // a short record and an unparsable amount go to the failure output; the rest is read
        final String broken = "D004" + "福岡　　　" + "00x45" + "20240201";
        final String path = write("broken.txt",
                (RECORD_1 + "\r\n" + "SHORT" + "\r\n" + broken + "\r\n" + RECORD_3 + "\r\n").getBytes(MS932));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    failFast: false
                    parameters:
                      input: "%s"
                %s
                """.formatted(path, SCHEMA)));
        PAssert.that(outputs.get("input").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001", "C003"), byCode(elements).keySet());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testFailFastByDefault() throws Exception {
        final String path = write("short.txt", (RECORD_1 + "\r\n" + "SHORT" + "\r\n").getBytes(MS932));
        MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                %s
                """.formatted(path, SCHEMA)));
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class, pipeline::run);
        Assertions.assertTrue(String.valueOf(e.getMessage()).contains("record length 5 does not match"), e.getMessage());
    }

    @Test
    public void testCsvWithAdditionalFields() throws Exception {
        final String path = write("data.csv", "id,name\n1,りんご\n2,みかん\n".getBytes(StandardCharsets.UTF_8));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: input
                    module: storage
                    parameters:
                      input: "%s"
                      format: csv
                      skipHeaderLines: 1
                      additionalFields: { resource: source_file, entry: source_entry, line: source_line }
                      schema:
                        fields:
                          - { name: id, type: int64 }
                          - { name: name, type: string }
                """.formatted(path)));
        final MCollection output = outputs.get("input");
        Assertions.assertEquals(List.of("id", "name", "source_file", "source_entry", "source_line"),
                output.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<String> rows = new HashSet<>();
            for(final MElement element : elements) {
                Assertions.assertTrue(element.getAsString("source_file").endsWith("data.csv"));
                // a plain file has no archive entry
                Assertions.assertNull(element.getPrimitiveValue("source_entry"));
                rows.add(element.getAsLong("id") + ":" + element.getAsString("name") + ":" + element.getAsLong("source_line"));
            }
            Assertions.assertEquals(Set.of("1:りんご:2", "2:みかん:3"), rows);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testValidation() throws Exception {
        final String path = write("lines.txt", (RECORD_1 + "\r\n").getBytes(MS932));
        // format contradicts the schema encoding
        assertInvalid("differs from schema.encoding.format: fwf", """
                      input: "%s"
                      format: csv
                %s
                """.formatted(path, SCHEMA));
        // fwf without a layout
        assertInvalid("format fwf requires parameters.schema", """
                      input: "%s"
                      format: fwf
                      schema:
                        fields:
                          - { name: code, type: string }
                """.formatted(path));
        // unknown projection field
        assertInvalid("[nothing]", """
                      input: "%s"
                      fields: [code, nothing]
                %s
                """.formatted(path, SCHEMA));
        // unknown additional field key / output name already used
        assertInvalid("additionalFields.file is not supported", """
                      input: "%s"
                      additionalFields: { file: _file }
                %s
                """.formatted(path, SCHEMA));
        assertInvalid("the output field name code is already used", """
                      input: "%s"
                      additionalFields: { resource: code }
                %s
                """.formatted(path, SCHEMA));
        // recordSplit: length needs the layout recordLength
        assertInvalid("requires recordLength in the fwf layout", """
                      input: "%s"
                      recordSplit: length
                      schema:
                        encoding: { format: fwf }
                        reference:
                          inline:
                            fields:
                              - { name: code, type: string, len: 4 }
                """.formatted(path));
        // not yet available on the avro / parquet paths
        assertInvalid("additionalFields is not supported for format avro", """
                      input: "%s"
                      format: avro
                      additionalFields: { resource: _file }
                """.formatted(path));
        assertInvalid("recordSplit is only supported for format fwf", """
                      input: "%s"
                      format: csv
                      recordSplit: length
                      schema:
                        fields:
                          - { name: code, type: string }
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
