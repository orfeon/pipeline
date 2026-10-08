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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * {@code parameters.partitions} of the storage source (work_fixedwidth.md §6.4): the entries of an
 * archive go to named outputs, each with its own format and schema. Synthetic data.
 */
public class StorageSourcePartitionsTest {

    private static final Charset MS932 = Charset.forName("windows-31j");

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @TempDir
    Path tempDir;

    // code (4) + name (10 bytes, full-width) + amount (5, implied scale 1)
    private static final String ORDER_1 = "A001" + "㈱東京　　" + "00123";
    private static final String ORDER_2 = "B002" + "大阪商店　" + "00045";
    private static final String ORDER_3 = "C003" + "名古屋　　" + "00007";

    private static final String ORDER_SCHEMA = """
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

    private static byte[] zip(final Map<String, byte[]> entries) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final ZipOutputStream zip = new ZipOutputStream(bytes, StandardCharsets.UTF_8)) {
            for(final Map.Entry<String, byte[]> entry : entries.entrySet()) {
                zip.putNextEntry(new ZipEntry(entry.getKey()));
                zip.write(entry.getValue());
                zip.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private String writePack() throws Exception {
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("ORD240101.txt", (ORDER_1 + "\r\n" + ORDER_2 + "\r\n").getBytes(MS932));
        entries.put("ITEM240101.csv", "sku,qty\nS-1,3\nS-2,5\n".getBytes(StandardCharsets.UTF_8));
        entries.put("docs/readme.txt", "no partition takes this entry".getBytes(StandardCharsets.UTF_8));
        entries.put("ORD240102.txt", (ORDER_3 + "\r\n").getBytes(MS932));
        entries.put("ITEM240102.csv", "sku,qty\nS-3,8\n".getBytes(StandardCharsets.UTF_8));
        return write("PACK240101.zip", zip(entries));
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

    // the schema block of a partition, moved out to the module level (4 columns to the left)
    private static String moduleLevel(final String partitionSchema) {
        return partitionSchema.lines().map(line -> line.substring(4)).collect(java.util.stream.Collectors.joining("\n"));
    }

    private static Set<String> rows(final Iterable<MElement> elements, final String key) {
        final Set<String> rows = new HashSet<>();
        for(final MElement element : elements) {
            rows.add(element.getAsString(key) + ":" + element.getAsString("_entry") + ":" + element.getAsLong("_line"));
        }
        return rows;
    }

    @Test
    public void testPartitionsWithTheirOwnFormatAndSchema() throws Exception {
        // one read of the archive, two record types: fixed-width orders and csv items
        final String path = writePack();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: pack
                    module: storage
                    parameters:
                      input: "%s"
                      archive: {}
                      additionalFields: { entry: _entry, line: _line }
                      partitions:
                        - name: orders
                          entries: ["ORD*.txt"]
                %s
                        - name: items
                          entries: ["ITEM*.csv"]
                          format: csv
                          skipHeaderLines: 1
                          schema:
                            fields:
                              - { name: sku, type: string }
                              - { name: qty, type: int64 }
                """.formatted(path, ORDER_SCHEMA)));

        // the outputs are the partitions: there is no unnamed output
        Assertions.assertNull(outputs.get("pack"));
        final MCollection orders = outputs.get("pack.orders");
        final MCollection items = outputs.get("pack.items");
        Assertions.assertEquals(List.of("code", "name", "amount", "_entry", "_line"),
                orders.getSchema().getFields().stream().map(Schema.Field::getName).toList());
        Assertions.assertEquals(List.of("sku", "qty", "_entry", "_line"),
                items.getSchema().getFields().stream().map(Schema.Field::getName).toList());

        PAssert.that(orders.getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:ORD240101.txt:1", "B002:ORD240101.txt:2", "C003:ORD240102.txt:1"), rows(elements, "code"));
            for(final MElement element : elements) {
                if("A001".equals(element.getAsString("code"))) {
                    Assertions.assertEquals("㈱東京", element.getAsString("name"));
                    Assertions.assertEquals(12.3, element.getAsDouble("amount"), 1e-9);
                }
            }
            return null;
        });
        PAssert.that(items.getCollection()).satisfies(elements -> {
            // the header of every csv entry is skipped
            Assertions.assertEquals(Set.of("S-1:ITEM240101.csv:2", "S-2:ITEM240101.csv:3", "S-3:ITEM240102.csv:2"), rows(elements, "sku"));
            for(final MElement element : elements) {
                if("S-2".equals(element.getAsString("sku"))) {
                    Assertions.assertEquals(5L, element.getAsLong("qty"));
                }
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testPartitionsInheritAndFirstMatchWins() throws Exception {
        // the partitions only route: format, schema and the rest come from the module level;
        // an entry that two partitions match goes to the first
        final String path = writePack();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: pack
                    module: storage
                    parameters:
                      input: "%s"
                      archive:
                        entries: ["ORD*"]
                      additionalFields: { entry: _entry, line: _line }
                      fields: [code]
                      partitions:
                        - name: first_day
                          entries: ["*240101*"]
                        - name: all_days
                          entries: ["ORD*.txt"]
                %s
                """.formatted(path, moduleLevel(ORDER_SCHEMA))));

        Assertions.assertEquals(List.of("code", "_entry", "_line"),
                outputs.get("pack.first_day").getSchema().getFields().stream().map(Schema.Field::getName).toList());
        PAssert.that(outputs.get("pack.first_day").getCollection()).satisfies(elements -> {
            // ITEM240101.csv matches *240101* too, but parameters.archive.entries leaves it out
            Assertions.assertEquals(Set.of("A001:ORD240101.txt:1", "B002:ORD240101.txt:2"), rows(elements, "code"));
            return null;
        });
        PAssert.that(outputs.get("pack.all_days").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("C003:ORD240102.txt:1"), rows(elements, "code"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testPartitionWithoutEntriesInTheArchiveIsEmpty() throws Exception {
        final String path = writePack();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: pack
                    module: storage
                    parameters:
                      input: "%s"
                      archive: {}
                      additionalFields: { entry: _entry, line: _line }
                      partitions:
                        - name: orders
                          entries: ["ORD*.txt"]
                %s
                        - name: returns
                          entries: ["RET*.txt"]
                %s
                """.formatted(path, ORDER_SCHEMA, ORDER_SCHEMA)));
        PAssert.that(outputs.get("pack.returns").getCollection()).empty();
        PAssert.that(outputs.get("pack.orders").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(3, rows(elements, "code").size());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testNullPartitionsIsNoPartitions() throws Exception {
        // an explicit null is an omitted parameter: the module has its one unnamed output
        final String path = writePack();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: pack
                    module: storage
                    parameters:
                      input: "%s"
                      archive:
                        entries: ["ORD*.txt"]
                      additionalFields: { entry: _entry, line: _line }
                      partitions: null
                %s
                """.formatted(path, moduleLevel(ORDER_SCHEMA))));
        PAssert.that(outputs.get("pack").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:ORD240101.txt:1", "B002:ORD240101.txt:2", "C003:ORD240102.txt:1"), rows(elements, "code"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testStreamedArchiveAndEntryCounters() throws Exception {
        // a tar.gz is read front to back: the same routing, and the entries no partition takes are counted
        final java.io.ByteArrayOutputStream tarBytes = new java.io.ByteArrayOutputStream();
        try(final org.apache.commons.compress.archivers.tar.TarArchiveOutputStream tar =
                    new org.apache.commons.compress.archivers.tar.TarArchiveOutputStream(new java.util.zip.GZIPOutputStream(tarBytes))) {
            final Map<String, byte[]> entries = new LinkedHashMap<>();
            entries.put("ORD240101.txt", (ORDER_1 + "\r\n" + ORDER_2 + "\r\n").getBytes(MS932));
            entries.put("docs/readme.txt", "no partition takes this entry".getBytes(StandardCharsets.UTF_8));
            entries.put("ITEM240101.csv", "sku,qty\nS-1,3\n".getBytes(StandardCharsets.UTF_8));
            entries.put("ORD240102.txt", (ORDER_3 + "\r\n").getBytes(MS932));
            entries.put("notes.md", "nor this one".getBytes(StandardCharsets.UTF_8));
            for(final Map.Entry<String, byte[]> entry : entries.entrySet()) {
                final org.apache.commons.compress.archivers.tar.TarArchiveEntry tarEntry =
                        new org.apache.commons.compress.archivers.tar.TarArchiveEntry(entry.getKey());
                tarEntry.setSize(entry.getValue().length);
                tar.putArchiveEntry(tarEntry);
                tar.write(entry.getValue());
                tar.closeArchiveEntry();
            }
        }
        final String path = write("PACK240101.tar.gz", tarBytes.toByteArray());
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: pack
                    module: storage
                    parameters:
                      input: "%s"
                      archive: {}
                      additionalFields: { entry: _entry, line: _line }
                      partitions:
                        - name: orders
                          entries: ["ORD*.txt"]
                %s
                        - name: items
                          entries: ["ITEM*.csv"]
                          format: csv
                          skipHeaderLines: 1
                          schema:
                            fields:
                              - { name: sku, type: string }
                              - { name: qty, type: int64 }
                """.formatted(path, ORDER_SCHEMA)));
        PAssert.that(outputs.get("pack.orders").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("A001:ORD240101.txt:1", "B002:ORD240101.txt:2", "C003:ORD240102.txt:1"), rows(elements, "code"));
            return null;
        });
        PAssert.that(outputs.get("pack.items").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Set.of("S-1:ITEM240101.csv:2"), rows(elements, "sku"));
            return null;
        });
        final org.apache.beam.sdk.PipelineResult result = pipeline.run();
        result.waitUntilFinish();

        final Map<String, Long> counters = new java.util.HashMap<>();
        for(final org.apache.beam.sdk.metrics.MetricResult<Long> counter : result.metrics()
                .queryMetrics(org.apache.beam.sdk.metrics.MetricsFilter.builder()
                        .addNameFilter(org.apache.beam.sdk.metrics.MetricNameFilter.inNamespace("pack"))
                        .build())
                .getCounters()) {
            counters.put(counter.getName().getName(), counter.getAttempted());
        }
        Assertions.assertEquals(3L, counters.get("archive_entries_read"), counters.toString());
        Assertions.assertEquals(2L, counters.get("archive_entries_skipped"), counters.toString());
    }

    @Test
    public void testTimestampAttributePerPartition() throws Exception {
        // the module-level timestampAttribute is one name for all the partitions: a partition may
        // declare its own, and one without the field keeps the default timestamp (with a warning)
        final Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("events.csv", "e1,2024-01-31T00:00:00Z\n".getBytes(StandardCharsets.UTF_8));
        entries.put("days.csv", "d1,2024-02-01\n".getBytes(StandardCharsets.UTF_8));
        entries.put("items.csv", "i1,3\n".getBytes(StandardCharsets.UTF_8));
        final String path = write("times.zip", zip(entries));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: pack
                    module: storage
                    timestampAttribute: at
                    parameters:
                      input: "%s"
                      format: csv
                      archive: {}
                      partitions:
                        - name: events
                          entries: ["events.csv"]
                          schema:
                            fields:
                              - { name: id, type: string }
                              - { name: at, type: timestamp }
                        - name: days
                          entries: ["days.csv"]
                          timestampAttribute: day
                          schema:
                            fields:
                              - { name: id, type: string }
                              - { name: day, type: date }
                        - name: items
                          entries: ["items.csv"]
                          schema:
                            fields:
                              - { name: id, type: string }
                              - { name: qty, type: int64 }
                """.formatted(path)));
        final long january31 = java.time.LocalDate.of(2024, 1, 31).toEpochDay() * 86_400_000L;
        final long february1 = java.time.LocalDate.of(2024, 2, 1).toEpochDay() * 86_400_000L;
        PAssert.that(outputs.get("pack.events").getCollection()).satisfies(elements -> {
            for(final MElement element : elements) {
                Assertions.assertEquals(january31, element.getEpochMillis());
            }
            return null;
        });
        PAssert.that(outputs.get("pack.days").getCollection()).satisfies(elements -> {
            for(final MElement element : elements) {
                Assertions.assertEquals(february1, element.getEpochMillis());
            }
            return null;
        });
        PAssert.that(outputs.get("pack.items").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                Assertions.assertTrue(element.getEpochMillis() < 0, "default timestamp: " + element.getEpochMillis());
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testValidation() throws Exception {
        final String path = writePack();
        final String partition = """
                        - name: orders
                          entries: ["ORD*.txt"]
                %s
                """.formatted(ORDER_SCHEMA);

        assertInvalid("parameters.partitions[0].schema.reference.destination is not applicable to source modules", """
                      input: "%s"
                      archive: {}
                      format: csv
                      additionalFields: { entry: _entry }
                      partitions:
                        - name: items
                          entries: ["ITEM*.csv"]
                          schema:
                            reference: { destination: true }
                """.formatted(path));
        assertInvalid("parameters.partitions[0].schema: it defines no fields", """
                      input: "%s"
                      archive: {}
                      format: csv
                      partitions:
                        - name: items
                          entries: ["ITEM*.csv"]
                          schema: {}
                """.formatted(path));
        assertInvalid("parameters.partitions[0].skipHeaderLines must be an integer. but: 1.7", """
                      input: "%s"
                      archive: {}
                      format: csv
                      partitions:
                        - name: items
                          entries: ["ITEM*.csv"]
                          skipHeaderLines: 1.7
                          schema:
                            fields:
                              - { name: sku, type: string }
                """.formatted(path));
        assertInvalid("parameters.partitions(orders).format: csv differs from schema.encoding.format: fwf", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - name: orders
                          entries: ["ORD*.txt"]
                          format: csv
                %s
                """.formatted(path, ORDER_SCHEMA));

        assertInvalid("parameters.partitions requires parameters.archive", """
                      input: "%s"
                      partitions:
                %s
                """.formatted(path, partition));
        assertInvalid("parameters.partitions[0].name is required", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - entries: ["ORD*.txt"]
                """.formatted(path));
        assertInvalid("parameters.partitions[1].name: orders is duplicated", """
                      input: "%s"
                      archive: {}
                      partitions:
                %s
                %s
                """.formatted(path, partition, partition));
        assertInvalid("parameters.partitions[0].entries is required", """
                      input: "%s"
                      archive: {}
                      format: csv
                      partitions:
                        - name: orders
                          schema:
                            fields:
                              - { name: sku, type: string }
                """.formatted(path));
        assertInvalid("parameters.partitions[0].format is required", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - name: items
                          entries: ["ITEM*.csv"]
                          schema:
                            fields:
                              - { name: sku, type: string }
                """.formatted(path));
        assertInvalid("parameters.partitions[0].entry is not supported", """
                      input: "%s"
                      archive: {}
                      format: csv
                      partitions:
                        - name: items
                          entry: "ITEM*.csv"
                          entries: ["ITEM*.csv"]
                """.formatted(path));
        assertInvalid("parameters.partitions[0].format: avro is not supported", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - name: items
                          entries: ["ITEM*"]
                          format: avro
                """.formatted(path));
        assertInvalid("parameters.partitions(items).schema is required for format csv", """
                      input: "%s"
                      archive: {}
                      format: csv
                      partitions:
                        - name: items
                          entries: ["ITEM*.csv"]
                """.formatted(path));
        assertInvalid("parameters.partitions(orders).fields: ", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - name: orders
                          entries: ["ORD*.txt"]
                          fields: [nothing]
                %s
                """.formatted(path, ORDER_SCHEMA));
        // "{module}.failures" is the failure output of a module
        assertInvalid("parameters.partitions[0].name: failures is reserved", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - name: failures
                          entries: ["ORD*.txt"]
                %s
                """.formatted(path, ORDER_SCHEMA));
        // a timestampAttribute a partition declares itself must be one of its fields
        assertInvalid("parameters.partitions(orders).timestampAttribute: day is not a field of this partition", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - name: orders
                          entries: ["ORD*.txt"]
                          timestampAttribute: day
                %s
                """.formatted(path, ORDER_SCHEMA));
        // the format derived from the module-level fwf schema is not a default for a partition with
        // a schema of its own: its format has to be declared
        assertInvalid("parameters.partitions[0].format is required", """
                      input: "%s"
                      archive: {}
                      partitions:
                        - name: items
                          entries: ["ITEM*.csv"]
                          schema:
                            fields:
                              - { name: sku, type: string }
                %s
                """.formatted(path, moduleLevel(ORDER_SCHEMA)));
        assertInvalid("parameters.partitions must be a non-empty array", """
                      input: "%s"
                      archive: {}
                      format: csv
                      partitions: []
                """.formatted(path));
        assertInvalid("parameters.partitions is not supported for format avro", """
                      input: "%s"
                      format: avro
                      partitions:
                %s
                """.formatted(path, partition));
    }

    private void assertInvalid(final String expected, final String parameters) {
        final String config = """
                sources:
                  - name: pack
                    module: storage
                    parameters:
                """ + parameters;
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(p, Config.load(config)));
        Assertions.assertTrue(String.valueOf(e.getMessage()).contains(expected), "expected '" + expected + "' in: " + e.getMessage());
    }

}
