package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class StorageSinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    // StorageSink resolves relative output paths against the working directory (Beam LocalFileSystem),
    // and splits the output on '/' (bucket/object), so a relative path under target/ is used here
    // instead of an absolute Windows path.
    private static final String BASE_DIR = "target/storage-sink-test";

    private static String createSourceJson() {
        return """
                {
                  "name": "create",
                  "module": "create",
                  "parameters": {
                    "type": "int64",
                    "elements": [0, 1, 2],
                    "select": [
                      { "name": "sequence" },
                      { "name": "message", "type": "string", "value": "hello" }
                    ]
                  }
                }
                """;
    }

    private static void cleanDir(final String dir) throws IOException {
        final Path path = Path.of(dir);
        if (Files.exists(path)) {
            try (Stream<Path> walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private static List<Path> listFiles(final String dir) throws IOException {
        try (Stream<Path> walk = Files.walk(Path.of(dir))) {
            return walk.filter(Files::isRegularFile).toList();
        }
    }

    @Test
    public void testWriteJson() throws Exception {
        final String dir = BASE_DIR + "/json";
        cleanDir(dir);
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/data",
                        "format": "json",
                        "suffix": ".json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(createSourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertFalse(files.isEmpty(), "json output files must exist under " + dir);
        final String content = String.join("\n", Files.readAllLines(files.getFirst(), StandardCharsets.UTF_8));
        Assertions.assertTrue(content.contains("\"message\":\"hello\""), "unexpected content: " + content);
        Assertions.assertEquals(3, content.lines().count());
    }

    @Test
    public void testWriteCsv() throws Exception {
        final String dir = BASE_DIR + "/csv";
        cleanDir(dir);
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/data",
                        "format": "csv",
                        "suffix": ".csv",
                        "header": true,
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(createSourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertFalse(files.isEmpty(), "csv output files must exist under " + dir);
        final List<String> lines = Files.readAllLines(files.getFirst(), StandardCharsets.UTF_8);
        Assertions.assertEquals(4, lines.size()); // header + 3 records
        Assertions.assertEquals("sequence,message", lines.getFirst());
    }

    // 3 records with category "a", 2 with category "b"
    private static String createCategorySourceJson() {
        return """
                {
                  "name": "create",
                  "module": "create",
                  "parameters": {
                    "type": "element",
                    "elements": [
                      { "category": "a", "index": 1 },
                      { "category": "b", "index": 2 },
                      { "category": "a", "index": 3 },
                      { "category": "b", "index": 4 },
                      { "category": "a", "index": 5 }
                    ]
                  },
                  "schema": {
                    "fields": [
                      { "name": "category", "type": "string" },
                      { "name": "index", "type": "int" }
                    ]
                  }
                }
                """;
    }

    private static long countLines(final Path file) throws IOException {
        return Files.readAllLines(file, StandardCharsets.UTF_8).stream().filter(l -> !l.isBlank()).count();
    }

    @Test
    public void testWriteJsonDynamicDestination() throws Exception {
        final String dir = BASE_DIR + "/dynamic-json";
        cleanDir(dir);
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/category_${category}/data",
                        "format": "json",
                        "suffix": ".json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(createCategorySourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertEquals(2, files.size(), "one file per category expected: " + files);

        final Path fileA = files.stream()
                .filter(p -> p.toString().replace('\\', '/').endsWith("category_a/data.json"))
                .findFirst().orElseThrow(() -> new AssertionError("category_a/data.json not found in " + files));
        final Path fileB = files.stream()
                .filter(p -> p.toString().replace('\\', '/').endsWith("category_b/data.json"))
                .findFirst().orElseThrow(() -> new AssertionError("category_b/data.json not found in " + files));

        final List<String> linesA = Files.readAllLines(fileA, StandardCharsets.UTF_8);
        Assertions.assertEquals(3, linesA.size());
        Assertions.assertTrue(linesA.stream().allMatch(l -> l.contains("\"category\":\"a\"")), "unexpected content: " + linesA);

        final List<String> linesB = Files.readAllLines(fileB, StandardCharsets.UTF_8);
        Assertions.assertEquals(2, linesB.size());
        Assertions.assertTrue(linesB.stream().allMatch(l -> l.contains("\"category\":\"b\"")), "unexpected content: " + linesB);
    }

    @Test
    public void testWriteJsonDynamicDestinationAutoSharding() throws Exception {
        final String dir = BASE_DIR + "/dynamic-json-autoshard";
        cleanDir(dir);
        // numShards is not set: file counts per destination are runner-dependent,
        // so only the record totals per destination directory are asserted
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/category_${category}/data",
                        "format": "json",
                        "suffix": ".json"
                      }
                    }
                  ]
                }
                """.formatted(createCategorySourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        long totalA = 0;
        long totalB = 0;
        for (final Path file : listFiles(dir)) {
            final String path = file.toString().replace('\\', '/');
            if (path.contains("/category_a/")) {
                totalA += countLines(file);
            } else if (path.contains("/category_b/")) {
                totalB += countLines(file);
            } else {
                Assertions.fail("unexpected output file: " + path);
            }
        }
        Assertions.assertEquals(3, totalA);
        Assertions.assertEquals(2, totalB);
    }

    @Test
    public void testWriteAvroDynamicDestination() throws Exception {
        final String dir = BASE_DIR + "/dynamic-avro";
        cleanDir(dir);
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/category_${category}/data",
                        "format": "avro",
                        "suffix": ".avro",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(createCategorySourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertEquals(2, files.size(), "one file per category expected: " + files);
        Assertions.assertEquals(3, readAvroCategoryCount(files, "category_a/data.avro", "a"));
        Assertions.assertEquals(2, readAvroCategoryCount(files, "category_b/data.avro", "b"));
    }

    private static int readAvroCategoryCount(
            final List<Path> files,
            final String pathSuffix,
            final String expectedCategory) throws IOException {

        final Path file = files.stream()
                .filter(p -> p.toString().replace('\\', '/').endsWith(pathSuffix))
                .findFirst().orElseThrow(() -> new AssertionError(pathSuffix + " not found in " + files));
        int count = 0;
        try (final DataFileReader<GenericRecord> reader =
                     new DataFileReader<>(new SeekableFileInput(file.toFile()), new GenericDatumReader<>())) {
            while (reader.hasNext()) {
                final GenericRecord record = reader.next();
                Assertions.assertEquals(expectedCategory, record.get("category").toString());
                count++;
            }
        }
        return count;
    }

    @Test
    public void testWriteJsonDynamicDestinationWithTimestamp() throws Exception {
        final String dir = BASE_DIR + "/dynamic-json-timestamp";
        cleanDir(dir);
        // ${__timestamp} holds the element event time (timestampAttribute), formatted in UTC
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "category": "x", "eventTime": "2024-10-10T00:00:00Z" },
                          { "category": "y", "eventTime": "2024-10-10T12:00:00Z" },
                          { "category": "z", "eventTime": "2024-11-20T00:00:00Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "category", "type": "string" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "eventTime"
                    }
                  ],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/dt=${__DateTimeUtils.formatTimestamp(__timestamp, 'yyyyMMdd')}/data",
                        "format": "json",
                        "suffix": ".json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertEquals(2, files.size(), "one file per event date expected: " + files);

        final Path file1010 = files.stream()
                .filter(p -> p.toString().replace('\\', '/').endsWith("dt=20241010/data.json"))
                .findFirst().orElseThrow(() -> new AssertionError("dt=20241010/data.json not found in " + files));
        Assertions.assertEquals(2, countLines(file1010));

        final Path file1120 = files.stream()
                .filter(p -> p.toString().replace('\\', '/').endsWith("dt=20241120/data.json"))
                .findFirst().orElseThrow(() -> new AssertionError("dt=20241120/data.json not found in " + files));
        Assertions.assertEquals(1, countLines(file1120));
    }

    @Test
    public void testWriteJsonTemplateSuffix() throws Exception {
        final String dir = BASE_DIR + "/template-suffix";
        cleanDir(dir);
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/data",
                        "format": "json",
                        "suffix": "-${shardIndex}of${numShards}.json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(createSourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertEquals(1, files.size(), "single output file expected: " + files);
        Assertions.assertTrue(
                files.getFirst().toString().replace('\\', '/').endsWith("/data-0of1.json"),
                "unexpected file name: " + files.getFirst());
        Assertions.assertEquals(3, countLines(files.getFirst()));
    }

    @Test
    public void testWriteParquet() throws Exception {
        final String dir = BASE_DIR + "/parquet";
        cleanDir(dir);
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/data",
                        "format": "parquet",
                        "suffix": ".parquet",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(createSourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertFalse(files.isEmpty(), "parquet output files must exist under " + dir);
        Assertions.assertTrue(Files.size(files.getFirst()) > 0, "parquet output file must not be empty");
    }

    @Test
    public void testWriteAvro() throws Exception {
        final String dir = BASE_DIR + "/avro";
        cleanDir(dir);
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/data",
                        "format": "avro",
                        "suffix": ".avro",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(createSourceJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertFalse(files.isEmpty(), "avro output files must exist under " + dir);
        Assertions.assertTrue(Files.size(files.getFirst()) > 0, "avro output file must not be empty");
    }

}
