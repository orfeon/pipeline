package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

    @Test
    public void testWriteArrow() throws Exception {
        final String dir = BASE_DIR + "/arrow";
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
                        "format": "arrow",
                        "suffix": ".arrow",
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
        Assertions.assertFalse(files.isEmpty(), "arrow output files must exist under " + dir);
        final List<Long> sequences = new ArrayList<>();
        try(final BufferAllocator allocator = new RootAllocator();
            final FileInputStream is = new FileInputStream(files.getFirst().toFile());
            final ArrowFileReader reader = new ArrowFileReader(is.getChannel(), allocator)) {

            while(reader.loadNextBatch()) {
                final VectorSchemaRoot root = reader.getVectorSchemaRoot();
                final BigIntVector sequence = (BigIntVector) root.getVector("sequence");
                final VarCharVector message = (VarCharVector) root.getVector("message");
                for(int i = 0; i < root.getRowCount(); i++) {
                    sequences.add(sequence.get(i));
                    Assertions.assertEquals("hello", new String(message.get(i), StandardCharsets.UTF_8));
                }
            }
        }
        Assertions.assertEquals(List.of(0L, 1L, 2L), sequences.stream().sorted().toList());
    }

    @Test
    public void testWriteArrowZstdMultiBatch() throws Exception {
        final String dir = BASE_DIR + "/arrow-zstd";
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
                        "format": "arrow",
                        "suffix": ".arrow",
                        "codec": "ZSTD",
                        "batchSize": 2,
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
        Assertions.assertFalse(files.isEmpty(), "arrow output files must exist under " + dir);
        long total = 0;
        int batches = 0;
        try(final BufferAllocator allocator = new RootAllocator();
            final FileInputStream is = new FileInputStream(files.getFirst().toFile());
            final ArrowFileReader reader = new ArrowFileReader(
                    is.getChannel(), allocator, CommonsCompressionFactory.INSTANCE)) {

            while(reader.loadNextBatch()) {
                total += reader.getVectorSchemaRoot().getRowCount();
                batches++;
            }
        }
        Assertions.assertEquals(3, total);
        Assertions.assertEquals(2, batches); // batchSize=2 with 3 records -> batches of 2 and 1
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
