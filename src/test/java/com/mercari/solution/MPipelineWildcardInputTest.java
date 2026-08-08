package com.mercari.solution;

import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
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

/**
 * Tests for wildcard inputs ({@code inputs: ["module.*"]}) and the assembly-time
 * {@code ${input.*}} template with sink fan-out. Uses the partition transform to produce
 * tagged outputs and the storage sink writing under target/ (same conventions as StorageSinkTest).
 */
public class MPipelineWildcardInputTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String BASE_DIR = "target/mpipeline-wildcard-test";

    // 3 records with category "a", 2 with category "b", partitioned into partition.a / partition.b
    private static String sourceAndPartitionJson() {
        return """
                  "sources": [
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
                  ],
                  "transforms": [
                    {
                      "name": "partition",
                      "module": "partition",
                      "inputs": ["create"],
                      "parameters": {
                        "exclusive": true,
                        "partitions": [
                          {
                            "name": "a",
                            "filter": [
                              { "key": "category", "op": "in", "value": ["a"] }
                            ]
                          },
                          {
                            "name": "b",
                            "filter": [
                              { "key": "category", "op": "in", "value": ["b"] }
                            ]
                          }
                        ]
                      }
                    }
                  ]
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

    private static long countLines(final Path file) throws IOException {
        return Files.readAllLines(file, StandardCharsets.UTF_8).stream().filter(l -> !l.isBlank()).count();
    }

    @Test
    public void testWildcardFanOutSinkWithInputTemplate() throws Exception {
        final String dir = BASE_DIR + "/fanout";
        cleanDir(dir);
        final String configJson = """
                {
                %s,
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["partition.*"],
                      "parameters": {
                        "output": "%s/${input.tag}/data",
                        "format": "json",
                        "suffix": ".json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(sourceAndPartitionJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertEquals(2, files.size(), "one file per partition expected: " + files);

        final Path fileA = files.stream()
                .filter(p -> p.toString().replace('\\', '/').endsWith("/a/data.json"))
                .findFirst().orElseThrow(() -> new AssertionError("a/data.json not found in " + files));
        Assertions.assertEquals(3, countLines(fileA));
        Assertions.assertTrue(Files.readAllLines(fileA, StandardCharsets.UTF_8).stream()
                .allMatch(l -> l.contains("\"category\":\"a\"")));

        final Path fileB = files.stream()
                .filter(p -> p.toString().replace('\\', '/').endsWith("/b/data.json"))
                .findFirst().orElseThrow(() -> new AssertionError("b/data.json not found in " + files));
        Assertions.assertEquals(2, countLines(fileB));
        Assertions.assertTrue(Files.readAllLines(fileB, StandardCharsets.UTF_8).stream()
                .allMatch(l -> l.contains("\"category\":\"b\"")));
    }

    @Test
    public void testWildcardFanOutSinkMixesRuntimeTemplate() throws Exception {
        final String dir = BASE_DIR + "/fanout-runtime";
        cleanDir(dir);
        // ${input.tag} resolves at assembly time, ${index} per element at runtime
        final String configJson = """
                {
                %s,
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["partition.*"],
                      "parameters": {
                        "output": "%s/${input.tag}/index_${index}/data",
                        "format": "json",
                        "suffix": ".json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(sourceAndPartitionJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertEquals(5, files.size(), "one file per element expected: " + files);
        Assertions.assertTrue(files.stream()
                        .map(p -> p.toString().replace('\\', '/'))
                        .anyMatch(p -> p.endsWith("/a/index_1/data.json")),
                "a/index_1/data.json not found in " + files);
        Assertions.assertTrue(files.stream()
                        .map(p -> p.toString().replace('\\', '/'))
                        .anyMatch(p -> p.endsWith("/b/index_2/data.json")),
                "b/index_2/data.json not found in " + files);
    }

    @Test
    public void testWildcardWithoutInputTemplateUnionsInputs() throws Exception {
        final String dir = BASE_DIR + "/union";
        cleanDir(dir);
        final String configJson = """
                {
                %s,
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["partition.*"],
                      "parameters": {
                        "output": "%s/data",
                        "format": "json",
                        "suffix": ".json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(sourceAndPartitionJson(), dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final List<Path> files = listFiles(dir);
        Assertions.assertEquals(1, files.size(), "single union output file expected: " + files);
        Assertions.assertEquals(5, countLines(files.getFirst()));
    }

    @Test
    public void testWildcardMatchingNoTaggedOutputThrows() throws Exception {
        // the create source registers only its plain name, so create.* matches nothing
        final String configJson = """
                {
                %s,
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["create.*"],
                      "parameters": {
                        "output": "%s/data",
                        "format": "json"
                      }
                    }
                  ]
                }
                """.formatted(sourceAndPartitionJson(), BASE_DIR + "/nomatch");

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

    @Test
    public void testInputTemplateOnTransformThrows() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "category": "a", "index": 1 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "category", "type": "string" },
                          { "name": "index", "type": "int" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "partition",
                      "module": "partition",
                      "inputs": ["create"],
                      "parameters": {
                        "partitions": [
                          {
                            "name": "a",
                            "filter": [
                              { "key": "category", "op": "in", "value": ["a"] }
                            ]
                          }
                        ]
                      }
                    },
                    {
                      "name": "select",
                      "module": "select",
                      "inputs": ["partition.*"],
                      "parameters": {
                        "fields": [
                          { "name": "category" },
                          { "name": "tag", "type": "string", "value": "${input.tag}" }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

}
