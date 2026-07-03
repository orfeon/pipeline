package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class DebugSinkTest {

    // Beam FileSystems treats a Windows absolute path ("C:/...") as a URI scheme,
    // so a relative path (resolved against the working directory) is used as workDir.
    private static final String WORK_DIR = "target/debug-sink-test";

    private static final String CONFIG_JSON = """
            {
              "sources": [
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
              ],
              "sinks": [
                {
                  "name": "debug",
                  "module": "debug",
                  "inputs": ["create"],
                  "parameters": {}
                }
              ]
            }
            """;

    @Test
    public void testDebugWithWorkDir() throws Exception {
        cleanDir(WORK_DIR);

        // On DirectRunner the debug sink writes inputs as Avro files under {workDir}/outputs/{name}
        final MPipeline.MPipelineServerOptions options = PipelineOptionsFactory
                .create()
                .as(MPipeline.MPipelineServerOptions.class);
        options.setWorkDir(WORK_DIR);

        final TestPipeline pipeline = TestPipeline
                .fromOptions(options)
                .enableAbandonedNodeEnforcement(false);

        final Config config = Config.load(CONFIG_JSON);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final Path outputDir = Path.of(WORK_DIR, "outputs", "debug");
        Assertions.assertTrue(Files.exists(outputDir), "debug output dir must exist: " + outputDir);
        Assertions.assertTrue(Files.exists(outputDir.resolve("schema.avro")), "schema.avro must exist");
        final List<Path> dataFiles = listFiles(outputDir).stream()
                .filter(p -> !p.getFileName().toString().equals("schema.avro"))
                .toList();
        Assertions.assertFalse(dataFiles.isEmpty(), "debug avro data files must exist under " + outputDir);
    }

    @Test
    public void testDebugWithoutWorkDir() throws Exception {
        // Without workDir the debug sink falls back to log output and the pipeline must terminate
        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

        final Config config = Config.load(CONFIG_JSON);
        MPipeline.apply(pipeline, config);
        pipeline.run();
    }

    private static void cleanDir(final String dir) throws IOException {
        final Path path = Path.of(dir);
        if (Files.exists(path)) {
            try (Stream<Path> walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private static List<Path> listFiles(final Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile).toList();
        }
    }

}
