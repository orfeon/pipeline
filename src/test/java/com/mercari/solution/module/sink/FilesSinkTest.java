package com.mercari.solution.module.sink;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class FilesSinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @TempDir
    Path tempDir;

    private String tempDirPath() {
        return tempDir.toString().replace('\\', '/');
    }

    @Test
    public void testWriteJsonFiles() throws Exception {
        final String dir = tempDirPath();
        final String configJson = """
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
                      "name": "files",
                      "module": "files",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/${sequence}.json",
                        "content": {
                          "format": "json"
                        }
                      }
                    }
                  ]
                }
                """.formatted(dir);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        Assertions.assertNotNull(outputs.get("files"));

        pipeline.run();

        for (long i = 0; i < 3; i++) {
            final Path file = tempDir.resolve(i + ".json");
            Assertions.assertTrue(Files.exists(file), "output file must exist: " + file);
            final JsonObject json = JsonParser
                    .parseString(Files.readString(file, StandardCharsets.UTF_8))
                    .getAsJsonObject();
            Assertions.assertEquals(i, json.get("sequence").getAsLong());
            Assertions.assertEquals("hello", json.get("message").getAsString());
        }
    }

    @Test
    public void testWriteTextTemplateContent() throws Exception {
        final String dir = tempDirPath();
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1],
                        "select": [
                          { "name": "sequence" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "files",
                      "module": "files",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/text/${sequence}.txt",
                        "content": {
                          "text": "id=${sequence}"
                        }
                      }
                    }
                  ]
                }
                """.formatted(dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        for (long i = 0; i < 2; i++) {
            final Path file = tempDir.resolve("text").resolve(i + ".txt");
            Assertions.assertTrue(Files.exists(file), "output file must exist: " + file);
            Assertions.assertEquals("id=" + i, Files.readString(file, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testWriteFieldContent() throws Exception {
        final String dir = tempDirPath();
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0],
                        "select": [
                          { "name": "sequence" },
                          { "name": "body", "type": "string", "value": "field content body" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "files",
                      "module": "files",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s/field/${sequence}.txt",
                        "content": {
                          "field": "body"
                        }
                      }
                    }
                  ]
                }
                """.formatted(dir);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final Path file = tempDir.resolve("field").resolve("0.txt");
        Assertions.assertTrue(Files.exists(file), "output file must exist: " + file);
        Assertions.assertEquals("field content body", Files.readString(file, StandardCharsets.UTF_8));
    }

}
