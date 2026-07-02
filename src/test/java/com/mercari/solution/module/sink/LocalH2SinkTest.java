package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LocalH2SinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testValidationErrors() throws Exception {
        // output must be a gs:// path and configs must not be empty
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
                          { "name": "sequence" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "h2",
                      "module": "localH2",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "/tmp/database.zip"
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("must be gcs path"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("configs must not be empty"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testValidationMissingOutput() throws Exception {
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
                          { "name": "sequence" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "h2",
                      "module": "localH2",
                      "inputs": ["create"],
                      "parameters": {
                        "configs": []
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("output must not be null"), "unexpected message: " + e.getMessage());
    }

}
