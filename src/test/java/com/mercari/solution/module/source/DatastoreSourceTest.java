package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Assembly-time tests for the datastore source's schema handling
 * (schema-redesign.md Phase 4, fields-only group). No emulator: the pipeline is assembled
 * but never run.
 */
public class DatastoreSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testParametersSchemaIsTheDefinition() throws Exception {
        // Datastore is schemaless: schema.fields is the sole definition (P5)
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "datastoreinput",
                      "module": "datastore",
                      "parameters": {
                        "projectId": "myproject",
                        "gql": "SELECT * FROM mykind",
                        "schema": {
                          "fields": [
                            { "name": "stringField", "type": "string" },
                            { "name": "longField", "type": "long" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("datastoreinput");
        Assertions.assertTrue(output.getSchema().hasField("stringField"));
        Assertions.assertTrue(output.getSchema().hasField("longField"));
    }

    @Test
    public void testMissingSchemaThrows() throws Exception {
        // previously this crashed with an NPE at assembly; now a precise error
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "datastoreinput",
                      "module": "datastore",
                      "parameters": {
                        "projectId": "myproject",
                        "gql": "SELECT * FROM mykind"
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("parameters.schema with fields is required"),
                "unexpected message: " + e.getMessage());
    }

}
