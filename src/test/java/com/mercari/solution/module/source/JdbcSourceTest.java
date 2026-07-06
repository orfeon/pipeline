package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Assembly-time tests for the jdbc source's schema handling
 * (schema-redesign.md Phase 4, fields-only group).
 */
public class JdbcSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testDeclaredSchemaSkipsInference() throws Exception {
        // with a declared schema the query-metadata inference (which needs a live DB
        // connection at assembly time) is skipped entirely
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "jdbcinput",
                      "module": "jdbc",
                      "parameters": {
                        "url": "jdbc:h2:mem:jdbcsourcetest",
                        "driver": "org.h2.Driver",
                        "user": "sa",
                        "password": "",
                        "query": "SELECT 1 AS id",
                        "schema": {
                          "fields": [
                            { "name": "id", "type": "long" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        Assertions.assertTrue(outputs.get("jdbcinput").getSchema().hasField("id"));
    }

}
