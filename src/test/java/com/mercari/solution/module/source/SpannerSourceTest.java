package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Launch-time validation tests for the spanner source tables mode. The tables.query content
 * checks run before any Spanner connection is opened, so no emulator is required here;
 * the read paths themselves are covered by SpannerIT.
 */
public class SpannerSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String tablesConfigJson(final String tablesJson) {
        return """
                {
                  "sources": [
                    {
                      "name": "spanner",
                      "module": "spanner",
                      "parameters": {
                        "projectId": "fake-project",
                        "instanceId": "fake-instance",
                        "databaseId": "fake-database",
                        "tables": %s
                      }
                    }
                  ]
                }
                """.formatted(tablesJson);
    }

    @Test
    public void testTablesQueryWithoutTableVariableThrows() throws Exception {
        final Config config = Config.load(tablesConfigJson("""
                { "includes": ["Users"], "query": "SELECT * FROM Users" }
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("must reference ${table}"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesQueryWithSplitterThrows() throws Exception {
        final Config config = Config.load(tablesConfigJson("""
                { "query": "SELECT * FROM ${table} --SPLITTER-- SELECT 1 FROM ${table}" }
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("does not support"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesQueryMustBeStringThrows() throws Exception {
        final Config config = Config.load(tablesConfigJson("""
                { "query": ["SELECT * FROM ${table}"] }
                """));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

    @Test
    public void testTablesWithTableParameterThrows() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "spanner",
                      "module": "spanner",
                      "parameters": {
                        "projectId": "fake-project",
                        "instanceId": "fake-instance",
                        "databaseId": "fake-database",
                        "table": "Users",
                        "tables": ["*"]
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

}
