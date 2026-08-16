package com.mercari.solution.module.source;

import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Launch-time validation tests for the postgres source tables mode. These checks run before
 * any database connection is opened, so no container is required here; the read paths
 * themselves are covered by PostgresIT.
 */
public class PostgresSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String configJson(final String parametersJson) {
        return """
                {
                  "sources": [
                    {
                      "name": "postgres",
                      "module": "postgres",
                      "parameters": {
                        "url": "jdbc:postgresql://localhost:5432/fake",
                        "user": "fake",
                        "password": "fake",
                        %s
                      }
                    }
                  ]
                }
                """.formatted(parametersJson);
    }

    @Test
    public void testMissingTableAndTablesThrows() throws Exception {
        final Config config = Config.load(configJson("""
                "splitSize": 1000
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("parameters.table or parameters.tables"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesWithTableParameterThrows() throws Exception {
        final Config config = Config.load(configJson("""
                "table": "users",
                "tables": ["*"]
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("must not be set together"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesWithSelectParameterThrows() throws Exception {
        final Config config = Config.load(configJson("""
                "select": "id",
                "tables": ["*"]
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("tables.select"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesInvalidTypeThrows() throws Exception {
        final Config config = Config.load(configJson("""
                "tables": "users"
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("pattern array or an object"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesSelectMustBeStringThrows() throws Exception {
        final Config config = Config.load(configJson("""
                "tables": { "includes": ["users"], "select": ["id"] }
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("'tables.select' must be a string"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesPatternsMustBeStringArrayThrows() throws Exception {
        final Config config = Config.load(configJson("""
                "tables": { "includes": [1, 2] }
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("must be a string array"), "unexpected message: " + e.getMessage());
    }

    private static String cdcConfigJson(final String parametersJson) {
        return """
                {
                  "sources": [
                    {
                      "name": "postgres",
                      "module": "postgres",
                      "mode": "changeDataCapture",
                      "parameters": {
                        "url": "jdbc:postgresql://localhost:5432/fake",
                        "user": "fake",
                        "password": "fake"%s
                      }
                    }
                  ]
                }
                """.formatted(parametersJson.isEmpty() ? "" : ",\n" + parametersJson);
    }

    @Test
    public void testCdcModeWithoutCdcParameterThrows() throws Exception {
        final Config config = Config.load(cdcConfigJson(""));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("parameters.cdc must not be null"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testCdcModeWithoutSlotAndPublicationThrows() throws Exception {
        final Config config = Config.load(cdcConfigJson("""
                "cdc": {}
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("parameters.cdc.slot"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("parameters.cdc.publication"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testCdcModeWithTableParameterThrows() throws Exception {
        final Config config = Config.load(cdcConfigJson("""
                "table": "users",
                "cdc": { "slot": "myslot", "publication": "mypub" }
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("not applicable"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testCdcParameterInBatchModeThrows() throws Exception {
        final Config config = Config.load(configJson("""
                "table": "users",
                "cdc": { "slot": "myslot", "publication": "mypub" }
                """));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("only applicable"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testTablesParameterMatching() {
        // bare patterns match public-schema tables only
        final PostgresSource.TablesParameter bare = PostgresSource.TablesParameter
                .of(JsonParser.parseString("[\"users\", \"item_*\"]"));
        Assertions.assertTrue(bare.matches("public", "users"));
        Assertions.assertTrue(bare.matches("public", "item_2024"));
        Assertions.assertFalse(bare.matches("public", "orders"));
        Assertions.assertFalse(bare.matches("other", "users"));

        // qualified patterns match schema.name
        final PostgresSource.TablesParameter qualified = PostgresSource.TablesParameter
                .of(JsonParser.parseString("[\"other.*\", \"audit.log_*\"]"));
        Assertions.assertTrue(qualified.matches("other", "users"));
        Assertions.assertTrue(qualified.matches("audit", "log_2024"));
        Assertions.assertFalse(qualified.matches("audit", "users"));
        Assertions.assertFalse(qualified.matches("public", "users"));

        // default includes is ["*"]: all public-schema tables; excludes filter matches out
        final PostgresSource.TablesParameter excludes = PostgresSource.TablesParameter
                .of(JsonParser.parseString("{\"excludes\": [\"tmp_*\"]}"));
        Assertions.assertTrue(excludes.matches("public", "users"));
        Assertions.assertFalse(excludes.matches("public", "tmp_work"));
        Assertions.assertFalse(excludes.matches("other", "users"));
    }

}
