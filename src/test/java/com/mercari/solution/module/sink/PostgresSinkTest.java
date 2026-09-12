package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Launch-time validation tests for the postgres sink. These checks run before any database
 * connection is opened, so no container is required here; the write paths themselves are
 * covered by PostgresIT.
 */
public class PostgresSinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String configJson(final String parametersJson) {
        return """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "id": 1, "name": "a", "op": "INSERT" } ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "int64" },
                          { "name": "name", "type": "string" },
                          { "name": "op", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "postgres",
                      "module": "postgres",
                      "inputs": ["create"],
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

    private String assertLaunchError(final String parametersJson) throws Exception {
        final Config config = Config.load(configJson(parametersJson));
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        return e.getMessage();
    }

    @Test
    public void testMissingTableThrows() throws Exception {
        final String message = assertLaunchError("""
                "op": "INSERT"
                """);
        Assertions.assertTrue(message.contains("parameters.table must not be null"), "unexpected message: " + message);
    }

    @Test
    public void testUnsupportedOpThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "op": "UPSERT"
                """);
        Assertions.assertTrue(message.contains("parameters.op: UPSERT is not supported"), "unexpected message: " + message);
    }

    @Test
    public void testMergeOpIsNotSelectableThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "op": "MERGE"
                """);
        Assertions.assertTrue(message.contains("selected implicitly"), "unexpected message: " + message);
    }

    @Test
    public void testOpFieldWithOpThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "op": "INSERT_OR_UPDATE",
                "opField": "op"
                """);
        Assertions.assertTrue(message.contains("must not be set together with parameters.opField"), "unexpected message: " + message);
    }

    @Test
    public void testOpFieldMissingInInputThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "opField": "operation"
                """);
        Assertions.assertTrue(message.contains("opField: operation does not exist"), "unexpected message: " + message);
    }

    @Test
    public void testUpdateConditionWithoutUpsertThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "updateCondition": "target.a < excluded.a"
                """);
        Assertions.assertTrue(message.contains("updateCondition is only applicable"), "unexpected message: " + message);
    }

    @Test
    public void testCdcParametersWithoutCdcThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "sequenceField": "seq",
                "onTruncate": "apply"
                """);
        Assertions.assertTrue(message.contains("sequenceField is only applicable"), "unexpected message: " + message);
        Assertions.assertTrue(message.contains("onTruncate is only applicable"), "unexpected message: " + message);
    }

    @Test
    public void testCdcRequiresEnvelopeInputThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "cdc": true
                """);
        Assertions.assertTrue(message.contains("requires unified change records"), "unexpected message: " + message);
    }

    @Test
    public void testIllegalEmptyTableThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "emptyTable": "drop"
                """);
        Assertions.assertTrue(message.contains("emptyTable must be true, false"), "unexpected message: " + message);
    }

    @Test
    public void testIllegalBatchParametersThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "batchSize": -1,
                "maxBatchBytes": 0
                """);
        Assertions.assertTrue(message.contains("batchSize must not be negative"), "unexpected message: " + message);
        Assertions.assertTrue(message.contains("maxBatchBytes must be positive"), "unexpected message: " + message);
    }

    @Test
    public void testIllegalSettingNameThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "users",
                "settings": { "synchronous_commit; DROP TABLE x": "off" }
                """);
        Assertions.assertTrue(message.contains("is not a valid setting name"), "unexpected message: " + message);
    }

    @Test
    public void testCreateTableWithTemplatedTableThrows() throws Exception {
        final String message = assertLaunchError("""
                "table": "${table}",
                "createTable": true,
                "cdc": true
                """);
        Assertions.assertTrue(message.contains("createTable requires a fixed table name"), "unexpected message: " + message);
    }

    @Test
    public void testPadSequence() {
        Assertions.assertEquals("00000000000000ff/0000000000000001", PostgresSink.padSequence("ff/1"));
        Assertions.assertEquals("0000000000000000", PostgresSink.padSequence("0"));
        // padded values compare as ChangeRecord.compareSequence does
        Assertions.assertTrue(PostgresSink.padSequence("100/1").compareTo(PostgresSink.padSequence("ff/2")) > 0);
        Assertions.assertTrue(PostgresSink.padSequence("ff/2").compareTo(PostgresSink.padSequence("ff/10")) < 0);
        Assertions.assertTrue(PostgresSink.padSequence("ff").compareTo(PostgresSink.padSequence("ff/0")) < 0);
    }

}
