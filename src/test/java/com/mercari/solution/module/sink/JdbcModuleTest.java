package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * End-to-end tests for the jdbc source and sink modules using an in-memory H2 database.
 * No Docker or network required: H2 runs inside the JVM
 * (DB_CLOSE_DELAY=-1 keeps the database alive across connections).
 */
public class JdbcModuleTest {

    private static final double DELTA = 1e-9;

    private static final String DRIVER = "org.h2.Driver";

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String h2Url(final String name) {
        // DATABASE_TO_LOWER keeps identifier (column label) casing identical to the config field names
        return "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE";
    }

    @Test
    public void testRoundTripInsert() throws Exception {
        final String url = h2Url("jdbc_module_test_insert");

        // pipeline 1: create source -> jdbc sink (createTable)
        final String sinkConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "a", "longvalue": 1, "doublevalue": 0.15, "stringvalue": "hello", "createdat": "2024-10-10T00:00:00Z" },
                          { "id": "b", "longvalue": 2, "doublevalue": 1.15, "stringvalue": "world", "createdat": "2024-10-20T00:00:00Z" },
                          { "id": "c", "longvalue": 3, "doublevalue": 2.15, "stringvalue": "!",     "createdat": "2024-10-30T00:00:00Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" },
                          { "name": "doublevalue", "type": "float64" },
                          { "name": "stringvalue", "type": "string" },
                          { "name": "createdat", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "jdbcSink",
                      "module": "jdbc",
                      "inputs": ["create"],
                      "parameters": {
                        "table": "results",
                        "url": "%s",
                        "driver": "%s",
                        "user": "sa",
                        "password": "",
                        "createTable": true,
                        "keyFields": ["id"]
                      }
                    }
                  ]
                }
                """.formatted(url, DRIVER);

        final Config sinkConfig = Config.load(sinkConfigJson);
        MPipeline.apply(pipeline, sinkConfig);
        pipeline.run().waitUntilFinish();

        // pipeline 2: jdbc source (query) -> assert
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "jdbcSource",
                      "module": "jdbc",
                      "parameters": {
                        "url": "%s",
                        "driver": "%s",
                        "user": "sa",
                        "password": "",
                        "query": "SELECT id, longvalue, doublevalue, stringvalue, createdat FROM results"
                      }
                    }
                  ]
                }
                """.formatted(url, DRIVER);

        final TestPipeline readPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Config sourceConfig = Config.load(sourceConfigJson);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, sourceConfig);
        final MCollection output = outputs.get("jdbcSource");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "a" -> {
                        Assertions.assertEquals(1L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(0.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals("hello", row.getAsString("stringvalue"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-10T00:00:00.000Z"), row.getPrimitiveValue("createdat"));
                    }
                    case "b" -> {
                        Assertions.assertEquals(2L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(1.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals("world", row.getAsString("stringvalue"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-20T00:00:00.000Z"), row.getPrimitiveValue("createdat"));
                    }
                    case "c" -> {
                        Assertions.assertEquals(3L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(2.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals("!", row.getAsString("stringvalue"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-30T00:00:00.000Z"), row.getPrimitiveValue("createdat"));
                    }
                    default -> Assertions.fail("unexpected id: " + row.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

    @Test
    public void testInsertOrUpdate() throws Exception {
        final String url = h2Url("jdbc_module_test_upsert");

        // pipeline 1: initial insert of two rows
        final String insertConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "a", "longvalue": 1 },
                          { "id": "b", "longvalue": 2 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "jdbcSink",
                      "module": "jdbc",
                      "inputs": ["create"],
                      "parameters": {
                        "table": "results",
                        "url": "%s",
                        "driver": "%s",
                        "user": "sa",
                        "password": "",
                        "createTable": true,
                        "keyFields": ["id"]
                      }
                    }
                  ]
                }
                """.formatted(url, DRIVER);

        MPipeline.apply(pipeline, Config.load(insertConfigJson));
        pipeline.run().waitUntilFinish();

        // pipeline 2: INSERT_OR_UPDATE (H2 MERGE): update key "b", insert new key "c"
        final String upsertConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "b", "longvalue": 20 },
                          { "id": "c", "longvalue": 30 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "jdbcSink",
                      "module": "jdbc",
                      "inputs": ["create"],
                      "parameters": {
                        "table": "results",
                        "url": "%s",
                        "driver": "%s",
                        "user": "sa",
                        "password": "",
                        "op": "INSERT_OR_UPDATE",
                        "keyFields": ["id"]
                      }
                    }
                  ]
                }
                """.formatted(url, DRIVER);

        final TestPipeline upsertPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(upsertPipeline, Config.load(upsertConfigJson));
        upsertPipeline.run().waitUntilFinish();

        // pipeline 3: read back and verify merge result
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "jdbcSource",
                      "module": "jdbc",
                      "parameters": {
                        "url": "%s",
                        "driver": "%s",
                        "user": "sa",
                        "password": "",
                        "query": "SELECT id, longvalue FROM results"
                      }
                    }
                  ]
                }
                """.formatted(url, DRIVER);

        final TestPipeline readPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("jdbcSource");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "a" -> Assertions.assertEquals(1L, row.getAsLong("longvalue"));
                    case "b" -> Assertions.assertEquals(20L, row.getAsLong("longvalue"));
                    case "c" -> Assertions.assertEquals(30L, row.getAsLong("longvalue"));
                    default -> Assertions.fail("unexpected id: " + row.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

    @Test
    public void testSinkValidationErrors() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
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
                      "name": "jdbcSink",
                      "module": "jdbc",
                      "inputs": ["create"],
                      "parameters": {}
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("must contain table"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("must contain connection url"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("must contain driverClassName"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("must contain user"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("must contain password"), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testSourceValidationErrors() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "jdbcSource",
                      "module": "jdbc",
                      "parameters": {}
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(
                IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("parameters.url must not be null"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("parameters.driver must not be null"), "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("parameters.query or parameters.table must not be null"), "unexpected message: " + e.getMessage());
    }

}
