package com.mercari.solution.module.sink;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -Dit -Dit.test=SpannerIT}) for the
 * spanner sink and source modules against the Cloud Spanner emulator managed by Testcontainers.
 *
 * The emulator container is bound to a random host port. The pipeline modules resolve the
 * emulator endpoint via {@code SpannerUtil.getEmulatorHost()}, which honors the
 * {@code SPANNER_EMULATOR_HOST} environment variable or system property, so this test only needs
 * to set the system property to the container's mapped endpoint (no fixed host ports required).
 */
@Testcontainers
public class SpannerIT {

    private static final double DELTA = 1e-9;

    private static final String PROJECT = "test-project";
    private static final String INSTANCE = "test-instance";
    private static final String DATABASE = "testdb";

    @Container
    private static final SpannerEmulatorContainer emulator = new SpannerEmulatorContainer(
            DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:latest"));

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @BeforeAll
    static void setupInstanceAndDatabase() throws Exception {
        // Point the pipeline modules (SpannerUtil.getEmulatorHost) at the container's mapped port
        System.setProperty("SPANNER_EMULATOR_HOST", emulator.getEmulatorGrpcEndpoint());

        try(final Spanner spanner = SpannerOptions.newBuilder()
                .setProjectId(PROJECT)
                .setEmulatorHost(emulator.getEmulatorGrpcEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService()) {

            spanner.getInstanceAdminClient()
                    .createInstance(InstanceInfo.newBuilder(InstanceId.of(PROJECT, INSTANCE))
                            .setInstanceConfigId(InstanceConfigId.of(PROJECT, "emulator-config"))
                            .setDisplayName("integration-test")
                            .setNodeCount(1)
                            .build())
                    .get(60, TimeUnit.SECONDS);

            spanner.getDatabaseAdminClient()
                    .createDatabase(INSTANCE, DATABASE, List.of(
                            "CREATE TABLE RoundTrip ( " +
                                    "id STRING(64) NOT NULL, " +
                                    "longvalue INT64, " +
                                    "doublevalue FLOAT64, " +
                                    "boolvalue BOOL, " +
                                    "createdat TIMESTAMP " +
                                    ") PRIMARY KEY (id)",
                            "CREATE TABLE DeleteTest ( " +
                                    "id STRING(64) NOT NULL, " +
                                    "longvalue INT64 " +
                                    ") PRIMARY KEY (id)",
                            "CREATE TABLE RoundTripElement ( " +
                                    "id STRING(64) NOT NULL, " +
                                    "longvalue INT64, " +
                                    "createdat TIMESTAMP, " +
                                    "birthday DATE " +
                                    ") PRIMARY KEY (id)",
                            // dedicated tables for the all-tables (tables parameter) source test
                            "CREATE TABLE AllTablesA ( " +
                                    "id STRING(64) NOT NULL, " +
                                    "value INT64 " +
                                    ") PRIMARY KEY (id)",
                            "CREATE TABLE AllTablesB ( " +
                                    "id STRING(64) NOT NULL, " +
                                    "label STRING(64) " +
                                    ") PRIMARY KEY (id)",
                            // dedicated table for the tables.query (common per-table query) test;
                            // named so the other test's "AllTables*" pattern does not match it
                            "CREATE TABLE QueryTablesC ( " +
                                    "id STRING(64) NOT NULL, " +
                                    "value INT64 " +
                                    ") PRIMARY KEY (id)"))
                    .get(60, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    static void cleanup() {
        System.clearProperty("SPANNER_EMULATOR_HOST");
    }

    /**
     * The spanner source outputs MElements wrapping com.google.cloud.spanner.Struct, encoded with
     * SerializableCoder. Reading struct values (PAssert) touches the Struct's lazily-decoded
     * internal state, which changes its serialized form and false-positives DirectRunner's
     * byte-level immutability check — so that check is disabled for the read pipelines.
     */
    private static TestPipeline createReadPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.setEnforceImmutability(false);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    @Test
    public void testRoundTripInsert() throws Exception {
        // pipeline 1: create source -> spanner sink (default mutationOp: INSERT_OR_UPDATE)
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
                          { "id": "a", "longvalue": 1, "doublevalue": 0.15, "boolvalue": true,  "createdat": "2024-10-10T00:00:00Z" },
                          { "id": "b", "longvalue": 2, "doublevalue": 1.15, "boolvalue": false, "createdat": "2024-10-20T00:00:00Z" },
                          { "id": "c", "longvalue": 3, "doublevalue": 2.15, "boolvalue": true,  "createdat": "2024-10-30T00:00:00Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" },
                          { "name": "doublevalue", "type": "float64" },
                          { "name": "boolvalue", "type": "bool" },
                          { "name": "createdat", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "spannerSink",
                      "module": "spanner",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "RoundTrip",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        MPipeline.apply(pipeline, Config.load(sinkConfigJson));
        pipeline.run().waitUntilFinish();

        // pipeline 2: spanner source (table read; the query path is covered by the other tests) -> assert
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "spannerSource",
                      "module": "spanner",
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "RoundTrip",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline readPipeline = createReadPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("spannerSource");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "a" -> {
                        Assertions.assertEquals(1L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(0.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, row.getPrimitiveValue("boolvalue"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-10T00:00:00.000Z"), row.getPrimitiveValue("createdat"));
                    }
                    case "b" -> {
                        Assertions.assertEquals(2L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(1.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.FALSE, row.getPrimitiveValue("boolvalue"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-20T00:00:00.000Z"), row.getPrimitiveValue("createdat"));
                    }
                    case "c" -> {
                        Assertions.assertEquals(3L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(2.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, row.getPrimitiveValue("boolvalue"));
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
    public void testRoundTripElementInput() throws Exception {
        // pipeline 1: create source WITHOUT the "outputType": "AVRO" workaround -> spanner sink.
        // The sink receives ELEMENT-typed inputs, exercising the ELEMENT-map path of
        // ElementToSpannerMutationConverter incl. timestamp and date columns.
        final String sinkConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "a", "longvalue": 1, "createdat": "2024-10-10T01:23:45Z", "birthday": "2000-05-15" },
                          { "id": "b", "longvalue": 2, "createdat": "2024-10-20T12:34:56Z", "birthday": "1999-12-31" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" },
                          { "name": "createdat", "type": "timestamp" },
                          { "name": "birthday", "type": "date" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "spannerSink",
                      "module": "spanner",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "RoundTripElement",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline writePipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: spanner source (query) -> assert timestamps and dates round-tripped
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "spannerSource",
                      "module": "spanner",
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "query": "SELECT id, longvalue, createdat, birthday FROM RoundTripElement",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline readPipeline = createReadPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("spannerSource");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "a" -> {
                        Assertions.assertEquals(1L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-10T01:23:45.000Z"), row.getPrimitiveValue("createdat"));
                        Assertions.assertEquals((int) java.time.LocalDate.parse("2000-05-15").toEpochDay(), row.getPrimitiveValue("birthday"));
                    }
                    case "b" -> {
                        Assertions.assertEquals(2L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-20T12:34:56.000Z"), row.getPrimitiveValue("createdat"));
                        Assertions.assertEquals((int) java.time.LocalDate.parse("1999-12-31").toEpochDay(), row.getPrimitiveValue("birthday"));
                    }
                    default -> Assertions.fail("unexpected id: " + row.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

    /**
     * The all-tables batch mode: the source lists matching base tables at assembly time and
     * outputs one tagged collection per table ({@code spannerAll.AllTablesA}, ...), each carrying
     * the {@code table} attribute. The storage sink consumes them via a wildcard input and fans
     * out per table using the assembly-time {@code ${input.table}} template.
     */
    @Test
    public void testAllTablesReadFanOutToStorage() throws Exception {
        // pipeline 1: populate the two dedicated tables
        final String sinkConfigJson = """
                {
                  "sources": [
                    {
                      "name": "createA",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "a1", "value": 1 },
                          { "id": "a2", "value": 2 },
                          { "id": "a3", "value": 3 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "value", "type": "int64" }
                        ]
                      }
                    },
                    {
                      "name": "createB",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "b1", "label": "x" },
                          { "id": "b2", "label": "y" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "label", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "sinkA",
                      "module": "spanner",
                      "inputs": ["createA"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "AllTablesA",
                        "emulator": true
                      }
                    },
                    {
                      "name": "sinkB",
                      "module": "spanner",
                      "inputs": ["createB"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "AllTablesB",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE, PROJECT, INSTANCE, DATABASE);

        final TestPipeline writePipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: all-tables source -> wildcard fan-out storage sink (per-table json files)
        final String outputDir = "target/spanner-it-alltables";
        final java.nio.file.Path outputPath = java.nio.file.Path.of(outputDir);
        if (java.nio.file.Files.exists(outputPath)) {
            try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(outputPath)) {
                walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }

        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "spannerAll",
                      "module": "spanner",
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "tables": {
                          "includes": ["AllTables*"]
                        },
                        "emulator": true
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "storage",
                      "module": "storage",
                      "inputs": ["spannerAll.*"],
                      "parameters": {
                        "output": "%s/${input.table}/data",
                        "format": "json",
                        "suffix": ".json",
                        "numShards": 1
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE, outputDir);

        final TestPipeline readPipeline = createReadPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));

        // one tagged output per matched table, with per-table schema and the table attribute
        final MCollection outputA = outputs.get("spannerAll.AllTablesA");
        Assertions.assertNotNull(outputA, "spannerAll.AllTablesA not found in: " + outputs.keySet());
        Assertions.assertEquals("AllTablesA", outputA.getAttributes().get("table"));
        Assertions.assertNotNull(outputA.getSchema().getField("value"));
        final MCollection outputB = outputs.get("spannerAll.AllTablesB");
        Assertions.assertNotNull(outputB, "spannerAll.AllTablesB not found in: " + outputs.keySet());
        Assertions.assertNotNull(outputB.getSchema().getField("label"));

        readPipeline.run().waitUntilFinish();

        final java.util.List<String> filesA;
        try (java.util.stream.Stream<String> lines = java.nio.file.Files.lines(
                java.nio.file.Path.of(outputDir, "AllTablesA", "data.json"))) {
            filesA = lines.filter(l -> !l.isBlank()).toList();
        }
        Assertions.assertEquals(3, filesA.size(), "unexpected AllTablesA content: " + filesA);
        Assertions.assertTrue(filesA.stream().allMatch(l -> l.contains("\"id\":\"a")), "unexpected AllTablesA content: " + filesA);

        final java.util.List<String> filesB;
        try (java.util.stream.Stream<String> lines = java.nio.file.Files.lines(
                java.nio.file.Path.of(outputDir, "AllTablesB", "data.json"))) {
            filesB = lines.filter(l -> !l.isBlank()).toList();
        }
        Assertions.assertEquals(2, filesB.size(), "unexpected AllTablesB content: " + filesB);
        Assertions.assertTrue(filesB.stream().allMatch(l -> l.contains("\"id\":\"b")), "unexpected AllTablesB content: " + filesB);
    }

    /**
     * The tables.query parameter: a common per-table query template ({@code ${table}} required)
     * replaces the generated default {@code SELECT * FROM <table>}; the output schema is then
     * resolved from the query, so computed columns such as a snapshot timestamp appear in it.
     */
    @Test
    public void testAllTablesCustomQuery() throws Exception {
        // pipeline 1: populate the dedicated table
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
                          { "id": "c1", "value": 10 },
                          { "id": "c2", "value": 20 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "value", "type": "int64" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "spannerSink",
                      "module": "spanner",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "QueryTablesC",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline writePipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: tables mode with a common query adding a computed column
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "spannerAll",
                      "module": "spanner",
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "tables": {
                          "includes": ["QueryTables*"],
                          "query": "SELECT id, value, CURRENT_TIMESTAMP() AS snapshot_at FROM ${table}"
                        },
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline readPipeline = createReadPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));

        final MCollection output = outputs.get("spannerAll.QueryTablesC");
        Assertions.assertNotNull(output, "spannerAll.QueryTablesC not found in: " + outputs.keySet());
        Assertions.assertEquals("QueryTablesC", output.getAttributes().get("table"));
        // the schema comes from the query, so the computed column must be present
        Assertions.assertNotNull(output.getSchema().getField("snapshot_at"));

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "c1" -> Assertions.assertEquals(10L, row.getAsLong("value"));
                    case "c2" -> Assertions.assertEquals(20L, row.getAsLong("value"));
                    default -> Assertions.fail("unexpected id: " + row.getAsString("id"));
                }
                Assertions.assertNotNull(row.getPrimitiveValue("snapshot_at"));
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

    @Test
    public void testDeleteMutation() throws Exception {
        // pipeline 1: insert three rows
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
                          { "id": "b", "longvalue": 2 },
                          { "id": "c", "longvalue": 3 }
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
                      "name": "spannerSink",
                      "module": "spanner",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "DeleteTest",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        MPipeline.apply(pipeline, Config.load(insertConfigJson));
        pipeline.run().waitUntilFinish();

        // pipeline 2: DELETE mutation for key "b"
        final String deleteConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "outputType": "AVRO",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "b" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "spannerSink",
                      "module": "spanner",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "DeleteTest",
                        "mutationOp": "DELETE",
                        "keyFields": ["id"],
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline deletePipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(deletePipeline, Config.load(deleteConfigJson));
        deletePipeline.run().waitUntilFinish();

        // pipeline 3: read back and verify only "a" and "c" remain
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "spannerSource",
                      "module": "spanner",
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "query": "SELECT id, longvalue FROM DeleteTest",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline readPipeline = createReadPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("spannerSource");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "a" -> Assertions.assertEquals(1L, row.getAsLong("longvalue"));
                    case "c" -> Assertions.assertEquals(3L, row.getAsLong("longvalue"));
                    default -> Assertions.fail("unexpected id: " + row.getAsString("id"));
                }
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        readPipeline.run().waitUntilFinish();
    }

}
