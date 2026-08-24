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
                                    ") PRIMARY KEY (id)",
                            // dedicated table for the FLOAT32/UUID column type support test
                            "CREATE TABLE TypesTableE ( " +
                                    "id STRING(64) NOT NULL, " +
                                    "f32 FLOAT32, " +
                                    "uid UUID " +
                                    ") PRIMARY KEY (id)",
                            // interleaved parent/child tables for the cdc apply mode tests
                            "CREATE TABLE CdcOrders ( " +
                                    "orderId STRING(64) NOT NULL, " +
                                    "status STRING(64), " +
                                    "amount INT64 " +
                                    ") PRIMARY KEY (orderId)",
                            "CREATE TABLE CdcOrderItems ( " +
                                    "orderId STRING(64) NOT NULL, " +
                                    "itemId INT64 NOT NULL, " +
                                    "sku STRING(64) " +
                                    ") PRIMARY KEY (orderId, itemId), INTERLEAVE IN PARENT CdcOrders ON DELETE CASCADE"))
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

    /** FLOAT32 and UUID columns round-trip through the sink and the all-tables source. */
    @Test
    public void testFloat32AndUuidColumns() throws Exception {
        final String uuidA = "0f4657bd-0e6b-4f8e-a0b1-8fca47b4b5d4";
        final String uuidB = "9d2c6a1e-3f5b-4c7d-8e9f-0a1b2c3d4e5f";

        // pipeline 1: write rows including float32 and uuid fields
        final String sinkConfigJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "e1", "f32": 1.5, "uid": "%s" },
                          { "id": "e2", "f32": -2.25, "uid": "%s" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "f32", "type": "float32" },
                          { "name": "uid", "type": "uuid" }
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
                        "table": "TypesTableE",
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(uuidA, uuidB, PROJECT, INSTANCE, DATABASE);

        final TestPipeline writePipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: all-tables source; the schema comes from INFORMATION_SCHEMA (FLOAT32/UUID parsing)
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
                        "tables": ["TypesTable*"],
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, DATABASE);

        final TestPipeline readPipeline = createReadPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));

        final MCollection output = outputs.get("spannerAll.TypesTableE");
        Assertions.assertNotNull(output, "spannerAll.TypesTableE not found in: " + outputs.keySet());
        Assertions.assertNotNull(output.getSchema().getField("f32"));
        Assertions.assertNotNull(output.getSchema().getField("uid"));

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "e1" -> {
                        Assertions.assertEquals(1.5F, (Float) row.getPrimitiveValue("f32"), (float) DELTA);
                        Assertions.assertEquals(uuidA, row.getPrimitiveValue("uid"));
                    }
                    case "e2" -> {
                        Assertions.assertEquals(-2.25F, (Float) row.getPrimitiveValue("f32"), (float) DELTA);
                        Assertions.assertEquals(uuidB, row.getPrimitiveValue("uid"));
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


    // ---- cdc apply mode ----

    private static String envelope(final String table, final String op, final String keys, final String after, final long commitMicros, final String sequence, final String transactionId) {
        final com.google.gson.JsonObject json = new com.google.gson.JsonObject();
        json.addProperty("table", table);
        json.addProperty("op", op);
        json.addProperty("keys", keys);
        json.addProperty("after", after);
        json.addProperty("commitTimestamp", commitMicros);
        json.addProperty("sequence", sequence);
        final com.google.gson.JsonObject source = new com.google.gson.JsonObject();
        source.addProperty("provider", "test");
        json.add("source", source);
        if(transactionId != null) {
            final com.google.gson.JsonObject transaction = new com.google.gson.JsonObject();
            transaction.addProperty("id", transactionId);
            json.add("transaction", transaction);
        }
        return json.toString();
    }

    private static String cdcConfig(final boolean transactional, final List<String> envelopes) {
        final StringBuilder elements = new StringBuilder();
        for(final String envelope : envelopes) {
            if(!elements.isEmpty()) {
                elements.append(",");
            }
            elements.append("{ \"payload\": ").append(new com.google.gson.Gson().toJson(envelope)).append(" }");
        }
        return """
                {
                  "sources": [
                    {
                      "name": "changes",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ %s ]
                      },
                      "schema": {
                        "fields": [ { "name": "payload", "type": "string" } ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "normalize",
                      "module": "cdc",
                      "inputs": ["changes"],
                      "parameters": { "format": "envelope", "field": "payload" }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "replica",
                      "module": "spanner",
                      "inputs": ["normalize"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "databaseId": "%s",
                        "table": "${table}",
                        "cdc": true,
                        "transactional": %s,
                        "emulator": true
                      }
                    }
                  ]
                }
                """.formatted(elements, PROJECT, INSTANCE, DATABASE, transactional);
    }

    private static Map<String, com.google.cloud.spanner.Struct> readRows(final String sql, final String keyColumn) {
        final Map<String, com.google.cloud.spanner.Struct> rows = new java.util.HashMap<>();
        try(final Spanner spanner = SpannerOptions.newBuilder()
                .setProjectId(PROJECT)
                .setEmulatorHost(emulator.getEmulatorGrpcEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService()) {
            final com.google.cloud.spanner.DatabaseClient client = spanner
                    .getDatabaseClient(com.google.cloud.spanner.DatabaseId.of(PROJECT, INSTANCE, DATABASE));
            try(final com.google.cloud.spanner.ResultSet resultSet = client.singleUse()
                    .executeQuery(com.google.cloud.spanner.Statement.of(sql))) {
                while(resultSet.next()) {
                    rows.put(resultSet.getCurrentRowAsStruct().getString(keyColumn), resultSet.getCurrentRowAsStruct());
                }
            }
        }
        return rows;
    }

    @Test
    public void testCdcApplyUpsertAndDelete() throws Exception {
        // parent first, then children, an update, a delete of a child, a control record
        final List<String> envelopes = List.of(
                envelope("CdcOrders", "INSERT", "{\"orderId\":\"o1\"}", "{\"status\":\"new\",\"amount\":10}", 1000L, "3e8/0", null),
                envelope("CdcOrders", "INSERT", "{\"orderId\":\"o2\"}", "{\"status\":\"new\",\"amount\":20}", 1000L, "3e8/1", null));
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(p1, Config.load(cdcConfig(false, envelopes)));
        p1.run().waitUntilFinish();

        final List<String> envelopes2 = List.of(
                envelope("CdcOrderItems", "INSERT", "{\"orderId\":\"o1\",\"itemId\":1}", "{\"sku\":\"A\"}", 2000L, "7d0/0", null),
                envelope("CdcOrderItems", "INSERT", "{\"orderId\":\"o1\",\"itemId\":2}", "{\"sku\":\"B\"}", 2000L, "7d0/1", null),
                // partial after: amount must be kept
                envelope("CdcOrders", "UPDATE", "{\"orderId\":\"o1\"}", "{\"status\":\"paid\"}", 2000L, "7d0/2", null),
                envelope("CdcOrders", "DELETE", "{\"orderId\":\"o2\"}", null, 2000L, "7d0/3", null),
                envelope("CdcOrders", "SCHEMA", null, null, 2000L, "7d0", null));
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(p2, Config.load(cdcConfig(false, envelopes2)));
        p2.run().waitUntilFinish();

        final Map<String, com.google.cloud.spanner.Struct> orders = readRows("SELECT orderId, status, amount FROM CdcOrders WHERE orderId LIKE 'o%'", "orderId");
        Assertions.assertEquals(1, orders.size());
        Assertions.assertEquals("paid", orders.get("o1").getString("status"));
        Assertions.assertEquals(10L, orders.get("o1").getLong("amount"));
        final Map<String, com.google.cloud.spanner.Struct> items = readRows("SELECT CAST(itemId AS STRING) AS itemId, sku FROM CdcOrderItems WHERE orderId = 'o1'", "itemId");
        Assertions.assertEquals(2, items.size());
        Assertions.assertEquals("B", items.get("2").getString("sku"));
    }

    @Test
    public void testCdcApplyTransactional() throws Exception {
        // one transaction inserts a parent and its children: the child before the parent in the
        // input, which only works when the whole transaction is one commit
        final List<String> envelopes = List.of(
                envelope("CdcOrderItems", "INSERT", "{\"orderId\":\"t1\",\"itemId\":1}", "{\"sku\":\"X\"}", 5000L, "1388/1", "tx-1"),
                envelope("CdcOrders", "INSERT", "{\"orderId\":\"t1\"}", "{\"status\":\"new\",\"amount\":1}", 5000L, "1388/0", "tx-1"),
                // same row twice in one transaction: collapsed to the latest
                envelope("CdcOrders", "UPDATE", "{\"orderId\":\"t1\"}", "{\"status\":\"paid\"}", 5000L, "1388/2", "tx-1"),
                // another transaction
                envelope("CdcOrders", "INSERT", "{\"orderId\":\"t2\"}", "{\"status\":\"new\",\"amount\":2}", 6000L, "1770/0", "tx-2"));
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        MPipeline.apply(p, Config.load(cdcConfig(true, envelopes)));
        p.run().waitUntilFinish();

        final Map<String, com.google.cloud.spanner.Struct> orders = readRows("SELECT orderId, status, amount FROM CdcOrders WHERE orderId LIKE 't%'", "orderId");
        Assertions.assertEquals(2, orders.size());
        Assertions.assertEquals("paid", orders.get("t1").getString("status"));
        Assertions.assertEquals(1L, orders.get("t1").getLong("amount"));
        Assertions.assertEquals("new", orders.get("t2").getString("status"));
        final Map<String, com.google.cloud.spanner.Struct> items = readRows("SELECT CAST(itemId AS STRING) AS itemId, sku FROM CdcOrderItems WHERE orderId = 't1'", "itemId");
        Assertions.assertEquals("X", items.get("1").getString("sku"));
    }

}
