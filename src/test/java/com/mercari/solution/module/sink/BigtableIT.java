package com.mercari.solution.module.sink;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=BigtableIT})
 * for the bigtable sink and source modules against the Cloud Bigtable emulator managed by
 * Testcontainers.
 *
 * The bigtable modules use Beam BigtableIO, pointed at the emulator via
 * {@code BigtableIO.write().withEmulator} / {@code BigtableIO.read().withEmulator}, wired from
 * the modules' {@code emulatorHost} parameter (resolution: parameter > BIGTABLE_EMULATOR_HOST
 * env var > system property, see {@code BigtableUtil.getEmulatorHost}). This test passes the
 * container's mapped endpoint as the {@code emulatorHost} parameter, so no fixed host ports or
 * env vars are required. With the emulator configured, Beam uses plaintext transport and no
 * credentials, so no real GCP credentials are needed.
 *
 * Tables and column families are created up front with the Bigtable admin client
 * (the emulator auto-creates the instance namespace, but tables must exist before writes).
 */
@Testcontainers
public class BigtableIT {

    private static final double DELTA = 1e-9;

    private static final String PROJECT = "test-project";
    private static final String INSTANCE = "test-instance";

    @Container
    private static final BigtableEmulatorContainer emulator = new BigtableEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

    @BeforeAll
    static void setupTables() throws Exception {
        try(final BigtableTableAdminClient admin = BigtableTableAdminClient.create(
                BigtableTableAdminSettings.newBuilderForEmulator(emulator.getHost(), emulator.getEmulatorPort())
                        .setProjectId(PROJECT)
                        .setInstanceId(INSTANCE)
                        .build())) {

            admin.createTable(CreateTableRequest.of("RoundTrip").addFamily("f"));
            admin.createTable(CreateTableRequest.of("DeleteTest").addFamily("f"));
        }
    }

    /**
     * Creates a TestPipeline. For read pipelines the DirectRunner's byte-level immutability
     * check is disabled, following the same workaround as SpannerIT (the source outputs
     * MElements built from protobuf Rows).
     */
    private static TestPipeline createPipeline(final boolean read) {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.as(GcpOptions.class).setProject(PROJECT);
        if(read) {
            options.setEnforceImmutability(false);
        }
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    @Test
    public void testRoundTrip() throws Exception {
        // pipeline 1: create source -> bigtable sink
        // (rowKey from the id field, fields written as cells of family "f" in the default bytes format)
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
                          { "id": "a", "longvalue": 1, "doublevalue": 0.15, "boolvalue": true },
                          { "id": "b", "longvalue": 2, "doublevalue": 1.15, "boolvalue": false },
                          { "id": "c", "longvalue": 3, "doublevalue": 2.15, "boolvalue": true }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "longvalue", "type": "int64" },
                          { "name": "doublevalue", "type": "float64" },
                          { "name": "boolvalue", "type": "bool" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "bigtableSink",
                      "module": "bigtable",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "tableId": "RoundTrip",
                        "rowKey": "${id}",
                        "columns": [
                          {
                            "family": "f",
                            "qualifiers": [
                              { "name": "id" },
                              { "name": "longvalue" },
                              { "name": "doublevalue" },
                              { "name": "boolvalue" }
                            ]
                          }
                        ],
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, emulator.getEmulatorEndpoint());

        final TestPipeline writePipeline = createPipeline(false);
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: bigtable source (read rows, decode cells per qualifier type) -> assert
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "bigtableSource",
                      "module": "bigtable",
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "tableId": "RoundTrip",
                        "columns": [
                          {
                            "family": "f",
                            "qualifiers": [
                              { "name": "id", "type": "string" },
                              { "name": "longvalue", "type": "int64" },
                              { "name": "doublevalue", "type": "float64" },
                              { "name": "boolvalue", "type": "bool" }
                            ]
                          }
                        ],
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, emulator.getEmulatorEndpoint());

        final TestPipeline readPipeline = createPipeline(true);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("bigtableSource");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for(final MElement row : rows) {
                switch (row.getAsString("id")) {
                    case "a" -> {
                        Assertions.assertEquals(1L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(0.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, row.getPrimitiveValue("boolvalue"));
                    }
                    case "b" -> {
                        Assertions.assertEquals(2L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(1.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.FALSE, row.getPrimitiveValue("boolvalue"));
                    }
                    case "c" -> {
                        Assertions.assertEquals(3L, row.getAsLong("longvalue"));
                        Assertions.assertEquals(2.15D, row.getAsDouble("doublevalue"), DELTA);
                        Assertions.assertEquals(Boolean.TRUE, row.getPrimitiveValue("boolvalue"));
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
    public void testDeleteFromRow() throws Exception {
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
                      "name": "bigtableSink",
                      "module": "bigtable",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "tableId": "DeleteTest",
                        "rowKey": "${id}",
                        "columns": [
                          {
                            "family": "f",
                            "qualifiers": [
                              { "name": "id" },
                              { "name": "longvalue" }
                            ]
                          }
                        ],
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, emulator.getEmulatorEndpoint());

        final TestPipeline writePipeline = createPipeline(false);
        MPipeline.apply(writePipeline, Config.load(insertConfigJson));
        writePipeline.run().waitUntilFinish();

        // pipeline 2: DELETE_FROM_ROW mutation for row key "b"
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
                      "name": "bigtableSink",
                      "module": "bigtable",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "tableId": "DeleteTest",
                        "rowKey": "${id}",
                        "mutationOp": "DELETE_FROM_ROW",
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, emulator.getEmulatorEndpoint());

        final TestPipeline deletePipeline = createPipeline(false);
        MPipeline.apply(deletePipeline, Config.load(deleteConfigJson));
        deletePipeline.run().waitUntilFinish();

        // pipeline 3: read back and verify only "a" and "c" remain
        final String sourceConfigJson = """
                {
                  "sources": [
                    {
                      "name": "bigtableSource",
                      "module": "bigtable",
                      "parameters": {
                        "projectId": "%s",
                        "instanceId": "%s",
                        "tableId": "DeleteTest",
                        "columns": [
                          {
                            "family": "f",
                            "qualifiers": [
                              { "name": "id", "type": "string" },
                              { "name": "longvalue", "type": "int64" }
                            ]
                          }
                        ],
                        "emulatorHost": "%s"
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, INSTANCE, emulator.getEmulatorEndpoint());

        final TestPipeline readPipeline = createPipeline(true);
        final Map<String, MCollection> outputs = MPipeline.apply(readPipeline, Config.load(sourceConfigJson));
        final MCollection output = outputs.get("bigtableSource");

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
