package com.mercari.solution.module.sink;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.util.cloud.google.BigQueryUtil;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=BigQueryIT})
 * for the bigquery sink module against the goccy BigQuery emulator managed by Testcontainers.
 *
 * Emulator wiring:
 * <ul>
 *   <li>Beam BigQueryIO resolves the REST endpoint from {@code BigQueryOptions#getBigQueryEndpoint},
 *       which this test sets to the container's HTTP endpoint via {@code TestPipeline.fromOptions}.</li>
 *   <li>The direct (non-Beam) BigQuery clients in {@code BigQueryUtil} (used for schema preflight,
 *       queries and test setup/verification here) resolve the endpoint from
 *       {@code BIGQUERY_EMULATOR_HOST} (env var > system property, see
 *       {@code BigQueryUtil.getEmulatorHost}); this test sets the system property.</li>
 * </ul>
 *
 * Write method: {@code STREAMING_INSERTS} (tabledata.insertAll), which the goccy emulator supports
 * well. {@code FILE_LOADS} is not usable because the load job's source URIs must be readable by the
 * emulator process (GCS or the container's filesystem), and {@code STORAGE_WRITE_API} support in the
 * emulator is partial (gRPC, see below).
 *
 * Reading back through the bigquery *source* module is not possible against this emulator:
 * Beam's only bounded read paths are {@code EXPORT} (runs an extract job to GCS, which the emulator
 * cannot write to) and {@code DIRECT_READ} (Storage Read API). For {@code DIRECT_READ}, Beam feeds
 * the single {@code bigQueryEndpoint} option to both the REST client and the gRPC storage client,
 * while the emulator serves REST and gRPC on different ports; additionally Beam's gRPC channel is
 * always TLS while the emulator's gRPC server is plaintext. Written rows are therefore verified via
 * {@code BigQueryUtil.query} (jobs.query) against the emulator instead.
 */
@Testcontainers
public class BigQueryIT {

    private static final double DELTA = 1e-9;

    // must match the project the BigQueryEmulatorContainer starts the emulator with
    private static final String PROJECT = "test-project";
    private static final String DATASET = "testds";

    @Container
    private static final BigQueryEmulatorContainer emulator = new BigQueryEmulatorContainer(
            DockerImageName.parse("ghcr.io/goccy/bigquery-emulator:latest"));

    @BeforeAll
    static void setupDatasetAndTable() throws Exception {
        // Point the direct BigQuery clients (BigQueryUtil.getEmulatorHost) at the container
        System.setProperty("BIGQUERY_EMULATOR_HOST", emulator.getEmulatorHttpEndpoint());

        final Bigquery bigquery = BigQueryUtil.getBigquery();
        bigquery.datasets()
                .insert(PROJECT, new Dataset()
                        .setDatasetReference(new DatasetReference()
                                .setProjectId(PROJECT)
                                .setDatasetId(DATASET)))
                .execute();
        bigquery.tables()
                .insert(PROJECT, DATASET, new Table()
                        .setTableReference(new TableReference()
                                .setProjectId(PROJECT)
                                .setDatasetId(DATASET)
                                .setTableId("roundtrip"))
                        .setSchema(new TableSchema().setFields(List.of(
                                new TableFieldSchema().setName("id").setType("STRING").setMode("NULLABLE"),
                                new TableFieldSchema().setName("longvalue").setType("INTEGER").setMode("NULLABLE"),
                                new TableFieldSchema().setName("doublevalue").setType("FLOAT").setMode("NULLABLE"),
                                new TableFieldSchema().setName("boolvalue").setType("BOOLEAN").setMode("NULLABLE")))))
                .execute();
    }

    @AfterAll
    static void cleanup() {
        System.clearProperty("BIGQUERY_EMULATOR_HOST");
    }

    /**
     * Creates a TestPipeline whose options point Beam BigQueryIO at the emulator's HTTP endpoint.
     */
    private static TestPipeline createPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.as(GcpOptions.class).setProject(PROJECT);
        final BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
        bigQueryOptions.setBigQueryEndpoint(emulator.getEmulatorHttpEndpoint());
        bigQueryOptions.setBigQueryProject(PROJECT);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    @Test
    public void testWriteStreamingInserts() throws Exception {
        // pipeline: create source -> bigquery sink (streaming inserts into the pre-created table)
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
                      "name": "bigquerySink",
                      "module": "bigquery",
                      "inputs": ["create"],
                      "parameters": {
                        "table": "%s.%s.roundtrip",
                        "method": "STREAMING_INSERTS",
                        "writeDisposition": "WRITE_APPEND",
                        "createDisposition": "CREATE_NEVER",
                        "outputResult": false
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, DATASET);

        final TestPipeline writePipeline = createPipeline();
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // verify the written rows via jobs.query against the emulator
        final List<TableRow> rows = BigQueryUtil.query(PROJECT,
                "SELECT id, longvalue, doublevalue, boolvalue FROM " + DATASET + ".roundtrip ORDER BY id");

        Assertions.assertNotNull(rows);
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("a", cell(rows.get(0), 0));
        Assertions.assertEquals(1L, Long.parseLong(cell(rows.get(0), 1)));
        Assertions.assertEquals(0.15D, Double.parseDouble(cell(rows.get(0), 2)), DELTA);
        Assertions.assertTrue(Boolean.parseBoolean(cell(rows.get(0), 3)));

        Assertions.assertEquals("b", cell(rows.get(1), 0));
        Assertions.assertEquals(2L, Long.parseLong(cell(rows.get(1), 1)));
        Assertions.assertEquals(1.15D, Double.parseDouble(cell(rows.get(1), 2)), DELTA);
        Assertions.assertFalse(Boolean.parseBoolean(cell(rows.get(1), 3)));

        Assertions.assertEquals("c", cell(rows.get(2), 0));
        Assertions.assertEquals(3L, Long.parseLong(cell(rows.get(2), 1)));
        Assertions.assertEquals(2.15D, Double.parseDouble(cell(rows.get(2), 2)), DELTA);
        Assertions.assertTrue(Boolean.parseBoolean(cell(rows.get(2), 3)));
    }

    @Test
    public void testWriteCreateIfNeeded() throws Exception {
        // pipeline: create source -> bigquery sink creating the destination table itself
        // (exercises the CreateTables path and ElementToTableRowConverter schema conversion)
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
                      "name": "bigquerySink",
                      "module": "bigquery",
                      "inputs": ["create"],
                      "parameters": {
                        "table": "%s.%s.createtest",
                        "method": "STREAMING_INSERTS",
                        "writeDisposition": "WRITE_APPEND",
                        "createDisposition": "CREATE_IF_NEEDED",
                        "outputResult": false
                      }
                    }
                  ]
                }
                """.formatted(PROJECT, DATASET);

        final TestPipeline writePipeline = createPipeline();
        MPipeline.apply(writePipeline, Config.load(sinkConfigJson));
        writePipeline.run().waitUntilFinish();

        // the sink should have created the table (schema preflight via the emulator-wired client)
        final TableSchema createdSchema = BigQueryUtil.getTableSchemaFromTable(
                new TableReference().setProjectId(PROJECT).setDatasetId(DATASET).setTableId("createtest"));
        Assertions.assertNotNull(createdSchema);
        Assertions.assertEquals(2, createdSchema.getFields().size());

        // verify the written rows via jobs.query against the emulator
        final List<TableRow> rows = BigQueryUtil.query(PROJECT,
                "SELECT id, longvalue FROM " + DATASET + ".createtest ORDER BY id");

        Assertions.assertNotNull(rows);
        Assertions.assertEquals(2, rows.size());

        Assertions.assertEquals("a", cell(rows.get(0), 0));
        Assertions.assertEquals(1L, Long.parseLong(cell(rows.get(0), 1)));
        Assertions.assertEquals("b", cell(rows.get(1), 0));
        Assertions.assertEquals(2L, Long.parseLong(cell(rows.get(1), 1)));
    }

    private static String cell(final TableRow row, final int index) {
        final Object value = row.getF().get(index).getV();
        return value == null ? null : value.toString();
    }

}
