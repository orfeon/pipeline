package com.mercari.solution.module;

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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
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
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=ActionBigQueryIT})
 * for the bigquery action against the goccy BigQuery emulator managed by Testcontainers.
 * Exercises the real Jobs API path: jobs.insert with a deterministic job id and jobs.get polling
 * until DONE — for all three triggers (once / perElement / collect with an elements-list template).
 * See BigQueryIT for the emulator wiring notes.
 */
@Testcontainers
public class ActionBigQueryIT {

    // must match the project the BigQueryEmulatorContainer starts the emulator with
    private static final String PROJECT = "test-project";
    private static final String DATASET = "testds";

    @Container
    private static final BigQueryEmulatorContainer emulator = new BigQueryEmulatorContainer(
            DockerImageName.parse("ghcr.io/goccy/bigquery-emulator:latest"));

    @BeforeAll
    static void setupDataset() throws Exception {
        // Point the direct BigQuery clients (BigQueryUtil.getEmulatorHost) at the container
        System.setProperty("BIGQUERY_EMULATOR_HOST", emulator.getEmulatorHttpEndpoint());

        final Bigquery bigquery = BigQueryUtil.getBigquery();
        bigquery.datasets()
                .insert(PROJECT, new Dataset()
                        .setDatasetReference(new DatasetReference()
                                .setProjectId(PROJECT)
                                .setDatasetId(DATASET)))
                .execute();
        for(final String table : List.of("action_rows", "action_rows_collect")) {
            bigquery.tables()
                    .insert(PROJECT, DATASET, new Table()
                            .setTableReference(new TableReference()
                                    .setProjectId(PROJECT)
                                    .setDatasetId(DATASET)
                                    .setTableId(table))
                            .setSchema(new TableSchema().setFields(List.of(
                                    new TableFieldSchema().setName("id").setType("STRING").setMode("NULLABLE"),
                                    new TableFieldSchema().setName("val").setType("INTEGER").setMode("NULLABLE")))))
                    .execute();
        }
    }

    @AfterAll
    static void cleanup() {
        System.clearProperty("BIGQUERY_EMULATOR_HOST");
    }

    private static TestPipeline createPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        options.as(GcpOptions.class).setProject(PROJECT);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    private static final String SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - field_string: a
                      field_long: 1
                    - field_string: b
                      field_long: 2
                schema:
                  fields:
                    - name: field_string
                      type: string
                    - name: field_long
                      type: int64
            """;

    @Test
    public void testQueryJobOnce() throws Exception {
        final TestPipeline pipeline = createPipeline();
        final String configYaml = """
                actions:
                  - name: bqjob
                    module: bigquery
                    operation: jobs.query
                    parameters:
                      projectId: test-project
                      query: SELECT 1 AS one
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("bqjob");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("bigquery", element.getPrimitiveValue("service"));
                Assertions.assertEquals("jobs.query", element.getPrimitiveValue("operation"));
                Assertions.assertNotNull(element.getPrimitiveValue("jobId"));
                Assertions.assertEquals("DONE", element.getPrimitiveValue("state"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testDmlQueryJobPerElement() throws Exception {
        final TestPipeline pipeline = createPipeline();
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: bqjob
                    module: bigquery
                    operation: jobs.query
                    trigger: perElement
                    inputs:
                      - input
                    parameters:
                      projectId: test-project
                      query: INSERT INTO testds.action_rows (id, val) VALUES ('${field_string}', ${field_long})
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("bqjob");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("DONE", element.getPrimitiveValue("state"));
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run().waitUntilFinish();

        final List<TableRow> rows = BigQueryUtil.query(
                PROJECT, "SELECT id, val FROM testds.action_rows ORDER BY id");
        Assertions.assertNotNull(rows);
        Assertions.assertEquals(2, rows.size());
    }

    @Test
    public void testDmlQueryJobCollect() throws Exception {
        // collect: all input elements gathered into ONE job, via the elements-list template
        final TestPipeline pipeline = createPipeline();
        final String configYaml = SOURCE_YAML + """
                actions:
                  - name: bqjob
                    module: bigquery
                    operation: jobs.query
                    trigger: collect
                    inputs:
                      - input
                    parameters:
                      projectId: test-project
                      query: "INSERT INTO testds.action_rows_collect (id, val) VALUES <#list elements as e>('${e.field_string}', ${e.field_long})<#if e?has_next>,</#if></#list>"
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("bqjob");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("DONE", element.getPrimitiveValue("state"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run().waitUntilFinish();

        final List<TableRow> rows = BigQueryUtil.query(
                PROJECT, "SELECT id, val FROM testds.action_rows_collect ORDER BY id");
        Assertions.assertNotNull(rows);
        Assertions.assertEquals(2, rows.size());
    }

}
