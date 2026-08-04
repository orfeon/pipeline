package com.mercari.solution.module.sink;

import com.google.api.services.bigquery.model.TableRow;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Construction-time (pipeline graph) tests for the bigquery sink module.
 * The pipelines are only applied, never run, so no BigQuery service is needed.
 */
public class BigQuerySinkTest {

    private static final String CREATE_SOURCE_JSON = """
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
            """;

    @Test
    public void testDatasetIdTableIdParameters() throws Exception {
        // datasetId/tableId (without table) passes validate() and must also work in applyParameters
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "bigquerySink",
                      "module": "bigquery",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "myproject",
                        "datasetId": "mydataset",
                        "tableId": "mytable",
                        "method": "STREAMING_INSERTS",
                        "writeDisposition": "WRITE_APPEND",
                        "createDisposition": "CREATE_NEVER",
                        "outputResult": false
                      }
                    }
                  ]
                }
                """.formatted(CREATE_SOURCE_JSON);

        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        Assertions.assertFalse(outputs.containsKey("bigquerySink"));
    }

    @Test
    public void testDestinationFunctionWithTemplate() {
        final SerializableFunction<TableRow, String> fn = BigQuerySink.createDestinationFunction(
                null, "myproject.mydataset.table_${category}", List.of("category"));

        final TableRow rowA = new TableRow().set("category", "a");
        Assertions.assertEquals("myproject.mydataset.table_a", fn.apply(rowA));
        // repeated calls reuse the cached template
        Assertions.assertEquals("myproject.mydataset.table_a", fn.apply(rowA));
        Assertions.assertEquals(
                "myproject.mydataset.table_b",
                fn.apply(new TableRow().set("category", "b")));
    }

    @Test
    public void testDestinationFunctionWithElement() {
        final Schema schema = Schema.builder()
                .withField(Schema.Field.of("category", Schema.FieldType.STRING.withNullable(false)))
                .build();
        final SerializableFunction<MElement, String> fn = BigQuerySink.createDestinationFunction(
                schema, "myproject.mydataset.table_${category}", List.of("category"));

        final MElement element = MElement.of(schema, Map.of("category", "a"), 0L);
        Assertions.assertEquals("myproject.mydataset.table_a", fn.apply(element));
    }

    @Test
    public void testDestinationFunctionStatic() {
        final SerializableFunction<TableRow, String> fn = BigQuerySink.createDestinationFunction(
                null, "myproject.mydataset.mytable", List.of());
        Assertions.assertEquals(
                "myproject.mydataset.mytable",
                fn.apply(new TableRow().set("category", "a")));
    }

    @Test
    public void testDynamicDestinationTableTemplate() throws Exception {
        // table with a template expression routes through DynamicDestinationFunc at graph build
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "bigquerySink",
                      "module": "bigquery",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "myproject",
                        "table": "myproject.mydataset.table_${id}",
                        "method": "STREAMING_INSERTS",
                        "writeDisposition": "WRITE_APPEND",
                        "createDisposition": "CREATE_NEVER",
                        "outputResult": false
                      }
                    }
                  ]
                }
                """.formatted(CREATE_SOURCE_JSON);

        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        Assertions.assertFalse(outputs.containsKey("bigquerySink"));
    }

    @Test
    public void testBatchStreamingInsertsDefaultOutputResult() throws Exception {
        // batch pipeline + STREAMING_INSERTS without outputResult: the default outputResult=true
        // must not route to WriteResult.getSuccessfulInserts (unsupported for batch), but fall
        // back to the non-result path
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "bigquerySink",
                      "module": "bigquery",
                      "inputs": ["create"],
                      "parameters": {
                        "projectId": "myproject",
                        "table": "myproject.mydataset.mytable",
                        "method": "STREAMING_INSERTS",
                        "writeDisposition": "WRITE_APPEND",
                        "createDisposition": "CREATE_NEVER"
                      }
                    }
                  ]
                }
                """.formatted(CREATE_SOURCE_JSON);

        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        Assertions.assertFalse(outputs.containsKey("bigquerySink"));
    }

}
