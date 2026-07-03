package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
