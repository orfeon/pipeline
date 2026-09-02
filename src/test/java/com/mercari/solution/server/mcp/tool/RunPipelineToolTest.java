package com.mercari.solution.server.mcp.tool;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RunPipelineToolTest {

    private static final String CONFIG = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  schema:
                    fields:
                      - {name: session_id, type: string}
                      - {name: seller_id, type: string}
                      - {name: start_price, type: float64}
                      - {name: session_time, type: timestamp}
                  elements:
                    - {session_id: A, seller_id: s1, start_price: 100.0, session_time: "2024-01-01T00:00:00Z"}
            transforms:
              - name: features
                module: feature
                inputs: [input]
                parameters:
                  sources:
                    - name: listings
                      eventTime: session_time
                      availability: atEventTime
                      fields:
                        - {name: session_id, type: string}
                        - {name: seller_id, type: string}
                        - {name: start_price, type: float64}
                  lineage:
                    - {fields: [session_id, seller_id, start_price], from: listings}
                  time: {field: session_time}
                  predictAt: "event_time - PT1H"
                  entities:
                    - {name: seller, keys: [seller_id]}
                  features:
                    - name: hist
                      scope: sequence
                      entity: seller
                      ops:
                        - {type: aggregate, fields: [start_price], funcs: [mean]}
                  output:
                    prefix: "${args.prefix}"
            sinks:
              - name: out
                module: debug
                inputs: [features]
            """;

    private static String text(final McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().getFirst()).text();
    }

    @Test
    public void testDryRunReportsSchemasAndFeaturePlans() {
        final McpSchema.CallToolResult result = new RunPipelineTool().sync(null,
                new McpSchema.CallToolRequest("run-pipeline", Map.of("config", CONFIG, "dryRun", true, "args", Map.of("prefix", "f_"))));
        Assertions.assertFalse(result.isError(), text(result));
        final JsonObject json = JsonParser.parseString(text(result)).getAsJsonObject();
        Assertions.assertEquals("ok", json.get("status").getAsString());
        Assertions.assertFalse(json.has("outputs"), "a dry run must not execute");
        // resolved schemas of every step
        Assertions.assertTrue(json.getAsJsonObject("spec").getAsJsonArray("modules").size() >= 2, json::toString);
        // the feature transform's validate --expand report against the real input schema
        final JsonArray plans = json.getAsJsonArray("featurePlans");
        Assertions.assertEquals(1, plans.size(), json::toString);
        final JsonObject plan = plans.get(0).getAsJsonObject();
        Assertions.assertEquals("features", plan.get("name").getAsString());
        Assertions.assertTrue(plan.get("ok").getAsBoolean(), plan::toString);
        final String describe = plan.get("describe").getAsString();
        Assertions.assertTrue(describe.contains("f_hist_all_start_price_mean"), describe); // ${args.prefix} applied
        Assertions.assertTrue(describe.contains("-- audit"), describe);
        Assertions.assertTrue(describe.contains("GROUP BY seller_id"), describe);
    }

    @Test
    public void testDryRunReportsErrors() {
        final String broken = CONFIG.replace("fields: [start_price]", "fields: [missing_field]");
        final McpSchema.CallToolResult result = new RunPipelineTool().sync(null,
                new McpSchema.CallToolRequest("run-pipeline", Map.of("config", broken, "dryRun", "true", "args", "{\"prefix\": \"f_\"}")));
        Assertions.assertTrue(result.isError(), text(result));
        final JsonObject json = JsonParser.parseString(text(result)).getAsJsonObject();
        Assertions.assertEquals("error", json.get("status").getAsString());
        Assertions.assertTrue(json.has("error"), json::toString);
    }

    @Test
    public void testMissingConfig() {
        final McpSchema.CallToolResult result = new RunPipelineTool().sync(null,
                new McpSchema.CallToolRequest("run-pipeline", Map.of("dryRun", true)));
        Assertions.assertTrue(result.isError());
    }

}
