package com.mercari.solution.server.mcp.tool;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.modelcontextprotocol.modern.McpSchema;
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
                Tool.request("run-pipeline", Map.of("config", CONFIG, "dryRun", true, "args", Map.of("prefix", "f_"))));
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
        // ... and the answer says which args it was assembled with
        Assertions.assertEquals("f_", JsonParser.parseString(text(result)).getAsJsonObject().getAsJsonObject("args").get("prefix").getAsString());
        Assertions.assertTrue(describe.contains("-- audit"), describe);
        Assertions.assertTrue(describe.contains("GROUP BY seller_id"), describe);
    }

    @Test
    public void testDryRunStructuredFeaturePlan() {
        final Map<String, Object> arguments = Map.of("config", CONFIG, "dryRun", true, "args", Map.of("prefix", "f_"));
        // json: the plan as structure instead of the text report
        final Map<String, Object> jsonArguments = new java.util.HashMap<>(arguments);
        jsonArguments.put("featurePlanFormat", "json");
        final McpSchema.CallToolResult result = new RunPipelineTool().sync(null, Tool.request("run-pipeline", jsonArguments));
        Assertions.assertFalse(result.isError(), text(result));
        final JsonObject entry = JsonParser.parseString(text(result)).getAsJsonObject()
                .getAsJsonArray("featurePlans").get(0).getAsJsonObject();
        Assertions.assertFalse(entry.has("describe"), entry::toString);
        final JsonObject plan = entry.getAsJsonObject("plan");
        Assertions.assertTrue(plan.getAsJsonArray("stages").size() > 0, plan::toString);
        boolean found = false;
        for (final com.google.gson.JsonElement column : plan.getAsJsonArray("columns")) {
            found |= "f_hist_all_start_price_mean".equals(column.getAsJsonObject().get("name").getAsString());
        }
        Assertions.assertTrue(found, plan::toString);
        Assertions.assertTrue(plan.has("diagnostics") && plan.get("diagnostics").isJsonArray(), plan::toString);

        // both: the two side by side
        jsonArguments.put("featurePlanFormat", "both");
        final JsonObject both = JsonParser.parseString(text(new RunPipelineTool().sync(null, Tool.request("run-pipeline", jsonArguments))))
                .getAsJsonObject().getAsJsonArray("featurePlans").get(0).getAsJsonObject();
        Assertions.assertTrue(both.has("describe") && both.has("plan"), both::toString);

        jsonArguments.put("featurePlanFormat", "yaml");
        Assertions.assertTrue(new RunPipelineTool().sync(null, Tool.request("run-pipeline", jsonArguments)).isError());
    }

    @Test
    public void testDryRunExpandsReferencedDocumentsWithTemplateArgs() {
        // a sources document referenced from the step (here as data:) that uses a template argument: the plan report
        // must expand it with the step's arguments as the assembly does, not fail a config that assembled
        final String sources = """
                sources:
                  - name: listings
                    eventTime: ${time_field}
                    availability: atEventTime
                    fields:
                      - {name: session_id, type: string}
                      - {name: seller_id, type: string}
                      - {name: start_price, type: float64}
                """;
        final String reference = "data:" + java.util.Base64.getEncoder()
                .encodeToString(sources.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // the step's inline sources (parameters.sources ... up to lineage) replaced by the reference
        final int from = CONFIG.indexOf("      sources:\n");
        final int to = CONFIG.indexOf("      lineage:\n");
        Assertions.assertTrue(from > 0 && to > from, CONFIG);
        final String config = "system:\n  args:\n    time_field: session_time\n"
                + CONFIG.substring(0, from) + "      sources: \"" + reference + "\"\n" + CONFIG.substring(to);
        final McpSchema.CallToolResult result = new RunPipelineTool().sync(null,
                Tool.request("run-pipeline", Map.of("config", config, "dryRun", true, "args", Map.of("prefix", "f_"))));
        Assertions.assertFalse(result.isError(), text(result));
        final JsonObject json = JsonParser.parseString(text(result)).getAsJsonObject();
        Assertions.assertEquals("ok", json.get("status").getAsString());
        final JsonObject plan = json.getAsJsonArray("featurePlans").get(0).getAsJsonObject();
        Assertions.assertTrue(plan.get("ok").getAsBoolean(), plan::toString);
    }

    @Test
    public void testDryRunReportsErrors() {
        final String broken = CONFIG.replace("fields: [start_price]", "fields: [missing_field]");
        final McpSchema.CallToolResult result = new RunPipelineTool().sync(null,
                Tool.request("run-pipeline", Map.of("config", broken, "dryRun", "true", "args", "{\"prefix\": \"f_\"}")));
        Assertions.assertTrue(result.isError(), text(result));
        final JsonObject json = JsonParser.parseString(text(result)).getAsJsonObject();
        Assertions.assertEquals("error", json.get("status").getAsString());
        Assertions.assertTrue(json.has("error"), json::toString);
        // the reason is readable on its own: the failing step, and a message without a stack trace in it
        final JsonObject error = json.getAsJsonObject("error");
        Assertions.assertEquals("features", error.get("name").getAsString(), error::toString);
        final String message = error.get("message").getAsString();
        Assertions.assertTrue(message.contains("missing_field"), error::toString);
        Assertions.assertFalse(message.contains("com.mercari.solution.MPipeline"), message);
        Assertions.assertEquals(message, error.getAsJsonArray("messages").get(0).getAsString());
    }

    @Test
    public void testMissingConfig() {
        final McpSchema.CallToolResult result = new RunPipelineTool().sync(null,
                Tool.request("run-pipeline", Map.of("dryRun", true)));
        Assertions.assertTrue(result.isError());
    }

}
