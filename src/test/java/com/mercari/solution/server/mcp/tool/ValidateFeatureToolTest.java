package com.mercari.solution.server.mcp.tool;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.server.agent.tool.FeatureValidator;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ValidateFeatureToolTest {

    private static final String CONFIG = """
            sources:
              - name: input
                module: create
                timestampAttribute: session_time
                parameters:
                  type: element
                  elements: []
            transforms:
              - name: features
                module: feature
                inputs: [input]
                parameters:
                  sources:
                    sources:
                      - name: listings
                        eventTime: session_time
                        fields:
                          - {name: session_id, type: string}
                          - {name: seller_id, type: string}
                          - {name: start_price, type: float64, kind: attribute}
                          - {name: sold, type: int32, availableAt: after(event), kind: outcome}
                        settlementLag: PT30M
                        ingestionLag: P1D
                  lineage:
                    - {fields: [session_id, seller_id, start_price, sold], from: listings}
                  time: {field: session_time}
                  predictAt: "event_time - PT10M"
                  entities:
                    - {name: seller, keys: [seller_id]}
                  features:
                    - name: recent
                      scope: sequence
                      entity: seller
                      ops:
                        - {type: lag, fields: [sold], k: 1}
                    - name: leak
                      scope: row
                      expr: "sold * 2"
            """;

    private static String text(final McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().getFirst()).text();
    }

    @Test
    public void testValidateFeatureReportsViolation() {
        final ValidateFeatureTool tool = new ValidateFeatureTool();
        final McpSchema.CallToolResult result = tool.sync(null,
                new McpSchema.CallToolRequest("validate-feature", Map.of("config", CONFIG)));
        Assertions.assertTrue(result.isError(), text(result));
        final JsonObject json = JsonParser.parseString(text(result)).getAsJsonObject();
        Assertions.assertFalse(json.get("ok").getAsBoolean());
        Assertions.assertTrue(text(result).contains("availability.violation"));
        Assertions.assertTrue(text(result).contains("recent_all_sold_lag1"));
        Assertions.assertTrue(json.getAsJsonObject("plan").has("stages"));
    }

    @Test
    public void testValidateFeatureTextFormatAndParameters() {
        final JsonObject config = com.mercari.solution.config.Config.convertConfigJson(
                CONFIG.replace("        - name: leak\n          scope: row\n          expr: \"sold * 2\"\n", ""),
                com.mercari.solution.config.Config.Format.yaml);
        final JsonObject parameters = config.getAsJsonArray("transforms").get(0).getAsJsonObject().getAsJsonObject("parameters");
        final ValidateFeatureTool tool = new ValidateFeatureTool();
        final McpSchema.CallToolResult result = tool.sync(null,
                new McpSchema.CallToolRequest("validate-feature", Map.of("parameters", parameters.toString(), "format", "text")));
        Assertions.assertFalse(result.isError(), text(result));
        Assertions.assertTrue(text(result).startsWith("feature plan "), text(result));
        Assertions.assertTrue(text(result).contains("windowShift"), text(result));
    }

    @Test
    public void testMissingArguments() {
        final McpSchema.CallToolResult result = new ValidateFeatureTool().sync(null,
                new McpSchema.CallToolRequest("validate-feature", Map.of()));
        Assertions.assertTrue(result.isError());
    }

    @Test
    public void testAgentToolReport() {
        final String report = FeatureValidator.create().validate(CONFIG, "features");
        Assertions.assertTrue(report.startsWith("ERROR"), report);
        Assertions.assertTrue(report.contains("availability.violation"), report);
        final String ok = FeatureValidator.create().validate(
                CONFIG.replace("        - name: leak\n          scope: row\n          expr: \"sold * 2\"\n", ""), null);
        Assertions.assertTrue(ok.startsWith("SUCCESS"), ok);
    }

}
