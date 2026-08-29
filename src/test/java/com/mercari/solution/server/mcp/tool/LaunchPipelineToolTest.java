package com.mercari.solution.server.mcp.tool;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** The launch itself needs cloud credentials; these cover argument handling and the user-error path. */
public class LaunchPipelineToolTest {

    private static final String CONFIG = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  schema:
                    fields:
                      - {name: id, type: string}
                  elements:
                    - {id: a}
            sinks:
              - name: out
                module: debug
                inputs: [input]
            """;

    private static String text(final McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().getFirst()).text();
    }

    @Test
    public void testUnknownTargetIsAUserError() {
        final McpSchema.CallToolResult result = new LaunchPipelineTool().sync(null,
                new McpSchema.CallToolRequest("launch-pipeline", Map.of("config", CONFIG, "runner", "dataflow", "environment", "nowhere")));
        Assertions.assertTrue(result.isError(), text(result));
        final JsonObject json = JsonParser.parseString(text(result)).getAsJsonObject();
        Assertions.assertEquals("error", json.get("status").getAsString());
        Assertions.assertTrue(json.get("error").getAsString().contains("Not supported launch target: dataflow/nowhere"), json::toString);
        Assertions.assertTrue(json.get("error").getAsString().contains("dataflow/flexTemplate"), json::toString);
    }

    @Test
    public void testInvalidConfigIsReportedBeforeLaunching() {
        final McpSchema.CallToolResult result = new LaunchPipelineTool().sync(null,
                new McpSchema.CallToolRequest("launch-pipeline", Map.of("config", "sources: [{name: x, module: nosuchmodule}]", "runner", "dataflow",
                        "parameters", Map.of("project", "p", "region", "r"))));
        Assertions.assertTrue(result.isError(), text(result));
        final JsonObject json = JsonParser.parseString(text(result)).getAsJsonObject();
        Assertions.assertEquals("error", json.get("status").getAsString());
    }

    @Test
    public void testMissingArguments() {
        final McpSchema.CallToolResult result = new LaunchPipelineTool().sync(null,
                new McpSchema.CallToolRequest("launch-pipeline", Map.of("config", CONFIG)));
        Assertions.assertTrue(result.isError());
        Assertions.assertTrue(text(result).contains("requires config and runner"));
    }

}
