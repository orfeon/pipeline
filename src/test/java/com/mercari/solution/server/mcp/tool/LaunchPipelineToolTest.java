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
    public void testTemplateArgsAreSubstitutedBeforeLaunch() throws Exception {
        // ${args.<name>} is substituted from the config's args / the launch args; a bare ${name} is not the DSL
        final String config = """
                args:
                  dateFrom: "2026-01-01"
                sources:
                  - name: input
                    module: create
                    parameters:
                      type: element
                      schema:
                        fields:
                          - {name: id, type: string}
                          - {name: note, type: string}
                      elements:
                        - {id: "${args.dateFrom}", note: "${dateFrom}"}
                sinks:
                  - name: out
                    module: debug
                    inputs: [input]
                """;
        final com.mercari.solution.config.Config loaded = com.mercari.solution.config.Config.load(config, null,
                com.mercari.solution.config.Config.Format.unknown, "{\"dateTo\": \"2026-02-01\"}");
        Assertions.assertTrue(loaded.getContent().contains("\"id\":\"2026-01-01\""), loaded.getContent());
        Assertions.assertTrue(loaded.getContent().contains("${dateFrom}"), loaded.getContent()); // untouched, by design
        Assertions.assertTrue(com.mercari.solution.config.Config.unresolvedArgs(loaded.getContent()).isEmpty());

        // an ${args.x} without a value anywhere is refused at launch instead of reaching the job as literal text
        final String missing = config.replace("args:\n  dateFrom: \"2026-01-01\"\n", "");
        final McpSchema.CallToolResult result = new LaunchPipelineTool().sync(null,
                new McpSchema.CallToolRequest("launch-pipeline", Map.of("config", missing, "runner", "dataflow", "parameters", Map.of("project", "p", "region", "r"))));
        Assertions.assertTrue(result.isError(), text(result));
        Assertions.assertTrue(text(result).contains("unresolved template arguments [dateFrom]"), text(result));
        // ... and passes once the launch args supply it (the launch then fails later, on the missing template)
        final McpSchema.CallToolResult supplied = new LaunchPipelineTool().sync(null,
                new McpSchema.CallToolRequest("launch-pipeline", Map.of("config", missing, "runner", "dataflow",
                        "parameters", Map.of("project", "p", "region", "r"), "args", Map.of("dateFrom", "2026-01-01"))));
        Assertions.assertFalse(text(supplied).contains("unresolved template arguments"), text(supplied));
    }

    @Test
    public void testMissingArguments() {
        final McpSchema.CallToolResult result = new LaunchPipelineTool().sync(null,
                new McpSchema.CallToolRequest("launch-pipeline", Map.of("config", CONFIG)));
        Assertions.assertTrue(result.isError());
        Assertions.assertTrue(text(result).contains("requires config and runner"));
    }

}
