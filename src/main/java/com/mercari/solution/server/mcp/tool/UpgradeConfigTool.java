package com.mercari.solution.server.mcp.tool;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.SchemaConfigUpgrader;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.McpSchema;
import jakarta.servlet.ServletContext;


@Tool.Module(
    name="upgrade-config",
    title="Upgrade Pipeline Config",
    description= """
        Rewrite deprecated schema declarations in a pipeline config into the current form:
        a module top-level 'schema' moves into 'parameters.schema', and old schema keys
        (avro / protobuf / useDestinationSchema and deprecated aliases) become the
        'encoding' / 'reference' declarations. See docs://module/common/schema.md.
        Returns JSON with the upgraded 'config' and the list of 'changes' applied.
        Declarations that cannot be rewritten safely are left unchanged with a note in 'changes'.
        The result config is always JSON, regardless of the input format.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "config": {
              "type": "string",
              "description": "Definition of pipeline. YAML or JSON format."
            }
          },
          "required": ["config"]
        }
        """,
    outputSchema = """
        """
)
public class UpgradeConfigTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {

    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpRequestContext ctx,
            final McpSchema.CallToolRequest request) {

        if(!request.arguments().containsKey("config")) {
            return Tool.result("upgrade-config mcp tool requires config parameter", true);
        }

        try {
            final String configText = request.arguments().get("config").toString();
            final JsonObject configJson = Config.convertConfigJson(configText, Config.Format.unknown);
            final SchemaConfigUpgrader.Result result = SchemaConfigUpgrader.upgrade(configJson);

            final JsonObject responseJson = new JsonObject();
            responseJson.add("config", result.config());
            final JsonArray changes = new JsonArray();
            for(final String change : result.changes()) {
                changes.add(change);
            }
            responseJson.add("changes", changes);

            return Tool.result(new GsonBuilder().setPrettyPrinting().create().toJson(responseJson), false);
        } catch (final Throwable e) {
            return Tool.result("upgrade-config failed: " + e.getMessage(), true);
        }
    }

}
