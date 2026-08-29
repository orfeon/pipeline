package com.mercari.solution.server.mcp.tool;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.config.Config;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.pipeline.feature.FeaturePlanService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.util.Map;

@Tool.Module(
    name="validate-feature",
    title="Validate Feature Spec",
    description= """
        Compile a `feature` transform specification without running a pipeline (validate --expand).
        Returns the expanded output columns with their derived availability time, leak-check status
        (staticSafe / windowShift / runtimeFilter / violation), lineage (derivedFrom, evidence), the evaluation
        stages (key changes = shuffles) and structured diagnostics (errors, warnings, hints).
        Pass either the feature step's `parameters` block (with inline or URI `sources`) or a whole pipeline
        config containing a `module: feature` transform. Optional `inputSchema` ({fields: [...]}) enables the
        lineage ↔ schema cross-check, `args` supplies template args for referenced files, `format` = "text"
        returns the human-readable report instead of JSON.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "parameters": {"type": ["object", "string"], "description": "feature transform parameters (object, or YAML/JSON text)"},
            "config": {"type": "string", "description": "whole pipeline config (YAML or JSON) containing a feature transform"},
            "name": {"type": "string", "description": "feature transform step name when config has several"},
            "inputSchema": {"type": "object", "description": "input relation schema: {fields: [{name, type}]}"},
            "args": {"type": "object", "description": "template args for referenced sources/features files"},
            "streaming": {"type": "boolean", "description": "check engine constraints for streaming execution"},
            "format": {"type": "string", "enum": ["json", "text"], "description": "response format (default json)"}
          }
        }
        """,
    outputSchema = """
        """
)
public class ValidateFeatureTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {

    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Map<String, Object> arguments = request.arguments();
        try {
            final JsonObject body = toRequest(arguments);
            final JsonObject result = FeaturePlanService.validate(body);
            final boolean ok = result.has("ok") && result.get("ok").getAsBoolean();
            final String text;
            if ("text".equals(String.valueOf(arguments.get("format"))) && result.has("describe")) {
                text = result.get("describe").getAsString();
            } else {
                text = new GsonBuilder().setPrettyPrinting().create().toJson(result);
            }
            return McpSchema.CallToolResult.builder()
                    .addTextContent(text)
                    .isError(!ok)
                    .build();
        } catch (final Throwable e) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("validate-feature failed: " + FailureUtil.convertThrowableMessage(e))
                    .isError(true)
                    .build();
        }
    }

    /** Builds the {@link FeaturePlanService#validate} request from the tool arguments. */
    static JsonObject toRequest(final Map<String, Object> arguments) {
        final JsonObject body = new JsonObject();
        if (arguments.containsKey("config")) {
            final JsonObject config = Config.convertConfigJson(String.valueOf(arguments.get("config")), Config.Format.unknown);
            for (final Map.Entry<String, JsonElement> e : config.entrySet()) body.add(e.getKey(), e.getValue());
        }
        if (arguments.containsKey("parameters")) {
            final Object parameters = arguments.get("parameters");
            if (parameters instanceof String text) {
                body.add("parameters", Config.convertConfigJson(text, Config.Format.unknown));
            } else {
                body.add("parameters", toJson(parameters));
            }
        }
        for (final String key : new String[]{"name", "inputSchema", "args", "streaming"}) {
            if (arguments.containsKey(key) && arguments.get(key) != null) body.add(key, toJson(arguments.get(key)));
        }
        if (!body.has("parameters") && !body.has("transforms")) {
            throw new IllegalArgumentException("validate-feature requires 'parameters' or 'config'");
        }
        return body;
    }

    private static JsonElement toJson(final Object value) {
        if (value instanceof JsonElement e) return e;
        return JsonParser.parseString(new GsonBuilder().create().toJson(value));
    }

}
