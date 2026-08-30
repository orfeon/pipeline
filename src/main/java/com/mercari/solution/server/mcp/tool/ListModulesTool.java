package com.mercari.solution.server.mcp.tool;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.server.api.SpecService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.util.List;
import java.util.Map;

/**
 * The module catalog ({@code server/docs/module/index.yaml}: title / description / tags per module), as a
 * compact list per type. The full JSON schemas are served by {@code /api/spec}; the per-module reference
 * by {@code read-docs}.
 */
@Tool.Module(
    name="list-modules",
    title="List Pipeline Modules",
    description= """
        List the pipeline modules available in mercari/pipeline, per type (source, transform, sink,
        action), each with a one-line description and tags. Pass 'type' to list one type only.
        Read a module's full reference (parameters, examples) with tool 'read-docs' using
        module '{type}/{name}' (e.g. 'source/bigquery').
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "title": "Module Type",
              "enum": ["source", "transform", "sink", "action"],
              "description": "Module type to list. If not specified, all types are listed."
            }
          },
          "required": []
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class ListModulesTool implements Tool {

    /** index.yaml section per module type. */
    private static final Map<String, String> SECTIONS = new java.util.LinkedHashMap<>(Map.of());
    static {
        SECTIONS.put("source", "sources");
        SECTIONS.put("transform", "transforms");
        SECTIONS.put("sink", "sinks");
        SECTIONS.put("action", "actions");
    }

    @Override
    public void init(ServletContext servletContext) {
        SpecService.init();
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final Object typeValue = request.arguments().get("type");
        final String type = typeValue == null || typeValue.toString().isBlank() ? null : typeValue.toString().trim();
        if (type != null && !SECTIONS.containsKey(type)) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("Unknown module type: '" + type + "'. Specify one of " + SECTIONS.keySet() + ", or omit it to list every type.")
                    .isError(true)
                    .build();
        }
        final JsonObject modules = SpecService.ModuleIndex.getModules();
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, String> section : SECTIONS.entrySet()) {
            if (type != null && !type.equals(section.getKey())) continue;
            final JsonElement list = modules == null ? null : modules.get(section.getValue());
            if (list == null || !list.isJsonArray() || list.getAsJsonArray().isEmpty()) continue;
            sb.append("## ").append(section.getKey()).append(" modules\n");
            for (final JsonElement e : list.getAsJsonArray()) {
                if (!e.isJsonObject()) continue;
                final JsonObject m = e.getAsJsonObject();
                final String name = m.has("name") && !m.get("name").getAsString().isBlank() ? m.get("name").getAsString() : null;
                if (name == null) continue;
                sb.append("- ").append(section.getKey()).append('/').append(name);
                if (m.has("description")) sb.append(": ").append(m.get("description").getAsString());
                if (m.has("tags") && m.get("tags").isJsonArray()) {
                    final List<String> tags = new java.util.ArrayList<>();
                    for (final JsonElement t : m.getAsJsonArray("tags")) tags.add(t.getAsString());
                    sb.append(" [").append(String.join(", ", tags)).append(']');
                }
                sb.append('\n');
            }
            sb.append('\n');
        }
        if (sb.isEmpty()) {
            return McpSchema.CallToolResult.builder().addTextContent("No modules found" + (type == null ? "." : " for type '" + type + "'.")).isError(true).build();
        }
        return McpSchema.CallToolResult.builder().addTextContent(sb.toString()).isError(false).build();
    }

    /** Kept for callers of the previous JSON-schema list ({@code /api/spec} serves the same data). */
    public static JsonArray schemaAbstracts(final String type) {
        return SpecService.getModuleAbstracts(type);
    }

}
