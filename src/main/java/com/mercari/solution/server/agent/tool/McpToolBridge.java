package com.mercari.solution.server.agent.tool;

import com.mercari.solution.server.mcp.tool.Tool;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The Pipeline Builder agent's tools are thin wrappers over the MCP tools: one implementation per capability,
 * published twice (MCP for external clients, langchain4j {@code @Tool} for the in-server agent). This bridge
 * invokes an MCP tool by name and renders its text result for the agent ({@code ERROR: ...} on failure).
 */
public final class McpToolBridge {

    private McpToolBridge() {}

    public static String call(final String tool, final Map<String, Object> arguments) {
        return call(tool, arguments, null);
    }

    /** @param successPrefix prepended to a successful result (e.g. {@code "SUCCESS: "}), null for none */
    public static String call(final String tool, final Map<String, Object> arguments, final String successPrefix) {
        final Map<String, Object> args = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> e : arguments.entrySet()) {
            if (e.getValue() != null) args.put(e.getKey(), e.getValue());
        }
        try {
            final McpSchema.CallToolResult result = Tool.find(tool).sync(null, new McpSchema.CallToolRequest(tool, args));
            final String text = text(result);
            if (Boolean.TRUE.equals(result.isError())) {
                return text.startsWith("ERROR") ? text : "ERROR: " + text;
            }
            return successPrefix == null ? text : successPrefix + text;
        } catch (final Throwable e) {
            return "ERROR: " + (e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }

    /** Arguments builder that skips nulls (optional langchain4j parameters arrive as null). */
    public static Map<String, Object> args(final Object... keyValues) {
        final Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            if (keyValues[i + 1] != null) map.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return map;
    }

    static String text(final McpSchema.CallToolResult result) {
        final StringBuilder sb = new StringBuilder();
        if (result.content() != null) {
            for (final McpSchema.Content c : result.content()) {
                if (c instanceof McpSchema.TextContent t) sb.append(t.text());
            }
        }
        return sb.toString();
    }

}
