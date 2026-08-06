package com.mercari.solution.server.mcp.tool;

import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class McpDocsToolsTest {

    private final DescribeModuleTool describeModuleTool = new DescribeModuleTool();
    private final ReadDocsTool readDocsTool = new ReadDocsTool();

    private static String text(final McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().getFirst()).text();
    }

    @Test
    public void testDescribeModule() {
        for (final String id : new String[]{"source/bigquery", "transform/select", "sink/spanner", "sink/localH2"}) {
            final McpSchema.CallToolResult result = describeModuleTool.sync(
                    null, new McpSchema.CallToolRequest("describe-module", Map.of("id", id)));
            Assertions.assertFalse(result.isError(), id + " -> " + text(result));
            Assertions.assertFalse(text(result).isBlank(), id);
        }
    }

    @Test
    public void testDescribeModuleInvalidId() {
        for (final String id : new String[]{"bigquery", "failure/pubsub", "source/../../secret", ""}) {
            final McpSchema.CallToolResult result = describeModuleTool.sync(
                    null, new McpSchema.CallToolRequest("describe-module", Map.of("id", id)));
            Assertions.assertTrue(result.isError(), id + " -> " + text(result));
        }
    }

    @Test
    public void testDescribeModuleNotFound() {
        final McpSchema.CallToolResult result = describeModuleTool.sync(
                null, new McpSchema.CallToolRequest("describe-module", Map.of("id", "source/nosuchmodule")));
        Assertions.assertTrue(result.isError());
        Assertions.assertTrue(text(result).contains("Not found module"), text(result));
    }

    @Test
    public void testReadDocs() {
        for (final String path : new String[]{
                "README.md",
                "system.md",
                "options/README.md",
                "module/README.md",
                "module/common/filter.md",
                "module/failure/pubsub.md"}) {
            final McpSchema.CallToolResult result = readDocsTool.sync(
                    null, new McpSchema.CallToolRequest("read-docs", Map.of("path", path)));
            Assertions.assertFalse(result.isError(), path + " -> " + text(result));
            Assertions.assertFalse(text(result).isBlank(), path);
        }
    }

    @Test
    public void testReadDocsRejectsInvalidPath() {
        for (final String path : new String[]{"../secrets.md", "module/index.yaml", ""}) {
            final McpSchema.CallToolResult result = readDocsTool.sync(
                    null, new McpSchema.CallToolRequest("read-docs", Map.of("path", path)));
            Assertions.assertTrue(result.isError(), path + " -> " + text(result));
        }
    }

    @Test
    public void testReadDocsNotFound() {
        final McpSchema.CallToolResult result = readDocsTool.sync(
                null, new McpSchema.CallToolRequest("read-docs", Map.of("path", "no-such-doc.md")));
        Assertions.assertTrue(result.isError());
        Assertions.assertTrue(text(result).contains("Document not found"), text(result));
    }

}
