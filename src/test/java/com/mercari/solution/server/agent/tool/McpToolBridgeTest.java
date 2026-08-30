package com.mercari.solution.server.agent.tool;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** The agent tools are wrappers over the MCP tools: every wrapped name must exist and errors must be prefixed. */
public class McpToolBridgeTest {

    @Test
    public void testWrappedToolsExist() {
        for (final String name : new String[]{"list-modules", "read-docs", "search-code", "read-source", "resolve-stack-trace",
                "find-module-source", "get-job", "get-job-logs", "get-job-progress", "list-job-errors", "list-failed-jobs", "run-pipeline",
                "validate-feature", "launch-pipeline"}) {
            Assertions.assertDoesNotThrow(() -> com.mercari.solution.server.mcp.tool.Tool.find(name), name);
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> com.mercari.solution.server.mcp.tool.Tool.find("no-such-tool"));
    }

    @Test
    public void testDocsWrappers() {
        final DocsReader docs = DocsReader.create();
        Assertions.assertTrue(docs.readDocs("transform/feature", null).contains("# Feature Transform Module"));
        Assertions.assertTrue(docs.readDocs("source/nosuchmodule", null).startsWith("ERROR"));
        Assertions.assertTrue(docs.readDocs(null, "module/common/filter.md").length() > 100);
        Assertions.assertTrue(docs.readDocs(null, "../outside.md").startsWith("ERROR"));
        final String listed = docs.listModules(DocsReader.ModuleType.transform);
        Assertions.assertTrue(listed.contains("## transform modules") && listed.contains("feature"), listed);
        Assertions.assertTrue(docs.listModules(null).contains("## source modules"));
    }

    @Test
    public void testRunAndValidateWrappers() {
        final String config = """
                sources:
                  - name: input
                    module: create
                    parameters:
                      type: element
                      schema: {fields: [{name: id, type: string}]}
                      elements: [{id: a}]
                sinks:
                  - name: out
                    module: debug
                    inputs: [input]
                """;
        final String ok = PipelineExecutor.create().execute(config, true, null);
        Assertions.assertTrue(ok.startsWith("SUCCESS: "), ok);
        final String bad = PipelineExecutor.create().execute(config.replace("module: debug", "module: nosuchsink"), true, null);
        Assertions.assertTrue(bad.startsWith("ERROR"), bad);
        Assertions.assertTrue(McpToolBridge.call("unknown-tool", Map.of()).startsWith("ERROR: unknown mcp tool"));
    }

}
