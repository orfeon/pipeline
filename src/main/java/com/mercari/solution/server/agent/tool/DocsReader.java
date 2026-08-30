package com.mercari.solution.server.agent.tool;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tools over the bundled documentation: wrappers of the MCP tools {@code list-modules} / {@code read-docs}. */
public class DocsReader {

    public enum ModuleType {
        source,
        transform,
        sink,
        // action modules (config section `actions`); their docs live under module/action/<service>.md
        action
    }

    @Tool("""
        List available module documentation.
        If type is specified, returns only modules of that type (source, transform, sink, or action).
        If type is not specified, returns all available module documentation across all types.
        Returns, per module, its title / description / tags. Use this tool to discover what modules are
        available before reading their details with getModule.
    """)
    public String listModules(@P(name = "type", description = "Module type to filter by. If not specified, all types are listed.", required = false) ModuleType type) {
        return McpToolBridge.call("list-modules", McpToolBridge.args("type", type == null ? null : type.name()));
    }

    @Tool("""
        Read the documentation for a specific module.
        Returns the full documentation including parameters, usage examples, and related information.
        Use listModules first to discover available module names if needed.
    """)
    public String getModule(
            @P(name = "type", description = "Module type: source, transform, sink, or action (action = the actions config section).") ModuleType type,
            @P(name = "name", description = "Module name (e.g. 'create', 'bigquery', 'beamsql', 'storage').") String name) {
        if (type == null) {
            return "ERROR: type is required. Specify one of: source, transform, sink, action.";
        }
        if (name == null || name.isBlank()) {
            return "ERROR: name is required. Specify the module name (e.g. 'create', 'beamsql', 'storage').";
        }
        return McpToolBridge.call("read-docs", McpToolBridge.args("module", type.name() + "/" + name.trim()));
    }

    @Tool("""
        Read any bundled documentation file by its path relative to the docs root.
        Module documentation may reference shared documents that are not module docs themselves,
        e.g. a link "../common/filter.md" inside "module/transform/select.md" resolves to
        "module/common/filter.md". Use this tool to follow such references.
        Shared documents include: module/common/filter.md, module/common/select.md,
        module/common/strategy.md, module/common/expression.md, module/common/schema.md,
        module/common/schema-migration.md, module/common/logging.md, module/common/template.md,
        module/common/union.md, module/common/bigtable.md, module/failure/pubsub.md,
        README.md (the config file structure reference), system.md (the config's system
        block reference), and options/README.md (pipeline options, with per-runner pages
        options/dataflow.md, options/direct.md, options/flink.md, options/spark.md,
        options/portable.md, options/gcp.md, options/aws.md, options/beamsql.md).
    """)
    public String getDocument(
            @P(name = "path", description = "Document path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md'.") String path) {
        return McpToolBridge.call("read-docs", McpToolBridge.args("path", path));
    }

    public static DocsReader create() {
        return new DocsReader();
    }

}
