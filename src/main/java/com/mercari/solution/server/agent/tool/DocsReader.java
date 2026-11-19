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
        available before reading their details with readDocs.
    """)
    public String listModules(@P(name = "type", description = "Module type to filter by. If not specified, all types are listed.", required = false) ModuleType type) {
        return McpToolBridge.call("list-modules", McpToolBridge.args("type", type == null ? null : type.name()));
    }

    @Tool(name = "readDocs", value = """
        Read bundled documentation: a module's reference (parameters, usage examples) by `module`
        ('{type}/{name}', type = source | transform | sink | action, e.g. 'transform/feature'), or any
        document by `path` relative to the docs root. Module documentation may reference shared documents,
        e.g. a link "../common/filter.md" inside "module/transform/select.md" resolves to
        "module/common/filter.md"; read those by path. Shared documents include: module/common/{filter,
        select,strategy,expression,schema,schema-migration,logging,template,union,bigtable}.md,
        module/failure/pubsub.md, README.md (the config file structure reference), system.md (the
        config's system block) and options/README.md (pipeline options, with per-runner pages
        options/dataflow.md, options/direct.md, options/prism.md, options/flink.md, options/spark.md,
        options/portable.md, options/gcp.md, options/aws.md, options/beamsql.md). Use listModules first to discover module names.
        A large document (over 30,000 characters) is not returned whole: the answer is its introduction and its
        outline (every section with its id and size). Read what you need with `section` (one id, or several
        separated by commas; a section comes with its subsections) instead of asking for the whole text;
        `outline: true` returns the outline of any document, `full: true` the whole text whatever its size.
    """)
    public String readDocs(
            @P(name = "module", description = "Module id '{type}/{name}', e.g. 'source/bigquery' (alternative to path).", required = false) String module,
            @P(name = "path", description = "Document path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md'.", required = false) String path,
            @P(name = "section", description = "Section id(s) from the document's outline, comma separated for several. Returns those sections only.", required = false) String section,
            @P(name = "outline", description = "true: return the outline (sections with id and size) instead of the text.", required = false) Boolean outline,
            @P(name = "full", description = "true: return the whole text even when the document is large.", required = false) Boolean full) {
        if ((module == null || module.isBlank()) && (path == null || path.isBlank())) {
            return "ERROR: pass module ('{type}/{name}', e.g. 'transform/feature') or path (e.g. 'module/common/filter.md').";
        }
        return McpToolBridge.call("read-docs", McpToolBridge.args("module", module, "path", path, "section", section, "outline", outline, "full", full));
    }

    public static DocsReader create() {
        return new DocsReader();
    }

}
