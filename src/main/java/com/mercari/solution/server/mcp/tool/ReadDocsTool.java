package com.mercari.solution.server.mcp.tool;

import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.McpSchema;
import jakarta.servlet.ServletContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Tool.Module(
    name="read-docs",
    title="Read Pipeline Documentation",
    description= """
        Read bundled documentation: a module's reference by 'module' ('{type}/{name}', e.g. 'source/bigquery',
        'transform/feature' - parameters, examples; use 'list-modules' to discover names), or any document by
        'path' relative to the docs root.
        A large document (over 30,000 characters) is not returned whole: the answer is its introduction and
        its outline - every section with its id and size. Read what you need with 'section' (one id, or
        several separated by commas; a section comes with its subsections), or pass 'full: true' for the
        whole text. 'outline: true' returns the outline of any document, small ones included. A section that
        is itself over the limit answers the same way: its own text and the outline of its subsections.
        Module documentation may reference shared documents,
        e.g. a link "../common/filter.md" inside "module/transform/select.md" resolves to
        "module/common/filter.md". Use this tool to follow such references.
        Available documents include: README.md (the config file structure reference),
        system.md (the config's system block reference), options/README.md (pipeline options,
        with per-runner pages options/dataflow.md, options/direct.md, options/prism.md,
        options/flink.md, options/spark.md, options/portable.md, options/gcp.md, options/aws.md,
        options/beamsql.md), module/README.md (module list), shared parameter docs
        module/common/{filter,select,strategy,expression,schema,schema-migration,logging,
        template,union,bigtable}.md, and module/failure/pubsub.md (dead-letter sink).
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "module": {
              "type": "string",
              "description": "Module id in '{type}/{name}' format (type: source | transform | sink | action), e.g. 'source/bigquery'. Alternative to 'path'."
            },
            "path": {
              "type": "string",
              "description": "Document path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md'."
            },
            "section": {
              "type": "string",
              "description": "Section id(s) from the document's outline, comma separated for several (e.g. 'sources-contract' or 'sources-contract,availability-check'). The heading text is accepted too. Returns those sections only, subsections included."
            },
            "outline": {
              "type": "boolean",
              "description": "true: return the document's outline (sections with id and size) instead of its text. With 'section': the outline of that section."
            },
            "full": {
              "type": "boolean",
              "description": "true: return the whole text even when it is over the size limit (default false: a large document or section answers with its own text and an outline)."
            }
          },
          "required": []
        }
        """,
    outputSchema = """
        """
)
public class ReadDocsTool implements Tool {

    // Same docs tree as the agent's DocsReader (src/main/resources/server/docs)
    private static final String DOCS_ROOT_PATH = "/server/docs/";
    private static final java.util.Set<String> MODULE_TYPES = java.util.Set.of("source", "transform", "sink", "action");

    /**
     * Normalize a docs-root-relative path: resolves '.'/'..' segments and rejects paths that escape the
     * docs root or do not point at a markdown file.
     */
    public static String normalizeDocPath(final String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String p = path.trim().replace('\\', '/');
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        final java.util.Deque<String> segments = new java.util.ArrayDeque<>();
        for (final String segment : p.split("/")) {
            switch (segment) {
                case "", "." -> { }
                case ".." -> {
                    if (segments.isEmpty()) {
                        return null;
                    }
                    segments.removeLast();
                }
                default -> segments.addLast(segment);
            }
        }
        if (segments.isEmpty()) {
            return null;
        }
        final String normalized = String.join("/", segments);
        return normalized.endsWith(".md") ? normalized : null;
    }

    @Override
    public void init(ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpRequestContext ctx,
            final McpSchema.CallToolRequest request) {

        final Object moduleObj = request.arguments().get("module");
        final Object pathObj = request.arguments().get("path");
        final String normalized;
        if(moduleObj != null && !moduleObj.toString().isBlank()) {
            // module id '{type}/{name}' → module/{type}/{name}.md (the former describe-module tool)
            final String id = moduleObj.toString().trim();
            final String[] parts = id.split("/");
            if(parts.length != 2 || !MODULE_TYPES.contains(parts[0]) || parts[1].isBlank()) {
                return Tool.result("Invalid module id: '" + id + "'. Specify '{type}/{name}' where type is one of source, transform, sink, action (e.g. 'source/bigquery', 'action/bigquery'); use tool 'list-modules' to discover names.", true);
            }
            normalized = "module/" + parts[0] + "/" + parts[1].toLowerCase() + ".md";
        } else {
            normalized = normalizeDocPath(pathObj == null ? null : pathObj.toString());
        }
        if(normalized == null) {
            return Tool.result("read-docs mcp tool requires 'module' ('{type}/{name}', e.g. 'source/bigquery') or 'path' (a .md path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md')", true);
        }

        final String resourcePath = DOCS_ROOT_PATH + normalized;
        try (final InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if(is == null) {
                final String message = moduleObj != null && !moduleObj.toString().isBlank()
                        ? "Not found module: " + moduleObj.toString().trim() + ". Use tool 'list-modules' to see available modules."
                        : "Document not found: '" + normalized + "'. Relative links resolve against the referencing document's directory (e.g. '../common/filter.md' in 'module/transform/select.md' is 'module/common/filter.md').";
                return Tool.result(message, true);
            }
            final String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return read(normalized, content, text(request, "section"), flag(request, "outline"), flag(request, "full"));
        } catch (Exception e) {
            return Tool.result("Failed to read document: '" + normalized + "', cause: " + e.getMessage(), true);
        }
    }

    /** Above this many characters a document (or a section) answers with its own text and an outline, unless {@code full}. */
    public static final int OUTLINE_THRESHOLD = 30_000;

    private static final String HOW_TO_READ = "Read a section with section: \"<id>\" (several: comma separated; a section comes with its subsections), or pass full: true for the whole text.";

    private static String text(final McpSchema.CallToolRequest request, final String name) {
        final Object value = request.arguments().get(name);
        return value == null || value.toString().isBlank() ? null : value.toString().trim();
    }

    private static boolean flag(final McpSchema.CallToolRequest request, final String name) {
        final Object value = request.arguments().get(name);
        return value != null && Boolean.parseBoolean(value.toString().trim());
    }

    /**
     * What a read answers: the whole text while it is small (or {@code full}); otherwise the reader's way in -
     * the introduction and the outline - and from there the sections it names. The same rule holds one level
     * down: a named section over the limit answers with its own text and the outline of its subsections.
     */
    static McpSchema.CallToolResult read(final String path, final String content, final String section,
                                         final boolean outline, final boolean full) {
        if (section == null && !outline && (full || content.length() <= OUTLINE_THRESHOLD)) {
            return Tool.result(content, false);
        }
        final DocSections doc = DocSections.parse(content);
        if (section == null) {
            if (doc.sections().isEmpty()) {
                // nothing to cut it by: the whole text is the only answer
                return Tool.result(content, false);
            }
            final StringBuilder sb = new StringBuilder();
            if (outline) {
                sb.append("# ").append(doc.title() == null ? path : doc.title()).append("\n");
                if (doc.description() != null) sb.append("\n").append(doc.description()).append("\n");
            } else {
                sb.append(doc.body(doc.root()).stripTrailing()).append("\n");
            }
            sb.append("\n---\n").append(path).append(" is ").append(DocSections.formatSize(doc.size()));
            sb.append(outline ? ". " : ": too large to return whole, so this is its introduction and its outline. ");
            sb.append(HOW_TO_READ).append("\n\n## Outline\n").append(doc.outline(doc.root()));
            return Tool.result(sb.toString(), false);
        }

        final java.util.List<DocSections.Section> selected = new java.util.ArrayList<>();
        for (final String reference : section.split(",")) {
            if (reference.isBlank()) continue;
            final java.util.List<DocSections.Section> found = doc.find(reference);
            if (found.size() != 1) {
                final StringBuilder sb = new StringBuilder();
                if (found.isEmpty()) {
                    sb.append("Section not found: '").append(reference.trim()).append("' in ").append(path)
                            .append(". The sections of the document:\n").append(doc.outline(doc.root()));
                } else {
                    sb.append("Section '").append(reference.trim()).append("' is ambiguous in ").append(path).append(". Use one of the ids:\n");
                    for (final DocSections.Section s : found) {
                        sb.append("- ").append(s.title()).append(" - id: `").append(s.id()).append("`\n");
                    }
                }
                return Tool.result(sb.toString(), true);
            }
            if (!selected.contains(found.getFirst())) selected.add(found.getFirst());
        }
        if (selected.isEmpty()) {
            return Tool.result("'section' names no section. The sections of " + path + ":\n" + doc.outline(doc.root()), true);
        }
        final StringBuilder sb = new StringBuilder();
        for (final DocSections.Section s : selected) {
            if (!sb.isEmpty()) sb.append("\n");
            final String heading = doc.body(s).lines().findFirst().orElse("");
            if (s.children().isEmpty() || !outline && (full || s.size() <= OUTLINE_THRESHOLD)) {
                // the section itself; asked for an outline it has none to give
                sb.append(outline ? heading + "\n\n(section `" + s.id() + "` has no subsection: " + DocSections.formatSize(s.size()) + ")\n"
                        : doc.content(s).stripTrailing() + "\n");
                continue;
            }
            sb.append(outline ? heading : doc.body(s).stripTrailing()).append("\n");
            sb.append("\n---\nsection `").append(s.id()).append("` is ").append(DocSections.formatSize(s.size()));
            sb.append(outline ? ". " : ": too large to return whole, so this is its own text and the outline of its subsections. ");
            sb.append(HOW_TO_READ).append("\n\n").append(doc.outline(s));
        }
        return Tool.result(sb.toString(), false);
    }

}
