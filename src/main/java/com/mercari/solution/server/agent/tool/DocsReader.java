package com.mercari.solution.server.agent.tool;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class DocsReader {

    private static final String DOCS_ROOT_PATH = "/server/docs";
    private static final String DOCS_BASE_PATH = DOCS_ROOT_PATH + "/module";

    public enum ModuleType {
        source,
        transform,
        sink
    }

    @Tool("""
        List available module documentation.
        If type is specified, returns only modules of that type (source, transform, or sink).
        If type is not specified, returns all available module documentation across all types.
        Use this tool to discover what modules are available before reading their details.
    """)
    public String listModules(@P(name = "type", description = "Module type to filter by. If not specified, all types are listed.", required = false) ModuleType type) {
        final List<ModuleType> types;
        if (type != null) {
            types = List.of(type);
        } else {
            types = List.of(ModuleType.values());
        }

        final StringBuilder result = new StringBuilder();
        for (final ModuleType t : types) {
            final List<String> modules = listModuleFiles(t);
            if (modules.isEmpty()) {
                continue;
            }
            result.append("## ").append(t.name()).append(" modules\n");
            for (final String module : modules) {
                final String title = extractTitle(t, module);
                if (title != null) {
                    result.append("- ").append(module).append(": ").append(title).append("\n");
                } else {
                    result.append("- ").append(module).append("\n");
                }
            }
            result.append("\n");
        }

        if (result.isEmpty()) {
            return "No module documentation found.";
        }
        return result.toString();
    }

    @Tool("""
        Read the documentation for a specific module.
        Returns the full documentation including parameters, usage examples, and related information.
        Use listModules first to discover available module names if needed.
    """)
    public String getModule(
            @P(name = "type", description = "Module type: source, transform, or sink.") ModuleType type,
            @P(name = "name", description = "Module name (e.g. 'create', 'bigquery', 'beamsql', 'storage').") String name) {

        if (type == null) {
            return "Error: type is required. Specify one of: source, transform, sink.";
        }
        if (name == null || name.isBlank()) {
            return "Error: name is required. Specify the module name (e.g. 'create', 'beamsql', 'storage').";
        }

        final String resourcePath = DOCS_BASE_PATH + "/" + type.name() + "/" + name.trim().toLowerCase() + ".md";
        try (final InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if (is == null) {
                return String.format("Documentation not found for %s module '%s'. Use listModules to see available modules.", type.name(), name);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            return String.format("Failed to read documentation for %s module '%s': %s", type.name(), name, e.getMessage());
        }
    }

    @Tool("""
        Read any bundled documentation file by its path relative to the docs root.
        Module documentation may reference shared documents that are not module docs themselves,
        e.g. a link "../common/filter.md" inside "module/transform/select.md" resolves to
        "module/common/filter.md". Use this tool to follow such references.
        Shared documents include: module/common/filter.md, module/common/select.md,
        module/common/strategy.md, module/common/expression.md, module/common/schema.md,
        module/common/schema-migration.md, module/common/logging.md, module/common/template.md,
        and system.md (the config's system block reference).
    """)
    public String getDocument(
            @P(name = "path", description = "Document path relative to the docs root, e.g. 'module/common/filter.md' or 'system.md'.") String path) {

        final String normalized = normalizeDocPath(path);
        if (normalized == null) {
            return "Error: invalid document path '" + path + "'. Specify a .md path relative to the docs root, e.g. 'module/common/filter.md'.";
        }
        final String resourcePath = DOCS_ROOT_PATH + "/" + normalized;
        try (final InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if (is == null) {
                return "Document not found: '" + normalized + "'. Relative links resolve against the referencing document's directory (e.g. '../common/filter.md' in 'module/transform/select.md' is 'module/common/filter.md').";
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            return String.format("Failed to read document '%s': %s", normalized, e.getMessage());
        }
    }

    /**
     * Normalize a docs-root-relative path: resolves '.'/'..' segments and rejects
     * paths that escape the docs root or do not point at a markdown file.
     */
    static String normalizeDocPath(final String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String p = path.trim().replace('\\', '/');
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        final Deque<String> segments = new ArrayDeque<>();
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
        if (!normalized.endsWith(".md")) {
            return null;
        }
        return normalized;
    }

    public static DocsReader create() {
        return new DocsReader();
    }

    private List<String> listModuleFiles(final ModuleType type) {
        final List<String> modules = new ArrayList<>();
        final String dirPath = DOCS_BASE_PATH + "/" + type.name();
        try (final InputStream is = getClass().getResourceAsStream(dirPath);
             final BufferedReader reader = (is != null) ? new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)) : null) {
            if (reader == null) {
                return modules;
            }
            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (trimmed.endsWith(".md") && !trimmed.equals("index.md")) {
                    modules.add(trimmed.substring(0, trimmed.length() - 3));
                }
            }
        } catch (final Exception e) {
            // directory listing not available
        }
        return modules;
    }

    private String extractTitle(final ModuleType type, final String moduleName) {
        final String resourcePath = DOCS_BASE_PATH + "/" + type.name() + "/" + moduleName + ".md";
        try (final InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if (is == null) {
                return null;
            }
            final BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            boolean inFrontMatter = false;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().equals("---")) {
                    if (inFrontMatter) {
                        break;
                    }
                    inFrontMatter = true;
                    continue;
                }
                if (inFrontMatter && line.trim().startsWith("title:")) {
                    return line.trim().substring("title:".length()).trim();
                }
            }
        } catch (final Exception e) {
            // ignore
        }
        return null;
    }

}
