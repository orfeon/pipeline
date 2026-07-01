package com.mercari.solution.server.agent.tool;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DocsReader {

    private static final String DOCS_BASE_PATH = "/server/docs/module";

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
