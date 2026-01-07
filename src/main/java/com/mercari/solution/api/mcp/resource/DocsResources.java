package com.mercari.solution.api.mcp.resource;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DocsResources implements Resources {

    private static final Logger LOG = LoggerFactory.getLogger(DocsResources.class);

    public List<McpServerFeatures.SyncResourceSpecification> sync(ServletContext servletContext) {
        final List<McpServerFeatures.SyncResourceSpecification> resources = new ArrayList<>();
        try {
            final URL docsUrl = servletContext.getResource("/docs");
            if(docsUrl == null || !docsUrl.getProtocol().equals("file")) {
                return resources;
            }
            final File docsDir = new File(docsUrl.toURI());
            final String basePath = docsDir.getPath();
            readDoc(resources, docsDir, basePath + "/");
        } catch (Throwable e){
            LOG.error(e.getMessage());
        }

        return resources;
    }

    private static void readDoc(
            final List<McpServerFeatures.SyncResourceSpecification> resources,
            final File file,
            final String basePath) {

        if(file == null) {
            return;
        }
        if(file.isDirectory()) {
            for(final File child : Objects.requireNonNull(file.listFiles())) {
                readDoc(resources, child, basePath);
            }
        } else if(file.isFile()) {
            if(!file.getName().endsWith(".md")) {
                return;
            }

            final String name = file.getPath().replaceFirst(basePath, "");
            try {
                final List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
                final String content = getContent(lines);

                McpSchema.Resource.builder().build();

                final McpServerFeatures.SyncResourceSpecification resource = new McpServerFeatures.SyncResourceSpecification(
                        McpSchema.Resource.builder()
                                .uri("docs://" + name)
                                .title(getTitle(lines))
                                .description(getDescription(lines))
                                .mimeType("text/plain")
                                .annotations(new McpSchema.Annotations(List.of(McpSchema.Role.ASSISTANT), 0.5D))
                                .build(),
                        (McpSyncServerExchange exchange, McpSchema.ReadResourceRequest request) -> {
                            final McpSchema.ResourceContents resourceContents = new McpSchema.TextResourceContents(
                                    request.uri(),
                                    "text/plain",
                                    content);
                            return new McpSchema.ReadResourceResult(List.of(resourceContents));
                        });

                resources.add(resource);
            } catch (IOException e) {
                LOG.error("e: " + e.getMessage());
            }
        }
    }

    private static String getTitle(final List<String> lines) {
        if(lines.isEmpty()) {
            return "";
        }
        return lines
                .getFirst()
                .replaceFirst("# ", "");
    }

    private static String getDescription(final List<String> lines) {
        if(lines.size() < 3) {
            return "";
        }
        return lines.get(2);
    }

    private static String getContent(final List<String> lines) {
        return String.join("\n", lines);
    }

}
