package com.mercari.solution.api.mcp;

import com.mercari.solution.api.mcp.prompt.Prompt;
import com.mercari.solution.api.mcp.resource.Resources;
import com.mercari.solution.api.mcp.tool.Tool;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;


public class PipelineMcpStreamableServer extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineMcpStreamableServer.class);

    private static final String HEADER_NAME = "header";

    private static final String INSTRUCTION = """
            Mercari Pipeline is a tool that defines and executes data pipelines in YAML or JSON format.
            This MCP server supports pipeline definitions in YAML or JSON.
            Specifically, it provides a list and details of built-in modules supported by Mercari Pipeline, as well as functions for verifying and executing defined pipelines.
            """;

    private HttpServletStreamableServerTransportProvider provider;

    public PipelineMcpStreamableServer() {
        super();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {

        this.provider = HttpServletStreamableServerTransportProvider.builder()
                .jsonMapper(McpJsonMapper.createDefault())
                .mcpEndpoint("/mcp")
                .disallowDelete(true)
                .contextExtractor((HttpServletRequest r) -> {
                    final String headerValue = r.getHeader(HEADER_NAME);
                    return headerValue != null ? McpTransportContext.create(Map.of("server-side-header-value", headerValue)) : McpTransportContext.EMPTY;
                })
                .keepAliveInterval(Duration.ofSeconds(60))
                .build();
        this.provider.init();

        super.init(config);
        final McpSyncServer syncServer = McpServer.sync(provider)
                .serverInfo("Mercari Pipeline MCP Server", "v1.0.0-beta2")
                .capabilities(McpSchema.ServerCapabilities.builder()
                        .resources(false, false)
                        .tools(false)
                        .prompts(false)
                        .logging()
                        .build())
                .tools(Tool.syncTools(getServletContext()))
                .resources(Resources.syncResources(getServletContext()))
                .prompts(Prompt.syncPrompts())
                .instructions(INSTRUCTION)
                .build();

        LOG.info("Mercari Pipeline MCP Server info: {}", syncServer.getServerInfo());
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.provider.service(req, resp);
    }

    @Override
    public void destroy() {
        if (this.provider != null) {
            this.provider.destroy();
        }
        super.destroy();
    }

}
