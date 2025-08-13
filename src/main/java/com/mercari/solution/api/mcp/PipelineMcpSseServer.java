package com.mercari.solution.api.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mercari.solution.api.mcp.prompt.Prompt;
import com.mercari.solution.api.mcp.resource.Resources;
import com.mercari.solution.api.mcp.tool.Tool;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletSseServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineMcpSseServer extends HttpServletSseServerTransportProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineMcpSseServer.class);

    private static final String INSTRUCTION = """
            Mercari Pipeline is a tool that defines and executes data pipelines in YAML or JSON format.
            This MCP server supports pipeline definitions in YAML or JSON.
            Specifically, it provides a list and details of built-in modules supported by Mercari Pipeline, as well as functions for verifying and executing defined pipelines.
            """;

    public PipelineMcpSseServer() {
        super(new ObjectMapper(), "/mcp/message", "/mcp/sse");
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        final McpSyncServer syncServer = McpServer.sync(this)
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

}
