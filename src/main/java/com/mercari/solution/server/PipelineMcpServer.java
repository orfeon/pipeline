package com.mercari.solution.server;

import com.mercari.solution.server.mcp.McpCaching;
import com.mercari.solution.server.mcp.prompt.Prompt;
import com.mercari.solution.server.mcp.resource.Resources;
import com.mercari.solution.server.mcp.tool.Tool;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.modern.server.feature.PromptsFeature;
import io.modelcontextprotocol.modern.server.feature.ResourcesFeature;
import io.modelcontextprotocol.modern.server.feature.ToolsFeature;
import io.modelcontextprotocol.modern.server.transport.HttpServletMcpTransport;
import io.modelcontextprotocol.server.transport.DefaultServerTransportSecurityValidator;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * The MCP endpoint ({@code /mcp}), speaking the 2026-07-28 revision of the protocol: stateless — no
 * {@code initialize} handshake and no session, every POST is a self-contained request carrying the
 * protocol version and the client's capabilities, answered by any instance of the service. Capabilities are
 * fetched with {@code server/discover}; lists and documents carry caching hints ({@link McpCaching});
 * long-running tools answer over a response stream.
 * <p>
 * The catalogs are fixed for the life of the server, so {@code subscriptions/listen} is not offered.
 * Authentication stays in front of the servlet (Cloud Run IAM / IAP).
 */
public class PipelineMcpServer extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineMcpServer.class);

    static final String ENDPOINT = "/mcp";

    /**
     * Comma-separated origins (e.g. {@code http://localhost:6274,https://example.com:*}) whose browser-based
     * clients may call the endpoint. Requests without an {@code Origin} header (every non-browser client)
     * always pass; any other origin is rejected (DNS-rebinding protection). This only lifts the origin
     * check — no CORS preflight or {@code Access-Control-Allow-*} handling is done here, so a client on
     * another origin still needs a proxy that terminates CORS.
     */
    static final String ENV_ALLOWED_ORIGINS = "MERCARI_PIPELINE_MCP_ALLOWED_ORIGINS";

    private static final String INSTRUCTION = """
            Mercari Pipeline is a tool that defines and executes data pipelines in YAML or JSON format.
            This MCP server supports pipeline definitions in YAML or JSON.
            Specifically, it provides a list and details of built-in modules supported by Mercari Pipeline, as well as functions for verifying and executing defined pipelines.
            """;

    private HttpServletMcpTransport transport;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        final McpServer server = createServer(getServletContext());
        this.transport = HttpServletMcpTransport.builder(server)
                .endpoint(ENDPOINT)
                .httpHeaderValidator(DefaultServerTransportSecurityValidator.builder()
                        .allowedOrigins(allowedOrigins(System.getenv(ENV_ALLOWED_ORIGINS)))
                        .build())
                .keepAliveInterval(Duration.ofSeconds(30))
                .build();
        this.transport.init(config);
        LOG.info("Mercari Pipeline MCP Server started: protocol {}, version {}",
                McpSchema.LATEST_PROTOCOL_VERSION, serverVersion());
    }

    static McpServer createServer(final ServletContext servletContext) {
        return McpServer.builder()
                .serverInfo(McpSchema.Implementation.builder("mercari-pipeline", serverVersion())
                        .title("Mercari Pipeline MCP Server")
                        .build())
                .instructions(INSTRUCTION)
                .feature(ToolsFeature.ofSync(Tool.repository(servletContext)))
                .feature(ResourcesFeature.ofSync(Resources.repository(servletContext)))
                .feature(PromptsFeature.ofSync(Prompt.repository()))
                .discoverCache(McpCaching.CATALOG_TTL_MS, McpSchema.CacheScope.PUBLIC)
                .build();
    }

    static List<String> allowedOrigins(final String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        // the validator compares strings and an Origin header never ends with '/': "http://localhost:6274/"
        // as copied from an address bar would otherwise match nothing
        return Arrays.stream(value.split(","))
                .map(origin -> origin.trim().replaceAll("/+$", ""))
                .filter(origin -> !origin.isEmpty())
                .toList();
    }

    private static String serverVersion() {
        final String version = ServerVersion.get();
        return version == null || version.isBlank() ? "unknown" : version;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.transport.service(req, resp);
    }

    @Override
    public void destroy() {
        if (this.transport != null) {
            this.transport.destroy();
        }
        super.destroy();
    }

}
