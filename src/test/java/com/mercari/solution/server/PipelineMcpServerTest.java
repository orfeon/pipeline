package com.mercari.solution.server;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.modern.JsonRpc;
import io.modelcontextprotocol.modern.McpSchema;
import io.modelcontextprotocol.modern.server.McpServer;
import io.modelcontextprotocol.modern.server.McpTransportResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The MCP server as a 2026-07-28 client sees it: every request is self-contained (protocol version and
 * client capabilities in {@code _meta}), there is no handshake, and what is published carries caching hints.
 * Requests are dispatched straight into the server, below the servlet transport.
 */
public class PipelineMcpServerTest {

    private static final McpServer SERVER = PipelineMcpServer.createServer(null);

    private static final String CONFIG = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  schema:
                    fields:
                      - {name: id, type: string}
                  elements:
                    - {id: a}
            sinks:
              - name: output
                module: debug
                inputs: [input]
            """;

    @Test
    public void testDiscover() {
        final Map<String, Object> result = result("server/discover", Map.of());
        Assertions.assertEquals(List.of("2026-07-28"), result.get("supportedVersions"));
        final Map<String, Object> capabilities = map(result.get("capabilities"));
        for (final String capability : List.of("tools", "resources", "prompts", "completions")) {
            Assertions.assertTrue(capabilities.containsKey(capability), capability + " in " + capabilities);
        }
        Assertions.assertTrue(String.valueOf(result.get("instructions")).contains("Mercari Pipeline"));
        Assertions.assertEquals("public", String.valueOf(result.get("cacheScope")));
        Assertions.assertTrue(((Number) result.get("ttlMs")).longValue() > 0);
        final McpSchema.Implementation serverInfo = Assertions.assertInstanceOf(McpSchema.Implementation.class,
                map(result.get("_meta")).get("io.modelcontextprotocol/serverInfo"));
        Assertions.assertEquals("mercari-pipeline", serverInfo.name());
    }

    @Test
    public void testLegacyHandshakeIsRefused() {
        final JsonRpc.JSONRPCResponse.JSONRPCError error = error("initialize", Map.of(
                "protocolVersion", "2025-11-25",
                "capabilities", Map.of(),
                "clientInfo", Map.of("name", "legacy", "version", "1")), false);
        Assertions.assertEquals(McpSchema.ErrorCodes.UNSUPPORTED_PROTOCOL_VERSION, error.code());
        Assertions.assertTrue(error.message().contains("2026-07-28"), error.message());
    }

    @Test
    public void testRequestWithoutMetaIsRefused() {
        final JsonRpc.JSONRPCResponse.JSONRPCError error = error("tools/list", Map.of(), false);
        Assertions.assertEquals(McpSchema.ErrorCodes.INVALID_PARAMS, error.code());
    }

    @Test
    public void testToolsList() {
        final Map<String, Object> result = result("tools/list", Map.of());
        final List<String> names = new ArrayList<>();
        for (final Object tool : (List<?>) result.get("tools")) {
            names.add(String.valueOf(map(tool).get("name")));
            Assertions.assertTrue(map(tool).get("inputSchema") instanceof Map, names.getLast());
        }
        Assertions.assertTrue(names.size() >= 14, "tools: " + names);
        Assertions.assertEquals(names.stream().sorted().toList(), names, "tools/list must be in a deterministic order");
        Assertions.assertTrue(names.containsAll(List.of("run-pipeline", "launch-pipeline", "read-docs")), names.toString());
        Assertions.assertEquals("public", String.valueOf(result.get("cacheScope")));
        Assertions.assertTrue(((Number) result.get("ttlMs")).longValue() > 0);
        Assertions.assertNull(result.get("nextCursor"));

        Assertions.assertEquals(McpSchema.ErrorCodes.INVALID_PARAMS, error("tools/list", Map.of("cursor", "nope"), true).code());
    }

    @Test
    public void testToolCall() {
        final Map<String, Object> result = result("tools/call", Map.of("name", "list-modules", "arguments", Map.of("type", "transform")));
        Assertions.assertNotEquals(Boolean.TRUE, result.get("isError"));
        Assertions.assertEquals("complete", result.get("resultType"));
        Assertions.assertTrue(text(result).contains("## transform modules"), text(result));

        // arguments are optional on the wire
        Assertions.assertTrue(text(result("tools/call", Map.of("name", "list-modules"))).contains("## source modules"));

        // arguments are validated against the published input schema before the tool runs
        final Map<String, Object> invalid = result("tools/call", Map.of("name", "list-modules", "arguments", Map.of("type", 5)));
        Assertions.assertEquals(Boolean.TRUE, invalid.get("isError"));
        Assertions.assertTrue(text(invalid).startsWith("Invalid arguments"), text(invalid));

        Assertions.assertEquals(McpSchema.ErrorCodes.INVALID_PARAMS,
                error("tools/call", Map.of("name", "no-such-tool", "arguments", Map.of()), true).code());
    }

    @Test
    public void testLongRunningToolStreams() {
        final Map<String, Object> meta = new LinkedHashMap<>(meta());
        meta.put("progressToken", "p-1");
        final Map<String, Object> params = new LinkedHashMap<>();
        params.put("name", "run-pipeline");
        params.put("arguments", Map.of("config", CONFIG, "dryRun", true));
        params.put("_meta", meta);

        final McpTransportResponse response = SERVER
                .handleBlocking(McpTransportContext.EMPTY, new JsonRpc.JSONRPCRequest("2.0", "tools/call", "s-1", params))
                .block();
        final McpTransportResponse.Streaming streaming = Assertions.assertInstanceOf(McpTransportResponse.Streaming.class, response);
        final List<JsonRpc.JSONRPCMessage> messages = streaming.messages().collectList().block();
        Assertions.assertNotNull(messages);
        Assertions.assertEquals(2, messages.size(), messages.toString());

        final JsonRpc.JSONRPCNotification progress = Assertions.assertInstanceOf(JsonRpc.JSONRPCNotification.class, messages.getFirst());
        Assertions.assertEquals("notifications/progress", progress.method());

        final JsonRpc.JSONRPCResponse last = Assertions.assertInstanceOf(JsonRpc.JSONRPCResponse.class, messages.getLast());
        Assertions.assertNull(last.error(), String.valueOf(last.error()));
        final Map<String, Object> result = map(last.result());
        Assertions.assertNotEquals(Boolean.TRUE, result.get("isError"), text(result));
        Assertions.assertTrue(text(result).contains("\"status\""), text(result));
    }

    @Test
    public void testResources() {
        final List<String> uris = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            final Map<String, Object> page = result("resources/list", cursor == null ? Map.of() : Map.of("cursor", cursor));
            for (final Object resource : (List<?>) page.get("resources")) {
                uris.add(String.valueOf(map(resource).get("uri")));
            }
            Assertions.assertEquals("public", String.valueOf(page.get("cacheScope")));
            cursor = (String) page.get("nextCursor");
            pages++;
        } while (cursor != null && pages < 100);
        Assertions.assertTrue(uris.size() > 20, "docs resources: " + uris.size());
        Assertions.assertEquals(uris.stream().sorted().distinct().toList(), uris, "resources/list must be ordered, without repeats");
        Assertions.assertTrue(uris.contains("docs://module/transform/feature.md"));
        Assertions.assertEquals(McpSchema.ErrorCodes.INVALID_PARAMS, error("resources/list", Map.of("cursor", "nope"), true).code());

        final Map<String, Object> read = result("resources/read", Map.of("uri", "docs://module/transform/feature.md"));
        final Map<String, Object> contents = map(((List<?>) read.get("contents")).getFirst());
        Assertions.assertEquals("text/markdown", contents.get("mimeType"));
        Assertions.assertTrue(String.valueOf(contents.get("text")).contains("# Feature Transform Module"));
        Assertions.assertEquals("public", String.valueOf(read.get("cacheScope")));
        Assertions.assertTrue(((Number) read.get("ttlMs")).longValue() > 0);

        Assertions.assertEquals(McpSchema.ErrorCodes.INVALID_PARAMS,
                error("resources/read", Map.of("uri", "docs://no-such-doc.md"), true).code());
    }

    @Test
    public void testResourceTemplateCompletion() {
        final Map<String, Object> templates = result("resources/templates/list", Map.of());
        final Map<String, Object> template = map(((List<?>) templates.get("resourceTemplates")).getFirst());
        final String uriTemplate = String.valueOf(template.get("uriTemplate"));
        Assertions.assertEquals("docs://module/{type}/{name}.md", uriTemplate);

        final Map<String, Object> ref = Map.of("type", "ref/resource", "uri", uriTemplate);
        final List<?> types = (List<?>) map(result("completion/complete", Map.of(
                "ref", ref, "argument", Map.of("name", "type", "value", "s"))).get("completion")).get("values");
        Assertions.assertEquals(List.of("sink", "source"), types);

        final List<?> names = (List<?>) map(result("completion/complete", Map.of(
                "ref", ref,
                "argument", Map.of("name", "name", "value", "big"),
                "context", Map.of("arguments", Map.of("type", "source")))).get("completion")).get("values");
        Assertions.assertTrue(names.contains("bigquery") && names.contains("bigtable"), names.toString());
    }

    @Test
    public void testPrompts() {
        final Map<String, Object> list = result("prompts/list", Map.of());
        final Map<String, Object> prompt = map(((List<?>) list.get("prompts")).getFirst());
        Assertions.assertEquals("design-pipeline", prompt.get("name"));
        Assertions.assertEquals("public", String.valueOf(list.get("cacheScope")));

        final Map<String, Object> get = result("prompts/get", Map.of("name", "design-pipeline"));
        final Map<String, Object> message = map(((List<?>) get.get("messages")).getFirst());
        Assertions.assertTrue(String.valueOf(map(message.get("content")).get("text")).contains("run-pipeline"));
    }

    private static Map<String, Object> meta() {
        return Map.of(
                "io.modelcontextprotocol/protocolVersion", "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities", Map.of(),
                "io.modelcontextprotocol/clientInfo", Map.of("name", "test", "version", "1"));
    }

    private static JsonRpc.JSONRPCResponse send(final String method, final Map<String, Object> params, final boolean withMeta) {
        final Map<String, Object> full = new LinkedHashMap<>(params);
        if (withMeta) {
            full.put("_meta", meta());
        }
        final McpTransportResponse response = SERVER
                .handleBlocking(McpTransportContext.EMPTY, new JsonRpc.JSONRPCRequest("2.0", method, "t-1", full))
                .block();
        if (response instanceof McpTransportResponse.Result result) {
            return result.response();
        }
        if (response instanceof McpTransportResponse.Error error) {
            return error.response();
        }
        throw new AssertionError(method + " answered with " + response);
    }

    private static Map<String, Object> result(final String method, final Map<String, Object> params) {
        final JsonRpc.JSONRPCResponse response = send(method, params, true);
        Assertions.assertNull(response.error(), method + ": " + response.error());
        return map(response.result());
    }

    private static JsonRpc.JSONRPCResponse.JSONRPCError error(final String method, final Map<String, Object> params, final boolean withMeta) {
        final JsonRpc.JSONRPCResponse response = send(method, params, withMeta);
        Assertions.assertNotNull(response.error(), method + " should fail: " + response.result());
        return response.error();
    }

    private static String text(final Map<String, Object> callToolResult) {
        return String.valueOf(map(((List<?>) callToolResult.get("content")).getFirst()).get("text"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(final Object value) {
        Assertions.assertInstanceOf(Map.class, value);
        return (Map<String, Object>) value;
    }

}
