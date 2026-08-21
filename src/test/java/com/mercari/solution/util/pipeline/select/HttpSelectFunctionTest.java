package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.sun.net.httpserver.HttpServer;
import org.joda.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HttpSelectFunctionTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static HttpServer server;
    private static String baseUrl;
    private static final ConcurrentLinkedQueue<String> authorizations = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<String> posted = new ConcurrentLinkedQueue<>();

    @BeforeAll
    public static void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/users/", exchange -> {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            final String path = exchange.getRequestURI().getPath();
            final String id = path.substring(path.lastIndexOf('/') + 1);
            final String body = switch (id) {
                case "u1" -> "{\"name\":\"alice\",\"age\":20,\"tags\":[\"a\",\"b\"]}";
                case "u2" -> "{\"name\":\"bob\",\"age\":30,\"tags\":[]}";
                default -> null;
            };
            respond(exchange, body == null ? 404 : 200, body == null ? "{\"error\":\"not found\"}" : body);
        });
        server.createContext("/echo", exchange -> {
            final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            posted.add(exchange.getRequestHeaders().getFirst("Content-Type") + "|" + body);
            respond(exchange, 200, body);
        });
        server.createContext("/bytes", exchange -> {
            final byte[] bytes = new byte[]{1, 2, 3};
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.createContext("/bad", exchange -> respond(exchange, 400, "{\"error\":\"bad\"}"));
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    public static void stopServer() {
        server.stop(0);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws java.io.IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static SelectFunction create(final String json) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        final SelectFunction selectFunction = SelectFunction.of(jsonObject, inputFields());
        selectFunction.setup();
        return selectFunction;
    }

    private static List<Schema.Field> inputFields() {
        return List.of(
                Schema.Field.of("uid", Schema.FieldType.STRING),
                Schema.Field.of("weight", Schema.FieldType.INT64));
    }

    private static Map<String, Object> input(final String uid, final long weight) {
        final Map<String, Object> input = new HashMap<>();
        input.put("uid", uid);
        input.put("weight", weight);
        return input;
    }

    @Test
    public void testTypedStructWithAuthAnd404AsNull() {
        authorizations.clear();
        final SelectFunction f = create("""
                { "name": "user", "func": "http",
                  "target": { "url": "%s/users/${uid}", "auth": { "type": "bearer", "token": "secret" } },
                  "response": { "schema": { "fields": [
                      { "name": "name", "type": "string" },
                      { "name": "age", "type": "int64" },
                      { "name": "tags", "type": "string", "mode": "repeated" } ] } } }
                """.formatted(baseUrl));
        Assertions.assertEquals(Schema.Type.element, f.getOutputFieldType().getType());
        final Map<?, ?> alice = (Map<?, ?>) f.apply(input("u1", 1L), TIMESTAMP);
        Assertions.assertEquals("alice", alice.get("name"));
        Assertions.assertEquals(20L, alice.get("age"));
        Assertions.assertEquals(List.of("a", "b"), alice.get("tags"));
        Assertions.assertNull(f.apply(input("u9", 1L), TIMESTAMP));
        Assertions.assertTrue(authorizations.stream().allMatch("Bearer secret"::equals), authorizations.toString());
    }

    @Test
    public void testTextDefaultPostJsonBodyAndBytes() {
        posted.clear();
        final SelectFunction echo = create("""
                { "name": "echo", "func": "http",
                  "target": { "url": "%s/echo", "method": "POST" },
                  "body": { "format": "json", "fields": ["uid"] } }
                """.formatted(baseUrl));
        Assertions.assertEquals(Schema.Type.string, echo.getOutputFieldType().getType());
        Assertions.assertEquals("{\"uid\":\"u1\"}", echo.apply(input("u1", 1L), TIMESTAMP));
        Assertions.assertEquals("application/json|{\"uid\":\"u1\"}", posted.poll());

        final SelectFunction template = create("""
                { "name": "t", "func": "http",
                  "target": { "url": "%s/echo", "method": "POST", "headers": { "Content-Type": "text/plain" } },
                  "body": { "format": "template", "template": "uid=${uid};w=${weight}" } }
                """.formatted(baseUrl));
        Assertions.assertEquals("uid=u2;w=5", template.apply(input("u2", 5L), TIMESTAMP));
        Assertions.assertEquals("text/plain|uid=u2;w=5", posted.poll());

        final SelectFunction bytes = create("""
                { "name": "b", "func": "http", "target": { "url": "%s/bytes" }, "response": { "format": "bytes" } }
                """.formatted(baseUrl));
        Assertions.assertEquals(Schema.Type.bytes, bytes.getOutputFieldType().getType());
        Assertions.assertArrayEquals(new byte[]{1, 2, 3}, ((ByteBuffer) bytes.apply(input("u1", 1L), TIMESTAMP)).array());
    }

    @Test
    public void testFailureAndValidation() {
        final SelectFunction bad = create("""
                { "name": "bad", "func": "http", "target": { "url": "%s/bad" } }
                """.formatted(baseUrl));
        Assertions.assertThrows(IllegalStateException.class, () -> bad.apply(input("u1", 1L), TIMESTAMP));
        // target required
        Assertions.assertThrows(IllegalArgumentException.class, () -> create("{ \"name\": \"x\", \"func\": \"http\" }"));
        // auth with a templated host needs allowedHosts
        Assertions.assertThrows(IllegalArgumentException.class, () -> create(
                "{ \"name\": \"x\", \"func\": \"http\", \"target\": { \"url\": \"https://${uid}.example.com/\", \"auth\": { \"type\": \"bearer\", \"token\": \"t\" } } }"));
    }
}
