package com.mercari.solution.module.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    record Received(String method, String path, String query, Map<String, List<String>> headers, String body) {}

    private static HttpServer server;
    private static int port;
    private static final Map<String, ConcurrentLinkedQueue<Received>> RECEIVED = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> COUNTERS = new ConcurrentHashMap<>();

    @BeforeAll
    public static void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.setExecutor(Executors.newFixedThreadPool(8));

        // paginated list: ?page=N -> 2 items per page, 3 pages; requires Authorization: Bearer secret
        server.createContext("/items", exchange -> {
            final Received r = record("items", exchange);
            if(!"Bearer secret".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                respond(exchange, 401, "{\"error\":\"unauthorized\"}");
                return;
            }
            final String path = exchange.getRequestURI().getPath();
            if(path.matches("/items/\\d+")) {
                final String id = path.substring(path.lastIndexOf('/') + 1);
                respond(exchange, 200, "{\"id\":" + id + ",\"detail\":\"detail-" + id + "\",\"tags\":[\"a\",\"b\"]}");
                return;
            }
            int page = 1;
            if(r.query() != null) {
                for(final String kv : r.query().split("&")) {
                    if(kv.startsWith("page=")) {
                        page = Integer.parseInt(kv.substring(5));
                    }
                }
            }
            final JsonArray items = new JsonArray();
            for(int i = 0; i < 2; i++) {
                final JsonObject item = new JsonObject();
                final int id = (page - 1) * 2 + i + 1;
                item.addProperty("id", id);
                item.addProperty("name", "item-" + id);
                item.addProperty("price", id * 1.5);
                items.add(item);
            }
            final JsonObject body = new JsonObject();
            body.add("items", items);
            body.addProperty("page", page);
            body.addProperty("has_more", page < 3);
            respond(exchange, 200, body.toString());
        });
        // cursor pagination via header: X-Next-Cursor until empty
        server.createContext("/cursor", exchange -> {
            final Received r = record("cursor", exchange);
            final String cursor = r.query() == null ? "" : r.query().replace("cursor=", "");
            final int n = cursor.isEmpty() ? 0 : Integer.parseInt(cursor);
            if(n < 2) {
                exchange.getResponseHeaders().add("X-Next-Cursor", String.valueOf(n + 1));
            }
            respond(exchange, 200, "[{\"v\":" + n + "}]");
        });
        // flaky: first call 503, then 200
        server.createContext("/flaky", exchange -> {
            record("flaky", exchange);
            if(COUNTERS.computeIfAbsent("flaky", k -> new AtomicInteger()).incrementAndGet() == 1) {
                respond(exchange, 503, "busy");
            } else {
                respond(exchange, 200, "{\"ok\":true}");
            }
        });
        server.createContext("/bad", exchange -> {
            record("bad", exchange);
            respond(exchange, 404, "{\"error\":\"not found\"}");
        });
        server.createContext("/text", exchange -> {
            record("text", exchange);
            exchange.getResponseHeaders().add("Content-Type", "text/plain");
            final byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try(final OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    @AfterAll
    public static void stopServer() {
        server.stop(0);
    }

    private static Received record(final String name, final HttpExchange exchange) throws IOException {
        final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        final Received r = new Received(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), exchange.getRequestURI().getQuery(),
                new HashMap<>(exchange.getRequestHeaders()), body);
        RECEIVED.computeIfAbsent(name, k -> new ConcurrentLinkedQueue<>()).add(r);
        return r;
    }

    private static void respond(final HttpExchange exchange, final int status, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try(final OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String url(final String path) {
        return "http://127.0.0.1:" + port + path;
    }

    private static List<Received> received(final String name) {
        return new ArrayList<>(RECEIVED.getOrDefault(name, new ConcurrentLinkedQueue<>()));
    }

    @Test
    public void testPaginationTypedRowsAndChaining() throws Exception {
        final String configYaml = """
                sources:
                  - name: api
                    module: http
                    parameters:
                      auth:
                        type: bearer
                        token: secret
                      requests:
                        - name: items
                          url: %s
                          params:
                            page: ${page}
                            size: "2"
                          loop:
                            vars: { page: 1 }
                            feeds: { page: "${page + 1}" }
                            condition: { key: payload.has_more, op: "=", value: true }
                          response:
                            rowsFrom: /items
                            schema:
                              fields:
                                - { name: id, type: int64 }
                                - { name: name, type: string }
                                - { name: price, type: float64 }
                        - name: detail
                          input: items
                          url: %s/${id}
                          response:
                            schema:
                              fields:
                                - { name: id, type: int64 }
                                - { name: detail, type: string }
                                - { name: tags, type: string, mode: repeated }
                """.formatted(url("/items"), url("/items"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("api").getCollection()).satisfies(elements -> {
            final Set<Long> ids = new TreeSet<>();
            for(final MElement e : elements) {
                ids.add((Long) e.getPrimitiveValue("id"));
                Assertions.assertEquals("item-" + e.getPrimitiveValue("id"), e.getPrimitiveValue("name"));
                Assertions.assertEquals(((Long) e.getPrimitiveValue("id")) * 1.5, (Double) e.getPrimitiveValue("price"), 0.0001);
            }
            Assertions.assertEquals(Set.of(1L, 2L, 3L, 4L, 5L, 6L), ids);
            return null;
        });
        PAssert.that(outputs.get("api.items").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(6, ((Collection<?>) elements).size());
            return null;
        });
        PAssert.that(outputs.get("api.detail").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("detail-" + e.getPrimitiveValue("id"), e.getPrimitiveValue("detail"));
                Assertions.assertEquals(List.of("a", "b"), e.getPrimitiveValue("tags"));
            }
            Assertions.assertEquals(6, count);
            return null;
        });
        pipeline.run();

        final List<Received> list = received("items").stream().filter(r -> r.path().equals("/items")).toList();
        Assertions.assertEquals(3, list.size());
        Assertions.assertTrue(list.stream().allMatch(r -> r.query().contains("size=2")));
        Assertions.assertEquals(Set.of("page=1", "page=2", "page=3"),
                new HashSet<>(list.stream().map(r -> Arrays.stream(r.query().split("&")).filter(q -> q.startsWith("page=")).findFirst().orElseThrow()).toList()));
        Assertions.assertEquals(6, received("items").stream().filter(r -> r.path().matches("/items/\\d+")).count());
    }

    @Test
    public void testRawOutputHeaderCursorRetryAndText() throws Exception {
        COUNTERS.remove("flaky");
        final String configYaml = """
                sources:
                  - name: cursor
                    module: http
                    parameters:
                      requests:
                        - url: %s
                          params:
                            cursor: ${cursor}
                          loop:
                            vars: { cursor: "" }
                            feeds: { cursor: "${headers['x-next-cursor']!''}" }
                            condition: { key: headers.x-next-cursor, op: "!=", value: null }
                          response:
                            rowsFrom: /
                  - name: flaky
                    module: http
                    parameters:
                      requests:
                        - url: %s
                          response:
                            retry: { initialBackoff: 10ms, maxBackoff: 20ms, maxAttempts: 3 }
                  - name: text
                    module: http
                    parameters:
                      requests:
                        - url: %s
                          response: { format: text }
                """.formatted(url("/cursor"), url("/flaky"), url("/text"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("cursor").getCollection()).satisfies(elements -> {
            final Set<String> payloads = new TreeSet<>();
            for(final MElement e : elements) {
                Assertions.assertEquals("cursor", e.getPrimitiveValue("name"));
                Assertions.assertEquals(200, e.getPrimitiveValue("statusCode"));
                Assertions.assertEquals("GET", e.getPrimitiveValue("method"));
                payloads.add((String) e.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(Set.of("{\"v\":0}", "{\"v\":1}", "{\"v\":2}"), payloads);
            return null;
        });
        PAssert.that(outputs.get("flaky").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals(2, e.getPrimitiveValue("attempts"));
                Assertions.assertEquals("{\"ok\":true}", e.getPrimitiveValue("payload"));
                Assertions.assertTrue(((String) e.getPrimitiveValue("url")).endsWith("/flaky"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        PAssert.that(outputs.get("text").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("hello world", e.getPrimitiveValue("body"));
                Assertions.assertNull(e.getPrimitiveValue("payload"));
            }
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(3, received("cursor").size());
        Assertions.assertEquals(2, received("flaky").size());
    }

    @Test
    public void testFailureRoutingAndValidate() throws Exception {
        final String configYaml = """
                sources:
                  - name: bad
                    module: http
                    failFast: false
                    parameters:
                      requests:
                        - url: %s
                """.formatted(url("/bad"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("bad").getCollection()).empty();
        pipeline.run();
        Assertions.assertEquals(1, received("bad").size());

        // several requests need names
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      requests:
                        - url: https://example.com/a
                        - url: https://example.com/b
                """)));
        // unknown input
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      requests:
                        - name: x
                          url: https://example.com/a
                          input: y
                """)));
        // cyclic input
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      requests:
                        - name: x
                          url: https://example.com/a
                          input: y
                        - name: y
                          url: https://example.com/b
                          input: x
                """)));
        // auth with templated host requires allowedHosts
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      auth: { type: bearer, token: t }
                      requests:
                        - url: https://${host}/a
                """)));
    }
}
