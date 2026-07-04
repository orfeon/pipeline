package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import com.sun.net.httpserver.HttpServer;
import org.joda.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lookup-join tests against a local JDK HttpServer: path-placeholder endpoint,
 * per-distinct-key request loop, 404 → LEFT JOIN null, and rowsFrom fan-out.
 */
public class RestLookupSourceTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static HttpServer server;
    private static String baseUrl;
    private static final AtomicInteger userRequests = new AtomicInteger();

    @BeforeAll
    public static void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // GET /users/{id} → one JSON object, 404 for unknown ids
        server.createContext("/users/", exchange -> {
            userRequests.incrementAndGet();
            final String path = exchange.getRequestURI().getPath();
            final String id = path.substring(path.lastIndexOf('/') + 1);
            final String body = switch (id) {
                case "u1" -> "{\"name\":\"alice\",\"age\":20,\"active\":true}";
                case "u2" -> "{\"name\":\"bob\",\"age\":30,\"active\":false}";
                default -> null;
            };
            respond(exchange, body == null ? 404 : 200,
                    body == null ? "{\"error\":\"not found\"}" : body);
        });
        // GET /orders?userId={id} → array of orders under /items
        server.createContext("/orders", exchange -> {
            final String query = exchange.getRequestURI().getQuery();
            final String id = query == null ? "" : query.replace("userId=", "");
            final String body = switch (id) {
                case "u1" -> "{\"items\":[{\"orderId\":\"o1\",\"amount\":100},"
                        + "{\"orderId\":\"o2\",\"amount\":200}]}";
                default -> "{\"items\":[]}";
            };
            respond(exchange, 200, body);
        });
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterAll
    public static void stopServer() {
        server.stop(0);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status,
            String body) throws java.io.IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("uid", Schema.FieldType.STRING),
                Schema.Field.of("weight", Schema.FieldType.INT64)));
    }

    private static MElement input(String uid, long weight) {
        return MElement.of(Map.of("uid", uid, "weight", weight), TIMESTAMP);
    }

    @Test
    public void testSingleRowLookupAnd404AsNull() {
        userRequests.set(0);
        final RestLookupSource source = RestLookupSource.builder()
                .withName("api")
                .withAllowedHosts(List.of("127.0.0.1"))
                .withTable(RestLookupSource.TableConfig.builder()
                        .withName("users")
                        .withEndpoint(baseUrl + "/users/{id}")
                        .withField("name", Schema.FieldType.STRING)
                        .withField("age", Schema.FieldType.INT64)
                        .withField("active", Schema.FieldType.BOOLEAN)
                        .build())
                .build();

        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql("""
                        SELECT i.uid AS uid, u.name AS name, u.age AS age, u.active AS active
                        FROM INPUT AS i
                        LEFT JOIN api.users AS u ON u.id = i.uid
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    input("u1", 1L), input("u2", 2L), input("u9", 3L),
                    input("u1", 4L)),  // duplicate key: must not trigger a second request
                    TIMESTAMP);
            Assertions.assertEquals(4, outputs.size());
            final Map<String, MElement> byUid = new HashMap<>();
            for (final MElement output : outputs) {
                byUid.put(output.getAsString("uid") + "#" + output.getPrimitiveValue("name"),
                        output);
            }
            Assertions.assertEquals(20L, byUid.get("u1#alice").getAsLong("age"));
            Assertions.assertEquals(false, byUid.get("u2#bob").getPrimitiveValue("active"));
            Assertions.assertTrue(byUid.containsKey("u9#null")); // 404 → LEFT JOIN null
            // 3 distinct keys → exactly 3 HTTP requests (u1 deduped)
            Assertions.assertEquals(3, userRequests.get());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testRowsFromFanOut() {
        final RestLookupSource source = RestLookupSource.builder()
                .withName("api")
                .withBaseUrl(baseUrl)
                .withTable(RestLookupSource.TableConfig.builder()
                        .withName("orders")
                        .withEndpoint("/orders")
                        .withParam("userId", "{userId}")
                        .withRowsFrom("/items")
                        .withField("orderId", Schema.FieldType.STRING)
                        .withField("amount", Schema.FieldType.INT64)
                        .build())
                .build();

        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql("""
                        SELECT i.uid AS uid, o.orderId AS orderId, o.amount * i.weight AS weighted
                        FROM INPUT AS i
                        JOIN api.orders AS o ON o.userId = i.uid
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    input("u1", 2L), input("u2", 3L)), TIMESTAMP);
            // u1 fans out to two orders; u2 has none (INNER join drops it)
            Assertions.assertEquals(2, outputs.size());
            final Map<String, Long> weightedByOrder = new HashMap<>();
            for (final MElement output : outputs) {
                Assertions.assertEquals("u1", output.getAsString("uid"));
                weightedByOrder.put(output.getAsString("orderId"),
                        output.getAsLong("weighted"));
            }
            Assertions.assertEquals(200L, weightedByOrder.get("o1"));
            Assertions.assertEquals(400L, weightedByOrder.get("o2"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testDisallowedHostIsRejected() {
        final RestLookupSource source = RestLookupSource.builder()
                .withName("api")
                .withAllowedHosts(List.of("example.com"))
                .withTable(RestLookupSource.TableConfig.builder()
                        .withName("users")
                        .withEndpoint(baseUrl + "/users/{id}")
                        .withField("name", Schema.FieldType.STRING)
                        .build())
                .build();

        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql("""
                        SELECT i.uid AS uid, u.name AS name
                        FROM INPUT AS i
                        JOIN api.users AS u ON u.id = i.uid
                        """)
                .build();
        query.setup();
        try {
            final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                    () -> query.execute(List.of(input("u1", 1L)), TIMESTAMP));
            Assertions.assertTrue(rootMessage(e).contains("not in the allowed hosts"),
                    "unexpected message: " + rootMessage(e));
        } finally {
            query.teardown();
        }
    }

    private static String rootMessage(final Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return String.valueOf(t.getMessage());
    }
}
