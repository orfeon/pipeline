package com.mercari.solution.module.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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

        // paginated list: ?page=N -> 2 items per page, 3 pages, has_more flag; requires Bearer secret
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
            final int page = pageOf(r.query());
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
        // nested pagination block {"data":{"items":[...],"has_more_data":bool}} and a per-item sub-resource
        server.createContext("/v1/courses", exchange -> {
            final Received r = record("courses", exchange);
            final String path = exchange.getRequestURI().getPath();
            if(path.matches("/v1/courses/[^/]+/enrollments")) {
                final String id = path.split("/")[3];
                final int page = pageOf(r.query());
                respond(exchange, 200, "{\"data\":{\"items\":[{\"course\":\"" + id + "\",\"user\":\"u" + page + "\"}],\"has_more_data\":" + (page < 2) + "}}");
                return;
            }
            final int page = pageOf(r.query());
            final JsonArray items = new JsonArray();
            for(int i = 0; i < 2; i++) {
                final JsonObject item = new JsonObject();
                item.addProperty("id", "c" + ((page - 1) * 2 + i + 1));
                items.add(item);
            }
            final JsonObject data = new JsonObject();
            data.add("items", items);
            data.addProperty("has_more_data", page < 2);
            final JsonObject body = new JsonObject();
            body.add("data", data);
            respond(exchange, 200, body.toString());
        });
        // cursor pagination via header: X-Next-Cursor until absent
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
        // POST search: echoes the JSON body it received
        server.createContext("/search", exchange -> {
            final Received r = record("search", exchange);
            respond(exchange, 200, "{\"echo\":" + r.body() + "}");
        });
        server.start();
    }

    @AfterAll
    public static void stopServer() {
        server.stop(0);
    }

    private static int pageOf(final String query) {
        int page = 1;
        if(query != null) {
            for(final String kv : query.split("&")) {
                if(kv.startsWith("page=")) {
                    page = Integer.parseInt(kv.substring(5));
                }
            }
        }
        return page;
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

    private static void cleanDir(final String dir) throws IOException {
        final Path path = Path.of(dir);
        if(Files.exists(path)) {
            try(final Stream<Path> walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private static List<Path> listFiles(final String dir) throws IOException {
        try(final Stream<Path> walk = Files.walk(Path.of(dir))) {
            return walk.filter(Files::isRegularFile).toList();
        }
    }

    @Test
    public void testPaginationTypedItemsAndChaining() throws Exception {
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
                          target:
                            url: %s
                            params:
                              page: ${page}
                              size: "2"
                          loop:
                            vars: { page: 1 }
                            next: { page: "${page + 1}" }
                            until: { key: payload.has_more, op: "=", value: false }
                          response:
                            itemsPath: /items
                            schema:
                              fields:
                                - { name: id, type: int64 }
                                - { name: name, type: string }
                                - { name: price, type: float64 }
                        - name: detail
                          input: items
                          target:
                            url: %s/${id}
                          rate: { count: 100 }
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
        Assertions.assertEquals(Set.of(1, 2, 3), new HashSet<>(list.stream().map(r -> pageOf(r.query())).toList()));
        Assertions.assertEquals(6, received("items").stream().filter(r -> r.path().matches("/items/\\d+")).count());
    }

    /** Raw parent pages archived to storage while a child fans out over the parent's response items (foreach). */
    @Test
    public void testForeachOverRawParentAndStorageSink() throws Exception {
        final String dir = "target/http-source-test/foreach";
        cleanDir(dir);
        final String configYaml = """
                system:
                  args:
                    start_date: "2026-01-01 00:00:00"
                sources:
                  - name: lms
                    module: http
                    parameters:
                      requests:
                        - name: courses
                          target:
                            url: %s
                            params:
                              page: ${page}
                              page_size: 200
                              status: published
                              last_update_from: '${args.start_date}'
                          loop:
                            vars: { page: 1 }
                            next: { page: "${page + 1}" }
                            until: { key: payload.data.has_more_data, op: "=", value: false }
                        - name: enrollments
                          input: courses
                          foreach: /data/items
                          target:
                            url: %s/${id}/enrollments
                            params:
                              page: ${page}
                          loop:
                            vars: { page: 1 }
                            next: { page: "${page + 1}" }
                            until: { key: payload.data.has_more_data, op: "=", value: false }
                sinks:
                  - name: archive
                    module: storage
                    inputs: [lms.courses, lms.enrollments]
                    parameters:
                      output: "%s/resource=${name}/${statusCode}"
                      format: avro
                      numShards: 1
                      suffix: ".avro"
                """.formatted(url("/v1/courses"), url("/v1/courses"), dir);
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("lms.courses").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("courses", e.getPrimitiveValue("name"));
                Assertions.assertEquals(200, e.getPrimitiveValue("statusCode"));
                Assertions.assertTrue(((String) e.getPrimitiveValue("payload")).contains("\"has_more_data\""));
            }
            Assertions.assertEquals(2, count);   // 2 pages, one raw record each
            return null;
        });
        PAssert.that(outputs.get("lms.enrollments").getCollection()).satisfies(elements -> {
            final Set<String> urls = new TreeSet<>();
            for(final MElement e : elements) {
                Assertions.assertEquals("enrollments", e.getPrimitiveValue("name"));
                urls.add((String) e.getPrimitiveValue("url"));
            }
            Assertions.assertEquals(8, urls.size());   // 4 courses x 2 pages
            return null;
        });
        pipeline.run();

        final List<Received> calls = received("courses");
        final List<Received> pages = calls.stream().filter(r -> r.path().equals("/v1/courses")).toList();
        Assertions.assertEquals(2, pages.size());
        for(final Received r : pages) {   // getQuery() decodes %xx (the space is form-encoded as +)
            Assertions.assertTrue(r.query().contains("page_size=200") && r.query().contains("status=published")
                    && r.query().contains("last_update_from=2026-01-01+00:00:00"), r.query());
        }
        final Set<String> enrollmentPaths = new TreeSet<>();
        calls.stream().filter(r -> r.path().endsWith("/enrollments")).forEach(r -> enrollmentPaths.add(r.path()));
        Assertions.assertEquals(Set.of("/v1/courses/c1/enrollments", "/v1/courses/c2/enrollments", "/v1/courses/c3/enrollments", "/v1/courses/c4/enrollments"), enrollmentPaths);

        final List<Path> files = listFiles(dir);
        final Set<String> dirs = new TreeSet<>();
        int rows = 0;
        for(final Path file : files) {
            final String f = file.toString().replace('\\', '/');
            // output "<dir>/resource=<name>/<statusCode>" -> the last segment is the file prefix
            dirs.add(f.substring(f.indexOf("resource="), f.lastIndexOf('/')));
            Assertions.assertTrue(f.substring(f.lastIndexOf('/') + 1).startsWith("200"), f);
            try(final org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> reader = new org.apache.avro.file.DataFileReader<>(
                    file.toFile(), new org.apache.avro.generic.GenericDatumReader<>())) {
                while(reader.hasNext()) {
                    final org.apache.avro.generic.GenericRecord record = reader.next();
                    rows++;
                    Assertions.assertEquals(200, record.get("statusCode"));
                    Assertions.assertNotNull(record.get("payload"));
                }
            }
        }
        Assertions.assertEquals(Set.of("resource=courses", "resource=enrollments"), dirs);
        Assertions.assertEquals(10, rows);
    }

    @Test
    public void testSingleRequestShorthandCursorRetryTextAndPostBody() throws Exception {
        COUNTERS.remove("flaky");
        final String configYaml = """
                sources:
                  - name: cursor
                    module: http
                    parameters:
                      target:
                        url: %s
                        params:
                          cursor: ${cursor}
                      loop:
                        vars: { cursor: "" }
                        next: { cursor: "${headers['x-next-cursor']!''}" }
                        until: { key: headers.x-next-cursor, op: "=", value: null }
                      response:
                        itemsPath: /
                  - name: flaky
                    module: http
                    parameters:
                      target: { url: %s }
                      response:
                        retry: { initialBackoff: 10ms, maxBackoff: 20ms, maxAttempts: 3 }
                  - name: text
                    module: http
                    parameters:
                      target: { url: %s }
                      response: { format: text }
                  - name: search
                    module: http
                    parameters:
                      target: { url: %s, method: POST }
                      body:
                        format: template
                        template: '{"q": "${utils.string.format("%%s", "shoes")}", "limit": 10}'
                      response:
                        schema:
                          fields:
                            - name: echo
                              type: element
                              fields:
                                - { name: q, type: string }
                                - { name: limit, type: int64 }
                """.formatted(url("/cursor"), url("/flaky"), url("/text"), url("/search"));
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
        PAssert.that(outputs.get("search").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                final Map<?, ?> echo = (Map<?, ?>) e.getPrimitiveValue("echo");
                Assertions.assertEquals("shoes", echo.get("q"));
                Assertions.assertEquals(10L, echo.get("limit"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(3, received("cursor").size());
        Assertions.assertEquals(2, received("flaky").size());
        final Received search = received("search").get(0);
        Assertions.assertEquals("POST", search.method());
        Assertions.assertEquals("application/json", search.headers().get("Content-type").get(0));
    }

    @Test
    public void testFailureRoutingAndValidate() throws Exception {
        final String configYaml = """
                sources:
                  - name: bad
                    module: http
                    failFast: false
                    parameters:
                      target: { url: %s }
                """.formatted(url("/bad"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("bad").getCollection()).empty();
        pipeline.run();
        Assertions.assertEquals(1, received("bad").size());

        // several requests need names
        assertInvalid("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      requests:
                        - target: { url: https://example.com/a }
                        - target: { url: https://example.com/b }
                """);
        // unknown input
        assertInvalid("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      requests:
                        - name: x
                          target: { url: https://example.com/a }
                          input: y
                """);
        // cyclic input
        assertInvalid("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      requests:
                        - name: x
                          target: { url: https://example.com/a }
                          input: y
                        - name: y
                          target: { url: https://example.com/b }
                          input: x
                """);
        // auth with templated host requires allowedHosts
        assertInvalid("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      auth: { type: bearer, token: t }
                      target: { url: https://${host}/a }
                """);
        // foreach needs an untyped parent
        assertInvalid("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      requests:
                        - name: x
                          target: { url: https://example.com/a }
                          response: { schema: { fields: [ { name: id, type: string } ] } }
                        - name: y
                          input: x
                          foreach: /items
                          target: { url: https://example.com/b/${id} }
                """);
        // single-request form cannot be mixed with requests
        assertInvalid("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      target: { url: https://example.com/a }
                      requests:
                        - target: { url: https://example.com/b }
                """);
        // loop needs until
        assertInvalid("""
                sources:
                  - name: a
                    module: http
                    parameters:
                      target: { url: https://example.com/a }
                      loop: { vars: { page: 1 }, next: { page: "${page + 1}" } }
                """);
    }

    private void assertInvalid(final String configYaml) {
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(configYaml)));
    }
}
