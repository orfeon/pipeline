package com.mercari.solution.module.action;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
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

public class HttpActionTest {

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
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.createContext("/notify", record("notify", (ex, body) -> respond(ex, 200, "{\"ok\":true}")));
        server.createContext("/bad", record("bad", (ex, body) -> respond(ex, 400, "{\"error\":\"nope\"}")));
        // async job: POST returns a job id + status url, GET /jobs/<id> goes PENDING -> RUNNING -> DONE (or FAILED for "bad" jobs)
        server.createContext("/jobs", record("jobs", (ex, body) -> {
            final String path = ex.getRequestURI().getPath();
            if("POST".equals(ex.getRequestMethod())) {
                final String id = body.contains("bad") ? "job-bad" : "job-" + COUNTERS.computeIfAbsent("jobs", k -> new AtomicInteger()).incrementAndGet();
                ex.getResponseHeaders().add("Location", "http://127.0.0.1:" + port + "/jobs/" + id);
                respond(ex, 202, "{\"id\":\"" + id + "\",\"statusUrl\":\"http://127.0.0.1:" + port + "/jobs/" + id + "\"}");
                return;
            }
            final String id = path.substring(path.lastIndexOf('/') + 1);
            final int n = COUNTERS.computeIfAbsent("poll-" + id, k -> new AtomicInteger()).incrementAndGet();
            final String state = id.equals("job-bad") ? (n < 2 ? "RUNNING" : "FAILED") : (n < 3 ? (n == 1 ? "PENDING" : "RUNNING") : "DONE");
            respond(ex, 200, "{\"id\":\"" + id + "\",\"state\":\"" + state + "\",\"result\":{\"rows\":" + (n * 10) + "}}");
        }));
        server.start();
    }

    @AfterAll
    public static void stopServer() {
        server.stop(0);
    }

    interface Handler {
        void handle(HttpExchange exchange, String body) throws IOException;
    }

    private static HttpHandler record(final String name, final Handler handler) {
        return exchange -> {
            final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            RECEIVED.computeIfAbsent(name, k -> new ConcurrentLinkedQueue<>()).add(new Received(
                    exchange.getRequestMethod(), exchange.getRequestURI().getPath(), exchange.getRequestURI().getQuery(),
                    new HashMap<>(exchange.getRequestHeaders()), body));
            handler.handle(exchange, body);
        };
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

    private static final String SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - table: users
                      rows: 10
                    - table: orders
                      rows: 20
                schema:
                  fields:
                    - name: table
                      type: string
                    - name: rows
                      type: int64
            """;

    @Test
    public void testOnceNotifyAfterSinkAndCollect() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: debug
                    module: debug
                    inputs: [input]
                  - name: notify
                    module: action.http
                    inputs: [input]
                    parameters:
                      trigger: once
                      target:
                        url: %s
                        headers:
                          X-Run: test-${__timestamp}
                      body:
                        format: template
                        template: '{"text": "pipeline finished"}'
                  - name: summary
                    module: action.http
                    inputs: [input]
                    parameters:
                      trigger: collect
                      target:
                        url: %s
                        params:
                          size: ${size}
                      body:
                        format: template
                        template: '{"tables": [<#list elements?sort_by("table") as e>"${e.table}:${e.rows}"<#sep>, </#list>]}'
                """.formatted(url("/notify/once"), url("/notify/collect"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("notify").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("http", element.getPrimitiveValue("service"));
                Assertions.assertEquals("POST", element.getPrimitiveValue("op"));
                Assertions.assertEquals("SUCCEEDED", element.getPrimitiveValue("state"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("jobId")).endsWith("/notify/once"));
                final JsonObject payload = JsonParser.parseString((String) element.getPrimitiveValue("payload")).getAsJsonObject();
                Assertions.assertEquals(200, payload.get("statusCode").getAsInt());
                Assertions.assertTrue(payload.getAsJsonObject("body").get("ok").getAsBoolean());
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        PAssert.that(outputs.get("summary").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(1, ((Collection<?>) elements).size());
            return null;
        });
        pipeline.run();

        final List<Received> notify = received("notify");
        final Received once = notify.stream().filter(r -> r.path().equals("/notify/once")).findFirst().orElseThrow();
        Assertions.assertEquals("{\"text\": \"pipeline finished\"}", once.body());
        Assertions.assertTrue(once.headers().get("X-run").get(0).startsWith("test-20"));
        Assertions.assertEquals("application/json", once.headers().get("Content-type").get(0));
        final Received collect = notify.stream().filter(r -> r.path().equals("/notify/collect")).findFirst().orElseThrow();
        Assertions.assertEquals("size=2", collect.query());
        Assertions.assertEquals("{\"tables\": [\"orders:20\", \"users:10\"]}", collect.body());
    }

    @Test
    public void testPerElementWithPoll() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: job
                    module: action.http
                    inputs: [input]
                    parameters:
                      trigger: perElement
                      target:
                        url: %s
                        method: POST
                      body:
                        format: json
                        fields: [table]
                      poll:
                        url: ${payload.statusUrl}
                        until: { key: payload.state, op: in, value: [DONE, FAILED] }
                        failWhen: { key: payload.state, op: "=", value: FAILED }
                        interval: 50ms
                        timeout: 30s
                """.formatted(url("/jobs"));
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("job");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("SUCCEEDED", element.getPrimitiveValue("state"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("jobId")).contains("/jobs/job-"));
                final JsonObject payload = JsonParser.parseString((String) element.getPrimitiveValue("payload")).getAsJsonObject();
                Assertions.assertEquals("DONE", payload.getAsJsonObject("body").get("state").getAsString());
                Assertions.assertEquals(30, payload.getAsJsonObject("body").getAsJsonObject("result").get("rows").getAsInt());
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        pipeline.run();

        // other tests share the endpoint (parallel execution): ignore the "bad" job
        final List<Received> jobs = received("jobs").stream().filter(r -> !r.path().contains("job-bad") && !r.body().contains("bad")).toList();
        Assertions.assertEquals(2, jobs.stream().filter(r -> r.method().equals("POST")).count());
        Assertions.assertEquals(6, jobs.stream().filter(r -> r.method().equals("GET")).count()); // 3 polls per job
        final Set<String> bodies = new HashSet<>();
        jobs.stream().filter(r -> r.method().equals("POST")).forEach(r -> bodies.add(r.body()));
        Assertions.assertEquals(Set.of("{\"table\":\"users\"}", "{\"table\":\"orders\"}"), bodies);
    }

    @Test
    public void testPollFailWhenAndRequestFailureRouting() throws Exception {
        final String configYaml = """
                sources:
                  - name: input
                    module: create
                    parameters:
                      type: element
                      elements:
                        - table: bad
                    schema:
                      fields:
                        - name: table
                          type: string
                sinks:
                  - name: job
                    module: action.http
                    inputs: [input]
                    failFast: false
                    parameters:
                      trigger: perElement
                      target:
                        url: %s
                      body: { format: json }
                      poll:
                        url: ${headers.location}
                        until: { key: payload.state, op: in, value: [DONE, FAILED] }
                        failWhen: { key: payload.state, op: "=", value: FAILED }
                        interval: 20ms
                  - name: bad
                    module: action.http
                    inputs: [input]
                    failFast: false
                    parameters:
                      trigger: once
                      target:
                        url: %s
                """.formatted(url("/jobs"), url("/bad"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        // failures are routed to the failure tag: no envelope record is emitted
        PAssert.that(outputs.get("job").getCollection()).empty();
        PAssert.that(outputs.get("bad").getCollection()).empty();
        pipeline.run();

        Assertions.assertEquals(1, received("bad").size());
        Assertions.assertEquals(2, received("jobs").stream().filter(r -> r.path().equals("/jobs/job-bad")).count());
    }

    @Test
    public void testValidate() {
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: a
                    module: action.http
                    inputs: [input]
                    parameters:
                      body: { format: json }
                """)));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: a
                    module: action.http
                    inputs: [input]
                    parameters:
                      target: { url: https://api.example.com/ }
                      poll: { interval: 1s }
                """)));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: a
                    module: action.http
                    inputs: [input]
                    parameters:
                      target: { url: https://api.example.com/ }
                      response:
                        partialFailure: { itemsPath: /items, errorCondition: { key: e, op: "!=", value: null } }
                """)));
    }
}
