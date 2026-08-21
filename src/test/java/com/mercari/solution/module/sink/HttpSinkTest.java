package com.mercari.solution.module.sink;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.pipeline.outbound.OutboundRequest;
import com.mercari.solution.util.pipeline.outbound.RequestSpec;
import com.mercari.solution.util.pipeline.outbound.ResponsePolicy;
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

public class HttpSinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    /** One received request. */
    record Received(String method, String path, String query, Map<String, List<String>> headers, String body) {}

    private static HttpServer server;
    private static int port;
    private static final java.security.KeyPair KEY_PAIR;
    static {
        try {
            final java.security.KeyPairGenerator gen = java.security.KeyPairGenerator.getInstance("RSA");
            gen.initialize(2048);
            KEY_PAIR = gen.generateKeyPair();
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String privateKeyPem() {
        final String b64 = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(KEY_PAIR.getPrivate().getEncoded());
        return "-----BEGIN PRIVATE KEY-----\n" + b64 + "\n-----END PRIVATE KEY-----\n";
    }
    private static final Map<String, ConcurrentLinkedQueue<Received>> RECEIVED = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> COUNTERS = new ConcurrentHashMap<>();

    @BeforeAll
    public static void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.setExecutor(Executors.newFixedThreadPool(8));

        // 201 + json body echoing an id
        server.createContext("/ok", record("ok", (ex, body) -> {
            final JsonObject json = body.isBlank() ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
            respond(ex, 201, "{\"id\":\"created-" + (json.has("user_id") ? json.get("user_id").getAsString() : "none") + "\",\"ok\":true}");
        }));
        // first call 503 with Retry-After, later calls 200
        server.createContext("/flaky", record("flaky", (ex, body) -> {
            final int n = COUNTERS.computeIfAbsent("flaky", k -> new AtomicInteger()).incrementAndGet();
            if(n == 1) {
                ex.getResponseHeaders().add("Retry-After", "0");
                respond(ex, 503, "busy");
            } else {
                respond(ex, 200, "{\"ok\":true}");
            }
        }));
        // echoes 200
        server.createContext("/echo", record("echo", (ex, body) -> respond(ex, 200, "{}")));
        // gzip-aware echo: records the decompressed body
        server.createContext("/gzip", exchange -> {
            byte[] raw = exchange.getRequestBody().readAllBytes();
            if("gzip".equalsIgnoreCase(exchange.getRequestHeaders().getFirst("Content-Encoding"))) {
                try(final java.util.zip.GZIPInputStream in = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(raw))) {
                    raw = in.readAllBytes();
                }
            }
            RECEIVED.computeIfAbsent("gzip", k -> new ConcurrentLinkedQueue<>()).add(new Received(
                    exchange.getRequestMethod(), exchange.getRequestURI().getPath(), exchange.getRequestURI().getQuery(),
                    new HashMap<>(exchange.getRequestHeaders()), new String(raw, StandardCharsets.UTF_8)));
            respond(exchange, 200, "{}");
        });
        // jwt-bearer token endpoint: verifies the RS256 assertion against the test public key
        server.createContext("/oauth/jwt", record("jwt", (ex, body) -> {
            final Map<String, String> form = new HashMap<>();
            for(final String kv : body.split("&")) {
                final String[] p = kv.split("=", 2);
                form.put(java.net.URLDecoder.decode(p[0], StandardCharsets.UTF_8), java.net.URLDecoder.decode(p[1], StandardCharsets.UTF_8));
            }
            if(!"urn:ietf:params:oauth:grant-type:jwt-bearer".equals(form.get("grant_type"))) {
                respond(ex, 400, "{\"error\":\"unsupported_grant_type\"}");
                return;
            }
            try {
                final String[] parts = form.get("assertion").split("\\.");
                final java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
                sig.initVerify(KEY_PAIR.getPublic());
                sig.update((parts[0] + "." + parts[1]).getBytes(StandardCharsets.UTF_8));
                if(!sig.verify(Base64.getUrlDecoder().decode(parts[2]))) {
                    respond(ex, 401, "{\"error\":\"bad_signature\"}");
                    return;
                }
                final JsonObject claims = JsonParser.parseString(new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8)).getAsJsonObject();
                if(!"svc@example.com".equals(claims.get("iss").getAsString()) || !claims.has("exp")) {
                    respond(ex, 401, "{\"error\":\"bad_claims\"}");
                    return;
                }
                respond(ex, 200, "{\"access_token\":\"jwt-token\",\"expires_in\":3600}");
            } catch (final Exception e) {
                respond(ex, 500, e.toString());
            }
        }));
        server.createContext("/secure-jwt", record("secure-jwt", (ex, body) -> {
            final String auth = ex.getRequestHeaders().getFirst("Authorization");
            respond(ex, "Bearer jwt-token".equals(auth) ? 200 : 401, "{}");
        }));
        // always 400
        server.createContext("/bad", record("bad", (ex, body) -> respond(ex, 400, "{\"error\":\"invalid\"}")));
        // 200 but ok=false when user_id == u2
        server.createContext("/conditional", record("conditional", (ex, body) -> {
            final JsonObject json = JsonParser.parseString(body).getAsJsonObject();
            respond(ex, 200, "{\"ok\":" + (!"u2".equals(json.get("user_id").getAsString())) + "}");
        }));
        // requires Authorization: Bearer <token-N>; first token is rejected once (401) to exercise refresh
        server.createContext("/secure", record("secure", (ex, body) -> {
            final String auth = ex.getRequestHeaders().getFirst("Authorization");
            if(auth == null || !auth.startsWith("Bearer ")) {
                respond(ex, 401, "no token");
                return;
            }
            final String token = auth.substring("Bearer ".length());
            if(token.equals("token-1")) {
                respond(ex, 401, "expired");
                return;
            }
            respond(ex, 200, "{\"ok\":true}");
        }));
        // oauth2 token endpoint: issues token-N
        server.createContext("/oauth/token", record("token", (ex, body) -> {
            final String auth = ex.getRequestHeaders().getFirst("Authorization");
            if(auth == null || !auth.startsWith("Basic ")) {
                respond(ex, 401, "no client auth");
                return;
            }
            final int n = COUNTERS.computeIfAbsent("token", k -> new AtomicInteger()).incrementAndGet();
            respond(ex, 200, "{\"access_token\":\"token-" + n + "\",\"expires_in\":3600,\"token_type\":\"Bearer\"}");
        }));
        // bulk: ndjson lines; items with "fail" -> error, items with "retry" -> 429 once
        server.createContext("/bulk", record("bulk", (ex, body) -> {
            final JsonArray items = new JsonArray();
            boolean errors = false;
            for(final String line : body.split("\n")) {
                if(line.isBlank()) {
                    continue;
                }
                final JsonObject doc = JsonParser.parseString(line).getAsJsonObject();
                final JsonObject item = new JsonObject();
                final JsonObject index = new JsonObject();
                final String id = doc.get("user_id").getAsString();
                if(id.contains("fail")) {
                    index.addProperty("status", 400);
                    index.addProperty("error", "mapping");
                    errors = true;
                } else if(id.contains("retry") && COUNTERS.computeIfAbsent("bulk-" + id, k -> new AtomicInteger()).incrementAndGet() == 1) {
                    index.addProperty("status", 429);
                    index.addProperty("error", "rejected");
                    errors = true;
                } else {
                    index.addProperty("status", 200);
                }
                item.add("index", index);
                items.add(item);
            }
            final JsonObject response = new JsonObject();
            response.addProperty("errors", errors);
            response.add("items", items);
            respond(ex, 200, response.toString());
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
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getQuery(),
                    new HashMap<>(exchange.getRequestHeaders()),
                    body));
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
                    - app_id: id111
                      user_id: u1
                      amount: 10
                      note: hello
                    - app_id: com.example.app
                      user_id: u2
                      amount: 20
                      note: null
                schema:
                  fields:
                    - name: app_id
                      type: string
                    - name: user_id
                      type: string
                    - name: amount
                      type: int64
                    - name: note
                      type: string
            """;

    private static Map<String, Integer> states(final Iterable<MElement> elements) {
        final Map<String, Integer> states = new TreeMap<>();
        for(final MElement element : elements) {
            states.merge((String) element.getPrimitiveValue("state"), 1, Integer::sum);
        }
        return states;
    }

    @Test
    public void testPerElementJsonWithTemplatesAndPayloadSchema() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs:
                      - input
                    parameters:
                      target:
                        url: %s/${app_id?url}
                        method: POST
                        params:
                          src: ${__source}
                          fixed: "1"
                        headers:
                          X-User: ${user_id}
                          X-Static: fixed
                          X-Sig: ${utils.string.sha256(__body)}
                      body:
                        format: json
                        omitNulls: true
                      response:
                        format: json
                        schema:
                          fields:
                            - name: id
                              type: string
                            - name: ok
                              type: boolean
                """.formatted(url("/ok"));
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("http");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("SUCCEEDED", element.getPrimitiveValue("state"));
                Assertions.assertEquals(201, element.getPrimitiveValue("statusCode"));
                Assertions.assertEquals(1, element.getPrimitiveValue("attempts"));
                Assertions.assertEquals(1L, element.getPrimitiveValue("elementCount"));
                Assertions.assertEquals(0L, element.getPrimitiveValue("failedCount"));
                Assertions.assertTrue((Long) element.getPrimitiveValue("bytes") > 0);
                Assertions.assertNull(element.getPrimitiveValue("error"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("body")).contains("created-"));
                final Map<?, ?> payload = (Map<?, ?>) element.getPrimitiveValue("payload");
                Assertions.assertNotNull(payload);
                Assertions.assertTrue(payload.get("id").toString().startsWith("created-"));
                Assertions.assertEquals(true, payload.get("ok"));
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        pipeline.run();

        final List<Received> requests = new ArrayList<>(received("ok").stream().filter(r -> r.path().startsWith("/ok/")).toList());
        Assertions.assertEquals(2, requests.size());
        requests.sort(Comparator.comparing(Received::path));
        final Received r1 = requests.get(0);
        Assertions.assertEquals("POST", r1.method());
        Assertions.assertEquals("/ok/com.example.app", r1.path());
        Assertions.assertTrue(r1.query().contains("src=input"));
        Assertions.assertTrue(r1.query().contains("fixed=1"));
        Assertions.assertEquals("u2", r1.headers().get("X-user").get(0));
        Assertions.assertEquals("fixed", r1.headers().get("X-static").get(0));
        Assertions.assertEquals("application/json", r1.headers().get("Content-type").get(0));
        final JsonObject body1 = JsonParser.parseString(r1.body()).getAsJsonObject();
        Assertions.assertEquals("u2", body1.get("user_id").getAsString());
        Assertions.assertEquals(20, body1.get("amount").getAsLong());
        Assertions.assertFalse(body1.has("note"));
        // signature header computed over the serialized body
        Assertions.assertEquals(TasksSink.sha256Hex(r1.body()), r1.headers().get("X-sig").get(0));
        final Received r2 = requests.get(1);
        Assertions.assertEquals("/ok/id111", r2.path());
        Assertions.assertEquals("hello", JsonParser.parseString(r2.body()).getAsJsonObject().get("note").getAsString());
    }

    @Test
    public void testRetryThenSuccessAndFailed() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: flaky
                    module: http
                    inputs:
                      - input
                    parameters:
                      target:
                        url: %s
                      response:
                        retry:
                          initialBackoff: 10ms
                          maxBackoff: 50ms
                          maxAttempts: 3
                      concurrency: 2
                  - name: bad
                    module: http
                    inputs:
                      - input
                    failFast: false
                    parameters:
                      target:
                        url: %s
                        method: PUT
                      body:
                        format: template
                        template: '{"u":"${user_id}","n":${amount}}'
                      response:
                        format: text
                """.formatted(url("/flaky"), url("/bad"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("flaky").getCollection()).satisfies(elements -> {
            int count = 0;
            int retried = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("SUCCEEDED", element.getPrimitiveValue("state"));
                Assertions.assertEquals(200, element.getPrimitiveValue("statusCode"));
                if((Integer) element.getPrimitiveValue("attempts") > 1) {
                    retried++;
                }
            }
            Assertions.assertEquals(2, count);
            Assertions.assertEquals(1, retried);
            return null;
        });
        PAssert.that(outputs.get("bad").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("FAILED", element.getPrimitiveValue("state"));
                Assertions.assertEquals(400, element.getPrimitiveValue("statusCode"));
                Assertions.assertEquals(1, element.getPrimitiveValue("attempts"));
                Assertions.assertEquals(1L, element.getPrimitiveValue("failedCount"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("error")).contains("status 400"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("body")).contains("invalid"));
                Assertions.assertNull(element.getPrimitiveValue("payload"));
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(3, received("flaky").size());
        final List<Received> bad = received("bad");
        Assertions.assertEquals(2, bad.size());
        Assertions.assertEquals("PUT", bad.get(0).method());
        Assertions.assertTrue(bad.get(0).body().startsWith("{\"u\":\"u"));
    }

    @Test
    public void testSuccessConditionOnPayload() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs:
                      - input
                    failFast: false
                    parameters:
                      target:
                        url: %s
                      response:
                        success:
                          condition:
                            key: payload.ok
                            op: "="
                            value: true
                """.formatted(url("/conditional"));
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("http");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Integer> states = states(elements);
            Assertions.assertEquals(1, states.get("SUCCEEDED"));
            Assertions.assertEquals(1, states.get("FAILED"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testOauth2ClientCredentialsWithRefreshOn401() throws Exception {
        COUNTERS.remove("token");
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs:
                      - input
                    parameters:
                      target:
                        url: %s
                        auth:
                          type: oauth2
                          tokenUrl: %s
                          clientId: my-client
                          clientSecret: my-secret
                          scope: write
                      concurrency: 1
                """.formatted(url("/secure"), url("/oauth/token"));
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("http");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Integer> states = states(elements);
            Assertions.assertEquals(2, states.get("SUCCEEDED"));
            return null;
        });
        pipeline.run();

        // token-1 rejected once -> refreshed to token-2 -> both elements succeed; token fetched exactly twice
        Assertions.assertEquals(2, COUNTERS.get("token").get());
        final List<Received> secure = received("secure");
        Assertions.assertEquals(3, secure.size());
        final Received token = received("token").get(0);
        Assertions.assertTrue(token.body().contains("grant_type=client_credentials"));
        Assertions.assertTrue(token.body().contains("scope=write"));
    }

    @Test
    public void testBatchNdjsonPartialFailureWithItemRetry() throws Exception {
        final String configYaml = """
                sources:
                  - name: input
                    module: create
                    parameters:
                      type: element
                      elements:
                        - user_id: ok1
                          tenant: a
                        - user_id: fail1
                          tenant: a
                        - user_id: retry1
                          tenant: a
                        - user_id: ok2
                          tenant: a
                    schema:
                      fields:
                        - name: user_id
                          type: string
                        - name: tenant
                          type: string
                sinks:
                  - name: bulk
                    module: http
                    inputs:
                      - input
                    failFast: false
                    parameters:
                      target:
                        url: %s
                        headers:
                          X-Tenant: ${tenant}
                      body:
                        format: ndjson
                        fields: [user_id]
                      batch:
                        maxSize: 10
                        key: ${tenant}
                      response:
                        retry:
                          initialBackoff: 10ms
                          maxBackoff: 20ms
                          maxAttempts: 3
                        partialFailure:
                          itemsPath: /items
                          errorCondition:
                            key: index.error
                            op: "!="
                            value: null
                          retryCondition:
                            key: index.status
                            op: "="
                            value: 429
                """.formatted(url("/bulk"));
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("bulk");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("PARTIAL", element.getPrimitiveValue("state"));
                Assertions.assertEquals(4L, element.getPrimitiveValue("elementCount"));
                Assertions.assertEquals(1L, element.getPrimitiveValue("failedCount"));
                Assertions.assertEquals(2, element.getPrimitiveValue("attempts"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("error")).contains("mapping"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();

        final List<Received> bulk = received("bulk");
        Assertions.assertEquals(2, bulk.size());
        Assertions.assertEquals("a", bulk.get(0).headers().get("X-tenant").get(0));
        Assertions.assertEquals("application/x-ndjson", bulk.get(0).headers().get("Content-type").get(0));
        Assertions.assertEquals(4, bulk.get(0).body().strip().split("\n").length);
        // only the retryable item is re-sent
        Assertions.assertEquals("{\"user_id\":\"retry1\"}", bulk.get(1).body().strip());
    }

    @Test
    public void testBatchJsonArrayWithWrapperAndMaxBytesSplit() throws Exception {
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs:
                      - input
                    parameters:
                      target:
                        url: %s
                      body:
                        format: json
                        wrapper: '{"records": ${body}}'
                        maxBytes: 120
                      batch:
                        maxSize: 10
                """.formatted(url("/ok"));
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("http");
        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("SUCCEEDED", element.getPrimitiveValue("state"));
                Assertions.assertEquals(1L, element.getPrimitiveValue("elementCount"));
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        pipeline.run();

        final List<Received> ok = received("ok").stream().filter(r -> r.body().startsWith("{\"records\"")).toList();
        Assertions.assertEquals(2, ok.size());
        for(final Received r : ok) {
            final JsonArray records = JsonParser.parseString(r.body()).getAsJsonObject().getAsJsonArray("records");
            Assertions.assertEquals(1, records.size());
        }
    }

    @Test
    public void testNdjsonTemplateWithJsonFunctionAndBytesBody() throws Exception {
        final String configYaml = """
                sources:
                  - name: input
                    module: create
                    parameters:
                      type: element
                      elements:
                        - id: a
                          op: index
                          title: "Hello \\"World\\""
                          payload: "AQID"
                        - id: b
                          op: delete
                          title: null
                          payload: "BAUG"
                    schema:
                      fields:
                        - name: id
                          type: string
                        - name: op
                          type: string
                        - name: title
                          type: string
                        - name: payload
                          type: bytes
                sinks:
                  - name: bulk
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: %s
                      body:
                        format: ndjson
                        fields: [id, title]
                        template: |
                          <#if op == "delete">
                          {"delete":{"_id":"${id}"}}
                          <#else>
                          {"index":{"_id":"${id}"}}
                          ${utils.json.toJson(__doc)}
                          </#if>
                      batch:
                        maxSize: 10
                        shards: 1
                  - name: raw
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: %s
                        method: PUT
                        headers:
                          Content-Type: application/octet-stream
                      body:
                        format: bytes
                        field: payload
                """.formatted(url("/echo/bulk"), url("/echo/raw"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("bulk").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Map.of("SUCCEEDED", 1), states(elements));
            return null;
        });
        PAssert.that(outputs.get("raw").getCollection()).satisfies(elements -> {
            Assertions.assertEquals(Map.of("SUCCEEDED", 2), states(elements));
            return null;
        });
        pipeline.run();

        final List<Received> echo = received("echo");
        final Received bulk = echo.stream().filter(r -> r.path().equals("/echo/bulk")).findFirst().orElseThrow();
        final List<String> lines = Arrays.asList(bulk.body().split("\n"));
        Assertions.assertEquals(3, lines.size());
        // order within the batch is not guaranteed; check the set of lines
        Assertions.assertTrue(lines.contains("{\"delete\":{\"_id\":\"b\"}}"), lines.toString());
        Assertions.assertTrue(lines.contains("{\"index\":{\"_id\":\"a\"}}"), lines.toString());
        Assertions.assertTrue(lines.contains("{\"id\":\"a\",\"title\":\"Hello \\\"World\\\"\"}"), lines.toString());
        Assertions.assertTrue(bulk.body().endsWith("\n"));

        final List<Received> raw = echo.stream().filter(r -> r.path().equals("/echo/raw")).toList();
        Assertions.assertEquals(2, raw.size());
        Assertions.assertEquals("application/octet-stream", raw.get(0).headers().get("Content-type").get(0));
        final Set<String> bodies = new HashSet<>();
        for(final Received r : raw) {
            bodies.add(Base64.getEncoder().encodeToString(r.body().getBytes(StandardCharsets.ISO_8859_1)));
        }
        Assertions.assertEquals(Set.of("AQID", "BAUG"), bodies);
    }

    @Test
    public void testGzipSignatureJwtBearerAndMultipart() throws Exception {
        final java.nio.file.Path pem = java.nio.file.Files.createTempFile("http-sink-test", ".pem");
        java.nio.file.Files.writeString(pem, privateKeyPem());
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: gz
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: %s
                        headers:
                          X-Sig: ${utils.string.hmacSha256(__body, "secret")}
                      body:
                        format: json
                        compression: gzip
                  - name: jwt
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: %s
                        auth:
                          type: oauth2
                          grant: jwtBearer
                          tokenUrl: %s
                          issuer: svc@example.com
                          scope: write
                          privateKey: |
                            %s
                  - name: upload
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: %s
                      body:
                        format: multipart
                        parts:
                          - name: meta
                            template: '{"user": "${user_id}"}'
                            contentType: application/json
                          - name: file
                            field: note
                            filename: ${user_id}.txt
                            contentType: text/plain
                """.formatted(url("/gzip"), url("/secure-jwt"), url("/oauth/jwt"),
                privateKeyPem().replace("\n", "\n                            "), url("/echo/upload"));
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        for(final String name : List.of("gz", "jwt", "upload")) {
            PAssert.that(outputs.get(name).getCollection()).satisfies(elements -> {
                Assertions.assertEquals(Map.of("SUCCEEDED", 2), states(elements));
                return null;
            });
        }
        pipeline.run();

        // (1) signature header is computed over the uncompressed body
        final List<Received> gz = received("gzip");
        Assertions.assertEquals(2, gz.size());
        for(final Received r : gz) {
            Assertions.assertEquals("gzip", r.headers().get("Content-encoding").get(0));
            Assertions.assertTrue(r.body().startsWith("{\"app_id\""), r.body());
            Assertions.assertEquals(hmac(r.body(), "secret"), r.headers().get("X-sig").get(0));
        }
        // (2) jwt bearer: one assertion exchange, token cached for both elements
        Assertions.assertEquals(1, received("jwt").size());
        Assertions.assertTrue(received("jwt").get(0).body().contains("scope=write"));
        Assertions.assertEquals(2, received("secure-jwt").size());
        // (3) multipart
        final List<Received> up = received("echo").stream().filter(r -> r.path().equals("/echo/upload")).toList();
        Assertions.assertEquals(2, up.size());
        final Received u1 = up.stream().filter(r -> r.body().contains("\"user\": \"u1\"")).findFirst().orElseThrow();
        final String ct = u1.headers().get("Content-type").get(0);
        Assertions.assertTrue(ct.startsWith("multipart/form-data; boundary="), ct);
        final String boundary = ct.substring("multipart/form-data; boundary=".length());
        Assertions.assertTrue(u1.body().contains("--" + boundary + "\r\nContent-Disposition: form-data; name=\"meta\"\r\nContent-Type: application/json\r\n\r\n{\"user\": \"u1\"}\r\n"), u1.body());
        Assertions.assertTrue(u1.body().contains("Content-Disposition: form-data; name=\"file\"; filename=\"u1.txt\"\r\nContent-Type: text/plain\r\n\r\nhello\r\n"), u1.body());
        Assertions.assertTrue(u1.body().endsWith("--" + boundary + "--\r\n"));
        // u2 has note=null: the file part is skipped
        final Received u2 = up.stream().filter(r -> r.body().contains("\"user\": \"u2\"")).findFirst().orElseThrow();
        Assertions.assertFalse(u2.body().contains("name=\"file\""));
    }

    private static String hmac(final String text, final String secret) throws Exception {
        final javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        final StringBuilder sb = new StringBuilder();
        for(final byte b : mac.doFinal(text.getBytes(StandardCharsets.UTF_8))) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Test
    public void testValidate() {
        // auth with templated host requires allowedHosts
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: https://${app_id}.example.com/
                        auth:
                          type: bearer
                          token: abc
                """)));
        // auth values must not reference element fields
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: https://api.example.com/
                        auth:
                          type: bearer
                          token: ${user_id}
                """)));
        // batch: per-request templates limited to batch.key fields
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: https://api.example.com/${user_id}
                      batch:
                        maxSize: 10
                        key: ${app_id}
                """)));
        // partialFailure requires batch
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        url: https://api.example.com/
                      response:
                        partialFailure:
                          itemsPath: /items
                          errorCondition: { key: error, op: "!=", value: null }
                """)));
        // missing url
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: http
                    module: http
                    inputs: [input]
                    parameters:
                      target:
                        method: POST
                """)));
    }

    @Test
    public void testResponsePolicyHelpers() {
        Assertions.assertEquals("https://api.example.com", RequestSpec.staticOrigin("https://api.example.com/v1/${id}"));
        Assertions.assertEquals("https://api.example.com", RequestSpec.staticOrigin("https://api.example.com"));
        Assertions.assertNull(RequestSpec.staticOrigin("https://${tenant}.example.com/v1"));
        Assertions.assertNull(RequestSpec.staticOrigin("${url}"));

        final ResponsePolicy.Parameters p = new ResponsePolicy.Parameters();
        p.setDefaults();
        final ResponsePolicy policy = new ResponsePolicy(p);
        policy.setup();
        final OutboundRequest.Response r503 = new OutboundRequest.Response(503, Map.of("Retry-After", List.of("2")), "x".getBytes(), 1L);
        Assertions.assertEquals(ResponsePolicy.Verdict.RETRY, policy.classify(r503, policy.parse(r503)));
        Assertions.assertEquals(2000L, policy.backoff(1, r503, java.time.Instant.now()).toMillis());
        final OutboundRequest.Response r404 = new OutboundRequest.Response(404, Map.of(), null, 1L);
        Assertions.assertEquals(ResponsePolicy.Verdict.FAILED, policy.classify(r404, policy.parse(r404)));
        final OutboundRequest.Response r200 = new OutboundRequest.Response(200, Map.of(), "{\"a\":{\"b\":[1,2]}}".getBytes(), 1L);
        final ResponsePolicy.Parsed parsed = policy.parse(r200);
        Assertions.assertEquals(ResponsePolicy.Verdict.SUCCESS, policy.classify(r200, parsed));
        Assertions.assertEquals(2, ((List<?>) ((Map<?, ?>) ((Map<?, ?>) parsed.payload()).get("a")).get("b")).size());
        Assertions.assertNull(policy.backoff(5, r503, java.time.Instant.now()));
    }
}
