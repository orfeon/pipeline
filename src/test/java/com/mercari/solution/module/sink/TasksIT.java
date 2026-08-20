package com.mercari.solution.module.sink;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.sun.net.httpserver.HttpServer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=TasksIT})
 * for the tasks sink against the community Cloud Tasks emulator
 * (<a href="https://github.com/aertje/cloud-tasks-emulator">aertje/cloud-tasks-emulator</a>,
 * gRPC only) managed by Testcontainers.
 *
 * The emulator really dispatches HTTP tasks, so this test also starts a tiny HTTP server on the
 * host and lets the container reach it via Testcontainers' {@code host.testcontainers.internal}
 * alias; it then asserts that the rendered URL, headers and body arrived at the target.
 */
@org.testcontainers.junit.jupiter.Testcontainers
public class TasksIT {

    private static final String QUEUE = "projects/test-project/locations/us-central1/queues/it-queue";

    private static final ConcurrentLinkedQueue<Map<String, Object>> received = new ConcurrentLinkedQueue<>();
    // The target server must be up and its port exposed BEFORE the container starts
    // (host.testcontainers.internal is wired at container start), hence the static initializer.
    private static final HttpServer server = startTarget();
    private static final int serverPort = server.getAddress().getPort();
    static {
        Testcontainers.exposeHostPorts(serverPort);
    }

    @Container
    private static final GenericContainer<?> emulator = new GenericContainer<>(
            DockerImageName.parse("ghcr.io/aertje/cloud-tasks-emulator:latest"))
            .withCommand("-host", "0.0.0.0", "-port", "8123", "-queue", QUEUE)
            .withExposedPorts(8123)
            .withAccessToHost(true)
            .waitingFor(Wait.forListeningPort());

    private static HttpServer startTarget() {
        try {
            return startTargetServer();
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static HttpServer startTargetServer() throws Exception {
        final HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
        server.createContext("/", exchange -> {
            final Map<String, Object> req = new HashMap<>();
            req.put("method", exchange.getRequestMethod());
            req.put("path", exchange.getRequestURI().toString());
            final Map<String, String> headers = new HashMap<>();
            exchange.getRequestHeaders().forEach((k, v) -> headers.put(k.toLowerCase(), String.join(",", v)));
            req.put("headers", headers);
            try(final InputStream is = exchange.getRequestBody()) {
                req.put("body", new String(is.readAllBytes(), StandardCharsets.UTF_8));
            }
            received.add(req);
            final byte[] ok = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, ok.length);
            exchange.getResponseBody().write(ok);
            exchange.close();
        });
        server.start();
        return server;
    }

    @AfterAll
    static void stopTarget() {
        if(server != null) {
            server.stop(0);
        }
    }

    private static List<Map<String, Object>> hookRequests() {
        final List<Map<String, Object>> reqs = new ArrayList<>();
        for(final Map<String, Object> r : received) {
            if(((String) r.get("path")).startsWith("/hook/")) {
                reqs.add(r);
            }
        }
        return reqs;
    }

    private static String endpoint() {
        return emulator.getHost() + ":" + emulator.getMappedPort(8123);
    }

    @Test
    public void testCreateAndDispatch() throws Exception {
        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final String configYaml = """
                sources:
                  - name: input
                    module: create
                    parameters:
                      type: element
                      elements:
                        - id: a
                          value: 1
                        - id: b
                          value: 2
                        - id: a
                          value: 3
                    schema:
                      fields:
                        - name: id
                          type: string
                        - name: value
                          type: int64
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      endpoint: %s
                      target:
                        url: http://host.testcontainers.internal:%d/hook/${id}
                        headers:
                          Content-Type: application/json
                          X-Id: ${id}
                      body:
                        format: json
                      task:
                        id: "${id}"
                        hashId: false
                """.formatted(QUEUE, endpoint(), serverPort);

        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Integer> states = new HashMap<>();
            for(final MElement element : elements) {
                states.merge((String) element.getPrimitiveValue("state"), 1, Integer::sum);
                Assertions.assertEquals(QUEUE, element.getPrimitiveValue("queue"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("taskName")).startsWith(QUEUE + "/tasks/"));
            }
            // three elements, two distinct names -> 2 created + 1 already exists
            Assertions.assertEquals(2, states.get("CREATED"));
            Assertions.assertEquals(1, states.get("ALREADY_EXISTS"));
            return null;
        });
        pipeline.run();

        // The emulator dispatches asynchronously; wait for both tasks to reach the target.
        final Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while(hookRequests().size() < 2 && Instant.now().isBefore(deadline)) {
            Thread.sleep(200);
        }
        final List<Map<String, Object>> reqs = hookRequests();
        Assertions.assertEquals(2, reqs.size(), "dispatched requests: " + reqs + "\nemulator logs:\n" + emulator.getLogs());
        reqs.sort(Comparator.comparing(r -> (String) r.get("path")));

        final Map<String, Object> ra = reqs.get(0);
        Assertions.assertEquals("POST", ra.get("method"));
        Assertions.assertEquals("/hook/a", ra.get("path"));
        @SuppressWarnings("unchecked")
        final Map<String, String> headers = (Map<String, String>) ra.get("headers");
        Assertions.assertEquals("a", headers.get("x-id"));
        Assertions.assertEquals("application/json", headers.get("content-type"));
        final JsonObject body = JsonParser.parseString((String) ra.get("body")).getAsJsonObject();
        Assertions.assertEquals("a", body.get("id").getAsString());
        Assertions.assertEquals(1, body.get("value").getAsLong()); // first 'a' won the name

        Assertions.assertEquals("/hook/b", reqs.get(1).get("path"));
    }

    @Test
    public void testActionCreateQueueEnqueueAndWaitForEmpty() throws Exception {
        // action.tasks create -> sink (waits on create) -> action.tasks waitForEmpty (inputs = sink records)
        final String queue2 = "projects/test-project/locations/us-central1/queues/it-queue-2";
        final TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final String configYaml = """
                sources:
                  - name: create
                    module: action.tasks
                    parameters:
                      op: create
                      queue: %1$s
                      endpoint: %2$s
                      rateLimits:
                        maxConcurrentDispatches: 2
                  - name: input
                    module: create
                    parameters:
                      type: element
                      elements:
                        - id: x
                        - id: y
                    schema:
                      fields:
                        - name: id
                          type: string
                sinks:
                  - name: enqueue
                    module: tasks
                    inputs: [input]
                    waits: [create]
                    parameters:
                      queue: %1$s
                      endpoint: %2$s
                      concurrency: 2
                      target:
                        url: http://host.testcontainers.internal:%3$d/wait/${id}
                  - name: drained
                    module: action.tasks
                    inputs: [enqueue]
                    parameters:
                      op: waitForEmpty
                      queue: %1$s
                      endpoint: %2$s
                      pollIntervalSeconds: 1
                      timeoutSeconds: 60
                """.formatted(queue2, endpoint(), serverPort);

        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("create", e.getPrimitiveValue("op"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
            }
            return null;
        });
        PAssert.that(outputs.get("enqueue").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("CREATED", e.getPrimitiveValue("state"));
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        PAssert.that(outputs.get("drained").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("waitForEmpty", e.getPrimitiveValue("op"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();

        // by the time waitForEmpty returned, both tasks had been dispatched to the target
        final Set<String> paths = new TreeSet<>();
        for(final Map<String, Object> r : received) {
            if(((String) r.get("path")).startsWith("/wait/")) {
                paths.add((String) r.get("path"));
            }
        }
        Assertions.assertEquals(Set.of("/wait/x", "/wait/y"), paths, "emulator logs:\n" + emulator.getLogs());
    }

}
