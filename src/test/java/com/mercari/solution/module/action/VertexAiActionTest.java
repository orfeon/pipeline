package com.mercari.solution.module.action;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.cloud.google.vertexai.VertexAiUtil;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class VertexAiActionTest {

    @BeforeAll
    static void noSleep() {
        // the poll backoff (2s -> 30s) is not what these tests exercise
        VertexAiAction.testSleeper = millis -> {};
    }

    private static final String SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - date: "2026-08-01"
                    - date: "2026-08-02"
                schema:
                  fields:
                    - name: date
                      type: string
            """;

    /** In-memory Vertex AI: jobs advance one state per get poll along a scripted path. */
    static class MemoryVertexAiClient implements VertexAiAction.VertexAiClient {

        final Map<String, JsonObject> jobs = new ConcurrentHashMap<>();
        final Map<String, Deque<String>> transitions = new ConcurrentHashMap<>();
        final Map<String, JsonObject> requests = new ConcurrentHashMap<>();
        final List<JsonObject> generateRequests = Collections.synchronizedList(new ArrayList<>());
        final ConcurrentLinkedQueue<String> ops = new ConcurrentLinkedQueue<>();
        final AtomicInteger counter = new AtomicInteger(0);
        List<String> path = List.of("JOB_STATE_PENDING", "JOB_STATE_RUNNING", "JOB_STATE_SUCCEEDED");
        String failedCount = "0";
        String errorMessage = "input rows were rejected";
        /** generateContent response template (candidate text); {@code finishReason} / {@code blockReason} scripted. */
        String responseText = "hello";
        String finishReason = "STOP";
        String blockReason = null;
        final AtomicInteger transientGets = new AtomicInteger(0);
        final AtomicInteger emptyLists = new AtomicInteger(0);

        JsonObject put(final String id, final String state, final String displayName, final long createSeconds) {
            final JsonObject job = new JsonObject();
            job.addProperty("name", "projects/myproject/locations/us-central1/batchPredictionJobs/" + id);
            job.addProperty("displayName", displayName);
            job.addProperty("state", state);
            job.addProperty("createTime", "2026-08-28T00:00:" + String.format("%02d", createSeconds % 60) + "Z");
            jobs.put(id, job);
            return job;
        }

        @Override
        public JsonObject generateContent(final String project, final String location, final String model, final JsonObject request) {
            ops.add("generate:" + location + ":" + model);
            generateRequests.add(request.deepCopy());
            final JsonObject response = new JsonObject();
            response.addProperty("responseId", "resp-" + counter.incrementAndGet());
            response.addProperty("modelVersion", model);
            if(blockReason != null) {
                final JsonObject feedback = new JsonObject();
                feedback.addProperty("blockReason", blockReason);
                response.add("promptFeedback", feedback);
                return response;
            }
            final JsonObject thought = new JsonObject();
            thought.addProperty("thought", true);
            thought.addProperty("text", "thinking...");
            final JsonObject part = new JsonObject();
            part.addProperty("text", responseText);
            final JsonArray parts = new JsonArray();
            parts.add(thought);
            parts.add(part);
            final JsonObject content = new JsonObject();
            content.addProperty("role", "model");
            content.add("parts", parts);
            final JsonObject candidate = new JsonObject();
            candidate.add("content", content);
            candidate.addProperty("finishReason", finishReason);
            final JsonArray candidates = new JsonArray();
            candidates.add(candidate);
            response.add("candidates", candidates);
            final JsonObject usage = new JsonObject();
            usage.addProperty("promptTokenCount", 12);
            usage.addProperty("candidatesTokenCount", 3);
            usage.addProperty("totalTokenCount", 15);
            response.add("usageMetadata", usage);
            return response;
        }

        @Override
        public JsonObject createBatchPredictionJob(final String project, final String location, final JsonObject job) {
            ops.add("create");
            final String id = String.valueOf(1000 + counter.incrementAndGet());
            requests.put(id, job.deepCopy());
            final JsonObject created = put(id, path.getFirst(), job.get("displayName").getAsString(), counter.get());
            created.addProperty("model", job.get("model").getAsString());
            transitions.put(id, new ArrayDeque<>(path.subList(1, path.size())));
            return created.deepCopy();
        }

        @Override
        public JsonObject getBatchPredictionJob(final String project, final String location, final String jobId) {
            ops.add("get:" + jobId);
            if(transientGets.getAndDecrement() > 0) {
                throw new VertexAiUtil.VertexAiException(503, "{}", "unavailable");
            }
            final JsonObject job = jobs.get(jobId);
            if(job == null) {
                throw new VertexAiUtil.VertexAiException(404, "{}", "not found: " + jobId);
            }
            final Deque<String> path = transitions.get(jobId);
            if(path != null && !path.isEmpty()) {
                final String next = path.poll();
                job.addProperty("state", next);
                if(VertexAiUtil.isTerminal(next)) {
                    final JsonObject stats = new JsonObject();
                    stats.addProperty("successfulCount", "98");
                    stats.addProperty("failedCount", failedCount);
                    stats.addProperty("incompleteCount", "0");
                    job.add("completionStats", stats);
                    final JsonObject outputInfo = new JsonObject();
                    outputInfo.addProperty("bigqueryOutputTable", "bq://myproject.llm.out_" + jobId);
                    job.add("outputInfo", outputInfo);
                    if("JOB_STATE_FAILED".equals(next) || "JOB_STATE_EXPIRED".equals(next)) {
                        final JsonObject error = new JsonObject();
                        error.addProperty("code", 3);
                        error.addProperty("message", errorMessage);
                        job.add("error", error);
                    }
                }
            }
            return job.deepCopy();
        }

        @Override
        public List<JsonObject> listBatchPredictionJobs(final String project, final String location, final String filter, final int limit) {
            ops.add("list:" + filter);
            if(emptyLists.getAndDecrement() > 0) {
                return List.of();
            }
            return jobs.values().stream()
                    .filter(j -> matches(j, filter))
                    .sorted(Comparator.comparing((JsonObject j) -> j.get("createTime").getAsString()).reversed())
                    .limit(limit)
                    .map(JsonObject::deepCopy)
                    .toList();
        }

        private static boolean matches(final JsonObject job, final String filter) {
            if(filter == null || filter.isBlank()) {
                return true;
            }
            for(final String term : filter.split(" AND ")) {
                final String[] kv = term.trim().split("=", 2);
                final String value = kv[1].replace("\"", "");
                switch (kv[0].trim()) {
                    case "display_name" -> {
                        if(!value.equals(job.get("displayName").getAsString())) return false;
                    }
                    case "state" -> {
                        if(!value.equals(job.get("state").getAsString())) return false;
                    }
                    default -> throw new IllegalArgumentException("unsupported filter: " + term);
                }
            }
            return true;
        }

        @Override
        public JsonObject cancelBatchPredictionJob(final String project, final String location, final String jobId) {
            ops.add("cancel:" + jobId);
            if(!jobs.containsKey(jobId)) {
                throw new VertexAiUtil.VertexAiException(404, "{}", "not found: " + jobId);
            }
            transitions.put(jobId, new ArrayDeque<>(List.of("JOB_STATE_CANCELLING", "JOB_STATE_CANCELLED")));
            return new JsonObject();
        }
    }

    private static MemoryVertexAiClient register(final String name) {
        final MemoryVertexAiClient client = new MemoryVertexAiClient();
        VertexAiAction.registerMemoryClient(name, client);
        return client;
    }

    private static MCollection run(final TestPipeline pipeline, final String yaml, final String step) throws Exception {
        return MPipeline.apply(pipeline, Config.load(yaml)).get(step);
    }

    private static JsonObject payload(final MElement e) {
        return JsonParser.parseString((String) e.getPrimitiveValue("payload")).getAsJsonObject();
    }

    private static String messages(final Throwable e) {
        final StringBuilder message = new StringBuilder();
        for(Throwable t = e; t != null; t = t.getCause()) {
            message.append(t.getMessage()).append(" ");
        }
        return message.toString();
    }

    @Test
    public void testCreateWaitsThenAdoptsExisting() throws Exception {
        final MemoryVertexAiClient client = register("create");
        final String yaml = """
                actions:
                  - name: create
                    module: vertexai
                    operation: batchPredictionJobs.create
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://create
                      displayName: classify-r1
                      model: gemini-2.5-flash
                      inputConfig:
                        bigquerySource:
                          inputUri: bq://myproject.llm.requests_r1
                      outputConfig:
                        bigqueryDestination:
                          outputUri: bq://myproject.llm.responses_r1
                      labels:
                        run: r1
                """;
        client.transientGets.set(1);
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p1, yaml, "create").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("vertexai", e.getPrimitiveValue("service"));
                Assertions.assertEquals("batchPredictionJobs.create", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("1001", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_SUCCEEDED", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals("JOB_STATE_SUCCEEDED", payload.get("state").getAsString());
                Assertions.assertEquals("bq://myproject.llm.out_1001", payload.getAsJsonObject("outputInfo").get("bigqueryOutputTable").getAsString());
                Assertions.assertEquals("0", payload.getAsJsonObject("completionStats").get("failedCount").getAsString());
                Assertions.assertFalse(payload.has("adopted"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p1.run();
        final JsonObject request = client.requests.get("1001");
        Assertions.assertEquals("publishers/google/models/gemini-2.5-flash", request.get("model").getAsString());
        Assertions.assertEquals("classify-r1", request.get("displayName").getAsString());
        // formats defaulted from the chosen source / destination
        Assertions.assertEquals("bigquery", request.getAsJsonObject("inputConfig").get("instancesFormat").getAsString());
        Assertions.assertEquals("bigquery", request.getAsJsonObject("outputConfig").get("predictionsFormat").getAsString());
        Assertions.assertEquals("r1", request.getAsJsonObject("labels").get("run").getAsString());
        // dedupe lookup before create, a transient 503 retried inside the wait: exactly one job submitted
        Assertions.assertEquals("list:display_name=\"classify-r1\"", client.ops.stream().filter(o -> o.startsWith("list")).findFirst().orElse(null));
        Assertions.assertEquals(1, client.ops.stream().filter(o -> o.equals("create")).count(), client.ops.toString());
        Assertions.assertTrue(client.ops.stream().filter(o -> o.equals("get:1001")).count() >= 2);

        // second run with the same displayName: the succeeded job is adopted
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, yaml, "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("1001", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_SUCCEEDED", e.getPrimitiveValue("state"));
                Assertions.assertTrue(payload(e).get("adopted").getAsBoolean());
            }
            return null;
        });
        p2.run();
        Assertions.assertEquals(1, client.requests.size());

        // a running job with the name, wait: false -> EXISTS
        client.put("9", "JOB_STATE_RUNNING", "classify-r1", 50);
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, yaml + "      wait: false\n", "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("9", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("EXISTS", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p3.run();

        // the newest same-named job failed -> not adopted, a new job is submitted (reported without waiting)
        client.put("19", "JOB_STATE_FAILED", "classify-r1", 55);
        final TestPipeline p4 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p4, yaml + "      wait: false\n", "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("1002", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_PENDING", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p4.run();

        // gcsSource.uris given as a single string becomes a list; jsonl formats defaulted
        final TestPipeline p5 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p5, """
                actions:
                  - name: create
                    module: vertexai
                    operation: batchPredictionJobs.create
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://create
                      displayName: gcs-r1
                      reuseExisting: false
                      wait: false
                      model: publishers/google/models/gemini-2.5-pro
                      inputConfig:
                        gcsSource:
                          uris: gs://my-bucket/requests/*.jsonl
                      outputConfig:
                        gcsDestination:
                          outputUriPrefix: gs://my-bucket/responses/
                """, "create");
        p5.run();
        final JsonObject gcsRequest = client.requests.get("1003");
        Assertions.assertEquals("publishers/google/models/gemini-2.5-pro", gcsRequest.get("model").getAsString());
        Assertions.assertEquals("jsonl", gcsRequest.getAsJsonObject("inputConfig").get("instancesFormat").getAsString());
        Assertions.assertEquals("gs://my-bucket/requests/*.jsonl", gcsRequest.getAsJsonObject("inputConfig").getAsJsonObject("gcsSource").getAsJsonArray("uris").get(0).getAsString());
        Assertions.assertEquals("jsonl", gcsRequest.getAsJsonObject("outputConfig").get("predictionsFormat").getAsString());
        Assertions.assertEquals(0, client.ops.stream().filter(o -> o.equals("list:display_name=\"gcs-r1\"")).count(), "reuseExisting: false must not list");
        VertexAiAction.unregisterMemoryClient("create");
    }

    @Test
    public void testFailedJobIsNonRetryableAndPartialIsConfigurable() throws Exception {
        final MemoryVertexAiClient client = register("failed");
        client.path = List.of("JOB_STATE_PENDING", "JOB_STATE_RUNNING", "JOB_STATE_FAILED");
        client.errorMessage = "boom";
        final String yaml = """
                actions:
                  - name: create
                    module: vertexai
                    operation: batchPredictionJobs.create
                    failFast: true
                    retry: { maxAttempts: 3, initialBackoff: 10ms }
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://failed
                      model: gemini-2.5-flash
                      inputConfig: { gcsSource: { uris: [gs://b/in.jsonl] } }
                      outputConfig: { gcsDestination: { outputUriPrefix: gs://b/out/ } }
                      %s
                """;
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p, yaml.formatted("displayName: x-r1"), "create");
        final String message = messages(Assertions.assertThrows(Exception.class, p::run));
        Assertions.assertTrue(message.contains("boom"), message);
        Assertions.assertTrue(message.contains("JOB_STATE_FAILED"), message);
        Assertions.assertEquals(1, client.ops.stream().filter(o -> o.equals("create")).count(), client.ops.toString());

        // PARTIALLY_SUCCEEDED is a normal completion by default (completionStats in the payload for failWhen)
        client.path = List.of("JOB_STATE_PENDING", "JOB_STATE_PARTIALLY_SUCCEEDED");
        client.failedCount = "2";
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, yaml.formatted("displayName: x-r2"), "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("JOB_STATE_PARTIALLY_SUCCEEDED", e.getPrimitiveValue("state"));
                Assertions.assertEquals("2", payload(e).getAsJsonObject("completionStats").get("failedCount").getAsString());
            }
            return null;
        });
        p2.run();

        // ... unless failOnPartial
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p3, yaml.formatted("displayName: x-r3\n      failOnPartial: true"), "create");
        Assertions.assertTrue(messages(Assertions.assertThrows(Exception.class, p3::run)).contains("JOB_STATE_PARTIALLY_SUCCEEDED"));

        // a re-run with the same displayName does not adopt the partially succeeded job: a new one is submitted
        final int submitted = client.requests.size();
        client.path = List.of("JOB_STATE_PENDING", "JOB_STATE_RUNNING", "JOB_STATE_SUCCEEDED");
        client.failedCount = "0";
        final TestPipeline p3b = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3b, yaml.formatted("displayName: x-r3\n      failOnPartial: true"), "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("JOB_STATE_SUCCEEDED", e.getPrimitiveValue("state"));
                Assertions.assertFalse(payload(e).has("adopted"));
            }
            return null;
        });
        p3b.run();
        Assertions.assertEquals(submitted + 1, client.requests.size());

        // a job that produced no displayName still runs (not idempotent, WARN)
        client.path = List.of("JOB_STATE_SUCCEEDED");
        final TestPipeline p4 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p4, yaml.formatted("wait: false"), "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertTrue(payload(e).get("displayName").getAsString().startsWith("create-"));
            }
            return null;
        });
        p4.run();
        VertexAiAction.unregisterMemoryClient("failed");
    }

    @Test
    public void testRejectedRequestIsNotRetried() throws Exception {
        final MemoryVertexAiClient client = register("rejected");
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p, """
                actions:
                  - name: get
                    module: vertexai
                    operation: batchPredictionJobs.get
                    failFast: true
                    retry: { maxAttempts: 3, initialBackoff: 10ms }
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://rejected, jobId: missing }
                """, "get");
        Assertions.assertThrows(Exception.class, p::run);
        Assertions.assertEquals(1, client.ops.stream().filter(o -> o.equals("get:missing")).count(), client.ops.toString());
        VertexAiAction.unregisterMemoryClient("rejected");
    }

    @Test
    public void testPerElementCreateNoWaitThenCollectWait() throws Exception {
        final MemoryVertexAiClient client = register("fanout");
        final String yaml = SOURCE_YAML + """
                actions:
                  - name: create
                    module: vertexai
                    operation: batchPredictionJobs.create
                    trigger: perElement
                    inputs: [input]
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://fanout
                      displayName: "classify-${date}"
                      model: gemini-2.5-flash
                      inputConfig:
                        bigquerySource: { inputUri: "bq://myproject.llm.requests_${date}" }
                      outputConfig:
                        bigqueryDestination: { outputUri: "bq://myproject.llm.responses_${date}" }
                      wait: false
                  - name: wait
                    module: vertexai
                    operation: batchPredictionJobs.wait
                    trigger: collect
                    inputs: [create]
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://fanout
                      jobIdField: jobId
                """;
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(p, Config.load(yaml));
        PAssert.that(outputs.get("create").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("JOB_STATE_PENDING", e.getPrimitiveValue("state"), "wait: false must not wait");
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        PAssert.that(outputs.get("wait").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("batchPredictionJobs.wait", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals(2, payload.get("count").getAsInt());
                Assertions.assertEquals("JOB_STATE_SUCCEEDED", payload.getAsJsonObject("firstJob").get("state").getAsString());
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p.run();
        Assertions.assertEquals(2, client.requests.size());
        final Set<String> seen = new HashSet<>();
        client.requests.values().forEach(r -> seen.add(r.get("displayName").getAsString() + "=" + r.getAsJsonObject("inputConfig").getAsJsonObject("bigquerySource").get("inputUri").getAsString()));
        Assertions.assertEquals(Set.of("classify-2026-08-01=bq://myproject.llm.requests_2026-08-01", "classify-2026-08-02=bq://myproject.llm.requests_2026-08-02"), seen);
        VertexAiAction.unregisterMemoryClient("fanout");
    }

    @Test
    public void testListWaitByFilterAndCancel() throws Exception {
        final MemoryVertexAiClient client = register("ops");
        client.put("1", "JOB_STATE_FAILED", "nightly", 10);
        client.put("2", "JOB_STATE_SUCCEEDED", "nightly", 20);
        client.put("3", "JOB_STATE_RUNNING", "other", 30);

        // batchPredictionJobs.list with a filter + failWhen on count
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p1, """
                actions:
                  - name: list
                    module: vertexai
                    operation: batchPredictionJobs.list
                    failWhen: payload.`count` = 0
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://ops
                      filter: display_name="nightly" AND state="JOB_STATE_SUCCEEDED"
                """, "list").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                Assertions.assertEquals("2", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals(1, payload(e).get("count").getAsInt());
            }
            return null;
        });
        p1.run();

        // wait by filter: polls until a matching job exists, then waits for it until terminal
        client.transitions.put("3", new ArrayDeque<>(List.of("JOB_STATE_RUNNING", "JOB_STATE_SUCCEEDED")));
        client.emptyLists.set(2);
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, """
                actions:
                  - name: wait
                    module: vertexai
                    operation: batchPredictionJobs.wait
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://ops
                      filter: display_name="other"
                """, "wait").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("3", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_SUCCEEDED", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p2.run();
        Assertions.assertTrue(client.ops.stream().filter(o -> o.equals("list:display_name=\"other\"")).count() >= 3, client.ops.toString());

        // wait by filter with waitUntil: none reports the newest match without polling, fails when nothing matches
        final TestPipeline p2c = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2c, """
                actions:
                  - name: wait
                    module: vertexai
                    operation: batchPredictionJobs.wait
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://ops, filter: 'display_name="nightly"', waitUntil: none }
                """, "wait").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("2", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_SUCCEEDED", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p2c.run();
        final int listsBefore = (int) client.ops.stream().filter(o -> o.startsWith("list:display_name=\"nothing\"")).count();
        final TestPipeline p2d = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p2d, """
                actions:
                  - name: wait
                    module: vertexai
                    operation: batchPredictionJobs.wait
                    failFast: true
                    retry: { maxAttempts: 3, initialBackoff: 10ms }
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://ops, filter: 'display_name="nothing"', waitUntil: none }
                """, "wait");
        Assertions.assertTrue(messages(Assertions.assertThrows(Exception.class, p2d::run)).contains("no vertex ai batch prediction job matches"));
        Assertions.assertEquals(listsBefore + 1, client.ops.stream().filter(o -> o.startsWith("list:display_name=\"nothing\"")).count(), "waitUntil none must not poll nor retry");

        // a list envelope's comma-joined jobId feeds a collected wait: each id is polled
        final TestPipeline p2e = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> chained = MPipeline.apply(p2e, Config.load("""
                actions:
                  - name: list
                    module: vertexai
                    operation: batchPredictionJobs.list
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://ops, filter: 'display_name="nightly"' }
                  - name: wait
                    module: vertexai
                    operation: batchPredictionJobs.wait
                    trigger: collect
                    inputs: [list]
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://ops, jobIdField: jobId, waitUntil: none }
                """));
        PAssert.that(chained.get("wait").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("2,1", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals(2, payload(e).get("count").getAsInt());
            }
            return null;
        });
        p2e.run();

        // waitUntil: running returns as soon as the job runs
        client.put("4", "JOB_STATE_PENDING", "r", 41);
        client.transitions.put("4", new ArrayDeque<>(List.of("JOB_STATE_RUNNING", "JOB_STATE_SUCCEEDED")));
        final TestPipeline p2b = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2b, """
                actions:
                  - name: wait
                    module: vertexai
                    operation: batchPredictionJobs.wait
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://ops, jobId: "4", waitUntil: running }
                """, "wait").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("JOB_STATE_RUNNING", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p2b.run();

        // cancel waits until CANCELLED (not a failure when we asked for it)
        client.put("5", "JOB_STATE_RUNNING", "c", 42);
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, """
                actions:
                  - name: cancel
                    module: vertexai
                    operation: batchPredictionJobs.cancel
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://ops, jobId: "5" }
                """, "cancel").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("JOB_STATE_CANCELLED", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p3.run();
        Assertions.assertTrue(client.ops.contains("cancel:5"), client.ops.toString());

        // a job cancelled by someone else while waited for -> failure
        client.put("6", "JOB_STATE_RUNNING", "k", 45);
        client.transitions.put("6", new ArrayDeque<>(List.of("JOB_STATE_CANCELLED")));
        final TestPipeline p3b = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p3b, """
                actions:
                  - name: wait
                    module: vertexai
                    operation: batchPredictionJobs.wait
                    failFast: true
                    parameters: { projectId: myproject, location: us-central1, endpoint: memory://ops, jobId: "6" }
                """, "wait");
        Assertions.assertTrue(messages(Assertions.assertThrows(Exception.class, p3b::run)).contains("cancelled"));
        VertexAiAction.unregisterMemoryClient("ops");
    }

    @Test
    public void testGenerateContentPromptShorthandAndCollect() throws Exception {
        final MemoryVertexAiClient client = register("gen");
        client.responseText = "{\"severity\": \"high\", \"summary\": \"2 dates failed\"}";
        final String yaml = SOURCE_YAML + """
                actions:
                  - name: triage
                    module: vertexai
                    operation: models.generateContent
                    trigger: collect
                    inputs: [input]
                    failWhen: payload.`json`.`severity` = 'critical'
                    parameters:
                      projectId: myproject
                      endpoint: memory://gen
                      model: gemini-2.5-flash
                      system: "You are an operator."
                      prompt: "Summarize ${size} runs: <#list elements as e>${e.date} </#list>"
                      responseSchema:
                        type: OBJECT
                        properties:
                          severity: { type: STRING, enum: [low, high, critical] }
                          summary: { type: STRING }
                      generationConfig:
                        temperature: 0
                      labels:
                        step: triage
                """;
        final String expectedText = client.responseText;
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p, yaml, "triage").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("models.generateContent", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("resp-1", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("STOP", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals("high", payload.getAsJsonObject("json").get("severity").getAsString());
                Assertions.assertEquals(expectedText, payload.get("text").getAsString());
                Assertions.assertEquals(15, payload.getAsJsonObject("usageMetadata").get("totalTokenCount").getAsInt());
                Assertions.assertEquals(1, payload.getAsJsonArray("candidates").size());
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p.run();
        // global endpoint by default for models.*
        Assertions.assertTrue(client.ops.contains("generate:global:gemini-2.5-flash"), client.ops.toString());
        final JsonObject request = client.generateRequests.getFirst();
        final String promptText = request.getAsJsonArray("contents").get(0).getAsJsonObject().getAsJsonArray("parts").get(0).getAsJsonObject().get("text").getAsString();
        Assertions.assertTrue(promptText.startsWith("Summarize 2 runs: "), promptText);
        Assertions.assertTrue(promptText.contains("2026-08-01") && promptText.contains("2026-08-02"), promptText);
        Assertions.assertEquals("user", request.getAsJsonArray("contents").get(0).getAsJsonObject().get("role").getAsString());
        Assertions.assertEquals("You are an operator.", request.getAsJsonObject("systemInstruction").getAsJsonArray("parts").get(0).getAsJsonObject().get("text").getAsString());
        final JsonObject generationConfig = request.getAsJsonObject("generationConfig");
        Assertions.assertEquals("application/json", generationConfig.get("responseMimeType").getAsString());
        Assertions.assertEquals("OBJECT", generationConfig.getAsJsonObject("responseSchema").get("type").getAsString());
        Assertions.assertEquals(0, generationConfig.get("temperature").getAsInt());
        Assertions.assertEquals("triage", request.getAsJsonObject("labels").get("step").getAsString());

        // failWhen on the parsed json
        client.responseText = "{\"severity\": \"critical\", \"summary\": \"x\"}";
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p2, yaml.replace("trigger: collect\n", "trigger: collect\n    failFast: true\n"), "triage");
        Assertions.assertTrue(messages(Assertions.assertThrows(Exception.class, p2::run)).contains("failWhen"));

        // a blocked prompt is reported as state BLOCKED (no text) rather than an error
        client.blockReason = "PROHIBITED_CONTENT";
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, """
                actions:
                  - name: ask
                    module: vertexai
                    operation: models.generateContent
                    parameters:
                      projectId: myproject
                      location: us-central1
                      endpoint: memory://gen
                      model: projects/myproject/locations/us-central1/endpoints/123
                      contents:
                        - role: user
                          parts: [{ text: "hi" }]
                """, "ask").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("BLOCKED", e.getPrimitiveValue("state"));
                Assertions.assertFalse(payload(e).has("text"));
                Assertions.assertEquals("PROHIBITED_CONTENT", payload(e).getAsJsonObject("promptFeedback").get("blockReason").getAsString());
            }
            return null;
        });
        p3.run();
        Assertions.assertTrue(client.ops.contains("generate:us-central1:projects/myproject/locations/us-central1/endpoints/123"), client.ops.toString());
        VertexAiAction.unregisterMemoryClient("gen");
    }

    @Test
    public void testValidation() {
        final String base = """
                actions:
                  - name: step
                    module: vertexai
                    operation: %s
                    parameters:
                      projectId: myproject
                      %s
                """;
        final String io = "inputConfig: { gcsSource: { uris: [gs://b/in.jsonl] } }\n      outputConfig: { gcsDestination: { outputUriPrefix: gs://b/out/ } }";
        assertInvalid(base.formatted("batchPredictionJobs.create", "model: gemini-2.5-flash\n      " + io), "location is required");
        assertInvalid(base.formatted("batchPredictionJobs.create", "location: global\n      model: gemini-2.5-flash\n      " + io), "location must be a regional location");
        assertInvalid(base.formatted("batchPredictionJobs.create", "location: us-central1\n      " + io), "model is required");
        assertInvalid(base.formatted("batchPredictionJobs.create", "location: us-central1\n      model: projects/p/locations/us-central1/endpoints/1\n      " + io), "not an endpoint");
        assertInvalid(base.formatted("batchPredictionJobs.wait", "location: us-central1\n      jobId: '1'\n      waitUntil: RUNNING"), "waitUntil must be one of");
        assertInvalid(base.formatted("batchPredictionJobs.create", "location: us-central1\n      model: m\n      outputConfig: { gcsDestination: { outputUriPrefix: gs://b/out/ } }"), "inputConfig is required");
        assertInvalid(base.formatted("batchPredictionJobs.create", "location: us-central1\n      model: m\n      inputConfig: { instancesFormat: jsonl }\n      outputConfig: { gcsDestination: { outputUriPrefix: gs://b/out/ } }"), "inputConfig requires gcsSource or bigquerySource");
        assertInvalid(base.formatted("batchPredictionJobs.create", "location: us-central1\n      model: m\n      inputConfig: { gcsSource: { uris: [gs://b/in.jsonl] } }"), "outputConfig is required");
        assertInvalid(base.formatted("batchPredictionJobs.get", "location: us-central1\n      filter: x"), "jobId is required");
        assertInvalid(base.formatted("batchPredictionJobs.wait", "location: us-central1\n      jobIdField: jobId"), "jobIdField requires trigger: collect");
        assertInvalid(base.formatted("batchPredictionJobs.wait", "location: us-central1"), "jobId, jobIdField or filter is required");
        assertInvalid(base.formatted("batchPredictionJobs.list", "location: us-central1\n      pageSize: 0"), "pageSize must be positive");
        assertInvalid(base.formatted("models.generateContent", "prompt: hi"), "model is required");
        assertInvalid(base.formatted("models.generateContent", "model: m"), "prompt or contents is required");
        assertInvalid(base.formatted("models.generateContent", "model: m\n      prompt: hi\n      contents: [{ role: user, parts: [{ text: hi }] }]"), "prompt and contents are exclusive");
        assertInvalid(base.formatted("models.generateContent", "model: m\n      prompt: hi\n      system: s\n      systemInstruction: { parts: [{ text: s }] }"), "system and systemInstruction are exclusive");
        assertInvalid(base.formatted("models.generateContent", "model: m\n      prompt: hi\n      responseSchema: { type: OBJECT }\n      generationConfig: { responseSchema: { type: OBJECT } }"), "responseSchema and generationConfig.responseSchema are exclusive");
        assertInvalid(base.formatted("models.generateContent", "model: m\n      prompt: hi\n      timeoutSeconds: 0"), "timeoutSeconds must be positive");
        // no projectId anywhere: pin the pipeline project to blank so the gcloud SDK default cannot supply one
        final PipelineOptions options = TestPipeline.testingPipelineOptions();
        options.as(GcpOptions.class).setProject("");
        final TestPipeline p = TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> run(p, """
                actions:
                  - name: step
                    module: vertexai
                    operation: models.generateContent
                    parameters:
                      model: m
                      prompt: hi
                """, "step"));
        Assertions.assertTrue(e.getMessage().contains("projectId is required"), e.getMessage());
    }

    private static void assertInvalid(final String yaml, final String expected) {
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> run(p, yaml, "step"));
        Assertions.assertTrue(e.getMessage().contains(expected), e.getMessage());
    }

    @Test
    public void testCredentialsFailureIsNotATransportError() {
        final VertexAiUtil util = new VertexAiUtil("http://127.0.0.1:1/", () -> { throw new IOException("Application Default Credentials are not available"); });
        final VertexAiUtil.CredentialsException e = Assertions.assertThrows(VertexAiUtil.CredentialsException.class,
                () -> util.getBatchPredictionJob("p", "us-central1", "1"));
        Assertions.assertTrue(e.getMessage().contains("access token"), e.getMessage());
        Assertions.assertInstanceOf(IOException.class, e.getCause());
    }

    @Test
    public void testUtilHelpers() {
        Assertions.assertEquals("publishers/google/models/gemini-2.5-flash", VertexAiUtil.modelResource("gemini-2.5-flash"));
        Assertions.assertEquals("publishers/google/models/x", VertexAiUtil.modelResource("publishers/google/models/x"));
        Assertions.assertEquals("projects/p/locations/l/endpoints/1", VertexAiUtil.modelResource("projects/p/locations/l/endpoints/1"));
        Assertions.assertEquals("projects/p/locations/global/publishers/google/models/g", VertexAiUtil.modelPath("p", "global", "g"));
        Assertions.assertEquals("projects/p/locations/l/endpoints/1", VertexAiUtil.modelPath("p", "l", "projects/p/locations/l/endpoints/1"));
        Assertions.assertEquals("https://aiplatform.googleapis.com/v1/", VertexAiUtil.endpoint("global"));
        Assertions.assertEquals("https://us-central1-aiplatform.googleapis.com/v1/", VertexAiUtil.endpoint("us-central1"));
        Assertions.assertEquals("123", VertexAiUtil.id("projects/p/locations/l/batchPredictionJobs/123"));
        Assertions.assertEquals("projects/p/locations/l/batchPredictionJobs/123", VertexAiUtil.batchPredictionJobName("p", "l", "123"));
        Assertions.assertEquals("projects/p/locations/l/batchPredictionJobs/123", VertexAiUtil.batchPredictionJobName("x", "y", "projects/p/locations/l/batchPredictionJobs/123"));

        final JsonObject job = JsonParser.parseString("""
                {"name":"projects/p/locations/l/batchPredictionJobs/1","state":"JOB_STATE_FAILED",
                 "error":{"code":3,"message":"bad"},"completionStats":{"successfulCount":"10","failedCount":"2"}}
                """).getAsJsonObject();
        Assertions.assertTrue(VertexAiUtil.describeFailure(job).contains("bad"));
        final Map<String, Object> payload = VertexAiUtil.toPayload(job);
        // int64 fields are strings in the REST JSON and stay strings
        Assertions.assertEquals("2", ((Map<?, ?>) payload.get("completionStats")).get("failedCount"));
        Assertions.assertEquals(3L, ((Map<?, ?>) payload.get("error")).get("code"));

        final JsonObject response = JsonParser.parseString("""
                {"candidates":[{"content":{"parts":[{"text":"a"},{"thought":true,"text":"t"},{"text":"b"}]},"finishReason":"MAX_TOKENS"}]}
                """).getAsJsonObject();
        Assertions.assertEquals("ab", VertexAiUtil.responseText(response));
        Assertions.assertEquals("MAX_TOKENS", VertexAiUtil.finishReason(response));
        Assertions.assertNull(VertexAiUtil.responseText(new JsonObject()));
        Assertions.assertNull(VertexAiUtil.finishReason(new JsonObject()));
    }

}
