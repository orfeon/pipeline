package com.mercari.solution.module.action;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.cloud.google.CloudBuildUtil;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BuildActionTest {

    private static final String SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - tenant: acme
                    - tenant: globex
                schema:
                  fields:
                    - name: tenant
                      type: string
            """;

    /** In-memory Cloud Build: builds advance one status per getBuild poll along a scripted path. */
    static class MemoryBuildClient implements BuildAction.BuildClient {

        final Map<String, JsonObject> builds = new ConcurrentHashMap<>();
        final Map<String, Deque<String>> transitions = new ConcurrentHashMap<>();
        final Map<String, JsonObject> requests = new ConcurrentHashMap<>();
        final ConcurrentLinkedQueue<String> ops = new ConcurrentLinkedQueue<>();
        final AtomicInteger counter = new AtomicInteger(0);
        List<String> path = List.of("QUEUED", "WORKING", "SUCCESS");
        /** base64 step outputs attached to a build when it reaches a terminal status. */
        List<String> outputs = List.of();
        String failureDetail = "step exited with non-zero status";
        /** getBuild calls that fail with 503 before succeeding (transient poll errors). */
        final AtomicInteger transientGets = new AtomicInteger(0);
        /** listBuilds calls that return nothing before the real result (a build that does not exist yet). */
        final AtomicInteger emptyLists = new AtomicInteger(0);

        JsonObject put(final String id, final String status, final List<String> tags, final long createSeconds) {
            final JsonObject build = new JsonObject();
            build.addProperty("id", id);
            build.addProperty("projectId", "myproject");
            build.addProperty("status", status);
            build.addProperty("createTime", "2026-08-28T00:00:" + String.format("%02d", createSeconds % 60) + "Z");
            build.addProperty("logUrl", "https://console.cloud.google.com/cloud-build/builds/" + id);
            final JsonArray tagArray = new JsonArray();
            tags.forEach(tagArray::add);
            build.add("tags", tagArray);
            builds.put(id, build);
            return build;
        }

        private JsonObject operation(final JsonObject build) {
            final JsonObject metadata = new JsonObject();
            metadata.add("build", build.deepCopy());
            final JsonObject operation = new JsonObject();
            operation.addProperty("name", "operations/build/myproject/" + build.get("id").getAsString());
            operation.add("metadata", metadata);
            return operation;
        }

        private JsonObject start(final String prefix, final JsonObject request) {
            final String id = prefix + "-" + counter.incrementAndGet();
            requests.put(id, request);
            final List<String> tags = new ArrayList<>();
            CloudBuildUtil_array(request, "tags").forEach(t -> tags.add(t.getAsString()));
            final JsonObject build = put(id, path.getFirst(), tags, counter.get());
            if(request.has("substitutions")) {
                build.add("substitutions", request.get("substitutions"));
            }
            transitions.put(id, new ArrayDeque<>(path.subList(1, path.size())));
            return operation(build);
        }

        private static JsonArray CloudBuildUtil_array(final JsonObject o, final String f) {
            return o.has(f) && o.get(f).isJsonArray() ? o.getAsJsonArray(f) : new JsonArray();
        }

        @Override
        public JsonObject createBuild(final String project, final String location, final JsonObject build) {
            ops.add("create");
            return start("build", build);
        }

        @Override
        public JsonObject getBuild(final String project, final String location, final String buildId) {
            ops.add("get:" + buildId);
            if(transientGets.getAndDecrement() > 0) {
                throw new CloudBuildUtil.CloudBuildException(503, "{}", "unavailable");
            }
            final JsonObject build = builds.get(buildId);
            if(build == null) {
                throw new CloudBuildUtil.CloudBuildException(404, "{}", "not found: " + buildId);
            }
            final Deque<String> path = transitions.get(buildId);
            if(path != null && !path.isEmpty()) {
                final String next = path.poll();
                build.addProperty("status", next);
                if(CloudBuildUtil.isTerminal(next)) {
                    final JsonObject results = new JsonObject();
                    final JsonArray stepOutputs = new JsonArray();
                    outputs.forEach(stepOutputs::add);
                    results.add("buildStepOutputs", stepOutputs);
                    build.add("results", results);
                    if(!"SUCCESS".equals(next) && !"CANCELLED".equals(next)) {
                        build.addProperty("statusDetail", failureDetail);
                        final JsonObject info = new JsonObject();
                        info.addProperty("type", "USER_BUILD_STEP");
                        info.addProperty("detail", failureDetail);
                        build.add("failureInfo", info);
                    }
                }
            }
            return build.deepCopy();
        }

        @Override
        public List<JsonObject> listBuilds(final String project, final String location, final String filter, final int limit) {
            ops.add("list:" + filter);
            if(emptyLists.getAndDecrement() > 0) {
                return List.of();
            }
            // supports the subset the action emits: tags="x" [AND tags="y"], status="S", build_trigger_id="t"
            return builds.values().stream()
                    .filter(b -> matches(b, filter))
                    .sorted(Comparator.comparing((JsonObject b) -> b.get("createTime").getAsString()).reversed())
                    .limit(limit)
                    .map(JsonObject::deepCopy)
                    .toList();
        }

        private static boolean matches(final JsonObject build, final String filter) {
            if(filter == null || filter.isBlank()) {
                return true;
            }
            for(final String term : filter.split(" AND ")) {
                final String[] kv = term.trim().split("=", 2);
                final String value = kv[1].replace("\"", "");
                switch (kv[0].trim()) {
                    case "tags" -> {
                        boolean found = false;
                        for(final var t : CloudBuildUtil_array(build, "tags")) {
                            found |= t.getAsString().equals(value);
                        }
                        if(!found) return false;
                    }
                    case "status" -> {
                        if(!value.equals(build.get("status").getAsString())) return false;
                    }
                    case "build_trigger_id" -> {
                        if(!build.has("buildTriggerId") || !value.equals(build.get("buildTriggerId").getAsString())) return false;
                    }
                    default -> throw new IllegalArgumentException("unsupported filter: " + term);
                }
            }
            return true;
        }

        @Override
        public JsonObject cancelBuild(final String project, final String location, final String buildId) {
            ops.add("cancel:" + buildId);
            final JsonObject build = builds.get(buildId);
            if(build == null) {
                throw new CloudBuildUtil.CloudBuildException(404, "{}", "not found: " + buildId);
            }
            transitions.put(buildId, new ArrayDeque<>(List.of("CANCELLED")));
            return build.deepCopy();
        }

        @Override
        public JsonObject runTrigger(final String project, final String location, final String trigger, final JsonObject source) {
            ops.add("run:" + trigger);
            final JsonObject request = new JsonObject();
            request.addProperty("trigger", trigger);
            if(source != null) {
                request.add("source", source);
            }
            final JsonObject operation = start("trig", request);
            builds.get(operation.getAsJsonObject("metadata").getAsJsonObject("build").get("id").getAsString()).addProperty("buildTriggerId", trigger);
            return operation;
        }
    }

    private static MemoryBuildClient register(final String name) {
        final MemoryBuildClient client = new MemoryBuildClient();
        BuildAction.registerMemoryClient(name, client);
        return client;
    }

    private static MCollection run(final TestPipeline pipeline, final String yaml, final String step) throws Exception {
        return MPipeline.apply(pipeline, Config.load(yaml)).get(step);
    }

    private static JsonObject payload(final MElement e) {
        return JsonParser.parseString((String) e.getPrimitiveValue("payload")).getAsJsonObject();
    }

    private static String b64(final String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testCreateScriptWaitsAndDecodesOutputsThenAdoptsExisting() throws Exception {
        final MemoryBuildClient client = register("create");
        client.outputs = List.of(b64("{\"rows\": 42, \"ok\": true}"), b64("plain text"));
        final String yaml = """
                actions:
                  - name: create
                    module: build
                    operation: builds.create
                    parameters:
                      projectId: myproject
                      location: asia-northeast1
                      endpoint: memory://create
                      tags: [report-r1]
                      image: python:3.12
                      script: |
                        python render.py --run $_RUN --project ${PROJECT_ID} --dir ${BUILDER_OUTPUT}
                        echo '{"rows": 42}' > $BUILDER_OUTPUT/output
                      env:
                        MODE: batch
                      substitutions:
                        _RUN: r1
                      options:
                        machineType: E2_HIGHCPU_8
                        logging: CLOUD_LOGGING_ONLY
                      serviceAccount: projects/myproject/serviceAccounts/build@myproject.iam.gserviceaccount.com
                      timeout: 1800s
                """;
        client.transientGets.set(1);
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p1, yaml, "create").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("build", e.getPrimitiveValue("service"));
                Assertions.assertEquals("builds.create", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("build-1", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("SUCCESS", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals("SUCCESS", payload.get("status").getAsString());
                Assertions.assertEquals(2, payload.getAsJsonArray("outputs").size());
                Assertions.assertEquals(42, payload.getAsJsonArray("outputs").get(0).getAsJsonObject().get("rows").getAsInt());
                Assertions.assertEquals("plain text", payload.getAsJsonArray("outputs").get(1).getAsString());
                Assertions.assertFalse(payload.has("adopted"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p1.run();
        final JsonObject request = client.requests.get("build-1");
        final JsonObject step = request.getAsJsonArray("steps").get(0).getAsJsonObject();
        Assertions.assertEquals("python:3.12", step.get("name").getAsString());
        // Cloud Build / shell ${...} are not template variables: passed through verbatim
        Assertions.assertTrue(step.get("script").getAsString().contains("python render.py --run $_RUN --project ${PROJECT_ID} --dir ${BUILDER_OUTPUT}"), step.get("script").getAsString());
        // a transient 503 on one poll is retried inside the wait: exactly one build was created
        Assertions.assertEquals(1, client.ops.stream().filter(o -> o.equals("create")).count(), client.ops.toString());
        Assertions.assertEquals("MODE=batch", step.getAsJsonArray("env").get(0).getAsString());
        Assertions.assertTrue(step.get("automapSubstitutions").getAsBoolean());
        Assertions.assertEquals("r1", request.getAsJsonObject("substitutions").get("_RUN").getAsString());
        Assertions.assertEquals("E2_HIGHCPU_8", request.getAsJsonObject("options").get("machineType").getAsString());
        Assertions.assertEquals("1800s", request.get("timeout").getAsString());
        Assertions.assertEquals("report-r1", request.getAsJsonArray("tags").get(0).getAsString());
        // dedupe lookup before create, then polled until SUCCESS
        Assertions.assertEquals("list:tags=\"report-r1\"", client.ops.stream().filter(o -> o.startsWith("list")).findFirst().orElse(null));
        Assertions.assertTrue(client.ops.stream().filter(o -> o.equals("get:build-1")).count() >= 2);

        // second run with the same tag: the succeeded build is adopted (no new build), state SUCCESS
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, yaml, "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("build-1", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("SUCCESS", e.getPrimitiveValue("state"));
                Assertions.assertTrue(payload(e).get("adopted").getAsBoolean());
            }
            return null;
        });
        p2.run();
        Assertions.assertEquals(1, client.requests.size());

        // a working build with the tag, wait: false -> EXISTS
        client.put("build-9", "WORKING", List.of("report-r1"), 50);
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, yaml + "      wait: false\n", "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("build-9", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("EXISTS", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p3.run();

        // reuseExisting: false -> a new build despite the tag
        final TestPipeline p4 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p4, yaml + "      reuseExisting: false\n      wait: false\n", "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("build-2", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("QUEUED", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p4.run();

        // the newest same-tagged build failed -> not adopted (an older success does not count), a new build runs
        client.put("build-19", "FAILURE", List.of("report-r1"), 55);
        final TestPipeline p5 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p5, yaml + "      wait: false\n", "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("build-3", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("QUEUED", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p5.run();

        // skipWhen on the first decoded output (payload.output)
        client.builds.remove("build-19");
        client.builds.remove("build-9");
        final TestPipeline p6 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p6, yaml.replace("operation: builds.create\n", "operation: builds.create\n    skipWhen: payload.`output`.`rows` > 40\n"), "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("SKIPPED", e.getPrimitiveValue("state"));
                Assertions.assertEquals(42, payload(e).getAsJsonObject("output").get("rows").getAsInt());
            }
            return null;
        });
        p6.run();
        BuildAction.unregisterMemoryClient("create");
    }

    @Test
    public void testFailedBuildIsNonRetryableWithDetail() throws Exception {
        final MemoryBuildClient client = register("failed");
        client.path = List.of("QUEUED", "WORKING", "FAILURE");
        client.failureDetail = "boom";
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p, """
                actions:
                  - name: create
                    module: build
                    operation: builds.create
                    failFast: true
                    retry: { maxAttempts: 3, initialBackoff: 10ms }
                    parameters:
                      projectId: myproject
                      endpoint: memory://failed
                      steps:
                        - name: gcr.io/cloud-builders/gcloud
                          args: [version]
                """, "create");
        final Exception e = Assertions.assertThrows(Exception.class, p::run);
        final StringBuilder message = new StringBuilder();
        for(Throwable t = e; t != null; t = t.getCause()) {
            message.append(t.getMessage()).append(" ");
        }
        Assertions.assertTrue(message.toString().contains("boom"), message.toString());
        Assertions.assertTrue(message.toString().contains("USER_BUILD_STEP"), message.toString());
        Assertions.assertEquals(1, client.ops.stream().filter(o -> o.equals("create")).count(), client.ops.toString());
        BuildAction.unregisterMemoryClient("failed");
    }

    @Test
    public void testRejectedRequestIsNotRetried() throws Exception {
        final MemoryBuildClient client = register("rejected");
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p, """
                actions:
                  - name: get
                    module: build
                    operation: builds.get
                    failFast: true
                    retry: { maxAttempts: 3, initialBackoff: 10ms }
                    parameters: { projectId: myproject, endpoint: memory://rejected, buildId: missing }
                """, "get");
        Assertions.assertThrows(Exception.class, p::run);
        Assertions.assertEquals(1, client.ops.stream().filter(o -> o.equals("get:missing")).count(), client.ops.toString());
        BuildAction.unregisterMemoryClient("rejected");
    }

    @Test
    public void testPerElementCreateNoWaitThenCollectWait() throws Exception {
        final MemoryBuildClient client = register("fanout");
        final String yaml = SOURCE_YAML + """
                actions:
                  - name: create
                    module: build
                    operation: builds.create
                    trigger: perElement
                    inputs: [input]
                    parameters:
                      projectId: myproject
                      location: asia-northeast1
                      endpoint: memory://fanout
                      tags: ["report-${tenant}"]
                      substitutions:
                        _TENANT: ${tenant}
                      steps:
                        - name: python:3.12
                          args: [python, render.py, "--tenant", "${tenant}"]
                      wait: false
                  - name: wait
                    module: build
                    operation: builds.wait
                    trigger: collect
                    inputs: [create]
                    parameters:
                      projectId: myproject
                      location: asia-northeast1
                      endpoint: memory://fanout
                      jobIdField: jobId
                """;
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(p, Config.load(yaml));
        PAssert.that(outputs.get("create").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("QUEUED", e.getPrimitiveValue("state"), "wait: false must not wait");
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        PAssert.that(outputs.get("wait").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("builds.wait", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals(2, payload.get("count").getAsInt());
                Assertions.assertEquals("SUCCESS", payload.getAsJsonObject("firstBuild").get("status").getAsString());
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p.run();
        Assertions.assertEquals(2, client.requests.size());
        final Set<String> seen = new HashSet<>();
        client.requests.values().forEach(r -> seen.add(
                r.getAsJsonArray("tags").get(0).getAsString() + "=" + r.getAsJsonObject("substitutions").get("_TENANT").getAsString()
                        + "=" + r.getAsJsonArray("steps").get(0).getAsJsonObject().getAsJsonArray("args").get(3).getAsString()));
        Assertions.assertEquals(Set.of("report-acme=acme=acme", "report-globex=globex=globex"), seen);
        BuildAction.unregisterMemoryClient("fanout");
    }

    @Test
    public void testListWaitByFilterCancelAndTriggerRun() throws Exception {
        final MemoryBuildClient client = register("ops");
        client.put("b-old", "FAILURE", List.of("nightly"), 10).addProperty("buildTriggerId", "trig-1");
        client.put("b-new", "SUCCESS", List.of("nightly"), 20).addProperty("buildTriggerId", "trig-1");
        client.put("b-other", "WORKING", List.of("other"), 30);

        // builds.list with a filter + failWhen on count
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p1, """
                actions:
                  - name: list
                    module: build
                    operation: builds.list
                    failWhen: payload.`count` = 0
                    parameters:
                      projectId: myproject
                      endpoint: memory://ops
                      filter: build_trigger_id="trig-1" AND status="SUCCESS"
                """, "list").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                Assertions.assertEquals("b-new", e.getPrimitiveValue("jobId"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals(1, payload.get("count").getAsInt());
                Assertions.assertEquals("b-new", payload.getAsJsonObject("firstBuild").get("id").getAsString());
            }
            return null;
        });
        p1.run();

        // builds.wait by filter: polls until a matching build exists, then waits for it until terminal
        client.transitions.put("b-other", new ArrayDeque<>(List.of("WORKING", "SUCCESS")));
        client.emptyLists.set(2);
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, """
                actions:
                  - name: wait
                    module: build
                    operation: builds.wait
                    parameters:
                      projectId: myproject
                      endpoint: memory://ops
                      filter: tags="other"
                """, "wait").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("b-other", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("SUCCESS", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p2.run();
        Assertions.assertTrue(client.ops.stream().filter(o -> o.equals("list:tags=\"other\"")).count() >= 3, client.ops.toString());

        // builds.wait with waitUntil: none reports the current status without waiting
        client.put("b-working", "WORKING", List.of(), 42);
        final TestPipeline p2b = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2b, """
                actions:
                  - name: wait
                    module: build
                    operation: builds.wait
                    parameters: { projectId: myproject, endpoint: memory://ops, buildId: b-working, waitUntil: none }
                """, "wait").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("WORKING", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p2b.run();

        // builds.cancel waits until CANCELLED (not a failure when we asked for it)
        client.put("b-cancel", "WORKING", List.of(), 40);
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, """
                actions:
                  - name: cancel
                    module: build
                    operation: builds.cancel
                    parameters: { projectId: myproject, endpoint: memory://ops, buildId: b-cancel }
                """, "cancel").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("CANCELLED", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p3.run();
        Assertions.assertTrue(client.ops.contains("cancel:b-cancel"), client.ops.toString());

        // a build cancelled by someone else while waited for -> failure
        client.put("b-killed", "WORKING", List.of(), 45);
        client.transitions.put("b-killed", new ArrayDeque<>(List.of("CANCELLED")));
        final TestPipeline p3b = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p3b, """
                actions:
                  - name: wait
                    module: build
                    operation: builds.wait
                    failFast: true
                    parameters: { projectId: myproject, endpoint: memory://ops, buildId: b-killed }
                """, "wait");
        final StringBuilder message = new StringBuilder();
        for(Throwable t = Assertions.assertThrows(Exception.class, p3b::run); t != null; t = t.getCause()) {
            message.append(t.getMessage()).append(" ");
        }
        Assertions.assertTrue(message.toString().contains("cancelled"), message.toString());

        // triggers.run with a RepoSource, waited until SUCCESS
        final TestPipeline p4 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p4, """
                actions:
                  - name: deploy
                    module: build
                    operation: triggers.run
                    parameters:
                      projectId: myproject
                      location: asia-northeast1
                      endpoint: memory://ops
                      triggerId: site-deploy
                      source:
                        branchName: main
                        substitutions: { _RUN: r1 }
                """, "deploy").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("triggers.run", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("trig-1", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("SUCCESS", e.getPrimitiveValue("state"));
                Assertions.assertEquals("site-deploy", payload(e).get("buildTriggerId").getAsString());
            }
            return null;
        });
        p4.run();
        final JsonObject runRequest = client.requests.get("trig-1");
        Assertions.assertEquals("main", runRequest.getAsJsonObject("source").get("branchName").getAsString());
        Assertions.assertEquals("r1", runRequest.getAsJsonObject("source").getAsJsonObject("substitutions").get("_RUN").getAsString());
        BuildAction.unregisterMemoryClient("ops");
    }

    @Test
    public void testValidation() {
        final String base = """
                actions:
                  - name: step
                    module: build
                    operation: %s
                    parameters:
                      projectId: myproject
                      %s
                """;
        assertInvalid(base.formatted("builds.create", "tags: [x]"), "steps (a Build steps list) or image + script");
        assertInvalid(base.formatted("builds.create", "script: echo hi"), "image is required with script");
        assertInvalid(base.formatted("builds.create", "image: alpine\n      script: echo hi\n      steps:\n        - name: alpine"), "steps and script are exclusive");
        assertInvalid(base.formatted("builds.create", "image: alpine\n      script: echo hi\n      substitutions:\n        TABLE: x"), "substitutions keys must start with '_'");
        assertInvalid(base.formatted("builds.create", "image: alpine\n      script: echo hi\n      options:\n        pool:\n          name: projects/p/locations/asia-northeast1/workerPools/w"), "options.pool (a private pool) requires a regional location");
        assertInvalid(base.formatted("builds.get", "filter: x"), "buildId is required");
        assertInvalid(base.formatted("builds.wait", "jobIdField: jobId"), "jobIdField requires trigger: collect");
        assertInvalid(base.formatted("builds.wait", "location: global"), "buildId, jobIdField or filter is required");
        assertInvalid(base.formatted("builds.list", "pageSize: 0"), "pageSize must be positive");
        assertInvalid(base.formatted("triggers.run", "location: global"), "triggerId is required");
        assertInvalid("""
                actions:
                  - name: step
                    module: build
                    operation: builds.get
                    parameters:
                      buildId: b
                """, "projectId is required");
    }

    private static void assertInvalid(final String yaml, final String expected) {
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> run(p, yaml, "step"));
        Assertions.assertTrue(e.getMessage().contains(expected), e.getMessage());
    }

    // The projectId-required branch, asserted on Parameters.validate directly: the pipeline-level
    // case in testValidation only reaches it when no ambient default GCP project is resolvable
    // (surefire isolates CLOUDSDK_CONFIG for that; this assertion holds even outside maven).
    @Test
    public void testValidateRequiresProjectId() {
        final BuildAction.Parameters parameters = new BuildAction.Parameters();
        parameters.op = BuildAction.Op.get;
        parameters.buildId = "b";
        final List<String> errors = parameters.validate("step", com.mercari.solution.module.Action.Trigger.once);
        Assertions.assertTrue(
                errors.stream().anyMatch(m -> m.contains("projectId is required")),
                errors.toString());
    }

    @Test
    public void testTemplateLeavesForeignExpressions() {
        final Map<String, Object> data = new HashMap<>();
        data.put("tenant", "acme");
        data.put("args", Map.of("run", "r1"));
        Assertions.assertEquals("gcr.io/${PROJECT_ID}/acme:${_TAG} r1 ${BUILDER_OUTPUT} $_RUN",
                BuildAction.template("gcr.io/${PROJECT_ID}/${tenant}:${_TAG} ${args.run} ${BUILDER_OUTPUT} $_RUN", data));
        Assertions.assertEquals("ACME", BuildAction.template("${tenant?upper_case}", data));
        Assertions.assertEquals("plain", BuildAction.template("plain", data));
        Assertions.assertNull(BuildAction.template(null, data));

        final JsonObject build = JsonParser.parseString("{\"status\":\"FAILURE\",\"statusDetail\":null,\"failureInfo\":{\"type\":null},\"logUrl\":null}").getAsJsonObject();
        Assertions.assertEquals(" [?]", CloudBuildUtil.describeFailure(build));
        Assertions.assertEquals("FAILURE", CloudBuildUtil.status(build));
    }

    @Test
    public void testPayloadAndOutputs() {
        final JsonObject build = JsonParser.parseString("""
                {"id":"b","status":"SUCCESS","timeout":"600s",
                 "options":{"diskSizeGb":"100","machineType":"E2_HIGHCPU_8"},
                 "results":{"numArtifacts":"3","buildStepOutputs":["%s","%s",""]}}
                """.formatted(b64("{\"n\": 1.5}"), b64("[1,2]"))).getAsJsonObject();
        final Map<String, Object> payload = BuildAction.toPayload(build);
        Assertions.assertEquals("SUCCESS", payload.get("status"));
        // int64 fields are strings in the REST JSON and stay strings (as in the discovery document)
        Assertions.assertEquals("100", ((Map<?, ?>) payload.get("options")).get("diskSizeGb"));
        Assertions.assertEquals("3", ((Map<?, ?>) payload.get("results")).get("numArtifacts"));
        final List<?> outputs = (List<?>) payload.get("outputs");
        Assertions.assertEquals(3, outputs.size());
        Assertions.assertEquals(1.5, ((Map<?, ?>) outputs.get(0)).get("n"));
        Assertions.assertEquals(1.5, ((Map<?, ?>) payload.get("output")).get("n"));
        Assertions.assertEquals("[1,2]", outputs.get(1));
        Assertions.assertNull(outputs.get(2));
        Assertions.assertEquals("https://console.cloud.google.com/cloud-build/builds;region=global/b?project=p", CloudBuildUtil.consoleUrl("p", "global", "b"));
    }

}
