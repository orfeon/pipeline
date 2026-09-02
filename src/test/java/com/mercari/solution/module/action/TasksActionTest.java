package com.mercari.solution.module.action;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.tasks.v2.Queue;
import com.google.cloud.tasks.v2.Task;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.FieldMask;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.sink.TasksSink;
import io.grpc.Status;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TasksActionTest {

    private static final String QUEUE = "projects/myproject/locations/asia-northeast1/queues/myqueue";

    private static final String SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - id: a
                    - id: b
                schema:
                  fields:
                    - name: id
                      type: string
            """;

    /** In-memory queue store. */
    static class MemoryQueueClient implements TasksAction.QueueClient {

        final Map<String, Queue> queues = new ConcurrentHashMap<>();
        final ConcurrentLinkedQueue<String> ops = new ConcurrentLinkedQueue<>();
        final Set<String> tasks = ConcurrentHashMap.newKeySet();
        final AtomicInteger remaining = new AtomicInteger(0);

        private Queue require(final String name) {
            final Queue q = queues.get(name);
            if(q == null) {
                throw new NotFoundException("not found: " + name, null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
            }
            return q;
        }

        @Override
        public Queue getQueue(final String name) {
            ops.add("get");
            return require(name);
        }

        @Override
        public Queue createQueue(final String parent, final Queue queue) {
            ops.add("create");
            if(queues.containsKey(queue.getName())) {
                throw new AlreadyExistsException("exists", null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false);
            }
            final Queue q = queue.toBuilder().setState(Queue.State.RUNNING).build();
            queues.put(q.getName(), q);
            return q;
        }

        @Override
        public Queue updateQueue(final Queue queue, final FieldMask mask) {
            ops.add("update:" + String.join(",", mask.getPathsList()));
            final Queue.Builder b = require(queue.getName()).toBuilder();
            if(mask.getPathsList().contains("rate_limits")) {
                b.setRateLimits(queue.getRateLimits());
            }
            if(mask.getPathsList().contains("retry_config")) {
                b.setRetryConfig(queue.getRetryConfig());
            }
            queues.put(queue.getName(), b.build());
            return b.build();
        }

        @Override
        public void deleteQueue(final String name) {
            ops.add("delete");
            require(name);
            queues.remove(name);
        }

        @Override
        public Queue pauseQueue(final String name) {
            ops.add("pause");
            final Queue q = require(name).toBuilder().setState(Queue.State.PAUSED).build();
            queues.put(name, q);
            return q;
        }

        @Override
        public Queue resumeQueue(final String name) {
            ops.add("resume");
            final Queue q = require(name).toBuilder().setState(Queue.State.RUNNING).build();
            queues.put(name, q);
            return q;
        }

        @Override
        public Queue purgeQueue(final String name) {
            ops.add("purge");
            return require(name);
        }

        @Override
        public int countTasks(final String queue, final int limit) {
            ops.add("count");
            // each poll drains one task
            return Math.max(0, remaining.getAndDecrement());
        }

        @Override
        public Task runTask(final String name) {
            ops.add("run:" + name);
            return Task.newBuilder().setName(name).build();
        }

        @Override
        public void deleteTask(final String name) {
            ops.add("deleteTask:" + name);
            if(!tasks.remove(name)) {
                throw new NotFoundException("not found: " + name, null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
            }
        }

        @Override
        public void close() {}
    }

    private static MemoryQueueClient register(final String name) {
        final MemoryQueueClient client = new MemoryQueueClient();
        TasksAction.registerMemoryClient(name, client);
        return client;
    }

    private static MCollection run(final TestPipeline pipeline, final String yaml, final String step) throws Exception {
        return MPipeline.apply(pipeline, Config.load(yaml)).get(step);
    }

    @Test
    public void testCreateAdoptsExistingAndUpdate() throws Exception {
        final MemoryQueueClient client = register("create");
        final String yaml = """
                actions:
                  - name: create
                    module: tasks
                    operation: queues.create
                    parameters:
                      queue: %s
                      endpoint: memory://create
                      rateLimits:
                        maxDispatchesPerSecond: 5.5
                        maxConcurrentDispatches: 3
                      retryConfig:
                        maxAttempts: 4
                        minBackoff: 2s
                        maxBackoff: PT1M
                """.formatted(QUEUE);
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p1, yaml, "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("tasks", e.getPrimitiveValue("service"));
                Assertions.assertEquals("queues.create", e.getPrimitiveValue("operation"));
                Assertions.assertEquals(QUEUE, e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                final JsonObject payload = JsonParser.parseString((String) e.getPrimitiveValue("payload")).getAsJsonObject();
                Assertions.assertEquals(5.5, payload.getAsJsonObject("rateLimits").get("maxDispatchesPerSecond").getAsDouble());
                Assertions.assertEquals("2s", payload.getAsJsonObject("retryConfig").get("minBackoff").getAsString());
                Assertions.assertEquals("60s", payload.getAsJsonObject("retryConfig").get("maxBackoff").getAsString());
            }
            return null;
        });
        p1.run();
        Assertions.assertEquals(1, client.queues.size());

        // second run: already exists -> adopted with state EXISTS
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, yaml, "create").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("EXISTS", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p2.run();

        // update only rate limits
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, """
                actions:
                  - name: update
                    module: tasks
                    operation: queues.update
                    parameters:
                      queue: %s
                      endpoint: memory://create
                      rateLimits:
                        maxConcurrentDispatches: 10
                """.formatted(QUEUE), "update").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("queues.update", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p3.run();
        Assertions.assertTrue(client.ops.contains("update:rate_limits"));
        Assertions.assertEquals(10, client.queues.get(QUEUE).getRateLimits().getMaxConcurrentDispatches());
        // retry config untouched
        Assertions.assertEquals(4, client.queues.get(QUEUE).getRetryConfig().getMaxAttempts());
        TasksAction.unregisterMemoryClient("create");
    }

    @Test
    public void testPauseResumePurgeGetDelete() throws Exception {
        final MemoryQueueClient client = register("ops");
        client.queues.put(QUEUE, Queue.newBuilder().setName(QUEUE).setState(Queue.State.RUNNING).build());
        for(final String op : List.of("pause", "get", "resume", "purge", "delete", "delete")) {
            final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
            final boolean secondDelete = op.equals("delete") && client.ops.contains("delete");
            PAssert.that(run(p, """
                    actions:
                      - name: step
                        module: tasks
                        operation: queues.%s
                        parameters:
                          queue: %s
                          endpoint: memory://ops
                    """.formatted(op, QUEUE), "step").getCollection()).satisfies(elements -> {
                int count = 0;
                for(final MElement e : elements) {
                    count++;
                    Assertions.assertEquals("queues." + op, e.getPrimitiveValue("operation"));
                    Assertions.assertEquals(secondDelete ? "NOT_FOUND" : "DONE", e.getPrimitiveValue("state"));
                    if(op.equals("pause")) {
                        Assertions.assertTrue(((String) e.getPrimitiveValue("payload")).contains("PAUSED"));
                    }
                }
                Assertions.assertEquals(1, count);
                return null;
            });
            p.run();
        }
        Assertions.assertEquals(List.of("pause", "get", "resume", "purge", "delete", "delete"), new ArrayList<>(client.ops));
        TasksAction.unregisterMemoryClient("ops");
    }

    @Test
    public void testWaitForEmptyAfterSinkAndTimeout() throws Exception {
        // sink enqueues (memory tasks client), then the action waits for the queue to drain:
        // the action's inputs are the sink's control records -> trigger once fires after the sink completes
        final MemoryQueueClient client = register("wait");
        client.remaining.set(3);
        final com.mercari.solution.module.sink.TasksSinkTest.MemoryTasksClient sinkClient =
                new com.mercari.solution.module.sink.TasksSinkTest.MemoryTasksClient();
        TasksSink.registerMemoryClient("waitSink", sinkClient);

        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p, SOURCE_YAML + """
                sinks:
                  - name: enqueue
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      endpoint: memory://waitSink
                      target:
                        url: https://api.example.com/${id}
                actions:
                  - name: wait
                    module: tasks
                    operation: queues.waitForEmpty
                    inputs: [enqueue]
                    parameters:
                      queue: %s
                      endpoint: memory://wait
                      pollIntervalSeconds: 1
                      timeoutSeconds: 60
                """.formatted(QUEUE, QUEUE), "wait").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("queues.waitForEmpty", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                final JsonObject payload = JsonParser.parseString((String) e.getPrimitiveValue("payload")).getAsJsonObject();
                Assertions.assertEquals(4, payload.get("polls").getAsInt()); // 3,2,1 remaining then 0
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p.run();
        Assertions.assertEquals(2, sinkClient.created.size());
        Assertions.assertEquals(4, client.ops.stream().filter("count"::equals).count());
        TasksSink.unregisterMemoryClient("waitSink");

        // timeout: queue never drains -> failure routed (failFast false -> empty output)
        client.remaining.set(Integer.MAX_VALUE / 2);
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, """
                actions:
                  - name: wait
                    module: tasks
                    operation: queues.waitForEmpty
                    failFast: false
                    parameters:
                      queue: %s
                      endpoint: memory://wait
                      pollIntervalSeconds: 1
                      timeoutSeconds: 1
                """.formatted(QUEUE), "wait").getCollection()).empty();
        p2.run();
        TasksAction.unregisterMemoryClient("wait");
    }

    @Test
    public void testRunTaskAndDeleteTaskPerElement() throws Exception {
        final MemoryQueueClient client = register("perElement");
        client.tasks.add(QUEUE + "/tasks/a");
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(p, Config.load(SOURCE_YAML + """
                actions:
                  - name: run
                    module: tasks
                    operation: tasks.run
                    trigger: perElement
                    inputs: [input]
                    parameters:
                      queue: %s
                      task: "${id}"
                      endpoint: memory://perElement
                  - name: del
                    module: tasks
                    operation: tasks.delete
                    trigger: perElement
                    inputs: [input]
                    parameters:
                      queue: %s
                      task: "${id}"
                      endpoint: memory://perElement
                """.formatted(QUEUE, QUEUE)));
        PAssert.that(outputs.get("run").getCollection()).satisfies(elements -> {
            final Set<String> ids = new TreeSet<>();
            for(final MElement e : elements) {
                Assertions.assertEquals("tasks.run", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                ids.add((String) e.getPrimitiveValue("jobId"));
            }
            Assertions.assertEquals(Set.of(QUEUE + "/tasks/a", QUEUE + "/tasks/b"), ids);
            return null;
        });
        PAssert.that(outputs.get("del").getCollection()).satisfies(elements -> {
            final Map<String, String> states = new HashMap<>();
            for(final MElement e : elements) {
                states.put((String) e.getPrimitiveValue("jobId"), (String) e.getPrimitiveValue("state"));
            }
            Assertions.assertEquals("DONE", states.get(QUEUE + "/tasks/a"));
            Assertions.assertEquals("NOT_FOUND", states.get(QUEUE + "/tasks/b"));
            return null;
        });
        p.run();
        TasksAction.unregisterMemoryClient("perElement");
    }

    @Test
    public void testValidation() {
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        // missing operation
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(p, Config.load("""
                actions:
                  - name: a
                    module: tasks
                    parameters:
                      queue: %s
                """.formatted(QUEUE))));
        // runTask without task, illegal queue, update without anything
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(p, Config.load("""
                actions:
                  - name: a
                    module: tasks
                    operation: tasks.run
                    parameters:
                      queue: myqueue
                """)));
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(p, Config.load("""
                actions:
                  - name: a
                    module: tasks
                    operation: queues.update
                    parameters:
                      queue: %s
                """.formatted(QUEUE))));
        Assertions.assertEquals(java.time.Duration.ofMinutes(5), TasksAction.parseDuration("5m"));
    }

}
