package com.mercari.solution.module.sink;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import io.grpc.Status;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TasksSinkTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String QUEUE = "projects/myproject/locations/asia-northeast1/queues/myqueue";

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
                      remind_at: "2030-01-01T00:00:00Z"
                      note: hello
                    - app_id: com.example.app
                      user_id: u2
                      amount: 20
                      remind_at: "2030-01-02T00:00:00Z"
                      note: null
                schema:
                  fields:
                    - name: app_id
                      type: string
                    - name: user_id
                      type: string
                    - name: amount
                      type: int64
                    - name: remind_at
                      type: timestamp
                    - name: note
                      type: string
            """;

    /** In-memory Cloud Tasks: records every request and emulates name-based dedup. */
    public static class MemoryTasksClient implements TasksSink.TasksClient {

        public final ConcurrentLinkedQueue<Task> created = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<String> createdQueues = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<ByteString> buffered = new ConcurrentLinkedQueue<>();
        final Set<String> names = ConcurrentHashMap.newKeySet();

        @Override
        public Task createTask(final String queue, final Task task) {
            if(!task.getName().isEmpty() && !names.add(task.getName())) {
                throw new AlreadyExistsException("exists: " + task.getName(), null,
                        GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false);
            }
            createdQueues.add(queue);
            created.add(task);
            final Task.Builder b = task.toBuilder();
            if(task.getName().isEmpty()) {
                b.setName(queue + "/tasks/" + UUID.randomUUID());
            }
            final Instant now = Instant.now();
            b.setCreateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build());
            if(!task.hasScheduleTime()) {
                b.setScheduleTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build());
            }
            return b.build();
        }

        @Override
        public Task bufferTask(final String queue, final String taskId, final ByteString body) {
            buffered.add(body);
            return Task.newBuilder().setName(queue + "/tasks/" + (taskId == null ? UUID.randomUUID() : taskId)).build();
        }

        @Override
        public void close() {}
    }

    private static MemoryTasksClient register(final String name) {
        final MemoryTasksClient client = new MemoryTasksClient();
        TasksSink.registerMemoryClient(name, client);
        return client;
    }

    @Test
    public void testHttpTargetPerElement() throws Exception {
        final MemoryTasksClient client = register("perElement");
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs:
                      - input
                    parameters:
                      queue: %s
                      endpoint: memory://perElement
                      target:
                        url: https://api.example.com/inappevent/${app_id?url}?src=${__source}
                        method: POST
                        headers:
                          Content-Type: application/json
                          X-User: ${user_id}
                          X-Static: fixed
                        auth:
                          type: gcpOidc
                          serviceAccount: invoker@myproject.iam.gserviceaccount.com
                      body:
                        format: json
                        omitNulls: true
                      task:
                        id: "${app_id}-${user_id}"
                        scheduleTime: "${remind_at}"
                        dispatchDeadline: 30s
                """.formatted(QUEUE);
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals(QUEUE, element.getPrimitiveValue("queue"));
                Assertions.assertEquals("CREATED", element.getPrimitiveValue("state"));
                Assertions.assertEquals(1L, element.getPrimitiveValue("elementCount"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("taskName")).startsWith(QUEUE + "/tasks/"));
                Assertions.assertTrue(((String) element.getPrimitiveValue("url")).startsWith("https://api.example.com/inappevent/"));
                Assertions.assertNotNull(element.getPrimitiveValue("scheduleTime"));
                Assertions.assertNotNull(element.getPrimitiveValue("createTime"));
                Assertions.assertTrue((Long) element.getPrimitiveValue("bytes") > 0);
                Assertions.assertNull(element.getPrimitiveValue("error"));
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        pipeline.run();

        final List<Task> tasks = new ArrayList<>(client.created);
        Assertions.assertEquals(2, tasks.size());
        tasks.sort(Comparator.comparing(t -> t.getHttpRequest().getUrl()));

        final Task t1 = tasks.get(0); // com.example.app
        Assertions.assertEquals("https://api.example.com/inappevent/com.example.app?src=input", t1.getHttpRequest().getUrl());
        Assertions.assertEquals(HttpMethod.POST, t1.getHttpRequest().getHttpMethod());
        Assertions.assertEquals("u2", t1.getHttpRequest().getHeadersMap().get("X-User"));
        Assertions.assertEquals("fixed", t1.getHttpRequest().getHeadersMap().get("X-Static"));
        Assertions.assertEquals("application/json", t1.getHttpRequest().getHeadersMap().get("Content-Type"));
        Assertions.assertEquals("invoker@myproject.iam.gserviceaccount.com", t1.getHttpRequest().getOidcToken().getServiceAccountEmail());
        Assertions.assertEquals("https://api.example.com/inappevent/com.example.app", t1.getHttpRequest().getOidcToken().getAudience());
        Assertions.assertEquals(30L, t1.getDispatchDeadline().getSeconds());
        Assertions.assertEquals(Instant.parse("2030-01-02T00:00:00Z").getEpochSecond(), t1.getScheduleTime().getSeconds());
        // hashed deterministic name
        Assertions.assertEquals(QUEUE + "/tasks/" + TasksSink.sha256Hex("com.example.app-u2"), t1.getName());
        // json body, nulls omitted
        final JsonObject body1 = JsonParser.parseString(t1.getHttpRequest().getBody().toString(StandardCharsets.UTF_8)).getAsJsonObject();
        Assertions.assertEquals("u2", body1.get("user_id").getAsString());
        Assertions.assertEquals(20, body1.get("amount").getAsLong());
        Assertions.assertFalse(body1.has("note"));

        final Task t2 = tasks.get(1); // id111
        final JsonObject body2 = JsonParser.parseString(t2.getHttpRequest().getBody().toString(StandardCharsets.UTF_8)).getAsJsonObject();
        Assertions.assertEquals("hello", body2.get("note").getAsString());
        Assertions.assertEquals(QUEUE + "/tasks/" + TasksSink.sha256Hex("id111-u1"), t2.getName());

        TasksSink.unregisterMemoryClient("perElement");
    }

    @Test
    public void testTemplateBodyDelayAndAlreadyExists() throws Exception {
        final MemoryTasksClient client = register("template");
        // both elements render the same id -> second one is ALREADY_EXISTS (treated as success)
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs:
                      - input
                    parameters:
                      queue: %s
                      endpoint: memory://template
                      target:
                        url: https://api.example.com/bulk
                        auth:
                          type: gcpOauth
                          serviceAccount: invoker@myproject.iam.gserviceaccount.com
                      body:
                        template: '{"user":"${user_id}","at":"${utils.datetime.formatTimestamp(remind_at, "yyyy-MM-dd HH:mm:ss.SSS", "UTC")}"}'
                      task:
                        id: "same-${user_id?substring(0,1)}"
                        hashId: false
                        delay: 10m
                """.formatted(QUEUE);
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Integer> states = new HashMap<>();
            for(final MElement element : elements) {
                states.merge((String) element.getPrimitiveValue("state"), 1, Integer::sum);
                Assertions.assertEquals(QUEUE + "/tasks/same-u", element.getPrimitiveValue("taskName"));
            }
            Assertions.assertEquals(1, states.get("CREATED"));
            Assertions.assertEquals(1, states.get("ALREADY_EXISTS"));
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(1, client.created.size());
        final Task task = client.created.peek();
        Assertions.assertEquals("https://www.googleapis.com/auth/cloud-platform", task.getHttpRequest().getOauthToken().getScope());
        final String body = task.getHttpRequest().getBody().toString(StandardCharsets.UTF_8);
        Assertions.assertTrue(body.matches("\\{\"user\":\"u[12]\",\"at\":\"2030-01-0[12] 00:00:00.000\"}"), body);
        final long delaySec = task.getScheduleTime().getSeconds() - Instant.now().getEpochSecond();
        Assertions.assertTrue(delaySec > 500 && delaySec <= 600, "delay: " + delaySec);

        TasksSink.unregisterMemoryClient("template");
    }

    @Test
    public void testBufferWhenTargetOmitted() throws Exception {
        final MemoryTasksClient client = register("buffer");
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs:
                      - input
                    parameters:
                      queue: %s
                      endpoint: memory://buffer
                """.formatted(QUEUE);
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("CREATED", element.getPrimitiveValue("state"));
                Assertions.assertNull(element.getPrimitiveValue("url"));
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(2, client.buffered.size());
        Assertions.assertEquals(0, client.created.size());
        for(final ByteString body : client.buffered) {
            final JsonObject json = JsonParser.parseString(body.toString(StandardCharsets.UTF_8)).getAsJsonObject();
            Assertions.assertTrue(json.has("user_id"));
            Assertions.assertTrue(json.has("note")); // omitNulls defaults to false -> null kept
        }
        TasksSink.unregisterMemoryClient("buffer");
    }

    @Test
    public void testMaxBytesFailureIsRouted() throws Exception {
        final MemoryTasksClient client = register("maxBytes");
        // element 1 (note=hello) exceeds maxBytes, element 2 (note=null, omitted) fits
        final String configYaml = SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    failFast: false
                    inputs:
                      - input
                    parameters:
                      queue: %s
                      endpoint: memory://maxBytes
                      target:
                        url: https://api.example.com/x
                      body:
                        omitNulls: true
                        maxBytes: 92
                """.formatted(QUEUE);
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Integer> states = new HashMap<>();
            for(final MElement element : elements) {
                states.merge((String) element.getPrimitiveValue("state"), 1, Integer::sum);
                if("FAILED".equals(element.getPrimitiveValue("state"))) {
                    Assertions.assertTrue(((String) element.getPrimitiveValue("error")).contains("maxBytes"));
                }
            }
            Assertions.assertEquals(1, states.get("CREATED"));
            Assertions.assertEquals(1, states.get("FAILED"));
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(1, client.created.size());
        TasksSink.unregisterMemoryClient("maxBytes");
    }

    @Test
    public void testValidation() {
        // illegal queue
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: myqueue
                """)));
        // scheduleTime and delay are exclusive, dispatchDeadline out of range, constant id
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      task:
                        id: constant
                        scheduleTime: "${remind_at}"
                        delay: 10m
                        dispatchDeadline: 1h
                """.formatted(QUEUE))));
        // template format requires template
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      body:
                        format: template
                """.formatted(QUEUE))));
    }

    private static final String BATCH_SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - tenant: t1
                      id: a
                    - tenant: t1
                      id: b
                    - tenant: t1
                      id: c
                    - tenant: t2
                      id: d
                    - tenant: t2
                      id: e
                schema:
                  fields:
                    - name: tenant
                      type: string
                    - name: id
                      type: string
            """;

    @Test
    public void testBatchJsonWithKey() throws Exception {
        final MemoryTasksClient client = register("batchJson");
        final String configYaml = BATCH_SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs:
                      - input
                    parameters:
                      queue: projects/myproject/locations/asia-northeast1/queues/${tenant}
                      endpoint: memory://batchJson
                      target:
                        url: https://api.example.com/${tenant}/bulk
                        headers:
                          X-Size: ${size}
                      batch:
                        maxSize: 2
                        key: "${tenant}"
                """;
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            long total = 0;
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("CREATED", element.getPrimitiveValue("state"));
                total += (Long) element.getPrimitiveValue("elementCount");
            }
            // t1: 3 elements -> 2 tasks, t2: 2 elements -> 1 task
            Assertions.assertEquals(3, count);
            Assertions.assertEquals(5, total);
            return null;
        });
        pipeline.run();

        final List<Task> tasks = new ArrayList<>(client.created);
        Assertions.assertEquals(3, tasks.size());
        for(final Task task : tasks) {
            final String url = task.getHttpRequest().getUrl();
            final String tenant = url.contains("/t1/") ? "t1" : "t2";
            Assertions.assertEquals("https://api.example.com/" + tenant + "/bulk", url);
            final var array = JsonParser.parseString(task.getHttpRequest().getBody().toString(StandardCharsets.UTF_8)).getAsJsonArray();
            Assertions.assertEquals(String.valueOf(array.size()), task.getHttpRequest().getHeadersMap().get("X-Size"));
            Assertions.assertTrue(array.size() >= 1 && array.size() <= 2);
            for(final var e : array) {
                Assertions.assertEquals(tenant, e.getAsJsonObject().get("tenant").getAsString());
            }
        }
        Assertions.assertTrue(client.createdQueues.stream().allMatch(q -> q.endsWith("/queues/t1") || q.endsWith("/queues/t2")));
        TasksSink.unregisterMemoryClient("batchJson");
    }

    @Test
    public void testBatchTemplateAndSplitOnMaxBytes() throws Exception {
        final MemoryTasksClient client = register("batchSplit");
        // no key -> random shards; maxSize 5 would pack everything into one task per shard,
        // but body.maxBytes forces recursive halving down to bodies of <= 2 ids
        final String configYaml = BATCH_SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs:
                      - input
                    parameters:
                      queue: %s
                      endpoint: memory://batchSplit
                      target:
                        url: https://api.example.com/bulk
                      body:
                        template: '[<#list elements as e>"${e.id}"<#sep>,</#list>]'
                        maxBytes: 9
                      batch:
                        maxSize: 5
                        shards: 1
                """.formatted(QUEUE);
        final Config config = Config.load(configYaml);
        final MCollection output = MPipeline.apply(pipeline, config).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            long total = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals("CREATED", element.getPrimitiveValue("state"));
                Assertions.assertTrue((Long) element.getPrimitiveValue("bytes") <= 9);
                total += (Long) element.getPrimitiveValue("elementCount");
            }
            Assertions.assertEquals(5, total);
            return null;
        });
        pipeline.run();

        final Set<String> ids = new TreeSet<>();
        for(final Task task : client.created) {
            final String body = task.getHttpRequest().getBody().toString(StandardCharsets.UTF_8);
            Assertions.assertTrue(body.length() <= 9, body);
            for(final var e : JsonParser.parseString(body).getAsJsonArray()) {
                ids.add(e.getAsString());
            }
        }
        Assertions.assertEquals(Set.of("a", "b", "c", "d", "e"), ids);
        TasksSink.unregisterMemoryClient("batchSplit");
    }

    @Test
    public void testBatchValidationRejectsNonKeyTemplates() {
        // url references 'id' which is not part of batch.key
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(BATCH_SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      target:
                        url: https://api.example.com/${id}
                      batch:
                        maxSize: 10
                        key: "${tenant}"
                """.formatted(QUEUE))));
        // batch without maxSize/maxBytes
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, Config.load(BATCH_SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      batch:
                        key: "${tenant}"
                """.formatted(QUEUE))));
    }

    @Test
    public void testAvroBodies() throws Exception {
        final MemoryTasksClient single = register("avroSingle");
        final String singleYaml = BATCH_SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      endpoint: memory://avroSingle
                      target:
                        url: https://api.example.com/avro
                      body:
                        format: avro
                """.formatted(QUEUE);
        MPipeline.apply(pipeline, Config.load(singleYaml));
        pipeline.run();
        Assertions.assertEquals(5, single.created.size());

        // decode one binary record with the schema derived from the fields
        final org.apache.avro.Schema avroSchema = com.mercari.solution.module.Schema.builder()
                .withField(com.mercari.solution.module.Schema.Field.of("tenant", com.mercari.solution.module.Schema.FieldType.STRING))
                .withField(com.mercari.solution.module.Schema.Field.of("id", com.mercari.solution.module.Schema.FieldType.STRING))
                .build().getAvroSchema();
        final Set<String> ids = new TreeSet<>();
        for(final Task task : single.created) {
            final org.apache.avro.io.BinaryDecoder decoder = org.apache.avro.io.DecoderFactory.get()
                    .binaryDecoder(task.getHttpRequest().getBody().toByteArray(), null);
            final org.apache.avro.generic.GenericRecord record =
                    new org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericRecord>(avroSchema).read(null, decoder);
            ids.add(record.get("id").toString());
        }
        Assertions.assertEquals(Set.of("a", "b", "c", "d", "e"), ids);
        TasksSink.unregisterMemoryClient("avroSingle");

        // batch: one Avro container file per task
        final MemoryTasksClient batch = register("avroBatch");
        final TestPipeline pipeline2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final String batchYaml = BATCH_SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      endpoint: memory://avroBatch
                      target:
                        url: https://api.example.com/avro
                      body:
                        format: avro
                      batch:
                        maxSize: 10
                        shards: 1
                """.formatted(QUEUE);
        MPipeline.apply(pipeline2, Config.load(batchYaml));
        pipeline2.run();
        Assertions.assertEquals(1, batch.created.size());
        final Set<String> batchIds = new TreeSet<>();
        try(final org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> reader =
                    new org.apache.avro.file.DataFileReader<>(
                            new org.apache.avro.file.SeekableByteArrayInput(batch.created.peek().getHttpRequest().getBody().toByteArray()),
                            new org.apache.avro.generic.GenericDatumReader<>())) {
            for(final org.apache.avro.generic.GenericRecord record : reader) {
                batchIds.add(record.get("id").toString());
            }
        }
        Assertions.assertEquals(Set.of("a", "b", "c", "d", "e"), batchIds);
        TasksSink.unregisterMemoryClient("avroBatch");
    }

    /** Async client: completes createTask on a background thread after a short delay. */
    static class AsyncMemoryTasksClient extends MemoryTasksClient {

        final java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxInFlight = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.ScheduledExecutorService executor = java.util.concurrent.Executors.newScheduledThreadPool(8);

        @Override
        public com.google.api.core.ApiFuture<Task> createTaskAsync(final String queue, final Task task) {
            final int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            final com.google.api.core.SettableApiFuture<Task> future = com.google.api.core.SettableApiFuture.create();
            executor.schedule(() -> {
                Task created = null;
                Throwable error = null;
                try {
                    created = createTask(queue, task);
                } catch (final Throwable e) {
                    error = e;
                }
                // decrement BEFORE completing the future: the DoFn may submit the next call as soon
                // as the future is done
                inFlight.decrementAndGet();
                if(error != null) {
                    future.setException(error);
                } else {
                    future.set(created);
                }
            }, 50, java.util.concurrent.TimeUnit.MILLISECONDS);
            return future;
        }
    }

    @Test
    public void testConcurrentCreate() throws Exception {
        final AsyncMemoryTasksClient client = new AsyncMemoryTasksClient();
        TasksSink.registerMemoryClient("concurrent", client);
        // 5 elements, ids a..e; 'a' and 'b' collide with pre-registered names -> ALREADY_EXISTS via the async path
        client.names.add(QUEUE + "/tasks/a");
        client.names.add(QUEUE + "/tasks/b");
        final String configYaml = BATCH_SOURCE_YAML + """
                sinks:
                  - name: tasks
                    module: tasks
                    inputs: [input]
                    parameters:
                      queue: %s
                      endpoint: memory://concurrent
                      concurrency: 4
                      target:
                        url: https://api.example.com/${id}
                      task:
                        id: "${id}"
                        hashId: false
                """.formatted(QUEUE);
        final MCollection output = MPipeline.apply(pipeline, Config.load(configYaml)).get("tasks");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Integer> states = new HashMap<>();
            final Set<String> names = new TreeSet<>();
            for(final MElement element : elements) {
                states.merge((String) element.getPrimitiveValue("state"), 1, Integer::sum);
                names.add((String) element.getPrimitiveValue("taskName"));
            }
            Assertions.assertEquals(3, states.get("CREATED"));
            Assertions.assertEquals(2, states.get("ALREADY_EXISTS"));
            Assertions.assertEquals(5, names.size());
            return null;
        });
        pipeline.run();

        Assertions.assertEquals(3, client.created.size());
        // the async path was used; no upper bound is asserted because DirectRunner runs several
        // bundles (= DoFn instances, each with its own in-flight window) against this shared client
        Assertions.assertTrue(client.maxInFlight.get() >= 1, "maxInFlight: " + client.maxInFlight.get());
        TasksSink.unregisterMemoryClient("concurrent");
    }

    @Test
    public void testHelpers() {
        Assertions.assertEquals(Duration.ofMinutes(10), TasksSink.parseDuration("PT10M"));
        Assertions.assertEquals(Duration.ofMinutes(10), TasksSink.parseDuration("10m"));
        Assertions.assertEquals(Duration.ofHours(2), TasksSink.parseDuration("2h"));
        Assertions.assertEquals(Duration.ofSeconds(30), TasksSink.parseDuration("30s"));
        Assertions.assertEquals(Duration.ofDays(1), TasksSink.parseDuration("1d"));
        Assertions.assertEquals(Duration.ofMillis(500), TasksSink.parseDuration("500ms"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> TasksSink.parseDuration("10 minutes"));

        Assertions.assertEquals(Instant.parse("2030-01-01T00:00:00Z"), TasksSink.toInstant("2030-01-01T00:00:00Z"));
        Assertions.assertEquals(Instant.ofEpochSecond(1_700_000_000L), TasksSink.toInstant(1_700_000_000L));
        Assertions.assertEquals(Instant.ofEpochMilli(1_700_000_000_000L), TasksSink.toInstant(1_700_000_000_000L));
        Assertions.assertEquals(Instant.ofEpochMilli(1_700_000_000_000L), TasksSink.toInstant(1_700_000_000_000_000L));
        Assertions.assertEquals(Instant.ofEpochSecond(1_700_000_000L), TasksSink.toInstant("1700000000"));

        Assertions.assertEquals(64, TasksSink.sha256Hex("abc").length());
        Assertions.assertEquals("https://a/b", TasksSink.stripQuery("https://a/b?x=1"));

        final JsonObject json = JsonParser.parseString("{\"a\":null,\"b\":{\"c\":null,\"d\":1},\"e\":[1,null]}").getAsJsonObject();
        Assertions.assertEquals("{\"b\":{\"d\":1},\"e\":[1,null]}", com.mercari.solution.util.pipeline.outbound.RequestRenderer.omitNulls(json).toString());
    }

}
