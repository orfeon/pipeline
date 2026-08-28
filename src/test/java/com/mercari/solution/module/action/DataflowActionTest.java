package com.mercari.solution.module.action;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.dataflow.v1beta3.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DataflowActionTest {

    private static final String SOURCE_YAML = """
            sources:
              - name: input
                module: create
                parameters:
                  type: element
                  elements:
                    - table: users
                    - table: items
                schema:
                  fields:
                    - name: table
                      type: string
            """;

    /** In-memory Dataflow: jobs advance one state per getJob poll along a scripted path. */
    static class MemoryDataflowClient implements DataflowAction.DataflowClient {

        final Map<String, Job> jobs = new ConcurrentHashMap<>();
        final Map<String, Deque<JobState>> transitions = new ConcurrentHashMap<>();
        final Map<String, LaunchFlexTemplateParameter> launches = new ConcurrentHashMap<>();
        final ConcurrentLinkedQueue<String> ops = new ConcurrentLinkedQueue<>();
        final AtomicInteger counter = new AtomicInteger(0);
        final List<JobMessage> messages = new ArrayList<>();
        /** States a newly launched job walks through on successive polls. */
        List<JobState> launchPath = List.of(JobState.JOB_STATE_PENDING, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_DONE);
        JobType launchType = JobType.JOB_TYPE_BATCH;

        Job put(final String id, final String name, final JobState state, final long createSeconds) {
            final Job job = Job.newBuilder()
                    .setId(id).setName(name).setProjectId("myproject").setLocation("asia-northeast1")
                    .setType(JobType.JOB_TYPE_BATCH)
                    .setCurrentState(state)
                    .setCreateTime(Timestamp.newBuilder().setSeconds(createSeconds))
                    .build();
            jobs.put(id, job);
            return job;
        }

        @Override
        public Job launchFlexTemplate(final String project, final String region, final LaunchFlexTemplateParameter parameter) {
            ops.add("launch");
            for(final Job job : jobs.values()) {
                if(job.getName().equals(parameter.getJobName()) && !DataflowActionTest.isTerminal(job.getCurrentState())) {
                    throw new AlreadyExistsException("already exists", null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false);
                }
            }
            final String id = "job-" + counter.incrementAndGet();
            launches.put(id, parameter);
            final Job job = put(id, parameter.getJobName(), launchPath.getFirst(), 1000 + counter.get())
                    .toBuilder().setType(launchType).build();
            jobs.put(id, job);
            transitions.put(id, new ArrayDeque<>(launchPath.subList(1, launchPath.size())));
            // like the real API: the launch response carries no type yet (the graph is not built while queued)
            return job.toBuilder().setType(JobType.JOB_TYPE_UNKNOWN).build();
        }

        @Override
        public Job getJob(final String project, final String region, final String jobId, final JobView view) {
            ops.add("get:" + jobId);
            final Job job = jobs.get(jobId);
            if(job == null) {
                throw new com.google.api.gax.rpc.NotFoundException("not found: " + jobId, null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
            }
            final Deque<JobState> path = transitions.get(jobId);
            if(path != null && !path.isEmpty()) {
                final Job next = job.toBuilder().setCurrentState(path.poll()).build();
                jobs.put(jobId, next);
                return next;
            }
            return job;
        }

        @Override
        public List<Job> listJobs(final String project, final String region, final ListJobsRequest.Filter filter, final int limit) {
            ops.add("list:" + filter);
            return jobs.values().stream()
                    .filter(j -> switch (filter) {
                        case ACTIVE -> !DataflowActionTest.isTerminal(j.getCurrentState());
                        case TERMINATED -> DataflowActionTest.isTerminal(j.getCurrentState());
                        default -> true;
                    })
                    .limit(limit)
                    .toList();
        }

        @Override
        public Job updateJob(final String project, final String region, final String jobId, final Job job, final FieldMask updateMask) {
            ops.add("update:" + jobId + ":" + (updateMask == null ? "" : String.join(",", updateMask.getPathsList())));
            final Job.Builder b = jobs.get(jobId).toBuilder();
            if(job.getRequestedState() != JobState.JOB_STATE_UNKNOWN) {
                b.setRequestedState(job.getRequestedState());
                final JobState target = job.getRequestedState();
                final JobState intermediate = JobState.JOB_STATE_DRAINED.equals(target) ? JobState.JOB_STATE_DRAINING : JobState.JOB_STATE_CANCELLING;
                transitions.putIfAbsent(jobId, new ArrayDeque<>(List.of(intermediate, target)));
            }
            if(job.hasRuntimeUpdatableParams()) {
                b.setRuntimeUpdatableParams(job.getRuntimeUpdatableParams());
            }
            jobs.put(jobId, b.build());
            return b.build();
        }

        @Override
        public List<JobMessage> listJobMessages(final String project, final String region, final String jobId, final JobMessageImportance minimumImportance, final int limit) {
            ops.add("messages:" + jobId);
            return messages;
        }

        @Override
        public void close() {}
    }

    static boolean isTerminal(final JobState state) {
        return switch (state) {
            case JOB_STATE_DONE, JOB_STATE_FAILED, JOB_STATE_CANCELLED, JOB_STATE_DRAINED, JOB_STATE_UPDATED -> true;
            default -> false;
        };
    }

    private static MemoryDataflowClient register(final String name) {
        final MemoryDataflowClient client = new MemoryDataflowClient();
        DataflowAction.registerMemoryClient(name, client);
        return client;
    }

    private static MCollection run(final TestPipeline pipeline, final String yaml, final String step) throws Exception {
        return MPipeline.apply(pipeline, Config.load(yaml)).get(step);
    }

    private static JsonObject payload(final MElement e) {
        return JsonParser.parseString((String) e.getPrimitiveValue("payload")).getAsJsonObject();
    }

    @Test
    public void testLaunchWaitsUntilDoneAndAdoptsExisting() throws Exception {
        final MemoryDataflowClient client = register("launch");
        final String yaml = """
                actions:
                  - name: launch
                    module: dataflow
                    operation: flexTemplates.launch
                    parameters:
                      projectId: myproject
                      region: asia-northeast1
                      endpoint: memory://launch
                      jobName: backfill-20260828
                      containerSpecGcsPath: gs://bucket/templates/dataflow.json
                      config: gs://bucket/configs/child.yaml
                      args:
                        table: users
                        run_id: r1
                      parameters:
                        extra: x
                      environment:
                        maxWorkers: 5
                        serviceAccountEmail: child@myproject.iam.gserviceaccount.com
                        additionalUserLabels:
                          team: data
                """;
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p1, yaml, "launch").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("dataflow", e.getPrimitiveValue("service"));
                Assertions.assertEquals("flexTemplates.launch", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("job-1", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_DONE", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals("backfill-20260828", payload.get("name").getAsString());
                Assertions.assertEquals("JOB_STATE_DONE", payload.get("currentState").getAsString());
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p1.run();
        final LaunchFlexTemplateParameter launched = client.launches.get("job-1");
        Assertions.assertEquals("gs://bucket/templates/dataflow.json", launched.getContainerSpecGcsPath());
        Assertions.assertEquals("gs://bucket/configs/child.yaml", launched.getParametersMap().get("config"));
        Assertions.assertEquals("users", launched.getParametersMap().get("args.table"));
        Assertions.assertEquals("r1", launched.getParametersMap().get("args.run_id"));
        Assertions.assertEquals("x", launched.getParametersMap().get("extra"));
        Assertions.assertEquals(5, launched.getEnvironment().getMaxWorkers());
        Assertions.assertEquals("child@myproject.iam.gserviceaccount.com", launched.getEnvironment().getServiceAccountEmail());
        Assertions.assertEquals("data", launched.getEnvironment().getAdditionalUserLabelsMap().get("team"));
        // polled until DONE
        Assertions.assertTrue(client.ops.stream().filter(o -> o.equals("get:job-1")).count() >= 2);

        // second run while the same-named job is still active -> adopted (payload.adopted) and waited for
        client.put("job-9", "backfill-20260828", JobState.JOB_STATE_RUNNING, 5000);
        client.transitions.put("job-9", new ArrayDeque<>(List.of(JobState.JOB_STATE_DONE)));
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, yaml, "launch").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("job-9", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_DONE", e.getPrimitiveValue("state"));
                Assertions.assertTrue(payload(e).get("adopted").getAsBoolean());
                Assertions.assertEquals("JOB_STATE_DONE", payload(e).get("currentState").getAsString());
            }
            return null;
        });
        p2.run();
        Assertions.assertEquals(1, client.launches.size());

        // adopted without wait -> EXISTS
        client.put("job-10", "backfill-20260828", JobState.JOB_STATE_RUNNING, 6000);
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, yaml + "      wait: false\n", "launch").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("EXISTS", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p3.run();
        DataflowAction.unregisterMemoryClient("launch");
    }

    @Test
    public void testLaunchStreamingWaitsUntilRunning() throws Exception {
        final MemoryDataflowClient client = register("streaming");
        client.launchType = JobType.JOB_TYPE_STREAMING;
        client.launchPath = List.of(JobState.JOB_STATE_PENDING, JobState.JOB_STATE_PENDING, JobState.JOB_STATE_RUNNING);
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p, """
                actions:
                  - name: launch
                    module: dataflow
                    operation: flexTemplates.launch
                    parameters:
                      projectId: myproject
                      region: asia-northeast1
                      endpoint: memory://streaming
                      jobName: consumer
                      containerSpecGcsPath: gs://bucket/templates/dataflow.json
                      config: gs://bucket/configs/consumer.yaml
                """, "launch").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("JOB_STATE_RUNNING", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p.run();
        DataflowAction.unregisterMemoryClient("streaming");
    }

    @Test
    public void testRejectedRequestIsNotRetried() throws Exception {
        final MemoryDataflowClient client = register("rejected");
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p, """
                actions:
                  - name: get
                    module: dataflow
                    operation: jobs.get
                    failFast: true
                    retry: { maxAttempts: 3, initialBackoff: 10ms }
                    parameters: { projectId: myproject, region: asia-northeast1, endpoint: memory://rejected, jobId: missing }
                """, "get");
        Assertions.assertThrows(Exception.class, p::run);
        Assertions.assertEquals(1, client.ops.stream().filter(o -> o.equals("get:missing")).count(), client.ops.toString());
        DataflowAction.unregisterMemoryClient("rejected");
    }

    @Test
    public void testLaunchFailedJobIsNonRetryableWithMessages() throws Exception {
        final MemoryDataflowClient client = register("failed");
        client.launchPath = List.of(JobState.JOB_STATE_PENDING, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_FAILED);
        client.messages.add(JobMessage.newBuilder().setMessageImportance(JobMessageImportance.JOB_MESSAGE_ERROR).setMessageText("Workflow failed. Causes: boom").build());
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p, """
                actions:
                  - name: launch
                    module: dataflow
                    operation: flexTemplates.launch
                    failFast: true
                    parameters:
                      projectId: myproject
                      region: asia-northeast1
                      endpoint: memory://failed
                      jobName: broken
                      containerSpecGcsPath: gs://bucket/templates/dataflow.json
                """, "launch");
        final Exception e = Assertions.assertThrows(Exception.class, p::run);
        final StringBuilder message = new StringBuilder();
        for(Throwable t = e; t != null; t = t.getCause()) {
            message.append(t.getMessage()).append(" ");
        }
        Assertions.assertTrue(message.toString().contains("boom"), message.toString());
        DataflowAction.unregisterMemoryClient("failed");
    }

    @Test
    public void testPerElementLaunchNoWaitThenCollectWait() throws Exception {
        final MemoryDataflowClient client = register("fanout");
        final String yaml = SOURCE_YAML + """
                actions:
                  - name: launch
                    module: dataflow
                    operation: flexTemplates.launch
                    trigger: perElement
                    inputs: [input]
                    parameters:
                      projectId: myproject
                      region: asia-northeast1
                      endpoint: memory://fanout
                      jobName: backfill-${table}
                      containerSpecGcsPath: gs://bucket/templates/dataflow.json
                      args:
                        table: ${table}
                      wait: false
                  - name: wait
                    module: dataflow
                    operation: jobs.wait
                    trigger: collect
                    inputs: [launch]
                    parameters:
                      projectId: myproject
                      region: asia-northeast1
                      endpoint: memory://fanout
                      jobIdField: jobId
                """;
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final Map<String, MCollection> outputs = MPipeline.apply(p, Config.load(yaml));
        PAssert.that(outputs.get("launch").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertFalse("JOB_STATE_DONE".equals(e.getPrimitiveValue("state")), "wait: false must not wait");
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        PAssert.that(outputs.get("wait").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement e : elements) {
                count++;
                Assertions.assertEquals("jobs.wait", e.getPrimitiveValue("operation"));
                Assertions.assertEquals("DONE", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals(2, payload.get("count").getAsInt());
                Assertions.assertEquals("JOB_STATE_DONE", payload.getAsJsonObject("firstJob").get("currentState").getAsString());
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        p.run();
        Assertions.assertEquals(2, client.launches.size());
        final Set<String> names = new HashSet<>();
        client.launches.values().forEach(l -> names.add(l.getJobName() + "=" + l.getParametersMap().get("args.table")));
        Assertions.assertEquals(Set.of("backfill-users=users", "backfill-items=items"), names);
        DataflowAction.unregisterMemoryClient("fanout");
    }

    @Test
    public void testGetListUpdateMessages() throws Exception {
        final MemoryDataflowClient client = register("ops");
        client.put("j-old", "consumer", JobState.JOB_STATE_DONE, 100);
        client.put("j-new", "consumer", JobState.JOB_STATE_RUNNING, 200);
        client.put("j-other", "other", JobState.JOB_STATE_RUNNING, 300);

        // jobs.get by name -> latest
        final TestPipeline p1 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p1, """
                actions:
                  - name: get
                    module: dataflow
                    operation: jobs.get
                    parameters: { projectId: myproject, region: asia-northeast1, endpoint: memory://ops, jobName: consumer }
                """, "get").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("j-new", e.getPrimitiveValue("jobId"));
                Assertions.assertEquals("JOB_STATE_RUNNING", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p1.run();

        // jobs.list ACTIVE filtered by name, with skipWhen on count
        final TestPipeline p2 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p2, """
                actions:
                  - name: list
                    module: dataflow
                    operation: jobs.list
                    skipWhen: payload.`count` > 0
                    parameters: { projectId: myproject, region: asia-northeast1, endpoint: memory://ops, filter: ACTIVE, jobName: consumer }
                """, "list").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("SKIPPED", e.getPrimitiveValue("state"));
                final JsonObject payload = payload(e);
                Assertions.assertEquals(1, payload.get("count").getAsInt());
                Assertions.assertEquals("j-new", payload.getAsJsonObject("firstJob").get("id").getAsString());
            }
            return null;
        });
        p2.run();

        // jobs.update drain (waits until DRAINED) + runtimeUpdatableParams
        final TestPipeline p3 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p3, """
                actions:
                  - name: drain
                    module: dataflow
                    operation: jobs.update
                    parameters:
                      projectId: myproject
                      region: asia-northeast1
                      endpoint: memory://ops
                      jobId: j-new
                      requestedState: JOB_STATE_DRAINED
                      runtimeUpdatableParams:
                        maxNumWorkers: 3
                """, "drain").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                Assertions.assertEquals("JOB_STATE_DRAINED", e.getPrimitiveValue("state"));
            }
            return null;
        });
        p3.run();
        Assertions.assertTrue(client.ops.contains("update:j-new:requested_state,runtime_updatable_params.max_num_workers"), client.ops.toString());
        Assertions.assertEquals(3, client.jobs.get("j-new").getRuntimeUpdatableParams().getMaxNumWorkers());

        // drain requested but the job ends CANCELLED (someone else cancelled it) -> failure
        client.put("j-drain", "drainme", JobState.JOB_STATE_RUNNING, 400);
        client.transitions.put("j-drain", new ArrayDeque<>(List.of(JobState.JOB_STATE_CANCELLED)));
        final TestPipeline p3b = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        run(p3b, """
                actions:
                  - name: drain
                    module: dataflow
                    operation: jobs.update
                    failFast: true
                    parameters: { projectId: myproject, region: asia-northeast1, endpoint: memory://ops, jobId: j-drain, requestedState: DRAINED }
                """, "drain");
        final StringBuilder message = new StringBuilder();
        for(Throwable t = Assertions.assertThrows(Exception.class, p3b::run); t != null; t = t.getCause()) {
            message.append(t.getMessage()).append(" ");
        }
        Assertions.assertTrue(message.toString().contains("cancelled"), message.toString());

        // jobs.messages.list
        client.messages.add(JobMessage.newBuilder().setMessageImportance(JobMessageImportance.JOB_MESSAGE_ERROR).setMessageText("oops").build());
        final TestPipeline p4 = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        PAssert.that(run(p4, """
                actions:
                  - name: messages
                    module: dataflow
                    operation: jobs.messages.list
                    parameters: { projectId: myproject, region: asia-northeast1, endpoint: memory://ops, jobId: j-other }
                """, "messages").getCollection()).satisfies(elements -> {
            for(final MElement e : elements) {
                final JsonObject payload = payload(e);
                Assertions.assertEquals(1, payload.get("count").getAsInt());
                Assertions.assertEquals("oops", payload.getAsJsonArray("messages").get(0).getAsJsonObject().get("messageText").getAsString());
                Assertions.assertEquals("JOB_STATE_RUNNING", payload.get("currentState").getAsString());
            }
            return null;
        });
        p4.run();
        DataflowAction.unregisterMemoryClient("ops");
    }

    @Test
    public void testValidation() {
        final String base = """
                actions:
                  - name: step
                    module: dataflow
                    operation: %s
                    parameters:
                      projectId: myproject
                      region: asia-northeast1
                      %s
                """;
        assertInvalid(base.formatted("flexTemplates.launch", "jobName: x"), "containerSpecGcsPath is required");
        assertInvalid(base.formatted("flexTemplates.launch", "containerSpecGcsPath: gs://b/t.json\n      jobName: Bad_Name"), "jobName must match");
        assertInvalid(base.formatted("jobs.get", "view: JOB_VIEW_SUMMARY"), "jobId or jobName is required");
        assertInvalid(base.formatted("jobs.update", "jobId: j"), "requires requestedState and/or runtimeUpdatableParams");
        assertInvalid(base.formatted("jobs.update", "jobId: j\n      requestedState: JOB_STATE_RUNNING"), "requestedState must be");
        assertInvalid(base.formatted("jobs.wait", "jobIdField: jobId"), "jobIdField requires trigger: collect");
        assertInvalid(base.formatted("jobs.list", "filter: RUNNING"), "filter must be one of");
        assertInvalid("""
                actions:
                  - name: step
                    module: dataflow
                    operation: jobs.get
                    parameters:
                      jobId: j
                """, "region is required");
    }

    private static void assertInvalid(final String yaml, final String expected) {
        final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> run(p, yaml, "step"));
        Assertions.assertTrue(e.getMessage().contains(expected), e.getMessage());
    }

    @Test
    public void testPayloadNumbers() {
        final Job job = Job.newBuilder().setId("j").setCurrentState(JobState.JOB_STATE_RUNNING)
                .setRuntimeUpdatableParams(RuntimeUpdatableParams.newBuilder().setMaxNumWorkers(7).setWorkerUtilizationHint(0.5))
                .build();
        final Map<String, Object> payload = com.mercari.solution.util.cloud.google.DataflowUtil.toPayload(job);
        Assertions.assertEquals("JOB_STATE_RUNNING", payload.get("currentState"));
        final Map<?, ?> rup = (Map<?, ?>) payload.get("runtimeUpdatableParams");
        Assertions.assertEquals(7L, rup.get("maxNumWorkers"));
        Assertions.assertEquals(0.5, rup.get("workerUtilizationHint"));
        Assertions.assertEquals("action-20", DataflowAction.defaultJobName("Action!").substring(0, 9));
    }

}
