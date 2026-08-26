package com.mercari.solution.module.action;

import com.mercari.solution.module.Action;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Action.Trigger;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.tasks.v2.CloudTasksClient;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import com.google.cloud.tasks.v2.ListTasksRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.cloud.tasks.v2.Queue;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Action service for Cloud Tasks queue operations — the control-plane counterpart of the
 * {@code tasks} sink. The sink enqueues data records as tasks; this action manages the queue
 * around them: create/update a queue with rate limits and retry config, pause / resume / purge,
 * wait until the queue has drained (so a later step can rely on every task having run), run or
 * delete a single task, or just read the queue state into the envelope payload.
 *
 * <p>Idempotency: {@code create} adopts an existing queue (ALREADY_EXISTS → state EXISTS),
 * {@code delete} / {@code deleteTask} treat NOT_FOUND as done, and pause / resume / purge /
 * update are naturally idempotent, so a retried bundle is harmless. {@code runTask} forces a
 * dispatch and may dispatch twice if the bundle is retried.
 */
@Action.Service(name = "tasks", operations = {
        "queues.create", "queues.update", "queues.delete", "queues.pause", "queues.resume", "queues.purge",
        "queues.get", "queues.waitForEmpty", "tasks.run", "tasks.delete"})
public class TasksAction implements ActionService {

    private static final Logger LOG = LoggerFactory.getLogger(TasksAction.class);

    private static final Pattern PATTERN_QUEUE = Pattern
            .compile("^(projects/[^/]+/locations/[^/]+)/queues/([^/]+)$");
    private static final Pattern PATTERN_SHORT_DURATION = Pattern.compile("^(\\d+)\\s*(ms|s|m|h|d)$");

    public static final String ENDPOINT_MEMORY_PREFIX = "memory://";

    public enum Op {
        create,
        update,
        delete,
        pause,
        resume,
        purge,
        get,
        waitForEmpty,
        runTask,
        deleteTask;

        static Op of(final String operation) {
            return switch (operation) {
                case "queues.create" -> create;
                case "queues.update" -> update;
                case "queues.delete" -> delete;
                case "queues.pause" -> pause;
                case "queues.resume" -> resume;
                case "queues.purge" -> purge;
                case "queues.get" -> get;
                case "queues.waitForEmpty" -> waitForEmpty;
                case "tasks.run" -> runTask;
                case "tasks.delete" -> deleteTask;
                default -> throw new IllegalModuleException("Not supported operation: " + operation);
            };
        }
    }

    public static class RateLimits implements Serializable {
        public Double maxDispatchesPerSecond;
        public Integer maxConcurrentDispatches;
    }

    public static class RetryConfig implements Serializable {
        public Integer maxAttempts;
        public String maxRetryDuration;
        public String minBackoff;
        public String maxBackoff;
        public Integer maxDoublings;
    }

    public static class Parameters implements Serializable {

        public Op op;
        public String queue;
        public String task;
        public RateLimits rateLimits;
        public RetryConfig retryConfig;
        public Long timeoutSeconds;
        public Long pollIntervalSeconds;
        public String endpoint;

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            final String prefix = "action module[" + name + "].parameters.";
            if(queue == null) {
                errorMessages.add(prefix + "queue must not be null");
            } else if(!TemplateUtil.isTemplateText(queue) && !PATTERN_QUEUE.matcher(queue).matches()) {
                errorMessages.add(prefix + "queue must be in format projects/{project}/locations/{location}/queues/{queue} but: " + queue);
            }
            if((Op.runTask.equals(op) || Op.deleteTask.equals(op)) && task == null) {
                errorMessages.add(prefix + "task must not be null for tasks.run / tasks.delete");
            }
            if(Op.update.equals(op) && rateLimits == null && retryConfig == null) {
                errorMessages.add(prefix + "queues.update requires rateLimits and/or retryConfig");
            }
            if(retryConfig != null) {
                for(final String d : Arrays.asList(retryConfig.maxRetryDuration, retryConfig.minBackoff, retryConfig.maxBackoff)) {
                    if(d != null) {
                        try {
                            parseDuration(d);
                        } catch (final IllegalArgumentException e) {
                            errorMessages.add(prefix + "retryConfig has an illegal duration: " + e.getMessage());
                        }
                    }
                }
            }
            if(timeoutSeconds != null && timeoutSeconds <= 0) {
                errorMessages.add(prefix + "timeoutSeconds must be positive");
            }
            if(pollIntervalSeconds != null && pollIntervalSeconds <= 0) {
                errorMessages.add(prefix + "pollIntervalSeconds must be positive");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(timeoutSeconds == null) {
                timeoutSeconds = 86400L;
            }
            if(pollIntervalSeconds == null) {
                pollIntervalSeconds = 10L;
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Client abstraction (gRPC in production, in-memory in tests via endpoint: memory://name)
    // ---------------------------------------------------------------------------------------

    public interface QueueClient extends AutoCloseable {
        Queue getQueue(String name);
        Queue createQueue(String parent, Queue queue);
        Queue updateQueue(Queue queue, FieldMask mask);
        void deleteQueue(String name);
        Queue pauseQueue(String name);
        Queue resumeQueue(String name);
        Queue purgeQueue(String name);
        /** Number of tasks currently in the queue, counting at most {@code limit}. */
        int countTasks(String queue, int limit);
        Task runTask(String name);
        void deleteTask(String name);
        @Override
        void close();
    }

    private static final Map<String, QueueClient> MEMORY_CLIENTS = new HashMap<>();

    public static void registerMemoryClient(final String name, final QueueClient client) {
        synchronized (MEMORY_CLIENTS) {
            MEMORY_CLIENTS.put(name, client);
        }
    }

    public static void unregisterMemoryClient(final String name) {
        synchronized (MEMORY_CLIENTS) {
            MEMORY_CLIENTS.remove(name);
        }
    }

    static QueueClient createClient(final Parameters parameters) throws IOException {
        if(parameters.endpoint != null && parameters.endpoint.startsWith(ENDPOINT_MEMORY_PREFIX)) {
            final String name = parameters.endpoint.substring(ENDPOINT_MEMORY_PREFIX.length());
            synchronized (MEMORY_CLIENTS) {
                final QueueClient client = MEMORY_CLIENTS.get(name);
                if(client == null) {
                    throw new IllegalStateException("in-memory tasks queue client is not registered: " + name);
                }
                return client;
            }
        }
        return new GrpcQueueClient(parameters);
    }

    static class GrpcQueueClient implements QueueClient {

        private final CloudTasksClient client;

        GrpcQueueClient(final Parameters parameters) throws IOException {
            final CloudTasksSettings.Builder builder = CloudTasksSettings.newBuilder();
            if(parameters.endpoint != null) {
                builder.setEndpoint(parameters.endpoint)
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
                                .setEndpoint(parameters.endpoint)
                                .setChannelConfigurator(b -> b.usePlaintext())
                                .build());
            } else {
                builder.setCredentialsProvider(GcpCredentialsCache::credentials);
            }
            this.client = CloudTasksClient.create(builder.build());
        }

        @Override
        public Queue getQueue(final String name) {
            return client.getQueue(name);
        }

        @Override
        public Queue createQueue(final String parent, final Queue queue) {
            return client.createQueue(parent, queue);
        }

        @Override
        public Queue updateQueue(final Queue queue, final FieldMask mask) {
            return client.updateQueue(queue, mask);
        }

        @Override
        public void deleteQueue(final String name) {
            client.deleteQueue(name);
        }

        @Override
        public Queue pauseQueue(final String name) {
            return client.pauseQueue(name);
        }

        @Override
        public Queue resumeQueue(final String name) {
            return client.resumeQueue(name);
        }

        @Override
        public Queue purgeQueue(final String name) {
            return client.purgeQueue(name);
        }

        @Override
        public int countTasks(final String queue, final int limit) {
            int count = 0;
            final ListTasksRequest request = ListTasksRequest.newBuilder()
                    .setParent(queue)
                    .setPageSize(Math.min(limit, 1000))
                    .build();
            for(final Task ignored : client.listTasks(request).iterateAll()) {
                count++;
                if(count >= limit) {
                    break;
                }
            }
            return count;
        }

        @Override
        public Task runTask(final String name) {
            return client.runTask(name);
        }

        @Override
        public void deleteTask(final String name) {
            client.deleteTask(name);
        }

        @Override
        public void close() {
            client.close();
        }
    }

    // ---------------------------------------------------------------------------------------

    private String name;
    private Trigger trigger;
    private String operation;
    private Parameters parameters;

    private transient QueueClient client;

    @Override
    public void configure(final String name, final Trigger trigger, final String operation, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.trigger = trigger;
        this.operation = operation;
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null) {
            throw new IllegalModuleException("action module[" + name + "].parameters must not be empty");
        }
        this.parameters.op = Op.of(operation);
        final List<String> errorMessages = this.parameters.validate(name);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
        this.parameters.setDefaults();
    }

    @Override
    public void setup() {
        try {
            this.client = createClient(parameters);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to create Cloud Tasks client", e);
        }
    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        final Map<String, Object> data = switch (trigger) {
            case perElement -> elements.getFirst().asPrimitiveMap();
            case once, collect -> Action.createCollectTemplateData(elements);
        };
        final String queue = template(parameters.queue, data);
        final Matcher matcher = PATTERN_QUEUE.matcher(queue);
        if(!matcher.matches()) {
            throw new IllegalArgumentException("rendered queue is illegal: " + queue);
        }
        final String parent = matcher.group(1);

        return switch (parameters.op) {
            case create -> {
                final Queue.Builder builder = Queue.newBuilder().setName(queue);
                applyRateLimits(builder);
                applyRetryConfig(builder);
                try {
                    final Queue created = client.createQueue(parent, builder.build());
                    yield result(operation, queue, "DONE", created);
                } catch (final AlreadyExistsException e) {
                    LOG.info("action module[{}] queue already exists, adopting: {}", name, queue);
                    yield result(operation, queue, "EXISTS", client.getQueue(queue));
                }
            }
            case update -> {
                final Queue.Builder builder = Queue.newBuilder().setName(queue);
                final FieldMask.Builder mask = FieldMask.newBuilder();
                if(applyRateLimits(builder)) {
                    mask.addPaths("rate_limits");
                }
                if(applyRetryConfig(builder)) {
                    mask.addPaths("retry_config");
                }
                yield result(operation, queue, "DONE", client.updateQueue(builder.build(), mask.build()));
            }
            case delete -> {
                try {
                    client.deleteQueue(queue);
                    yield ActionResult.of(operation, queue, "DONE", null);
                } catch (final NotFoundException e) {
                    yield ActionResult.of(operation, queue, "NOT_FOUND", null);
                }
            }
            case pause -> result(operation, queue, "DONE", client.pauseQueue(queue));
            case resume -> result(operation, queue, "DONE", client.resumeQueue(queue));
            case purge -> result(operation, queue, "DONE", client.purgeQueue(queue));
            case get -> result(operation, queue, "DONE", client.getQueue(queue));
            case waitForEmpty -> {
                final Instant deadline = Instant.now().plusSeconds(parameters.timeoutSeconds);
                final Instant started = Instant.now();
                int remaining;
                int polls = 0;
                while(true) {
                    remaining = client.countTasks(queue, 1000);
                    polls++;
                    if(remaining == 0) {
                        break;
                    }
                    if(Instant.now().isAfter(deadline)) {
                        throw new IllegalStateException("action module[" + name + "] timed out waiting for queue to drain: "
                                + queue + " (" + remaining + (remaining >= 1000 ? "+" : "") + " tasks remaining)");
                    }
                    LOG.info("action module[{}] waiting for queue {} to drain: {}{} tasks remaining",
                            name, queue, remaining, remaining >= 1000 ? "+" : "");
                    Thread.sleep(parameters.pollIntervalSeconds * 1000L);
                }
                final JsonObject payload = new JsonObject();
                payload.addProperty("polls", polls);
                payload.addProperty("waitedSeconds", java.time.Duration.between(started, Instant.now()).toSeconds());
                yield ActionResult.of(operation, queue, "DONE", payload.toString());
            }
            case runTask -> {
                final String taskName = taskName(queue, template(parameters.task, data));
                final Task task = client.runTask(taskName);
                yield ActionResult.of(operation, taskName, "DONE", JsonFormat.printer().omittingInsignificantWhitespace().print(task));
            }
            case deleteTask -> {
                final String taskName = taskName(queue, template(parameters.task, data));
                try {
                    client.deleteTask(taskName);
                    yield ActionResult.of(operation, taskName, "DONE", null);
                } catch (final NotFoundException e) {
                    yield ActionResult.of(operation, taskName, "NOT_FOUND", null);
                }
            }
        };
    }

    private boolean applyRateLimits(final Queue.Builder builder) {
        if(parameters.rateLimits == null) {
            return false;
        }
        final com.google.cloud.tasks.v2.RateLimits.Builder rl = com.google.cloud.tasks.v2.RateLimits.newBuilder();
        if(parameters.rateLimits.maxDispatchesPerSecond != null) {
            rl.setMaxDispatchesPerSecond(parameters.rateLimits.maxDispatchesPerSecond);
        }
        if(parameters.rateLimits.maxConcurrentDispatches != null) {
            rl.setMaxConcurrentDispatches(parameters.rateLimits.maxConcurrentDispatches);
        }
        builder.setRateLimits(rl);
        return true;
    }

    private boolean applyRetryConfig(final Queue.Builder builder) {
        if(parameters.retryConfig == null) {
            return false;
        }
        final com.google.cloud.tasks.v2.RetryConfig.Builder rc = com.google.cloud.tasks.v2.RetryConfig.newBuilder();
        if(parameters.retryConfig.maxAttempts != null) {
            rc.setMaxAttempts(parameters.retryConfig.maxAttempts);
        }
        if(parameters.retryConfig.maxRetryDuration != null) {
            rc.setMaxRetryDuration(toProtoDuration(parameters.retryConfig.maxRetryDuration));
        }
        if(parameters.retryConfig.minBackoff != null) {
            rc.setMinBackoff(toProtoDuration(parameters.retryConfig.minBackoff));
        }
        if(parameters.retryConfig.maxBackoff != null) {
            rc.setMaxBackoff(toProtoDuration(parameters.retryConfig.maxBackoff));
        }
        if(parameters.retryConfig.maxDoublings != null) {
            rc.setMaxDoublings(parameters.retryConfig.maxDoublings);
        }
        builder.setRetryConfig(rc);
        return true;
    }

    private static ActionResult result(final String op, final String queue, final String state, final Queue q) throws IOException {
        final String payload = q == null ? null : JsonFormat.printer().omittingInsignificantWhitespace().print(q);
        return ActionResult.of(op, queue, state, payload);
    }

    private static String taskName(final String queue, final String task) {
        return task.startsWith("projects/") ? task : queue + "/tasks/" + task;
    }

    private static String template(final String text, final Map<String, Object> data) {
        if(text == null || !TemplateUtil.isTemplateText(text)) {
            return text;
        }
        return TemplateUtil.executeStrictTemplate(text, data);
    }

    private static Duration toProtoDuration(final String text) {
        final java.time.Duration d = parseDuration(text);
        return Duration.newBuilder().setSeconds(d.getSeconds()).setNanos(d.getNano()).build();
    }

    static java.time.Duration parseDuration(final String text) {
        return com.mercari.solution.util.pipeline.outbound.Durations.parse(text);
    }

}
