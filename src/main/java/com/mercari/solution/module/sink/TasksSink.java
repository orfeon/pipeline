package com.mercari.solution.module.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.tasks.v2.*;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import com.mercari.solution.util.cloud.google.IAMUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.domain.text.template.StringFunctions;
import com.mercari.solution.util.pipeline.outbound.AuthProvider;
import com.mercari.solution.util.pipeline.outbound.BatchSpec;
import com.mercari.solution.util.pipeline.outbound.Durations;
import com.mercari.solution.util.pipeline.outbound.OutboundRequest;
import com.mercari.solution.util.pipeline.outbound.RequestRenderer;
import com.mercari.solution.util.pipeline.outbound.RequestSpec;
import freemarker.template.Template;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sink that enqueues each input element as a Google Cloud Tasks HTTP task.
 *
 * <p>Design (see docs/module/sink/tasks.md): one element = one task. Rate limiting and
 * target-side retries belong to the queue; the sink only retries the createTask RPC itself.
 * Idempotency is achieved with named tasks ({@code task.id} template). The sink emits one
 * control record per element (CREATED / ALREADY_EXISTS / FAILED) so downstream steps can
 * {@code waits} on it; failures are also routed to {@code failureSinks}.
 */
@Sink.Module(name="tasks")
public class TasksSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(TasksSink.class);



    private static final Pattern PATTERN_QUEUE = Pattern
            .compile("^projects/[^/]+/locations/[^/]+/queues/[^/]+$");
    private static final Pattern PATTERN_TASK_ID = Pattern.compile("^[A-Za-z0-9_-]{1,500}$");
    private static final Pattern PATTERN_SIMPLE_FIELD = Pattern.compile("^\\$\\{\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*}$");
    private static final String SCOPE_CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform";

    public static final String ENDPOINT_MEMORY_PREFIX = "memory://";

    public static class Parameters implements Serializable {

        private String queue;
        private RequestSpec.Target target;
        private RequestSpec.Body body;
        private TaskParameters task;
        private BatchSpec batch;
        private RetryParameters retry;
        private OnAlreadyExists onAlreadyExists;
        private Integer concurrency;
        private String endpoint;

        private void validate(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(concurrency != null && concurrency < 1) {
                errorMessages.add("parameters.concurrency must be >= 1 but: " + concurrency);
            }
            if(queue == null) {
                errorMessages.add("parameters.queue must not be null");
            } else if(!TemplateUtil.isTemplateText(queue) && !PATTERN_QUEUE.matcher(queue).matches()) {
                errorMessages.add("parameters.queue must be in format projects/{project}/locations/{location}/queues/{queue} but: " + queue);
            }
            if(target != null) {
                errorMessages.addAll(target.validate("parameters.target", inputSchema, true));
                if(target.auth != null && !target.auth.isNone()) {
                    if(!AuthProvider.Type.gcpOidc.equals(target.auth.type) && !AuthProvider.Type.gcpOauth.equals(target.auth.type)) {
                        errorMessages.add("parameters.target.auth.type must be none, gcpOidc or gcpOauth for Cloud Tasks (the queue attaches the token) but: " + target.auth.type);
                    }
                    if(target.auth.serviceAccount == null && !IAMUtil.isOnGcp()) {
                        errorMessages.add("parameters.target.auth.serviceAccount must not be null when not running on GCP (metadata server unavailable)");
                    }
                }
            }
            if(body != null) {
                errorMessages.addAll(body.validate("parameters.body", inputSchema));
            }
            if(task != null) {
                errorMessages.addAll(task.validate());
            }
            if(retry != null) {
                errorMessages.addAll(retry.validate());
            }
            if(batch != null) {
                errorMessages.addAll(batch.validate("parameters.batch"));
                final Map<String, String> perTaskTemplates = new LinkedHashMap<>();
                perTaskTemplates.put("parameters.queue", queue);
                if(target != null) {
                    perTaskTemplates.put("parameters.target.url", target.url);
                    if(target.headers != null) {
                        target.headers.forEach((k, v) -> perTaskTemplates.put("parameters.target.headers." + k, v));
                    }
                    if(target.params != null) {
                        target.params.forEach((k, v) -> perTaskTemplates.put("parameters.target.params." + k, v));
                    }
                }
                if(task != null) {
                    perTaskTemplates.put("parameters.task.id", task.id);
                    perTaskTemplates.put("parameters.task.scheduleTime", task.scheduleTime);
                    perTaskTemplates.put("parameters.task.delay", task.delay);
                }
                errorMessages.addAll(batch.validateKeyConstraint("parameters.batch", inputSchema, perTaskTemplates));
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(body == null) {
                body = new RequestSpec.Body();
            }
            body.setDefaults();
            if(task == null) {
                task = new TaskParameters();
            }
            task.setDefaults();
            if(retry == null) {
                retry = new RetryParameters();
            }
            retry.setDefaults();
            if(onAlreadyExists == null) {
                onAlreadyExists = OnAlreadyExists.success;
            }
            if(concurrency == null) {
                concurrency = 1;
            }
            if(target != null) {
                target.setDefaults();
                if(!target.auth.isNone() && target.auth.serviceAccount == null) {
                    target.auth.serviceAccount = IAMUtil.getMetadataServiceAccount();
                }
                if(AuthProvider.Type.gcpOauth.equals(target.auth.type) && target.auth.scope == null) {
                    target.auth.scope = SCOPE_CLOUD_PLATFORM;
                }
            }
            if(batch != null) {
                batch.setDefaults();
            }
        }
    }

    public static class TaskParameters implements Serializable {

        private String id;
        private Boolean hashId;
        private String scheduleTime;
        private String delay;
        private String dispatchDeadline;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(scheduleTime != null && delay != null) {
                errorMessages.add("parameters.task.scheduleTime and parameters.task.delay are exclusive");
            }
            if(delay != null && !TemplateUtil.isTemplateText(delay)) {
                try {
                    Durations.parse(delay);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.task.delay is illegal: " + e.getMessage());
                }
            }
            if(dispatchDeadline != null) {
                try {
                    final java.time.Duration d = Durations.parse(dispatchDeadline);
                    if(d.getSeconds() < 15 || d.getSeconds() > 30 * 60) {
                        errorMessages.add("parameters.task.dispatchDeadline must be between 15s and 30m but: " + dispatchDeadline);
                    }
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.task.dispatchDeadline is illegal: " + e.getMessage());
                }
            }
            if(id != null && !TemplateUtil.isTemplateText(id)) {
                errorMessages.add("parameters.task.id must be a template containing element fields (a constant id would collide for every element): " + id);
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(hashId == null) {
                hashId = true;
            }
        }
    }

    public static class RetryParameters implements Serializable {

        private Integer maxAttempts;
        private Double initialRetryDelaySeconds;
        private Double maxRetryDelaySeconds;
        private Double totalTimeoutSeconds;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(maxAttempts != null && maxAttempts < 1) {
                errorMessages.add("parameters.retry.maxAttempts must be >= 1 but: " + maxAttempts);
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(maxAttempts == null) {
                maxAttempts = 5;
            }
            if(initialRetryDelaySeconds == null) {
                initialRetryDelaySeconds = 0.5D;
            }
            if(maxRetryDelaySeconds == null) {
                maxRetryDelaySeconds = 10D;
            }
            if(totalTimeoutSeconds == null) {
                totalTimeoutSeconds = 60D;
            }
        }

        RetrySettings toRetrySettings() {
            return RetrySettings.newBuilder()
                    .setMaxAttempts(maxAttempts)
                    .setInitialRetryDelayDuration(java.time.Duration.ofMillis((long) (initialRetryDelaySeconds * 1000)))
                    .setRetryDelayMultiplier(2.0D)
                    .setMaxRetryDelayDuration(java.time.Duration.ofMillis((long) (maxRetryDelaySeconds * 1000)))
                    .setInitialRpcTimeoutDuration(java.time.Duration.ofSeconds(20))
                    .setRpcTimeoutMultiplier(1.0D)
                    .setMaxRpcTimeoutDuration(java.time.Duration.ofSeconds(20))
                    .setTotalTimeoutDuration(java.time.Duration.ofMillis((long) (totalTimeoutSeconds * 1000)))
                    .build();
        }
    }

    public enum OnAlreadyExists {
        success,
        fail
    }

    public enum State {
        CREATED,
        ALREADY_EXISTS,
        FAILED
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        if (parameters == null) {
            throw new IllegalModuleException("tasks sink module parameters must not be empty!");
        }
        final Schema inputSchema = Union.createUnionSchema(inputs);
        parameters.validate(inputSchema);
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final Schema outputSchema = Optional.ofNullable(getSchema()).orElse(inputSchema);
        if(RequestSpec.Format.protobuf.equals(parameters.body.format) && outputSchema.getProtobuf() == null) {
            throw new IllegalModuleException("body.format protobuf requires schema.protobuf (descriptorFile / messageName)");
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs;
        if(parameters.batch == null) {
            outputs = input
                    .apply("CreateTasks", ParDo
                            .of(new CreateTaskDoFn(getName(), parameters, inputSchema, outputSchema, inputs.getAllInputs(), failureTag, getFailFast(), getLoggings()))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        } else {
            @SuppressWarnings("unchecked")
            final Coder<MElement> elementCoder = (Coder<MElement>) input.getCoder();
            outputs = input
                    .apply("WithBatchKey", ParDo.of(new BatchSpec.KeyDoFn(new BatchKeyRenderer(getName(), parameters, inputSchema, inputs.getAllInputs()), parameters.batch.shards)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), elementCoder))
                    .apply("GroupIntoBatches", parameters.batch.groupIntoBatches())
                    .apply("CreateBatchTasks", ParDo
                            .of(new CreateBatchTaskDoFn(getName(), parameters, inputSchema, outputSchema, inputs.getAllInputs(), failureTag, getFailFast(), getLoggings()))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        }

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple.of(outputs.get(outputTag), createOutputSchema());
    }

    public static Schema createOutputSchema() {
        return Schema.builder()
                .withField(Schema.Field.of("queue", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("taskName", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("url", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("state", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("scheduleTime", Schema.FieldType.TIMESTAMP.withNullable(true)))
                .withField(Schema.Field.of("createTime", Schema.FieldType.TIMESTAMP.withNullable(true)))
                .withField(Schema.Field.of("elementCount", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("bytes", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("error", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // Client abstraction (gRPC client in production, in-memory client in unit tests)
    // ---------------------------------------------------------------------------------------

    /** Thin boundary around the Cloud Tasks API so the DoFn can be tested without network. */
    public interface TasksClient extends AutoCloseable {
        /** Creates a task with an explicit HTTP target. Throws {@link AlreadyExistsException} on duplicate names. */
        Task createTask(String queue, Task task);
        /** Asynchronous variant used when {@code concurrency > 1}; defaults to the synchronous call. */
        default ApiFuture<Task> createTaskAsync(String queue, Task task) {
            try {
                return ApiFutures.immediateFuture(createTask(queue, task));
            } catch (final RuntimeException e) {
                return ApiFutures.immediateFailedFuture(e);
            }
        }
        /** Buffers a task into a queue that has a queue-level HTTP target (tasks:buffer). */
        Task bufferTask(String queue, String taskId, ByteString body);
        @Override
        void close();
    }

    public interface TasksClientFactory extends Serializable {
        TasksClient create(Parameters parameters) throws IOException;
    }

    private static final Map<String, TasksClient> MEMORY_CLIENTS = new HashMap<>();

    /** Registers an in-memory client reachable via {@code endpoint: memory://<name>} (tests). */
    public static void registerMemoryClient(final String name, final TasksClient client) {
        synchronized (MEMORY_CLIENTS) {
            MEMORY_CLIENTS.put(name, client);
        }
    }

    public static void unregisterMemoryClient(final String name) {
        synchronized (MEMORY_CLIENTS) {
            MEMORY_CLIENTS.remove(name);
        }
    }

    static TasksClient createClient(final Parameters parameters) throws IOException {
        if(parameters.endpoint != null && parameters.endpoint.startsWith(ENDPOINT_MEMORY_PREFIX)) {
            final String name = parameters.endpoint.substring(ENDPOINT_MEMORY_PREFIX.length());
            synchronized (MEMORY_CLIENTS) {
                final TasksClient client = MEMORY_CLIENTS.get(name);
                if(client == null) {
                    throw new IllegalStateException("in-memory tasks client is not registered: " + name);
                }
                return client;
            }
        }
        return new GrpcTasksClient(parameters);
    }

    static class GrpcTasksClient implements TasksClient {

        private final Parameters parameters;
        private final CloudTasksClient client;

        private static final Set<StatusCode.Code> RETRYABLE_CODES = Set.of(
                StatusCode.Code.UNAVAILABLE,
                StatusCode.Code.DEADLINE_EXCEEDED,
                StatusCode.Code.RESOURCE_EXHAUSTED,
                StatusCode.Code.INTERNAL);

        GrpcTasksClient(final Parameters parameters) throws IOException {
            this.parameters = parameters;
            final CloudTasksSettings.Builder builder = CloudTasksSettings.newBuilder();
            configure(builder, parameters);
            builder.createTaskSettings()
                    .setRetryableCodes(RETRYABLE_CODES)
                    .setRetrySettings(parameters.retry.toRetrySettings());
            this.client = CloudTasksClient.create(builder.build());
        }

        private static void configure(final com.google.api.gax.rpc.ClientSettings.Builder<?, ?> builder, final Parameters parameters) {
            if(parameters.endpoint != null) {
                // Emulator: plaintext channel, no credentials
                builder.setEndpoint(parameters.endpoint)
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
                                .setEndpoint(parameters.endpoint)
                                .setChannelConfigurator(b -> b.usePlaintext())
                                .build());
            } else {
                builder.setCredentialsProvider(GcpCredentialsCache::credentials);
            }
        }

        @Override
        public Task createTask(final String queue, final Task task) {
            return client.createTask(CreateTaskRequest.newBuilder()
                    .setParent(queue)
                    .setTask(task)
                    .build());
        }

        @Override
        public ApiFuture<Task> createTaskAsync(final String queue, final Task task) {
            return client.createTaskCallable().futureCall(CreateTaskRequest.newBuilder()
                    .setParent(queue)
                    .setTask(task)
                    .build());
        }

        @Override
        public Task bufferTask(final String queue, final String taskId, final ByteString body) {
            // The Java client does not expose tasks:buffer yet; call the REST surface directly.
            if(parameters.endpoint != null) {
                throw new UnsupportedOperationException("tasks:buffer (target omitted) is not supported against a custom endpoint / emulator");
            }
            try {
                final String path = taskId == null ? "/tasks:buffer" : "/tasks/" + taskId + ":buffer";
                final java.net.http.HttpRequest.Builder request = java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create("https://cloudtasks.googleapis.com/v2/" + queue + path))
                        .header("Authorization", "Bearer " + GcpCredentialsCache.accessToken().getTokenValue())
                        .header("Content-Type", "application/octet-stream")
                        .timeout(java.time.Duration.ofSeconds(30));
                request.POST(body == null
                        ? java.net.http.HttpRequest.BodyPublishers.noBody()
                        : java.net.http.HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()));
                final java.net.http.HttpResponse<String> response = httpClient()
                        .send(request.build(), java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if(response.statusCode() == 409) {
                    throw new AlreadyExistsException(
                            "task already exists: " + queue + "/tasks/" + taskId,
                            null,
                            com.google.api.gax.grpc.GrpcStatusCode.of(io.grpc.Status.Code.ALREADY_EXISTS),
                            false);
                }
                if(response.statusCode() / 100 != 2) {
                    throw new IllegalStateException("tasks:buffer failed with status " + response.statusCode() + ": " + response.body());
                }
                final JsonObject json = new com.google.gson.Gson().fromJson(response.body(), JsonObject.class);
                final JsonObject task = json.has("task") ? json.getAsJsonObject("task") : json;
                final Task.Builder out = Task.newBuilder();
                if(task.has("name")) {
                    out.setName(task.get("name").getAsString());
                }
                if(task.has("scheduleTime")) {
                    out.setScheduleTime(toProtoTimestamp(task.get("scheduleTime").getAsString()));
                }
                if(task.has("createTime")) {
                    out.setCreateTime(toProtoTimestamp(task.get("createTime").getAsString()));
                }
                return out.build();
            } catch (final IOException e) {
                throw new IllegalStateException("tasks:buffer request failed", e);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("tasks:buffer request interrupted", e);
            }
        }

        private java.net.http.HttpClient http;

        private synchronized java.net.http.HttpClient httpClient() {
            if(http == null) {
                http = java.net.http.HttpClient.newBuilder()
                        .connectTimeout(java.time.Duration.ofSeconds(10))
                        .build();
            }
            return http;
        }

        private static Timestamp toProtoTimestamp(final String rfc3339) {
            final Instant instant = Instant.parse(rfc3339);
            return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
        }

        @Override
        public void close() {
            client.close();
        }
    }

    // ---------------------------------------------------------------------------------------
    // Request building (pure, unit-testable) and the DoFn
    // ---------------------------------------------------------------------------------------

    /** Result of building a request for one element (or one batch of elements). */
    static class BuiltTask {
        final String queue;
        final String taskId;
        final Task task;       // null when buffering (target omitted)
        final ByteString body;
        final String url;
        final int elementCount;

        BuiltTask(String queue, String taskId, Task task, ByteString body, String url, int elementCount) {
            this.queue = queue;
            this.taskId = taskId;
            this.task = task;
            this.body = body;
            this.url = url;
            this.elementCount = elementCount;
        }
    }

    /**
     * Turns one element (or one batch) into a Cloud Tasks request: the shared
     * {@link RequestRenderer} renders url / params / headers / body, this class adds the
     * task-specific parts (queue, name, schedule, token).
     */
    static class RequestBuilder implements Serializable {

        private final String name;
        private final Parameters parameters;
        private final Schema inputSchema;
        private final RequestRenderer renderer;

        private transient Template queueTemplate;
        private transient String staticQueue;
        private transient Template idTemplate;
        private transient Template scheduleTimeTemplate;
        private transient String scheduleTimeField;
        private transient Template delayTemplate;
        private transient java.time.Duration staticDelay;
        private transient com.google.protobuf.Duration dispatchDeadline;

        RequestBuilder(
                final String name,
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final List<String> inputNames) {

            this.name = name;
            this.parameters = parameters;
            this.inputSchema = inputSchema;
            // target omitted (tasks:buffer): render the body only, against a placeholder target
            final RequestSpec.Target target;
            if(parameters.target != null) {
                target = parameters.target;
            } else {
                target = new RequestSpec.Target();
                target.url = "https://buffer.invalid/";
                target.setDefaults();
            }
            final List<String> extraTexts = new ArrayList<>();
            extraTexts.add(parameters.queue);
            extraTexts.add(parameters.task.id);
            extraTexts.add(parameters.task.scheduleTime);
            extraTexts.add(parameters.task.delay);
            extraTexts.removeIf(Objects::isNull);
            this.renderer = new RequestRenderer(name, target, parameters.body,
                    parameters.batch == null ? null : parameters.batch.key, parameters.batch != null,
                    inputSchema, outputSchema, inputNames, extraTexts);
        }

        boolean isBatch() {
            return parameters.batch != null;
        }

        void setup() {
            renderer.setup();
            final Map<String, Object> staticValues = new HashMap<>();
            TemplateUtil.setFunctions(staticValues);

            if(TemplateUtil.isTemplateText(parameters.queue) && !isStatic(parameters.queue)) {
                this.queueTemplate = TemplateUtil.createStrictTemplate(name + ".queue", parameters.queue);
            } else {
                this.staticQueue = render(name + ".queue", parameters.queue, staticValues);
            }
            if(parameters.task.id != null) {
                this.idTemplate = TemplateUtil.createStrictTemplate(name + ".id", parameters.task.id);
            }
            if(parameters.task.scheduleTime != null) {
                final Matcher matcher = PATTERN_SIMPLE_FIELD.matcher(parameters.task.scheduleTime.trim());
                if(matcher.matches() && inputSchema.hasField(matcher.group(1))) {
                    this.scheduleTimeField = matcher.group(1);
                } else {
                    this.scheduleTimeTemplate = TemplateUtil.createStrictTemplate(name + ".scheduleTime", parameters.task.scheduleTime);
                }
            }
            if(parameters.task.delay != null) {
                if(TemplateUtil.isTemplateText(parameters.task.delay)) {
                    this.delayTemplate = TemplateUtil.createStrictTemplate(name + ".delay", parameters.task.delay);
                } else {
                    this.staticDelay = Durations.parse(parameters.task.delay);
                }
            }
            if(parameters.task.dispatchDeadline != null) {
                final java.time.Duration d = Durations.parse(parameters.task.dispatchDeadline);
                this.dispatchDeadline = com.google.protobuf.Duration.newBuilder()
                        .setSeconds(d.getSeconds()).setNanos(d.getNano()).build();
            }
        }

        private static final Pattern PATTERN_DYNAMIC_VAR = Pattern
                .compile("\\$\\{[^}]*\\b(__timestamp|__source|__element|__doc|elements|size|key)\\b[^}]*}");

        private boolean isStatic(final String text) {
            return TemplateUtil.extractTemplateArgs(text, inputSchema).isEmpty()
                    && !PATTERN_DYNAMIC_VAR.matcher(text).find();
        }

        private static String render(final String name, final String text, final Map<String, Object> values) {
            if(text == null) {
                return null;
            }
            if(!TemplateUtil.isTemplateText(text)) {
                return text;
            }
            return TemplateUtil.executeStrictTemplate(TemplateUtil.createStrictTemplate(name, text), values);
        }

        Map<String, Object> createTemplateValues(final MElement element) {
            return renderer.createTemplateValues(element);
        }

        Map<String, Object> createTemplateValues(final List<MElement> elements, final String key) {
            return renderer.createTemplateValues(elements, key);
        }

        String renderBatchKey(final MElement element) {
            return renderer.renderBatchKey(element);
        }

        BuiltTask build(final MElement element) {
            return build(List.of(element), createTemplateValues(element));
        }

        BuiltTask build(final List<MElement> elements, final String key) {
            return build(elements, createTemplateValues(elements, key));
        }

        private BuiltTask build(final List<MElement> elements, final Map<String, Object> values) {
            final String queue = queueTemplate != null ? TemplateUtil.executeStrictTemplate(queueTemplate, values) : staticQueue;
            if(!PATTERN_QUEUE.matcher(queue).matches()) {
                throw new IllegalArgumentException("rendered queue is illegal: " + queue);
            }

            final OutboundRequest request = renderer.build(elements, values);
            final ByteString body = request.body() == null ? null : ByteString.copyFrom(request.body());

            String taskId = null;
            if(idTemplate != null) {
                final String rendered = TemplateUtil.executeStrictTemplate(idTemplate, values);
                taskId = parameters.task.hashId ? sha256Hex(rendered) : rendered;
                if(!PATTERN_TASK_ID.matcher(taskId).matches()) {
                    throw new IllegalArgumentException("task id must match [A-Za-z0-9_-]{1,500} but: " + taskId + " (set task.hashId: true)");
                }
            }

            if(parameters.target == null) {
                return new BuiltTask(queue, taskId, null, body, null, elements.size());
            }

            final String url = request.url();
            final HttpRequest.Builder httpRequest = HttpRequest.newBuilder()
                    .setUrl(url)
                    .setHttpMethod(HttpMethod.valueOf(request.method()));
            httpRequest.putAllHeaders(request.headers());
            if(body != null) {
                httpRequest.setBody(body);
            }
            switch (parameters.target.auth.type) {
                case gcpOidc -> httpRequest.setOidcToken(OidcToken.newBuilder()
                        .setServiceAccountEmail(parameters.target.auth.serviceAccount)
                        .setAudience(Optional.ofNullable(parameters.target.auth.audience).orElseGet(() -> stripQuery(url)))
                        .build());
                case gcpOauth -> httpRequest.setOauthToken(OAuthToken.newBuilder()
                        .setServiceAccountEmail(parameters.target.auth.serviceAccount)
                        .setScope(parameters.target.auth.scope)
                        .build());
                default -> {}
            }

            final Task.Builder task = Task.newBuilder().setHttpRequest(httpRequest);
            if(taskId != null) {
                task.setName(queue + "/tasks/" + taskId);
            }
            final Instant scheduleTime = resolveScheduleTime(values);
            if(scheduleTime != null) {
                task.setScheduleTime(Timestamp.newBuilder()
                        .setSeconds(scheduleTime.getEpochSecond()).setNanos(scheduleTime.getNano()).build());
            }
            if(dispatchDeadline != null) {
                task.setDispatchDeadline(dispatchDeadline);
            }
            return new BuiltTask(queue, taskId, task.build(), body, url, elements.size());
        }

        Instant resolveScheduleTime(final Map<String, Object> values) {
            if(scheduleTimeField != null) {
                final Object value = values.get(scheduleTimeField);
                return value == null ? null : toInstant(value);
            }
            if(scheduleTimeTemplate != null) {
                final String text = TemplateUtil.executeStrictTemplate(scheduleTimeTemplate, values);
                if(text == null || text.isBlank()) {
                    return null;
                }
                return toInstant(text.trim());
            }
            java.time.Duration delay = staticDelay;
            if(delayTemplate != null) {
                final String text = TemplateUtil.executeStrictTemplate(delayTemplate, values);
                if(text == null || text.isBlank()) {
                    return null;
                }
                delay = Durations.parse(text.trim());
            }
            if(delay != null) {
                return Instant.now().plus(delay);
            }
            return null;
        }
    }

    static Instant toInstant(final Object value) {
        return switch (value) {
            case Instant i -> i;
            case org.joda.time.Instant j -> Instant.ofEpochMilli(j.getMillis());
            case Long l -> {
                // epoch seconds / millis / micros by magnitude
                if(l < 100_000_000_000L) {
                    yield Instant.ofEpochSecond(l);
                } else if(l < 100_000_000_000_000L) {
                    yield Instant.ofEpochMilli(l);
                } else {
                    yield DateTimeUtil.toInstant(l);
                }
            }
            case Integer i -> Instant.ofEpochSecond(i);
            case String s -> {
                final String t = s.trim();
                if(t.matches("^\\d+$")) {
                    yield toInstant(Long.parseLong(t));
                }
                final Instant parsed = DateTimeUtil.toInstant(t, true);
                if(parsed == null) {
                    throw new IllegalArgumentException("illegal scheduleTime: " + s);
                }
                yield parsed;
            }
            default -> {
                final Instant parsed = DateTimeUtil.toInstant(value);
                if(parsed == null) {
                    throw new IllegalArgumentException("illegal scheduleTime: " + value);
                }
                yield parsed;
            }
        };
    }

    static String sha256Hex(final String text) {
        return StringFunctions.sha256Hex(text);
    }

    static String stripQuery(final String url) {
        final int i = url.indexOf('?');
        return i < 0 ? url : url.substring(0, i);
    }

    /** Output sink abstraction so results can be emitted from both @ProcessElement and @FinishBundle. */
    private interface Emitter {
        void output(MElement element, org.joda.time.Instant timestamp, BoundedWindow window);
        void failure(BadRecord badRecord, org.joda.time.Instant timestamp, BoundedWindow window);
    }

    /** One in-flight createTask call (concurrency > 1). */
    private record Pending(
            List<MElement> elements,
            BuiltTask built,
            ApiFuture<Task> future,
            org.joda.time.Instant timestamp,
            BoundedWindow window) {}

    /**
     * Shared client lifecycle + output/failure plumbing of the per-element and batch DoFns.
     *
     * <p>With {@code concurrency > 1} createTask calls are issued asynchronously: up to
     * {@code concurrency} calls are in flight per bundle; the oldest is awaited when the limit is
     * reached and all are drained at bundle end, so the bundle only commits once every task is
     * created (or routed to failures).
     */
    private abstract static class BaseTaskDoFn<InputT> extends DoFn<InputT, MElement> {

        protected final String name;
        protected final Parameters parameters;
        protected final RequestBuilder builder;
        protected final TupleTag<BadRecord> failureTag;
        protected final boolean failFast;
        protected final Map<String, Logging> logging;

        protected transient TasksClient client;
        private transient boolean ownsClient;
        private transient Deque<Pending> pending;

        BaseTaskDoFn(
                final String name,
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final List<String> inputNames,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast,
                final List<Logging> loggings) {

            this.name = name;
            this.parameters = parameters;
            this.builder = new RequestBuilder(name, parameters, inputSchema, outputSchema, inputNames);
            this.failureTag = failureTag;
            this.failFast = failFast;
            this.logging = Logging.map(loggings);
        }

        @Setup
        public void setup() throws IOException {
            this.builder.setup();
            this.client = createClient(parameters);
            this.ownsClient = parameters.endpoint == null || !parameters.endpoint.startsWith(ENDPOINT_MEMORY_PREFIX);
            this.pending = new ArrayDeque<>();
        }

        @Teardown
        public void teardown() {
            if(client != null && ownsClient) {
                client.close();
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            drain(0, new Emitter() {
                @Override
                public void output(final MElement element, final org.joda.time.Instant timestamp, final BoundedWindow window) {
                    c.output(element, timestamp, window);
                }
                @Override
                public void failure(final BadRecord badRecord, final org.joda.time.Instant timestamp, final BoundedWindow window) {
                    c.output(failureTag, badRecord, timestamp, window);
                }
            });
        }

        protected Emitter emitter(final ProcessContext c) {
            return new Emitter() {
                @Override
                public void output(final MElement element, final org.joda.time.Instant timestamp, final BoundedWindow window) {
                    c.outputWithTimestamp(element, timestamp);
                }
                @Override
                public void failure(final BadRecord badRecord, final org.joda.time.Instant timestamp, final BoundedWindow window) {
                    c.outputWithTimestamp(failureTag, badRecord, timestamp);
                }
            };
        }

        /** Sends one built task: synchronously (concurrency 1) or as an in-flight future. */
        protected void send(
                final Emitter emitter,
                final List<MElement> elements,
                final BuiltTask built,
                final org.joda.time.Instant timestamp,
                final BoundedWindow window) {

            if(parameters.concurrency <= 1 || built.task == null) {
                Task created;
                State state;
                try {
                    created = built.task != null
                            ? client.createTask(built.queue, built.task)
                            : client.bufferTask(built.queue, built.taskId, built.body);
                    state = State.CREATED;
                } catch (final AlreadyExistsException e) {
                    if(OnAlreadyExists.fail.equals(parameters.onAlreadyExists)) {
                        throw e;
                    }
                    created = built.task;
                    state = State.ALREADY_EXISTS;
                }
                emit(emitter, built, created, state, timestamp, window);
                return;
            }
            pending.addLast(new Pending(elements, built, client.createTaskAsync(built.queue, built.task), timestamp, window));
            drain(parameters.concurrency - 1, emitter);
        }

        /** Awaits in-flight calls until at most {@code keep} remain. */
        private void drain(final int keep, final Emitter emitter) {
            while(pending != null && pending.size() > keep) {
                final Pending p = pending.pollFirst();
                try {
                    Task created;
                    State state;
                    try {
                        created = p.future().get();
                        state = State.CREATED;
                    } catch (final java.util.concurrent.ExecutionException e) {
                        if(e.getCause() instanceof AlreadyExistsException) {
                            if(OnAlreadyExists.fail.equals(parameters.onAlreadyExists)) {
                                throw e.getCause();
                            }
                            created = p.built().task;
                            state = State.ALREADY_EXISTS;
                        } else {
                            throw e.getCause() != null ? e.getCause() : e;
                        }
                    }
                    emit(emitter, p.built(), created, state, p.timestamp(), p.window());
                } catch (final Throwable e) {
                    fail(emitter, p.elements(), p.built(), e, p.timestamp(), p.window());
                }
            }
        }

        private void emit(
                final Emitter emitter,
                final BuiltTask built,
                final Task created,
                final State state,
                final org.joda.time.Instant timestamp,
                final BoundedWindow window) {

            final MElement output = createOutput(built, created, state, null, timestamp);
            Logging.log(LOG, logging, "output", output);
            emitter.output(output, timestamp, window);
        }

        /** Routes failed elements to the failure tag and (with failFast=false) emits a FAILED record. */
        protected void fail(
                final Emitter emitter,
                final List<MElement> elements,
                final BuiltTask built,
                final Throwable e,
                final org.joda.time.Instant timestamp,
                final BoundedWindow window) {

            for(final MElement element : elements) {
                final BadRecord badRecord = processError("Failed to create task: " + name, element, e, failFast);
                emitter.failure(badRecord, timestamp, window);
            }
            emitter.output(createOutput(
                    built != null ? built : new BuiltTask(parameters.queue, null, null, null, null, elements.size()),
                    null, State.FAILED, e.getMessage(), timestamp), timestamp, window);
        }

        private MElement createOutput(
                final BuiltTask built,
                final Task task,
                final State state,
                final String error,
                final org.joda.time.Instant timestamp) {

            final MElement.Builder b = MElement.builder()
                    .withString("queue", built.queue)
                    .withString("taskName", task != null && !task.getName().isEmpty() ? task.getName()
                            : (built.taskId != null ? built.queue + "/tasks/" + built.taskId : null))
                    .withString("url", built.url)
                    .withString("state", state.name())
                    .withInt64("elementCount", (long) built.elementCount)
                    .withInt64("bytes", built.body == null ? 0L : (long) built.body.size())
                    .withString("error", error)
                    .withTimestamp("timestamp", timestamp)
                    .withEventTime(timestamp);
            if(task != null && task.hasScheduleTime()) {
                b.withTimestamp("scheduleTime", Instant.ofEpochSecond(task.getScheduleTime().getSeconds(), task.getScheduleTime().getNanos()));
            } else {
                b.withTimestamp("scheduleTime", (Instant) null);
            }
            if(task != null && task.hasCreateTime()) {
                b.withTimestamp("createTime", Instant.ofEpochSecond(task.getCreateTime().getSeconds(), task.getCreateTime().getNanos()));
            } else {
                b.withTimestamp("createTime", (Instant) null);
            }
            return b.build();
        }
    }

    private static class CreateTaskDoFn extends BaseTaskDoFn<MElement> {

        CreateTaskDoFn(
                final String name,
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final List<String> inputNames,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast,
                final List<Logging> loggings) {
            super(name, parameters, inputSchema, outputSchema, inputNames, failureTag, failFast, loggings);
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            Logging.log(LOG, logging, "input", input);
            final Emitter emitter = emitter(c);
            BuiltTask built = null;
            try {
                built = builder.build(input);
                send(emitter, List.of(input), built, c.timestamp(), window);
            } catch (final Throwable e) {
                fail(emitter, List.of(input), built, e, c.timestamp(), window);
            }
        }
    }

    /** Renders batch.key with the sink's request builder. */
    private static class BatchKeyRenderer implements BatchSpec.KeyRenderer {
        private final RequestBuilder builder;

        BatchKeyRenderer(final String name, final Parameters parameters, final Schema inputSchema, final List<String> inputNames) {
            this.builder = new RequestBuilder(name, parameters, inputSchema, inputSchema, inputNames);
        }

        @Override
        public void setup() {
            builder.setup();
        }

        @Override
        public String render(final MElement element) {
            return builder.renderBatchKey(element);
        }
    }

    private static class CreateBatchTaskDoFn extends BaseTaskDoFn<KV<String, Iterable<MElement>>> {

        CreateBatchTaskDoFn(
                final String name,
                final Parameters parameters,
                final Schema inputSchema,
                final Schema outputSchema,
                final List<String> inputNames,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast,
                final List<Logging> loggings) {
            super(name, parameters, inputSchema, outputSchema, inputNames, failureTag, failFast, loggings);
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            final KV<String, Iterable<MElement>> kv = c.element();
            if(kv == null || kv.getValue() == null) {
                return;
            }
            final List<MElement> elements = new ArrayList<>();
            for(final MElement element : kv.getValue()) {
                Logging.log(LOG, logging, "input", element);
                elements.add(element);
            }
            if(elements.isEmpty()) {
                return;
            }
            final String key = parameters.batch.key == null ? null : kv.getKey();
            process(emitter(c), elements, key, c.timestamp(), window);
        }

        /** Builds and sends one task for the batch; on body.maxBytes overflow splits it in halves. */
        private void process(
                final Emitter emitter,
                final List<MElement> elements,
                final String key,
                final org.joda.time.Instant timestamp,
                final BoundedWindow window) {

            BuiltTask built = null;
            try {
                built = builder.build(elements, key);
                send(emitter, elements, built, timestamp, window);
            } catch (final RequestRenderer.BodyTooLargeException e) {
                if(elements.size() > 1) {
                    final int mid = elements.size() / 2;
                    process(emitter, elements.subList(0, mid), key, timestamp, window);
                    process(emitter, elements.subList(mid, elements.size()), key, timestamp, window);
                } else {
                    fail(emitter, elements, null, e, timestamp, window);
                }
            } catch (final Throwable e) {
                fail(emitter, elements, built, e, timestamp, window);
            }
        }
    }

}
