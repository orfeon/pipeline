package com.mercari.solution.module.sink;

import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.outbound.*;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import freemarker.template.Template;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Sink that sends each input element (or batch of elements) as a gRPC request and tracks the
 * outcome. The service contract is a protoc descriptor set ({@code --include_imports}); requests
 * and responses are {@link DynamicMessage}s (the grpcurl mechanism, shared with the query
 * transform's grpc lookup source through {@link GrpcSupport}).
 *
 * <p>Mirrors the http sink: {@code auth} (the shared {@link AuthProvider}, sent as call metadata),
 * declarative {@code response.success} / {@code retry} classification on gRPC status codes,
 * keyed micro-batching ({@code batch}: client-streaming methods receive one message per element,
 * unary methods a single message with the elements in {@code batch.repeatedField}), bounded
 * in-flight {@code concurrency}, per-worker {@code rate}, and one control record per call
 * (SUCCEEDED / FAILED) with the response as JSON payload; failed elements go to failureSinks.
 */
@Sink.Module(name="grpc")
public class GrpcSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcSink.class);

    private static final Pattern PATTERN_DYNAMIC_VAR = Pattern
            .compile("\\$\\{[^}]*\\b(__timestamp|__source|__element|elements|size|key)\\b[^}]*}");

    public static class Parameters implements Serializable {

        private String target;
        private Boolean plaintext;
        private String descriptorSetPath;
        private String method;
        private Map<String, String> metadata;
        private AuthProvider.Parameters auth;
        private RequestParameters request;
        private ResponseParameters response;
        private GrpcBatch batch;
        private String deadline;
        private Integer concurrency;
        private RateParameters rate;
        private Integer maxInboundMessageBytes;

        private void validate(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(target == null) {
                errorMessages.add("parameters.target must not be null");
            }
            if(descriptorSetPath == null) {
                errorMessages.add("parameters.descriptorSetPath must not be null");
            }
            if(method == null) {
                errorMessages.add("parameters.method must not be null (package.Service/Method)");
            }
            if(auth != null) {
                errorMessages.addAll(auth.validate("parameters.auth"));
                if(!auth.isNone() && auth.type != null) {
                    for(final String text : Arrays.asList(auth.token, auth.value, auth.tokenUrl, auth.clientId, auth.clientSecret)) {
                        if(text != null && !TemplateUtil.extractTemplateArgs(text, inputSchema).isEmpty()) {
                            errorMessages.add("parameters.auth values must not reference element fields: " + text);
                        }
                    }
                }
            }
            if(request != null) {
                errorMessages.addAll(request.validate(inputSchema));
            }
            if(response != null) {
                errorMessages.addAll(response.validate());
            }
            if(batch != null) {
                errorMessages.addAll(batch.validate("parameters.batch"));
                final Map<String, String> perRequestTemplates = new LinkedHashMap<>();
                if(metadata != null) {
                    metadata.forEach((k, v) -> perRequestTemplates.put("parameters.metadata." + k, v));
                }
                errorMessages.addAll(batch.validateKeyConstraint("parameters.batch", inputSchema, perRequestTemplates));
            }
            if(deadline != null) {
                try {
                    Durations.parse(deadline);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.deadline is illegal: " + e.getMessage());
                }
            }
            if(concurrency != null && concurrency < 1) {
                errorMessages.add("parameters.concurrency must be >= 1 but: " + concurrency);
            }
            if(rate != null && (rate.count == null || rate.count <= 0)) {
                errorMessages.add("parameters.rate.count must be positive");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(plaintext == null) {
                plaintext = false;
            }
            if(metadata == null) {
                metadata = new HashMap<>();
            }
            if(auth == null) {
                auth = new AuthProvider.Parameters();
            }
            auth.setDefaults();
            if(request == null) {
                request = new RequestParameters();
            }
            request.setDefaults();
            if(response == null) {
                response = new ResponseParameters();
            }
            response.setDefaults();
            if(batch != null) {
                batch.setDefaults();
            }
            if(deadline == null) {
                deadline = "60s";
            }
            if(concurrency == null) {
                concurrency = 1;
            }
            if(maxInboundMessageBytes == null) {
                maxInboundMessageBytes = 0;
            }
        }
    }

    public static class RequestParameters implements Serializable {
        private Mapping mapping;
        private List<String> fields;
        private String template;
        private Boolean omitNulls;

        private List<String> validate(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(Mapping.template.equals(mapping) && template == null) {
                errorMessages.add("parameters.request.template must not be null when request.mapping is template");
            }
            if(fields != null) {
                for(final String f : fields) {
                    if(!inputSchema.hasField(f)) {
                        errorMessages.add("parameters.request.fields " + f + " is not in input schema");
                    }
                }
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(mapping == null) {
                mapping = template != null ? Mapping.template : Mapping.fields;
            }
            if(omitNulls == null) {
                omitNulls = true;
            }
        }
    }

    public static class ResponseParameters implements Serializable {
        private JsonElement successCondition;
        private Retry retry;
        private String successConditionJson;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(retry != null) {
                errorMessages.addAll(retry.validate());
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(successCondition != null) {
                successConditionJson = successCondition.toString();
                successCondition = null;
            }
            if(retry == null) {
                retry = new Retry();
            }
            retry.setDefaults();
        }
    }

    public static class Retry implements Serializable {
        private List<String> statuses;
        private Integer maxAttempts;
        private String initialBackoff;
        private String maxBackoff;
        private String totalTimeout;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(statuses != null) {
                for(final String status : statuses) {
                    try {
                        Status.Code.valueOf(status);
                    } catch (final IllegalArgumentException e) {
                        errorMessages.add("parameters.response.retry.statuses contains an unknown gRPC status: " + status);
                    }
                }
            }
            final ResponsePolicy.Retry r = new ResponsePolicy.Retry();
            r.maxAttempts = maxAttempts;
            r.initialBackoff = initialBackoff;
            r.maxBackoff = maxBackoff;
            r.totalTimeout = totalTimeout;
            final ResponsePolicy.Parameters p = new ResponsePolicy.Parameters();
            p.retry = r;
            errorMessages.addAll(p.validate("parameters.response"));
            return errorMessages;
        }

        private void setDefaults() {
            if(statuses == null) {
                statuses = List.of("UNAVAILABLE", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED", "ABORTED");
            }
        }

        ResponsePolicy.Parameters toPolicy() {
            final ResponsePolicy.Retry r = new ResponsePolicy.Retry();
            r.maxAttempts = maxAttempts;
            r.initialBackoff = initialBackoff;
            r.maxBackoff = maxBackoff;
            r.totalTimeout = totalTimeout;
            final ResponsePolicy.Parameters p = new ResponsePolicy.Parameters();
            p.retry = r;
            p.format = ResponsePolicy.Format.none;
            p.setDefaults();
            return p;
        }
    }

    /** Batch spec plus the gRPC-specific target of a unary batch. */
    public static class GrpcBatch extends BatchSpec {
        private String repeatedField;
    }

    public static class RateParameters implements Serializable {
        private Double count;
        private String unit;

        double permitsPerSecond() {
            return "minute".equals(unit) ? count / 60D : count;
        }
    }

    public enum Mapping {
        fields,
        template
    }

    public enum State {
        SUCCEEDED,
        FAILED
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        if(parameters == null) {
            throw new IllegalModuleException("grpc sink module parameters must not be empty!");
        }
        final Schema inputSchema = Union.createUnionSchema(inputs);
        parameters.validate(inputSchema);
        parameters.setDefaults();

        // the descriptor set only needs to be readable on the launcher: ship its bytes
        final byte[] descriptorSetBytes;
        try {
            descriptorSetBytes = Files.readAllBytes(Path.of(parameters.descriptorSetPath));
        } catch (final IOException e) {
            throw new IllegalModuleException("failed to read gRPC descriptor set file: " + parameters.descriptorSetPath + ": " + e.getMessage());
        }
        // resolve the method now so a typo fails at assembly
        final Descriptors.MethodDescriptor method = GrpcSupport.resolveMethod(GrpcSupport.linkDescriptorSet(descriptorSetBytes), parameters.method);
        if(method.isServerStreaming()) {
            throw new IllegalModuleException("parameters.method " + parameters.method + " is server-streaming; the grpc sink supports unary and client-streaming methods");
        }
        if(parameters.batch != null && !method.isClientStreaming() && parameters.batch.repeatedField == null
                && !Mapping.template.equals(parameters.request.mapping)) {
            throw new IllegalModuleException("parameters.batch.repeatedField (or request.mapping template) is required to batch into the unary method " + parameters.method);
        }
        if(parameters.batch != null && parameters.batch.repeatedField != null) {
            final Descriptors.FieldDescriptor field = method.getInputType().findFieldByName(parameters.batch.repeatedField);
            if(field == null || !field.isRepeated() || field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                throw new IllegalModuleException("parameters.batch.repeatedField " + parameters.batch.repeatedField
                        + " must be a repeated message field of " + method.getInputType().getFullName());
            }
        }

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs;
        if(parameters.batch == null) {
            outputs = input
                    .apply("SendRequests", ParDo
                            .of(new SendDoFn(getName(), parameters, descriptorSetBytes, inputSchema, inputs.getAllInputs(), failureTag, getFailFast(), getLoggings()))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        } else {
            @SuppressWarnings("unchecked")
            final Coder<MElement> elementCoder = (Coder<MElement>) input.getCoder();
            outputs = input
                    .apply("WithBatchKey", ParDo.of(new BatchSpec.KeyDoFn(new BatchKeyRenderer(getName(), parameters, inputSchema, inputs.getAllInputs()), parameters.batch.shards)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), elementCoder))
                    .apply("GroupIntoBatches", parameters.batch.groupIntoBatches())
                    .apply("SendBatchRequests", ParDo
                            .of(new SendBatchDoFn(getName(), parameters, descriptorSetBytes, inputSchema, inputs.getAllInputs(), failureTag, getFailFast(), getLoggings()))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        }

        errorHandler.addError(outputs.get(failureTag));
        return MCollectionTuple.of(outputs.get(outputTag), createOutputSchema());
    }

    public static Schema createOutputSchema() {
        return Schema.builder()
                .withField(Schema.Field.of("target", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("method", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("state", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("status", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("statusMessage", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("payload", Schema.FieldType.JSON.withNullable(true)))
                .withField(Schema.Field.of("attempts", Schema.FieldType.INT32.withNullable(false)))
                .withField(Schema.Field.of("durationMs", Schema.FieldType.INT64.withNullable(true)))
                .withField(Schema.Field.of("elementCount", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("bytes", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("error", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // Request building
    // ---------------------------------------------------------------------------------------

    /** One built call: the request message(s) and the rendered metadata. */
    record Built(List<DynamicMessage> messages, Map<String, String> metadata, int bytes) {}

    /** Turns elements into DynamicMessages: field mapping via JSON, or a FreeMarker protobuf-JSON template. */
    static class RequestBuilder implements Serializable {

        private final String name;
        private final Parameters parameters;
        private final Schema inputSchema;
        private final List<String> inputNames;
        private final List<String> templateArgs;

        private transient Template bodyTemplate;
        private transient Template keyTemplate;
        private transient Map<String, Template> metadataTemplates;
        private transient Map<String, String> staticMetadata;
        private transient Descriptors.Descriptor inputType;
        private transient Descriptors.FieldDescriptor repeatedField;
        private transient JsonFormat.Parser parser;

        RequestBuilder(final String name, final Parameters parameters, final Schema inputSchema, final List<String> inputNames) {
            this.name = name;
            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.inputNames = inputNames;
            final Set<String> args = new HashSet<>();
            final List<String> texts = new ArrayList<>(parameters.metadata.values());
            texts.add(parameters.request.template);
            if(parameters.batch != null) {
                texts.add(parameters.batch.key);
            }
            for(final String text : texts) {
                if(text != null) {
                    args.addAll(TemplateUtil.extractTemplateArgs(text, inputSchema));
                }
            }
            this.templateArgs = new ArrayList<>(args);
        }

        void setup(final Descriptors.MethodDescriptor method) {
            this.inputType = method.getInputType();
            this.parser = JsonFormat.parser().ignoringUnknownFields();
            if(parameters.batch != null && parameters.batch.repeatedField != null) {
                this.repeatedField = inputType.findFieldByName(parameters.batch.repeatedField);
            }
            final Map<String, Object> staticValues = new HashMap<>();
            TemplateUtil.setFunctions(staticValues);
            this.metadataTemplates = new HashMap<>();
            this.staticMetadata = new HashMap<>();
            for(final Map.Entry<String, String> entry : parameters.metadata.entrySet()) {
                if(TemplateUtil.isTemplateText(entry.getValue()) && !isStatic(entry.getValue())) {
                    metadataTemplates.put(entry.getKey(), TemplateUtil.createStrictTemplate(name + ".metadata." + entry.getKey(), entry.getValue()));
                } else {
                    staticMetadata.put(entry.getKey(), TemplateUtil.isTemplateText(entry.getValue())
                            ? TemplateUtil.executeStrictTemplate(TemplateUtil.createStrictTemplate(name + ".metadata." + entry.getKey(), entry.getValue()), staticValues)
                            : entry.getValue());
                }
            }
            if(parameters.request.template != null) {
                this.bodyTemplate = TemplateUtil.createStrictTemplate(name + ".request", parameters.request.template);
            }
            if(parameters.batch != null && parameters.batch.key != null) {
                this.keyTemplate = TemplateUtil.createStrictTemplate(name + ".batchKey", parameters.batch.key);
            }
        }

        /** Compiles only the batch key template (the batch-key DoFn needs no descriptor). */
        void setupKeyOnly() {
            if(parameters.batch != null && parameters.batch.key != null) {
                this.keyTemplate = TemplateUtil.createStrictTemplate(name + ".batchKey", parameters.batch.key);
            }
        }

        private boolean isStatic(final String text) {
            return TemplateUtil.extractTemplateArgs(text, inputSchema).isEmpty() && !PATTERN_DYNAMIC_VAR.matcher(text).find();
        }

        Map<String, Object> createTemplateValues(final MElement element) {
            final Map<String, Object> values = element.asStandardMap(inputSchema, templateArgs);
            values.put(RequestRenderer.VAR_ELEMENT, element.asStandardMap(inputSchema));
            values.put(RequestRenderer.VAR_TIMESTAMP, Instant.ofEpochMilli(element.getEpochMillis()));
            values.put(RequestRenderer.VAR_SOURCE, element.getIndex() < inputNames.size() ? inputNames.get(element.getIndex()) : "");
            TemplateUtil.setFunctions(values);
            return values;
        }

        String renderBatchKey(final MElement element) {
            return keyTemplate == null ? null : TemplateUtil.executeStrictTemplate(keyTemplate, createTemplateValues(element));
        }

        Built build(final List<MElement> elements, final String key, final boolean clientStreaming) {
            final Map<String, Object> values = createTemplateValues(elements.get(0));
            if(parameters.batch != null) {
                values.put("elements", elements.stream().map(e -> e.asStandardMap(inputSchema)).toList());
                values.put("size", elements.size());
                values.put("key", key);
            }
            final Map<String, String> metadata = new LinkedHashMap<>(staticMetadata);
            for(final Map.Entry<String, Template> entry : metadataTemplates.entrySet()) {
                metadata.put(entry.getKey(), TemplateUtil.executeStrictTemplate(entry.getValue(), values));
            }
            final List<DynamicMessage> messages = new ArrayList<>();
            int bytes = 0;
            if(parameters.batch == null || clientStreaming) {
                // one message per element (unary: exactly one element)
                for(final MElement element : elements) {
                    final DynamicMessage message = toMessage(inputType, elementJson(element, parameters.batch == null ? values : createTemplateValues(element)));
                    bytes += message.getSerializedSize();
                    messages.add(message);
                }
            } else if(Mapping.template.equals(parameters.request.mapping)) {
                // one request rendered from the whole batch (template sees elements / size / key)
                final DynamicMessage message = toMessage(inputType, TemplateUtil.executeStrictTemplate(bodyTemplate, values));
                bytes = message.getSerializedSize();
                messages.add(message);
            } else {
                // one request with the elements in the repeated field
                final DynamicMessage.Builder builder = DynamicMessage.newBuilder(inputType);
                for(final MElement element : elements) {
                    builder.addRepeatedField(repeatedField, toMessage(repeatedField.getMessageType(), elementJson(element, null)));
                }
                final DynamicMessage message = builder.build();
                bytes = message.getSerializedSize();
                messages.add(message);
            }
            return new Built(messages, metadata, bytes);
        }

        private String elementJson(final MElement element, final Map<String, Object> values) {
            if(Mapping.template.equals(parameters.request.mapping)) {
                return TemplateUtil.executeStrictTemplate(bodyTemplate, values != null ? values : createTemplateValues(element));
            }
            final JsonObject json = parameters.request.fields == null
                    ? ElementToJsonConverter.convert(inputSchema, element.asPrimitiveMap())
                    : ElementToJsonConverter.convert(inputSchema, element.asPrimitiveMap(), parameters.request.fields);
            final JsonElement out = parameters.request.omitNulls ? RequestRenderer.omitNulls(json) : json;
            return out == null ? "{}" : out.toString();
        }

        private DynamicMessage toMessage(final Descriptors.Descriptor type, final String json) {
            final DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
            try {
                parser.merge(json, builder);
            } catch (final IOException e) {
                throw new IllegalArgumentException("request JSON does not match message " + type.getFullName() + ": " + e.getMessage() + " json: " + abbreviate(json), e);
            }
            return builder.build();
        }
    }

    // ---------------------------------------------------------------------------------------
    // Sending
    // ---------------------------------------------------------------------------------------

    record Failed(MElement element, String error) {}

    static class Outcome {
        Built built;
        DynamicMessage response;
        Status status;
        int attempts;
        int elementCount;
        long durationMs;
        final List<Failed> failed = new ArrayList<>();
        String error;

        State state() {
            return failed.isEmpty() ? State.SUCCEEDED : State.FAILED;
        }
    }

    private interface Emitter {
        void output(MElement element, org.joda.time.Instant timestamp, BoundedWindow window);
        void failure(BadRecord badRecord, org.joda.time.Instant timestamp, BoundedWindow window);
    }

    private record Pending(List<MElement> elements, CompletableFuture<Outcome> future, org.joda.time.Instant timestamp, BoundedWindow window) {}

    private abstract static class BaseSendDoFn<InputT> extends DoFn<InputT, MElement> {

        protected final String name;
        protected final Parameters parameters;
        private final byte[] descriptorSetBytes;
        protected final RequestBuilder builder;
        protected final TupleTag<BadRecord> failureTag;
        protected final boolean failFast;
        protected final Map<String, Logging> logging;

        private transient ManagedChannel managedChannel;
        private transient Channel channel;
        private transient MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod;
        private transient boolean clientStreaming;
        private transient AuthProvider auth;
        private transient ResponsePolicy policy;
        private transient Set<Status.Code> retryStatuses;
        private transient Filter.ConditionNode successCondition;
        private transient JsonFormat.Printer printer;
        private transient RateLimiter rateLimiter;
        private transient long deadlineMillis;
        private transient Deque<Pending> pending;

        BaseSendDoFn(String name, Parameters parameters, byte[] descriptorSetBytes, Schema inputSchema, List<String> inputNames,
                     TupleTag<BadRecord> failureTag, boolean failFast, List<Logging> loggings) {
            this.name = name;
            this.parameters = parameters;
            this.descriptorSetBytes = descriptorSetBytes;
            this.builder = new RequestBuilder(name, parameters, inputSchema, inputNames);
            this.failureTag = failureTag;
            this.failFast = failFast;
            this.logging = Logging.map(loggings);
        }

        @Setup
        public void setup() {
            final Descriptors.MethodDescriptor method = GrpcSupport.resolveMethod(GrpcSupport.linkDescriptorSet(descriptorSetBytes), parameters.method);
            this.clientStreaming = method.isClientStreaming();
            this.grpcMethod = GrpcSupport.methodDescriptor(method, GrpcSupport.methodType(method));
            this.builder.setup(method);
            this.auth = AuthProvider.create(parameters.auth, (parameters.plaintext ? "http://" : "https://") + parameters.target);
            this.managedChannel = GrpcSupport.createChannel(parameters.target, parameters.plaintext, parameters.maxInboundMessageBytes);
            this.channel = GrpcSupport.withHeaders(managedChannel, Map.of(), auth);
            this.policy = new ResponsePolicy(parameters.response.retry.toPolicy());
            this.policy.setup();
            this.retryStatuses = new HashSet<>();
            for(final String status : parameters.response.retry.statuses) {
                retryStatuses.add(Status.Code.valueOf(status));
            }
            this.successCondition = parameters.response.successConditionJson == null ? null : Filter.parse(parameters.response.successConditionJson);
            this.printer = JsonFormat.printer().omittingInsignificantWhitespace();
            this.rateLimiter = parameters.rate == null ? null : RateLimiter.create(parameters.rate.permitsPerSecond());
            this.deadlineMillis = Durations.parse(parameters.deadline).toMillis();
            this.pending = new ArrayDeque<>();
        }

        @Teardown
        public void teardown() {
            if(managedChannel != null) {
                managedChannel.shutdown();
                try {
                    managedChannel.awaitTermination(5, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
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

        protected boolean isClientStreaming() {
            return clientStreaming;
        }

        protected void send(final Emitter emitter, final List<MElement> elements, final Built built,
                            final org.joda.time.Instant timestamp, final BoundedWindow window) {
            final Outcome outcome = new Outcome();
            outcome.elementCount = elements.size();
            outcome.built = built;
            if(rateLimiter != null) {
                rateLimiter.acquire();
            }
            pending.addLast(new Pending(elements, execute(elements, built, 1, Instant.now(), outcome, false), timestamp, window));
            drain(parameters.concurrency - 1, emitter);
        }

        /** One attempt plus retries (backoff) and one auth refresh on UNAUTHENTICATED; never completes exceptionally. */
        private CompletableFuture<Outcome> execute(final List<MElement> elements, final Built built, final int attempt,
                                                   final Instant startedAt, final Outcome outcome, final boolean authRetried) {
            outcome.attempts = Math.max(outcome.attempts, attempt);
            final long start = System.currentTimeMillis();
            return call(built).handle((response, error) -> {
                outcome.durationMs = System.currentTimeMillis() - start;
                if(error != null) {
                    final Throwable cause = error instanceof java.util.concurrent.CompletionException && error.getCause() != null ? error.getCause() : error;
                    final Status status = Status.fromThrowable(cause);
                    outcome.status = status;
                    if(status.getCode() == Status.Code.UNAUTHENTICATED && !authRetried && !auth.isNone()) {
                        LOG.warn("{}: UNAUTHENTICATED from {}, refreshing credentials once", name, parameters.target);
                        auth.invalidate();
                        return execute(elements, built, attempt + 1, startedAt, outcome, true);
                    }
                    if(retryStatuses.contains(status.getCode())) {
                        final java.time.Duration backoff = policy.backoff(attempt, null, startedAt);
                        if(backoff != null) {
                            LOG.warn("{}: {} from {}, retrying in {} ms (attempt {})", name, status.getCode(), parameters.target, backoff.toMillis(), attempt + 1);
                            return CompletableFuture
                                    .supplyAsync(() -> execute(elements, built, attempt + 1, startedAt, outcome, authRetried),
                                            CompletableFuture.delayedExecutor(backoff.toMillis(), TimeUnit.MILLISECONDS))
                                    .thenCompose(f -> f);
                        }
                    }
                    return failAll(outcome, elements, status.getCode() + " after " + attempt + " attempt(s): " + status.getDescription());
                }
                outcome.response = response;
                outcome.status = Status.OK;
                if(successCondition != null) {
                    final Map<String, Object> values = new HashMap<>();
                    values.put("status", "OK");
                    values.put("payload", JsonToMapConverter.convert(printJson(response)));
                    if(!Filter.filter(successCondition, values)) {
                        return failAll(outcome, elements, "response did not satisfy response.successCondition: " + abbreviate(printJson(response)));
                    }
                }
                return CompletableFuture.completedFuture(outcome);
            }).thenCompose(f -> f);
        }

        private CompletableFuture<DynamicMessage> call(final Built built) {
            final CallOptions options = deadlineMillis > 0 ? CallOptions.DEFAULT.withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS) : CallOptions.DEFAULT;
            final Channel ch = built.metadata().isEmpty() ? channel : GrpcSupport.withHeaders(managedChannel, built.metadata(), auth);
            final CompletableFuture<DynamicMessage> future = new CompletableFuture<>();
            final StreamObserver<DynamicMessage> observer = new StreamObserver<>() {
                private DynamicMessage last;
                @Override
                public void onNext(final DynamicMessage value) {
                    last = value;
                }
                @Override
                public void onError(final Throwable t) {
                    future.completeExceptionally(t);
                }
                @Override
                public void onCompleted() {
                    future.complete(last);
                }
            };
            try {
                if(clientStreaming) {
                    final StreamObserver<DynamicMessage> requests = ClientCalls.asyncClientStreamingCall(ch.newCall(grpcMethod, options), observer);
                    for(final DynamicMessage message : built.messages()) {
                        requests.onNext(message);
                    }
                    requests.onCompleted();
                } else {
                    ClientCalls.asyncUnaryCall(ch.newCall(grpcMethod, options), built.messages().get(0), observer);
                }
            } catch (final StatusRuntimeException e) {
                future.completeExceptionally(e);
            }
            return future;
        }

        private static CompletableFuture<Outcome> failAll(final Outcome outcome, final List<MElement> elements, final String error) {
            outcome.error = error;
            for(final MElement e : elements) {
                outcome.failed.add(new Failed(e, error));
            }
            return CompletableFuture.completedFuture(outcome);
        }

        private void drain(final int keep, final Emitter emitter) {
            while(pending != null && pending.size() > keep) {
                final Pending p = pending.pollFirst();
                Outcome outcome;
                try {
                    outcome = p.future().get();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for grpc call", e);
                } catch (final java.util.concurrent.ExecutionException e) {
                    outcome = new Outcome();
                    outcome.elementCount = p.elements().size();
                    failAll(outcome, p.elements(), "unexpected error: " + (e.getCause() == null ? e : e.getCause()));
                }
                emit(emitter, outcome, p.timestamp(), p.window());
            }
        }

        private void emit(final Emitter emitter, final Outcome outcome, final org.joda.time.Instant timestamp, final BoundedWindow window) {
            for(final Failed failed : outcome.failed) {
                emitter.failure(processError("Failed to send grpc request: " + name, failed.element(),
                        new IllegalStateException(failed.error()), failFast), timestamp, window);
            }
            final MElement output = createOutput(outcome, timestamp);
            Logging.log(LOG, logging, "output", output);
            emitter.output(output, timestamp, window);
        }

        protected void fail(final Emitter emitter, final List<MElement> elements, final Throwable e,
                            final org.joda.time.Instant timestamp, final BoundedWindow window) {
            LOG.warn("{}: failed to build request", name, e);
            final Outcome outcome = new Outcome();
            outcome.elementCount = elements.size();
            failAll(outcome, elements, "failed to build request: " + e.getMessage());
            emit(emitter, outcome, timestamp, window);
        }

        private String printJson(final DynamicMessage message) {
            try {
                return message == null ? null : printer.print(message);
            } catch (final com.google.protobuf.InvalidProtocolBufferException e) {
                return null;
            }
        }

        private MElement createOutput(final Outcome outcome, final org.joda.time.Instant timestamp) {
            return MElement.builder()
                    .withString("target", parameters.target)
                    .withString("method", parameters.method)
                    .withString("state", outcome.state().name())
                    .withString("status", outcome.status == null ? null : outcome.status.getCode().name())
                    .withString("statusMessage", outcome.status == null ? null : outcome.status.getDescription())
                    .withString("payload", printJson(outcome.response))
                    .withPrimitiveValue("attempts", outcome.attempts)
                    .withPrimitiveValue("durationMs", outcome.durationMs)
                    .withInt64("elementCount", (long) outcome.elementCount)
                    .withInt64("bytes", outcome.built == null ? 0L : (long) outcome.built.bytes())
                    .withString("error", outcome.error)
                    .withTimestamp("timestamp", timestamp)
                    .withEventTime(timestamp)
                    .build();
        }
    }

    private static class SendDoFn extends BaseSendDoFn<MElement> {
        SendDoFn(String name, Parameters parameters, byte[] descriptorSetBytes, Schema inputSchema, List<String> inputNames,
                 TupleTag<BadRecord> failureTag, boolean failFast, List<Logging> loggings) {
            super(name, parameters, descriptorSetBytes, inputSchema, inputNames, failureTag, failFast, loggings);
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            Logging.log(LOG, logging, "input", input);
            final Emitter emitter = emitter(c);
            try {
                send(emitter, List.of(input), builder.build(List.of(input), null, isClientStreaming()), c.timestamp(), window);
            } catch (final Throwable e) {
                fail(emitter, List.of(input), e, c.timestamp(), window);
            }
        }
    }

    private static class BatchKeyRenderer implements BatchSpec.KeyRenderer {
        private final RequestBuilder builder;

        BatchKeyRenderer(String name, Parameters parameters, Schema inputSchema, List<String> inputNames) {
            this.builder = new RequestBuilder(name, parameters, inputSchema, inputNames);
        }

        @Override
        public void setup() {
            builder.setupKeyOnly();
        }

        @Override
        public String render(final MElement element) {
            return builder.renderBatchKey(element);
        }
    }

    private static class SendBatchDoFn extends BaseSendDoFn<KV<String, Iterable<MElement>>> {
        SendBatchDoFn(String name, Parameters parameters, byte[] descriptorSetBytes, Schema inputSchema, List<String> inputNames,
                      TupleTag<BadRecord> failureTag, boolean failFast, List<Logging> loggings) {
            super(name, parameters, descriptorSetBytes, inputSchema, inputNames, failureTag, failFast, loggings);
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
            final Emitter emitter = emitter(c);
            try {
                send(emitter, elements, builder.build(elements, parameters.batch.key == null ? null : kv.getKey(), isClientStreaming()), c.timestamp(), window);
            } catch (final Throwable e) {
                fail(emitter, elements, e, c.timestamp(), window);
            }
        }
    }

    static String abbreviate(final String text) {
        if(text == null) {
            return "";
        }
        return text.length() > 512 ? text.substring(0, 512) + "..." : text;
    }
}
