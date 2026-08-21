package com.mercari.solution.module.sink;

import com.google.common.util.concurrent.RateLimiter;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.outbound.AuthProvider;
import com.mercari.solution.util.pipeline.outbound.Durations;
import com.mercari.solution.util.pipeline.outbound.HttpTransport;
import com.mercari.solution.util.pipeline.outbound.OutboundRequest;
import com.mercari.solution.util.pipeline.outbound.RequestRenderer;
import com.mercari.solution.util.pipeline.outbound.RequestSpec;
import com.mercari.solution.util.pipeline.outbound.ResponsePolicy;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
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

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Sink that delivers each input element (or batch of elements) to an HTTP endpoint and tracks the
 * outcome.
 *
 * <p>Design (see docs/module/sink/http.md and work_http.md): the {@code target} / {@code body} /
 * {@code batch} contract is shared with the {@code tasks} sink; responses are classified
 * declaratively ({@code response.success} / {@code retry} / {@code partialFailure}); auth is a
 * worker-scoped {@link AuthProvider}; up to {@code concurrency} requests are in flight per bundle
 * and the bundle only commits once all of them resolved. One control record per request
 * (SUCCEEDED / PARTIAL / FAILED) is emitted; failed elements also go to {@code failureSinks}.
 */
@Sink.Module(name="http")
public class HttpSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);


    public static class Parameters implements Serializable {

        private RequestSpec.Target target;
        private RequestSpec.Body body;
        private ResponsePolicy.Parameters response;
        private BatchParameters batch;
        private Integer concurrency;
        private RateParameters rate;
        private HttpTransport.TimeoutParameters timeout;
        private HttpTransport.Parameters http;

        private void validate(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(target == null) {
                errorMessages.add("parameters.target must not be null");
            } else {
                errorMessages.addAll(target.validate("parameters.target", inputSchema, http != null && http.allowedHosts != null));
            }
            if(body != null) {
                errorMessages.addAll(body.validate("parameters.body", inputSchema));
            }
            if(response != null) {
                errorMessages.addAll(response.validate("parameters.response"));
            }
            if(batch != null) {
                errorMessages.addAll(batch.validate());
                errorMessages.addAll(validateBatchTemplateArgs(inputSchema));
            } else if(response != null && response.partialFailure != null) {
                errorMessages.add("parameters.response.partialFailure requires parameters.batch");
            }
            if(concurrency != null && concurrency < 1) {
                errorMessages.add("parameters.concurrency must be >= 1 but: " + concurrency);
            }
            if(rate != null) {
                errorMessages.addAll(rate.validate());
            }
            if(timeout != null) {
                errorMessages.addAll(timeout.validate("parameters.timeout"));
            }
            if(http != null) {
                errorMessages.addAll(http.validate("parameters.http"));
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        /** In batch mode per-request templates may only reference batch.key fields (see TasksSink). */
        private List<String> validateBatchTemplateArgs(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            final Set<String> keyArgs = new HashSet<>();
            if(batch.key != null) {
                keyArgs.addAll(TemplateUtil.extractTemplateArgs(batch.key, inputSchema));
            }
            final Map<String, String> perRequestTemplates = new LinkedHashMap<>();
            if(target != null) {
                perRequestTemplates.put("target.url", target.url);
                if(target.headers != null) {
                    target.headers.forEach((k, v) -> perRequestTemplates.put("target.headers." + k, v));
                }
                if(target.params != null) {
                    target.params.forEach((k, v) -> perRequestTemplates.put("target.params." + k, v));
                }
            }
            for(final Map.Entry<String, String> entry : perRequestTemplates.entrySet()) {
                if(entry.getValue() == null) {
                    continue;
                }
                for(final String arg : TemplateUtil.extractTemplateArgs(entry.getValue(), inputSchema)) {
                    if(!keyArgs.contains(arg)) {
                        errorMessages.add("parameters." + entry.getKey() + " references field '" + arg
                                + "' which is not part of parameters.batch.key (in batch mode per-request templates may only use batch.key fields)");
                    }
                }
            }
            return errorMessages;
        }

        private void setDefaults() {
            target.setDefaults();
            if(body == null) {
                body = new RequestSpec.Body();
            }
            body.setDefaults();
            if(response == null) {
                response = new ResponsePolicy.Parameters();
            }
            response.setDefaults();
            if(batch != null) {
                batch.setDefaults();
            }
            if(concurrency == null) {
                concurrency = 1;
            }
            if(timeout == null) {
                timeout = new HttpTransport.TimeoutParameters();
            }
            timeout.setDefaults();
            if(http == null) {
                http = new HttpTransport.Parameters();
            }
            http.setDefaults();
            if(http.allowedHosts == null && !target.auth.isNone()) {
                final String origin = RequestSpec.staticOrigin(target.url);
                if(origin != null) {
                    http.allowedHosts = List.of(java.net.URI.create(origin).getHost());
                }
            }
        }
    }

    public static class BatchParameters implements Serializable {

        private Integer maxSize;
        private String maxBytes;
        private String maxBufferingDuration;
        private String key;
        private Integer shards;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(maxSize != null && maxSize < 1) {
                errorMessages.add("parameters.batch.maxSize must be >= 1 but: " + maxSize);
            }
            if(maxBytes != null) {
                try {
                    Durations.parseBytes(maxBytes);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.batch.maxBytes is illegal: " + e.getMessage());
                }
            }
            if(maxSize == null && maxBytes == null) {
                errorMessages.add("parameters.batch requires maxSize and/or maxBytes");
            }
            if(maxBufferingDuration != null) {
                try {
                    Durations.parse(maxBufferingDuration);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.batch.maxBufferingDuration is illegal: " + e.getMessage());
                }
            }
            if(shards != null && shards < 1) {
                errorMessages.add("parameters.batch.shards must be >= 1 but: " + shards);
            }
            if(key != null && !TemplateUtil.isTemplateText(key)) {
                errorMessages.add("parameters.batch.key must be a template on element fields but: " + key);
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(shards == null) {
                shards = 8;
            }
        }
    }

    public static class RateParameters implements Serializable {

        private Double count;
        private RateUnit unit;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(count == null || count <= 0) {
                errorMessages.add("parameters.rate.count must be positive");
            }
            return errorMessages;
        }

        double permitsPerSecond() {
            return switch (unit == null ? RateUnit.second : unit) {
                case second -> count;
                case minute -> count / 60D;
            };
        }
    }

    public enum RateUnit {
        second,
        minute
    }

    public enum State {
        SUCCEEDED,
        PARTIAL,
        FAILED
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        if (parameters == null) {
            throw new IllegalModuleException("http sink module parameters must not be empty!");
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

        final ResponsePolicy policy = new ResponsePolicy(parameters.response);
        final Schema resultSchema = createOutputSchema(policy.schema());

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs;
        if(parameters.batch == null) {
            outputs = input
                    .apply("SendRequests", ParDo
                            .of(new SendDoFn(getName(), parameters, policy, inputSchema, outputSchema, inputs.getAllInputs(), failureTag, getFailFast(), getLoggings()))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        } else {
            @SuppressWarnings("unchecked")
            final Coder<MElement> elementCoder = (Coder<MElement>) input.getCoder();
            final Long maxBytes = parameters.batch.maxBytes == null ? null : Durations.parseBytes(parameters.batch.maxBytes);
            GroupIntoBatches<String, MElement> groupIntoBatches = parameters.batch.maxSize != null
                    ? GroupIntoBatches.ofSize(parameters.batch.maxSize.longValue())
                    : GroupIntoBatches.ofByteSize(maxBytes);
            if(parameters.batch.maxSize != null && maxBytes != null) {
                groupIntoBatches = groupIntoBatches.withByteSize(maxBytes);
            }
            if(parameters.batch.maxBufferingDuration != null) {
                groupIntoBatches = groupIntoBatches.withMaxBufferingDuration(
                        org.joda.time.Duration.millis(Durations.parse(parameters.batch.maxBufferingDuration).toMillis()));
            }
            outputs = input
                    .apply("WithBatchKey", ParDo.of(new BatchKeyDoFn(getName(), parameters, inputSchema, inputs.getAllInputs())))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), elementCoder))
                    .apply("GroupIntoBatches", groupIntoBatches)
                    .apply("SendBatchRequests", ParDo
                            .of(new SendBatchDoFn(getName(), parameters, policy, inputSchema, outputSchema, inputs.getAllInputs(), failureTag, getFailFast(), getLoggings()))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        }

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple.of(outputs.get(outputTag), resultSchema);
    }

    public static Schema createOutputSchema(final Schema payloadSchema) {
        final Schema.Builder builder = Schema.builder()
                .withField(Schema.Field.of("url", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("method", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("state", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("statusCode", Schema.FieldType.INT32.withNullable(true)))
                .withField(Schema.Field.of("headers", Schema.FieldType.map(Schema.FieldType.array(Schema.FieldType.STRING)).withNullable(true)))
                .withField(Schema.Field.of("body", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("blob", Schema.FieldType.BYTES.withNullable(true)));
        if(payloadSchema != null) {
            builder.withField(Schema.Field.of("payload", Schema.FieldType.element(payloadSchema).withNullable(true)));
        } else {
            builder.withField(Schema.Field.of("payload", Schema.FieldType.JSON.withNullable(true)));
        }
        return builder
                .withField(Schema.Field.of("attempts", Schema.FieldType.INT32.withNullable(false)))
                .withField(Schema.Field.of("durationMs", Schema.FieldType.INT64.withNullable(true)))
                .withField(Schema.Field.of("elementCount", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("failedCount", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("bytes", Schema.FieldType.INT64.withNullable(false)))
                .withField(Schema.Field.of("error", Schema.FieldType.STRING.withNullable(true)))
                .withField(Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .build();
    }

    /** Scheme + host of a url whose host part contains no template, or null. */
    // ---------------------------------------------------------------------------------------
    // Sending: async state machine with retry / partial retry, bounded in-flight per bundle
    // ---------------------------------------------------------------------------------------

    /** One failed element with its reason. */
    record Failed(MElement element, String error) {}

    /** Final result for one logical request (after all retries / partial retries). */
    static class Outcome {
        OutboundRequest request;
        OutboundRequest.Response response;
        ResponsePolicy.Parsed parsed;
        int attempts;
        int elementCount;
        final List<Failed> failed = new ArrayList<>();
        String error;

        State state() {
            if(failed.isEmpty()) {
                return State.SUCCEEDED;
            }
            return failed.size() >= elementCount ? State.FAILED : State.PARTIAL;
        }
    }

    /** Output sink abstraction so results can be emitted from both @ProcessElement and @FinishBundle. */
    private interface Emitter {
        void output(MElement element, org.joda.time.Instant timestamp, BoundedWindow window);
        void failure(BadRecord badRecord, org.joda.time.Instant timestamp, BoundedWindow window);
    }

    private record Pending(
            List<MElement> elements,
            CompletableFuture<Outcome> future,
            org.joda.time.Instant timestamp,
            BoundedWindow window) {}

    private abstract static class BaseSendDoFn<InputT> extends DoFn<InputT, MElement> {

        protected final String name;
        protected final Parameters parameters;
        protected final ResponsePolicy policy;
        protected final RequestRenderer builder;
        protected final TupleTag<BadRecord> failureTag;
        protected final boolean failFast;
        protected final Map<String, Logging> logging;

        protected transient HttpTransport transport;
        private transient RateLimiter rateLimiter;
        private transient Deque<Pending> pending;

        BaseSendDoFn(
                final String name,
                final Parameters parameters,
                final ResponsePolicy policy,
                final Schema inputSchema,
                final Schema outputSchema,
                final List<String> inputNames,
                final TupleTag<BadRecord> failureTag,
                final boolean failFast,
                final List<Logging> loggings) {

            this.name = name;
            this.parameters = parameters;
            this.policy = policy;
            this.builder = new RequestRenderer(name, parameters.target, parameters.body, parameters.batch == null ? null : parameters.batch.key, parameters.batch != null, inputSchema, outputSchema, inputNames);
            this.failureTag = failureTag;
            this.failFast = failFast;
            this.logging = Logging.map(loggings);
        }

        @Setup
        public void setup() {
            this.builder.setup();
            this.policy.setup();
            final AuthProvider auth = AuthProvider.create(parameters.target.auth, RequestSpec.staticOrigin(parameters.target.url));
            this.transport = new HttpTransport(parameters.http, parameters.timeout, auth);
            this.rateLimiter = parameters.rate == null ? null : RateLimiter.create(parameters.rate.permitsPerSecond());
            this.pending = new ArrayDeque<>();
        }

        @Teardown
        public void teardown() {
            if(transport != null) {
                transport.close();
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

        /** Starts one logical request and keeps at most {@code concurrency} in flight. */
        protected void send(
                final Emitter emitter,
                final List<MElement> elements,
                final OutboundRequest request,
                final org.joda.time.Instant timestamp,
                final BoundedWindow window) {

            final Outcome outcome = new Outcome();
            outcome.elementCount = elements.size();
            final CompletableFuture<Outcome> future = execute(elements, request, 1, Instant.now(), outcome, false);
            pending.addLast(new Pending(elements, future, timestamp, window));
            drain(parameters.concurrency - 1, emitter);
        }

        /**
         * One attempt plus whatever follows it (retry after backoff, partial retry of failed items,
         * one auth refresh on 401). The returned future completes with the final outcome and never
         * exceptionally: transport errors are retried and eventually turned into failed elements.
         */
        private CompletableFuture<Outcome> execute(
                final List<MElement> elements,
                final OutboundRequest request,
                final int attempt,
                final Instant startedAt,
                final Outcome outcome,
                final boolean authRetried) {

            if(rateLimiter != null && attempt == 1) {
                rateLimiter.acquire();
            }
            outcome.request = outcome.request == null ? request : outcome.request;
            outcome.attempts = Math.max(outcome.attempts, attempt);
            return transport.send(request).handle((response, error) -> {
                if(error != null) {
                    final Throwable cause = error instanceof java.util.concurrent.CompletionException && error.getCause() != null ? error.getCause() : error;
                    final java.time.Duration backoff = policy.isRetryable(cause) ? policy.backoff(attempt, null, startedAt) : null;
                    if(backoff == null) {
                        return failAll(outcome, elements, "request failed after " + attempt + " attempt(s): " + cause);
                    }
                    LOG.warn("{}: request to {} failed ({}), retrying in {} ms (attempt {})", name, request.url(), cause.toString(), backoff.toMillis(), attempt + 1);
                    return after(backoff, () -> execute(elements, request, attempt + 1, startedAt, outcome, authRetried));
                }
                outcome.response = response;
                final ResponsePolicy.Parsed parsed = policy.parse(response);
                outcome.parsed = parsed;

                if(response.statusCode() == 401 && !authRetried && !transport.auth().isNone()) {
                    LOG.warn("{}: 401 from {}, refreshing credentials once", name, request.url());
                    transport.auth().invalidate();
                    return execute(elements, request, attempt + 1, startedAt, outcome, true);
                }

                final ResponsePolicy.Verdict verdict = policy.classify(response, parsed);
                switch (verdict) {
                    case SUCCESS -> {
                        if(elements.size() > 1 && policy.hasPartialFailure()) {
                            final List<ResponsePolicy.ItemVerdict> items = policy.items(parsed, elements.size());
                            if(items != null) {
                                final List<MElement> retryElements = new ArrayList<>();
                                for(final ResponsePolicy.ItemVerdict item : items) {
                                    switch (item.verdict()) {
                                        case FAILED -> outcome.failed.add(new Failed(elements.get(item.index()), "item failed: " + item.error()));
                                        case RETRY -> retryElements.add(elements.get(item.index()));
                                        default -> {}
                                    }
                                }
                                if(!retryElements.isEmpty()) {
                                    final java.time.Duration backoff = policy.backoff(attempt, response, startedAt);
                                    if(backoff == null) {
                                        for(final MElement e : retryElements) {
                                            outcome.failed.add(new Failed(e, "item retry exhausted after " + attempt + " attempt(s)"));
                                        }
                                        return CompletableFuture.completedFuture(outcome);
                                    }
                                    LOG.warn("{}: {} of {} items retryable, retrying in {} ms (attempt {})", name, retryElements.size(), elements.size(), backoff.toMillis(), attempt + 1);
                                    return after(backoff, () -> {
                                        final OutboundRequest sub;
                                        try {
                                            sub = builder.build(retryElements, (String) null);
                                        } catch (final RuntimeException e) {
                                            for(final MElement el : retryElements) {
                                                outcome.failed.add(new Failed(el, "failed to rebuild partial batch: " + e.getMessage()));
                                            }
                                            return CompletableFuture.completedFuture(outcome);
                                        }
                                        return execute(retryElements, sub, attempt + 1, startedAt, outcome, authRetried);
                                    });
                                }
                            }
                        }
                        return CompletableFuture.completedFuture(outcome);
                    }
                    case RETRY -> {
                        final java.time.Duration backoff = policy.backoff(attempt, response, startedAt);
                        if(backoff == null) {
                            return failAll(outcome, elements, "status " + response.statusCode() + " after " + attempt + " attempt(s): " + abbreviate(parsed.text()));
                        }
                        LOG.warn("{}: status {} from {}, retrying in {} ms (attempt {})", name, response.statusCode(), request.url(), backoff.toMillis(), attempt + 1);
                        return after(backoff, () -> execute(elements, request, attempt + 1, startedAt, outcome, authRetried));
                    }
                    default -> {
                        return failAll(outcome, elements, "status " + response.statusCode() + ": " + abbreviate(parsed.text()));
                    }
                }
            }).thenCompose(f -> f);
        }

        private static CompletableFuture<Outcome> failAll(final Outcome outcome, final List<MElement> elements, final String error) {
            outcome.error = error;
            for(final MElement e : elements) {
                outcome.failed.add(new Failed(e, error));
            }
            return CompletableFuture.completedFuture(outcome);
        }

        private static CompletableFuture<Outcome> after(final java.time.Duration delay, final java.util.function.Supplier<CompletableFuture<Outcome>> next) {
            return CompletableFuture
                    .supplyAsync(next, CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS))
                    .thenCompose(f -> f);
        }

        /** Awaits in-flight requests until at most {@code keep} remain. */
        private void drain(final int keep, final Emitter emitter) {
            while(pending != null && pending.size() > keep) {
                final Pending p = pending.pollFirst();
                Outcome outcome;
                try {
                    outcome = p.future().get();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting for http request", e);
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
                final BadRecord badRecord = processError("Failed to send http request: " + name, failed.element(),
                        new IllegalStateException(failed.error()), failFast);
                emitter.failure(badRecord, timestamp, window);
            }
            final MElement output = createOutput(outcome, timestamp);
            Logging.log(LOG, logging, "output", output);
            emitter.output(output, timestamp, window);
        }

        /** Routes a build-time failure (before any request) to failures and emits a FAILED record. */
        protected void fail(
                final Emitter emitter,
                final List<MElement> elements,
                final Throwable e,
                final org.joda.time.Instant timestamp,
                final BoundedWindow window) {

            LOG.warn("{}: failed to build request", name, e);
            final Outcome outcome = new Outcome();
            outcome.elementCount = elements.size();
            failAll(outcome, elements, "failed to build request: " + e.getMessage());
            emit(emitter, outcome, timestamp, window);
        }

        private MElement createOutput(final Outcome outcome, final org.joda.time.Instant timestamp) {
            final State state = outcome.state();
            final OutboundRequest.Response response = outcome.response;
            final ResponsePolicy.Parsed parsed = outcome.parsed;
            final MElement.Builder b = MElement.builder()
                    .withString("url", outcome.request == null ? null : outcome.request.url())
                    .withString("method", parameters.target.method)
                    .withString("state", state.name())
                    .withPrimitiveValue("statusCode", response == null ? null : response.statusCode())
                    .withPrimitiveValue("headers", response == null ? null : new HashMap<>(response.headers()))
                    .withString("body", parsed == null ? null : parsed.text())
                    .withPrimitiveValue("blob", parsed == null || parsed.bytes() == null ? null : java.nio.ByteBuffer.wrap(parsed.bytes()))
                    .withPrimitiveValue("attempts", outcome.attempts)
                    .withPrimitiveValue("durationMs", response == null ? null : response.durationMs())
                    .withInt64("elementCount", (long) outcome.elementCount)
                    .withInt64("failedCount", (long) outcome.failed.size())
                    .withInt64("bytes", outcome.request == null ? 0L : (long) outcome.request.bodySize())
                    .withString("error", outcome.error != null ? outcome.error
                            : (outcome.failed.isEmpty() ? null : outcome.failed.get(0).error()))
                    .withTimestamp("timestamp", timestamp)
                    .withEventTime(timestamp);
            if(policy.schema() != null) {
                b.withPrimitiveValue("payload", parsed != null && parsed.payload() instanceof Map<?, ?> m ? m : null);
            } else {
                b.withString("payload", parsed != null && parsed.payload() != null && parsed.values().get("json") != null
                        ? parsed.values().get("json").toString() : null);
            }
            return b.build();
        }

        private static String abbreviate(final String text) {
            if(text == null) {
                return "";
            }
            return text.length() > 512 ? text.substring(0, 512) + "..." : text;
        }
    }

    private static class SendDoFn extends BaseSendDoFn<MElement> {

        SendDoFn(String name, Parameters parameters, ResponsePolicy policy, Schema inputSchema, Schema outputSchema,
                 List<String> inputNames, TupleTag<BadRecord> failureTag, boolean failFast, List<Logging> loggings) {
            super(name, parameters, policy, inputSchema, outputSchema, inputNames, failureTag, failFast, loggings);
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
                final OutboundRequest request = builder.build(input);
                send(emitter, List.of(input), request, c.timestamp(), window);
            } catch (final Throwable e) {
                fail(emitter, List.of(input), e, c.timestamp(), window);
            }
        }
    }

    /** Assigns the batch grouping key: rendered batch.key, or a random shard when omitted. */
    private static class BatchKeyDoFn extends DoFn<MElement, KV<String, MElement>> {

        private final RequestRenderer builder;
        private final int shards;

        BatchKeyDoFn(final String name, final Parameters parameters, final Schema inputSchema, final List<String> inputNames) {
            this.builder = new RequestRenderer(name, parameters.target, parameters.body, parameters.batch.key, true, inputSchema, inputSchema, inputNames);
            this.shards = parameters.batch.shards;
        }

        @Setup
        public void setup() {
            builder.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            final String key = builder.renderBatchKey(input);
            if(key != null) {
                c.output(KV.of(key, input));
            } else {
                c.output(KV.of("shard-" + java.util.concurrent.ThreadLocalRandom.current().nextInt(shards), input));
            }
        }
    }

    private static class SendBatchDoFn extends BaseSendDoFn<KV<String, Iterable<MElement>>> {

        SendBatchDoFn(String name, Parameters parameters, ResponsePolicy policy, Schema inputSchema, Schema outputSchema,
                      List<String> inputNames, TupleTag<BadRecord> failureTag, boolean failFast, List<Logging> loggings) {
            super(name, parameters, policy, inputSchema, outputSchema, inputNames, failureTag, failFast, loggings);
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

        /** Builds and sends one request for the batch; on body.maxBytes overflow splits it in halves. */
        private void process(
                final Emitter emitter,
                final List<MElement> elements,
                final String key,
                final org.joda.time.Instant timestamp,
                final BoundedWindow window) {

            try {
                final OutboundRequest request = builder.build(elements, key);
                send(emitter, elements, request, timestamp, window);
            } catch (final RequestRenderer.BodyTooLargeException e) {
                if(elements.size() > 1) {
                    final int mid = elements.size() / 2;
                    process(emitter, elements.subList(0, mid), key, timestamp, window);
                    process(emitter, elements.subList(mid, elements.size()), key, timestamp, window);
                } else {
                    fail(emitter, elements, e, timestamp, window);
                }
            } catch (final Throwable e) {
                fail(emitter, elements, e, timestamp, window);
            }
        }
    }
}
