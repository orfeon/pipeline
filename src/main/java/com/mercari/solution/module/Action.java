package com.mercari.solution.module;

import com.google.common.reflect.ClassPath;
import com.google.gson.JsonObject;
import com.mercari.solution.config.ActionConfig;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.action.ActionResult;
import com.mercari.solution.module.action.ActionService;
import com.mercari.solution.module.action.NonRetryableException;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.outbound.Durations;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import com.google.gson.JsonElement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The fourth module kind, declared in the {@code actions} config section: a control-plane step that
 * executes one operation against an external service from inside the pipeline (run a BigQuery job,
 * launch a Vertex AI batch prediction job, call an HTTP endpoint, write a result-history file).
 *
 * Unlike {@link Source}/{@link Transform}/{@link Sink}, {@code Action} is a single concrete module whose
 * behavior is supplied by a pluggable {@link ActionService}; the config's {@code module} field names the
 * service. Services are discovered by scanning {@code com.mercari.solution.module.action} for classes
 * annotated with {@link Service} — the same convention as the other module registries.
 *
 * Trigger topologies ({@link Trigger}, an {@link ActionConfig} field):
 * <ul>
 *   <li>{@code once} — an internally generated seed element gated by {@code Wait.on} over all
 *       waits and inputs (inputs are pure completion signals).</li>
 *   <li>{@code perElement} — the flattened inputs drive one execution per element.</li>
 *   <li>{@code collect} — the flattened inputs are gathered (single-key GroupByKey) and the
 *       service fires once with the full element list. In streaming this happens per window.</li>
 * </ul>
 *
 * Every execution emits one record with the common envelope schema
 * ({@code service, operation, jobId, state, startedAt, finishedAt, payload}); failures are routed as
 * {@code BadRecord}s honoring {@code failFast} / {@code failureSinks}.
 */
public class Action extends Module<MCollectionTuple> {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Service {
        String name();
        /**
         * Operations the service supports (config field {@code operation}). Empty means the service
         * has a single operation and the config must not set {@code operation}. Values follow the
         * {@code resource.method} convention of the backing API where it has resources
         * (e.g. {@code queues.pause}, {@code jobs.load}), plain verbs otherwise.
         */
        String[] operations() default {};
    }

    /**
     * Firing semantics of an action step, given as the module-level {@code trigger} field.
     * <ul>
     *   <li>{@code once} (default) — fire exactly once after every input and wait completes;
     *       inputs act purely as completion signals and no element is delivered.</li>
     *   <li>{@code perElement} — fire once per input element.</li>
     *   <li>{@code collect} — gather all input elements and fire once with the full list.
     *       The elements are materialized on a single worker: intended for control records
     *       (file lists, job results), not large data.</li>
     * </ul>
     */
    public enum Trigger {
        once,
        perElement,
        collect
    }

    /**
     * Module-level retry of a failed firing (the service threw): the firing is re-executed on the
     * same worker with exponential backoff before it is routed to failure handling. Applies on top
     * of Beam's own bundle retries, so services should stay idempotent (see {@link ActionService}).
     */
    public static class Retry implements Serializable {

        public Integer maxAttempts;
        public String initialBackoff;
        public String maxBackoff;

        public List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(maxAttempts != null && maxAttempts < 1) {
                errorMessages.add(prefix + ".maxAttempts must be >= 1 but: " + maxAttempts);
            }
            for(final Map.Entry<String, String> e : Map.of(
                    "initialBackoff", initialBackoff == null ? "" : initialBackoff,
                    "maxBackoff", maxBackoff == null ? "" : maxBackoff).entrySet()) {
                if(!e.getValue().isEmpty()) {
                    try {
                        Durations.parse(e.getValue());
                    } catch (final IllegalArgumentException ex) {
                        errorMessages.add(prefix + "." + e.getKey() + " is illegal: " + ex.getMessage());
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(maxAttempts == null) {
                maxAttempts = 3;
            }
            if(initialBackoff == null) {
                initialBackoff = "1s";
            }
            if(maxBackoff == null) {
                maxBackoff = "30s";
            }
        }

        /** No retry at all: a single attempt. */
        static Retry none() {
            final Retry retry = new Retry();
            retry.maxAttempts = 1;
            retry.initialBackoff = "0s";
            retry.maxBackoff = "0s";
            return retry;
        }

        /** Backoff before the retry following the given (1-based) failed attempt (shared formula, see {@link Durations#exponentialBackoff}). */
        long backoffMillis(final int attempt) {
            return Durations.exponentialBackoff(Durations.parse(initialBackoff), Durations.parse(maxBackoff), attempt).toMillis();
        }

    }

    /**
     * Lazy holder: the classpath scan runs only when a service is resolved at assembly time, never
     * on workers (the DoFns below use static helpers of this class but never touch the registry).
     */
    private static final class Registry {
        static final Map<String, Class<ActionService>> SERVICES =
                findServicesInPackage("com.mercari.solution.module.action");
    }

    private Trigger trigger;
    private String operation;
    private Strategy strategy;
    private List<String> inputNames;
    private Retry retry;
    private Boolean fireOnEmpty;
    // condition JSON text (SQL-like text is kept as a JSON string primitive); null when absent
    private String failWhenJson;
    private String skipWhenJson;

    public Trigger getTrigger() {
        return trigger;
    }

    public String getOperation() {
        return operation;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public List<String> getInputNames() {
        return inputNames;
    }

    public Retry getRetry() {
        return retry;
    }

    public Boolean getFireOnEmpty() {
        return fireOnEmpty;
    }

    public String getFailWhenJson() {
        return failWhenJson;
    }

    public String getSkipWhenJson() {
        return skipWhenJson;
    }

    private void setup(
            final @NonNull ActionConfig config,
            final PipelineOptions options,
            final List<MCollection> waits,
            final MErrorHandler errorHandler) {

        super.setup(config, options, waits, errorHandler);
        this.trigger = Optional.ofNullable(config.getTrigger()).orElse(Trigger.once);
        this.operation = validateOperation(config.getName(), config.getModule(), config.getOperation());
        this.strategy = Optional
                .ofNullable(config.getStrategy())
                .orElseGet(Strategy::createDefaultStrategy);
        this.inputNames = config.getInputs();
        if(config.getRetry() == null) {
            this.retry = Retry.none();
        } else {
            final List<String> errorMessages = config.getRetry().validate("action module[" + config.getName() + "].retry");
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
            config.getRetry().setDefaults();
            this.retry = config.getRetry();
        }
        this.fireOnEmpty = Optional.ofNullable(config.getFireOnEmpty()).orElse(false);
        if(this.fireOnEmpty && !Trigger.collect.equals(this.trigger)) {
            throw new IllegalModuleException(
                    "action module[" + config.getName() + "] fireOnEmpty applies to trigger: collect only");
        }
        if(this.fireOnEmpty && !this.strategy.isDefault()) {
            // Combine.globally's default value (the empty firing) exists only in the global window
            throw new IllegalModuleException(
                    "action module[" + config.getName() + "] fireOnEmpty requires the default strategy (global window): remove strategy");
        }
        this.failWhenJson = parseCondition(config.getName(), "failWhen", config.getFailWhen());
        this.skipWhenJson = parseCondition(config.getName(), "skipWhen", config.getSkipWhen());
    }

    /** Validates a post-execution condition at assembly time and keeps it as JSON text (the DoFn re-parses it on the worker). */
    private static String parseCondition(final String name, final String field, final JsonElement condition) {
        if(condition == null || condition.isJsonNull()) {
            return null;
        }
        if(condition.isJsonPrimitive() && condition.getAsJsonPrimitive().isString() && condition.getAsString().isBlank()) {
            return null;
        }
        try {
            Filter.parse(condition);
        } catch (final RuntimeException e) {
            throw new IllegalModuleException(
                    "action module[" + name + "]." + field + " is an illegal condition: " + condition + ", cause: " + e.getMessage());
        }
        return condition.toString();
    }

    /**
     * Thrown when a module-level {@code failWhen} condition matches an execution result; not
     * retried (the result is already final).
     */
    public static class ConditionFailedException extends NonRetryableException {
        public ConditionFailedException(final String message) {
            super(message);
        }
    }

    /**
     * Values visible to {@code failWhen} / {@code skipWhen}: the envelope fields ({@code service},
     * {@code operation}, {@code jobId}, {@code state}) and {@code payload} — the typed payload map when
     * the service supplied one, otherwise the payload text (JSON text is descended into by the
     * dotted-path lookup, with numbers read as they are written).
     */
    public static Map<String, Object> createConditionValues(final String service, final ActionResult result) {
        final Map<String, Object> values = new HashMap<>();
        values.put("service", service);
        values.put("operation", result.getOperation());
        values.put("jobId", result.getJobId());
        values.put("state", result.getState());
        values.put("payload", result.getPayloadValues() != null ? result.getPayloadValues() : parsePayloadText(result.getPayload()));
        return values;
    }

    /**
     * A text payload is exposed as a map when it is a JSON object (so {@code payload.<path>} works),
     * otherwise as the text itself (comparable as a whole; a dotted path into it never matches).
     */
    private static Object parsePayloadText(final String payload) {
        if(payload == null) {
            return null;
        }
        try {
            final JsonElement json = new com.google.gson.Gson().fromJson(payload, JsonElement.class);
            if(json != null && json.isJsonObject()) {
                return com.mercari.solution.util.schema.converter.JsonToMapConverter.convert(json);
            }
        } catch (final RuntimeException ignored) {
            // not JSON
        }
        return payload;
    }

    private static boolean matches(final Filter.ConditionNode condition, final Map<String, Object> values) {
        try {
            return Filter.filter(condition, values);
        } catch (final IllegalArgumentException e) {
            // e.g. a dotted path into a non-JSON text payload: treat as "does not match" rather than failing the firing
            LOG.warn("action condition could not be evaluated against the result ({}); treating it as not matched", e.getMessage());
            return false;
        }
    }

    static String abbreviate(final String text, final int max) {
        if(text == null || text.length() <= max) {
            return text;
        }
        return text.substring(0, max) + "...(" + text.length() + " chars)";
    }

    /**
     * Applies the post-execution conditions: a matching {@code failWhen} fails the firing
     * ({@link ConditionFailedException}); otherwise a matching {@code skipWhen} turns the result
     * into {@code state: SKIPPED} (jobId and payload kept).
     */
    static ActionResult applyConditions(
            final String service,
            final ActionResult result,
            final Filter.ConditionNode failWhen,
            final String failWhenJson,
            final Filter.ConditionNode skipWhen) {

        if(result == null || (failWhen == null && skipWhen == null)) {
            return result;
        }
        final Map<String, Object> values = createConditionValues(service, result);
        if(failWhen != null && matches(failWhen, values)) {
            // the payload can be large (e.g. a Job resource with every source uri): keep the message bounded
            throw new ConditionFailedException(
                    "action service: " + service + " result matched failWhen: " + failWhenJson
                            + ". jobId: " + result.getJobId() + ", state: " + result.getState()
                            + ", payload: " + abbreviate(result.getPayload(), 1024));
        }
        if(skipWhen != null && matches(skipWhen, values)) {
            return result.withState("SKIPPED");
        }
        return result;
    }

    public static @NonNull Action create(
            final @NonNull ActionConfig config,
            final @NonNull PipelineOptions options,
            final @NonNull List<MCollection> waits,
            final @NonNull MErrorHandler errorHandler) {

        if(!Registry.SERVICES.containsKey(config.getModule())) {
            throw new IllegalModuleException("", "pipeline",
                    "Not supported action module: " + config.getModule() + ". supported modules: " + serviceNames());
        }
        final Action action = new Action();
        action.setup(config, options, waits, errorHandler);
        return action;
    }

    public static Set<String> serviceNames() {
        return new TreeSet<>(Registry.SERVICES.keySet());
    }

    /** Operations declared by the service (empty for single-operation services). */
    public static List<String> operations(final String module) {
        final Class<ActionService> clazz = Registry.SERVICES.get(module);
        if(clazz == null) {
            return List.of();
        }
        return List.of(clazz.getAnnotation(Service.class).operations());
    }

    private static String validateOperation(final String name, final String module, final String operation) {
        final List<String> operations = operations(module);
        if(operations.isEmpty()) {
            if(operation != null && !operation.isBlank()) {
                throw new IllegalModuleException(
                        "action module[" + name + "] service: " + module + " has a single operation: remove operation: " + operation);
            }
            return null;
        }
        if(operation == null || operation.isBlank()) {
            throw new IllegalModuleException(
                    "action module[" + name + "] service: " + module + " requires operation. supported operations: " + operations);
        }
        if(!operations.contains(operation)) {
            throw new IllegalModuleException(
                    "action module[" + name + "] service: " + module + " does not support operation: " + operation
                            + ". supported operations: " + operations);
        }
        return operation;
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        if(!Trigger.once.equals(trigger) && inputs.size() == 0) {
            throw new IllegalModuleException(
                    "action module[" + getName() + "] with trigger: " + trigger + " requires inputs");
        }

        final Pipeline pipeline = inputs.getPipeline();
        final JsonObject parametersJson = Config.convertConfigJson(getParametersText(), Config.Format.json);
        if(parametersJson.has("trigger")) {
            throw new IllegalModuleException(
                    "action module[" + getName() + "] parameters.trigger is not supported: declare trigger at the module level (next to name/module)");
        }
        if(parametersJson.has("op") || parametersJson.has("operation")) {
            throw new IllegalModuleException(
                    "action module[" + getName() + "] parameters.op/operation is not supported: declare operation at the module level (next to name/module)");
        }
        final Schema inputSchema = inputs.size() == 0 ? null : Union.createUnionSchema(inputs);
        final ActionService service = createService(
                getName(), getModule(), trigger, operation, parametersJson, pipeline.getOptions(), inputSchema);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = switch (trigger) {
            case once -> {
                final List<PCollection<?>> waits = new ArrayList<>();
                if(getWaits() != null) {
                    waits.addAll(getWaits());
                }
                waits.addAll(inputs.getAll().values());

                final Schema seedSchema = MElement.dummySchema();
                PCollection<MElement> seed = pipeline.begin()
                        .apply("Seed", Create
                                .of("")
                                .withCoder(StringUtf8Coder.of()))
                        .apply("ToElement", ParDo.of(new SeedDoFn()))
                        .setCoder(ElementCoder.of(seedSchema));
                if(!waits.isEmpty()) {
                    seed = seed
                            .apply("Wait", Wait.on(waits))
                            .setCoder(ElementCoder.of(seedSchema));
                }
                yield seed
                        .apply("Action", ParDo
                                .of(new ExecuteDoFn(getModule(), service, false, retry, failWhenJson, skipWhenJson, getFailFast(), failureTag))
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            }
            case perElement -> union(inputs)
                    .apply("Action", ParDo
                            .of(new ExecuteDoFn(getModule(), service, true, retry, failWhenJson, skipWhenJson, getFailFast(), failureTag))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            case collect -> {
                final PCollection<MElement> unioned = union(inputs);
                @SuppressWarnings("unchecked")
                final Coder<MElement> elementCoder = (Coder<MElement>) unioned.getCoder();
                final PCollection<Iterable<MElement>> collected;
                if(fireOnEmpty) {
                    // Combine.globally with its default value fires once with an empty list when no
                    // element arrives (global window only: Beam rejects defaults in other windows)
                    collected = unioned
                            .apply("Collect", Combine.globally(new CollectFn()))
                            .setCoder(IterableCoder.of(elementCoder));
                } else {
                    collected = unioned
                            .apply("WithSingleKey", WithKeys.of(""))
                            .setCoder(KvCoder.of(StringUtf8Coder.of(), elementCoder))
                            .apply("Collect", GroupByKey.create())
                            .apply("Values", Values.create());
                }
                yield collected
                        .apply("Action", ParDo
                                .of(new CollectExecuteDoFn(getModule(), service, retry, failWhenJson, skipWhenJson, getFailFast(), failureTag))
                                .withOutputTags(outputTag, TupleTagList.of(failureTag)));
            }
        };

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), ActionResult.createOutputSchema());
    }

    private PCollection<MElement> union(final MCollectionTuple inputs) {
        return inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(strategy));
    }

    /**
     * Instantiates and configures the service named by {@code module}. Also used by tests that
     * exercise a service outside a pipeline.
     */
    public static ActionService createService(
            final String name,
            final String module,
            final Trigger trigger,
            final String operation,
            final JsonObject parameters,
            final PipelineOptions options,
            final Schema inputSchema) {

        final Class<ActionService> clazz = Registry.SERVICES.get(module);
        if(clazz == null) {
            throw new IllegalModuleException(name, "action",
                    "Not supported action module: " + module + ". supported modules: " + serviceNames());
        }
        final ActionService service;
        try {
            service = clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("Failed to instantiate action service: " + module + ", class: " + clazz, e);
        }
        service.configure(name, trigger == null ? Trigger.once : trigger, operation, parameters, options, inputSchema);
        return service;
    }

    /**
     * Template context for {@code collect} trigger templates: exposes {@code elements}
     * (the list of element field maps) and {@code size}.
     */
    public static Map<String, Object> createCollectTemplateData(final List<MElement> elements) {
        final List<Map<String, Object>> maps = elements.stream()
                .map(MElement::asPrimitiveMap)
                .toList();
        return Map.of("elements", maps, "size", maps.size());
    }

    /**
     * Runs the service, retrying with backoff per the module's {@link Retry} before giving up.
     * Failures that re-execution cannot fix — {@link NonRetryableException}, configuration/template
     * errors, interruption — are not retried.
     */
    private static ActionResult executeWithRetry(
            final String serviceName,
            final ActionService service,
            final Retry retry,
            final List<MElement> elements) throws Exception {

        int attempt = 1;
        while(true) {
            try {
                return service.execute(elements);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (final Exception e) {
                if(attempt >= retry.maxAttempts || !isRetryable(e)) {
                    throw e;
                }
                final long backoff = retry.backoffMillis(attempt);
                LOG.warn("action service: {} attempt {}/{} failed: {}. retrying in {} ms",
                        serviceName, attempt, retry.maxAttempts, e.getMessage(), backoff);
                try {
                    Thread.sleep(backoff);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
                attempt++;
            }
        }
    }

    static boolean isRetryable(final Throwable e) {
        Throwable t = e;
        while(t != null) {
            if(t instanceof NonRetryableException
                    || t instanceof IllegalModuleException
                    || t instanceof IllegalArgumentException
                    || t instanceof InterruptedException
                    || t instanceof freemarker.template.TemplateException) {
                return false;
            }
            t = t.getCause() == t ? null : t.getCause();
        }
        return true;
    }

    private static class CollectFn extends Combine.CombineFn<MElement, List<MElement>, Iterable<MElement>> {

        @Override
        public List<MElement> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<MElement> addInput(final List<MElement> accumulator, final MElement input) {
            accumulator.add(input);
            return accumulator;
        }

        @Override
        public List<MElement> mergeAccumulators(final Iterable<List<MElement>> accumulators) {
            final List<MElement> merged = new ArrayList<>();
            for(final List<MElement> accumulator : accumulators) {
                merged.addAll(accumulator);
            }
            return merged;
        }

        @Override
        public Iterable<MElement> extractOutput(final List<MElement> accumulator) {
            return accumulator;
        }

        @Override
        public Coder<List<MElement>> getAccumulatorCoder(final CoderRegistry registry, final Coder<MElement> inputCoder) {
            return ListCoder.of(inputCoder);
        }

        @Override
        public Coder<Iterable<MElement>> getDefaultOutputCoder(final CoderRegistry registry, final Coder<MElement> inputCoder) {
            return IterableCoder.of(inputCoder);
        }

    }

    private static MElement createEnvelope(
            final String service,
            final ActionResult result,
            final Instant startedAt,
            final org.joda.time.Instant eventTime) {

        final MElement.Builder builder = MElement.builder()
                .withString("service", service)
                .withTimestamp("startedAt", startedAt)
                .withTimestamp("finishedAt", Instant.now())
                .withEventTime(eventTime);
        if(result != null) {
            builder
                    .withString("operation", result.getOperation())
                    .withString("jobId", result.getJobId())
                    .withString("state", result.getState())
                    .withString("payload", result.getPayload());
        }
        return builder.build();
    }

    private static class ExecuteDoFn extends DoFn<MElement, MElement> {

        private final String serviceName;
        private final ActionService service;
        private final boolean perElement;
        private final Retry retry;
        private final String failWhenJson;
        private final String skipWhenJson;

        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        private transient Filter.ConditionNode failWhen;
        private transient Filter.ConditionNode skipWhen;


        ExecuteDoFn(
                final String serviceName,
                final ActionService service,
                final boolean perElement,
                final Retry retry,
                final String failWhenJson,
                final String skipWhenJson,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.serviceName = serviceName;
            this.service = service;
            this.perElement = perElement;
            this.retry = retry;
            this.failWhenJson = failWhenJson;
            this.skipWhenJson = skipWhenJson;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.service.setup();
            this.failWhen = failWhenJson == null ? null : Filter.parse(failWhenJson);
            this.skipWhen = skipWhenJson == null ? null : Filter.parse(skipWhenJson);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            final Instant startedAt = Instant.now();
            try {
                final ActionResult result = applyConditions(serviceName,
                        executeWithRetry(serviceName, service, retry, perElement ? List.of(input) : List.of()),
                        failWhen, failWhenJson, skipWhen);
                c.output(createEnvelope(serviceName, result, startedAt, c.timestamp()));
            } catch (final Throwable e) {
                final BadRecord badRecord = Module.processError(
                        "Failed to execute action service: " + serviceName, input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

    }

    private static class CollectExecuteDoFn extends DoFn<Iterable<MElement>, MElement> {

        private final String serviceName;
        private final ActionService service;
        private final Retry retry;
        private final String failWhenJson;
        private final String skipWhenJson;

        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        private transient Filter.ConditionNode failWhen;
        private transient Filter.ConditionNode skipWhen;


        CollectExecuteDoFn(
                final String serviceName,
                final ActionService service,
                final Retry retry,
                final String failWhenJson,
                final String skipWhenJson,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.serviceName = serviceName;
            this.service = service;
            this.retry = retry;
            this.failWhenJson = failWhenJson;
            this.skipWhenJson = skipWhenJson;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.service.setup();
            this.failWhen = failWhenJson == null ? null : Filter.parse(failWhenJson);
            this.skipWhen = skipWhenJson == null ? null : Filter.parse(skipWhenJson);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Iterable<MElement> input = c.element();
            if(input == null) {
                return;
            }
            final List<MElement> elements = new ArrayList<>();
            for(final MElement element : input) {
                elements.add(element);
            }
            final Instant startedAt = Instant.now();
            try {
                final ActionResult result = applyConditions(serviceName,
                        executeWithRetry(serviceName, service, retry, elements),
                        failWhen, failWhenJson, skipWhen);
                c.output(createEnvelope(serviceName, result, startedAt, c.timestamp()));
            } catch (final Throwable e) {
                final MElement first = elements.isEmpty() ? MElement.createDummyElement(c.timestamp()) : elements.getFirst();
                final BadRecord badRecord = Module.processError(
                        "Failed to execute action service: " + serviceName, first, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

    }

    private static class SeedDoFn extends DoFn<String, MElement> {

        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(MElement.createDummyElement(c.timestamp()));
        }

    }

    @SuppressWarnings("unchecked")
    private static Map<String, Class<ActionService>> findServicesInPackage(final String packageName) {
        final ClassPath classPath;
        try {
            classPath = ClassPath.from(Action.class.getClassLoader());
        } catch (IOException ioe) {
            throw new RuntimeException("Reading classpath resource failed", ioe);
        }
        return classPath.getTopLevelClassesRecursive(packageName)
                .stream()
                .map(ClassPath.ClassInfo::load)
                .filter(clazz -> clazz.isAnnotationPresent(Service.class))
                .peek(clazz -> {
                    if(!ActionService.class.isAssignableFrom(clazz)) {
                        throw new IllegalArgumentException(
                                "action service: " + clazz.getName() + " with @Action.Service must implement ActionService");
                    }
                })
                .map(clazz -> (Class<ActionService>) clazz.asSubclass(ActionService.class))
                .collect(Collectors.toMap(
                        c -> c.getAnnotation(Service.class).name(),
                        c -> c));
    }

}
