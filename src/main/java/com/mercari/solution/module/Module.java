package com.mercari.solution.module;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.config.ModuleConfig;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class Module<T extends PInput> extends PTransform<T, MCollectionTuple> {

    protected static final Logger LOG = LoggerFactory.getLogger(Module.class);
    protected static final Counter ERROR_COUNTER = Metrics.counter("pipeline", "error");;

    private String name;
    private String module;
    private String jobName;
    private String description;

    private String parametersText;

    private Set<String> tags;
    private transient List<PCollection<?>> waits;
    private List<Logging> loggings;
    private Map<String, Logging> loggingMap;

    private Map<String, String> templateArgs;
    private Boolean failFast;
    private List<FailureSink> failureSinks;
    private Boolean outputFailure;
    private DataType outputType;

    private MPipeline.Runner runner;


    private transient MErrorHandler pipelineErrorHandler;

    @Override
    public String getName() {
        return name;
    }

    public String getModule() {
        return module;
    }

    public String getJobName() {
        return jobName;
    }

    public String getDescription() {
        return description;
    }

    public String getParametersText() {
        return parametersText;
    }

    public List<Logging> getLoggings() {
        return loggings;
    }

    public List<PCollection<?>> getWaits() {
        return waits;
    }

    public Map<String, String> getTemplateArgs() {
        return templateArgs;
    }

    public Boolean getFailFast() {
        return failFast;
    }

    public List<FailureSink> getFailureSinks() {
        return failureSinks;
    }

    public Boolean getOutputFailure() {
        return outputFailure;
    }

    public DataType getOutputType() {
        return outputType;
    }

    public MPipeline.Runner getRunner() {
        return runner;
    }

    public boolean hasFailures() {
        return failureSinks != null && !failureSinks.isEmpty();
    }

    protected void setup(
            final ModuleConfig config,
            final PipelineOptions options,
            final List<MCollection> waits,
            final MErrorHandler errorHandler) {

        this.name = config.getName();
        this.module = config.getModule();
        this.jobName = options.getJobName();
        this.description = config.getDescription();
        // Missing parameters block behaves as empty so each module's own validation reports precise errors
        this.parametersText = config.getParameters() == null ? "{}" : config.getParameters().toString();

        this.tags = Optional.ofNullable(config.getTags()).orElseGet(HashSet::new);
        this.waits = new ArrayList<>();
        for(final MCollection wait : waits) {
            this.waits.add(wait.getCollection());
        }
        this.loggings = Optional
                .ofNullable(config.getLoggings())
                .map(l -> {
                    l.forEach(ll -> ll.setModuleName(name));
                    return l;
                })
                .orElseGet(ArrayList::new);
        this.loggingMap = Logging.map(this.loggings);

        this.failFast = Optional
                .ofNullable(config.getFailFast())
                .orElseGet(() -> !OptionUtil.isStreaming(options));
        this.failureSinks = Optional
                .ofNullable(config.getFailureSinks())
                .map(l -> l.stream().map(ll -> FailureSink.create(ll, config.getName(), options)).toList())
                .orElseGet(ArrayList::new);
        this.outputFailure = Optional
                .ofNullable(config.getOutputFailure())
                .orElse(false);
        this.outputType = config.getOutputType();
        this.runner = OptionUtil.getRunner(options);
        this.templateArgs = config.getArgs();

        this.pipelineErrorHandler = errorHandler;
    }

    /**
     * Resolves the module schema (schema-redesign.md Phase 3).
     * {@code parameters.schema} is the canonical location; the module top-level {@code schema}
     * remains as a deprecated fallback. Declaring both is an error, as is {@code parameters.schema}
     * on a module that does not declare schema support (top-level on an unsupported module stays a
     * warning for backward compatibility until Phase 5).
     */
    protected static Schema resolveSchema(
            final ModuleConfig config,
            final Schema topLevelSchema,
            final boolean schemaSupported) {

        final JsonObject parameters = config.getParameters();
        if(parameters != null && parameters.has("schema")) {
            if(!schemaSupported) {
                throw new IllegalModuleException(
                        "module: " + config.getModule() + " does not support schema. remove parameters.schema");
            }
            if(topLevelSchema != null) {
                throw new IllegalModuleException(
                        "schema must not be declared both at the module top level and in parameters.schema");
            }
            return Schema.parse(parameters.get("schema"));
        }
        if(topLevelSchema != null) {
            if(schemaSupported) {
                LOG.warn("module: {} declares schema at the module top level; this location is deprecated — move it into parameters.schema (see docs module/common/schema.md)",
                        config.getName());
            } else {
                LOG.warn("module: {} declares schema but does not consume it; the declaration is ignored (this will become an error in a future version)",
                        config.getName());
            }
        }
        return topLevelSchema;
    }

    protected <ParameterT> ParameterT getParameters(Class<ParameterT> clazz) {
        try {
            final JsonObject parametersJson = Config.convertConfigJson(parametersText, Config.Format.json);
            final ParameterT parameters = new Gson().fromJson(parametersJson, clazz);
            if (parameters == null) {
                throw new IllegalModuleException("parameters must not be empty");
            }
            return parameters;
        } catch (final IllegalModuleException e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalModuleException("Illegal parameters for class: " + clazz, e);
        }
    }

    private ErrorHandler.BadRecordErrorHandler<?> registerErrorHandler(final PInput input) {
        final FailureSink.FailureSinks failureSinks = FailureSink.merge(getFailureSinks());
        return input.getPipeline().registerBadRecordErrorHandler(failureSinks);
    }

    @Override
    public MCollectionTuple expand(T input) {
        if(getFailFast()) {
            try(final MErrorHandler errorHandler = MErrorHandler.empty()) {
                return expand(input, errorHandler);
            }
        } else if(pipelineErrorHandler != null && !pipelineErrorHandler.isEmpty()) {
            return expand(input, pipelineErrorHandler);
        } else if(hasFailures()) {
            try(final MErrorHandler errorHandler = MErrorHandler.of(registerErrorHandler(input))) {
                return expand(input, errorHandler);
            }
        } else {
            try(final MErrorHandler errorHandler = MErrorHandler.dummy(input.getPipeline())) {
                return expand(input, errorHandler);
            }
        }
    }

    public abstract MCollectionTuple expand(
            final T inputs,
            final MErrorHandler errorHandler);

    public static BadRecord processError(
            final String message,
            final Map<String, Object> input,
            final Throwable e,
            final Boolean failFast) {

        return processError(message, MElement.of(input, Instant.now()), e, failFast);
    }

    public static BadRecord processError(
            final String message,
            final MElement input,
            final Throwable e,
            final Boolean failFast) {

        ERROR_COUNTER.inc();
        final String errorMessage = FailureUtil.convertThrowableMessage(e);
        LOG.error("{} for input: {} error: {}", message, input, errorMessage);
        if(failFast) {
            throw new IllegalStateException(errorMessage, e);
        }
        return FailureUtil.createBadRecord(input, message, e);
    }

}
