package com.mercari.solution;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.config.*;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(MPipeline.class);

    public interface MPipelineOptions extends PipelineOptions {

        @Description("Config text body or config resource name.")
        @Validation.Required
        String getConfig();
        void setConfig(String config);

        @Description("Context for pipeline job.")
        String getContext();
        void setContext(String context);

        @Description("Config format. json or yaml.")
        @Default.Enum("unknown")
        Config.Format getFormat();
        void setFormat(Config.Format format);

        @Description("Request body content for the request source module (set per run in HTTP serve mode).")
        String getRequestBody();
        void setRequestBody(String requestBody);

        @Description("Force HTTP serve mode on/off. When unset, serve mode activates if the PORT environment variable is set (Cloud Run Service).")
        Boolean getServe();
        void setServe(Boolean serve);

        @Description("Load the config and assemble the pipeline (module validation, schema resolution, feature plan compilation) without running it.")
        @Default.Boolean(false)
        boolean getDryRun();
        void setDryRun(boolean dryRun);

        @Description("feature transform: in-memory sort buffer (MB) per key of the keyed stages before spilling to worker-local disk. Default: derived from the worker heap (16-256 MB). The transform parameter engine.spill.memoryMB takes precedence.")
        Integer getFeatureSpillMemoryMB();
        void setFeatureSpillMemoryMB(Integer featureSpillMemoryMB);

    }

    public interface MPipelineServerOptions extends MPipelineOptions {

        @Description("Working directory path for server.")
        String getWorkDir();
        void setWorkDir(String workDir);

    }

    public enum Runner {
        direct,
        dataflow,
        prism,
        portable,
        flink,
        spark
    }

    public static void main(final String[] args) throws IOException {

        final MPipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(OptionUtil.filterPipelineArgs(args))
                .as(MPipelineOptions.class);

        if(MPipelineHttpServer.isServeMode(pipelineOptions)) {
            MPipelineHttpServer.serve(pipelineOptions, args);
            return;
        }

        final Runner runner = OptionUtil.getRunner(pipelineOptions);
        LOG.info("Runner: {}", runner);

        final Config config = Config.load(
                pipelineOptions.getConfig(),
                pipelineOptions.getContext(),
                pipelineOptions.getFormat(),
                args);

        Options.setOptions(pipelineOptions, config.getOptions());

        final Pipeline pipeline = Pipeline.create(pipelineOptions);

        if(pipelineOptions.getDryRun()) {
            // assemble only: module validation, schema resolution and feature plan compilation run at
            // assembly time, so a failing config fails here without launching a job (same path as a real
            // run: system.failure.alterConfig fallback and the empty-pipeline short-circuit included)
            final Map<String, MCollection> outputs = apply(pipeline, pipelineOptions, args, config);
            final StringBuilder report = new StringBuilder("dry run: pipeline assembled successfully (not run)\n");
            for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
                if(entry.getKey().endsWith(".failures")) {
                    continue;
                }
                report.append("  output ").append(entry.getKey()).append(": ").append(entry.getValue().getSchema()).append('\n');
            }
            LOG.info(report.toString());
            System.out.println(report);
            return;
        }

        final Map<String, MCollection> outputs = apply(pipeline, pipelineOptions, args, config);

        for(final Map.Entry<String, MCollection> entry : outputs.entrySet()) {
            if(entry.getKey().endsWith(".failures")) {
                continue;
            }
            LOG.info("output: {}, schema: {}", entry.getKey(), entry.getValue().getSchema());
        }

        final PipelineResult result = pipeline.run();
        if(runner == Runner.direct || runner == Runner.prism) {
            // These runners execute the job inside this process: a non-blocking submission (Prism's job
            // service; direct with blockOnRun=false) would be killed by the JVM exiting here, and a
            // Cloud Run Job would report success having processed nothing. Dataflow must return right
            // after submission (the FlexTemplate launcher's exit is not the job's end), and flink /
            // spark / portable keep their own attached / detached semantics (an attached run() blocks
            // by itself; a detached submission is the caller's choice to not wait).
            final PipelineResult.State state = result.waitUntilFinish();
            LOG.info("Pipeline finished with state: {}", state);
            if(!PipelineResult.State.DONE.equals(state)) {
                throw new IllegalStateException("Pipeline finished with state: " + state);
            }
        }
    }

    private static Map<String, MCollection> apply(
            final Pipeline pipeline,
            final MPipelineOptions pipelineOptions,
            final String[] args,
            final Config config) throws IOException {

        if(Optional.ofNullable(config.getEmpty()).orElse(false)) {
            // the trivial transform keeps the pipeline runnable; main() runs it (running it here too
            // used to submit the pipeline twice)
            LOG.info("Empty pipeline");
            pipeline.apply("Empty", Create.of("").withCoder(StringUtf8Coder.of()));
            return new HashMap<>();
        }

        try {
            return apply(pipeline, config);
        } catch (final Throwable e) {
            LOG.error("Failed to apply pipeline config: {}", config);
            if(config.getSystem().getFailure().getAlterConfig() == null) {
                throw e;
            }
            final Config alterConfig = Config.load(
                    config.getSystem().getFailure().getAlterConfig(),
                    pipelineOptions.getContext(),
                    pipelineOptions.getFormat(),
                    args);
            return apply(pipeline, pipelineOptions, args, alterConfig);
        }
    }

    public static Map<String, MCollection> apply(final Pipeline pipeline, final Config config) {

        final Map<String, MCollection> outputs = new HashMap<>();
        final Set<String> executedModuleNames = new HashSet<>();
        final Set<String> moduleNames = moduleNames(config);
        final Set<String> controlOutputModuleNames = controlOutputModuleNames(config);

        final int size = moduleNames.size();

        try(final MErrorHandler errorHandler = MErrorHandler.createPipelineErrorHandler(pipeline, config)) {
            int preOutputSize = 0;
            while(preOutputSize < size) {
                setResult(pipeline, config.getSources(), outputs, executedModuleNames, controlOutputModuleNames, errorHandler);
                setResult(pipeline, config.getTransforms(), outputs, executedModuleNames, controlOutputModuleNames, errorHandler);
                setResult(pipeline, config.getSinks(), outputs, executedModuleNames, controlOutputModuleNames, errorHandler);
                setResult(pipeline, config.getActions(), outputs, executedModuleNames, controlOutputModuleNames, errorHandler);
                if(preOutputSize == executedModuleNames.size()) {
                    moduleNames.removeAll(executedModuleNames);
                    final String message = String.format("No input for modules: %s", String.join(",", moduleNames));
                    throw new IllegalModuleException("", "pipeline", message);
                }
                preOutputSize = executedModuleNames.size();
            }
        }

        return outputs;
    }

    private static void setResult(
            final Pipeline pipeline,
            final List<? extends ModuleConfig> moduleConfigs,
            final Map<String, MCollection> outputs,
            final Set<String> executedModuleNames,
            final Set<String> controlOutputModuleNames,
            final MErrorHandler errorHandler) {

        final List<ModuleConfig> notDoneModules = new ArrayList<>();
        for(final ModuleConfig moduleConfig : moduleConfigs) {
            // Skip null config(ketu comma)
            if(moduleConfig == null) {
                continue;
            }

            // Ignore if parameter ignore is true
            if(moduleConfig.getIgnore() != null && moduleConfig.getIgnore()) {
                continue;
            }

            // Skip already done module.
            if(executedModuleNames.contains(moduleConfig.getName())) {
                continue;
            }

            // Add queue if wait not done.
            if(!outputs.keySet().containsAll(moduleConfig.getWaits())) {
                notDoneModules.add(moduleConfig);
                continue;
            }
            final List<MCollection> waits = moduleConfig.getWaits()
                    .stream()
                    .map(outputs::get)
                    .toList();

            // Add queue if sideInputs not done.
            if(!outputs.keySet().containsAll(moduleConfig.getSideInputs())) {
                notDoneModules.add(moduleConfig);
                continue;
            }
            final List<MCollection> sideInputs = moduleConfig.getSideInputs()
                    .stream()
                    .map(outputs::get)
                    .toList();

            // Add queue if inputs not done.
            final List<String> rawInputNames = switch (moduleConfig) {
                case TransformConfig transformConfig -> transformConfig.getInputs();
                case SinkConfig sinkConfig -> sinkConfig.getInputs();
                case ActionConfig actionConfig -> actionConfig.getInputs();
                default -> new ArrayList<String>();
            };

            final List<String> inputNames = resolveInputNames(rawInputNames, outputs, executedModuleNames);
            if(inputNames == null || !outputs.keySet().containsAll(inputNames)) {
                notDoneModules.add(moduleConfig);
                continue;
            }
            final List<MCollection> inputs = inputNames.stream()
                    .map(outputs::get)
                    .collect(Collectors.toList());

            lintControlPlaneInputs(moduleConfig, inputNames, controlOutputModuleNames);

            try {
                final MCollectionTuple output = switch (moduleConfig) {
                    case SourceConfig sourceConfig -> {
                        final Source source = Source.create(
                                sourceConfig, pipeline.getOptions(), waits, errorHandler);
                        yield pipeline
                                .begin()
                                .apply(moduleConfig.getName(), source);
                    }
                    case TransformConfig transformConfig -> {
                        if(containsInputTemplate(transformConfig, rawInputNames)) {
                            throw new IllegalModuleException(
                                    "assembly-time ${input.*} template is currently supported only for sink modules with wildcard inputs");
                        }
                        final Transform transform = Transform.create(
                                transformConfig, pipeline.getOptions(), waits, sideInputs, errorHandler);
                        final MCollectionTuple transformInput = inputs.isEmpty()
                                ? MCollectionTuple.empty(pipeline)
                                : MCollectionTuple.mergeCollection(inputs);
                        yield transformInput.apply(moduleConfig.getName(), transform);
                    }
                    case SinkConfig sinkConfig -> {
                        if(containsInputTemplate(sinkConfig, rawInputNames)) {
                            yield applyFanOutSinks(pipeline, sinkConfig, rawInputNames, inputs, waits, errorHandler);
                        }
                        final Sink sink = Sink.create(
                                sinkConfig, pipeline.getOptions(), waits, errorHandler);
                        final MCollectionTuple input = inputs.isEmpty()
                                ? MCollectionTuple.empty(pipeline)
                                : MCollectionTuple.mergeCollection(inputs);
                        yield input.apply(moduleConfig.getName(), sink);
                    }
                    case ActionConfig actionConfig -> {
                        final Action action = Action.create(
                                actionConfig, pipeline.getOptions(), waits, errorHandler);
                        final MCollectionTuple input = inputs.isEmpty()
                                ? MCollectionTuple.empty(pipeline)
                                : MCollectionTuple.mergeCollection(inputs);
                        yield input.apply(moduleConfig.getName(), action);
                    }
                    default -> throw new IllegalModuleException("Not supported config type: " + moduleConfig);
                };
                outputs.putAll(output.withSource(moduleConfig.getName()).asCollectionMap());
                executedModuleNames.add(moduleConfig.getName());

            } catch (final IllegalModuleException e) {
                throw new IllegalModuleException(moduleConfig.getName(), moduleConfig.getModule(), e.errorMessages);
            } catch (final Throwable e) {
                throw new IllegalModuleException(moduleConfig.getName(), moduleConfig.getModule(), e);
            }
        }

        if(notDoneModules.isEmpty()) {
            return;
        }
        if(notDoneModules.size() == moduleConfigs.size()) {
            return;
        }
        setResult(pipeline, notDoneModules, outputs, executedModuleNames, controlOutputModuleNames, errorHandler);
    }

    /**
     * Names of modules whose outputs are control records rather than data: every action
     * (their output is the execution result envelope) and every sink (their output is the
     * write result, e.g. written file paths).
     */
    private static Set<String> controlOutputModuleNames(final Config config) {
        final Set<String> names = new HashSet<>();
        names.addAll(config.getActions().stream()
                .filter(Objects::nonNull)
                .map(ActionConfig::getName)
                .collect(Collectors.toSet()));
        names.addAll(config.getSinks().stream()
                .filter(Objects::nonNull)
                .map(SinkConfig::getName)
                .collect(Collectors.toSet()));
        return names;
    }

    /**
     * Two-plane lint: data modules (transforms/sinks) should consume data outputs
     * (sources/transforms) via inputs; control records — action envelopes and sink results —
     * are meant for action inputs or waits. Deliberate crossings (e.g. aggregating a file list
     * before a summary notification) stay allowed, so this warns instead of failing.
     */
    private static void lintControlPlaneInputs(
            final ModuleConfig moduleConfig,
            final List<String> inputNames,
            final Set<String> controlOutputModuleNames) {

        if(!(moduleConfig instanceof TransformConfig) && !(moduleConfig instanceof SinkConfig)) {
            return;
        }
        for(final String inputName : inputNames) {
            final String sourceModuleName = inputName.contains(".")
                    ? inputName.substring(0, inputName.indexOf("."))
                    : inputName;
            if(controlOutputModuleNames.contains(sourceModuleName) || controlOutputModuleNames.contains(inputName)) {
                LOG.warn("module: {} consumes control records (output of: {}) as data inputs."
                                + " Control records (action envelopes, sink results) are meant for action inputs or waits;"
                                + " if this is deliberate (e.g. aggregating results before an action), you can ignore this warning",
                        moduleConfig.getName(), inputName);
            }
        }
    }

    /**
     * Expands wildcard inputs ({@code module.*}) into the tagged outputs the referenced module
     * registered. Returns null while a referenced module has not been built yet, so the caller
     * re-queues the module. Matching no output at all is a configuration error.
     */
    private static List<String> resolveInputNames(
            final List<String> inputNames,
            final Map<String, MCollection> outputs,
            final Set<String> executedModuleNames) {

        if(inputNames.stream().noneMatch(MPipeline::isWildcardInput)) {
            return inputNames;
        }
        final List<String> resolved = new ArrayList<>();
        for(final String inputName : inputNames) {
            if(!isWildcardInput(inputName)) {
                resolved.add(inputName);
                continue;
            }
            final String moduleName = inputName.substring(0, inputName.length() - 2);
            if(!executedModuleNames.contains(moduleName)) {
                return null;
            }
            final List<String> matched = outputs.keySet().stream()
                    .filter(name -> name.startsWith(moduleName + "."))
                    // failure streams are routed via outputFailure/failureSinks, not wildcards
                    .filter(name -> !name.equals(moduleName + ".failures"))
                    .sorted()
                    .toList();
            if(matched.isEmpty()) {
                throw new IllegalModuleException("", "pipeline",
                        "wildcard input: " + inputName + " matched no output of module: " + moduleName);
            }
            resolved.addAll(matched);
        }
        return resolved;
    }

    private static boolean isWildcardInput(final String inputName) {
        return inputName.endsWith(".*");
    }

    // The reserved ${input.*} namespace activates only when the module opted into wildcard
    // inputs; otherwise the text is left untouched for runtime per-element templating.
    private static boolean containsInputTemplate(
            final ModuleConfig config,
            final List<String> rawInputNames) {

        return rawInputNames.stream().anyMatch(MPipeline::isWildcardInput)
                && config.getParameters() != null
                && TemplateUtil.containsInputTemplate(config.getParameters().toString());
    }

    /**
     * Builds one sink instance per input collection, resolving ${input.*} expressions in the
     * parameters against that collection's assembly-time context (name, tag and attributes such
     * as the source table name). Instances are named {@code <sinkName>.<tag>}.
     */
    private static MCollectionTuple applyFanOutSinks(
            final Pipeline pipeline,
            final SinkConfig sinkConfig,
            final List<String> rawInputNames,
            final List<MCollection> inputs,
            final List<MCollection> waits,
            final MErrorHandler errorHandler) {

        final Map<String, String> tags = new LinkedHashMap<>();
        for(final MCollection input : inputs) {
            tags.put(input.getName(), inputTag(input.getName(), rawInputNames));
        }
        // tag collisions across different wildcards fall back to the unambiguous full input name
        final Set<String> duplicatedTags = tags.values().stream()
                .filter(tag -> Collections.frequency(new ArrayList<>(tags.values()), tag) > 1)
                .collect(Collectors.toSet());

        for(final MCollection input : inputs) {
            final String tag = tags.get(input.getName());
            final String suffix = duplicatedTags.contains(tag) ? input.getName() : tag;
            final String childName = sinkConfig.getName() + "." + suffix;

            final Map<String, Object> context = new LinkedHashMap<>(input.getAttributes());
            context.put("name", input.getName());
            context.put("tag", tag);

            final SinkConfig childConfig = copySinkConfigForInput(sinkConfig, input.getName(), childName, context);
            final Sink sink = Sink.create(childConfig, pipeline.getOptions(), waits, errorHandler);
            MCollectionTuple
                    .mergeCollection(List.of(input))
                    .apply(childName, sink);
            LOG.info("sink module: {} fanned out to: {} for input: {}", sinkConfig.getName(), childName, input.getName());
        }
        return MCollectionTuple.empty(pipeline);
    }

    // The tag is the wildcard-matched part of the collection name ("Users" for "db.Users" via
    // "db.*"); inputs listed explicitly keep their full name.
    private static String inputTag(final String inputName, final List<String> rawInputNames) {
        for(final String rawInputName : rawInputNames) {
            if(!isWildcardInput(rawInputName)) {
                continue;
            }
            final String moduleName = rawInputName.substring(0, rawInputName.length() - 2);
            if(inputName.startsWith(moduleName + ".")) {
                return inputName.substring(moduleName.length() + 1);
            }
        }
        return inputName;
    }

    private static SinkConfig copySinkConfigForInput(
            final SinkConfig config,
            final String inputName,
            final String childName,
            final Map<String, Object> context) {

        final Gson gson = new Gson();
        final JsonObject json = gson.toJsonTree(config).getAsJsonObject();
        json.addProperty("name", childName);
        final JsonArray inputsArray = new JsonArray();
        inputsArray.add(inputName);
        json.add("inputs", inputsArray);
        json.remove("input");
        if(json.has("parameters") && json.get("parameters").isJsonObject()) {
            json.add("parameters", resolveInputTemplates(json.get("parameters"), context));
        }
        return gson.fromJson(json, SinkConfig.class);
    }

    // Walks the parameters tree and resolves ${input.*} expressions in string leaves; every
    // other ${...} survives for runtime templating.
    private static JsonElement resolveInputTemplates(final JsonElement json, final Map<String, Object> context) {
        if(json.isJsonPrimitive() && json.getAsJsonPrimitive().isString()) {
            final String text = json.getAsString();
            if(TemplateUtil.containsInputTemplate(text)) {
                return new JsonPrimitive(TemplateUtil.executeInputTemplate(text, context));
            }
            return json;
        } else if(json.isJsonArray()) {
            final JsonArray array = new JsonArray();
            for(final JsonElement element : json.getAsJsonArray()) {
                array.add(resolveInputTemplates(element, context));
            }
            return array;
        } else if(json.isJsonObject()) {
            final JsonObject object = new JsonObject();
            for(final Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
                object.add(entry.getKey(), resolveInputTemplates(entry.getValue(), context));
            }
            return object;
        }
        return json;
    }

    private static Set<String> moduleNames(final Config config) {
        final Set<String> moduleNames = new HashSet<>();
        moduleNames.addAll(config.getSources().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getIgnore() == null || !c.getIgnore())
                .map(SourceConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getTransforms().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getIgnore() == null || !c.getIgnore())
                .map(TransformConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getSinks().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getIgnore() == null || !c.getIgnore())
                .map(SinkConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getActions().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getIgnore() == null || !c.getIgnore())
                .map(ActionConfig::getName)
                .collect(Collectors.toSet()));
        return moduleNames;
    }

}
