package com.mercari.solution.module.transform;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.MPipeline;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Transform;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.profile.ProfileAxis;
import com.mercari.solution.util.pipeline.profile.ProfileReport;
import com.mercari.solution.util.pipeline.profile.ProfileSpec;
import com.mercari.solution.util.pipeline.profile.ProfileStages;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data profiling transform (docs/design/profile-dsl.md): observes the input dataset and emits
 * what it looks like as records — one per field (default), per group × field ({@code groups}),
 * per value ({@code values}), per bin ({@code bins}), per declared field pair ({@code pairs}),
 * per field against the target ({@code target}), per key ({@code keys}) and one {@code summary}
 * — and, when {@code output.report} is set, as a single-file HTML report. Statistics that
 * compare groups come from exact counts of a second pass over fixed edges. Batch only.
 */
@Transform.Module(name = "profile")
public class ProfileTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(ProfileTransform.class);

    private static final Set<String> PARAMETERS = Set.of(
            "output", "outputs", "run", "fields", "values", "keys", "target", "segments", "time", "mode", "baseline",
            "compare", "bins", "drift", "accuracy", "associations", "sample", "report", "fanout");
    /** Declared in the design (profile-dsl.md §13, stages 2 and 3) and not built yet: rejected rather than ignored. */
    private static final Set<String> NOT_IMPLEMENTED = Set.of("previous", "expectations", "unnest", "preset");
    private static final Set<String> OUTPUTS = Set.of("bins", "values");

    private static class Parameters implements Serializable {

        private JsonElement output;      // report uri, or {report:, payload:}
        private List<String> outputs;    // optional record outputs: bins, values
        private Run run;
        private FieldsFilter fields;
        private String values;           // show | hide
        private List<String> keys;
        private JsonElement target;      // field name or {field:, positive:} — the binary outcome every field is related to
        private JsonElement segments;    // array of field names or [{field:, topK:}]
        private JsonElement time;        // field name or {field:, granularity:}
        private String mode;             // union (default) | compare — how multiple inputs are treated
        private String baseline;         // compare mode: reference input of the drift statistics
        private List<List<String>> compare;   // declared field pairs, e.g. [[list_price, sold_price]]
        private Bins bins;
        private Drift drift;
        private String accuracy;         // low | default | high
        private Associations associations;
        private SampleParameters sample;
        private Report report;
        private Integer fanout;

        private static class Run implements Serializable {
            private String id;
            private String dataset;
            private String partition;
        }

        private static class FieldsFilter implements Serializable {
            private List<String> include;
            private List<String> exclude;
        }

        private static class Bins implements Serializable {
            private String mode;         // exact (the only one built)
            private Integer count;
            private JsonElement edges;   // {field: [split points]}
        }

        private static class Drift implements Serializable {
            private String axis;
            private List<String> exclude;
        }

        private static class Associations implements Serializable {
            private String numeric;      // all | none
        }

        private static class SampleParameters implements Serializable {
            private Boolean enabled;
            private Integer k;
        }

        private static class Report implements Serializable {
            private String title;
        }

        private void validate(final JsonObject raw) {
            final List<String> errorMessages = new ArrayList<>();
            for(final String key : raw.keySet()) {
                if(NOT_IMPLEMENTED.contains(key)) {
                    errorMessages.add("parameters." + key + " is not implemented yet (docs/design/profile-dsl.md §13)");
                } else if("compareWith".equals(key)) {
                    errorMessages.add("parameters.compareWith was a parameter of the removed profile sink; comparing with a previous run (parameters.previous) is not implemented yet");
                } else if(!PARAMETERS.contains(key)) {
                    errorMessages.add("unknown parameter: parameters." + key);
                }
            }
            if(values != null && !"show".equals(values) && !"hide".equals(values)) {
                errorMessages.add("parameters.values must be `show` or `hide`");
            }
            if(accuracy != null && !Set.of("low", "default", "high").contains(accuracy)) {
                errorMessages.add("parameters.accuracy must be one of `low`, `default`, `high`");
            }
            if(associations != null && associations.numeric != null
                    && !Set.of("all", "none").contains(associations.numeric)) {
                errorMessages.add("parameters.associations.numeric must be `all` or `none`");
            }
            if(output != null) {
                if(output.isJsonObject()) {
                    for(final String key : output.getAsJsonObject().keySet()) {
                        if("sketches".equals(key)) {
                            errorMessages.add("parameters.output.sketches was a parameter of the removed profile sink; the sketches output is not implemented yet");
                        } else if(!Set.of("report", "payload").contains(key)) {
                            errorMessages.add("parameters.output accepts `report` and `payload`, not `" + key + "`");
                        }
                    }
                } else if(!output.isJsonPrimitive()) {
                    errorMessages.add("parameters.output must be a report uri or an object {report, payload}");
                }
            }
            if(outputs != null) {
                for(final String name : outputs) {
                    if(!OUTPUTS.contains(name)) {
                        errorMessages.add("parameters.outputs accepts `bins` and `values`, not `" + name
                                + "` (`sketches` and `sample` are not implemented yet)");
                    }
                }
            }
            if(sample != null && sample.k != null && sample.k < 1) {
                errorMessages.add("parameters.sample.k must be positive");
            }
            if(segments != null && !segments.isJsonArray()) {
                errorMessages.add("parameters.segments must be an array of field names or {field, topK} objects");
            }
            if(time != null && !time.isJsonPrimitive() && !time.isJsonObject()) {
                errorMessages.add("parameters.time must be a field name or a {field, granularity} object");
            }
            if(time != null && time.isJsonObject()) {
                for(final String key : time.getAsJsonObject().keySet()) {
                    if(Set.of("timezone", "groups").contains(key)) {
                        errorMessages.add("parameters.time." + key + " is not implemented yet (docs/design/profile-dsl.md §13)");
                    }
                }
            }
            if(target != null) {
                if(target.isJsonObject()) {
                    final JsonObject o = target.getAsJsonObject();
                    if(!o.has("field") || !o.get("field").isJsonPrimitive()) {
                        errorMessages.add("parameters.target object form requires `field`");
                    }
                    if(o.has("positive") && !o.get("positive").isJsonPrimitive()) {
                        errorMessages.add("parameters.target.positive must be a scalar value");
                    }
                    if(o.has("type")) {
                        errorMessages.add("parameters.target.type (a numeric target) is not implemented yet (docs/design/profile-dsl.md §13)");
                    }
                } else if(!target.isJsonPrimitive() || !target.getAsJsonPrimitive().isString()) {
                    errorMessages.add("parameters.target must be a field name or a {field, positive} object");
                }
            }
            if(mode != null && !Set.of("union", "compare").contains(mode)) {
                errorMessages.add("parameters.mode must be `union` or `compare`");
            }
            if(baseline != null && !"compare".equals(mode)) {
                errorMessages.add("parameters.baseline requires mode: compare");
            }
            if(compare != null) {
                for(final List<String> pair : compare) {
                    if(pair == null || pair.size() != 2) {
                        errorMessages.add("parameters.compare entries must be pairs of two field names");
                    }
                }
            }
            if(bins != null) {
                if(bins.mode != null && !"exact".equals(bins.mode)) {
                    errorMessages.add("sketch".equals(bins.mode)
                            ? "parameters.bins.mode: sketch is not implemented yet (docs/design/profile-dsl.md §13)"
                            : "parameters.bins.mode must be `exact`");
                }
                if(bins.count != null && (bins.count < 2 || bins.count > 100)) {
                    errorMessages.add("parameters.bins.count must be between 2 and 100");
                }
                if(bins.edges != null && !bins.edges.isJsonObject()) {
                    errorMessages.add(bins.edges.isJsonPrimitive() && "previous".equals(bins.edges.getAsString())
                            ? "parameters.bins.edges: previous is not implemented yet (docs/design/profile-dsl.md §13)"
                            : "parameters.bins.edges must be an object {field: [split points]}");
                }
            }
            if(fanout != null && fanout < 1) {
                errorMessages.add("parameters.fanout must be positive");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if(values == null) {
                values = "show";
            }
            if(accuracy == null) {
                accuracy = "default";
            }
            if(associations == null) {
                associations = new Associations();
            }
            if(associations.numeric == null) {
                associations.numeric = "all";
            }
            if(fanout == null) {
                fanout = 16;
            }
            if(outputs == null) {
                outputs = List.of("bins", "values");
            }
        }

        private String outputOf(final String name) {
            if(output == null) {
                return null;
            }
            if(output.isJsonPrimitive()) {
                return "report".equals(name) ? output.getAsString() : null;
            }
            final JsonObject o = output.getAsJsonObject();
            return o.has(name) && o.get(name).isJsonPrimitive() ? o.get(name).getAsString() : null;
        }

        /** Declares the target on the spec from the shorthand (field name) or {field, positive} form. */
        private void applyTarget(final ProfileSpec spec) {
            if(target == null) {
                return;
            }
            final String field;
            Object positive = null;
            if(target.isJsonPrimitive()) {
                field = target.getAsString();
            } else {
                final JsonObject o = target.getAsJsonObject();
                field = o.get("field").getAsString();
                if(o.has("positive")) {
                    // keep the literal text (`1` stays "1", not 1.0): the spec coerces it by the field's type
                    final JsonPrimitive p = o.getAsJsonPrimitive("positive");
                    positive = p.isBoolean() ? (Object) p.getAsBoolean() : p.getAsString();
                }
            }
            try {
                spec.withTarget(field, positive);
            } catch (final IllegalArgumentException e) {
                throw new IllegalModuleException("parameters.target: " + e.getMessage());
            }
        }

        /** Expands segments/time shorthand into comparison axes, validated against the resolved spec. */
        private List<ProfileAxis> parseAxes(final ProfileSpec spec) {
            final List<ProfileAxis> axes = new ArrayList<>();
            final List<String> errorMessages = new ArrayList<>();
            if(segments != null) {
                for(final JsonElement entry : segments.getAsJsonArray()) {
                    final ProfileAxis axis = new ProfileAxis();
                    axis.kind = ProfileAxis.Kind.segments;
                    if(entry.isJsonPrimitive()) {
                        axis.field = entry.getAsString();
                    } else if(entry.isJsonObject()) {
                        final JsonObject o = entry.getAsJsonObject();
                        if(!o.has("field")) {
                            errorMessages.add("parameters.segments entry requires `field`: " + entry);
                            continue;
                        }
                        axis.field = o.get("field").getAsString();
                        if(o.has("topK")) {
                            axis.topK = o.get("topK").getAsInt();
                        }
                    } else {
                        errorMessages.add("parameters.segments entry must be a field name or {field, topK}: " + entry);
                        continue;
                    }
                    axes.add(axis);
                }
            }
            if(time != null) {
                final ProfileAxis axis = new ProfileAxis();
                axis.kind = ProfileAxis.Kind.time;
                axis.granularity = "month";
                if(time.isJsonPrimitive()) {
                    axis.field = time.getAsString();
                } else {
                    final JsonObject o = time.getAsJsonObject();
                    if(!o.has("field")) {
                        errorMessages.add("parameters.time requires `field`");
                    } else {
                        axis.field = o.get("field").getAsString();
                    }
                    if(o.has("granularity")) {
                        axis.granularity = o.get("granularity").getAsString();
                        if(!ProfileAxis.GRANULARITIES.contains(axis.granularity)) {
                            errorMessages.add("parameters.time.granularity must be one of " + ProfileAxis.GRANULARITIES);
                        }
                    }
                }
                if(axis.field != null) {
                    axes.add(axis);
                }
            }

            for(final ProfileAxis axis : axes) {
                final List<ProfileSpec.FieldSpec> fieldSpecs = spec.getFields();
                for(int i = 0; i < fieldSpecs.size(); i++) {
                    if(fieldSpecs.get(i).path.equals(axis.field)) {
                        axis.fieldIndex = i;
                        axis.sourceType = fieldSpecs.get(i).sourceType;
                        axis.symbols = fieldSpecs.get(i).symbols;
                        if(ProfileAxis.Kind.time.equals(axis.kind)
                                && !ProfileSpec.ProfileType.TIMESTAMP.equals(fieldSpecs.get(i).profileType)) {
                            errorMessages.add("parameters.time field must be a timestamp/date field: " + axis.field);
                        }
                        break;
                    }
                }
                if(axis.fieldIndex < 0) {
                    errorMessages.add("parameters." + axis.kind + " field not found in input schema: " + axis.field);
                }
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(String.join(", ", errorMessages));
            }
            return axes;
        }

        /** The declared split points per field: sorted, strictly increasing, on numeric-like fields only. */
        private Map<String, double[]> parseDeclaredEdges(final ProfileSpec spec) {
            final Map<String, double[]> declared = new HashMap<>();
            if(bins == null || bins.edges == null) {
                return declared;
            }
            final List<String> errorMessages = new ArrayList<>();
            for(final Map.Entry<String, JsonElement> entry : bins.edges.getAsJsonObject().entrySet()) {
                final String path = entry.getKey();
                final ProfileSpec.FieldSpec fieldSpec = spec.getFields().stream()
                        .filter(f -> f.path.equals(path)).findFirst().orElse(null);
                if(fieldSpec == null) {
                    errorMessages.add("parameters.bins.edges field not found in input schema: " + path);
                    continue;
                }
                if(!Set.of(ProfileSpec.ProfileType.NUMERIC, ProfileSpec.ProfileType.ARRAY_LENGTH).contains(fieldSpec.profileType)) {
                    errorMessages.add("parameters.bins.edges fields must be numeric: " + path);
                    continue;
                }
                if(!entry.getValue().isJsonArray() || entry.getValue().getAsJsonArray().isEmpty()) {
                    errorMessages.add("parameters.bins.edges." + path + " must be a non-empty array of numbers");
                    continue;
                }
                final double[] edges = new double[entry.getValue().getAsJsonArray().size()];
                boolean valid = true;
                for(int i = 0; i < edges.length; i++) {
                    final JsonElement edge = entry.getValue().getAsJsonArray().get(i);
                    if(!edge.isJsonPrimitive() || !edge.getAsJsonPrimitive().isNumber()
                            || !Double.isFinite(edge.getAsDouble())) {
                        valid = false;
                        break;
                    }
                    edges[i] = edge.getAsDouble();
                    if(i > 0 && !(edges[i] > edges[i - 1])) {
                        valid = false;
                        break;
                    }
                }
                if(!valid) {
                    errorMessages.add("parameters.bins.edges." + path + " must be strictly increasing finite numbers");
                    continue;
                }
                declared.put(path, edges);
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(String.join(", ", errorMessages));
            }
            return declared;
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        // every parameter is optional: a module without a `parameters` block is a valid declaration
        final String parametersText = getParametersText();
        final boolean declared = parametersText != null && !parametersText.isBlank() && !"null".equals(parametersText.strip());
        final Parameters parameters = declared ? getParameters(Parameters.class) : new Parameters();
        final JsonObject raw = declared
                ? com.mercari.solution.config.Config.convertConfigJson(parametersText, com.mercari.solution.config.Config.Format.json)
                : new JsonObject();
        parameters.validate(raw);
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        if(PCollection.IsBounded.UNBOUNDED.equals(input.isBounded())) {
            throw new IllegalModuleException("profile transform supports batch (bounded) inputs only");
        }
        // one result per run: the edges of the counting pass are a side input of the global window
        if(!(input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows)) {
            throw new IllegalModuleException(
                    "profile transform requires the global window (input or strategy window: " + input.getWindowingStrategy().getWindowFn() + ")");
        }
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final boolean showValues = "show".equals(parameters.values);
        final boolean sampleEnabled = showValues
                && (parameters.sample == null || !Boolean.FALSE.equals(parameters.sample.enabled));
        final ProfileSpec spec = ProfileSpec.of(
                inputSchema,
                parameters.fields == null || parameters.fields.include == null ? null : new HashSet<>(parameters.fields.include),
                parameters.fields == null || parameters.fields.exclude == null ? null : new HashSet<>(parameters.fields.exclude),
                parameters.keys == null ? null : new HashSet<>(parameters.keys),
                parameters.accuracy,
                sampleEnabled,
                "all".equals(parameters.associations.numeric));
        if(parameters.sample != null && parameters.sample.k != null) {
            spec.getSketchParameters().sampleK = parameters.sample.k;
        }
        if(spec.getFields().isEmpty()) {
            throw new IllegalModuleException("profile transform has no profilable fields for input schema: " + inputSchema);
        }
        if(parameters.keys != null) {
            for(final String key : parameters.keys) {
                if(spec.getFields().stream().noneMatch(f -> f.path.equals(key))) {
                    throw new IllegalModuleException("parameters.keys field not found in the profiled fields: " + key);
                }
            }
        }
        parameters.applyTarget(spec);
        final List<ProfileAxis> axes = parameters.parseAxes(spec);

        // compare mode: the union inputs become a comparison axis with a fixed baseline
        if("compare".equals(parameters.mode)) {
            final List<String> inputNames = new ArrayList<>(inputs.getAll().keySet());
            if(inputNames.size() < 2) {
                throw new IllegalModuleException("parameters.mode compare requires two or more inputs");
            }
            if(parameters.baseline != null && !inputNames.contains(parameters.baseline)) {
                throw new IllegalModuleException("parameters.baseline must be one of the inputs: " + inputNames);
            }
            final ProfileAxis inputsAxis = new ProfileAxis();
            inputsAxis.kind = ProfileAxis.Kind.inputs;
            inputsAxis.field = "(input)";
            inputsAxis.inputNames = inputNames;
            inputsAxis.baseline = parameters.baseline != null ? parameters.baseline : inputNames.getFirst();
            inputsAxis.topK = inputNames.size();
            axes.addFirst(inputsAxis);
        }

        // declared comparison pairs must be numeric fields of the profiled schema
        final List<String[]> comparePairs = new ArrayList<>();
        if(parameters.compare != null) {
            for(final List<String> pair : parameters.compare) {
                for(final String path : pair) {
                    final ProfileSpec.FieldSpec fieldSpec = spec.getFields().stream()
                            .filter(f -> f.path.equals(path)).findFirst().orElse(null);
                    if(fieldSpec == null) {
                        throw new IllegalModuleException("parameters.compare field not found in input schema: " + path);
                    }
                    if(!ProfileSpec.ProfileType.NUMERIC.equals(fieldSpec.profileType)) {
                        throw new IllegalModuleException("parameters.compare fields must be numeric: " + path);
                    }
                }
                comparePairs.add(pair.toArray(new String[0]));
            }
        }

        final ProfileReport.Config config = new ProfileReport.Config();
        config.title = parameters.report != null && parameters.report.title != null
                ? parameters.report.title
                : getName();
        config.showValues = showValues;
        config.jobName = getJobName();
        config.moduleName = getName();
        config.inputNames = new ArrayList<>(inputs.getAllInputs());
        config.runId = parameters.run != null && parameters.run.id != null ? parameters.run.id : getJobName();
        config.dataset = parameters.run != null && parameters.run.dataset != null ? parameters.run.dataset : getName();
        config.partition = parameters.run == null ? null : parameters.run.partition;
        config.axes = axes;
        config.comparePairs = comparePairs;
        config.declaredEdges = parameters.parseDeclaredEdges(spec);
        if(parameters.bins != null && parameters.bins.count != null) {
            config.binsCount = parameters.bins.count;
        }
        if(parameters.drift != null) {
            config.driftAxis = parameters.drift.axis;
            if(parameters.drift.exclude != null) {
                config.driftExclude = new HashSet<>(parameters.drift.exclude);
            }
            if(config.driftAxis != null && axes.stream().noneMatch(axis -> axis.label().equals(config.driftAxis))) {
                throw new IllegalModuleException("parameters.drift.axis must be one of the declared axes "
                        + axes.stream().map(ProfileAxis::label).toList() + ": " + config.driftAxis);
            }
        }
        config.outputBins = parameters.outputs.contains("bins");
        config.outputValues = parameters.outputs.contains("values");
        config.reportOutput = parameters.outputOf("report");
        config.payloadOutput = parameters.outputOf("payload");
        // the second pass runs only when something reads it (profile-dsl.md §9.5)
        config.countingPass = !axes.isEmpty() || spec.getTarget() != null || !comparePairs.isEmpty() || config.outputBins;
        config.expandedParametersJson = buildExpandedParameters(parameters, spec, config).toString();

        for(final String path : new String[] { config.reportOutput, config.payloadOutput }) {
            if(path != null && !ResourceUtil.isStorageUri(path)
                    && getRunner() != null
                    && !Set.of(MPipeline.Runner.direct, MPipeline.Runner.prism).contains(getRunner())) {
                LOG.warn("profile transform `{}` output `{}` is not a storage uri: on runner {} it is written to a worker's local disk",
                        getName(), path, getRunner());
            }
        }

        final ProfileStages.Outputs outputs = ProfileStages.apply(
                input, spec, config, parameters.fanout, Boolean.TRUE.equals(getFailFast()));
        if(errorHandler != null) {
            errorHandler.addError(outputs.failures());
        }
        return MCollectionTuple
                .of(outputs.fields(), ProfileReport.fieldsSchema())
                .and("groups", outputs.groups(), ProfileReport.groupsSchema())
                .and("values", outputs.values(), ProfileReport.valuesSchema())
                .and("bins", outputs.bins(), ProfileReport.binsSchema())
                .and("pairs", outputs.pairs(), ProfileReport.pairsSchema())
                .and("target", outputs.target(), ProfileReport.targetSchema())
                .and("keys", outputs.keys(), ProfileReport.keysSchema())
                .and("summary", outputs.summary(), ProfileReport.summarySchema());
    }

    /** The expanded (defaults applied) configuration recorded in the manifest. */
    private static JsonObject buildExpandedParameters(
            final Parameters parameters, final ProfileSpec spec, final ProfileReport.Config config) {

        final JsonObject expanded = new JsonObject();
        if(config.reportOutput != null) {
            expanded.addProperty("output.report", config.reportOutput);
        }
        if(config.payloadOutput != null) {
            expanded.addProperty("output.payload", config.payloadOutput);
        }
        expanded.addProperty("outputs", String.join(",", parameters.outputs));
        expanded.addProperty("run.id", config.runId);
        expanded.addProperty("run.dataset", config.dataset);
        if(config.partition != null) {
            expanded.addProperty("run.partition", config.partition);
        }
        expanded.addProperty("values", parameters.values);
        expanded.addProperty("accuracy", parameters.accuracy);
        expanded.addProperty("associations.numeric", parameters.associations.numeric);
        expanded.addProperty("sample.enabled", spec.isSampleEnabled());
        expanded.addProperty("sample.k", spec.getSketchParameters().sampleK);
        expanded.addProperty("fanout", parameters.fanout);
        expanded.addProperty("bins.mode", "exact");
        expanded.addProperty("bins.count", config.binsCount);
        if(config.hasDeclaredEdges()) {
            expanded.addProperty("bins.edges", parameters.bins.edges.toString());
        }
        expanded.addProperty("countingPass", config.countingPass);
        if(parameters.keys != null && !parameters.keys.isEmpty()) {
            expanded.addProperty("keys", String.join(",", parameters.keys));
        }
        if(spec.getTarget() != null) {
            expanded.addProperty("target", spec.getTarget().path);
            expanded.addProperty("target.positive", spec.getTarget().positiveLabel());
        }
        if(parameters.segments != null) {
            expanded.addProperty("segments", parameters.segments.toString());
        }
        if(parameters.time != null) {
            expanded.addProperty("time", parameters.time.toString());
        }
        if(parameters.mode != null) {
            expanded.addProperty("mode", parameters.mode);
            if(parameters.baseline != null) {
                expanded.addProperty("baseline", parameters.baseline);
            }
        }
        if(parameters.compare != null && !parameters.compare.isEmpty()) {
            expanded.addProperty("compare", parameters.compare.toString());
        }
        final ProfileAxis driftAxis = config.driftAxis();
        if(driftAxis != null) {
            expanded.addProperty("drift.axis", driftAxis.label());
        }
        if(!config.driftExclude.isEmpty()) {
            expanded.addProperty("drift.exclude", String.join(",", config.driftExclude));
        }
        expanded.addProperty("profiledFields", spec.getFields().size());
        expanded.addProperty("skippedFields", spec.getSkipped().size());
        return expanded;
    }
}
