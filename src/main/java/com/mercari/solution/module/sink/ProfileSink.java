package com.mercari.solution.module.sink;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Sink;
import com.mercari.solution.util.cloud.google.StorageUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.profile.ProfileAccumulator;
import com.mercari.solution.util.pipeline.profile.ProfileAxis;
import com.mercari.solution.util.pipeline.profile.ProfileCombineFn;
import com.mercari.solution.util.pipeline.profile.ProfileRenderer;
import com.mercari.solution.util.pipeline.profile.ProfileSpec;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data profiling sink: observes the input dataset with Apache DataSketches
 * (KLL quantiles, CPC distinct counts, Frequent Items, Theta set sketches, VarOpt sampling,
 * streaming moments/correlation) and writes a single self-contained interactive HTML report
 * that embeds the view payload, the run manifest and the mergeable sketch binaries.
 */
@Sink.Module(name="profile")
public class ProfileSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(ProfileSink.class);

    private static class Parameters implements Serializable {

        private JsonElement output;      // string shorthand or {report:, sketches:}
        private FieldsFilter fields;
        private String values;           // show | hide
        private List<String> keys;
        private JsonElement segments;    // array of field names or [{field:, topK:}]
        private JsonElement time;        // field name or {field:, granularity:}
        private String mode;             // union (default) | compare — how multiple inputs are treated
        private String baseline;         // compare mode: reference input for drift metrics
        private List<List<String>> compare;   // declared field pairs, e.g. [[list_price, sold_price]]
        private String compareWith;      // uri of a past report to compare against
        private String accuracy;         // low | default | high
        private Associations associations;
        private SampleParameters sample;
        private Report report;
        private Integer fanout;

        private transient String reportOutput;
        private transient String sketchesOutput;

        private static class FieldsFilter implements Serializable {
            private List<String> include;
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
            private String locale;
        }

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
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
                if(output.isJsonPrimitive()) {
                    // ok: string shorthand
                } else if(output.isJsonObject()) {
                    final JsonObject o = output.getAsJsonObject();
                    if(!o.has("report")) {
                        errorMessages.add("parameters.output object form requires `report`");
                    }
                } else {
                    errorMessages.add("parameters.output must be a string or an object {report, sketches}");
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
            if(fanout != null && fanout < 1) {
                errorMessages.add("parameters.fanout must be positive");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
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
        }

        private void resolveOutput(final String name, final org.apache.beam.sdk.options.PipelineOptions options) {
            if(output != null) {
                if(output.isJsonPrimitive()) {
                    reportOutput = output.getAsString();
                } else {
                    final JsonObject o = output.getAsJsonObject();
                    reportOutput = o.get("report").getAsString();
                    if(o.has("sketches")) {
                        sketchesOutput = o.get("sketches").getAsString();
                    }
                }
                return;
            }
            // convention fallback: workDir (direct runner server) → tempLocation
            final String workDir = options.as(MPipeline.MPipelineServerOptions.class).getWorkDir();
            final String jobName = options.getJobName();
            if(workDir != null && !workDir.isEmpty()) {
                reportOutput = joinPath(workDir, name, "report.html");
            } else if(options.getTempLocation() != null) {
                reportOutput = joinPath(options.getTempLocation(), "profile", jobName, name, "report.html");
            } else {
                throw new IllegalArgumentException(
                        "profile sink could not resolve an output location. set parameters.output (or pipeline tempLocation)");
            }
        }

        private static String joinPath(final String base, final String... parts) {
            final StringBuilder sb = new StringBuilder(base.endsWith("/") ? base.substring(0, base.length() - 1) : base);
            for(final String part : parts) {
                sb.append("/").append(part);
            }
            return sb.toString();
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
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
            return axes;
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();
        parameters.resolveOutput(getName(), inputs.getPipeline().getOptions());

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        if(PCollection.IsBounded.UNBOUNDED.equals(input.isBounded())) {
            throw new IllegalArgumentException("profile sink supports batch (bounded) inputs only");
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
            throw new IllegalArgumentException("profile sink has no profilable fields for input schema: " + inputSchema);
        }
        final List<ProfileAxis> axes = parameters.parseAxes(spec);

        // compare mode: the union inputs become a comparison axis with a fixed baseline
        if("compare".equals(parameters.mode)) {
            final List<String> inputNames = new ArrayList<>(inputs.getAll().keySet());
            if(inputNames.size() < 2) {
                throw new IllegalArgumentException("parameters.mode compare requires two or more inputs");
            }
            if(parameters.baseline != null && !inputNames.contains(parameters.baseline)) {
                throw new IllegalArgumentException("parameters.baseline must be one of the inputs: " + inputNames);
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
                        throw new IllegalArgumentException("parameters.compare field not found in input schema: " + path);
                    }
                    if(!ProfileSpec.ProfileType.NUMERIC.equals(fieldSpec.profileType)) {
                        throw new IllegalArgumentException("parameters.compare fields must be numeric: " + path);
                    }
                }
                comparePairs.add(pair.toArray(new String[0]));
            }
        }

        final ProfileRenderer.Config rendererConfig = new ProfileRenderer.Config();
        rendererConfig.title = parameters.report != null && parameters.report.title != null
                ? parameters.report.title
                : getName();
        rendererConfig.showValues = showValues;
        rendererConfig.jobName = getJobName();
        rendererConfig.moduleName = getName();
        rendererConfig.inputNames = new ArrayList<>(inputs.getAllInputs());
        rendererConfig.expandedParametersJson = buildExpandedParameters(parameters, spec).toString();
        rendererConfig.sketchesOutput = parameters.sketchesOutput;
        rendererConfig.axes = axes;
        rendererConfig.comparePairs = comparePairs;
        rendererConfig.compareWithSource = parameters.compareWith;

        final boolean globalWindow = input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows;
        final Combine.Globally<MElement, ProfileAccumulator> combine = Combine.globally(new ProfileCombineFn(spec));
        final PCollection<ProfileAccumulator> profile = input
                .apply("Profile", globalWindow
                        ? combine.withFanout(parameters.fanout)
                        : combine.withFanout(parameters.fanout).withoutDefaults())
                .setCoder(SerializableCoder.of(ProfileAccumulator.class));

        // comparison axes: same combine per (axis, group value) key, delivered to the renderer as a side input
        PCollectionView<Map<String, ProfileAccumulator>> subProfilesView = null;
        if(!axes.isEmpty()) {
            final ProfileSpec groupSpec = ProfileSpec.of(
                    inputSchema,
                    parameters.fields == null || parameters.fields.include == null ? null : new HashSet<>(parameters.fields.include),
                    parameters.fields == null || parameters.fields.exclude == null ? null : new HashSet<>(parameters.fields.exclude),
                    null,
                    parameters.accuracy,
                    false,
                    false);
            subProfilesView = input
                    .apply("KeyByAxis", ParDo.of(new KeyByAxisDoFn(axes)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()))
                    .apply("ProfileGroups", Combine.perKey(new ProfileCombineFn(groupSpec)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ProfileAccumulator.class)))
                    .apply("AsSubProfiles", View.asMap());
        }

        final RenderDoFn renderDoFn = new RenderDoFn(
                rendererConfig, parameters.reportOutput, parameters.sketchesOutput, subProfilesView);
        final PCollection<MElement> results = profile
                .apply("RenderReport", subProfilesView == null
                        ? ParDo.of(renderDoFn)
                        : ParDo.of(renderDoFn).withSideInputs(subProfilesView));

        return MCollectionTuple.of(results, createOutputSchema());
    }

    private static Schema createOutputSchema() {
        return Schema.builder()
                .withField("output", Schema.FieldType.STRING)
                .withField("rows", Schema.FieldType.INT64)
                .withField("fields", Schema.FieldType.INT64)
                .withField("bytes", Schema.FieldType.INT64)
                .build();
    }

    /** The expanded (defaults applied) configuration recorded in the manifest / appendix. */
    private static JsonObject buildExpandedParameters(final Parameters parameters, final ProfileSpec spec) {
        final JsonObject expanded = new JsonObject();
        expanded.addProperty("output", parameters.reportOutput);
        if(parameters.sketchesOutput != null) {
            expanded.addProperty("outputSketches", parameters.sketchesOutput);
        }
        expanded.addProperty("values", parameters.values);
        expanded.addProperty("accuracy", parameters.accuracy);
        expanded.addProperty("associations.numeric", parameters.associations.numeric);
        expanded.addProperty("sample.enabled", spec.isSampleEnabled());
        expanded.addProperty("sample.k", spec.getSketchParameters().sampleK);
        expanded.addProperty("fanout", parameters.fanout);
        if(parameters.keys != null && !parameters.keys.isEmpty()) {
            expanded.addProperty("keys", String.join(",", parameters.keys));
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
        if(parameters.compareWith != null) {
            expanded.addProperty("compareWith", parameters.compareWith);
        }
        expanded.addProperty("profiledFields", spec.getFields().size());
        expanded.addProperty("skippedFields", spec.getSkipped().size());
        return expanded;
    }

    /** Assigns each row to its group on every comparison axis (a row is emitted once per axis). */
    private static class KeyByAxisDoFn extends DoFn<MElement, KV<String, MElement>> {

        private final List<ProfileAxis> axes;

        KeyByAxisDoFn(final List<ProfileAxis> axes) {
            this.axes = axes;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            try {
                final Map<String, Object> primitives = element.asPrimitiveMap();
                for(final ProfileAxis axis : axes) {
                    final String group = ProfileAxis.Kind.inputs.equals(axis.kind)
                            ? axis.groupOfInputIndex(element.getIndex())
                            : axis.groupValue(ProfileSpec.getValue(primitives, axis.field));
                    if(group != null) {
                        c.output(KV.of(axis.groupKey(group), element));
                    }
                }
            } catch (final Throwable e) {
                // rows that fail extraction are already counted as error rows by the global profile
            }
        }
    }

    private static class RenderDoFn extends DoFn<ProfileAccumulator, MElement> {

        private final ProfileRenderer.Config config;
        private final String reportOutput;
        private final String sketchesOutput;
        private final PCollectionView<Map<String, ProfileAccumulator>> subProfilesView;

        RenderDoFn(
                final ProfileRenderer.Config config,
                final String reportOutput,
                final String sketchesOutput,
                final PCollectionView<Map<String, ProfileAccumulator>> subProfilesView) {

            this.config = config;
            this.reportOutput = reportOutput;
            this.sketchesOutput = sketchesOutput;
            this.subProfilesView = subProfilesView;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            // rendering queries the sketches, which mutates their lazily-sorted internal state —
            // work on copies so the input element and the cached side input stay byte-identical
            final ProfileAccumulator accumulator = c.element() == null ? null : c.element().copy();
            if(accumulator == null) {
                return;
            }
            final Map<String, ProfileAccumulator> subProfiles = new java.util.HashMap<>();
            if(subProfilesView != null) {
                for(final Map.Entry<String, ProfileAccumulator> entry : c.sideInput(subProfilesView).entrySet()) {
                    subProfiles.put(entry.getKey(), entry.getValue().copy());
                }
            }
            final ProfileRenderer.PastReport past = loadPastReport(config.compareWithSource);
            final ProfileRenderer.Result result = ProfileRenderer.render(accumulator, subProfiles, past, config);
            write(reportOutput, result.html.getBytes(StandardCharsets.UTF_8), "text/html");
            LOG.info("profile sink wrote report to: {} ({} bytes, rows: {})",
                    reportOutput, result.html.length(), accumulator.getRowCount());
            if(sketchesOutput != null) {
                final String sketchesJson = ProfileRenderer.buildSketches(accumulator, config).toString();
                write(sketchesOutput, sketchesJson.getBytes(StandardCharsets.UTF_8), "application/json");
                LOG.info("profile sink wrote sketches to: {}", sketchesOutput);
            }

            final MElement output = MElement.builder()
                    .withString("output", reportOutput)
                    .withInt64("rows", accumulator.getRowCount())
                    .withInt64("fields", (long) accumulator.getFieldCount())
                    .withInt64("bytes", (long) result.html.getBytes(StandardCharsets.UTF_8).length)
                    .withEventTime(c.timestamp())
                    .build();
            c.output(output);
        }

        private static ProfileRenderer.PastReport loadPastReport(final String source) {
            if(source == null) {
                return null;
            }
            final String html;
            if(source.startsWith("gs://")) {
                html = StorageUtil.readString(source);
            } else {
                try {
                    html = Files.readString(Paths.get(source));
                } catch (final Exception e) {
                    throw new IllegalStateException("failed to read compareWith target: " + source, e);
                }
            }
            if(html == null) {
                throw new IllegalStateException("compareWith target not found: " + source);
            }
            return ProfileRenderer.PastReport.parse(source, html);
        }

        private static void write(final String path, final byte[] bytes, final String contentType) throws Exception {
            if(path.startsWith("gs://")) {
                StorageUtil.writeBytes(path, bytes, contentType, java.util.Map.of(), java.util.Map.of());
            } else {
                final Path localPath = Paths.get(path);
                if(localPath.getParent() != null) {
                    Files.createDirectories(localPath.getParent());
                }
                Files.write(localPath, bytes);
            }
        }
    }
}
