package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.feature.SourceContract.Json;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * The "intent" side of the DSL (work-feature.md §3–§5): the parsed {@code parameters} block of a
 * {@code feature} transform. Parsing is syntactic only; cross-references and semantics are resolved by
 * {@link FeaturePlanCompiler}.
 */
public class FeatureSpec implements Serializable {

    public enum Scope { row, context, sequence, population }
    public enum FitMode {
        expanding, fold, statik;

        /** static / fold: statistics fitted over the input and applied by lookup (a fit stage, not a keyed stage). */
        public boolean isLookup() { return this != expanding; }

        /** The value written to the {@code fit} column coordinate. */
        public String token() { return this == statik ? "static" : name(); }

        public static boolean isLookupToken(final String token) { return "static".equals(token) || "fold".equals(token); }
    }
    public enum NullPolicy { keep, fillZero, indicator }
    public enum Combine { product, zip }

    public record LineageEntry(List<String> fields, String from, String eventTime) implements Serializable {}
    public record EntityDef(String name, List<String> keys, Duration minInterval) implements Serializable {}
    public record ContextDef(String name, List<String> keys) implements Serializable {}
    public record BaselineDef(String name, String context, String expr) implements Serializable {}

    public static class Window implements Serializable {
        public Integer maxEvents;
        public Duration maxAge;
        public String filter;

        /** Short token for generated names (§4.3): 365d, n20, 365d_n20, all. */
        public String token() {
            final List<String> parts = new ArrayList<>();
            if (maxAge != null) parts.add(Durations.shortName(maxAge));
            if (maxEvents != null) parts.add("n" + maxEvents);
            return parts.isEmpty() ? "all" : String.join("_", parts);
        }
    }

    public static class Op implements Serializable {
        public String type;
        public List<String> fields = new ArrayList<>();
        public String expr;
        public String predicate;
        public Integer k;
        public List<Double> halflife = new ArrayList<>();
        public List<String> funcs = new ArrayList<>();
        public String value;
        public List<String> unit = new ArrayList<>();
        public String decayBy;
        /** Output name override (replaces the field / anonymous-expression segment, or the op suffix). */
        public String as;
        /** countByValue / ratioByValue: emit one column per listed value instead of a map. */
        public List<String> values = new ArrayList<>();
    }

    public static class KeySet implements Serializable {
        public List<String> keys = new ArrayList<>();
        public List<Window> windows = new ArrayList<>();
        public String structure;
        public String hierarchyJson;
        public String shrinkageJson;
        public String parentRef;
        public Integer maxDepth;
    }

    /** One factorization output: {@code pair: [a, b]}, {@code embedding: field (dims)} or {@code sum: true}. */
    public static class FmOutput implements Serializable {
        public String kind;
        public List<String> pair = new ArrayList<>();
        public String embedding;
        public Integer dims;
        public String as;
    }

    public static class Target implements Serializable {
        public String field;
        public String expr;
        public boolean ref;
        public List<String> stats = new ArrayList<>();
        /** Target name override (replaces the field name or the anonymous e{n}). */
        public String as;
    }

    public static class FeatureDef implements Serializable {
        public String name;
        public Scope scope;
        public String type;
        public String computeAtExpression;
        public Duration validFor;

        // row
        public String expr;
        public String input;
        public List<String> inputs = new ArrayList<>();
        public List<String> derive = new ArrayList<>();
        public boolean cyclical;
        public List<Double> edges = new ArrayList<>();
        public List<String> values = new ArrayList<>();
        public String baseline;
        public String on;

        // context / sequence
        public String context;
        public boolean excludeSelf;
        public List<Op> ops = new ArrayList<>();
        public String entity;
        public List<Window> windows = new ArrayList<>();

        // population
        public List<KeySet> keySets = new ArrayList<>();
        public List<Target> targets = new ArrayList<>();
        public Combine combine = Combine.product;
        public String naming;
        public Integer maxFeatures;
        public String offset;
        public boolean emitConfidence;
        public String shrinkageJson;
        public String smoothingJson;
        public String fitJson;

        // factorization
        public String variant;
        public List<String> fields = new ArrayList<>();
        public Integer latentDim;
        public String taskTarget;
        public String taskTargetExpr;
        public String taskOffset;
        public List<FmOutput> fmOutputs = new ArrayList<>();
        public Integer epochs;
        public Double reg;
        public Long seed;

        public String location() {
            return "features." + name;
        }
    }

    public static class FitSpec implements Serializable {
        public String orderBy;
        public FitMode mode = FitMode.expanding;
        public Duration minHistory;
        public String groupBy;
        /** Number of folds for {@code fit.mode: fold} (out-of-fold statistics). */
        public Integer folds = 5;
        /** Root URI of fit artifacts ({@code <uri>/<planHash>/<block>.avro}); null = fit in-pipeline only. */
        public String artifactUri;
        /** Re-fit and overwrite even when an artifact for the plan hash exists. */
        public boolean refit;
        /** Explicit artifact version replacing the plan hash in artifact paths (pin a fitted version). */
        public String artifactId;

        static void parseArtifact(final JsonObject fit, final FitSpec spec) {
            if (fit == null || !fit.has("artifact")) return;
            final JsonElement a = fit.get("artifact");
            if (a.isJsonPrimitive()) {
                spec.artifactUri = a.getAsString();
            } else if (a.isJsonObject()) {
                spec.artifactUri = Json.string(a.getAsJsonObject(), "uri");
                spec.refit = Json.bool(a.getAsJsonObject(), "refit", false);
                if (Json.string(a.getAsJsonObject(), "id") != null) spec.artifactId = Json.string(a.getAsJsonObject(), "id");
            }
        }
    }

    public static class OutputSpec implements Serializable {
        public String prefix = "";
        public NullPolicy nullPolicy = NullPolicy.keep;
        public List<String> exclude = new ArrayList<>();
        public String groupBy;
        public List<String> parentFields = new ArrayList<>();
        /** Field name of the child array in grouped output (default "rows"; rename to dodge reserved words). */
        public String childName = "rows";
        /** Which input fields pass through to the output: all (default) | keys (time.field, entity / context keys, parentFields) | none. */
        public String passThrough = "all";
    }

    /** Engine (runtime) knobs that do not change the plan: {@code engine.spill} of the keyed stages' sorter. */
    public static class EngineSpec implements Serializable {
        /** In-memory sort buffer per key (MB); null = derived from the worker heap. */
        public Integer spillMemoryMB;
        /** Spill directory on the worker; null = java.io.tmpdir. */
        public String spillDirectory;
        /** Deflate the spilled chunk files. */
        public boolean spillCompress = false;
    }

    public List<LineageEntry> lineage = new ArrayList<>();
    public EngineSpec engine = new EngineSpec();
    public String timeField;
    public List<String> orderTieBreak = new ArrayList<>();
    public String predictAtExpression;
    public AvailableAt predictAt;
    public List<EntityDef> entities = new ArrayList<>();
    public List<ContextDef> contexts = new ArrayList<>();
    public List<BaselineDef> baselines = new ArrayList<>();
    public List<FeatureDef> features = new ArrayList<>();
    public FitSpec fit = new FitSpec();
    public OutputSpec output = new OutputSpec();

    public static FeatureSpec parse(final JsonObject parameters, final Diagnostics diagnostics) {
        final FeatureSpec spec = new FeatureSpec();

        if (parameters.has("lineage") && parameters.get("lineage").isJsonArray()) {
            for (final JsonElement e : parameters.getAsJsonArray("lineage")) {
                if (!e.isJsonObject()) continue;
                final JsonObject o = e.getAsJsonObject();
                final List<String> fields = Json.strings(o, "fields");
                final String from = Json.string(o, "from");
                if (fields.isEmpty() || from == null) {
                    diagnostics.error("lineage.invalid", "lineage", "each lineage entry requires 'fields' and 'from'");
                    continue;
                }
                spec.lineage.add(new LineageEntry(fields, from, Json.string(o, "eventTime")));
            }
        } else {
            diagnostics.error("lineage.missing", "lineage", "lineage is required (fields → source mapping)");
        }

        if (parameters.has("time") && parameters.get("time").isJsonObject()) {
            final JsonObject time = parameters.getAsJsonObject("time");
            spec.timeField = Json.string(time, "field");
            spec.orderTieBreak = Json.strings(time, "orderTieBreak");
        }
        if (spec.timeField == null) {
            diagnostics.error("time.field", "time", "time.field is required");
        }

        spec.predictAtExpression = Json.string(parameters, "predictAt");
        if (spec.predictAtExpression == null) {
            diagnostics.error("predictAt.missing", "predictAt", "predictAt is required");
            spec.predictAt = AvailableAt.atEventTime();
        } else {
            try {
                spec.predictAt = AvailableAt.parseTimeExpression(spec.predictAtExpression);
                if (!spec.predictAt.isStatic()) {
                    diagnostics.error("predictAt.invalid", "predictAt", "predictAt must be event_time ± duration");
                }
            } catch (final IllegalArgumentException e) {
                diagnostics.error("predictAt.invalid", "predictAt", e.getMessage());
                spec.predictAt = AvailableAt.atEventTime();
            }
        }

        for (final JsonObject o : objects(parameters, "entities")) {
            final String name = Json.string(o, "name");
            final List<String> keys = Json.strings(o, "keys");
            if (name == null || keys.isEmpty()) {
                diagnostics.error("entities.invalid", "entities", "each entity requires 'name' and 'keys'");
                continue;
            }
            spec.entities.add(new EntityDef(name, keys, Json.duration(o, "minInterval", null, diagnostics, "entities." + name)));
        }
        for (final JsonObject o : objects(parameters, "contexts")) {
            final String name = Json.string(o, "name");
            final List<String> keys = Json.strings(o, "keys");
            if (name == null || keys.isEmpty()) {
                diagnostics.error("contexts.invalid", "contexts", "each context requires 'name' and 'keys'");
                continue;
            }
            spec.contexts.add(new ContextDef(name, keys));
        }
        for (final JsonObject o : objects(parameters, "baselines")) {
            final String name = Json.string(o, "name");
            final String expr = Json.string(o, "expr");
            if (name == null || expr == null) {
                diagnostics.error("baselines.invalid", "baselines", "each baseline requires 'name' and 'expr'");
                continue;
            }
            spec.baselines.add(new BaselineDef(name, Json.string(o, "context"), expr));
        }

        for (final JsonObject o : objects(parameters, "features")) {
            final FeatureDef def = parseFeature(o, diagnostics);
            if (def != null) spec.features.add(def);
        }
        if (spec.features.isEmpty()) {
            diagnostics.error("features.missing", "features", "features must not be empty");
        }

        if (parameters.has("fit") && parameters.get("fit").isJsonObject()) {
            final JsonObject fit = parameters.getAsJsonObject("fit");
            spec.fit.orderBy = Json.string(fit, "orderBy");
            spec.fit.mode = parseFitMode(Json.string(fit, "mode"), diagnostics, "fit");
            spec.fit.minHistory = Json.duration(fit, "minHistory", null, diagnostics, "fit");
            spec.fit.groupBy = Json.string(fit, "groupBy");
            if (Json.integer(fit, "folds") != null) spec.fit.folds = Json.integer(fit, "folds");
            FitSpec.parseArtifact(fit, spec.fit);
        }

        if (parameters.has("output") && parameters.get("output").isJsonObject()) {
            final JsonObject out = parameters.getAsJsonObject("output");
            spec.output.prefix = Json.string(out, "prefix") == null ? "" : Json.string(out, "prefix");
            final String nullPolicy = Json.string(out, "nullPolicy");
            if (nullPolicy != null) {
                try {
                    spec.output.nullPolicy = NullPolicy.valueOf(nullPolicy);
                } catch (final IllegalArgumentException e) {
                    diagnostics.error("output.nullPolicy", "output", "nullPolicy must be keep | fillZero | indicator");
                }
            }
            spec.output.exclude = Json.strings(out, "exclude");
            spec.output.groupBy = Json.string(out, "groupBy");
            spec.output.parentFields = Json.strings(out, "parentFields");
            if (Json.string(out, "childName") != null) spec.output.childName = Json.string(out, "childName");
            if (Json.string(out, "passThrough") != null) {
                final String passThrough = Json.string(out, "passThrough");
                if (!List.of("all", "keys", "none").contains(passThrough)) {
                    diagnostics.error("output.passThrough", "output", "output.passThrough must be all | keys | none: " + passThrough);
                } else {
                    spec.output.passThrough = passThrough;
                }
            }
        }
        if (parameters.has("engine") && parameters.get("engine").isJsonObject()) {
            final JsonObject engine = parameters.getAsJsonObject("engine");
            if (engine.has("spill") && engine.get("spill").isJsonObject()) {
                final JsonObject spill = engine.getAsJsonObject("spill");
                final Integer memoryMB = Json.integer(spill, "memoryMB");
                if (memoryMB != null) {
                    if (memoryMB < 1) {
                        diagnostics.error("engine.spill.memoryMB", "engine.spill", "engine.spill.memoryMB must be >= 1: " + memoryMB);
                    } else {
                        spec.engine.spillMemoryMB = memoryMB;
                    }
                }
                spec.engine.spillDirectory = Json.string(spill, "directory");
                spec.engine.spillCompress = Json.bool(spill, "compress", false);
            }
        }
        return spec;
    }

    static FitMode parseFitMode(final String text, final Diagnostics diagnostics, final String location) {
        if (text == null) return FitMode.expanding;
        return switch (text) {
            case "expanding" -> FitMode.expanding;
            case "fold" -> FitMode.fold;
            case "static" -> FitMode.statik;
            default -> {
                diagnostics.error("fit.mode", location, "fit.mode must be expanding | fold | static: " + text);
                yield FitMode.expanding;
            }
        };
    }

    private static FeatureDef parseFeature(final JsonObject o, final Diagnostics diagnostics) {
        final FeatureDef def = new FeatureDef();
        def.name = Json.string(o, "name");
        if (def.name == null) {
            diagnostics.error("features.name", "features", "each feature requires 'name'");
            return null;
        }
        final String loc = def.location();
        final String scope = Json.string(o, "scope");
        if (scope == null) {
            diagnostics.error("features.scope", loc, "scope is required (row | context | sequence | population)");
            return null;
        }
        try {
            def.scope = Scope.valueOf(scope);
        } catch (final IllegalArgumentException e) {
            diagnostics.error("features.scope", loc, "scope must be row | context | sequence | population: " + scope);
            return null;
        }
        def.type = Json.string(o, "type");
        def.computeAtExpression = Json.string(o, "computeAt");
        def.validFor = Json.duration(o, "validFor", null, diagnostics, loc);

        def.expr = Json.string(o, "expr");
        def.input = Json.string(o, "input");
        def.inputs = Json.strings(o, "inputs");
        def.derive = Json.strings(o, "derive");
        def.cyclical = Json.bool(o, "cyclical", false);
        def.edges = doubles(o, "edges");
        def.values = Json.strings(o, "values");
        def.baseline = Json.string(o, "baseline");
        def.on = Json.string(o, "on");

        def.context = Json.string(o, "context");
        def.excludeSelf = Json.bool(o, "excludeSelf", false);
        def.entity = Json.string(o, "entity");
        def.windows = parseWindows(o, diagnostics, loc);
        if (o.has("ops")) {
            for (final JsonElement e : arrayOf(o.get("ops"))) {
                final Op op = parseOp(e, diagnostics, loc);
                if (op != null) def.ops.add(op);
            }
        }

        for (final JsonObject ks : objects(o, "keySets")) {
            final KeySet keySet = new KeySet();
            keySet.keys = Json.strings(ks, "keys");
            keySet.windows = parseWindows(ks, diagnostics, loc + ".keySets");
            keySet.structure = Json.string(ks, "structure");
            keySet.hierarchyJson = ks.has("hierarchy") ? ks.get("hierarchy").toString() : null;
            keySet.shrinkageJson = ks.has("shrinkage") && ks.get("shrinkage").isJsonObject() ? ks.get("shrinkage").toString() : null;
            keySet.parentRef = Json.string(ks, "parentRef");
            keySet.maxDepth = Json.integer(ks, "maxDepth");
            def.keySets.add(keySet);
        }
        for (final JsonObject t : objects(o, "targets")) {
            final Target target = new Target();
            if (t.has("field") && t.get("field").isJsonObject()) {
                final JsonObject f = t.getAsJsonObject("field");
                target.ref = true;
            } else {
                target.field = Json.string(t, "field");
            }
            target.expr = Json.string(t, "expr");
            target.stats = Json.strings(t, "stats");
            target.as = Json.string(t, "as");
            def.targets.add(target);
        }
        final String combine = Json.string(o, "combine");
        if (combine != null) {
            try {
                def.combine = Combine.valueOf(combine);
            } catch (final IllegalArgumentException e) {
                diagnostics.error("features.combine", loc, "combine must be product | zip: " + combine);
            }
        }
        def.naming = Json.string(o, "naming");
        def.maxFeatures = Json.integer(o, "maxFeatures");
        def.offset = Json.string(o, "offset");
        def.emitConfidence = Json.bool(o, "emitConfidence", false);
        def.shrinkageJson = o.has("shrinkage") && o.get("shrinkage").isJsonObject() ? o.get("shrinkage").toString() : null;
        def.smoothingJson = o.has("smoothing") && o.get("smoothing").isJsonObject() ? o.get("smoothing").toString() : null;
        def.variant = Json.string(o, "variant");
        def.fields = Json.strings(o, "fields");
        def.latentDim = Json.integer(o, "latentDim");
        if (o.has("task") && o.get("task").isJsonObject()) {
            final JsonObject task = o.getAsJsonObject("task");
            def.taskTarget = Json.string(task, "target") != null ? Json.string(task, "target") : Json.string(task, "field");
            def.taskTargetExpr = Json.string(task, "expr");
            def.taskOffset = Json.string(task, "offset");
        }
        for (final JsonObject out : objects(o, "outputs")) {
            final FmOutput fm = new FmOutput();
            fm.as = Json.string(out, "as");
            if (out.has("pair")) {
                fm.kind = "pair";
                fm.pair = Json.strings(out, "pair");
            } else if (out.has("embedding")) {
                fm.kind = "embedding";
                fm.embedding = Json.string(out, "embedding");
                fm.dims = Json.integer(out, "dims");
            } else if (Json.bool(out, "sum", false)) {
                fm.kind = "sum";
            } else {
                diagnostics.error("factorization.outputs", loc, "each output requires pair: [a, b], embedding: <field> or sum: true");
                continue;
            }
            def.fmOutputs.add(fm);
        }
        if (o.has("als") && o.get("als").isJsonObject()) {
            final JsonObject als = o.getAsJsonObject("als");
            try {
                def.epochs = Json.integer(als, "epochs");
                def.reg = Json.string(als, "reg") == null ? null : Double.parseDouble(Json.string(als, "reg"));
                def.seed = Json.string(als, "seed") == null ? null : Long.parseLong(Json.string(als, "seed"));
            } catch (final RuntimeException e) {
                diagnostics.error("factorization.als", loc, "als.epochs / reg / seed must be numeric: " + als);
            }
        }
        def.fitJson = o.has("fit") && o.get("fit").isJsonObject() ? o.get("fit").toString() : null;
        return def;
    }

    private static List<Window> parseWindows(final JsonObject o, final Diagnostics diagnostics, final String loc) {
        final List<Window> windows = new ArrayList<>();
        final List<JsonElement> elements = new ArrayList<>();
        if (o.has("windows")) {
            elements.addAll(arrayOf(o.get("windows")));
            if (o.has("window")) {
                diagnostics.error("window.both", loc, "use either 'window' or 'windows', not both");
            }
        } else if (o.has("window")) {
            elements.add(o.get("window"));
        }
        for (final JsonElement e : elements) {
            if (!e.isJsonObject()) {
                diagnostics.error("window.invalid", loc, "window must be an object with maxEvents / maxAge / filter");
                continue;
            }
            final JsonObject w = e.getAsJsonObject();
            final Window window = new Window();
            window.maxEvents = Json.integer(w, "maxEvents");
            window.maxAge = Json.duration(w, "maxAge", null, diagnostics, loc);
            window.filter = Json.string(w, "filter");
            for (final String key : w.keySet()) {
                if (!List.of("maxEvents", "maxAge", "filter").contains(key)) {
                    diagnostics.error("window.nearEdge", loc,
                            "window." + key + " is not allowed: the near edge is derived from sources.ingestionLag (§4.3)");
                }
            }
            windows.add(window);
        }
        return windows;
    }

    private static Op parseOp(final JsonElement e, final Diagnostics diagnostics, final String loc) {
        final Op op = new Op();
        if (e.isJsonPrimitive()) {
            op.type = e.getAsString();
            return op;
        }
        if (!e.isJsonObject()) {
            diagnostics.error("ops.invalid", loc, "each op must be a string or an object");
            return null;
        }
        final JsonObject o = e.getAsJsonObject();
        op.type = Json.string(o, "type");
        if (op.type == null) {
            diagnostics.error("ops.type", loc, "op.type is required");
            return null;
        }
        op.fields = Json.strings(o, "fields");
        final String field = Json.string(o, "field");
        if (field != null) op.fields.add(field);
        op.expr = Json.string(o, "expr");
        op.predicate = Json.string(o, "predicate");
        op.k = Json.integer(o, "k");
        op.halflife = doubles(o, "halflife");
        op.funcs = Json.strings(o, "funcs");
        op.value = Json.string(o, "value");
        op.unit = Json.strings(o, "unit");
        op.decayBy = Json.string(o, "decayBy");
        op.as = Json.string(o, "as");
        op.values = Json.strings(o, "values");
        return op;
    }

    private static List<Double> doubles(final JsonObject o, final String key) {
        final List<Double> list = new ArrayList<>();
        if (!o.has(key)) return list;
        for (final JsonElement e : arrayOf(o.get(key))) {
            if (e.isJsonPrimitive()) list.add(e.getAsDouble());
        }
        return list;
    }

    private static List<JsonElement> arrayOf(final JsonElement e) {
        final List<JsonElement> list = new ArrayList<>();
        if (e == null || e.isJsonNull()) return list;
        if (e.isJsonArray()) {
            for (final JsonElement v : e.getAsJsonArray()) list.add(v);
        } else {
            list.add(e);
        }
        return list;
    }

    private static List<JsonObject> objects(final JsonObject parent, final String key) {
        final List<JsonObject> list = new ArrayList<>();
        if (parent == null || !parent.has(key) || !parent.get(key).isJsonArray()) return list;
        final JsonArray array = parent.getAsJsonArray(key);
        for (final JsonElement e : array) {
            if (e.isJsonObject()) list.add(e.getAsJsonObject());
        }
        return list;
    }

}
