package com.mercari.solution.util.pipeline.evaluation;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.pipeline.feature.FeatureLineage;
import com.mercari.solution.util.pipeline.feature.FeaturePlanCompiler;
import com.mercari.solution.util.pipeline.glm.Family;
import com.mercari.solution.util.pipeline.glm.StatMath;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mercari.solution.util.pipeline.glm.SpecJson.bool;
import static com.mercari.solution.util.pipeline.glm.SpecJson.integer;
import static com.mercari.solution.util.pipeline.glm.SpecJson.longValue;
import static com.mercari.solution.util.pipeline.glm.SpecJson.number;
import static com.mercari.solution.util.pipeline.glm.SpecJson.string;
import static com.mercari.solution.util.pipeline.glm.SpecJson.strings;

/**
 * Parsed and validated parameters of the evaluation transform (design: docs/design/evaluation-dsl.md). Rides
 * inside DoFns, so it holds plain fields only. {@link #parse} reads the config, {@link #resolve} applies the
 * upstream contract (feature roles / lineage) and the input schema, and fixes the column layout of
 * {@link EvaluationRow#x}: the prediction columns, then the calibration fields, then the utility field.
 */
public final class EvaluationSpec implements Serializable {

    public static final String ROLE_SELECTION = "selection";
    public static final String ROLE_REPORT = "report";
    public static final List<String> ROLES = List.of(ROLE_SELECTION, ROLE_REPORT);
    /** the name under which the baseline's own metrics are reported; not available to a prediction set */
    public static final String BASELINE_NAME = "baseline";

    public static final String OFFSET_SCALE_PROB = "prob";
    public static final String OFFSET_SCALE_LOG = "log";
    public static final List<String> OFFSET_SCALES = List.of(OFFSET_SCALE_PROB, OFFSET_SCALE_LOG);

    public static final String TABLE_RELIABILITY = "reliability";
    public static final String TABLE_EDGE = "edge";
    public static final List<String> TABLE_TYPES = List.of(TABLE_RELIABILITY, TABLE_EDGE);
    public static final String BY_PREDICTION = "prediction";
    public static final String BY_DIVERGENCE = "divergence";
    public static final String BY_FIELD = "field";
    public static final List<String> BYS = List.of(BY_PREDICTION, BY_DIVERGENCE, BY_FIELD);

    public static final int MAX_BOOTSTRAP = 10_000;

    /** A prediction set: a column in a baseline form, or a grouped softmax of a score with an optional offset. */
    public static final class Prediction implements Serializable {
        public String name;
        /** the probability-like column and its form (null when the set is a score) */
        public String field;
        public String form;
        public String scoreField;
        public String offsetField;
        public String offsetScale = OFFSET_SCALE_PROB;
        public double temperature = 1d;
        /** position of the set's first column in {@link EvaluationRow#x} (score sets: score, then offset) */
        public int offset;

        public boolean isScore() {
            return scoreField != null;
        }

        public int columns() {
            return isScore() ? (offsetField != null ? 2 : 1) : 1;
        }

        public String describe() {
            return isScore()
                    ? name + "=softmax(" + scoreField + (offsetField != null ? " + " + offsetField + "[" + offsetScale + "]" : "") + (temperature != 1d ? " / T=" + temperature : "") + ")"
                    : name + "=" + field + ":" + form;
        }
    }

    /** A time split with its role: a time range on the time field, or a value of the split column. */
    public static final class Split implements Serializable {
        public String name;
        public String role;
        public String from;
        public String to;
        public Long fromMillis;
        public Long toMillis;

        public boolean isSelection() {
            return ROLE_SELECTION.equals(role);
        }
    }

    /** A calibration table (design §7). */
    public static final class Table implements Serializable {
        public String type;
        public String by;
        public String field;
        public int bins;
        public double[] edges;
        public double[] thresholds;
        /** position of {@code field} in {@link EvaluationRow#x} (by: field) */
        public int fieldIndex = -1;

        public boolean isQuantile() {
            return TABLE_RELIABILITY.equals(type) && !BY_FIELD.equals(by);
        }

        public boolean isEdge() {
            return TABLE_EDGE.equals(type);
        }

        /** Number of bins: the quantile bins, the edge intervals (edges + 1), or the thresholds. */
        public int binCount() {
            if (isEdge()) return thresholds.length;
            return isQuantile() ? bins : edges.length + 1;
        }
    }

    /** A declared slice: a categorical field, or a period bucket of a time field. */
    public static final class Slice implements Serializable {
        public String field;
        public String bucket;
        public String fieldType;

        public String name() {
            return bucket == null ? field : field + "/" + bucket;
        }
    }

    public String family;
    public String group;
    public String labelField;
    public String labelExpr;
    public boolean normalizeTies = true;
    public String baselineField;
    public String baselineForm;
    public String timeField;
    public String timeFieldType;
    public String weightField;
    public List<String> rowId = new ArrayList<>();
    public String utilityField;
    public String manifest;
    public List<Prediction> predictions = new ArrayList<>();
    /** splits by time range (declaration order); empty when {@link #splitField} is set */
    public List<Split> splits = new ArrayList<>();
    /** splits by column: the column, and split name → role */
    public String splitField;
    public int bootstrapSamples = 1000;
    public long bootstrapSeed = 0L;
    public String bootstrapUnit;
    public List<Table> tables = new ArrayList<>();
    public List<Slice> slices = new ArrayList<>();
    /** SHA-256 (16 hex, the feature plan hash width) of the canonical parameters without the manifest location */
    public String parametersHash;
    public String manifestPlanHash;
    public String manifestOutputHash;
    /** informational notes produced by resolution (role defaults applied) */
    public List<String> notes = new ArrayList<>();

    /** utility field position in {@link EvaluationRow#x} (−1 when none) */
    public int utilityIndex = -1;
    /** every numeric column carried in {@link EvaluationRow#x}, in order */
    public List<String> rowColumns = new ArrayList<>();

    private EvaluationSpec() {}

    public Family family() {
        return Family.of(family);
    }

    public boolean isGrouped() {
        return Family.GROUPED_MULTINOMIAL.equals(family());
    }

    public boolean hasBaseline() {
        return baselineField != null;
    }

    public boolean hasBootstrap() {
        return bootstrapSamples > 0;
    }

    public boolean hasUtility() {
        return utilityIndex >= 0;
    }

    public boolean hasQuantileTables() {
        return tables.stream().anyMatch(Table::isQuantile);
    }

    /** The split names in declaration order (time-range splits) or role-map order (column splits). */
    public List<String> splitNames() {
        final List<String> names = new ArrayList<>();
        for (final Split s : splits) names.add(s.name);
        return names;
    }

    public Split split(final String name) {
        for (final Split s : splits) if (s.name.equals(name)) return s;
        return null;
    }

    /** The split of a time (time-range splits): its name, or null when the time falls outside every range. */
    public String splitOf(final long time) {
        for (final Split s : splits) {
            if (s.fromMillis != null && time < s.fromMillis) continue;
            if (s.toMillis != null && time > s.toMillis) continue;
            return s.name;
        }
        return null;
    }

    /** Prediction names, the baseline first (its index is 0 in every per-unit array). */
    public List<String> predictionNames() {
        final List<String> names = new ArrayList<>();
        names.add(BASELINE_NAME);
        for (final Prediction p : predictions) names.add(p.name);
        return names;
    }

    // ---- parsing -------------------------------------------------------------------------------------------

    /** Parses the {@code parameters} block; throws {@link IllegalArgumentException} listing every error. */
    public static EvaluationSpec parse(final JsonObject p) {
        final List<String> errors = new ArrayList<>();
        final EvaluationSpec s = new EvaluationSpec();
        if (p == null) throw new IllegalArgumentException("parameters must not be empty");

        s.family = string(p, "family");
        if (s.family == null) s.family = Family.GROUPED_MULTINOMIAL.id();
        if (Family.of(s.family) == null) {
            errors.add("unknown family '" + s.family + "' (available in this version: " + Family.GROUPED_MULTINOMIAL.id() + ", " + Family.BINOMIAL.id() + ")");
        } else if (s.family() != Family.GROUPED_MULTINOMIAL && s.family() != Family.BINOMIAL) {
            errors.add("family " + s.family + " is not supported by the evaluation transform in this version (available: " + Family.GROUPED_MULTINOMIAL.id() + ", " + Family.BINOMIAL.id() + ")");
        }
        s.group = string(p, "group");

        final JsonElement label = p.get("label");
        if (label != null && !label.isJsonNull()) {
            if (label.isJsonPrimitive()) {
                s.labelField = label.getAsString();
            } else if (label.isJsonObject()) {
                final JsonObject o = label.getAsJsonObject();
                s.labelField = string(o, "field");
                s.labelExpr = string(o, "expr");
                final Boolean ties = bool(o, "normalizeTies");
                if (ties != null) s.normalizeTies = ties;
                if (s.labelField != null && s.labelExpr != null) errors.add("label: specify either field or expr, not both");
            } else {
                errors.add("label must be a field name or an object {field | expr, normalizeTies}");
            }
        }

        final List<String> forms = Family.formsFor(s.family);
        final JsonElement baseline = p.get("baseline");
        if (baseline != null && !baseline.isJsonNull()) {
            if (baseline.isJsonPrimitive()) {
                s.baselineField = baseline.getAsString();
                s.baselineForm = forms.get(0);
            } else if (baseline.isJsonObject()) {
                final JsonObject o = baseline.getAsJsonObject();
                s.baselineField = string(o, "field");
                s.baselineForm = string(o, "form");
                if (s.baselineForm == null) s.baselineForm = forms.get(0);
                if (s.baselineField == null) errors.add("baseline.field is required when baseline is declared");
                if (!forms.contains(s.baselineForm)) errors.add("baseline.form '" + s.baselineForm + "' is not valid for family " + s.family + " (available: " + forms + ")");
            } else {
                errors.add("baseline must be a field name or an object {field, form}");
            }
        }

        final JsonElement time = p.get("time");
        if (time != null && !time.isJsonNull()) {
            if (time.isJsonPrimitive()) {
                s.timeField = time.getAsString();
            } else if (time.isJsonObject()) {
                s.timeField = string(time.getAsJsonObject(), "field");
            } else {
                errors.add("time must be a field name or an object {field}");
            }
        }
        final JsonElement weight = p.get("weight");
        if (weight != null && !weight.isJsonNull()) {
            s.weightField = weight.isJsonObject() ? string(weight.getAsJsonObject(), "field") : weight.getAsString();
        }
        s.rowId = strings(p, "rowId", errors);
        final JsonElement utility = p.get("utility");
        if (utility != null && !utility.isJsonNull()) {
            s.utilityField = utility.isJsonObject() ? string(utility.getAsJsonObject(), "field") : utility.getAsString();
            if (s.utilityField == null) errors.add("utility.field is required when utility is declared");
        }
        s.manifest = string(p, "manifest");

        // predictions
        final JsonElement predictions = p.get("predictions");
        if (predictions == null || predictions.isJsonNull()) {
            errors.add("predictions is required (a list of prediction sets: {name, prob} or {name, field, form} or {name, score, offset, offsetScale, temperature})");
        } else if (!predictions.isJsonArray()) {
            errors.add("predictions must be a list of prediction sets");
        } else {
            final Set<String> names = new HashSet<>();
            int i = 0;
            for (final JsonElement e : predictions.getAsJsonArray()) {
                final String at = "predictions[" + i++ + "]";
                if (!e.isJsonObject()) {
                    errors.add(at + " must be an object");
                    continue;
                }
                final JsonObject o = e.getAsJsonObject();
                final Prediction d = new Prediction();
                d.name = string(o, "name");
                if (d.name == null || d.name.isBlank()) errors.add(at + ".name is required");
                else if (BASELINE_NAME.equals(d.name)) errors.add(at + ".name '" + BASELINE_NAME + "' is reserved for the baseline's own metrics");
                else if (!names.add(d.name)) errors.add(at + ".name '" + d.name + "' is duplicated");
                d.scoreField = string(o, "score");
                d.offsetField = string(o, "offset");
                final String prob = string(o, "prob");
                d.field = prob != null ? prob : string(o, "field");
                d.form = prob != null ? Family.FORM_PROB : string(o, "form");
                if (d.scoreField != null) {
                    if (d.field != null) errors.add(at + ": specify either a probability column (prob / field) or a score, not both");
                    final String scale = string(o, "offsetScale");
                    if (scale != null) d.offsetScale = scale;
                    if (!OFFSET_SCALES.contains(d.offsetScale)) errors.add(at + ".offsetScale '" + d.offsetScale + "' is unknown (available: " + OFFSET_SCALES + ")");
                    final Double t = number(o, "temperature");
                    if (t != null) d.temperature = t;
                    if (!(d.temperature > 0)) errors.add(at + ".temperature must be > 0");
                } else if (d.field == null) {
                    errors.add(at + ": a prediction set needs prob, field (+ form) or score");
                } else {
                    if (d.form == null) d.form = forms.get(0);
                    if (!forms.contains(d.form)) errors.add(at + ".form '" + d.form + "' is not valid for family " + s.family + " (available: " + forms + ")");
                    if (o.has("temperature") || o.has("offset")) errors.add(at + ": temperature / offset apply to a score set only");
                }
                s.predictions.add(d);
            }
            if (s.predictions.isEmpty()) errors.add("predictions must list at least one prediction set");
        }

        // splits
        final JsonElement splits = p.get("splits");
        if (splits == null || splits.isJsonNull()) {
            errors.add("splits is required: named time ranges with a role ({name: {from, to, role: selection | report}}) or {field, roles: {value: role}}");
        } else if (!splits.isJsonObject()) {
            errors.add("splits must be an object");
        } else {
            final JsonObject o = splits.getAsJsonObject();
            if (o.has("field") && o.get("field").isJsonPrimitive()) {
                s.splitField = o.get("field").getAsString();
                final JsonElement roles = o.get("roles");
                if (roles == null || !roles.isJsonObject()) {
                    errors.add("splits.roles is required with splits.field: {<split value>: selection | report}");
                } else {
                    for (final Map.Entry<String, JsonElement> e : roles.getAsJsonObject().entrySet()) {
                        final Split sp = new Split();
                        sp.name = e.getKey();
                        sp.role = e.getValue().isJsonPrimitive() ? e.getValue().getAsString() : null;
                        if (sp.role == null || !ROLES.contains(sp.role)) errors.add("splits.roles." + sp.name + " must be one of " + ROLES);
                        s.splits.add(sp);
                    }
                }
            } else {
                for (final Map.Entry<String, JsonElement> e : o.entrySet()) {
                    final Split sp = new Split();
                    sp.name = e.getKey();
                    if (!e.getValue().isJsonObject()) {
                        errors.add("splits." + sp.name + " must be an object {from, to, role}");
                        continue;
                    }
                    final JsonObject so = e.getValue().getAsJsonObject();
                    sp.role = string(so, "role");
                    sp.from = string(so, "from");
                    sp.to = string(so, "to");
                    if (sp.role == null || !ROLES.contains(sp.role)) errors.add("splits." + sp.name + ".role must be one of " + ROLES);
                    sp.fromMillis = parseTime(sp.from, false, "splits." + sp.name + ".from", errors);
                    sp.toMillis = parseTime(sp.to, true, "splits." + sp.name + ".to", errors);
                    if (sp.fromMillis != null && sp.toMillis != null && sp.fromMillis > sp.toMillis) errors.add("splits." + sp.name + ": from is after to");
                    s.splits.add(sp);
                }
                // ranges must not overlap; every selection range must end before every report range starts
                for (int i = 0; i < s.splits.size(); i++) {
                    for (int j = i + 1; j < s.splits.size(); j++) {
                        final Split a = s.splits.get(i), b = s.splits.get(j);
                        if (overlaps(a, b)) errors.add("splits." + a.name + " and splits." + b.name + " overlap in time; splits must be disjoint");
                    }
                }
                for (final Split sel : s.splits) {
                    if (!sel.isSelection()) continue;
                    for (final Split rep : s.splits) {
                        if (rep.isSelection()) continue;
                        final long selEnd = sel.toMillis == null ? Long.MAX_VALUE : sel.toMillis;
                        final long repStart = rep.fromMillis == null ? Long.MIN_VALUE : rep.fromMillis;
                        if (selEnd >= repStart) errors.add("splits." + sel.name + " (selection) must end before splits." + rep.name + " (report) starts: the selection period must never see the report period");
                    }
                }
            }
            if (s.splits.isEmpty()) errors.add("splits must declare at least one split");
            else if (s.splits.stream().noneMatch(sp -> ROLE_REPORT.equals(sp.role))) errors.add("splits must declare at least one split with role " + ROLE_REPORT);
        }

        // bootstrap
        final JsonElement bootstrap = p.get("bootstrap");
        if (bootstrap != null && !bootstrap.isJsonNull()) {
            if (bootstrap.isJsonObject()) {
                final JsonObject o = bootstrap.getAsJsonObject();
                final Integer n = integer(o, "samples");
                if (n != null) s.bootstrapSamples = n;
                final Long seed = longValue(o, "seed");
                if (seed != null) s.bootstrapSeed = seed;
                s.bootstrapUnit = string(o, "unit");
                if (s.bootstrapSamples < 0 || s.bootstrapSamples > MAX_BOOTSTRAP) errors.add("bootstrap.samples must be in [0, " + MAX_BOOTSTRAP + "] (every accumulator carries 6 x samples doubles)");
            } else if (bootstrap.isJsonPrimitive() && bootstrap.getAsJsonPrimitive().isBoolean() && !bootstrap.getAsBoolean()) {
                s.bootstrapSamples = 0;
            } else {
                errors.add("bootstrap must be an object {samples, seed, unit} or false");
            }
        }

        // calibration tables
        final JsonElement calibration = p.get("calibration");
        if (calibration != null && !calibration.isJsonNull()) {
            if (!calibration.isJsonArray()) {
                errors.add("calibration must be a list of tables");
            } else {
                int i = 0;
                for (final JsonElement e : calibration.getAsJsonArray()) {
                    final String at = "calibration[" + i++ + "]";
                    if (!e.isJsonObject()) {
                        errors.add(at + " must be an object");
                        continue;
                    }
                    final JsonObject o = e.getAsJsonObject();
                    final Table t = new Table();
                    t.type = string(o, "type");
                    if (t.type == null) t.type = TABLE_RELIABILITY;
                    if (!TABLE_TYPES.contains(t.type)) {
                        errors.add(at + ".type '" + t.type + "' is unknown (available: " + TABLE_TYPES + ")");
                        continue;
                    }
                    if (t.isEdge()) {
                        t.thresholds = numbers(o, "thresholds", at + ".thresholds", errors);
                        if (t.thresholds == null || t.thresholds.length == 0) errors.add(at + ".thresholds is required for type edge (ratios p_model / p_baseline)");
                        else for (final double th : t.thresholds) if (!(th > 0)) errors.add(at + ".thresholds must be > 0");
                    } else {
                        t.by = string(o, "by");
                        if (t.by == null) t.by = BY_PREDICTION;
                        if (!BYS.contains(t.by)) errors.add(at + ".by '" + t.by + "' is unknown (available: " + BYS + ")");
                        if (BY_FIELD.equals(t.by)) {
                            t.field = string(o, "field");
                            if (t.field == null) errors.add(at + ".field is required for by: field");
                            t.edges = numbers(o, "edges", at + ".edges", errors);
                            if (t.edges == null || t.edges.length == 0) errors.add(at + ".edges is required for by: field (ascending bin boundaries)");
                            else for (int k = 1; k < t.edges.length; k++) if (!(t.edges[k] > t.edges[k - 1])) errors.add(at + ".edges must be strictly ascending");
                        } else {
                            final Integer bins = integer(o, "bins");
                            t.bins = bins == null ? 10 : bins;
                            if (t.bins < 2 || t.bins > 1000) errors.add(at + ".bins must be in [2, 1000]");
                            if (o.has("edges")) errors.add(at + ".edges apply to by: field only (quantile bins otherwise)");
                        }
                    }
                    s.tables.add(t);
                }
            }
        }

        // slices
        final JsonElement slices = p.get("slices");
        if (slices != null && !slices.isJsonNull()) {
            if (!slices.isJsonArray()) {
                errors.add("slices must be a list of {field} or {field, bucket}");
            } else {
                int i = 0;
                for (final JsonElement e : slices.getAsJsonArray()) {
                    final String at = "slices[" + i++ + "]";
                    final Slice sl = new Slice();
                    if (e.isJsonPrimitive()) {
                        sl.field = e.getAsString();
                    } else if (e.isJsonObject()) {
                        sl.field = string(e.getAsJsonObject(), "field");
                        sl.bucket = string(e.getAsJsonObject(), "bucket");
                        if (sl.bucket != null && !StatMath.PERIOD_BUCKETS.contains(sl.bucket)) errors.add(at + ".bucket '" + sl.bucket + "' is unknown (available: " + StatMath.PERIOD_BUCKETS + ")");
                    } else {
                        errors.add(at + " must be a field name or an object {field, bucket}");
                        continue;
                    }
                    if (sl.field == null && sl.bucket == null) errors.add(at + ".field is required");
                    s.slices.add(sl);
                }
            }
        }

        final JsonObject canonical = p.deepCopy();
        canonical.remove("manifest");
        s.parametersHash = FeaturePlanCompiler.sha256(FeaturePlanCompiler.canonical(canonical));
        if (!errors.isEmpty()) throw new IllegalArgumentException(String.join("; ", errors));
        return s;
    }

    private static boolean overlaps(final Split a, final Split b) {
        final long aFrom = a.fromMillis == null ? Long.MIN_VALUE : a.fromMillis, aTo = a.toMillis == null ? Long.MAX_VALUE : a.toMillis;
        final long bFrom = b.fromMillis == null ? Long.MIN_VALUE : b.fromMillis, bTo = b.toMillis == null ? Long.MAX_VALUE : b.toMillis;
        return aFrom <= bTo && bFrom <= aTo;
    }

    /** An ISO instant, or a date (the start of the UTC day; {@code endOfDay}: its last millisecond). */
    static Long parseTime(final String text, final boolean endOfDay, final String key, final List<String> errors) {
        if (text == null) return null;
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (final RuntimeException ignored) {
            // not an instant: try a date
        }
        try {
            final LocalDate d = LocalDate.parse(text);
            final long start = d.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
            return endOfDay ? start + 86_400_000L - 1 : start;
        } catch (final RuntimeException e) {
            errors.add(key + " must be an ISO-8601 instant (2025-12-31T23:59:59Z) or date (2025-12-31): " + text);
            return null;
        }
    }

    private static double[] numbers(final JsonObject o, final String key, final String at, final List<String> errors) {
        if (!o.has(key) || o.get(key).isJsonNull()) return null;
        if (!o.get(key).isJsonArray()) {
            errors.add(at + " must be a list of numbers");
            return null;
        }
        final List<Double> values = new ArrayList<>();
        for (final JsonElement e : o.getAsJsonArray(key)) {
            if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isNumber()) values.add(e.getAsDouble());
            else errors.add(at + " must be a list of numbers");
        }
        return values.stream().mapToDouble(Double::doubleValue).toArray();
    }

    // ---- resolution ----------------------------------------------------------------------------------------

    /**
     * Applies the feature transform's role defaults, validates every field against the input schema and fixes
     * the column layout. Throws {@link IllegalArgumentException} listing every error.
     */
    public EvaluationSpec resolve(final Schema inputSchema, final FeatureLineage lineage) {
        final List<String> errors = new ArrayList<>();
        final FeatureLineage l = lineage == null ? new FeatureLineage() : lineage;
        if (group == null && l.roles.containsKey("group")) {
            group = l.roles.get("group");
            notes.add("group defaulted to the feature transform's role: " + group);
        }
        if (labelField == null && labelExpr == null && l.roles.containsKey("label")) {
            labelField = l.roles.get("label");
            notes.add("label defaulted to the feature transform's role: " + labelField);
        }
        if (baselineField == null && l.roles.containsKey("baseline")) {
            baselineField = l.roles.get("baseline");
            if (baselineForm == null) baselineForm = Family.formsFor(family).get(0);
            notes.add("baseline defaulted to the feature transform's role: " + baselineField);
        }
        if (weightField == null && l.roles.containsKey("weight")) {
            weightField = l.roles.get("weight");
            notes.add("weight defaulted to the feature transform's role: " + weightField);
        }
        if (timeField == null && l.timeField != null) {
            timeField = l.timeField;
            notes.add("time.field defaulted to the feature transform's time field: " + timeField);
        }
        manifestPlanHash = l.planHash;
        manifestOutputHash = l.outputHash;
        if (!hasBaseline()) notes.add("no baseline: the prior (" + (isGrouped() ? "the uniform share within the group" : "the split's label mean") + ") is the reference");

        if (labelField == null && labelExpr == null) errors.add("label is required (a field name, {field} or {expr})");
        if (isGrouped() && group == null) errors.add("group is required for family " + Family.GROUPED_MULTINOMIAL.id());
        if (group == null) {
            if (Family.FORM_INVERSE_SHARE.equals(baselineForm)) errors.add("baseline.form inverseShare needs group (the share is taken within the group)");
            for (final Prediction d : predictions) {
                if (d.isScore()) errors.add("predictions '" + d.name + "': a score set (grouped softmax) needs group");
                if (Family.FORM_INVERSE_SHARE.equals(d.form)) errors.add("predictions '" + d.name + "': form inverseShare needs group");
            }
        }
        final boolean timeRanges = splitField == null && splits.stream().anyMatch(sp -> sp.fromMillis != null || sp.toMillis != null);
        if (timeField == null && timeRanges) errors.add("splits declare time ranges but time.field is not set (nor a feature time role): the element timestamp of a bounded source is not an event time");
        for (final Slice sl : slices) {
            if (sl.bucket != null && sl.field == null) sl.field = timeField;
            if (sl.field == null) errors.add("slices: a bucket slice needs a field or time.field");
        }

        final Map<String, Schema.Field> fields = new HashMap<>();
        if (inputSchema != null) for (final Schema.Field f : inputSchema.getFields()) fields.put(f.getName(), f);
        final List<String[]> refs = new ArrayList<>(List.of(new String[]{"group", group}, new String[]{"label.field", labelField},
                new String[]{"baseline.field", baselineField}, new String[]{"time.field", timeField}, new String[]{"weight.field", weightField},
                new String[]{"utility.field", utilityField}, new String[]{"splits.field", splitField}, new String[]{"bootstrap.unit", bootstrapUnit}));
        for (final Prediction d : predictions) {
            refs.add(new String[]{"predictions '" + d.name + "' " + (d.isScore() ? "score" : "field"), d.isScore() ? d.scoreField : d.field});
            if (d.offsetField != null) refs.add(new String[]{"predictions '" + d.name + "' offset", d.offsetField});
        }
        for (int i = 0; i < tables.size(); i++) if (tables.get(i).field != null) refs.add(new String[]{"calibration[" + i + "].field", tables.get(i).field});
        for (int i = 0; i < slices.size(); i++) refs.add(new String[]{"slices[" + i + "].field", slices.get(i).field});
        for (final String[] ref : refs) {
            if (ref[1] != null && !fields.containsKey(ref[1])) errors.add(ref[0] + " '" + ref[1] + "' is not an input field");
        }
        for (final String id : rowId) if (!fields.containsKey(id)) errors.add("rowId '" + id + "' is not an input field");
        if (timeField != null && fields.containsKey(timeField)) timeFieldType = fields.get(timeField).getFieldType().getType().name();
        for (final Slice sl : slices) {
            if (sl.field != null && fields.containsKey(sl.field)) {
                sl.fieldType = fields.get(sl.field).getFieldType().getType().name();
                if (sl.bucket != null && !"timestamp".equals(sl.fieldType) && !"date".equals(sl.fieldType) && !"string".equals(sl.fieldType) && !"int64".equals(sl.fieldType)) {
                    errors.add("slices '" + sl.field + "' with bucket " + sl.bucket + " needs a timestamp / date field (" + sl.fieldType + ")");
                }
            }
        }
        if (labelExpr != null) {
            for (final String v : ExpressionUtil.createDefaultExpression(labelExpr).getVariableNames()) {
                if (!fields.containsKey(v)) errors.add("label.expr variable '" + v + "' is not an input field");
            }
        }

        // column layout: prediction columns (a score set's score and offset side by side), then the calibration
        // fields and the utility, each column carried once
        rowColumns = new ArrayList<>();
        for (final Prediction d : predictions) {
            d.offset = rowColumns.size();
            if (d.isScore()) {
                rowColumns.add(d.scoreField);
                if (d.offsetField != null) rowColumns.add(d.offsetField);
            } else {
                rowColumns.add(d.field);
            }
        }
        for (final Table t : tables) if (t.field != null) t.fieldIndex = column(t.field);
        if (utilityField != null) utilityIndex = column(utilityField);
        for (final String c : rowColumns) {
            if (fields.containsKey(c) && !FeatureLineage.isNumeric(fields.get(c))) {
                errors.add("column '" + c + "' must be numeric (" + fields.get(c).getFieldType().getType() + ")");
            }
        }
        if (!errors.isEmpty()) throw new IllegalArgumentException(String.join("; ", errors));
        return this;
    }

    /** The position of a column in {@link #rowColumns}, appended when new. */
    private int column(final String name) {
        final int i = rowColumns.indexOf(name);
        if (i >= 0) return i;
        rowColumns.add(name);
        return rowColumns.size() - 1;
    }

    /** Name → role of every split. */
    public Map<String, String> roles() {
        final Map<String, String> roles = new LinkedHashMap<>();
        for (final Split s : splits) roles.put(s.name, s.role);
        return roles;
    }
}
