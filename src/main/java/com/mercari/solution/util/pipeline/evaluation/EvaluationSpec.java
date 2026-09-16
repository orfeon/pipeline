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
    /** field types a time / bucket field may have (a string is parsed as ISO-8601) */
    static final List<String> TIME_FIELD_TYPES = List.of("timestamp", "datetime", "date", "string");
    public static final String OFFSET_SCALE_LOG = "log";
    public static final List<String> OFFSET_SCALES = List.of(OFFSET_SCALE_PROB, OFFSET_SCALE_LOG);

    public static final String TABLE_RELIABILITY = "reliability";
    public static final String TABLE_EDGE = "edge";
    public static final List<String> TABLE_TYPES = List.of(TABLE_RELIABILITY, TABLE_EDGE);
    public static final String FIT_TEMPERATURE = "temperature";
    public static final String FIT_BLEND = "blend";
    public static final List<String> FIT_TYPES = List.of(FIT_TEMPERATURE, FIT_BLEND);
    public static final List<String> CALIBRATION_TYPES = List.of(TABLE_RELIABILITY, TABLE_EDGE, FIT_TEMPERATURE, FIT_BLEND);
    /** derived prediction set suffixes */
    public static final String SUFFIX_TEMPERATURE = "@T";
    public static final String SUFFIX_BLEND = "@blend";
    public static final String BY_PREDICTION = "prediction";
    public static final String BY_DIVERGENCE = "divergence";
    public static final String BY_FIELD = "field";
    public static final List<String> BYS = List.of(BY_PREDICTION, BY_DIVERGENCE, BY_FIELD);

    public static final int MAX_BOOTSTRAP = 10_000;

    public static final String DISCOVERY_OUTPUT_PASSED = "passed";
    public static final String DISCOVERY_OUTPUT_ALL = "all";
    public static final List<String> DISCOVERY_OUTPUTS = List.of(DISCOVERY_OUTPUT_PASSED, DISCOVERY_OUTPUT_ALL);
    public static final List<String> DISCOVERY_METRICS = List.of("excessLogScore", "logScore", "hitAt1", "brier");

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

    /**
     * A calibration fit (design §7.1): estimated on a selection split, applied as a derived prediction set.
     * {@code temperature}: η = o + f / T over a grid of T; {@code blend}: η = a·f + b·o (+ an intercept for the
     * binomial family) by Newton's method, with f the set's score (or log / logit of its probability) and o its
     * offset (or the baseline's log share / logit).
     */
    public static final class Fit implements Serializable {
        public String type;
        public String fitOn;
        /** the prediction sets the fit applies to (names); empty = every declared set */
        public List<String> of = new ArrayList<>();
        /** temperature grid: min, max, count (linear) */
        public double gridMin = 0.25;
        public double gridMax = 4d;
        public int gridSize = 76;
        public double l2 = 1e-4;
        public int maxIter = 10;
        public double tol = 1e-8;

        public boolean isTemperature() {
            return FIT_TEMPERATURE.equals(type);
        }

        /** The grid values (linear between min and max; {@code gridSize >= 2} by validation). */
        public double[] grid() {
            final double[] g = new double[gridSize];
            for (int i = 0; i < gridSize; i++) g[i] = gridMin + (gridMax - gridMin) * i / (gridSize - 1);
            return g;
        }
    }

    /**
     * Slice discovery (design §7.2): candidate slices from low-cardinality dimensions (numeric ones quantile-binned)
     * up to {@code maxDepth}, scored on {@code discoverOn} against the random-subset null and confirmed on
     * {@code confirmOn}.
     */
    public static final class Discovery implements Serializable {
        public List<Dimension> dimensions = new ArrayList<>();
        public int maxDepth = 2;
        public int minSupport = 100;
        public String discoverOn;
        public String confirmOn;
        /** the compared sets (names; declared or derived); empty = every set */
        public List<String> of = new ArrayList<>();
        public String metric = "excessLogScore";
        public double quantile = 0.99;
        public int maxCandidates = 20_000;
        public String output = DISCOVERY_OUTPUT_PASSED;
        /** the set indices (0-based among the compared sets) the discovery runs for; fixed by resolve */
        public List<Integer> sets = new ArrayList<>();

        public boolean hasNumeric() {
            return dimensions.stream().anyMatch(Dimension::isNumeric);
        }
    }

    /** A discovery dimension: a categorical field (its text), or a numeric field binned by quantile. */
    public static final class Dimension implements Serializable {
        public String field;
        /** quantile bins of a numeric dimension (0 = categorical) */
        public int bins;
        public String fieldType;
        /** categorical: position in {@link EvaluationRow#dims}; numeric: position in {@link EvaluationRow#x} */
        public int index = -1;

        public boolean isNumeric() {
            return bins > 0;
        }

        public String name() {
            return isNumeric() ? field + "/q" + bins : field;
        }
    }

    /** A derived prediction set: a base set under a fitted calibration. */
    public static final class Derived implements Serializable {
        public String name;
        /** index into {@link #predictions} */
        public int base;
        /** index into {@link #fits} */
        public int fit;

        public Derived(final String name, final int base, final int fit) {
            this.name = name;
            this.base = base;
            this.fit = fit;
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
    public List<Fit> fits = new ArrayList<>();
    /** the derived prediction sets, in (fit, base set) order; fixed by {@link #resolve} */
    public List<Derived> derived = new ArrayList<>();
    /** output.calibration: URI / path of the fitted-parameters JSON (null = not written) */
    public String calibrationUri;
    /** sliceDiscovery (null = none) */
    public Discovery discovery;
    /** the categorical discovery dimensions carried in {@link EvaluationRow#dims}, in order */
    public List<String> dimColumns = new ArrayList<>();
    public List<Slice> slices = new ArrayList<>();
    /** SHA-256 (16 hex, the feature plan hash width) of the canonical parameters without the file locations (manifest, output) */
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

    /** The split of a time (time-range splits): its name, or null when the time falls outside every range; a range-less split takes every row, timed or not. */
    public String splitOf(final long time) {
        for (final Split s : splits) {
            if (s.fromMillis == null && s.toMillis == null) return s.name;
            if (time == EvaluationRow.NO_TIME) continue;
            if (s.fromMillis != null && time < s.fromMillis) continue;
            if (s.toMillis != null && time > s.toMillis) continue;
            return s.name;
        }
        return null;
    }

    /** Prediction names, the baseline first (its index is 0 in every per-unit array), the derived sets last. */
    public List<String> predictionNames() {
        final List<String> names = new ArrayList<>();
        names.add(BASELINE_NAME);
        for (final Prediction p : predictions) names.add(p.name);
        for (final Derived d : derived) names.add(d.name);
        return names;
    }

    /** Number of compared sets: the declared prediction sets plus the derived ones (the baseline not counted). */
    public int setCount() {
        return predictions.size() + derived.size();
    }

    public boolean hasFits() {
        return !fits.isEmpty();
    }

    public boolean hasDiscovery() {
        return discovery != null;
    }

    /** The derived sets of a fit, with their positions among the compared sets (declared sets first). */
    public List<Integer> derivedOf(final int fit) {
        final List<Integer> out = new ArrayList<>();
        for (int i = 0; i < derived.size(); i++) if (derived.get(i).fit == fit) out.add(predictions.size() + i);
        return out;
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
                if (prob != null && (o.has("field") || o.has("form"))) errors.add(at + ": prob is a probability column; specify either prob or field + form, not both");
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
                    final String declaredType = string(o, "type");
                    final String type = declaredType == null ? TABLE_RELIABILITY : declaredType;
                    if (FIT_TYPES.contains(type)) {
                        final Fit f = new Fit();
                        f.type = type;
                        f.fitOn = string(o, "fitOn");
                        if (f.fitOn == null) errors.add(at + ".fitOn is required for type " + type + " (the selection split the fit is estimated on)");
                        f.of = strings(o, "of", errors);
                        if (f.isTemperature()) {
                            final double[] grid = numbers(o, "grid", at + ".grid", errors);
                            if (grid != null) {
                                if (grid.length != 3) {
                                    errors.add(at + ".grid must be [min, max, count]");
                                } else {
                                    f.gridMin = grid[0];
                                    f.gridMax = grid[1];
                                    f.gridSize = (int) grid[2];
                                    if (!(f.gridMin > 0) || !(f.gridMax > f.gridMin) || grid[2] != f.gridSize || f.gridSize < 2 || f.gridSize > 10_000) errors.add(at + ".grid must be [min > 0, max > min, an integer count in 2..10000]");
                                }
                            }
                        } else {
                            final Double l2 = number(o, "l2");
                            if (l2 != null) f.l2 = l2;
                            final Integer maxIter = integer(o, "maxIter");
                            if (maxIter != null) f.maxIter = maxIter;
                            final Double tol = number(o, "tol");
                            if (tol != null) f.tol = tol;
                            if (f.l2 < 0) errors.add(at + ".l2 must be >= 0");
                            if (f.maxIter < 1 || f.maxIter > 100) errors.add(at + ".maxIter must be in [1, 100] (every iteration is one pass over the selection split)");
                            if (f.tol <= 0) errors.add(at + ".tol must be > 0");
                        }
                        s.fits.add(f);
                        continue;
                    }
                    final Table t = new Table();
                    t.type = type;
                    if (!TABLE_TYPES.contains(t.type)) {
                        errors.add(at + ".type '" + t.type + "' is unknown (available: " + CALIBRATION_TYPES + ")");
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

        final JsonElement discovery = p.get("sliceDiscovery");
        if (discovery != null && !discovery.isJsonNull()) {
            if (!discovery.isJsonObject()) {
                errors.add("sliceDiscovery must be an object {dimensions, maxDepth, minSupport, discoverOn, confirmOn, of, metric, quantile, maxCandidates, output}");
            } else {
                final JsonObject o = discovery.getAsJsonObject();
                final Discovery d = new Discovery();
                final JsonElement dims = o.get("dimensions");
                if (dims == null || !dims.isJsonArray() || dims.getAsJsonArray().isEmpty()) {
                    errors.add("sliceDiscovery.dimensions is required: a list of fields ({field} or {field, bins} for a numeric field)");
                } else {
                    int i = 0;
                    for (final JsonElement e : dims.getAsJsonArray()) {
                        final String at = "sliceDiscovery.dimensions[" + i++ + "]";
                        final Dimension dim = new Dimension();
                        if (e.isJsonPrimitive()) {
                            dim.field = e.getAsString();
                        } else if (e.isJsonObject()) {
                            dim.field = string(e.getAsJsonObject(), "field");
                            final Integer bins = integer(e.getAsJsonObject(), "bins");
                            dim.bins = bins == null ? 0 : bins;
                            if (bins != null && (bins < 2 || bins > 50)) errors.add(at + ".bins must be in [2, 50]");
                        } else {
                            errors.add(at + " must be a field name or an object {field, bins}");
                            continue;
                        }
                        if (dim.field == null) errors.add(at + ".field is required");
                        d.dimensions.add(dim);
                    }
                }
                final Integer maxDepth = integer(o, "maxDepth");
                if (maxDepth != null) d.maxDepth = maxDepth;
                if (d.maxDepth < 1 || d.maxDepth > 3) errors.add("sliceDiscovery.maxDepth must be in [1, 3]");
                final Integer minSupport = integer(o, "minSupport");
                if (minSupport != null) d.minSupport = minSupport;
                if (d.minSupport < 2) errors.add("sliceDiscovery.minSupport must be >= 2 (units of the discovery split)");
                d.discoverOn = string(o, "discoverOn");
                d.confirmOn = string(o, "confirmOn");
                if (d.discoverOn == null || d.confirmOn == null) errors.add("sliceDiscovery.discoverOn and confirmOn are required (two different splits)");
                else if (d.discoverOn.equals(d.confirmOn)) errors.add("sliceDiscovery.discoverOn and confirmOn must be different splits (a slice found in a window must be confirmed in another)");
                d.of = strings(o, "of", errors);
                final String metric = string(o, "metric");
                if (metric != null) d.metric = metric;
                if (!DISCOVERY_METRICS.contains(d.metric)) errors.add("sliceDiscovery.metric '" + d.metric + "' is unknown (available: " + DISCOVERY_METRICS + ")");
                final Double q = number(o, "quantile");
                if (q != null) d.quantile = q;
                if (!(d.quantile > 0 && d.quantile < 1)) errors.add("sliceDiscovery.quantile must be in (0, 1)");
                final Integer maxCandidates = integer(o, "maxCandidates");
                if (maxCandidates != null) d.maxCandidates = maxCandidates;
                if (d.maxCandidates < 1 || d.maxCandidates > 1_000_000) errors.add("sliceDiscovery.maxCandidates must be in [1, 1000000]");
                final String out = string(o, "output");
                if (out != null) d.output = out;
                if (!DISCOVERY_OUTPUTS.contains(d.output)) errors.add("sliceDiscovery.output must be one of " + DISCOVERY_OUTPUTS);
                s.discovery = d;
            }
        }

        final JsonElement output = p.get("output");
        if (output != null && !output.isJsonNull()) {
            if (output.isJsonObject()) {
                final JsonObject o = output.getAsJsonObject();
                s.calibrationUri = string(o, "calibration");
                if (o.has("calibration") && (s.calibrationUri == null || s.calibrationUri.isBlank())) errors.add("output.calibration must be a URI or path of the fitted-parameters file to write");
            } else {
                errors.add("output must be an object {calibration: <uri>}");
            }
        }

        final JsonObject canonical = p.deepCopy();
        canonical.remove("manifest");
        canonical.remove("output");
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
        if (!isGrouped()) {
            // a score set / inverseShare normalise within the group: on a non-grouped family every unit is one row and the share is 1
            final String grouped = "family " + Family.GROUPED_MULTINOMIAL.id();
            if (Family.FORM_INVERSE_SHARE.equals(baselineForm)) errors.add("baseline.form inverseShare needs " + grouped + " (the share is taken within the group)");
            for (final Prediction d : predictions) {
                if (d.isScore()) errors.add("predictions '" + d.name + "': a score set (grouped softmax) needs " + grouped);
                if (Family.FORM_INVERSE_SHARE.equals(d.form)) errors.add("predictions '" + d.name + "': form inverseShare needs " + grouped);
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
        if (timeField != null && fields.containsKey(timeField)) {
            timeFieldType = fields.get(timeField).getFieldType().getType().name();
            // an int64 would be read as epoch microseconds (the framework's primitive timestamp), silently unassigning every row
            if (!TIME_FIELD_TYPES.contains(timeFieldType)) errors.add("time.field '" + timeField + "' must be a timestamp / datetime / date field (" + timeFieldType + ")");
        }
        for (final Slice sl : slices) {
            if (sl.field != null && fields.containsKey(sl.field)) {
                sl.fieldType = fields.get(sl.field).getFieldType().getType().name();
                if (sl.bucket != null && !TIME_FIELD_TYPES.contains(sl.fieldType)) {
                    errors.add("slices '" + sl.field + "' with bucket " + sl.bucket + " needs a timestamp / date field (" + sl.fieldType + ")");
                }
            }
        }
        if (labelExpr != null) {
            for (final String v : ExpressionUtil.createDefaultExpression(labelExpr).getVariableNames()) {
                if (!fields.containsKey(v)) errors.add("label.expr variable '" + v + "' is not an input field");
            }
        }

        // calibration fits: estimated on a selection split, applied to the named (or every) prediction set
        derived = new ArrayList<>();
        // the derived names share the declared sets' namespace: a declared set may not be named like a derived one
        final Set<String> declaredNames = new HashSet<>();
        for (final Prediction p : predictions) declaredNames.add(p.name);
        final Set<String> derivedNames = new HashSet<>();
        for (int i = 0; i < fits.size(); i++) {
            final Fit f = fits.get(i);
            final String at = "calibration (" + f.type + " #" + i + ")";
            final Split on = f.fitOn == null ? null : split(f.fitOn);
            if (f.fitOn != null && on == null) errors.add(at + ".fitOn '" + f.fitOn + "' is not a declared split (available: " + splitNames() + ")");
            else if (on != null && !on.isSelection()) errors.add(at + ".fitOn '" + f.fitOn + "' has role " + on.role + ": a calibration is fitted on a selection split only, never on the report split");
            final List<Integer> bases = new ArrayList<>();
            if (f.of.isEmpty()) {
                for (int j = 0; j < predictions.size(); j++) bases.add(j);
            } else {
                for (final String name : f.of) {
                    int found = -1;
                    for (int j = 0; j < predictions.size(); j++) if (predictions.get(j).name.equals(name)) found = j;
                    if (found < 0) errors.add(at + ".of '" + name + "' is not a declared prediction set");
                    else bases.add(found);
                }
            }
            for (final int j : bases) {
                final Prediction d = predictions.get(j);
                if (!f.isTemperature() && !hasBaseline() && !(d.isScore() && d.offsetField != null)) {
                    errors.add(at + " on '" + d.name + "': a blend needs an offset (the set's own, or the baseline) as its second column");
                }
                final String name = d.name + (f.isTemperature() ? SUFFIX_TEMPERATURE : SUFFIX_BLEND);
                if (declaredNames.contains(name)) errors.add(at + " on '" + d.name + "': the derived set '" + name + "' collides with a declared prediction set of that name");
                else if (!derivedNames.add(name)) errors.add(at + " on '" + d.name + "': the derived set '" + name + "' is declared twice (one " + f.type + " fit per prediction set)");
                derived.add(new Derived(name, j, i));
            }
        }

        // slice discovery: the splits' roles, the sets, the dimensions' types
        dimColumns = new ArrayList<>();
        if (discovery != null) {
            final Split on = discovery.discoverOn == null ? null : split(discovery.discoverOn);
            final Split confirm = discovery.confirmOn == null ? null : split(discovery.confirmOn);
            if (discovery.discoverOn != null && on == null) errors.add("sliceDiscovery.discoverOn '" + discovery.discoverOn + "' is not a declared split (available: " + splitNames() + ")");
            else if (on != null && !on.isSelection()) errors.add("sliceDiscovery.discoverOn '" + discovery.discoverOn + "' has role " + on.role + ": slices are discovered on a selection split and confirmed on another");
            if (discovery.confirmOn != null && confirm == null) errors.add("sliceDiscovery.confirmOn '" + discovery.confirmOn + "' is not a declared split (available: " + splitNames() + ")");
            final List<String> names = predictionNames();
            discovery.sets = new ArrayList<>();
            if (discovery.of.isEmpty()) {
                for (int j = 0; j < setCount(); j++) discovery.sets.add(j);
            } else {
                for (final String name : discovery.of) {
                    final int idx = names.indexOf(name);
                    if (idx <= 0) errors.add("sliceDiscovery.of '" + name + "' is not a compared prediction set (available: " + names.subList(1, names.size()) + ")");
                    else discovery.sets.add(idx - 1);
                }
            }
            if (!hasBaseline() && !isGrouped() && "excessLogScore".equals(discovery.metric)) {
                errors.add("sliceDiscovery.metric excessLogScore needs a baseline for family binomial (the prior reference is not a per-unit value); use logScore");
            }
            if (!isGrouped() && "hitAt1".equals(discovery.metric)) {
                errors.add("sliceDiscovery.metric hitAt1 needs family " + Family.GROUPED_MULTINOMIAL.id() + " (a binomial unit has no top-1 pick); use logScore or brier");
            }
            final Set<String> seen = new HashSet<>();
            for (int i = 0; i < discovery.dimensions.size(); i++) {
                final Dimension dim = discovery.dimensions.get(i);
                final String at = "sliceDiscovery.dimensions[" + i + "]";
                if (dim.field == null) continue;
                if (!seen.add(dim.field)) errors.add(at + " '" + dim.field + "' is declared twice");
                if (!fields.containsKey(dim.field)) {
                    errors.add(at + " '" + dim.field + "' is not an input field");
                    continue;
                }
                dim.fieldType = fields.get(dim.field).getFieldType().getType().name();
                final boolean numeric = FeatureLineage.isNumeric(fields.get(dim.field));
                if (dim.isNumeric() && !numeric) errors.add(at + " '" + dim.field + "' has bins but is not numeric (" + dim.fieldType + ")");
                if (!dim.isNumeric() && numeric && !"bool".equals(dim.fieldType) && !"int32".equals(dim.fieldType) && !"int64".equals(dim.fieldType)) {
                    errors.add(at + " '" + dim.field + "' is a " + dim.fieldType + " field: give it bins (quantile bins) or declare a categorical field");
                }
                if (!dim.isNumeric()) {
                    dim.index = dimColumns.size();
                    dimColumns.add(dim.field);
                }
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
        if (discovery != null) for (final Dimension dim : discovery.dimensions) if (dim.isNumeric() && dim.field != null && fields.containsKey(dim.field) && FeatureLineage.isNumeric(fields.get(dim.field))) dim.index = column(dim.field);
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
