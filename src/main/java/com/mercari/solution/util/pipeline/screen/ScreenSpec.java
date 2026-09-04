package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Parsed and validated parameters of the screen transform. Rides inside DoFns, so it holds plain fields only.
 * {@link #parse} reads the config; {@link #resolve} applies the upstream contract (feature manifest roles /
 * lineage, or the input schema's lineage options) and chooses the candidate columns.
 */
public final class ScreenSpec implements Serializable {

    public static final String FAMILY_GROUPED_MULTINOMIAL = "groupedMultinomial";
    public static final String FAMILY_BINOMIAL = "binomial";
    public static final List<String> FAMILIES = List.of(FAMILY_GROUPED_MULTINOMIAL, FAMILY_BINOMIAL);
    public static final List<String> PLANNED_FAMILIES = List.of("gaussian", "poisson");

    public static final String TRANSFORM_RAW = "raw";
    public static final String TRANSFORM_RANK = "rank";
    public static final String TRANSFORM_ABSDEV = "absdev";
    public static final List<String> TRANSFORMS = List.of(TRANSFORM_RAW, TRANSFORM_RANK, TRANSFORM_ABSDEV);

    public static final String FORM_PROB = "prob";
    public static final String FORM_LOG_PROB = "logProb";
    public static final String FORM_INVERSE_SHARE = "inverseShare";
    public static final List<String> BASELINE_FORMS = List.of(FORM_PROB, FORM_LOG_PROB, FORM_INVERSE_SHARE);

    public static final String NOISE_PREFIX = "__noise_";
    public static final String SHUFFLE_PREFIX = "__shuffle_";

    public String family;
    public String group;
    public String labelField;
    public String labelExpr;
    public boolean normalizeTies = true;
    public String baselineField;
    public String baselineForm;
    public String timeField;
    public String timeFieldType;
    public String timeTo;
    public String timeFrom;
    public Long timeToMillis;
    public Long timeFromMillis;
    public String weightField;
    public List<String> rowId = new ArrayList<>();
    public List<String> candidateInclude = new ArrayList<>();
    public List<String> candidateExclude = new ArrayList<>();
    public String candidateManifest;
    public List<String> transforms = new ArrayList<>();
    /** True when the config lists transforms explicitly (a defaulted group must not override them). */
    public boolean transformsExplicit;
    public String periodsField;
    public String periodsFieldType;
    public String periodsBucket;
    public int noise = 100;
    public String shuffleField;
    public int shuffleN = 0;
    public double quantile = 0.99;
    public long seed = 0L;
    public Double leakZ;
    /** conditioning.fields as written (names / globs); empty = no partial test */
    public List<String> conditioningPatterns = new ArrayList<>();
    public double conditioningL2 = 1e-4;
    public int conditioningMaxIter = 10;
    public double conditioningTol = 1e-8;

    /** resolved candidate column names (input schema order) */
    public List<String> candidates = new ArrayList<>();
    /** resolved conditioning column names (input schema order) */
    public List<String> conditioningFields = new ArrayList<>();
    /** informational notes produced by resolution (role defaults applied, columns excluded by lineage) */
    public List<String> notes = new ArrayList<>();

    private ScreenSpec() {}

    public boolean isGrouped() {
        return group != null;
    }

    public boolean isGroupedMultinomial() {
        return FAMILY_GROUPED_MULTINOMIAL.equals(family);
    }

    public boolean hasBaseline() {
        return baselineField != null;
    }

    public boolean hasShuffle() {
        return shuffleField != null && shuffleN > 0;
    }

    public boolean hasConditioning() {
        return !conditioningFields.isEmpty();
    }

    /** Position of the shuffle reference column in {@link ScreenRow#x} (after the candidates). */
    public int shuffleIndex() {
        return candidates.size();
    }

    /** Position of the first conditioning column in {@link ScreenRow#x}. */
    public int conditioningOffset() {
        return candidates.size() + (hasShuffle() ? 1 : 0);
    }

    /** Every column carried in {@link ScreenRow#x}: candidates, the shuffle reference, the conditioning fields. */
    public List<String> rowColumns() {
        final List<String> columns = new ArrayList<>(candidates);
        if (hasShuffle()) columns.add(shuffleField);
        columns.addAll(conditioningFields);
        return columns;
    }

    /** Column names in key order: candidates, noise placebos, shuffle placebos. */
    public List<String> columnNames() {
        final List<String> names = new ArrayList<>(candidates);
        for (int i = 0; i < noise; i++) names.add(NOISE_PREFIX + i);
        for (int i = 0; i < (hasShuffle() ? shuffleN : 0); i++) names.add(SHUFFLE_PREFIX + i);
        return names;
    }

    public int columnCount() {
        return candidates.size() + noise + (hasShuffle() ? shuffleN : 0);
    }

    public boolean isPlacebo(final int column) {
        return column >= candidates.size();
    }

    /** Accumulator key of (column, transform). */
    public int key(final int column, final int transform) {
        return column * transforms.size() + transform;
    }

    // ---- parsing -------------------------------------------------------------------------------------------

    /** Parses the {@code parameters} block; throws {@link IllegalArgumentException} listing every error. */
    public static ScreenSpec parse(final JsonObject p) {
        final List<String> errors = new ArrayList<>();
        final ScreenSpec s = new ScreenSpec();
        if (p == null) throw new IllegalArgumentException("parameters must not be empty");

        s.family = string(p, "family");
        if (s.family == null) s.family = FAMILY_GROUPED_MULTINOMIAL;
        if (PLANNED_FAMILIES.contains(s.family)) {
            errors.add("family '" + s.family + "' is not implemented in this version (available: " + FAMILIES + ")");
        } else if (!FAMILIES.contains(s.family)) {
            errors.add("unknown family '" + s.family + "' (available: " + FAMILIES + ")");
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

        final JsonElement baseline = p.get("baseline");
        if (baseline != null && !baseline.isJsonNull()) {
            if (baseline.isJsonPrimitive()) {
                s.baselineField = baseline.getAsString();
                s.baselineForm = FORM_PROB;
            } else if (baseline.isJsonObject()) {
                final JsonObject o = baseline.getAsJsonObject();
                s.baselineField = string(o, "field");
                s.baselineForm = string(o, "form");
                if (s.baselineForm == null) s.baselineForm = FORM_PROB;
                if (s.baselineField == null) errors.add("baseline.field is required when baseline is declared");
                if (!BASELINE_FORMS.contains(s.baselineForm)) errors.add("unknown baseline.form '" + s.baselineForm + "' (available: " + BASELINE_FORMS + ")");
            } else {
                errors.add("baseline must be a field name or an object {field, form}");
            }
        }

        final JsonElement time = p.get("time");
        if (time != null && !time.isJsonNull()) {
            if (time.isJsonPrimitive()) {
                s.timeField = time.getAsString();
            } else if (time.isJsonObject()) {
                final JsonObject o = time.getAsJsonObject();
                s.timeField = string(o, "field");
                s.timeTo = string(o, "to");
                s.timeFrom = string(o, "from");
                s.timeToMillis = parseInstant(s.timeTo, "time.to", errors);
                s.timeFromMillis = parseInstant(s.timeFrom, "time.from", errors);
                if (s.timeToMillis != null && s.timeFromMillis != null && s.timeFromMillis > s.timeToMillis) errors.add("time.from must not be after time.to");
            } else {
                errors.add("time must be a field name or an object {field, to, from}");
            }
        }

        final JsonElement weight = p.get("weight");
        if (weight != null && !weight.isJsonNull()) {
            s.weightField = weight.isJsonObject() ? string(weight.getAsJsonObject(), "field") : weight.getAsString();
        }
        s.rowId = strings(p, "rowId", errors);

        final JsonElement candidates = p.get("candidates");
        if (candidates != null && candidates.isJsonObject()) {
            final JsonObject o = candidates.getAsJsonObject();
            s.candidateInclude = strings(o, "include", errors);
            s.candidateExclude = strings(o, "exclude", errors);
            s.candidateManifest = string(o, "manifest");
        } else if (candidates != null && candidates.isJsonArray()) {
            s.candidateInclude = strings(p, "candidates", errors);
        } else if (candidates != null && !candidates.isJsonNull()) {
            errors.add("candidates must be an object {include, exclude, manifest} or a list of name globs");
        }
        if (s.candidateInclude.isEmpty()) s.candidateInclude = List.of("*");

        final JsonElement transforms = p.get("transforms");
        if (transforms != null && !transforms.isJsonNull()) {
            if (transforms.isJsonArray()) {
                for (final JsonElement e : transforms.getAsJsonArray()) {
                    final String name = e.isJsonObject() ? string(e.getAsJsonObject(), "type") : e.getAsString();
                    if (!TRANSFORMS.contains(name)) errors.add("unknown transform '" + name + "' (available: " + TRANSFORMS + ")");
                    else if (!s.transforms.contains(name)) s.transforms.add(name);
                }
            } else {
                errors.add("transforms must be a list (available: " + TRANSFORMS + ")");
            }
        }
        s.transformsExplicit = !s.transforms.isEmpty();
        if (s.transforms.isEmpty()) {
            s.transforms = s.group != null ? new ArrayList<>(TRANSFORMS) : new ArrayList<>(List.of(TRANSFORM_RAW));
        }

        final JsonElement periods = p.get("periods");
        if (periods != null && !periods.isJsonNull()) {
            if (periods.isJsonObject()) {
                final JsonObject o = periods.getAsJsonObject();
                s.periodsField = string(o, "field");
                s.periodsBucket = string(o, "bucket");
                if (s.periodsBucket == null) s.periodsBucket = "year";
                if (!ScreenMath.PERIOD_BUCKETS.contains(s.periodsBucket)) errors.add("unknown periods.bucket '" + s.periodsBucket + "' (available: " + ScreenMath.PERIOD_BUCKETS + ")");
            } else if (periods.isJsonPrimitive()) {
                s.periodsBucket = periods.getAsString();
                if (!ScreenMath.PERIOD_BUCKETS.contains(s.periodsBucket)) errors.add("unknown periods bucket '" + s.periodsBucket + "' (available: " + ScreenMath.PERIOD_BUCKETS + ")");
            } else {
                errors.add("periods must be an object {field, bucket} or a bucket name");
            }
        }

        final JsonElement placebo = p.get("placebo");
        if (placebo != null && placebo.isJsonObject()) {
            final JsonObject o = placebo.getAsJsonObject();
            final Integer noise = integer(o, "noise");
            if (noise != null) s.noise = noise;
            final JsonElement shuffle = o.get("shuffle");
            if (shuffle != null && shuffle.isJsonObject()) {
                s.shuffleField = string(shuffle.getAsJsonObject(), "field");
                final Integer n = integer(shuffle.getAsJsonObject(), "n");
                s.shuffleN = n == null ? 100 : n;
                if (s.shuffleField == null) errors.add("placebo.shuffle.field is required");
            }
            final Double q = number(o, "quantile");
            if (q != null) s.quantile = q;
            final Long seed = longValue(o, "seed");
            if (seed != null) s.seed = seed;
            if (s.noise < 0) errors.add("placebo.noise must be >= 0");
            if (s.shuffleN < 0) errors.add("placebo.shuffle.n must be >= 0");
            if (s.quantile <= 0 || s.quantile >= 1) errors.add("placebo.quantile must be in (0, 1)");
        } else if (placebo != null && !placebo.isJsonNull()) {
            errors.add("placebo must be an object {noise, shuffle: {field, n}, quantile, seed}");
        }

        final JsonElement flags = p.get("flags");
        if (flags != null && flags.isJsonObject()) {
            s.leakZ = number(flags.getAsJsonObject(), "leakZ");
            if (s.leakZ != null && s.leakZ <= 0) errors.add("flags.leakZ must be > 0");
        }

        final JsonElement conditioning = p.get("conditioning");
        if (conditioning != null && !conditioning.isJsonNull()) {
            if (conditioning.isJsonObject()) {
                final JsonObject o = conditioning.getAsJsonObject();
                s.conditioningPatterns = strings(o, "fields", errors);
                if (s.conditioningPatterns.isEmpty()) errors.add("conditioning.fields is required (names or globs of the conditioning columns)");
                final Double l2 = number(o, "l2");
                if (l2 != null) s.conditioningL2 = l2;
                final Integer maxIter = integer(o, "maxIter");
                if (maxIter != null) s.conditioningMaxIter = maxIter;
                final Double tol = number(o, "tol");
                if (tol != null) s.conditioningTol = tol;
                if (s.conditioningL2 < 0) errors.add("conditioning.l2 must be >= 0");
                if (s.conditioningMaxIter < 1 || s.conditioningMaxIter > 100) errors.add("conditioning.maxIter must be in [1, 100] (every iteration is one pass over the data)");
                if (s.conditioningTol <= 0) errors.add("conditioning.tol must be > 0");
            } else if (conditioning.isJsonArray()) {
                s.conditioningPatterns = strings(p, "conditioning", errors);
            } else {
                errors.add("conditioning must be an object {fields, l2, maxIter, tol} or a list of field names");
            }
        }
        if (p.has("output") && p.get("output").isJsonObject() && p.getAsJsonObject("output").has("selection")) {
            errors.add("output.selection is not implemented in this version (the summary output carries passedColumns)");
        }

        // rules that depend on group are checked in resolve (group may still come from the manifest roles)
        if (!errors.isEmpty()) throw new IllegalArgumentException(String.join("; ", errors));
        return s;
    }

    private static Long parseInstant(final String text, final String key, final List<String> errors) {
        if (text == null) return null;
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (final RuntimeException e) {
            errors.add(key + " must be an ISO-8601 instant such as 2025-12-31T23:59:59Z: " + text);
            return null;
        }
    }

    // ---- resolution ----------------------------------------------------------------------------------------

    /** Column lineage known about the input: from the feature transform's schema options or its manifest. */
    public static final class Lineage implements Serializable {
        public final Map<String, Entry> columns = new LinkedHashMap<>();
        /** role name → column (feature manifest {@code roles}) */
        public final Map<String, String> roles = new LinkedHashMap<>();
        public String timeField;

        public record Entry(String scope, String block, Set<String> derivedFrom, String evidence) implements Serializable {}

        public static Lineage fromSchema(final Schema schema) {
            final Lineage l = new Lineage();
            if (schema == null) return l;
            for (final Schema.Field f : schema.getFields()) {
                final Map<String, String> o = f.getOptions();
                if (o == null || !o.containsKey("feature.scope")) continue;
                final Set<String> derived = new LinkedHashSet<>();
                final String d = o.get("feature.derivedFrom");
                if (d != null && !d.isEmpty()) for (final String s : d.split(",")) derived.add(s.trim());
                l.columns.put(f.getName(), new Entry(o.get("feature.scope"), o.get("feature.block"), derived, o.get("feature.evidence")));
            }
            return l;
        }

        /** Reads a feature transform manifest (see {@code FeaturePlan.toManifest}). */
        public static Lineage fromManifest(final String json) {
            final Lineage l = new Lineage();
            final JsonObject m;
            try {
                m = JsonParser.parseString(json).getAsJsonObject();
            } catch (final JsonParseException | IllegalStateException e) {
                throw new IllegalArgumentException("candidates.manifest is not a JSON object (a local path that does not exist is read as literal content): " + e.getMessage());
            }
            l.timeField = string(m, "timeField");
            if (m.has("roles") && m.get("roles").isJsonObject()) {
                for (final Map.Entry<String, JsonElement> e : m.getAsJsonObject("roles").entrySet()) {
                    if (!e.getValue().isJsonObject()) continue;
                    final JsonObject r = e.getValue().getAsJsonObject();
                    String column = string(r, "column");
                    if (column == null && r.has("keys") && r.get("keys").isJsonArray()) {
                        final JsonArray keys = r.getAsJsonArray("keys");
                        if (keys.size() == 1) column = keys.get(0).getAsString();
                    }
                    if (column != null) l.roles.put(e.getKey(), column);
                }
            }
            if (m.has("columns") && m.get("columns").isJsonArray()) {
                for (final JsonElement e : m.getAsJsonArray("columns")) {
                    if (!e.isJsonObject()) continue;
                    final JsonObject c = e.getAsJsonObject();
                    final String name = string(c, "name");
                    if (name == null) continue;
                    final Set<String> derived = new LinkedHashSet<>();
                    String evidence = null;
                    if (c.has("lineage") && c.get("lineage").isJsonObject()) {
                        final JsonObject lineage = c.getAsJsonObject("lineage");
                        if (lineage.has("derivedFrom") && lineage.get("derivedFrom").isJsonArray()) {
                            for (final JsonElement d : lineage.getAsJsonArray("derivedFrom")) derived.add(d.getAsString());
                        }
                        evidence = string(lineage, "evidence");
                    }
                    l.columns.put(name, new Entry(string(c, "scope"), string(c, "block"), derived, evidence));
                }
            }
            return l;
        }

        public Lineage merge(final Lineage other) {
            if (other == null) return this;
            other.columns.forEach(columns::putIfAbsent);
            other.roles.forEach(roles::putIfAbsent);
            if (timeField == null) timeField = other.timeField;
            return this;
        }
    }

    /**
     * Applies role defaults, validates the fields against the input schema and chooses the candidate columns:
     * numeric input fields matching {@code candidates.include}, minus {@code candidates.exclude} (name globs and
     * lineage selectors {@code derivedFrom:} / {@code scope:} / {@code block:} / {@code evidence:}), minus every
     * role field. Throws {@link IllegalArgumentException} listing every error.
     */
    public ScreenSpec resolve(final Schema inputSchema, final Lineage lineage) {
        final List<String> errors = new ArrayList<>();
        final Lineage l = lineage == null ? new Lineage() : lineage;
        if (group == null && l.roles.containsKey("group")) {
            group = l.roles.get("group");
            notes.add("group defaulted to manifest role: " + group);
            if (!transformsExplicit) transforms = new ArrayList<>(TRANSFORMS);
        }
        if (labelField == null && labelExpr == null && l.roles.containsKey("label")) {
            labelField = l.roles.get("label");
            notes.add("label defaulted to manifest role: " + labelField);
        }
        if (baselineField == null && l.roles.containsKey("baseline")) {
            baselineField = l.roles.get("baseline");
            if (baselineForm == null) baselineForm = FORM_PROB;
            notes.add("baseline defaulted to manifest role: " + baselineField);
        }
        if (weightField == null && l.roles.containsKey("weight")) {
            weightField = l.roles.get("weight");
            notes.add("weight defaulted to manifest role: " + weightField);
        }
        if (timeField == null && l.timeField != null) {
            timeField = l.timeField;
            notes.add("time.field defaulted to manifest timeField: " + timeField);
        }
        if (periodsBucket != null && periodsField == null) periodsField = timeField;
        if (timeField == null && (timeToMillis != null || timeFromMillis != null)) {
            errors.add("time.from / time.to require time.field (or a manifest timeField): the element timestamp of a bounded source is not an event time");
        }

        if (labelField == null && labelExpr == null) errors.add("label is required (a field name, {field} or {expr})");
        if (isGroupedMultinomial() && group == null) errors.add("group is required for family groupedMultinomial");
        if (group == null) {
            for (final String t : transforms) {
                if (!TRANSFORM_RAW.equals(t)) errors.add("transform '" + t + "' needs group (within-group " + t + "); independent rows support raw only in this version");
            }
            if (hasShuffle()) errors.add("placebo.shuffle needs group (within-group permutation)");
            if (FORM_INVERSE_SHARE.equals(baselineForm)) errors.add("baseline.form inverseShare needs group (the share is taken within the group)");
        }
        if (periodsBucket != null && periodsField == null) errors.add("periods needs a field (periods.field or time.field)");

        final Map<String, Schema.Field> fields = new HashMap<>();
        if (inputSchema != null) for (final Schema.Field f : inputSchema.getFields()) fields.put(f.getName(), f);
        for (final String[] ref : new String[][]{{"group", group}, {"label.field", labelField}, {"baseline.field", baselineField},
                {"time.field", timeField}, {"weight.field", weightField}, {"periods.field", periodsField}, {"placebo.shuffle.field", shuffleField}}) {
            if (ref[1] != null && !fields.containsKey(ref[1])) errors.add(ref[0] + " '" + ref[1] + "' is not an input field");
        }
        for (final String id : rowId) if (!fields.containsKey(id)) errors.add("rowId '" + id + "' is not an input field");
        if (timeField != null && fields.containsKey(timeField)) timeFieldType = fields.get(timeField).getFieldType().getType().name();
        if (periodsField != null && fields.containsKey(periodsField)) periodsFieldType = fields.get(periodsField).getFieldType().getType().name();
        if (shuffleField != null && fields.containsKey(shuffleField) && !isNumeric(fields.get(shuffleField))) {
            errors.add("placebo.shuffle.field '" + shuffleField + "' must be numeric (" + fields.get(shuffleField).getFieldType().getType() + "); a non-numeric reference makes every shuffle placebo degenerate");
        }

        final Set<String> reserved = new HashSet<>();
        for (final String r : new String[]{group, labelField, baselineField, timeField, weightField, periodsField}) if (r != null) reserved.add(r);
        reserved.addAll(rowId);
        if (labelExpr != null) {
            reserved.addAll(com.mercari.solution.util.ExpressionUtil.createDefaultExpression(labelExpr).getVariableNames());
        }

        final List<Pattern> includes = candidateInclude.stream().filter(s -> s.indexOf(':') <= 0).map(ScreenMath::glob).toList();
        final List<String> includeSelectors = candidateInclude.stream().filter(s -> s.indexOf(':') > 0).toList();
        candidates = new ArrayList<>();
        final List<String> excludedByLineage = new ArrayList<>();
        if (inputSchema != null) {
            for (final Schema.Field f : inputSchema.getFields()) {
                if (!isNumeric(f)) continue;
                final String name = f.getName();
                if (reserved.contains(name)) continue;
                final Lineage.Entry entry = l.columns.get(name);
                boolean included = includes.stream().anyMatch(p -> p.matcher(name).matches());
                if (!included) included = includeSelectors.stream().anyMatch(s -> selectorMatches(s, entry));
                if (!included) continue;
                boolean excluded = false;
                for (final String pattern : candidateExclude) {
                    if (pattern.indexOf(':') > 0) {
                        if (selectorMatches(pattern, entry)) {
                            excluded = true;
                            excludedByLineage.add(name + " (" + pattern + ")");
                            break;
                        }
                    } else if (ScreenMath.glob(pattern).matcher(name).matches()) {
                        excluded = true;
                        break;
                    }
                }
                if (!excluded) candidates.add(name);
            }
        }
        if (!excludedByLineage.isEmpty()) notes.add("excluded by lineage: " + excludedByLineage);
        final boolean usesSelectors = candidateExclude.stream().anyMatch(s -> s.indexOf(':') > 0) || !includeSelectors.isEmpty();
        if (usesSelectors && l.columns.isEmpty()) {
            errors.add("candidates use lineage selectors (derivedFrom: / scope: / block: / evidence:) but no lineage is available: "
                    + "put the feature transform directly upstream or set candidates.manifest to its manifest URI");
        }
        if (candidates.isEmpty()) errors.add("no candidate column: candidates.include " + candidateInclude + " matched no numeric input field (after exclusions)");

        // conditioning columns: numeric fields matching the patterns, never the label / group / time / weight roles
        conditioningFields = new ArrayList<>();
        if (!conditioningPatterns.isEmpty() && inputSchema != null) {
            final Set<String> roleOnly = new HashSet<>();
            for (final String r : new String[]{group, labelField, timeField, weightField, periodsField}) if (r != null) roleOnly.add(r);
            if (labelExpr != null) roleOnly.addAll(com.mercari.solution.util.ExpressionUtil.createDefaultExpression(labelExpr).getVariableNames());
            for (final String pattern : conditioningPatterns) {
                final Pattern glob = ScreenMath.glob(pattern);
                boolean matched = false;
                for (final Schema.Field f : inputSchema.getFields()) {
                    if (!isNumeric(f) || roleOnly.contains(f.getName()) || !glob.matcher(f.getName()).matches()) continue;
                    matched = true;
                    if (!conditioningFields.contains(f.getName())) conditioningFields.add(f.getName());
                }
                if (!matched) errors.add("conditioning.fields '" + pattern + "' matched no numeric input field (role fields cannot be conditioned on)");
            }
            if (conditioningFields.size() > 500) errors.add("conditioning.fields resolved to " + conditioningFields.size() + " columns; the Newton Gram matrix is k x k, keep k <= 500");
        }
        if (!errors.isEmpty()) throw new IllegalArgumentException(String.join("; ", errors));
        return this;
    }

    static boolean selectorMatches(final String pattern, final Lineage.Entry entry) {
        if (entry == null) return false;
        final int colon = pattern.indexOf(':');
        final String selector = pattern.substring(0, colon);
        final String value = pattern.substring(colon + 1);
        return switch (selector) {
            case "derivedFrom" -> entry.derivedFrom() != null && entry.derivedFrom().contains(value);
            case "evidence" -> value.equals(entry.evidence());
            case "scope" -> value.equals(entry.scope());
            case "block" -> value.equals(entry.block());
            default -> false;
        };
    }

    static boolean isNumeric(final Schema.Field f) {
        return switch (f.getFieldType().getType()) {
            case int32, int64, float32, float64, bool -> true;
            default -> false;
        };
    }

    // ---- json helpers --------------------------------------------------------------------------------------

    static String string(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull()) return null;
        final JsonElement e = o.get(key);
        return e.isJsonPrimitive() ? e.getAsString() : null;
    }

    static Boolean bool(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsBoolean();
    }

    static Integer integer(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsInt();
    }

    static Long longValue(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsLong();
    }

    static Double number(final JsonObject o, final String key) {
        if (o == null || !o.has(key) || o.get(key).isJsonNull() || !o.get(key).isJsonPrimitive()) return null;
        return o.get(key).getAsDouble();
    }

    static List<String> strings(final JsonObject o, final String key, final List<String> errors) {
        final List<String> out = new ArrayList<>();
        if (o == null || !o.has(key) || o.get(key).isJsonNull()) return out;
        final JsonElement e = o.get(key);
        if (e.isJsonPrimitive()) {
            out.add(e.getAsString());
        } else if (e.isJsonArray()) {
            for (final JsonElement i : e.getAsJsonArray()) {
                if (i.isJsonPrimitive()) out.add(i.getAsString());
                else errors.add(key + " must be a list of strings");
            }
        } else {
            errors.add(key + " must be a list of strings");
        }
        return out;
    }
}
