package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureLineage;
import com.mercari.solution.util.pipeline.feature.FeaturePlanCompiler;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.Family;
import com.mercari.solution.util.pipeline.glm.StatMath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.mercari.solution.util.pipeline.glm.SpecJson.bool;
import static com.mercari.solution.util.pipeline.glm.SpecJson.integer;
import static com.mercari.solution.util.pipeline.glm.SpecJson.longValue;
import static com.mercari.solution.util.pipeline.glm.SpecJson.number;
import static com.mercari.solution.util.pipeline.glm.SpecJson.parseInstant;
import static com.mercari.solution.util.pipeline.glm.SpecJson.string;
import static com.mercari.solution.util.pipeline.glm.SpecJson.strings;

/**
 * Parsed and validated parameters of the screen transform. Rides inside DoFns, so it holds plain fields only.
 * {@link #parse} reads the config; {@link #resolve} applies the upstream contract (feature manifest roles /
 * lineage, or the input schema's lineage options) and chooses the candidate columns.
 */
public final class ScreenSpec implements Serializable {

    /** conditioning.missing: the window mean of the column (standardised: 0) */
    public static final String MISSING_MEAN = "mean";
    /** conditioning.missing: the unit's baseline-weighted mean of its observed values (grouped family only) */
    public static final String MISSING_GROUP_MEAN = "groupMean";
    public static final List<String> MISSINGS = List.of(MISSING_MEAN, MISSING_GROUP_MEAN);
    public static final String TRANSFORM_RAW = "raw";
    public static final String TRANSFORM_RANK = "rank";
    public static final String TRANSFORM_ABSDEV = "absdev";
    public static final List<String> TRANSFORMS = List.of(TRANSFORM_RAW, TRANSFORM_RANK, TRANSFORM_ABSDEV);

    public static final String NOISE_PREFIX = "__noise_";
    public static final String SHUFFLE_PREFIX = "__shuffle_";

    /** the family's config name (see {@link Family#NAMES}) */
    public String family;
    /** {@link #family} resolved once by {@link #parse} (null while unknown) — the scorers read it per row */
    private Family resolvedFamily;
    public String group;
    public String labelField;
    public String labelExpr;
    public boolean normalizeTies = true;
    public String baselineField;
    public String baselineForm;
    /** what an invalid baseline value does to the unit: skip it whole (default) or drop the row */
    public String baselineInvalid = Baselines.INVALID_SKIP_UNIT;
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
    /** conditioning.missing: how a missing conditioning value enters F̃ ({@link #MISSING_MEAN} / {@link #MISSING_GROUP_MEAN}) */
    public String conditioningMissing = MISSING_MEAN;

    /** output.selection: URI / path of the pass-list file (null = not written) */
    public String selectionUri;
    /** SHA-256 (16 hex, the feature plan hash width) of the canonical parameters without the file locations (output, candidates.manifest): the identity of this screen configuration */
    public String parametersHash;
    /** plan / output hash of the upstream feature manifest (candidates.manifest), when given */
    public String manifestPlanHash;
    public String manifestOutputHash;

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

    /** Whether an invalid baseline value drops its row ({@code baseline.invalid: dropRow}) instead of skipping the unit. */
    public boolean baselineDropsRows() {
        return hasBaseline() && Baselines.INVALID_DROP_ROW.equals(baselineInvalid);
    }

    /** The parsed family (null while unknown: parse reports the error). */
    public Family family() {
        if (resolvedFamily == null) resolvedFamily = Family.of(family);
        return resolvedFamily;
    }

    public boolean isGroupedMultinomial() {
        return family() == Family.GROUPED_MULTINOMIAL;
    }

    public boolean isBinomial() {
        return family() == Family.BINOMIAL;
    }

    public boolean isGaussian() {
        return family() == Family.GAUSSIAN;
    }

    public boolean isPoisson() {
        return family() == Family.POISSON;
    }

    /**
     * The Fisher weight of a row family at the mean μ: binomial μ(1 − μ), poisson μ, gaussian 1 (σ² is applied by
     * the report). One definition for the marginal moments, the conditioning fit and the report's prior mode.
     */
    public double fisherWeight(final double mu) {
        return family().fisherWeight(mu);
    }

    /** The link of a row family at the mean μ (the intercept that reproduces μ without a baseline). */
    public double link(final double mu) {
        return family().link(mu);
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
        if (s.family == null) s.family = Family.GROUPED_MULTINOMIAL.id();
        s.resolvedFamily = Family.of(s.family);
        if (s.resolvedFamily == null) {
            errors.add("unknown family '" + s.family + "' (available: " + Family.NAMES + ")");
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
            final List<String> forms = Family.formsFor(s.family);
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
                final String invalid = string(o, "invalid");
                if (invalid != null) s.baselineInvalid = invalid;
                if (!Baselines.INVALIDS.contains(s.baselineInvalid)) errors.add("baseline.invalid '" + s.baselineInvalid + "' is unknown (available: " + Baselines.INVALIDS + ")");
            } else {
                errors.add("baseline must be a field name or an object {field, form, invalid}");
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
                if (!StatMath.PERIOD_BUCKETS.contains(s.periodsBucket)) errors.add("unknown periods.bucket '" + s.periodsBucket + "' (available: " + StatMath.PERIOD_BUCKETS + ")");
            } else if (periods.isJsonPrimitive()) {
                s.periodsBucket = periods.getAsString();
                if (!StatMath.PERIOD_BUCKETS.contains(s.periodsBucket)) errors.add("unknown periods bucket '" + s.periodsBucket + "' (available: " + StatMath.PERIOD_BUCKETS + ")");
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
                final String missing = string(o, "missing");
                if (missing != null) {
                    s.conditioningMissing = missing;
                    if (!MISSINGS.contains(missing)) {
                        errors.add("unknown conditioning.missing '" + missing + "' (available: " + MISSINGS + ")");
                    } else if (MISSING_GROUP_MEAN.equals(missing) && s.resolvedFamily != null && !s.isGroupedMultinomial()) {
                        errors.add("conditioning.missing " + MISSING_GROUP_MEAN + " needs family " + Family.GROUPED_MULTINOMIAL.id() + " (the fill is the unit's baseline-weighted mean); the row families use " + MISSING_MEAN);
                    }
                }
                if (s.conditioningL2 < 0) errors.add("conditioning.l2 must be >= 0");
                if (s.conditioningMaxIter < 1 || s.conditioningMaxIter > 100) errors.add("conditioning.maxIter must be in [1, 100] (every iteration is one pass over the data)");
                if (s.conditioningTol <= 0) errors.add("conditioning.tol must be > 0");
            } else if (conditioning.isJsonArray()) {
                s.conditioningPatterns = strings(p, "conditioning", errors);
                if (s.conditioningPatterns.isEmpty()) errors.add("conditioning must list at least one field (names or globs of the conditioning columns)");
            } else {
                errors.add("conditioning must be an object {fields, l2, maxIter, tol, missing} or a list of field names");
            }
        }
        final JsonElement output = p.get("output");
        if (output != null && !output.isJsonNull()) {
            if (output.isJsonObject()) {
                final JsonObject o = output.getAsJsonObject();
                s.selectionUri = string(o, "selection");
                if (o.has("selection") && (s.selectionUri == null || s.selectionUri.isBlank())) errors.add("output.selection must be a URI or path of the pass-list file to write");
            } else {
                errors.add("output must be an object {selection: <uri>}");
            }
        }
        // the same digest (and width) as the feature manifest's planHash / outputHash written beside it
        s.parametersHash = FeaturePlanCompiler.sha256(FeaturePlanCompiler.canonical(withoutLocations(p)));

        // rules that depend on group are checked in resolve (group may still come from the manifest roles)
        if (!errors.isEmpty()) throw new IllegalArgumentException(String.join("; ", errors));
        return s;
    }

    // ---- resolution ----------------------------------------------------------------------------------------

    /**
     * Applies role defaults, validates the fields against the input schema and chooses the candidate columns:
     * numeric input fields matching {@code candidates.include}, minus {@code candidates.exclude} (name globs and
     * lineage selectors {@code derivedFrom:} / {@code scope:} / {@code block:} / {@code evidence:} / {@code kind:}), minus every
     * role field. Throws {@link IllegalArgumentException} listing every error.
     */
    public ScreenSpec resolve(final Schema inputSchema, final FeatureLineage lineage) {
        final List<String> errors = new ArrayList<>();
        final FeatureLineage l = lineage == null ? new FeatureLineage() : lineage;
        if (group == null && l.roles.containsKey("group")) {
            group = l.roles.get("group");
            notes.add("group defaulted to the feature transform's role: " + group);
            if (!transformsExplicit) transforms = new ArrayList<>(TRANSFORMS);
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
            if (Family.FORM_INVERSE_SHARE.equals(baselineForm)) errors.add("baseline.form inverseShare needs group (the share is taken within the group)");
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
        if (shuffleField != null && fields.containsKey(shuffleField) && !FeatureLineage.isNumeric(fields.get(shuffleField))) {
            errors.add("placebo.shuffle.field '" + shuffleField + "' must be numeric (" + fields.get(shuffleField).getFieldType().getType() + "); a non-numeric reference makes every shuffle placebo degenerate");
        }

        final Set<String> reserved = new HashSet<>();
        for (final String r : new String[]{group, labelField, baselineField, timeField, weightField, periodsField}) if (r != null) reserved.add(r);
        reserved.addAll(rowId);
        // every label column of the upstream feature transform (a direction: future block has several), not only the
        // selected one: a post-event label is never a candidate feature
        reserved.addAll(l.labels);
        if (labelExpr != null) {
            reserved.addAll(com.mercari.solution.util.ExpressionUtil.createDefaultExpression(labelExpr).getVariableNames());
        }

        final List<Pattern> includes = candidateInclude.stream().filter(s -> !FeatureLineage.isSelector(s)).map(StatMath::glob).toList();
        final List<String> includeSelectors = candidateInclude.stream().filter(FeatureLineage::isSelector).toList();
        candidates = new ArrayList<>();
        final List<String> excludedByLineage = new ArrayList<>();
        if (inputSchema != null) {
            for (final Schema.Field f : inputSchema.getFields()) {
                if (!FeatureLineage.isNumeric(f)) continue;
                final String name = f.getName();
                if (reserved.contains(name)) continue;
                final FeatureLineage.Entry entry = l.columns.get(name);
                boolean included = includes.stream().anyMatch(p -> p.matcher(name).matches());
                if (!included) included = includeSelectors.stream().anyMatch(s -> FeatureLineage.selectorMatches(s, entry));
                if (!included) continue;
                boolean excluded = false;
                for (final String pattern : candidateExclude) {
                    if (FeatureLineage.isSelector(pattern)) {
                        if (FeatureLineage.selectorMatches(pattern, entry)) {
                            excluded = true;
                            excludedByLineage.add(name + " (" + pattern + ")");
                            break;
                        }
                    } else if (StatMath.glob(pattern).matcher(name).matches()) {
                        excluded = true;
                        break;
                    }
                }
                if (!excluded) candidates.add(name);
            }
        }
        if (!excludedByLineage.isEmpty()) notes.add("excluded by lineage: " + excludedByLineage);
        final boolean usesSelectors = candidateExclude.stream().anyMatch(FeatureLineage::isSelector) || !includeSelectors.isEmpty();
        if (usesSelectors && l.columns.isEmpty()) {
            errors.add("candidates use lineage selectors (derivedFrom: / scope: / block: / evidence: / kind:) but no lineage is available: "
                    + "put the feature transform directly upstream or set candidates.manifest to its manifest URI");
        }
        if (candidates.isEmpty()) errors.add("no candidate column: candidates.include " + candidateInclude + " matched no numeric input field (after exclusions)");

        // conditioning columns: numeric fields matching the patterns, never the label / group / time / weight roles
        conditioningFields = new ArrayList<>();
        if (!conditioningPatterns.isEmpty() && inputSchema != null) {
            final Set<String> roleOnly = new HashSet<>();
            for (final String r : new String[]{group, labelField, baselineField, timeField, weightField, periodsField}) if (r != null) roleOnly.add(r);
            roleOnly.addAll(l.labels);
            if (labelExpr != null) roleOnly.addAll(com.mercari.solution.util.ExpressionUtil.createDefaultExpression(labelExpr).getVariableNames());
            for (final String pattern : conditioningPatterns) {
                final Pattern glob = StatMath.glob(pattern);
                boolean matched = false;
                for (final Schema.Field f : inputSchema.getFields()) {
                    if (!FeatureLineage.isNumeric(f) || roleOnly.contains(f.getName()) || !glob.matcher(f.getName()).matches()) continue;
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

    // ---- identity ------------------------------------------------------------------------------------------

    /**
     * The parameters without what does not change the screen: the pass-list destination ({@code output}) and the
     * manifest location ({@code candidates.manifest}, whose content identity travels as planHash / outputHash).
     * Mirrors the feature transform's plan hash, which excludes its artifact and output locations the same way.
     */
    static JsonObject withoutLocations(final JsonObject parameters) {
        final JsonObject copy = parameters.deepCopy();
        copy.remove("output");
        if (copy.has("candidates") && copy.get("candidates").isJsonObject()) {
            final JsonObject candidates = copy.getAsJsonObject("candidates");
            candidates.remove("manifest");
            if (candidates.isEmpty()) copy.remove("candidates");
        }
        return copy;
    }
}
