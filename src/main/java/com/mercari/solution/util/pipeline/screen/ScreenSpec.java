package com.mercari.solution.util.pipeline.screen;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureLineage;
import com.mercari.solution.util.pipeline.feature.FeaturePlanCompiler;
import com.mercari.solution.util.pipeline.glm.Baselines;
import com.mercari.solution.util.pipeline.glm.Family;
import com.mercari.solution.util.pipeline.glm.SketchAccumulator;
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
    /** the binned score test (DSL doc §12.1): a df = k − 1 block, never in the default list */
    public static final String TRANSFORM_BINNED = "binned";
    public static final List<String> TRANSFORMS_AVAILABLE = List.of(TRANSFORM_RAW, TRANSFORM_RANK, TRANSFORM_ABSDEV, TRANSFORM_BINNED);
    /** bins.edges: window value quantiles (the sketch pre-pass) or the within-unit rank (grouped only) */
    public static final String EDGES_VALUE = "value";
    public static final String EDGES_RANK = "rank";
    /** the statistic kinds a placebo threshold is pooled over: the df = 1 transforms, and the binned block test */
    public static final String KIND_DF1 = "df1";
    public static final String KIND_BINNED = "binned";
    public static final int BINS_DEFAULT = 10;
    public static final int BINS_MAX = 100;
    /** heterogeneity.by: the period buckets, or a declared field's levels (DSL doc §7.1) */
    public static final String HET_PERIODS = "periods";
    public static final String HET_FIELD = "field";
    /** the level a null modifier value takes */
    public static final String LEVEL_NULL = "(null)";
    /** the heterogeneity test's own placebo kind */
    public static final String KIND_HET = "het";

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
    /** bins.k: the number of value / position bins of the binned test (the missing bin comes on top) */
    public int binsK = BINS_DEFAULT;
    /** bins.edges: {@link #EDGES_VALUE} (window quantiles) or {@link #EDGES_RANK} (within-unit rank, grouped only) */
    public String binsEdges = EDGES_VALUE;
    /** True when the config declares a {@code bins} block (which needs the binned transform). */
    public boolean binsExplicit;
    /** heterogeneity.by: {@link #HET_PERIODS} / {@link #HET_FIELD} (null = no heterogeneity test) */
    public String heterogeneityBy;
    /** heterogeneity.field: the modifier field (by = field); read per row, per unit (its first row) for the grouped family */
    public String heterogeneityField;

    /** suggestions: the one-candidate derivation suggestions from the binned sums (DSL doc §9.4; needs the binned transform) */
    public boolean suggestionsOn;
    /** pairs.fields: declared pairs of conditioning fields (names); pairs.among: fields whose every pair is tested */
    public List<String[]> pairFields = new ArrayList<>();
    public List<String> pairAmong = new ArrayList<>();
    /** pairs.maxPairs: the bound on the pairs a run tests (each costs 2 + k doubles per partial key) */
    public int pairMaxPairs = PAIRS_MAX_DEFAULT;
    /** pairs.placebo: noise placebos per pair (member × noise column), the pair kind's calibration */
    public int pairPlacebo = PAIR_PLACEBO_DEFAULT;
    /** the resolved pairs as indices into {@link #conditioningFields} (DSL doc §8.6) */
    public List<int[]> pairs = new ArrayList<>();

    public static final int PAIRS_MAX_DEFAULT = 200;
    public static final int PAIR_PLACEBO_DEFAULT = 5;

    /** joint: the candidates' joint sums (S, the m × m Fisher matrix, the pHd matrix) for the several-candidate suggestions (DSL doc §9.5) */
    public boolean jointOn;
    /** joint.include: globs / selectors choosing the joint columns among the candidates (default every candidate) */
    public List<String> jointInclude = new ArrayList<>();
    /** joint.maxColumns: the bound on the joint columns (O(m²) state and per-row work) */
    public int jointMaxColumns = JOINT_MAX_COLUMNS_DEFAULT;
    /** joint.noise: noise placebo columns carried in the joint sums (the pHd null scale) */
    public int jointNoise = JOINT_NOISE_DEFAULT;
    /** joint.directions: pHd directions reported */
    public int jointDirections = 3;
    /** joint.redundancy: |correlation| at and above which candidates are one redundancy cluster */
    public double jointRedundancy = 0.95;
    /** joint.select: the forward selection's maximum number of steps */
    public int jointSelect = 10;
    /** the resolved joint columns as candidate indices */
    public List<Integer> jointColumns = new ArrayList<>();

    public static final int JOINT_MAX_COLUMNS_DEFAULT = 200;
    public static final int JOINT_NOISE_DEFAULT = 10;

    /** Joint columns carried: the chosen candidates, then the noise columns. */
    public int jointColumnCount() {
        return jointColumns.size() + jointNoiseCount();
    }

    public int jointNoiseCount() {
        return Math.min(jointNoise, noise);
    }

    /** The column (in the scorer's column order: candidates, noise placebos, shuffle placebos) of joint column {@code j}. */
    public int jointColumn(final int j) {
        return j < jointColumns.size() ? jointColumns.get(j) : candidates.size() + (j - jointColumns.size());
    }

    public boolean isJointNoise(final int j) {
        return j >= jointColumns.size();
    }
    /** the pair test's own placebo kind and record transform */
    public static final String KIND_PAIR = "pair";
    public static final String TRANSFORM_PRODUCT = "product";

    public boolean hasPairs() {
        return !pairs.isEmpty();
    }

    /** The pair's record name: {@code a*b}; a placebo pair {@code a*__noise_<r>}. */
    public String pairName(final int pair) {
        if (pair < pairs.size()) return conditioningFields.get(pairs.get(pair)[0]) + "*" + conditioningFields.get(pairs.get(pair)[1]);
        final int q = pair - pairs.size();
        return conditioningFields.get(pairs.get(q / pairPlacebo)[0]) + "*" + NOISE_PREFIX + (q % pairPlacebo);
    }

    /** The real pairs, then {@code pairPlacebo} placebo pairs per real pair (its first member × a noise column). */
    public int pairCount() {
        return pairs.size() * (1 + pairPlacebo);
    }

    public boolean isPlaceboPair(final int pair) {
        return pair >= pairs.size();
    }

    /** The accumulator key of a pair (after every column × transform key). */
    public int pairKey(final int pair) {
        return columnCount() * transforms.size() + pair;
    }

    /** The conditioning-field indices of a pair's members; for a placebo pair the first member and the noise column index (as the second value, negative: −1 − r). */
    public int[] pairMembers(final int pair) {
        if (pair < pairs.size()) return pairs.get(pair);
        final int q = pair - pairs.size();
        return new int[]{pairs.get(q / pairPlacebo)[0], -1 - (q % pairPlacebo)};
    }

    public boolean hasHeterogeneity() {
        return heterogeneityBy != null;
    }

    public boolean heterogeneityByPeriods() {
        return HET_PERIODS.equals(heterogeneityBy);
    }

    /** The modifier as the summary names it: {@code periods} or {@code field:<name>}. */
    public String heterogeneityLabel() {
        return heterogeneityBy == null ? null : heterogeneityByPeriods() ? HET_PERIODS : HET_FIELD + ":" + heterogeneityField;
    }
    public String periodsField;
    public String periodsFieldType;
    public String periodsBucket;
    public int noise = 100;
    public String shuffleField;
    public int shuffleN = 0;
    public double quantile = 0.99;
    public long seed = 0L;
    public Double leakZ;
    /** flags.leakZ.on: the z the leak flag reads — {@link #LEAK_ON_MARGINAL} (default) or {@link #LEAK_ON_PARTIAL} (needs conditioning) */
    public String leakOn = LEAK_ON_MARGINAL;
    public static final String LEAK_ON_MARGINAL = "marginal";
    public static final String LEAK_ON_PARTIAL = "partial";
    public static final List<String> LEAK_ONS = List.of(LEAK_ON_MARGINAL, LEAK_ON_PARTIAL);
    /** conditioning.fields as written (names / globs); empty = no partial test */
    public List<String> conditioningPatterns = new ArrayList<>();
    public double conditioningL2 = 1e-4;
    public int conditioningMaxIter = 10;
    public double conditioningTol = 1e-8;
    /** conditioning.missing: how a missing conditioning value enters F̃ ({@link #MISSING_MEAN} / {@link #MISSING_GROUP_MEAN}) */
    public String conditioningMissing = MISSING_MEAN;

    /**
     * pass.minPeriodsAgree: how many of the usable period buckets of the effective test must agree with its overall
     * sign for {@code passed} — a share in (0, 1] or a count above 1 (null = the placebo cut alone)
     */
    public Double minPeriodsAgree;
    /**
     * pass.minGain: a practical floor on the effective test's gain (est_gain / partial_gain, the average
     * log-likelihood improvement per unit): {@code passed} needs the gain above {@code max(threshold, minGain)}
     * (null = the placebo threshold alone)
     */
    public Double minGain;

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

    /** Whether the leak flag asks for the partial z ({@code flags.leakZ.on: partial}); the report falls back without a partial test. */
    public boolean leakOnPartial() {
        return leakZ != null && LEAK_ON_PARTIAL.equals(leakOn);
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

    /**
     * Whether the run needs the window quantile sketches (engine doc §2): independent rows (no group) with a
     * {@code rank} or {@code absdev} transform, whose "within the unit" would be a single row.
     */
    public boolean needsWindowQuantiles() {
        return (group == null && (transforms.contains(TRANSFORM_RANK) || transforms.contains(TRANSFORM_ABSDEV)))
                || (hasBinned() && EDGES_VALUE.equals(binsEdges));
    }

    public boolean hasBinned() {
        return transforms.contains(TRANSFORM_BINNED);
    }

    public static boolean isBinned(final String transform) {
        return TRANSFORM_BINNED.equals(transform);
    }

    /** The statistic kind a transform's placebo threshold is pooled over. */
    public static String kind(final String transform) {
        return isBinned(transform) ? KIND_BINNED : TRANSFORM_PRODUCT.equals(transform) ? KIND_PAIR : KIND_DF1;
    }

    /** Bins of the binned test: {@code bins.k} value / position bins plus the missing bin (the last index). */
    public int binCount() {
        return binsK + 1;
    }

    /** The index of the missing bin. */
    public int missingBin() {
        return binsK;
    }

    /** Accumulator key of (column, transform). */
    public int key(final int column, final int transform) {
        return column * transforms.size() + transform;
    }

    /**
     * Whether {@code agree} of {@code nPeriods} usable periods satisfy {@code pass.minPeriodsAgree}: always when
     * it is not declared; otherwise a share (≤ 1) of the usable periods or a count (> 1), and never with no
     * usable period.
     */
    public boolean periodsAgree(final long agree, final long nPeriods) {
        if (minPeriodsAgree == null) return true;
        if (nPeriods <= 0) return false;
        final double required = minPeriodsAgree <= 1 ? minPeriodsAgree * nPeriods - 1e-9 : minPeriodsAgree;
        return agree >= required;
    }

    /**
     * The cut the effective test's gain must exceed for {@code passed}: the placebo (or theoretical) threshold,
     * lifted to {@code pass.minGain} when that is declared and higher. NaN stays NaN (no scorable unit).
     */
    public double gainCut(final double threshold) {
        if (minGain == null || Double.isNaN(threshold)) return threshold;
        return Math.max(threshold, minGain);
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
                    if (!TRANSFORMS_AVAILABLE.contains(name)) errors.add("unknown transform '" + name + "' (available: " + TRANSFORMS_AVAILABLE + ")");
                    else if (!s.transforms.contains(name)) s.transforms.add(name);
                }
            } else {
                errors.add("transforms must be a list (available: " + TRANSFORMS_AVAILABLE + ")");
            }
        }
        final JsonElement bins = p.get("bins");
        if (bins != null && !bins.isJsonNull()) {
            s.binsExplicit = true;
            if (bins.isJsonObject()) {
                final JsonObject o = bins.getAsJsonObject();
                final Double k = number(o, "k");
                if (k != null) {
                    if (k < 2 || k > BINS_MAX || k != Math.rint(k)) errors.add("bins.k must be an integer in [2, " + BINS_MAX + "]");
                    else s.binsK = k.intValue();
                }
                final String edges = string(o, "edges");
                if (edges != null) {
                    if (!EDGES_VALUE.equals(edges) && !EDGES_RANK.equals(edges)) errors.add("bins.edges must be value (window quantiles) or rank (the within-unit rank; needs group)");
                    else s.binsEdges = edges;
                }
            } else if (bins.isJsonPrimitive() && bins.getAsJsonPrimitive().isNumber()) {
                final double k = bins.getAsDouble();
                if (k < 2 || k > BINS_MAX || k != Math.rint(k)) errors.add("bins must be an integer in [2, " + BINS_MAX + "]");
                else s.binsK = (int) k;
            } else {
                errors.add("bins must be an object {k, edges} or the number of bins");
            }
            if (!s.transforms.contains(TRANSFORM_BINNED)) errors.add("bins needs the binned transform (transforms: [..., binned])");
        }
        final JsonElement het = p.get("heterogeneity");
        if (het != null && !het.isJsonNull()) {
            if (het.isJsonPrimitive() && het.getAsJsonPrimitive().isString()) {
                // "periods", or a field name
                final String v = het.getAsString();
                if (HET_PERIODS.equals(v)) s.heterogeneityBy = HET_PERIODS;
                else {
                    s.heterogeneityBy = HET_FIELD;
                    s.heterogeneityField = v;
                }
            } else if (het.isJsonObject()) {
                final JsonObject o = het.getAsJsonObject();
                final String by = string(o, "by");
                final String field = string(o, "field");
                if (by == null) {
                    if (field != null) {
                        s.heterogeneityBy = HET_FIELD;
                        s.heterogeneityField = field;
                    } else {
                        errors.add("heterogeneity must name a modifier: periods, or {field: <name>}");
                    }
                } else if (HET_PERIODS.equals(by)) {
                    s.heterogeneityBy = HET_PERIODS;
                } else if (HET_FIELD.equals(by)) {
                    if (field == null) errors.add("heterogeneity.by field needs heterogeneity.field");
                    s.heterogeneityBy = HET_FIELD;
                    s.heterogeneityField = field;
                } else {
                    errors.add("heterogeneity.by must be periods or field");
                }
            } else {
                errors.add("heterogeneity must be periods, a field name, or an object {by, field}");
            }
        }
        final JsonElement suggestions = p.get("suggestions");
        if (suggestions != null && !suggestions.isJsonNull()) {
            if (suggestions.isJsonPrimitive() && suggestions.getAsJsonPrimitive().isBoolean()) {
                s.suggestionsOn = suggestions.getAsBoolean();
            } else if (suggestions.isJsonObject()) {
                final Boolean enabled = suggestions.getAsJsonObject().has("enabled") ? suggestions.getAsJsonObject().get("enabled").getAsBoolean() : Boolean.TRUE;
                s.suggestionsOn = enabled;
            } else {
                errors.add("suggestions must be a boolean or an object {enabled}");
            }
            if (s.suggestionsOn && !s.transforms.contains(TRANSFORM_BINNED)) {
                errors.add("suggestions read the binned sums: add binned to transforms");
            }
        }
        final JsonElement pairs = p.get("pairs");
        if (pairs != null && !pairs.isJsonNull()) {
            if (pairs.isJsonObject()) {
                final JsonObject o = pairs.getAsJsonObject();
                if (o.has("fields") && o.get("fields").isJsonArray()) {
                    for (final JsonElement e : o.getAsJsonArray("fields")) {
                        if (!e.isJsonArray() || e.getAsJsonArray().size() != 2) {
                            errors.add("pairs.fields must be a list of [a, b] pairs of field names");
                            continue;
                        }
                        s.pairFields.add(new String[]{e.getAsJsonArray().get(0).getAsString(), e.getAsJsonArray().get(1).getAsString()});
                    }
                }
                s.pairAmong = strings(o, "among", errors);
                final Double max = number(o, "maxPairs");
                if (max != null) {
                    if (max < 1 || max != Math.rint(max)) errors.add("pairs.maxPairs must be a positive integer");
                    else s.pairMaxPairs = max.intValue();
                }
                final Double placebo = number(o, "placebo");
                if (placebo != null) {
                    if (placebo < 0 || placebo != Math.rint(placebo)) errors.add("pairs.placebo must be a non-negative integer");
                    else s.pairPlacebo = placebo.intValue();
                }
                if (s.pairFields.isEmpty() && s.pairAmong.isEmpty()) errors.add("pairs needs fields ([[a, b], ...]) or among ([names / globs])");
            } else {
                errors.add("pairs must be an object {fields: [[a, b], ...], among: [...], maxPairs, placebo}");
            }
        }
        final JsonElement joint = p.get("joint");
        if (joint != null && !joint.isJsonNull()) {
            if (joint.isJsonPrimitive() && joint.getAsJsonPrimitive().isBoolean()) {
                s.jointOn = joint.getAsBoolean();
            } else if (joint.isJsonObject()) {
                final JsonObject o = joint.getAsJsonObject();
                s.jointOn = !o.has("enabled") || o.get("enabled").getAsBoolean();
                s.jointInclude = strings(o, "include", errors);
                final Double max = number(o, "maxColumns");
                if (max != null) {
                    if (max < 2 || max != Math.rint(max)) errors.add("joint.maxColumns must be an integer of at least 2");
                    else s.jointMaxColumns = max.intValue();
                }
                final Double jn = number(o, "noise");
                if (jn != null) {
                    if (jn < 0 || jn != Math.rint(jn)) errors.add("joint.noise must be a non-negative integer");
                    else s.jointNoise = jn.intValue();
                }
                final Double directions = number(o, "directions");
                if (directions != null) {
                    if (directions < 1 || directions != Math.rint(directions)) errors.add("joint.directions must be a positive integer");
                    else s.jointDirections = directions.intValue();
                }
                final Double redundancy = number(o, "redundancy");
                if (redundancy != null) {
                    if (!(redundancy > 0 && redundancy <= 1)) errors.add("joint.redundancy must be in (0, 1]");
                    else s.jointRedundancy = redundancy;
                }
                final Double select = number(o, "select");
                if (select != null) {
                    if (select < 0 || select != Math.rint(select)) errors.add("joint.select must be a non-negative integer");
                    else s.jointSelect = select.intValue();
                }
            } else {
                errors.add("joint must be a boolean or an object {include, maxColumns, noise, directions, redundancy, select}");
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
            final JsonElement leak = flags.getAsJsonObject().get("leakZ");
            if (leak != null && !leak.isJsonNull()) {
                // a number (or a numeric string, as the scalar form always accepted) or {z, on}
                s.leakZ = numeric(leak.isJsonObject() ? leak.getAsJsonObject().get("z") : leak);
                if (leak.isJsonObject()) {
                    if (s.leakZ == null) errors.add("flags.leakZ.z is required and must be a number (the |z| above which a candidate is a leak suspect)");
                    final JsonElement on = leak.getAsJsonObject().get("on");
                    if (on != null && !on.isJsonNull()) s.leakOn = on.isJsonPrimitive() ? on.getAsString() : on.toString();
                    if (!LEAK_ONS.contains(s.leakOn)) errors.add("flags.leakZ.on '" + s.leakOn + "' is unknown (available: " + LEAK_ONS + ")");
                } else if (s.leakZ == null) {
                    errors.add("flags.leakZ must be a number or an object {z, on: marginal | partial}");
                }
            }
            // NaN would never compare above a |z|: the flag would silently never fire
            if (s.leakZ != null && !(s.leakZ > 0)) errors.add("flags.leakZ must be > 0");
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
        if (s.leakOnPartial() && s.conditioningPatterns.isEmpty()) {
            errors.add("flags.leakZ.on partial needs conditioning (the flag reads the partial z)");
        }
        final JsonElement pass = p.get("pass");
        if (pass != null && !pass.isJsonNull()) {
            if (pass.isJsonObject()) {
                final JsonObject o = pass.getAsJsonObject();
                s.minPeriodsAgree = number(o, "minPeriodsAgree");
                if (s.minPeriodsAgree != null) {
                    if (!(s.minPeriodsAgree > 0)) errors.add("pass.minPeriodsAgree must be > 0 (a share of the usable periods up to 1, or a count above 1)");
                    else if (s.minPeriodsAgree > 1 && s.minPeriodsAgree != Math.rint(s.minPeriodsAgree)) errors.add("pass.minPeriodsAgree above 1 is a count of periods and must be an integer");
                    if (s.periodsBucket == null) errors.add("pass.minPeriodsAgree needs periods (the sign agreement is read per period bucket)");
                }
                s.minGain = number(o, "minGain");
                if (s.minGain != null && !(s.minGain > 0 && Double.isFinite(s.minGain))) {
                    errors.add("pass.minGain must be a positive finite number (a floor on est_gain / partial_gain, the average log-likelihood improvement per unit)");
                }
            } else {
                errors.add("pass must be an object {minPeriodsAgree, minGain}");
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

    /** A JSON number or numeric string as a double; null for anything else (a non-numeric string, a boolean, an array / object). */
    private static Double numeric(final JsonElement e) {
        if (e == null || !e.isJsonPrimitive()) return null;
        try {
            return e.getAsDouble();
        } catch (final NumberFormatException ex) {
            return null;
        }
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
            if (needsWindowQuantiles()) {
                notes.add("rank / absdev of independent rows are taken against the window's quantile sketch (KLL k=" + SketchAccumulator.K + ", rank error about 0.8%; noise placebos use the exact normal cdf)");
            }
            if (hasShuffle()) errors.add("placebo.shuffle needs group (within-group permutation)");
            if (hasBinned() && EDGES_RANK.equals(binsEdges)) errors.add("bins.edges rank needs group (the position bins read the within-unit rank); use edges: value for independent rows");
            if (Family.FORM_INVERSE_SHARE.equals(baselineForm)) errors.add("baseline.form inverseShare needs group (the share is taken within the group)");
        }
        if (periodsBucket != null && periodsField == null) errors.add("periods needs a field (periods.field or time.field)");
        if (heterogeneityByPeriods() && periodsBucket == null) errors.add("heterogeneity: periods needs periods (the levels are the period buckets)");
        if (HET_FIELD.equals(heterogeneityBy) && heterogeneityField != null && isGroupedMultinomial()) {
            notes.add("heterogeneity by " + heterogeneityField + ": the grouped family reads the modifier per unit (the value of the unit's first row)");
        }

        final Map<String, Schema.Field> fields = new HashMap<>();
        if (inputSchema != null) for (final Schema.Field f : inputSchema.getFields()) fields.put(f.getName(), f);
        for (final String[] ref : new String[][]{{"group", group}, {"label.field", labelField}, {"baseline.field", baselineField},
                {"time.field", timeField}, {"weight.field", weightField}, {"periods.field", periodsField}, {"placebo.shuffle.field", shuffleField},
                {"heterogeneity.field", heterogeneityField}}) {
            if (ref[1] != null && !fields.containsKey(ref[1])) errors.add(ref[0] + " '" + ref[1] + "' is not an input field");
        }
        for (final String id : rowId) if (!fields.containsKey(id)) errors.add("rowId '" + id + "' is not an input field");
        if (timeField != null && fields.containsKey(timeField)) timeFieldType = fields.get(timeField).getFieldType().getType().name();
        if (periodsField != null && fields.containsKey(periodsField)) periodsFieldType = fields.get(periodsField).getFieldType().getType().name();
        if (shuffleField != null && fields.containsKey(shuffleField) && !FeatureLineage.isNumeric(fields.get(shuffleField))) {
            errors.add("placebo.shuffle.field '" + shuffleField + "' must be numeric (" + fields.get(shuffleField).getFieldType().getType() + "); a non-numeric reference makes every shuffle placebo degenerate");
        }

        final Set<String> reserved = new HashSet<>();
        for (final String r : new String[]{group, labelField, baselineField, timeField, weightField, periodsField, heterogeneityField}) if (r != null) reserved.add(r);
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
        // pairs: every member is a conditioning field (the product is tested at the fitted means of a model holding
        // its members), declared as pairs or as a set whose every pair is tested, within the bound
        pairs = new ArrayList<>();
        if (!pairFields.isEmpty() || !pairAmong.isEmpty()) {
            if (conditioningFields.isEmpty()) errors.add("pairs need conditioning: a product is tested at the fitted means of a model holding both members (declare them in conditioning.fields)");
            // the placebo block may follow pairs in the parameters: checked once both are read
            if (pairPlacebo > noise) errors.add("pairs.placebo (" + pairPlacebo + ") exceeds placebo.noise (" + noise + "): a placebo pair is a member times a noise column");
            final Set<String> seen = new HashSet<>();
            for (final String[] pf : pairFields) {
                final int a = conditioningFields.indexOf(pf[0]), b = conditioningFields.indexOf(pf[1]);
                if (a < 0 || b < 0) errors.add("pairs.fields [" + pf[0] + ", " + pf[1] + "]: both members must be conditioning fields (" + conditioningFields + ")");
                else if (a == b) errors.add("pairs.fields [" + pf[0] + ", " + pf[1] + "]: a pair needs two different fields");
                else if (seen.add(Math.min(a, b) + ":" + Math.max(a, b))) pairs.add(new int[]{Math.min(a, b), Math.max(a, b)});
            }
            final List<Integer> among = new ArrayList<>();
            for (final String pattern : pairAmong) {
                final Pattern glob = StatMath.glob(pattern);
                boolean matched = false;
                for (int i = 0; i < conditioningFields.size(); i++) {
                    if (glob.matcher(conditioningFields.get(i)).matches()) {
                        matched = true;
                        if (!among.contains(i)) among.add(i);
                    }
                }
                if (!matched) errors.add("pairs.among '" + pattern + "' matched no conditioning field (" + conditioningFields + ")");
            }
            for (int i = 0; i < among.size(); i++) {
                for (int j = i + 1; j < among.size(); j++) {
                    final int a = Math.min(among.get(i), among.get(j)), b = Math.max(among.get(i), among.get(j));
                    if (seen.add(a + ":" + b)) pairs.add(new int[]{a, b});
                }
            }
            if (pairs.size() > pairMaxPairs) errors.add("pairs: " + pairs.size() + " pairs exceed pairs.maxPairs " + pairMaxPairs + " (each costs 2 + k doubles per partial key; declare fewer members or raise the bound)");
        }
        // joint columns: the candidates matching joint.include (every candidate by default), within the bound
        jointColumns = new ArrayList<>();
        if (jointOn) {
            final List<Pattern> globs = jointInclude.stream().filter(s -> !FeatureLineage.isSelector(s)).map(StatMath::glob).toList();
            final List<String> selectors = jointInclude.stream().filter(FeatureLineage::isSelector).toList();
            for (int c = 0; c < candidates.size(); c++) {
                final String name = candidates.get(c);
                boolean in = jointInclude.isEmpty() || globs.stream().anyMatch(g -> g.matcher(name).matches());
                if (!in && !selectors.isEmpty()) {
                    final FeatureLineage.Entry entry = l.columns.get(name);
                    in = selectors.stream().anyMatch(s -> FeatureLineage.selectorMatches(s, entry));
                }
                if (in) jointColumns.add(c);
            }
            if (jointColumns.size() < 2) errors.add("joint needs at least two candidate columns (joint.include " + jointInclude + " kept " + jointColumns.size() + ")");
            if (jointColumns.size() > jointMaxColumns) errors.add("joint: " + jointColumns.size() + " columns exceed joint.maxColumns " + jointMaxColumns + " (the joint sums are m x m per bundle and O(m²) per row; narrow joint.include or raise the bound)");
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
