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
    /** the binned score test (DSL doc §6.1): a df = k − 1 block, never in the default list */
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
    /** heterogeneity.by: {@link #HET_PERIODS} / {@link #HET_FIELD} (null = no heterogeneity test) */
    public String heterogeneityBy;
    /** heterogeneity.field: the modifier field (by = field); read per row, per unit (its first row) for the grouped family */
    public String heterogeneityField;

    /** suggestions: the one-candidate derivation suggestions from the binned sums (DSL doc §9.4; needs the binned transform) */
    public boolean suggestionsOn;
    /** pairs.fields: declared pairs of conditioning fields (names); pairs.among: fields whose every pair is tested */
    public List<String[]> pairFields = new ArrayList<>();
    public List<String> pairAmong = new ArrayList<>();
    /** pairs.maxPairs: the bound on the pairs a run tests (each costs 2 + k doubles per partial key, plus its 2-D grid) */
    public int pairMaxPairs = PAIRS_MAX_DEFAULT;
    /** pairs.placebo: noise placebos per pair (member × noise column), the pair kind's calibration */
    public int pairPlacebo = PAIR_PLACEBO_DEFAULT;
    /** the resolved pairs as indices into {@link #conditioningFields} (DSL doc §8.6) */
    public List<int[]> pairs = new ArrayList<>();
    /** pairs.shape: value bins per member of the pair's 2-D grid (the interaction shape, DSL doc §8.7); 0 = off */
    public int pairShapeBins = PAIR_SHAPE_BINS_DEFAULT;

    public static final int PAIR_SHAPE_BINS_DEFAULT = 4;

    public boolean hasPairShape() {
        return hasPairs() && pairShapeBins > 0;
    }

    /** The partial-pass key of a real pair's 2-D grid: after every pair key. */
    public int pairGridKey(final int pair) {
        return pairKey(pairCount()) + pair;
    }

    /** The x column of conditioning field {@code member}. */
    public int conditioningColumn(final int member) {
        return conditioningOffset() + member;
    }

    /**
     * The number of sketches of the window pre-pass: the leading columns of {@link ScreenRow#x} up to the last one it
     * feeds ({@link #sketchedColumns}), so a sketch index is the x column.
     */
    public int sketchColumns() {
        final int[] fed = sketchedColumns();
        return fed.length == 0 ? 0 : fed[fed.length - 1] + 1;
    }

    /**
     * The x columns the window sketch pre-pass feeds, ascending: the candidates and the shuffle reference when their
     * sketches are read ({@link #needsCandidateSketches}: rank / absdev of independent rows, value bins), the
     * candidates when the joint reads their minima ({@link #needsJointMinima}), plus the members of the real pairs
     * when their 2-D grids need value edges — not every candidate for a pair shape alone.
     */
    public int[] sketchedColumns() {
        final java.util.TreeSet<Integer> fed = new java.util.TreeSet<>();
        if (needsCandidateSketches()) for (int c = 0; c < conditioningOffset(); c++) fed.add(c);
        if (needsJointMinima()) for (int c = 0; c < candidates.size(); c++) fed.add(c);
        if (hasPairShape()) for (final int[] pair : pairs) for (final int member : pair) fed.add(conditioningColumn(member));
        return fed.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * the resolved placebo pairs {@code [member, noise column]}: up to {@code pairPlacebo} per pair, its first member
     * times a noise column that member is not already paired with (pairs sharing a member never repeat a placebo)
     */
    public List<int[]> pairPlacebos = new ArrayList<>();

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
    /** joint.pairs: at most this many difference / ratio suggestions (the pairs with the largest joint excess) */
    public int jointPairs = 10;
    /** joint.excess: a pair's joint χ² must exceed the better single one by this factor for a difference / ratio suggestion */
    public double jointExcess = 1.5;
    /** the resolved joint columns as candidate indices */
    public List<Integer> jointColumns = new ArrayList<>();

    public static final int JOINT_MAX_COLUMNS_DEFAULT = 200;
    public static final int JOINT_NOISE_DEFAULT = 10;

    /** categorical.include: globs / lineage selectors choosing the categorical candidates among the string fields (DSL doc §6.2) */
    public List<String> categoricalInclude = new ArrayList<>();
    /** categorical.maxLevels: the named levels kept (the most frequent); the rest fold into one "(other)" level */
    public int categoricalMaxLevels = CATEGORICAL_MAX_LEVELS_DEFAULT;
    /** categorical.placebo: placebo columns per categorical candidate (levels redrawn from the window frequencies) */
    public int categoricalPlacebo = CATEGORICAL_PLACEBO_DEFAULT;
    /** the resolved categorical candidates (field names) */
    public List<String> categoricals = new ArrayList<>();

    public static final int CATEGORICAL_MAX_LEVELS_DEFAULT = 32;
    public static final int CATEGORICAL_PLACEBO_DEFAULT = 5;
    /** the categorical block test's record transform and placebo kind */
    public static final String TRANSFORM_LEVELS = "levels";
    public static final String KIND_LEVELS = "levels";
    /** the level a null categorical value takes, and the fold of the levels beyond maxLevels */
    public static final String LEVEL_OTHER = "(other)";
    /** the marginal / partial keys of the categorical blocks start here (real columns, then their placebos) */
    public static final int CATEGORICAL_KEY_BASE = 1_000_000;

    public boolean hasCategoricals() {
        return !categoricals.isEmpty();
    }

    /** The key of categorical column {@code c} (real), or of its placebo {@code r} ({@code r >= 0}). */
    public int categoricalKey(final int c, final int r) {
        return CATEGORICAL_KEY_BASE + c * (1 + categoricalPlacebo) + (r + 1);
    }

    /** The name of categorical column {@code c}'s placebo {@code r}. */
    public String categoricalPlaceboName(final int c, final int r) {
        return categoricals.get(c) + "*" + NOISE_PREFIX + r;
    }

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
    /** the pair test's own placebo kind and record transform */
    public static final String KIND_PAIR = "pair";
    public static final String TRANSFORM_PRODUCT = "product";

    public boolean hasPairs() {
        return !pairs.isEmpty();
    }

    /** The member field names {@code [a, b]} of a (real) pair. */
    public String[] pairFieldNames(final int pair) {
        return new String[]{conditioningFields.get(pairs.get(pair)[0]), conditioningFields.get(pairs.get(pair)[1])};
    }

    /** The pair's record name: {@code a*b}; a placebo pair {@code a*__noise_<r>}. */
    public String pairName(final int pair) {
        if (pair < pairs.size()) return String.join("*", pairFieldNames(pair));
        final int[] placebo = pairPlacebos.get(pair - pairs.size());
        return conditioningFields.get(placebo[0]) + "*" + NOISE_PREFIX + placebo[1];
    }

    /** The real pairs, then the placebo pairs ({@link #pairPlacebos}: a first member × a noise column). */
    public int pairCount() {
        return pairs.size() + pairPlacebos.size();
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
        final int[] placebo = pairPlacebos.get(pair - pairs.size());
        return new int[]{placebo[0], -1 - placebo[1]};
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
     * Whether the run needs the window quantile sketches (engine doc §2): the candidates' ({@link #needsCandidateSketches}),
     * or the pair members' for the 2-D grids of the interaction shape (DSL doc §8.7).
     */
    public boolean needsWindowQuantiles() {
        return needsCandidateSketches() || hasPairShape() || hasCategoricals();
    }

    /**
     * Whether the candidates' sketches are read: independent rows (no group) with a {@code rank} or {@code absdev}
     * transform, whose "within the unit" would be a single row, or the binned test's value edges.
     */
    public boolean needsCandidateSketches() {
        return windowTransforms() || (hasBinned() && EDGES_VALUE.equals(binsEdges));
    }

    /**
     * Whether rank / absdev are taken against the window's sketches: independent rows only — a grouped run keeps
     * them within the group even when the sketches are there for the value bins or a pair's grid.
     */
    public boolean windowTransforms() {
        return group == null && (transforms.contains(TRANSFORM_RANK) || transforms.contains(TRANSFORM_ABSDEV));
    }

    /**
     * Whether the report reads the candidates' window minima (the joint's ratio suggestions, DSL doc §9.5): the
     * sketch pre-pass then runs for the finalize step alone when the scoring passes do not need it.
     */
    public boolean needsJointMinima() {
        return jointOn && jointPairs > 0;
    }

    public boolean hasBinned() {
        return transforms.contains(TRANSFORM_BINNED);
    }

    public static boolean isBinned(final String transform) {
        return TRANSFORM_BINNED.equals(transform);
    }

    /** The statistic kind a transform's placebo threshold is pooled over. */
    public static String kind(final String transform) {
        return isBinned(transform) ? KIND_BINNED : TRANSFORM_PRODUCT.equals(transform) ? KIND_PAIR : TRANSFORM_LEVELS.equals(transform) ? KIND_LEVELS : KIND_DF1;
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
        // Math.max propagates a NaN threshold
        return minGain == null ? threshold : Math.max(threshold, minGain);
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
                    if (field != null) errors.add("heterogeneity.field is read with by: field only (by: periods takes the period buckets as the levels)");
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
                // {enabled} (absent / null = on); a non-boolean is an error, not a silent false or an unchecked exception
                final JsonElement enabled = suggestions.getAsJsonObject().get("enabled");
                if (enabled == null || enabled.isJsonNull()) {
                    s.suggestionsOn = true;
                } else if (enabled.isJsonPrimitive() && enabled.getAsJsonPrimitive().isBoolean()) {
                    s.suggestionsOn = enabled.getAsBoolean();
                } else {
                    errors.add("suggestions.enabled must be a boolean");
                }
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
                final JsonElement fields = o.get("fields");
                if (fields != null && !fields.isJsonNull() && !fields.isJsonArray()) {
                    errors.add("pairs.fields must be a list of [a, b] pairs of field names");
                } else if (fields != null && fields.isJsonArray()) {
                    for (final JsonElement e : fields.getAsJsonArray()) {
                        // both members field names (a null / object member would throw from getAsString)
                        if (!e.isJsonArray() || e.getAsJsonArray().size() != 2
                                || !e.getAsJsonArray().get(0).isJsonPrimitive() || !e.getAsJsonArray().get(1).isJsonPrimitive()) {
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
                final JsonElement shape = o.get("shape");
                if (shape != null && !shape.isJsonNull()) {
                    if (shape.isJsonPrimitive() && shape.getAsJsonPrimitive().isBoolean()) {
                        if (!shape.getAsBoolean()) s.pairShapeBins = 0;
                    } else if (shape.isJsonPrimitive() && shape.getAsJsonPrimitive().isNumber()) {
                        final double k = shape.getAsDouble();
                        // one bin per member has no edge to split at: no grid (and no shape) would come out of it
                        if (k < 0 || k == 1 || k > 20 || k != Math.rint(k)) errors.add("pairs.shape must be 0 (off) or an integer in [2, 20] (value bins per member of the 2-D grid)");
                        else s.pairShapeBins = (int) k;
                    } else {
                        errors.add("pairs.shape must be a boolean or the number of bins per member");
                    }
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
                final Double jp = number(o, "pairs");
                if (jp != null) {
                    if (jp < 0 || jp != Math.rint(jp)) errors.add("joint.pairs must be a non-negative integer");
                    else s.jointPairs = jp.intValue();
                }
                final Double excess = number(o, "excess");
                if (excess != null) {
                    if (!(excess >= 1)) errors.add("joint.excess must be at least 1");
                    else s.jointExcess = excess;
                }
            } else {
                errors.add("joint must be a boolean or an object {include, maxColumns, noise, directions, redundancy, select, pairs, excess}");
            }
        }
        final JsonElement categorical = p.get("categorical");
        if (categorical != null && !categorical.isJsonNull()) {
            if (categorical.isJsonObject()) {
                final JsonObject o = categorical.getAsJsonObject();
                s.categoricalInclude = strings(o, "include", errors);
                final Double maxLevels = number(o, "maxLevels");
                if (maxLevels != null) {
                    if (maxLevels < 2 || maxLevels > 1000 || maxLevels != Math.rint(maxLevels)) errors.add("categorical.maxLevels must be an integer in [2, 1000]");
                    else s.categoricalMaxLevels = maxLevels.intValue();
                }
                final Double placebo = number(o, "placebo");
                if (placebo != null) {
                    if (placebo < 0 || placebo != Math.rint(placebo)) errors.add("categorical.placebo must be a non-negative integer");
                    else s.categoricalPlacebo = placebo.intValue();
                }
                if (s.categoricalInclude.isEmpty()) errors.add("categorical needs include ([names / globs / selectors] of the string fields to test)");
            } else if (categorical.isJsonArray()) {
                s.categoricalInclude = strings(p, "categorical", errors);
            } else {
                errors.add("categorical must be an object {include, maxLevels, placebo} or a list of include globs");
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
                // a declared but malformed floor (a list, an object, a non-numeric string) is an error, never silently no floor
                final JsonElement minGain = o.get("minGain");
                if (minGain != null && !minGain.isJsonNull()) {
                    s.minGain = numeric(minGain);
                    if (s.minGain == null || !(s.minGain > 0 && Double.isFinite(s.minGain))) {
                        errors.add("pass.minGain must be a positive finite number (a floor on est_gain / partial_gain, the average log-likelihood improvement per unit)");
                    }
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
            if (windowTransforms()) {
                notes.add("rank / absdev of independent rows are taken against the window's quantile sketch (KLL k=" + SketchAccumulator.K + ", rank error about 0.8%, randomised compaction: beyond k values a re-run can shift them within that error; noise placebos use the exact normal cdf)");
            }
            if (hasShuffle()) errors.add("placebo.shuffle needs group (within-group permutation)");
            if (hasBinned() && EDGES_RANK.equals(binsEdges)) errors.add("bins.edges rank needs group (the position bins read the within-unit rank); use edges: value for independent rows");
            if (Family.FORM_INVERSE_SHARE.equals(baselineForm)) errors.add("baseline.form inverseShare needs group (the share is taken within the group)");
        }
        if (periodsBucket != null && periodsField == null) errors.add("periods needs a field (periods.field or time.field)");
        if (heterogeneityByPeriods() && periodsBucket == null) errors.add("heterogeneity: periods needs periods (the levels are the period buckets)");
        if (hasHeterogeneity() && transforms.stream().allMatch(ScreenSpec::isBinned)) {
            errors.add("heterogeneity needs a raw / rank / absdev transform (the binned block test has no direction to differ across the levels)");
        }
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
                if (!included(includes, includeSelectors, name, entry)) continue;
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
        pairPlacebos = new ArrayList<>();
        if (!pairFields.isEmpty() || !pairAmong.isEmpty()) {
            final int pairErrors = errors.size();
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
            // among matching a single field (and no declared pair) would silently test nothing
            if (pairs.isEmpty() && errors.size() == pairErrors) errors.add("pairs resolved to no pair: among needs at least two matching conditioning fields (" + conditioningFields + ")");
            if (pairs.size() > pairMaxPairs) errors.add("pairs: " + pairs.size() + " pairs exceed pairs.maxPairs " + pairMaxPairs + " (each costs 2 + k doubles per partial key, and its 2-D grid 2 K doubles — plus K² for groupedMultinomial — with K = pairs.shape²; declare fewer members or raise the bound)");
            // the placebo pairs: each pair's first member times a noise column it is not already paired with, spread
            // over the noise columns, so pairs sharing a member never repeat a placebo (an identical column would
            // duplicate a record name and a calibration sample); a member already paired with every noise column
            // brings no further placebo
            if (pairPlacebo <= noise) {
                final Set<Long> used = new HashSet<>();
                for (int i = 0; i < pairs.size(); i++) {
                    final int member = pairs.get(i)[0];
                    for (int j = 0; j < pairPlacebo; j++) {
                        for (int step = 0; step < noise; step++) {
                            final int r = (int) (((long) i * pairPlacebo + j + step) % noise);
                            if (used.add((long) member * noise + r)) {
                                pairPlacebos.add(new int[]{member, r});
                                break;
                            }
                        }
                    }
                }
            }
        }
        // joint columns: the candidates matching joint.include (every candidate by default), within the bound
        jointColumns = new ArrayList<>();
        if (jointOn) {
            final List<Pattern> globs = jointInclude.stream().filter(s -> !FeatureLineage.isSelector(s)).map(StatMath::glob).toList();
            final List<String> selectors = jointInclude.stream().filter(FeatureLineage::isSelector).toList();
            if (!selectors.isEmpty() && l.columns.isEmpty()) {
                errors.add("joint.include uses lineage selectors " + selectors + " but no lineage is available: "
                        + "put the feature transform directly upstream or set candidates.manifest to its manifest URI");
            }
            for (int c = 0; c < candidates.size(); c++) {
                final String name = candidates.get(c);
                if (jointInclude.isEmpty() || included(globs, selectors, name, l.columns.get(name))) jointColumns.add(c);
            }
            if (jointColumns.size() < 2) errors.add("joint needs at least two candidate columns (joint.include " + jointInclude + " kept " + jointColumns.size() + ")");
            if (jointColumns.size() > jointMaxColumns) errors.add("joint: " + jointColumns.size() + " columns exceed joint.maxColumns " + jointMaxColumns + " (the joint sums are m x m per bundle and O(m²) per row; narrow joint.include or raise the bound)");
        }
        // categorical candidates: the string fields matching categorical.include, never a role field
        categoricals = new ArrayList<>();
        if (!categoricalInclude.isEmpty() && inputSchema != null) {
            final List<Pattern> globs = categoricalInclude.stream().filter(s -> !FeatureLineage.isSelector(s)).map(StatMath::glob).toList();
            final List<String> selectors = categoricalInclude.stream().filter(FeatureLineage::isSelector).toList();
            for (final Schema.Field f : inputSchema.getFields()) {
                if (f.getFieldType().getType() != Schema.Type.string || reserved.contains(f.getName())) continue;
                final String name = f.getName();
                boolean in = globs.stream().anyMatch(g -> g.matcher(name).matches());
                if (!in && !selectors.isEmpty()) in = selectors.stream().anyMatch(s -> FeatureLineage.selectorMatches(s, l.columns.get(name)));
                if (in) categoricals.add(name);
            }
            if (categoricals.isEmpty()) errors.add("categorical.include " + categoricalInclude + " matched no string input field (role fields cannot be candidates)");
            if (categoricalPlacebo > 0 && noise == 0) notes.add("categorical placebos redraw the levels from the window frequencies; they need no noise column");
        }
        if (!errors.isEmpty()) throw new IllegalArgumentException(String.join("; ", errors));
        return this;
    }

    /** Whether a column matches an include list: one of its name globs, or one of its lineage selectors (no entry: none). */
    private static boolean included(final List<Pattern> globs, final List<String> selectors, final String name, final FeatureLineage.Entry entry) {
        return globs.stream().anyMatch(g -> g.matcher(name).matches()) || selectors.stream().anyMatch(s -> FeatureLineage.selectorMatches(s, entry));
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
