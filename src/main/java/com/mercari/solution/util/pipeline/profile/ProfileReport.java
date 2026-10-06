package com.mercari.solution.util.pipeline.profile;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.theta.CompactSketch;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The profile's results as a model and as records (profile-dsl.md §5): pure functions from the
 * first pass's {@link ProfileAccumulator} and the counting pass's {@link ProfileCells} to the
 * statistics, and from those to the output records. The report's payload is rendered from the
 * same model ({@link ProfileRenderer}), so the records and the report cannot disagree.
 */
public class ProfileReport {

    public static final int FORMAT_VERSION = 2;

    /** Group key of the whole dataset in the counting pass. */
    public static final String ALL_KEY = "\u001Eall";
    /** Group keys of the two target classes in the counting pass. */
    public static final String TARGET_POSITIVE_KEY = "\u001Etarget\u001Fpositive";
    public static final String TARGET_NEGATIVE_KEY = "\u001Etarget\u001Fnegative";

    public static final String NULL_BIN = "(null)";

    private static final double[] QUANTILE_RANKS = { 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99 };
    public static final String[] QUANTILE_NAMES = { "p01", "p05", "p25", "p50", "p75", "p95", "p99" };

    /** Notable codes in severity order (profile-dsl.md §5.2). */
    public static final List<String> NOTABLE_CODES = List.of(
            "all_null", "null_like_only", "constant", "high_null", "null_like_value", "dominant_value", "skewed", "unique_like");

    private static final Set<String> NULL_WORDS = Set.of("null", "none", "nan", "n/a", "na", "-");

    // ---- configuration ----

    /** Everything the report, the renderer and the stages share about one run's declarations. */
    public static class Config implements Serializable {
        public String title;
        public boolean showValues = true;
        public String jobName;
        public String moduleName;
        public List<String> inputNames = List.of();
        public String runId;
        public String dataset;
        public String partition;
        public String expandedParametersJson;   // JSON text (JsonObject is not Serializable)

        public List<ProfileAxis> axes = List.of();
        public List<String[]> comparePairs = List.of();
        public Map<String, double[]> declaredEdges = Map.of();   // field path → declared split points
        public int binsCount = 10;
        public String driftAxis;                 // axis id, or null: the inputs axis when there is one
        public Set<String> driftExclude = Set.of();
        public boolean outputBins = true;
        public boolean outputValues = true;
        public boolean countingPass = true;

        public String reportOutput;
        public String payloadOutput;
        public long embedLimitBytes = 25_000_000L;
        public int histogramBins = 256;
        public int timestampBins = 64;
        public int timeGroupLimit = 60;
        public int topKShow = 20;
        public int sampleRowsInPayload = 1_000;
        public int categoryLimit = 50;

        public boolean hasDeclaredEdges() {
            return declaredEdges != null && !declaredEdges.isEmpty();
        }

        /** The axis whose groups feed the drift columns of the default output (profile-dsl.md §6.3), or null. */
        public ProfileAxis driftAxis() {
            for(final ProfileAxis axis : axes) {
                if(driftAxis != null ? driftAxis.equals(axis.label()) : ProfileAxis.Kind.inputs.equals(axis.kind)) {
                    return axis;
                }
            }
            return null;
        }
    }

    // ---- model ----

    public static class ValueRow {
        public String value;
        public long count;
        public long lower;
        public long upper;
        public boolean exact;
    }

    public static class FieldResult {
        public int index;
        public String path;
        public String type;
        public String sourceType;
        public boolean isKey;
        public long rows;
        public long count;
        public long nulls;
        public long errors;
        public double nullRate;
        public Double distinct;
        public Double distinctLower;
        public Double distinctUpper;
        public boolean distinctExact;
        // numeric-like
        public Double min;
        public Double max;
        public Double sum;
        public Double mean;
        public Double stddev;
        public Double skewness;
        public Long zeros;
        public Long nans;
        public Long infs;
        public Double[] quantiles;
        public Double rankError;
        // string
        public Long empties;
        public Long blanks;
        public Double lengthMin;
        public Double lengthMax;
        public Double lengthP50;
        public Double lengthP95;
        public String top;
        public Double topShare;
        // bool
        public Long trues;
        public Long falses;
        public Long nullLike;
        public Double nullLikeRate;
        public List<String> notable = new ArrayList<>();
        // association with the target / drift against the reference of the drift axis
        public Double association;
        public String associationKind;
        public Double drift;
        public String driftKind;
        public String driftVs;
        public Double nullShift;

        // the field's frequent values: the exact table, the certain frequent items, or the candidates
        public List<ValueRow> values = new ArrayList<>();
        public String valuesKind;       // exact | frequent | candidates | null
        public Long valuesMaximumError;

        // cells of the counting pass (null without one, or when the field has none)
        public double[] cellEdges;      // numeric-like: cells + 1 edges from min to max
        public String[] cellLabels;     // categorical: one label per cell
        public long[] cells;            // whole-dataset counts per cell
        public int[] binOfCell;         // numeric-like without declared edges: cell → bin
        public Double[] binLower;       // per bin; null for an open end of declared edges
        public Double[] binUpper;
        public String[] binLabels;      // categorical bins
        public long[] bins;             // whole-dataset counts per bin
        public String edgesKind;        // quantile | declared | values

        public boolean numericLike() {
            return !"string".equals(type) && !"bool".equals(type);
        }
    }

    public static class GroupField {
        public long count;
        public long nulls;
        public Double nullRate;
        public Double min;
        public Double max;
        public Double sum;
        public Double mean;
        public Double stddev;
        public Double p50;
        public Double ks;
        public Double tvd;
        public Double psi;
        public Double nullShift;
        public Double noiseKs;
        public Double noisePsi;
        public long[] cells;
        public long[] bins;
    }

    public static class GroupResult {
        public String value;
        public boolean baseline;
        public long rows;
        public Long targetPositive;
        public Double targetRate;
        public GroupField[] fields;     // by field index; null entries for fields without cells
    }

    public static class AxisResult {
        public ProfileAxis axis;
        public String label;
        public long truncatedGroups;
        public List<GroupResult> groups = new ArrayList<>();
    }

    public static class TargetField {
        public int index;
        public long count;
        public long nullRows;
        public Double rateWhenNull;
        public Double ks;
        public Double tvd;
        public Double iv;
        public Double pointBiserial;
        public Double meanPositive;
        public Double meanNegative;
        public long[] positiveBins;
        public long[] negativeBins;
    }

    public static class TargetResult {
        public String field;
        public String positive;
        public long positiveRows;
        public long negativeRows;
        public long nullRows;
        public Double rate;
        public String warning;
        public GroupResult positiveGroup;
        public GroupResult negativeGroup;
        public List<TargetField> fields = new ArrayList<>();
    }

    public static class PairResult {
        public String a;
        public String b;
        public String error;
        public long countA;
        public long countB;
        public Double ks;
        public Double psi;
        public Double noiseKs;
        public Double noisePsi;
        public double[] edges;          // bins + 1 edges
        public long[] binsA;
        public long[] binsB;
    }

    public static class KeyResult {
        public String key;
        public Double distinct;
        public Double distinctLower;
        public Double distinctUpper;
        public Double keyness;
        public long nullKeys;
    }

    public static class Result {
        public Instant generatedAt;
        public long rows;
        public long errorRows;
        public List<FieldResult> fields = new ArrayList<>();
        public List<AxisResult> axes = new ArrayList<>();
        public TargetResult target;
        public List<PairResult> pairs = new ArrayList<>();
        public List<KeyResult> keys = new ArrayList<>();

        public Map<String, Long> notableCounts() {
            final Map<String, Long> counts = new LinkedHashMap<>();
            for(final String code : NOTABLE_CODES) {
                counts.put(code, 0L);
            }
            for(final FieldResult field : fields) {
                for(final String code : field.notable) {
                    counts.merge(code, 1L, Long::sum);
                }
            }
            return counts;
        }
    }

    // ---- null sentinels ----

    /**
     * Whether a string value is a null sentinel (profile-dsl.md §5.2): the empty string, a
     * whitespace-only value (full-width space included), or one of the words that stand for a
     * missing value. Reported, never converted — the profile still counts it as non-null.
     */
    public static boolean isNullLike(final String value) {
        if(value == null) {
            return false;
        }
        if(value.isBlank()) {
            return true;
        }
        final String trimmed = value.strip();
        return trimmed.length() <= 4 && NULL_WORDS.contains(trimmed.toLowerCase(Locale.ROOT));
    }

    // ---- statistics over exact counts (profile-dsl.md §6) ----

    public static long total(final long[] counts) {
        long total = 0;
        for(final long c : counts) {
            total += c;
        }
        return total;
    }

    /** Largest cumulative share difference over aligned ordered cells, or null when a side is empty. */
    public static Double ks(final long[] a, final long[] b) {
        final long totalA = total(a);
        final long totalB = total(b);
        if(totalA == 0 || totalB == 0) {
            return null;
        }
        double ks = 0;
        double cumA = 0;
        double cumB = 0;
        for(int i = 0; i < a.length; i++) {
            cumA += (double) a[i] / totalA;
            cumB += (double) b[i] / totalB;
            ks = Math.max(ks, Math.abs(cumA - cumB));
        }
        return ks;
    }

    /** Total variation distance (half the L1 distance of the shares), or null when a side is empty. */
    public static Double tvd(final long[] a, final long[] b) {
        final long totalA = total(a);
        final long totalB = total(b);
        if(totalA == 0 || totalB == 0) {
            return null;
        }
        double sum = 0;
        for(int i = 0; i < a.length; i++) {
            sum += Math.abs((double) a[i] / totalA - (double) b[i] / totalB);
        }
        return sum / 2;
    }

    /**
     * Population stability index over aligned bins with 0.5-per-bin smoothing; the information
     * value is the same function of the two classes' bins. Null when a side is empty.
     */
    public static Double psi(final long[] a, final long[] b) {
        final long totalA = total(a);
        final long totalB = total(b);
        if(totalA == 0 || totalB == 0) {
            return null;
        }
        final double k = a.length;
        double psi = 0;
        for(int i = 0; i < a.length; i++) {
            final double pa = (a[i] + 0.5) / (totalA + 0.5 * k);
            final double pb = (b[i] + 0.5) / (totalB + 0.5 * k);
            psi += (pa - pb) * Math.log(pa / pb);
        }
        return psi;
    }

    /** The size KS reaches between two random samples of one distribution (95%). */
    public static Double noiseKs(final long n1, final long n2) {
        return n1 <= 0 || n2 <= 0 ? null : 1.36 * Math.sqrt(1d / n1 + 1d / n2);
    }

    /** The expected PSI between two random samples of one distribution over {@code bins} bins. */
    public static Double noisePsi(final int bins, final long n1, final long n2) {
        return n1 <= 0 || n2 <= 0 || bins < 2 ? null : (bins - 1) * (1d / n1 + 1d / n2);
    }

    /**
     * Assigns ordered cells to at most {@code bins} bins of about equal count: a bin is closed at
     * the first cell where the cumulative count reaches its share of the total.
     */
    public static int[] mergeBins(final long[] cells, final int bins) {
        final int[] binOfCell = new int[cells.length];
        final long total = total(cells);
        if(total == 0 || bins <= 1) {
            return binOfCell;
        }
        int bin = 0;
        int target = 1;     // the next share of the total to reach, in units of total / bins
        long cumulative = 0;
        for(int i = 0; i < cells.length; i++) {
            binOfCell[i] = bin;
            cumulative += cells[i];
            if(target < bins && cumulative * bins >= (long) target * total) {
                // one heavy cell can pass several targets: they all close with it, and bins stay dense
                while(target < bins && cumulative * bins >= (long) target * total) {
                    target += 1;
                }
                bin += 1;
            }
        }
        return binOfCell;
    }

    /** Number of bins of a cell → bin assignment (a trailing bin no cell fell in does not count). */
    public static int binCount(final int[] binOfCell) {
        return binOfCell.length == 0 ? 0 : binOfCell[binOfCell.length - 1] + 1;
    }

    public static long[] toBins(final long[] cells, final int[] binOfCell) {
        final long[] bins = new long[binCount(binOfCell)];
        for(int i = 0; i < binOfCell.length && i < cells.length; i++) {
            bins[binOfCell[i]] += cells[i];
        }
        return bins;
    }

    /** The median read off ordered cells by linear interpolation within the cell that holds it. */
    public static Double median(final long[] cells, final double[] edges, final double min, final double max) {
        final long total = total(cells);
        if(total == 0) {
            return null;
        }
        final double rank = total / 2d;
        double cumulative = 0;
        for(int i = 0; i < cells.length; i++) {
            if(cells[i] > 0 && cumulative + cells[i] >= rank) {
                final double lower = Math.max(edges[i], min);
                final double upper = Math.min(edges[i + 1], max);
                final double t = (rank - cumulative) / cells[i];
                return upper > lower ? lower + t * (upper - lower) : lower;
            }
            cumulative += cells[i];
        }
        return max;
    }

    /** Wilson 95% interval of a rate. */
    public static double[] wilson(final long positives, final long n) {
        if(n <= 0) {
            return null;
        }
        final double z = 1.959964;
        final double p = (double) positives / n;
        final double denominator = 1 + z * z / n;
        final double centre = (p + z * z / (2 * n)) / denominator;
        final double half = z * Math.sqrt(p * (1 - p) / n + z * z / (4d * n * n)) / denominator;
        // at a rate of 0 or 1 the bound equals the rate up to rounding: never report an interval that excludes it
        return new double[] { Math.min(p, Math.max(0, centre - half)), Math.max(p, Math.min(1, centre + half)) };
    }

    private static long[] minus(final long[] all, final long[] part) {
        final long[] rest = new long[all.length];
        for(int i = 0; i < all.length; i++) {
            rest[i] = Math.max(0L, all[i] - (i < part.length ? part[i] : 0L));
        }
        return rest;
    }

    // ---- building the model ----

    /**
     * @param cells the counting pass's groups by key, or null when the run had no counting pass
     * @param groupTotals total distinct groups per axis id (the groups are bounded before the shuffle)
     */
    public static Result build(
            final ProfileAccumulator accumulator,
            final Map<String, ProfileCells> cells,
            final ProfileEdges edges,
            final Config config,
            final Map<String, Long> groupTotals) {

        final ProfileSpec spec = accumulator.getSpec();
        final Result result = new Result();
        result.generatedAt = Instant.now();
        result.rows = accumulator.getRowCount();
        result.errorRows = accumulator.getErrorCount();

        final ProfileCells all = cells == null ? null : cells.get(ALL_KEY);
        for(int i = 0; i < spec.getFields().size(); i++) {
            final FieldResult field = buildField(i, accumulator, config);
            if(all != null && edges != null) {
                attachCells(field, accumulator.getField(i), all, edges, config);
            }
            result.fields.add(field);
        }

        if(cells != null && edges != null && all != null) {
            for(final ProfileAxis axis : config.axes) {
                result.axes.add(buildAxis(axis, result, cells, all, config, groupTotals, spec));
            }
            if(spec.getTarget() != null) {
                result.target = buildTarget(accumulator, result, cells, all, spec);
            }
            for(int p = 0; p < edges.getPairCount(); p++) {
                result.pairs.add(buildPair(p, accumulator, all, edges, config));
            }
            annotateDrift(result, config);
        } else if(spec.getTarget() != null) {
            result.target = targetTotals(accumulator, spec);
        }

        for(final Integer keyIndex : spec.getKeyFieldIndices()) {
            result.keys.add(buildKey(keyIndex, accumulator));
        }
        return result;
    }

    private static FieldResult buildField(final int index, final ProfileAccumulator accumulator, final Config config) {
        final ProfileSpec spec = accumulator.getSpec();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();
        final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(index);
        final ProfileAccumulator.FieldAccumulator field = accumulator.getField(index);

        final FieldResult r = new FieldResult();
        r.index = index;
        r.path = fieldSpec.path;
        r.type = fieldSpec.profileType.name().toLowerCase(Locale.ROOT);
        if(ProfileSpec.ProfileType.ARRAY_LENGTH.equals(fieldSpec.profileType)) {
            r.type = "array";
        }
        r.sourceType = fieldSpec.sourceType;
        r.isKey = fieldSpec.isKey;
        r.rows = accumulator.getRowCount();
        r.count = field.count;
        r.nulls = field.nullCount;
        r.errors = field.errorCount;
        final long total = field.count + field.nullCount + field.errorCount + field.nanCount + field.infCount;
        r.nullRate = total == 0 ? 0d : (double) field.nullCount / total;

        final CpcSketch cpc = field.cpcResult(params);
        if(cpc != null && field.count > 0) {
            r.distinct = cpc.getEstimate();
            r.distinctLower = cpc.getLowerBound(2);
            r.distinctUpper = cpc.getUpperBound(2);
        }

        switch (fieldSpec.profileType) {
            case NUMERIC, ARRAY_LENGTH, TIMESTAMP -> {
                final boolean numeric = !ProfileSpec.ProfileType.TIMESTAMP.equals(fieldSpec.profileType);
                if(numeric) {
                    r.zeros = field.zeroCount;
                    r.nans = field.nanCount;
                    r.infs = field.infCount;
                }
                if(field.count > 0) {
                    r.min = field.min;
                    r.max = field.max;
                    if(numeric) {
                        r.sum = field.sum;
                        r.mean = field.mean;
                        final double n = field.count;
                        if(n > 1) {
                            r.stddev = Math.sqrt(field.m2 / (n - 1));
                            if(field.m2 > 0) {
                                r.skewness = Math.sqrt(n) * field.m3 / Math.pow(field.m2, 1.5);
                            }
                        }
                    }
                    final KllDoublesSketch kll = field.getKll();
                    if(kll != null && !kll.isEmpty()) {
                        r.quantiles = new Double[QUANTILE_RANKS.length];
                        for(int q = 0; q < QUANTILE_RANKS.length; q++) {
                            r.quantiles[q] = kll.getQuantile(QUANTILE_RANKS[q], QuantileSearchCriteria.INCLUSIVE);
                        }
                        r.rankError = kll.getNormalizedRankError(false);
                    }
                    final Map<Double, Long> table = field.getNumericValues();
                    if(table != null) {
                        r.distinct = (double) table.size();
                        r.distinctLower = r.distinct;
                        r.distinctUpper = r.distinct;
                        r.distinctExact = true;
                        r.valuesKind = "exact";
                        table.entrySet().stream()
                                .sorted((a, b) -> {
                                    final int c = Long.compare(b.getValue(), a.getValue());
                                    return c != 0 ? c : Double.compare(a.getKey(), b.getKey());
                                })
                                .forEach(e -> r.values.add(valueRow(canonicalNumber(e.getKey()), e.getValue(), e.getValue(), e.getValue(), true)));
                    }
                }
            }
            case STRING -> {
                r.empties = field.emptyCount;
                r.blanks = field.blankCount;
                r.nullLike = field.nullLikeCount;
                r.nullLikeRate = total == 0 ? 0d : (double) (field.nullCount + field.nullLikeCount) / total;
                final KllDoublesSketch lengthKll = field.getKll();
                if(lengthKll != null && !lengthKll.isEmpty()) {
                    r.lengthMin = lengthKll.getMinItem();
                    r.lengthMax = lengthKll.getMaxItem();
                    r.lengthP50 = lengthKll.getQuantile(0.5, QuantileSearchCriteria.INCLUSIVE);
                    r.lengthP95 = lengthKll.getQuantile(0.95, QuantileSearchCriteria.INCLUSIVE);
                }
                if(field.count > 0) {
                    buildStringValues(r, field, params);
                }
            }
            case BOOL -> {
                r.trues = field.trueCount;
                r.falses = field.falseCount;
                if(field.count > 0) {
                    r.valuesKind = "exact";
                    r.values.add(valueRow("true", field.trueCount, field.trueCount, field.trueCount, true));
                    r.values.add(valueRow("false", field.falseCount, field.falseCount, field.falseCount, true));
                    r.values.sort((a, b) -> Long.compare(b.count, a.count));
                }
            }
        }
        if(!r.values.isEmpty() && field.count > 0 && !"numeric".equals(r.type) && !"array".equals(r.type)) {
            r.top = config.showValues ? shorten(r.values.getFirst().value) : null;
            r.topShare = (double) r.values.getFirst().count / field.count;
        }
        // a non-finite number is not JSON and is no statistic: an overflowed sum or moment is reported as absent
        r.sum = finite(r.sum);
        r.mean = finite(r.mean);
        r.stddev = finite(r.stddev);
        r.skewness = finite(r.skewness);
        buildNotable(r, fieldSpec, field);
        return r;
    }

    private static Double finite(final Double value) {
        return value == null || Double.isNaN(value) || Double.isInfinite(value) ? null : value;
    }

    private static void buildStringValues(
            final FieldResult r,
            final ProfileAccumulator.FieldAccumulator field,
            final ProfileSpec.SketchParameters params) {

        final Map<String, Long> table = field.getStringValues();
        if(table != null) {
            r.distinct = (double) table.size();
            r.distinctLower = r.distinct;
            r.distinctUpper = r.distinct;
            r.distinctExact = true;
            r.valuesKind = "exact";
            table.entrySet().stream()
                    .sorted((a, b) -> {
                        final int c = Long.compare(b.getValue(), a.getValue());
                        return c != 0 ? c : a.getKey().compareTo(b.getKey());
                    })
                    .forEach(e -> r.values.add(valueRow(e.getKey(), e.getValue(), e.getValue(), e.getValue(), true)));
            return;
        }
        final ItemsSketch<String> fi = field.getFrequentItems();
        if(fi == null) {
            return;
        }
        r.valuesMaximumError = fi.getMaximumError();
        ItemsSketch.Row<String>[] rows = fi.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        r.valuesKind = "frequent";
        if(rows.length == 0) {
            // no value is frequent enough to be certain of: the candidates, with their bounds
            rows = fi.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
            r.valuesKind = "candidates";
        }
        for(int i = 0; i < rows.length && i < params.topKKeep; i++) {
            r.values.add(valueRow(rows[i].getItem(), rows[i].getEstimate(), rows[i].getLowerBound(), rows[i].getUpperBound(), false));
        }
        if(r.values.isEmpty()) {
            r.valuesKind = null;
        }
    }

    private static ValueRow valueRow(final String value, final long count, final long lower, final long upper, final boolean exact) {
        final ValueRow row = new ValueRow();
        row.value = value;
        row.count = count;
        row.lower = lower;
        row.upper = upper;
        row.exact = exact;
        return row;
    }

    /** A value as it is written out: at most 256 characters (the tables themselves hold values whole). */
    public static String shorten(final String value) {
        return value != null && value.length() > 256 ? value.substring(0, 256) : value;
    }

    public static String canonicalNumber(final double v) {
        if(v == Math.rint(v) && Math.abs(v) < 1e15) {
            return String.valueOf((long) v);
        }
        return String.valueOf(v);
    }

    private static void buildNotable(
            final FieldResult r,
            final ProfileSpec.FieldSpec fieldSpec,
            final ProfileAccumulator.FieldAccumulator field) {

        if(field.count == 0 && field.nullCount > 0) {
            r.notable.add("all_null");
            return;
        }
        if(field.count == 0) {
            return;
        }
        final boolean string = ProfileSpec.ProfileType.STRING.equals(fieldSpec.profileType);
        final boolean nullLikeOnly = string && field.nullLikeCount == field.count;
        if(nullLikeOnly) {
            r.notable.add("null_like_only");
        }
        final boolean constant = string
                ? r.distinct != null && r.distinct <= 1.5
                : !ProfileSpec.ProfileType.BOOL.equals(fieldSpec.profileType) && field.min == field.max;
        if(constant) {
            r.notable.add("constant");
        }
        if(r.nullRate > 0.5) {
            r.notable.add("high_null");
        }
        if(string && !nullLikeOnly && r.values.stream().anyMatch(v -> isNullLike(v.value))) {
            r.notable.add("null_like_value");
        }
        if(!constant && r.topShare != null && r.topShare > 0.9) {
            r.notable.add("dominant_value");
        }
        if(ProfileSpec.ProfileType.NUMERIC.equals(fieldSpec.profileType) && r.skewness != null && Math.abs(r.skewness) > 2) {
            r.notable.add("skewed");
        }
        // a continuous field is expected to be nearly unique: only strings and integers are worth the note
        final boolean discrete = string || (ProfileSpec.ProfileType.NUMERIC.equals(fieldSpec.profileType)
                && fieldSpec.sourceType != null && fieldSpec.sourceType.startsWith("int"));
        if(discrete && r.distinct != null && field.count > 100 && r.distinct >= 0.95 * field.count) {
            r.notable.add("unique_like");
        }
    }

    /** The whole-dataset cells and bins of a field (profile-dsl.md §6.1). */
    private static void attachCells(
            final FieldResult r,
            final ProfileAccumulator.FieldAccumulator field,
            final ProfileCells all,
            final ProfileEdges edges,
            final Config config) {

        final ProfileSpec.FieldSpec fieldSpec = fieldSpecOf(r);
        final int size = edges.cellCount(fieldSpec, r.index);
        if(size == 0) {
            return;
        }
        r.cells = all.cells(r.index, size);
        if(r.numericLike()) {
            final double[] splits = edges.getSplits(r.index);
            r.cellEdges = new double[splits.length + 2];
            r.cellEdges[0] = field.min;
            System.arraycopy(splits, 0, r.cellEdges, 1, splits.length);
            r.cellEdges[r.cellEdges.length - 1] = field.max;
            final double[] declared = edges.getDeclared(r.index);
            if(declared != null) {
                r.edgesKind = "declared";
                r.bins = all.declared(r.index, declared.length + 1);
                r.binLower = new Double[declared.length + 1];
                r.binUpper = new Double[declared.length + 1];
                for(int b = 0; b <= declared.length; b++) {
                    r.binLower[b] = b == 0 ? null : (Double) declared[b - 1];
                    r.binUpper[b] = b == declared.length ? null : (Double) declared[b];
                }
            } else {
                r.edgesKind = "quantile";
                r.binOfCell = mergeBins(r.cells, config.binsCount);
                r.bins = toBins(r.cells, r.binOfCell);
                r.binLower = new Double[r.bins.length];
                r.binUpper = new Double[r.bins.length];
                for(int c = 0; c < r.binOfCell.length; c++) {
                    final int b = r.binOfCell[c];
                    if(r.binLower[b] == null) {
                        r.binLower[b] = r.cellEdges[c];
                    }
                    r.binUpper[b] = r.cellEdges[c + 1];
                }
            }
        } else {
            r.edgesKind = "values";
            if("bool".equals(r.type)) {
                r.cellLabels = new String[] { "true", "false" };
            } else {
                final String[] categories = edges.getCategories(r.index);
                r.cellLabels = new String[categories.length + 1];
                System.arraycopy(categories, 0, r.cellLabels, 0, categories.length);
                r.cellLabels[categories.length] = ProfileEdges.OTHER;
            }
            r.binLabels = r.cellLabels;
            r.bins = r.cells;
        }
    }

    private static ProfileSpec.FieldSpec fieldSpecOf(final FieldResult r) {
        final ProfileSpec.ProfileType type = switch (r.type) {
            case "string" -> ProfileSpec.ProfileType.STRING;
            case "bool" -> ProfileSpec.ProfileType.BOOL;
            case "timestamp" -> ProfileSpec.ProfileType.TIMESTAMP;
            case "array" -> ProfileSpec.ProfileType.ARRAY_LENGTH;
            default -> ProfileSpec.ProfileType.NUMERIC;
        };
        return new ProfileSpec.FieldSpec(r.path, type, r.sourceType, null);
    }

    /** A group's counts of one field over the field's bins. */
    private static long[] groupBins(final FieldResult field, final ProfileCells group, final long[] groupCells) {
        if(!field.numericLike()) {
            return groupCells;
        }
        if("declared".equals(field.edgesKind)) {
            return group.declared(field.index, field.bins.length);
        }
        return toBins(groupCells, field.binOfCell);
    }

    private static GroupField groupField(final FieldResult field, final ProfileCells group) {
        final GroupField g = new GroupField();
        g.count = group.count(field.index);
        g.nulls = group.nulls(field.index);
        final long observed = g.count + g.nulls;
        g.nullRate = observed == 0 ? null : (Double) ((double) g.nulls / observed);
        g.cells = group.cells(field.index, field.cells.length);
        g.bins = groupBins(field, group, g.cells);
        if(field.numericLike() && group.hasNumeric(field.index)) {
            g.min = group.min(field.index);
            g.max = group.max(field.index);
            if(!"timestamp".equals(field.type)) {
                g.sum = group.sum(field.index);
                g.mean = group.mean(field.index);
                if(g.count > 1) {
                    g.stddev = Math.sqrt(group.m2(field.index) / (g.count - 1));
                }
            }
            g.p50 = median(g.cells, field.cellEdges, g.min, g.max);
        }
        return g;
    }

    /** The drift columns of a group field against a reference (profile-dsl.md §6.3). */
    private static void compare(
            final FieldResult field,
            final GroupField g,
            final long[] referenceCells,
            final long[] referenceBins,
            final long referenceCount,
            final long referenceNulls) {

        if(g.count > 0 && referenceCount > 0) {
            if(field.numericLike()) {
                g.ks = ks(g.cells, referenceCells);
            } else {
                g.tvd = tvd(g.cells, referenceCells);
            }
            g.psi = psi(g.bins, referenceBins);
            g.noiseKs = noiseKs(g.count, referenceCount);
            g.noisePsi = noisePsi(g.bins.length, g.count, referenceCount);
        }
        final long referenceObserved = referenceCount + referenceNulls;
        if(g.nullRate != null && referenceObserved > 0) {
            g.nullShift = g.nullRate - (double) referenceNulls / referenceObserved;
        }
    }

    private static GroupResult groupResult(
            final String value, final ProfileCells group, final Result result, final ProfileSpec spec) {

        final GroupResult g = new GroupResult();
        g.value = value;
        g.rows = group.getRows();
        if(spec.getTarget() != null && group.getTargetPositive() + group.getTargetNegative() > 0) {
            g.targetPositive = group.getTargetPositive();
            g.targetRate = (double) group.getTargetPositive() / (group.getTargetPositive() + group.getTargetNegative());
        }
        g.fields = new GroupField[result.fields.size()];
        for(final FieldResult field : result.fields) {
            if(field.cells != null) {
                g.fields[field.index] = groupField(field, group);
            }
        }
        return g;
    }

    private static AxisResult buildAxis(
            final ProfileAxis axis,
            final Result result,
            final Map<String, ProfileCells> cells,
            final ProfileCells all,
            final Config config,
            final Map<String, Long> groupTotals,
            final ProfileSpec spec) {

        final AxisResult axisResult = new AxisResult();
        axisResult.axis = axis;
        axisResult.label = axis.label();

        final List<Map.Entry<String, ProfileCells>> groups = new ArrayList<>();
        for(final Map.Entry<String, ProfileCells> entry : cells.entrySet()) {
            if(axis.groupOfKey(entry.getKey()) != null) {
                groups.add(entry);
            }
        }
        if(ProfileAxis.Kind.inputs.equals(axis.kind)) {
            groups.sort(java.util.Comparator.comparingInt(entry -> {
                final int i = axis.inputNames.indexOf(axis.groupOfKey(entry.getKey()));
                return i < 0 ? Integer.MAX_VALUE : i;
            }));
        } else if(ProfileAxis.Kind.time.equals(axis.kind)) {
            groups.sort(Map.Entry.comparingByKey());
            if(groups.size() > config.timeGroupLimit) {
                groups.subList(0, groups.size() - config.timeGroupLimit).clear();
            }
        } else {
            groups.sort((a, b) -> {
                final int c = Long.compare(b.getValue().getRows(), a.getValue().getRows());
                return c != 0 ? c : a.getKey().compareTo(b.getKey());
            });
            if(groups.size() > axis.topK) {
                groups.subList(axis.topK, groups.size()).clear();
            }
        }
        final long totalGroups = groupTotals != null && groupTotals.containsKey(axis.id())
                ? groupTotals.get(axis.id()) : groups.size();
        axisResult.truncatedGroups = Math.max(0L, totalGroups - groups.size());

        ProfileCells baseline = null;
        for(final Map.Entry<String, ProfileCells> entry : groups) {
            final String value = axis.groupOfKey(entry.getKey());
            final GroupResult g = groupResult(value, entry.getValue(), result, spec);
            g.baseline = axis.baseline != null && axis.baseline.equals(value);
            if(g.baseline) {
                baseline = entry.getValue();
            }
            axisResult.groups.add(g);
        }
        // the reference: the baseline group of the inputs axis, the rows outside the group elsewhere
        for(final GroupResult g : axisResult.groups) {
            if(g.baseline) {
                continue;
            }
            for(final FieldResult field : result.fields) {
                final GroupField gf = g.fields[field.index];
                if(gf == null) {
                    continue;
                }
                if(ProfileAxis.Kind.inputs.equals(axis.kind)) {
                    if(baseline == null) {
                        continue;
                    }
                    final long[] referenceCells = baseline.cells(field.index, field.cells.length);
                    compare(field, gf, referenceCells, groupBins(field, baseline, referenceCells),
                            baseline.count(field.index), baseline.nulls(field.index));
                } else {
                    final long[] restCells = minus(field.cells, gf.cells);
                    final long[] restBins = minus(field.bins, gf.bins);
                    compare(field, gf, restCells, restBins,
                            Math.max(0L, all.count(field.index) - gf.count),
                            Math.max(0L, all.nulls(field.index) - gf.nulls));
                }
            }
        }
        return axisResult;
    }

    /** The class totals alone, when the run had no counting pass. */
    private static TargetResult targetTotals(final ProfileAccumulator accumulator, final ProfileSpec spec) {
        final ProfileSpec.TargetSpec target = spec.getTarget();
        final TargetResult t = new TargetResult();
        t.field = target.path;
        t.positive = target.positiveLabel();
        t.positiveRows = accumulator.getTargetPositiveRows();
        t.negativeRows = accumulator.getTargetNegativeRows();
        t.nullRows = accumulator.getTargetNullRows();
        t.rate = accumulator.getTargetRate();
        t.warning = targetWarning(accumulator, target);
        return t;
    }

    private static TargetResult buildTarget(
            final ProfileAccumulator accumulator,
            final Result result,
            final Map<String, ProfileCells> cells,
            final ProfileCells all,
            final ProfileSpec spec) {

        final TargetResult t = targetTotals(accumulator, spec);
        final ProfileCells positive = cells.get(TARGET_POSITIVE_KEY);
        final ProfileCells negative = cells.get(TARGET_NEGATIVE_KEY);
        if(positive == null || negative == null) {
            return t;
        }
        t.positiveGroup = groupResult("positive", positive, result, spec);
        t.negativeGroup = groupResult("negative", negative, result, spec);
        final int targetIndex = spec.getTarget().fieldIndex;
        for(final FieldResult field : result.fields) {
            if(field.index == targetIndex || field.cells == null) {
                continue;
            }
            final GroupField p = t.positiveGroup.fields[field.index];
            final GroupField q = t.negativeGroup.fields[field.index];
            final TargetField f = new TargetField();
            f.index = field.index;
            f.count = p.count + q.count;
            f.nullRows = p.nulls + q.nulls;
            f.rateWhenNull = f.nullRows == 0 ? null : (Double) ((double) p.nulls / f.nullRows);
            f.positiveBins = p.bins;
            f.negativeBins = q.bins;
            if(p.count > 0 && q.count > 0) {
                if(field.numericLike()) {
                    f.ks = ks(p.cells, q.cells);
                } else {
                    f.tvd = tvd(p.cells, q.cells);
                }
                f.iv = psi(p.bins, q.bins);
                if(p.mean != null && q.mean != null) {
                    f.meanPositive = p.mean;
                    f.meanNegative = q.mean;
                    // point-biserial correlation: standardized mean difference between the classes
                    final double n = p.count + q.count;
                    final double mean = (p.count * p.mean + q.count * q.mean) / n;
                    final double m2 = positive.m2(field.index) + negative.m2(field.index)
                            + p.count * (p.mean - mean) * (p.mean - mean) + q.count * (q.mean - mean) * (q.mean - mean);
                    final double sd = Math.sqrt(m2 / n);
                    if(sd > 0 && Double.isFinite(sd)) {
                        final double r = (p.mean - q.mean) / sd * Math.sqrt((p.count / n) * (q.count / n));
                        if(Double.isFinite(r)) {
                            f.pointBiserial = r;
                        }
                    }
                }
                field.association = field.numericLike() ? f.ks : f.tvd;
                field.associationKind = field.numericLike() ? "ks" : "tvd";
            }
            t.fields.add(f);
        }
        return t;
    }

    /**
     * A one-class target is almost always a wrong {@code positive} value (case, quoting, type):
     * the per-field statistics need both classes, so say so instead of reporting an empty analysis.
     */
    public static String targetWarning(final ProfileAccumulator accumulator, final ProfileSpec.TargetSpec target) {
        if(accumulator.getRowCount() == 0) {
            return null;
        }
        if(accumulator.getTargetPositiveRows() + accumulator.getTargetNegativeRows() == 0) {
            return "every row has a null or unreadable " + target.path + " value: no target analysis was possible";
        }
        if(accumulator.getTargetPositiveRows() == 0) {
            return "no row matched the positive value `" + target.positiveLabel() + "` of " + target.path
                    + " — check parameters.target.positive (case, quoting); every row was classed negative and the per-field statistics are skipped";
        }
        if(accumulator.getTargetNegativeRows() == 0) {
            return "every row matched the positive value `" + target.positiveLabel() + "` of " + target.path
                    + " — there is no negative class to compare against and the per-field statistics are skipped";
        }
        return null;
    }

    private static PairResult buildPair(
            final int pair,
            final ProfileAccumulator accumulator,
            final ProfileCells all,
            final ProfileEdges edges,
            final Config config) {

        final PairResult r = new PairResult();
        r.a = config.comparePairs.get(pair)[0];
        r.b = config.comparePairs.get(pair)[1];
        final double[] splits = edges.getPairSplits(pair);
        final int[] members = edges.getPairFields(pair);
        if(splits == null || members[0] < 0 || members[1] < 0) {
            r.error = "both fields need numeric observations to compare";
            return r;
        }
        final long[] cellsA = all.pairA(pair, splits.length + 1);
        final long[] cellsB = all.pairB(pair, splits.length + 1);
        r.countA = total(cellsA);
        r.countB = total(cellsB);
        if(r.countA == 0 || r.countB == 0) {
            r.error = "both fields need numeric observations to compare";
            return r;
        }
        final long[] pooled = new long[cellsA.length];
        for(int i = 0; i < pooled.length; i++) {
            pooled[i] = cellsA[i] + cellsB[i];
        }
        final int[] binOfCell = mergeBins(pooled, config.binsCount);
        r.binsA = toBins(cellsA, binOfCell);
        r.binsB = toBins(cellsB, binOfCell);
        r.ks = ks(cellsA, cellsB);
        r.psi = psi(r.binsA, r.binsB);
        r.noiseKs = noiseKs(r.countA, r.countB);
        r.noisePsi = noisePsi(r.binsA.length, r.countA, r.countB);
        final ProfileAccumulator.FieldAccumulator fieldA = accumulator.getField(members[0]);
        final ProfileAccumulator.FieldAccumulator fieldB = accumulator.getField(members[1]);
        final double min = Math.min(fieldA.min, fieldB.min);
        final double max = Math.max(fieldA.max, fieldB.max);
        r.edges = new double[r.binsA.length + 1];
        r.edges[0] = min;
        for(int c = 0; c < binOfCell.length; c++) {
            r.edges[binOfCell[c] + 1] = c < splits.length ? splits[c] : max;
        }
        return r;
    }

    /** The drift columns of the default output: the largest KS / TVD over the groups of the drift axis. */
    private static void annotateDrift(final Result result, final Config config) {
        final ProfileAxis driftAxis = config.driftAxis();
        if(driftAxis == null) {
            return;
        }
        for(final AxisResult axis : result.axes) {
            if(axis.axis != driftAxis) {
                continue;
            }
            for(final FieldResult field : result.fields) {
                if(config.driftExclude.contains(field.path)) {
                    continue;
                }
                for(final GroupResult group : axis.groups) {
                    final GroupField gf = group.fields[field.index];
                    if(gf == null || group.baseline) {
                        continue;
                    }
                    final Double drift = field.numericLike() ? gf.ks : gf.tvd;
                    if(drift != null && (field.drift == null || drift > field.drift)) {
                        field.drift = drift;
                        field.driftKind = field.numericLike() ? "ks" : "tvd";
                        field.driftVs = group.value;
                    }
                    if(gf.nullShift != null && (field.nullShift == null || Math.abs(gf.nullShift) > Math.abs(field.nullShift))) {
                        field.nullShift = gf.nullShift;
                    }
                }
            }
        }
    }

    private static KeyResult buildKey(final int index, final ProfileAccumulator accumulator) {
        final ProfileSpec spec = accumulator.getSpec();
        final ProfileAccumulator.FieldAccumulator field = accumulator.getField(index);
        final KeyResult key = new KeyResult();
        key.key = spec.getFields().get(index).path;
        key.nullKeys = field.nullCount;
        final CompactSketch theta = field.thetaResult(spec.getSketchParameters());
        if(theta != null && field.count > 0) {
            key.distinct = theta.getEstimate();
            key.distinctLower = theta.getLowerBound(2);
            key.distinctUpper = theta.getUpperBound(2);
            // an estimate above the number of keyed rows is sketch error, not more keys than rows
            key.keyness = Math.min(1d, theta.getEstimate() / field.count);
        }
        return key;
    }

    // ---- record schemas ----

    private static Schema.Builder identity() {
        return Schema.builder()
                .withField("runId", Schema.FieldType.STRING)
                .withField("dataset", Schema.FieldType.STRING)
                .withField("partition", Schema.FieldType.STRING)
                .withField("generatedAt", Schema.FieldType.TIMESTAMP)
                .withField("formatVersion", Schema.FieldType.INT64);
    }

    public static Schema fieldsSchema() {
        final Schema.Builder b = identity()
                .withField("field", Schema.FieldType.STRING)
                .withField("type", Schema.FieldType.STRING)
                .withField("sourceType", Schema.FieldType.STRING)
                .withField("rows", Schema.FieldType.INT64)
                .withField("count", Schema.FieldType.INT64)
                .withField("nulls", Schema.FieldType.INT64)
                .withField("nullRate", Schema.FieldType.FLOAT64)
                .withField("errors", Schema.FieldType.INT64)
                .withField("distinct", Schema.FieldType.FLOAT64)
                .withField("distinct_lo", Schema.FieldType.FLOAT64)
                .withField("distinct_hi", Schema.FieldType.FLOAT64)
                .withField("distinctExact", Schema.FieldType.BOOLEAN)
                .withField("min", Schema.FieldType.FLOAT64)
                .withField("max", Schema.FieldType.FLOAT64)
                .withField("minTime", Schema.FieldType.TIMESTAMP)
                .withField("maxTime", Schema.FieldType.TIMESTAMP)
                .withField("sum", Schema.FieldType.FLOAT64)
                .withField("mean", Schema.FieldType.FLOAT64)
                .withField("stddev", Schema.FieldType.FLOAT64)
                .withField("skewness", Schema.FieldType.FLOAT64)
                .withField("zeros", Schema.FieldType.INT64)
                .withField("nans", Schema.FieldType.INT64)
                .withField("infs", Schema.FieldType.INT64);
        for(final String name : QUANTILE_NAMES) {
            b.withField(name, Schema.FieldType.FLOAT64);
        }
        return b
                .withField("rankError", Schema.FieldType.FLOAT64)
                .withField("empties", Schema.FieldType.INT64)
                .withField("blanks", Schema.FieldType.INT64)
                .withField("lengthMin", Schema.FieldType.FLOAT64)
                .withField("lengthMax", Schema.FieldType.FLOAT64)
                .withField("lengthP50", Schema.FieldType.FLOAT64)
                .withField("top", Schema.FieldType.STRING)
                .withField("topShare", Schema.FieldType.FLOAT64)
                .withField("trues", Schema.FieldType.INT64)
                .withField("falses", Schema.FieldType.INT64)
                .withField("nullLike", Schema.FieldType.INT64)
                .withField("nullLikeRate", Schema.FieldType.FLOAT64)
                .withField("notable", Schema.FieldType.array(Schema.FieldType.STRING))
                .withField("association", Schema.FieldType.FLOAT64)
                .withField("associationKind", Schema.FieldType.STRING)
                .withField("drift", Schema.FieldType.FLOAT64)
                .withField("driftKind", Schema.FieldType.STRING)
                .withField("driftVs", Schema.FieldType.STRING)
                .withField("nullShift", Schema.FieldType.FLOAT64)
                .build();
    }

    public static Schema groupsSchema() {
        return identity()
                .withField("axis", Schema.FieldType.STRING)
                .withField("group", Schema.FieldType.STRING)
                .withField("baseline", Schema.FieldType.BOOLEAN)
                .withField("field", Schema.FieldType.STRING)
                .withField("groupRows", Schema.FieldType.INT64)
                .withField("count", Schema.FieldType.INT64)
                .withField("nulls", Schema.FieldType.INT64)
                .withField("nullRate", Schema.FieldType.FLOAT64)
                .withField("min", Schema.FieldType.FLOAT64)
                .withField("max", Schema.FieldType.FLOAT64)
                .withField("sum", Schema.FieldType.FLOAT64)
                .withField("mean", Schema.FieldType.FLOAT64)
                .withField("stddev", Schema.FieldType.FLOAT64)
                .withField("p50", Schema.FieldType.FLOAT64)
                .withField("ks", Schema.FieldType.FLOAT64)
                .withField("tvd", Schema.FieldType.FLOAT64)
                .withField("psi", Schema.FieldType.FLOAT64)
                .withField("nullShift", Schema.FieldType.FLOAT64)
                .withField("noiseKs", Schema.FieldType.FLOAT64)
                .withField("noisePsi", Schema.FieldType.FLOAT64)
                .build();
    }

    public static Schema valuesSchema() {
        return identity()
                .withField("field", Schema.FieldType.STRING)
                .withField("value", Schema.FieldType.STRING)
                .withField("rank", Schema.FieldType.INT64)
                .withField("count", Schema.FieldType.INT64)
                .withField("count_lo", Schema.FieldType.INT64)
                .withField("count_hi", Schema.FieldType.INT64)
                .withField("share", Schema.FieldType.FLOAT64)
                .withField("exact", Schema.FieldType.BOOLEAN)
                .build();
    }

    public static Schema binsSchema() {
        return identity()
                .withField("field", Schema.FieldType.STRING)
                .withField("axis", Schema.FieldType.STRING)
                .withField("group", Schema.FieldType.STRING)
                .withField("bin", Schema.FieldType.INT64)
                .withField("lower", Schema.FieldType.FLOAT64)
                .withField("upper", Schema.FieldType.FLOAT64)
                .withField("value", Schema.FieldType.STRING)
                .withField("count", Schema.FieldType.INT64)
                .withField("share", Schema.FieldType.FLOAT64)
                .withField("positives", Schema.FieldType.INT64)
                .withField("rate", Schema.FieldType.FLOAT64)
                .withField("rate_lo", Schema.FieldType.FLOAT64)
                .withField("rate_hi", Schema.FieldType.FLOAT64)
                .withField("edgesKind", Schema.FieldType.STRING)
                .withField("exact", Schema.FieldType.BOOLEAN)
                .build();
    }

    public static Schema targetSchema() {
        return identity()
                .withField("field", Schema.FieldType.STRING)
                .withField("type", Schema.FieldType.STRING)
                .withField("count", Schema.FieldType.INT64)
                .withField("ks", Schema.FieldType.FLOAT64)
                .withField("tvd", Schema.FieldType.FLOAT64)
                .withField("iv", Schema.FieldType.FLOAT64)
                .withField("pointBiserial", Schema.FieldType.FLOAT64)
                .withField("meanPositive", Schema.FieldType.FLOAT64)
                .withField("meanNegative", Schema.FieldType.FLOAT64)
                .withField("rateWhenNull", Schema.FieldType.FLOAT64)
                .withField("nullRows", Schema.FieldType.INT64)
                .build();
    }

    public static Schema pairsSchema() {
        return identity()
                .withField("a", Schema.FieldType.STRING)
                .withField("b", Schema.FieldType.STRING)
                .withField("countA", Schema.FieldType.INT64)
                .withField("countB", Schema.FieldType.INT64)
                .withField("ks", Schema.FieldType.FLOAT64)
                .withField("psi", Schema.FieldType.FLOAT64)
                .withField("noiseKs", Schema.FieldType.FLOAT64)
                .withField("noisePsi", Schema.FieldType.FLOAT64)
                .build();
    }

    public static Schema keysSchema() {
        return identity()
                .withField("kind", Schema.FieldType.STRING)
                .withField("key", Schema.FieldType.STRING)
                .withField("distinct", Schema.FieldType.FLOAT64)
                .withField("distinct_lo", Schema.FieldType.FLOAT64)
                .withField("distinct_hi", Schema.FieldType.FLOAT64)
                .withField("keyness", Schema.FieldType.FLOAT64)
                .withField("nullKeys", Schema.FieldType.INT64)
                .build();
    }

    public static Schema summarySchema() {
        final Schema.Builder b = identity()
                .withField("rows", Schema.FieldType.INT64)
                .withField("errorRows", Schema.FieldType.INT64)
                .withField("fields", Schema.FieldType.INT64)
                .withField("report", Schema.FieldType.STRING)
                .withField("reportBytes", Schema.FieldType.INT64)
                .withField("payload", Schema.FieldType.STRING)
                .withField("degradations", Schema.FieldType.array(Schema.FieldType.STRING));
        for(final String code : NOTABLE_CODES) {
            b.withField(notableColumn(code), Schema.FieldType.INT64);
        }
        return b
                .withField("targetRate", Schema.FieldType.FLOAT64)
                .withField("targetWarning", Schema.FieldType.STRING)
                .build();
    }

    /** {@code all_null} → {@code allNull}: the summary's count column of a notable code. */
    public static String notableColumn(final String code) {
        final StringBuilder sb = new StringBuilder();
        boolean upper = false;
        for(final char c : code.toCharArray()) {
            if(c == '_') {
                upper = true;
            } else {
                sb.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return sb.toString();
    }

    // ---- records ----

    private static MElement.Builder record(final Config config, final Result result) {
        return MElement.builder()
                .withString("runId", config.runId)
                .withString("dataset", config.dataset)
                .withString("partition", config.partition)
                .withTimestamp("generatedAt", result.generatedAt)
                .withInt64("formatVersion", (long) FORMAT_VERSION);
    }

    private static Instant instantOf(final Double epochMillis) {
        return epochMillis == null ? null : Instant.ofEpochMilli(epochMillis.longValue());
    }

    public static List<MElement> fieldRecords(final Result result, final Config config) {
        final List<MElement> records = new ArrayList<>();
        for(final FieldResult f : result.fields) {
            final boolean time = "timestamp".equals(f.type);
            final MElement.Builder b = record(config, result)
                    .withString("field", f.path)
                    .withString("type", f.type)
                    .withString("sourceType", f.sourceType)
                    .withInt64("rows", f.rows)
                    .withInt64("count", f.count)
                    .withInt64("nulls", f.nulls)
                    .withFloat64("nullRate", f.nullRate)
                    .withInt64("errors", f.errors)
                    .withFloat64("distinct", f.distinct)
                    .withFloat64("distinct_lo", f.distinctLower)
                    .withFloat64("distinct_hi", f.distinctUpper)
                    .withBool("distinctExact", f.distinctExact)
                    .withFloat64("min", time ? null : f.min)
                    .withFloat64("max", time ? null : f.max)
                    .withTimestamp("minTime", time ? instantOf(f.min) : null)
                    .withTimestamp("maxTime", time ? instantOf(f.max) : null)
                    .withFloat64("sum", f.sum)
                    .withFloat64("mean", f.mean)
                    .withFloat64("stddev", f.stddev)
                    .withFloat64("skewness", f.skewness)
                    .withInt64("zeros", f.zeros)
                    .withInt64("nans", f.nans)
                    .withInt64("infs", f.infs);
            for(int q = 0; q < QUANTILE_NAMES.length; q++) {
                b.withFloat64(QUANTILE_NAMES[q], f.quantiles == null || time ? null : f.quantiles[q]);
            }
            records.add(b
                    .withFloat64("rankError", f.rankError)
                    .withInt64("empties", f.empties)
                    .withInt64("blanks", f.blanks)
                    .withFloat64("lengthMin", f.lengthMin)
                    .withFloat64("lengthMax", f.lengthMax)
                    .withFloat64("lengthP50", f.lengthP50)
                    .withString("top", f.top)
                    .withFloat64("topShare", f.topShare)
                    .withInt64("trues", f.trues)
                    .withInt64("falses", f.falses)
                    .withInt64("nullLike", f.nullLike)
                    .withFloat64("nullLikeRate", f.nullLikeRate)
                    .withStringList("notable", new ArrayList<>(f.notable))
                    .withFloat64("association", f.association)
                    .withString("associationKind", f.associationKind)
                    .withFloat64("drift", f.drift)
                    .withString("driftKind", f.driftKind)
                    .withString("driftVs", f.driftVs)
                    .withFloat64("nullShift", f.nullShift)
                    .build());
        }
        return records;
    }

    /** With {@code values: hide}, segment group labels are raw values; time buckets and input names are not. */
    public static String groupLabel(final ProfileAxis axis, final String value, final int index, final boolean showValues) {
        if(showValues || !ProfileAxis.Kind.segments.equals(axis.kind) || ProfileAxis.NULL_GROUP.equals(value)) {
            return value;
        }
        return "group #" + (index + 1);
    }

    public static List<MElement> groupRecords(final Result result, final Config config) {
        final List<MElement> records = new ArrayList<>();
        for(final AxisResult axis : result.axes) {
            for(int g = 0; g < axis.groups.size(); g++) {
                final GroupResult group = axis.groups.get(g);
                final String label = groupLabel(axis.axis, group.value, g, config.showValues);
                for(final FieldResult field : result.fields) {
                    final GroupField gf = group.fields[field.index];
                    if(gf == null) {
                        continue;
                    }
                    final boolean time = "timestamp".equals(field.type);
                    records.add(record(config, result)
                            .withString("axis", axis.label)
                            .withString("group", label)
                            .withBool("baseline", group.baseline)
                            .withString("field", field.path)
                            .withInt64("groupRows", group.rows)
                            .withInt64("count", gf.count)
                            .withInt64("nulls", gf.nulls)
                            .withFloat64("nullRate", gf.nullRate)
                            .withFloat64("min", time ? null : gf.min)
                            .withFloat64("max", time ? null : gf.max)
                            .withFloat64("sum", gf.sum)
                            .withFloat64("mean", gf.mean)
                            .withFloat64("stddev", gf.stddev)
                            .withFloat64("p50", time ? null : gf.p50)
                            .withFloat64("ks", gf.ks)
                            .withFloat64("tvd", gf.tvd)
                            .withFloat64("psi", gf.psi)
                            .withFloat64("nullShift", gf.nullShift)
                            .withFloat64("noiseKs", gf.noiseKs)
                            .withFloat64("noisePsi", gf.noisePsi)
                            .build());
                }
            }
        }
        return records;
    }

    public static List<MElement> valueRecords(final Result result, final Config config) {
        final List<MElement> records = new ArrayList<>();
        for(final FieldResult field : result.fields) {
            final boolean raw = !"bool".equals(field.type);
            for(int i = 0; i < field.values.size(); i++) {
                final ValueRow value = field.values.get(i);
                records.add(record(config, result)
                        .withString("field", field.path)
                        .withString("value", raw && !config.showValues ? "#" + (i + 1) : shorten(value.value))
                        .withInt64("rank", (long) (i + 1))
                        .withInt64("count", value.count)
                        .withInt64("count_lo", value.lower)
                        .withInt64("count_hi", value.upper)
                        .withFloat64("share", field.count == 0 ? null : (Double) ((double) value.count / field.count))
                        .withBool("exact", value.exact)
                        .build());
            }
        }
        return records;
    }

    /** The label of a categorical bin as it is written out. */
    public static String binLabel(final FieldResult field, final int bin, final boolean showValues) {
        final String label = field.binLabels[bin];
        if(showValues || "bool".equals(field.type) || ProfileEdges.OTHER.equals(label)) {
            return shorten(label);
        }
        return "#" + (bin + 1);
    }

    private static void binRecords(
            final List<MElement> records,
            final Result result,
            final Config config,
            final FieldResult field,
            final String axis,
            final String group,
            final long[] bins,
            final long nulls,
            final long[] positives,
            final long[] negatives) {

        final long total = total(bins);
        for(int b = 0; b < bins.length; b++) {
            final MElement.Builder record = record(config, result)
                    .withString("field", field.path)
                    .withString("axis", axis)
                    .withString("group", group)
                    .withInt64("bin", (long) b)
                    .withFloat64("lower", field.binLower == null ? null : field.binLower[b])
                    .withFloat64("upper", field.binUpper == null ? null : field.binUpper[b])
                    .withString("value", field.binLabels == null ? null : binLabel(field, b, config.showValues))
                    .withInt64("count", bins[b])
                    .withFloat64("share", total == 0 ? null : (Double) ((double) bins[b] / total))
                    .withString("edgesKind", field.edgesKind)
                    .withBool("exact", true);
            if(positives != null) {
                final long classed = positives[b] + negatives[b];
                final double[] interval = wilson(positives[b], classed);
                record.withInt64("positives", positives[b])
                        .withFloat64("rate", classed == 0 ? null : (Double) ((double) positives[b] / classed))
                        .withFloat64("rate_lo", interval == null ? null : (Double) interval[0])
                        .withFloat64("rate_hi", interval == null ? null : (Double) interval[1]);
            } else {
                record.withInt64("positives", null)
                        .withFloat64("rate", null)
                        .withFloat64("rate_lo", null)
                        .withFloat64("rate_hi", null);
            }
            records.add(record.build());
        }
        if(nulls > 0) {
            records.add(record(config, result)
                    .withString("field", field.path)
                    .withString("axis", axis)
                    .withString("group", group)
                    .withInt64("bin", (long) bins.length)
                    .withFloat64("lower", null)
                    .withFloat64("upper", null)
                    .withString("value", NULL_BIN)
                    .withInt64("count", nulls)
                    .withFloat64("share", null)
                    .withInt64("positives", null)
                    .withFloat64("rate", null)
                    .withFloat64("rate_lo", null)
                    .withFloat64("rate_hi", null)
                    .withString("edgesKind", field.edgesKind)
                    .withBool("exact", true)
                    .build());
        }
    }

    public static List<MElement> binRecords(final Result result, final Config config) {
        final List<MElement> records = new ArrayList<>();
        final Map<Integer, TargetField> targetFields = new LinkedHashMap<>();
        if(result.target != null) {
            for(final TargetField targetField : result.target.fields) {
                targetFields.put(targetField.index, targetField);
            }
        }
        for(final FieldResult field : result.fields) {
            if(field.bins == null) {
                continue;
            }
            final TargetField targetField = targetFields.get(field.index);
            binRecords(records, result, config, field, null, null, field.bins, field.nulls,
                    targetField == null ? null : targetField.positiveBins,
                    targetField == null ? null : targetField.negativeBins);
            if(result.target != null && result.target.positiveGroup != null) {
                for(final GroupResult classGroup : List.of(result.target.positiveGroup, result.target.negativeGroup)) {
                    final GroupField gf = classGroup.fields[field.index];
                    if(gf != null) {
                        binRecords(records, result, config, field, "target", classGroup.value, gf.bins, gf.nulls, null, null);
                    }
                }
            }
            for(final AxisResult axis : result.axes) {
                for(int g = 0; g < axis.groups.size(); g++) {
                    final GroupField gf = axis.groups.get(g).fields[field.index];
                    if(gf != null) {
                        binRecords(records, result, config, field, axis.label,
                                groupLabel(axis.axis, axis.groups.get(g).value, g, config.showValues), gf.bins, gf.nulls, null, null);
                    }
                }
            }
        }
        return records;
    }

    public static List<MElement> targetRecords(final Result result, final Config config) {
        final List<MElement> records = new ArrayList<>();
        if(result.target == null) {
            return records;
        }
        for(final TargetField f : result.target.fields) {
            final FieldResult field = result.fields.get(f.index);
            records.add(record(config, result)
                    .withString("field", field.path)
                    .withString("type", field.type)
                    .withInt64("count", f.count)
                    .withFloat64("ks", f.ks)
                    .withFloat64("tvd", f.tvd)
                    .withFloat64("iv", f.iv)
                    .withFloat64("pointBiserial", f.pointBiserial)
                    .withFloat64("meanPositive", "timestamp".equals(field.type) ? null : f.meanPositive)
                    .withFloat64("meanNegative", "timestamp".equals(field.type) ? null : f.meanNegative)
                    .withFloat64("rateWhenNull", f.rateWhenNull)
                    .withInt64("nullRows", f.nullRows)
                    .build());
        }
        return records;
    }

    public static List<MElement> pairRecords(final Result result, final Config config) {
        final List<MElement> records = new ArrayList<>();
        for(final PairResult pair : result.pairs) {
            records.add(record(config, result)
                    .withString("a", pair.a)
                    .withString("b", pair.b)
                    .withInt64("countA", pair.countA)
                    .withInt64("countB", pair.countB)
                    .withFloat64("ks", pair.ks)
                    .withFloat64("psi", pair.psi)
                    .withFloat64("noiseKs", pair.noiseKs)
                    .withFloat64("noisePsi", pair.noisePsi)
                    .build());
        }
        return records;
    }

    public static List<MElement> keyRecords(final Result result, final Config config) {
        final List<MElement> records = new ArrayList<>();
        for(final KeyResult key : result.keys) {
            records.add(record(config, result)
                    .withString("kind", "key")
                    .withString("key", key.key)
                    .withFloat64("distinct", key.distinct)
                    .withFloat64("distinct_lo", key.distinctLower)
                    .withFloat64("distinct_hi", key.distinctUpper)
                    .withFloat64("keyness", key.keyness)
                    .withInt64("nullKeys", key.nullKeys)
                    .build());
        }
        return records;
    }

    /**
     * @param reportBytes size of the written report, null when none was written
     * @param degradations what the report shed to fit its size limit
     */
    public static MElement summaryRecord(
            final Result result, final Config config, final Long reportBytes, final List<String> degradations) {

        final MElement.Builder b = record(config, result)
                .withInt64("rows", result.rows)
                .withInt64("errorRows", result.errorRows)
                .withInt64("fields", (long) result.fields.size())
                .withString("report", reportBytes == null ? null : config.reportOutput)
                .withInt64("reportBytes", reportBytes)
                .withString("payload", config.payloadOutput)
                .withStringList("degradations", new ArrayList<>(degradations));
        for(final Map.Entry<String, Long> entry : result.notableCounts().entrySet()) {
            b.withInt64(notableColumn(entry.getKey()), entry.getValue());
        }
        return b
                .withFloat64("targetRate", result.target == null ? null : result.target.rate)
                .withString("targetWarning", result.target == null ? null : result.target.warning)
                .build();
    }
}
