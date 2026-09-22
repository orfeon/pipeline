package com.mercari.solution.util.pipeline.feature;

import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Expanding-fit encoding (docs/design/feature-dsl.md §5, engine §4.4): for one keySet stage, the conditional
 * statistics of each target over the key's past contributions. A contribution at t' is visible to the
 * row at t only if {@code t' ≤ t − windowShift}, which is exactly the pending-contribution rule — the
 * target's value is unknown to the system until its effective availability time.
 *
 * <p>Statistics run on the incremental path of {@link SequenceEvaluator} (a {@link Summary} state per column
 * advanced by monotonic fold / evict pointers), so a key's whole history is never re-scanned per row. This
 * evaluator only changes what a past row <em>contributes</em> (the target minus its baseline offset; a bare 0
 * for the row counts of the target-less levels; the category of a distribution) and the null convention of the
 * hidden sums. Phase 1 emits raw statistics (count / mean / rate / std / distribution); structured shrinkage
 * lives in the composed row columns.
 */
public class PopulationEvaluator extends SequenceEvaluator {

    public PopulationEvaluator(final List<OutputColumn> columns) {
        super(columns);
    }

    PopulationEvaluator(final List<OutputColumn> columns, final boolean forceScan) {
        super(columns, forceScan);
    }

    /** Hidden statistic of an offset block on a logit / log scale: Σ baseline over the rows counted by the level's {@code sum}. */
    public static final String SUM_OFFSET = "sumoff";
    /**
     * Hidden statistic of an offset block on the logit scale: Σ b(1 − b), the Fisher information of the rows counted by the
     * level's {@code sum} at their baseline — the denominator of the score-type term ({@link Shrinkage#ownScore}; on log
     * the information is Σ baseline itself, so the level reads {@code sumoff} for it).
     */
    public static final String SUM_INFO = "suminfo";

    /**
     * Stats the expanding (per-key replay) engine can serve: every statistic with a summary family
     * ({@link OperatorCatalog#summary}) — which excludes {@code share}, a row composition of two hidden counts —
     * plus the hidden {@code sum} / {@code sumoff} of the lattice levels.
     */
    public static boolean isSupported(final String stat) {
        return summary(stat) != null;
    }

    /** Σ baseline and Σ b(1 − b) accumulate like any other sum: the hidden statistics read the sum of a moments summary. */
    private static final Summary.Spec SUM_OFFSET_SUMMARY =
            new Summary.Spec(Summary.Summaries.MOMENTS, Summary.Readout.of("sum"));

    /** Whether a stat is one of the hidden offset sums ({@code sumoff} / {@code suminfo}). */
    static boolean isOffsetSum(final String stat) {
        return SUM_OFFSET.equals(stat) || SUM_INFO.equals(stat);
    }

    /** The summary family of an encoding stat: the catalog's, plus the hidden offset sums of an offset block. */
    private static Summary.Spec summary(final String stat) {
        return isOffsetSum(stat) ? SUM_OFFSET_SUMMARY : OperatorCatalog.summary(stat);
    }

    @Override
    String statToken(final OutputColumn c) {
        return "encoding".equals(c.getOperator()) ? c.getCoordinates().get("stat") : null;
    }

    @Override
    Summary.Spec summaryOf(final OutputColumn c) {
        return "encoding".equals(c.getOperator()) ? summary(c.getCoordinates().get("stat")) : null;
    }

    @Override
    Object contribution(final ColumnPlan plan, final Past p) {
        // target-less statistics (count / share denominators) count every visible row
        if (plan.field == null) return 0d;
        // count matches the scan path: the rows the level's sums are taken over, whatever the target type
        if ("count".equals(plan.stat)) return counted(plan, p.values()) ? 0d : null;
        if ("distribution".equals(plan.stat)) return p.values().get(plan.field);
        final KV<Double, Double> t = FeatureValues.offsetTarget(p.values(), plan.field, plan.offset);
        if (t == null) return null;
        // Σ baseline / Σ b(1 − b) run over the same rows the level's sum counts (the pair is null unless target and baseline are present)
        if (SUM_OFFSET.equals(plan.stat)) return baseline(t);
        if (SUM_INFO.equals(plan.stat)) return information(t);
        return t.getKey();
    }

    /**
     * Whether a past row is counted: a non-null target of any type and, under an offset, a baseline — the rows the
     * level's Σ(y − b) is taken over, so {@code n} and the sums agree (and match the static / fold / forward fits).
     */
    private static boolean counted(final ColumnPlan plan, final Map<String, Object> values) {
        if (values.get(plan.field) == null) return false;
        if (plan.offset == null) return true;
        final Double b = FeatureValues.toDouble(values.get(plan.offset));
        return b != null && !b.isNaN();
    }

    private static double baseline(final KV<Double, Double> target) {
        return target.getValue() == null ? 0d : target.getValue();
    }

    /** The row's information at its baseline on the logit scale, {@code b(1 − b)} (the only scale with a column of its own). */
    private static double information(final KV<Double, Double> target) {
        return Shrinkage.information(Shrinkage.Scale.logit, baseline(target));
    }

    /** The hidden {@code sum} / {@code sumoff} / {@code suminfo} of a level reads 0 (not null) when nothing contributed: the composition adds sums. */
    @Override
    Object readStatistic(final OutputColumn c, final ColumnPlan plan, final Serializable state, final long nowMillis) {
        final Object value = plan.summary.<Serializable>typed().read(state, plan.summary.readout());
        return value == null && ("sum".equals(plan.stat) || isOffsetSum(plan.stat)) ? 0d : value;
    }

    /** Scan fallback (equivalence testing and any non-incremental configuration). */
    @Override
    Object evaluateScan(final OutputColumn c, final ColumnPlan plan, final Map<String, Object> row,
                        final long nowMillis, final List<Past> window) {
        final String stat = c.getCoordinates().get("stat");
        if ("count".equals(stat)) {
            if (plan.field == null) return (long) window.size();
            long n = 0;
            for (final Past p : window) if (counted(plan, p.values())) n++;
            return n;
        }
        if ("distribution".equals(stat)) {
            final Map<String, Long> counts = new TreeMap<>();
            for (final Past p : window) {
                final Object v = p.values().get(plan.field);
                if (v != null) counts.merge(v.toString(), 1L, Long::sum);
            }
            if (counts.isEmpty()) return null;
            final double total = counts.values().stream().mapToLong(Long::longValue).sum();
            final Map<String, Object> dist = new LinkedHashMap<>();
            for (final Map.Entry<String, Long> e : counts.entrySet()) dist.put(e.getKey(), e.getValue() / total);
            return dist;
        }
        final Double quantile = OperatorCatalog.quantileProbability(stat);
        if (quantile != null) {
            final double[] values = new double[window.size()];
            int n = 0;
            for (final Past p : window) {
                final KV<Double, Double> t = FeatureValues.offsetTarget(p.values(), plan.field, plan.offset);
                if (t != null) values[n++] = t.getKey();
            }
            if (n == 0) return null;
            java.util.Arrays.sort(values, 0, n);
            return OrderStatistics.quantile(quantile, values, n);
        }
        final boolean offsetSum = isOffsetSum(stat);
        double n = 0, sum = 0, sumSq = 0, sumOff = 0, sumInfo = 0;
        for (final Past p : window) {
            final KV<Double, Double> t = FeatureValues.offsetTarget(p.values(), plan.field, plan.offset);
            if (t == null) continue;
            n++;
            if (offsetSum) {
                sumOff += baseline(t);
                sumInfo += information(t);
                continue;
            }
            final double v = t.getKey();
            sum += v;
            sumSq += v * v;
        }
        return switch (stat) {
            case "sum" -> sum;
            case SUM_OFFSET -> sumOff;
            case SUM_INFO -> sumInfo;
            case "mean", "rate" -> n == 0 ? null : sum / n;
            case "std" -> {
                if (n < 2) yield null;
                final double mean = sum / n;
                yield Math.sqrt(Math.max(0, sumSq / n - mean * mean));
            }
            default -> throw new IllegalStateException("unsupported encoding stat: " + stat);
        };
    }

}
