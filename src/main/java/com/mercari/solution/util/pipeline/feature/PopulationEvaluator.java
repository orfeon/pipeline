package com.mercari.solution.util.pipeline.feature;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Expanding-fit encoding (work-feature.md §5, engine §4.4): for one keySet stage, the conditional
 * statistics of each target over the key's past contributions. A contribution at t' is visible to the
 * row at t only if {@code t' ≤ t − windowShift}, which is exactly the pending-contribution rule — the
 * target's value is unknown to the system until its effective availability time.
 *
 * <p>Statistics run on the incremental path of {@link SequenceEvaluator} (running sufficient statistics
 * advanced by monotonic fold / evict pointers), so a key's whole history is never re-scanned per row.
 * Phase 1 emits raw statistics (count / mean / rate / std / distribution); structured shrinkage lives in
 * the composed row columns.
 */
public class PopulationEvaluator extends SequenceEvaluator {

    public PopulationEvaluator(final List<OutputColumn> columns) {
        super(columns);
    }

    PopulationEvaluator(final List<OutputColumn> columns, final boolean forceScan) {
        super(columns, forceScan);
    }

    /** Stats that need information outside the key's own history and are not available yet. */
    public static boolean isSupported(final String stat) {
        return switch (stat) {
            case "count", "sum", "mean", "rate", "std", "distribution" -> true;
            default -> false;
        };
    }

    @Override
    String incrementalStat(final OutputColumn c) {
        if (!"encoding".equals(c.getOperator())) return null;
        final String stat = c.getCoordinates().get("stat");
        return isSupported(stat) ? stat : null;
    }

    @Override
    void contribute(final ColumnPlan plan, final Accumulator acc, final Past p, final int sign) {
        if (plan.field == null) {
            // target-less statistics (count / share denominators) count every visible row
            acc.n += sign;
            return;
        }
        if ("count".equals(plan.stat)) {
            // count matches the scan path: non-null target values, regardless of type or offset
            if (p.values().get(plan.field) != null) acc.n += sign;
            return;
        }
        if ("distribution".equals(plan.stat)) {
            final Object v = p.values().get(plan.field);
            if (v == null) return;
            if (acc.valueCounts == null) acc.valueCounts = new TreeMap<>();
            acc.valueCounts.merge(v.toString(), (long) sign, Long::sum);
            acc.n += sign;
            return;
        }
        Double v = FeatureValues.toDouble(p.values().get(plan.field));
        if (v == null) return;
        if (plan.offset != null) {
            final Double b = FeatureValues.toDouble(p.values().get(plan.offset));
            if (b == null) return;
            v -= b;
        }
        acc.n += sign;
        acc.sum += sign * v;
        acc.sumSq += sign * v * v;
    }

    @Override
    Object readStatistic(final OutputColumn c, final ColumnPlan plan, final Accumulator acc) {
        final double n = acc == null ? 0 : acc.n;
        return switch (plan.stat) {
            case "count" -> (long) n;
            case "sum" -> acc == null || acc.n == 0 ? 0d : acc.sum;
            case "mean", "rate" -> n == 0 ? null : acc.sum / n;
            case "std" -> {
                if (n < 2) yield null;
                final double mean = acc.sum / n;
                yield Math.sqrt(Math.max(0, acc.sumSq / n - mean * mean));
            }
            case "distribution" -> {
                if (acc == null || acc.valueCounts == null || n == 0) yield null;
                final Map<String, Object> dist = new LinkedHashMap<>();
                for (final Map.Entry<String, Long> e : acc.valueCounts.entrySet()) {
                    if (e.getValue() > 0) dist.put(e.getKey(), e.getValue() / n);
                }
                yield dist;
            }
            default -> throw new IllegalStateException("unsupported encoding stat: " + plan.stat);
        };
    }

    /** Scan fallback (equivalence testing and any non-incremental configuration). */
    @Override
    Object evaluateScan(final OutputColumn c, final ColumnPlan plan, final Map<String, Object> row,
                        final long nowMillis, final List<Past> window) {
        final String stat = c.getCoordinates().get("stat");
        if ("count".equals(stat)) {
            if (plan.field == null) return (long) window.size();
            long n = 0;
            for (final Past p : window) if (p.values().get(plan.field) != null) n++;
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
        double n = 0, sum = 0, sumSq = 0;
        for (final Past p : window) {
            Double v = FeatureValues.toDouble(p.values().get(plan.field));
            if (v == null) continue;
            if (plan.offset != null) {
                final Double b = FeatureValues.toDouble(p.values().get(plan.offset));
                if (b == null) continue;
                v -= b;
            }
            n++;
            sum += v;
            sumSq += v * v;
        }
        return switch (stat) {
            case "sum" -> sum;
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
