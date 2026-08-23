package com.mercari.solution.util.pipeline.feature;

import java.util.ArrayList;
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
 * <p>Phase 1 emits raw statistics (count / mean / rate / std / distribution); structured shrinkage and
 * global-denominator stats (share) are phase 2.
 */
public class PopulationEvaluator extends SequenceEvaluator {

    public PopulationEvaluator(final List<OutputColumn> columns) {
        super(columns);
    }

    /** Stats that need information outside the key's own history and are not available in phase 1. */
    public static boolean isSupported(final String stat) {
        return switch (stat) {
            case "count", "sum", "mean", "rate", "std", "distribution" -> true;
            default -> false;
        };
    }

    @Override
    Object evaluateColumn(final OutputColumn c, final Map<String, Object> row, final long nowMillis, final List<Past> history) {
        final List<Past> window = select(c, row, nowMillis, history);
        final String field = c.coordinates.get("field");
        final String offset = c.coordinates.containsKey("offset") ? "__baseline_" + c.coordinates.get("offset") : null;
        final String stat = c.coordinates.get("stat");
        if ("count".equals(stat)) {
            if (field == null) return (long) window.size();
            long n = 0;
            for (final Past p : window) if (p.values().get(field) != null) n++;
            return n;
        }
        if ("distribution".equals(stat)) {
            final Map<String, Long> counts = new TreeMap<>();
            for (final Past p : window) {
                final Object v = p.values().get(field);
                if (v != null) counts.merge(v.toString(), 1L, Long::sum);
            }
            final double total = counts.values().stream().mapToLong(Long::longValue).sum();
            final Map<String, Object> dist = new LinkedHashMap<>();
            for (final Map.Entry<String, Long> e : counts.entrySet()) dist.put(e.getKey(), e.getValue() / total);
            return dist;
        }
        final List<Double> values = new ArrayList<>();
        for (final Past p : window) {
            final Double v = FeatureValues.toDouble(p.values().get(field));
            if (v == null) continue;
            if (offset != null) {
                final Double b = FeatureValues.toDouble(p.values().get(offset));
                if (b == null) continue;
                values.add(v - b);
            } else {
                values.add(v);
            }
        }
        return switch (stat) {
            case "sum" -> values.stream().mapToDouble(d -> d).sum();
            case "mean", "rate" -> values.isEmpty() ? null : values.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
            case "std" -> {
                if (values.size() < 2) yield null;
                final double mean = values.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
                yield Math.sqrt(values.stream().mapToDouble(d -> (d - mean) * (d - mean)).sum() / values.size());
            }
            default -> throw new IllegalStateException("unsupported encoding stat: " + stat);
        };
    }

}
