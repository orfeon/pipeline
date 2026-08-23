package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.pipeline.Filter;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

/**
 * Evaluates sequence-scope columns for one row from that entity's strictly-past history
 * (rows ordered by event time, ascending). Implements the window semantics of work-feature.md §4.3 and
 * the near-edge shift of §6.2: a past row contributes only if {@code t' ≤ t − windowShift}.
 */
public class SequenceEvaluator implements Serializable {

    /** A buffered past row: event time millis + the projected field values. */
    public record Past(long millis, Map<String, Object> values) implements Serializable {}

    private final List<OutputColumn> columns;
    private transient Map<String, Filter.ConditionNode> conditions;

    public SequenceEvaluator(final List<OutputColumn> columns) {
        this.columns = columns;
    }

    public List<OutputColumn> getColumns() {
        return columns;
    }

    /** Fields that must be kept in the per-entity buffer for these columns. */
    public Set<String> bufferedFields() {
        final Set<String> fields = new LinkedHashSet<>();
        for (final OutputColumn c : columns) fields.addAll(c.pastInputs);
        return fields;
    }

    /** Longest retention needed; null means unbounded (some window has no maxAge). */
    public Duration retention() {
        Duration max = Duration.ZERO;
        for (final OutputColumn c : columns) {
            final String maxAge = c.coordinates.get("maxAge");
            if (maxAge == null) return null;
            final Duration d = Duration.parse(maxAge).plus(c.windowShift == null ? Duration.ZERO : c.windowShift);
            if (d.compareTo(max) > 0) max = d;
        }
        return max;
    }

    public void setup() {
        conditions = new HashMap<>();
        for (final OutputColumn c : columns) {
            for (final String key : List.of("filter", "predicate")) {
                final String text = c.coordinates.get(key);
                if (text != null && !conditions.containsKey(text)) {
                    conditions.put(text, Filter.parse(text.replace("$self.", FeatureValues.SELF_PREFIX)));
                }
            }
        }
    }

    public void evaluate(final Map<String, Object> row, final long nowMillis, final List<Past> history) {
        for (final OutputColumn c : columns) {
            row.put(c.canonicalName, evaluateColumn(c, row, nowMillis, history));
        }
    }

    Object evaluateColumn(final OutputColumn c, final Map<String, Object> row, final long nowMillis, final List<Past> history) {
        final List<Past> window = select(c, row, nowMillis, history);
        final String field = c.coordinates.get("field");
        switch (c.operator) {
            case "lag" -> {
                final int k = Integer.parseInt(c.coordinates.get("k"));
                return window.size() >= k ? window.get(window.size() - k).values.get(field) : null;
            }
            case "delta" -> {
                final int k = Integer.parseInt(c.coordinates.get("k"));
                if (window.size() < k + 1) return null;
                final Double a = FeatureValues.toDouble(window.get(window.size() - k).values.get(field));
                final Double b = FeatureValues.toDouble(window.get(window.size() - k - 1).values.get(field));
                return a == null || b == null ? null : a - b;
            }
            case "trend" -> {
                final int k = Integer.parseInt(c.coordinates.get("k"));
                final List<Double> ys = new ArrayList<>();
                for (int i = Math.max(0, window.size() - k); i < window.size(); i++) {
                    final Double y = FeatureValues.toDouble(window.get(i).values.get(field));
                    if (y != null) ys.add(y);
                }
                return slope(ys);
            }
            case "ewma" -> {
                final double halflife = Double.parseDouble(c.coordinates.get("halflife"));
                final boolean byTime = "time".equals(c.coordinates.get("decayBy"));
                double num = 0, den = 0;
                for (int i = 0; i < window.size(); i++) {
                    final Past p = window.get(i);
                    final Double v = FeatureValues.toDouble(p.values.get(field));
                    if (v == null) continue;
                    final double steps = byTime ? (nowMillis - p.millis) / 86_400_000d : (window.size() - 1 - i);
                    final double w = Math.pow(0.5, steps / halflife);
                    num += w * v;
                    den += w;
                }
                return den == 0 ? null : num / den;
            }
            case "runLength" -> {
                final String value = c.coordinates.get("value");
                long run = 0;
                for (int i = window.size() - 1; i >= 0; i--) {
                    final Object v = window.get(i).values.get(field);
                    if (v != null && value.equals(v.toString())) run++;
                    else break;
                }
                return run;
            }
            case "sinceEvent" -> {
                final Filter.ConditionNode condition = conditions.get(c.coordinates.get("predicate"));
                for (int i = window.size() - 1; i >= 0; i--) {
                    if (Filter.filter(condition, window.get(i).values)) {
                        final String unit = c.coordinates.getOrDefault("unit", "events");
                        return "events".equals(unit) ? (Object) (long) (window.size() - i)
                                : (nowMillis - window.get(i).millis) / 86_400_000d;
                    }
                }
                return null;
            }
            case "countMatch" -> {
                final Filter.ConditionNode condition = conditions.get(c.coordinates.get("predicate"));
                long n = 0;
                for (final Past p : window) if (Filter.filter(condition, p.values)) n++;
                return n;
            }
            case "aggregate" -> {
                return aggregate(c.coordinates.get("func"), window, field, c);
            }
            default -> throw new IllegalStateException("unsupported sequence operator: " + c.operator);
        }
    }

    protected List<Past> select(final OutputColumn c, final Map<String, Object> row, final long nowMillis, final List<Past> history) {
        final long nearEdge = c.windowShift == null ? nowMillis : nowMillis - c.windowShift.toMillis();
        final String maxAge = c.coordinates.get("maxAge");
        final long farEdge = maxAge == null ? Long.MIN_VALUE : nowMillis - Duration.parse(maxAge).toMillis();
        final String filter = c.coordinates.get("filter");
        final Filter.ConditionNode condition = filter == null ? null : conditions.get(filter);
        final Map<String, Object> selfValues = new HashMap<>();
        if (condition != null) {
            for (final Map.Entry<String, Object> e : row.entrySet()) selfValues.put(FeatureValues.SELF_PREFIX + e.getKey(), e.getValue());
        }
        final List<Past> selected = new ArrayList<>();
        for (final Past p : history) {
            // strictly past: t' < t, then the near edge shift for late-arriving fields
            if (p.millis >= nowMillis || p.millis > nearEdge || p.millis < farEdge) continue;
            if (condition != null) {
                final Map<String, Object> scope = new HashMap<>(p.values);
                scope.putAll(selfValues);
                if (!Filter.filter(condition, scope)) continue;
            }
            selected.add(p);
        }
        final String maxEvents = c.coordinates.get("maxEvents");
        if (maxEvents != null) {
            final int n = Integer.parseInt(maxEvents);
            if (selected.size() > n) return new ArrayList<>(selected.subList(selected.size() - n, selected.size()));
        }
        return selected;
    }

    static Object aggregate(final String func, final List<Past> window, final String field, final OutputColumn c) {
        final List<Double> values = new ArrayList<>();
        Object first = null, last = null;
        for (final Past p : window) {
            final Object v = p.values.get(field);
            if (v == null) continue;
            if (first == null) first = v;
            last = v;
            final Double d = FeatureValues.toDouble(v);
            if (d != null) values.add(d);
        }
        return switch (func) {
            case "count" -> (long) values.size();
            case "first" -> first;
            case "last" -> last;
            case "sum" -> values.isEmpty() ? null : values.stream().mapToDouble(d -> d).sum();
            case "mean", "avg", "rate" -> values.isEmpty() ? null : values.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
            case "min" -> values.isEmpty() ? null : FeatureValues.cast(values.stream().mapToDouble(d -> d).min().orElse(Double.NaN), c.fieldType);
            case "max" -> values.isEmpty() ? null : FeatureValues.cast(values.stream().mapToDouble(d -> d).max().orElse(Double.NaN), c.fieldType);
            case "std" -> {
                if (values.size() < 2) yield null;
                final double mean = values.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
                yield Math.sqrt(values.stream().mapToDouble(d -> (d - mean) * (d - mean)).sum() / values.size());
            }
            default -> throw new IllegalStateException("unsupported aggregate func: " + func);
        };
    }

    static Double slope(final List<Double> ys) {
        final int n = ys.size();
        if (n < 2) return null;
        final double xMean = (n - 1) / 2d;
        final double yMean = ys.stream().mapToDouble(d -> d).average().orElse(0);
        double num = 0, den = 0;
        for (int i = 0; i < n; i++) {
            num += (i - xMean) * (ys.get(i) - yMean);
            den += (i - xMean) * (i - xMean);
        }
        return den == 0 ? null : num / den;
    }

}
