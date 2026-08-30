package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.ExpressionUtil;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Evaluates context-scope columns over the rows that share a context key (one co-occurrence group).
 * Groups are expected to be small (the rows of one event), so per-row "others" statistics are O(n²).
 */
public class ContextEvaluator implements Serializable {

    private static final Pattern OP_CALL = Pattern.compile("^\\s*([A-Za-z_]+)\\s*\\((.*)\\)\\s*$");

    private final List<OutputColumn> columns;
    private transient Map<String, ExpressionUtil.Expression> expressions;
    private transient Map<String, String> baselineOps;

    public ContextEvaluator(final List<OutputColumn> columns) {
        this.columns = columns;
    }

    public List<OutputColumn> getColumns() {
        return columns;
    }

    public void setup() {
        expressions = new HashMap<>();
        baselineOps = new HashMap<>();
        for (final OutputColumn c : columns) {
            if (!"baseline".equals(c.operator)) continue;
            final String expr = c.coordinates.get("expr");
            final Matcher m = OP_CALL.matcher(expr);
            if (m.matches() && OperatorCatalog.get(FeatureSpec.Scope.context, m.group(1)) != null) {
                baselineOps.put(c.canonicalName, m.group(1));
                expressions.put(c.canonicalName, ExpressionUtil.createDefaultExpression(m.group(2)));
            } else {
                expressions.put(c.canonicalName, ExpressionUtil.createDefaultExpression(expr));
            }
        }
    }

    public void evaluate(final List<Map<String, Object>> rows) {
        for (final OutputColumn c : columns) {
            evaluateColumn(c, rows);
        }
    }

    /** Evaluates one context column for every row of the group (rows are mutated in place). */
    public void evaluateColumn(final OutputColumn c, final List<Map<String, Object>> rows) {
        final String op = "baseline".equals(c.operator) ? baselineOps.get(c.canonicalName) : c.operator;
        final boolean excludeSelf = "true".equals(c.coordinates.get("excludeSelf"));
        if (op == null) {
            // row-level baseline expression evaluated per row
            final ExpressionUtil.Expression e = expressions.get(c.canonicalName);
            for (final Map<String, Object> row : rows) row.put(c.canonicalName, FeatureValues.evaluate(e, row));
            return;
        }
        final List<Object> values = new ArrayList<>(rows.size());
        for (final Map<String, Object> row : rows) {
            if ("baseline".equals(c.operator)) {
                values.add(FeatureValues.evaluate(expressions.get(c.canonicalName), row));
            } else if (c.coordinates.containsKey("field")) {
                values.add(row.get(c.coordinates.get("field")));
            } else {
                values.add(null);
            }
        }
        final String value = c.coordinates.get("value");
        for (int i = 0; i < rows.size(); i++) {
            Object result = apply(op, values, i, excludeSelf);
            if (value != null && result instanceof Map<?, ?> map) {
                // per-value column of countByValue / ratioByValue: absent value = 0 count / null ratio
                final Object picked = map.get(value);
                result = picked != null ? picked : "countByValue".equals(op) ? 0L : null;
            }
            rows.get(i).put(c.canonicalName, result);
        }
    }

    static Object apply(final String op, final List<Object> values, final int self, final boolean excludeSelf) {
        return switch (op) {
            case "groupSize" -> (long) (excludeSelf ? values.size() - 1 : values.size());
            case "countByValue" -> countByValue(values, self, excludeSelf);
            case "ratioByValue" -> {
                final Map<String, Long> counts = countByValue(values, self, excludeSelf);
                final double total = counts.values().stream().mapToLong(Long::longValue).sum();
                final Map<String, Object> ratios = new LinkedHashMap<>();
                for (final Map.Entry<String, Long> e : counts.entrySet()) ratios.put(e.getKey(), total == 0 ? null : e.getValue() / total);
                yield ratios;
            }
            case "entropy" -> {
                final Map<String, Long> counts = countByValue(values, self, excludeSelf);
                final double total = counts.values().stream().mapToLong(Long::longValue).sum();
                if (total == 0) yield null;
                double h = 0;
                for (final long n : counts.values()) {
                    final double p = n / total;
                    h -= p * Math.log(p);
                }
                yield h;
            }
            default -> numeric(op, values, self, excludeSelf);
        };
    }

    private static Map<String, Long> countByValue(final List<Object> values, final int self, final boolean excludeSelf) {
        final Map<String, Long> counts = new TreeMap<>();
        for (int i = 0; i < values.size(); i++) {
            if (excludeSelf && i == self) continue;
            final Object v = values.get(i);
            if (v == null) continue;
            counts.merge(v.toString(), 1L, Long::sum);
        }
        return counts;
    }

    private static Object numeric(final String op, final List<Object> values, final int self, final boolean excludeSelf) {
        final Double x = FeatureValues.toDouble(values.get(self));
        if (x == null) return null;
        final List<Double> others = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            if (excludeSelf && i == self) continue;
            final Double d = FeatureValues.toDouble(values.get(i));
            if (d != null) others.add(d);
        }
        if (others.isEmpty()) return null;
        switch (op) {
            case "rank" -> {
                long rank = 1;
                for (final Double d : others) if (d > x) rank++;
                return rank;
            }
            case "zscore" -> {
                final double mean = others.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
                final double var = others.stream().mapToDouble(d -> (d - mean) * (d - mean)).sum() / others.size();
                final double std = Math.sqrt(var);
                return std == 0 ? null : (x - mean) / std;
            }
            case "gapToBest" -> {
                return x - others.stream().mapToDouble(d -> d).max().orElse(Double.NaN);
            }
            case "shareOfTotal", "share" -> {
                // excludeSelf: share relative to the OTHER rows' total (otherwise the group total incl. self)
                final double sum = others.stream().mapToDouble(d -> d).sum();
                return sum == 0 ? null : x / sum;
            }
            case "percentile" -> {
                long le = 0;
                for (final Double d : others) if (d <= x) le++;
                return (double) le / others.size();
            }
            case "median_diff" -> {
                final List<Double> sorted = new ArrayList<>(others);
                Collections.sort(sorted);
                final int n = sorted.size();
                final double median = n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
                return x - median;
            }
            default -> throw new IllegalStateException("unsupported context operator: " + op);
        }
    }

}
