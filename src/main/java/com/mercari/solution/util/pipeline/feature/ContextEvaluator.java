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
        if ("softmax".equals(c.operator)) {
            softmax(c, rows);
            return;
        }
        if ("shuffle".equals(c.operator)) {
            shuffle(c, rows);
            return;
        }
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
        // group-constant ops (no excludeSelf) are evaluated once for the group, not once per row
        final boolean groupConstant = !excludeSelf && (value != null || List.of("countByValue", "ratioByValue", "entropy", "groupSize").contains(op));
        Object shared = groupConstant ? apply(op, values, 0, false) : null;
        for (int i = 0; i < rows.size(); i++) {
            Object result = groupConstant ? shared : apply(op, values, i, excludeSelf);
            if (value != null && result instanceof Map<?, ?> map) {
                // per-value column of countByValue / ratioByValue: absent value = 0 count / null ratio
                final Object picked = map.get(valueKey(value));
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

    /**
     * Group softmax in probability space: p_i = w_i · exp(f_i / T) / Σ_j w_j · exp(f_j / T), w = the offset value
     * (1 without an offset; {@code offsetScale: log} takes exp first). A null offset makes the row null and drops it
     * from the denominator; an offset of 0 gives p = 0; a null score falls back to 0 ({@code scoreNull: zero}) or
     * makes the row null ({@code scoreNull: null}). Scores are shifted by the group maximum for stability.
     */
    static void softmax(final OutputColumn c, final List<Map<String, Object>> rows) {
        final String field = c.coordinates.get("field");
        final String offset = c.coordinates.get("offset");
        final double temperature = Double.parseDouble(c.coordinates.getOrDefault("temperature", "1"));
        final boolean logScale = "log".equals(c.coordinates.get("offsetScale"));
        final boolean scoreNullIsNull = "null".equals(c.coordinates.get("scoreNull"));
        final int n = rows.size();
        final double[] weights = new double[n];
        final double[] scores = new double[n];
        final boolean[] active = new boolean[n];
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            final Map<String, Object> row = rows.get(i);
            Double w = offset == null ? Double.valueOf(1d) : FeatureValues.toDouble(row.get(offset));
            if (w != null && logScale && offset != null) w = Math.exp(w); // no offset: w = 1 whatever the scale
            if (w == null || Double.isNaN(w) || Double.isInfinite(w) || w < 0) continue;
            Double f = FeatureValues.toDouble(row.get(field));
            if (f == null || Double.isNaN(f)) {
                if (scoreNullIsNull) continue;
                f = 0d;
            }
            weights[i] = w;
            scores[i] = f / temperature;
            active[i] = true;
            if (w > 0) max = Math.max(max, scores[i]);
        }
        double denominator = 0d;
        for (int i = 0; i < n; i++) if (active[i] && weights[i] > 0) denominator += weights[i] * Math.exp(scores[i] - max);
        for (int i = 0; i < n; i++) {
            final Map<String, Object> row = rows.get(i);
            if (!active[i] || denominator <= 0 || Double.isNaN(denominator)) {
                row.put(c.canonicalName, null);
            } else {
                row.put(c.canonicalName, weights[i] == 0 ? 0d : weights[i] * Math.exp(scores[i] - max) / denominator);
            }
        }
    }

    /**
     * Placebo permutation: the field's values are reassigned across the group by a permutation drawn from
     * hash(seed, group key), applied to the rows ordered by (time.field, orderTieBreak) — so the multiset of values
     * per group is preserved and the result is a pure function of the group.
     */
    static void shuffle(final OutputColumn c, final List<Map<String, Object>> rows) {
        final String field = c.coordinates.get("field");
        final long seed = Long.parseLong(c.coordinates.get("seed"));
        final List<String> order = new ArrayList<>(List.of(c.coordinates.get("order").split(",")));
        final String tieBreak = c.coordinates.getOrDefault("tieBreak", "");
        if (!tieBreak.isEmpty()) order.addAll(List.of(tieBreak.split(",")));
        final List<String> contextKeys = c.coordinates.get("contextKeys").isEmpty() ? List.of() : List.of(c.coordinates.get("contextKeys").split(","));
        final int n = rows.size();
        final Integer[] sorted = new Integer[n];
        for (int i = 0; i < n; i++) sorted[i] = i;
        Arrays.sort(sorted, (a, b) -> compareIdentity(rows.get(a), rows.get(b), order));
        final String groupKey = n == 0 ? "" : FeatureValues.keyWithNullTokens(rows.get(0), contextKeys);
        final java.util.SplittableRandom random = FeatureValues.seededRandom(seed, groupKey);
        final int[] permutation = new int[n];
        for (int i = 0; i < n; i++) permutation[i] = i;
        for (int i = n - 1; i > 0; i--) {
            final int j = random.nextInt(i + 1);
            final int t = permutation[i];
            permutation[i] = permutation[j];
            permutation[j] = t;
        }
        final List<Object> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) values.add(rows.get(sorted[i]).get(field));
        for (int i = 0; i < n; i++) rows.get(sorted[i]).put(c.canonicalName, values.get(permutation[i]));
    }

    private static int compareIdentity(final Map<String, Object> a, final Map<String, Object> b, final List<String> order) {
        for (int i = 0; i < order.size(); i++) {
            final Object va = a.get(order.get(i));
            final Object vb = b.get(order.get(i));
            final int cmp;
            if (i == 0) {
                // time.field: compare as instants
                final Long ma = FeatureValues.toEpochMillis(va);
                final Long mb = FeatureValues.toEpochMillis(vb);
                cmp = ma == null ? (mb == null ? 0 : -1) : mb == null ? 1 : Long.compare(ma, mb);
            } else {
                cmp = va == null ? (vb == null ? 0 : -1) : vb == null ? 1 : va.toString().compareTo(vb.toString());
            }
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    /** Map key of a categorical value: integral numbers without a fractional part ("1", not "1.0"), so `values: [1]` matches int and double fields alike. */
    static String valueKey(final Object v) {
        if (v instanceof Number n && !(v instanceof Long) && !(v instanceof Integer)) {
            final double d = n.doubleValue();
            if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) < 1e15) return Long.toString((long) d);
        }
        if (v instanceof String s) {
            try {
                final double d = Double.parseDouble(s);
                if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) < 1e15 && s.contains(".")) return Long.toString((long) d);
            } catch (final NumberFormatException ignored) {
                // not numeric
            }
        }
        return v.toString();
    }

    private static Map<String, Long> countByValue(final List<Object> values, final int self, final boolean excludeSelf) {
        final Map<String, Long> counts = new TreeMap<>();
        for (int i = 0; i < values.size(); i++) {
            if (excludeSelf && i == self) continue;
            final Object v = values.get(i);
            if (v == null) continue;
            counts.merge(valueKey(v), 1L, Long::sum);
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
