package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.ExpressionUtil;

import java.io.Serializable;
import java.util.*;

/**
 * Evaluates context-scope columns over the rows that share a context key (one co-occurrence group).
 * Groups are expected to be small (the rows of one event), so per-row "others" statistics are O(n²).
 *
 * <p>Every column's coordinates are resolved into a {@link Plan} once per worker ({@link #setup()}): the group loop
 * reads parsed numbers and arrays, never strings. The group solvers of one field share their work through
 * {@link #harvilleCache} — the places of a harville are one pass, whatever {@code top} the columns ask for.
 */
public class ContextEvaluator implements Serializable {

    private final List<OutputColumn> columns;
    private transient Map<String, Plan> plans;
    /** The largest place any column of a group solver asks for, by solver key: one pass serves them all. */
    private transient Map<String, Integer> places;
    private transient List<Map<String, Object>> cachedGroup;
    private transient Map<String, double[][]> harvilleCache;

    public ContextEvaluator(final List<OutputColumn> columns) {
        this.columns = columns;
    }

    public List<OutputColumn> getColumns() {
        return columns;
    }

    public void setup() {
        plans = new HashMap<>();
        places = new HashMap<>();
        for (final OutputColumn c : columns) {
            final Plan plan = plans.computeIfAbsent(c.canonicalName, name -> plan(c));
            if (plan.harville() != null) {
                places.merge(plan.harville().key(), plan.harville().top(), Math::max);
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
        final Plan plan = plan(c.canonicalName, c);
        if (plan.softmax() != null) {
            softmax(c.canonicalName, plan.field(), plan.softmax(), rows);
            return;
        }
        if (plan.shuffle() != null) {
            shuffle(c.canonicalName, plan.field(), plan.shuffle(), rows);
            return;
        }
        if (plan.residualize() != null || plan.harville() != null) {
            solve(c.canonicalName, plan, rows);
            return;
        }
        final String op = plan.op();
        if (op == null) {
            // row-level baseline expression evaluated per row
            for (final Map<String, Object> row : rows) row.put(c.canonicalName, FeatureValues.evaluate(plan.expression(), row));
            return;
        }
        final List<Object> values = new ArrayList<>(rows.size());
        for (final Map<String, Object> row : rows) {
            if (plan.expression() != null) {
                values.add(FeatureValues.evaluate(plan.expression(), row));
            } else if (plan.field() != null) {
                values.add(row.get(plan.field()));
            } else {
                values.add(null);
            }
        }
        final String value = plan.value();
        // group-constant ops (no excludeSelf) are evaluated once for the group, not once per row
        final boolean groupConstant = plan.groupConstant();
        final Object shared = groupConstant ? apply(op, values, 0, false) : null;
        for (int i = 0; i < rows.size(); i++) {
            Object result = groupConstant ? shared : apply(op, values, i, plan.excludeSelf());
            if (value != null && result instanceof Map<?, ?> map) {
                // per-value column of countByValue / ratioByValue: absent value = 0 count / null ratio
                final Object picked = map.get(valueKey(value));
                result = picked != null ? picked : "countByValue".equals(op) ? 0L : null;
            }
            rows.get(i).put(c.canonicalName, result);
        }
    }

    // --- plans ------------------------------------------------------------------------------------

    /**
     * One column's coordinates, parsed. {@code op} is the operator to apply over the group's values (null for a
     * plain row expression); the parameterised ops carry their own record and are dispatched on it.
     */
    private record Plan(String op, String field, boolean excludeSelf, String value, ExpressionUtil.Expression expression,
                        Softmax softmax, Residualize residualize, Harville harville, Shuffle shuffle) {

        /** The ops whose reading is the same for every row of the group, so the group is read once. */
        private static final Set<String> GROUP_CONSTANT = Set.of("countByValue", "ratioByValue", "entropy", "groupSize");

        static Plan of(final String op, final String field, final boolean excludeSelf, final String value,
                       final ExpressionUtil.Expression expression) {
            return new Plan(op, field, excludeSelf, value, expression, null, null, null, null);
        }

        Plan with(final Softmax s) { return new Plan(op, field, excludeSelf, value, expression, s, null, null, null); }

        Plan with(final Residualize r) { return new Plan(op, field, excludeSelf, value, expression, null, r, null, null); }

        Plan with(final Harville h) { return new Plan(op, field, excludeSelf, value, expression, null, null, h, null); }

        Plan with(final Shuffle p) { return new Plan(op, field, excludeSelf, value, expression, null, null, null, p); }

        boolean groupConstant() {
            return !excludeSelf && (value != null || GROUP_CONSTANT.contains(op));
        }
    }

    private record Softmax(String offset, double temperature, boolean logScale, boolean scoreNullIsNull) {}

    private record Residualize(String[] against) {}

    /** {@code key} names the pass this column reads: the same field, discount and bound are computed once per group. */
    private record Harville(int top, double[] discount, int maxGroupSize, String key) {}

    /** {@code order} is the row identity followed by the tie break, {@code contextKeys} the group key. */
    private record Shuffle(long seed, List<String> order, List<String> contextKeys) {}

    /** The column's plan, built on first use when {@link #setup()} has not run (a direct call from a test). */
    private Plan plan(final String name, final OutputColumn c) {
        if (plans == null) plans = new HashMap<>();
        return plans.computeIfAbsent(name, key -> plan(c));
    }

    private static Plan plan(final OutputColumn c) {
        final Map<String, String> at = c.coordinates;
        final boolean excludeSelf = "true".equals(at.get("excludeSelf"));
        final String field = at.get("field");
        if ("baseline".equals(c.operator)) {
            // an expression, or a context op applied to one (share(1 / bid)); the compiler has rejected the ops
            // that cannot be called this way (baselines.expr.op), so an op found here is one apply() computes
            final OperatorCatalog.Call call = OperatorCatalog.parseContextCall(at.get("expr"));
            final boolean isOp = call != null && call.operator().baselineCallable();
            return Plan.of(isOp ? call.operator().name() : null, field, excludeSelf, at.get("value"),
                    ExpressionUtil.createDefaultExpression(isOp ? call.arguments() : at.get("expr")));
        }
        final Plan plan = Plan.of(c.operator, field, excludeSelf, at.get("value"), null);
        return switch (c.operator) {
            case "softmax" -> plan.with(new Softmax(at.get("offset"), Double.parseDouble(at.getOrDefault("temperature", "1")),
                    "log".equals(at.get("offsetScale")), "null".equals(at.get("scoreNull"))));
            case "residualize" -> plan.with(new Residualize(split(at.get("against"))));
            case "harville" -> {
                final String discount = at.get("discount");
                final int maxGroupSize = Integer.parseInt(at.get("maxGroupSize"));
                yield plan.with(new Harville(Integer.parseInt(at.get("top")), doubles(discount), maxGroupSize,
                        field + "\u0000" + discount + "\u0000" + maxGroupSize));
            }
            case "shuffle" -> {
                final List<String> order = new ArrayList<>(List.of(split(at.get("order"))));
                order.addAll(List.of(split(at.get("tieBreak"))));
                yield plan.with(new Shuffle(Long.parseLong(at.get("seed")), List.copyOf(order), List.of(split(at.get("contextKeys")))));
            }
            default -> plan;
        };
    }

    private static String[] split(final String joined) {
        return joined == null || joined.isEmpty() ? new String[0] : joined.split(",");
    }

    private static double[] doubles(final String joined) {
        final String[] parts = split(joined);
        final double[] values = new double[parts.length];
        for (int i = 0; i < parts.length; i++) values[i] = Double.parseDouble(parts[i]);
        return values;
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

    /** Group softmax over one column (the plan of {@code c}); see {@link #softmax(String, String, Softmax, List)}. */
    static void softmax(final OutputColumn c, final List<Map<String, Object>> rows) {
        final Plan plan = plan(c);
        softmax(c.canonicalName, plan.field(), plan.softmax(), rows);
    }

    /**
     * Group softmax in probability space: p_i = w_i · exp(f_i / T) / Σ_j w_j · exp(f_j / T), w = the offset value
     * (1 without an offset; {@code offsetScale: log} takes exp first). A null offset makes the row null and drops it
     * from the denominator; an offset of 0 gives p = 0; a null score falls back to 0 ({@code scoreNull: zero}) or
     * makes the row null ({@code scoreNull: null}). Scores are shifted by the group maximum for stability.
     */
    static void softmax(final String name, final String field, final Softmax plan, final List<Map<String, Object>> rows) {
        final String offset = plan.offset();
        final int n = rows.size();
        final double[] weights = new double[n];
        final double[] scores = new double[n];
        final boolean[] active = new boolean[n];
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            final Map<String, Object> row = rows.get(i);
            Double w = offset == null ? Double.valueOf(1d) : FeatureValues.toDouble(row.get(offset));
            if (w != null && offset != null && plan.logScale()) w = Math.exp(w); // no offset: w = 1 whatever the scale
            if (w == null || Double.isNaN(w) || Double.isInfinite(w) || w < 0) continue;
            Double f = FeatureValues.toDouble(row.get(field));
            if (f == null || Double.isNaN(f)) {
                if (plan.scoreNullIsNull()) continue;
                f = 0d;
            }
            weights[i] = w;
            scores[i] = f / plan.temperature();
            active[i] = true;
            if (w > 0) max = Math.max(max, scores[i]);
        }
        double denominator = 0d;
        for (int i = 0; i < n; i++) if (active[i] && weights[i] > 0) denominator += weights[i] * Math.exp(scores[i] - max);
        for (int i = 0; i < n; i++) {
            final Map<String, Object> row = rows.get(i);
            if (!active[i] || denominator <= 0 || Double.isNaN(denominator)) {
                row.put(name, null);
            } else {
                row.put(name, weights[i] == 0 ? 0d : weights[i] * Math.exp(scores[i] - max) / denominator);
            }
        }
    }

    /**
     * The group solvers ({@link GroupOps}): the op's fields become one vector per channel over the rows of the group
     * (a missing or non-finite value is NaN), the solver returns every row's value (NaN = null). The places of a
     * harville are one pass per (field, discount, bound) — the {@code top} columns of a group read the same one.
     */
    private void solve(final String name, final Plan plan, final List<Map<String, Object>> rows) {
        final double[] result;
        if (plan.residualize() != null) {
            final String[] against = plan.residualize().against();
            final double[][] x = new double[against.length][];
            for (int k = 0; k < against.length; k++) x[k] = channel(rows, against[k]);
            result = GroupOps.residualize(channel(rows, plan.field()), x, plan.excludeSelf());
        } else {
            result = harvillePlaces(plan, rows)[plan.harville().top() - 1];
        }
        for (int i = 0; i < rows.size(); i++) rows.get(i).put(name, Double.isNaN(result[i]) ? null : (Object) result[i]);
    }

    /**
     * The harville pass this column reads, computed once per group: a new group arrives as a new list, so the cache
     * is dropped the moment the identity changes. A column asking for a place beyond what the cached pass holds
     * (only possible when {@link #setup()} has not seen every column) recomputes it.
     */
    private double[][] harvillePlaces(final Plan plan, final List<Map<String, Object>> rows) {
        if (cachedGroup != rows) {
            cachedGroup = rows;
            harvilleCache = new HashMap<>();
        }
        final Harville harville = plan.harville();
        double[][] cached = harvilleCache.get(harville.key());
        if (cached == null || cached.length < harville.top()) {
            final int maxTop = Math.max(harville.top(), places == null ? 0 : places.getOrDefault(harville.key(), 0));
            cached = GroupOps.harvillePlaces(channel(rows, plan.field()), maxTop, harville.discount(), harville.maxGroupSize());
            harvilleCache.put(harville.key(), cached);
        }
        return cached;
    }

    /**
     * Placebo permutation: the field's values are reassigned across the group by a permutation drawn from
     * hash(seed, group key), applied to the rows ordered by (time.field, orderTieBreak) — so the multiset of values
     * per group is preserved and the result is a pure function of the group.
     */
    static void shuffle(final OutputColumn c, final List<Map<String, Object>> rows) {
        final Plan plan = plan(c);
        shuffle(c.canonicalName, plan.field(), plan.shuffle(), rows);
    }

    static void shuffle(final String name, final String field, final Shuffle plan, final List<Map<String, Object>> rows) {
        final List<String> order = plan.order();
        final int n = rows.size();
        final Integer[] sorted = new Integer[n];
        for (int i = 0; i < n; i++) sorted[i] = i;
        Arrays.sort(sorted, (a, b) -> compareIdentity(rows.get(a), rows.get(b), order));
        final String groupKey = n == 0 ? "" : FeatureValues.keyWithNullTokens(rows.get(0), plan.contextKeys());
        final java.util.SplittableRandom random = FeatureValues.seededRandom(plan.seed(), groupKey);
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
        for (int i = 0; i < n; i++) rows.get(sorted[i]).put(name, values.get(permutation[i]));
    }

    private static double[] channel(final List<Map<String, Object>> rows, final String field) {
        final double[] values = new double[rows.size()];
        for (int i = 0; i < values.length; i++) {
            final Double d = FeatureValues.toDouble(rows.get(i).get(field));
            values[i] = d == null || !Double.isFinite(d) ? Double.NaN : d;
        }
        return values;
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
