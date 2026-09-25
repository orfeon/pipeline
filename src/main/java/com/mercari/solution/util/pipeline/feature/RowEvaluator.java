package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.ExpressionUtil;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Evaluates row-scope columns (expr / datetime / bin / cross / residual / vector / row baselines / null indicators)
 * in place on a primitive row map. Stateless; compiled expressions are rebuilt in {@link #setup()}.
 */
public class RowEvaluator implements Serializable {

    private final List<OutputColumn> columns;
    private transient Map<String, ExpressionUtil.Expression> expressions;
    private transient Map<String, VectorOps.Plan> vectors;

    public RowEvaluator(final List<OutputColumn> columns) {
        this.columns = columns;
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public List<OutputColumn> getColumns() {
        return columns;
    }

    private transient Map<String, List<Shrinkage.Level>> lattices;
    private transient Map<String, Shrinkage> shrinkages;
    /** Per-level pseudo-counts from the variance-components side input (null → declared priorWeight). */
    private transient Map<String, Double> lambdas;

    public void setLambdas(final Map<String, Double> lambdas) {
        this.lambdas = lambdas;
    }

    /** The current variance-components pseudo-counts (never null). */
    public Map<String, Double> lambdas() {
        return lambdas == null ? Map.of() : lambdas;
    }

    /**
     * Whether any composed column of this evaluator reads variance-components pseudo-counts from the stage's λ
     * estimate: a lattice column (hidden {@code levels}). A joint column also declares the weights, but its
     * pseudo-counts are estimated inside the fit's solve and never read here.
     */
    public boolean needsVarianceComponents() {
        return columns.stream().anyMatch(c -> "varianceComponents".equals(c.coordinates.get("weights")) && c.coordinates.containsKey("levels"));
    }

    private Map<String, Double> lambdasFor(final OutputColumn c) {
        return "varianceComponents".equals(c.coordinates.get("weights")) ? lambdas : null;
    }

    public void setup() {
        expressions = new HashMap<>();
        lattices = new HashMap<>();
        shrinkages = new HashMap<>();
        vectors = new HashMap<>();
        for (final OutputColumn c : columns) {
            if ("vector".equals(c.operator)) vectors.put(c.canonicalName, VectorOps.Plan.of(c.coordinates));
            final String expr = c.coordinates.get("expr");
            if (expr != null && ("expr".equals(c.operator) || "baseline".equals(c.operator))) {
                expressions.put(c.canonicalName, ExpressionUtil.createDefaultExpression(expr));
            }
            final String levels = c.coordinates.get("levels");
            if (levels != null) {
                lattices.put(c.canonicalName, Shrinkage.parseLevels(levels));
                if (c.coordinates.containsKey("scale")) {
                    shrinkages.put(c.canonicalName, Shrinkage.of(
                            Shrinkage.Scale.valueOf(c.coordinates.get("scale")),
                            Double.parseDouble(c.coordinates.get("priorWeight")),
                            Boolean.parseBoolean(c.coordinates.get("leaveNodeOut")),
                            c.coordinates.containsKey("family") ? Shrinkage.Family.valueOf(c.coordinates.get("family")) : null));
                }
            }
        }
    }

    /**
     * Placebo draw: a 64-bit hash of (seed, row identity) seeds a {@link java.util.SplittableRandom}, so the value
     * is a pure function of the row (same across re-runs, workers and parallel branches) and carries no information.
     */
    static Object noise(final OutputColumn c, final Map<String, Object> row) {
        final List<String> identity = List.of(c.coordinates.get("identity").split(","));
        final String key = FeatureValues.keyWithNullTokens(row, identity);
        final long seed = Long.parseLong(c.coordinates.get("seed"));
        final java.util.SplittableRandom random = FeatureValues.seededRandom(seed, key);
        return "uniform".equals(c.coordinates.get("distribution")) ? random.nextDouble() : random.nextGaussian();
    }

    public void evaluate(final Map<String, Object> row) {
        for (final OutputColumn c : columns) {
            row.put(c.canonicalName, evaluateColumn(c, row));
        }
    }

    Object evaluateColumn(final OutputColumn c, final Map<String, Object> row) {
        final List<String> inputs = new ArrayList<>(c.inputs);
        return switch (c.operator) {
            case "expr", "baseline" -> {
                final ExpressionUtil.Expression expression = expressions.get(c.canonicalName);
                yield expression == null ? null : FeatureValues.evaluate(expression, row);
            }
            case "datetime" -> datetime(c, row.get(inputs.get(0)));
            case "bin" -> bin(c, FeatureValues.toDouble(row.get(inputs.get(0))));
            case "cross" -> {
                final StringBuilder sb = new StringBuilder();
                for (final String in : inputs) {
                    final Object v = row.get(in);
                    if (v == null) yield null;
                    if (!sb.isEmpty()) sb.append('|');
                    sb.append(v);
                }
                yield sb.toString();
            }
            case "indicator" -> {
                final Object v = row.get(inputs.get(0));
                yield v == null ? null : (FeatureValues.matchesDeclared(v, c.coordinates.get("value")) ? 1L : 0L);
            }
            case "equals" -> {
                final Object a = row.get(inputs.get(0));
                final Object b = row.get(inputs.get(1));
                if (a == null || b == null) yield null;
                if (a instanceof Number x && b instanceof Number y) yield x.doubleValue() == y.doubleValue() ? 1L : 0L;
                yield a.toString().equals(b.toString()) ? 1L : 0L;
            }
            case "residual" -> residual(c, row);
            case "isnull" -> row.get(c.coordinates.get("indicatorOf")) == null;
            case "copy" -> FeatureValues.toDouble(row.get(inputs.get(0)));
            case "noise" -> noise(c, row);
            case "vector" -> vectors.get(c.canonicalName).evaluate(row.get(inputs.get(0)));
            case "share" -> {
                final List<Shrinkage.Level> levels = lattices.get(c.canonicalName);
                final Double leaf = FeatureValues.toDouble(row.get(levels.get(0).nColumn()));
                final Double root = FeatureValues.toDouble(row.get(levels.get(1).nColumn()));
                yield leaf == null || root == null || root == 0 ? null : leaf / root;
            }
            case "fitStat" -> {
                // statistic derived from fitted leaf sufficient statistics (fit.mode static)
                final Shrinkage.Level leaf = lattices.get(c.canonicalName).get(0);
                final Double n = FeatureValues.toDouble(row.get(leaf.nColumn()));
                if (n == null || n == 0) yield "count".equals(c.coordinates.get("stat")) ? (Object) 0L : null;
                final Double sum = FeatureValues.toDouble(row.get(leaf.sumColumn()));
                yield switch (c.coordinates.get("stat")) {
                    case "count" -> n.longValue();
                    case "mean", "rate" -> sum == null ? null : sum / n;
                    case "std" -> {
                        final String sumSqColumn = leaf.nColumn().substring(0, leaf.nColumn().length() - "__n".length()) + "__sumsq";
                        final Double sumSq = FeatureValues.toDouble(row.get(sumSqColumn));
                        if (sum == null || sumSq == null || n < 2) yield null;
                        final double mean = sum / n;
                        yield Math.sqrt(Math.max(0, sumSq / n - mean * mean));
                    }
                    default -> throw new IllegalStateException("unsupported static stat: " + c.coordinates.get("stat"));
                };
            }
            case "compose" -> {
                final Shrinkage.Composition composition = composition(c, row);
                yield isDistribution(c) ? composition.distribution() : composition.value();
            }
            case "mapValue" -> {
                // one category's share of a distribution map (targets[].values); keys may be CharSequence after a coder round trip
                final Object m = row.get(inputs.get(0));
                if (!(m instanceof Map<?, ?> map)) yield null;
                yield shareOf(map, c.coordinates.get("value"), Boolean.parseBoolean(c.coordinates.get("numeric")));
            }
            case "mapReadout" -> {
                // a readout of a distribution map (transitionStats emit): the row's own value's share, its surprisal,
                // the map's entropy, or its probability-weighted mean over integer codes
                final Object m = row.get(inputs.get(0));
                if (!(m instanceof Map<?, ?> map) || map.isEmpty()) yield null;
                final String readout = c.coordinates.get("readout");
                switch (readout) {
                    case "ownValueProb", "surprisal" -> {
                        final Object own = row.get(c.coordinates.get("field"));
                        if (own == null) yield null;
                        // the map is keyed by the value's own string form (Summary.Counts / the scan path both key
                        // by toString), which is also how a toValueProb value is written: not ContextEvaluator.valueKey
                        final Double p = shareOf(map, own.toString(), false);
                        if (p == null) yield null;
                        yield "surprisal".equals(readout) ? (p > 0 ? Double.valueOf(-Math.log(p)) : null) : p;
                    }
                    case "entropy" -> {
                        double h = 0;
                        boolean any = false;
                        for (final Object v : map.values()) {
                            final Double p = FeatureValues.toDouble(v);
                            if (p == null || p <= 0) continue;
                            any = true;
                            h -= p * Math.log(p);
                        }
                        yield any ? Double.valueOf(h) : null;
                    }
                    case "expected" -> {
                        double e = 0;
                        boolean any = false;
                        for (final Map.Entry<?, ?> entry : map.entrySet()) {
                            final Double p = FeatureValues.toDouble(entry.getValue());
                            if (p == null || entry.getKey() == null) continue;
                            try {
                                e += Double.parseDouble(entry.getKey().toString()) * p;
                            } catch (final NumberFormatException notNumeric) {
                                yield null;
                            }
                            any = true;
                        }
                        yield any ? Double.valueOf(e) : null;
                    }
                    default -> throw new IllegalStateException("unsupported map readout: " + readout);
                }
            }
            case "deviation" -> composition(c, row).deviations()[Integer.parseInt(c.coordinates.get("level"))];
            case "effectiveN" -> composition(c, row).effectiveN();
            default -> throw new IllegalStateException("unsupported row operator: " + c.operator);
        };
    }

    /**
     * One category's share of a distribution map: 0 when the map holds no mass for it, null when the entry it holds
     * is not a number. Keys may be CharSequence after a coder round trip, hence the fallback over the entries, where
     * the exact text wins. Over a {@code numeric} target (a declared targets[].values / toValueProb on a numeric field)
     * a key also matches by its number: a float64 category is keyed by its text ({@code "1.0"}), which a declared
     * {@code 1} must find. Otherwise the text is exact (a string category {@code "1.0"} is not {@code "1"}; a row's own
     * value is the key's own text).
     */
    private static Double shareOf(final Map<?, ?> map, final String value, final boolean numeric) {
        Object v = map.get(value);
        if (v == null) {
            final Double number = numeric ? FeatureValues.toDouble(value) : null;
            Object byNumber = null;
            for (final Map.Entry<?, ?> e : map.entrySet()) {
                if (e.getKey() == null) continue;
                final String key = e.getKey().toString();
                if (value.equals(key)) {
                    v = e.getValue();
                    break;
                }
                if (number != null && byNumber == null) {
                    final Double k = FeatureValues.toDouble(key);
                    if (k != null && (k.doubleValue() == number || (k.isNaN() && number.isNaN()))) byNumber = e.getValue();
                }
            }
            if (v == null) v = byNumber;
        }
        return v == null ? Double.valueOf(0d) : FeatureValues.toDouble(v);
    }

    /** A Dirichlet-Multinomial column composes a category distribution instead of a scalar. */
    private static boolean isDistribution(final OutputColumn c) {
        return Shrinkage.Family.dirichletMultinomial.name().equals(c.coordinates.get("family"));
    }

    private Shrinkage.Composition composition(final OutputColumn c, final Map<String, Object> row) {
        final Shrinkage shrinkage = shrinkages.get(c.canonicalName);
        final List<Shrinkage.Level> levels = lattices.get(c.canonicalName);
        return isDistribution(c) ? shrinkage.composeDistribution(row, levels, lambdasFor(c)) : shrinkage.compose(row, levels, lambdasFor(c));
    }

    private static Object datetime(final OutputColumn c, final Object value) {
        final LocalDateTime dt = FeatureValues.toDateTime(value, c.coordinates.get("inputType"));
        if (dt == null) return null;
        final String derive = c.coordinates.get("derive");
        final long raw;
        final double period;
        switch (derive) {
            case "year" -> { raw = dt.getYear(); period = 0; }
            case "month" -> { raw = dt.getMonthValue(); period = 12; }
            case "day" -> { raw = dt.getDayOfMonth(); period = 31; }
            case "dayOfWeek" -> { raw = dt.getDayOfWeek().getValue(); period = 7; }
            case "dayOfYear" -> { raw = dt.getDayOfYear(); period = 366; }
            case "weekOfYear" -> { raw = dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR); period = 53; }
            case "hour" -> { raw = dt.getHour(); period = 24; }
            case "minute" -> { raw = dt.getMinute(); period = 60; }
            default -> throw new IllegalStateException("unsupported datetime derivation: " + derive);
        }
        final String trig = c.coordinates.get("trig");
        if (trig == null) return raw;
        if (period == 0) return (double) raw;
        final double angle = 2 * Math.PI * raw / period;
        return "sin".equals(trig) ? Math.sin(angle) : Math.cos(angle);
    }

    private static Object bin(final OutputColumn c, final Double value) {
        if (value == null) return null;
        final String edges = c.coordinates.get("edges").replace("[", "").replace("]", "");
        long index = 0;
        for (final String edge : edges.split(",")) {
            if (edge.isBlank()) continue;
            if (value >= Double.parseDouble(edge.trim())) index++;
            else break;
        }
        return index;
    }

    private static Object residual(final OutputColumn c, final Map<String, Object> row) {
        final List<String> inputs = new ArrayList<>(c.inputs);
        final Double x = FeatureValues.toDouble(row.get(inputs.get(0)));
        final Double b = FeatureValues.toDouble(row.get(inputs.get(1)));
        if (x == null || b == null) return null;
        final double r = switch (c.coordinates.getOrDefault("on", "identity")) {
            case "logit" -> logit(x) - logit(b);
            case "log" -> Math.log(x) - Math.log(b);
            default -> x - b;
        };
        return Double.isNaN(r) || Double.isInfinite(r) ? null : r;
    }

    private static double logit(final double p) {
        return Math.log(p / (1 - p));
    }

}
