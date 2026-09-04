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
 * Evaluates row-scope columns (expr / datetime / bin / cross / residual / row baselines / null indicators)
 * in place on a primitive row map. Stateless; compiled expressions are rebuilt in {@link #setup()}.
 */
public class RowEvaluator implements Serializable {

    private final List<OutputColumn> columns;
    private transient Map<String, ExpressionUtil.Expression> expressions;

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

    /** Whether any composed column of this evaluator needs variance-components pseudo-counts. */
    public boolean needsVarianceComponents() {
        return columns.stream().anyMatch(c -> "varianceComponents".equals(c.coordinates.get("weights")));
    }

    private Map<String, Double> lambdasFor(final OutputColumn c) {
        return "varianceComponents".equals(c.coordinates.get("weights")) ? lambdas : null;
    }

    public void setup() {
        expressions = new HashMap<>();
        lattices = new HashMap<>();
        shrinkages = new HashMap<>();
        for (final OutputColumn c : columns) {
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
                            Boolean.parseBoolean(c.coordinates.get("leaveNodeOut"))));
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
                yield v == null ? null : (c.coordinates.get("value").equals(v.toString()) ? 1L : 0L);
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
            case "compose" -> shrinkages.get(c.canonicalName).compose(row, lattices.get(c.canonicalName), lambdasFor(c)).value();
            case "deviation" -> {
                final Shrinkage.Composition composition = shrinkages.get(c.canonicalName).compose(row, lattices.get(c.canonicalName), lambdasFor(c));
                yield composition.deviations()[Integer.parseInt(c.coordinates.get("level"))];
            }
            case "effectiveN" -> shrinkages.get(c.canonicalName).compose(row, lattices.get(c.canonicalName), lambdasFor(c)).effectiveN();
            default -> throw new IllegalStateException("unsupported row operator: " + c.operator);
        };
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
