package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureSpec.Scope;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for the operators the DSL accepts (docs/design/feature-dsl.md §8 contract point (1)):
 * name, scope, input signature, output type and whether a fit is involved. The compiler validates
 * against it; the generative DSL builds its search space from it.
 */
public final class OperatorCatalog {

    public enum InputKind {
        /** no input field (constant / group-level) */ none,
        /** numeric field */ numeric,
        /** categorical (string / enum / int) field */ categorical,
        /** any field type; output type follows the input */ any,
        /** boolean predicate over fields */ predicate
    }

    public record Operator(Scope scope, String name, InputKind input, Schema.FieldType output, boolean fit, String description) {
        /** Output type: null in the catalog means "same as input". */
        public Schema.FieldType outputFor(final Schema.FieldType inputType) {
            return output != null ? output : inputType;
        }
    }

    private static final Map<String, Operator> OPERATORS = new LinkedHashMap<>();

    private static void register(final Scope scope, final String name, final InputKind input,
                                 final Schema.FieldType output, final boolean fit, final String description) {
        OPERATORS.put(scope + "." + name, new Operator(scope, name, input, output, fit, description));
    }

    private static final Schema.FieldType F64 = Schema.FieldType.FLOAT64;
    private static final Schema.FieldType I64 = Schema.FieldType.INT64;

    static {
        // row
        register(Scope.row, "expr", InputKind.numeric, F64, false, "numeric expression over the row");
        register(Scope.row, "datetime", InputKind.any, I64, false, "calendar decomposition (month, dayOfWeek, weekOfYear, hour, dayOfYear); cyclical → sin/cos float64");
        register(Scope.row, "bin", InputKind.numeric, I64, false, "fixed-edge discretization");
        register(Scope.row, "cross", InputKind.categorical, Schema.FieldType.STRING, false, "categorical cross product");
        register(Scope.row, "residual", InputKind.numeric, F64, false, "difference to a named baseline on identity / logit / log scale");
        register(Scope.row, "indicator", InputKind.categorical, I64, false, "one 0/1 column per listed value of a categorical field");
        register(Scope.row, "equals", InputKind.any, I64, false, "1 when two fields are equal, 0 otherwise (null if either is null)");
        register(Scope.row, "noise", InputKind.none, F64, false, "placebo: deterministic pseudo-random value from the row identity and a seed (normal | uniform)");
        register(Scope.row, "vector", InputKind.numeric, F64, false, "scalar readouts (funcs) of a numeric array field after optional slice / diff / normalize steps");

        // context
        register(Scope.context, "rank", InputKind.numeric, I64, false, "rank within the group (1 = largest)");
        register(Scope.context, "zscore", InputKind.numeric, F64, false, "(x - mean) / std within the group");
        register(Scope.context, "gapToBest", InputKind.numeric, F64, false, "x - max within the group");
        register(Scope.context, "shareOfTotal", InputKind.numeric, F64, false, "x / sum within the group");
        register(Scope.context, "percentile", InputKind.numeric, F64, false, "empirical percentile within the group");
        register(Scope.context, "median_diff", InputKind.numeric, F64, false, "x - median within the group");
        register(Scope.context, "share", InputKind.numeric, F64, false, "alias of shareOfTotal (baselines)");
        register(Scope.context, "groupSize", InputKind.none, I64, false, "number of rows in the group");
        register(Scope.context, "countByValue", InputKind.categorical, Schema.FieldType.map(I64), false, "count per value within the group");
        register(Scope.context, "ratioByValue", InputKind.categorical, Schema.FieldType.map(F64), false, "ratio per value within the group");
        register(Scope.context, "entropy", InputKind.categorical, F64, false, "entropy of the value distribution within the group");
        register(Scope.context, "softmax", InputKind.numeric, F64, false, "probability within the group: offset * exp(score / temperature), normalised over the group");
        register(Scope.context, "shuffle", InputKind.any, null, false, "placebo: the field's values permuted within the group (deterministic from seed and group key)");

        // sequence (deterministic, strictly-past window)
        register(Scope.sequence, "lag", InputKind.any, null, false, "value k events back");
        register(Scope.sequence, "delta", InputKind.numeric, F64, false, "difference between lag k and lag k+1");
        register(Scope.sequence, "trend", InputKind.numeric, F64, false, "regression slope over the last k events");
        register(Scope.sequence, "ewma", InputKind.numeric, F64, false, "exponentially weighted moving average (halflife, decayBy events|time)");
        register(Scope.sequence, "runLength", InputKind.any, I64, false, "length of the trailing run equal to value");
        register(Scope.sequence, "sinceEvent", InputKind.predicate, null, false, "events / days since the predicate last held");
        register(Scope.sequence, "countMatch", InputKind.predicate, I64, false, "number of past rows where the predicate holds");
        register(Scope.sequence, "aggregate", InputKind.numeric, null, false, "count / mean / min / max / sum / std over the window");
        register(Scope.sequence, "regression", InputKind.numeric, F64, false, "two-series statistics of field regressed against another field over the window: cov / corr / beta / intercept / r2; lag pairs the field with the other series k events earlier (lead-lag)");
        register(Scope.sequence, "fracdiff", InputKind.numeric, F64, false, "fractional difference of order d over the last k events (fixed-width truncation)");

        // population (fit)
        register(Scope.population, "encoding", InputKind.any, F64, true, "shrinkage-smoothed conditional statistics over structured key space");
        register(Scope.population, "spectralEmbedding", InputKind.categorical, F64, true, "co-occurrence operator + PPMI + truncated SVD (v1)");
        register(Scope.population, "transitionStats", InputKind.categorical, F64, true, "transition operator statistics (v1)");
        register(Scope.population, "svd", InputKind.numeric, F64, true, "truncated SVD / PCA scores of a numeric vector (fields or an array), fitted on the whole input");
        register(Scope.population, "quantileTransform", InputKind.numeric, F64, true, "empirical CDF position (or normal score) of a value, quantile knots fitted on the whole input");
        register(Scope.population, "discretize", InputKind.numeric, I64, true, "fitted discretization (v1)");
        register(Scope.population, "factorization", InputKind.categorical, F64, true, "factorization machine (v1)");
    }

    private OperatorCatalog() {}

    public static Operator get(final Scope scope, final String name) {
        return OPERATORS.get(scope + "." + name);
    }

    public static List<Operator> all() {
        return List.copyOf(OPERATORS.values());
    }

    /** Aggregate functions accepted by sequence.aggregate and their output types. */
    public static Schema.FieldType aggregateOutput(final String func, final Schema.FieldType inputType) {
        return switch (func) {
            case "count" -> I64;
            case "mean", "avg", "std", "sum", "rate" -> F64;
            case "min", "max", "last", "first" -> inputType;
            default -> null;
        };
    }

    /** The readouts of the row {@code vector} op ({@link VectorOps#read}; {@code polyfit} expands to one column per coefficient). */
    public static final List<String> VECTOR_FUNCS = List.of("length", "sum", "mean", "std", "min", "max", "argmin", "argmax", "first", "last", "slope", "norm", "polyfit");

    /** The vector → vector rescalings of the row {@code vector} op ({@link VectorOps#normalize}). */
    public static final List<String> VECTOR_NORMALIZATIONS = List.of("sum", "mean", "l2", "zscore");

    /** Output type of a {@code vector} readout, or null for an unknown one. */
    public static Schema.FieldType vectorOutput(final String func) {
        if (!VECTOR_FUNCS.contains(func)) return null;
        return switch (func) {
            case "length", "argmin", "argmax" -> I64;
            default -> F64;
        };
    }

    /**
     * The aggregate functions defined under a per-event weight ({@code weightBy}): {@code count} reads Σw (the
     * effective count, FLOAT64), {@code sum} Σw·x, {@code mean} Σw·x / Σw, {@code std} the weighted population
     * deviation. Order / extreme statistics ({@code min / max / first / last}) have no weighted form.
     */
    public static final List<String> WEIGHTED_FUNCS = List.of("count", "sum", "mean", "avg", "rate", "std");

    /** Output type of an aggregate function under {@code weightBy}, or null when it has no weighted form. */
    public static Schema.FieldType weightedAggregateOutput(final String func) {
        return WEIGHTED_FUNCS.contains(func) ? F64 : null;
    }

    /**
     * {@link #summary(String)} for a statistic that may carry a per-event weight. A weight may read the current row
     * ({@code $self}): it is then a different number for every (row, event) pair, so no running state — nothing
     * folded once per event — can serve it, whatever the family. Weighted statistics are therefore scan-only; a
     * weight over the event alone would fit a weighted-moments family, which does not exist yet.
     */
    public static Summary.Spec summary(final String stat, final boolean weighted) {
        return weighted ? null : summary(stat);
    }

    /**
     * Encoding statistics: whether a target is required, the output type, and whether the statistic is
     * derived from the sufficient statistics (n, Σy, Σy²) — the ones a static / fold fit keeps per key.
     * {@code distribution} and the quantiles need the key's value distribution (expanding only).
     */
    public record Stat(String name, boolean requiresTarget, Schema.FieldType output, boolean sufficient) {}

    /** The stat tokens a target may request (plus the {@code quantile<NN>} / {@code q<NN>} family). */
    public static final List<String> STATS = List.of("count", "share", "mean", "rate", "std", "distribution", "quantile");

    public static final String AVAILABLE_STATS = String.join(" | ", STATS) + " (median) | quantile<NN> / q<NN>";

    public static Stat stat(final String name) {
        if (name == null) return null;
        return switch (name) {
            case "count" -> new Stat(name, false, I64, true);
            case "share" -> new Stat(name, false, F64, true);
            case "mean", "rate", "std" -> new Stat(name, true, F64, true);
            case "distribution" -> new Stat(name, true, Schema.FieldType.map(F64), false);
            default -> quantileProbability(name) == null ? null : new Stat(name, true, F64, false);
        };
    }

    /**
     * The {@link Summary} family a statistic token runs on incrementally — the single place that decides which
     * statistics the keyed replay can serve from running state (and, being monoids, which can be combined per
     * block or per partition): {@code count / sum / mean / avg / rate / std} → moments, {@code max / min} →
     * extrema (not invertible: scan under a window), {@code distribution} → value counts, the quantile tokens →
     * exact order statistics, {@code cov / corr / beta / intercept / r2} → the cross moments of a pair (a lagged pairing is
     * not a per-event contribution and stays on the scan path, see {@code SequenceEvaluator.summaryOf}). Null for a token without a family ({@code share}, {@code first} / {@code last}, an
     * unknown token): such a statistic is scan-only.
     */
    public static Summary.Spec summary(final String stat) {
        if (stat == null) return null;
        return switch (stat) {
            case "count", "sum", "mean", "avg", "rate", "std" -> new Summary.Spec(Summary.Summaries.MOMENTS, Summary.Readout.of(stat));
            case "max", "min" -> new Summary.Spec(Summary.Summaries.EXTREMA, Summary.Readout.of(stat));
            case "distribution" -> new Summary.Spec(Summary.Summaries.COUNTS, Summary.Readout.of(stat));
            case "cov", "corr", "beta", "intercept", "r2" -> new Summary.Spec(Summary.Summaries.REGRESSION, Summary.Readout.of(stat));
            default -> {
                final Double p = quantileProbability(stat);
                yield p == null ? null : new Summary.Spec(Summary.Summaries.ORDER, Summary.Readout.of("quantile", p));
            }
        };
    }

    private static final java.util.regex.Pattern QUANTILE = java.util.regex.Pattern.compile("^(?:quantile|q)(\\d{1,3})$");

    /**
     * The probability of a quantile stat token — {@code quantile} (the median), {@code quantile<NN>} or
     * {@code q<NN>} with NN a percentage 0..100 (e.g. {@code q90}) — or null for any other stat.
     */
    public static Double quantileProbability(final String stat) {
        if (stat == null) return null;
        if ("quantile".equals(stat)) return 0.5;
        final java.util.regex.Matcher m = QUANTILE.matcher(stat);
        if (!m.matches()) return null;
        final int percent = Integer.parseInt(m.group(1));
        return percent > 100 ? null : percent / 100d;
    }

    /** The readouts of the sequence {@code regression} op (the {@link Summary.Regression} family). */
    public static final List<String> REGRESSION_FUNCS = List.of("cov", "corr", "beta", "intercept", "r2");

    public static List<String> datetimeDerivations() {
        return List.of("year", "month", "day", "dayOfWeek", "dayOfYear", "weekOfYear", "hour", "minute");
    }

    /** Population types implemented by the engine; the other registered ones parse but fail compilation. */
    public static final List<String> IMPLEMENTED_POPULATION_TYPES = List.of("encoding", "factorization", "discretize", "quantileTransform", "svd");

    public static boolean isImplemented(final Scope scope, final String name) {
        if (scope != Scope.population) return get(scope, name) != null;
        return IMPLEMENTED_POPULATION_TYPES.contains(name);
    }

    public static boolean isNumeric(final Schema.FieldType type) {
        if (type == null) return false;
        return switch (type.getType()) {
            case int8, int16, int32, int64, float16, float32, float64, decimal -> true;
            default -> false;
        };
    }

    public static boolean isCategorical(final Schema.FieldType type) {
        if (type == null) return false;
        return switch (type.getType()) {
            case string, enumeration, bool, int8, int16, int32, int64 -> true;
            default -> false;
        };
    }

}
