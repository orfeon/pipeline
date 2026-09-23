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

    /**
     * @param baselineCallable the op may be called as a function from a {@code baselines[].expr}
     *                         ({@code share(1 / bid)}): it reads one value per row of the group and returns one
     *                         number per row. An op that needs coordinates of its own (softmax, ratingProb, shuffle and the
     *                         group solvers) is not callable that way — {@link ContextEvaluator} has no place to
     *                         take them from, so the compiler rejects the call instead (baselines.expr.op).
     */
    public record Operator(Scope scope, String name, InputKind input, Schema.FieldType output, boolean fit,
                           boolean baselineCallable, String description) {
        /** Output type: null in the catalog means "same as input". */
        public Schema.FieldType outputFor(final Schema.FieldType inputType) {
            return output != null ? output : inputType;
        }
    }

    private static final Map<String, Operator> OPERATORS = new LinkedHashMap<>();

    private static void register(final Scope scope, final String name, final InputKind input,
                                 final Schema.FieldType output, final boolean fit, final String description) {
        register(scope, name, input, output, fit, false, description);
    }

    /** A context op that a baseline expression may call as a function (see {@link Operator#baselineCallable()}). */
    private static void registerCallable(final Scope scope, final String name, final InputKind input,
                                         final Schema.FieldType output, final String description) {
        register(scope, name, input, output, false, true, description);
    }

    private static void register(final Scope scope, final String name, final InputKind input,
                                 final Schema.FieldType output, final boolean fit, final boolean baselineCallable,
                                 final String description) {
        OPERATORS.put(scope + "." + name, new Operator(scope, name, input, output, fit, baselineCallable, description));
    }

    private static final Schema.FieldType F64 = Schema.FieldType.FLOAT64;
    private static final Schema.FieldType I64 = Schema.FieldType.INT64;

    static {
        // row
        register(Scope.row, "expr", InputKind.numeric, F64, false, "numeric expression over the row");
        register(Scope.row, "datetime", InputKind.any, I64, false, "calendar decomposition (month, dayOfWeek, weekOfYear, hour, dayOfYear); cyclical -> sin/cos float64");
        register(Scope.row, "bin", InputKind.numeric, I64, false, "fixed-edge discretization");
        register(Scope.row, "cross", InputKind.categorical, Schema.FieldType.STRING, false, "categorical cross product");
        register(Scope.row, "residual", InputKind.numeric, F64, false, "difference to a named baseline on identity / logit / log scale");
        register(Scope.row, "indicator", InputKind.categorical, I64, false, "one 0/1 column per listed value of a categorical field");
        register(Scope.row, "equals", InputKind.any, I64, false, "1 when two fields are equal, 0 otherwise (null if either is null)");
        register(Scope.row, "noise", InputKind.none, F64, false, "placebo: deterministic pseudo-random value from the row identity and a seed (normal | uniform)");
        register(Scope.row, "vector", InputKind.numeric, F64, false, "scalar readouts (funcs) of a numeric array field after optional slice / diff / normalize steps");

        // context
        registerCallable(Scope.context, "rank", InputKind.numeric, I64, "rank within the group (1 = largest)");
        registerCallable(Scope.context, "zscore", InputKind.numeric, F64, "(x - mean) / std within the group");
        registerCallable(Scope.context, "gapToBest", InputKind.numeric, F64, "x - max within the group");
        registerCallable(Scope.context, "shareOfTotal", InputKind.numeric, F64, "x / sum within the group");
        registerCallable(Scope.context, "percentile", InputKind.numeric, F64, "empirical percentile within the group");
        registerCallable(Scope.context, "median_diff", InputKind.numeric, F64, "x - median within the group");
        registerCallable(Scope.context, "share", InputKind.numeric, F64, "alias of shareOfTotal (baselines)");
        registerCallable(Scope.context, "groupSize", InputKind.none, I64, "number of rows in the group");
        register(Scope.context, "countByValue", InputKind.categorical, Schema.FieldType.map(I64), false, "count per value within the group");
        register(Scope.context, "ratioByValue", InputKind.categorical, Schema.FieldType.map(F64), false, "ratio per value within the group");
        registerCallable(Scope.context, "entropy", InputKind.categorical, F64, "entropy of the value distribution within the group");
        register(Scope.context, "softmax", InputKind.numeric, F64, false, "probability within the group: offset * exp(score / temperature), normalised over the group");
        register(Scope.context, "ratingProb", InputKind.numeric, F64, false, "the probability a rating model gives the row within the group: exp(mu / c) normalised over the group, c = sqrt(sum(sigma^2 + beta^2)) over the group (the Plackett-Luce contest of a rating)");
        register(Scope.context, "residualize", InputKind.numeric, F64, false, "residual of the field regressed (with an intercept) on the 'against' fields over the rows of the group; excludeSelf fits on the other rows");
        register(Scope.context, "harville", InputKind.numeric, F64, false, "probability of finishing within the first k places (top: [2, 3]) from win probabilities, by the Harville forward computation (discount: exponents for the 2nd / 3rd place)");
        register(Scope.context, "shuffle", InputKind.any, null, false, "placebo: the field's values permuted within the group (deterministic from seed and group key)");

        // sequence (deterministic; a strictly-past window, or a strictly-future one for labels)
        register(Scope.sequence, "barrier", InputKind.numeric, I64, false, "future windows only: 1 / -1 when the path first moves up / down by the barrier from the current row's value, 0 when neither is touched within the window");
        register(Scope.sequence, "lag", InputKind.any, null, false, "value k events back");
        register(Scope.sequence, "delta", InputKind.numeric, F64, false, "difference between lag k and lag k+1");
        register(Scope.sequence, "trend", InputKind.numeric, F64, false, "regression slope over the last k events");
        register(Scope.sequence, "ewma", InputKind.numeric, F64, false, "exponentially weighted moving average (halflife, decayBy events|time|a declared calendar clock)");
        register(Scope.sequence, "runLength", InputKind.any, I64, false, "length of the trailing run equal to value");
        register(Scope.sequence, "sinceEvent", InputKind.predicate, null, false, "events / days since the predicate last held");
        register(Scope.sequence, "countMatch", InputKind.predicate, I64, false, "number of past rows where the predicate holds");
        register(Scope.sequence, "aggregate", InputKind.numeric, null, false, "count / mean / min / max / sum / std over the window");
        register(Scope.sequence, "regression", InputKind.numeric, F64, false, "two-series statistics of field regressed against another field over the window: cov / corr / beta / intercept / r2; lag pairs the field with the other series k events earlier (lead-lag)");
        register(Scope.sequence, "fracdiff", InputKind.numeric, F64, false, "fractional difference of order d over the last k events (fixed-width truncation)");
        register(Scope.sequence, "rating", InputKind.numeric, F64, false, "sequential rating of the entity from the contests it took part in (method elo | bradleyTerry | plackettLuce; field = the outcome, context = the contest; funcs mu | sigma | count | delta; bradleyTerry pairs all | adjacent | mean; tauPer = the period tau is the drift of instead of one contest): every contest moves all its players at once, so the state is not mergeable - one replay per pool (global key, or the partition of a reduced window filter)");
        register(Scope.sequence, "dynamics", InputKind.numeric, F64, false, "general form lift -> summarize.dynamics (lti: exponential | legendre | fourier measure, order, halflife / period, decayBy events|time|a declared calendar clock): one column per state component");

        // population (fit)
        register(Scope.population, "encoding", InputKind.any, F64, true, "shrinkage-smoothed conditional statistics over structured key space");
        register(Scope.population, "spectralEmbedding", InputKind.categorical, F64, true, "coordinates of a categorical state from the values around it in an entity's sequence (pair counts, PPMI, symmetric factorisation)");
        register(Scope.population, "transitionStats", InputKind.categorical, F64, true, "distribution of an entity's next value given its previous one(s): an expanding, shrunk distribution encoding over the lag path");
        register(Scope.population, "svd", InputKind.numeric, F64, true, "truncated SVD / PCA scores of a numeric vector (fields or an array), fitted on the whole input");
        register(Scope.population, "quantileTransform", InputKind.numeric, F64, true, "empirical CDF position (or normal score) of a value, quantile knots fitted on the whole input");
        register(Scope.population, "discretize", InputKind.numeric, I64, true, "fitted discretization (v1)");
        register(Scope.population, "smooth", InputKind.numeric, F64, true, "smooth curve of a target over a numeric key (penalised B-splines, strength by REML), solved from the input's moments");
        register(Scope.population, "factorization", InputKind.categorical, F64, true, "factorization machine (v1)");
    }

    private OperatorCatalog() {}

    public static Operator get(final Scope scope, final String name) {
        return OPERATORS.get(scope + "." + name);
    }

    /** A context op called as a function from a baseline expression, with the expression it is applied to. */
    public record Call(Operator operator, String arguments) {}

    private static final java.util.regex.Pattern CALL = java.util.regex.Pattern.compile("^\\s*([A-Za-z_]+)\\s*\\((.*)\\)\\s*$");

    /**
     * The context op a {@code baselines[].expr} calls ({@code share(1 / bid)} -> share applied to {@code 1 / bid}),
     * or null when the expression is not a single call of a catalogued context op (an ordinary expression, or a call
     * of an {@link com.mercari.solution.util.ExpressionUtil} function). The compiler and {@link ContextEvaluator}
     * read the same parse, so a call the evaluator cannot route is rejected at compile time.
     */
    public static Call parseContextCall(final String expression) {
        if (expression == null) return null;
        final java.util.regex.Matcher m = CALL.matcher(expression);
        if (!m.matches()) return null;
        final Operator operator = get(Scope.context, m.group(1));
        return operator == null ? null : new Call(operator, m.group(2));
    }

    public static List<Operator> all() {
        return List.copyOf(OPERATORS.values());
    }

    /** What sequence.aggregate accepts, for the "unknown func" message. */
    public static final String AVAILABLE_AGGREGATES = "count | sum | mean | avg | rate | std | skew | kurt | min | max | first | last | zeroCross | peaks"
            + " | acf<j> | pacf<j> | ar<p>_<i> (j, p up to " + SeriesStats.MAX_LAG + ")";

    /**
     * Aggregate functions accepted by sequence.aggregate and their output types: the moments, the shape of the
     * distribution ({@code skew} / {@code kurt}), the extremes and ends, and the order-dependent series readouts of
     * {@link SeriesStats} ({@code zeroCross}, {@code peaks}, {@code acf<j>}, {@code pacf<j>}, {@code ar<p>_<i>}).
     */
    public static Schema.FieldType aggregateOutput(final String func, final Schema.FieldType inputType) {
        return switch (func) {
            case "count" -> I64;
            case "mean", "avg", "std", "sum", "rate", "skew", "kurt" -> F64;
            case "min", "max", "last", "first" -> inputType;
            default -> {
                final SeriesStats.Readout series = SeriesStats.parse(func);
                yield series == null ? null : SeriesStats.isCount(series) ? I64 : F64;
            }
        };
    }

    /** The readouts of the row {@code vector} op ({@link VectorOps#read}; {@code polyfit} expands to one column per coefficient). */
    public static final List<String> VECTOR_FUNCS = List.of("length", "sum", "mean", "std", "min", "max", "argmin", "argmax", "first", "last", "slope", "norm", "polyfit", "vector");

    /** The vector → vector rescalings of the row {@code vector} op ({@link VectorOps#normalize}). */
    public static final List<String> VECTOR_NORMALIZATIONS = List.of("sum", "mean", "l2", "zscore");

    /** Output type of a {@code vector} readout, or null for an unknown one. */
    public static Schema.FieldType vectorOutput(final String func) {
        if (!VECTOR_FUNCS.contains(func)) return null;
        return switch (func) {
            case "length", "argmin", "argmax" -> I64;
            case "vector" -> Schema.FieldType.array(F64);
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
     * block or per partition): {@code count / sum / mean / avg / rate / std} → moments, {@code skew / kurt} → the
     * power sums up to order four, {@code max / min} →
     * extrema (not invertible: scan under a window), {@code distribution} → value counts, the quantile tokens →
     * exact order statistics, {@code cov / corr / beta / intercept / r2} → the cross moments of a pair (a lagged pairing is
     * not a per-event contribution and stays on the scan path, see {@code SequenceEvaluator.summaryOf}). Null for a token without a family ({@code share}, {@code first} / {@code last}, an
     * unknown token): such a statistic is scan-only.
     */
    public static Summary.Spec summary(final String stat) {
        if (stat == null) return null;
        return switch (stat) {
            case "count", "sum", "mean", "avg", "rate", "std" -> new Summary.Spec(Summary.Summaries.MOMENTS, Summary.Readout.of(stat));
            case "skew", "kurt" -> new Summary.Spec(Summary.Summaries.SHAPE, Summary.Readout.of(stat));
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

    /**
     * The sequence ops a {@code direction: future} window accepts: those whose value does not depend on reading the
     * window forwards or backwards (moments, counts, extremes, distance decay), plus the ones that read naturally
     * from the current row outwards — {@code lag} (the k-th next event), {@code sinceEvent} (until), {@code runLength}
     * (the run starting next), {@code aggregate first / last} (nearest / furthest) — and the label op {@code barrier}.
     */
    public static final List<String> FUTURE_OPS = List.of("aggregate", "lag", "ewma", "sinceEvent", "countMatch", "runLength", "regression", "barrier");

    /** The readouts of the sequence {@code regression} op (the {@link Summary.Regression} family). */
    public static final List<String> REGRESSION_FUNCS = List.of("cov", "corr", "beta", "intercept", "r2");

    public static List<String> datetimeDerivations() {
        return List.of("year", "month", "day", "dayOfWeek", "dayOfYear", "weekOfYear", "hour", "minute");
    }

    /** Population types implemented by the engine; the other registered ones parse but fail compilation. */
    public static final List<String> IMPLEMENTED_POPULATION_TYPES = List.of("encoding", "factorization", "discretize", "quantileTransform", "svd", "smooth", "transitionStats", "spectralEmbedding");

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
