package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.feature.SourceContract.Json;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shrinkage configuration (docs/developer/feature-dsl.md §5.5) and the row-local composition of a generalization
 * lattice (§5.3.1): every level of the lattice has its own sufficient statistics per row
 * ({@code n}, {@code sum} over the key's past contributions, computed by hidden population stages) and the
 * final value is a top-down backoff from the global level to the leaf:
 *
 * <pre>
 *   est(root)  = t(mean_root)
 *   est(level) = est(parent) + w · (t(mean_level) − est(parent)),  w = n / (n + λ)
 * </pre>
 *
 * with leave-node-out (the leaf's own statistics are subtracted from every ancestor) and, for an
 * {@code additive} entry, the parent being {@code est(root) + Σ main-effect deviations} (sequential
 * estimator). {@code t} is the declared scale (identity / logit / log); the composed value is returned on
 * the original scale, deviations on the transform scale.
 */
public final class Shrinkage implements Serializable {

    public enum Estimator { backoff, sequential }
    public enum Scale { identity, logit, log }

    /** Default pseudo-count for fixed weights when none is declared. */
    public static final double DEFAULT_PRIOR_WEIGHT = 20d;

    public static final String ADDITIVE = "additive";
    public static final String GLOBAL = "global";

    public final boolean enabled;
    public final Estimator estimator;
    public final String weights;
    public final double priorWeight;
    public final Scale scale;
    public final boolean leaveNodeOut;
    public final List<String> outputs;

    private Shrinkage(final boolean enabled, final Estimator estimator, final String weights, final double priorWeight,
                      final Scale scale, final boolean leaveNodeOut, final List<String> outputs) {
        this.enabled = enabled;
        this.estimator = estimator;
        this.weights = weights;
        this.priorWeight = priorWeight;
        this.scale = scale;
        this.leaveNodeOut = leaveNodeOut;
        this.outputs = outputs;
    }

    /** Runtime instance rebuilt from a composed column's coordinates. */
    public static Shrinkage of(final Scale scale, final double priorWeight, final boolean leaveNodeOut) {
        return new Shrinkage(true, null, "fixed", priorWeight, scale, leaveNodeOut, List.of("composed"));
    }

    public static Shrinkage disabled() {
        return new Shrinkage(false, null, "fixed", DEFAULT_PRIOR_WEIGHT, Scale.identity, true, List.of("composed"));
    }

    /**
     * Parses the block-level {@code shrinkage} (or legacy {@code smoothing: {type: bayesian, priorWeight}})
     * merged with a keySet-level override. {@code estimator} left null means "derive from the lattice".
     */
    public static Shrinkage parse(final JsonObject block, final JsonObject legacySmoothing, final JsonObject override,
                                  final Diagnostics diagnostics, final String location) {
        if (block == null && legacySmoothing == null && override == null) {
            return disabled();
        }
        Estimator estimator = null;
        String weights = "fixed";
        double priorWeight = DEFAULT_PRIOR_WEIGHT;
        Scale scale = Scale.identity;
        boolean leaveNodeOut = true;
        List<String> outputs = List.of("composed");
        if (legacySmoothing != null) {
            final String type = Json.string(legacySmoothing, "type");
            if (type != null && !"bayesian".equals(type)) {
                diagnostics.error("encoding.smoothing.type", location, "smoothing.type must be bayesian (use shrinkage for the general form)");
            }
            try {
                if (Json.string(legacySmoothing, "priorWeight") != null) priorWeight = Double.parseDouble(Json.string(legacySmoothing, "priorWeight"));
            } catch (final NumberFormatException e) {
                diagnostics.error("encoding.smoothing.priorWeight", location, "smoothing.priorWeight must be numeric");
            }
        }
        for (final JsonObject o : new JsonObject[]{block, override}) {
            if (o == null) continue;
            final String est = Json.string(o, "estimator");
            if (est != null) {
                switch (est) {
                    case "backoff" -> estimator = Estimator.backoff;
                    case "sequential" -> estimator = Estimator.sequential;
                    case "joint" -> diagnostics.error("encoding.shrinkage.estimator", location, "estimator: joint is not implemented yet (sequential is available)");
                    default -> diagnostics.error("encoding.shrinkage.estimator", location, "estimator must be backoff | sequential | joint: " + est);
                }
            }
            final String w = Json.string(o, "weights");
            if (w != null) {
                switch (w) {
                    case "fixed" -> weights = "fixed";
                    case "varianceComponents" -> {
                        weights = "varianceComponents";
                        diagnostics.info("encoding.shrinkage.weights", location,
                                "weights: varianceComponents estimates λ = σ²/τ² per level from the whole batch (method of moments); the estimate is not time-expanding (structural, §6.3)");
                    }
                    case "heldOut" -> diagnostics.error("encoding.shrinkage.weights", location, "weights: heldOut is not implemented yet");
                    default -> diagnostics.error("encoding.shrinkage.weights", location, "weights must be fixed | varianceComponents | heldOut: " + w);
                }
            }
            if (Json.string(o, "priorWeight") != null) {
                try {
                    priorWeight = Double.parseDouble(Json.string(o, "priorWeight"));
                } catch (final NumberFormatException e) {
                    diagnostics.error("encoding.shrinkage.priorWeight", location, "priorWeight must be numeric");
                }
                if (priorWeight < 0) diagnostics.error("encoding.shrinkage.priorWeight", location, "priorWeight must be >= 0");
            }
            final String s = Json.string(o, "scale");
            if (s != null) {
                try {
                    scale = Scale.valueOf(s);
                } catch (final IllegalArgumentException e) {
                    diagnostics.error("encoding.shrinkage.scale", location, "scale must be identity | logit | log: " + s);
                }
            }
            if (o.has("leaveNodeOut")) leaveNodeOut = Json.bool(o, "leaveNodeOut", true);
            final String ps = Json.string(o, "parentStatistic");
            if (ps != null && !"token".equals(ps)) {
                diagnostics.warning("encoding.shrinkage.parentStatistic", location, "parentStatistic: " + ps + " is not implemented yet; using token");
            }
            final List<String> out = Json.strings(o, "output");
            if (!out.isEmpty()) {
                for (final String v : out) {
                    if (!List.of("composed", "deviations", "effectiveN").contains(v)) {
                        diagnostics.error("encoding.shrinkage.output", location, "output must be composed | deviations | effectiveN: " + v);
                    }
                }
                outputs = out;
            }
            if (Json.string(o, "family") != null) {
                diagnostics.warning("encoding.shrinkage.family", location, "family is v1 and ignored (gaussian pseudo-counts are used)");
            }
        }
        return new Shrinkage(true, estimator, weights, priorWeight, scale, leaveNodeOut, outputs);
    }

    public boolean emits(final String output) {
        return outputs.contains(output);
    }

    // ------------------------------------------------------------------------------------------
    // composition
    // ------------------------------------------------------------------------------------------

    /**
     * One lattice level resolved to the hidden statistics columns of the row. {@code additive} levels carry
     * the main-effect chains instead of statistics.
     */
    public record Level(String token, String nColumn, String sumColumn, List<List<Level>> mainEffects) implements Serializable {
        boolean isAdditive() {
            return mainEffects != null;
        }
    }

    /** The statistics-carrying levels of a chain, additive entries expanded to their main-effect chains. */
    public static List<Level> leaves(final List<Level> levels) {
        final List<Level> leaves = new ArrayList<>();
        for (final Level level : levels) {
            if (level.isAdditive()) {
                for (final List<Level> main : level.mainEffects()) leaves.addAll(leaves(main));
            } else {
                leaves.add(level);
            }
        }
        return leaves;
    }

    /** Result of one composition: value on the original scale plus the per-level deviations (transform scale). */
    public record Composition(Double value, Double[] deviations, Double effectiveN) {}

    double transform(final double m) {
        return switch (scale) {
            case identity -> m;
            case logit -> {
                final double p = Math.min(1 - 1e-6, Math.max(1e-6, m));
                yield Math.log(p / (1 - p));
            }
            case log -> Math.log(Math.max(m, 1e-12));
        };
    }

    double inverse(final double t) {
        return switch (scale) {
            case identity -> t;
            case logit -> 1 / (1 + Math.exp(-t));
            case log -> Math.exp(t);
        };
    }

    private static double n(final Map<String, Object> row, final String column) {
        final Double d = FeatureValues.toDouble(row.get(column));
        return d == null ? 0 : d;
    }

    /**
     * Composes the leaf estimate for a lattice given leaf → root. Leave-node-out subtracts the leaf's own
     * statistics from every ancestor (the leaf's contributions are contained in each of them).
     */
    public Composition compose(final Map<String, Object> row, final List<Level> levels) {
        return compose(row, levels, null);
    }

    /**
     * @param lambdas per-level pseudo-counts keyed by the level's {@code n} column (variance components);
     *                null or missing entries fall back to {@link #priorWeight}
     */
    public Composition compose(final Map<String, Object> row, final List<Level> levels, final Map<String, Double> lambdas) {
        final Double[] deviations = new Double[levels.size()];
        final double leafN = n(row, levels.get(0).nColumn());
        final double leafSum = n(row, levels.get(0).sumColumn());
        final double[] effectiveN = new double[1];
        final Double est = estimate(row, levels, 0, leafN, leafSum, deviations, effectiveN, lambdas, false);
        return new Composition(est == null ? null : inverse(est), deviations, est == null ? null : effectiveN[0]);
    }

    private double lambda(final Level level, final Map<String, Double> lambdas) {
        if (lambdas == null) return priorWeight;
        final Double l = lambdas.get(level.nColumn());
        return l == null ? priorWeight : l;
    }

    private Double estimate(final Map<String, Object> row, final List<Level> levels, final int index,
                            final double looN, final double looSum, final Double[] deviations, final double[] effectiveN,
                            final Map<String, Double> lambdas, final boolean subtractLeaf) {
        final Level level = levels.get(index);
        if (level.isAdditive()) {
            // sequential estimator: parent of the cell is the additive prediction of the main effects.
            // every main-effect level also contains the cell's rows, so leave-node-out subtracts the leaf
            // statistics at every level of the main chains (subtractLeaf = true).
            final Double root = estimate(row, levels, index + 1, looN, looSum, deviations, effectiveN, lambdas, false);
            if (root == null) return null;
            double sum = root;
            for (final List<Level> main : level.mainEffects()) {
                final Double[] mainDev = new Double[main.size()];
                final double[] ignored = new double[1];
                final Double mainEst = estimate(row, main, 0, looN, looSum, mainDev, ignored, lambdas, true);
                if (mainEst != null) sum += mainEst - root;
            }
            deviations[index] = sum - root;
            return sum;
        }
        double n = n(row, level.nColumn());
        double s = n(row, level.sumColumn());
        if ((index > 0 || subtractLeaf) && leaveNodeOut) {
            n -= looN;
            s -= looSum;
        }
        final Double own = n > 0 ? transform(s / n) : null;
        if (index == levels.size() - 1) {
            effectiveN[0] = n;
            return own;
        }
        final Double parent = estimate(row, levels, index + 1, looN, looSum, deviations, effectiveN, lambdas, subtractLeaf);
        if (own == null) {
            deviations[index] = 0d;
            return parent;
        }
        if (parent == null) {
            effectiveN[0] = n;
            deviations[index] = 0d;
            return own;
        }
        final double lambda = lambda(level, lambdas);
        final double w = lambda == 0 ? 1 : Double.isInfinite(lambda) ? 0 : n / (n + lambda);
        final double dev = w * (own - parent);
        deviations[index] = dev;
        // effective sample size: own n plus the prior mass actually backed by the parent (§5.5 rule 6)
        effectiveN[0] = n + (lambda == 0 ? 0 : Double.isInfinite(lambda) ? effectiveN[0] : lambda * Math.min(1, effectiveN[0] / lambda));
        return parent + dev;
    }

    /**
     * Method-of-moments (one-way random effects) pseudo-count for one level from per-key sufficient
     * statistics: λ = σ² / τ² with σ² the within-key variance and τ² the between-key variance of the key
     * means. A non-positive τ² (no signal at this level) yields +∞, i.e. full shrinkage; too few keys yield
     * null (fall back to priorWeight).
     *
     * @param keyCount K, {@code n} N = Σ n_k, {@code sum} Σ S_k, {@code sumSq} Σ Q_k,
     *                 {@code sumSqOverN} Σ S_k² / n_k, {@code sumNSq} Σ n_k²
     */
    public static Double lambdaFromMoments(final long keyCount, final double n, final double sum, final double sumSq,
                                           final double sumSqOverN, final double sumNSq) {
        if (keyCount < 2 || n - keyCount < 1) return null;
        final double withinSS = sumSq - sumSqOverN;
        final double sigma2 = withinSS / (n - keyCount);
        final double betweenSS = sumSqOverN - sum * sum / n;
        final double n0 = (n - sumNSq / n) / (keyCount - 1);
        if (n0 <= 0) return null;
        final double tau2 = (betweenSS / (keyCount - 1) - sigma2) / n0;
        if (tau2 <= 0) return Double.POSITIVE_INFINITY;
        if (sigma2 <= 0) return 0d;
        return sigma2 / tau2;
    }

    /** Serializes a level chain into a coordinate string; the row evaluator rebuilds it with {@link #parseLevels}. */
    static String encodeLevels(final List<Level> levels) {
        final StringBuilder sb = new StringBuilder();
        for (final Level l : levels) {
            if (!sb.isEmpty()) sb.append(';');
            if (l.isAdditive()) {
                sb.append(ADDITIVE).append('(');
                boolean first = true;
                for (final List<Level> main : l.mainEffects()) {
                    if (!first) sb.append('|');
                    sb.append(encodeLevels(main));
                    first = false;
                }
                sb.append(')');
            } else {
                sb.append(l.token()).append(',').append(l.nColumn()).append(',').append(l.sumColumn());
            }
        }
        return sb.toString();
    }

    static List<Level> parseLevels(final String text) {
        final List<Level> levels = new ArrayList<>();
        int i = 0;
        while (i < text.length()) {
            if (text.startsWith(ADDITIVE + "(", i)) {
                final int close = matchingParen(text, i + ADDITIVE.length());
                final String inner = text.substring(i + ADDITIVE.length() + 1, close);
                final List<List<Level>> mains = new ArrayList<>();
                for (final String part : splitTopLevel(inner, '|')) mains.add(parseLevels(part));
                levels.add(new Level(ADDITIVE, null, null, mains));
                i = close + 1;
            } else {
                int end = i;
                while (end < text.length() && text.charAt(end) != ';') end++;
                final String[] parts = text.substring(i, end).split(",", -1);
                levels.add(new Level(parts[0], parts[1], parts[2], null));
                i = end;
            }
            if (i < text.length() && text.charAt(i) == ';') i++;
        }
        return levels;
    }

    private static int matchingParen(final String text, final int open) {
        int depth = 0;
        for (int i = open; i < text.length(); i++) {
            if (text.charAt(i) == '(') depth++;
            if (text.charAt(i) == ')' && --depth == 0) return i;
        }
        throw new IllegalArgumentException("unbalanced level encoding: " + text);
    }

    private static List<String> splitTopLevel(final String text, final char separator) {
        final List<String> parts = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < text.length(); i++) {
            final char ch = text.charAt(i);
            if (ch == '(') depth++;
            if (ch == ')') depth--;
            if (ch == separator && depth == 0) {
                parts.add(text.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(text.substring(start));
        return parts;
    }

}
