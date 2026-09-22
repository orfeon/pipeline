package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.feature.SourceContract.Json;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shrinkage configuration (docs/design/feature-dsl.md §5.5) and the row-local composition of a generalization
 * lattice (§5.3.1): every level of the lattice has its own sufficient statistics per row
 * ({@code n}, {@code sum} over the key's past contributions, computed by hidden population stages) and the
 * final value is a top-down backoff from the global level to the leaf:
 *
 * <pre>
 *   est(root)  = t(mean_root)
 *   est(level) = est(parent) + w · (t(mean_level) − est(parent)),  w = n / (n + λ)
 * </pre>
 *
 * with leave-node-out (the leaf's own statistics are subtracted from every ancestor — the leaf being the deepest
 * level that has rows, {@link #effectiveLeaf}) and, for an
 * {@code additive} entry, the parent being {@code est(root) + Σ main-effect deviations} (sequential
 * estimator). {@code t} is the declared scale (identity / logit / log); the composed value is returned on
 * the original scale, deviations on the transform scale.
 *
 * <p><b>Baseline offset</b> (spec §3 rule 5: the offset is an additive term δ on the shrinkage scale,
 * {@code logit(p) = logit(b) + δ} on logit, {@code log(μ) = log(b) + δ} on log). Each level's own estimate is the
 * <b>score-type</b> one-step estimate from the baseline, {@code δ̂ = S / V} with {@code S = Σ(y − b)} the score of
 * the log-likelihood at δ = 0 and {@code V = Σ b(1 − b)} (logit) / {@code Σb} (log) its Fisher information — one
 * Newton step from the baseline, exact to first order in δ and finite for every cell, a cell with no success
 * included (the transformed mean {@code t(ȳ) − t(b̄)} is undefined there and its clamp leaked the constant
 * ±13.8 / −27.6 into the value, growing with n). The levels shrink that term toward the parent's with the weight
 * {@code V / (V + λ′)} — the information the level holds against the prior's, so a key of rare events shrinks
 * more than a key of the same row count at even odds — where λ′ is the pseudo-count in rows times the root
 * level's information per row (or, under variance components, 1 / τ² estimated on the score scale,
 * {@link #lambdaFromScore}); the composed value is the term itself (a log-odds / log-rate ratio against the
 * baseline; on identity the mean residual, as without the extra sums). Both {@code S} and {@code V} are sums
 * over the level's rows, so every fit mode serves the estimate from its sufficient statistics.
 *
 * <p><b>Conjugate families</b> (§5.1.1, §5.5 {@code family}). Shrinkage is "add pseudo sufficient statistics
 * inherited from the parent": with pseudo-count {@code m = λ} the posterior mean of every conjugate family is
 * {@code (Σy + m · parent) / (n + m) = parent + n / (n + m) · (ȳ − parent)}, i.e. the recursion above on the
 * identity scale — the Gaussian, Beta-Binomial (rate of a 0/1 target) and Gamma-Poisson (mean of a count
 * target) point estimates coincide, and the one-way method-of-moments pseudo-count of
 * {@link #lambdaFromMoments} is also Kleinman's Beta-Binomial moment estimator, so the family changes
 * neither the estimate nor λ for scalar statistics. What it adds is (a) the {@code distribution} statistic
 * under the Dirichlet-Multinomial family — per-category pseudo-counts, {@link #composeDistribution} — and
 * (b) the declaration check that conjugate closed forms are not combined with a logit / log scale (rule 7:
 * transformed scales use the Gaussian approximation).
 */
public final class Shrinkage implements Serializable {

    public enum Estimator { backoff, sequential, joint }
    public enum Scale { identity, logit, log }

    /** Conjugate family of the shrinkage (§5.1.1): derived from the statistic unless declared. */
    public enum Family {
        gaussian, betaBinomial, gammaPoisson, dirichletMultinomial;

        /** Conjugate closed form (pseudo sufficient statistics): everything but the Gaussian approximation. */
        public boolean isConjugate() {
            return this != gaussian;
        }

        /** Whether the family's sufficient statistics are those of the statistic (§5.1.1 table). */
        public boolean accepts(final String stat) {
            return switch (this) {
                case dirichletMultinomial -> "distribution".equals(stat);
                default -> "mean".equals(stat) || "rate".equals(stat);
            };
        }
    }

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
    /** Declared family; null = derive from the statistic ({@link #familyFor}). */
    public final Family family;

    private Shrinkage(final boolean enabled, final Estimator estimator, final String weights, final double priorWeight,
                      final Scale scale, final boolean leaveNodeOut, final List<String> outputs, final Family family) {
        this.enabled = enabled;
        this.estimator = estimator;
        this.weights = weights;
        this.priorWeight = priorWeight;
        this.scale = scale;
        this.leaveNodeOut = leaveNodeOut;
        this.outputs = outputs;
        this.family = family;
    }

    /** Runtime instance rebuilt from a composed column's coordinates. */
    public static Shrinkage of(final Scale scale, final double priorWeight, final boolean leaveNodeOut) {
        return of(scale, priorWeight, leaveNodeOut, null);
    }

    /** Runtime instance rebuilt from a composed column's coordinates ({@code family} coordinate included). */
    public static Shrinkage of(final Scale scale, final double priorWeight, final boolean leaveNodeOut, final Family family) {
        return new Shrinkage(true, null, "fixed", priorWeight, scale, leaveNodeOut, List.of("composed"), family);
    }

    public static Shrinkage disabled() {
        return new Shrinkage(false, null, "fixed", DEFAULT_PRIOR_WEIGHT, Scale.identity, true, List.of("composed"), null);
    }

    /** The family a statistic shrinks under by default (§5.1.1 table); null = the statistic is not shrunk. */
    public static Family familyFor(final String stat) {
        return switch (stat) {
            case "mean" -> Family.gaussian;
            case "rate" -> Family.betaBinomial;
            case "distribution" -> Family.dirichletMultinomial;
            default -> null;
        };
    }

    /** The declared family, else the statistic's default. */
    /**
     * The declared family, else the statistic's default — on logit / log the Gaussian approximation of §5.5 rule 7
     * (the conjugate closed forms need the identity scale), and null for a distribution there (no Gaussian
     * counterpart: the statistic is emitted unshrunk).
     */
    public Family resolveFamily(final String stat) {
        if (family != null) return family;
        final Family derived = familyFor(stat);
        if (derived == null || !derived.isConjugate() || scale == Scale.identity) return derived;
        return derived == Family.dirichletMultinomial ? null : Family.gaussian;
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
        Family family = null;
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
                    case "joint" -> estimator = Estimator.joint;
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
            final String f = Json.string(o, "family");
            if (f != null) {
                try {
                    family = Family.valueOf(f);
                } catch (final IllegalArgumentException e) {
                    diagnostics.error("encoding.shrinkage.family", location, "family must be gaussian | betaBinomial | gammaPoisson | dirichletMultinomial: " + f);
                }
            }
        }
        return new Shrinkage(true, estimator, weights, priorWeight, scale, leaveNodeOut, outputs, family);
    }

    public boolean emits(final String output) {
        return outputs.contains(output);
    }

    // ------------------------------------------------------------------------------------------
    // composition
    // ------------------------------------------------------------------------------------------

    /**
     * One lattice level resolved to the hidden statistics columns of the row. {@code additive} levels carry
     * the main-effect chains instead of statistics. {@code offColumn} is the level's hidden Σ baseline (the
     * {@code __sumoff} column an offset block on a logit / log scale registers) and {@code infoColumn} its Fisher
     * information {@code V} — the {@code __suminfo} column (Σ b(1 − b)) on logit, the Σ baseline column itself on
     * log; both null otherwise. A level with an information column is composed by the score-type estimate
     * ({@link #ownScore}); the others by the transformed mean ({@link #own}).
     */
    public record Level(String token, String nColumn, String sumColumn, String offColumn, String infoColumn, List<List<Level>> mainEffects) implements Serializable {
        public Level(final String token, final String nColumn, final String sumColumn, final List<List<Level>> mainEffects) {
            this(token, nColumn, sumColumn, null, null, mainEffects);
        }

        public Level(final String token, final String nColumn, final String sumColumn, final String offColumn, final List<List<Level>> mainEffects) {
            this(token, nColumn, sumColumn, offColumn, null, mainEffects);
        }

        boolean isAdditive() {
            return mainEffects != null;
        }

        /** Whether the level is composed on the score scale (an offset term on logit / log). */
        boolean isScore() {
            return infoColumn != null;
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

    /**
     * Result of one composition: value on the original scale plus the per-level deviations (transform scale);
     * {@code distribution} is the composed probability per category for the Dirichlet-Multinomial family
     * (then {@code value} and {@code deviations} are null).
     */
    public record Composition(Double value, Double[] deviations, Double effectiveN, Map<String, Double> distribution) {
        public Composition(final Double value, final Double[] deviations, final Double effectiveN) {
            this(value, deviations, effectiveN, null);
        }
    }

    double transform(final double m) {
        return transform(scale, m);
    }

    double inverse(final double t) {
        return inverse(scale, t);
    }

    /** The shrinkage scale: identity, logit (clamped to [1e-6, 1 − 1e-6]) or log (floored at 1e-12). */
    static double transform(final Scale scale, final double m) {
        return switch (scale) {
            case identity -> m;
            case logit -> {
                final double p = Math.min(1 - 1e-6, Math.max(1e-6, m));
                yield Math.log(p / (1 - p));
            }
            case log -> Math.log(Math.max(m, 1e-12));
        };
    }

    static double inverse(final Scale scale, final double t) {
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
     * statistics from every ancestor (the leaf's contributions are contained in each of them). The leaf is the
     * <b>effective</b> one ({@link #effectiveLeaf}): a row whose declared leaf has no rows (a key never seen, or a
     * null component — the short history of a {@code structure: sequence} path) backs off to a coarser level, and
     * that level's rows are what its ancestors contain — so the row reads what it would read had the lattice been
     * declared from that level.
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
        // without leave-node-out nothing is subtracted anywhere, so the effective leaf does not matter
        final int leafIndex = leaveNodeOut ? effectiveLeaf(row, levels) : 0;
        final Level leaf = levels.get(leafIndex);
        final double leafN = n(row, leaf.nColumn());
        final double leafSum = n(row, leaf.sumColumn());
        final double leafInfo = leaf.infoColumn() == null ? 0 : n(row, leaf.infoColumn());
        final double[] effectiveN = new double[1];
        final Double est = estimate(row, levels, 0, leafIndex, leafN, leafSum, leafInfo, deviations, effectiveN, lambdas,
                false, rootInfoPerRow(row, levels));
        return new Composition(est == null ? null : output(scale, est, levels.get(0).offColumn() != null), deviations, est == null ? null : effectiveN[0]);
    }

    /**
     * The information per row of the chain's root — the coarsest level, its whole totals — which converts a
     * pseudo-count in rows into the score scale ({@code λ′ = priorWeight · ī}): "priorWeight rows of average
     * information", one constant for every key of the lattice. 1 without a score level, or when the root holds nothing.
     */
    private static double rootInfoPerRow(final Map<String, Object> row, final List<Level> levels) {
        final Level root = levels.get(levels.size() - 1);
        if (!root.isScore()) return 1;
        final double n = n(row, root.nColumn()), info = n(row, root.infoColumn());
        return n > 0 && info > 0 ? info / n : 1;
    }

    /**
     * The level leave-node-out subtracts: the deepest level of the chain that has rows. The levels below it are empty
     * (they defer to their parent with a zero deviation), so it is the level the back-off actually starts from.
     *
     * <p>A lattice that contains an {@code additive} entry keeps its <b>declared</b> leaf, whatever the chain above it
     * holds: the main-effect chains behind that entry subtract the cell they generalise — every main level contains
     * that cell, and an empty cell has nothing to subtract — while a coarser level of the chain (a coarse cross,
     * §5.3.1) is contained in no main level, so subtracting it there would take a main level's {@code n} below zero
     * and silently drop its main effect. Cross / additive lattices are therefore unchanged by the back-off.
     */
    static int effectiveLeaf(final Map<String, Object> row, final List<Level> levels) {
        int leaf = -1;
        for (int i = 0; i < levels.size(); i++) {
            final Level level = levels.get(i);
            if (level.isAdditive()) return 0;
            if (leaf < 0 && n(row, level.nColumn()) > 0) leaf = i;
        }
        return leaf < 0 ? 0 : leaf;
    }

    /**
     * A level's own estimate on the transform scale without an offset term: {@code t(Σy / n)} — on the identity scale
     * also under an offset, where the hidden sum is {@code Σ(y − b)} and the estimate the mean residual.
     *
     * @param n   the rows of the level (> 0)
     * @param sum Σy of those rows (Σ(y − b) under an identity offset)
     */
    static double own(final Scale scale, final double n, final double sum) {
        return transform(scale, sum / n);
    }

    /**
     * A level's (or a joint cell's) own score-type estimate of the offset term: {@code δ̂ = S / V}, the one-step
     * (Fisher scoring) estimate from the baseline — {@code S = Σ(y − b)}, {@code V = Σ b(1 − b)} on logit and
     * {@code Σb} on log. Exact to first order in δ, finite whatever the counts (no transform, no clamp), and
     * conservative for a large |δ|: a key with no success in n rows reads {@code −Σb / V}, which grows with n toward
     * {@code −1 / (1 − b̄)} on logit (−1 on log) instead of diverging. Null when the rows carry no information
     * ({@code V ≤ 0}: every baseline at 0 or 1), as for {@code n = 0}.
     *
     * @param sum  S, Σ(y − b) of the level's rows
     * @param info V, the information of the same rows
     */
    static Double ownScore(final double sum, final double info) {
        return info > 0 ? sum / info : null;
    }

    /** The Fisher information one row contributes at its baseline: {@code b(1 − b)} on logit, {@code b} on log, 1 otherwise. */
    public static double information(final Scale scale, final double baseline) {
        return switch (scale) {
            case identity -> 1d;
            case logit -> baseline * (1 - baseline);
            case log -> baseline;
        };
    }

    /**
     * The composed value of a shrunk estimate {@code eta} on the transform scale: mapped back to the original scale —
     * or, under a baseline offset on a logit / log scale, the additive term itself (spec §3 rule 5:
     * {@code logit(p) = logit(baseline) + δ}, and the value is δ — a log-odds / log-rate ratio, not a probability / rate).
     */
    static double output(final Scale scale, final double eta, final boolean offset) {
        return offset && scale != Scale.identity ? eta : inverse(scale, eta);
    }

    /**
     * The level's pseudo-count in rows: the estimated one (variance components, keyed by the level's {@code n} column)
     * or {@link #priorWeight}. For a score level an estimated entry is {@code λ′ = 1 / τ²} on the score scale already
     * ({@link #lambdaFromScore}) and is returned as is; the fallback is converted by the caller.
     */
    private double lambda(final Level level, final Map<String, Double> lambdas) {
        if (lambdas == null) return priorWeight;
        final Double l = lambdas.get(level.nColumn());
        return l == null ? priorWeight : l;
    }

    /**
     * @param leafIndex     the chain's effective leaf, whose statistics are {@code looN} / {@code looSum} / {@code looInfo}:
     *                      the levels above it contain them; the (empty) levels below it and the leaf itself do not
     * @param rootInfoPerRow the root's information per row ({@link #rootInfoPerRow}): converts a pseudo-count in rows
     *                      into the score scale for the score levels
     */
    private Double estimate(final Map<String, Object> row, final List<Level> levels, final int index, final int leafIndex,
                            final double looN, final double looSum, final double looInfo, final Double[] deviations, final double[] effectiveN,
                            final Map<String, Double> lambdas, final boolean subtractLeaf, final double rootInfoPerRow) {
        final Level level = levels.get(index);
        if (level.isAdditive()) {
            // sequential estimator: parent of the cell is the additive prediction of the main effects.
            // every main-effect level also contains the cell's rows, so leave-node-out subtracts the leaf
            // statistics at every level of the main chains (subtractLeaf = true).
            final Double root = estimate(row, levels, index + 1, leafIndex, looN, looSum, looInfo, deviations, effectiveN, lambdas, false, rootInfoPerRow);
            if (root == null) return null;
            double sum = root;
            for (final List<Level> main : level.mainEffects()) {
                final Double[] mainDev = new Double[main.size()];
                final double[] ignored = new double[1];
                // a main chain is a list of its own: the leaf is none of its levels (-1), every one of them contains it
                final Double mainEst = estimate(row, main, 0, -1, looN, looSum, looInfo, mainDev, ignored, lambdas, true, rootInfoPerRow);
                if (mainEst != null) sum += mainEst - root;
            }
            deviations[index] = sum - root;
            return sum;
        }
        double n = n(row, level.nColumn());
        double s = n(row, level.sumColumn());
        final boolean score = level.isScore();
        double info = score ? n(row, level.infoColumn()) : 0;
        if ((index > leafIndex || subtractLeaf) && leaveNodeOut) {
            n -= looN;
            s -= looSum;
            info -= looInfo;
        }
        final Double own = n <= 0 ? null : score ? ownScore(s, info) : Double.valueOf(own(scale, n, s));
        if (index == levels.size() - 1) {
            effectiveN[0] = n;
            return own;
        }
        final Double parent = estimate(row, levels, index + 1, leafIndex, looN, looSum, looInfo, deviations, effectiveN, lambdas, subtractLeaf, rootInfoPerRow);
        if (own == null) {
            deviations[index] = 0d;
            return parent;
        }
        if (parent == null) {
            effectiveN[0] = n;
            deviations[index] = 0d;
            return own;
        }
        // the shrinkage weight: rows against the pseudo-count in rows — or, on the score scale, the level's information
        // against the prior's (λ′ = 1 / τ²: the estimated one as is, a declared pseudo-count times the root's information
        // per row), so the effective sample size stays in rows either way
        final double declared = lambda(level, lambdas);
        final boolean estimated = lambdas != null && lambdas.containsKey(level.nColumn());
        final double lambda = score && !estimated ? declared * rootInfoPerRow : declared;
        final double mass = score ? info : n;
        final double w = lambda == 0 ? 1 : Double.isInfinite(lambda) ? 0 : mass / (mass + lambda);
        final double dev = w * (own - parent);
        deviations[index] = dev;
        // effective sample size: own n plus the prior mass actually backed by the parent (§5.5 rule 6), in rows
        final double lambdaRows = score ? lambda / rootInfoPerRow : lambda;
        effectiveN[0] = n + (lambdaRows == 0 ? 0 : Double.isInfinite(lambdaRows) ? effectiveN[0] : lambdaRows * Math.min(1, effectiveN[0] / lambdaRows));
        return parent + dev;
    }

    // ------------------------------------------------------------------------------------------
    // Dirichlet-Multinomial: the distribution statistic shrunk along a chain lattice
    // ------------------------------------------------------------------------------------------

    /**
     * Composes a shrunk category distribution (§5.1.1: per-category pseudo-counts). Every level carries its
     * hidden {@code n} and a {@code distribution} column (category → share of the level's rows); the
     * top-down pass is {@code p(level) = (counts(level) + λ · p(parent)) / (n(level) + λ)} over the union of
     * the categories seen at any level, with leave-node-out subtracting the leaf's counts from every ancestor.
     * Additive entries are not defined for distributions (rejected at compile time). Returns a null
     * distribution when no level has data.
     *
     * @param lambdas per-level pseudo-counts keyed by the level's {@code n} column, as in {@link #compose}
     */
    public Composition composeDistribution(final Map<String, Object> row, final List<Level> levels, final Map<String, Double> lambdas) {
        final int leafIndex = leaveNodeOut ? effectiveLeaf(row, levels) : 0;
        final Level leaf = levels.get(leafIndex);
        final double leafN = n(row, leaf.nColumn());
        final Map<String, Double> leafCounts = counts(row, leaf, leafN);
        final double[] effectiveN = new double[1];
        final Map<String, Double> p = estimateDistribution(row, levels, 0, leafIndex, leafN, leafCounts, effectiveN, lambdas);
        return new Composition(null, null, p == null ? null : effectiveN[0], p);
    }

    /** Category counts of a level = its share per category × its n (the hidden distribution column holds shares). */
    private static Map<String, Double> counts(final Map<String, Object> row, final Level level, final double n) {
        final Map<String, Double> counts = new java.util.TreeMap<>();
        final Object value = row.get(level.sumColumn());
        if (!(value instanceof Map<?, ?> shares) || n <= 0) return counts;
        for (final Map.Entry<?, ?> e : shares.entrySet()) {
            final Double share = FeatureValues.toDouble(e.getValue());
            if (share != null) counts.put(e.getKey().toString(), share * n);
        }
        return counts;
    }

    private Map<String, Double> estimateDistribution(final Map<String, Object> row, final List<Level> levels, final int index, final int leafIndex,
                                                     final double looN, final Map<String, Double> looCounts,
                                                     final double[] effectiveN, final Map<String, Double> lambdas) {
        final Level level = levels.get(index);
        double n = n(row, level.nColumn());
        final Map<String, Double> counts = counts(row, level, n);
        if (index > leafIndex && leaveNodeOut) {
            n -= looN;
            for (final Map.Entry<String, Double> e : looCounts.entrySet()) counts.merge(e.getKey(), -e.getValue(), Double::sum);
        }
        Map<String, Double> own = null;
        if (n > 0) {
            own = new java.util.TreeMap<>();
            for (final Map.Entry<String, Double> e : counts.entrySet()) {
                if (e.getValue() > 1e-9) own.put(e.getKey(), e.getValue() / n);
            }
        }
        if (index == levels.size() - 1) {
            effectiveN[0] = n;
            return own;
        }
        final Map<String, Double> parent = estimateDistribution(row, levels, index + 1, leafIndex, looN, looCounts, effectiveN, lambdas);
        if (own == null) return parent;
        if (parent == null) {
            effectiveN[0] = n;
            return own;
        }
        final double lambda = lambda(level, lambdas);
        final double w = lambda == 0 ? 1 : Double.isInfinite(lambda) ? 0 : n / (n + lambda);
        final Map<String, Double> composed = new java.util.TreeMap<>();
        for (final String category : union(own.keySet(), parent.keySet())) {
            composed.put(category, w * own.getOrDefault(category, 0d) + (1 - w) * parent.getOrDefault(category, 0d));
        }
        effectiveN[0] = n + (lambda == 0 ? 0 : Double.isInfinite(lambda) ? effectiveN[0] : lambda * Math.min(1, effectiveN[0] / lambda));
        return composed;
    }

    private static java.util.Set<String> union(final java.util.Set<String> a, final java.util.Set<String> b) {
        final java.util.Set<String> u = new java.util.TreeSet<>(a);
        u.addAll(b);
        return u;
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

    /**
     * The score-scale pseudo-count {@code λ′ = 1 / τ²} of one level from its keys' score statistics — the moment
     * estimator of the between-key variance of the one-step terms {@code δ̂_k = S_k / V_k}, whose sampling variance is
     * {@code 1 / V_k} (DerSimonian–Laird): {@code Q = Σ S_k² / V_k − (Σ S_k)² / Σ V_k} has expectation
     * {@code (K − 1) + τ² (Σ V_k − Σ V_k² / Σ V_k)}. A non-positive τ² (no signal at this level) yields +∞, i.e. full
     * shrinkage; fewer than two keys, or no information, yields null (fall back to priorWeight). Keys without
     * information ({@code V_k ≤ 0}) are not keys of the level here — they have no term of their own.
     *
     * @param keyCount K (keys with {@code V_k > 0}), {@code sumV} Σ V_k, {@code sumV2} Σ V_k², {@code sumS} Σ S_k,
     *                 {@code sumS2OverV} Σ S_k² / V_k
     */
    public static Double lambdaFromScore(final long keyCount, final double sumV, final double sumV2, final double sumS, final double sumS2OverV) {
        if (keyCount < 2 || sumV <= 0) return null;
        final double q = sumS2OverV - sumS * sumS / sumV;
        final double denominator = sumV - sumV2 / sumV;
        if (denominator <= 0) return null;
        final double tau2 = (q - (keyCount - 1)) / denominator;
        if (tau2 <= 0) return Double.POSITIVE_INFINITY;
        return 1 / tau2;
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
                if (l.offColumn() != null) sb.append(',').append(l.offColumn());
                if (l.infoColumn() != null) sb.append(',').append(l.infoColumn());
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
                levels.add(new Level(parts[0], parts[1], parts[2],
                        parts.length > 3 && !parts[3].isEmpty() ? parts[3] : null,
                        parts.length > 4 && !parts[4].isEmpty() ? parts[4] : null, null));
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
