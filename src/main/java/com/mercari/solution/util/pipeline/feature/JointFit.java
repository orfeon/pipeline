package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.domain.math.MatrixOps;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * {@code estimator: joint} (docs/design/feature-dsl.md §5.5 rule 1): every level of a generalization lattice is
 * fitted <b>simultaneously</b> as a mixed model over the indicator basis of its contexts,
 *
 * <pre>
 *   t(y) = μ + Σ_level e_level(key_level(row)) + ε,   e_level(k) ~ N(0, τ²_level)
 * </pre>
 *
 * solved once as a ridge / BLUP system on the cells of the lattice (the cross of every key field a level
 * uses, aggregated to (n, Σy, Σy²) in one pass): minimise {@code Σ_cells w_c (z_c − μ − Σ e)² + Σ_level
 * λ_level ‖e_level‖²} with {@code z_c = t(ȳ_c)} on the declared scale and the delta-method weight
 * {@code w_c = n_c · v(ȳ_c)} (v = 1 / p(1−p) / μ for identity / logit / log) — or, under a baseline offset on a
 * transformed scale, the score-type cell term {@code z_c = S_c / V_c} weighted by its information {@code w_c = V_c}
 * ({@link Shrinkage#ownScore}: the one-step estimate of the offset term, with λ then on the score scale — a
 * pseudo-count in rows times the fit's information per row, or {@link Shrinkage#lambdaFromScore}). Unlike the sequential estimator
 * (main effects first, interactions on the residual) the joint solve separates confounded contexts without
 * an order-dependent bias; unlike back-off it needs the whole cell table, so it lives in the fit stage
 * ({@code fit.mode: static | fold | forward}) and not in the row-local expanding replay.
 *
 * <p>The variance components are fixed first, per layer, by the closed-form moment estimator of
 * {@link Shrinkage#lambdaFromMoments} over the level's contexts ({@code weights: varianceComponents}) or
 * declared ({@code fixed}: λ = priorWeight); a level whose between-context variance truncates to zero is
 * fixed at 0 (fully shrunk). The system is symmetric positive definite, so block Gauss–Seidel over
 * (intercept, level 1, level 2, ...) converges; the iteration runs on the aggregated cells only — no
 * re-scan of the rows — on one worker.
 *
 * <p>Variants: {@code fold} solves once per fold on the totals minus the fold's cells (out-of-fold
 * effects); {@code forward} solves once per window change point on the cells of the blocks a usable block
 * reads ({@code (U − W, U]}, the encoding path's window). The artifact holds the whole-input solution only
 * ({@code <id>.joint.avro}), like the encoding-level statistics.
 *
 * <p>A <b>fixed level</b> ({@code shrinkage.fixedLevels}) is not shrunk: its contexts are fixed effects, the
 * intercept with them. The solve then absorbs it exactly — within each of its contexts the weighted means are
 * taken out — and solves the shrunk levels on what remains by conjugate gradients with the diagonal as the
 * preconditioner ({@link #solveAbsorbed}): block Gauss–Seidel over a crossed design converges like alternating
 * projections, slowly where the contexts are weakly connected, and the unpenalised fit the λ estimate needs is
 * its worst case. Only {@code μ + e} of a fixed context is identified; it is reported as {@code μ} = the weighted
 * mean of the fixed contexts' levels and {@code e} = the level minus {@code μ}. Under {@code weights:
 * varianceComponents} the one shrunk level's λ is Henderson's method III ({@link #henderson}) rather than the
 * one-way moments over its contexts, which in a crossed design count the fixed level's variance as noise.
 */
public final class JointFit implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JointFit.class);

    /** Separates a cell key from its fold / block tag in the aggregation key. */
    static final char TAG = (char) 2;
    static final String FOLD_PREFIX = "#";

    static final int MAX_ITERATIONS = 1000;
    /** Conjugate-gradient cap of an absorbed solve (the unpenalised fit of a weakly connected design needs the most). */
    static final int MAX_CG_ITERATIONS = 10000;
    /** Relative residual at which the conjugate gradients stop. */
    static final double CG_TOLERANCE = 1e-12;

    /** One lattice level: its token and the key fields of its contexts (empty = the global intercept). */
    public record Level(String token, List<String> keys) implements Serializable {
        boolean isGlobal() {
            return keys.isEmpty();
        }
    }

    /**
     * One aggregated cell (or a fold / block tagged part of one): the finest partition of the lattice.
     * {@code sumOff} is the cell's Σ baseline under an offset block and {@code sumInfo} its Σ b(1 − b) (0 otherwise):
     * the cell's information is {@code sumInfo} on logit, {@code sumOff} on log.
     */
    public record Cell(String key, double n, double sum, double sumSq, double sumOff, double sumInfo) implements Serializable {
        public Cell(final String key, final double n, final double sum, final double sumSq) {
            this(key, n, sum, sumSq, 0d, 0d);
        }

        public Cell(final String key, final double n, final double sum, final double sumSq, final double sumOff) {
            this(key, n, sum, sumSq, sumOff, 0d);
        }

        /** The cell's information on a transformed scale ({@link Shrinkage#information}), 0 on identity. */
        double information(final Shrinkage.Scale scale) {
            return switch (scale) {
                case identity -> n;      // one per row, as Shrinkage.information reads it
                case logit -> sumInfo;
                case log -> sumOff;
            };
        }
    }

    /** The fitted effects of one solve. */
    public static final class Solution implements Serializable {
        public double mu = Double.NaN;
        /** Per effect level (the non-global levels, lattice order): λ used (+∞ = level fixed at 0) — on the score scale for an offset term. */
        public double[] lambdas;
        /** The fit's information per row under an offset term (λ / infoPerRow = the pseudo-count in rows); 1 otherwise. */
        public double infoPerRow = 1;
        /** Per effect level: context key → effect on the transform scale. */
        public List<Map<String, Double>> effects;
        /** Leaf context key → n (rows of the fit), for effectiveN. */
        public Map<String, Double> leafN = new HashMap<>();
        public double rows;
        public int iterations;
        public double maxDelta;
        /**
         * Under a fixed level: the residual variance of the unpenalised fit (Henderson's method III), NaN when λ was
         * not estimated that way.
         */
        public double sigma2 = Double.NaN;
        /**
         * Under a fixed level: the information of the first shrunk level's contexts once the fixed level is absorbed,
         * in rows ({@code Σ_cells w − Σ_fixed (Σ w)² / W}, over the ridge unit) — what effectiveN adds λ to.
         */
        public Map<String, Double> information = new HashMap<>();

        boolean isEmpty() {
            return Double.isNaN(mu);
        }
    }

    public final List<Level> levels;
    /** The effect levels (non-global) in lattice order. */
    public final List<Level> effectLevels;
    public final Shrinkage.Scale scale;
    /** Whether the target was offset by a baseline: the estimate is then the additive term on the scale (spec §3 rule 5). */
    public final boolean offset;
    public final Solution total;
    /** fit.mode fold: the out-of-fold solution per fold, or null. */
    public final Solution[] folds;
    /**
     * fit.mode forward: the windowed solution per window change point (every observed block, and — under a
     * window — every block index at which an observed block leaves the window), or null. The floor entry of a
     * row's usable block is the solution over exactly the blocks that block's window covers.
     */
    public final TreeMap<Long, Solution> blocks;
    /** fit.mode forward: the block indices that carried data (minBlocks counts these, not the change points). */
    public final TreeSet<Long> observedBlocks;
    /** The effect level that is not shrunk ({@code shrinkage.fixedLevels}), or −1. */
    public final int fixedLevel;
    /** The lattice is the main effects alone ({@code structure: additive}): no leaf level a row must have. */
    public final boolean mainEffects;

    JointFit(final List<Level> levels, final Shrinkage.Scale scale, final boolean offset, final Solution total,
             final Solution[] folds, final TreeMap<Long, Solution> blocks, final TreeSet<Long> observedBlocks) {
        this(levels, scale, offset, total, folds, blocks, observedBlocks, -1, false);
    }

    JointFit(final List<Level> levels, final Shrinkage.Scale scale, final boolean offset, final Solution total,
             final Solution[] folds, final TreeMap<Long, Solution> blocks, final TreeSet<Long> observedBlocks, final int fixedLevel,
             final boolean mainEffects) {
        this.levels = levels;
        this.effectLevels = effectLevelsOf(levels);
        this.scale = scale;
        this.offset = offset;
        this.total = total;
        this.folds = folds;
        this.blocks = blocks;
        this.observedBlocks = observedBlocks;
        this.fixedLevel = fixedLevel;
        this.mainEffects = mainEffects;
    }

    /** The index of a level token among the effect levels, or −1 (null / empty token = no fixed level). */
    public static int effectLevelIndex(final List<Level> levels, final String token) {
        if (token == null || token.isEmpty()) return -1;
        final List<Level> effects = effectLevelsOf(levels);
        for (int l = 0; l < effects.size(); l++) if (effects.get(l).token().equals(token)) return l;
        throw new IllegalArgumentException("no effect level '" + token + "' in " + encodeLevels(levels));
    }

    /**
     * The effect level effectiveN and variance read: the leaf, or under a fixed level the first shrunk level. The one
     * rule: the compiler names those columns' level with it, the solve fills that level's information.
     */
    public static int effectiveNLevel(final int fixedLevel) {
        return fixedLevel == 0 ? 1 : 0;
    }

    public int effectiveNLevel() {
        return effectiveNLevel(fixedLevel);
    }

    /**
     * Whether a row takes part in the fit and reads an estimate — the one rule of the cell extraction and of
     * {@link #estimate}: a lattice with a leaf (flat / hierarchy / cross / sequence) needs the leaf key; a lattice of
     * main effects alone needs any one of its keys, each null key leaving the row out of that level only (no indicator
     * in the fit, no effect in the estimate), so the order the keys are declared in decides nothing; a fixed level's key
     * is needed in either case (its context carries the row's intercept).
     */
    public static boolean takesPart(final List<Level> effectLevels, final int fixedLevel, final boolean mainEffects, final Map<String, Object> row) {
        if (effectLevels.isEmpty()) return true;
        if (fixedLevel >= 0) {
            if (FeatureValues.key(row, effectLevels.get(fixedLevel).keys()) == null) return false;
            if (mainEffects) return true;
        }
        if (!mainEffects) return FeatureValues.key(row, effectLevels.get(0).keys()) != null;
        for (final Level l : effectLevels) if (FeatureValues.key(row, l.keys()) != null) return true;
        return false;
    }

    /** The effect levels (non-global) of a lattice in lattice order. */
    static List<Level> effectLevelsOf(final List<Level> levels) {
        final List<Level> out = new ArrayList<>();
        for (final Level l : levels) if (!l.isGlobal()) out.add(l);
        return List.copyOf(out);
    }

    /** The key fields of every level, in first-appearance order: the cell of the lattice. */
    public static List<String> cellKeysOf(final List<Level> levels) {
        final List<String> keys = new ArrayList<>();
        for (final Level l : levels) for (final String k : l.keys()) if (!keys.contains(k)) keys.add(k);
        return keys;
    }

    // ------------------------------------------------------------------------------------------
    // fitting
    // ------------------------------------------------------------------------------------------

    static String foldEntry(final int fold, final String cellKey) {
        return FOLD_PREFIX + fold + TAG + cellKey;
    }

    static String blockEntry(final String cellKey, final long block) {
        return cellKey + TAG + block;
    }

    /**
     * Fits the model from the gathered aggregation entries: plain cell keys (static / fold totals),
     * {@link #foldEntry fold-tagged} parts ({@code folds > 1}) and {@link #blockEntry block-tagged} parts
     * ({@code forward}).
     *
     * @param offset the target is offset by a baseline: cells carry Σb and the solve fits the additive term (see {@link #solve})
     */
    public static JointFit fit(final List<Level> levels, final Shrinkage.Scale scale, final boolean offset, final String weights, final double priorWeight,
                               final Collection<Cell> entries, final int folds, final boolean forward, final int windowBlocks) {
        return fit(levels, scale, offset, weights, priorWeight, entries, folds, forward, windowBlocks, -1, false);
    }

    /**
     * {@link #fit} with a fixed (unshrunk) effect level (−1 for none) over a lattice of main effects alone or not
     * ({@link #takesPart}).
     */
    public static JointFit fit(final List<Level> levels, final Shrinkage.Scale scale, final boolean offset, final String weights, final double priorWeight,
                               final Collection<Cell> entries, final int folds, final boolean forward, final int windowBlocks, final int fixedLevel,
                               final boolean mainEffects) {
        return fit(levels, scale, offset, weights, priorWeight, entries, folds, forward, windowBlocks, fixedLevel, mainEffects, 1);
    }

    /**
     * {@link #fit} that leaves out the forward change points no row reads: those with fewer than {@code minBlocks}
     * observed blocks at or before them ({@link #solutionFor} returns null for every usable block they serve). The
     * model does not record it: read it with the same {@code minBlocks} (or a larger one) — a smaller one would read
     * null where the full fit has a solution.
     */
    public static JointFit fit(final List<Level> levels, final Shrinkage.Scale scale, final boolean offset, final String weights, final double priorWeight,
                               final Collection<Cell> entries, final int folds, final boolean forward, final int windowBlocks, final int fixedLevel,
                               final boolean mainEffects, final int minBlocks) {
        final List<String> cellKeys = cellKeysOf(levels);
        final Map<String, Cell> total = new HashMap<>();
        final Map<Integer, Map<String, Cell>> foldParts = new HashMap<>();
        final TreeMap<Long, Map<String, Cell>> blockParts = new TreeMap<>();
        for (final Cell e : entries) {
            if (e.key().startsWith(FOLD_PREFIX) && e.key().indexOf(TAG) > 0) {
                final int at = e.key().indexOf(TAG);
                final int fold = Integer.parseInt(e.key().substring(FOLD_PREFIX.length(), at));
                merge(foldParts.computeIfAbsent(fold, f -> new HashMap<>()), e.key().substring(at + 1), e);
            } else if (forward) {
                final int at = e.key().lastIndexOf(TAG);
                final long block = Long.parseLong(e.key().substring(at + 1));
                final String cell = e.key().substring(0, at);
                merge(blockParts.computeIfAbsent(block, b -> new HashMap<>()), cell, e);
                merge(total, cell, e);
            } else {
                merge(total, e.key(), e);
            }
        }
        final Solution totalSolution = solve(levels, cellKeys, total.values(), scale, offset, weights, priorWeight, fixedLevel);
        LOG.info("joint fit: {} cells, {} rows, {} iterations (max delta {})", total.size(), totalSolution.rows, totalSolution.iterations, totalSolution.maxDelta);
        Solution[] foldSolutions = null;
        if (folds > 1) {
            foldSolutions = new Solution[folds];
            for (int f = 0; f < folds; f++) {
                final Map<String, Cell> part = foldParts.getOrDefault(f, Map.of());
                final Map<String, Cell> outOfFold = new HashMap<>();
                for (final Map.Entry<String, Cell> e : total.entrySet()) {
                    final Cell own = part.get(e.getKey());
                    final Cell rest = own == null ? e.getValue() : subtract(e.getValue(), own);
                    if (rest != null) outOfFold.put(e.getKey(), rest);
                }
                foldSolutions[f] = solve(levels, cellKeys, outOfFold.values(), scale, offset, weights, priorWeight, fixedLevel);
            }
        }
        TreeMap<Long, Solution> blockSolutions = null;
        TreeSet<Long> observed = null;
        if (forward) {
            observed = new TreeSet<>(blockParts.keySet());
            blockSolutions = new TreeMap<>();
            // the cells a usable block U reads are those of the blocks in (U − W, U] (W = 0: every block ≤ U), the
            // same window the encoding path applies per row; that set changes only when a block enters (at its
            // own index) or leaves (at index + W), so one solve per change point serves every U by floor lookup
            final TreeSet<Long> changePoints = BlockSeries.changePoints(observed, windowBlocks);
            final Map<String, Cell> window = new HashMap<>();
            final List<Long> order = new ArrayList<>(observed);
            int added = 0, oldest = 0; // blocks merged so far / the oldest block still inside the window
            for (final long at : changePoints) {
                while (added < order.size() && order.get(added) <= at) {
                    for (final Map.Entry<String, Cell> e : blockParts.get(order.get(added)).entrySet()) merge(window, e.getKey(), e.getValue());
                    added++;
                }
                if (windowBlocks > 0) {
                    while (oldest < added && order.get(oldest) <= at - windowBlocks) {
                        for (final Map.Entry<String, Cell> e : blockParts.get(order.get(oldest)).entrySet()) {
                            final Cell rest = subtract(window.get(e.getKey()), e.getValue());
                            if (rest == null) window.remove(e.getKey());
                            else window.put(e.getKey(), rest);
                        }
                        oldest++;
                    }
                }
                // every observed block is a change point, so the usable blocks this one serves (up to the next) see
                // exactly the `added` observed blocks: below minBlocks no row reads it, and it is not solved (nor shipped)
                if (added < minBlocks) continue;
                blockSolutions.put(at, solve(levels, cellKeys, window.values(), scale, offset, weights, priorWeight, fixedLevel));
            }
            LOG.info("joint fit: {} forward change points solved, {} before minBlocks {} left out", blockSolutions.size(),
                    changePoints.size() - blockSolutions.size(), minBlocks);
        }
        return new JointFit(levels, scale, offset, totalSolution, foldSolutions, blockSolutions, observed, fixedLevel, mainEffects);
    }

    private static void merge(final Map<String, Cell> into, final String key, final Cell part) {
        into.merge(key, new Cell(key, part.n(), part.sum(), part.sumSq(), part.sumOff(), part.sumInfo()),
                (a, b) -> new Cell(key, a.n() + b.n(), a.sum() + b.sum(), a.sumSq() + b.sumSq(), a.sumOff() + b.sumOff(), a.sumInfo() + b.sumInfo()));
    }

    /** {@code total − part}; null when nothing remains. */
    static Cell subtract(final Cell total, final Cell part) {
        if (total == null) return null;
        final double n = total.n() - part.n();
        if (n <= 1e-9) return null;
        return new Cell(total.key(), n, total.sum() - part.sum(), total.sumSq() - part.sumSq(), total.sumOff() - part.sumOff(), total.sumInfo() - part.sumInfo());
    }

    /** Delta-method weight factor of the transformed cell mean: 1 / p(1−p) / μ. */
    static double varianceFactor(final Shrinkage.Scale scale, final double mean) {
        return switch (scale) {
            case identity -> 1d;
            case logit -> {
                final double p = Math.min(1 - 1e-6, Math.max(1e-6, mean));
                yield p * (1 - p);
            }
            case log -> Math.max(mean, 1e-12);
        };
    }

    /**
     * One ridge / BLUP solve over the given cells (block Gauss–Seidel until the largest effect change is
     * below {@code 1e-10 · (1 + max |z|)} or {@link #MAX_ITERATIONS}).
     *
     * <p>λ is a pseudo-count in rows, as for the other estimators: a context's ridge is {@code λ · v̄} with
     * {@code v̄ = Σw / Σn} its mean variance factor, so its shrink weight is {@code Σw / (Σw + λ v̄) = n / (n + λ)}
     * for cells of equal weight, and the identity scale ({@code v = 1}) is the plain ridge. A cell whose key
     * is null on a level (see {@link FeatureValues#keyWithNulls}) carries no indicator for that level: it
     * still informs the intercept and the levels whose keys it has.
     *
     * @param offset the cells' targets are offset by a baseline: on a transformed scale the cell term is the score-type
     *               one-step estimate {@code z_c = S_c / V_c} ({@link Shrinkage#ownScore}, spec §3 rule 5) weighted by
     *               its information {@code w_c = V_c}, and λ is on the score scale — the pseudo-count in rows times
     *               the fit's information per row, or {@link Shrinkage#lambdaFromScore} over the contexts; a cell
     *               without information ({@code V_c ≤ 0}) has no term and is left out. On identity the mean residual
     *               as before
     */
    public static Solution solve(final List<Level> levels, final List<String> cellKeys, final Collection<Cell> input,
                                 final Shrinkage.Scale scale, final boolean offset, final String weights, final double priorWeight) {
        return solve(levels, cellKeys, input, scale, offset, weights, priorWeight, -1);
    }

    /**
     * {@link #solve} with a fixed effect level ({@code fixedLevel} ≥ 0): its λ is 0, a cell without its key is left out
     * (a fixed context carries the intercept), and the system is solved by {@link #solveAbsorbed}.
     */
    public static Solution solve(final List<Level> levels, final List<String> cellKeys, final Collection<Cell> input,
                                 final Shrinkage.Scale scale, final boolean offset, final String weights, final double priorWeight,
                                 final int fixedLevel) {
        final Solution solution = new Solution();
        final List<Level> effectLevels = effectLevelsOf(levels);
        final int L = effectLevels.size();
        solution.lambdas = new double[L];
        solution.effects = new ArrayList<>();
        for (int l = 0; l < L; l++) solution.effects.add(new HashMap<>());
        final boolean score = offset && scale != Shrinkage.Scale.identity;
        final List<Cell> cells = new ArrayList<>();
        // a cell without rows — or, under an offset term, without information (every baseline at 0 or 1) — has no term
        for (final Cell c : input) if (c.n() > 0 && (!score || c.information(scale) > 0)) cells.add(c);
        if (fixedLevel >= 0) {
            final List<Integer> fixedPositions = new ArrayList<>();
            for (final String k : effectLevels.get(fixedLevel).keys()) fixedPositions.add(cellKeys.indexOf(k));
            cells.removeIf(c -> {
                final List<String> parts = FeatureValues.keyComponents(c.key());
                for (final int p : fixedPositions) if (parts.get(p) == null) return true;
                return false;
            });
        }
        if (cells.isEmpty()) return solution;
        cells.sort(Comparator.comparing(Cell::key));
        final int m = cells.size();
        final double[] z = new double[m], w = new double[m];
        double maxZ = 0, rows = 0;
        for (int i = 0; i < m; i++) {
            final Cell c = cells.get(i);
            if (score) {
                // the one-step term of the cell (S / V, the cells without information were left out above), weighted by V
                final double information = c.information(scale);
                z[i] = c.sum() / information;
                w[i] = information;
            } else {
                // the observed statistic (Σy / n) sets the delta-method weight of the transformed mean
                z[i] = Shrinkage.own(scale, c.n(), c.sum());
                w[i] = c.n() * varianceFactor(scale, c.sum() / c.n());
            }
            maxZ = Math.max(maxZ, Math.abs(z[i]));
            rows += c.n();
        }
        solution.rows = rows;
        if (score) {
            double information = 0;
            for (int i = 0; i < m; i++) information += w[i];
            solution.infoPerRow = information / rows;
        }
        // contexts of every level: the projection of the cell onto the level's key fields (−1 = a null key: no indicator)
        final int[][] ctx = new int[L][m];
        final List<List<String>> contextKeys = new ArrayList<>();
        final List<List<String>> components = new ArrayList<>(m);
        for (final Cell c : cells) components.add(FeatureValues.keyComponents(c.key()));
        for (int l = 0; l < L; l++) {
            final int[] positions = new int[effectLevels.get(l).keys().size()];
            for (int p = 0; p < positions.length; p++) positions[p] = cellKeys.indexOf(effectLevels.get(l).keys().get(p));
            final Map<String, Integer> dictionary = new HashMap<>();
            final List<String> keysInOrder = new ArrayList<>();
            for (int i = 0; i < m; i++) {
                final List<String> parts = new ArrayList<>(positions.length);
                boolean present = true;
                for (final int p : positions) {
                    final String part = components.get(i).get(p);
                    if (part == null) present = false;
                    parts.add(part);
                }
                if (!present) {
                    ctx[l][i] = -1;
                    continue;
                }
                final String key = FeatureValues.keyOf(parts);
                Integer index = dictionary.get(key);
                if (index == null) {
                    index = keysInOrder.size();
                    dictionary.put(key, index);
                    keysInOrder.add(key);
                }
                ctx[l][i] = index;
            }
            contextKeys.add(keysInOrder);
        }
        // per level and context: rows, weight and the sufficient statistics (pseudo-counts by moments, the leaf n)
        final double[][] nsum = new double[L][], wsum = new double[L][];
        for (int l = 0; l < L; l++) {
            final int K = contextKeys.get(l).size();
            nsum[l] = new double[K];
            wsum[l] = new double[K];
            final double[] sum = new double[K], sumSq = new double[K];
            for (int i = 0; i < m; i++) {
                final int k = ctx[l][i];
                if (k < 0) continue;
                nsum[l][k] += cells.get(i).n();
                wsum[l][k] += w[i];
                sum[k] += cells.get(i).sum();
                sumSq[k] += cells.get(i).sumSq();
            }
            // under a fixed level effectiveN reads the absorbed information of the first shrunk level, not the leaf's n
            if (l == 0 && fixedLevel < 0) for (int k = 0; k < K; k++) solution.leafN.put(contextKeys.get(0).get(k), nsum[0][k]);
            if (l == fixedLevel) {
                solution.lambdas[l] = 0; // not shrunk
                continue;
            }
            // the pseudo-count: in rows — or, on the score scale, λ′ = priorWeight · (the fit's information per row), so a
            // declared pseudo-count keeps its meaning of "rows of average information" across the estimators
            double lambda = score ? priorWeight * solution.infoPerRow : priorWeight;
            // under a fixed level the shrunk level's λ is estimated over the absorbed design (solveAbsorbed): the one-way
            // moments over its contexts would count the fixed level's variance as noise
            if ("varianceComponents".equals(weights) && fixedLevel < 0) {
                final Double estimated;
                if (score) {
                    // the contexts' score statistics: every context here has information (its cells do), w = V
                    double sumV = 0, sumV2 = 0, sumS = 0, sumS2OverV = 0;
                    for (int k = 0; k < K; k++) {
                        sumV += wsum[l][k];
                        sumV2 += wsum[l][k] * wsum[l][k];
                        sumS += sum[k];
                        sumS2OverV += sum[k] * sum[k] / wsum[l][k];
                    }
                    estimated = Shrinkage.lambdaFromScore(K, sumV, sumV2, sumS, sumS2OverV);
                } else {
                    double totalN = 0, totalSum = 0, totalSumSq = 0, sumSqOverN = 0, sumNSq = 0;
                    for (int k = 0; k < K; k++) {
                        totalN += nsum[l][k];
                        totalSum += sum[k];
                        totalSumSq += sumSq[k];
                        sumSqOverN += sum[k] * sum[k] / nsum[l][k];
                        sumNSq += nsum[l][k] * nsum[l][k];
                    }
                    estimated = Shrinkage.lambdaFromMoments(K, totalN, totalSum, totalSumSq, sumSqOverN, sumNSq);
                }
                if (estimated != null) lambda = estimated;
            }
            solution.lambdas[l] = lambda;
        }
        if (fixedLevel >= 0) return solveAbsorbed(solution, cells, z, w, ctx, contextKeys, nsum, wsum, fixedLevel, score,
                "varianceComponents".equals(weights) && scale == Shrinkage.Scale.identity);
        // block Gauss–Seidel: intercept, then every level's contexts, on the weighted transformed cell means
        final double[] eta = new double[m];
        final double[][] e = new double[L][], ridge = new double[L][], numerator = new double[L][], delta = new double[L][];
        for (int l = 0; l < L; l++) {
            final int K = contextKeys.get(l).size();
            e[l] = new double[K];
            numerator[l] = new double[K];
            delta[l] = new double[K];
            ridge[l] = new double[K];
            // the row pseudo-count λ in the units of the context's weights: λ · v̄ (v̄ = 1 on the identity scale) — on the
            // score scale λ is in those units already (the weights are the information)
            final double lambda = Math.max(solution.lambdas[l], 1e-9);
            for (int k = 0; k < K; k++) ridge[l][k] = score ? lambda : lambda * (wsum[l][k] / nsum[l][k]);
        }
        double totalW = 0, mu = 0;
        for (int i = 0; i < m; i++) {
            totalW += w[i];
            mu += w[i] * z[i];
        }
        mu /= totalW;
        Arrays.fill(eta, mu);
        final double tolerance = 1e-10 * (1 + maxZ);
        int iteration = 0;
        double maxDelta = Double.POSITIVE_INFINITY;
        while (iteration < MAX_ITERATIONS && maxDelta > tolerance) {
            iteration++;
            maxDelta = 0;
            double residual = 0;
            for (int i = 0; i < m; i++) residual += w[i] * (z[i] - eta[i]);
            final double dMu = residual / totalW;
            mu += dMu;
            for (int i = 0; i < m; i++) eta[i] += dMu;
            maxDelta = Math.max(maxDelta, Math.abs(dMu));
            // coarse → fine: with every λ > 0 the solution is unique and the order is immaterial; when a level's
            // ridge vanishes (λ = 0, or a negligible priorWeight) the system is rank-deficient and Gauss–Seidel
            // converges to the solution its sweep order selects — sweeping the coarser levels first attributes
            // shared signal to them, the hierarchical (ANOVA) convention of the lattice
            for (int l = L - 1; l >= 0; l--) {
                if (Double.isInfinite(solution.lambdas[l])) continue; // fixed at 0
                final int[] c = ctx[l];
                Arrays.fill(numerator[l], 0);
                for (int i = 0; i < m; i++) if (c[i] >= 0) numerator[l][c[i]] += w[i] * (z[i] - eta[i] + e[l][c[i]]);
                for (int k = 0; k < e[l].length; k++) {
                    final double updated = numerator[l][k] / (wsum[l][k] + ridge[l][k]);
                    delta[l][k] = updated - e[l][k];
                    e[l][k] = updated;
                    maxDelta = Math.max(maxDelta, Math.abs(delta[l][k]));
                }
                for (int i = 0; i < m; i++) if (c[i] >= 0) eta[i] += delta[l][c[i]];
            }
        }
        solution.mu = mu;
        solution.iterations = iteration;
        solution.maxDelta = maxDelta;
        for (int l = 0; l < L; l++) {
            final Map<String, Double> effects = solution.effects.get(l);
            for (int k = 0; k < e[l].length; k++) effects.put(contextKeys.get(l).get(k), e[l][k]);
        }
        return solution;
    }

    /**
     * The solve under a fixed level F: the fixed contexts (with the intercept) are absorbed exactly — the system on the
     * shrunk levels' effects {@code x} is {@code (Zᵀ W (I − P_F) Z + R) x = Zᵀ W (I − P_F) z}, {@code P_F} the
     * weighted mean within each fixed context and R the ridge — and solved by conjugate gradients preconditioned with
     * its diagonal ({@link AbsorbedSystem}); a fixed context's level is then the weighted mean of {@code z − Z x} over
     * its cells. With {@code estimate} (identity scale, {@code weights: varianceComponents}) and one shrunk level, its λ
     * comes from {@link #henderson} — the unpenalised fit first, then the shrunk one.
     */
    private static Solution solveAbsorbed(final Solution solution, final List<Cell> cells, final double[] z, final double[] w,
                                          final int[][] ctx, final List<List<String>> contextKeys, final double[][] nsum, final double[][] wsum,
                                          final int fixed, final boolean score, final boolean estimate) {
        final int m = cells.size(), L = ctx.length;
        final int[] f = ctx[fixed];
        final int fixedContexts = contextKeys.get(fixed).size();
        final double[] fixedWeight = new double[fixedContexts];
        for (int i = 0; i < m; i++) fixedWeight[f[i]] += w[i];
        final List<Integer> shrunk = new ArrayList<>();
        for (int l = 0; l < L; l++) if (l != fixed) shrunk.add(l);
        final double[][] information = new double[L][];
        for (final int l : shrunk) information[l] = absorbedInformation(ctx[l], f, w, wsum[l], fixedWeight);
        if (estimate && shrunk.size() == 1) {
            final int a = shrunk.get(0);
            final Henderson h = henderson(cells, z, w, ctx[a], information[a], f, fixedWeight);
            if (h != null) {
                solution.sigma2 = h.sigma2();
                solution.lambdas[a] = h.lambda();
            } else {
                LOG.warn("joint fit, Henderson III: no residual degrees of freedom or no information beside the fixed level ({} cells):"
                        + " lambda = priorWeight {}, no sigma2 (variance reads null)", m, solution.lambdas[a]);
            }
        }
        // the levels with a finite λ are solved; one whose λ is +∞ keeps every effect at 0, as in the Gauss–Seidel solve
        final List<Integer> active = new ArrayList<>();
        for (final int l : shrunk) if (!Double.isInfinite(solution.lambdas[l])) active.add(l);
        final int[][] activeCtx = new int[active.size()][];
        final double[][] ridge = new double[active.size()][], activeInformation = new double[active.size()][];
        for (int j = 0; j < active.size(); j++) {
            final int l = active.get(j);
            activeCtx[j] = ctx[l];
            activeInformation[j] = information[l];
            ridge[j] = new double[wsum[l].length];
            final double lambda = Math.max(solution.lambdas[l], 1e-9);
            for (int k = 0; k < ridge[j].length; k++) ridge[j][k] = score ? lambda : lambda * (wsum[l][k] / nsum[l][k]);
        }
        final AbsorbedSystem system = new AbsorbedSystem(activeCtx, f, w, fixedWeight, ridge, activeInformation);
        final double[] x = system.solve(system.project(z));
        final double[] fitted = system.predict(x);
        final double[] level = system.fixedLevels(z, fitted);
        double mu = 0, totalW = 0;
        for (int r = 0; r < fixedContexts; r++) {
            mu += fixedWeight[r] * level[r];
            totalW += fixedWeight[r];
        }
        mu /= totalW;
        solution.mu = mu;
        solution.iterations = system.iterations;
        solution.maxDelta = system.residual;
        for (int r = 0; r < fixedContexts; r++) solution.effects.get(fixed).put(contextKeys.get(fixed).get(r), level[r] - mu);
        for (final int l : shrunk) {
            final int j = active.indexOf(l);
            for (int k = 0; k < contextKeys.get(l).size(); k++) {
                solution.effects.get(l).put(contextKeys.get(l).get(k), j < 0 ? 0d : x[system.offsets[j] + k]);
            }
        }
        // effectiveN's level: its contexts' information in rows (over the ridge unit: rows on identity, a context's mean
        // delta-method weight on logit / log, the fit's information per row on the score scale)
        final int neff = effectiveNLevel(fixed); // = shrunk.get(0)
        for (int k = 0; k < contextKeys.get(neff).size(); k++) {
            final double unit = score ? solution.infoPerRow : wsum[neff][k] / nsum[neff][k];
            solution.information.put(contextKeys.get(neff).get(k), information[neff][k] / unit);
        }
        return solution;
    }

    /** Per context of a shrunk level: {@code Σ_cells w − Σ_fixed (Σ_{cells of both} w)² / W_fixed}, the diagonal of the absorbed system. */
    static double[] absorbedInformation(final int[] contexts, final int[] fixed, final double[] w, final double[] contextWeight, final double[] fixedWeight) {
        final double[] information = contextWeight.clone();
        final Map<Long, Double> pairs = new HashMap<>();
        for (int i = 0; i < w.length; i++) {
            if (contexts[i] < 0) continue;
            pairs.merge((long) contexts[i] * fixedWeight.length + fixed[i], w[i], Double::sum);
        }
        // in key order, so the sums do not depend on the map's iteration order
        for (final Map.Entry<Long, Double> e : new TreeMap<>(pairs).entrySet()) {
            final int k = (int) (e.getKey() / fixedWeight.length), r = (int) (e.getKey() % fixedWeight.length);
            information[k] -= e.getValue() * e.getValue() / fixedWeight[r];
        }
        for (int k = 0; k < information.length; k++) information[k] = Math.max(information[k], 0d);
        return information;
    }

    /** The λ of Henderson's method III and the residual variance it rests on. */
    record Henderson(double sigma2, double lambda) {}

    /**
     * Henderson's method III for one shrunk level A beside the fixed level F, on the identity scale (cell weights =
     * rows): the reduction {@code R(A | F) = SSE(F) − SSE(F + A)} of the unpenalised fits has expectation
     * {@code (rank(F, A) − rank(F)) σ² + tr(Zᵀ (I − P_F) Z) τ²}, with {@code σ² = SSE(F + A) / (N − rank(F, A))},
     * {@code rank(F, A) = K_F + K_A − c} (c = the connected components of the bipartite graph of F and A contexts that
     * hold an A context and no row without an A key: such a row breaks the component's one dependency
     * {@code Σ F = Σ A}) and the trace the sum of the A contexts' absorbed information. λ = σ² / τ²; a τ² ≤ 0 fixes
     * the level at 0 (λ = +∞, as the moment estimator truncates); null (the declared priorWeight) without residual
     * degrees of freedom or information.
     */
    static Henderson henderson(final List<Cell> cells, final double[] z, final double[] w, final int[] a, final double[] information,
                               final int[] f, final double[] fixedWeight) {
        final int m = cells.size(), fixedContexts = fixedWeight.length, contexts = information.length;
        double sumSq = 0, rows = 0;
        final double[] fixedSum = new double[fixedContexts];
        for (int i = 0; i < m; i++) {
            sumSq += cells.get(i).sumSq();
            rows += cells.get(i).n();
            fixedSum[f[i]] += cells.get(i).sum();
        }
        double sseFixed = sumSq;
        for (int r = 0; r < fixedContexts; r++) sseFixed -= fixedSum[r] * fixedSum[r] / fixedWeight[r];
        final AbsorbedSystem unpenalised = new AbsorbedSystem(new int[][]{a}, f, w, fixedWeight, new double[][]{new double[contexts]},
                new double[][]{information});
        final double[] fitted = unpenalised.predict(unpenalised.solve(unpenalised.project(z)));
        final double[] level = unpenalised.fixedLevels(z, fitted);
        double sseFull = 0;
        for (int i = 0; i < m; i++) {
            final Cell c = cells.get(i);
            final double fit = level[f[i]] + fitted[i];
            sseFull += c.sumSq() - 2 * fit * c.sum() + c.n() * fit * fit;
        }
        sseFull = Math.max(sseFull, 0d);
        // union-find over the F contexts [0, K_F) and the A contexts [K_F, K_F + K_A)
        final int[] parent = new int[fixedContexts + contexts];
        for (int v = 0; v < parent.length; v++) parent[v] = v;
        // an F context holding a row without an A key keeps its component's F and A columns independent
        final boolean[] open = new boolean[fixedContexts];
        for (int i = 0; i < m; i++) {
            if (a[i] >= 0) union(parent, f[i], fixedContexts + a[i]);
            else open[f[i]] = true;
        }
        final boolean[] openRoot = new boolean[parent.length];
        for (int r = 0; r < fixedContexts; r++) if (open[r]) openRoot[find(parent, r)] = true;
        final Set<Integer> components = new HashSet<>();
        for (int k = 0; k < contexts; k++) {
            final int root = find(parent, fixedContexts + k);
            if (!openRoot[root]) components.add(root);
        }
        final double rank = fixedContexts + contexts - components.size();
        final double df = rows - rank;
        double trace = 0;
        for (final double v : information) trace += v;
        if (df <= 0 || trace <= 0) return null;
        final double sigma2 = sseFull / df;
        final double tau2 = (sseFixed - sseFull - (rank - fixedContexts) * sigma2) / trace;
        LOG.info("joint fit, Henderson III: sigma2 {} tau2 {} ({} rows, rank {}, {} components, {} iterations)", sigma2, tau2, rows, rank,
                components.size(), unpenalised.iterations);
        return new Henderson(sigma2, tau2 > 0 ? sigma2 / tau2 : Double.POSITIVE_INFINITY);
    }

    private static int find(final int[] parent, int v) {
        while (parent[v] != v) {
            parent[v] = parent[parent[v]];
            v = parent[v];
        }
        return v;
    }

    private static void union(final int[] parent, final int u, final int v) {
        final int ru = find(parent, u), rv = find(parent, v);
        if (ru != rv) parent[Math.max(ru, rv)] = Math.min(ru, rv);
    }

    /**
     * The system of the shrunk levels once a fixed level is absorbed, applied matrix-free over the cells: one product
     * is two passes over them. Its diagonal (the absorbed information plus the ridge) preconditions the conjugate
     * gradients; a context without information and without ridge (its every cell alone in its fixed context — its
     * column is 0) is left at 0.
     */
    static final class AbsorbedSystem {
        final int[][] ctx;
        final int[] offsets;
        final int[] fixed;
        final double[] w, fixedWeight, ridge, diagonal;
        final int dimension;
        int iterations;
        double residual;
        /** Scratch of one product (the cells' shrunk effects, the fixed contexts' means), reused across the iterations. */
        private final double[] cellScratch, meanScratch;

        AbsorbedSystem(final int[][] ctx, final int[] fixed, final double[] w, final double[] fixedWeight, final double[][] ridge,
                       final double[][] information) {
            this.ctx = ctx;
            this.fixed = fixed;
            this.w = w;
            this.fixedWeight = fixedWeight;
            this.cellScratch = new double[w.length];
            this.meanScratch = new double[fixedWeight.length];
            this.offsets = new int[ctx.length];
            int d = 0;
            for (int l = 0; l < ctx.length; l++) {
                offsets[l] = d;
                d += ridge[l].length;
            }
            this.dimension = d;
            this.ridge = new double[d];
            this.diagonal = new double[d];
            for (int l = 0; l < ctx.length; l++) {
                for (int k = 0; k < ridge[l].length; k++) {
                    this.ridge[offsets[l] + k] = ridge[l][k];
                    this.diagonal[offsets[l] + k] = information[l][k] + ridge[l][k];
                }
            }
        }

        /** {@code (Z x)_i}: the shrunk effects of each cell. */
        double[] predict(final double[] x) {
            return predictInto(x, new double[w.length]);
        }

        private double[] predictInto(final double[] x, final double[] u) {
            Arrays.fill(u, 0d);
            for (int l = 0; l < ctx.length; l++) {
                final int[] c = ctx[l];
                for (int i = 0; i < u.length; i++) if (c[i] >= 0) u[i] += x[offsets[l] + c[i]];
            }
            return u;
        }

        /** {@code Zᵀ W (I − P_F) v}. */
        double[] project(final double[] v) {
            return projectInto(v, new double[dimension]);
        }

        private double[] projectInto(final double[] v, final double[] out) {
            final double[] mean = meanScratch;
            Arrays.fill(mean, 0d);
            for (int i = 0; i < v.length; i++) mean[fixed[i]] += w[i] * v[i];
            for (int r = 0; r < mean.length; r++) mean[r] /= fixedWeight[r];
            Arrays.fill(out, 0d);
            for (int i = 0; i < v.length; i++) {
                final double d = w[i] * (v[i] - mean[fixed[i]]);
                for (int l = 0; l < ctx.length; l++) if (ctx[l][i] >= 0) out[offsets[l] + ctx[l][i]] += d;
            }
            return out;
        }

        /** Per fixed context: the weighted mean of {@code z − Z x} over its cells. */
        double[] fixedLevels(final double[] z, final double[] fitted) {
            final double[] level = new double[fixedWeight.length];
            for (int i = 0; i < z.length; i++) level[fixed[i]] += w[i] * (z[i] - fitted[i]);
            for (int r = 0; r < level.length; r++) level[r] /= fixedWeight[r];
            return level;
        }

        /** {@code (Zᵀ W (I − P_F) Z + R) x} into {@code out}. */
        private void multiplyInto(final double[] x, final double[] out) {
            projectInto(predictInto(x, cellScratch), out);
            for (int j = 0; j < dimension; j++) out[j] += ridge[j] * x[j];
        }

        /** Preconditioned conjugate gradients from 0 until the relative residual is below {@link #CG_TOLERANCE}. */
        double[] solve(final double[] b) {
            final double[] x = new double[dimension];
            final double[] r = b.clone();
            final double bNorm = MatrixOps.norm(b);
            iterations = 0;
            residual = 0;
            if (!(bNorm > 0)) return x;
            // not converged until an iteration says so: a breakdown on the first step must not read as a solve
            residual = 1;
            final double[] s = new double[dimension], q = new double[dimension];
            precondition(r, s);
            final double[] p = s.clone();
            double rs = MatrixOps.dot(r, s);
            for (int it = 1; it <= MAX_CG_ITERATIONS; it++) {
                multiplyInto(p, q);
                final double pq = MatrixOps.dot(p, q);
                if (!(pq > 0)) break;
                final double alpha = rs / pq;
                for (int j = 0; j < dimension; j++) {
                    x[j] += alpha * p[j];
                    r[j] -= alpha * q[j];
                }
                iterations = it;
                residual = MatrixOps.norm(r) / bNorm;
                if (residual <= CG_TOLERANCE) break;
                precondition(r, s);
                final double next = MatrixOps.dot(r, s);
                final double beta = next / rs;
                rs = next;
                for (int j = 0; j < dimension; j++) p[j] = s[j] + beta * p[j];
            }
            if (residual > CG_TOLERANCE) LOG.warn("joint fit: conjugate gradients stopped at relative residual {} after {} iterations", residual, iterations);
            return x;
        }

        private void precondition(final double[] r, final double[] s) {
            for (int j = 0; j < dimension; j++) s[j] = diagonal[j] > 0 ? r[j] / diagonal[j] : 0d;
        }
    }

    // ------------------------------------------------------------------------------------------
    // apply
    // ------------------------------------------------------------------------------------------

    /**
     * The solution a row reads: its fold's ({@code fold} non-null and fitted per fold), the one whose window
     * its usable block falls in ({@code usableBlock} non-null and fitted per block; null when fewer than
     * {@code minBlocks} observed blocks precede it or nothing lies inside its window), else the whole-input
     * solution.
     */
    public Solution solutionFor(final Integer fold, final Long usableBlock, final int minBlocks) {
        if (folds != null && fold != null && fold >= 0 && fold < folds.length) return folds[fold];
        if (blocks != null) {
            if (usableBlock == null) return null;
            final Map.Entry<Long, Solution> floor = blocks.floorEntry(usableBlock);
            if (floor == null || !BlockSeries.reached(observedBlocks, usableBlock, minBlocks)) return null;
            return floor.getValue().isEmpty() ? null : floor.getValue();
        }
        return total;
    }

    /**
     * The composed estimate on the original scale — or, under an offset, the additive term on the shrinkage scale
     * itself; null without a solution or for a row that takes no part ({@link #takesPart}: a null leaf key).
     */
    public Double estimate(final Solution s, final Map<String, Object> row) {
        if (s == null || s.isEmpty()) return null;
        if (!takesPart(effectLevels, fixedLevel, mainEffects, row)) return null;
        double eta = s.mu;
        for (int l = 0; l < effectLevels.size(); l++) {
            final Double e = effect(s, l, row);
            if (e != null) eta += e;
        }
        return Shrinkage.output(scale, eta, offset);
    }

    /**
     * The effect of one level for the row (transform scale): 0 for an unseen context, null without a key. A fixed
     * level has no prior to fall back to: its unseen context is null (the estimate then reads the average fixed
     * context, {@code μ}).
     */
    public Double effect(final Solution s, final int level, final Map<String, Object> row) {
        if (s == null || s.isEmpty()) return null;
        final String key = FeatureValues.key(row, effectLevels.get(level).keys());
        if (key == null) return null;
        return level == fixedLevel ? s.effects.get(level).get(key) : s.effects.get(level).getOrDefault(key, 0d);
    }

    /**
     * A {@code lookups} column: the effect of {@code level} — {@code μ +} it with {@code level read} — at the context the
     * row's {@code fields} name (a past context in a lag column, read with the model of the row's time). Null without a
     * solution, with a null field, or for a context the solve did not fit: no prior to fall back to, the question is
     * what the model fitted for that context.
     */
    public Double lookup(final Solution s, final int level, final Map<String, Object> row, final List<String> fields, final boolean levelRead) {
        if (s == null || s.isEmpty()) return null;
        final String key = FeatureValues.key(row, fields);
        if (key == null) return null;
        final Double e = s.effects.get(level).get(key);
        if (e == null) return null;
        return levelRead ? s.mu + e : e;
    }

    /**
     * Effective sample size of the leaf: its n plus the leaf pseudo-count (the whole fit when fully shrunk). Under a
     * fixed level it is that of the first shrunk level: its information once the fixed level is absorbed — the rows
     * that tell its contexts apart, a row alone in its fixed context telling nothing — plus λ.
     */
    public Double effectiveN(final Solution s, final Map<String, Object> row) {
        if (s == null || s.isEmpty()) return null;
        if (effectLevels.isEmpty()) return s.rows;
        final int level = effectiveNLevel();
        final String key = FeatureValues.key(row, effectLevels.get(level).keys());
        if (key == null) return null;
        final double n = fixedLevel >= 0 ? s.information.getOrDefault(key, 0d) : s.leafN.getOrDefault(key, 0d);
        final double lambda = s.lambdas[level] / s.infoPerRow; // in rows (a score-scale λ divided by the information per row)
        return Double.isInfinite(lambda) ? s.rows : n + lambda;
    }

    /**
     * Under a fixed level with an estimated λ: the approximate posterior variance of the row's effect on the first
     * shrunk level, {@code σ² / effectiveN} — the diagonal of the absorbed system, its coupling to the contexts that
     * share fixed contexts left out; τ² = σ² / λ for an unseen context, 0 for a level fixed at 0. Null otherwise.
     */
    public Double variance(final Solution s, final Map<String, Object> row) {
        if (s == null || s.isEmpty() || Double.isNaN(s.sigma2) || fixedLevel < 0) return null;
        final int level = effectiveNLevel();
        if (Double.isInfinite(s.lambdas[level])) return FeatureValues.key(row, effectLevels.get(level).keys()) == null ? null : 0d;
        final Double n = effectiveN(s, row);
        return n == null || !(n > 0) ? null : s.sigma2 / n;
    }

    // ------------------------------------------------------------------------------------------
    // coordinates and artifact
    // ------------------------------------------------------------------------------------------

    /** {@code token=key1,key2;token2=key;global=} — the lattice levels for the column coordinates. */
    public static String encodeLevels(final List<Level> levels) {
        final StringBuilder sb = new StringBuilder();
        for (final Level l : levels) {
            if (!sb.isEmpty()) sb.append(';');
            sb.append(l.token()).append('=').append(String.join(",", l.keys()));
        }
        return sb.toString();
    }

    public static List<Level> parseLevels(final String text) {
        final List<Level> levels = new ArrayList<>();
        for (final String part : text.split(";")) {
            final int eq = part.indexOf('=');
            final String keys = part.substring(eq + 1);
            levels.add(new Level(part.substring(0, eq), keys.isEmpty() ? List.of() : List.of(keys.split(","))));
        }
        return levels;
    }

    public static final Schema SCHEMA = new Schema.Parser().parse("""
            {"type": "record", "name": "FeatureJointFit", "namespace": "com.mercari.solution.feature",
             "fields": [
               {"name": "kind", "type": "string"},
               {"name": "level", "type": "string"},
               {"name": "key", "type": "string"},
               {"name": "value", "type": "double"}
             ]}
            """);

    public static String artifactPath(final String artifactUri, final String planHash, final String id) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + id + ".joint.avro";
    }

    public static String manifestPath(final String artifactUri, final String planHash, final String id) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + id + ".joint.manifest.json";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String id) {
        return ResourceUtil.exists(artifactPath(artifactUri, planHash, id));
    }

    /** Persists the whole-input solution (fold / forward parts are always re-fitted) plus a manifest. */
    public static void write(final String artifactUri, final String planHash, final String id, final JointFit fit) {
        final String path = artifactPath(artifactUri, planHash, id);
        final Solution s = fit.total;
        final List<Level> effectLevels = fit.effectLevels;
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            long entries = 0;
            try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
                writer.create(SCHEMA, bytes);
                writer.append(record("mu", "", "", s.mu));
                writer.append(record("rows", "", "", s.rows));
                writer.append(record("infoPerRow", "", "", s.infoPerRow));
                for (int l = 0; l < effectLevels.size(); l++) {
                    final String token = effectLevels.get(l).token();
                    writer.append(record("lambda", token, "", s.lambdas[l]));
                    for (final Map.Entry<String, Double> e : new TreeMap<>(s.effects.get(l)).entrySet()) {
                        writer.append(record("effect", token, e.getKey(), e.getValue()));
                        entries++;
                    }
                }
                for (final Map.Entry<String, Double> e : new TreeMap<>(s.leafN).entrySet()) writer.append(record("leafN", "", e.getKey(), e.getValue()));
                if (!Double.isNaN(s.sigma2)) writer.append(record("sigma2", "", "", s.sigma2));
                for (final Map.Entry<String, Double> e : new TreeMap<>(s.information).entrySet()) writer.append(record("information", "", e.getKey(), e.getValue()));
            }
            ResourceUtil.writeBytes(path, bytes.toByteArray());
            final JsonObject manifest = FitArtifact.manifest(planHash, id);
            manifest.addProperty("estimator", "joint");
            manifest.addProperty("scale", fit.scale.name());
            manifest.addProperty("offset", fit.offset);
            final JsonArray levels = new JsonArray();
            for (final Level l : fit.levels) {
                final JsonObject o = new JsonObject();
                o.addProperty("token", l.token());
                o.addProperty("keys", String.join(",", l.keys()));
                levels.add(o);
            }
            manifest.add("levels", levels);
            if (fit.fixedLevel >= 0) {
                // the solve absorbed this level (conjugate gradients); its λ is 0 and μ the weighted mean of its contexts
                manifest.addProperty("fixedLevel", effectLevels.get(fit.fixedLevel).token());
                manifest.addProperty("solver", "conjugateGradient");
                if (!Double.isNaN(s.sigma2)) manifest.addProperty("sigma2", s.sigma2);
            }
            manifest.addProperty("mu", s.mu);
            final JsonObject lambdas = new JsonObject();
            final JsonObject contexts = new JsonObject();
            for (int l = 0; l < effectLevels.size(); l++) {
                lambdas.add(effectLevels.get(l).token(), FitArtifact.lambdaJson(s.lambdas[l]));
                contexts.addProperty(effectLevels.get(l).token(), s.effects.get(l).size());
            }
            manifest.add("lambdas", lambdas);
            manifest.add("contexts", contexts);
            manifest.addProperty("rows", s.rows);
            if (fit.offset && fit.scale != Shrinkage.Scale.identity) manifest.addProperty("infoPerRow", s.infoPerRow);
            manifest.addProperty("iterations", s.iterations);
            manifest.addProperty("maxDelta", s.maxDelta);
            manifest.addProperty("entries", entries);
            ResourceUtil.writeString(manifestPath(artifactUri, planHash, id), manifest.toString());
            LOG.info("wrote joint fit artifact {} ({} effects)", path, entries);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write joint fit artifact: " + path, e);
        }
    }

    private static GenericRecord record(final String kind, final String level, final String key, final double value) {
        final GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("kind", kind);
        record.put("level", level);
        record.put("key", key);
        record.put("value", value);
        return record;
    }

    /** Loads the whole-input solution written by {@link #write} (the geometry comes from the column coordinates). */
    public static JointFit read(final String artifactUri, final String planHash, final String id, final List<Level> levels,
                                final Shrinkage.Scale scale, final boolean offset) {
        return read(artifactUri, planHash, id, levels, scale, offset, -1, false);
    }

    /** {@link #read} of a fit with a fixed effect level (−1 for none) over a lattice of main effects alone or not. */
    public static JointFit read(final String artifactUri, final String planHash, final String id, final List<Level> levels,
                                final Shrinkage.Scale scale, final boolean offset, final int fixedLevel, final boolean mainEffects) {
        final String path = artifactPath(artifactUri, planHash, id);
        final Solution s = new Solution();
        final List<Level> effectLevels = effectLevelsOf(levels);
        final Map<String, Integer> levelIndex = new HashMap<>();
        for (int l = 0; l < effectLevels.size(); l++) levelIndex.put(effectLevels.get(l).token(), l);
        s.lambdas = new double[effectLevels.size()];
        s.effects = new ArrayList<>();
        for (int l = 0; l < effectLevels.size(); l++) s.effects.add(new HashMap<>());
        boolean hasInfoPerRow = false;
        try (final DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(ResourceUtil.readBytes(path)), new GenericDatumReader<>(SCHEMA))) {
            while (reader.hasNext()) {
                final GenericRecord record = reader.next();
                final String kind = record.get("kind").toString();
                final String level = record.get("level").toString();
                final String key = record.get("key").toString();
                final double value = (Double) record.get("value");
                switch (kind) {
                    case "mu" -> s.mu = value;
                    case "rows" -> s.rows = value;
                    case "infoPerRow" -> { s.infoPerRow = value; hasInfoPerRow = true; }
                    case "lambda" -> { if (levelIndex.containsKey(level)) s.lambdas[levelIndex.get(level)] = value; }
                    case "effect" -> { if (levelIndex.containsKey(level)) s.effects.get(levelIndex.get(level)).put(key, value); }
                    case "leafN" -> s.leafN.put(key, value);
                    case "sigma2" -> s.sigma2 = value;
                    case "information" -> s.information.put(key, value);
                    default -> { }
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read joint fit artifact: " + path, e);
        }
        if (offset && scale != Shrinkage.Scale.identity && !hasInfoPerRow) {
            // the effects of such an artifact were fitted as the transformed means t(ȳ_c) − t(b̄_c) (with the clamp the
            // score-type estimator replaced), not as score terms: serving them as terms would silently mix estimators
            throw new IllegalStateException("joint fit artifact " + path + " was written before the score-type offset estimator"
                    + " (no infoPerRow): its effects are on the old transformed-mean scale; refit the block (fit.artifact.refit: true)");
        }
        LOG.info("loaded joint fit artifact {}", path);
        return new JointFit(levels, scale, offset, s, null, null, null, fixedLevel, mainEffects);
    }

}
