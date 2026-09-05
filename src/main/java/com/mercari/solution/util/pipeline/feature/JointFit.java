package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.file.ResourceUtil;
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
 * {@code w_c = n_c · v(ȳ_c)} (v = 1 / p(1−p) / μ for identity / logit / log). Unlike the sequential estimator
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
 * effects); {@code forward} solves once per time block on the cumulative cells up to that block (minus the
 * blocks beyond the window). The artifact holds the whole-input solution only ({@code <id>.joint.avro}),
 * like the encoding-level statistics.
 */
public final class JointFit implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JointFit.class);

    /** Separates a cell key from its fold / block tag in the aggregation key. */
    static final char TAG = (char) 2;
    static final String FOLD_PREFIX = "#";

    static final int MAX_ITERATIONS = 1000;

    /** One lattice level: its token and the key fields of its contexts (empty = the global intercept). */
    public record Level(String token, List<String> keys) implements Serializable {
        boolean isGlobal() {
            return keys.isEmpty();
        }
    }

    /** One aggregated cell (or a fold / block tagged part of one): the finest partition of the lattice. */
    public record Cell(String key, double n, double sum, double sumSq) implements Serializable {}

    /** The fitted effects of one solve. */
    public static final class Solution implements Serializable {
        public double mu = Double.NaN;
        /** Per effect level (the non-global levels, lattice order): λ used (+∞ = level fixed at 0). */
        public double[] lambdas;
        /** Per effect level: context key → effect on the transform scale. */
        public List<Map<String, Double>> effects;
        /** Leaf context key → n (rows of the fit), for effectiveN. */
        public Map<String, Double> leafN = new HashMap<>();
        public double rows;
        public int iterations;
        public double maxDelta;

        boolean isEmpty() {
            return Double.isNaN(mu);
        }
    }

    public final List<Level> levels;
    public final List<String> cellKeys;
    public final Shrinkage.Scale scale;
    public final Solution total;
    /** fit.mode fold: the out-of-fold solution per fold, or null. */
    public final Solution[] folds;
    /** fit.mode forward: the cumulative solution per block index, or null. */
    public final TreeMap<Long, Solution> blocks;

    JointFit(final List<Level> levels, final List<String> cellKeys, final Shrinkage.Scale scale, final Solution total,
             final Solution[] folds, final TreeMap<Long, Solution> blocks) {
        this.levels = levels;
        this.cellKeys = cellKeys;
        this.scale = scale;
        this.total = total;
        this.folds = folds;
        this.blocks = blocks;
    }

    /** The effect levels (non-global) in lattice order. */
    public List<Level> effectLevels() {
        final List<Level> out = new ArrayList<>();
        for (final Level l : levels) if (!l.isGlobal()) out.add(l);
        return out;
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
     */
    public static JointFit fit(final List<Level> levels, final Shrinkage.Scale scale, final String weights, final double priorWeight,
                               final Collection<Cell> entries, final int folds, final boolean forward, final int windowBlocks) {
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
        final Solution totalSolution = solve(levels, cellKeys, total.values(), scale, weights, priorWeight);
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
                foldSolutions[f] = solve(levels, cellKeys, outOfFold.values(), scale, weights, priorWeight);
            }
        }
        TreeMap<Long, Solution> blockSolutions = null;
        if (forward) {
            blockSolutions = new TreeMap<>();
            final Map<String, Cell> cumulative = new HashMap<>();
            final List<Long> order = new ArrayList<>(blockParts.keySet());
            int oldest = 0; // index of the oldest block still inside the window
            for (int i = 0; i < order.size(); i++) {
                final long block = order.get(i);
                for (final Map.Entry<String, Cell> e : blockParts.get(block).entrySet()) merge(cumulative, e.getKey(), e.getValue());
                if (windowBlocks > 0) {
                    while (order.get(oldest) <= block - windowBlocks) {
                        for (final Map.Entry<String, Cell> e : blockParts.get(order.get(oldest)).entrySet()) {
                            final Cell rest = subtract(cumulative.get(e.getKey()), e.getValue());
                            if (rest == null) cumulative.remove(e.getKey());
                            else cumulative.put(e.getKey(), rest);
                        }
                        oldest++;
                    }
                }
                blockSolutions.put(block, solve(levels, cellKeys, cumulative.values(), scale, weights, priorWeight));
            }
        }
        return new JointFit(levels, cellKeys, scale, totalSolution, foldSolutions, blockSolutions);
    }

    private static void merge(final Map<String, Cell> into, final String key, final Cell part) {
        into.merge(key, new Cell(key, part.n(), part.sum(), part.sumSq()),
                (a, b) -> new Cell(key, a.n() + b.n(), a.sum() + b.sum(), a.sumSq() + b.sumSq()));
    }

    /** {@code total − part}; null when nothing remains. */
    static Cell subtract(final Cell total, final Cell part) {
        if (total == null) return null;
        final double n = total.n() - part.n();
        if (n <= 1e-9) return null;
        return new Cell(total.key(), n, total.sum() - part.sum(), total.sumSq() - part.sumSq());
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
     */
    public static Solution solve(final List<Level> levels, final List<String> cellKeys, final Collection<Cell> input,
                                 final Shrinkage.Scale scale, final String weights, final double priorWeight) {
        final Solution solution = new Solution();
        final List<Level> effectLevels = new ArrayList<>();
        for (final Level l : levels) if (!l.isGlobal()) effectLevels.add(l);
        final int L = effectLevels.size();
        solution.lambdas = new double[L];
        solution.effects = new ArrayList<>();
        for (int l = 0; l < L; l++) solution.effects.add(new TreeMap<>());
        final List<Cell> cells = new ArrayList<>();
        for (final Cell c : input) if (c.n() > 0) cells.add(c);
        if (cells.isEmpty()) return solution;
        cells.sort(Comparator.comparing(Cell::key));
        final int m = cells.size();
        final double[] z = new double[m], w = new double[m];
        double maxZ = 0, rows = 0;
        for (int i = 0; i < m; i++) {
            final Cell c = cells.get(i);
            final double mean = c.sum() / c.n();
            z[i] = Shrinkage.transform(scale, mean);
            w[i] = c.n() * varianceFactor(scale, mean);
            maxZ = Math.max(maxZ, Math.abs(z[i]));
            rows += c.n();
        }
        solution.rows = rows;
        // contexts of every level: the projection of the cell onto the level's key fields
        final int[][] ctx = new int[L][m];
        final List<List<String>> contextKeys = new ArrayList<>();
        final List<List<String>> components = new ArrayList<>(m);
        for (final Cell c : cells) components.add(FeatureValues.keyComponents(c.key()));
        for (int l = 0; l < L; l++) {
            final int[] positions = new int[effectLevels.get(l).keys().size()];
            for (int p = 0; p < positions.length; p++) positions[p] = cellKeys.indexOf(effectLevels.get(l).keys().get(p));
            final Map<String, Integer> dictionary = new TreeMap<>();
            final List<String> keysInOrder = new ArrayList<>();
            for (int i = 0; i < m; i++) {
                final List<String> parts = new ArrayList<>(positions.length);
                for (final int p : positions) parts.add(components.get(i).get(p));
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
        // pseudo-counts per level (moments over the level's contexts), and the leaf n per context
        final boolean[] fixedAtZero = new boolean[L];
        for (int l = 0; l < L; l++) {
            final int K = contextKeys.get(l).size();
            final double[] n = new double[K], sum = new double[K], sumSq = new double[K];
            for (int i = 0; i < m; i++) {
                n[ctx[l][i]] += cells.get(i).n();
                sum[ctx[l][i]] += cells.get(i).sum();
                sumSq[ctx[l][i]] += cells.get(i).sumSq();
            }
            if (l == 0) for (int k = 0; k < K; k++) solution.leafN.put(contextKeys.get(0).get(k), n[k]);
            double lambda = priorWeight;
            if ("varianceComponents".equals(weights)) {
                double totalN = 0, totalSum = 0, totalSumSq = 0, sumSqOverN = 0, sumNSq = 0;
                for (int k = 0; k < K; k++) {
                    totalN += n[k];
                    totalSum += sum[k];
                    totalSumSq += sumSq[k];
                    sumSqOverN += sum[k] * sum[k] / n[k];
                    sumNSq += n[k] * n[k];
                }
                final Double estimated = Shrinkage.lambdaFromMoments(K, totalN, totalSum, totalSumSq, sumSqOverN, sumNSq);
                if (estimated != null) lambda = estimated;
            }
            if (Double.isInfinite(lambda)) fixedAtZero[l] = true;
            solution.lambdas[l] = lambda;
        }
        // block Gauss–Seidel: intercept, then every level's contexts, on the weighted transformed cell means
        final double[] eta = new double[m];
        final double[][] e = new double[L][];
        final double[][] wsum = new double[L][];
        for (int l = 0; l < L; l++) {
            e[l] = new double[contextKeys.get(l).size()];
            wsum[l] = new double[contextKeys.get(l).size()];
            for (int i = 0; i < m; i++) wsum[l][ctx[l][i]] += w[i];
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
                if (fixedAtZero[l]) continue;
                final double lambda = Math.max(solution.lambdas[l], 1e-9);
                final double[] numerator = new double[e[l].length];
                for (int i = 0; i < m; i++) numerator[ctx[l][i]] += w[i] * (z[i] - eta[i] + e[l][ctx[l][i]]);
                final double[] delta = new double[e[l].length];
                for (int k = 0; k < e[l].length; k++) {
                    final double updated = numerator[k] / (wsum[l][k] + lambda);
                    delta[k] = updated - e[l][k];
                    e[l][k] = updated;
                    maxDelta = Math.max(maxDelta, Math.abs(delta[k]));
                }
                for (int i = 0; i < m; i++) eta[i] += delta[ctx[l][i]];
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

    // ------------------------------------------------------------------------------------------
    // apply
    // ------------------------------------------------------------------------------------------

    /**
     * The solution a row reads: its fold's ({@code fold} non-null and fitted per fold), the block floor of
     * its usable block ({@code usableBlock} non-null and fitted per block; null when fewer than
     * {@code minBlocks} solved blocks precede it), else the whole-input solution.
     */
    public Solution solutionFor(final Integer fold, final Long usableBlock, final int minBlocks) {
        if (folds != null && fold != null && fold >= 0 && fold < folds.length) return folds[fold];
        if (blocks != null) {
            if (usableBlock == null) return null;
            final Map.Entry<Long, Solution> floor = blocks.floorEntry(usableBlock);
            if (floor == null || blocks.headMap(usableBlock, true).size() < minBlocks) return null;
            return floor.getValue();
        }
        return total;
    }

    /** The composed estimate on the original scale; null without a leaf key or a solution. */
    public Double estimate(final Solution s, final Map<String, Object> row) {
        if (s == null || s.isEmpty()) return null;
        final List<Level> effectLevels = effectLevels();
        if (!effectLevels.isEmpty() && FeatureValues.key(row, effectLevels.get(0).keys()) == null) return null;
        double eta = s.mu;
        for (int l = 0; l < effectLevels.size(); l++) {
            final Double e = effect(s, l, row);
            if (e != null) eta += e;
        }
        return Shrinkage.inverse(scale, eta);
    }

    /** The effect of one level for the row (transform scale): 0 for an unseen context, null without a key. */
    public Double effect(final Solution s, final int level, final Map<String, Object> row) {
        if (s == null || s.isEmpty()) return null;
        final String key = FeatureValues.key(row, effectLevels().get(level).keys());
        if (key == null) return null;
        return s.effects.get(level).getOrDefault(key, 0d);
    }

    /** Effective sample size of the leaf: its n plus the leaf pseudo-count (the whole fit when fully shrunk). */
    public Double effectiveN(final Solution s, final Map<String, Object> row) {
        if (s == null || s.isEmpty()) return null;
        final List<Level> effectLevels = effectLevels();
        if (effectLevels.isEmpty()) return s.rows;
        final String key = FeatureValues.key(row, effectLevels.get(0).keys());
        if (key == null) return null;
        final double n = s.leafN.getOrDefault(key, 0d);
        final double lambda = s.lambdas[0];
        return Double.isInfinite(lambda) ? s.rows : n + lambda;
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
        final List<Level> effectLevels = fit.effectLevels();
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            long entries = 0;
            try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
                writer.create(SCHEMA, bytes);
                writer.append(record("mu", "", "", s.mu));
                writer.append(record("rows", "", "", s.rows));
                for (int l = 0; l < effectLevels.size(); l++) {
                    final String token = effectLevels.get(l).token();
                    writer.append(record("lambda", token, "", s.lambdas[l]));
                    for (final Map.Entry<String, Double> e : s.effects.get(l).entrySet()) {
                        writer.append(record("effect", token, e.getKey(), e.getValue()));
                        entries++;
                    }
                }
                for (final Map.Entry<String, Double> e : new TreeMap<>(s.leafN).entrySet()) writer.append(record("leafN", "", e.getKey(), e.getValue()));
            }
            ResourceUtil.writeBytes(path, bytes.toByteArray());
            final JsonObject manifest = FitArtifact.manifest(planHash, id);
            manifest.addProperty("estimator", "joint");
            manifest.addProperty("scale", fit.scale.name());
            final JsonArray levels = new JsonArray();
            for (final Level l : fit.levels) {
                final JsonObject o = new JsonObject();
                o.addProperty("token", l.token());
                o.addProperty("keys", String.join(",", l.keys()));
                levels.add(o);
            }
            manifest.add("levels", levels);
            manifest.addProperty("mu", s.mu);
            final JsonObject lambdas = new JsonObject();
            final JsonObject contexts = new JsonObject();
            for (int l = 0; l < effectLevels.size(); l++) {
                lambdas.addProperty(effectLevels.get(l).token(), s.lambdas[l]);
                contexts.addProperty(effectLevels.get(l).token(), s.effects.get(l).size());
            }
            manifest.add("lambdas", lambdas);
            manifest.add("contexts", contexts);
            manifest.addProperty("rows", s.rows);
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
    public static JointFit read(final String artifactUri, final String planHash, final String id, final List<Level> levels, final Shrinkage.Scale scale) {
        final String path = artifactPath(artifactUri, planHash, id);
        final Solution s = new Solution();
        final List<Level> effectLevels = new ArrayList<>();
        for (final Level l : levels) if (!l.isGlobal()) effectLevels.add(l);
        final Map<String, Integer> levelIndex = new HashMap<>();
        for (int l = 0; l < effectLevels.size(); l++) levelIndex.put(effectLevels.get(l).token(), l);
        s.lambdas = new double[effectLevels.size()];
        s.effects = new ArrayList<>();
        for (int l = 0; l < effectLevels.size(); l++) s.effects.add(new HashMap<>());
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
                    case "lambda" -> { if (levelIndex.containsKey(level)) s.lambdas[levelIndex.get(level)] = value; }
                    case "effect" -> { if (levelIndex.containsKey(level)) s.effects.get(levelIndex.get(level)).put(key, value); }
                    case "leafN" -> s.leafN.put(key, value);
                    default -> { }
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read joint fit artifact: " + path, e);
        }
        LOG.info("loaded joint fit artifact {}", path);
        return new JointFit(levels, cellKeysOf(levels), scale, s, null, null);
    }

}
