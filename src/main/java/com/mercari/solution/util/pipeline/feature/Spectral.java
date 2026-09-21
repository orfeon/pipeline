package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Spectral embedding of a categorical state (docs/design/feature-dsl.md §4.4, {@code type: spectralEmbedding}): the
 * values an entity takes one after another are counted as co-occurring pairs within a window of steps, the counts
 * become a positive pointwise mutual information matrix, and its leading eigenvectors — scaled by the square root
 * of their eigenvalue's magnitude, the symmetric factorisation of the matrix — are the coordinates of each value.
 * Values that appear in the same neighbourhoods get close coordinates; nothing but the sequence order is used (no
 * target), so the embedding is as available as the values themselves.
 *
 * <p>The fit state is the pair counts ({@link PairCounts}): a sum of per-row contributions, hence a
 * {@link Summary} monoid — one {@code Combine} per time block, merged over the blocks a row may read
 * ({@link BlockSeries}) — and the small symmetric eigenproblem (one row per value) is solved on one worker with the
 * Jacobi rotation of {@link Svd}. Components are oriented so their largest loading is positive: a re-fit on the same
 * data reproduces the columns.
 *
 * <p>The {@code maxValues} vocabulary cap is applied by the engine before the pairs are counted (the
 * co-occurrence-mass pre-pass of FeatureStages.vocabularyView), because the state is quadratic in the values it
 * holds; {@link #fit} applies the same cap to whatever state it is given, which is what a direct caller (a test, a
 * re-fit from a state) relies on.
 */
public final class Spectral implements Serializable, FitArtifact.Model {

    private static final Logger LOG = LoggerFactory.getLogger(Spectral.class);

    public static final String PPMI = "ppmi";
    public static final int DEFAULT_WINDOW = 2;
    public static final int DEFAULT_RANK = 8;
    public static final int DEFAULT_MAX_VALUES = 256;
    /** The eigenproblem is dense and cubic in the vocabulary. */
    public static final int MAX_VALUES = 1024;
    /**
     * A ceiling on the values one {@link PairCounts} may hold. The engine caps the vocabulary <em>before</em> the
     * pairs are counted (FeatureStages.vocabularyView), so a state that reaches this was accumulated unfiltered —
     * a hard failure naming the cause beats an accumulator that grows to one cell per co-occurring pair of distinct
     * values and takes the worker (or the coder) down with no explanation.
     */
    public static final int MAX_STATE_VALUES = 4 * MAX_VALUES;

    /**
     * Unordered co-occurrence counts: {@code counts[a][b]} with {@code a ≤ b} (string order) is the number of times
     * the two values were seen within the window of one another, whichever came first.
     */
    public static final class PairCounts implements Serializable {
        public final TreeMap<String, TreeMap<String, Long>> counts = new TreeMap<>();
        public long pairs;
        /** The rows that contributed (one row adds a pair per previous value in its window): what {@code fit.minRows} counts. */
        public long rows;
        /**
         * Every distinct value the state holds. {@link #counts} keys only the smaller value of each pair, so its
         * size is not the vocabulary: one value that sorts before every other absorbs the whole state into a
         * single row ({@code counts.size() == 1}) however many partners it has. The ceiling is checked here.
         */
        final TreeSet<String> values = new TreeSet<>();

        public void add(final String a, final String b, final long n) {
            final boolean ordered = a.compareTo(b) <= 0;
            counts.computeIfAbsent(ordered ? a : b, k -> new TreeMap<>()).merge(ordered ? b : a, n, Long::sum);
            values.add(a);
            values.add(b);
            pairs += n;
            bound();
        }

        public void merge(final PairCounts other) {
            for (final Map.Entry<String, TreeMap<String, Long>> row : other.counts.entrySet()) {
                final TreeMap<String, Long> into = counts.computeIfAbsent(row.getKey(), k -> new TreeMap<>());
                for (final Map.Entry<String, Long> e : row.getValue().entrySet()) into.merge(e.getKey(), e.getValue(), Long::sum);
            }
            values.addAll(other.values);
            pairs += other.pairs;
            rows += other.rows;
            bound();
        }

        /** {@link Spectral#MAX_STATE_VALUES}, checked on the one number that is O(1) to read. */
        private void bound() {
            if (values.size() > MAX_STATE_VALUES) {
                throw new IllegalStateException("spectralEmbedding: the co-occurrence counts hold more than " + MAX_STATE_VALUES
                        + " distinct values. The vocabulary must be capped before the pairs are counted: bucket or discretize the field"
                        + " upstream (a spectralEmbedding state is quadratic in the values it counts, and maxValues is at most " + MAX_VALUES + ")");
            }
        }
    }

    /**
     * The pair counts as a {@link Summary} family. A contribution is a {@code String[]}: the row's value followed by
     * the values of the steps before it that fall in the window; every (value, earlier value) is one pair. A monoid
     * without inverse use (a fit never evicts).
     */
    public static final Summary<PairCounts> SUMMARY = new PairCountsSummary();

    static final class PairCountsSummary implements Summary<PairCounts> {
        @Override
        public PairCounts create() {
            return new PairCounts();
        }

        @Override
        public void update(final PairCounts state, final Object contribution, final int sign) {
            if (sign < 0) throw new UnsupportedOperationException("pair counts are not evicted");
            final String[] values = (String[]) contribution;
            for (int i = 1; i < values.length; i++) state.add(values[0], values[i], 1);
            if (values.length > 1) state.rows++;
        }

        @Override
        public boolean invertible() {
            return false;
        }

        @Override
        public void merge(final PairCounts into, final PairCounts other) {
            into.merge(other);
        }

        @Override
        public Object read(final PairCounts state, final Readout readout) {
            if ("count".equals(readout.name())) return state.pairs;
            throw new IllegalArgumentException("pair counts are fitted, not read: " + readout.name());
        }

        @Override
        public double count(final PairCounts state) {
            return state.pairs;
        }
    }

    /** The embedded values, by decreasing co-occurrence mass (ties in string order). */
    public final String[] vocabulary;
    /** {@code vocabulary.length × rank} coordinates. */
    public final double[][] embedding;
    /** The eigenvalue of each component (by decreasing magnitude; a PPMI matrix is not definite, so some are negative). */
    public final double[] eigenvalues;
    /** Pairs counted (of the vocabulary, when the extraction capped it before accumulating). */
    public final long pairs;
    /**
     * Distinct values the fit left out — they read null: beyond the {@code maxValues} cap, or kept by the cap but
     * co-occurring only with values it dropped (no co-occurrence row, hence no position rather than the origin).
     */
    public final int dropped;
    /**
     * Lazily built lookup of {@link #vocabulary}. One model instance is shared by every worker thread (the
     * artifact {@code MODEL_CACHE} and the fit side input both hand out the same object), so the field is
     * {@code volatile}: without it a thread could see a non-null reference to a map another thread has not
     * finished publishing. Racing builds are harmless — they produce the same map.
     */
    private transient volatile Map<String, Integer> index;

    /**
     * How the coordinates were brought into those of the fit before them ({@link #alignTo}): null for a fit that
     * stands alone. After {@code procrustes} a column is a mixture of components, so {@link #eigenvalues} are the
     * spectrum of the fitted components rather than one value per column.
     */
    public final String alignment;

    Spectral(final String[] vocabulary, final double[][] embedding, final double[] eigenvalues, final long pairs, final int dropped) {
        this(vocabulary, embedding, eigenvalues, pairs, dropped, null);
    }

    Spectral(final String[] vocabulary, final double[][] embedding, final double[] eigenvalues, final long pairs, final int dropped, final String alignment) {
        this.alignment = alignment;
        this.vocabulary = vocabulary;
        this.embedding = embedding;
        this.eigenvalues = eigenvalues;
        this.pairs = pairs;
        this.dropped = dropped;
    }

    public int rank() {
        return eigenvalues.length;
    }

    @Override
    public boolean isEmpty() {
        return eigenvalues.length == 0;
    }

    @Override
    public String describe() {
        return vocabulary.length + " values, rank " + rank() + ", " + pairs + " pairs";
    }

    /**
     * The row of {@link #embedding} a value sits at, or null (missing, unseen in the fit, left out by the fit,
     * nothing fitted). The row and {@link #coordinate} are what the apply path reads: no array leaves the model,
     * which one worker shares across every one of its threads.
     */
    public Integer indexOf(final String value) {
        if (value == null || isEmpty()) return null;
        Map<String, Integer> at = index;
        if (at == null) {
            at = new HashMap<>();
            for (int i = 0; i < vocabulary.length; i++) at.put(vocabulary[i], i);
            index = at;
        }
        return at.get(value);
    }

    /** Coordinate {@code k} of the value at {@code row} ({@link #indexOf}), or null beyond the fitted rank. */
    public Double coordinate(final int row, final int k) {
        return k < 0 || k >= eigenvalues.length ? null : embedding[row][k];
    }

    /** A copy of the coordinates of a value, or null as {@link #indexOf}. */
    public double[] embed(final String value) {
        final Integer i = indexOf(value);
        return i == null ? null : embedding[i].clone();
    }

    /**
     * This fit in the coordinates of {@code previous} ({@link Alignment}): the coordinates of the values both fits
     * embed are paired, and every coordinate of this fit is rotated (or sign-flipped) towards them, so the columns of
     * consecutive forward fits continue one another. Distances between values are unchanged. This fit is returned as
     * it is when there is nothing to align to (no mode, an empty fit on either side, no value in common).
     */
    public Spectral alignTo(final Spectral previous, final String mode) {
        if (previous == null || previous.isEmpty() || isEmpty()) return this;
        final int k = rank();
        final List<double[]> a = new ArrayList<>(), b = new ArrayList<>();
        for (int i = 0; i < vocabulary.length; i++) {
            final Integer at = previous.indexOf(vocabulary[i]);
            if (at == null) continue;
            a.add(embedding[i]);
            b.add(previous.embedding[at]);
        }
        final double[][] map = Alignment.map(mode, Alignment.cross(a, b, k), a);
        if (map == null) return this;
        final double[][] rotated = new double[vocabulary.length][];
        for (int i = 0; i < rotated.length; i++) rotated[i] = Alignment.apply(embedding[i], map);
        return new Spectral(vocabulary, rotated, eigenvalues, pairs, dropped, mode);
    }

    /**
     * @param warn whether an empty fit / a capped vocabulary is reported: a forward fit solves one model per change
     *             point ({@link BlockSeries#models}), and an empty window at a leave point is normal
     */
    public static Spectral fit(final PairCounts state, final int rank, final int maxValues, final boolean warn) {
        // the dense symmetric counts: a pair adds one to both (a, b) and (b, a) — two to the diagonal when a = b
        final TreeMap<String, Double> mass = new TreeMap<>();
        for (final Map.Entry<String, TreeMap<String, Long>> row : state.counts.entrySet()) {
            for (final Map.Entry<String, Long> e : row.getValue().entrySet()) {
                mass.merge(row.getKey(), (double) e.getValue(), Double::sum);
                mass.merge(e.getKey(), (double) e.getValue(), Double::sum);
            }
        }
        if (mass.size() < 2) {
            if (warn) LOG.warn("spectralEmbedding: {} distinct value(s) in {} pair(s); no embedding, every value maps to null", mass.size(), state.pairs);
            // nothing is fitted, so every value the state held is one the fit left out
            return new Spectral(new String[0], new double[0][], new double[0], state.pairs, mass.size());
        }
        final List<String> ordered = new ArrayList<>(mass.keySet());
        ordered.sort((x, y) -> {
            final int byMass = Double.compare(mass.get(y), mass.get(x));
            return byMass != 0 ? byMass : x.compareTo(y);
        });
        final int cap = Math.min(ordered.size(), maxValues);
        if (warn && ordered.size() > cap) {
            LOG.warn("spectralEmbedding: {} distinct values exceed maxValues {}; the {} with the least co-occurrence read null", ordered.size(), maxValues, ordered.size() - cap);
        }
        // the co-occurrence mass a candidate keeps within the cap: a value whose every partner was dropped has an
        // all-zero row, and its coordinates would be the origin — indistinguishable from a fitted position — so it
        // is left out and reads null like the values beyond the cap. Removing a zero row changes no other row's
        // mass, so one pass is exact (no cascade).
        final Map<String, Integer> candidates = new HashMap<>();
        for (int i = 0; i < cap; i++) candidates.put(ordered.get(i), i);
        final double[] keptMass = new double[cap];
        for (final Map.Entry<String, TreeMap<String, Long>> row : state.counts.entrySet()) {
            final Integer i = candidates.get(row.getKey());
            if (i == null) continue;
            for (final Map.Entry<String, Long> e : row.getValue().entrySet()) {
                final Integer j = candidates.get(e.getKey());
                if (j == null) continue;
                keptMass[i] += e.getValue();
                keptMass[j] += e.getValue();
            }
        }
        final List<String> kept = new ArrayList<>(cap);
        for (int i = 0; i < cap; i++) if (keptMass[i] > 0) kept.add(ordered.get(i));
        if (warn && kept.size() < cap) {
            LOG.warn("spectralEmbedding: {} value(s) within maxValues {} co-occur only with values beyond it; without a co-occurrence row they read null too",
                    cap - kept.size(), maxValues);
        }
        if (kept.size() < 2) {
            if (warn) LOG.warn("spectralEmbedding: {} co-occurring value(s) among the {} distinct in {} pair(s); no embedding, every value maps to null",
                    kept.size(), ordered.size(), state.pairs);
            return new Spectral(new String[0], new double[0][], new double[0], state.pairs, ordered.size());
        }
        final int v = kept.size();
        final int dropped = ordered.size() - v;
        final String[] vocabulary = kept.toArray(new String[0]);
        final Map<String, Integer> at = new HashMap<>();
        for (int i = 0; i < v; i++) at.put(vocabulary[i], i);
        final double[][] c = new double[v][v];
        for (final Map.Entry<String, TreeMap<String, Long>> row : state.counts.entrySet()) {
            final Integer i = at.get(row.getKey());
            if (i == null) continue;
            for (final Map.Entry<String, Long> e : row.getValue().entrySet()) {
                final Integer j = at.get(e.getKey());
                if (j == null) continue;
                c[i][j] += e.getValue();
                c[j][i] += e.getValue();
            }
        }
        // positive pointwise mutual information over the kept vocabulary: max(0, ln(C_ab · T / (r_a · r_b)))
        final double[] rowSum = new double[v];
        double total = 0;
        for (int i = 0; i < v; i++) {
            for (int j = 0; j < v; j++) rowSum[i] += c[i][j];
            total += rowSum[i];
        }
        final double[][] ppmi = new double[v][v];
        boolean informative = false;
        for (int i = 0; i < v; i++) {
            for (int j = 0; j < v; j++) {
                if (c[i][j] > 0) ppmi[i][j] = Math.max(0, Math.log(c[i][j] * total / (rowSum[i] * rowSum[j])));
                informative |= ppmi[i][j] > 0;
            }
        }
        // every co-occurrence at or below independence: the matrix is all zeros, and so would every coordinate be —
        // the origin for every value, indistinguishable from a fitted position. Nothing was learnt: read null
        if (!informative) {
            if (warn) LOG.warn("spectralEmbedding: no pair of the {} value(s) co-occurs more than chance in {} pair(s) (the PPMI matrix is all zeros);"
                    + " no embedding, every value maps to null", v, state.pairs);
            return new Spectral(new String[0], new double[0][], new double[0], state.pairs, ordered.size());
        }
        // the symmetric factorisation keeps the components of largest |eigenvalue| (the singular values of the matrix)
        final double[][] eigen = Svd.jacobi(ppmi, true);
        final int k = Math.min(rank, v);
        final double[][] embedding = new double[v][k];
        final double[] eigenvalues = new double[k];
        for (int r = 0; r < k; r++) {
            final double[] vector = eigen[r + 1];
            eigenvalues[r] = eigen[0][r];
            final double scale = Svd.sign(vector) * Math.sqrt(Math.abs(eigenvalues[r]));
            for (int i = 0; i < v; i++) embedding[i][r] = scale * vector[i];
        }
        return new Spectral(vocabulary, embedding, eigenvalues, state.pairs, dropped);
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static final FitArtifact.Json<Spectral> ARTIFACT = new FitArtifact.Json<>("spectralEmbedding", "spectral",
            Spectral::fromJson, "the columns", "on an input with at least two co-occurring values");

    @Override
    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("pairs", pairs);
        json.addProperty("rank", rank());
        json.addProperty("dropped", dropped);
        if (alignment != null) json.addProperty("alignment", alignment);
        final JsonArray values = new JsonArray();
        for (final double e : eigenvalues) values.add(e);
        json.add("eigenvalues", values);
        final JsonArray rows = new JsonArray();
        for (int i = 0; i < vocabulary.length; i++) {
            final JsonObject row = new JsonObject();
            row.addProperty("value", vocabulary[i]);
            final JsonArray coordinates = new JsonArray();
            for (final double x : embedding[i]) coordinates.add(x);
            row.add("embedding", coordinates);
            rows.add(row);
        }
        json.add("values", rows);
        return json;
    }

    public static Spectral fromJson(final JsonObject json) {
        final JsonElement pairs = json.get("pairs");
        if (pairs == null || !pairs.isJsonPrimitive()) throw new IllegalStateException("spectralEmbedding artifact lacks 'pairs': " + json);
        final JsonElement dropped = json.get("dropped");
        if (dropped == null || !dropped.isJsonPrimitive()) throw new IllegalStateException("spectralEmbedding artifact lacks 'dropped': " + json);
        final JsonArray values = array(json, "eigenvalues");
        final double[] eigenvalues = new double[values.size()];
        for (int i = 0; i < eigenvalues.length; i++) eigenvalues[i] = values.get(i).getAsDouble();
        final JsonArray rows = array(json, "values");
        final String[] vocabulary = new String[rows.size()];
        final double[][] embedding = new double[rows.size()][];
        for (int i = 0; i < vocabulary.length; i++) {
            final JsonObject row = rows.get(i).getAsJsonObject();
            vocabulary[i] = row.get("value").getAsString();
            final JsonArray coordinates = array(row, "embedding");
            // the apply path indexes a row by component ({@link #coordinate}), which only bounds itself by the
            // eigenvalue count: a row shorter than that would throw per row instead of naming the truncated artifact
            if (coordinates.size() != eigenvalues.length) {
                throw new IllegalStateException("spectralEmbedding artifact: value '" + vocabulary[i] + "' carries " + coordinates.size()
                        + " coordinate(s) but the model has rank " + eigenvalues.length + ": " + json);
            }
            embedding[i] = new double[coordinates.size()];
            for (int r = 0; r < embedding[i].length; r++) embedding[i][r] = coordinates.get(r).getAsDouble();
        }
        return new Spectral(vocabulary, embedding, eigenvalues, pairs.getAsLong(), dropped.getAsInt(),
                json.has("alignment") ? json.get("alignment").getAsString() : null);
    }

    /** A required array member: a truncated artifact must say which member is missing, not throw a NullPointerException. */
    private static JsonArray array(final JsonObject json, final String name) {
        final JsonElement element = json.get(name);
        if (element == null || !element.isJsonArray()) throw new IllegalStateException("spectralEmbedding artifact lacks the array '" + name + "': " + json);
        return element.getAsJsonArray();
    }

}
