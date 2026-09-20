package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
 * ({@link BlockSeries}) — and the small symmetric eigenproblem (one row per distinct value, capped at
 * {@code maxValues} by co-occurrence mass) is solved on one worker with the Jacobi rotation of {@link Svd}.
 * Components are oriented so their largest loading is positive: a re-fit on the same data reproduces the columns.
 */
public final class Spectral implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Spectral.class);

    public static final String PPMI = "ppmi";
    public static final int DEFAULT_WINDOW = 2;
    public static final int DEFAULT_RANK = 8;
    public static final int DEFAULT_MAX_VALUES = 256;
    /** The eigenproblem is dense and cubic in the vocabulary. */
    public static final int MAX_VALUES = 1024;

    /**
     * Unordered co-occurrence counts: {@code counts[a][b]} with {@code a ≤ b} (string order) is the number of times
     * the two values were seen within the window of one another, whichever came first.
     */
    public static final class PairCounts implements Serializable {
        public final TreeMap<String, TreeMap<String, Long>> counts = new TreeMap<>();
        public long pairs;

        public void add(final String a, final String b, final long n) {
            final boolean ordered = a.compareTo(b) <= 0;
            counts.computeIfAbsent(ordered ? a : b, k -> new TreeMap<>()).merge(ordered ? b : a, n, Long::sum);
            pairs += n;
        }

        public void merge(final PairCounts other) {
            for (final Map.Entry<String, TreeMap<String, Long>> row : other.counts.entrySet()) {
                final TreeMap<String, Long> into = counts.computeIfAbsent(row.getKey(), k -> new TreeMap<>());
                for (final Map.Entry<String, Long> e : row.getValue().entrySet()) into.merge(e.getKey(), e.getValue(), Long::sum);
            }
            pairs += other.pairs;
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
    /** Pairs counted. */
    public final long pairs;
    /** Distinct values left out by the {@code maxValues} cap (they read null). */
    public final int dropped;
    /**
     * Lazily built lookup of {@link #vocabulary}. One model instance is shared by every worker thread (the
     * artifact {@code MODEL_CACHE} and the fit side input both hand out the same object), so the field is
     * {@code volatile}: without it a thread could see a non-null reference to a map another thread has not
     * finished publishing. Racing builds are harmless — they produce the same map.
     */
    private transient volatile Map<String, Integer> index;

    Spectral(final String[] vocabulary, final double[][] embedding, final double[] eigenvalues, final long pairs, final int dropped) {
        this.vocabulary = vocabulary;
        this.embedding = embedding;
        this.eigenvalues = eigenvalues;
        this.pairs = pairs;
        this.dropped = dropped;
    }

    public int rank() {
        return eigenvalues.length;
    }

    public boolean isEmpty() {
        return eigenvalues.length == 0;
    }

    /**
     * The coordinates of a value, or null (missing, unseen in the fit, beyond the vocabulary cap, nothing fitted).
     * The returned array is the model's own row — read it, never modify it: one model serves every thread of a worker.
     */
    public double[] embed(final String value) {
        if (value == null || isEmpty()) return null;
        Map<String, Integer> at = index;
        if (at == null) {
            at = new HashMap<>();
            for (int i = 0; i < vocabulary.length; i++) at.put(vocabulary[i], i);
            index = at;
        }
        final Integer i = at.get(value);
        return i == null ? null : embedding[i];
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
            return new Spectral(new String[0], new double[0][], new double[0], state.pairs, 0);
        }
        final List<String> ordered = new ArrayList<>(mass.keySet());
        ordered.sort((x, y) -> {
            final int byMass = Double.compare(mass.get(y), mass.get(x));
            return byMass != 0 ? byMass : x.compareTo(y);
        });
        final int v = Math.min(ordered.size(), maxValues);
        final int dropped = ordered.size() - v;
        if (warn && dropped > 0) {
            LOG.warn("spectralEmbedding: {} distinct values exceed maxValues {}; the {} with the least co-occurrence read null", ordered.size(), maxValues, dropped);
        }
        final String[] vocabulary = ordered.subList(0, v).toArray(new String[0]);
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
        for (int i = 0; i < v; i++) {
            for (int j = 0; j < v; j++) {
                if (c[i][j] > 0) ppmi[i][j] = Math.max(0, Math.log(c[i][j] * total / (rowSum[i] * rowSum[j])));
            }
        }
        final double[][] eigen = Svd.jacobi(ppmi);
        // the symmetric factorisation keeps the components of largest |eigenvalue| (the singular values of the matrix)
        final Integer[] order = new Integer[v];
        for (int i = 0; i < v; i++) order[i] = i;
        Arrays.sort(order, (x, y) -> {
            final int byMagnitude = Double.compare(Math.abs(eigen[0][y]), Math.abs(eigen[0][x]));
            return byMagnitude != 0 ? byMagnitude : Integer.compare(x, y);
        });
        final int k = Math.min(rank, v);
        final double[][] embedding = new double[v][k];
        final double[] eigenvalues = new double[k];
        for (int r = 0; r < k; r++) {
            final double[] vector = eigen[order[r] + 1];
            eigenvalues[r] = eigen[0][order[r]];
            // deterministic orientation: the largest-magnitude loading is positive (the first one on a tie)
            int arg = 0;
            for (int i = 1; i < v; i++) if (Math.abs(vector[i]) > Math.abs(vector[arg]) + 1e-12) arg = i;
            final double scale = (vector[arg] < 0 ? -1 : 1) * Math.sqrt(Math.abs(eigenvalues[r]));
            for (int i = 0; i < v; i++) embedding[i][r] = scale * vector[i];
        }
        return new Spectral(vocabulary, embedding, eigenvalues, state.pairs, dropped);
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static String artifactPath(final String artifactUri, final String planHash, final String block) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + block + ".spectral.json";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String block) {
        return ResourceUtil.exists(artifactPath(artifactUri, planHash, block));
    }

    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("pairs", pairs);
        json.addProperty("rank", rank());
        json.addProperty("dropped", dropped);
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
            embedding[i] = new double[coordinates.size()];
            for (int r = 0; r < embedding[i].length; r++) embedding[i][r] = coordinates.get(r).getAsDouble();
        }
        return new Spectral(vocabulary, embedding, eigenvalues, pairs.getAsLong(), dropped.getAsInt());
    }

    /** A required array member: a truncated artifact must say which member is missing, not throw a NullPointerException. */
    private static JsonArray array(final JsonObject json, final String name) {
        final JsonElement element = json.get(name);
        if (element == null || !element.isJsonArray()) throw new IllegalStateException("spectralEmbedding artifact lacks the array '" + name + "': " + json);
        return element.getAsJsonArray();
    }

    public static void write(final String artifactUri, final String planHash, final String block, final Spectral spectral) {
        final String path = artifactPath(artifactUri, planHash, block);
        final JsonObject json = FitArtifact.manifest(planHash, block);
        for (final Map.Entry<String, JsonElement> e : spectral.toJson().entrySet()) json.add(e.getKey(), e.getValue());
        ResourceUtil.writeString(path, json.toString());
        LOG.info("wrote spectralEmbedding artifact {} ({} values, rank {}, {} pairs)", path, spectral.vocabulary.length, spectral.rank(), spectral.pairs);
    }

    public static Spectral read(final String artifactUri, final String planHash, final String block) {
        final String path = artifactPath(artifactUri, planHash, block);
        final Spectral spectral = fromJson(JsonParser.parseString(ResourceUtil.readString(path)).getAsJsonObject());
        if (spectral.isEmpty()) {
            LOG.warn("loaded spectralEmbedding artifact {} without an embedding ({} pairs): the columns of block '{}' read null for every row; re-fit it on an input with at least two co-occurring values (fit.artifact.refit: true)", path, spectral.pairs, block);
        } else {
            LOG.info("loaded spectralEmbedding artifact {} ({} values, rank {}, {} pairs)", path, spectral.vocabulary.length, spectral.rank(), spectral.pairs);
        }
        return spectral;
    }

}
