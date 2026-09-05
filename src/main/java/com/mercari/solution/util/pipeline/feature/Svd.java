package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Truncated SVD / PCA of a numeric vector feature (docs/design/feature-dsl.md §4.4, {@code type: svd}; the
 * "Compress" step of §1.4): the vector is centred (and optionally standardised) with the whole-input moments and
 * projected onto the leading {@code rank} right singular vectors, giving {@code rank} decorrelated scores ordered
 * by explained variance. Fitted from sufficient statistics only — (n, Σx, Σxxᵀ) is one {@code Combine} over the
 * rows, so no row leaves the workers — and the d × d covariance is diagonalised on the driver (cyclic Jacobi;
 * d is the vector length, tens to a few hundred). The components' sign is fixed (largest-magnitude loading
 * positive) so a re-fit on the same data reproduces the same scores.
 *
 * <p>A vector with a missing component (null / NaN) or a length other than the fitted one takes no part in the
 * fit and maps to null scores. A fit with fewer than two vectors has no components and maps every vector to null
 * (the artifact is still written).
 */
public final class Svd implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Svd.class);

    /** Vector length (0 when nothing was fitted). */
    public final int dimension;
    /** Per-dimension mean subtracted before the projection (zeros when {@code center: false}). */
    public final double[] mean;
    /** Per-dimension divisor (ones unless {@code standardize: true}: the standard deviation, or the RMS when {@code center: false}). */
    public final double[] scale;
    /** {@code rank × dimension} loadings, ordered by decreasing variance. */
    public final double[][] components;
    /** Variance of each component (the eigenvalues of the fitted covariance / correlation matrix). */
    public final double[] variances;
    /** Trace of the fitted matrix: Σ variances over every dimension. */
    public final double totalVariance;
    public final long n;

    Svd(final int dimension, final double[] mean, final double[] scale, final double[][] components,
        final double[] variances, final double totalVariance, final long n) {
        this.dimension = dimension;
        this.mean = mean;
        this.scale = scale;
        this.components = components;
        this.variances = variances;
        this.totalVariance = totalVariance;
        this.n = n;
    }

    public int rank() {
        return components.length;
    }

    /**
     * Sufficient statistics of the vectors: count, per-dimension sums and the flattened d × d sum of products,
     * both taken relative to an anchor ({@code shift}, the first accepted vector) so that the covariance is not
     * formed by cancelling two huge raw moments when the values carry a large offset (epoch times, ids). Merging
     * re-expresses the other accumulator relative to this anchor (exact algebra, additions only).
     */
    public static final class Moments implements Serializable {
        public int dimension;
        public long n;
        /** Vectors that were null or had a NaN component. */
        public long skipped;
        /** Vectors of a length other than the fitted one (a mixed-length array input). */
        public long mismatched;
        public double[] shift = new double[0];
        /** Σ (x − shift). */
        public double[] sum = new double[0];
        /** Σ (x − shift)(x − shift)ᵀ, flattened row-major. */
        public double[] products = new double[0];

        public void add(final double[] x) {
            if (x == null) {
                skipped++;
                return;
            }
            for (final double v : x) {
                if (Double.isNaN(v)) {
                    skipped++;
                    return;
                }
            }
            if (n == 0) {
                dimension = x.length;
                shift = x.clone();
                sum = new double[dimension];
                products = new double[dimension * dimension];
            } else if (x.length != dimension) {
                mismatched++;
                return;
            }
            n++;
            for (int i = 0; i < dimension; i++) {
                final double di = x[i] - shift[i];
                sum[i] += di;
                for (int j = 0; j < dimension; j++) products[i * dimension + j] += di * (x[j] - shift[j]);
            }
        }

        public void merge(final Moments other) {
            skipped += other.skipped;
            mismatched += other.mismatched;
            if (other.n == 0) return;
            if (n == 0) {
                dimension = other.dimension;
                shift = other.shift.clone();
                sum = other.sum.clone();
                products = other.products.clone();
                n = other.n;
                return;
            }
            if (dimension != other.dimension) {
                mismatched += other.n; // a mixed-length input: the first length seen wins (reported by fit)
                return;
            }
            // other's sums are relative to other.shift; with δ = other.shift − shift and z = y + δ:
            // Σz = Σy + n_b δ, Σzzᵀ = Σyyᵀ + (Σy)δᵀ + δ(Σy)ᵀ + n_b δδᵀ
            final int d = dimension;
            final double[] delta = new double[d];
            for (int i = 0; i < d; i++) delta[i] = other.shift[i] - shift[i];
            for (int i = 0; i < d; i++) {
                for (int j = 0; j < d; j++) {
                    products[i * d + j] += other.products[i * d + j] + other.sum[i] * delta[j] + delta[i] * other.sum[j] + other.n * delta[i] * delta[j];
                }
            }
            for (int i = 0; i < d; i++) sum[i] += other.sum[i] + other.n * delta[i];
            n += other.n;
        }
    }

    /** Fits the leading {@code rank} components (capped at the dimension) from the moments. */
    public static Svd fit(final Moments m, final int rank, final boolean center, final boolean standardize) {
        final int d = m.dimension;
        if (m.mismatched > 0) {
            LOG.warn("svd: {} vector(s) of a length other than the fitted {} were skipped; the fitted length is whichever was seen first, so normalise the array length upstream", m.mismatched, d);
        }
        if (m.n < 2 || d == 0) {
            LOG.warn("svd: {} vector(s) to fit (dimension {}); no components, every vector maps to null", m.n, d);
            return new Svd(d, new double[d], ones(d), new double[0][], new double[0], 0, m.n);
        }
        final double n = m.n;
        final double[] mean = new double[d];
        if (center) for (int i = 0; i < d; i++) mean[i] = m.shift[i] + m.sum[i] / n;
        // covariance (centred, n − 1) or the uncentred second-moment matrix (n), both from the anchored sums
        final double[][] c = new double[d][d];
        for (int i = 0; i < d; i++) {
            for (int j = 0; j < d; j++) {
                final double p = m.products[i * d + j];
                c[i][j] = center
                        ? (p - m.sum[i] * m.sum[j] / n) / (n - 1)
                        : (p + m.sum[i] * m.shift[j] + m.shift[i] * m.sum[j] + n * m.shift[i] * m.shift[j]) / n;
            }
        }
        final double[] scale = ones(d);
        if (standardize) {
            for (int i = 0; i < d; i++) scale[i] = c[i][i] > 0 ? Math.sqrt(c[i][i]) : 1d;
            for (int i = 0; i < d; i++) for (int j = 0; j < d; j++) c[i][j] /= scale[i] * scale[j];
        }
        double trace = 0;
        for (int i = 0; i < d; i++) trace += c[i][i];
        final double[][] eigen = jacobi(c);
        final int k = Math.min(rank, d);
        if (k < rank) LOG.warn("svd: rank {} exceeds the vector dimension {}; fitting {} component(s), the remaining score columns are null", rank, d, k);
        final double[][] components = new double[k][];
        final double[] variances = new double[k];
        for (int r = 0; r < k; r++) {
            components[r] = eigen[r + 1].clone();
            variances[r] = Math.max(0, eigen[0][r]);
            // deterministic orientation: the largest-magnitude loading is positive
            int arg = 0;
            for (int i = 1; i < d; i++) if (Math.abs(components[r][i]) > Math.abs(components[r][arg])) arg = i;
            if (components[r][arg] < 0) for (int i = 0; i < d; i++) components[r][i] = -components[r][i];
        }
        return new Svd(d, mean, scale, components, variances, trace, m.n);
    }

    private static double[] ones(final int d) {
        final double[] v = new double[d];
        Arrays.fill(v, 1d);
        return v;
    }

    /**
     * Cyclic Jacobi eigendecomposition of a symmetric matrix: returns {@code [eigenvalues, v_0, v_1, ...]} with the
     * eigenvalues sorted in decreasing order and {@code v_r} the corresponding unit eigenvector (a row).
     */
    static double[][] jacobi(final double[][] input) {
        final int d = input.length;
        final double[][] a = new double[d][];
        for (int i = 0; i < d; i++) a[i] = input[i].clone();
        final double[][] v = new double[d][d];
        for (int i = 0; i < d; i++) v[i][i] = 1;
        // convergence is judged relative to the matrix scale (squared Frobenius norm), not absolutely
        double norm = 0;
        for (int i = 0; i < d; i++) for (int j = 0; j < d; j++) norm += a[i][j] * a[i][j];
        for (int sweep = 0; sweep < 100; sweep++) {
            double off = 0;
            for (int i = 0; i < d; i++) for (int j = i + 1; j < d; j++) off += a[i][j] * a[i][j];
            if (off <= 1e-30 * norm) break;
            for (int p = 0; p < d; p++) {
                for (int q = p + 1; q < d; q++) {
                    if (Math.abs(a[p][q]) < 1e-300) continue;
                    final double theta = (a[q][q] - a[p][p]) / (2 * a[p][q]);
                    // theta == 0 (equal diagonals) must still rotate by 45°: sign(0) would make the rotation a no-op
                    final double t = (theta >= 0 ? 1 : -1) / (Math.abs(theta) + Math.sqrt(theta * theta + 1));
                    final double cos = 1 / Math.sqrt(t * t + 1), sin = t * cos;
                    for (int k = 0; k < d; k++) {
                        final double akp = a[k][p], akq = a[k][q];
                        a[k][p] = cos * akp - sin * akq;
                        a[k][q] = sin * akp + cos * akq;
                    }
                    for (int k = 0; k < d; k++) {
                        final double apk = a[p][k], aqk = a[q][k];
                        a[p][k] = cos * apk - sin * aqk;
                        a[q][k] = sin * apk + cos * aqk;
                    }
                    for (int k = 0; k < d; k++) {
                        final double vkp = v[k][p], vkq = v[k][q];
                        v[k][p] = cos * vkp - sin * vkq;
                        v[k][q] = sin * vkp + cos * vkq;
                    }
                }
            }
        }
        final Integer[] order = new Integer[d];
        for (int i = 0; i < d; i++) order[i] = i;
        Arrays.sort(order, (x, y) -> Double.compare(a[y][y], a[x][x]));
        final double[][] out = new double[d + 1][];
        out[0] = new double[d];
        for (int r = 0; r < d; r++) {
            out[0][r] = a[order[r]][order[r]];
            out[r + 1] = new double[d];
            for (int k = 0; k < d; k++) out[r + 1][k] = v[k][order[r]];
        }
        return out;
    }

    /** The component scores of a vector, or null (missing component, wrong length, nothing fitted). */
    public double[] transform(final double[] x) {
        if (x == null || x.length != dimension || components.length == 0) return null;
        final double[] z = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            if (Double.isNaN(x[i])) return null;
            z[i] = (x[i] - mean[i]) / scale[i];
        }
        final double[] scores = new double[components.length];
        for (int r = 0; r < components.length; r++) {
            double s = 0;
            for (int i = 0; i < dimension; i++) s += z[i] * components[r][i];
            scores[r] = s;
        }
        return scores;
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static String artifactPath(final String artifactUri, final String planHash, final String block) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + block + ".svd.json";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String block) {
        return ResourceUtil.exists(artifactPath(artifactUri, planHash, block));
    }

    private static JsonArray array(final double[] values) {
        final JsonArray a = new JsonArray();
        for (final double v : values) a.add(v);
        return a;
    }

    private static double[] doubles(final JsonArray a) {
        final double[] v = new double[a.size()];
        for (int i = 0; i < v.length; i++) v[i] = a.get(i).getAsDouble();
        return v;
    }

    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("dimension", dimension);
        json.addProperty("rank", rank());
        json.addProperty("n", n);
        json.add("mean", array(mean));
        json.add("scale", array(scale));
        final JsonArray rows = new JsonArray();
        for (final double[] c : components) rows.add(array(c));
        json.add("components", rows);
        json.add("variances", array(variances));
        json.addProperty("totalVariance", totalVariance);
        return json;
    }

    public static Svd fromJson(final JsonObject json) {
        final JsonElement n = json.get("n");
        if (n == null || !n.isJsonPrimitive()) throw new IllegalStateException("svd artifact lacks 'n': " + json);
        final JsonArray rows = json.getAsJsonArray("components");
        final double[][] components = new double[rows.size()][];
        for (int r = 0; r < components.length; r++) components[r] = doubles(rows.get(r).getAsJsonArray());
        return new Svd(json.get("dimension").getAsInt(), doubles(json.getAsJsonArray("mean")), doubles(json.getAsJsonArray("scale")),
                components, doubles(json.getAsJsonArray("variances")), json.get("totalVariance").getAsDouble(), n.getAsLong());
    }

    public static void write(final String artifactUri, final String planHash, final String block, final Svd svd) {
        final String path = artifactPath(artifactUri, planHash, block);
        final JsonObject json = FitArtifact.manifest(planHash, block);
        for (final Map.Entry<String, JsonElement> e : svd.toJson().entrySet()) json.add(e.getKey(), e.getValue());
        ResourceUtil.writeString(path, json.toString());
        LOG.info("wrote svd artifact {} (dimension {}, rank {}, n={})", path, svd.dimension, svd.rank(), svd.n);
    }

    public static Svd read(final String artifactUri, final String planHash, final String block) {
        final String path = artifactPath(artifactUri, planHash, block);
        final Svd svd = fromJson(JsonParser.parseString(ResourceUtil.readString(path)).getAsJsonObject());
        LOG.info("loaded svd artifact {} (dimension {}, rank {}, n={})", path, svd.dimension, svd.rank(), svd.n);
        return svd;
    }

}
