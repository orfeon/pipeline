package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.math.NormalDistribution;
import com.mercari.solution.util.pipeline.glm.StatMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fitted Gaussian copula of a few numeric inputs (docs/design/feature-dsl.md §4.4, {@code type: copula}): the
 * dependence of their normal scores {@code z = Φ⁻¹(F̂(x))} — the marginals are quantile transforms fitted before this
 * block — as one correlation matrix {@code R}, solved from the scores' moments (n, Σz, Σzzᵀ: the {@link Svd#SUMMARY}
 * family, so the block shares the svd blocks' Combine). The paper's two-step (IFM) estimate: the scores are
 * standardised with their fitted mean and standard deviation (≈ 0 and 1 for in-sample marginals), their correlation
 * is shrunk towards the identity and its eigenvalues are floored, so every readout below stays finite.
 *
 * <p>Readouts of a row (a score that is missing — null / NaN — is marginalised out: the Gaussian copula of the
 * observed inputs is the one with the sub-matrix of {@code R}):
 * <ul>
 *   <li>{@link #conditional}: the standardised position of input j given the observed inputs of S,
 *       {@code (z_j − r_jS R_SS⁻¹ z_S) / sqrt(1 − r_jS R_SS⁻¹ r_Sj)} — N(0, 1) under the model; {@code Φ} of it is the
 *       conditional copula {@code C_{j|S}(u_j | u_S)};</li>
 *   <li>{@link #jointRank}: the χ²(k) CDF of {@code zᵀ R⁻¹ z} over the k observed inputs — how atypical the vector is
 *       as a whole, on a probability scale;</li>
 *   <li>{@link #logDensity}: {@code log c(u) = −½ log det R − ½ zᵀ (R⁻¹ − I) z} — what the dependence alone makes of
 *       the combination (positive: more common than independent inputs would make it).</li>
 * </ul>
 * The solved systems depend only on which inputs a row has, so they are cached per (input, observed mask): a row
 * costs a few dot products.
 */
public final class Copula implements Serializable, FitArtifact.Model {

    private static final Logger LOG = LoggerFactory.getLogger(Copula.class);

    public static final String GAUSSIAN = "gaussian";
    /** {@code shrink: oas} — the Oracle Approximating Shrinkage intensity (Chen et al. 2010), from R and n alone. */
    public static final String OAS = "oas";
    public static final String NONE = "none";
    public static final double DEFAULT_EIGEN_FLOOR = 1e-6;
    /** The probability clamp of the marginals a copula block declares itself (a quantileTransform's default is 1e-6). */
    public static final double DEFAULT_MARGINAL_CLIP = 0.001;
    /** The observed-input masks are bits of an int. */
    public static final int MAX_INPUTS = 31;

    public final int dimension;
    public final long n;
    /** Fitted mean / standard deviation of every score (empty when nothing was fitted). */
    public final double[] mean;
    public final double[] sd;
    /** The correlation the readouts use: shrunk and eigenvalue-floored, unit diagonal (empty when nothing was fitted). */
    public final double[][] correlation;
    /** The shrinkage intensity applied (the OAS estimate, or the declared one). */
    public final double shrinkage;
    /** How {@link #shrinkage} was chosen: {@code oas} | {@code fixed} | {@code none}. */
    public final String shrinkMethod;
    /** Eigenvalues raised to the floor. */
    public final int floored;

    private transient volatile Map<Long, Conditional> conditionals;
    private transient volatile Map<Integer, Joint> joints;

    Copula(final int dimension, final long n, final double[] mean, final double[] sd, final double[][] correlation,
           final double shrinkage, final String shrinkMethod, final int floored) {
        this.dimension = dimension;
        this.n = n;
        this.mean = mean;
        this.sd = sd;
        this.correlation = correlation;
        this.shrinkage = shrinkage;
        this.shrinkMethod = shrinkMethod;
        this.floored = floored;
    }

    static Copula empty(final int dimension, final long n) {
        return new Copula(dimension, n, new double[0], new double[0], new double[0][], 0, NONE, 0);
    }

    /**
     * Solves the copula from the moments of the score vectors.
     *
     * @param shrink     {@code oas}, {@code none} or a fixed intensity in [0, 1) as text
     * @param eigenFloor the smallest eigenvalue the correlation keeps (> 0)
     * @param loud       whether a degenerate fit is reported (a forward change point is quiet)
     */
    public static Copula fit(final Svd.Moments m, final String shrink, final double eigenFloor, final boolean loud) {
        final int d = m.dimension;
        if (m.n < 2 || d == 0) {
            if (loud) LOG.warn("copula: {} score vector(s) to fit (dimension {}); nothing is fitted, every readout is null", m.n, d);
            return empty(d, m.n);
        }
        final double n = m.n;
        final double[] mean = new double[d];
        final double[] sd = new double[d];
        final double[][] cov = new double[d][d];
        for (int i = 0; i < d; i++) {
            mean[i] = m.shift[i] + m.sum[i] / n;
            for (int j = 0; j < d; j++) cov[i][j] = (m.products[i * d + j] - m.sum[i] * m.sum[j] / n) / (n - 1);
        }
        for (int i = 0; i < d; i++) {
            // a constant score (every value on one mass point) has no correlation with anything
            if (!(cov[i][i] > 1e-12)) {
                if (loud) LOG.warn("copula: input {} has no spread over {} rows (one value, or one mass point); nothing is fitted, every readout is null", i, m.n);
                return empty(d, m.n);
            }
            sd[i] = Math.sqrt(cov[i][i]);
        }
        final double[][] r = new double[d][d];
        for (int i = 0; i < d; i++) {
            for (int j = 0; j < d; j++) r[i][j] = i == j ? 1d : cov[i][j] / (sd[i] * sd[j]);
        }
        final double rho;
        final String method;
        if (OAS.equals(shrink)) {
            rho = oas(r, m.n);
            method = OAS;
        } else if (NONE.equals(shrink)) {
            rho = 0;
            method = NONE;
        } else {
            rho = Double.parseDouble(shrink);
            method = rho == 0 ? NONE : "fixed";
        }
        for (int i = 0; i < d; i++) {
            for (int j = 0; j < d; j++) if (i != j) r[i][j] *= 1 - rho;
        }
        final int floored = floor(r, eigenFloor);
        if (loud) {
            LOG.info("copula: fitted the correlation of {} scores over {} rows (shrinkage {} = {}{})", d, m.n, method, rho,
                    floored == 0 ? "" : ", " + floored + " eigenvalue(s) raised to the floor " + eigenFloor);
        }
        return new Copula(d, m.n, mean, sd, r, rho, method, floored);
    }

    /**
     * The OAS intensity of a correlation matrix (tr R = p, the target is the identity):
     * {@code min(1, ((1 − 2/p) tr(R²) + p²) / ((n + 1 − 2/p)(tr(R²) − p)))} — Chen, Wiesel, Eldar & Hero (2010) eq. 23
     * with {@code tr(Σ̂) = p}. Derived for Gaussian data, which normal scores are by construction.
     */
    static double oas(final double[][] r, final long n) {
        final int p = r.length;
        double tr2 = 0;
        for (int i = 0; i < p; i++) for (int j = 0; j < p; j++) tr2 += r[i][j] * r[i][j];
        final double denominator = (n + 1 - 2d / p) * (tr2 - p);
        if (!(denominator > 0)) return 1d;
        return Math.max(0, Math.min(1, ((1 - 2d / p) * tr2 + (double) p * p) / denominator));
    }

    /** Raises the eigenvalues of {@code r} below {@code floor} to it and restores the unit diagonal; returns how many were raised. */
    static int floor(final double[][] r, final double floor) {
        final int d = r.length;
        final SymmetricEigen.Result eigen = SymmetricEigen.leading(r, d, false);
        int raised = 0;
        for (final double v : eigen.values()) if (v < floor) raised++;
        if (raised == 0) return 0;
        final double[][] rebuilt = new double[d][d];
        for (int k = 0; k < eigen.values().length; k++) {
            final double v = Math.max(floor, eigen.values()[k]);
            final double[] u = eigen.vectors()[k];
            for (int i = 0; i < d; i++) for (int j = 0; j < d; j++) rebuilt[i][j] += v * u[i] * u[j];
        }
        for (int i = 0; i < d; i++) {
            for (int j = 0; j < d; j++) r[i][j] = i == j ? 1d : rebuilt[i][j] / Math.sqrt(rebuilt[i][i] * rebuilt[j][j]);
        }
        return raised;
    }

    @Override
    public boolean isEmpty() {
        return correlation.length == 0;
    }

    @Override
    public String describe() {
        return "dimension " + dimension + ", shrinkage " + shrinkMethod + " = " + shrinkage + ", n=" + n;
    }

    /** The standardised score of input i, or NaN when the score is missing. */
    private double standardised(final Double[] scores, final int i) {
        final Double z = scores[i];
        return z == null || z.isNaN() || z.isInfinite() ? Double.NaN : (z - mean[i]) / sd[i];
    }

    /**
     * The standardised conditional position of input {@code j} given the observed inputs among {@code given}, or null
     * when j is missing or nothing was fitted. With none of {@code given} observed it is j's own standardised score.
     */
    public Double conditional(final Double[] scores, final int j, final int[] given) {
        if (isEmpty()) return null;
        final double zj = standardised(scores, j);
        if (Double.isNaN(zj)) return null;
        final double[] zs = new double[given.length];
        int mask = 0;
        for (int k = 0; k < given.length; k++) {
            zs[k] = standardised(scores, given[k]);
            if (!Double.isNaN(zs[k])) mask |= 1 << given[k];
        }
        // the observed conditioning set alone decides the system — whichever readout lists it — so β is kept per input
        final Conditional c = conditionals().computeIfAbsent(((long) j << 32) | (mask & 0xffffffffL), key -> solveConditional(j, key.intValue()));
        double predicted = 0;
        for (int k = 0; k < given.length; k++) if ((mask & (1 << given[k])) != 0) predicted += c.beta[given[k]] * zs[k];
        return (zj - predicted) / c.sd;
    }

    /** {@code Φ(conditional)}: the conditional copula {@code C_{j|S}(u_j | u_S)} at the row. */
    public Double conditionalProb(final Double[] scores, final int j, final int[] given) {
        final Double z = conditional(scores, j, given);
        return z == null ? null : NormalDistribution.cdf(z);
    }

    /** The χ²(k) CDF of {@code zᵀ R⁻¹ z} over the k observed inputs, or null with none observed / nothing fitted. */
    public Double jointRank(final Double[] scores) {
        final double[] q = quadratic(scores);
        if (q == null) return null;
        return 1 - StatMath.chiSquareUpperTail(q[0], (int) q[2]);
    }

    /** {@code log c(u)} over the observed inputs, or null with none observed / nothing fitted. */
    public Double logDensity(final Double[] scores) {
        final double[] q = quadratic(scores);
        if (q == null) return null;
        return -0.5 * q[3] - 0.5 * (q[0] - q[1]);
    }

    /** {zᵀ R_OO⁻¹ z, zᵀ z, |O|, log det R_OO} over the observed inputs O, or null. */
    private double[] quadratic(final Double[] scores) {
        if (isEmpty()) return null;
        final double[] z = new double[dimension];
        int mask = 0;
        for (int i = 0; i < dimension; i++) {
            z[i] = standardised(scores, i);
            if (!Double.isNaN(z[i])) mask |= 1 << i;
        }
        if (mask == 0) return null;
        final Joint joint = joints().computeIfAbsent(mask, this::solveJoint);
        final int k = joint.index.length;
        double form = 0, squares = 0;
        for (int a = 0; a < k; a++) {
            final double za = z[joint.index[a]];
            squares += za * za;
            for (int b = 0; b < k; b++) form += za * joint.precision[a][b] * z[joint.index[b]];
        }
        return new double[]{Math.max(0, form), squares, k, joint.logDet};
    }

    /** β per input index (0 outside {@code mask}) and the conditional standard deviation of j given the inputs in {@code mask}. */
    private Conditional solveConditional(final int j, final int mask) {
        final int[] observed = indicesOf(mask);
        final double[] beta = new double[dimension];
        if (observed.length == 0) return new Conditional(beta, 1d);
        final int k = observed.length;
        final double[][] sub = new double[k][k];
        final double[] rhs = new double[k];
        for (int a = 0; a < k; a++) {
            rhs[a] = correlation[observed[a]][j];
            for (int b = 0; b < k; b++) sub[a][b] = correlation[observed[a]][observed[b]];
        }
        final double[][] l = cholesky(sub);
        final double[] w = solve(l, rhs);
        double explained = 0;
        for (int a = 0; a < k; a++) explained += w[a] * rhs[a];
        for (int a = 0; a < k; a++) beta[observed[a]] = w[a];
        // the eigenvalue floor keeps R positive definite, so the conditional variance is at least about the floor
        return new Conditional(beta, Math.sqrt(Math.max(1 - explained, 1e-12)));
    }

    private Joint solveJoint(final int mask) {
        final int[] index = indicesOf(mask);
        final int k = index.length;
        final double[][] sub = new double[k][k];
        for (int a = 0; a < k; a++) for (int b = 0; b < k; b++) sub[a][b] = correlation[index[a]][index[b]];
        final double[][] l = cholesky(sub);
        double logDet = 0;
        for (int a = 0; a < k; a++) logDet += 2 * Math.log(l[a][a]);
        final double[][] precision = new double[k][k];
        for (int b = 0; b < k; b++) {
            final double[] e = new double[k];
            e[b] = 1;
            final double[] column = solve(l, e);
            for (int a = 0; a < k; a++) precision[a][b] = column[a];
        }
        return new Joint(index, precision, logDet);
    }

    /** The input indices in {@code mask}, ascending. */
    private int[] indicesOf(final int mask) {
        final int[] index = new int[Integer.bitCount(mask)];
        for (int i = 0, a = 0; i < dimension; i++) if ((mask & (1 << i)) != 0) index[a++] = i;
        return index;
    }

    /** The lower Cholesky factor of a positive-definite matrix (the floored correlation and its principal sub-matrices are). */
    static double[][] cholesky(final double[][] a) {
        final int k = a.length;
        final double[][] l = new double[k][k];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j <= i; j++) {
                double s = a[i][j];
                for (int t = 0; t < j; t++) s -= l[i][t] * l[j][t];
                if (i == j) {
                    l[i][i] = Math.sqrt(Math.max(s, 1e-300));
                } else {
                    l[i][j] = s / l[j][j];
                }
            }
        }
        return l;
    }

    /** Solves {@code L Lᵀ x = b}. */
    static double[] solve(final double[][] l, final double[] b) {
        final int k = b.length;
        final double[] y = new double[k];
        for (int i = 0; i < k; i++) {
            double s = b[i];
            for (int t = 0; t < i; t++) s -= l[i][t] * y[t];
            y[i] = s / l[i][i];
        }
        final double[] x = new double[k];
        for (int i = k - 1; i >= 0; i--) {
            double s = y[i];
            for (int t = i + 1; t < k; t++) s -= l[t][i] * x[t];
            x[i] = s / l[i][i];
        }
        return x;
    }

    private Map<Long, Conditional> conditionals() {
        Map<Long, Conditional> map = conditionals;
        if (map == null) {
            synchronized (this) {
                if (conditionals == null) conditionals = new ConcurrentHashMap<>();
                map = conditionals;
            }
        }
        return map;
    }

    private Map<Integer, Joint> joints() {
        Map<Integer, Joint> map = joints;
        if (map == null) {
            synchronized (this) {
                if (joints == null) joints = new ConcurrentHashMap<>();
                map = joints;
            }
        }
        return map;
    }

    private record Conditional(double[] beta, double sd) {}

    private record Joint(int[] index, double[][] precision, double logDet) {}

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static final FitArtifact.Json<Copula> ARTIFACT = new FitArtifact.Json<>("copula", "copula", Copula::fromJson,
            "the columns", "on an input whose scores vary");

    @Override
    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("family", GAUSSIAN);
        json.addProperty("dimension", dimension);
        json.addProperty("n", n);
        json.addProperty("shrinkMethod", shrinkMethod);
        json.addProperty("shrinkage", shrinkage);
        json.addProperty("floored", floored);
        json.add("mean", array(mean));
        json.add("sd", array(sd));
        final JsonArray rows = new JsonArray();
        for (final double[] row : correlation) rows.add(array(row));
        json.add("correlation", rows);
        return json;
    }

    private static JsonArray array(final double[] values) {
        final JsonArray array = new JsonArray();
        for (final double v : values) array.add(v);
        return array;
    }

    private static double[] doubles(final JsonElement element) {
        final JsonArray array = element.getAsJsonArray();
        final double[] values = new double[array.size()];
        for (int i = 0; i < values.length; i++) values[i] = array.get(i).getAsDouble();
        return values;
    }

    public static Copula fromJson(final JsonObject json) {
        final JsonElement n = json.get("n");
        if (n == null || !n.isJsonPrimitive()) throw new IllegalStateException("copula artifact lacks 'n': " + json);
        final JsonArray rows = json.getAsJsonArray("correlation");
        final double[][] correlation = new double[rows.size()][];
        for (int i = 0; i < correlation.length; i++) correlation[i] = doubles(rows.get(i));
        return new Copula(json.get("dimension").getAsInt(), n.getAsLong(), doubles(json.get("mean")), doubles(json.get("sd")), correlation,
                json.get("shrinkage").getAsDouble(), json.get("shrinkMethod").getAsString(), json.get("floored").getAsInt());
    }

}
