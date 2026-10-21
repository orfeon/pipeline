package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.domain.math.NormalDistribution;
import com.mercari.solution.util.pipeline.glm.StatMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
 *   <li>{@link #expected}: the conditional mean of input j's score given the observed inputs of S, in the score's own
 *       units ({@code mean_j + sd_j r_jS R_SS⁻¹ z_S}: the Gaussian copula regression of j on S) — it does not read j;</li>
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
    /**
     * The solved systems kept per cache (conditional: per input and mask; joint: per mask). A few inputs have a handful
     * of masks; many sparse inputs could have millions, each holding up to a d × d matrix, so past the bound a system
     * is solved for the row and not kept.
     */
    static final int MAX_CACHED_SYSTEMS = 4096;
    /** {@code diagnose.tail}: the upper threshold of the tail report (its mirror 1 − tail is the lower one). */
    public static final double DEFAULT_DIAGNOSE_TAIL = 0.95;
    /** A pair is flagged when its observed joint-tail share exceeds the implied one by more than this many standard errors. */
    static final double FLAG_STANDARD_ERRORS = 2;
    /** {@link #upperOrthant} integrates over [max(a, −range), range]: the normal density is negligible beyond it. */
    private static final double ORTHANT_RANGE = 9d;
    private static final double SQRT_2PI = Math.sqrt(2 * Math.PI);

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
    /** The {@code diagnose} report of a whole-input fit as JSON text ({@link #diagnose}), or null. */
    public final String diagnostics;

    private transient volatile Map<Long, Conditional> conditionals;
    private transient volatile Map<Integer, Joint> joints;

    Copula(final int dimension, final long n, final double[] mean, final double[] sd, final double[][] correlation,
           final double shrinkage, final String shrinkMethod, final int floored) {
        this(dimension, n, mean, sd, correlation, shrinkage, shrinkMethod, floored, null);
    }

    Copula(final int dimension, final long n, final double[] mean, final double[] sd, final double[][] correlation,
           final double shrinkage, final String shrinkMethod, final int floored, final String diagnostics) {
        this.dimension = dimension;
        this.n = n;
        this.mean = mean;
        this.sd = sd;
        this.correlation = correlation;
        this.shrinkage = shrinkage;
        this.shrinkMethod = shrinkMethod;
        this.floored = floored;
        this.diagnostics = diagnostics;
    }

    /** The same model with a {@code diagnose} report attached (it travels to the artifact; the readouts ignore it). */
    public Copula withDiagnostics(final String report) {
        return new Copula(dimension, n, mean, sd, correlation, shrinkage, shrinkMethod, floored, report);
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
        // the fitted model is reported by its block (CopulaSpec#fit, with the block name) through describe()
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
        return "dimension " + dimension + ", shrinkage " + shrinkMethod + " = " + shrinkage + ", n=" + n
                + (floored == 0 ? "" : ", " + floored + " eigenvalue(s) raised to the floor");
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
        final Conditional c = system(scores, j, given, zs);
        return (zj - predicted(c, given, zs)) / c.sd;
    }

    /**
     * The conditional mean of input {@code j}'s score given the observed inputs among {@code given}, in the score's own
     * units ({@code mean_j + sd_j r_jS R_SS⁻¹ z_S}: the Gaussian copula regression of j on S), or null when nothing was
     * fitted. It does not read j, so it is defined whether or not j is observed; with none of {@code given} observed it
     * is j's mean score.
     */
    public Double expected(final Double[] scores, final int j, final int[] given) {
        if (isEmpty()) return null;
        final double[] zs = new double[given.length];
        final Conditional c = system(scores, j, given, zs);
        return mean[j] + sd[j] * predicted(c, given, zs);
    }

    /**
     * The regression of j on the observed inputs among {@code given} (their standardised scores written to {@code zs},
     * NaN = missing), solved once per observed set.
     */
    private Conditional system(final Double[] scores, final int j, final int[] given, final double[] zs) {
        int mask = 0;
        for (int k = 0; k < given.length; k++) {
            zs[k] = standardised(scores, given[k]);
            if (!Double.isNaN(zs[k])) mask |= 1 << given[k];
        }
        // the observed conditioning set alone decides the system — whichever readout lists it — so β is kept per input
        final Map<Long, Conditional> cache = conditionals();
        final long key = ((long) j << 32) | (mask & 0xffffffffL);
        Conditional c = cache.get(key);
        if (c == null) {
            c = solveConditional(j, mask);
            // the masks are up to 2^31: beyond the bound a system is solved per row instead of kept
            if (cache.size() < MAX_CACHED_SYSTEMS) cache.putIfAbsent(key, c);
        }
        return c;
    }

    /** {@code r_jS R_SS⁻¹ z_S} over the observed scores (standardised units). */
    private static double predicted(final Conditional c, final int[] given, final double[] zs) {
        double predicted = 0;
        for (int k = 0; k < given.length; k++) if (!Double.isNaN(zs[k])) predicted += c.beta[given[k]] * zs[k];
        return predicted;
    }

    /** {@code Φ(conditional)}: the conditional copula {@code C_{j|S}(u_j | u_S)} at the row. */
    public Double conditionalProb(final Double[] scores, final int j, final int[] given) {
        final Double z = conditional(scores, j, given);
        return z == null ? null : NormalDistribution.cdf(z);
    }

    /** The χ²(k) CDF of {@code zᵀ R⁻¹ z} over the k observed inputs, or null with none observed / nothing fitted. */
    public Double jointRank(final Double[] scores) {
        return jointRankOf(quadratic(scores));
    }

    /** {@code log c(u)} over the observed inputs, or null with none observed / nothing fitted. */
    public Double logDensity(final Double[] scores) {
        return logDensityOf(quadratic(scores));
    }

    /** {@link #jointRank} from a row's {@link #quadratic} (null = none observed / nothing fitted). */
    static Double jointRankOf(final double[] q) {
        if (q == null) return null;
        // the lower tail directly: a row near the centre reads a small probability with its digits, not 1 − (1 − p)
        return StatMath.chiSquareCdf(q[0], (int) q[2]);
    }

    /** {@link #logDensity} from a row's {@link #quadratic} (null = none observed / nothing fitted). */
    static Double logDensityOf(final double[] q) {
        if (q == null) return null;
        return -0.5 * q[3] - 0.5 * (q[0] - q[1]);
    }

    /**
     * {zᵀ R_OO⁻¹ z, zᵀ z, |O|, log det R_OO} over the observed inputs O, or null — what {@link #jointRank} and
     * {@link #logDensity} read, so a row computes it once for both.
     */
    double[] quadratic(final Double[] scores) {
        if (isEmpty()) return null;
        final double[] z = new double[dimension];
        int mask = 0;
        for (int i = 0; i < dimension; i++) {
            z[i] = standardised(scores, i);
            if (!Double.isNaN(z[i])) mask |= 1 << i;
        }
        if (mask == 0) return null;
        final Map<Integer, Joint> cache = joints();
        Joint joint = cache.get(mask);
        if (joint == null) {
            joint = solveJoint(mask);
            if (cache.size() < MAX_CACHED_SYSTEMS) cache.putIfAbsent(mask, joint);
        }
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
    // diagnose
    // ------------------------------------------------------------------------------------------

    /**
     * The first {@code d} coordinates of vector moments — the scores of a {@code diagnose} vector, whose remaining
     * coordinates are indicators. The sums are copied, not recomputed, so the correlation is the one the plain score
     * vector gives.
     */
    static Svd.Moments head(final Svd.Moments m, final int d) {
        final Svd.Moments head = new Svd.Moments();
        head.skipped = m.skipped;
        head.mismatched = m.mismatched;
        if (m.n == 0) return head;
        final int full = m.dimension;
        head.dimension = d;
        head.n = m.n;
        head.shift = java.util.Arrays.copyOf(m.shift, d);
        head.sum = java.util.Arrays.copyOf(m.sum, d);
        head.products = new double[d * d];
        for (int i = 0; i < d; i++) System.arraycopy(m.products, i * full, head.products, i * d, d);
        return head;
    }

    /**
     * The row's {@code diagnose} vector: the scores, then 1 / 0 for a score above the upper threshold, below the lower one
     * and on a point mass ({@code mass[i]} null = no flag for that input: 0). Null when a score is missing — the fit is
     * complete-case, and the indicators must count the rows the correlation is fitted on.
     */
    static double[] diagnoseVector(final double[] scores, final Double[] mass, final double upper) {
        final int d = scores.length;
        final double[] x = new double[4 * d];
        for (int i = 0; i < d; i++) {
            x[i] = scores[i];
            x[d + i] = scores[i] > upper ? 1 : 0;
            x[2 * d + i] = scores[i] < -upper ? 1 : 0;
            x[3 * d + i] = mass[i] != null && mass[i] > 0.5 ? 1 : 0;
        }
        return x;
    }

    /**
     * P(X > a, Y > b) for standard normals with correlation rho (Simpson over x of φ(x) P(Y > b | x)). The range is
     * {@code [max(a, −9), 9]}: φ is negligible beyond ±9 (Φ(−9) ≈ 1e-19), so a threshold far below the mean (a score
     * column that is not centred) still integrates the whole upper range instead of a fixed width above {@code a}.
     */
    static double upperOrthant(final double a, final double b, final double rho) {
        if (rho > 1 - 1e-9) return NormalDistribution.cdf(-Math.max(a, b));
        if (rho < -1 + 1e-9) return Math.max(0, NormalDistribution.cdf(-a) - NormalDistribution.cdf(b));
        final double lo = Math.max(a, -ORTHANT_RANGE);
        if (lo >= ORTHANT_RANGE) return 0d;
        final double s = Math.sqrt(1 - rho * rho);
        final int steps = 4000;
        final double h = (ORTHANT_RANGE - lo) / steps;
        double sum = 0;
        for (int k = 0; k <= steps; k++) {
            final double x = lo + k * h;
            final double f = Math.exp(-0.5 * x * x) * NormalDistribution.cdf(-(b - rho * x) / s);
            sum += (k == 0 || k == steps ? 1 : k % 2 == 1 ? 4 : 2) * f;
        }
        return sum * h / 3 / SQRT_2PI;
    }

    /** What {@link #diagnose} found: the JSON report and the log lines (findings to warn about, the rest to inform). */
    public record Report(JsonObject json, List<String> warnings, List<String> lines) {}

    /**
     * The {@code diagnose} report of a fitted model over the moments of its rows' {@link #diagnoseVector}s. Per pair:
     * the share of rows beyond the threshold in one input that are beyond it in the other too (the two directions
     * pooled: co-exceedances over the mean exceedance count), above {@code tail} and below {@code 1 − tail}, next to the
     * share a Gaussian copula implies at the same exceedance rates — its thresholds placed where the observed share of
     * rows beyond them falls, so only the dependence is compared, not the marginals' shape — with the scores' sample
     * correlation (the shrunk one the readouts use is reported beside it as {@code rhoModel}) and its binomial standard
     * error. A Gaussian copula has no tail dependence, but at a finite threshold its share is far from 0 (ρ = 0.5 at
     * 0.95: about 0.24), which is what the observed share is held against. Per input with a point-mass flag: the share
     * of rows on a mass; per pair of them: how often both are,
     * against the product of the shares (what independent masses would give).
     */
    public static Report diagnose(final Svd.Moments m, final int d, final Copula model, final double tail, final int[][] pairs,
                                  final boolean[] hasMass, final List<String> names) {
        final JsonObject json = new JsonObject();
        final List<String> warnings = new ArrayList<>();
        final List<String> lines = new ArrayList<>();
        json.addProperty("tail", tail);
        json.addProperty("rows", m.n);
        if (model.isEmpty() || m.n == 0 || m.dimension != 4 * d) {
            json.addProperty("skipped", "nothing fitted");
            return new Report(json, warnings, lines);
        }
        final double n = m.n;
        final JsonArray tails = new JsonArray();
        int flagged = 0;
        for (final int[] pair : pairs) {
            final int a = pair[0], b = pair[1];
            // the sample correlation, not the shrunk one the readouts use: the question is whether the Gaussian family
            // misses joint extremes, and shrinkage alone would otherwise read as missing tail dependence
            final double rho = sampleCorrelation(m, a, b);
            final JsonObject entry = new JsonObject();
            entry.addProperty("a", names.get(a));
            entry.addProperty("b", names.get(b));
            entry.addProperty("rho", rho);
            entry.addProperty("rhoModel", model.correlation[a][b]);
            for (final boolean high : new boolean[]{true, false}) {
                final int offset = high ? d : 2 * d;
                final double na = count(m, offset + a), nb = count(m, offset + b), nab = cross(m, offset + a, offset + b);
                // the thresholds in the model's standardised units, mirrored for the lower tail (the orthant is symmetric)
                // the copula's thresholds at the observed exceedance rates (mirrored for the lower tail: the orthant is
                // symmetric), so a marginal that is not quite normal - ties at the top, a score column with its own
                // shape - is not read as dependence. No row beyond in one input: none together, 0 (not 0 / 0 - a NaN
                // would land in the artifact as a bare token strict JSON readers reject)
                final double implied;
                if (na > 0 && nb > 0) {
                    final double pa = Math.min(na / n, 1 - 1e-12), pb = Math.min(nb / n, 1 - 1e-12);
                    implied = upperOrthant(NormalDistribution.inverseNormal(1 - pa), NormalDistribution.inverseNormal(1 - pb), rho) / ((pa + pb) / 2);
                } else {
                    implied = 0d;
                }
                final double exceeding = (na + nb) / 2;
                final JsonObject side = new JsonObject();
                side.addProperty("rows", exceeding);
                side.addProperty("together", nab);
                side.addProperty("implied", implied);
                if (exceeding > 0) {
                    final double observed = nab / exceeding;
                    final double se = Math.sqrt(implied * (1 - implied) / exceeding);
                    side.addProperty("observed", observed);
                    side.addProperty("excess", observed - implied);
                    side.addProperty("se", se);
                    final String line = String.format(Locale.ROOT, "(%s, %s) %s tail: %.3f of the rows beyond the threshold in one are beyond it in the other too, the Gaussian copula implies %.3f +- %.3f (rho %.3f, %d rows beyond)",
                            names.get(a), names.get(b), high ? "upper" : "lower", observed, implied, se, rho, Math.round(exceeding));
                    if (observed - implied > FLAG_STANDARD_ERRORS * se && nab >= 5) {
                        flagged++;
                        warnings.add(line + ": joint extremes are more frequent than the model's dependence allows (no tail dependence) - a t copula, or a tail feature, would see them");
                    } else {
                        lines.add(line);
                    }
                }
                entry.add(high ? "upper" : "lower", side);
            }
            tails.add(entry);
        }
        json.add("pairs", tails);
        json.addProperty("flagged", flagged);
        // point masses: per input with a flag, per pair of them
        final JsonObject mass = new JsonObject();
        final JsonObject shares = new JsonObject();
        for (int i = 0; i < d; i++) if (hasMass[i]) shares.addProperty(names.get(i), count(m, 3 * d + i) / n);
        mass.add("shares", shares);
        final JsonArray together = new JsonArray();
        for (final int[] pair : pairs) {
            final int a = pair[0], b = pair[1];
            if (!hasMass[a] || !hasMass[b]) continue;
            final double sa = count(m, 3 * d + a) / n, sb = count(m, 3 * d + b) / n;
            if (sa == 0 || sb == 0) continue;
            final double both = cross(m, 3 * d + a, 3 * d + b) / n;
            final JsonObject entry = new JsonObject();
            entry.addProperty("a", names.get(a));
            entry.addProperty("b", names.get(b));
            entry.addProperty("both", both);
            entry.addProperty("independent", sa * sb);
            together.add(entry);
            final String line = String.format(Locale.ROOT, "(%s, %s) point masses: both on a mass in %.3f of the rows, %.3f if independent (shares %.3f / %.3f)",
                    names.get(a), names.get(b), both, sa * sb, sa, sb);
            if (both >= 0.01 && both > 2 * sa * sb) {
                warnings.add(line + ": tied values that occur together put a spike into the scores the correlation is fitted on and narrow the conditional spread around it");
            } else {
                lines.add(line);
            }
        }
        mass.add("pairs", together);
        json.add("mass", mass);
        return new Report(json, warnings, lines);
    }

    /** Σ x_k over the rows (an indicator's count), from the anchored sums. */
    private static double count(final Svd.Moments m, final int k) {
        return Math.rint(m.sum[k] + m.n * m.shift[k]);
    }

    /** The sample correlation of scores {@code a} and {@code b} (no shrinkage, no eigenvalue floor), from the anchored sums. */
    static double sampleCorrelation(final Svd.Moments m, final int a, final int b) {
        final int dim = m.dimension;
        final double n = m.n;
        final double cab = m.products[a * dim + b] - m.sum[a] * m.sum[b] / n;
        final double caa = m.products[a * dim + a] - m.sum[a] * m.sum[a] / n;
        final double cbb = m.products[b * dim + b] - m.sum[b] * m.sum[b] / n;
        return Math.max(-1d, Math.min(1d, cab / Math.sqrt(caa * cbb)));
    }

    /** Σ x_k x_l over the rows (two indicators' joint count), from the anchored sums. */
    private static double cross(final Svd.Moments m, final int k, final int l) {
        final double rk = m.sum[k] + m.n * m.shift[k], rl = m.sum[l] + m.n * m.shift[l];
        return Math.rint(m.products[k * m.dimension + l] + m.shift[k] * rl + m.shift[l] * rk - m.n * m.shift[k] * m.shift[l]);
    }

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
        if (diagnostics != null) json.add("diagnostics", JsonParser.parseString(diagnostics));
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
                json.get("shrinkage").getAsDouble(), json.get("shrinkMethod").getAsString(), json.get("floored").getAsInt(),
                json.has("diagnostics") && json.get("diagnostics").isJsonObject() ? json.get("diagnostics").toString() : null);
    }

}
