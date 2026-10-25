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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fitted copula of a few numeric inputs (docs/design/feature-dsl.md §4.4, {@code type: copula}): the dependence of
 * their normal scores {@code z = Φ⁻¹(F̂(x))} — the marginals are quantile transforms fitted before this block — as one
 * correlation matrix {@code R}, solved from the scores' moments (n, Σz, Σzzᵀ: the {@link Svd#SUMMARY} family, so the
 * block shares the svd blocks' Combine). The paper's two-step (IFM) estimate: the scores are standardised with their
 * fitted mean and standard deviation (≈ 0 and 1 for in-sample marginals), their correlation is shrunk towards the
 * identity and its eigenvalues are floored, so every readout below stays finite.
 *
 * <p>Two families read the same {@code R}. The <b>Gaussian</b> copula takes the standardised scores as they are. The
 * <b>t</b> copula ({@code family: t}, degrees of freedom {@code df} declared) maps every standardised score to the t
 * quantile at its probability, {@code y = T_ν⁻¹(Φ(z))}, and reads the vector {@code y} as multivariate t with
 * correlation {@code R}: the same {@code R} (the normal scores' correlation is a consistent estimate of the Gaussian
 * copula's parameter and a close one of the t copula's — the fit solves moments, so {@code ν} is not estimated but
 * declared; {@code diagnose} reports the {@code ν} that would match the observed joint-tail shares). A t copula has
 * tail dependence — joint extremes beyond what the correlation alone implies — which the Gaussian has not.
 *
 * <p>Readouts of a row (a score that is missing — null / NaN — is marginalised out: the copula of the observed inputs
 * is the one with the sub-matrix of {@code R} and, for t, the same {@code ν}):
 * <ul>
 *   <li>{@link #conditional}: the standardised position of input j given the observed inputs of S,
 *       {@code (z_j − r_jS R_SS⁻¹ z_S) / sqrt(1 − r_jS R_SS⁻¹ r_Sj)} — N(0, 1) under the Gaussian model; {@code Φ} of
 *       it is the conditional copula {@code C_{j|S}(u_j | u_S)}. Under t the conditional of {@code y_j} given
 *       {@code y_S} is a t with {@code ν + |S|} degrees of freedom around the same regression, scaled by
 *       {@code sqrt((ν + y_Sᵀ R_SS⁻¹ y_S) / (ν + |S|))}; the readout is the normal score at that t position, so it is
 *       N(0, 1) under either family and reads the same way;</li>
 *   <li>{@link #expected}: the conditional median of input j's score given the observed inputs of S, in the score's
 *       own units — under the Gaussian model {@code mean_j + sd_j r_jS R_SS⁻¹ z_S}, the conditional mean too (the
 *       Gaussian copula regression of j on S); under t the regression's position mapped back to a normal score. It does
 *       not read j;</li>
 *   <li>{@link #jointRank}: the χ²(k) CDF of {@code zᵀ R⁻¹ z} over the k observed inputs — under t the F(k, ν) CDF of
 *       {@code yᵀ R⁻¹ y / k} — how atypical the vector is as a whole, on a probability scale;</li>
 *   <li>{@link #logDensity}: {@code log c(u)}, what the dependence alone makes of the combination (positive: more common
 *       than independent inputs would make it); {@code −½ log det R − ½ zᵀ (R⁻¹ − I) z} for the Gaussian, the
 *       multivariate t density over the product of its t marginals for t.</li>
 * </ul>
 * The solved systems depend only on which inputs a row has, so they are cached per (input, observed mask): a row
 * costs a few dot products (and, under t, one t quantile per input).
 */
public final class Copula implements Serializable, FitArtifact.Model {

    private static final Logger LOG = LoggerFactory.getLogger(Copula.class);

    public static final String GAUSSIAN = "gaussian";
    /** {@code family: t}: the t copula with declared degrees of freedom. */
    public static final String T = "t";
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
    /** The degrees of freedom {@code diagnose} searches for a matching t copula: below 1 the family is hardly a model, above 1e4 it is the Gaussian. */
    static final double DF_MATCH_MIN = 1d, DF_MATCH_MAX = 1e4;

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
    /** {@link #GAUSSIAN} or {@link #T}. */
    public final String family;
    /** The degrees of freedom of a t copula (NaN for the Gaussian). */
    public final double df;
    /** The {@code diagnose} report of a whole-input fit as JSON text ({@link #diagnose}), or null. */
    public final String diagnostics;

    private transient volatile Map<Long, Conditional> conditionals;
    private transient volatile Map<Integer, Joint> joints;

    Copula(final int dimension, final long n, final double[] mean, final double[] sd, final double[][] correlation,
           final double shrinkage, final String shrinkMethod, final int floored) {
        this(dimension, n, mean, sd, correlation, shrinkage, shrinkMethod, floored, GAUSSIAN, Double.NaN, null);
    }

    Copula(final int dimension, final long n, final double[] mean, final double[] sd, final double[][] correlation,
           final double shrinkage, final String shrinkMethod, final int floored, final String family, final double df, final String diagnostics) {
        this.dimension = dimension;
        this.n = n;
        this.mean = mean;
        this.sd = sd;
        this.correlation = correlation;
        this.shrinkage = shrinkage;
        this.shrinkMethod = shrinkMethod;
        this.floored = floored;
        this.family = family;
        this.df = df;
        this.diagnostics = diagnostics;
    }

    /** The same model with a {@code diagnose} report attached (it travels to the artifact; the readouts ignore it). */
    public Copula withDiagnostics(final String report) {
        return new Copula(dimension, n, mean, sd, correlation, shrinkage, shrinkMethod, floored, family, df, report);
    }

    static Copula empty(final int dimension, final long n) {
        return empty(dimension, n, GAUSSIAN, Double.NaN);
    }

    static Copula empty(final int dimension, final long n, final String family, final double df) {
        return new Copula(dimension, n, new double[0], new double[0], new double[0][], 0, NONE, 0, family, df, null);
    }

    /** {@link #fit(Svd.Moments, String, double, String, double, boolean)} of the Gaussian family. */
    public static Copula fit(final Svd.Moments m, final String shrink, final double eigenFloor, final boolean loud) {
        return fit(m, shrink, eigenFloor, GAUSSIAN, Double.NaN, loud);
    }

    /**
     * Solves the copula from the moments of the score vectors.
     *
     * @param shrink     {@code oas}, {@code none} or a fixed intensity in [0, 1) as text
     * @param eigenFloor the smallest eigenvalue the correlation keeps (> 0)
     * @param family     {@link #GAUSSIAN} or {@link #T}
     * @param df         the degrees of freedom of a t copula (> 0; ignored by the Gaussian)
     * @param loud       whether a degenerate fit is reported (a forward change point is quiet)
     */
    public static Copula fit(final Svd.Moments m, final String shrink, final double eigenFloor, final String family, final double df, final boolean loud) {
        final boolean t = T.equals(family);
        if (!t && !GAUSSIAN.equals(family)) throw new IllegalArgumentException("copula family must be " + GAUSSIAN + " or " + T + ": " + family);
        if (t && !(df > 0) || t && Double.isInfinite(df)) throw new IllegalArgumentException("a t copula needs positive degrees of freedom: " + df);
        final double nu = t ? df : Double.NaN;
        final int d = m.dimension;
        if (m.n < 2 || d == 0) {
            if (loud) LOG.warn("copula: {} score vector(s) to fit (dimension {}); nothing is fitted, every readout is null", m.n, d);
            return empty(d, m.n, family, nu);
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
                return empty(d, m.n, family, nu);
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
        return new Copula(d, m.n, mean, sd, r, rho, method, floored, family, nu, null);
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

    /** Whether the readouts are those of the t copula. */
    public boolean isT() {
        return T.equals(family);
    }

    @Override
    public String describe() {
        return (isT() ? "family t (df " + df + "), " : "") + "dimension " + dimension + ", shrinkage " + shrinkMethod + " = " + shrinkage + ", n=" + n
                + (floored == 0 ? "" : ", " + floored + " eigenvalue(s) raised to the floor");
    }

    /** The standardised score of input i, or NaN when the score is missing. */
    private double standardised(final Double[] scores, final int i) {
        final Double z = scores[i];
        return z == null || z.isNaN() || z.isInfinite() ? Double.NaN : (z - mean[i]) / sd[i];
    }

    /** The row's coordinate in the copula's latent space: the standardised score, or under t its t quantile ({@link #toT}). */
    private double latent(final Double[] scores, final int i) {
        final double z = standardised(scores, i);
        return isT() && !Double.isNaN(z) ? toT(z, df) : z;
    }

    /** The t quantile at the probability of a standard normal score: {@code T_ν⁻¹(Φ(z))}, through the tail so a far score keeps its digits. */
    static double toT(final double z, final double df) {
        if (z == 0 || Double.isNaN(z)) return z;
        final double tail = Math.max(NormalDistribution.cdf(-Math.abs(z)), Double.MIN_NORMAL);
        final double t = StatMath.studentTQuantileOfLowerTail(tail, df);
        return z < 0 ? -t : t;
    }

    /** The standard normal score at the probability of a t variate: {@code Φ⁻¹(T_ν(y))}, through the tail. */
    static double toNormal(final double y, final double df) {
        if (y == 0 || Double.isNaN(y)) return y;
        final double tail = StatMath.studentTLowerTail(Math.abs(y), df);
        final double z = -NormalDistribution.inverseNormal(Math.max(tail, Double.MIN_NORMAL));
        return y < 0 ? -z : z;
    }

    /**
     * The standardised conditional position of input {@code j} given the observed inputs among {@code given}, or null
     * when j is missing or nothing was fitted. N(0, 1) under the model (the t position is mapped to its normal score).
     * With none of {@code given} observed it is j's own standardised score.
     */
    public Double conditional(final Double[] scores, final int j, final int[] given) {
        final double[] c = conditionalPosition(scores, j, given);
        if (c == null) return null;
        return isT() ? toNormal(c[0], c[1]) : c[0];
    }

    /** {@code Φ(conditional)}: the conditional copula {@code C_{j|S}(u_j | u_S)} at the row (under t, the t CDF of the t position). */
    public Double conditionalProb(final Double[] scores, final int j, final int[] given) {
        final double[] c = conditionalPosition(scores, j, given);
        if (c == null) return null;
        return isT() ? StatMath.studentTCdf(c[0], c[1]) : NormalDistribution.cdf(c[0]);
    }

    /**
     * {position, degrees of freedom} of j given the observed inputs of {@code given}: the regression residual over its
     * conditional standard deviation — under t divided by the scale {@code sqrt((ν + y_Sᵀ R_SS⁻¹ y_S) / (ν + k))}, so it
     * is t with {@code ν + k} degrees of freedom; under the Gaussian the position is N(0, 1) (df = ∞). Null when j is
     * missing or nothing was fitted.
     */
    private double[] conditionalPosition(final Double[] scores, final int j, final int[] given) {
        if (isEmpty()) return null;
        final double yj = latent(scores, j);
        if (Double.isNaN(yj)) return null;
        final double[] ys = new double[given.length];
        final Conditional c = system(scores, j, given, ys);
        final double position = (yj - predicted(c, given, ys)) / c.sd;
        if (!isT()) return new double[]{position, Double.POSITIVE_INFINITY};
        final int k = c.observed.length;
        final double scale = Math.sqrt((df + quadraticGiven(c, given, ys)) / (df + k));
        return new double[]{position / scale, df + k};
    }

    /**
     * The conditional median of input {@code j}'s score given the observed inputs among {@code given}, in the score's
     * own units — under the Gaussian {@code mean_j + sd_j r_jS R_SS⁻¹ z_S}, the conditional mean too (the Gaussian copula
     * regression of j on S); under t the regression's t position mapped to its normal score. Null when nothing was
     * fitted. It does not read j, so it is defined whether or not j is observed. With none of {@code given} observed
     * it is null, not j's mean score: the mean carries nothing about the row and would read like a row whose inputs put
     * j exactly at the average (as {@link #jointRank} is null with nothing observed).
     */
    public Double expected(final Double[] scores, final int j, final int[] given) {
        if (isEmpty()) return null;
        final double[] ys = new double[given.length];
        final Conditional c = system(scores, j, given, ys);
        boolean observed = false;
        for (final double y : ys) observed |= !Double.isNaN(y);
        if (!observed) return null;
        final double mu = predicted(c, given, ys);
        return mean[j] + sd[j] * (isT() ? toNormal(mu, df) : mu);
    }

    /**
     * The regression of j on the observed inputs among {@code given} (their latent coordinates written to {@code ys},
     * NaN = missing), solved once per observed set.
     */
    private Conditional system(final Double[] scores, final int j, final int[] given, final double[] ys) {
        int mask = 0;
        for (int k = 0; k < given.length; k++) {
            ys[k] = latent(scores, given[k]);
            if (!Double.isNaN(ys[k])) mask |= 1 << given[k];
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

    /** {@code r_jS R_SS⁻¹ y_S} over the observed coordinates (latent units). */
    private static double predicted(final Conditional c, final int[] given, final double[] ys) {
        double predicted = 0;
        for (int k = 0; k < given.length; k++) if (!Double.isNaN(ys[k])) predicted += c.beta[given[k]] * ys[k];
        return predicted;
    }

    /** {@code y_Sᵀ R_SS⁻¹ y_S} over the observed conditioning coordinates, through the system's Cholesky factor (0 with none observed). */
    private static double quadraticGiven(final Conditional c, final int[] given, final double[] ys) {
        final int k = c.observed.length;
        if (k == 0) return 0d;
        final double[] y = new double[k];
        for (int a = 0; a < k; a++) {
            for (int p = 0; p < given.length; p++) if (given[p] == c.observed[a]) y[a] = ys[p];
        }
        // forward substitution L w = y: the form is w'w
        double q = 0;
        final double[] w = new double[k];
        for (int a = 0; a < k; a++) {
            double s = y[a];
            for (int t = 0; t < a; t++) s -= c.chol[a][t] * w[t];
            w[a] = s / c.chol[a][a];
            q += w[a] * w[a];
        }
        return q;
    }

    /** The χ²(k) CDF of {@code zᵀ R⁻¹ z} over the k observed inputs (under t the F(k, ν) CDF of {@code yᵀ R⁻¹ y / k}), or null with none observed / nothing fitted. */
    public Double jointRank(final Double[] scores) {
        return jointRankOf(quadratic(scores));
    }

    /** {@code log c(u)} over the observed inputs, or null with none observed / nothing fitted. */
    public Double logDensity(final Double[] scores) {
        return logDensityOf(quadratic(scores));
    }

    /** {@link #jointRank} from a row's {@link #quadratic} (null = none observed / nothing fitted). */
    Double jointRankOf(final double[] q) {
        if (q == null) return null;
        final int k = (int) q[2];
        // the lower tail directly: a row near the centre reads a small probability with its digits, not 1 − (1 − p)
        if (!isT()) return StatMath.chiSquareCdf(q[0], k);
        return StatMath.fCdf(q[0] / k, k, df);
    }

    /**
     * {@link #logDensity} from a row's {@link #quadratic} (null = none observed / nothing fitted). Under t:
     * {@code log t_{ν,R}(y) − Σ log t_ν(y_i)} = {@code lnΓ((ν+k)/2) + (k−1) lnΓ(ν/2) − k lnΓ((ν+1)/2) − ½ log det R
     * − (ν+k)/2 log(1 + yᵀR⁻¹y/ν) + (ν+1)/2 Σ log(1 + y_i²/ν)}.
     */
    Double logDensityOf(final double[] q) {
        if (q == null) return null;
        if (!isT()) return -0.5 * q[3] - 0.5 * (q[0] - q[1]);
        final double k = q[2];
        return StatMath.logGamma((df + k) / 2) + (k - 1) * StatMath.logGamma(df / 2) - k * StatMath.logGamma((df + 1) / 2)
                - 0.5 * q[3] - (df + k) / 2 * Math.log1p(q[0] / df) + (df + 1) / 2 * q[4];
    }

    /**
     * {yᵀ R_OO⁻¹ y, yᵀ y, |O|, log det R_OO, Σ log(1 + y_i² / ν)} over the observed inputs O (latent coordinates; the
     * last is 0 under the Gaussian), or null — what {@link #jointRank} and {@link #logDensity} read, so a row computes
     * it once for both.
     */
    double[] quadratic(final Double[] scores) {
        if (isEmpty()) return null;
        final double[] y = new double[dimension];
        int mask = 0;
        for (int i = 0; i < dimension; i++) {
            y[i] = latent(scores, i);
            if (!Double.isNaN(y[i])) mask |= 1 << i;
        }
        if (mask == 0) return null;
        final Map<Integer, Joint> cache = joints();
        Joint joint = cache.get(mask);
        if (joint == null) {
            joint = solveJoint(mask);
            if (cache.size() < MAX_CACHED_SYSTEMS) cache.putIfAbsent(mask, joint);
        }
        final int k = joint.index.length;
        final boolean t = isT();
        double form = 0, squares = 0, logs = 0;
        for (int a = 0; a < k; a++) {
            final double ya = y[joint.index[a]];
            squares += ya * ya;
            if (t) logs += Math.log1p(ya * ya / df);
            for (int b = 0; b < k; b++) form += ya * joint.precision[a][b] * y[joint.index[b]];
        }
        return new double[]{Math.max(0, form), squares, k, joint.logDet, logs};
    }

    /** β per input index (0 outside {@code mask}), the conditional standard deviation of j given the inputs in {@code mask}, and the system itself. */
    private Conditional solveConditional(final int j, final int mask) {
        final int[] observed = indicesOf(mask);
        final double[] beta = new double[dimension];
        if (observed.length == 0) return new Conditional(beta, 1d, observed, new double[0][]);
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
        return new Conditional(beta, Math.sqrt(Math.max(1 - explained, 1e-12)), observed, l);
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

    /** {@code observed} = the conditioning inputs (ascending), {@code chol} the lower Cholesky factor of their correlation (for the t scale). */
    private record Conditional(double[] beta, double sd, int[] observed, double[][] chol) {}

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
        head.shift = Arrays.copyOf(m.shift, d);
        head.sum = Arrays.copyOf(m.sum, d);
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

    /** {@link #upperOrthant(double, double, double, int)} at the resolution the report uses. */
    static double upperOrthant(final double a, final double b, final double rho) {
        return upperOrthant(a, b, rho, 4000);
    }

    /**
     * P(X > a, Y > b) for standard normals with correlation rho (Simpson over x of φ(x) P(Y > b | x), {@code steps}
     * intervals). The range is {@code [max(a, −9), 9]}: φ is negligible beyond ±9 (Φ(−9) ≈ 1e-19), so a threshold far
     * below the mean (a score column that is not centred) still integrates the whole upper range instead of a fixed
     * width above {@code a}.
     */
    static double upperOrthant(final double a, final double b, final double rho, final int steps) {
        if (rho > 1 - 1e-9) return NormalDistribution.cdf(-Math.max(a, b));
        if (rho < -1 + 1e-9) return Math.max(0, NormalDistribution.cdf(-a) - NormalDistribution.cdf(b));
        final double lo = Math.max(a, -ORTHANT_RANGE);
        if (lo >= ORTHANT_RANGE) return 0d;
        final double s = Math.sqrt(1 - rho * rho);
        final double h = (ORTHANT_RANGE - lo) / steps;
        double sum = 0;
        for (int k = 0; k <= steps; k++) {
            final double x = lo + k * h;
            final double f = Math.exp(-0.5 * x * x) * NormalDistribution.cdf(-(b - rho * x) / s);
            sum += (k == 0 || k == steps ? 1 : k % 2 == 1 ? 4 : 2) * f;
        }
        return sum * h / 3 / SQRT_2PI;
    }

    /**
     * P(T₁ > a, T₂ > b) for a bivariate t with correlation rho and {@code df} degrees of freedom: the scale mixture
     * {@code T = Z / sqrt(W / df)}, W ~ χ²(df), so the normal orthant at thresholds {@code a s, b s} averaged over
     * {@code s = sqrt(W / df)} — Simpson over {@code log W} between the 1e-7 and 1 − 1e-7 quantiles (the density in
     * {@code log W} is smooth and bell-shaped for every df, no singularity at 0), each node a coarser {@link #upperOrthant}.
     * Accurate to about 1e-6, which is what a share held against a binomial standard error needs.
     */
    static double upperOrthantT(final double a, final double b, final double rho, final double df) {
        if (Double.isInfinite(df)) return upperOrthant(a, b, rho, 400);
        final double shape = df / 2;
        final double wLo = Math.log(2 * StatMath.gammaQuantile(shape, 1e-7)), wHi = Math.log(2 * StatMath.gammaQuantile(shape, 1 - 1e-7));
        final int intervals = 200;
        final double h = (wHi - wLo) / intervals;
        final double logNorm = -shape * Math.log(2) - StatMath.logGamma(shape);
        double sum = 0;
        for (int k = 0; k <= intervals; k++) {
            final double w = wLo + k * h;
            final double big = Math.exp(w);
            // chi-square density times W (the Jacobian of log W)
            final double density = Math.exp(logNorm + shape * w - big / 2);
            final double s = Math.sqrt(big / df);
            sum += (k == 0 || k == intervals ? 1 : k % 2 == 1 ? 4 : 2) * density * upperOrthant(a * s, b * s, rho, 400);
        }
        return sum * h / 3;
    }

    /**
     * The upper tail-dependence coefficient of a t copula with correlation rho and {@code df} degrees of freedom,
     * {@code 2 T_{ν+1}(−sqrt((ν + 1)(1 − ρ) / (1 + ρ)))}: the limit share of rows beyond a quantile in one input that
     * are beyond it in the other (a Gaussian copula's is 0).
     */
    static double tailDependence(final double rho, final double df) {
        if (rho <= -1 + 1e-12) return 0d;
        if (rho >= 1 - 1e-12) return 1d;
        return 2 * StatMath.studentTCdf(-Math.sqrt((df + 1) * (1 - rho) / (1 + rho)), df + 1);
    }

    /**
     * The share of rows beyond the thresholds in one input that are beyond them in the other too — the joint
     * exceedance over the mean exceedance rate — that a copula of {@code family} (df for t) implies at the observed
     * exceedance rates {@code pa}, {@code pb} and correlation rho.
     */
    static double impliedShare(final double pa, final double pb, final double rho, final String family, final double df) {
        final double mean = (pa + pb) / 2;
        if (T.equals(family)) {
            return upperOrthantT(StatMath.studentTQuantile(1 - pa, df), StatMath.studentTQuantile(1 - pb, df), rho, df) / mean;
        }
        return upperOrthant(NormalDistribution.inverseNormal(1 - pa), NormalDistribution.inverseNormal(1 - pb), rho) / mean;
    }

    /**
     * The degrees of freedom of the t copula whose implied share ({@link #impliedShare}) equals {@code observed} at
     * these exceedance rates and correlation: NaN when the Gaussian's share is already at or above it (no finite df
     * matches), {@link #DF_MATCH_MIN} when even that df implies less; else by bisection on log df.
     */
    static double matchingDf(final double observed, final double pa, final double pb, final double rho) {
        if (!(observed > impliedShare(pa, pb, rho, GAUSSIAN, Double.NaN))) return Double.NaN;
        if (observed >= impliedShare(pa, pb, rho, T, DF_MATCH_MIN)) return DF_MATCH_MIN;
        double lo = Math.log(DF_MATCH_MIN), hi = Math.log(DF_MATCH_MAX);
        for (int i = 0; i < 20; i++) {
            final double mid = 0.5 * (lo + hi);
            // the implied share decreases with df towards the Gaussian's
            if (impliedShare(pa, pb, rho, T, Math.exp(mid)) > observed) lo = mid; else hi = mid;
        }
        return Math.exp(0.5 * (lo + hi));
    }

    /** What {@link #diagnose} found: the JSON report and the log lines (findings to warn about, the rest to inform). */
    public record Report(JsonObject json, List<String> warnings, List<String> lines) {}

    /**
     * The {@code diagnose} report of a fitted model over the moments of its rows' {@link #diagnoseVector}s. Per pair:
     * the share of rows beyond the threshold in one input that are beyond it in the other too (the two directions
     * pooled: co-exceedances over the mean exceedance count), above {@code tail} and below {@code 1 − tail}, next to the
     * share the model's family implies at the same exceedance rates — its thresholds placed where the observed share of
     * rows beyond them falls, so only the dependence is compared, not the marginals' shape — with the scores' sample
     * correlation (the shrunk one the readouts use is reported beside it as {@code rhoModel}) and its binomial standard
     * error. A Gaussian copula has no tail dependence, but at a finite threshold its share is far from 0 (ρ = 0.5 at
     * 0.95: about 0.24), which is what the observed share is held against; where the observed share exceeds the
     * Gaussian's, {@code dfMatch} is the degrees of freedom of the t copula that would match it, and {@code dfSuggested}
     * their median over the pair tails — the {@code df} to declare with {@code family: t}. A t model reports its implied
     * share at the declared df and the pair's tail-dependence coefficient. Per input with a point-mass flag: the share
     * of rows on a mass; per pair of them: how often both are, against the product of the shares (what independent
     * masses would give).
     */
    public static Report diagnose(final Svd.Moments m, final int d, final Copula model, final double tail, final int[][] pairs,
                                  final boolean[] hasMass, final List<String> names) {
        final JsonObject json = new JsonObject();
        final List<String> warnings = new ArrayList<>();
        final List<String> lines = new ArrayList<>();
        json.addProperty("tail", tail);
        json.addProperty("rows", m.n);
        json.addProperty("family", model.family);
        if (model.isT()) json.addProperty("df", model.df);
        if (model.isEmpty() || m.n == 0 || m.dimension != 4 * d) {
            json.addProperty("skipped", "nothing fitted");
            return new Report(json, warnings, lines);
        }
        final double n = m.n;
        final JsonArray tails = new JsonArray();
        final List<Double> matched = new ArrayList<>();
        final String modelName = model.isT() ? "t copula (df " + model.df + ")" : "Gaussian copula";
        int flagged = 0;
        for (final int[] pair : pairs) {
            final int a = pair[0], b = pair[1];
            // the sample correlation, not the shrunk one the readouts use: the question is whether the family
            // misses joint extremes, and shrinkage alone would otherwise read as missing tail dependence
            final double rho = sampleCorrelation(m, a, b);
            final JsonObject entry = new JsonObject();
            entry.addProperty("a", names.get(a));
            entry.addProperty("b", names.get(b));
            entry.addProperty("rho", rho);
            entry.addProperty("rhoModel", model.correlation[a][b]);
            if (model.isT()) entry.addProperty("tailDependence", tailDependence(model.correlation[a][b], model.df));
            for (final boolean high : new boolean[]{true, false}) {
                final int offset = high ? d : 2 * d;
                final double na = count(m, offset + a), nb = count(m, offset + b), nab = cross(m, offset + a, offset + b);
                // the copula's thresholds at the observed exceedance rates (mirrored for the lower tail: the orthant is
                // symmetric), so a marginal that is not quite normal - ties at the top, a score column with its own
                // shape - is not read as dependence. No row beyond in one input: none together, 0 (not 0 / 0 - a NaN
                // would land in the artifact as a bare token strict JSON readers reject)
                final double implied;
                final double pa, pb;
                if (na > 0 && nb > 0) {
                    pa = Math.min(na / n, 1 - 1e-12);
                    pb = Math.min(nb / n, 1 - 1e-12);
                    implied = impliedShare(pa, pb, rho, model.family, model.df);
                } else {
                    pa = pb = 0;
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
                    // the t copula that would match the share (NaN: the Gaussian's share is already at or above it)
                    final double dfMatch = pa > 0 && pb > 0 && nab >= 5 ? matchingDf(observed, pa, pb, rho) : Double.NaN;
                    if (!Double.isNaN(dfMatch)) {
                        side.addProperty("dfMatch", dfMatch);
                        matched.add(dfMatch);
                    }
                    final String match = Double.isNaN(dfMatch) ? "" : String.format(Locale.ROOT, " (a t copula with df %.1f matches the share)", dfMatch);
                    final String line = String.format(Locale.ROOT, "(%s, %s) %s tail: %.3f of the rows beyond the threshold in one are beyond it in the other too, the %s implies %.3f +- %.3f (rho %.3f, %d rows beyond)%s",
                            names.get(a), names.get(b), high ? "upper" : "lower", observed, modelName, implied, se, rho, Math.round(exceeding), match);
                    if (observed - implied > FLAG_STANDARD_ERRORS * se && nab >= 5) {
                        flagged++;
                        warnings.add(line + (model.isT()
                                ? ": joint extremes are more frequent than the declared df allows - a smaller df would see them"
                                : ": joint extremes are more frequent than the model's dependence allows (no tail dependence) - a t copula (family: t with the matching df), or a tail feature, would see them"));
                    } else if (model.isT() && implied - observed > FLAG_STANDARD_ERRORS * se && exceeding >= 5) {
                        lines.add(line + ": the declared df over-states the joint extremes of this pair - a larger df (or the Gaussian) is closer here");
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
        // a suggestion only once some tail is beyond what the family allows: a share a standard error above the
        // Gaussian's is matched by some large df too, and would read as a finding
        if (flagged > 0 && !matched.isEmpty()) {
            final double[] sorted = matched.stream().mapToDouble(Double::doubleValue).sorted().toArray();
            final double suggested = StatMath.quantile(sorted, 0.5);
            json.addProperty("dfSuggested", suggested);
            json.addProperty("dfMatched", sorted.length);
            lines.add(String.format(Locale.ROOT, "a t copula with df %.1f matches the observed joint-tail shares (median over %d pair tail(s) above the Gaussian's share%s)",
                    suggested, sorted.length, model.isT() ? "; declared df " + model.df : ""));
        }
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
        json.addProperty("family", family);
        if (isT()) json.addProperty("df", df);
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
        // an artifact written before the t family names no family: Gaussian
        final String family = json.has("family") && json.get("family").isJsonPrimitive() ? json.get("family").getAsString() : GAUSSIAN;
        final double df = T.equals(family) ? json.get("df").getAsDouble() : Double.NaN;
        return new Copula(json.get("dimension").getAsInt(), n.getAsLong(), doubles(json.get("mean")), doubles(json.get("sd")), correlation,
                json.get("shrinkage").getAsDouble(), json.get("shrinkMethod").getAsString(), json.get("floored").getAsInt(), family, df,
                json.has("diagnostics") && json.get("diagnostics").isJsonObject() ? json.get("diagnostics").toString() : null);
    }

}
