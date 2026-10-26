package com.mercari.solution.util.pipeline.glm;

import com.mercari.solution.util.domain.math.NormalDistribution;
import com.mercari.solution.util.pipeline.feature.OrderStatistics;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Pure numeric helpers of the supervised transforms (screen, evaluation): tail probabilities, quantiles, multiple-comparison
 * correction, calendar buckets and name globs. No Beam, no state. Randomness and value coercions are the
 * feature transform's ({@code FeatureValues}), used directly.
 */
public final class StatMath {

    private StatMath() {}

    /** Complementary error function (see {@link NormalDistribution#erfc}). */
    public static double erfc(final double x) {
        return NormalDistribution.erfc(x);
    }

    /** Upper tail probability of a chi-square(1) statistic: P(X > chi2). */
    public static double chiSquare1UpperTail(final double chi2) {
        if (Double.isNaN(chi2)) return Double.NaN;
        if (chi2 <= 0) return 1d;
        return erfc(Math.sqrt(chi2 / 2d));
    }

    /** Standard normal quantile (see {@link NormalDistribution#inverseNormal}). */
    public static double inverseNormal(final double p) {
        return NormalDistribution.inverseNormal(p);
    }

    /** Quantile of a chi-square(1) distribution: the square of the normal quantile at (1 + q) / 2. */
    public static double chiSquare1Quantile(final double q) {
        final double z = inverseNormal((1 + q) / 2d);
        return z * z;
    }

    /**
     * Upper tail probability of a chi-square statistic with {@code df} degrees of freedom: the regularized
     * upper incomplete gamma Q(df / 2, chi2 / 2) (series below a + 1, continued fraction above); df = 1 takes
     * the erfc form.
     */
    public static double chiSquareUpperTail(final double chi2, final int df) {
        if (df <= 1) return chiSquare1UpperTail(chi2);
        if (Double.isNaN(chi2)) return Double.NaN;
        if (chi2 <= 0) return 1d;
        return regularizedGammaQ(df / 2d, chi2 / 2d);
    }

    /**
     * ln P(X > chi2) of a chi-square(df) statistic: {@link #chiSquareUpperTail} on the log scale, finite where the
     * tail itself underflows to 0 (a df = 1 statistic past about 1,450, a z past about 38) — two extreme tails
     * still compare.
     */
    public static double logChiSquareUpperTail(final double chi2, final int df) {
        if (Double.isNaN(chi2)) return Double.NaN;
        if (chi2 <= 0) return 0d;
        if (df <= 1) return NormalDistribution.logErfc(Math.sqrt(chi2 / 2d));
        final double a = df / 2d, x = chi2 / 2d;
        if (x < a + 1) return Math.log(regularizedGammaQ(a, x));
        return -x + a * Math.log(x) - logGamma(a) + Math.log(gammaQContinuedFraction(a, x));
    }

    /** Quantile of a chi-square(df) distribution by bisection on the upper tail (df = 1 in closed form). */
    public static double chiSquareQuantile(final double q, final int df) {
        if (df <= 1) return chiSquare1Quantile(q);
        if (!(q > 0 && q < 1)) return Double.NaN;
        double lo = 0, hi = Math.max(10d, df + 10 * Math.sqrt(2d * df));
        while (chiSquareUpperTail(hi, df) > 1 - q) hi *= 2;
        for (int i = 0; i < 200 && hi - lo > 1e-12 * hi; i++) {
            final double mid = 0.5 * (lo + hi);
            if (chiSquareUpperTail(mid, df) > 1 - q) lo = mid; else hi = mid;
        }
        return 0.5 * (lo + hi);
    }

    /**
     * P(X ≤ chi2) of a chi-square(df) statistic: the regularized lower incomplete gamma P(df / 2, chi2 / 2), computed
     * directly — the series below a + 1, so a value near 0 keeps its digits rather than being {@code 1 − Q} — and
     * {@code 1 − Q} by the continued fraction above it, where P is close to 1 anyway. Every df, 1 included.
     */
    public static double chiSquareCdf(final double chi2, final int df) {
        if (Double.isNaN(chi2)) return Double.NaN;
        if (chi2 <= 0) return 0d;
        return regularizedGammaP(Math.max(1, df) / 2d, chi2 / 2d);
    }

    /** Regularized lower incomplete gamma P(a, x) = γ(a, x) / Γ(a) (Numerical Recipes gammp). */
    public static double regularizedGammaP(final double a, final double x) {
        if (x <= 0) return 0d;
        if (x < a + 1) return gammaPSeries(a, x, logGamma(a));
        return 1d - Math.exp(-x + a * Math.log(x) - logGamma(a)) * gammaQContinuedFraction(a, x);
    }

    /** The series of P(a, x) for x < a + 1. */
    private static double gammaPSeries(final double a, final double x, final double logGammaA) {
        double ap = a, sum = 1d / a, del = sum;
        for (int n = 1; n < 1000; n++) {
            ap += 1;
            del *= x / ap;
            sum += del;
            if (Math.abs(del) < Math.abs(sum) * 1e-15) break;
        }
        return sum * Math.exp(-x + a * Math.log(x) - logGammaA);
    }

    /** Regularized upper incomplete gamma Q(a, x) = Γ(a, x) / Γ(a) (Numerical Recipes gammq). */
    public static double regularizedGammaQ(final double a, final double x) {
        return regularizedGammaQ(a, x, logGamma(a));
    }

    /**
     * {@link #regularizedGammaQ(double, double)} with {@code logGammaA} = {@link #logGamma}(a) given: a caller that
     * evaluates many x at one shape (a quadrature) computes it once instead of once per call.
     */
    public static double regularizedGammaQ(final double a, final double x, final double logGammaA) {
        if (x <= 0) return 1d;
        if (x < a + 1) return 1d - gammaPSeries(a, x, logGammaA);
        return Math.exp(-x + a * Math.log(x) - logGammaA) * gammaQContinuedFraction(a, x);
    }

    /** The continued fraction of Q(a, x) for x ≥ a + 1 (modified Lentz): Q = e^{−x} x^a / Γ(a) times it. */
    private static double gammaQContinuedFraction(final double a, final double x) {
        final double tiny = 1e-300;
        double b = x + 1 - a, c = 1 / tiny, d = 1 / b, h = d;
        for (int i = 1; i < 1000; i++) {
            final double an = -i * (i - a);
            b += 2;
            d = an * d + b;
            if (Math.abs(d) < tiny) d = tiny;
            c = b + an / c;
            if (Math.abs(c) < tiny) c = tiny;
            d = 1 / d;
            final double del = d * c;
            h *= del;
            if (Math.abs(del - 1) < 1e-15) break;
        }
        return h;
    }

    /** ln Γ(x) for x > 0 (Lanczos, g = 7). */
    public static double logGamma(final double x) {
        final double[] g = {0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313,
                -176.61502916214059, 12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7};
        if (x < 0.5) return Math.log(Math.PI / Math.sin(Math.PI * x)) - logGamma(1 - x);
        final double xx = x - 1;
        double a = g[0];
        final double t = xx + 7.5;
        for (int i = 1; i < 9; i++) a += g[i] / (xx + i);
        return 0.5 * Math.log(2 * Math.PI) + (xx + 0.5) * Math.log(t) - t + Math.log(a);
    }

    /**
     * Quantile of the Gamma(a, 1) distribution — the x with P(a, x) = p — by bisection on the regularized incomplete
     * gamma (a real shape, unlike {@link #chiSquareQuantile}); NaN outside (0, 1).
     */
    public static double gammaQuantile(final double a, final double p) {
        if (!(p > 0 && p < 1) || !(a > 0)) return Double.NaN;
        // above the median the upper tail is compared, so a quantile near 1 keeps the tail's digits
        final boolean upper = p > 0.5;
        final double q = 1 - p;
        double lo = 0, hi = Math.max(1d, a + 10 * Math.sqrt(a) + 10);
        while (upper ? regularizedGammaQ(a, hi) > q : regularizedGammaP(a, hi) < p) hi *= 2;
        // a small shape puts a lower quantile hundreds of orders of magnitude below 1 (a = 0.05: 1e-140 at 1e-7), which
        // the halving from lo = 0 must reach before the relative tolerance can bite
        for (int i = 0; i < 1200 && hi - lo > 1e-14 * hi; i++) {
            final double mid = 0.5 * (lo + hi);
            if (upper ? regularizedGammaQ(a, mid) > q : regularizedGammaP(a, mid) < p) lo = mid; else hi = mid;
        }
        return 0.5 * (lo + hi);
    }

    /**
     * Regularized incomplete beta I_x(a, b) (Numerical Recipes betai: the continued fraction below the symmetry point
     * {@code (a + 1) / (a + b + 2)}, 1 minus the mirrored one above it — so a small lower tail keeps its digits).
     */
    public static double regularizedBeta(final double a, final double b, final double x) {
        if (Double.isNaN(x) || Double.isNaN(a) || Double.isNaN(b)) return Double.NaN;
        if (x <= 0) return 0d;
        if (x >= 1) return 1d;
        final double logBt = logGamma(a + b) - logGamma(a) - logGamma(b) + a * Math.log(x) + b * Math.log(1 - x);
        if (x < (a + 1) / (a + b + 2)) return Math.exp(logBt) * betaContinuedFraction(a, b, x) / a;
        return 1d - Math.exp(logBt) * betaContinuedFraction(b, a, 1 - x) / b;
    }

    /** The continued fraction of I_x(a, b) (modified Lentz; Numerical Recipes betacf). */
    private static double betaContinuedFraction(final double a, final double b, final double x) {
        final double tiny = 1e-300;
        final double qab = a + b, qap = a + 1, qam = a - 1;
        double c = 1, d = 1 - qab * x / qap;
        if (Math.abs(d) < tiny) d = tiny;
        d = 1 / d;
        double h = d;
        for (int m = 1; m <= 10000; m++) {
            final int m2 = 2 * m;
            double aa = m * (b - m) * x / ((qam + m2) * (a + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < tiny) d = tiny;
            c = 1 + aa / c;
            if (Math.abs(c) < tiny) c = tiny;
            d = 1 / d;
            h *= d * c;
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < tiny) d = tiny;
            c = 1 + aa / c;
            if (Math.abs(c) < tiny) c = tiny;
            d = 1 / d;
            final double del = d * c;
            h *= del;
            if (Math.abs(del - 1) < 1e-15) break;
        }
        return h;
    }

    /**
     * The x with I_x(a, b) = p: Numerical Recipes invbetai (an initial guess by the Abramowitz–Stegun normal
     * approximation, Halley steps kept inside (0, 1)), followed by a bisection on the bracket the steps leave when
     * they have not met the tolerance — the quantile of a t with a small lower tail lands here with p ~ 1e-15.
     */
    public static double inverseRegularizedBeta(final double a, final double b, final double p) {
        if (Double.isNaN(p) || !(a > 0) || !(b > 0)) return Double.NaN;
        if (p <= 0) return p == 0 ? 0d : Double.NaN;
        if (p >= 1) return p == 1 ? 1d : Double.NaN;
        final double a1 = a - 1, b1 = b - 1;
        double x;
        if (a >= 1 && b >= 1) {
            final double pp = p < 0.5 ? p : 1 - p;
            final double t = Math.sqrt(-2 * Math.log(pp));
            double z = (2.30753 + t * 0.27061) / (1 + t * (0.99229 + t * 0.04481)) - t;
            if (p < 0.5) z = -z;
            final double al = (z * z - 3) / 6;
            final double h = 2 / (1 / (2 * a - 1) + 1 / (2 * b - 1));
            final double w = z * Math.sqrt(al + h) / h - (1 / (2 * b - 1) - 1 / (2 * a - 1)) * (al + 5 / 6d - 2 / (3 * h));
            x = a / (a + b * Math.exp(2 * w));
        } else {
            final double lna = Math.log(a / (a + b)), lnb = Math.log(b / (a + b));
            final double t = Math.exp(a * lna) / a, u = Math.exp(b * lnb) / b, w = t + u;
            x = p < t / w ? Math.pow(a * w * p, 1 / a) : 1 - Math.pow(b * w * (1 - p), 1 / b);
        }
        final double afac = -logGamma(a) - logGamma(b) + logGamma(a + b);
        double lo = 0, hi = 1;
        for (int j = 0; j < 20; j++) {
            if (x <= 0 || x >= 1) break;
            final double err = regularizedBeta(a, b, x) - p;
            if (err < 0) lo = Math.max(lo, x); else hi = Math.min(hi, x);
            // relative to p: an absolute 1e-15 would accept the initial guess of any p below it (a far lower tail)
            if (Math.abs(err) <= 1e-15 * p) return x;
            final double density = Math.exp(a1 * Math.log(x) + b1 * Math.log(1 - x) + afac);
            if (!(density > 0) || Double.isInfinite(density)) break;
            final double u = err / density;
            final double step = u / (1 - 0.5 * Math.min(1, u * (a1 / x - b1 / (1 - x))));
            double next = x - step;
            if (next <= lo || next >= hi) next = 0.5 * (lo + hi);
            if (Math.abs(next - x) <= 1e-14 * Math.max(x, 1e-300)) return next;
            x = next;
        }
        // the Halley steps did not settle: finish on the bracket they left
        for (int i = 0; i < 200 && hi - lo > 1e-15 * Math.max(hi, 1e-300); i++) {
            final double mid = 0.5 * (lo + hi);
            if (regularizedBeta(a, b, mid) < p) lo = mid; else hi = mid;
        }
        return 0.5 * (lo + hi);
    }

    /** P(T ≤ t) of a Student t with {@code df} degrees of freedom (a real df; the normal cdf for an infinite one). */
    public static double studentTCdf(final double t, final double df) {
        if (Double.isNaN(t) || Double.isNaN(df) || !(df > 0)) return Double.NaN;
        if (Double.isInfinite(df)) return NormalDistribution.cdf(t);
        if (Double.isInfinite(t)) return t < 0 ? 0d : 1d;
        final double tail = studentTLowerTail(Math.abs(t), df);
        return t < 0 ? tail : 1 - tail;
    }

    /**
     * P(T ≤ −|t|): the lower tail of a Student t at {@code −absT}, computed directly so a far tail keeps its digits. Near
     * the centre ({@code |t| < 0.5}, a tail above 0.3 for every df) it is {@code ½ − ½ I_{t²/(df+t²)}(1/2, df/2)}: there
     * {@code df / (df + t²)} is within an ulp of 1 and would read every small |t| as the centre.
     */
    public static double studentTLowerTail(final double absT, final double df) {
        if (Double.isInfinite(df)) return NormalDistribution.cdf(-absT);
        if (Double.isInfinite(absT)) return 0d;
        final double t2 = absT * absT;
        if (absT < CENTRAL_T) return 0.5 - 0.5 * regularizedBeta(0.5, df / 2, t2 / (df + t2));
        return 0.5 * regularizedBeta(df / 2, 0.5, df / (df + t2));
    }

    /** Below this |t| the Student t functions work on {@code t² / (df + t²)} rather than {@code df / (df + t²)}. */
    private static final double CENTRAL_T = 0.5;
    /** Above this lower tail (every df: |t| below about 0.5) the t quantile is solved on {@code t² / (df + t²)}. */
    private static final double CENTRAL_TAIL = 0.3;

    /** The quantile of a Student t with {@code df} degrees of freedom; ±∞ at 0 / 1, NaN outside [0, 1]. */
    public static double studentTQuantile(final double p, final double df) {
        if (Double.isNaN(p) || Double.isNaN(df) || !(df > 0) || p < 0 || p > 1) return Double.NaN;
        if (Double.isInfinite(df)) return NormalDistribution.inverseNormal(p);
        if (p == 0.5) return 0d;
        if (p == 0) return Double.NEGATIVE_INFINITY;
        if (p == 1) return Double.POSITIVE_INFINITY;
        final double t = studentTQuantileOfLowerTail(Math.min(p, 1 - p), df);
        return p < 0.5 ? -t : t;
    }

    /**
     * The |t| whose lower tail P(T ≤ −|t|) is {@code tail} (in (0, 0.5]): I_x(df / 2, 1 / 2) = 2 tail, x = df / (df + t²).
     * Near the centre (a tail above 0.3) the mirror I_y(1 / 2, df / 2) = 1 − 2 tail on y = t² / (df + t²) instead: x would
     * be within an ulp of 1, and a small |t| would come back as noise.
     */
    public static double studentTQuantileOfLowerTail(final double tail, final double df) {
        if (!(tail > 0)) return Double.POSITIVE_INFINITY;
        if (tail >= 0.5) return 0d;
        if (Double.isInfinite(df)) return -NormalDistribution.inverseNormal(tail);
        if (tail > CENTRAL_TAIL) {
            // 2 tail is in (0.6, 1), so 1 − 2 tail is exact
            final double y = inverseRegularizedBeta(0.5, df / 2, 1 - 2 * tail);
            return y <= 0 ? 0d : Math.sqrt(df * y / (1 - y));
        }
        final double x = inverseRegularizedBeta(df / 2, 0.5, 2 * tail);
        return x >= 1 ? 0d : Math.sqrt(df * (1 - x) / x);
    }

    /** P(F ≤ f) of an F distribution with {@code d1} / {@code d2} degrees of freedom (real): I_{d1 f / (d1 f + d2)}(d1 / 2, d2 / 2). */
    public static double fCdf(final double f, final double d1, final double d2) {
        if (Double.isNaN(f) || !(d1 > 0) || !(d2 > 0)) return Double.NaN;
        if (f <= 0) return 0d;
        if (Double.isInfinite(d2)) return chiSquareCdfReal(d1 * f, d1);
        return regularizedBeta(d1 / 2, d2 / 2, d1 * f / (d1 * f + d2));
    }

    /** {@link #chiSquareCdf} for a real number of degrees of freedom. */
    public static double chiSquareCdfReal(final double chi2, final double df) {
        if (Double.isNaN(chi2) || !(df > 0)) return Double.NaN;
        if (chi2 <= 0) return 0d;
        return regularizedGammaP(df / 2, chi2 / 2);
    }

    /** Type-7 (linear interpolation) sample quantile of a sorted array; NaN for an empty array. */
    public static double quantile(final double[] sorted, final double q) {
        if (sorted == null || sorted.length == 0) return Double.NaN;
        return OrderStatistics.quantile(q, sorted, sorted.length);
    }

    /** Median of the finite entries of an array (NaN when none). */
    public static double medianFinite(final double[] values) {
        final double[] finite = finite(values);
        Arrays.sort(finite);
        return quantile(finite, 0.5);
    }

    public static double[] finite(final double[] values) {
        int n = 0;
        for (final double v : values) if (isFinite(v)) n++;
        final double[] out = new double[n];
        int i = 0;
        for (final double v : values) if (isFinite(v)) out[i++] = v;
        return out;
    }

    public static boolean isFinite(final double v) {
        return !Double.isNaN(v) && !Double.isInfinite(v);
    }

    /**
     * Benjamini–Hochberg q-values: q_(i) = min_{j >= i} p_(j) * m / j over the ascending order of p. The
     * result is aligned with the input; NaN p-values keep NaN and are excluded from m.
     */
    public static double[] benjaminiHochberg(final double[] p) {
        final int n = p.length;
        final double[] q = new double[n];
        Arrays.fill(q, Double.NaN);
        final Integer[] order = new Integer[n];
        int m = 0;
        for (int i = 0; i < n; i++) if (!Double.isNaN(p[i])) order[m++] = i;
        final Integer[] valid = Arrays.copyOf(order, m);
        Arrays.sort(valid, (x, y) -> Double.compare(p[x], p[y]));
        double running = 1d;
        for (int rank = m; rank >= 1; rank--) {
            final int idx = valid[rank - 1];
            running = Math.min(running, p[idx] * m / rank);
            q[idx] = Math.min(1d, running);
        }
        return q;
    }

    /** Calendar bucket label of an epoch-millisecond instant in UTC. */
    public static String periodBucket(final long epochMillis, final String bucket) {
        final ZonedDateTime t = Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC);
        return switch (bucket) {
            case "year" -> String.format("%04d", t.getYear());
            case "quarter" -> String.format("%04d-Q%d", t.getYear(), (t.getMonthValue() - 1) / 3 + 1);
            case "month" -> String.format("%04d-%02d", t.getYear(), t.getMonthValue());
            case "week" -> String.format("%04d-W%02d", t.get(IsoFields.WEEK_BASED_YEAR), t.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
            case "day" -> LocalDate.ofInstant(t.toInstant(), ZoneOffset.UTC).toString();
            default -> throw new IllegalArgumentException("unknown period bucket: " + bucket + " (available: " + PERIOD_BUCKETS + ")");
        };
    }

    public static final List<String> PERIOD_BUCKETS = List.of("year", "quarter", "month", "week", "day");

    /** Name glob ({@code *} = any run, {@code ?} = one character) compiled to a regex over the whole name. */
    public static Pattern glob(final String glob) {
        final StringBuilder sb = new StringBuilder("^");
        for (final char ch : glob.toCharArray()) {
            switch (ch) {
                case '*' -> sb.append(".*");
                case '?' -> sb.append('.');
                default -> sb.append(Pattern.quote(String.valueOf(ch)));
            }
        }
        return Pattern.compile(sb.append('$').toString());
    }
}
