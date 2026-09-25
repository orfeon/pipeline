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

    /** Regularized upper incomplete gamma Q(a, x) = Γ(a, x) / Γ(a) (Numerical Recipes gammq). */
    static double regularizedGammaQ(final double a, final double x) {
        if (x <= 0) return 1d;
        if (x < a + 1) {
            // series for P(a, x)
            double ap = a, sum = 1d / a, del = sum;
            for (int n = 1; n < 1000; n++) {
                ap += 1;
                del *= x / ap;
                sum += del;
                if (Math.abs(del) < Math.abs(sum) * 1e-15) break;
            }
            return 1d - sum * Math.exp(-x + a * Math.log(x) - logGamma(a));
        }
        // continued fraction for Q(a, x) (modified Lentz)
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
        return Math.exp(-x + a * Math.log(x) - logGamma(a)) * h;
    }

    /** ln Γ(x) for x > 0 (Lanczos, g = 7). */
    static double logGamma(final double x) {
        final double[] g = {0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313,
                -176.61502916214059, 12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7};
        if (x < 0.5) return Math.log(Math.PI / Math.sin(Math.PI * x)) - logGamma(1 - x);
        final double xx = x - 1;
        double a = g[0];
        final double t = xx + 7.5;
        for (int i = 1; i < 9; i++) a += g[i] / (xx + i);
        return 0.5 * Math.log(2 * Math.PI) + (xx + 0.5) * Math.log(t) - t + Math.log(a);
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
