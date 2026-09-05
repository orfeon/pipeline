package com.mercari.solution.util.pipeline.screen;

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
 * Pure numeric helpers of the screen transform: tail probabilities, quantiles, multiple-comparison
 * correction, calendar buckets and name globs. No Beam, no state. Randomness and value coercions are the
 * feature transform's ({@code FeatureValues}), used directly.
 */
public final class ScreenMath {

    private ScreenMath() {}

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

    static double[] finite(final double[] values) {
        int n = 0;
        for (final double v : values) if (isFinite(v)) n++;
        final double[] out = new double[n];
        int i = 0;
        for (final double v : values) if (isFinite(v)) out[i++] = v;
        return out;
    }

    static boolean isFinite(final double v) {
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
