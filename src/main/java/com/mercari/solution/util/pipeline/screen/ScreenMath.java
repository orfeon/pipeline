package com.mercari.solution.util.pipeline.screen;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.regex.Pattern;

/**
 * Pure numeric helpers of the screen transform: tail probabilities, quantiles, multiple-comparison
 * correction, deterministic randomness, calendar buckets and name globs. No Beam, no state.
 */
public final class ScreenMath {

    private ScreenMath() {}

    private static final double SQRT_PI = Math.sqrt(Math.PI);

    /**
     * Complementary error function, accurate to ~1e-15 relative: a Taylor series of erf below 2.5 and the
     * Abramowitz–Stegun 7.1.14 continued fraction (evaluated backwards) above.
     */
    public static double erfc(final double x) {
        if (Double.isNaN(x)) return Double.NaN;
        if (x < 0) return 2d - erfc(-x);
        if (x == 0) return 1d;
        if (x > 27) return 0d;
        if (x < 2.5) {
            // erf(x) = 2/sqrt(pi) * sum_n (-1)^n x^(2n+1) / (n! (2n+1))
            double term = x;
            double sum = x;
            final double x2 = x * x;
            for (int n = 1; n < 200; n++) {
                term *= -x2 / n;
                final double add = term / (2 * n + 1);
                sum += add;
                if (Math.abs(add) < 1e-17 * Math.abs(sum)) break;
            }
            return 1d - 2d / SQRT_PI * sum;
        }
        // sqrt(pi) e^{x^2} erfc(x) = 1/(x + (1/2)/(x + 1/(x + (3/2)/(x + 2/(x + ...)))))
        double tail = 0d;
        for (int n = 200; n >= 1; n--) {
            tail = (n / 2d) / (x + tail);
        }
        return Math.exp(-x * x) / SQRT_PI / (x + tail);
    }

    /** Upper tail probability of a chi-square(1) statistic: P(X > chi2). */
    public static double chiSquare1UpperTail(final double chi2) {
        if (Double.isNaN(chi2)) return Double.NaN;
        if (chi2 <= 0) return 1d;
        return erfc(Math.sqrt(chi2 / 2d));
    }

    /** Standard normal quantile (Acklam's rational approximation refined by one Newton step, ~1e-15). */
    public static double inverseNormal(final double p) {
        if (Double.isNaN(p) || p <= 0 || p >= 1) {
            if (p == 0) return Double.NEGATIVE_INFINITY;
            if (p == 1) return Double.POSITIVE_INFINITY;
            return Double.NaN;
        }
        final double[] a = {-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00};
        final double[] b = {-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01};
        final double[] c = {-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00};
        final double[] d = {7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00};
        final double low = 0.02425;
        double x;
        if (p < low) {
            final double q = Math.sqrt(-2 * Math.log(p));
            x = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
        } else if (p <= 1 - low) {
            final double q = p - 0.5;
            final double r = q * q;
            x = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
        } else {
            final double q = Math.sqrt(-2 * Math.log(1 - p));
            x = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
        }
        // one Halley refinement step against erfc
        final double e = 0.5 * erfc(-x / Math.sqrt(2)) - p;
        final double u = e * Math.sqrt(2 * Math.PI) * Math.exp(x * x / 2);
        x = x - u / (1 + x * u / 2);
        return x;
    }

    /** Quantile of a chi-square(1) distribution: the square of the normal quantile at (1 + q) / 2. */
    public static double chiSquare1Quantile(final double q) {
        final double z = inverseNormal((1 + q) / 2d);
        return z * z;
    }

    /** Type-7 (linear interpolation) sample quantile of a sorted array; NaN for an empty array. */
    public static double quantile(final double[] sorted, final double q) {
        if (sorted == null || sorted.length == 0) return Double.NaN;
        if (sorted.length == 1) return sorted[0];
        final double h = (sorted.length - 1) * Math.min(1d, Math.max(0d, q));
        final int lo = (int) Math.floor(h);
        final int hi = Math.min(lo + 1, sorted.length - 1);
        return sorted[lo] + (h - lo) * (sorted[hi] - sorted[lo]);
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

    /** Deterministic generator from a seed and a key (same derivation as the feature transform's noise op). */
    public static SplittableRandom seededRandom(final long seed, final String key) {
        final long h = Hashing.murmur3_128((int) (seed ^ (seed >>> 32)))
                .hashString(seed + String.valueOf((char) 0) + key, StandardCharsets.UTF_8).asLong();
        return new SplittableRandom(h);
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

    /** Epoch millis of a timestamp-like primitive: micros Long, Integer/Long epoch days for {@code date}, Instant, ISO string. */
    public static Long toEpochMillis(final Object value, final String type) {
        if (value == null) return null;
        if ("date".equals(type)) {
            if (value instanceof Number n) return n.longValue() * 86_400_000L;
            if (value instanceof String s) {
                try {
                    return LocalDate.parse(s).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
                } catch (final RuntimeException e) {
                    return null;
                }
            }
            return null;
        }
        if (value instanceof Long l) return l / 1000L;
        if (value instanceof Integer i) return i.longValue() / 1000L;
        if (value instanceof Instant i) return i.toEpochMilli();
        if (value instanceof org.joda.time.Instant i) return i.getMillis();
        if (value instanceof String s) {
            try {
                return Instant.parse(s).toEpochMilli();
            } catch (final RuntimeException e) {
                return null;
            }
        }
        return null;
    }

    /** Numeric coercion of a primitive: numbers, booleans (1 / 0) and numeric strings; null otherwise. */
    public static Double toDouble(final Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.doubleValue();
        if (value instanceof Boolean b) return b ? 1d : 0d;
        if (value instanceof String s) {
            try {
                return Double.parseDouble(s);
            } catch (final NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
