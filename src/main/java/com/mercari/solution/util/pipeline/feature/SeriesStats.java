package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Order-dependent scalar readouts of a value series (a sequence window in time order): sign changes, local
 * maxima, the sample autocorrelation and what the Yule–Walker equations derive from it. Pure functions over
 * {@code double[]}, Beam-free. Unlike a moment, none of them is a sum of per-event contributions — each reads
 * neighbouring values — so they have no {@link Summary} family and are evaluated by scanning the window; a readout
 * that is undefined on the series at hand (too short, no spread) reads null, never NaN.
 */
public final class SeriesStats {

    private SeriesStats() {}

    /** The largest lag / order a token may carry: beyond it a window of tens of events has no stable estimate. */
    public static final int MAX_LAG = 20;

    /**
     * A parsed readout token: {@code zeroCross}, {@code peaks}, {@code acf<j>} (lag j), {@code pacf<j>} (partial
     * autocorrelation at lag j) or {@code ar<p>_<i>} (the i-th coefficient of the Yule–Walker AR(p) fit).
     */
    public record Readout(String kind, int order, int index) implements Serializable {}

    private static final Pattern LAGGED = Pattern.compile("^(acf|pacf)(\\d{1,2})$");
    private static final Pattern AR = Pattern.compile("^ar(\\d{1,2})_(\\d{1,2})$");

    /** The readout of a token, or null when the token is not a series readout (or its lag / order is out of 1..{@link #MAX_LAG}). */
    public static Readout parse(final String token) {
        if (token == null) return null;
        if ("zeroCross".equals(token) || "peaks".equals(token)) return new Readout(token, 0, 0);
        final Matcher lagged = LAGGED.matcher(token);
        if (lagged.matches()) {
            final int lag = Integer.parseInt(lagged.group(2));
            return lag < 1 || lag > MAX_LAG ? null : new Readout(lagged.group(1), lag, 0);
        }
        final Matcher ar = AR.matcher(token);
        if (ar.matches()) {
            final int order = Integer.parseInt(ar.group(1)), index = Integer.parseInt(ar.group(2));
            return order < 1 || order > MAX_LAG || index < 1 || index > order ? null : new Readout("ar", order, index);
        }
        return null;
    }

    /** Whether the readout is a count (INT64) rather than a coefficient (FLOAT64). */
    public static boolean isCount(final Readout readout) {
        return "zeroCross".equals(readout.kind()) || "peaks".equals(readout.kind());
    }

    public static Object read(final Readout readout, final double[] x) {
        return switch (readout.kind()) {
            case "zeroCross" -> x.length == 0 ? null : (Object) zeroCross(x);
            case "peaks" -> x.length == 0 ? null : (Object) peaks(x);
            case "acf" -> acf(x, readout.order());
            case "pacf" -> {
                final double[] phi = yuleWalker(x, readout.order());
                yield phi == null ? null : phi[readout.order() - 1];
            }
            case "ar" -> {
                final double[] phi = yuleWalker(x, readout.order());
                yield phi == null ? null : phi[readout.index() - 1];
            }
            default -> throw new IllegalArgumentException("unsupported series readout: " + readout.kind());
        };
    }

    /** Sign changes between consecutive non-zero values (a zero neither has a sign nor resets the last one). */
    public static long zeroCross(final double[] x) {
        long crossings = 0;
        double last = 0;
        for (final double v : x) {
            if (v == 0) continue;
            if (last != 0 && (v > 0) != (last > 0)) crossings++;
            last = v;
        }
        return crossings;
    }

    /** Strict local maxima: values above both neighbours (the two ends have one neighbour and never count). */
    public static long peaks(final double[] x) {
        long peaks = 0;
        for (int i = 1; i + 1 < x.length; i++) {
            if (x[i] > x[i - 1] && x[i] > x[i + 1]) peaks++;
        }
        return peaks;
    }

    /**
     * The sample autocorrelation at {@code lag}: Σ_{t ≥ lag} (x_t − x̄)(x_{t−lag} − x̄) / Σ_t (x_t − x̄)² (the biased
     * estimator, which keeps the autocorrelation sequence positive semi-definite for {@link #yuleWalker}). Null when
     * the series is not longer than the lag or has no spread.
     */
    public static Double acf(final double[] x, final int lag) {
        final double[] r = autocorrelations(x, lag);
        return r == null ? null : r[lag];
    }

    /** r[0..maxLag] (r[0] = 1), or null when the series is not longer than {@code maxLag} or has no spread. */
    static double[] autocorrelations(final double[] x, final int maxLag) {
        final int n = x.length;
        if (n <= maxLag) return null;
        double mean = 0;
        for (final double v : x) mean += v;
        mean /= n;
        double c0 = 0, scale = 0;
        for (final double v : x) {
            c0 += (v - mean) * (v - mean);
            scale += v * v;
        }
        // no spread beyond the rounding of the values themselves: the ratio would be noise
        if (!(c0 > 1e-14 * scale)) return null;
        final double[] r = new double[maxLag + 1];
        r[0] = 1;
        for (int j = 1; j <= maxLag; j++) {
            double c = 0;
            for (int t = j; t < n; t++) c += (x[t] - mean) * (x[t - j] - mean);
            r[j] = c / c0;
        }
        return r;
    }

    /**
     * The AR({@code order}) coefficients φ₁..φ_p of the Yule–Walker equations R φ = r, by the Levinson–Durbin
     * recursion (the last coefficient of the order-j solution is the partial autocorrelation at lag j). Null when
     * the autocorrelations are undefined or the recursion meets a perfectly predictable series (zero innovation
     * variance).
     */
    public static double[] yuleWalker(final double[] x, final int order) {
        final double[] r = autocorrelations(x, order);
        if (r == null) return null;
        double[] phi = new double[0];
        double error = 1;
        for (int j = 1; j <= order; j++) {
            double reflection = r[j];
            for (int i = 1; i < j; i++) reflection -= phi[i - 1] * r[j - i];
            if (!(error > 1e-12)) return null;
            reflection /= error;
            final double[] next = new double[j];
            for (int i = 1; i < j; i++) next[i - 1] = phi[i - 1] - reflection * phi[j - i - 1];
            next[j - 1] = reflection;
            phi = next;
            error *= 1 - reflection * reflection;
        }
        return phi;
    }

}
