package com.mercari.solution.util.pipeline.feature;

import java.util.List;

/**
 * Mean-reversion readouts of the spread between two series over one window — the {@code regression} funcs
 * {@code residual}, {@code residualZ}, {@code ar1}, {@code halfLife} and {@code adf}. The window's pairs
 * {@code (x_i, y_i)} in time order give the least-squares line {@code y = a + b x}; its residuals
 * {@code e_i = y_i − a − b x_i} are the spread, and the readouts say where the spread stands and how fast it
 * returns (Engle &amp; Granger's second stage over a window):
 *
 * <ul>
 *   <li>{@link #residual}: the spread at the window's latest event, {@code e_(n−1)}, in the units of {@code field};</li>
 *   <li>{@link #residualZ}: that spread over the residual standard error {@code √(Σ e² / (n − 2))} — how far from its
 *       usual size it stands;</li>
 *   <li>{@link #ar1}: the lag-1 autoregression of the spread, {@code φ = Σ e_i e_(i−1) / Σ e_(i−1)²} (no intercept: the
 *       residuals have mean 0) — 1 is a random walk (no cointegration), 0 a spread that forgets itself every event;</li>
 *   <li>{@link #halfLife}: {@code −ln 2 / ln φ}, the events the spread takes to halve, for {@code 0 < φ < 1} (null for a
 *       spread that does not revert, {@code φ ≥ 1}, or overshoots, {@code φ ≤ 0});</li>
 *   <li>{@link #adf}: the Dickey–Fuller statistic of the spread, {@code (φ − 1) / se(φ)} from {@code Δe_i = (φ − 1) e_(i−1)
 *       + u_i} (no lags, no constant: the spread is a residual). More negative = stronger reversion; the critical values
 *       are Engle–Granger's, not the normal's, so it is a strength to rank by, not a test.</li>
 * </ul>
 *
 * The residuals are consecutive among the window's complete pairs (a pair with a missing value is left out, the two
 * around it become neighbours). No {@link Summary} family: the residuals depend on the line, which every entering
 * event moves, so the window is read again at every row (the scan path, like {@link RankDependence}). Every readout
 * is null with fewer than three pairs ({@code adf} four), when {@code x} is constant (no line) or when the spread has no
 * spread; the values are finite.
 */
public final class Cointegration {

    public static final String RESIDUAL = "residual";
    public static final String RESIDUAL_Z = "residualZ";
    public static final String AR1 = "ar1";
    public static final String HALF_LIFE = "halfLife";
    public static final String ADF = "adf";
    /** The regression funcs computed here (scan-only). */
    public static final List<String> FUNCS = List.of(RESIDUAL, RESIDUAL_Z, AR1, HALF_LIFE, ADF);

    private Cointegration() {
    }

    /** The func over the first {@code n} pairs ({@code x[i]}, {@code y[i]}), in time order. */
    public static Double of(final String func, final double[] x, final double[] y, final int n) {
        return switch (func) {
            case RESIDUAL -> residual(x, y, n);
            case RESIDUAL_Z -> residualZ(x, y, n);
            case AR1 -> ar1(x, y, n);
            case HALF_LIFE -> halfLife(x, y, n);
            case ADF -> adf(x, y, n);
            default -> throw new IllegalArgumentException("not a cointegration func: " + func);
        };
    }

    public static Double residual(final double[] x, final double[] y, final int n) {
        final double[] e = residuals(x, y, n);
        return e == null ? null : e[n - 1];
    }

    public static Double residualZ(final double[] x, final double[] y, final int n) {
        final double[] e = residuals(x, y, n);
        if (e == null) return null;
        double sum = 0;
        for (int i = 0; i < n; i++) sum += e[i] * e[i];
        final double sd = Math.sqrt(sum / (n - 2));
        return sd > 0 ? e[n - 1] / sd : null;
    }

    public static Double ar1(final double[] x, final double[] y, final int n) {
        final double[] e = residuals(x, y, n);
        return e == null ? null : phi(e, n);
    }

    public static Double halfLife(final double[] x, final double[] y, final int n) {
        final Double phi = ar1(x, y, n);
        return phi == null || !(phi > 0 && phi < 1) ? null : -Math.log(2) / Math.log(phi);
    }

    public static Double adf(final double[] x, final double[] y, final int n) {
        // m = n − 1 differences: the slope's standard error needs m − 1 > 1 degrees of freedom
        if (n < 4) return null;
        final double[] e = residuals(x, y, n);
        if (e == null) return null;
        final Double phi = phi(e, n);
        if (phi == null) return null;
        final double gamma = phi - 1;
        double lagged = 0, residual = 0;
        for (int i = 1; i < n; i++) {
            final double u = e[i] - e[i - 1] - gamma * e[i - 1];
            lagged += e[i - 1] * e[i - 1];
            residual += u * u;
        }
        final double variance = residual / (n - 2);
        if (!(variance > 0) || !(lagged > 0)) return null;
        return gamma / Math.sqrt(variance / lagged);
    }

    /** {@code Σ e_i e_(i−1) / Σ e_(i−1)²} over the consecutive residuals, null when the lagged ones carry no spread. */
    private static Double phi(final double[] e, final int n) {
        double cross = 0, lagged = 0;
        for (int i = 1; i < n; i++) {
            cross += e[i] * e[i - 1];
            lagged += e[i - 1] * e[i - 1];
        }
        return lagged > 0 ? cross / lagged : null;
    }

    /** The residuals of the least-squares line of y on x (with an intercept), or null below three pairs or for a constant x. */
    private static double[] residuals(final double[] x, final double[] y, final int n) {
        if (n < 3) return null;
        double sx = 0, sy = 0;
        for (int i = 0; i < n; i++) {
            sx += x[i];
            sy += y[i];
        }
        final double mx = sx / n, my = sy / n;
        double sxx = 0, sxy = 0;
        for (int i = 0; i < n; i++) {
            sxx += (x[i] - mx) * (x[i] - mx);
            sxy += (x[i] - mx) * (y[i] - my);
        }
        if (!(sxx > 0)) return null;
        final double beta = sxy / sxx, alpha = my - beta * mx;
        final double[] e = new double[n];
        for (int i = 0; i < n; i++) e[i] = y[i] - alpha - beta * x[i];
        return e;
    }
}
