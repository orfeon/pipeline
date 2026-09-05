package com.mercari.solution.util.domain.math;

/**
 * Standard normal distribution helpers shared by the feature ({@code quantileTransform}) and screen transforms:
 * the complementary error function and the quantile (inverse CDF). Pure functions, no allocation per call.
 */
public final class NormalDistribution {

    private NormalDistribution() {}

    private static final double SQRT_PI = Math.sqrt(Math.PI);
    private static final double SQRT_2 = Math.sqrt(2);
    private static final double SQRT_2PI = Math.sqrt(2 * Math.PI);

    // Acklam's rational approximation coefficients
    private static final double[] A = {-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00};
    private static final double[] B = {-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01};
    private static final double[] C = {-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00};
    private static final double[] D = {7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00};
    private static final double LOW = 0.02425;

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

    /**
     * Standard normal quantile Φ⁻¹(p): Acklam's rational approximation refined by one Halley step against
     * {@link #erfc} (~1e-15). {@code ±∞} at 0 / 1, NaN outside (0, 1).
     */
    public static double inverseNormal(final double p) {
        if (Double.isNaN(p) || p <= 0 || p >= 1) {
            if (p == 0) return Double.NEGATIVE_INFINITY;
            if (p == 1) return Double.POSITIVE_INFINITY;
            return Double.NaN;
        }
        double x;
        if (p < LOW) {
            final double q = Math.sqrt(-2 * Math.log(p));
            x = (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5]) / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1);
        } else if (p <= 1 - LOW) {
            final double q = p - 0.5;
            final double r = q * q;
            x = (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1);
        } else {
            final double q = Math.sqrt(-2 * Math.log(1 - p));
            x = -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5]) / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1);
        }
        // one Halley refinement step against erfc
        final double e = 0.5 * erfc(-x / SQRT_2) - p;
        final double u = e * SQRT_2PI * Math.exp(x * x / 2);
        return x - u / (1 + x * u / 2);
    }

}
