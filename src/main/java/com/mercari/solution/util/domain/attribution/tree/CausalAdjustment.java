package com.mercari.solution.util.domain.attribution.tree;

import com.mercari.solution.util.domain.math.MatrixOps;

import java.io.Serializable;

/**
 * Causal correction of the Type 2 (product) volume/rate split along a declared sibling edge
 * {@code rate -> volume} (Zhou et al., Sec. 6).
 *
 * <p>Let {@code y = n · X̄} with {@code X̄} (rate) causing {@code n} (volume). A function
 * {@code f̂₀} from {@code X̄} to {@code n} is fitted on baseline-period granules (e.g. days). The
 * volume contribution is then the unbiased estimator of the volume mechanism's own change,
 * conditional on the new-period rate — total-change scale:
 *
 * <ul>
 *   <li>{@code simplified} (Eq. 6, Proposition 6.2): {@code (n₁ − Σⱼ f̂₀(X̄₁ⱼ)) · X̄₁} with period
 *       aggregates {@code n₁}, {@code X̄₁};</li>
 *   <li>{@code full} (Eq. 5, Theorem 6.1): {@code Σⱼ (n₁ⱼ − f̂₀(X̄₁ⱼ)) · X̄₁ⱼ};</li>
 *   <li>{@code elasticity}: no granules — {@code n₀* = n₀ + β · ΔX̄} with a user-supplied slope.</li>
 * </ul>
 *
 * The rate child receives the remainder {@code Δy − volume} (its direct plus indirect effect), so
 * additivity is exact. {@code auto} tests Proposition 6.2's slope-stability assumption (the
 * slope coefficients are equal across periods, an F-test on the pooled interaction model) and
 * uses the full estimator when it is rejected.
 */
public final class CausalAdjustment {

    private static final int MAX_IRLS_ITERATIONS = 50;
    private static final double HUBER_DELTA = 1.345;

    private CausalAdjustment() {
    }

    /** Fitted polynomial {@code f̂(x) = Σ beta[i] xⁱ}. */
    public record Fit(double[] beta, double r2, int n) implements Serializable {

        public double predict(final double x) {
            double value = 0;
            double pow = 1;
            for(final double b : beta) {
                value += b * pow;
                pow *= x;
            }
            return value;
        }
    }

    public record Diagnostics(
            double[] beta,
            double r2,
            int baselineGranules,
            int targetGranules,
            Double interactionPValue) implements Serializable {
    }

    /** Volume / rate contributions on the total-change scale, and how they were obtained. */
    public record Result(
            double volumeContribution,
            double rateContribution,
            String estimator,       // simplified | full | elasticity | fallback
            Diagnostics diagnostics,
            String warning) implements Serializable {
    }

    /**
     * @param edge the declared edge (model, robust, estimator, elasticity)
     * @param causal causal block (alpha, minGranules)
     * @param x0 baseline-period rate per granule (null or empty when no granularity)
     * @param y0 baseline-period volume per granule
     * @param x1 target-period rate per granule
     * @param y1 target-period volume per granule
     * @param n0 baseline volume (period aggregate)
     * @param n1 target volume (period aggregate)
     * @param xbar0 baseline rate (period aggregate)
     * @param xbar1 target rate (period aggregate)
     * @param deltaY parent change {@code n₁X̄₁ − n₀X̄₀}
     */
    public static Result adjust(
            final MetricTreeSpec.Edge edge,
            final MetricTreeSpec.Causal causal,
            final double[] x0, final double[] y0,
            final double[] x1, final double[] y1,
            final double n0, final double n1,
            final double xbar0, final double xbar1,
            final double deltaY) {

        if(edge.elasticity() != null) {
            final double n0Star = n0 + edge.elasticity() * (xbar1 - xbar0);
            final double volume = (n1 - n0Star) * xbar1;
            return new Result(volume, deltaY - volume, "elasticity",
                    new Diagnostics(new double[]{ n0 - edge.elasticity() * xbar0, edge.elasticity() }, Double.NaN, 0, 0, null),
                    null);
        }

        final int minGranules = causal == null ? MetricTreeSpec.Causal.DEFAULT_MIN_GRANULES : causal.minGranules();
        final int j0 = x0 == null ? 0 : x0.length;
        final int j1 = x1 == null ? 0 : x1.length;
        final int degree = MetricTreeSpec.Model.quadratic.equals(edge.model()) ? 2 : 1;
        if(j0 < Math.max(minGranules, degree + 2) || j1 < 1) {
            final double volume = (n1 - n0) * xbar1;
            return new Result(volume, deltaY - volume, "fallback",
                    new Diagnostics(new double[0], Double.NaN, j0, j1, null),
                    "causal edge " + edge.from() + "->" + edge.to() + " not applied: " + j0
                            + " baseline granules < minGranules " + minGranules + " (plain MTCD split used)");
        }

        final Fit fit = fit(x0, y0, degree, edge.robust());
        Double pValue = null;
        MetricTreeSpec.Estimator estimator = edge.estimator() == null ? MetricTreeSpec.Estimator.auto : edge.estimator();
        if(MetricTreeSpec.Estimator.auto.equals(estimator)) {
            final double alpha = causal == null ? MetricTreeSpec.Causal.DEFAULT_ALPHA : causal.slopeStabilityAlpha();
            pValue = j1 >= degree + 2 ? slopeStabilityPValue(x0, y0, x1, y1, degree) : null;
            estimator = pValue != null && pValue < alpha
                    ? MetricTreeSpec.Estimator.full : MetricTreeSpec.Estimator.simplified;
        }

        final double volume;
        if(MetricTreeSpec.Estimator.full.equals(estimator)) {
            double sum = 0;
            for(int j = 0; j < j1; j++) {
                sum += (y1[j] - fit.predict(x1[j])) * x1[j];
            }
            volume = sum;
        } else {
            double predicted = 0;
            for(int j = 0; j < j1; j++) {
                predicted += fit.predict(x1[j]);
            }
            volume = (n1 - predicted) * xbar1;
        }
        return new Result(volume, deltaY - volume, estimator.name(),
                new Diagnostics(fit.beta, fit.r2, j0, j1, pValue), null);
    }

    /** Polynomial least squares; Huber IRLS when {@code robust}. */
    public static Fit fit(final double[] x, final double[] y, final int degree, final boolean robust) {
        final int n = x.length;
        final double[] w = new double[n];
        java.util.Arrays.fill(w, 1.0);
        double[] beta = weightedFit(x, y, w, degree);
        if(robust) {
            for(int iteration = 0; iteration < MAX_IRLS_ITERATIONS; iteration++) {
                final double[] residuals = new double[n];
                for(int i = 0; i < n; i++) {
                    residuals[i] = y[i] - predict(beta, x[i]);
                }
                final double scale = mad(residuals) / 0.6745;
                if(!(scale > 0)) {
                    break;
                }
                for(int i = 0; i < n; i++) {
                    final double r = Math.abs(residuals[i]) / scale;
                    w[i] = r <= HUBER_DELTA ? 1.0 : HUBER_DELTA / r;
                }
                final double[] next = weightedFit(x, y, w, degree);
                double change = 0;
                for(int i = 0; i < beta.length; i++) {
                    change = Math.max(change, Math.abs(next[i] - beta[i]) / Math.max(1e-12, Math.abs(beta[i])));
                }
                beta = next;
                if(change < 1e-8) {
                    break;
                }
            }
        }
        double meanY = 0;
        for(final double v : y) {
            meanY += v;
        }
        meanY /= n;
        double ssTot = 0;
        double ssRes = 0;
        for(int i = 0; i < n; i++) {
            ssTot += (y[i] - meanY) * (y[i] - meanY);
            final double r = y[i] - predict(beta, x[i]);
            ssRes += r * r;
        }
        final double r2 = ssTot > 0 ? 1 - ssRes / ssTot : Double.NaN;
        return new Fit(beta, r2, n);
    }

    private static double predict(final double[] beta, final double x) {
        double value = 0;
        double pow = 1;
        for(final double b : beta) {
            value += b * pow;
            pow *= x;
        }
        return value;
    }

    private static double[] weightedFit(final double[] x, final double[] y, final double[] w, final int degree) {
        final int p = degree + 1;
        final double[][] xtx = new double[p][p];
        final double[] xty = new double[p];
        for(int i = 0; i < x.length; i++) {
            final double[] row = new double[p];
            double pow = 1;
            for(int j = 0; j < p; j++) {
                row[j] = pow;
                pow *= x[i];
            }
            for(int a = 0; a < p; a++) {
                xty[a] += w[i] * row[a] * y[i];
                for(int b = 0; b < p; b++) {
                    xtx[a][b] += w[i] * row[a] * row[b];
                }
            }
        }
        return MatrixOps.solveGram(xtx, xty, 0);
    }

    private static double mad(final double[] values) {
        final double[] abs = new double[values.length];
        final double med = median(values.clone());
        for(int i = 0; i < values.length; i++) {
            abs[i] = Math.abs(values[i] - med);
        }
        return median(abs);
    }

    private static double median(final double[] values) {
        java.util.Arrays.sort(values);
        final int n = values.length;
        return n % 2 == 1 ? values[n / 2] : (values[n / 2 - 1] + values[n / 2]) / 2;
    }

    /**
     * F-test of equal slope coefficients across periods: restricted model = common polynomial
     * slopes with a period-specific intercept (Proposition 6.2's "change is only from the
     * intercept"), full model = separate fits per period. Returns the p-value (small = the
     * slope-stability assumption is rejected).
     */
    public static double slopeStabilityPValue(
            final double[] x0, final double[] y0, final double[] x1, final double[] y1, final int degree) {

        final int n = x0.length + x1.length;
        final int p = degree + 1;
        // Full: separate OLS per period
        final double rssFull = rss(x0, y0, weightedFit(x0, y0, ones(x0.length), degree))
                + rss(x1, y1, weightedFit(x1, y1, ones(x1.length), degree));
        // Restricted: columns [1, I(period), x, x², ...]
        final int q = p + 1;
        final double[][] xtx = new double[q][q];
        final double[] xty = new double[q];
        final double[] xAll = new double[n];
        final double[] yAll = new double[n];
        final double[] period = new double[n];
        System.arraycopy(x0, 0, xAll, 0, x0.length);
        System.arraycopy(x1, 0, xAll, x0.length, x1.length);
        System.arraycopy(y0, 0, yAll, 0, y0.length);
        System.arraycopy(y1, 0, yAll, y0.length, y1.length);
        for(int i = x0.length; i < n; i++) {
            period[i] = 1;
        }
        for(int i = 0; i < n; i++) {
            final double[] row = new double[q];
            row[0] = 1;
            row[1] = period[i];
            double pow = xAll[i];
            for(int j = 2; j < q; j++) {
                row[j] = pow;
                pow *= xAll[i];
            }
            for(int a = 0; a < q; a++) {
                xty[a] += row[a] * yAll[i];
                for(int b = 0; b < q; b++) {
                    xtx[a][b] += row[a] * row[b];
                }
            }
        }
        final double[] betaR = MatrixOps.solveGram(xtx, xty, 0);
        double rssRestricted = 0;
        for(int i = 0; i < n; i++) {
            double fitted = betaR[0] + betaR[1] * period[i];
            double pow = xAll[i];
            for(int j = 2; j < q; j++) {
                fitted += betaR[j] * pow;
                pow *= xAll[i];
            }
            rssRestricted += (yAll[i] - fitted) * (yAll[i] - fitted);
        }
        final int df1 = degree;            // slope coefficients that are freed
        final int df2 = n - 2 * p;         // residual df of the full model
        if(df2 <= 0 || !(rssFull > 0)) {
            return 1.0;
        }
        final double f = ((rssRestricted - rssFull) / df1) / (rssFull / df2);
        if(!(f > 0)) {
            return 1.0;
        }
        // P(F > f) = I_{df2/(df2 + df1 f)}(df2/2, df1/2)
        return regularizedIncompleteBeta(df2 / (df2 + df1 * f), df2 / 2.0, df1 / 2.0);
    }

    private static double rss(final double[] x, final double[] y, final double[] beta) {
        double sum = 0;
        for(int i = 0; i < x.length; i++) {
            final double r = y[i] - predict(beta, x[i]);
            sum += r * r;
        }
        return sum;
    }

    private static double[] ones(final int n) {
        final double[] w = new double[n];
        java.util.Arrays.fill(w, 1.0);
        return w;
    }

    /** Regularized incomplete beta function I_x(a, b) (Numerical Recipes continued fraction). */
    static double regularizedIncompleteBeta(final double x, final double a, final double b) {
        if(x <= 0) {
            return 0;
        }
        if(x >= 1) {
            return 1;
        }
        final double lbeta = logGamma(a + b) - logGamma(a) - logGamma(b) + a * Math.log(x) + b * Math.log(1 - x);
        if(x < (a + 1) / (a + b + 2)) {
            return Math.exp(lbeta) * betaContinuedFraction(x, a, b) / a;
        }
        return 1 - Math.exp(lbeta) * betaContinuedFraction(1 - x, b, a) / b;
    }

    private static double betaContinuedFraction(final double x, final double a, final double b) {
        final double tiny = 1e-300;
        final double qab = a + b;
        final double qap = a + 1;
        final double qam = a - 1;
        double c = 1;
        double d = 1 - qab * x / qap;
        if(Math.abs(d) < tiny) {
            d = tiny;
        }
        d = 1 / d;
        double h = d;
        for(int m = 1; m <= 300; m++) {
            final int m2 = 2 * m;
            double aa = m * (b - m) * x / ((qam + m2) * (a + m2));
            d = 1 + aa * d;
            if(Math.abs(d) < tiny) {
                d = tiny;
            }
            c = 1 + aa / c;
            if(Math.abs(c) < tiny) {
                c = tiny;
            }
            d = 1 / d;
            h *= d * c;
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
            d = 1 + aa * d;
            if(Math.abs(d) < tiny) {
                d = tiny;
            }
            c = 1 + aa / c;
            if(Math.abs(c) < tiny) {
                c = tiny;
            }
            d = 1 / d;
            final double del = d * c;
            h *= del;
            if(Math.abs(del - 1) < 1e-14) {
                break;
            }
        }
        return h;
    }

    private static double logGamma(final double x) {
        final double[] cof = {
                76.18009172947146, -86.50532032941677, 24.01409824083091,
                -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5 };
        double y = x;
        final double tmp = x + 5.5 - (x + 0.5) * Math.log(x + 5.5);
        double ser = 1.000000000190015;
        for(final double c : cof) {
            ser += c / ++y;
        }
        return -tmp + Math.log(2.5066282746310005 * ser / x);
    }
}
