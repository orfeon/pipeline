package com.mercari.solution.util.pipeline.glm;

import com.mercari.solution.util.domain.math.MatrixOps;

/**
 * The offset GLM of the supervised transforms, per unit and pure: the fitted means of η = offset + F·θ and one
 * Newton pass evaluation {@code [n, ll, g, G]} at θ. The grouped family is the conditional logit within the unit
 * (softmax of log p + F·θ, no intercept); the row families read the baseline mean through the family's link
 * (logit / log / identity) or, in prior mode, an intercept column that the caller puts into F. Driven by
 * {@link FitState} across passes.
 */
public final class GlmFit {

    private GlmFit() {}

    /**
     * Fitted means at θ: grouped softmax of log p + F·θ within the unit; binomial σ(logit p + F·θ); gaussian
     * μ + F·θ (identity link); poisson exp(log μ + F·θ). In prior mode ({@code prior}) the offset is 0 and the
     * intercept column of F carries the prior.
     *
     * @param p the baseline mean per row (the grouped share, the binomial probability, the gaussian value, the
     *          poisson rate); ignored in prior mode for the row families
     * @param f the design (n × k), intercept column included when the family carries one
     */
    public static double[] fitted(final Family family, final boolean prior, final double[] p, final double[][] f, final double[] theta) {
        final int n = f.length;
        final double[] eta = new double[n];
        for (int i = 0; i < n; i++) {
            double e = MatrixOps.dot(f[i], theta);
            if (family.isGrouped()) {
                e += p[i] > 0 ? Math.log(p[i]) : Double.NEGATIVE_INFINITY;
            } else if (!prior) {
                e += switch (family) {
                    case BINOMIAL -> Math.log(p[i] / (1 - p[i]));
                    case POISSON -> Math.log(p[i]);
                    default -> p[i];
                };
            }
            eta[i] = e;
        }
        return means(family, eta);
    }

    /** The means of the linear predictor: softmax within the unit (grouped), σ(η) clamped (binomial), exp(η) (poisson), η (gaussian). */
    public static double[] means(final Family family, final double[] eta) {
        final int n = eta.length;
        final double[] mu = new double[n];
        switch (family) {
            case GROUPED_MULTINOMIAL -> softmax(eta, mu);
            case BINOMIAL -> {
                for (int i = 0; i < n; i++) mu[i] = Baselines.clamp(1d / (1d + Math.exp(-eta[i])));
            }
            case POISSON -> {
                for (int i = 0; i < n; i++) mu[i] = Math.exp(Math.min(eta[i], 700d));
            }
            default -> System.arraycopy(eta, 0, mu, 0, n);
        }
        return mu;
    }

    /** Softmax of η into {@code out} (shifted by the maximum for stability); an all −∞ unit gives NaN. */
    public static void softmax(final double[] eta, final double[] out) {
        final int n = eta.length;
        double max = Double.NEGATIVE_INFINITY;
        for (final double e : eta) if (e > max) max = e;
        double sum = 0;
        for (int i = 0; i < n; i++) {
            out[i] = Math.exp(eta[i] - max);
            sum += out[i];
        }
        for (int i = 0; i < n; i++) out[i] /= sum;
    }

    /** Log-likelihood term of one row of a row family (gaussian at σ² = 1: the fit is least squares, σ² enters the report). */
    public static double rowLogLikelihood(final Family family, final double y, final double mu) {
        return switch (family) {
            case BINOMIAL -> y * Math.log(mu) + (1 - y) * Math.log(1 - mu);
            case POISSON -> y * Math.log(Math.max(mu, 1e-300)) - mu;
            default -> -0.5 * (y - mu) * (y - mu);
        };
    }

    /**
     * One Newton pass evaluation of a unit at the fitted means {@code mu}: {@code [units, ll, g(k), G(k*k)]}
     * (weighted). Units are 1 per group (grouped family, weighted by {@code unitWeight}) or the row weight mass
     * (row families), each weighted like ll / g / G so that the average objective of {@link FitState} is
     * invariant to a rescaling of the weight column.
     *
     * @param y  the labels (grouped: shares summing to 1)
     * @param mu the fitted means at θ ({@link #fitted})
     * @param w  the row weights (row families)
     * @param f  the design (n × k)
     */
    public static double[] evaluate(final Family family, final double[] y, final double[] mu, final double[] w, final double unitWeight,
                                    final double[][] f, final int k) {
        final int n = f.length;
        final double[] out = new double[FitState.evaluationLength(k)];
        if (family.isGrouped()) {
            final double wu = unitWeight;
            double ll = 0;
            final double[] pf = new double[k];
            for (int i = 0; i < n; i++) {
                if (y[i] > 0) ll += y[i] * Math.log(Math.max(mu[i], 1e-300));
                for (int a = 0; a < k; a++) {
                    out[2 + a] += wu * (y[i] - mu[i]) * f[i][a];
                    pf[a] += mu[i] * f[i][a];
                    for (int b = 0; b < k; b++) out[2 + k + a * k + b] += wu * mu[i] * f[i][a] * f[i][b];
                }
            }
            for (int a = 0; a < k; a++) for (int b = 0; b < k; b++) out[2 + k + a * k + b] -= wu * pf[a] * pf[b];
            // the unit count carries the same weight as ll / g / G, so the average objective is weight-invariant
            out[0] = wu;
            out[1] = wu * ll;
        } else {
            double ll = 0;
            double wsum = 0;
            for (int i = 0; i < n; i++) {
                final double wi = w[i];
                wsum += wi;
                ll += wi * rowLogLikelihood(family, y[i], mu[i]);
                final double v = family.fisherWeight(mu[i]);
                for (int a = 0; a < k; a++) {
                    out[2 + a] += wi * (y[i] - mu[i]) * f[i][a];
                    for (int b = 0; b < k; b++) out[2 + k + a * k + b] += wi * v * f[i][a] * f[i][b];
                }
            }
            out[0] = wsum;
            out[1] = ll;
        }
        return out;
    }
}
