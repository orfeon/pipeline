package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.pipeline.glm.StatMath;

import java.util.List;

/**
 * The result probabilities of a match whose two scores are Poisson — the {@code poisson} rating's model read forward
 * (the context op {@code poissonProb}): side {@code i} scores {@code Poisson(λ_i)}, {@code λ_i = exp(attack_i −
 * defence_j + offset_i)}, and the two scores are independent, Dixon–Coles dependent ({@code rho}: the four low-score
 * results reweighted by {@code τ}, the marginals unchanged) or bivariate Poisson ({@code covariance}: a common
 * component of that mean added to both, so a side's expected score is {@code λ_i + covariance}).
 *
 * <p>Readouts: {@link #expected}, {@link #win} / {@link #draw} / {@link #lose} (the difference of the scores), {@link #over}
 * (the total beyond a threshold) and {@link #bothScore}. Under {@code covariance} the common part cancels in the
 * difference, so win / draw / lose are those of the independent model at the own rates (a Skellam law), while the total
 * carries it twice. Under {@code rho} the correction moves one mass {@code λ_0 λ_1 ρ e^(−λ_0 − λ_1)} out of the 0–0 and
 * 1–1 cells into 0–1 and 1–0 (a negative {@code rho}: the other way) — and a {@code rho} that makes any {@code τ}
 * non-positive at these rates is left out, as the rating leaves it out of its update.
 *
 * <p>The difference law is summed over the Poisson masses up to the point their tail is below the double's precision;
 * the sums run in a fixed order, so the result is a pure function of the two rates. A rate (or common part) above
 * {@link #MAX_RATE}, or a rate that is not positive, has no model: {@link #of} returns null.
 */
public final class PoissonMatch {

    /** The readouts of the op: {@code over} takes the thresholds of the {@code over} list, the others are one column each. */
    public static final List<String> FUNCS = List.of("expected", "win", "draw", "lose", "over", "bothScore");

    /** The total an {@code over} is read against without an {@code over} list (a common totals line). */
    public static final double DEFAULT_OVER = 2.5;

    /** Rates past this read null: the difference law is summed over the masses, about {@code λ + 12 √λ} terms. */
    public static final double MAX_RATE = 1e4;

    private final double lambda0, lambda1, covariance;
    /** Dixon–Coles: the mass moved between the four low-score cells ({@code λ_0 λ_1 ρ e^(−λ_0 − λ_1)}), 0 without. */
    private final double tilt;
    private final double winIndependent, drawIndependent, loseIndependent;

    private PoissonMatch(final double lambda0, final double lambda1, final double covariance, final double tilt,
                         final double win, final double draw, final double lose) {
        this.lambda0 = lambda0;
        this.lambda1 = lambda1;
        this.covariance = covariance;
        this.tilt = tilt;
        this.winIndependent = win;
        this.drawIndependent = draw;
        this.loseIndependent = lose;
    }

    /**
     * The match of two sides at these rates, or null when a rate is not a positive number up to {@link #MAX_RATE},
     * {@code rho} is outside (−1, 1), {@code covariance} is not a rate in [0, {@link #MAX_RATE}], or both are non-zero
     * (two models of the dependence: {@link #over} reads the common part and not the tilt; both 0 = independent scores).
     */
    public static PoissonMatch of(final double lambda0, final double lambda1, final double rho, final double covariance) {
        if (!(lambda0 > 0) || !(lambda1 > 0) || lambda0 > MAX_RATE || lambda1 > MAX_RATE) return null;
        if (!(Math.abs(rho) < 1) || !(covariance >= 0) || covariance > MAX_RATE) return null;
        if (rho != 0d && covariance != 0d) return null;
        // the difference of the own parts: P(x > y) = Σ_y p_1(y) P(x > y), P(x = y) = Σ_y p_1(y) p_0(y), P(x < y) = Σ_y p_1(y) P(x < y),
        // the tails of p_0 accumulated from both ends so that every term keeps its digits
        final double largest = Math.max(lambda0, lambda1);
        final int n = (int) Math.ceil(largest + 12 * Math.sqrt(largest) + 40);
        final double[] logFactorial = new double[n];
        for (int k = 0; k < n; k++) logFactorial[k] = StatMath.logGamma(k + 1d);
        final double[] p0 = pmf(lambda0, logFactorial), p1 = pmf(lambda1, logFactorial), above = new double[n];
        double tail = 0;
        for (int y = n - 1; y >= 0; y--) {
            above[y] = tail;
            tail += p0[y];
        }
        double win = 0, draw = 0, lose = 0, below = 0;
        for (int y = 0; y < n; y++) {
            win += p1[y] * above[y];
            draw += p1[y] * p0[y];
            lose += p1[y] * below;
            below += p0[y];
        }
        // equal rates: the two sides are one law, so the two sums must read as one too (they differ in the last bit by their
        // paths) - whichever row the group makes side 0, the readouts are the same bits
        if (lambda0 == lambda1) lose = win;
        double tilt = 0;
        if (rho != 0d) {
            // τ of 0–0, 0–1 (side 0 scoring 0), 1–0, 1–1: all four must stay probabilities, else the correction is left out
            final double tau00 = 1 - lambda0 * lambda1 * rho, tau01 = 1 + lambda0 * rho, tau10 = 1 + lambda1 * rho, tau11 = 1 - rho;
            if (tau00 > 0 && tau01 > 0 && tau10 > 0 && tau11 > 0) tilt = lambda0 * lambda1 * rho * Math.exp(-lambda0 - lambda1);
        }
        return new PoissonMatch(lambda0, lambda1, covariance, tilt, win, draw, lose);
    }

    /**
     * The Poisson masses {@code P(k)}, {@code k <} the length of {@code logFactorial} ({@code ln k!}, shared by the two
     * sides), in logs (no underflow of {@code e^(−λ)} at a large rate).
     */
    private static double[] pmf(final double lambda, final double[] logFactorial) {
        final double[] p = new double[logFactorial.length];
        final double log = Math.log(lambda);
        for (int k = 0; k < p.length; k++) p[k] = Math.exp(k * log - lambda - logFactorial[k]);
        return p;
    }

    /** The rate of side {@code i} (0 or 1): its own part. */
    public double rate(final int i) {
        return i == 0 ? lambda0 : lambda1;
    }

    /** The expected score of side {@code i}: its rate plus the common part. */
    public double expected(final int i) {
        return rate(i) + covariance;
    }

    /** The probability side {@code i} scores more than the other. */
    public double win(final int i) {
        // the 1–0 cell (side 0 ahead) and the 0–1 cell (side 1 ahead) each gain the tilt
        return (i == 0 ? winIndependent : loseIndependent) + tilt;
    }

    /** The probability of equal scores. */
    public double draw() {
        return drawIndependent - 2 * tilt;
    }

    /** The probability side {@code i} scores less than the other. */
    public double lose(final int i) {
        return win(1 - i);
    }

    /**
     * The probability the total of the two scores exceeds {@code threshold} (a line: 2.5 reads a total of 3 or more; 1
     * for a negative threshold). Under {@code covariance} the common part counts twice in the total.
     */
    public double over(final double threshold) {
        if (threshold < 0) return 1d;
        final long m = (long) Math.floor(threshold);
        final double rate = lambda0 + lambda1;
        if (covariance == 0d) {
            // the four low-score cells hold totals 0, 1, 1, 2: below the line they move −tilt (0–0 alone) or +tilt (0–0, 0–1, 1–0); from 2 on all four, which cancel
            return tailAbove(rate, m) + (m == 0 ? tilt : m == 1 ? -tilt : 0d);
        }
        // total = 2K + S, S ~ Poisson(λ_0 + λ_1): P(total > m) = Σ_k P(K = k) P(S > m − 2k), 1 once 2k > m. The sum stops
        // where K's own tail is below the double's precision (as the difference law does): a far line costs no more than a near one
        double sum = 0;
        final long half = m / 2;
        final long last = Math.min(half, (long) Math.ceil(covariance + 12 * Math.sqrt(covariance) + 40));
        final double logCovariance = Math.log(covariance);
        for (long k = 0; k <= last; k++) {
            sum += Math.exp(k * logCovariance - covariance - StatMath.logGamma(k + 1d)) * tailAbove(rate, m - 2 * k);
        }
        return sum + tailAbove(covariance, half);
    }

    /** The probability both sides score at least once. */
    public double bothScore() {
        // 1 − P(x = 0) − P(y = 0) + P(0, 0); under rho the 0–0 cell loses the tilt (the marginals keep theirs)
        return 1 - Math.exp(-lambda0 - covariance) - Math.exp(-lambda1 - covariance) + Math.exp(-lambda0 - lambda1 - covariance) - tilt;
    }

    /** {@code P(S > m)} for {@code S ~ Poisson(rate)}: the regularized lower incomplete gamma {@code P(m + 1, rate)}. */
    private static double tailAbove(final double rate, final long m) {
        return m < 0 ? 1d : StatMath.regularizedGammaP(m + 1d, rate);
    }
}
