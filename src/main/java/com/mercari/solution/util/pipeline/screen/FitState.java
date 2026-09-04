package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.domain.math.MatrixOps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * State of the unrolled Newton fit of the conditioning model (η = offset + F̃·θ, L2-penalised average
 * log-likelihood). One pass evaluates {@code [n, ll, g, G]} at {@link #proposal}; {@link #advance} is the
 * controller: accept the proposal when the objective did not decrease (then propose a full Newton step),
 * otherwise halve the step from the best point (backtracking costs one pass per halving, never a second
 * kind of pass). Converged states propose their best point and later passes are skipped.
 */
public final class FitState implements Serializable {

    /** step-size floor below which a rejected step ends the fit */
    static final double MIN_ALPHA = 1e-3;
    /** Newton direction floor (max-norm on standardised columns) */
    static final double MIN_DIRECTION = 1e-7;

    public final int k;
    public int iteration = 0;
    public double[] proposal;
    public boolean hasBest = false;
    public double[] bestTheta;
    public double bestLl = Double.NaN;
    public double bestObjective = Double.NEGATIVE_INFINITY;
    public double[] bestGrad;
    public double[][] bestG;
    public double[] direction;
    public double alpha = 1d;
    public boolean converged = false;
    public double nUnits = 0;
    public double ll0 = Double.NaN;
    public int rejected = 0;
    public final List<Double> objectiveHistory = new ArrayList<>();

    private FitState(final int k) {
        this.k = k;
    }

    public static FitState initial(final int k) {
        final FitState s = new FitState(k);
        s.proposal = new double[k];
        return s;
    }

    /** Layout of a pass evaluation: {@code [n, ll, g(k), G(k*k)]}. */
    public static int evaluationLength(final int k) {
        return 2 + k + k * k;
    }

    /**
     * Applies one pass evaluation at {@link #proposal}. {@code eval} is null / empty when the pass was skipped
     * (already converged): the state is returned unchanged.
     */
    public FitState advance(final double[] eval, final double l2, final double tol) {
        if (converged || eval == null || eval.length == 0) return this;
        final double n = eval[0];
        if (!(n > 0)) {
            converged = true;
            if (!hasBest) bestTheta = proposal.clone();
            proposal = bestTheta.clone();
            return this;
        }
        final double ll = eval[1];
        final double[] g = Arrays.copyOfRange(eval, 2, 2 + k);
        final double[][] G = new double[k][k];
        for (int i = 0; i < k; i++) for (int j = 0; j < k; j++) G[i][j] = eval[2 + k + i * k + j];
        final double objective = ll / n - 0.5 * l2 * MatrixOps.dot(proposal, proposal);
        if (iteration == 0) ll0 = ll;
        nUnits = n;
        objectiveHistory.add(objective);
        iteration++;
        boolean finite = Double.isFinite(objective);
        for (int i = 2; finite && i < eval.length; i++) finite = Double.isFinite(eval[i]);
        if (!finite && !hasBest) {
            // the evaluation at the starting point is not finite: nothing to fit on; the remaining passes are skipped
            // and the report sees no best point (the summary's conditioningConverged is false). A non-finite matrix
            // must never reach solveGram (its SVD fallback does not terminate on NaN).
            converged = true;
            return this;
        }
        if (finite && (!hasBest || objective >= bestObjective - 1e-12)) {
            final double improvement = hasBest ? objective - bestObjective : Double.POSITIVE_INFINITY;
            hasBest = true;
            bestTheta = proposal.clone();
            bestLl = ll;
            bestObjective = objective;
            bestGrad = g;
            bestG = G;
            // Newton direction on the penalised average objective: (G/n + l2 I) d = g/n - l2 θ
            final double[][] a = new double[k][k];
            final double[] b = new double[k];
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) a[i][j] = G[i][j] / n;
                b[i] = g[i] / n - l2 * bestTheta[i];
            }
            direction = MatrixOps.solveGram(a, b, l2);
            alpha = 1d;
            double maxStep = 0;
            for (final double d : direction) maxStep = Math.max(maxStep, Math.abs(d));
            if (maxStep < MIN_DIRECTION || improvement < tol) {
                converged = true;
                proposal = bestTheta.clone();
            } else {
                proposal = step(bestTheta, direction, alpha);
            }
        } else {
            rejected++;
            alpha /= 2;
            if (alpha < MIN_ALPHA) {
                converged = true;
                proposal = bestTheta.clone();
            } else {
                proposal = step(bestTheta, direction, alpha);
            }
        }
        return this;
    }

    /** In-sample gain per unit of the fitted conditioning model over θ = 0. */
    public double gainPerUnit() {
        return hasBest && nUnits > 0 && !Double.isNaN(ll0) ? (bestLl - ll0) / nUnits : Double.NaN;
    }

    private static double[] step(final double[] theta, final double[] direction, final double alpha) {
        final double[] out = new double[theta.length];
        for (int i = 0; i < out.length; i++) out[i] = theta[i] + alpha * direction[i];
        return out;
    }
}
