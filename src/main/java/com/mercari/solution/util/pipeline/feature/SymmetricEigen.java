package com.mercari.solution.util.pipeline.feature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * The leading eigenpairs of a dense symmetric matrix. The fits that need an eigendecomposition keep a handful of
 * components of a matrix with hundreds of rows ({@link Spectral}: one row per embedded value; {@link Svd}: one per
 * vector dimension), and a forward fit solves one such problem per change point. The cyclic Jacobi sweep of
 * {@link Svd#jacobi} returns every eigenpair at {@code O(d³)} per sweep — seconds for 700 rows, times every change
 * point of every block — so beyond {@link #DENSE_LIMIT} rows the leading {@code k} are found by a <b>restarted block
 * Krylov iteration with a Rayleigh–Ritz step</b> instead:
 *
 * <pre>
 *   X ← m orthonormal start vectors (m = k plus a guard of extra directions)
 *   repeat:  V = orth[X, AX, A²X, …, A^q X]          (every vector against all before it; a vector that adds
 *                                                       no direction is dropped)
 *            H = Vᵀ A V = W Θ Wᵀ                       (Jacobi on the small matrix)
 *            X ← the m leading Ritz vectors V W
 *            stop when ‖A x_i − θ_i x_i‖ ≤ tol · ‖A‖_F for the k leading pairs
 * </pre>
 *
 * A Krylov space approximates the eigenvalues at <em>both ends</em> of the spectrum, which for a symmetric matrix are
 * the ones of largest magnitude — what an indefinite matrix (PPMI) is ordered by, and the largest values of a positive
 * semi-definite one (a covariance). Against plain subspace iteration ({@code q = 0}), whose error falls like
 * {@code |λ_{m+1} / λ_i|} per product with {@code A}, the polynomial the Krylov space finds falls like a Chebyshev
 * polynomial of the gap: a slowly decaying spectrum — the usual one for co-occurrence data — costs tens of products
 * instead of hundreds. The block makes multiplicity harmless (a pair {@code ±λ}, or a repeated eigenvalue, up to the
 * block size), and a start close to the answer — the fit of the block before, under forward — saves restarts.
 *
 * <p>Deterministic: the start is the given vectors completed by a fixed pseudo-random sequence. If the iteration has
 * not settled within {@link #MAX_RESTARTS}, the full Jacobi decomposition answers instead: slower, never wrong.
 */
final class SymmetricEigen {

    private static final Logger LOG = LoggerFactory.getLogger(SymmetricEigen.class);

    /** Up to this many rows the full decomposition is fast enough, and exact to rounding. */
    static final int DENSE_LIMIT = 128;
    /** Products with the matrix per restart: the Krylov space of a restart is {@code [X, AX, …, A^q X]}. */
    static final int KRYLOV_STEPS = 8;
    static final int MAX_RESTARTS = 40;
    /** Residual {@code ‖A x − θ x‖} of an accepted pair, relative to {@code ‖A‖_F}. */
    static final double TOLERANCE = 1e-12;

    /**
     * @param values   the leading eigenvalues (by decreasing magnitude, or value for a PSD matrix): {@code k} of
     *                 them, or every one of them when the matrix has fewer rows than {@code k}
     * @param vectors  their unit eigenvectors, one per row
     * @param restarts Krylov restarts taken; 0 = the full decomposition answered
     */
    record Result(double[] values, double[][] vectors, int restarts) {
    }

    private SymmetricEigen() {
    }

    /**
     * @param byMagnitude order by {@code |λ|} (an indefinite matrix). When false the matrix must be positive
     *                    semi-definite, for which the two orders coincide
     * @param warm        start vectors of length {@code d} (any number, not necessarily orthonormal), or null
     */
    static Result leading(final double[][] a, final int k, final boolean byMagnitude, final double[][] warm) {
        return leading(a, k, byMagnitude, warm, true);
    }

    /**
     * @param loud whether a hand-over to the full decomposition is reported: a forward fit solves one model per
     *             change point, and every one of them meets the same spectrum — only the whole-input fit reports,
     *             as it does for everything else it could say about the fit
     */
    static Result leading(final double[][] a, final int k, final boolean byMagnitude, final double[][] warm, final boolean loud) {
        return leading(a, k, byMagnitude, warm, loud, MAX_RESTARTS);
    }

    static Result leading(final double[][] a, final int k, final boolean byMagnitude, final double[][] warm, final boolean loud, final int maxRestarts) {
        final int d = a.length;
        final int m = Math.min(d, Math.max(2 * k, k + 8));
        // the iteration pays when its spaces are small next to the matrix
        if (d <= DENSE_LIMIT || 4 * m >= d) return dense(a, k, byMagnitude);
        final int steps = Math.max(1, Math.min(KRYLOV_STEPS, d / (2 * m) - 1));

        double frobenius = 0;
        for (final double[] row : a) for (final double v : row) frobenius += v * v;
        frobenius = Math.sqrt(frobenius);
        if (frobenius == 0) return dense(a, k, byMagnitude);

        final SplittableRandom random = new SplittableRandom(0x5EEDL);
        double[][] x = new double[m][];
        for (int j = 0; j < m; j++) {
            x[j] = warm != null && j < warm.length && warm[j] != null && warm[j].length == d ? warm[j].clone() : gaussian(random, d);
        }

        for (int restart = 1; restart <= maxRestarts; restart++) {
            // V = orth[X, AX, …]: `images` holds A v for every basis vector, the block products being the next block
            final List<double[]> basis = new ArrayList<>(m * (steps + 1)), images = new ArrayList<>(m * (steps + 1));
            List<double[]> block = new ArrayList<>(m);
            for (final double[] start : x) {
                if (orthonormalise(start, basis)) {
                    basis.add(start);
                    block.add(start);
                }
            }
            // a start without a direction of its own (a repeated or zero warm vector): fresh ones keep the block full
            for (int attempt = 0; block.size() < m && attempt < 4 * m; attempt++) {
                final double[] fresh = gaussian(random, d);
                if (orthonormalise(fresh, basis)) {
                    basis.add(fresh);
                    block.add(fresh);
                }
            }
            for (int step = 0; step <= steps && !block.isEmpty(); step++) {
                final List<double[]> next = new ArrayList<>(block.size());
                for (final double[] v : block) {
                    final double[] image = multiply(a, v);
                    images.add(image);
                    if (step == steps) continue;
                    final double[] candidate = image.clone();
                    // a product that adds no direction — an invariant subspace was reached — is dropped
                    if (orthonormalise(candidate, basis)) {
                        basis.add(candidate);
                        next.add(candidate);
                    }
                }
                block = next;
            }
            final int n = basis.size();
            final double[][] h = new double[n][n];
            for (int i = 0; i < n; i++) {
                for (int j = i; j < n; j++) {
                    final double dot = dot(basis.get(i), images.get(j));
                    h[i][j] = dot;
                    h[j][i] = dot;
                }
            }
            final double[][] ritz = Svd.jacobi(h, byMagnitude);
            final int kept = Math.min(m, n);
            final double[][] xr = combine(basis, ritz, kept, d), yr = combine(images, ritz, kept, d);
            double worst = 0;
            for (int i = 0; i < Math.min(k, kept); i++) {
                // V is orthonormal to rounding and no further — its last directions are deliberately kept at that
                // level — so x = V w need not come out exactly unit, and the residual below does not pin its length
                // ((Ax − θx) scales with x). Normalise it here, which is also what the caller receives: Spectral
                // scales a whole coordinate column by the vector and Svd projects rows onto it.
                final double length = Math.sqrt(dot(xr[i], xr[i]));
                if (length > 0) {
                    for (int l = 0; l < d; l++) {
                        xr[i][l] /= length;
                        yr[i][l] /= length;
                    }
                }
                double residual = 0;
                for (int l = 0; l < d; l++) {
                    final double r = yr[i][l] - ritz[0][i] * xr[i][l];
                    residual += r * r;
                }
                worst = Math.max(worst, Math.sqrt(residual));
            }
            if (worst <= TOLERANCE * frobenius && kept >= k) {
                final double[] values = new double[k];
                final double[][] vectors = new double[k][];
                for (int i = 0; i < k; i++) {
                    values[i] = ritz[0][i];
                    vectors[i] = xr[i];
                }
                return new Result(values, vectors, restart);
            }
            x = xr;
        }
        if (loud) {
            LOG.info("leading eigenpairs: the block Krylov iteration ({} of {} directions) had not settled after {} restart(s); using the full decomposition",
                    m, d, maxRestarts);
        }
        return dense(a, k, byMagnitude);
    }

    private static Result dense(final double[][] a, final int k, final boolean byMagnitude) {
        final double[][] eigen = Svd.jacobi(a, byMagnitude);
        final int kept = Math.min(k, a.length);
        final double[] values = new double[kept];
        final double[][] vectors = new double[kept][];
        for (int i = 0; i < kept; i++) {
            values[i] = eigen[0][i];
            vectors[i] = eigen[i + 1];
        }
        return new Result(values, vectors, 0);
    }

    /** The {@code count} leading Ritz combinations of {@code vectors}: row {@code i} is {@code Σ_j w_i[j] · vectors[j]}. */
    private static double[][] combine(final List<double[]> vectors, final double[][] ritz, final int count, final int d) {
        final double[][] out = new double[count][d];
        for (int i = 0; i < count; i++) {
            final double[] w = ritz[i + 1], into = out[i];
            for (int j = 0; j < vectors.size(); j++) {
                final double weight = w[j];
                if (weight == 0) continue;
                final double[] from = vectors.get(j);
                for (int l = 0; l < d; l++) into[l] += weight * from[l];
            }
        }
        return out;
    }

    private static double[] multiply(final double[][] a, final double[] x) {
        final double[] out = new double[a.length];
        for (int i = 0; i < a.length; i++) out[i] = dot(a[i], x);
        return out;
    }

    private static double dot(final double[] x, final double[] y) {
        double s = 0;
        for (int l = 0; l < x.length; l++) s += x[l] * y[l];
        return s;
    }

    private static double[] gaussian(final SplittableRandom random, final int d) {
        final double[] v = new double[d];
        for (int l = 0; l < d; l++) v[l] = random.nextGaussian();
        return v;
    }

    /**
     * Modified Gram–Schmidt (twice) of {@code v} against an orthonormal {@code basis}, in place; false — and
     * {@code v} is not to be used — when nothing of it is left.
     */
    private static boolean orthonormalise(final double[] v, final List<double[]> basis) {
        final double before = Math.sqrt(dot(v, v));
        if (!(before > 0)) return false;
        for (int pass = 0; pass < 2; pass++) {
            for (final double[] b : basis) {
                final double projection = dot(v, b);
                for (int l = 0; l < v.length; l++) v[l] -= projection * b[l];
            }
        }
        // "nothing left" is rounding level, no more: close to convergence the new directions ARE tiny next to their
        // products — as small as the residuals — and dropping them would stall the iteration above its tolerance
        final double norm = Math.sqrt(dot(v, v));
        if (!(norm > 1e-13 * before)) return false;
        for (int l = 0; l < v.length; l++) v[l] /= norm;
        return true;
    }

}
