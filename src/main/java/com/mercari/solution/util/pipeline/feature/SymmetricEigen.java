package com.mercari.solution.util.pipeline.feature;

import org.ojalgo.matrix.decomposition.Eigenvalue;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.R064Store;

import java.util.Arrays;

/**
 * The leading eigenpairs of a dense symmetric matrix. The fits that need an eigendecomposition keep a handful of
 * components of a matrix with hundreds of rows ({@link Spectral}: one row per embedded value; {@link Svd}: one per
 * vector dimension), and a forward fit solves one such problem per change point. Up to {@link #DENSE_LIMIT} rows the
 * cyclic Jacobi sweep of {@link Svd#jacobi} answers (exact to rounding, and the path every earlier artifact was
 * written by); beyond it, ojalgo's symmetric decomposition (Householder tridiagonalisation, then QR) — the same
 * {@code O(d³)} but with the constant of a real implementation: a 700-row matrix in 0.15 s where the sweep takes
 * 7–15 s, 1024 rows in 0.5 s.
 *
 * <p>A restarted block Krylov iteration for the leading pairs alone was measured against it on the same matrices and
 * lost on slowly decaying spectra — the usual ones for co-occurrence data — while winning only on fast decay
 * (0.16 s vs 0.18 s at 700 rows), so the fit pays a full decomposition and keeps what it needs.
 *
 * <p>Deterministic, single-threaded, and ordered like the sweep: by decreasing magnitude for an indefinite matrix
 * (PPMI), by decreasing value for a positive semi-definite one (a covariance), ties in the order the decomposition
 * returned them.
 */
final class SymmetricEigen {

    /** Up to this many rows the Jacobi sweep is fast enough, and what every earlier fit was solved by. */
    static final int DENSE_LIMIT = 128;

    /**
     * @param values  the leading eigenvalues: {@code k} of them, or every one when the matrix has fewer rows
     * @param vectors their unit eigenvectors, one per row (oriented by the decomposition; the fits orient them)
     */
    record Result(double[] values, double[][] vectors) {
    }

    private SymmetricEigen() {
    }

    /**
     * @param byMagnitude order by {@code |λ|} (an indefinite matrix). When false the matrix must be positive
     *                    semi-definite, for which the two orders coincide
     */
    static Result leading(final double[][] a, final int k, final boolean byMagnitude) {
        final int d = a.length;
        final int kept = Math.min(k, d);
        if (d <= DENSE_LIMIT) {
            final double[][] eigen = Svd.jacobi(a, byMagnitude);
            final double[] values = new double[kept];
            final double[][] vectors = new double[kept][];
            for (int i = 0; i < kept; i++) {
                values[i] = eigen[0][i];
                vectors[i] = eigen[i + 1];
            }
            return new Result(values, vectors);
        }
        final Eigenvalue<Double> decomposition = Eigenvalue.R064.make(true);
        if (!decomposition.decompose(R064Store.FACTORY.rows(a))) {
            throw new IllegalStateException("eigendecomposition of a " + d + " x " + d + " symmetric matrix failed");
        }
        final MatrixStore<Double> diagonal = decomposition.getD(), v = decomposition.getV();
        final double[] all = new double[d];
        for (int i = 0; i < d; i++) all[i] = diagonal.doubleValue(i, i);
        final Integer[] order = new Integer[d];
        for (int i = 0; i < d; i++) order[i] = i;
        // the sort of the sweep: stable, so equal magnitudes keep the decomposition's order
        Arrays.sort(order, (x, y) -> Double.compare(byMagnitude ? Math.abs(all[y]) : all[y], byMagnitude ? Math.abs(all[x]) : all[x]));
        final double[] values = new double[kept];
        final double[][] vectors = new double[kept][d];
        for (int i = 0; i < kept; i++) {
            values[i] = all[order[i]];
            for (int l = 0; l < d; l++) vectors[i][l] = v.doubleValue(l, order[i]);
        }
        return new Result(values, vectors);
    }

}
