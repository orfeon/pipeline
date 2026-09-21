package com.mercari.solution.util.pipeline.feature;

import java.util.List;

/**
 * The gauge of a forward spectral fit ({@code fit.align}, docs/design/feature-engine.md §9.6.2). An eigendecomposition
 * fixes its coordinates only up to what the matrix cannot see: the sign of every eigenvector, and any rotation inside
 * a group of (nearly) equal eigenvalues. A static fit is solved once, so a fixed rule — the largest loading positive —
 * is enough to make it reproducible. A forward fit is solved again at every change point, and there the same rule is
 * discontinuous: the eigenvectors move continuously with the data while the loading that happens to be the largest
 * changes hands, and close eigenvalues swap order, so the columns a model reads across blocks flip and mix although
 * the fitted subspace hardly moved.
 *
 * <p>The cure is to express each solution in the coordinates of the one before it. With the rows of {@code A} (this
 * fit) and {@code B} (the previous, already aligned fit) paired over what the two fits share, the orthogonal
 * {@code R} minimising {@code ‖A R − B‖_F} is {@code U Vᵀ} of {@code AᵀB = U Σ Vᵀ} (orthogonal Procrustes). It contains
 * the sign fix — for well-separated eigenvalues {@code R} is a diagonal of ±1 — and handles what signs cannot: a
 * rotation inside a near-degenerate eigenspace and a swap of order are orthogonal maps too. Distances and inner
 * products between the rotated coordinates are those of the unrotated ones.
 *
 * <p>Pure functions of small {@code k × k} matrices ({@code k} = the fitted rank), deterministic.
 */
final class Alignment {

    static final String PROCRUSTES = "procrustes", SIGN = "sign", NONE = "none";
    static final List<String> MODES = List.of(PROCRUSTES, SIGN, NONE);

    /** Singular values below this fraction of the largest carry no direction to align to. */
    private static final double NULL_RATIO = 1e-7;

    private Alignment() {
    }

    /** {@code AᵀB} of two matrices with paired rows; {@code b}'s rows are read up to {@code k} columns (missing ones are zeros). */
    static double[][] cross(final List<double[]> a, final List<double[]> b, final int k) {
        final double[][] m = new double[k][k];
        for (int row = 0; row < a.size(); row++) {
            final double[] x = a.get(row), y = b.get(row);
            final int shared = Math.min(k, y.length);
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < shared; j++) m[i][j] += x[i] * y[j];
            }
        }
        return m;
    }

    /**
     * The {@code k × k} map the columns of {@code A} are multiplied by ({@code A R}) under {@code mode}, from
     * {@code cross = AᵀB}; null when there is nothing to align to (no mode, or an all-zero cross product).
     *
     * @param a the rows of {@code A}: a column the previous fit says nothing about keeps the orientation a static fit
     *          would give it (its largest entry positive)
     */
    static double[][] map(final String mode, final double[][] cross, final List<double[]> a) {
        if (mode == null || NONE.equals(mode)) return null;
        double norm = 0;
        for (final double[] row : cross) for (final double v : row) norm += v * v;
        if (norm == 0) return null;
        return SIGN.equals(mode) ? signs(cross) : rotation(cross, a);
    }

    /** A diagonal of ±1: every column is flipped to correlate positively with its own predecessor (kept when uncorrelated). */
    static double[][] signs(final double[][] cross) {
        final int k = cross.length;
        final double[][] r = new double[k][k];
        for (int i = 0; i < k; i++) r[i][i] = cross[i][i] < 0 ? -1 : 1;
        return r;
    }

    /**
     * {@code R = U Vᵀ} of {@code cross = U Σ Vᵀ}. The singular vectors come from the symmetric eigenproblem of
     * {@code crossᵀ cross} (the Jacobi solver the fits themselves use) with {@code u_i = cross v_i / σ_i}; a direction
     * without a singular value — a component the previous fit did not have, or no overlap along it — is completed to
     * an orthonormal basis and oriented like a static fit.
     */
    static double[][] rotation(final double[][] cross, final List<double[]> a) {
        final int k = cross.length;
        final double[][] gram = new double[k][k];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                double s = 0;
                for (int l = 0; l < k; l++) s += cross[l][i] * cross[l][j];
                gram[i][j] = s;
            }
        }
        final double[][] eigen = Svd.jacobi(gram);
        final double[][] u = new double[k][];
        final double top = Math.sqrt(Math.max(0, eigen[0][0]));
        int aligned = 0;
        for (int i = 0; i < k; i++) {
            final double sigma = Math.sqrt(Math.max(0, eigen[0][i]));
            if (!(sigma > NULL_RATIO * top)) break;
            final double[] v = eigen[i + 1];
            final double[] column = new double[k];
            for (int l = 0; l < k; l++) {
                double s = 0;
                for (int j = 0; j < k; j++) s += cross[l][j] * v[j];
                column[l] = s / sigma;
            }
            if (!orthonormalise(column, u, i)) break;
            u[i] = column;
            aligned++;
        }
        // the directions nothing speaks for: the standard basis vectors, orthogonalised in order
        int next = 0;
        for (int i = aligned; i < k; i++) {
            double[] column = null;
            while (column == null && next < k) {
                final double[] candidate = new double[k];
                candidate[next++] = 1;
                if (orthonormalise(candidate, u, i)) column = candidate;
            }
            if (column == null) throw new IllegalStateException("alignment: no orthonormal completion of rank " + k);
            u[i] = column;
        }
        final double[][] r = new double[k][k];
        for (int i = 0; i < k; i++) {
            final double[] v = eigen[i + 1];
            double sign = 1;
            if (i >= aligned) {
                // R's column for a free direction is decided by u_i alone up to its sign: orient A·(u_i v_iᵀ)'s mass
                final double[] image = new double[a.size()];
                for (int row = 0; row < image.length; row++) {
                    double s = 0;
                    for (int l = 0; l < k; l++) s += a.get(row)[l] * u[i][l];
                    image[row] = s;
                }
                sign = image.length == 0 ? 1 : Svd.sign(image);
            }
            for (int l = 0; l < k; l++) for (int j = 0; j < k; j++) r[l][j] += sign * u[i][l] * v[j];
        }
        return r;
    }

    /** Modified Gram–Schmidt (twice) of {@code column} against {@code basis[0 .. count)}; false when nothing is left of it. */
    private static boolean orthonormalise(final double[] column, final double[][] basis, final int count) {
        for (int pass = 0; pass < 2; pass++) {
            for (int b = 0; b < count; b++) {
                double dot = 0;
                for (int l = 0; l < column.length; l++) dot += column[l] * basis[b][l];
                for (int l = 0; l < column.length; l++) column[l] -= dot * basis[b][l];
            }
        }
        double norm = 0;
        for (final double v : column) norm += v * v;
        norm = Math.sqrt(norm);
        if (!(norm > 1e-8)) return false;
        for (int l = 0; l < column.length; l++) column[l] /= norm;
        return true;
    }

    /** {@code row · R}. */
    static double[] apply(final double[] row, final double[][] r) {
        final double[] out = new double[r.length];
        for (int j = 0; j < r.length; j++) {
            double s = 0;
            for (int i = 0; i < r.length; i++) s += row[i] * r[i][j];
            out[j] = s;
        }
        return out;
    }

}
