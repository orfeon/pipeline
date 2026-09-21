package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

/**
 * The leading eigenpairs by the restarted block Krylov iteration: they are those of the full Jacobi decomposition — for the indefinite
 * matrix of an embedding (ordered by magnitude, with pairs of equal magnitude) and for a covariance — a warm start
 * changes the number of restarts and not the answer, and what the iteration cannot settle falls back to the full solve.
 */
public class SymmetricEigenTest {

    /** {@code Q diag(spectrum) Qᵀ} with a pseudo-random orthogonal {@code Q} (Gram–Schmidt of a Gaussian matrix). */
    private static double[][] withSpectrum(final double[] spectrum, final long seed) {
        final int d = spectrum.length;
        final Random random = new Random(seed);
        final double[][] q = new double[d][d];
        for (int i = 0; i < d; i++) {
            for (int l = 0; l < d; l++) q[i][l] = random.nextGaussian();
            for (int pass = 0; pass < 2; pass++) {
                for (int b = 0; b < i; b++) {
                    double dot = 0;
                    for (int l = 0; l < d; l++) dot += q[i][l] * q[b][l];
                    for (int l = 0; l < d; l++) q[i][l] -= dot * q[b][l];
                }
            }
            double norm = 0;
            for (int l = 0; l < d; l++) norm += q[i][l] * q[i][l];
            for (int l = 0; l < d; l++) q[i][l] /= Math.sqrt(norm);
        }
        final double[][] a = new double[d][d];
        for (int r = 0; r < d; r++) {
            for (int i = 0; i < d; i++) {
                final double scaled = spectrum[r] * q[r][i];
                for (int j = 0; j < d; j++) a[i][j] += scaled * q[r][j];
            }
        }
        return a;
    }

    /** Eigenvalues of geometrically decaying magnitude and alternating sign, like a PPMI matrix's. */
    private static double[] indefinite(final int d) {
        final double[] spectrum = new double[d];
        for (int i = 0; i < d; i++) spectrum[i] = (i % 3 == 1 ? -1 : 1) * 10 * Math.pow(0.93, i);
        return spectrum;
    }

    private static void assertSamePairs(final double[][] a, final SymmetricEigen.Result result, final int k, final boolean byMagnitude) {
        assertSamePairs(a, result, k, Svd.jacobi(a, byMagnitude));
    }

    /** The reference decomposition of a few hundred rows is the slow path this class exists to avoid: pass it in when it is already at hand. */
    private static void assertSamePairs(final double[][] a, final SymmetricEigen.Result result, final int k, final double[][] full) {
        Assertions.assertEquals(k, result.values().length);
        for (int i = 0; i < k; i++) {
            Assertions.assertEquals(full[0][i], result.values()[i], 1e-9, "eigenvalue " + i);
            double dot = 0, norm = 0;
            for (int l = 0; l < a.length; l++) {
                dot += full[i + 1][l] * result.vectors()[i][l];
                norm += result.vectors()[i][l] * result.vectors()[i][l];
            }
            Assertions.assertEquals(1, norm, 1e-12, "unit vector " + i);
            Assertions.assertEquals(1, Math.abs(dot), 1e-9, "eigenvector " + i + " up to its sign");
        }
    }

    @Test
    public void testLeadingPairsOfAnIndefiniteMatrix() {
        final double[][] a = withSpectrum(indefinite(300), 1);
        final SymmetricEigen.Result result = SymmetricEigen.leading(a, 6, true, null);
        Assertions.assertTrue(result.restarts() > 0, "300 rows are beyond the dense limit");
        assertSamePairs(a, result, 6, true);
        // by magnitude: the second eigenvalue is the negative one
        Assertions.assertTrue(result.values()[1] < 0, "values " + java.util.Arrays.toString(result.values()));
    }

    @Test
    public void testCovarianceOrder() {
        final double[] spectrum = new double[200];
        for (int i = 0; i < spectrum.length; i++) spectrum[i] = 5 * Math.pow(0.9, i);
        final double[][] a = withSpectrum(spectrum, 2);
        final SymmetricEigen.Result result = SymmetricEigen.leading(a, 4, false, null);
        Assertions.assertTrue(result.restarts() > 0);
        assertSamePairs(a, result, 4, false);
    }

    /** λ and −λ have the same magnitude: the iteration cannot tell them apart, the Ritz step does. */
    @Test
    public void testPairsOfEqualMagnitude() {
        final double[] spectrum = indefinite(260);
        for (int i = 0; i < spectrum.length; i++) spectrum[i] /= 2; // the rest stays below the two pairs
        spectrum[0] = 12;
        spectrum[1] = -12;
        spectrum[2] = 7;
        spectrum[3] = -7;
        final double[][] a = withSpectrum(spectrum, 3);
        final SymmetricEigen.Result result = SymmetricEigen.leading(a, 4, true, null);
        Assertions.assertTrue(result.restarts() > 0);
        // within a pair the order is Jacobi's too (a stable sort of |λ|), but only the set is a property of the matrix
        final double[] sorted = result.values().clone();
        java.util.Arrays.sort(sorted);
        Assertions.assertArrayEquals(new double[]{-12, -7, 7, 12}, sorted, 1e-9);
        for (int i = 0; i < 4; i++) {
            // A v = λ v
            double residual = 0;
            for (int row = 0; row < a.length; row++) {
                double av = 0;
                for (int l = 0; l < a.length; l++) av += a[row][l] * result.vectors()[i][l];
                residual += Math.pow(av - result.values()[i] * result.vectors()[i][row], 2);
            }
            Assertions.assertEquals(0, Math.sqrt(residual), 1e-8, "pair " + i);
        }
    }

    /** The answer of a neighbouring problem as the start: the same pairs, in fewer steps. */
    @Test
    public void testWarmStart() {
        // a spectrum that decays slowly — one Krylov space is not enough from a random start
        final int d = 400;
        final double[] spectrum = new double[d];
        for (int i = 0; i < d; i++) spectrum[i] = (i % 3 == 1 ? -1 : 1) * 10 * Math.pow(0.985, i);
        final double[][] a = withSpectrum(spectrum, 4);
        final SymmetricEigen.Result cold = SymmetricEigen.leading(a, 6, true, null);
        // the matrix one block of data later: a small symmetric perturbation
        final Random random = new Random(9);
        final double[][] next = new double[d][d];
        for (int i = 0; i < d; i++) {
            for (int j = i; j < d; j++) {
                final double moved = a[i][j] + 1e-7 * random.nextGaussian();
                next[i][j] = moved;
                next[j][i] = moved;
            }
        }
        final SymmetricEigen.Result fresh = SymmetricEigen.leading(next, 6, true, null);
        final SymmetricEigen.Result warm = SymmetricEigen.leading(next, 6, true, cold.vectors());
        final double[][] full = Svd.jacobi(next, true);
        assertSamePairs(next, warm, 6, full);
        Assertions.assertTrue(warm.restarts() < fresh.restarts(), warm.restarts() + " warm vs " + fresh.restarts() + " cold");
        // a start that is no help — vectors of another length, a repeated vector — is ignored or replaced, not trusted
        assertSamePairs(next, SymmetricEigen.leading(next, 6, true, new double[][]{new double[7], cold.vectors()[0], cold.vectors()[0]}), 6, full);
    }

    @Test
    public void testSmallLowRankAndUnsettledMatrices() {
        // small: the full decomposition answers
        final double[][] small = withSpectrum(new double[]{4, -3, 2, 1, 0.5}, 5);
        final SymmetricEigen.Result dense = SymmetricEigen.leading(small, 2, true, null);
        Assertions.assertEquals(0, dense.restarts());
        assertSamePairs(small, dense, 2, true);
        // more pairs asked for than the matrix has rows
        Assertions.assertEquals(5, SymmetricEigen.leading(small, 9, true, null).values().length);

        // rank 3 in 200 rows: the block is kept of full rank by fresh directions, the pairs beyond the rank are zeros
        final double[] spectrum = new double[200];
        spectrum[0] = 3;
        spectrum[1] = -2;
        spectrum[2] = 1;
        final SymmetricEigen.Result low = SymmetricEigen.leading(withSpectrum(spectrum, 6), 5, true, null);
        Assertions.assertArrayEquals(new double[]{3, -2, 1}, java.util.Arrays.copyOf(low.values(), 3), 1e-9);
        Assertions.assertEquals(0, low.values()[3], 1e-9);
        Assertions.assertEquals(0, low.values()[4], 1e-9);

        // every magnitude equal (A² = I): [X, AX] is already invariant, so one restart is exact
        final double[] signs = new double[180];
        for (int i = 0; i < signs.length; i++) signs[i] = i % 2 == 0 ? 1 : -1;
        final SymmetricEigen.Result involution = SymmetricEigen.leading(withSpectrum(signs, 7), 3, true, null);
        Assertions.assertEquals(1, involution.restarts());
        for (final double value : involution.values()) Assertions.assertEquals(1, Math.abs(value), 1e-9);

        // an iteration that is not given the restarts it needs hands over to the full decomposition: same pairs
        final double[] slow = new double[300];
        for (int i = 0; i < slow.length; i++) slow[i] = (i % 2 == 0 ? 1 : -1) * Math.pow(0.999, i);
        final double[][] hard = withSpectrum(slow, 8);
        final SymmetricEigen.Result handed = SymmetricEigen.leading(hard, 3, true, null, true, 1);
        Assertions.assertEquals(0, handed.restarts());
        assertSamePairs(hard, handed, 3, true);
    }

}
