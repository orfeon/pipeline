package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

/**
 * The leading eigenpairs of a large symmetric matrix are those of the Jacobi sweep — for the indefinite matrix of an
 * embedding (ordered by magnitude, with pairs of equal magnitude) and for a covariance — bit-for-bit reproducible,
 * and a small matrix still takes the sweep itself.
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
        final double[][] full = Svd.jacobi(a, byMagnitude);
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
        final SymmetricEigen.Result result = SymmetricEigen.leading(a, 6, true);
        assertSamePairs(a, result, 6, true);
        // by magnitude: the second eigenvalue is the negative one
        Assertions.assertTrue(result.values()[1] < 0, "values " + java.util.Arrays.toString(result.values()));
        // the same matrix again: the same bits
        final SymmetricEigen.Result again = SymmetricEigen.leading(a, 6, true);
        Assertions.assertArrayEquals(result.values(), again.values(), 0);
        for (int i = 0; i < 6; i++) Assertions.assertArrayEquals(result.vectors()[i], again.vectors()[i], 0);
    }

    @Test
    public void testCovarianceOrder() {
        final double[] spectrum = new double[200];
        for (int i = 0; i < spectrum.length; i++) spectrum[i] = 5 * Math.pow(0.9, i);
        final double[][] a = withSpectrum(spectrum, 2);
        assertSamePairs(a, SymmetricEigen.leading(a, 4, false), 4, false);
    }

    /** λ and −λ have the same magnitude: both are leading, and each is an eigenpair. */
    @Test
    public void testPairsOfEqualMagnitude() {
        final double[] spectrum = indefinite(260);
        for (int i = 0; i < spectrum.length; i++) spectrum[i] /= 2; // the rest stays below the two pairs
        spectrum[0] = 12;
        spectrum[1] = -12;
        spectrum[2] = 7;
        spectrum[3] = -7;
        final double[][] a = withSpectrum(spectrum, 3);
        final SymmetricEigen.Result result = SymmetricEigen.leading(a, 4, true);
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

    @Test
    public void testSmallAndLowRankMatrices() {
        // small: the sweep itself answers, so an earlier artifact is reproduced bit for bit
        final double[][] small = withSpectrum(new double[]{4, -3, 2, 1, 0.5}, 5);
        final SymmetricEigen.Result dense = SymmetricEigen.leading(small, 2, true);
        final double[][] sweep = Svd.jacobi(small, true);
        Assertions.assertArrayEquals(new double[]{sweep[0][0], sweep[0][1]}, dense.values(), 0);
        Assertions.assertArrayEquals(sweep[1], dense.vectors()[0], 0);
        // more pairs asked for than the matrix has rows
        Assertions.assertEquals(5, SymmetricEigen.leading(small, 9, true).values().length);
        Assertions.assertEquals(200, SymmetricEigen.leading(withSpectrum(new double[200], 6), 300, true).values().length);

        // rank 3 in 200 rows: the pairs beyond the rank are zeros
        final double[] spectrum = new double[200];
        spectrum[0] = 3;
        spectrum[1] = -2;
        spectrum[2] = 1;
        final SymmetricEigen.Result low = SymmetricEigen.leading(withSpectrum(spectrum, 6), 5, true);
        Assertions.assertArrayEquals(new double[]{3, -2, 1}, java.util.Arrays.copyOf(low.values(), 3), 1e-9);
        Assertions.assertEquals(0, low.values()[3], 1e-9);
        Assertions.assertEquals(0, low.values()[4], 1e-9);
    }

}
