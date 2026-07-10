package com.mercari.solution.util.domain.math;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class MatrixOpsTest {

    private static final double DELTA = 1e-9;

    @Test
    public void testCosineSimilarity() {
        Assertions.assertEquals(1d,
                MatrixOps.cosineSimilarity(new double[]{1, 2, 3}, new double[]{2, 4, 6}), DELTA);
        Assertions.assertEquals(0d,
                MatrixOps.cosineSimilarity(new double[]{1, 0}, new double[]{0, 1}), DELTA);
        Assertions.assertEquals(-1d,
                MatrixOps.cosineSimilarity(new double[]{1, 1}, new double[]{-1, -1}), DELTA);
        // A zero-norm vector has no direction.
        Assertions.assertNull(
                MatrixOps.cosineSimilarity(new double[]{0, 0}, new double[]{1, 1}));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.cosineSimilarity(new double[]{1}, new double[]{1, 2}));
    }

    @Test
    public void testMultiply() {
        final double[][] rotate90 = {{0, -1}, {1, 0}};
        Assertions.assertArrayEquals(new double[]{-2, 1},
                MatrixOps.multiply(rotate90, new double[]{1, 2}), DELTA);
        // Non-square: 2x3 projection down to 2 dimensions.
        final double[][] projection = {{1, 0, 0}, {0, 1, 0}};
        Assertions.assertArrayEquals(new double[]{7, 8},
                MatrixOps.multiply(projection, new double[]{7, 8, 9}), DELTA);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.multiply(projection, new double[]{1, 2}));
    }

    @Test
    public void testSolveSquare() {
        // 2x + y = 5, x + 3y = 10 -> x = 1, y = 3
        final double[] solution = MatrixOps.solve(
                new double[][]{{2, 1}, {1, 3}}, new double[]{5, 10});
        Assertions.assertArrayEquals(new double[]{1, 3}, solution, DELTA);
    }

    @Test
    public void testSolveOverdeterminedLeastSquares() {
        // Four noisy-free points on y = 2x + 1 with a design matrix [1, x].
        final double[][] design = {{1, 0}, {1, 1}, {1, 2}, {1, 3}};
        final double[] y = {1, 3, 5, 7};
        Assertions.assertArrayEquals(new double[]{1, 2},
                MatrixOps.solve(design, y), DELTA);
    }

    @Test
    public void testInverse() {
        final double[][] inverse = MatrixOps.inverse(new double[][]{{4, 7}, {2, 6}});
        Assertions.assertArrayEquals(new double[]{0.6, -0.7}, inverse[0], DELTA);
        Assertions.assertArrayEquals(new double[]{-0.2, 0.4}, inverse[1], DELTA);
    }

    @Test
    public void testMahalanobis() {
        // Identity precision: the euclidean distance.
        final double[][] identity = {{1, 0}, {0, 1}};
        Assertions.assertEquals(5d, MatrixOps.mahalanobis(
                new double[]{3, 4}, new double[]{0, 0}, identity), DELTA);
        // Unit-variance diagonal scaled: variance 4 in dim 1 halves its contribution.
        final double[][] precision = MatrixOps.inverse(new double[][]{{4, 0}, {0, 1}});
        Assertions.assertEquals(Math.sqrt(1 + 16), MatrixOps.mahalanobis(
                new double[]{2, 4}, new double[]{0, 0}, precision), DELTA);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.mahalanobis(new double[]{1, 2}, new double[]{0, 0},
                        new double[][]{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}));
    }

    @Test
    public void testPolyfit() {
        // Exact line fit.
        final double[] line = MatrixOps.polyfit(
                new double[]{0, 1, 2, 3}, new double[]{1, 3, 5, 7}, 1);
        Assertions.assertArrayEquals(new double[]{1, 2}, line, DELTA);
        // Exact quadratic fit: y = 2 - x + 3x^2.
        final double[] quad = MatrixOps.polyfit(
                new double[]{-1, 0, 1, 2}, new double[]{6, 2, 4, 12}, 2);
        Assertions.assertArrayEquals(new double[]{2, -1, 3}, quad, DELTA);
        // Degree too high for the number of points.
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.polyfit(new double[]{0, 1}, new double[]{1, 2}, 2));
    }

    @Test
    public void testSolveGram() {
        // Accumulate X^T X / X^T y for y = 1 + 2 x1 + 3 x2 over exact points.
        final double[][] xs = {{1, 0, 0}, {1, 1, 0}, {1, 0, 1}, {1, 1, 1}, {1, 2, 1}};
        final double[] ys = new double[xs.length];
        for (int i = 0; i < xs.length; i++) {
            ys[i] = 1 + 2 * xs[i][1] + 3 * xs[i][2];
        }
        final double[][] xtx = new double[3][3];
        final double[] xty = new double[3];
        for (int r = 0; r < xs.length; r++) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    xtx[i][j] += xs[r][i] * xs[r][j];
                }
                xty[i] += xs[r][i] * ys[r];
            }
        }
        Assertions.assertArrayEquals(new double[]{1, 2, 3},
                MatrixOps.solveGram(xtx, xty, 0d), DELTA);
        // Ridge shrinks the coefficients toward zero.
        final double[] ridge = MatrixOps.solveGram(xtx, xty, 10d);
        Assertions.assertTrue(Math.abs(ridge[1]) < 2 && Math.abs(ridge[2]) < 3);
        // A singular Gram matrix (duplicate feature) still yields a solution via SVD.
        final double[][] singular = {{2, 2}, {2, 2}};
        final double[] solution = MatrixOps.solveGram(singular, new double[]{2, 2}, 0d);
        Assertions.assertEquals(2d, 2 * solution[0] + 2 * solution[1], 1e-6);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.solveGram(xtx, new double[]{1, 2}, 0d));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.solveGram(xtx, xty, -1d));
    }

    @Test
    public void testToVector() {
        Assertions.assertNull(MatrixOps.toVector(null));
        Assertions.assertArrayEquals(new double[]{1, 2.5},
                MatrixOps.toVector(List.of(1L, 2.5d)), DELTA);
        Assertions.assertArrayEquals(new double[]{1, 2},
                MatrixOps.toVector(new float[]{1f, 2f}), DELTA);
        Assertions.assertArrayEquals(new double[]{1, 2},
                MatrixOps.toVector(new long[]{1L, 2L}), DELTA);
        Assertions.assertArrayEquals(new double[]{1, 2},
                MatrixOps.toVector(new Object[]{1, 2.0}), DELTA);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.toVector(List.of("a")));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.toVector("not a vector"));
        // A null element is not numeric.
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.toVector(Arrays.asList(1d, null)));
    }

    @Test
    public void testToMatrix() {
        Assertions.assertNull(MatrixOps.toMatrix(null));
        final double[][] fromLists = MatrixOps.toMatrix(List.of(List.of(1, 2), List.of(3L, 4d)));
        Assertions.assertArrayEquals(new double[]{1, 2}, fromLists[0], DELTA);
        Assertions.assertArrayEquals(new double[]{3, 4}, fromLists[1], DELTA);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.toMatrix(List.of(List.of(1, 2), List.of(3))));
        // Flat row-major split.
        final double[][] flat = MatrixOps.toMatrix(List.of(1, 2, 3, 4, 5, 6), 3);
        Assertions.assertArrayEquals(new double[]{1, 2, 3}, flat[0], DELTA);
        Assertions.assertArrayEquals(new double[]{4, 5, 6}, flat[1], DELTA);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                MatrixOps.toMatrix(List.of(1, 2, 3), 2));
    }
}
