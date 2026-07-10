package com.mercari.solution.util.domain.ml;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

/**
 * Logistic regression (IRLS / block Newton on the shared ojalgo core).
 * Correctness is pinned self-containedly through the MLE's first-order
 * optimality condition — at the fitted coefficients the penalized gradient
 * {@code X^T (y − p) − l2·β} must vanish — rather than external reference
 * values.
 */
public class LogisticRegressionTest {

    private static final double L2 = 0.1;
    private static final int MAX_ITERATION = 100;
    private static final double TOLERANCE = 1e-10;

    /** x with an intercept column; labels mostly increase with x (not separable). */
    private static double[][] binaryX() {
        final double[] xs = {-2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2};
        final double[][] X = new double[xs.length][2];
        for (int i = 0; i < xs.length; i++) {
            X[i][0] = xs[i];
            X[i][1] = 1;
        }
        return X;
    }

    private static double[][] binaryY() {
        final double[] ys = {0, 0, 1, 0, 0, 1, 1, 0, 1};
        final double[][] Y = new double[ys.length][1];
        for (int i = 0; i < ys.length; i++) {
            Y[i][0] = ys[i];
        }
        return Y;
    }

    @Test
    public void testBinaryGradientOptimality() {
        final double[][] X = binaryX();
        final double[][] Y = binaryY();
        final double[][] beta = LinearModelUtil.logisticRegressionBinary(X, Y, L2, MAX_ITERATION, TOLERANCE);
        Assertions.assertEquals(2, beta.length);
        Assertions.assertEquals(1, beta[0].length);

        for (int a = 0; a < X[0].length; a++) {
            double gradient = -L2 * beta[a][0];
            for (int i = 0; i < X.length; i++) {
                double score = 0;
                for (int b = 0; b < X[0].length; b++) {
                    score += X[i][b] * beta[b][0];
                }
                final double p = 1 / (1 + Math.exp(-score));
                gradient += X[i][a] * (Y[i][0] - p);
            }
            Assertions.assertEquals(0d, gradient, 1e-8,
                    "penalized gradient must vanish at the MLE (feature " + a + ")");
        }
        // Labels increase with x: the slope must be positive.
        Assertions.assertTrue(beta[0][0] > 0);
    }

    @Test
    public void testBinaryModelProbabilities() {
        final LinearModelUtil.LogisticModel model = LinearModelUtil.logisticBinaryModel(
                binaryX(), binaryY(), L2, MAX_ITERATION, TOLERANCE);
        Assertions.assertEquals(2, model.getNumClasses());

        final List<Double> low = model.inference(new double[]{-2, 1});
        final List<Double> high = model.inference(new double[]{2, 1});
        // [P(y=1), P(y=0)], summing to 1; a larger x means a larger P(y=1).
        Assertions.assertEquals(2, low.size());
        Assertions.assertEquals(1d, low.get(0) + low.get(1), 1e-12);
        Assertions.assertEquals(1d, high.get(0) + high.get(1), 1e-12);
        Assertions.assertTrue(high.get(0) > low.get(0));
    }

    /** Three 2D clusters (+ intercept), seeded — overlapping but separable in tendency. */
    private static double[][] multiX(final Random random, final int perClass) {
        final double[][] centers = {{2, 0}, {-2, 0}, {0, 2}};
        final double[][] X = new double[centers.length * perClass][3];
        for (int c = 0; c < centers.length; c++) {
            for (int i = 0; i < perClass; i++) {
                final int row = c * perClass + i;
                X[row][0] = centers[c][0] + random.nextGaussian();
                X[row][1] = centers[c][1] + random.nextGaussian();
                X[row][2] = 1;
            }
        }
        return X;
    }

    private static double[][] multiY(final int numClasses, final int perClass) {
        final double[][] Y = new double[numClasses * perClass][numClasses];
        for (int c = 0; c < numClasses; c++) {
            for (int i = 0; i < perClass; i++) {
                Y[c * perClass + i][c] = 1;
            }
        }
        return Y;
    }

    @Test
    public void testMultiGradientOptimalityAndPredictions() {
        final int perClass = 30;
        final double[][] X = multiX(new Random(1L), perClass);
        final double[][] Y = multiY(3, perClass);
        final double[][] beta = LinearModelUtil.logisticRegressionMulti(X, Y, L2, MAX_ITERATION, TOLERANCE);
        Assertions.assertEquals(3, beta.length);      // features (2 + intercept)
        Assertions.assertEquals(2, beta[0].length);   // K - 1 columns

        final LinearModelUtil.LogisticModel model = LinearModelUtil.LogisticModel.of(beta);
        // Penalized gradient vanishes for every non-reference class block.
        for (int k = 0; k < 2; k++) {
            for (int a = 0; a < X[0].length; a++) {
                double gradient = -L2 * beta[a][k];
                for (int i = 0; i < X.length; i++) {
                    gradient += X[i][a] * (Y[i][k] - model.inference(X[i]).get(k));
                }
                Assertions.assertEquals(0d, gradient, 1e-6,
                        "class " + k + " feature " + a);
            }
        }

        // Probabilities sum to 1 and the class centers classify to their own class.
        final double[][] centers = {{2, 0, 1}, {-2, 0, 1}, {0, 2, 1}};
        for (int c = 0; c < centers.length; c++) {
            final List<Double> probabilities = model.inference(centers[c]);
            Assertions.assertEquals(3, probabilities.size());
            double sum = 0;
            int argMax = 0;
            for (int k = 0; k < probabilities.size(); k++) {
                sum += probabilities.get(k);
                if (probabilities.get(k) > probabilities.get(argMax)) {
                    argMax = k;
                }
            }
            Assertions.assertEquals(1d, sum, 1e-12);
            Assertions.assertEquals(c, argMax, "center of class " + c);
        }
    }

    @Test
    public void testMultiWithTwoClassesMatchesBinary() {
        final double[][] X = binaryX();
        final double[][] Y = binaryY();
        final double[][] oneHot = new double[Y.length][2];
        for (int i = 0; i < Y.length; i++) {
            oneHot[i][0] = Y[i][0];
            oneHot[i][1] = 1 - Y[i][0];
        }
        final double[][] binary = LinearModelUtil.logisticRegressionBinary(X, Y, L2, MAX_ITERATION, TOLERANCE);
        final double[][] multi = LinearModelUtil.logisticRegressionMulti(X, oneHot, L2, MAX_ITERATION, TOLERANCE);
        for (int a = 0; a < X[0].length; a++) {
            Assertions.assertEquals(binary[a][0], multi[a][0], 1e-8);
        }
    }

    @Test
    public void testRegularizationShrinksAndSeparableDataStaysFinite() {
        // Perfectly separable: the unpenalized MLE diverges; l2 keeps it finite.
        final double[][] X = {{-2, 1}, {-1, 1}, {1, 1}, {2, 1}};
        final double[][] Y = {{0}, {0}, {1}, {1}};
        final double[][] weak = LinearModelUtil.logisticRegressionBinary(X, Y, 0.01, MAX_ITERATION, TOLERANCE);
        final double[][] strong = LinearModelUtil.logisticRegressionBinary(X, Y, 10d, MAX_ITERATION, TOLERANCE);
        Assertions.assertTrue(Double.isFinite(weak[0][0]) && Double.isFinite(weak[1][0]));
        Assertions.assertTrue(Math.abs(strong[0][0]) < Math.abs(weak[0][0]));
    }

    @Test
    public void testValidation() {
        final double[][] X = {{1, 1}, {2, 1}};
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                LinearModelUtil.logisticRegressionBinary(X, new double[][]{{0.5}, {1}}, L2, 10, 1e-6));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                LinearModelUtil.logisticRegressionBinary(X, new double[][]{{0}, {1}}, -1, 10, 1e-6));
        // Not one-hot: a row with two 1s.
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                LinearModelUtil.logisticRegressionMulti(X, new double[][]{{1, 1}, {0, 1}}, L2, 10, 1e-6));
        // K must be >= 2.
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                LinearModelUtil.logisticRegressionMulti(X, new double[][]{{1}, {1}}, L2, 10, 1e-6));
    }
}
