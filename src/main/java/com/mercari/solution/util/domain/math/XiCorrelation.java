package com.mercari.solution.util.domain.math;

import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.commons.math3.stat.ranking.NaturalRanking;
import org.apache.commons.math3.stat.ranking.TiesStrategy;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;


public class XiCorrelation {

    /**
     * Calculate Chatterjee Xi correlation by Apache Commons Math
     * Ref (arXiv:1909.10140v4)
     * @param xArray Array of X (double[])
     * @param yArray Array of Y (double[])
     * @return XiCorr
     */
    public static double calculate(final double[] xArray, final double[] yArray) {
        if (xArray == null || yArray == null || xArray.length != yArray.length) {
            throw new IllegalArgumentException("xArray and yArray length must be same");
        }
        final int n = xArray.length;
        if (n < 2) {
            throw new IllegalArgumentException("Array size must be over 2");
        }

        final Integer[] indices = new Integer[n];
        for (int i = 0; i < n; i++) indices[i] = i;

        shuffle(indices, new Random());

        Arrays.sort(indices, Comparator.comparingDouble(i -> xArray[i]));

        final double[] yOrdered = new double[n];
        final double[] yNegated = new double[n];
        for (int i = 0; i < n; i++) {
            double val = yArray[indices[i]];
            yOrdered[i] = val;
            yNegated[i] = -val;
        }

        final NaturalRanking ranking = new NaturalRanking(NaNStrategy.FAILED, TiesStrategy.MAXIMUM);

        final double[] r = ranking.rank(yOrdered);
        final double[] l = ranking.rank(yNegated);

        double sumDiff = 0;
        for (int i = 0; i < n - 1; i++) {
            sumDiff += Math.abs(r[i + 1] - r[i]);
        }

        double sumL = 0;
        for (int i = 0; i < n; i++) {
            sumL += l[i] * (n - l[i]);
        }

        if (sumL == 0) return 0.0;

        return 1.0 - (n * sumDiff) / (2.0 * sumL);
    }

    // Helper: shuffle array (Fisher-Yates)
    private static void shuffle(final Integer[] array, final Random rand) {
        for (int i = array.length - 1; i > 0; i--) {
            final int index = rand.nextInt(i + 1);
            final int temp = array[index];
            array[index] = array[i];
            array[i] = temp;
        }
    }

    // For checking
    public static void main(String[] args) {
        // Test data: y = x^2
        double[] x = new double[21];
        double[] y = new double[21];
        for (int i = 0; i < 21; i++) {
            double val = i - 10;
            x[i] = val;
            y[i] = val * val;
        }

        double result = calculate(x, y);
        System.out.println("Result (y=x^2): " + result);
    }
}