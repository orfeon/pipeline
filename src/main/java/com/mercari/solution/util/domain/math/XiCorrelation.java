package com.mercari.solution.util.domain.math;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;


public class XiCorrelation {

    /**
     * Calculate Chatterjee Xi correlation
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

        final double[] r = rankMaximum(yOrdered);
        final double[] l = rankMaximum(yNegated);

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

    /**
     * 1-based natural ranking where every member of a tie group receives the
     * group's <em>maximum</em> rank (commons-math's
     * {@code NaturalRanking(NaNStrategy.FAILED, TiesStrategy.MAXIMUM)} —
     * exactly what the xi statistic's {@code r_i = #{j : y_j <= y_i}} needs).
     * NaN values are rejected.
     */
    static double[] rankMaximum(final double[] values) {
        final int n = values.length;
        final Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            if (Double.isNaN(values[i])) {
                throw new IllegalArgumentException("NaN is not allowed at position " + i);
            }
            order[i] = i;
        }
        Arrays.sort(order, Comparator.comparingDouble(i -> values[i]));

        final double[] ranks = new double[n];
        int i = 0;
        while (i < n) {
            int j = i;
            while (j + 1 < n && values[order[j + 1]] == values[order[i]]) {
                j++;
            }
            for (int k = i; k <= j; k++) {
                ranks[order[k]] = j + 1;
            }
            i = j + 1;
        }
        return ranks;
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
