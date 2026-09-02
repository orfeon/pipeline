package com.mercari.solution.util.domain.attribution.tree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class CausalAdjustmentTest {

    @Test
    public void testLinearFit() {
        final double[] x = { 1, 2, 3, 4, 5 };
        final double[] y = { 3, 5, 7, 9, 11 };   // y = 1 + 2x
        final CausalAdjustment.Fit fit = CausalAdjustment.fit(x, y, 1, false);
        Assertions.assertEquals(1.0, fit.beta()[0], 1e-9);
        Assertions.assertEquals(2.0, fit.beta()[1], 1e-9);
        Assertions.assertEquals(1.0, fit.r2(), 1e-9);
        Assertions.assertEquals(13.0, fit.predict(6), 1e-9);
    }

    @Test
    public void testQuadraticFit() {
        final double[] x = { -2, -1, 0, 1, 2, 3 };
        final double[] y = new double[x.length];
        for(int i = 0; i < x.length; i++) {
            y[i] = 2 - x[i] + 0.5 * x[i] * x[i];
        }
        final CausalAdjustment.Fit fit = CausalAdjustment.fit(x, y, 2, false);
        Assertions.assertEquals(2.0, fit.beta()[0], 1e-9);
        Assertions.assertEquals(-1.0, fit.beta()[1], 1e-9);
        Assertions.assertEquals(0.5, fit.beta()[2], 1e-9);
    }

    @Test
    public void testRobustFitResistsOutlier() {
        final Random random = new Random(1);
        final int n = 60;
        final double[] x = new double[n];
        final double[] y = new double[n];
        for(int i = 0; i < n; i++) {
            x[i] = i / 10.0;
            y[i] = 1 + 2 * x[i] + 0.1 * random.nextGaussian();
        }
        y[5] += 200;  // gross outlier
        final CausalAdjustment.Fit ols = CausalAdjustment.fit(x, y, 1, false);
        final CausalAdjustment.Fit huber = CausalAdjustment.fit(x, y, 1, true);
        Assertions.assertTrue(Math.abs(huber.beta()[1] - 2.0) < Math.abs(ols.beta()[1] - 2.0));
        Assertions.assertEquals(2.0, huber.beta()[1], 0.1);
    }

    @Test
    public void testSlopeStabilityTest() {
        final Random random = new Random(3);
        final int n = 80;
        final double[] x0 = new double[n];
        final double[] y0 = new double[n];
        final double[] x1 = new double[n];
        final double[] y1same = new double[n];
        final double[] y1diff = new double[n];
        for(int i = 0; i < n; i++) {
            x0[i] = random.nextGaussian();
            y0[i] = 1 + 2 * x0[i] + 0.3 * random.nextGaussian();
            x1[i] = random.nextGaussian();
            y1same[i] = 4 + 2 * x1[i] + 0.3 * random.nextGaussian();   // intercept only
            y1diff[i] = 4 + 0.5 * x1[i] + 0.3 * random.nextGaussian(); // slope changed
        }
        Assertions.assertTrue(CausalAdjustment.slopeStabilityPValue(x0, y0, x1, y1same, 1) > 0.05);
        Assertions.assertTrue(CausalAdjustment.slopeStabilityPValue(x0, y0, x1, y1diff, 1) < 0.001);
    }

    @Test
    public void testRegularizedIncompleteBeta() {
        Assertions.assertEquals(0.5, CausalAdjustment.regularizedIncompleteBeta(0.5, 1, 1), 1e-12);
        Assertions.assertEquals(0.25, CausalAdjustment.regularizedIncompleteBeta(0.25, 1, 1), 1e-12);
        // I_x(2, 3) = 6x² − 8x³ + 3x⁴
        final double x = 0.3;
        Assertions.assertEquals(6 * x * x - 8 * x * x * x + 3 * x * x * x * x,
                CausalAdjustment.regularizedIncompleteBeta(x, 2, 3), 1e-10);
        // symmetry I_x(a,b) = 1 − I_{1−x}(b,a)
        Assertions.assertEquals(1 - CausalAdjustment.regularizedIncompleteBeta(0.8, 5, 2),
                CausalAdjustment.regularizedIncompleteBeta(0.2, 2, 5), 1e-10);
    }

    @Test
    public void testAdjustAdditivity() {
        final MetricTreeSpec.Edge edge = new MetricTreeSpec.Edge("x", "n", MetricTreeSpec.Model.linear, false,
                MetricTreeSpec.Estimator.simplified, null);
        final MetricTreeSpec.Causal causal = new MetricTreeSpec.Causal("d", 0.05, 3);
        final double[] x0 = { 1, 2, 3, 4, 5 };
        final double[] y0 = { 10, 8, 6, 4, 2 };
        final double[] x1 = { 3, 4, 5, 6, 7 };
        final double[] y1 = { 7, 5, 3, 1, -1 };
        final double n0 = 30;
        final double n1 = 15;
        final double deltaY = n1 * 5 - n0 * 3;
        final CausalAdjustment.Result result = CausalAdjustment.adjust(edge, causal, x0, y0, x1, y1, n0, n1, 3, 5, deltaY);
        Assertions.assertEquals("simplified", result.estimator());
        Assertions.assertEquals(deltaY, result.volumeContribution() + result.rateContribution(), 1e-9);
        // f̂0(x) = 12 − 2x exactly; Σ f̂0(x1_j) = 60 − 2·25 = 10 → (15 − 10) · 5 = 25
        Assertions.assertEquals(25.0, result.volumeContribution(), 1e-9);
    }
}
