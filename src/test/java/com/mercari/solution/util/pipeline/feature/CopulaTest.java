package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.domain.math.MatrixOps;
import com.mercari.solution.util.pipeline.glm.StatMath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class CopulaTest {

    /** {@code n} draws of a Gaussian vector with correlation {@code r} (Cholesky of r times independent normals). */
    private static double[][] draws(final double[][] r, final int n, final long seed) {
        final double[][] l = Copula.cholesky(r);
        final Random random = new Random(seed);
        final double[][] x = new double[n][r.length];
        for (int t = 0; t < n; t++) {
            final double[] e = new double[r.length];
            for (int i = 0; i < e.length; i++) e[i] = random.nextGaussian();
            for (int i = 0; i < e.length; i++) for (int k = 0; k <= i; k++) x[t][i] += l[i][k] * e[k];
        }
        return x;
    }

    private static Svd.Moments moments(final double[][] x) {
        final Svd.Moments m = new Svd.Moments();
        for (final double[] v : x) m.add(v);
        return m;
    }

    private static Double[] boxed(final double... v) {
        final Double[] b = new Double[v.length];
        for (int i = 0; i < v.length; i++) b[i] = Double.isNaN(v[i]) ? null : v[i];
        return b;
    }

    private static final double[][] R = {{1, 0.6, 0.3}, {0.6, 1, -0.2}, {0.3, -0.2, 1}};

    @Test
    public void testRecoversTheCorrelation() {
        final Copula c = Copula.fit(moments(draws(R, 20_000, 1)), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertFalse(c.isEmpty());
        for (int i = 0; i < 3; i++) for (int j = 0; j < 3; j++) Assertions.assertEquals(R[i][j], c.correlation[i][j], 0.03, "R[" + i + "][" + j + "]");
        Assertions.assertEquals(0, c.floored);
    }

    /** conditional = the OLS residual of the standardised score on the standardised conditioning scores, over its standard deviation. */
    @Test
    public void testConditionalIsTheStandardisedRegressionResidual() {
        final double[][] x = draws(R, 500, 2);
        final Copula c = Copula.fit(moments(x), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        // β = R_SS⁻¹ r_Sj for j = 0, S = {1, 2}
        final double[][] rss = {{c.correlation[1][1], c.correlation[1][2]}, {c.correlation[2][1], c.correlation[2][2]}};
        final double[] beta = MatrixOps.solve(rss, new double[]{c.correlation[1][0], c.correlation[2][0]});
        final double s = Math.sqrt(1 - beta[0] * c.correlation[1][0] - beta[1] * c.correlation[2][0]);
        for (int t = 0; t < 20; t++) {
            final double[] z = new double[3];
            for (int i = 0; i < 3; i++) z[i] = (x[t][i] - c.mean[i]) / c.sd[i];
            final double expected = (z[0] - beta[0] * z[1] - beta[1] * z[2]) / s;
            Assertions.assertEquals(expected, c.conditional(boxed(x[t]), 0, new int[]{1, 2}), 1e-9);
            Assertions.assertEquals(com.mercari.solution.util.domain.math.NormalDistribution.cdf(expected), c.conditionalProb(boxed(x[t]), 0, new int[]{1, 2}), 1e-12);
        }
        // a single conditioning input: β = ρ, s = sqrt(1 − ρ²)
        final double rho = c.correlation[0][1];
        final double[] row = x[0];
        final double z0 = (row[0] - c.mean[0]) / c.sd[0], z1 = (row[1] - c.mean[1]) / c.sd[1];
        Assertions.assertEquals((z0 - rho * z1) / Math.sqrt(1 - rho * rho), c.conditional(boxed(row), 0, new int[]{1}), 1e-9);
    }

    /** A missing score is marginalised out: the readouts equal those that never listed it. */
    @Test
    public void testMissingScoresAreMarginalisedOut() {
        final Copula c = Copula.fit(moments(draws(R, 500, 3)), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        final Double[] full = boxed(0.4, -1.1, 0.7);
        final Double[] missing = boxed(0.4, Double.NaN, 0.7);
        Assertions.assertEquals(c.conditional(full, 0, new int[]{2}), c.conditional(missing, 0, new int[]{1, 2}), 1e-12);
        // nothing of S observed: the standardised score itself
        Assertions.assertEquals((0.4 - c.mean[0]) / c.sd[0], c.conditional(boxed(0.4, Double.NaN, Double.NaN), 0, new int[]{1, 2}), 1e-12);
        // the target missing: null
        Assertions.assertNull(c.conditional(boxed(Double.NaN, 1, 1), 0, new int[]{1, 2}));
        // joint readouts over the observed pair = the two-input closed forms with that pair's correlation
        final double rho = c.correlation[0][2];
        final double x = (0.4 - c.mean[0]) / c.sd[0], y = (0.7 - c.mean[2]) / c.sd[2];
        final double q = (x * x - 2 * rho * x * y + y * y) / (1 - rho * rho);
        Assertions.assertEquals(1 - Math.exp(-q / 2), c.jointRank(missing), 1e-9);
        Assertions.assertEquals(-0.5 * Math.log(1 - rho * rho) - 0.5 * (q - x * x - y * y), c.logDensity(missing), 1e-9);
        Assertions.assertNull(c.jointRank(boxed(Double.NaN, Double.NaN, Double.NaN)));
        Assertions.assertNull(c.logDensity(new Double[3]));
    }

    @Test
    public void testJointReadoutsOverEveryInput() {
        final Copula c = Copula.fit(moments(draws(R, 500, 4)), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        final double[] raw = {1.2, 0.3, -0.8};
        final double[] z = new double[3];
        for (int i = 0; i < 3; i++) z[i] = (raw[i] - c.mean[i]) / c.sd[i];
        final double[][] precision = MatrixOps.inverse(c.correlation);
        double form = 0, squares = 0;
        for (int i = 0; i < 3; i++) {
            squares += z[i] * z[i];
            for (int j = 0; j < 3; j++) form += z[i] * precision[i][j] * z[j];
        }
        final double[][] l = Copula.cholesky(c.correlation);
        final double logDet = 2 * (Math.log(l[0][0]) + Math.log(l[1][1]) + Math.log(l[2][2]));
        Assertions.assertEquals(1 - StatMath.chiSquareUpperTail(form, 3), c.jointRank(boxed(raw)), 1e-9);
        // a row at the fitted centre: the probability is tiny but keeps its digits (the χ²(3) CDF near 0 ≈ (x/2)^1.5 / Γ(2.5))
        final double[] centre = new double[3];
        for (int i = 0; i < 3; i++) centre[i] = c.mean[i] + 1e-6 * c.sd[i];
        final Double near = c.jointRank(boxed(centre));
        Assertions.assertTrue(near > 0 && near < 1e-15, "jointRank at the centre: " + near);
        Assertions.assertEquals(StatMath.chiSquareCdf(c.quadratic(boxed(centre))[0], 3), near, 1e-12 * near);
        Assertions.assertEquals(-0.5 * logDet - 0.5 * (form - squares), c.logDensity(boxed(raw)), 1e-9);
        // independent inputs: log c = 0 everywhere
        final Copula independent = Copula.fit(moments(draws(new double[][]{{1, 0}, {0, 1}}, 200, 5)), "0.999", Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertEquals(0, independent.logDensity(boxed(2, -2)), 1e-3);
    }

    /** OAS: close to 1 for unrelated inputs and few rows, small for strongly related inputs and many rows; a fixed value applies as declared. */
    @Test
    public void testShrinkage() {
        final Copula unrelated = Copula.fit(moments(draws(new double[][]{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}, 30, 6)), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertTrue(unrelated.shrinkage > 0.5, "unrelated: " + unrelated.shrinkage);
        final Copula related = Copula.fit(moments(draws(R, 5_000, 7)), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertTrue(related.shrinkage < 0.01, "related: " + related.shrinkage);
        Assertions.assertEquals("oas", related.shrinkMethod);
        final Svd.Moments m = moments(draws(R, 500, 8));
        final Copula none = Copula.fit(m, Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        final Copula fixed = Copula.fit(m, "0.25", Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertEquals("fixed", fixed.shrinkMethod);
        Assertions.assertEquals(0.75 * none.correlation[0][1], fixed.correlation[0][1], 1e-12);
        Assertions.assertEquals(1d, fixed.correlation[1][1]);
    }

    /** Two identical inputs make R singular: the floor keeps it positive definite and every readout finite. */
    @Test
    public void testEigenvalueFloor() {
        final double[][] x = draws(new double[][]{{1, 0}, {0, 1}}, 200, 9);
        final double[][] three = new double[x.length][];
        for (int t = 0; t < x.length; t++) three[t] = new double[]{x[t][0], x[t][0], x[t][1]};
        final Copula c = Copula.fit(moments(three), Copula.NONE, 1e-4, false);
        Assertions.assertEquals(1, c.floored);
        Assertions.assertTrue(c.correlation[0][1] < 1);
        Assertions.assertTrue(Double.isFinite(c.conditional(boxed(0.5, 0.4, 0.1), 0, new int[]{1, 2})));
        Assertions.assertTrue(Double.isFinite(c.logDensity(boxed(0.5, 0.4, 0.1))));
    }

    @Test
    public void testDegenerateFitsAreEmpty() {
        final Svd.Moments one = new Svd.Moments();
        one.add(new double[]{1, 2});
        final Copula empty = Copula.fit(one, Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertTrue(empty.isEmpty());
        Assertions.assertNull(empty.conditional(boxed(1, 2), 0, new int[]{1}));
        Assertions.assertNull(empty.jointRank(boxed(1, 2)));
        // a constant score (one mass point) has no correlation
        final Svd.Moments constant = new Svd.Moments();
        for (int t = 0; t < 10; t++) constant.add(new double[]{t, 3});
        Assertions.assertTrue(Copula.fit(constant, Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false).isEmpty());
    }

    @Test
    public void testArtifactRoundTrip() {
        final Copula c = Copula.fit(moments(draws(R, 300, 10)), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        final Copula back = Copula.fromJson(c.toJson());
        Assertions.assertEquals(c.n, back.n);
        Assertions.assertEquals(c.shrinkage, back.shrinkage);
        Assertions.assertEquals(c.shrinkMethod, back.shrinkMethod);
        final Double[] row = boxed(0.3, Double.NaN, -0.4);
        Assertions.assertEquals(c.conditional(row, 2, new int[]{0, 1}), back.conditional(row, 2, new int[]{0, 1}));
        Assertions.assertEquals(c.logDensity(row), back.logDensity(row));
        Assertions.assertTrue(Copula.fromJson(Copula.empty(3, 1).toJson()).isEmpty());
    }

    /** A model fitted on merged per-block moments equals the one fitted on all the rows at once (what a forward change point reads). */
    @Test
    public void testBlockSeriesMergeEqualsOneFit() {
        final double[][] x = draws(R, 300, 11);
        final Svd.Moments a = new Svd.Moments(), b = new Svd.Moments();
        for (int t = 0; t < x.length; t++) (t < 120 ? a : b).add(x[t]);
        final Svd.Moments merged = Svd.SUMMARY.create();
        Svd.SUMMARY.merge(merged, a);
        Svd.SUMMARY.merge(merged, b);
        final Copula once = Copula.fit(moments(x), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        final Copula parts = Copula.fit(merged, Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        for (int i = 0; i < 3; i++) for (int j = 0; j < 3; j++) Assertions.assertEquals(once.correlation[i][j], parts.correlation[i][j], 1e-9);
        Assertions.assertEquals(once.shrinkage, parts.shrinkage, 1e-12);
    }
}
