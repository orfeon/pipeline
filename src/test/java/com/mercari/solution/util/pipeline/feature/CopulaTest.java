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

    /**
     * expected = the fitted value of that regression in the score's own units: score − expected = sd · s · conditional.
     * It does not read its own input (defined with it missing), and a missing conditioning score is marginalised out.
     */
    @Test
    public void testExpectedIsTheRegressionFittedValue() {
        final double[][] x = draws(R, 500, 4);
        final Copula c = Copula.fit(moments(x), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        final double[][] rss = {{c.correlation[1][1], c.correlation[1][2]}, {c.correlation[2][1], c.correlation[2][2]}};
        final double[] beta = MatrixOps.solve(rss, new double[]{c.correlation[1][0], c.correlation[2][0]});
        final double s = Math.sqrt(1 - beta[0] * c.correlation[1][0] - beta[1] * c.correlation[2][0]);
        for (int t = 0; t < 20; t++) {
            final double z1 = (x[t][1] - c.mean[1]) / c.sd[1], z2 = (x[t][2] - c.mean[2]) / c.sd[2];
            final double fitted = c.mean[0] + c.sd[0] * (beta[0] * z1 + beta[1] * z2);
            final Double[] row = boxed(x[t]);
            Assertions.assertEquals(fitted, c.expected(row, 0, new int[]{1, 2}), 1e-9);
            Assertions.assertEquals(x[t][0] - fitted, c.sd[0] * s * c.conditional(row, 0, new int[]{1, 2}), 1e-9);
            // its own score is not read
            Assertions.assertEquals(fitted, c.expected(boxed(Double.NaN, x[t][1], x[t][2]), 0, new int[]{1, 2}), 1e-12);
        }
        // a missing conditioning score: the regression on the observed one alone
        final double rho = c.correlation[0][2];
        Assertions.assertEquals(c.mean[0] + c.sd[0] * rho * (0.7 - c.mean[2]) / c.sd[2], c.expected(boxed(0.4, Double.NaN, 0.7), 0, new int[]{1, 2}), 1e-12);
        Assertions.assertEquals(c.expected(boxed(0.4, -1.1, 0.7), 0, new int[]{2}), c.expected(boxed(0.4, Double.NaN, 0.7), 0, new int[]{1, 2}), 1e-12);
        // nothing of S observed: null (the mean score would read like an average row), whether or not j is; nothing fitted: null
        Assertions.assertNull(c.expected(new Double[3], 0, new int[]{1, 2}));
        Assertions.assertNull(c.expected(boxed(0.4, Double.NaN, Double.NaN), 0, new int[]{1, 2}));
        Assertions.assertNotNull(c.expected(boxed(Double.NaN, Double.NaN, 0.7), 0, new int[]{1, 2}));
        Assertions.assertNull(Copula.fit(new Svd.Moments(), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false).expected(boxed(0, 1, 2), 0, new int[]{1, 2}));
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

    /** {@code n} draws of a multivariate t with correlation {@code r} and {@code df} degrees of freedom (a Gaussian vector over a chi-square scale). */
    private static double[][] tDraws(final double[][] r, final double df, final int n, final long seed) {
        // the loop below draws ceil(df / 2) exponentials: an odd df would silently be a chi-square(df + 1)
        if (df <= 0 || df % 2 != 0) throw new IllegalArgumentException("tDraws needs an even df: " + df);
        final double[][] x = draws(r, n, seed);
        final Random random = new Random(seed + 1);
        for (final double[] row : x) {
            // chi-square(df) as a gamma(df / 2) draw: the sum of exponentials for an integer df / 2
            double chi2 = 0;
            for (int k = 0; k < df / 2; k++) chi2 += -2 * Math.log(1 - random.nextDouble());
            final double scale = Math.sqrt(df / chi2);
            for (int i = 0; i < row.length; i++) row[i] *= scale;
        }
        return x;
    }

    /** Sample mean, variance and kurtosis (E[(x − m)⁴] / var²) of the non-null values. */
    private static double[] momentsOf(final java.util.List<Double> values) {
        double n = 0, s = 0;
        for (final Double v : values) if (v != null) { n++; s += v; }
        final double mean = s / n;
        double v2 = 0, v4 = 0;
        for (final Double v : values) if (v != null) { final double d = v - mean; v2 += d * d; v4 += d * d * d * d; }
        v2 /= n;
        v4 /= n;
        return new double[]{mean, v2, v4 / (v2 * v2), n};
    }

    /**
     * Rows of a t(4) copula with correlation R, scored by rank: under {@code family: t, df: 4} the conditional readouts are
     * N(0, 1) (kurtosis 3) and jointRank is uniform — under the Gaussian family on the same rows the conditionals are
     * heavy-tailed and jointRank piles up near 1. conditionalProb is the probability of the conditional position under
     * either family, expected is the position a conditional of 0 sits at, and a huge df is the Gaussian.
     */
    @Test
    public void testTFamilyReadouts() {
        final int n = 20_000;
        final double[][] z = normalScores(tDraws(R, 4, n, 31));
        final Svd.Moments m = moments(z);
        final Copula t = Copula.fit(m, Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, Copula.T, 4, false);
        final Copula gaussian = Copula.fit(m, Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertTrue(t.isT());
        Assertions.assertEquals(4d, t.df);
        Assertions.assertArrayEquals(gaussian.correlation[0], t.correlation[0], 0d, "the same R: the normal scores' correlation");
        Assertions.assertTrue(t.describe().startsWith("family t (df 4.0)"), t.describe());
        final java.util.List<Double> condT = new java.util.ArrayList<>(), condG = new java.util.ArrayList<>(), rankT = new java.util.ArrayList<>(), rankG = new java.util.ArrayList<>();
        final int[] given = {0, 1};
        for (final double[] row : z) {
            final Double[] scores = boxed(row);
            condT.add(t.conditional(scores, 2, given));
            condG.add(gaussian.conditional(scores, 2, given));
            rankT.add(t.jointRank(scores));
            rankG.add(gaussian.jointRank(scores));
            // the probability readout is the position's under its own family
            Assertions.assertEquals(com.mercari.solution.util.domain.math.NormalDistribution.cdf(t.conditional(scores, 2, given)), t.conditionalProb(scores, 2, given), 1e-12);
        }
        final double[] mt = momentsOf(condT), mg = momentsOf(condG);
        Assertions.assertEquals(0, mt[0], 0.03, "t conditional mean");
        Assertions.assertEquals(1, mt[1], 0.04, "t conditional variance");
        Assertions.assertEquals(3, mt[2], 0.15, "t conditional kurtosis (normal)");
        Assertions.assertTrue(mg[2] > 3.4, "the Gaussian family reads heavy-tailed conditionals on t rows: kurtosis " + mg[2]);
        final double[] rt = momentsOf(rankT);
        Assertions.assertEquals(0.5, rt[0], 0.01, "t jointRank is uniform: mean");
        Assertions.assertEquals(1 / 12d, rt[1], 0.004, "t jointRank is uniform: variance");
        final double tailT = rankT.stream().filter(v -> v > 0.99).count() / (double) n, tailG = rankG.stream().filter(v -> v > 0.99).count() / (double) n;
        Assertions.assertEquals(0.01, tailT, 0.004, "t jointRank beyond 0.99");
        Assertions.assertTrue(tailG > 0.02, "the Gaussian family over-reads joint extremes on t rows: " + tailG);
        // expected: the score at which the conditional reads 0 (it does not read input 2 itself)
        final Double[] row = boxed(1.1, -0.4, Double.NaN);
        final Double expected = t.expected(row, 2, given);
        Assertions.assertNotNull(expected);
        Assertions.assertEquals(0, t.conditional(boxed(1.1, -0.4, expected), 2, given), 1e-9);
        Assertions.assertNotEquals(gaussian.expected(row, 2, given), expected, "the t regression's position is mapped through the t, not the normal");
        // with none of given observed: the input's own standardised score, as under the Gaussian
        Assertions.assertEquals(gaussian.conditional(boxed(Double.NaN, Double.NaN, 0.7), 2, given), t.conditional(boxed(Double.NaN, Double.NaN, 0.7), 2, given), 1e-9);
        Assertions.assertNull(t.expected(boxed(Double.NaN, Double.NaN, 0.7), 2, given));
        // one observed input: the copula density is 1 under either family
        Assertions.assertEquals(0, t.logDensity(boxed(Double.NaN, 1.3, Double.NaN)), 1e-12);
        // a huge df is the Gaussian copula
        final Copula wide = Copula.fit(m, Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, Copula.T, 1e7, false);
        for (final double[] r : java.util.Arrays.copyOf(z, 200)) {
            final Double[] scores = boxed(r);
            Assertions.assertEquals(gaussian.conditional(scores, 2, given), wide.conditional(scores, 2, given), 1e-4);
            Assertions.assertEquals(gaussian.jointRank(scores), wide.jointRank(scores), 1e-4);
            Assertions.assertEquals(gaussian.logDensity(scores), wide.logDensity(scores), 1e-4);
            Assertions.assertEquals(gaussian.expected(scores, 2, given), wide.expected(scores, 2, given), 1e-4);
        }
    }

    /** The t copula density integrates to 1 over the unit square (so does the Gaussian's), and reads above 1 in the joint tails where the Gaussian's does not. */
    @Test
    public void testTDensityIntegratesToOne() {
        final double[][] r = {{1, 0.6}, {0.6, 1}};
        final Copula t = new Copula(2, 100, new double[]{0, 0}, new double[]{1, 1}, r, 0, Copula.NONE, 0, Copula.T, 3, null);
        final Copula g = new Copula(2, 100, new double[]{0, 0}, new double[]{1, 1}, r, 0, Copula.NONE, 0);
        final int grid = 400;
        double massT = 0, massG = 0;
        for (int i = 0; i < grid; i++) {
            for (int j = 0; j < grid; j++) {
                final Double[] scores = boxed(com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal((i + 0.5) / grid),
                        com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal((j + 0.5) / grid));
                massT += Math.exp(t.logDensity(scores));
                massG += Math.exp(g.logDensity(scores));
            }
        }
        Assertions.assertEquals(1, massT / (grid * grid), 3e-3);
        Assertions.assertEquals(1, massG / (grid * grid), 3e-3);
        // both inputs in the upper 0.5% together: more common under t than under the Gaussian; opposite corners: rarer than
        // independence under the Gaussian, while the t's common scale inflates every corner (its opposite-corner tail dependence
        // is 2 T_{ν+1}(−sqrt((ν+1)(1+ρ)/(1−ρ))) > 0), so there too it reads above the Gaussian
        final double far = com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal(0.995);
        Assertions.assertTrue(t.logDensity(boxed(far, far)) > g.logDensity(boxed(far, far)), "joint tail");
        Assertions.assertTrue(g.logDensity(boxed(far, -far)) < 0, "opposite tails under the Gaussian");
        Assertions.assertTrue(t.logDensity(boxed(far, -far)) > g.logDensity(boxed(far, -far)), "opposite tails under t");
        // jointRank at the centre keeps its digits under t as well
        final Double near = t.jointRank(boxed(1e-6, 1e-6));
        Assertions.assertTrue(near > 0 && near < 1e-10, "jointRank near the centre: " + near);
    }

    /** The artifact keeps family and df; one written before the t family (no family key) is Gaussian. */
    @Test
    public void testTArtifactRoundTrip() {
        final Copula c = Copula.fit(moments(draws(R, 300, 10)), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, Copula.T, 2.5, false);
        final com.google.gson.JsonObject json = c.toJson();
        Assertions.assertEquals("t", json.get("family").getAsString());
        Assertions.assertEquals(2.5, json.get("df").getAsDouble());
        final Copula back = Copula.fromJson(json);
        Assertions.assertTrue(back.isT());
        Assertions.assertEquals(2.5, back.df);
        final Double[] row = boxed(0.9, -1.1, 0.2);
        Assertions.assertEquals(c.conditional(row, 2, new int[]{0, 1}), back.conditional(row, 2, new int[]{0, 1}));
        Assertions.assertEquals(c.jointRank(row), back.jointRank(row));
        Assertions.assertEquals(c.logDensity(row), back.logDensity(row));
        final com.google.gson.JsonObject old = Copula.fit(moments(draws(R, 300, 10)), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false).toJson();
        old.remove("family");
        final Copula gaussian = Copula.fromJson(old);
        Assertions.assertFalse(gaussian.isT());
        Assertions.assertEquals(Copula.GAUSSIAN, gaussian.family);
        Assertions.assertTrue(Double.isNaN(gaussian.df));
        Assertions.assertFalse(json.has("diagnostics"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Copula.fit(moments(draws(R, 300, 10)), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, Copula.T, 0, false));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Copula.fit(moments(draws(R, 300, 10)), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, "clayton", 2, false));
        // the family is a readout parameter: the Gaussian artifact read as t is the t fit of the same moments, and back
        final Copula asT = gaussian.withFamily(Copula.T, 2.5);
        Assertions.assertTrue(asT.isT());
        Assertions.assertEquals(c.conditional(row, 2, new int[]{0, 1}), asT.conditional(row, 2, new int[]{0, 1}));
        Assertions.assertEquals(c.logDensity(row), asT.logDensity(row));
        Assertions.assertTrue(Double.isNaN(asT.withFamily(Copula.GAUSSIAN, 7).df));
        Assertions.assertThrows(IllegalArgumentException.class, () -> gaussian.withFamily(Copula.T, Double.NaN));
    }

    /** {@code T⁻¹(Φ(z))} and back keep their digits into the far tails (scores well past what a clipped marginal emits). */
    @Test
    public void testTCoordinatesRoundTrip() {
        for (final double df : new double[]{1, 2.5, 4, 30}) {
            for (final double z : new double[]{-20, -9, -8, -7.5, -3, -0.3, 1e-9, 1e-6, 0.3, 3, 7.5, 8, 9, 20}) {
                Assertions.assertEquals(z, Copula.toNormal(Copula.toT(z, df), df), 1e-12 * Math.max(1, Math.abs(z)), "df " + df + " z " + z);
            }
        }
    }

    /**
     * The bivariate t orthant: at thresholds 0 it is the normal's (1/4 + asin ρ / 2π, every elliptical copula), a huge df is
     * the Gaussian orthant, a reference value from a numerical integration (scipy), the tail-dependence coefficient, and the
     * df that matches a share.
     */
    @Test
    public void testUpperOrthantT() {
        Assertions.assertEquals(0.25 + Math.asin(0.5) / (2 * Math.PI), Copula.upperOrthantT(0, 0, 0.5, 3), 2e-5);
        Assertions.assertEquals(0.05588805946, Copula.upperOrthantT(1.0, 1.5, 0.5, 4), 5e-6);
        Assertions.assertEquals(Copula.upperOrthant(1.0, 1.5, 0.5), Copula.upperOrthantT(1.0, 1.5, 0.5, 1e6), 2e-5);
        Assertions.assertEquals(Copula.upperOrthant(1.0, 1.5, 0.5), Copula.upperOrthantT(1.0, 1.5, 0.5, Double.POSITIVE_INFINITY), 1e-6);
        // the t orthant beyond a far threshold exceeds the normal's (tail dependence), and grows as df falls
        final double q = com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal(0.95);
        final double t3 = Copula.upperOrthantT(StatMath.studentTQuantile(0.95, 3), StatMath.studentTQuantile(0.95, 3), 0.5, 3);
        final double t10 = Copula.upperOrthantT(StatMath.studentTQuantile(0.95, 10), StatMath.studentTQuantile(0.95, 10), 0.5, 10);
        Assertions.assertTrue(t3 > t10 && t10 > Copula.upperOrthant(q, q, 0.5), t3 + " > " + t10 + " > " + Copula.upperOrthant(q, q, 0.5));
        Assertions.assertEquals(0.2531699951, Copula.tailDependence(0.5, 4), 1e-9);
        Assertions.assertEquals(0d, Copula.tailDependence(-1, 4));
        Assertions.assertEquals(1d, Copula.tailDependence(1, 4));
        // the share a t(5) copula implies is matched by df 5; the Gaussian's share by no finite df; a share beyond df 1's by the floor
        final double share5 = Copula.impliedShare(0.05, 0.06, 0.4, Copula.T, 5);
        Assertions.assertEquals(5, Copula.matchingDf(share5, 0.05, 0.06, 0.4), 0.15);
        Assertions.assertTrue(Double.isNaN(Copula.matchingDf(Copula.impliedShare(0.05, 0.06, 0.4, Copula.GAUSSIAN, Double.NaN), 0.05, 0.06, 0.4)));
        Assertions.assertEquals(Copula.DF_MATCH_MIN, Copula.matchingDf(0.99, 0.05, 0.06, 0.4));
    }

    /** P(X > a, Y > b): independence is the product, ρ → 1 the larger threshold, and ρ = 0.5 at the 0.95 quantile ≈ 0.244 × 0.05. */
    @Test
    public void testUpperOrthant() {
        final double t = com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal(0.95);
        Assertions.assertEquals(0.05 * 0.05, Copula.upperOrthant(t, t, 0), 1e-9);
        final double a = 0.3, b = -0.4;
        Assertions.assertEquals(com.mercari.solution.util.domain.math.NormalDistribution.cdf(-a) * com.mercari.solution.util.domain.math.NormalDistribution.cdf(-b),
                Copula.upperOrthant(a, b, 0), 1e-9);
        Assertions.assertEquals(0.244, Copula.upperOrthant(t, t, 0.5) / 0.05, 1e-3);
        Assertions.assertEquals(0.392, Copula.upperOrthant(t, t, 0.7) / 0.05, 1e-3);
        Assertions.assertEquals(0.05, Copula.upperOrthant(t, t, 1), 1e-9);
        // the orthant grows with the correlation
        Assertions.assertTrue(Copula.upperOrthant(t, t, 0.2) < Copula.upperOrthant(t, t, 0.6));
        // a threshold far below the mean (a score column that is not centred): X > a is almost sure, the orthant is P(Y > b)
        Assertions.assertEquals(0.05, Copula.upperOrthant(-10, t, 0.5), 1e-9);
        Assertions.assertEquals(0, Copula.upperOrthant(40, t, 0.5), 1e-300);
    }

    /** Ranks → normal scores, the way a quantile transform with a knot per value scores a column. */
    private static double[][] normalScores(final double[][] x) {
        final int n = x.length, d = x[0].length;
        final double[][] z = new double[n][d];
        for (int i = 0; i < d; i++) {
            final int col = i;
            final Integer[] order = new Integer[n];
            for (int t = 0; t < n; t++) order[t] = t;
            java.util.Arrays.sort(order, java.util.Comparator.comparingDouble(t -> x[t][col]));
            for (int r = 0; r < n; r++) z[order[r]][i] = com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal((r + 1d) / (n + 1));
        }
        return z;
    }

    private static Svd.Moments diagnoseMoments(final double[][] z, final Double[][] mass, final double tail) {
        final double upper = com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal(tail);
        final Svd.Moments m = new Svd.Moments();
        for (int t = 0; t < z.length; t++) m.add(Copula.diagnoseVector(z[t], mass == null ? new Double[z[t].length] : mass[t], upper));
        return m;
    }

    /**
     * Gaussian scores: the observed joint-tail shares sit within the standard error of the implied ones, nothing is
     * flagged. A t(3) scale mixture with the same correlation has tail dependence: both tails are flagged.
     */
    @Test
    public void testDiagnoseFlagsTailDependence() {
        final double[][] r = {{1, 0.5}, {0.5, 1}};
        final int n = 20_000;
        final double[][] gaussian = draws(r, n, 21);
        final double[][] mixture = draws(r, n, 22);
        final Random random = new Random(23);
        for (final double[] row : mixture) {
            double chi2 = 0;
            for (int k = 0; k < 3; k++) {
                final double g = random.nextGaussian();
                chi2 += g * g;
            }
            final double scale = Math.sqrt(3 / chi2);
            for (int i = 0; i < row.length; i++) row[i] *= scale;
        }
        final java.util.List<String> names = java.util.List.of("a", "b");
        final int[][] pairs = {{0, 1}};
        final boolean[] noMass = {false, false};
        for (final boolean dependent : new boolean[]{false, true}) {
            final Svd.Moments m = diagnoseMoments(normalScores(dependent ? mixture : gaussian), null, 0.95);
            final Copula model = Copula.fit(Copula.head(m, 2), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
            final Copula.Report report = Copula.diagnose(m, 2, model, 0.95, pairs, noMass, names);
            final com.google.gson.JsonObject upper = report.json().getAsJsonArray("pairs").get(0).getAsJsonObject().getAsJsonObject("upper");
            final double observed = upper.get("observed").getAsDouble(), implied = upper.get("implied").getAsDouble(), se = upper.get("se").getAsDouble();
            Assertions.assertEquals(1000, upper.get("rows").getAsDouble(), 2, "5% of 20000 rows beyond the 0.95 score");
            if (dependent) {
                Assertions.assertTrue(observed - implied > 2 * se, report::toString);
                Assertions.assertEquals(2, report.json().get("flagged").getAsInt(), report::toString);
                Assertions.assertEquals(2, report.warnings().size(), report::toString);
                // the data are a t(3) scale mixture: the matching t copula has about 3 degrees of freedom
                final double suggested = report.json().get("dfSuggested").getAsDouble();
                Assertions.assertTrue(suggested > 1.5 && suggested < 6, report::toString);
                Assertions.assertEquals(2, report.json().get("dfMatched").getAsInt(), report::toString);
                Assertions.assertTrue(upper.has("dfMatch"), report::toString);
                Assertions.assertTrue(report.lines().get(report.lines().size() - 1).contains("matches the observed joint-tail shares"), report::toString);
                // the same rows under a t copula with the true df: the implied share is within noise, nothing is flagged
                final Copula t3 = Copula.fit(Copula.head(m, 2), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, Copula.T, 3, false);
                final Copula.Report ofT = Copula.diagnose(m, 2, t3, 0.95, pairs, noMass, names);
                final com.google.gson.JsonObject pair = ofT.json().getAsJsonArray("pairs").get(0).getAsJsonObject();
                Assertions.assertEquals("t", ofT.json().get("family").getAsString());
                Assertions.assertEquals(3d, ofT.json().get("df").getAsDouble());
                Assertions.assertEquals(Copula.tailDependence(t3.correlation[0][1], 3), pair.get("tailDependence").getAsDouble());
                for (final String side : new String[]{"upper", "lower"}) {
                    final com.google.gson.JsonObject s = pair.getAsJsonObject(side);
                    Assertions.assertTrue(Math.abs(s.get("observed").getAsDouble() - s.get("implied").getAsDouble()) < 3 * s.get("se").getAsDouble(), ofT::toString);
                    Assertions.assertTrue(s.get("implied").getAsDouble() > implied + 0.05, "the t copula implies a larger joint-tail share than the Gaussian: " + ofT);
                }
                Assertions.assertEquals(0, ofT.json().get("flagged").getAsInt(), ofT::toString);
                Assertions.assertFalse(ofT.json().has("dfSuggested"), ofT::toString);
            } else {
                Assertions.assertTrue(Math.abs(observed - implied) < 3 * se, report::toString);
                Assertions.assertEquals(0, report.json().get("flagged").getAsInt(), report::toString);
                Assertions.assertTrue(report.warnings().isEmpty(), report::toString);
                Assertions.assertEquals(2, report.lines().size(), report::toString);
            }
        }
    }

    /**
     * Gaussian scores are not flagged for what is not tail dependence: a heavily shrunk model (the implied share comes
     * from the sample correlation, the model's one is reported beside it) and a marginal whose top 8% is one tied
     * score (the thresholds sit at the observed exceedance rates, not at the nominal 5%).
     */
    @Test
    public void testDiagnoseIgnoresShrinkageAndMarginalShape() {
        final double[][] r = {{1, 0.5}, {0.5, 1}};
        final java.util.List<String> names = java.util.List.of("a", "b");
        final double[][] z = normalScores(draws(r, 20_000, 27));
        final Svd.Moments m = diagnoseMoments(z, null, 0.95);
        final Copula shrunk = Copula.fit(Copula.head(m, 2), "0.3", Copula.DEFAULT_EIGEN_FLOOR, false);
        final Copula.Report report = Copula.diagnose(m, 2, shrunk, 0.95, new int[][]{{0, 1}}, new boolean[2], names);
        final com.google.gson.JsonObject pair = report.json().getAsJsonArray("pairs").get(0).getAsJsonObject();
        Assertions.assertEquals(Copula.sampleCorrelation(m, 0, 1), pair.get("rho").getAsDouble());
        Assertions.assertEquals(shrunk.correlation[0][1], pair.get("rhoModel").getAsDouble());
        Assertions.assertEquals(0.7 * pair.get("rho").getAsDouble(), pair.get("rhoModel").getAsDouble(), 1e-6);
        Assertions.assertEquals(0, report.json().get("flagged").getAsInt(), report::toString);

        final double pile = com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal(0.92);
        final double tied = com.mercari.solution.util.domain.math.NormalDistribution.inverseNormal(0.96);
        for (final double[] row : z) if (row[0] > pile) row[0] = tied;
        final Svd.Moments piled = diagnoseMoments(z, null, 0.95);
        final Copula model = Copula.fit(Copula.head(piled, 2), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        final Copula.Report shaped = Copula.diagnose(piled, 2, model, 0.95, new int[][]{{0, 1}}, new boolean[2], names);
        final com.google.gson.JsonObject upper = shaped.json().getAsJsonArray("pairs").get(0).getAsJsonObject().getAsJsonObject("upper");
        Assertions.assertEquals(1300, upper.get("rows").getAsDouble(), 2, "8% of the rows beyond in a, 5% in b");
        Assertions.assertTrue(Math.abs(upper.get("observed").getAsDouble() - upper.get("implied").getAsDouble()) < 3 * upper.get("se").getAsDouble(), shaped::toString);
        Assertions.assertEquals(0, shaped.json().get("flagged").getAsInt(), shaped::toString);
    }

    /** Point masses that occur together are reported against the product of their shares; independent ones are not flagged. */
    @Test
    public void testDiagnosePointMasses() {
        final double[][] z = draws(new double[][]{{1, 0.3, 0}, {0.3, 1, 0}, {0, 0, 1}}, 4000, 24);
        final Double[][] mass = new Double[z.length][3];
        final Random random = new Random(25);
        for (int t = 0; t < z.length; t++) {
            final boolean together = t % 10 < 3;                   // inputs 0 and 1: on their mass in the same 30% of rows
            mass[t][0] = together ? 1d : 0d;
            mass[t][1] = together ? 1d : 0d;
            mass[t][2] = random.nextDouble() < 0.3 ? 1d : 0d;      // input 2: as often, independently
        }
        final Svd.Moments m = diagnoseMoments(z, mass, 0.95);
        final Copula model = Copula.fit(Copula.head(m, 3), Copula.NONE, Copula.DEFAULT_EIGEN_FLOOR, false);
        final Copula.Report report = Copula.diagnose(m, 3, model, 0.95, new int[][]{{0, 1}, {0, 2}}, new boolean[]{true, true, true}, java.util.List.of("x", "y", "w"));
        final com.google.gson.JsonObject masses = report.json().getAsJsonObject("mass");
        Assertions.assertEquals(0.3, masses.getAsJsonObject("shares").get("x").getAsDouble(), 1e-9);
        final com.google.gson.JsonObject xy = masses.getAsJsonArray("pairs").get(0).getAsJsonObject();
        Assertions.assertEquals(0.3, xy.get("both").getAsDouble(), 1e-9);
        Assertions.assertEquals(0.09, xy.get("independent").getAsDouble(), 1e-9);
        Assertions.assertEquals(1, report.warnings().stream().filter(w -> w.contains("point masses")).count(), report::toString);
        final com.google.gson.JsonObject xw = masses.getAsJsonArray("pairs").get(1).getAsJsonObject();
        Assertions.assertEquals(0.09, xw.get("both").getAsDouble(), 0.02);
        // an input without a flag has no share and no pair
        final Copula.Report partial = Copula.diagnose(m, 3, model, 0.95, new int[][]{{0, 1}}, new boolean[]{true, false, true}, java.util.List.of("x", "y", "w"));
        Assertions.assertFalse(partial.json().getAsJsonObject("mass").getAsJsonObject("shares").has("y"));
        Assertions.assertEquals(0, partial.json().getAsJsonObject("mass").getAsJsonArray("pairs").size());
    }

    /** The scores' block of the diagnose moments is the plain score moments: the same correlation, bit for bit, and n. */
    @Test
    public void testDiagnoseVectorKeepsTheScoreMoments() {
        final double[][] z = draws(R, 500, 26);
        final Svd.Moments diagnosed = diagnoseMoments(z, null, 0.9);
        Assertions.assertEquals(12, diagnosed.dimension);
        final Svd.Moments head = Copula.head(diagnosed, 3);
        final Copula plain = Copula.fit(moments(z), Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        final Copula fromHead = Copula.fit(head, Copula.OAS, Copula.DEFAULT_EIGEN_FLOOR, false);
        Assertions.assertEquals(plain.n, fromHead.n);
        for (int i = 0; i < 3; i++) Assertions.assertArrayEquals(plain.correlation[i], fromHead.correlation[i]);
        Assertions.assertEquals(plain.shrinkage, fromHead.shrinkage);
        // the report travels with the model to the artifact and back
        final Copula withReport = fromHead.withDiagnostics(Copula.diagnose(diagnosed, 3, fromHead, 0.9, new int[][]{{0, 1}}, new boolean[3], java.util.List.of("p", "q", "r")).json().toString());
        final Copula back = Copula.fromJson(withReport.toJson());
        Assertions.assertEquals(withReport.diagnostics, back.diagnostics);
        Assertions.assertNull(Copula.fromJson(plain.toJson()).diagnostics);
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
