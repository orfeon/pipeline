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
            } else {
                Assertions.assertTrue(Math.abs(observed - implied) < 3 * se, report::toString);
                Assertions.assertEquals(0, report.json().get("flagged").getAsInt(), report::toString);
                Assertions.assertTrue(report.warnings().isEmpty(), report::toString);
                Assertions.assertEquals(2, report.lines().size(), report::toString);
            }
        }
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
