package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.domain.math.MatrixOps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class SmoothTest {

    private static final Smooth.Basis CUBIC = new Smooth.Basis(0, 10, 8, 3);

    private static Svd.Moments moments(final Smooth.Basis basis, final double[] x, final double[] y) {
        final Svd.Moments m = new Svd.Moments();
        for (int i = 0; i < x.length; i++) m.add(Smooth.contribution(basis, x[i], y[i]));
        return m;
    }

    /** Keys uniform on the range (a few beyond it) and a target {@code f(x) + noise}. */
    private static double[][] sample(final int n, final long seed, final java.util.function.DoubleUnaryOperator f, final double noise) {
        final Random random = new Random(seed);
        final double[] x = new double[n], y = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = random.nextDouble() * 10;
            y[i] = f.applyAsDouble(x[i]) + noise * random.nextGaussian();
        }
        return new double[][]{x, y};
    }

    @Test
    public void testBasis() {
        // degree 1 = hat functions over the knots 0, 1, 2, 3, 4
        final Smooth.Basis hats = new Smooth.Basis(0, 4, 4, 1);
        Assertions.assertEquals(5, hats.size());
        Assertions.assertArrayEquals(new double[]{0, 0.5, 0.5, 0, 0}, hats.evaluate(1.5), 1e-12);
        Assertions.assertArrayEquals(new double[]{0, 0, 0, 0.25, 0.75}, hats.evaluate(3.75), 1e-12);
        Assertions.assertArrayEquals(new double[]{0, 0, 0, 0, 1}, hats.evaluate(4), 1e-12);
        // a uniform cubic B-spline at a knot: 1/6, 4/6, 1/6
        Assertions.assertArrayEquals(new double[]{1 / 6d, 4 / 6d, 1 / 6d, 0, 0, 0, 0, 0, 0, 0, 0}, CUBIC.evaluate(0), 1e-12);
        // non-negative, summing to one, degree + 1 alive; a key beyond the range is clamped to its end
        for (final double x : new double[]{0, 0.3, 1.25, 5, 7.77, 9.999, 10}) {
            final double[] b = CUBIC.evaluate(x);
            double sum = 0;
            int alive = 0;
            for (final double v : b) {
                Assertions.assertTrue(v >= 0);
                sum += v;
                if (v > 0) alive++;
            }
            Assertions.assertEquals(1, sum, 1e-12);
            Assertions.assertTrue(alive <= 4);
        }
        Assertions.assertArrayEquals(CUBIC.evaluate(0), CUBIC.evaluate(-3), 0);
        Assertions.assertArrayEquals(CUBIC.evaluate(10), CUBIC.evaluate(1e9), 0);
        // evaluateInto leaves the rest of the vector alone
        final double[] z = {9, 9, 9, 9, 9, 9, 9};
        hats.evaluateInto(1.5, z, 1);
        Assertions.assertArrayEquals(new double[]{9, 0, 0.5, 0.5, 0, 0, 9}, z, 1e-12);
    }

    @Test
    public void testPenalty() {
        // second differences over 4 coefficients: D = [[1, −2, 1, 0], [0, 1, −2, 1]], P = DᵀD
        final double[][] p = Smooth.penalty(4, 2);
        Assertions.assertArrayEquals(new double[]{1, -2, 1, 0}, p[0], 1e-12);
        Assertions.assertArrayEquals(new double[]{-2, 5, -4, 1}, p[1], 1e-12);
        Assertions.assertArrayEquals(new double[]{1, -4, 5, -2}, p[2], 1e-12);
        Assertions.assertArrayEquals(new double[]{0, 1, -2, 1}, p[3], 1e-12);
        // first and third differences annihilate constants / quadratics
        final double[][] p3 = Smooth.penalty(6, 3);
        final double[] quadratic = {0, 1, 4, 9, 16, 25};
        Assertions.assertArrayEquals(new double[6], MatrixOps.multiply(p3, quadratic), 1e-9);
        Assertions.assertArrayEquals(new double[]{1, -1, 0}, Smooth.penalty(3, 1)[0], 1e-12);
    }

    /** The design matrix and the dense penalised solve, straight from the rows: the reference of the moment-based fit. */
    private static double[] denseSolve(final Smooth.Basis basis, final double[] x, final double[] y, final int order, final double lambda) {
        final int m = basis.size();
        final double[][] a = Smooth.penalty(m, order);
        for (int i = 0; i < m; i++) for (int j = 0; j < m; j++) a[i][j] *= lambda;
        final double[] r = new double[m];
        for (int row = 0; row < x.length; row++) {
            final double[] b = basis.evaluate(x[row]);
            for (int i = 0; i < m; i++) {
                r[i] += b[i] * y[row];
                for (int j = 0; j < m; j++) a[i][j] += b[i] * b[j];
            }
        }
        return MatrixOps.solve(a, r);
    }

    @Test
    public void testDeclaredStrengthMatchesTheDenseSolve() {
        final double[][] xy = sample(400, 7, v -> Math.sin(v) + 0.1 * v, 0.2);
        for (final double lambda : new double[]{0.01, 1, 250}) {
            final Smooth smooth = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, 2, lambda, true);
            Assertions.assertFalse(smooth.estimated);
            Assertions.assertEquals(lambda, smooth.lambda);
            Assertions.assertArrayEquals(denseSolve(CUBIC, xy[0], xy[1], 2, lambda), smooth.coefficients, 1e-7);
            Assertions.assertEquals(400, smooth.n);
        }
    }

    @Test
    public void testAHeavyPenaltyIsTheLeastSquaresLine() {
        final double[][] xy = sample(300, 11, v -> 3 - 0.4 * v + 0.05 * v * v, 0.5);
        // the order-2 penalty leaves lines alone (uniform B-splines reproduce them), so λ → ∞ is ordinary least squares
        double sx = 0, sy = 0, sxx = 0, sxy = 0;
        for (int i = 0; i < 300; i++) {
            sx += xy[0][i];
            sy += xy[1][i];
            sxx += xy[0][i] * xy[0][i];
            sxy += xy[0][i] * xy[1][i];
        }
        final double slope = (sxy - sx * sy / 300) / (sxx - sx * sx / 300), intercept = sy / 300 - slope * sx / 300;
        final Smooth smooth = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, 2, 1e9, true);
        for (final double x : new double[]{0, 2.5, 5, 9.9}) Assertions.assertEquals(intercept + slope * x, smooth.curve(x), 1e-4);
        Assertions.assertEquals(2, smooth.edf, 1e-3);
        // beyond the range the curve is constant
        Assertions.assertEquals(smooth.curve(10d), smooth.curve(25d), 0);
        Assertions.assertEquals(1.5 - smooth.curve(5d), smooth.residual(5d, 1.5), 1e-12);
        Assertions.assertNull(smooth.curve(null));
        Assertions.assertNull(smooth.curve(Double.NaN));
        Assertions.assertNull(smooth.residual(5d, null));
    }

    @Test
    public void testRemlRecoversASmoothSignal() {
        final double[][] xy = sample(3000, 3, Math::sin, 0.3);
        final Smooth smooth = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, 2, null, true);
        Assertions.assertTrue(smooth.estimated);
        for (double x = 0.25; x < 10; x += 0.25) Assertions.assertEquals(Math.sin(x), smooth.curve(x), 0.08, "at " + x);
        Assertions.assertTrue(smooth.edf > 4 && smooth.edf < 11, "edf " + smooth.edf);
        Assertions.assertEquals(0.09, smooth.sigma2, 0.01);

        // a target that is a line plus noise needs no wiggle: REML shrinks the curve to (nearly) the line
        final double[][] line = sample(3000, 5, v -> 2 + 0.5 * v, 0.3);
        final Smooth straight = Smooth.fit(moments(CUBIC, line[0], line[1]), CUBIC, 2, null, true);
        Assertions.assertTrue(straight.edf < 3.5, "edf " + straight.edf);
        for (double x = 0.5; x < 10; x += 0.5) Assertions.assertEquals(2 + 0.5 * x, straight.curve(x), 0.05);

        // an exact line: every strength fits it perfectly, the criterion falls back to the heaviest penalty without failing
        final double[][] exact = sample(200, 9, v -> 2 + 0.5 * v, 0);
        final Smooth noiseless = Smooth.fit(moments(CUBIC, exact[0], exact[1]), CUBIC, 2, null, true);
        for (double x = 0.5; x < 10; x += 0.5) Assertions.assertEquals(2 + 0.5 * x, noiseless.curve(x), 1e-6);
    }

    /**
     * A criterion still falling at the heavy end of the search has no minimum to locate — the curve is the penalty's
     * polynomial whatever strength is reported, and a minimiser would return whichever point of the plateau rounding
     * favours. The fit reports the end of the search itself and says so; an interior minimum carries no mark.
     */
    @Test
    public void testRemlAtTheEndOfTheSearchIsMarked() {
        // a key that takes three values cannot tell a curve from the line through them
        final int n = 600;
        final double[] x = new double[n], y = new double[n];
        final java.util.Random random = new java.util.Random(11);
        for (int i = 0; i < n; i++) {
            x[i] = 2 + 3 * (i % 3);
            y[i] = 1 + 0.5 * x[i] + 0.3 * random.nextGaussian();
        }
        final Smooth line = Smooth.fit(moments(CUBIC, x, y), CUBIC, 2, null, true);
        Assertions.assertTrue(line.estimated);
        Assertions.assertEquals(Smooth.POLYNOMIAL, line.limit, "λ = " + line.lambda + ", edf = " + line.edf);
        Assertions.assertEquals(2, line.edf, 1e-3);
        for (final double at : new double[]{2, 5, 8}) Assertions.assertEquals(1 + 0.5 * at, line.curve(at), 0.05);
        // the same rows in another order: the same strength to the last bit, which a refinement on the plateau does not give
        final double[] xr = new double[n], yr = new double[n];
        for (int i = 0; i < n; i++) {
            xr[i] = x[n - 1 - i];
            yr[i] = y[n - 1 - i];
        }
        Assertions.assertEquals(line.lambda, Smooth.fit(moments(CUBIC, xr, yr), CUBIC, 2, null, true).lambda, line.lambda * 1e-9);
        Assertions.assertEquals(Smooth.POLYNOMIAL, Smooth.fromJson(line.toJson()).limit);

        final double[][] xy = sample(3000, 3, Math::sin, 0.3);
        final Smooth interior = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, 2, null, true);
        Assertions.assertNull(interior.limit);
        Assertions.assertFalse(interior.toJson().has("limit"));
        Assertions.assertNull(Smooth.fit(moments(CUBIC, x, y), CUBIC, 2, 5d, true).limit, "a declared strength is not searched");
    }

    /** log|M| by Gaussian elimination with partial pivoting (M positive definite here). */
    private static double logDeterminant(final double[][] input) {
        final int m = input.length;
        final double[][] a = new double[m][];
        for (int i = 0; i < m; i++) a[i] = input[i].clone();
        double log = 0;
        for (int c = 0; c < m; c++) {
            int pivot = c;
            for (int i = c + 1; i < m; i++) if (Math.abs(a[i][c]) > Math.abs(a[pivot][c])) pivot = i;
            final double[] swap = a[c];
            a[c] = a[pivot];
            a[pivot] = swap;
            log += Math.log(Math.abs(a[c][c]));
            for (int i = c + 1; i < m; i++) {
                final double f = a[i][c] / a[c][c];
                for (int j = c; j < m; j++) a[i][j] -= f * a[c][j];
            }
        }
        return log;
    }

    /** The REML criterion from the rows: (n − d) log(‖y − Xβ‖² + λβᵀPβ) + log|XᵀX + λP| − (m − d) log λ. */
    private static double criterion(final Smooth.Basis basis, final double[] x, final double[] y, final int order, final double lambda) {
        final int m = basis.size();
        final double[] beta = denseSolve(basis, x, y, order, lambda);
        final double[][] p = Smooth.penalty(m, order);
        final double[][] a = new double[m][m];
        double rss = 0;
        for (int row = 0; row < x.length; row++) {
            final double[] b = basis.evaluate(x[row]);
            double f = 0;
            for (int i = 0; i < m; i++) {
                f += b[i] * beta[i];
                for (int j = 0; j < m; j++) a[i][j] += b[i] * b[j];
            }
            rss += (y[row] - f) * (y[row] - f);
        }
        final double[] pb = MatrixOps.multiply(p, beta);
        for (int i = 0; i < m; i++) {
            rss += lambda * beta[i] * pb[i];
            for (int j = 0; j < m; j++) a[i][j] += lambda * p[i][j];
        }
        return (x.length - order) * Math.log(rss) + logDeterminant(a) - (m - order) * Math.log(lambda);
    }

    @Test
    public void testRemlMinimisesTheCriterionComputedFromTheRows() {
        final double[][] xy = sample(500, 21, v -> Math.cos(v / 2) + 100, 0.4);
        for (final int order : new int[]{1, 2, 3}) {
            final Smooth smooth = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, order, null, true);
            final double chosen = criterion(CUBIC, xy[0], xy[1], order, smooth.lambda);
            for (double e = -6; e <= 8; e += 0.25) {
                final double other = criterion(CUBIC, xy[0], xy[1], order, Math.pow(10, e));
                Assertions.assertTrue(chosen <= other + 1e-6, "order " + order + ": λ = " + smooth.lambda + " (" + chosen + ") loses to 1e" + e + " (" + other + ")");
            }
        }
    }

    @Test
    public void testMergedTimeBlocksEqualTheWholeFit() {
        final double[][] xy = sample(900, 13, v -> Math.sqrt(v), 0.2);
        final Map<Long, Svd.Moments> parts = new LinkedHashMap<>();
        for (int i = 0; i < 900; i++) {
            parts.computeIfAbsent((long) (i / 300), k -> new Svd.Moments()).add(Smooth.contribution(CUBIC, xy[0][i], xy[1][i]));
        }
        final BlockSeries<Svd.Moments> series = new BlockSeries<>(Svd.SUMMARY, parts);
        final Smooth whole = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, 2, 5d, true);
        Assertions.assertArrayEquals(whole.coefficients, Smooth.fit(series.total(), CUBIC, 2, 5d, true).coefficients, 1e-9);
        // a forward row reads the blocks before its own: the fit over them is the fit of their rows
        final double[] x2 = java.util.Arrays.copyOf(xy[0], 600), y2 = java.util.Arrays.copyOf(xy[1], 600);
        final Smooth prefix = Smooth.fit(moments(CUBIC, x2, y2), CUBIC, 2, null, true);
        final Smooth read = series.models(0, s -> Smooth.fit(s, CUBIC, 2, null, false)).get(1L);
        Assertions.assertEquals(prefix.lambda, read.lambda, prefix.lambda * 1e-4);
        Assertions.assertArrayEquals(prefix.coefficients, read.coefficients, 1e-6);
    }

    @Test
    public void testATargetWithALargeLevel() {
        final double[][] xy = sample(500, 17, Math::sin, 0.1);
        final double[] lifted = new double[500];
        for (int i = 0; i < 500; i++) lifted[i] = xy[1][i] + 1e9;
        final Smooth plain = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, 2, 1d, true);
        final Smooth high = Smooth.fit(moments(CUBIC, xy[0], lifted), CUBIC, 2, 1d, true);
        for (double x = 0; x <= 10; x += 0.5) Assertions.assertEquals(plain.curve(x), high.curve(x) - 1e9, 1e-5);
    }

    @Test
    public void testDegenerateInputs() {
        // no row, too few rows, a row without a key or a target
        Assertions.assertTrue(Smooth.fit(new Svd.Moments(), CUBIC, 2, null, false).isEmpty());
        Assertions.assertTrue(Smooth.fit(moments(CUBIC, new double[]{1, 2}, new double[]{1, 2}), CUBIC, 2, null, false).isEmpty());
        Assertions.assertNull(Smooth.fit(new Svd.Moments(), CUBIC, 2, null, false).curve(3d));
        Assertions.assertNull(Smooth.contribution(CUBIC, null, 1d));
        Assertions.assertNull(Smooth.contribution(CUBIC, 1d, Double.NaN));
        Assertions.assertNull(Smooth.contribution(CUBIC, Double.NaN, 1d));
        // every row at one key: the system is singular without the jitter, and the curve there is the target mean
        final double[] x = {4, 4, 4, 4, 4}, y = {1, 2, 3, 4, 5};
        for (final Double lambda : new Double[]{1d, null}) {
            final Smooth smooth = Smooth.fit(moments(CUBIC, x, y), CUBIC, 2, lambda, false);
            Assertions.assertFalse(smooth.isEmpty());
            Assertions.assertEquals(3, smooth.curve(4d), 1e-3);
        }
    }

    @Test
    public void testArtifactJson() {
        final double[][] xy = sample(200, 1, Math::sin, 0.1);
        final Smooth smooth = Smooth.fit(moments(CUBIC, xy[0], xy[1]), CUBIC, 2, null, true);
        final Smooth back = Smooth.fromJson(com.google.gson.JsonParser.parseString(smooth.toJson().toString()).getAsJsonObject());
        Assertions.assertEquals(smooth.basis, back.basis);
        Assertions.assertEquals(smooth.lambda, back.lambda);
        Assertions.assertEquals(smooth.edf, back.edf);
        Assertions.assertEquals(smooth.sigma2, back.sigma2);
        Assertions.assertTrue(back.estimated);
        Assertions.assertEquals(200, back.n);
        Assertions.assertArrayEquals(smooth.coefficients, back.coefficients, 0);
        Assertions.assertEquals(smooth.curve(3.3), back.curve(3.3));
        // an empty fit has neither a strength nor a variance, and still round-trips
        final Smooth empty = Smooth.fromJson(com.google.gson.JsonParser.parseString(Smooth.fit(new Svd.Moments(), CUBIC, 2, null, false).toJson().toString()).getAsJsonObject());
        Assertions.assertTrue(empty.isEmpty());
        Assertions.assertTrue(Double.isNaN(empty.lambda));
    }

}
