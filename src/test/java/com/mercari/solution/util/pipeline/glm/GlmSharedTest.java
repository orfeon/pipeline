package com.mercari.solution.util.pipeline.glm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Pins the contract of the shared classes the screen and evaluation transforms both read. */
public class GlmSharedTest {

    @Test
    public void testFamilyNamesFormsAndWeights() {
        Assertions.assertEquals(List.of("groupedMultinomial", "binomial", "gaussian", "poisson"), Family.NAMES);
        Assertions.assertEquals(Family.BINOMIAL, Family.of("binomial"));
        Assertions.assertNull(Family.of("gamma"));
        Assertions.assertEquals(List.of("prob", "logProb", "inverseShare"), Family.GROUPED_MULTINOMIAL.forms());
        Assertions.assertEquals(List.of("value"), Family.GAUSSIAN.forms());
        Assertions.assertEquals(List.of("rate", "logRate"), Family.POISSON.forms());
        Assertions.assertEquals(Family.PROBABILITY_FORMS, Family.formsFor("unknown"));
        Assertions.assertEquals(0.25, Family.BINOMIAL.fisherWeight(0.5), 1e-15);
        Assertions.assertEquals(3d, Family.POISSON.fisherWeight(3d), 1e-15);
        Assertions.assertEquals(1d, Family.GAUSSIAN.fisherWeight(3d), 1e-15);
        Assertions.assertEquals(0d, Family.BINOMIAL.link(0.5), 1e-15);
        Assertions.assertEquals(Math.log(3), Family.POISSON.link(3d), 1e-15);
        Assertions.assertEquals(3d, Family.GAUSSIAN.link(3d), 1e-15);
        Assertions.assertTrue(Family.GROUPED_MULTINOMIAL.isGrouped());
        Assertions.assertFalse(Family.BINOMIAL.isGrouped());
    }

    @Test
    public void testBaselineMeans() {
        final double[] p = new double[3];
        // inverse share: 1/x normalised within the group
        Assertions.assertEquals(Baselines.Skip.NONE, Baselines.means(Family.GROUPED_MULTINOMIAL, Family.FORM_INVERSE_SHARE, new double[]{2, 4, 4}, p));
        Assertions.assertArrayEquals(new double[]{0.5, 0.25, 0.25}, p, 1e-15);
        // grouped probabilities are normalised too
        Assertions.assertEquals(Baselines.Skip.NONE, Baselines.means(Family.GROUPED_MULTINOMIAL, Family.FORM_PROB, new double[]{0.2, 0.2, 0.4}, p));
        Assertions.assertArrayEquals(new double[]{0.25, 0.25, 0.5}, p, 1e-15);
        // log probabilities: shifted by the maximum, then normalised
        Assertions.assertEquals(Baselines.Skip.NONE, Baselines.means(Family.GROUPED_MULTINOMIAL, Family.FORM_LOG_PROB, new double[]{Math.log(0.2), Math.log(0.2), Math.log(0.4)}, p));
        Assertions.assertArrayEquals(new double[]{0.25, 0.25, 0.5}, p, 1e-15);
        // binomial: kept as is, clamped
        final double[] b = new double[2];
        Assertions.assertEquals(Baselines.Skip.NONE, Baselines.means(Family.BINOMIAL, Family.FORM_PROB, new double[]{0, 0.3}, b));
        Assertions.assertEquals(Baselines.EPS, b[0], 0d);
        Assertions.assertEquals(0.3, b[1], 0d);
        // invalid values
        Assertions.assertEquals(Baselines.Skip.INVALID_BASELINE, Baselines.means(Family.BINOMIAL, Family.FORM_PROB, new double[]{1.5, 0.3}, b));
        Assertions.assertEquals(Baselines.Skip.INVALID_BASELINE, Baselines.means(Family.GROUPED_MULTINOMIAL, Family.FORM_INVERSE_SHARE, new double[]{0, 4, 4}, p));
        Assertions.assertEquals(Baselines.Skip.INVALID_BASELINE, Baselines.means(Family.POISSON, Family.FORM_RATE, new double[]{0, 4, 4}, p));
        Assertions.assertEquals(Baselines.Skip.INVALID_BASELINE, Baselines.means(Family.GAUSSIAN, Family.FORM_VALUE, new double[]{Double.NaN, 4, 4}, p));
        Assertions.assertEquals(Baselines.Skip.NONE, Baselines.means(Family.POISSON, Family.FORM_LOG_RATE, new double[]{0, Math.log(2), 1}, p));
        Assertions.assertArrayEquals(new double[]{1, 2, Math.E}, p, 1e-15);
    }

    @Test
    public void testLabelNormalisation() {
        final double[] tie = {1, 1, 0};
        Assertions.assertEquals(Baselines.Skip.NONE, Baselines.normalizeLabels(Family.GROUPED_MULTINOMIAL, true, tie));
        Assertions.assertArrayEquals(new double[]{0.5, 0.5, 0}, tie, 1e-15);
        final double[] kept = {1, 1, 0};
        Baselines.normalizeLabels(Family.GROUPED_MULTINOMIAL, false, kept);
        Assertions.assertArrayEquals(new double[]{1, 1, 0}, kept, 1e-15);
        Assertions.assertEquals(Baselines.Skip.NO_POSITIVE_LABEL, Baselines.normalizeLabels(Family.GROUPED_MULTINOMIAL, true, new double[]{0, 0}));
        final double[] row = {0, 0};
        Assertions.assertEquals(Baselines.Skip.NONE, Baselines.normalizeLabels(Family.BINOMIAL, true, row));
    }

    @Test
    public void testFittedMeansAndEvaluation() {
        // grouped: softmax of log p + f θ; at θ = 0 the fitted means are the baseline shares
        final double[] p = {0.5, 0.3, 0.2};
        final double[][] f = {{1}, {0}, {-1}};
        Assertions.assertArrayEquals(p, GlmFit.fitted(Family.GROUPED_MULTINOMIAL, false, p, f, new double[]{0}), 1e-15);
        final double[] mu = GlmFit.fitted(Family.GROUPED_MULTINOMIAL, false, p, f, new double[]{1});
        final double z = 0.5 * Math.E + 0.3 + 0.2 / Math.E;
        Assertions.assertArrayEquals(new double[]{0.5 * Math.E / z, 0.3 / z, 0.2 / Math.E / z}, mu, 1e-15);
        // one Newton evaluation at θ = 0 for the winner on row 0: ll = log 0.5, g = Σ (y − p) f = 0.5·1 − 0.3·0 + 0.2·1 = 0.7
        final double[] eval = GlmFit.evaluate(Family.GROUPED_MULTINOMIAL, new double[]{1, 0, 0}, p, null, 1d, f, 1);
        Assertions.assertEquals(1d, eval[0], 0d);
        Assertions.assertEquals(Math.log(0.5), eval[1], 1e-15);
        Assertions.assertEquals(0.7, eval[2], 1e-15);
        // G = Σ p f² − (Σ p f)² = 0.5 + 0.2 − 0.3² = 0.61
        Assertions.assertEquals(0.61, eval[3], 1e-15);
        // binomial without a baseline: an intercept column carries the prior
        final double[][] fi = {{1}, {1}};
        final double[] b = GlmFit.fitted(Family.BINOMIAL, true, null, fi, new double[]{0});
        Assertions.assertArrayEquals(new double[]{0.5, 0.5}, b, 1e-15);
        final double[] be = GlmFit.evaluate(Family.BINOMIAL, new double[]{1, 0}, b, new double[]{1, 1}, 1d, fi, 1);
        Assertions.assertEquals(2d, be[0], 0d);
        Assertions.assertEquals(2 * Math.log(0.5), be[1], 1e-15);
        Assertions.assertEquals(0d, be[2], 1e-15);
        Assertions.assertEquals(0.5, be[3], 1e-15);
    }
}
