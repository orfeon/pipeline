package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ShrinkageTest {

    @Test
    public void testFamilyDerivationAndParse() {
        Assertions.assertEquals(Shrinkage.Family.gaussian, Shrinkage.familyFor("mean"));
        Assertions.assertEquals(Shrinkage.Family.betaBinomial, Shrinkage.familyFor("rate"));
        Assertions.assertEquals(Shrinkage.Family.dirichletMultinomial, Shrinkage.familyFor("distribution"));
        for (final String stat : List.of("count", "share", "std", "quantile", "q90")) Assertions.assertNull(Shrinkage.familyFor(stat), stat);
        Assertions.assertTrue(Shrinkage.Family.gammaPoisson.accepts("mean"));
        Assertions.assertFalse(Shrinkage.Family.gammaPoisson.accepts("distribution"));
        Assertions.assertTrue(Shrinkage.Family.dirichletMultinomial.accepts("distribution"));
        Assertions.assertFalse(Shrinkage.Family.dirichletMultinomial.accepts("rate"));
        Assertions.assertFalse(Shrinkage.Family.gaussian.isConjugate());
        Assertions.assertTrue(Shrinkage.Family.betaBinomial.isConjugate());

        final Diagnostics diagnostics = new Diagnostics();
        final Shrinkage declared = Shrinkage.parse(JsonParser.parseString("{family: gammaPoisson, estimator: joint}").getAsJsonObject(), null, null, diagnostics, "features.enc");
        Assertions.assertFalse(diagnostics.hasErrors(), diagnostics::toString);
        Assertions.assertEquals(Shrinkage.Family.gammaPoisson, declared.resolveFamily("mean"));
        Assertions.assertEquals(Shrinkage.Estimator.joint, declared.estimator);
        final Shrinkage derived = Shrinkage.parse(JsonParser.parseString("{priorWeight: 3}").getAsJsonObject(), null, null, diagnostics, "features.enc");
        Assertions.assertNull(derived.family);
        Assertions.assertEquals(Shrinkage.Family.betaBinomial, derived.resolveFamily("rate"));
        Shrinkage.parse(JsonParser.parseString("{family: poisson}").getAsJsonObject(), null, null, diagnostics, "features.enc");
        Assertions.assertTrue(diagnostics.getMessages().stream().anyMatch(m -> m.code().equals("encoding.shrinkage.family")));
    }

    @Test
    public void testDistributionCompositionWithLeaveNodeOut() {
        // leaf (n=2) is all "x"; the global level (n=8) is half x half y. Leave-node-out removes the leaf's two x
        // from the parent → parent {x: 1/3, y: 2/3} over 6 rows; λ = 2 → w = 2 / (2 + 2) = 1/2
        final Shrinkage shrinkage = Shrinkage.of(Shrinkage.Scale.identity, 2, true, Shrinkage.Family.dirichletMultinomial);
        final List<Shrinkage.Level> levels = List.of(
                new Shrinkage.Level("seller", "s_n", "s_dist", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_dist", null));
        final Map<String, Object> row = Map.of("s_n", 2.0, "s_dist", Map.of("x", 1.0), "g_n", 8.0, "g_dist", Map.of("x", 0.5, "y", 0.5));
        final Shrinkage.Composition c = shrinkage.composeDistribution(row, levels, null);
        Assertions.assertNull(c.value());
        Assertions.assertEquals(2.0 / 3.0, c.distribution().get("x"), 1e-9);
        Assertions.assertEquals(1.0 / 3.0, c.distribution().get("y"), 1e-9);
        Assertions.assertEquals(4.0, c.effectiveN(), 1e-9); // 2 own + λ backed by the parent's 6 rows

        // an empty leaf reads the (leave-node-out) parent; an empty lattice reads null
        final Shrinkage.Composition parentOnly = shrinkage.composeDistribution(Map.of("s_n", 0.0, "g_n", 4.0, "g_dist", Map.of("x", 0.25, "y", 0.75)), levels, null);
        Assertions.assertEquals(0.25, parentOnly.distribution().get("x"), 1e-9);
        Assertions.assertNull(shrinkage.composeDistribution(Map.of("s_n", 0.0, "g_n", 0.0), levels, null).distribution());
        // variance-components λ for the level: ∞ = full shrinkage to the parent
        final Shrinkage.Composition full = shrinkage.composeDistribution(row, levels, Map.of("s_n", Double.POSITIVE_INFINITY));
        Assertions.assertEquals(1.0 / 3.0, full.distribution().get("x"), 1e-9);
        // the composition is a probability distribution
        Assertions.assertEquals(1.0, c.distribution().values().stream().mapToDouble(d -> d).sum(), 1e-9);
    }

    /**
     * A baseline offset on a logit scale: each level's own term is logit(observed) − logit(mean baseline) from the
     * hidden Σ(y − b) and Σb, the leaf shrinks toward the parent's term, and the composed value is the term itself
     * (not a probability). On the identity scale the extra sum changes nothing (Σ(y − b) / n as before).
     */
    @Test
    public void testOffsetOnLogitScaleComposesAdditiveTerm() {
        // leaf: n = 4, Σy = 3, Σb = 2 → Σ(y − b) = 1; global (leave-node-out): n = 10 − 4 = 6, Σy = 3 − ... given below
        final Shrinkage logit = Shrinkage.of(Shrinkage.Scale.logit, 2, true);
        final List<Shrinkage.Level> levels = List.of(
                new Shrinkage.Level("seller", "s_n", "s_sum", "s_off", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", "g_off", null));
        // global totals include the leaf: n = 10, Σ(y − b) = 1 + (−1) = 0, Σb = 2 + 4 = 6 → without the leaf n = 6, Σ(y − b) = −1, Σb = 4
        final Map<String, Object> row = Map.of("s_n", 4.0, "s_sum", 1.0, "s_off", 2.0, "g_n", 10.0, "g_sum", 0.0, "g_off", 6.0);
        final java.util.function.DoubleUnaryOperator lg = p -> Math.log(p / (1 - p));
        final double own = lg.applyAsDouble(3.0 / 4) - lg.applyAsDouble(2.0 / 4);      // observed 3/4 vs mean baseline 1/2
        final double root = lg.applyAsDouble(3.0 / 6) - lg.applyAsDouble(4.0 / 6);     // (Σ(y − b) + Σb) / n = (−1 + 4) / 6 = 1/2 vs 2/3
        final double w = 4.0 / (4 + 2);
        final Shrinkage.Composition c = logit.compose(row, levels, null);
        Assertions.assertEquals(root + w * (own - root), c.value(), 1e-12);
        Assertions.assertEquals(w * (own - root), c.deviations()[0], 1e-12);
        Assertions.assertTrue(c.value() > 0, "a key whose observed rate beats its baseline has a positive log-odds term");

        // the levels encode / parse with the offset column
        final List<Shrinkage.Level> parsed = Shrinkage.parseLevels(Shrinkage.encodeLevels(levels));
        Assertions.assertEquals("s_off", parsed.get(0).offColumn());
        Assertions.assertEquals("g_off", parsed.get(1).offColumn());
        Assertions.assertNull(Shrinkage.parseLevels(Shrinkage.encodeLevels(List.of(new Shrinkage.Level("seller", "s_n", "s_sum", null)))).get(0).offColumn());

        // identity: the mean residual, with or without the offset column
        final Shrinkage identity = Shrinkage.of(Shrinkage.Scale.identity, 2, true);
        final List<Shrinkage.Level> plain = List.of(
                new Shrinkage.Level("seller", "s_n", "s_sum", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", null));
        final double ownId = 1.0 / 4, rootId = -1.0 / 6;
        Assertions.assertEquals(rootId + w * (ownId - rootId), identity.compose(row, levels, null).value(), 1e-12);
        Assertions.assertEquals(identity.compose(row, plain, null).value(), identity.compose(row, levels, null).value(), 1e-12);
    }

    /**
     * A level whose mean baseline is outside the scale's domain (Σb = 0 here) has no term: it defers to its parent
     * with a zero deviation instead of leaking the transform's clamp (log(1e-12) = −27.6) into the composed value;
     * with every level undefined there is no estimate at all. The identity scale never has an undefined baseline.
     */
    @Test
    public void testOffsetTermWithUndefinedBaselineDefersToParent() {
        final Shrinkage log = Shrinkage.of(Shrinkage.Scale.log, 2, true);
        final List<Shrinkage.Level> levels = List.of(
                new Shrinkage.Level("seller", "s_n", "s_sum", "s_off", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", "g_off", null));
        // leaf: n = 4, Σ(y − b) = 2, Σb = 0 (cold-start baseline); global without the leaf: n = 6, Σ(y − b) = −1, Σb = 8
        final Map<String, Object> row = Map.of("s_n", 4.0, "s_sum", 2.0, "s_off", 0.0, "g_n", 10.0, "g_sum", 1.0, "g_off", 8.0);
        final Shrinkage.Composition c = log.compose(row, levels, null);
        Assertions.assertEquals(Math.log(7.0 / 6) - Math.log(8.0 / 6), c.value(), 1e-12, "the parent's term log(Σy / Σb)");
        Assertions.assertEquals(0d, c.deviations()[0], 0d);
        Assertions.assertNull(log.compose(Map.of("s_n", 4.0, "s_sum", 2.0, "s_off", 0.0, "g_n", 4.0, "g_sum", 2.0, "g_off", 0.0), levels, null).value());
        Assertions.assertFalse(Shrinkage.baselineDefined(Shrinkage.Scale.logit, 4, 4), "a mean baseline of 1 is outside logit");
        Assertions.assertTrue(Shrinkage.baselineDefined(Shrinkage.Scale.identity, 4, 0));
        Assertions.assertEquals(0.5, Shrinkage.own(Shrinkage.Scale.identity, 4, 2, 0, true), 0d);

        // the rows a level counts and sums: target and (under an offset) baseline present, NaN missing; a target-less
        // statistic counts every row without consulting the baseline
        Assertions.assertEquals(org.apache.beam.sdk.values.KV.of(0.25, 0.5), FeatureValues.offsetTarget(Map.of("y", 0.75, "b", 0.5), "y", "b"));
        Assertions.assertNull(FeatureValues.offsetTarget(Map.of("y", 0.75), "y", "b"), "a row without a baseline is outside every statistic");
        Assertions.assertNull(FeatureValues.offsetTarget(Map.of("y", 0.75, "b", Double.NaN), "y", "b"));
        Assertions.assertEquals(org.apache.beam.sdk.values.KV.of(0.75, null), FeatureValues.offsetTarget(Map.of("y", 0.75), "y", null));
        Assertions.assertEquals(org.apache.beam.sdk.values.KV.of(0d, null), FeatureValues.offsetTarget(Map.of("y", 0.75), null, "b"));
    }

    /**
     * The one-way moment estimator λ = σ²/τ² equals Kleinman's Beta-Binomial moment estimator m = (1 − ρ) / ρ with
     * ρ = (BMS − WMS) / (BMS + (n₀ − 1) WMS) on 0/1 data — the reason a declared betaBinomial family changes
     * neither the pseudo-count nor the point estimate of a rate.
     */
    @Test
    public void testMomentLambdaEqualsKleinmanBetaBinomialEstimator() {
        final double[][] keys = {{1, 1, 1, 0, 1}, {0, 0, 1, 0}, {1, 1, 0}, {0, 0, 0, 0, 0, 1}};
        double n = 0, sum = 0, sumSq = 0, sumSqOverN = 0, sumNSq = 0;
        for (final double[] k : keys) {
            double s = 0;
            for (final double y : k) s += y;
            n += k.length;
            sum += s;
            sumSq += s; // y² = y
            sumSqOverN += s * s / k.length;
            sumNSq += (double) k.length * k.length;
        }
        final int K = keys.length;
        final double lambda = Shrinkage.lambdaFromMoments(K, n, sum, sumSq, sumSqOverN, sumNSq);
        // Kleinman (1973) via the ANOVA mean squares
        final double grand = sum / n;
        double bss = 0, wss = 0;
        for (final double[] k : keys) {
            double s = 0;
            for (final double y : k) s += y;
            final double p = s / k.length;
            bss += k.length * (p - grand) * (p - grand);
            for (final double y : k) wss += (y - p) * (y - p);
        }
        final double bms = bss / (K - 1), wms = wss / (n - K);
        final double n0 = (n - sumNSq / n) / (K - 1);
        final double rho = (bms - wms) / (bms + (n0 - 1) * wms);
        Assertions.assertEquals((1 - rho) / rho, lambda, 1e-9);
    }

}
