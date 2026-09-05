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
