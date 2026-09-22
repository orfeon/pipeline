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
     * Leave-node-out follows the back-off. A row whose declared leaf is empty (a key never seen, a null component of a
     * {@code structure: sequence} path) starts from the deepest level that has rows, and that level's rows are what
     * the levels above it contain: the row reads exactly what the lattice declared from that level reads. Subtracting
     * the (empty) declared leaf instead would shrink the level toward ancestors that still hold its own rows.
     */
    @Test
    public void testLeaveNodeOutSubtractsTheEffectiveLeaf() {
        final Shrinkage shrinkage = Shrinkage.of(Shrinkage.Scale.identity, 2, true);
        final Shrinkage.Level path3 = new Shrinkage.Level("k1_k2_k3", "p3_n", "p3_sum", null);
        final Shrinkage.Level path2 = new Shrinkage.Level("k1_k2", "p2_n", "p2_sum", null);
        final Shrinkage.Level path1 = new Shrinkage.Level("k1", "p1_n", "p1_sum", null);
        final Shrinkage.Level global = new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", null);
        final List<Shrinkage.Level> full = List.of(path3, path2, path1, global);
        final List<Shrinkage.Level> suffix = List.of(path2, path1, global);

        // the oldest step is missing: no p3 statistics at all (the hidden columns of a null key read null)
        final Map<String, Object> row = Map.of("p2_n", 2.0, "p2_sum", 2.0, "p1_n", 6.0, "p1_sum", 3.0, "g_n", 16.0, "g_sum", 4.0);
        // by hand: root = (4 − 2) / (16 − 2) = 1/7; k1 = (3 − 2) / (6 − 2) = 1/4 with w = 4 / 6; leaf = 1 with w = 2 / 4
        final double root = 1.0 / 7, k1 = root + 4.0 / 6 * (0.25 - root), expected = k1 + 0.5 * (1 - k1);
        final Shrinkage.Composition c = shrinkage.compose(row, full, null);
        Assertions.assertEquals(expected, c.value(), 1e-12);
        final Shrinkage.Composition declared = shrinkage.compose(row, suffix, null);
        Assertions.assertEquals(declared.value(), c.value(), 0d, "the same value as the lattice declared from the suffix");
        Assertions.assertEquals(declared.effectiveN(), c.effectiveN(), 0d);
        Assertions.assertArrayEquals(new Double[]{0d, declared.deviations()[0], declared.deviations()[1], declared.deviations()[2]}, c.deviations());
        // a leaf seen with n = 0 is the same row
        final Map<String, Object> unseen = new java.util.HashMap<>(row);
        unseen.put("p3_n", 0.0);
        unseen.put("p3_sum", 0.0);
        Assertions.assertEquals(expected, shrinkage.compose(unseen, full, null).value(), 0d);
        Assertions.assertEquals(1, Shrinkage.effectiveLeaf(row, full));

        // a leaf with rows is untouched: its statistics leave every ancestor, the intermediate levels keep theirs
        final Map<String, Object> seen = new java.util.HashMap<>(row);
        seen.put("p3_n", 1.0);
        seen.put("p3_sum", 0.0);
        final double rootSeen = 4.0 / 15, k1Seen = rootSeen + 5.0 / 7 * (3.0 / 5 - rootSeen), k2Seen = k1Seen + 1.0 / 3 * (2.0 - k1Seen);
        Assertions.assertEquals(k2Seen + 1.0 / 3 * (0 - k2Seen), shrinkage.compose(seen, full, null).value(), 1e-12);
        Assertions.assertEquals(0, Shrinkage.effectiveLeaf(seen, full));

        // only the root has rows: nothing to subtract, the row reads the global mean; an empty lattice reads null
        Assertions.assertEquals(0.25, shrinkage.compose(Map.of("g_n", 16.0, "g_sum", 4.0), full, null).value(), 0d);
        Assertions.assertNull(shrinkage.compose(Map.of(), full, null).value());
        // leaveNodeOut: false subtracts nothing at any level
        final double plainK1 = 0.25 + 6.0 / 8 * (0.5 - 0.25);
        Assertions.assertEquals(plainK1 + 0.5 * (1 - plainK1), Shrinkage.of(Shrinkage.Scale.identity, 2, false).compose(row, full, null).value(), 1e-12);

        // an additive entry keeps the declared leaf: the main-effect chains subtract the cell they generalise, and an
        // empty cell has nothing to subtract — A keeps its rows against the root, as before
        final List<Shrinkage.Level> additive = List.of(
                new Shrinkage.Level("cell", "c_n", "c_sum", null),
                new Shrinkage.Level(Shrinkage.ADDITIVE, null, null, List.of(List.of(path1, global))),
                global);
        Assertions.assertEquals(0, Shrinkage.effectiveLeaf(row, additive));
        Assertions.assertEquals(0.25 + 6.0 / 8 * (0.5 - 0.25), shrinkage.compose(row, additive, null).value(), 1e-12);

        // ... and it keeps it even when a coarser level of the chain has rows (§5.3.1: an empty cell over a coarse
        // cross). That level is contained in no main-effect level, so subtracting it there would take the main level's
        // n below zero and drop the main effect: the cell — empty, nothing to subtract — stays the leave-node-out node
        final Shrinkage.Level mainEffect = new Shrinkage.Level("k2", "m_n", "m_sum", null);
        final List<Shrinkage.Level> coarseAdditive = List.of(
                new Shrinkage.Level("k1_k2", "c_n", "c_sum", null),
                path1,
                new Shrinkage.Level(Shrinkage.ADDITIVE, null, null, List.of(List.of(mainEffect, global))),
                global);
        final Map<String, Object> coarse = new java.util.HashMap<>(row);
        coarse.put("m_n", 4.0);
        coarse.put("m_sum", 3.0);
        Assertions.assertEquals(0, Shrinkage.effectiveLeaf(coarse, coarseAdditive));
        // marginal 4/16, main effect 3/4 with w = 4/6, the coarse cross 3/6 with w = 6/8 — all against full ancestors
        final double marginal = 0.25, additivePrediction = marginal + 4.0 / 6 * (0.75 - marginal);
        Assertions.assertEquals(additivePrediction + 6.0 / 8 * (0.5 - additivePrediction),
                shrinkage.compose(coarse, coarseAdditive, null).value(), 1e-12);

        // distributions follow the same rule
        final Shrinkage dm = Shrinkage.of(Shrinkage.Scale.identity, 2, true, Shrinkage.Family.dirichletMultinomial);
        final Map<String, Object> dist = Map.of(
                "p2_n", 2.0, "p2_sum", Map.of("x", 1.0),
                "p1_n", 6.0, "p1_sum", Map.of("x", 0.5, "y", 0.5),
                "g_n", 16.0, "g_sum", Map.of("x", 0.25, "y", 0.75));
        final Map<String, Double> shrunk = dm.composeDistribution(dist, full, null).distribution();
        Assertions.assertEquals(dm.composeDistribution(dist, suffix, null).distribution(), shrunk);
        // root x = (4 − 2) / 14, k1 x = (3 − 2) / 4 with w = 4 / 6, leaf x = 1 with w = 2 / 4: the scalar chain above
        Assertions.assertEquals(expected, shrunk.get("x"), 1e-12);
    }

    /**
     * A baseline offset on a logit scale: each level's own term is the score-type one-step estimate S / V from the
     * hidden Σ(y − b) and Σ b(1 − b), the leaf shrinks toward the parent's term by information (V / (V + λ′), λ′ = the
     * pseudo-count in rows times the root's information per row; an estimated λ is on the score scale already), and
     * the composed value is the term itself (not a probability). On the identity scale nothing changes (Σ(y − b) / n).
     */
    @Test
    public void testOffsetOnLogitScaleComposesScoreTerm() {
        final Shrinkage logit = Shrinkage.of(Shrinkage.Scale.logit, 2, true);
        final List<Shrinkage.Level> levels = List.of(
                new Shrinkage.Level("seller", "s_n", "s_sum", "s_off", "s_info", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", "g_off", "g_info", null));
        // leaf: n = 4, S = Σ(y − b) = 1, Σb = 2, V = Σ b(1 − b) = 0.8; the global totals contain the leaf: n = 10, S = 0,
        // Σb = 6, V = 2 → without the leaf n = 6, S = −1, V = 1.2
        final Map<String, Object> row = Map.of("s_n", 4.0, "s_sum", 1.0, "s_off", 2.0, "s_info", 0.8,
                "g_n", 10.0, "g_sum", 0.0, "g_off", 6.0, "g_info", 2.0);
        final double own = 1.0 / 0.8;
        final double root = -1.0 / 1.2;
        final double lambda = 2 * (2.0 / 10);            // priorWeight rows × the root's information per row
        final double w = 0.8 / (0.8 + lambda);
        final Shrinkage.Composition c = logit.compose(row, levels, null);
        Assertions.assertEquals(root + w * (own - root), c.value(), 1e-12);
        Assertions.assertEquals(w * (own - root), c.deviations()[0], 1e-12);
        Assertions.assertTrue(c.value() > 0, "a key whose observed rate beats its baseline has a positive log-odds term");
        // the effective sample size stays in rows: n plus the pseudo-count in rows backed by the parent
        Assertions.assertEquals(4 + 2 * Math.min(1, 6 / 2.0), c.effectiveN(), 1e-12);
        // an estimated λ (variance components) is 1 / τ² on the score scale: used as is
        final double estimated = 0.5;
        final double wEstimated = 0.8 / (0.8 + estimated);
        Assertions.assertEquals(root + wEstimated * (own - root), logit.compose(row, levels, Map.of("s_n", estimated)).value(), 1e-12);

        // the levels encode / parse with the offset and information columns (on log the information is Σb itself)
        final List<Shrinkage.Level> parsed = Shrinkage.parseLevels(Shrinkage.encodeLevels(levels));
        Assertions.assertEquals("s_off", parsed.get(0).offColumn());
        Assertions.assertEquals("s_info", parsed.get(0).infoColumn());
        Assertions.assertEquals("g_info", parsed.get(1).infoColumn());
        final Shrinkage.Level onLog = new Shrinkage.Level("seller", "s_n", "s_sum", "s_off", "s_off", null);
        Assertions.assertEquals("s_off", Shrinkage.parseLevels(Shrinkage.encodeLevels(List.of(onLog))).get(0).infoColumn());
        final Shrinkage.Level plainLevel = new Shrinkage.Level("seller", "s_n", "s_sum", null);
        Assertions.assertNull(Shrinkage.parseLevels(Shrinkage.encodeLevels(List.of(plainLevel))).get(0).offColumn());
        Assertions.assertNull(Shrinkage.parseLevels(Shrinkage.encodeLevels(List.of(plainLevel))).get(0).infoColumn());

        // identity: the mean residual from the plain sums (an identity offset registers no offset columns)
        final Shrinkage identity = Shrinkage.of(Shrinkage.Scale.identity, 2, true);
        final List<Shrinkage.Level> plain = List.of(
                new Shrinkage.Level("seller", "s_n", "s_sum", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", null));
        final double ownId = 1.0 / 4, rootId = -1.0 / 6, wId = 4.0 / (4 + 2);
        Assertions.assertEquals(rootId + wId * (ownId - rootId), identity.compose(row, plain, null).value(), 1e-12);
    }

    /**
     * The score-type term of a cell without a single success is finite and bounded: with every baseline at b̄ it is
     * −1 / (1 − b̄) whatever n (the transformed mean read the clamp's −13.8 there, growing with n), and the composed
     * value approaches it from 0 as the key's information grows against the prior's — a key of rare events (small
     * b(1 − b)) is trusted less than a key of the same row count at even odds.
     */
    @Test
    public void testScoreTermOfCellsWithoutSuccessIsBounded() {
        final double b = 0.067;
        final Shrinkage logit = Shrinkage.of(Shrinkage.Scale.logit, 30, true);
        final List<Shrinkage.Level> levels = List.of(
                new Shrinkage.Level("horse", "h_n", "h_sum", "h_off", "h_info", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", "g_off", "g_info", null));
        // the root: 10 000 rows at the market rate (S = 0), the key's rows contained in them
        final double info = Shrinkage.information(Shrinkage.Scale.logit, b);
        double previous = 0;
        for (final int n : new int[]{1, 2, 4, 8, 64, 1_000_000}) {
            final Map<String, Object> row = Map.of("h_n", (double) n, "h_sum", -n * b, "h_off", n * b, "h_info", n * info,
                    "g_n", 10_000d + n, "g_sum", -n * b, "g_off", (10_000 + n) * b, "g_info", (10_000 + n) * info);
            final double value = logit.compose(row, levels, null).value();
            Assertions.assertTrue(Double.isFinite(value) && value < 0, n + " rows: " + value);
            Assertions.assertTrue(value < previous, "monotone in n: " + value + " after " + previous);
            Assertions.assertTrue(value >= -1 / (1 - b) - 1e-9, "bounded by the term itself: " + value);
            // the term: S / V = −1 / (1 − b̄); the weight: information against priorWeight rows of the root's information
            final double w = n * info / (n * info + 30 * info);
            Assertions.assertEquals(-w / (1 - b), value, 1e-9);
            previous = value;
        }
        Assertions.assertEquals(-1 / (1 - b), Shrinkage.ownScore(-8 * b, 8 * info), 1e-12);
        Assertions.assertNull(Shrinkage.ownScore(0, 0), "no information (every baseline at 0 or 1): no term");
        // the same 8 rows at even odds carry 3.7× the information and are trusted accordingly
        final double even = Shrinkage.information(Shrinkage.Scale.logit, 0.5);
        Assertions.assertTrue(even / info > 3.5);
        Assertions.assertEquals(0d, Shrinkage.information(Shrinkage.Scale.logit, 1), 0d);
        Assertions.assertEquals(0.3, Shrinkage.information(Shrinkage.Scale.log, 0.3), 0d);
        Assertions.assertEquals(1d, Shrinkage.information(Shrinkage.Scale.identity, 0.3), 0d);
    }

    /**
     * A level whose rows carry no information (Σb = 0 on log: a cold-start baseline) has no term: it defers to its parent
     * with a zero deviation; with every level uninformative there is no estimate at all. On log the information column
     * is the Σ baseline column itself.
     */
    @Test
    public void testOffsetTermWithoutInformationDefersToParent() {
        final Shrinkage log = Shrinkage.of(Shrinkage.Scale.log, 2, true);
        final List<Shrinkage.Level> levels = List.of(
                new Shrinkage.Level("seller", "s_n", "s_sum", "s_off", "s_off", null),
                new Shrinkage.Level(Shrinkage.GLOBAL, "g_n", "g_sum", "g_off", "g_off", null));
        // leaf: n = 4, Σ(y − b) = 2, Σb = 0 (cold-start baseline); global without the leaf: n = 6, Σ(y − b) = −1, Σb = 8
        final Map<String, Object> row = Map.of("s_n", 4.0, "s_sum", 2.0, "s_off", 0.0, "g_n", 10.0, "g_sum", 1.0, "g_off", 8.0);
        final Shrinkage.Composition c = log.compose(row, levels, null);
        Assertions.assertEquals(-1.0 / 8, c.value(), 1e-12, "the parent's term S / Σb");
        Assertions.assertEquals(0d, c.deviations()[0], 0d);
        Assertions.assertNull(log.compose(Map.of("s_n", 4.0, "s_sum", 2.0, "s_off", 0.0, "g_n", 4.0, "g_sum", 2.0, "g_off", 0.0), levels, null).value());
        Assertions.assertEquals(0.5, Shrinkage.own(Shrinkage.Scale.identity, 4, 2), 0d);

        // the rows a level counts and sums: target and (under an offset) baseline present, NaN missing; a target-less
        // statistic counts every row without consulting the baseline
        Assertions.assertEquals(org.apache.beam.sdk.values.KV.of(0.25, 0.5), FeatureValues.offsetTarget(Map.of("y", 0.75, "b", 0.5), "y", "b"));
        Assertions.assertNull(FeatureValues.offsetTarget(Map.of("y", 0.75), "y", "b"), "a row without a baseline is outside every statistic");
        Assertions.assertNull(FeatureValues.offsetTarget(Map.of("y", 0.75, "b", Double.NaN), "y", "b"));
        Assertions.assertEquals(org.apache.beam.sdk.values.KV.of(0.75, null), FeatureValues.offsetTarget(Map.of("y", 0.75), "y", null));
        Assertions.assertEquals(org.apache.beam.sdk.values.KV.of(0d, null), FeatureValues.offsetTarget(Map.of("y", 0.75), null, "b"));
    }

    /**
     * The score-scale pseudo-count: with every key at the same information V the DerSimonian–Laird moment estimate is
     * τ̂² = sample variance of the terms − 1 / V, and λ′ = 1 / τ̂²; identical terms (no signal) give +∞, one key null.
     */
    @Test
    public void testLambdaFromScoreIsTheMomentEstimateOfTheBetweenKeyVariance() {
        final double v = 40;
        final double[] terms = {0.3, -0.1, 0.5, -0.4, 0.2};
        double sumS = 0, sumS2OverV = 0, mean = 0;
        for (final double t : terms) mean += t / terms.length;
        double variance = 0;
        for (final double t : terms) {
            variance += (t - mean) * (t - mean) / (terms.length - 1);
            sumS += t * v;
            sumS2OverV += t * v * t * v / v;
        }
        final double lambda = Shrinkage.lambdaFromScore(terms.length, terms.length * v, terms.length * v * v, sumS, sumS2OverV);
        Assertions.assertEquals(1 / (variance - 1 / v), lambda, 1e-9);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, Shrinkage.lambdaFromScore(3, 3 * v, 3 * v * v, 3 * 0.2 * v, 3 * 0.2 * v * 0.2 * v / v), "identical terms: no between-key variance");
        Assertions.assertNull(Shrinkage.lambdaFromScore(1, v, v * v, 0.2 * v, 0.04 * v));
        Assertions.assertNull(Shrinkage.lambdaFromScore(3, 0, 0, 0, 0), "no information");
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
