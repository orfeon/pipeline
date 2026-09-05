package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class JointFitTest {

    private static final List<JointFit.Level> LEVELS = List.of(
            new JointFit.Level("a_b", List.of("a", "b")),
            new JointFit.Level("a", List.of("a")),
            new JointFit.Level("b", List.of("b")),
            new JointFit.Level(Shrinkage.GLOBAL, List.of()));

    private static JointFit.Cell cell(final String a, final String b, final double n, final double mean) {
        return new JointFit.Cell(FeatureValues.keyOf(List.of(a, b)), n, n * mean, n * mean * mean);
    }

    private static Map<String, Object> row(final String a, final String b) {
        final Map<String, Object> row = new HashMap<>();
        row.put("a", a);
        row.put("b", b);
        return row;
    }

    @Test
    public void testBalancedDesignRecoversAnovaDecomposition() {
        // cell means μ + α_a + β_b + γ_ab with Σα = Σβ = Σγ (per row / column) = 0; a negligible ridge leaves the
        // ANOVA terms: on a balanced design the joint solve is exactly "cell − row − column + grand"
        final double mu = 0.5, alpha = 0.1, beta = 0.2, gamma = 0.05;
        final List<JointFit.Cell> cells = List.of(
                cell("a1", "b1", 10, mu + alpha + beta + gamma), cell("a1", "b2", 10, mu + alpha - beta - gamma),
                cell("a2", "b1", 10, mu - alpha + beta - gamma), cell("a2", "b2", 10, mu - alpha - beta + gamma));
        final JointFit.Solution s = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.identity, "fixed", 1e-9);
        Assertions.assertEquals(mu, s.mu, 1e-6);
        Assertions.assertEquals(alpha, s.effects.get(1).get(FeatureValues.keyOf(List.of("a1"))), 1e-6);
        Assertions.assertEquals(-alpha, s.effects.get(1).get(FeatureValues.keyOf(List.of("a2"))), 1e-6);
        Assertions.assertEquals(beta, s.effects.get(2).get(FeatureValues.keyOf(List.of("b1"))), 1e-6);
        Assertions.assertEquals(gamma, s.effects.get(0).get(FeatureValues.keyOf(List.of("a1", "b1"))), 1e-6);
        Assertions.assertEquals(-gamma, s.effects.get(0).get(FeatureValues.keyOf(List.of("a1", "b2"))), 1e-6);
        final JointFit fit = new JointFit(LEVELS, Shrinkage.Scale.identity, s, null, null, null);
        Assertions.assertEquals(mu + alpha + beta + gamma, fit.estimate(s, row("a1", "b1")), 1e-6);
        // an unseen context contributes 0: a new b falls back to the a main effect, an unseen everything to the intercept
        Assertions.assertEquals(mu + alpha, fit.estimate(s, row("a1", "b3")), 1e-6);
        Assertions.assertEquals(mu + alpha + beta, fit.estimate(s, row("a1", "b1")) - fit.effect(s, 0, row("a1", "b1")), 1e-6);
        Assertions.assertEquals(mu, fit.estimate(s, row("a9", "b9")), 1e-6);
        Assertions.assertNull(fit.estimate(s, row(null, "b1")));
        Assertions.assertEquals(40.0, s.rows, 1e-9);
        Assertions.assertEquals(10 + 1e-9, fit.effectiveN(s, row("a1", "b1")), 1e-12);
    }

    /** Gaussian elimination on the ridge normal equations (Xᵀ W X + Λ) β = Xᵀ W z, the reference the iteration must reach. */
    private static double[] denseRidge(final List<JointFit.Cell> cells, final double[] lambdas, final Map<String, Integer>[] dictionaries) {
        final List<String> cellKeys = JointFit.cellKeysOf(LEVELS);
        final int L = lambdas.length;
        int dimension = 1;
        final int[] offsets = new int[L];
        for (int l = 0; l < L; l++) {
            offsets[l] = dimension;
            dimension += dictionaries[l].size();
        }
        final double[][] m = new double[dimension][dimension];
        final double[] rhs = new double[dimension];
        for (final JointFit.Cell c : cells) {
            final List<String> components = FeatureValues.keyComponents(c.key());
            final int[] columns = new int[L + 1];
            columns[0] = 0;
            for (int l = 0; l < L; l++) {
                final List<String> parts = new ArrayList<>();
                for (final String k : LEVELS.get(l).keys()) parts.add(components.get(cellKeys.indexOf(k)));
                columns[l + 1] = offsets[l] + dictionaries[l].get(FeatureValues.keyOf(parts));
            }
            final double z = c.sum() / c.n(), w = c.n();
            for (final int i : columns) {
                rhs[i] += w * z;
                for (final int j : columns) m[i][j] += w;
            }
        }
        for (int l = 0; l < L; l++) for (int k = 0; k < dictionaries[l].size(); k++) m[offsets[l] + k][offsets[l] + k] += lambdas[l];
        // elimination with partial pivoting
        for (int p = 0; p < dimension; p++) {
            int pivot = p;
            for (int i = p + 1; i < dimension; i++) if (Math.abs(m[i][p]) > Math.abs(m[pivot][p])) pivot = i;
            final double[] t = m[p]; m[p] = m[pivot]; m[pivot] = t;
            final double tr = rhs[p]; rhs[p] = rhs[pivot]; rhs[pivot] = tr;
            for (int i = p + 1; i < dimension; i++) {
                final double f = m[i][p] / m[p][p];
                for (int j = p; j < dimension; j++) m[i][j] -= f * m[p][j];
                rhs[i] -= f * rhs[p];
            }
        }
        final double[] beta = new double[dimension];
        for (int i = dimension - 1; i >= 0; i--) {
            double s = rhs[i];
            for (int j = i + 1; j < dimension; j++) s -= m[i][j] * beta[j];
            beta[i] = s / m[i][i];
        }
        return beta;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUnbalancedDesignMatchesDenseRidgeSolve() {
        final Random random = new Random(7);
        final List<JointFit.Cell> cells = new ArrayList<>();
        final String[] as = {"a1", "a2", "a3"}, bs = {"b1", "b2", "b3", "b4"};
        for (final String a : as) {
            for (final String b : bs) {
                if (random.nextDouble() < 0.25) continue; // missing cells: the lattice is unbalanced and confounded
                cells.add(cell(a, b, 1 + random.nextInt(50), random.nextGaussian()));
            }
        }
        final double priorWeight = 3.5;
        final JointFit.Solution s = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.identity, "fixed", priorWeight);
        final Map<String, Integer>[] dictionaries = new Map[3];
        for (int l = 0; l < 3; l++) {
            dictionaries[l] = new TreeMap<>();
            for (final String key : new TreeSet<>(s.effects.get(l).keySet())) dictionaries[l].put(key, dictionaries[l].size());
        }
        final double[] beta = denseRidge(cells, new double[]{priorWeight, priorWeight, priorWeight}, dictionaries);
        Assertions.assertEquals(beta[0], s.mu, 1e-7, "intercept");
        int i = 1;
        for (int l = 0; l < 3; l++) {
            for (final String key : dictionaries[l].keySet()) {
                Assertions.assertEquals(beta[i++], s.effects.get(l).get(key), 1e-7, "level " + l + " " + key);
            }
        }
        Assertions.assertTrue(s.iterations < JointFit.MAX_ITERATIONS, "converged in " + s.iterations);
    }

    @Test
    public void testJointSeparatesConfoundedMainEffects() {
        // a1 almost always comes with b1, a2 with b2; the outcome depends on a only. Marginal (sequential-style)
        // estimates make b look as strong as a; the joint solve attributes the signal to a and leaves b ≈ 0
        final List<JointFit.Cell> cells = List.of(
                cell("a1", "b1", 100, 1.0), cell("a2", "b2", 100, 0.0),
                cell("a1", "b2", 3, 1.0), cell("a2", "b1", 3, 0.0));
        final List<JointFit.Level> mains = List.of(
                new JointFit.Level("a", List.of("a")), new JointFit.Level("b", List.of("b")), new JointFit.Level(Shrinkage.GLOBAL, List.of()));
        final JointFit.Solution s = JointFit.solve(mains, JointFit.cellKeysOf(mains), cells, Shrinkage.Scale.identity, "fixed", 0.01);
        final double a = s.effects.get(0).get(FeatureValues.keyOf(List.of("a1"))) - s.effects.get(0).get(FeatureValues.keyOf(List.of("a2")));
        final double b = s.effects.get(1).get(FeatureValues.keyOf(List.of("b1"))) - s.effects.get(1).get(FeatureValues.keyOf(List.of("b2")));
        Assertions.assertEquals(1.0, a, 1e-2);
        Assertions.assertEquals(0.0, b, 1e-2);
        // the marginal of b1 (103 rows, 100 of them a1) is 0.97: what a per-level estimate would attribute to b
        Assertions.assertEquals(100.0 / 103, (100 * 1.0 + 3 * 0.0) / 103, 1e-9);
    }

    @Test
    public void testVarianceComponentsFixLevelsWithoutSignalAtZero() {
        // b carries no between-context variance: its moment estimate truncates (λ = ∞) and its effects are fixed at 0
        final List<JointFit.Cell> cells = List.of(
                cell("a1", "b1", 20, 0.8), cell("a1", "b2", 20, 0.8),
                cell("a2", "b1", 20, 0.2), cell("a2", "b2", 20, 0.2));
        final JointFit.Solution s = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.identity, "varianceComponents", 5);
        Assertions.assertTrue(Double.isInfinite(s.lambdas[2]), Arrays.toString(s.lambdas));
        Assertions.assertEquals(0.0, s.effects.get(2).get(FeatureValues.keyOf(List.of("b1"))), 0.0);
        Assertions.assertTrue(s.effects.get(1).get(FeatureValues.keyOf(List.of("a1"))) > 0.2, s.effects.toString());
    }

    @Test
    public void testLogitScaleUsesDeltaMethodWeights() {
        final List<JointFit.Cell> cells = List.of(cell("a1", "b1", 50, 0.9), cell("a2", "b1", 50, 0.5));
        final List<JointFit.Level> level = List.of(new JointFit.Level("a", List.of("a")), new JointFit.Level(Shrinkage.GLOBAL, List.of()));
        final JointFit.Solution s = JointFit.solve(level, List.of("a", "b"), cells, Shrinkage.Scale.logit, "fixed", 1e-9);
        final JointFit fit = new JointFit(level, Shrinkage.Scale.logit, s, null, null, null);
        // with a negligible ridge every cell is reproduced on the original scale, whatever the weights
        Assertions.assertEquals(0.9, fit.estimate(s, row("a1", "b1")), 1e-6);
        Assertions.assertEquals(0.5, fit.estimate(s, row("a2", "b1")), 1e-6);
        Assertions.assertEquals(50 * 0.9 * 0.1, JointFit.varianceFactor(Shrinkage.Scale.logit, 0.9) * 50, 1e-12);
        Assertions.assertEquals(2.0, JointFit.varianceFactor(Shrinkage.Scale.log, 2.0), 1e-12);
    }

    @Test
    public void testPriorWeightIsARowPseudoCountOnEveryScale() {
        // one level over the intercept: the joint solve is the closed form e = w / (w + λ v̄) · (z − μ) — with the ridge in
        // the units of the context's weights the shrink weight is n / (n + λ), as under backoff / sequential, so a leaf
        // with n = 20 rows and priorWeight 20 keeps half its deviation whatever the outcome rate
        final List<JointFit.Level> level = List.of(new JointFit.Level("a", List.of("a")), new JointFit.Level(Shrinkage.GLOBAL, List.of()));
        final double n = 20, lambda = 20;
        for (final double p : new double[]{0.1, 0.5}) {
            // a2 anchors the intercept with a huge, near-unshrinkable cell so μ ≈ logit(0.3)
            final List<JointFit.Cell> cells = List.of(cell("a1", "b1", n, p), cell("a2", "b1", 1e9, 0.3));
            final JointFit.Solution s = JointFit.solve(level, List.of("a", "b"), cells, Shrinkage.Scale.logit, "fixed", lambda);
            final double expected = n / (n + lambda) * (Shrinkage.transform(Shrinkage.Scale.logit, p) - s.mu);
            Assertions.assertEquals(expected, s.effects.get(0).get(FeatureValues.keyOf(List.of("a1"))), 1e-6, "p = " + p);
        }
        // identity: a plain ridge, the same closed form with v̄ = 1
        final List<JointFit.Cell> cells = List.of(cell("a1", "b1", n, 2.0), cell("a2", "b1", 1e9, 1.0));
        final JointFit.Solution s = JointFit.solve(level, List.of("a", "b"), cells, Shrinkage.Scale.identity, "fixed", lambda);
        Assertions.assertEquals(0.5 * (2.0 - s.mu), s.effects.get(0).get(FeatureValues.keyOf(List.of("a1"))), 1e-6);
    }

    @Test
    public void testNullCoarseKeyKeepsTheRowInTheLevelsItHas() {
        // chain (a, b) → a → global: a cell whose b is null still informs the intercept and the a effect; it has no
        // leaf indicator and no leaf context (the cross lattice LEVELS would split a1's signal between a and b by
        // sweep order — a rank-deficient design, not what this test is about)
        final List<JointFit.Level> chain = List.of(LEVELS.get(0), LEVELS.get(1), LEVELS.get(3));
        final Map<String, Object> nullB = row("a1", null);
        final String nullCell = FeatureValues.keyWithNulls(nullB, List.of("a", "b"));
        Assertions.assertEquals(Arrays.asList("a1", null), FeatureValues.keyComponents(nullCell));
        final List<JointFit.Cell> cells = List.of(
                cell("a1", "b1", 10, 1.0), new JointFit.Cell(nullCell, 30, 30 * 1.0, 30 * 1.0),
                cell("a2", "b1", 10, 0.0), cell("a2", "b2", 10, 0.0));
        final JointFit.Solution s = JointFit.solve(chain, JointFit.cellKeysOf(chain), cells, Shrinkage.Scale.identity, "fixed", 1e-9);
        Assertions.assertEquals(60.0, s.rows, 1e-9, "the null-b rows count");
        Assertions.assertEquals(2.0 / 3, s.mu, 1e-6, "μ over every row, the null-b ones included");
        Assertions.assertEquals(1.0 / 3, s.effects.get(1).get(FeatureValues.keyOf(List.of("a1"))), 1e-6, "a1 fitted on 40 rows");
        Assertions.assertEquals(Set.of(FeatureValues.keyOf(List.of("a1", "b1")), FeatureValues.keyOf(List.of("a2", "b1")), FeatureValues.keyOf(List.of("a2", "b2"))),
                s.effects.get(0).keySet(), "no leaf context for a null b");
        Assertions.assertEquals(10.0, s.leafN.get(FeatureValues.keyOf(List.of("a1", "b1"))), 1e-9);
        Assertions.assertFalse(s.leafN.containsKey(nullCell), "no leaf context without the leaf key");
        final JointFit fit = new JointFit(chain, Shrinkage.Scale.identity, s, null, null, null);
        // the leaf (a, b) is null for such a row at apply time too → no estimate; a row of a1 with a fresh b reads μ + e_a1
        Assertions.assertNull(fit.estimate(s, nullB));
        Assertions.assertEquals(1.0, fit.estimate(s, row("a1", "b9")), 1e-6);
        Assertions.assertEquals(0.0, fit.estimate(s, row("a2", "b9")), 1e-6);
    }

    @Test
    public void testFoldAndForwardVariants() {
        final String x = FeatureValues.keyOf(List.of("a1", "b1")), y = FeatureValues.keyOf(List.of("a2", "b1"));
        // fold: every row of cell x is in fold 0, of cell y in fold 1
        final List<JointFit.Cell> entries = List.of(
                new JointFit.Cell(x, 10, 10, 10), new JointFit.Cell(JointFit.foldEntry(0, x), 10, 10, 10),
                new JointFit.Cell(y, 10, 0, 0), new JointFit.Cell(JointFit.foldEntry(1, y), 10, 0, 0));
        final JointFit fold = JointFit.fit(LEVELS, Shrinkage.Scale.identity, "fixed", 1, entries, 2, false, 0);
        Assertions.assertEquals(2, fold.folds.length);
        // out of fold 0 only cell y remains: its intercept is y's mean and x is unseen (0 effects) → a fold-0 row of x reads y's mean
        Assertions.assertEquals(0.0, fold.estimate(fold.solutionFor(0, null, 1), row("a1", "b1")), 1e-9);
        Assertions.assertEquals(1.0, fold.estimate(fold.solutionFor(1, null, 1), row("a2", "b1")), 1e-9);
        // a row without a fold unit reads the whole-input solution (both cells)
        final double total = fold.estimate(fold.solutionFor(null, null, 1), row("a1", "b1"));
        Assertions.assertTrue(total > 0.5 && total < 1.0, Double.toString(total));

        // forward: block 1 holds x, block 2 holds y; the block-2 solution is cumulative, windowBlocks 1 keeps the current block only
        final List<JointFit.Cell> blocks = List.of(new JointFit.Cell(JointFit.blockEntry(x, 1), 10, 10, 10), new JointFit.Cell(JointFit.blockEntry(y, 2), 10, 0, 0));
        final JointFit forward = JointFit.fit(LEVELS, Shrinkage.Scale.identity, "fixed", 1, blocks, 0, true, 0);
        Assertions.assertEquals(Set.of(1L, 2L), forward.blocks.keySet());
        Assertions.assertNull(forward.solutionFor(null, 0L, 1), "nothing before the first block");
        Assertions.assertEquals(1.0, forward.estimate(forward.solutionFor(null, 1L, 1), row("a1", "b1")), 1e-9);
        Assertions.assertEquals(1.0, forward.total.rows / 20, 1e-9);
        Assertions.assertEquals(20.0, forward.solutionFor(null, 5L, 1).rows, 1e-9);
        Assertions.assertNull(forward.solutionFor(null, 1L, 2), "minBlocks 2 needs two solved blocks");
        Assertions.assertNotNull(forward.solutionFor(null, 2L, 2));
        final JointFit windowed = JointFit.fit(LEVELS, Shrinkage.Scale.identity, "fixed", 1, blocks, 0, true, 1);
        Assertions.assertEquals(10.0, windowed.solutionFor(null, 2L, 1).rows, 1e-9);
        // the window is anchored to the row's usable block, not to the last observed block: a row past the window of
        // every block reads nothing (the encoding path's rule), a row inside block 2's window reads block 2 only
        Assertions.assertEquals(Set.of(1L, 2L), windowed.observedBlocks);
        Assertions.assertEquals(Set.of(1L, 2L, 3L), windowed.blocks.keySet(), "change points: blocks enter at 1, 2 and leave at 2, 3");
        Assertions.assertNull(windowed.solutionFor(null, 3L, 1), "block 2 left the window at usable block 3");
        Assertions.assertNull(windowed.solutionFor(null, 7L, 1));
        Assertions.assertEquals(20.0, forward.solutionFor(null, 7L, 1).rows, 1e-9, "no window: cumulative");
        // W = 3 over blocks 1, 2: usable 4 reads block 2 only ((1, 4]), usable 5 reads nothing, minBlocks counts observed blocks
        final JointFit wide = JointFit.fit(LEVELS, Shrinkage.Scale.identity, "fixed", 1, blocks, 0, true, 3);
        Assertions.assertEquals(20.0, wide.solutionFor(null, 3L, 1).rows, 1e-9);
        Assertions.assertEquals(10.0, wide.solutionFor(null, 4L, 1).rows, 1e-9);
        Assertions.assertEquals(0.0, wide.estimate(wide.solutionFor(null, 4L, 1), row("a2", "b1")), 1e-9);
        Assertions.assertNull(wide.solutionFor(null, 5L, 1));
        Assertions.assertNull(wide.solutionFor(null, 4L, 3), "two observed blocks, minBlocks 3");
    }

    @Test
    public void testArtifactRoundTrip() {
        final List<JointFit.Cell> cells = List.of(cell("a1", "b1", 10, 0.9), cell("a1", "b2", 4, 0.3), cell("a2", "b1", 7, 0.5));
        final JointFit fit = JointFit.fit(LEVELS, Shrinkage.Scale.logit, "varianceComponents", 2, cells, 0, false, 0);
        final String dir = "target/feature-artifacts/" + UUID.randomUUID(); // relative: Beam FileSystems treats a Windows drive letter as a scheme
        JointFit.write(dir, "hash", "enc__a_b__e1", fit);
        Assertions.assertTrue(JointFit.exists(dir, "hash", "enc__a_b__e1"));
        final JointFit back = JointFit.read(dir, "hash", "enc__a_b__e1", LEVELS, Shrinkage.Scale.logit);
        for (final String[] r : new String[][]{{"a1", "b1"}, {"a1", "b2"}, {"a2", "b1"}, {"a2", "b2"}, {"a3", "b1"}}) {
            Assertions.assertEquals(fit.estimate(fit.total, row(r[0], r[1])), back.estimate(back.total, row(r[0], r[1])), 1e-12, Arrays.toString(r));
            Assertions.assertEquals(fit.effectiveN(fit.total, row(r[0], r[1])), back.effectiveN(back.total, row(r[0], r[1])), 1e-12);
        }
        Assertions.assertArrayEquals(fit.total.lambdas, back.total.lambdas, 1e-12);
        final String manifest = new String(com.mercari.solution.util.domain.file.ResourceUtil.readBytes(JointFit.manifestPath(dir, "hash", "enc__a_b__e1")), java.nio.charset.StandardCharsets.UTF_8);
        Assertions.assertTrue(manifest.contains("\"estimator\":\"joint\""), manifest);
        Assertions.assertTrue(manifest.contains("\"lambdas\""), manifest);
        // a fully shrunk level (λ = ∞) must not leave a bare Infinity token: the manifest stays strict JSON
        final List<JointFit.Cell> flat = List.of(cell("a1", "b1", 20, 0.8), cell("a1", "b2", 20, 0.8), cell("a2", "b1", 20, 0.2), cell("a2", "b2", 20, 0.2));
        final JointFit infinite = JointFit.fit(LEVELS, Shrinkage.Scale.identity, "varianceComponents", 5, flat, 0, false, 0);
        Assertions.assertTrue(Double.isInfinite(infinite.total.lambdas[2]));
        JointFit.write(dir, "hash", "enc__a_b__e2", infinite);
        final String strict = new String(com.mercari.solution.util.domain.file.ResourceUtil.readBytes(JointFit.manifestPath(dir, "hash", "enc__a_b__e2")), java.nio.charset.StandardCharsets.UTF_8);
        final com.google.gson.stream.JsonReader reader = new com.google.gson.stream.JsonReader(new java.io.StringReader(strict));
        reader.setStrictness(com.google.gson.Strictness.STRICT);
        Assertions.assertEquals("Infinity", com.google.gson.JsonParser.parseReader(reader).getAsJsonObject().getAsJsonObject("lambdas").get("b").getAsString(), strict);
        Assertions.assertTrue(Double.isInfinite(JointFit.read(dir, "hash", "enc__a_b__e2", LEVELS, Shrinkage.Scale.identity).total.lambdas[2]), "the avro artifact keeps the double");
    }

    @Test
    public void testLevelEncodingAndKeyComponents() {
        Assertions.assertEquals("a_b=a,b;a=a;b=b;global=", JointFit.encodeLevels(LEVELS));
        Assertions.assertEquals(LEVELS, JointFit.parseLevels(JointFit.encodeLevels(LEVELS)));
        Assertions.assertEquals(List.of("a", "b"), JointFit.cellKeysOf(LEVELS));
        // the length prefix makes separators inside values harmless
        final List<String> components = List.of("x:1", "", "pq", "12:34");
        Assertions.assertEquals(components, FeatureValues.keyComponents(FeatureValues.keyOf(components)));
        final Map<String, Object> row = new HashMap<>(Map.of("a", "x:1", "b", 12));
        Assertions.assertEquals(List.of("x:1", "12"), FeatureValues.keyComponents(FeatureValues.key(row, List.of("a", "b"))));
    }

}
