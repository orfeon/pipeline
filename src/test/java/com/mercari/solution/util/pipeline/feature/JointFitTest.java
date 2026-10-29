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

    /**
     * Under an offset term a cell without information (Σb = 0 on log: a cold-start baseline) has no term and is left
     * out of the solve like an empty cell — its rows do not count; the cells with information are fitted as their
     * score-type terms S / V weighted by V, the ridge being the pseudo-count in rows times the fit's information per
     * row. On the identity scale the same cell stays (the residual is defined).
     */
    @Test
    public void testOffsetCellWithoutInformationIsSkipped() {
        final List<JointFit.Cell> cells = List.of(
                new JointFit.Cell(FeatureValues.keyOf(List.of("a1", "b1")), 10, 2, 4, 5),
                new JointFit.Cell(FeatureValues.keyOf(List.of("a2", "b1")), 10, -1, 3, 6),
                new JointFit.Cell(FeatureValues.keyOf(List.of("a1", "b2")), 5, 3, 9, 0));
        final JointFit.Solution log = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.log, true, "fixed", 1);
        Assertions.assertEquals(20, log.rows, 0d, "the Σb = 0 cell's rows are not fitted");
        Assertions.assertNull(log.effects.get(0).get(FeatureValues.keyOf(List.of("a1", "b2"))));
        Assertions.assertEquals(11.0 / 20, log.infoPerRow, 1e-12, "the information per fitted row");
        // the intercept alone (a huge ridge) is the information-weighted mean of the cells' terms: (2 + (−1)) / (5 + 6)
        final JointFit.Solution pooled = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.log, true, "fixed", 1e12);
        Assertions.assertEquals(1.0 / 11, pooled.mu, 1e-9);
        Assertions.assertTrue(Math.abs(log.mu) < 5, "no clamp constant (−27.6) in the intercept: " + log.mu);
        // a logit cell reads its information from Σ b(1 − b)
        final List<JointFit.Cell> logitCells = List.of(
                new JointFit.Cell(FeatureValues.keyOf(List.of("a1", "b1")), 10, 2, 4, 5, 2.5),
                new JointFit.Cell(FeatureValues.keyOf(List.of("a2", "b1")), 10, -1, 3, 6, 0));
        final JointFit.Solution logit = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), logitCells, Shrinkage.Scale.logit, true, "fixed", 1e12);
        Assertions.assertEquals(10, logit.rows, 0d);
        Assertions.assertEquals(2 / 2.5, logit.mu, 1e-9);
        final JointFit.Solution identity = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.identity, true, "fixed", 1);
        Assertions.assertEquals(25, identity.rows, 0d);
    }

    @Test
    public void testBalancedDesignRecoversAnovaDecomposition() {
        // cell means μ + α_a + β_b + γ_ab with Σα = Σβ = Σγ (per row / column) = 0; a negligible ridge leaves the
        // ANOVA terms: on a balanced design the joint solve is exactly "cell − row − column + grand"
        final double mu = 0.5, alpha = 0.1, beta = 0.2, gamma = 0.05;
        final List<JointFit.Cell> cells = List.of(
                cell("a1", "b1", 10, mu + alpha + beta + gamma), cell("a1", "b2", 10, mu + alpha - beta - gamma),
                cell("a2", "b1", 10, mu - alpha + beta - gamma), cell("a2", "b2", 10, mu - alpha - beta + gamma));
        final JointFit.Solution s = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.identity, false, "fixed", 1e-9);
        Assertions.assertEquals(mu, s.mu, 1e-6);
        Assertions.assertEquals(alpha, s.effects.get(1).get(FeatureValues.keyOf(List.of("a1"))), 1e-6);
        Assertions.assertEquals(-alpha, s.effects.get(1).get(FeatureValues.keyOf(List.of("a2"))), 1e-6);
        Assertions.assertEquals(beta, s.effects.get(2).get(FeatureValues.keyOf(List.of("b1"))), 1e-6);
        Assertions.assertEquals(gamma, s.effects.get(0).get(FeatureValues.keyOf(List.of("a1", "b1"))), 1e-6);
        Assertions.assertEquals(-gamma, s.effects.get(0).get(FeatureValues.keyOf(List.of("a1", "b2"))), 1e-6);
        final JointFit fit = new JointFit(LEVELS, Shrinkage.Scale.identity, false, s, null, null, null);
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
        final JointFit.Solution s = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.identity, false, "fixed", priorWeight);
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
        final JointFit.Solution s = JointFit.solve(mains, JointFit.cellKeysOf(mains), cells, Shrinkage.Scale.identity, false, "fixed", 0.01);
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
        final JointFit.Solution s = JointFit.solve(LEVELS, JointFit.cellKeysOf(LEVELS), cells, Shrinkage.Scale.identity, false, "varianceComponents", 5);
        Assertions.assertTrue(Double.isInfinite(s.lambdas[2]), Arrays.toString(s.lambdas));
        Assertions.assertEquals(0.0, s.effects.get(2).get(FeatureValues.keyOf(List.of("b1"))), 0.0);
        Assertions.assertTrue(s.effects.get(1).get(FeatureValues.keyOf(List.of("a1"))) > 0.2, s.effects.toString());
    }

    @Test
    public void testLogitScaleUsesDeltaMethodWeights() {
        final List<JointFit.Cell> cells = List.of(cell("a1", "b1", 50, 0.9), cell("a2", "b1", 50, 0.5));
        final List<JointFit.Level> level = List.of(new JointFit.Level("a", List.of("a")), new JointFit.Level(Shrinkage.GLOBAL, List.of()));
        final JointFit.Solution s = JointFit.solve(level, List.of("a", "b"), cells, Shrinkage.Scale.logit, false, "fixed", 1e-9);
        final JointFit fit = new JointFit(level, Shrinkage.Scale.logit, false, s, null, null, null);
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
            final JointFit.Solution s = JointFit.solve(level, List.of("a", "b"), cells, Shrinkage.Scale.logit, false, "fixed", lambda);
            final double expected = n / (n + lambda) * (Shrinkage.transform(Shrinkage.Scale.logit, p) - s.mu);
            Assertions.assertEquals(expected, s.effects.get(0).get(FeatureValues.keyOf(List.of("a1"))), 1e-6, "p = " + p);
        }
        // identity: a plain ridge, the same closed form with v̄ = 1
        final List<JointFit.Cell> cells = List.of(cell("a1", "b1", n, 2.0), cell("a2", "b1", 1e9, 1.0));
        final JointFit.Solution s = JointFit.solve(level, List.of("a", "b"), cells, Shrinkage.Scale.identity, false, "fixed", lambda);
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
        final JointFit.Solution s = JointFit.solve(chain, JointFit.cellKeysOf(chain), cells, Shrinkage.Scale.identity, false, "fixed", 1e-9);
        Assertions.assertEquals(60.0, s.rows, 1e-9, "the null-b rows count");
        Assertions.assertEquals(2.0 / 3, s.mu, 1e-6, "μ over every row, the null-b ones included");
        Assertions.assertEquals(1.0 / 3, s.effects.get(1).get(FeatureValues.keyOf(List.of("a1"))), 1e-6, "a1 fitted on 40 rows");
        Assertions.assertEquals(Set.of(FeatureValues.keyOf(List.of("a1", "b1")), FeatureValues.keyOf(List.of("a2", "b1")), FeatureValues.keyOf(List.of("a2", "b2"))),
                s.effects.get(0).keySet(), "no leaf context for a null b");
        Assertions.assertEquals(10.0, s.leafN.get(FeatureValues.keyOf(List.of("a1", "b1"))), 1e-9);
        Assertions.assertFalse(s.leafN.containsKey(nullCell), "no leaf context without the leaf key");
        final JointFit fit = new JointFit(chain, Shrinkage.Scale.identity, false, s, null, null, null);
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
        final JointFit fold = JointFit.fit(LEVELS, Shrinkage.Scale.identity, false, "fixed", 1, entries, 2, false, 0);
        Assertions.assertEquals(2, fold.folds.length);
        // out of fold 0 only cell y remains: its intercept is y's mean and x is unseen (0 effects) → a fold-0 row of x reads y's mean
        Assertions.assertEquals(0.0, fold.estimate(fold.solutionFor(0, null, 1), row("a1", "b1")), 1e-9);
        Assertions.assertEquals(1.0, fold.estimate(fold.solutionFor(1, null, 1), row("a2", "b1")), 1e-9);
        // a row without a fold unit reads the whole-input solution (both cells)
        final double total = fold.estimate(fold.solutionFor(null, null, 1), row("a1", "b1"));
        Assertions.assertTrue(total > 0.5 && total < 1.0, Double.toString(total));

        // forward: block 1 holds x, block 2 holds y; the block-2 solution is cumulative, windowBlocks 1 keeps the current block only
        final List<JointFit.Cell> blocks = List.of(new JointFit.Cell(JointFit.blockEntry(x, 1), 10, 10, 10), new JointFit.Cell(JointFit.blockEntry(y, 2), 10, 0, 0));
        final JointFit forward = JointFit.fit(LEVELS, Shrinkage.Scale.identity, false, "fixed", 1, blocks, 0, true, 0);
        Assertions.assertEquals(Set.of(1L, 2L), forward.blocks.keySet());
        Assertions.assertNull(forward.solutionFor(null, 0L, 1), "nothing before the first block");
        Assertions.assertEquals(1.0, forward.estimate(forward.solutionFor(null, 1L, 1), row("a1", "b1")), 1e-9);
        Assertions.assertEquals(1.0, forward.total.rows / 20, 1e-9);
        Assertions.assertEquals(20.0, forward.solutionFor(null, 5L, 1).rows, 1e-9);
        Assertions.assertNull(forward.solutionFor(null, 1L, 2), "minBlocks 2 needs two solved blocks");
        Assertions.assertNotNull(forward.solutionFor(null, 2L, 2));
        final JointFit windowed = JointFit.fit(LEVELS, Shrinkage.Scale.identity, false, "fixed", 1, blocks, 0, true, 1);
        Assertions.assertEquals(10.0, windowed.solutionFor(null, 2L, 1).rows, 1e-9);
        // the window is anchored to the row's usable block, not to the last observed block: a row past the window of
        // every block reads nothing (the encoding path's rule), a row inside block 2's window reads block 2 only
        Assertions.assertEquals(Set.of(1L, 2L), windowed.observedBlocks);
        Assertions.assertEquals(Set.of(1L, 2L, 3L), windowed.blocks.keySet(), "change points: blocks enter at 1, 2 and leave at 2, 3");
        Assertions.assertNull(windowed.solutionFor(null, 3L, 1), "block 2 left the window at usable block 3");
        Assertions.assertNull(windowed.solutionFor(null, 7L, 1));
        Assertions.assertEquals(20.0, forward.solutionFor(null, 7L, 1).rows, 1e-9, "no window: cumulative");
        // W = 3 over blocks 1, 2: usable 4 reads block 2 only ((1, 4]), usable 5 reads nothing, minBlocks counts observed blocks
        final JointFit wide = JointFit.fit(LEVELS, Shrinkage.Scale.identity, false, "fixed", 1, blocks, 0, true, 3);
        Assertions.assertEquals(20.0, wide.solutionFor(null, 3L, 1).rows, 1e-9);
        Assertions.assertEquals(10.0, wide.solutionFor(null, 4L, 1).rows, 1e-9);
        Assertions.assertEquals(0.0, wide.estimate(wide.solutionFor(null, 4L, 1), row("a2", "b1")), 1e-9);
        Assertions.assertNull(wide.solutionFor(null, 5L, 1));
        Assertions.assertNull(wide.solutionFor(null, 4L, 3), "two observed blocks, minBlocks 3");
    }

    @Test
    public void testArtifactRoundTrip() {
        final List<JointFit.Cell> cells = List.of(cell("a1", "b1", 10, 0.9), cell("a1", "b2", 4, 0.3), cell("a2", "b1", 7, 0.5));
        final JointFit fit = JointFit.fit(LEVELS, Shrinkage.Scale.logit, false, "varianceComponents", 2, cells, 0, false, 0);
        final String dir = "target/feature-artifacts/" + UUID.randomUUID(); // relative: Beam FileSystems treats a Windows drive letter as a scheme
        JointFit.write(dir, "hash", "enc__a_b__e1", fit);
        Assertions.assertTrue(JointFit.exists(dir, "hash", "enc__a_b__e1"));
        final JointFit back = JointFit.read(dir, "hash", "enc__a_b__e1", LEVELS, Shrinkage.Scale.logit, false);
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
        final JointFit infinite = JointFit.fit(LEVELS, Shrinkage.Scale.identity, false, "varianceComponents", 5, flat, 0, false, 0);
        Assertions.assertTrue(Double.isInfinite(infinite.total.lambdas[2]));
        JointFit.write(dir, "hash", "enc__a_b__e2", infinite);
        final String strict = new String(com.mercari.solution.util.domain.file.ResourceUtil.readBytes(JointFit.manifestPath(dir, "hash", "enc__a_b__e2")), java.nio.charset.StandardCharsets.UTF_8);
        final com.google.gson.stream.JsonReader reader = new com.google.gson.stream.JsonReader(new java.io.StringReader(strict));
        reader.setStrictness(com.google.gson.Strictness.STRICT);
        Assertions.assertEquals("Infinity", com.google.gson.JsonParser.parseReader(reader).getAsJsonObject().getAsJsonObject("lambdas").get("b").getAsString(), strict);
        Assertions.assertTrue(Double.isInfinite(JointFit.read(dir, "hash", "enc__a_b__e2", LEVELS, Shrinkage.Scale.identity, false).total.lambdas[2]), "the avro artifact keeps the double");
    }

    /** structure: additive over (a, b): the two main effects and the intercept, no cell level. */
    private static final List<JointFit.Level> MAINS = List.of(
            new JointFit.Level("a", List.of("a")), new JointFit.Level("b", List.of("b")), new JointFit.Level(Shrinkage.GLOBAL, List.of()));

    /**
     * The reference of a fixed level a: elimination on the normal equations of {@code Σ w (z − l_a − e_b)² + λ Σ e_b²}
     * over the unknowns (l_a per a — the intercept with it —, e_b per b); returns l then e in the dictionaries' order.
     */
    private static double[] denseFixed(final List<JointFit.Cell> cells, final double lambda, final List<String> as, final List<String> bs) {
        final int dimension = as.size() + bs.size();
        final double[][] m = new double[dimension][dimension];
        final double[] rhs = new double[dimension];
        for (final JointFit.Cell c : cells) {
            final List<String> parts = FeatureValues.keyComponents(c.key());
            final List<Integer> columns = new ArrayList<>();
            columns.add(as.indexOf(FeatureValues.keyOf(List.of(parts.get(0)))));
            if (parts.get(1) != null) columns.add(as.size() + bs.indexOf(FeatureValues.keyOf(List.of(parts.get(1)))));
            final double z = c.sum() / c.n(), w = c.n();
            for (final int i : columns) {
                rhs[i] += w * z;
                for (final int j : columns) m[i][j] += w;
            }
        }
        for (int k = 0; k < bs.size(); k++) m[as.size() + k][as.size() + k] += lambda;
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
        final double[] x = new double[dimension];
        for (int i = dimension - 1; i >= 0; i--) {
            double s = rhs[i];
            for (int j = i + 1; j < dimension; j++) s -= m[i][j] * x[j];
            x[i] = s / m[i][i];
        }
        return x;
    }

    /**
     * A fixed level is absorbed exactly: the conjugate-gradient solve reaches the normal equations of the unshrunk a
     * (with the intercept) beside the ridge on b, reports a's level as μ + e_a (μ the row-weighted mean of the levels),
     * reads null for an unseen a (the estimate then reads μ) and 0 for an unseen b, and effectiveN of b counts what
     * tells b apart once a is absorbed: a row alone in its a tells nothing.
     */
    @Test
    public void testFixedLevelIsAbsorbedExactly() {
        final Random random = new Random(11);
        final List<JointFit.Cell> cells = new ArrayList<>();
        final List<String> as = new ArrayList<>(), bs = new ArrayList<>();
        for (int a = 0; a < 6; a++) as.add(FeatureValues.keyOf(List.of("a" + a)));
        for (int b = 0; b < 9; b++) bs.add(FeatureValues.keyOf(List.of("b" + b)));
        for (int a = 0; a < 6; a++) {
            for (int b = 0; b < 9; b++) {
                if (random.nextDouble() < 0.55) continue;
                final int n = random.nextDouble() < 0.7 ? 1 : 2 + random.nextInt(3);
                cells.add(cell("a" + a, "b" + b, n, a * 0.3 + random.nextGaussian()));
            }
        }
        // a lone row of a6 (nothing else in its context) and a row with a null b: both stay in a's levels only
        cells.add(cell("a6", "b0", 1, 5.0));
        as.add(FeatureValues.keyOf(List.of("a6")));
        cells.add(new JointFit.Cell(FeatureValues.keyWithNulls(row("a0", null), List.of("a", "b")), 3, 3 * 0.4, 3 * 0.4 * 0.4));
        final double lambda = 2.5;
        final JointFit fit = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "fixed", lambda, cells, 0, false, 0, 0, true);
        final JointFit.Solution s = fit.total;
        // the dictionaries of the reference follow the solution's contexts (b8 may be unobserved)
        bs.retainAll(s.effects.get(1).keySet());
        final double[] x = denseFixed(cells, lambda, as, bs);
        for (int a = 0; a < as.size(); a++) Assertions.assertEquals(x[a], s.mu + s.effects.get(0).get(as.get(a)), 1e-9, "level " + as.get(a));
        for (int b = 0; b < bs.size(); b++) Assertions.assertEquals(x[as.size() + b], s.effects.get(1).get(bs.get(b)), 1e-9, "effect " + bs.get(b));
        Assertions.assertEquals(0.0, s.lambdas[0], 0d, "the fixed level is not shrunk");
        Assertions.assertEquals(lambda, s.lambdas[1], 0d);
        Assertions.assertTrue(s.iterations > 0 && s.maxDelta <= JointFit.CG_TOLERANCE, s.iterations + " / " + s.maxDelta);
        // μ = the row-weighted mean of the levels: the fixed effects sum to 0 with those weights
        final Map<String, Double> weight = new HashMap<>();
        for (final JointFit.Cell c : cells) weight.merge(FeatureValues.keyOf(List.of(FeatureValues.keyComponents(c.key()).get(0))), c.n(), Double::sum);
        double weighted = 0;
        for (final Map.Entry<String, Double> e : s.effects.get(0).entrySet()) weighted += weight.get(e.getKey()) * e.getValue();
        Assertions.assertEquals(0.0, weighted, 1e-9);
        // apply: an unseen a has no fixed effect (null) and the estimate reads the average a; an unseen b reads 0
        Assertions.assertNull(fit.effect(s, 0, row("a9", "b1")));
        Assertions.assertEquals(s.mu + s.effects.get(1).get(FeatureValues.keyOf(List.of("b1"))), fit.estimate(s, row("a9", "b1")), 1e-12);
        Assertions.assertEquals(0.0, fit.effect(s, 1, row("a1", "b99")), 0d);
        Assertions.assertEquals(s.mu + s.effects.get(0).get(as.get(1)), fit.estimate(s, row("a1", "b99")), 1e-12);
        // effectiveN reads b (the first shrunk level): Σ over its cells of n − n² / N_a, plus λ
        Assertions.assertEquals(1, fit.effectiveNLevel());
        final Map<String, Double> rowsOfA = new HashMap<>();
        for (final JointFit.Cell c : cells) rowsOfA.merge(FeatureValues.keyComponents(c.key()).get(0), c.n(), Double::sum);
        for (final String b : List.of("b0", "b3")) {
            double information = 0;
            for (final JointFit.Cell c : cells) {
                final List<String> parts = FeatureValues.keyComponents(c.key());
                if (b.equals(parts.get(1))) information += c.n() - c.n() * c.n() / rowsOfA.get(parts.get(0));
            }
            Assertions.assertEquals(information + lambda, fit.effectiveN(s, row("a0", b)), 1e-9, b);
        }
        Assertions.assertEquals(lambda, fit.effectiveN(s, row("a0", "b99")), 1e-12, "an unseen b: the prior's pseudo-count");
        Assertions.assertNull(fit.variance(s, row("a0", "b0")), "weights: fixed estimates no sigma2");
        // without a fixed level the same cells take the Gauss–Seidel path: unchanged
        final JointFit.Solution plain = JointFit.solve(MAINS, JointFit.cellKeysOf(MAINS), cells, Shrinkage.Scale.identity, false, "fixed", lambda);
        Assertions.assertTrue(plain.information.isEmpty());
        Assertions.assertEquals(JointFit.solve(MAINS, JointFit.cellKeysOf(MAINS), cells, Shrinkage.Scale.identity, false, "fixed", lambda, -1).mu, plain.mu, 0d);
    }

    /** {@code y = 0.3 + e_a + b_b + ε}: {@code groups} contexts of a with {@code size} b's each, drawn from {@code members}. */
    private static List<JointFit.Cell> crossedDesign(final long seed, final int groups, final int size, final int members,
                                                     final double sigma2, final double tau2, final double groupVariance) {
        final Random random = new Random(seed);
        final double[] effect = new double[members];
        for (int h = 0; h < members; h++) effect[h] = Math.sqrt(tau2) * random.nextGaussian();
        final List<JointFit.Cell> cells = new ArrayList<>();
        for (int r = 0; r < groups; r++) {
            final double group = Math.sqrt(groupVariance) * random.nextGaussian();
            final Set<Integer> picked = new TreeSet<>();
            // members drawn near the group's position in the pool: the design is crossed but not fully mixed
            final int centre = random.nextInt(members);
            while (picked.size() < size) picked.add(Math.floorMod(centre + (int) Math.round(members / 20.0 * random.nextGaussian()), members));
            for (final int h : picked) cells.add(cell("r" + r, "h" + h, 1, 0.3 + group + effect[h] + Math.sqrt(sigma2) * random.nextGaussian()));
        }
        return cells;
    }

    /**
     * Henderson's method III recovers λ = σ² / τ² of the shrunk level beside the fixed one, where the one-way moments
     * over its contexts (what varianceComponents does without a fixed level) count the fixed level's variance as noise
     * and over-shrink; variance = σ² / effectiveN.
     */
    @Test
    public void testHendersonEstimatesLambdaBesideAFixedLevel() {
        final double sigma2 = 4, tau2 = 1;
        final List<JointFit.Cell> cells = crossedDesign(3, 3000, 10, 6000, sigma2, tau2, 2);
        final JointFit fit = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "varianceComponents", 20, cells, 0, false, 0, 0, true);
        final JointFit.Solution s = fit.total;
        Assertions.assertEquals(sigma2, s.sigma2, 0.25, "sigma2");
        Assertions.assertEquals(sigma2 / tau2, s.lambdas[1], 0.8, "lambda " + s.lambdas[1]);
        final JointFit.Solution oneWay = JointFit.solve(MAINS, JointFit.cellKeysOf(MAINS), cells, Shrinkage.Scale.identity, false, "varianceComponents", 20);
        Assertions.assertTrue(Math.abs(oneWay.lambdas[1] - sigma2 / tau2) > 2 * Math.abs(s.lambdas[1] - sigma2 / tau2),
                "one-way " + oneWay.lambdas[1] + " vs Henderson " + s.lambdas[1]);
        final Map<String, Object> row = row("r0", FeatureValues.keyComponents(cells.get(0).key()).get(1));
        Assertions.assertEquals(s.sigma2 / fit.effectiveN(s, row), fit.variance(s, row), 1e-12);
        Assertions.assertEquals(s.sigma2 / s.lambdas[1], fit.variance(s, row("r0", "unseen")), 1e-12, "an unseen context: the prior variance tau2");
        // a level without between-context variance is fixed at 0 (λ = +∞), as under the moment estimator: once the a's
        // are taken out every b means 0, so R(b | a) = 0 < (rank − K_a) σ² — here σ² = SSE / (6 − (2 + 3 − 1)) = 4 / 2
        final List<JointFit.Cell> noB = List.of(cell("r0", "h1", 1, 1), cell("r0", "h2", 1, 3), cell("r0", "h3", 1, 2),
                cell("r1", "h1", 1, 3), cell("r1", "h2", 1, 1), cell("r1", "h3", 1, 2));
        final JointFit flat = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "varianceComponents", 20, noB, 0, false, 0, 0, true);
        Assertions.assertEquals(2.0, flat.total.sigma2, 1e-9);
        Assertions.assertTrue(Double.isInfinite(flat.total.lambdas[1]), Double.toString(flat.total.lambdas[1]));
        Assertions.assertTrue(flat.total.effects.get(1).values().stream().allMatch(v -> v == 0d));
        Assertions.assertEquals(0d, flat.variance(flat.total, row("r0", "h1")), 0d);
    }

    /**
     * A row without the shrunk key in a fixed context breaks the component's dependency {@code Σ F = Σ A}: here (r0 h1 1,
     * r0 h2 3, r0 — 2, r1 h1 4, r1 h2 2) the design [F, A] has full rank 4, so σ² = SSE / (5 − 4) with SSE = 4 (the one
     * left null vector (1, −1, 0, −1, 1) of the design gives (1 − 3 − 4 + 2)² / 4); counting the component as one
     * dependency would read rank 3 and σ² = 2.
     */
    @Test
    public void testHendersonRankWithRowsWithoutTheShrunkKey() {
        final List<JointFit.Cell> cells = List.of(cell("r0", "h1", 1, 1), cell("r0", "h2", 1, 3),
                new JointFit.Cell(FeatureValues.keyWithNulls(row("r0", null), List.of("a", "b")), 1, 2, 4),
                cell("r1", "h1", 1, 4), cell("r1", "h2", 1, 2));
        final JointFit fit = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "varianceComponents", 20, cells, 0, false, 0, 0, true);
        Assertions.assertEquals(4.0, fit.total.sigma2, 1e-9);
    }

    /** The cells of (a, b, n, mean) rows keyed in the order of {@code keys}, null components kept. */
    private static List<JointFit.Cell> cellsOf(final List<String> keys, final Object[][] rows) {
        final List<JointFit.Cell> cells = new ArrayList<>();
        for (final Object[] r : rows) {
            final double n = (Double) r[2], mean = (Double) r[3];
            cells.add(new JointFit.Cell(FeatureValues.keyWithNulls(row((String) r[0], (String) r[1]), keys), n, n * mean, n * mean * mean));
        }
        return cells;
    }

    /**
     * Over main effects alone a null key leaves the row out of that level only — in the fit and in the estimate —
     * so declaring [a, b] or [b, a] fits and reads the same: a row needs one key (no fixed level) or the fixed key.
     */
    @Test
    public void testMainEffectsNullKeysDoNotDependOnTheKeyOrder() {
        final Object[][] rows = {
                {"a1", "b1", 3.0, 1.0}, {"a1", "b2", 2.0, 0.4}, {"a2", "b1", 4.0, 0.7}, {"a2", "b2", 1.0, 0.1},
                {null, "b1", 2.0, 0.9}, {"a2", null, 3.0, 0.2}, {"a3", "b2", 2.0, 0.5}};
        final List<JointFit.Level> ba = List.of(MAINS.get(1), MAINS.get(0), MAINS.get(2));
        final List<Map<String, Object>> probes = List.of(row("a1", "b1"), row(null, "b2"), row("a2", null), row(null, null), row("a9", "b1"));
        // without a fixed level: a row with either key takes part
        final JointFit ab = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "fixed", 1, cellsOf(List.of("a", "b"), rows), 0, false, 0, -1, true);
        final JointFit reversed = JointFit.fit(ba, Shrinkage.Scale.identity, false, "fixed", 1, cellsOf(List.of("b", "a"), rows), 0, false, 0, -1, true);
        Assertions.assertEquals(ab.total.rows, reversed.total.rows, 0d);
        Assertions.assertEquals(17.0, ab.total.rows, 0d, "the null-key rows are fitted");
        for (final Map<String, Object> r : probes) {
            final Double x = ab.estimate(ab.total, r), y = reversed.estimate(reversed.total, r);
            if (r.get("a") == null && r.get("b") == null) {
                Assertions.assertNull(x);
                Assertions.assertNull(y);
            } else {
                Assertions.assertEquals(x, y, 1e-8, r.toString());
            }
        }
        // the leaf rule stays for a lattice with a leaf: a chain needs its leaf key
        Assertions.assertFalse(JointFit.takesPart(JointFit.effectLevelsOf(LEVELS), -1, false, row(null, "b1")));
        Assertions.assertTrue(JointFit.takesPart(JointFit.effectLevelsOf(MAINS), -1, true, row(null, "b1")));
        // a fixed a: its key is needed (null a reads null), a null b reads the a level alone — whichever comes first
        final JointFit fixedAb = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "fixed", 1, cellsOf(List.of("a", "b"), rows), 0, false, 0, 0, true);
        final JointFit fixedBa = JointFit.fit(ba, Shrinkage.Scale.identity, false, "fixed", 1, cellsOf(List.of("b", "a"), rows), 0, false, 0, 1, true);
        Assertions.assertEquals(15.0, fixedAb.total.rows, 0d, "the null-a rows leave the fit");
        Assertions.assertEquals(fixedAb.total.rows, fixedBa.total.rows, 0d);
        Assertions.assertEquals(1, fixedAb.effectiveNLevel(), "effectiveN reads b");
        Assertions.assertEquals(0, fixedBa.effectiveNLevel(), "effectiveN reads b");
        for (final Map<String, Object> r : probes) {
            final Double x = fixedAb.estimate(fixedAb.total, r), y = fixedBa.estimate(fixedBa.total, r);
            if (r.get("a") == null) {
                Assertions.assertNull(x, r.toString());
                Assertions.assertNull(y, r.toString());
            } else {
                Assertions.assertEquals(x, y, 1e-9, r.toString());
            }
            Assertions.assertEquals(fixedAb.effectiveN(fixedAb.total, r), fixedBa.effectiveN(fixedBa.total, r));
        }
        Assertions.assertEquals(fixedAb.total.mu + fixedAb.total.effects.get(0).get(FeatureValues.keyOf(List.of("a2"))),
                fixedAb.estimate(fixedAb.total, row("a2", null)), 1e-12, "a null b adds no b effect");
    }

    /**
     * A lookup reads a level of the solution at the context another field of the row names: the effect, or μ + it
     * (the fixed level's identified level); null for a context the solve did not fit — on a shrunk level too, where
     * the row's own unseen context reads the prior's 0 — and for a null field.
     */
    @Test
    public void testLookupReadsALevelAtAnotherField() {
        final List<JointFit.Cell> cells = cellsOf(List.of("a", "b"), new Object[][]{
                {"a1", "b1", 3.0, 1.0}, {"a1", "b2", 2.0, 0.4}, {"a2", "b1", 4.0, 0.7}, {"a2", "b2", 1.0, 0.1}});
        final JointFit fit = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "fixed", 1, cells, 0, false, 0, 0, true);
        final JointFit.Solution s = fit.total;
        final Map<String, Object> row = new HashMap<>(Map.of("a", "a9", "b", "b9", "prev_a", "a2", "prev_b", "b1"));
        final double a2 = s.effects.get(0).get(FeatureValues.keyOf(List.of("a2")));
        Assertions.assertEquals(a2, fit.lookup(s, 0, row, List.of("prev_a"), false), 0d);
        Assertions.assertEquals(s.mu + a2, fit.lookup(s, 0, row, List.of("prev_a"), true), 0d, "level = mu + effect");
        Assertions.assertEquals(s.effects.get(1).get(FeatureValues.keyOf(List.of("b1"))), fit.lookup(s, 1, row, List.of("prev_b"), false), 0d);
        // a context the solve did not fit reads null on either level; the row's own unseen b reads 0 through effect()
        Assertions.assertNull(fit.lookup(s, 0, row, List.of("a"), true));
        Assertions.assertNull(fit.lookup(s, 1, row, List.of("b"), false));
        Assertions.assertEquals(0d, fit.effect(s, 1, row), 0d);
        row.put("prev_a", null);
        Assertions.assertNull(fit.lookup(s, 0, row, List.of("prev_a"), true), "a null field");
        Assertions.assertNull(fit.lookup(new JointFit.Solution(), 0, row, List.of("prev_b"), true), "no solution");
    }

    @Test
    public void testFixedLevelForwardAndArtifact() {
        final List<JointFit.Cell> cells = crossedDesign(9, 300, 6, 500, 1, 1, 1);
        final List<JointFit.Cell> blocks = new ArrayList<>();
        for (int i = 0; i < cells.size(); i++) {
            final JointFit.Cell c = cells.get(i);
            blocks.add(new JointFit.Cell(JointFit.blockEntry(c.key(), i * 3L / cells.size()), c.n(), c.sum(), c.sumSq()));
        }
        final JointFit forward = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "varianceComponents", 20, blocks, 0, true, 2, 0, true);
        Assertions.assertEquals(Set.of(0L, 1L, 2L, 3L, 4L), forward.blocks.keySet());
        for (final JointFit.Solution s : forward.blocks.values()) {
            if (s.isEmpty()) continue;
            Assertions.assertEquals(0d, s.lambdas[0], 0d);
            Assertions.assertFalse(Double.isNaN(s.sigma2));
        }
        final JointFit fit = JointFit.fit(MAINS, Shrinkage.Scale.identity, false, "varianceComponents", 20, cells, 0, false, 0, 0, true);
        final String dir = "target/feature-artifacts/" + UUID.randomUUID();
        JointFit.write(dir, "hash", "enc__a_b__e1", fit);
        final JointFit back = JointFit.read(dir, "hash", "enc__a_b__e1", MAINS, Shrinkage.Scale.identity, false, 0, true);
        for (final String[] r : new String[][]{{"r0", "h1"}, {"r1", "h2"}, {"r9999", "h3"}, {"r2", "unseen"}}) {
            final Map<String, Object> row = row(r[0], r[1]);
            Assertions.assertEquals(fit.estimate(fit.total, row), back.estimate(back.total, row), 1e-12, Arrays.toString(r));
            Assertions.assertEquals(fit.effect(fit.total, 0, row), back.effect(back.total, 0, row), Arrays.toString(r));
            Assertions.assertEquals(fit.effectiveN(fit.total, row), back.effectiveN(back.total, row), 1e-12);
            Assertions.assertEquals(fit.variance(fit.total, row), back.variance(back.total, row), 1e-12);
        }
        final String manifest = new String(com.mercari.solution.util.domain.file.ResourceUtil.readBytes(JointFit.manifestPath(dir, "hash", "enc__a_b__e1")), java.nio.charset.StandardCharsets.UTF_8);
        Assertions.assertTrue(manifest.contains("\"fixedLevel\":\"a\"") && manifest.contains("\"sigma2\""), manifest);
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
