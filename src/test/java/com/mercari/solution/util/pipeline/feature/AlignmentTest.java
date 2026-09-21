package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * {@code fit.align}: the forward fits of an eigendecomposition are chained into one coordinate system. The pure maps
 * ({@link Alignment}), what they do to the two models that have a gauge ({@link Svd}, {@link Spectral}), and the chain
 * a fit block runs over its change points.
 */
public class AlignmentTest {

    private static List<double[]> random(final int rows, final int k, final long seed) {
        final Random random = new Random(seed);
        final List<double[]> a = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            final double[] row = new double[k];
            for (int j = 0; j < k; j++) row[j] = random.nextGaussian() * (k - j);
            a.add(row);
        }
        return a;
    }

    /** A rotation by {@code angle} in the (0, 1) plane, then a flip of the last axis. */
    private static double[][] orthogonal(final int k, final double angle) {
        final double[][] q = new double[k][k];
        for (int i = 0; i < k; i++) q[i][i] = 1;
        q[0][0] = Math.cos(angle);
        q[0][1] = -Math.sin(angle);
        q[1][0] = Math.sin(angle);
        q[1][1] = Math.cos(angle);
        q[k - 1][k - 1] = -1;
        return q;
    }

    private static void assertOrthogonal(final double[][] r) {
        for (int i = 0; i < r.length; i++) {
            for (int j = 0; j < r.length; j++) {
                double dot = 0;
                for (int l = 0; l < r.length; l++) dot += r[l][i] * r[l][j];
                Assertions.assertEquals(i == j ? 1 : 0, dot, 1e-12, "RᵀR[" + i + "][" + j + "]");
            }
        }
    }

    @Test
    public void testProcrustesRecoversAnOrthogonalMap() {
        final int k = 4;
        final List<double[]> a = random(60, k, 3);
        final double[][] q = orthogonal(k, 0.7);
        final List<double[]> b = new ArrayList<>();
        for (final double[] row : a) b.add(Alignment.apply(row, q));
        final double[][] r = Alignment.map(Alignment.PROCRUSTES, Alignment.cross(a, b, k), a);
        assertOrthogonal(r);
        for (int i = 0; i < k; i++) Assertions.assertArrayEquals(q[i], r[i], 1e-10);
        for (int row = 0; row < a.size(); row++) Assertions.assertArrayEquals(b.get(row), Alignment.apply(a.get(row), r), 1e-9);
        // signs alone cannot express the rotation: they only settle each column against its own predecessor
        final double[][] s = Alignment.map(Alignment.SIGN, Alignment.cross(a, b, k), a);
        for (int i = 0; i < k; i++) for (int j = 0; j < k; j++) Assertions.assertEquals(i != j ? 0 : i == k - 1 ? -1 : 1, s[i][j], 0);
        Assertions.assertNull(Alignment.map(Alignment.NONE, Alignment.cross(a, b, k), a));
        Assertions.assertNull(Alignment.map(null, Alignment.cross(a, b, k), a));
    }

    /**
     * A component the previous fit did not have has nothing to align to: the columns that have a predecessor follow
     * it, the new one takes the direction left over and the orientation a static fit gives (largest entry positive).
     */
    @Test
    public void testAComponentWithoutAPredecessor() {
        final int k = 3;
        // orthonormal columns (the loadings of an svd): with AᵀA = I the zero-padded target is matched exactly. For a
        // general A the padded solution trades the match of the shared columns against the norm it moves into them —
        // the usual treatment of configurations of unequal rank, exact only in this case
        final List<double[]> a = random(40, k, 5);
        for (int j = 0; j < k; j++) {
            for (int b = 0; b < j; b++) {
                double dot = 0;
                for (final double[] row : a) dot += row[j] * row[b];
                for (final double[] row : a) row[j] -= dot * row[b];
            }
            double norm = 0;
            for (final double[] row : a) norm += row[j] * row[j];
            for (final double[] row : a) row[j] /= Math.sqrt(norm);
        }
        final double[][] q = orthogonal(k, -1.1);
        final List<double[]> previous = new ArrayList<>();
        for (final double[] row : a) {
            final double[] full = Alignment.apply(row, q);
            previous.add(new double[]{full[0], full[1]});
        }
        final double[][] r = Alignment.map(Alignment.PROCRUSTES, Alignment.cross(a, previous, k), a);
        assertOrthogonal(r);
        final double[] free = new double[a.size()];
        for (int row = 0; row < a.size(); row++) {
            final double[] aligned = Alignment.apply(a.get(row), r);
            Assertions.assertEquals(previous.get(row)[0], aligned[0], 1e-9);
            Assertions.assertEquals(previous.get(row)[1], aligned[1], 1e-9);
            free[row] = aligned[2];
        }
        Assertions.assertEquals(1, Svd.sign(free), 0);
        // nothing in common at all: no map, the fit keeps its own orientation
        final List<double[]> zeros = new ArrayList<>();
        for (int i = 0; i < a.size(); i++) zeros.add(new double[k]);
        Assertions.assertNull(Alignment.map(Alignment.PROCRUSTES, Alignment.cross(a, zeros, k), a));
    }

    private static Svd.Moments cloud(final int n, final long seed, final double[] deviations) {
        final Random random = new Random(seed);
        final Svd.Moments m = new Svd.Moments();
        for (int i = 0; i < n; i++) {
            final double[] x = new double[deviations.length];
            for (int j = 0; j < x.length; j++) x[j] = deviations[j] * random.nextGaussian();
            m.add(x);
        }
        return m;
    }

    private static double distance(final Svd a, final Svd b) {
        double d = 0;
        for (int r = 0; r < a.rank(); r++) for (int i = 0; i < a.dimension; i++) d += Math.pow(a.components[r][i] - b.components[r][i], 2);
        return Math.sqrt(d);
    }

    /**
     * Two fits of the same near-isotropic plane: the eigenvectors inside it are wherever the sample put them, so the
     * unaligned scores of one probe differ wildly, while the aligned fit scores it like its predecessor. What the fit
     * explains does not change — same residual, same variance in total — and a component's variance is the data's
     * along its new direction.
     */
    @Test
    public void testSvdAlignTo() {
        final double[] deviations = {1, 1, 0.1};
        final Svd first = Svd.fit(cloud(500, 1, deviations), 2, true, false, false);
        final Svd second = Svd.fit(cloud(500, 2, deviations), 2, true, false, false);
        final Svd aligned = second.alignTo(first, Alignment.PROCRUSTES);
        Assertions.assertTrue(distance(second, first) > 0.3, "the test needs fits that disagree: " + distance(second, first));
        Assertions.assertTrue(distance(aligned, first) < 0.05, "aligned " + distance(aligned, first));
        Assertions.assertEquals(Alignment.PROCRUSTES, aligned.alignment);
        Assertions.assertNull(second.alignment);
        final double[] probe = {0.8, -1.3, 0.05};
        Assertions.assertArrayEquals(first.transform(probe), aligned.transform(probe), 0.1);
        Assertions.assertArrayEquals(second.residual(probe), aligned.residual(probe), 1e-12);
        Assertions.assertEquals(second.variances[0] + second.variances[1], aligned.variances[0] + aligned.variances[1], 1e-12);
        Assertions.assertEquals(second.totalVariance, aligned.totalVariance, 0);
        // the rows stay orthonormal
        for (int r = 0; r < 2; r++) {
            for (int q = 0; q < 2; q++) {
                double dot = 0;
                for (int i = 0; i < 3; i++) dot += aligned.components[r][i] * aligned.components[q][i];
                Assertions.assertEquals(r == q ? 1 : 0, dot, 1e-12);
            }
        }
        // a flipped copy of a fit is that fit again, under either mode; none leaves it alone
        final double[][] flipped = {first.components[0].clone(), first.components[1].clone()};
        for (int i = 0; i < 3; i++) flipped[1][i] = -flipped[1][i];
        final Svd mirror = new Svd(3, first.mean, first.scale, flipped, first.variances, first.totalVariance, first.n);
        Assertions.assertEquals(0, distance(mirror.alignTo(first, Alignment.SIGN), first), 1e-12);
        Assertions.assertEquals(0, distance(mirror.alignTo(first, Alignment.PROCRUSTES), first), 1e-12);
        Assertions.assertSame(mirror, mirror.alignTo(first, Alignment.NONE));
        Assertions.assertSame(mirror, mirror.alignTo(null, Alignment.PROCRUSTES));
        Assertions.assertEquals(Alignment.PROCRUSTES, Svd.fromJson(aligned.toJson()).alignment);
        Assertions.assertFalse(second.toJson().has("alignment"));
    }

    private static Spectral.PairCounts sequence(final int steps, final long seed) {
        // two communities of values that follow their own kind, with an occasional crossing
        final String[][] communities = {{"a", "b", "c", "d"}, {"p", "q", "r", "s"}};
        final Random random = new Random(seed);
        final Spectral.PairCounts state = new Spectral.PairCounts();
        int community = 0;
        String before = communities[0][0];
        for (int i = 0; i < steps; i++) {
            if (random.nextDouble() < 0.1) community = 1 - community;
            final String value = communities[community][random.nextInt(4)];
            state.add(value, before, 1);
            before = value;
        }
        return state;
    }

    private static double distance(final Spectral model, final String a, final String b) {
        final double[] x = model.embed(a), y = model.embed(b);
        double d = 0;
        for (int i = 0; i < x.length; i++) d += (x[i] - y[i]) * (x[i] - y[i]);
        return Math.sqrt(d);
    }

    /** The coordinates move towards the previous fit's while every distance between two values stays what it was. */
    @Test
    public void testSpectralAlignTo() {
        final Spectral first = Spectral.fit(sequence(3000, 1), 3, 64, false);
        final Spectral second = Spectral.fit(sequence(3000, 2), 3, 64, false);
        final Spectral aligned = second.alignTo(first, Alignment.PROCRUSTES);
        double before = 0, after = 0;
        for (final String value : first.vocabulary) {
            final double[] target = first.embed(value), was = second.embed(value), now = aligned.embed(value);
            for (int i = 0; i < 3; i++) {
                before += Math.pow(was[i] - target[i], 2);
                after += Math.pow(now[i] - target[i], 2);
            }
        }
        Assertions.assertTrue(after <= before + 1e-12, after + " > " + before);
        for (final String a : second.vocabulary) {
            for (final String b : second.vocabulary) Assertions.assertEquals(distance(second, a, b), distance(aligned, a, b), 1e-10);
        }
        Assertions.assertArrayEquals(second.eigenvalues, aligned.eigenvalues, 0);
        Assertions.assertEquals(Alignment.PROCRUSTES, Spectral.fromJson(aligned.toJson()).alignment);

        // columns swapped and one negated — what an eigenvalue crossing and a changed largest loading do to a re-fit
        final double[][] scrambled = new double[first.vocabulary.length][];
        for (int i = 0; i < scrambled.length; i++) scrambled[i] = new double[]{first.embedding[i][1], -first.embedding[i][0], first.embedding[i][2]};
        final Spectral crossed = new Spectral(first.vocabulary, scrambled, first.eigenvalues, first.pairs, first.dropped);
        final Spectral restored = crossed.alignTo(first, Alignment.PROCRUSTES);
        for (int i = 0; i < scrambled.length; i++) Assertions.assertArrayEquals(first.embedding[i], restored.embedding[i], 1e-10);
        // signs settle a flip but cannot undo the swap
        final Spectral signed = crossed.alignTo(first, Alignment.SIGN);
        Assertions.assertEquals(Alignment.SIGN, signed.alignment);
        double off = 0;
        for (int i = 0; i < scrambled.length; i++) off += Math.abs(signed.embedding[i][0] - first.embedding[i][0]);
        Assertions.assertTrue(off > 1e-3);
    }

    private static FeatureStages.SvdSpec spec(final String align) {
        final FeatureStages.Forward forward = new FeatureStages.Forward(ForwardBlocks.ofSize(Duration.ofDays(7)), 1, 0L, 0, "t", "timestamp");
        return new FeatureStages.SvdSpec("block", List.of("x", "y", "z"), null, 2, true, false, null, false, List.of(), new int[0], forward, 0L, 0L, align);
    }

    /**
     * The chain a forward fit block runs: every change point in the coordinates of the one before it, the whole-input
     * model (what a static serving run loads) at the end of the chain — and strictly forward in time: the fit of a
     * block is the same whatever comes after it.
     */
    @Test
    public void testForwardFitsAreChainedInTimeOrder() {
        final double[] deviations = {1, 1, 0.1};
        final Map<Long, Svd.Moments> parts = new TreeMap<>();
        for (long block = 0; block < 8; block++) parts.put(block, cloud(300, 100 + block, deviations));

        final FeatureStages.ForwardModel<Svd> free = spec(Alignment.NONE).solve(parts, "hash");
        final FeatureStages.ForwardModel<Svd> chained = spec(Alignment.PROCRUSTES).solve(parts, "hash");
        double worstFree = 0, worstChained = 0;
        for (long block = 1; block < 8; block++) {
            worstFree = Math.max(worstFree, distance(free.byBlock().get(block), free.byBlock().get(block - 1)));
            worstChained = Math.max(worstChained, distance(chained.byBlock().get(block), chained.byBlock().get(block - 1)));
        }
        Assertions.assertTrue(worstFree > 0.3, "the test needs fits that jump without alignment: " + worstFree);
        Assertions.assertTrue(worstChained < 0.1, "chained " + worstChained);
        // the first fit has no predecessor and keeps the static orientation
        Assertions.assertEquals(0, distance(chained.byBlock().get(0L), free.byBlock().get(0L)), 0);
        // the whole-input model reads the same state as the last prefix: it ends the chain
        Assertions.assertEquals(0, distance(chained.total(), chained.byBlock().get(7L)), 1e-9);
        Assertions.assertTrue(distance(free.total(), chained.total()) > 1e-3 || worstFree < 1e-3);

        // walk-forward: later blocks leave the earlier fits untouched
        final Map<Long, Svd.Moments> head = new TreeMap<>();
        for (long block = 0; block < 4; block++) head.put(block, cloud(300, 100 + block, deviations));
        final FeatureStages.ForwardModel<Svd> early = spec(Alignment.PROCRUSTES).solve(head, "hash");
        for (long block = 0; block < 4; block++) Assertions.assertEquals(0, distance(early.byBlock().get(block), chained.byBlock().get(block)), 0);
    }

}
