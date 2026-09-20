package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * fit.mode forward: the per-level Combine of {@link VarianceComponents#lambdasByBlockView} (level-wide moments as
 * a step function over blocks) must reproduce the in-memory reference {@link VarianceComponents#lambdasByBlock(Map)}
 * whatever the order the keys are added in or how the partial accumulators are split.
 */
public class VarianceComponentsTest {

    /** A cumulative series over random sparse blocks (n ≥ 1 per block). */
    private static ForwardBlocks.Series series(final Random random, final int blocks) {
        final long[] block = new long[blocks];
        final double[] n = new double[blocks], sum = new double[blocks], sumSq = new double[blocks];
        long b = random.nextInt(5);
        double cn = 0, cs = 0, cq = 0;
        for (int i = 0; i < blocks; i++) {
            b += 1 + random.nextInt(4);
            final int rows = 1 + random.nextInt(3);
            for (int r = 0; r < rows; r++) {
                final double y = random.nextGaussian();
                cn += 1;
                cs += y;
                cq += y * y;
            }
            block[i] = b;
            n[i] = cn;
            sum[i] = cs;
            sumSq[i] = cq;
        }
        return new ForwardBlocks.Series(block, n, sum, sumSq);
    }

    private static Map<Long, Double> lambdas(final VarianceComponents.BlockMoments acc) {
        final Map<Long, Double> out = new TreeMap<>();
        for (final Map.Entry<Long, VarianceComponents.Moments> e : acc.byBlock.entrySet()) {
            final VarianceComponents.Moments m = e.getValue();
            final Double lambda = Shrinkage.lambdaFromMoments(m.keys, m.n, m.sum, m.sumSq, m.sumSqOverN, m.sumNSq);
            if (lambda != null) out.put(e.getKey(), lambda);
        }
        return out;
    }

    /** Equal blocks and λ up to the rounding of a different summation order. */
    private static void assertLambdas(final Map<Long, Double> expected, final Map<Long, Double> actual, final String message) {
        Assertions.assertEquals(expected.keySet(), actual.keySet(), message);
        for (final Map.Entry<Long, Double> e : expected.entrySet()) {
            Assertions.assertEquals(e.getValue(), actual.get(e.getKey()), Math.abs(e.getValue()) * 1e-9, message + " block " + e.getKey());
        }
    }

    @Test
    public void testBlockMomentsMatchesReference() {
        final Random random = new Random(7);
        for (int trial = 0; trial < 20; trial++) {
            final int keys = 1 + random.nextInt(12);
            final Map<String, ForwardBlocks.Series> byEntry = new HashMap<>();
            final List<ForwardBlocks.Series> all = new ArrayList<>();
            for (int k = 0; k < keys; k++) {
                final ForwardBlocks.Series s = series(random, 1 + random.nextInt(6));
                byEntry.put(FitArtifact.entryKey("lvl__n", "k" + k), s);
                all.add(s);
            }
            final TreeMap<Long, Double> expected = VarianceComponents.lambdasByBlock(byEntry).get("lvl__n");

            // one accumulator, keys added in order
            final VarianceComponents.BlockMomentsFn fn = new VarianceComponents.BlockMomentsFn();
            VarianceComponents.BlockMoments single = fn.createAccumulator();
            for (final ForwardBlocks.Series s : all) single = fn.addInput(single, s);
            assertLambdas(expected, lambdas(fn.extractOutput(single)), "trial " + trial);

            // partial accumulators of random size, merged (combiner lifting), keys in reverse order
            final List<VarianceComponents.BlockMoments> partials = new ArrayList<>();
            VarianceComponents.BlockMoments part = fn.createAccumulator();
            for (int i = all.size() - 1; i >= 0; i--) {
                part = fn.addInput(part, all.get(i));
                if (random.nextBoolean()) {
                    partials.add(part);
                    part = fn.createAccumulator();
                }
            }
            partials.add(part);
            partials.add(fn.createAccumulator()); // an empty partial changes nothing
            assertLambdas(expected, lambdas(fn.extractOutput(fn.mergeAccumulators(partials))), "trial " + trial);
        }
    }

    @Test
    public void testBlockMomentsStepFunction() {
        // key a has blocks 1, 2; key b block 2 only: at block 1 a single key (no estimate), at block 2 both
        final VarianceComponents.BlockMomentsFn fn = new VarianceComponents.BlockMomentsFn();
        VarianceComponents.BlockMoments acc = fn.createAccumulator();
        acc = fn.addInput(acc, new ForwardBlocks.Series(new long[]{1, 2}, new double[]{4, 8}, new double[]{2, 4}, new double[]{2, 4}));
        acc = fn.addInput(acc, new ForwardBlocks.Series(new long[]{2}, new double[]{4}, new double[]{0}, new double[]{0}));
        Assertions.assertEquals(1L, acc.byBlock.get(1L).keys);
        Assertions.assertEquals(4.0, acc.byBlock.get(1L).n, 0d);
        Assertions.assertEquals(2L, acc.byBlock.get(2L).keys);
        Assertions.assertEquals(12.0, acc.byBlock.get(2L).n, 0d);
        // a key whose blocks start later carries its value forward over the other key's later blocks
        acc = fn.addInput(acc, new ForwardBlocks.Series(new long[]{0, 5}, new double[]{1, 2}, new double[]{1, 3}, new double[]{1, 5}));
        Assertions.assertEquals(List.of(0L, 1L, 2L, 5L), new ArrayList<>(acc.byBlock.keySet()));
        Assertions.assertEquals(3L, acc.byBlock.get(2L).keys);
        Assertions.assertEquals(13.0, acc.byBlock.get(2L).n, 0d);
        Assertions.assertEquals(3L, acc.byBlock.get(5L).keys);
        Assertions.assertEquals(14.0, acc.byBlock.get(5L).n, 0d);
        final Map<String, TreeMap<Long, Double>> reference = VarianceComponents.lambdasByBlock(Map.of(
                FitArtifact.entryKey("l", "a"), new ForwardBlocks.Series(new long[]{1, 2}, new double[]{4, 8}, new double[]{2, 4}, new double[]{2, 4}),
                FitArtifact.entryKey("l", "b"), new ForwardBlocks.Series(new long[]{2}, new double[]{4}, new double[]{0}, new double[]{0}),
                FitArtifact.entryKey("l", "c"), new ForwardBlocks.Series(new long[]{0, 5}, new double[]{1, 2}, new double[]{1, 3}, new double[]{1, 5})));
        assertLambdas(reference.get("l"), lambdas(acc), "step");
    }

    /**
     * A time fold reads the whole input's λ, estimated from its levels' totals ({@code lambdasFromKeyStats}, whose
     * in-memory twin is {@code lambdasInMemory}): the same value as the last step of the per-block λ a forward level
     * reads, without building the step function.
     */
    @Test
    public void testWholeInputLambdaIsLastStep() {
        final Random random = new Random(11);
        for (int trial = 0; trial < 20; trial++) {
            final int keys = 2 + random.nextInt(12);
            final Map<String, ForwardBlocks.Series> byEntry = new HashMap<>();
            final Map<String, VarianceComponents.KeyStats> totals = new HashMap<>();
            for (int k = 0; k < keys; k++) {
                final ForwardBlocks.Series s = series(random, 1 + random.nextInt(6));
                final String entry = FitArtifact.entryKey("lvl__n", "k" + k);
                byEntry.put(entry, s);
                totals.put(entry, s.totals());
            }
            final Map.Entry<Long, Double> last = VarianceComponents.lambdasByBlock(byEntry).get("lvl__n").lastEntry();
            final Double whole = VarianceComponents.lambdasInMemory(totals).get("lvl__n");
            if (last == null) {
                Assertions.assertNull(whole, "trial " + trial);
            } else {
                Assertions.assertNotNull(whole, "trial " + trial);
                // λ = ∞ (full shrinkage) must match exactly: a relative delta of ∞ would accept any value
                final double delta = Double.isInfinite(last.getValue()) ? 0d : Math.abs(last.getValue()) * 1e-9;
                Assertions.assertEquals(last.getValue(), whole, delta, "trial " + trial);
            }
        }
    }

}
