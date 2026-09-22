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

    /**
     * An offset term: the per-key Combine accumulates Σb and Σ b(1 − b) from the extracted (y − b, b) pairs, the level
     * moments keep the score statistics of both transformed scales next to the identity ones, and the level's score
     * scale picks which λ is derived — 1 / τ² by {@link Shrinkage#lambdaFromScore} on logit (V = Σ b(1 − b)) or log
     * (V = Σb), the identity moments otherwise.
     */
    @Test
    public void testScoreMomentsFollowTheLevelScale() {
        final VarianceComponents.KeyStatsFn keyFn = new VarianceComponents.KeyStatsFn();
        final double[][] rows = {{1 - 0.2, 0.2}, {0 - 0.3, 0.3}, {1 - 0.5, 0.5}};   // (y − b, b)
        VarianceComponents.KeyStats a = keyFn.createAccumulator();
        for (final double[] r : rows) a = keyFn.addInput(a, org.apache.beam.sdk.values.KV.of(r[0], r[1]));
        Assertions.assertEquals(3, a.n, 0d);
        Assertions.assertEquals(1.0, a.sumOff, 1e-12);
        Assertions.assertEquals(0.2 * 0.8 + 0.3 * 0.7 + 0.5 * 0.5, a.sumInfo, 1e-12);
        VarianceComponents.KeyStats b = keyFn.createAccumulator();
        b = keyFn.addInput(b, org.apache.beam.sdk.values.KV.of(0 - 0.4, 0.4));
        b = keyFn.addInput(b, org.apache.beam.sdk.values.KV.of(0 - 0.1, 0.1));
        final VarianceComponents.KeyStats merged = keyFn.mergeAccumulators(List.of(a, b));
        Assertions.assertEquals(a.sumInfo + b.sumInfo, merged.sumInfo, 1e-12);
        Assertions.assertEquals(a.sumInfo - b.sumInfo, VarianceComponents.subtract(a, b).sumInfo, 1e-12);

        final VarianceComponents.MomentsFn fn = new VarianceComponents.MomentsFn();
        final VarianceComponents.Moments m = fn.mergeAccumulators(List.of(fn.addInput(fn.createAccumulator(), a), fn.addInput(fn.createAccumulator(), b)));
        Assertions.assertEquals(2, m.keysLogit);
        Assertions.assertEquals(a.sumInfo + b.sumInfo, m.sumVLogit, 1e-12);
        Assertions.assertEquals(a.sum + b.sum, m.sumSLogit, 1e-12);
        Assertions.assertEquals(a.sum * a.sum / a.sumInfo + b.sum * b.sum / b.sumInfo, m.sumS2OverVLogit, 1e-12);
        Assertions.assertEquals(a.sumOff + b.sumOff, m.sumVLog, 1e-12);
        final Double logit = VarianceComponents.lambdaOf(m, "logit");
        Assertions.assertEquals(Shrinkage.lambdaFromScore(2, m.sumVLogit, m.sumV2Logit, m.sumSLogit, m.sumS2OverVLogit), logit);
        Assertions.assertEquals(Shrinkage.lambdaFromScore(2, m.sumVLog, m.sumV2Log, m.sumSLog, m.sumS2OverVLog), VarianceComponents.lambdaOf(m, "log"));
        Assertions.assertEquals(Shrinkage.lambdaFromMoments(m.keys, m.n, m.sum, m.sumSq, m.sumSqOverN, m.sumNSq), VarianceComponents.lambdaOf(m, null));
        // the in-memory derivation keys the scale by level id
        final Map<String, VarianceComponents.KeyStats> stats = new HashMap<>();
        stats.put(FitArtifact.entryKey("lvl__n", "a"), a);
        stats.put(FitArtifact.entryKey("lvl__n", "b"), b);
        Assertions.assertEquals(logit, VarianceComponents.lambdasInMemory(stats, Map.of("lvl__n", "logit")).get("lvl__n"));
        Assertions.assertEquals(VarianceComponents.lambdaOf(m, null), VarianceComponents.lambdasInMemory(stats).get("lvl__n"));
    }

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

    /** A cumulative series of an offset level: residuals y - b of a 0/1 target against a baseline b in (0.05, 0.95). */
    private static ForwardBlocks.Series offsetSeries(final Random random, final int blocks) {
        final long[] block = new long[blocks];
        final double[] n = new double[blocks], sum = new double[blocks], sumSq = new double[blocks], sumOff = new double[blocks], sumInfo = new double[blocks];
        long b = random.nextInt(5);
        double cn = 0, cs = 0, cq = 0, co = 0, ci = 0;
        for (int i = 0; i < blocks; i++) {
            b += 1 + random.nextInt(4);
            final int rows = 1 + random.nextInt(3);
            for (int r = 0; r < rows; r++) {
                final double baseline = 0.05 + 0.9 * random.nextDouble();
                final double y = (random.nextDouble() < baseline ? 1 : 0) - baseline;
                cn += 1;
                cs += y;
                cq += y * y;
                co += baseline;
                ci += Shrinkage.information(Shrinkage.Scale.logit, baseline);
            }
            block[i] = b;
            n[i] = cn;
            sum[i] = cs;
            sumSq[i] = cq;
            sumOff[i] = co;
            sumInfo[i] = ci;
        }
        return new ForwardBlocks.Series(block, n, sum, sumSq, sumOff, sumInfo);
    }

    private static Map<Long, Double> lambdas(final VarianceComponents.BlockMoments acc, final String scoreScale) {
        final Map<Long, Double> out = new TreeMap<>();
        for (final Map.Entry<Long, VarianceComponents.Moments> e : acc.byBlock.entrySet()) {
            final Double lambda = VarianceComponents.lambdaOf(e.getValue(), scoreScale);
            if (lambda != null) out.put(e.getKey(), lambda);
        }
        return out;
    }

    /**
     * fit.mode forward under an offset term: the per-block λ of a score level (1 / τ² on the logit or log scale,
     * {@link Shrinkage#lambdaFromScore}) computed by the pipeline's Combine — one accumulator, and partial accumulators
     * merged out of order — equals the in-memory reference for that scale, and differs from the identity moments' λ.
     */
    @Test
    public void testScoreLambdaByBlockMatchesReference() {
        final Random random = new Random(11);
        for (final String scale : new String[]{"logit", "log"}) {
            for (int trial = 0; trial < 10; trial++) {
                final int keys = 2 + random.nextInt(12);
                final Map<String, ForwardBlocks.Series> byEntry = new HashMap<>();
                final List<ForwardBlocks.Series> all = new ArrayList<>();
                for (int k = 0; k < keys; k++) {
                    final ForwardBlocks.Series s = offsetSeries(random, 1 + random.nextInt(6));
                    byEntry.put(FitArtifact.entryKey("lvl__n", "k" + k), s);
                    all.add(s);
                }
                final TreeMap<Long, Double> expected = VarianceComponents.lambdasByBlock(byEntry, Map.of("lvl__n", scale)).get("lvl__n");
                final VarianceComponents.BlockMomentsFn fn = new VarianceComponents.BlockMomentsFn();
                VarianceComponents.BlockMoments single = fn.createAccumulator();
                for (final ForwardBlocks.Series s : all) single = fn.addInput(single, s);
                assertLambdas(expected, lambdas(fn.extractOutput(single), scale), scale + " trial " + trial);
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
                assertLambdas(expected, lambdas(fn.mergeAccumulators(partials), scale), scale + " merged trial " + trial);
                // a score scale is not the identity moments: the reference of the raw scale is another number
                final TreeMap<Long, Double> identity = VarianceComponents.lambdasByBlock(byEntry, Map.of()).get("lvl__n");
                if (!expected.isEmpty() && !identity.isEmpty()) {
                    final long last = expected.lastKey();
                    if (identity.containsKey(last) && Double.isFinite(expected.get(last)) && Double.isFinite(identity.get(last))) {
                        Assertions.assertNotEquals(identity.get(last), expected.get(last), 1e-9, scale + " trial " + trial);
                    }
                }
            }
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
