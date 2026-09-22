package com.mercari.solution.util.pipeline.feature;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The totals a time fold's λ is estimated from under {@code fit.fold.until}: a level's series clipped at the last
 * training block ({@link FeatureStages#seriesTotals}, inclusive — the block of {@code until} itself is training), a
 * level without an until block read whole, a series that starts after the until block contributing nothing, and
 * a level outside the stage dropped.
 */
public class FeatureStagesSeriesTotalsTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static ForwardBlocks.Series series(final long[] blocks, final double[] n) {
        final double[] sum = new double[n.length], sumSq = new double[n.length], off = new double[n.length], info = new double[n.length];
        for (int i = 0; i < n.length; i++) {
            sum[i] = 2 * n[i];
            sumSq[i] = 4 * n[i];
        }
        return new ForwardBlocks.Series(blocks, n, sum, sumSq, off, info);
    }

    @Test
    public void testTotalsAreClippedAtTheUntilBlockInclusive() {
        final ForwardBlocks.Series a = series(new long[]{10, 12, 15}, new double[]{1, 3, 6});
        final PCollection<KV<String, ForwardBlocks.Series>> input = pipeline
                .apply(Create.of(List.of(
                        KV.of(FitArtifact.entryKey("clipped__n", "a"), a),
                        KV.of(FitArtifact.entryKey("clipped__n", "late"), series(new long[]{14, 16}, new double[]{2, 5})),
                        KV.of(FitArtifact.entryKey("whole__n", "a"), a),
                        KV.of(FitArtifact.entryKey("other__n", "a"), a)))
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ForwardBlocks.Series.class))));
        final Map<String, Long> until = new HashMap<>();
        until.put("clipped__n", 13L);   // between the blocks 12 and 15: the totals through block 12
        final PCollection<KV<String, VarianceComponents.KeyStats>> totals =
                FeatureStages.seriesTotals(input, Set.of("clipped__n", "whole__n"), until, "Totals");
        PAssert.that(totals).satisfies(rows -> {
            final Map<String, Double> n = new HashMap<>();
            for (final KV<String, VarianceComponents.KeyStats> row : rows) n.put(row.getKey(), row.getValue().n);
            Assertions.assertEquals(3d, n.get(FitArtifact.entryKey("clipped__n", "a")), "blocks 10 and 12 are training, 15 is not");
            Assertions.assertFalse(n.containsKey(FitArtifact.entryKey("clipped__n", "late")), "a series that starts after the until block has no training rows");
            Assertions.assertEquals(6d, n.get(FitArtifact.entryKey("whole__n", "a")), "no until block: the whole series");
            Assertions.assertFalse(n.containsKey(FitArtifact.entryKey("other__n", "a")), "a level outside the stage");
            Assertions.assertEquals(2, n.size());
            return null;
        });
        pipeline.run();

        // the until block itself is training (inclusive): a series with a block at exactly that index counts it
        final TestPipeline second = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final PCollection<KV<String, ForwardBlocks.Series>> exact = second
                .apply(Create.of(List.of(KV.of(FitArtifact.entryKey("clipped__n", "a"), a)))
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ForwardBlocks.Series.class))));
        final Map<String, Long> at = new HashMap<>();
        at.put("clipped__n", 12L);
        PAssert.that(FeatureStages.seriesTotals(exact, Set.of("clipped__n"), at, "Totals")).satisfies(rows -> {
            for (final KV<String, VarianceComponents.KeyStats> row : rows) Assertions.assertEquals(3d, row.getValue().n);
            return null;
        });
        second.run();
    }
}
