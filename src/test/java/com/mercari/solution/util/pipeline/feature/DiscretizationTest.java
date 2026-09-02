package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DiscretizationTest {

    @Test
    public void testQuantileEdgesAndBins() {
        final double[] values = {200, 50, 120, 80, 60, 100};
        final double[] copy = values.clone();
        final Discretization d = Discretization.fitQuantile(values, values.length, 3, null);
        Assertions.assertArrayEquals(copy, values, "the input must not be sorted in place");
        Assertions.assertEquals(3, d.bins());
        Assertions.assertEquals(60 + 20 * 2 / 3d, d.edges[0], 1e-9);
        Assertions.assertEquals(100 + 20 / 3d, d.edges[1], 1e-9);
        Assertions.assertEquals(-1L, d.bin(null));
        Assertions.assertEquals(-1L, d.bin(Double.NaN));
        Assertions.assertEquals(0L, d.bin(49.9));
        Assertions.assertEquals(1L, d.bin(50.0));
        Assertions.assertEquals(2L, d.bin(d.edges[0]));
        Assertions.assertEquals(3L, d.bin(200.0));
        Assertions.assertEquals(4L, d.bin(200.1));
        // JSON round trip keeps everything that decides a bin
        final Discretization back = Discretization.fromJson(d.toJson());
        for (final double v : new double[]{-1, 50, 70, 73.4, 100, 150, 200, 300}) Assertions.assertEquals(d.bin(v), back.bin(v), "v=" + v);
        Assertions.assertThrows(IllegalStateException.class, () -> {
            final com.google.gson.JsonObject json = d.toJson();
            json.remove("n");
            Discretization.fromJson(json);
        });
    }

    @Test
    public void testBinCountRules() {
        final double[] values = new double[1000];
        for (int i = 0; i < values.length; i++) values[i] = i;
        // minSamplesPerBin alone is capped by the default of 10 bins
        Assertions.assertEquals(10, Discretization.fitQuantile(values, values.length, null, 10).bins());
        // and by bins when both are given; fewer samples than a bin needs collapses to one bin
        Assertions.assertEquals(4, Discretization.fitQuantile(values, values.length, 4, 10).bins());
        Assertions.assertEquals(5, Discretization.fitQuantile(values, values.length, 8, 200).bins());
        Assertions.assertEquals(1, Discretization.fitQuantile(values, 50, 8, 100).bins());
        // ties: a constant input yields one bin whatever was requested
        final double[] constant = new double[100];
        java.util.Arrays.fill(constant, 7);
        final Discretization one = Discretization.fitQuantile(constant, constant.length, 8, null);
        Assertions.assertEquals(1, one.bins());
        Assertions.assertEquals(1L, one.bin(7.0));
        Assertions.assertEquals(2L, one.bin(8.0));
    }

    @Test
    public void testEmptyFit() {
        final Discretization d = Discretization.fitQuantile(new double[0], 0, 5, null);
        Assertions.assertEquals(0, d.n);
        Assertions.assertEquals(1, d.bins());
        Assertions.assertEquals(1L, d.bin(123.0));
        Assertions.assertEquals(-1L, d.bin(null));
        Assertions.assertEquals(1L, Discretization.fromJson(d.toJson()).bin(-5.0));
    }

    @Test
    public void testGatherDoublesCompacts() {
        final FeatureStages.GatherDoublesFn fn = new FeatureStages.GatherDoublesFn();
        FeatureStages.Doubles a = fn.createAccumulator();
        for (int i = 0; i < 100; i++) a = fn.addInput(a, (double) i);
        Assertions.assertTrue(a.values.length > a.size);
        Assertions.assertEquals(100, fn.compact(a).values.length, "compact drops the spare capacity");
        FeatureStages.Doubles b = fn.createAccumulator();
        for (int i = 0; i < 37; i++) b = fn.addInput(b, (double) -i);
        final FeatureStages.Doubles merged = fn.mergeAccumulators(List.of(a, b, fn.createAccumulator()));
        Assertions.assertEquals(137, merged.size);
        Assertions.assertEquals(137, merged.values.length, "the merge allocates the sum of the parts once");
        Assertions.assertEquals(137, fn.extractOutput(merged).values.length);
        Assertions.assertEquals(-36d, merged.values[136]);
        // an empty gather (input without values) still yields an accumulator: the fit sees n = 0
        final FeatureStages.Doubles empty = fn.extractOutput(fn.createAccumulator());
        Assertions.assertEquals(0, empty.size);
        Assertions.assertEquals(0, empty.values.length);
        empty.add(1.5);
        Assertions.assertEquals(1, empty.size);
    }

}
