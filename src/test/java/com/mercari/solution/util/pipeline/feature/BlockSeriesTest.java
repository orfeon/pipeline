package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class BlockSeriesTest {

    private static Summary.Moments.State moments(final double... values) {
        final Summary.Moments.State s = Summary.Summaries.MOMENTS.create();
        for (final double v : values) Summary.Summaries.MOMENTS.update(s, v, 1);
        return s;
    }

    private static double sum(final Summary.Moments.State s) {
        return s == null ? Double.NaN : s.sum;
    }

    /** Blocks 10, 12, 15 with sums 1, 2, 4: prefix windows, ranged windows and the change points under a window. */
    @Test
    public void testWindowsAndChangePoints() {
        final Map<Long, Summary.Moments.State> parts = Map.of(10L, moments(1), 12L, moments(2), 15L, moments(4));
        final BlockSeries<Summary.Moments.State> series = new BlockSeries<>(Summary.Summaries.MOMENTS, parts);
        Assertions.assertEquals(new TreeSet<>(List.of(10L, 12L, 15L)), series.observed());
        // every block up to the usable one
        Assertions.assertNull(series.window(9, 0));
        Assertions.assertEquals(1, sum(series.window(10, 0)), 1e-12);
        Assertions.assertEquals(3, sum(series.window(13, 0)), 1e-12);
        Assertions.assertEquals(7, sum(series.window(99, 0)), 1e-12);
        Assertions.assertEquals(7, sum(series.total()), 1e-12);
        // a window of 2 blocks: (usable − 2, usable]
        Assertions.assertEquals(2, sum(series.window(13, 2)), 1e-12);   // {12, 13}
        Assertions.assertEquals(2, sum(series.window(12, 2)), 1e-12);   // (10, 12] = {11, 12}: block 12 only
        Assertions.assertNull(series.window(14, 1));                    // (13, 14]: nothing
        Assertions.assertEquals(4, sum(series.window(16, 2)), 1e-12);   // {15, 16}
        // change points: the observed blocks and, under a window, where each leaves
        Assertions.assertEquals(new TreeSet<>(List.of(10L, 12L, 15L)), series.changePoints(0));
        Assertions.assertEquals(new TreeSet<>(List.of(10L, 12L, 14L, 15L, 17L)), series.changePoints(2));
    }

    /** One model per change point; a row reads the floor entry of its usable block unless too few blocks precede it. */
    @Test
    public void testModelsAndLookup() {
        final Map<Long, Summary.Moments.State> parts = Map.of(10L, moments(1), 12L, moments(2), 15L, moments(4));
        final BlockSeries<Summary.Moments.State> series = new BlockSeries<>(Summary.Summaries.MOMENTS, parts);
        final TreeMap<Long, Double> prefix = series.models(0, s -> s.sum);
        Assertions.assertEquals(Map.of(10L, 1.0, 12L, 3.0, 15L, 7.0), prefix);
        final TreeSet<Long> observed = series.observed();
        Assertions.assertNull(BlockSeries.lookup(prefix, observed, 9, 1));
        Assertions.assertEquals(1.0, BlockSeries.lookup(prefix, observed, 10, 1));
        Assertions.assertEquals(1.0, BlockSeries.lookup(prefix, observed, 11, 1));
        Assertions.assertEquals(3.0, BlockSeries.lookup(prefix, observed, 14, 1));
        Assertions.assertEquals(7.0, BlockSeries.lookup(prefix, observed, 40, 1));
        // minBlocks counts observed blocks at or before the usable block
        Assertions.assertNull(BlockSeries.lookup(prefix, observed, 11, 2));
        Assertions.assertEquals(3.0, BlockSeries.lookup(prefix, observed, 12, 2));
        Assertions.assertNull(BlockSeries.lookup(prefix, observed, 14, 3));
        Assertions.assertEquals(7.0, BlockSeries.lookup(prefix, observed, 15, 3));
        // under a window the model at a leave point is fitted on the empty state (the fit decides what that means)
        final TreeMap<Long, Double> windowed = series.models(2, s -> s.n == 0 ? Double.NaN : s.sum);
        Assertions.assertEquals(1.0, windowed.get(10L));
        Assertions.assertEquals(2.0, windowed.get(12L));   // (10, 12]: block 12 only
        Assertions.assertTrue(Double.isNaN(windowed.get(14L)), "block 12 left at 14, nothing else inside");
        Assertions.assertEquals(4.0, windowed.get(15L));
        Assertions.assertTrue(Double.isNaN(windowed.get(17L)));
        Assertions.assertEquals(2.0, BlockSeries.lookup(windowed, observed, 13, 1));
        Assertions.assertTrue(Double.isNaN(BlockSeries.lookup(windowed, observed, 14, 1)));
        Assertions.assertNull(BlockSeries.lookup(null, observed, 14, 1));
        Assertions.assertTrue(new BlockSeries<>(Summary.Summaries.MOMENTS, Map.of()).isEmpty());
        Assertions.assertNull(new BlockSeries<>(Summary.Summaries.MOMENTS, Map.of()).total());
    }

    /** A non-invertible family (extrema) is served by merging the range: the algebra needs the monoid law only. */
    @Test
    public void testNonInvertibleFamily() {
        final Summary<Summary.Extrema.State> family = Summary.Summaries.EXTREMA;
        final Map<Long, Summary.Extrema.State> parts = new HashMap<>();
        for (final long[] e : new long[][]{{1, 5}, {2, 9}, {3, 2}}) {
            final Summary.Extrema.State s = family.create();
            family.update(s, (double) e[1], 1);
            parts.put(e[0], s);
        }
        final BlockSeries<Summary.Extrema.State> series = new BlockSeries<>(family, parts);
        Assertions.assertEquals(9.0, series.window(3, 0).max);
        Assertions.assertEquals(2.0, series.window(3, 1).max);
        Assertions.assertEquals(9.0, series.window(3, 2).max);
        Assertions.assertEquals(2.0, series.window(3, 0).min);
    }

}
