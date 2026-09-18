package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.*;

/**
 * The algebra every {@link Summary} family must satisfy: {@code merge} is associative with {@code create} as its
 * identity (a monoid — summaries of disjoint row sets combine in any order), and an {@link Summary#invertible}
 * family removes a contribution exactly (a group — a window can evict). The readouts are checked against the
 * direct formulas on the same values.
 */
public class SummaryTest {

    private static final Summary.Readout COUNT = Summary.Readout.of("count");

    /** Folds the values in one by one. */
    private static <S extends Serializable> S fold(final Summary<S> family, final List<?> values) {
        final S s = family.create();
        for (final Object v : values) family.update(s, v, 1);
        return s;
    }

    /** Merging the two halves in either order equals folding everything, for every readout given. */
    private static <S extends Serializable> void assertMonoid(final Summary<S> family, final List<?> values, final Summary.Readout... readouts) {
        assertMonoid(family, values, Assertions::assertEquals, readouts);
    }

    /** {@link #assertMonoid} with the comparison given: a family that re-anchors on merge agrees up to floating rounding. */
    private static <S extends Serializable> void assertMonoid(final Summary<S> family, final List<?> values,
                                                              final java.util.function.BiConsumer<Object, Object> assertSame, final Summary.Readout... readouts) {
        final int cut = values.size() / 3;
        final S left = fold(family, values.subList(0, cut)), right = fold(family, values.subList(cut, values.size()));
        final S all = fold(family, values);
        final S leftFirst = fold(family, values.subList(0, cut));
        family.merge(leftFirst, right);
        final S rightFirst = fold(family, values.subList(cut, values.size()));
        family.merge(rightFirst, left);
        final S withEmpty = fold(family, values);
        family.merge(withEmpty, family.create());
        for (final Summary.Readout r : readouts) {
            assertSame.accept(family.read(all, r), family.read(leftFirst, r));
            assertSame.accept(family.read(all, r), family.read(rightFirst, r));
            assertSame.accept(family.read(all, r), family.read(withEmpty, r));
        }
        Assertions.assertEquals(family.count(all), family.count(leftFirst), 1e-12);
    }

    /** Random adds and removes read the same as a fresh fold of the values currently inside (up to floating rounding). */
    private static <S extends Serializable> void assertInvertible(final Summary<S> family, final Random random, final java.util.function.Supplier<Object> draw,
                                                                    final java.util.function.BiConsumer<Object, Object> assertClose, final Summary.Readout... readouts) {
        Assertions.assertTrue(family.invertible());
        final S s = family.create();
        final List<Object> inside = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            if (!inside.isEmpty() && random.nextInt(3) == 0) {
                final Object v = inside.remove(random.nextInt(inside.size()));
                family.update(s, v, -1);
            } else {
                final Object v = draw.get();
                inside.add(v);
                family.update(s, v, 1);
            }
            final S fresh = fold(family, inside);
            for (final Summary.Readout r : readouts) assertClose.accept(family.read(fresh, r), family.read(s, r));
        }
    }

    private static void assertClose(final Object expected, final Object actual) {
        if (expected == null || actual == null) {
            Assertions.assertEquals(expected, actual);
        } else if (expected instanceof Double e) {
            Assertions.assertEquals(e, (Double) actual, 1e-9 * Math.max(1, Math.abs(e)));
        } else {
            Assertions.assertEquals(expected, actual);
        }
    }

    @Test
    public void testMoments() {
        final Summary<Summary.Moments.State> m = Summary.Summaries.MOMENTS;
        final List<Double> xs = List.of(1.0, 2.0, 4.0, 7.0);
        final Summary.Moments.State s = fold(m, xs);
        Assertions.assertEquals(4L, m.read(s, COUNT));
        Assertions.assertEquals(14.0, (Double) m.read(s, Summary.Readout.of("sum")), 1e-12);
        Assertions.assertEquals(3.5, (Double) m.read(s, Summary.Readout.of("mean")), 1e-12);
        Assertions.assertEquals(3.5, (Double) m.read(s, Summary.Readout.of("rate")), 1e-12);
        Assertions.assertEquals(Math.sqrt((1 + 4 + 16 + 49) / 4.0 - 3.5 * 3.5), (Double) m.read(s, Summary.Readout.of("std")), 1e-12);
        // the empty state: count 0, everything else null; a single value has no std
        final Summary.Moments.State empty = m.create();
        Assertions.assertEquals(0L, m.read(empty, COUNT));
        Assertions.assertNull(m.read(empty, Summary.Readout.of("sum")));
        Assertions.assertNull(m.read(empty, Summary.Readout.of("mean")));
        Assertions.assertNull(m.read(fold(m, List.of(3.0)), Summary.Readout.of("std")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> m.read(s, Summary.Readout.of("max")));
        final Summary.Readout[] readouts = {COUNT, Summary.Readout.of("sum"), Summary.Readout.of("mean"), Summary.Readout.of("std")};
        assertMonoid(m, xs, readouts);
        final Random random = new Random(3);
        assertInvertible(m, random, () -> Math.round(random.nextGaussian() * 1000) / 100.0, SummaryTest::assertClose, readouts);
    }

    /** 1, 2, 3, 4, 10: mean 4, deviations −3, −2, −1, 0, 6 → m₂ = 10, m₃ = 36, m₄ = 278.8. */
    @Test
    public void testShape() {
        final Summary<Summary.Shape.State> h = Summary.Summaries.SHAPE;
        final List<Double> xs = List.of(1.0, 2.0, 3.0, 4.0, 10.0);
        final Summary.Shape.State s = fold(h, xs);
        final Summary.Readout skew = Summary.Readout.of("skew"), kurt = Summary.Readout.of("kurt");
        Assertions.assertEquals(5L, h.read(s, COUNT));
        Assertions.assertEquals(36 / Math.pow(10, 1.5), (Double) h.read(s, skew), 1e-12);
        Assertions.assertEquals(278.8 / 100 - 3, (Double) h.read(s, kurt), 1e-12);
        // a symmetric series has no skew; the two-point distribution has the smallest excess kurtosis there is (−2)
        Assertions.assertEquals(0.0, (Double) h.read(fold(h, List.of(-2.0, -1.0, 0.0, 1.0, 2.0)), skew), 1e-12);
        Assertions.assertEquals(-2.0, (Double) h.read(fold(h, List.of(1.0, 3.0, 1.0, 3.0)), kurt), 1e-12);
        // too few values, and a series without spread (exactly, or up to the rounding of a non-representable constant)
        Assertions.assertNull(h.read(fold(h, List.of(1.0, 2.0)), skew));
        Assertions.assertNull(h.read(fold(h, List.of(1.0, 2.0, 4.0)), kurt));
        Assertions.assertNull(h.read(h.create(), skew));
        Assertions.assertNull(h.read(fold(h, List.of(7.0, 7.0, 7.0, 7.0)), skew));
        final Summary.Shape.State constant = fold(h, List.of(5.0, 0.1, 0.1, 0.1, 0.1));
        h.update(constant, 5.0, -1);
        Assertions.assertNull(h.read(constant, kurt), "the evicted 5.0 leaves a constant series: no spread, not noise");
        Assertions.assertThrows(IllegalArgumentException.class, () -> h.read(s, Summary.Readout.of("mean")));
        // the anchor keeps a level offset out of the fourth powers
        final List<Double> offset = xs.stream().map(x -> x + 1e6).toList();
        Assertions.assertEquals(36 / Math.pow(10, 1.5), (Double) h.read(fold(h, offset), skew), 1e-9);
        Assertions.assertEquals(278.8 / 100 - 3, (Double) h.read(fold(h, offset), kurt), 1e-9);

        final Summary.Readout[] readouts = {COUNT, skew, kurt};
        assertMonoid(h, xs, SummaryTest::assertClose, readouts);
        assertMonoid(h, offset, SummaryTest::assertClose, readouts);
        final Random random = new Random(13);
        assertInvertible(h, random, () -> Math.round(Math.exp(random.nextGaussian()) * 1000) / 100.0, SummaryTest::assertClose, readouts);
        // emptied by eviction, the state starts over exactly
        final Summary.Shape.State emptied = fold(h, xs);
        for (final Double x : xs) h.update(emptied, x, -1);
        Assertions.assertFalse(emptied.anchored);
        Assertions.assertEquals(0.0, emptied.s4, 0);
    }

    @Test
    public void testExtrema() {
        final Summary<Summary.Extrema.State> e = Summary.Summaries.EXTREMA;
        final List<Double> xs = List.of(3.0, -1.0, 7.5, 2.0);
        final Summary.Extrema.State s = fold(e, xs);
        Assertions.assertEquals(7.5, e.read(s, Summary.Readout.of("max")));
        Assertions.assertEquals(-1.0, e.read(s, Summary.Readout.of("min")));
        Assertions.assertEquals(4L, e.read(s, COUNT));
        Assertions.assertNull(e.read(e.create(), Summary.Readout.of("max")));
        Assertions.assertFalse(e.invertible());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> e.update(s, 3.0, -1));
        assertMonoid(e, xs, Summary.Readout.of("max"), Summary.Readout.of("min"), COUNT);
    }

    @Test
    public void testCounts() {
        final Summary<Summary.Counts.State> c = Summary.Summaries.COUNTS;
        final List<Object> xs = List.of("a", "b", "a", 1, "a");
        final Summary.Counts.State s = fold(c, xs);
        final Map<?, ?> dist = (Map<?, ?>) c.read(s, Summary.Readout.of("distribution"));
        Assertions.assertEquals(3.0 / 5, (Double) dist.get("a"), 1e-12);
        Assertions.assertEquals(1.0 / 5, (Double) dist.get("b"), 1e-12);
        Assertions.assertEquals(1.0 / 5, (Double) dist.get("1"), 1e-12, "categories are matched by their string form");
        Assertions.assertEquals(5L, c.read(s, COUNT));
        Assertions.assertNull(c.read(c.create(), Summary.Readout.of("distribution")));
        // evicting the only "b" drops it from the distribution (no zero-share category)
        c.update(s, "b", -1);
        Assertions.assertFalse(((Map<?, ?>) c.read(s, Summary.Readout.of("distribution"))).containsKey("b"));
        assertMonoid(c, xs, Summary.Readout.of("distribution"), COUNT);
        final Random random = new Random(5);
        assertInvertible(c, random, () -> "g" + random.nextInt(4), (expected, actual) -> {
            if (expected == null || actual == null) Assertions.assertEquals(expected, actual);
            else if (expected instanceof Map<?, ?> em) {
                final Map<?, ?> am = (Map<?, ?>) actual;
                Assertions.assertEquals(em.keySet(), am.keySet());
                for (final Object k : em.keySet()) Assertions.assertEquals((Double) em.get(k), (Double) am.get(k), 1e-12);
            } else Assertions.assertEquals(expected, actual);
        }, Summary.Readout.of("distribution"), COUNT);
    }

    @Test
    public void testOrder() {
        final Summary<Summary.Order.State> o = Summary.Summaries.ORDER;
        final List<Double> xs = List.of(5.0, 1.0, 3.0, 9.0, 7.0);
        final Summary.Order.State s = fold(o, xs);
        Assertions.assertEquals(5.0, (Double) o.read(s, Summary.Readout.of("quantile", 0.5)), 1e-12);
        Assertions.assertEquals(1.0 + 0.25 * 4 * 2, (Double) o.read(s, Summary.Readout.of("quantile", 0.25)), 1e-12); // type 7: h = 1 → 3.0
        Assertions.assertEquals(5L, o.read(s, COUNT));
        Assertions.assertNull(o.read(o.create(), Summary.Readout.of("quantile", 0.5)));
        final Summary.Readout[] readouts = {Summary.Readout.of("quantile", 0.5), Summary.Readout.of("quantile", 0.9), COUNT};
        assertMonoid(o, xs, readouts);
        final Random random = new Random(9);
        assertInvertible(o, random, () -> (double) random.nextInt(50), SummaryTest::assertClose, readouts);
    }

    /**
     * x = 1..5, y = 2x + 1 + (0.5, −0.5, 0, 0.5, −0.5): var(x) = 2, cov = 3.8 → beta 1.9, intercept 7 − 1.9 · 3 = 1.3;
     * var(y) = 7.4 (Σy² = 282 over 5, mean 7) → corr = 3.8 / √14.8.
     */
    @Test
    public void testRegression() {
        final Summary<Summary.Regression.State> g = Summary.Summaries.REGRESSION;
        final List<double[]> pairs = List.of(new double[]{1, 3.5}, new double[]{2, 4.5}, new double[]{3, 7}, new double[]{4, 9.5}, new double[]{5, 10.5});
        final Summary.Regression.State s = fold(g, pairs);
        Assertions.assertEquals(5L, g.read(s, COUNT));
        Assertions.assertEquals(3.8, (Double) g.read(s, Summary.Readout.of("cov")), 1e-12);
        Assertions.assertEquals(1.9, (Double) g.read(s, Summary.Readout.of("beta")), 1e-12);
        Assertions.assertEquals(1.3, (Double) g.read(s, Summary.Readout.of("intercept")), 1e-12);
        Assertions.assertEquals(3.8 / Math.sqrt(2 * 7.4), (Double) g.read(s, Summary.Readout.of("corr")), 1e-12);
        Assertions.assertEquals(3.8 * 3.8 / (2 * 7.4), (Double) g.read(s, Summary.Readout.of("r2")), 1e-12);
        // fewer than two pairs: nothing but the count; a constant series has no correlation, a constant x no slope
        Assertions.assertNull(g.read(g.create(), Summary.Readout.of("cov")));
        Assertions.assertNull(g.read(fold(g, pairs.subList(0, 1)), Summary.Readout.of("beta")));
        final Summary.Regression.State flatX = fold(g, List.of(new double[]{2, 1}, new double[]{2, 5}));
        Assertions.assertNull(g.read(flatX, Summary.Readout.of("beta")));
        Assertions.assertNull(g.read(flatX, Summary.Readout.of("corr")));
        Assertions.assertEquals(0.0, (Double) g.read(flatX, Summary.Readout.of("cov")), 1e-12);
        final Summary.Regression.State flatY = fold(g, List.of(new double[]{1, 4}, new double[]{3, 4}));
        Assertions.assertEquals(0.0, (Double) g.read(flatY, Summary.Readout.of("beta")), 1e-12);
        Assertions.assertNull(g.read(flatY, Summary.Readout.of("r2")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> g.read(s, Summary.Readout.of("mean")));
        // the anchor keeps a large common offset out of the products: a price level of 1e9 with unit moves
        final List<double[]> offset = new ArrayList<>();
        for (final double[] p : pairs) offset.add(new double[]{p[0] + 1e9, p[1] + 3e9});
        Assertions.assertEquals(1.9, (Double) g.read(fold(g, offset), Summary.Readout.of("beta")), 1e-9);
        Assertions.assertEquals(3.8 / Math.sqrt(2 * 7.4), (Double) g.read(fold(g, offset), Summary.Readout.of("corr")), 1e-9);
        Assertions.assertEquals(1.3 + 3e9 - 1.9 * 1e9, (Double) g.read(fold(g, offset), Summary.Readout.of("intercept")), 1e-3);

        final Summary.Readout[] readouts = {COUNT, Summary.Readout.of("cov"), Summary.Readout.of("corr"), Summary.Readout.of("beta"), Summary.Readout.of("intercept"), Summary.Readout.of("r2")};
        assertMonoid(g, pairs, SummaryTest::assertClose, readouts);
        assertMonoid(g, offset, SummaryTest::assertClose, readouts);
        final Random random = new Random(17);
        assertInvertible(g, random, () -> {
            final double x = Math.round(random.nextGaussian() * 1000) / 100.0;
            return new double[]{x, 0.7 * x + Math.round(random.nextGaussian() * 300) / 100.0};
        }, SummaryTest::assertClose, readouts);
        // emptied by eviction, the state starts over exactly (no rounding residue, a fresh anchor)
        final Summary.Regression.State emptied = fold(g, pairs);
        for (final double[] p : pairs) g.update(emptied, p, -1);
        Assertions.assertEquals(0, g.count(emptied), 0);
        Assertions.assertFalse(emptied.anchored);
        Assertions.assertEquals(0.0, emptied.sxy, 0);
    }

    @Test
    public void testCatalogMapsStatisticsToFamilies() {
        for (final String func : OperatorCatalog.REGRESSION_FUNCS) Assertions.assertSame(Summary.Summaries.REGRESSION, OperatorCatalog.summary(func).family(), func);
        Assertions.assertSame(Summary.Summaries.MOMENTS, OperatorCatalog.summary("mean").family());
        Assertions.assertSame(Summary.Summaries.MOMENTS, OperatorCatalog.summary("count").family());
        Assertions.assertEquals("std", OperatorCatalog.summary("std").readout().name());
        Assertions.assertSame(Summary.Summaries.SHAPE, OperatorCatalog.summary("skew").family());
        Assertions.assertEquals("kurt", OperatorCatalog.summary("kurt").readout().name());
        for (final String series : List.of("zeroCross", "peaks", "acf1", "pacf2", "ar2_1")) Assertions.assertNull(OperatorCatalog.summary(series), series + " reads neighbouring values: scan only");
        Assertions.assertSame(Summary.Summaries.EXTREMA, OperatorCatalog.summary("max").family());
        Assertions.assertSame(Summary.Summaries.COUNTS, OperatorCatalog.summary("distribution").family());
        final Summary.Spec q90 = OperatorCatalog.summary("q90");
        Assertions.assertSame(Summary.Summaries.ORDER, q90.family());
        Assertions.assertEquals("quantile", q90.readout().name());
        Assertions.assertEquals(0.9, q90.readout().parameter(), 1e-12);
        Assertions.assertEquals(0.5, OperatorCatalog.summary("quantile").readout().parameter(), 1e-12);
        // scan-only tokens have no family: share (a composition of two hidden counts), first / last (positional), unknown
        for (final String scanOnly : List.of("share", "first", "last", "q101", "median_diff")) Assertions.assertNull(OperatorCatalog.summary(scanOnly), scanOnly);
        Assertions.assertNull(OperatorCatalog.summary(null));
        // the population evaluator serves exactly the statistics with a family
        Assertions.assertTrue(PopulationEvaluator.isSupported("sum"));
        Assertions.assertTrue(PopulationEvaluator.isSupported("q25"));
        Assertions.assertFalse(PopulationEvaluator.isSupported("share"));
    }

}
