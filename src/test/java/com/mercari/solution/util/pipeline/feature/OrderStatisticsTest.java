package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class OrderStatisticsTest {

    @Test
    public void testMatchesSortedListUnderInsertsAndDeletes() {
        final Random random = new Random(7);
        final OrderStatistics order = new OrderStatistics();
        final List<Double> reference = new ArrayList<>();
        // enough values to split blocks several times, with ties and deletions of present / absent values
        for (int i = 0; i < 20_000; i++) {
            final double v = random.nextInt(500) / 4d;
            if (!reference.isEmpty() && random.nextInt(3) == 0) {
                final double target = random.nextBoolean() ? reference.get(random.nextInt(reference.size())) : v;
                final boolean present = reference.remove((Double) target);
                Assertions.assertEquals(present, order.remove(target), "remove " + target + " @" + i);
            } else {
                order.add(v);
                reference.add(v);
            }
            Assertions.assertEquals(reference.size(), order.size());
            if (i % 997 == 0 && !reference.isEmpty()) {
                final List<Double> sorted = new ArrayList<>(reference);
                Collections.sort(sorted);
                for (int k = 0; k < sorted.size(); k += Math.max(1, sorted.size() / 50)) {
                    Assertions.assertEquals(sorted.get(k), order.select(k), "select " + k + " @" + i);
                }
                final double[] array = sorted.stream().mapToDouble(Double::doubleValue).toArray();
                for (final double p : new double[]{0, 0.1, 0.25, 0.5, 0.9, 1}) {
                    Assertions.assertEquals(OrderStatistics.quantile(p, array, array.length), order.quantile(p), 1e-12, "quantile " + p + " @" + i);
                }
            }
        }
        Assertions.assertTrue(order.size() > 1024, "blocks must have split: " + order.size());
    }

    @Test
    public void testBlocksEmptyAndRefill() {
        // remove everything (blocks disappear), then refill: the rank tree must be rebuilt correctly
        final OrderStatistics order = new OrderStatistics();
        for (int i = 0; i < 5000; i++) order.add(i % 700);
        for (int i = 0; i < 5000; i++) Assertions.assertTrue(order.remove(i % 700), "remove " + i);
        Assertions.assertEquals(0, order.size());
        Assertions.assertNull(order.quantile(0.5));
        Assertions.assertFalse(order.remove(1));
        for (int i = 3000; i > 0; i--) order.add(i);
        Assertions.assertEquals(3000, order.size());
        Assertions.assertEquals(1, order.select(0), 1e-12);
        Assertions.assertEquals(3000, order.select(2999), 1e-12);
        Assertions.assertEquals(1500.5, order.quantile(0.5), 1e-12);
        // interpolation across a block boundary: both neighbours are read from the right blocks
        for (int k = 0; k < 2999; k += 7) Assertions.assertEquals(k + 1.5, order.quantile((k + 0.5) / 2999), 1e-9, "k=" + k);
    }

    @Test
    public void testType7Quantile() {
        final double[] sorted = {80, 100, 200};
        Assertions.assertEquals(80, OrderStatistics.quantile(0, sorted, 3), 1e-12);
        Assertions.assertEquals(90, OrderStatistics.quantile(0.25, sorted, 3), 1e-12);
        Assertions.assertEquals(100, OrderStatistics.quantile(0.5, sorted, 3), 1e-12);
        Assertions.assertEquals(200, OrderStatistics.quantile(1, sorted, 3), 1e-12);
        Assertions.assertEquals(150, OrderStatistics.quantile(0.5, new double[]{100, 200}, 2), 1e-12);
        final OrderStatistics empty = new OrderStatistics();
        Assertions.assertNull(empty.quantile(0.5));
        empty.add(5);
        Assertions.assertEquals(5, empty.quantile(0.99), 1e-12);
    }

}
