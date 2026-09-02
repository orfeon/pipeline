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
