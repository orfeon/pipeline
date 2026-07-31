package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SyntheticReferenceTest {

    @Test
    public void testMarginalIndependenceModel() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d1", "d2"), List.of("m"));
        builder.addTarget(new String[]{"a", "x"}, new double[]{90});
        builder.addTarget(new String[]{"a", "y"}, new double[]{10});
        builder.addTarget(new String[]{"b", "x"}, new double[]{10});
        builder.addTarget(new String[]{"b", "y"}, new double[]{90});

        final LeafTable table = SyntheticReference.marginal(builder.build());

        // T=200, all marginals = 100 -> expected = 200 * 0.5 * 0.5 = 50 everywhere
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            Assertions.assertEquals(50.0, table.baselineValue(0, leaf), 1e-9);
        }
        // Targets untouched
        Assertions.assertEquals(200.0, table.targetTotal(0), 1e-9);
    }

    @Test
    public void testMarginalZeroTotalProducesZeroBaseline() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d1"), List.of("m"));
        builder.addTarget(new String[]{"a"}, new double[]{0});
        builder.addTarget(new String[]{"b"}, new double[]{0});

        final LeafTable table = SyntheticReference.marginal(builder.build());
        Assertions.assertEquals(0.0, table.baselineTotal(0), 1e-9);
    }

    @Test
    public void testMarginalRejectsNegativeTargets() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d1"), List.of("m"));
        builder.addTarget(new String[]{"a"}, new double[]{100});
        builder.addTarget(new String[]{"b"}, new double[]{-1});

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SyntheticReference.marginal(builder.build()));
    }
}
