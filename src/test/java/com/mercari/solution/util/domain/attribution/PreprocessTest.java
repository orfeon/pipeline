package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class PreprocessTest {

    @Test
    public void testMaxCardinalityBucketsTailValuesIntoOther() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("id"), List.of("m"));
        final double[] volumes = { 500, 400, 300, 20, 10 };
        for(int i = 0; i < volumes.length; i++) {
            builder.addBaseline(new String[]{"v" + i}, new double[]{volumes[i]});
            builder.addTarget(new String[]{"v" + i}, new double[]{volumes[i]});
        }
        final LeafTable table = Preprocess.applyGuards(
                builder.build(), new EngineConfig.Guards(0, 3, 3));

        final Set<String> values = table.dimensionValues(0);
        Assertions.assertEquals(Set.of("v0", "v1", "v2", Preprocess.OTHER_VALUE), values);
        // v3 + v4 merged into "other"
        final int otherLeaf = findLeaf(table, Preprocess.OTHER_VALUE);
        Assertions.assertEquals(30.0, table.targetValue(0, otherLeaf), 1e-9);
        Assertions.assertEquals(4, table.leafCount());
    }

    @Test
    public void testMinSupportBucketsLowVolumeValuesIntoOther() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("id"), List.of("m"));
        builder.addBaseline(new String[]{"a"}, new double[]{100});
        builder.addTarget(new String[]{"a"}, new double[]{100});
        builder.addBaseline(new String[]{"b"}, new double[]{100});
        builder.addTarget(new String[]{"b"}, new double[]{100});
        builder.addBaseline(new String[]{"c"}, new double[]{0.5});
        builder.addTarget(new String[]{"c"}, new double[]{0.5});

        final LeafTable table = Preprocess.applyGuards(
                builder.build(), new EngineConfig.Guards(0.005, 3, 200));

        Assertions.assertEquals(Set.of("a", "b", Preprocess.OTHER_VALUE), table.dimensionValues(0));
    }

    @Test
    public void testGuardsNoopWhenWithinLimits() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("id"), List.of("m"));
        builder.addBaseline(new String[]{"a"}, new double[]{100});
        builder.addTarget(new String[]{"a"}, new double[]{100});
        final LeafTable original = builder.build();

        final LeafTable table = Preprocess.applyGuards(original, EngineConfig.Guards.defaults());
        Assertions.assertSame(original, table);
    }

    @Test
    public void testQuantileBinning() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("price"), List.of("m"));
        for(int value = 1; value <= 8; value++) {
            builder.addBaseline(new String[]{Integer.toString(value)}, new double[]{1});
            builder.addTarget(new String[]{Integer.toString(value)}, new double[]{1});
        }
        builder.addBaseline(new String[]{"abc"}, new double[]{1});
        builder.addTarget(new String[]{"abc"}, new double[]{1});

        final LeafTable table = Preprocess.bin(builder.build(), List.of(
                DimensionSpec.binned("price", DimensionSpec.Binning.Method.quantile, 2)));

        Assertions.assertEquals(Set.of("[1,5)", "[5,8]", Preprocess.OTHER_VALUE), table.dimensionValues(0));
        // 4 leaves in each bin, 1 unparseable
        Assertions.assertEquals(3, table.leafCount());
        Assertions.assertEquals(4.0, table.targetValue(0, findLeaf(table, "[1,5)")), 1e-9);
        Assertions.assertEquals(4.0, table.targetValue(0, findLeaf(table, "[5,8]")), 1e-9);
    }

    @Test
    public void testWidthBinning() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("price"), List.of("m"));
        for(final String value : List.of("0", "2", "5", "10")) {
            builder.addBaseline(new String[]{value}, new double[]{1});
            builder.addTarget(new String[]{value}, new double[]{1});
        }
        final LeafTable table = Preprocess.bin(builder.build(), List.of(
                DimensionSpec.binned("price", DimensionSpec.Binning.Method.width, 2)));

        Assertions.assertEquals(Set.of("[0,5)", "[5,10]"), table.dimensionValues(0));
        Assertions.assertEquals(2.0, table.targetValue(0, findLeaf(table, "[0,5)")), 1e-9);
        Assertions.assertEquals(2.0, table.targetValue(0, findLeaf(table, "[5,10]")), 1e-9);
    }

    @Test
    public void testFlatDimensionsUntouchedByBinning() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("region"), List.of("m"));
        builder.addBaseline(new String[]{"tokyo"}, new double[]{1});
        builder.addTarget(new String[]{"tokyo"}, new double[]{1});
        final LeafTable original = builder.build();

        final LeafTable table = Preprocess.bin(original, List.of(DimensionSpec.flat("region")));
        Assertions.assertSame(original, table);
    }

    private static int findLeaf(final LeafTable table, final String value) {
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            if(value.equals(table.dimValue(leaf, 0))) {
                return leaf;
            }
        }
        throw new AssertionError("leaf not found: " + value);
    }
}
