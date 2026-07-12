package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AdtributorTest {

    /** 3x3 grid, baseline 100 per leaf, target +50 on every region=a leaf. */
    private static LeafTable fixture() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("region", "category"), List.of("m"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String category : List.of("x", "y", "z")) {
                builder.addBaseline(new String[]{region, category}, new double[]{100});
                builder.addTarget(new String[]{region, category}, new double[]{"a".equals(region) ? 150 : 100});
            }
        }
        return builder.build();
    }

    private static EngineConfig config(final double teep, final double tep) {
        return new EngineConfig(
                EngineConfig.Algorithm.adtributor,
                EngineConfig.RiskLocParams.defaults(),
                new EngineConfig.AdtributorParams(teep, tep),
                EngineConfig.Guards.defaults(),
                DerivedAllocation.Method.gre,
                3);
    }

    @Test
    public void testSingleDimensionCulprit() {
        final LeafTable table = fixture();
        final List<Finding> findings = new Adtributor()
                .localize(table, table.measureVector("m"), config(0.1, 0.67));

        Assertions.assertFalse(findings.isEmpty());
        final Finding top = findings.getFirst();
        // region=a fully explains the change: EP = (450-300)/(1050-900) = 1.0
        Assertions.assertEquals(1, top.slices().size());
        Assertions.assertEquals("a", top.slices().getFirst().values()[0]);
        Assertions.assertEquals(0, top.slices().getFirst().dims()[0]);
        Assertions.assertEquals(1.0, top.explanatoryPower(), 1e-9);
        Assertions.assertEquals(300.0, top.baselineSum(), 1e-9);
        Assertions.assertEquals(450.0, top.targetSum(), 1e-9);
        Assertions.assertEquals(3, top.leafCount());
        // region=a has higher surprise than the uniform category candidate
        Assertions.assertTrue(top.surprise() > 0);
    }

    @Test
    public void testTeepFiltersLowExplanatoryPowerValues() {
        final LeafTable table = fixture();
        // Each category value has EP = 1/3 < teep -> the category dimension yields no candidate
        final List<Finding> findings = new Adtributor()
                .localize(table, table.measureVector("m"), config(0.4, 0.67));

        Assertions.assertEquals(1, findings.size());
        Assertions.assertEquals("a", findings.getFirst().slices().getFirst().values()[0]);
    }

    @Test
    public void testNoFindingWhenNoChange() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d"), List.of("m"));
        builder.addBaseline(new String[]{"a"}, new double[]{100});
        builder.addTarget(new String[]{"a"}, new double[]{100});
        builder.addBaseline(new String[]{"b"}, new double[]{100});
        builder.addTarget(new String[]{"b"}, new double[]{100});
        final LeafTable table = builder.build();

        final List<Finding> findings = new Adtributor()
                .localize(table, table.measureVector("m"), config(0.1, 0.67));
        Assertions.assertTrue(findings.isEmpty());
    }

    @Test
    public void testJsDivergenceZeroSafety() {
        // p = 0: p-term drops, q-term = q * ln(2)
        final double surprise = Adtributor.jsDivergence(0, 50, 100, 100);
        Assertions.assertEquals(0.5 * 0.5 * Math.log(2), surprise, 1e-9);
        // p = q = 0
        Assertions.assertEquals(0.0, Adtributor.jsDivergence(0, 0, 100, 100), 1e-9);
        // identical distributions
        Assertions.assertEquals(0.0, Adtributor.jsDivergence(50, 50, 100, 100), 1e-9);
    }
}
