package com.mercari.solution.util.domain.attribution.algorithm;

import com.mercari.solution.util.domain.attribution.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SqueezeTest {

    private static EngineConfig config() {
        return new EngineConfig(
                EngineConfig.Algorithm.squeeze,
                EngineConfig.RiskLocParams.defaults(),
                EngineConfig.AdtributorParams.defaults(),
                EngineConfig.SqueezeParams.defaults(),
                new EngineConfig.Guards(0, 2, 0),
                DerivedAllocation.Method.gre,
                EngineConfig.EpBasis.auto,
                5);
    }

    @Test
    public void testSingleLayer1RootCause() {
        // region=a triples (100 -> 300) on both leaves; everything else is flat
        final LeafTable.Builder builder = LeafTable.builder(List.of("region", "category"), List.of("m"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String category : List.of("x", "y")) {
                builder.addBaseline(new String[]{region, category}, new double[]{100});
                builder.addTarget(new String[]{region, category}, new double[]{"a".equals(region) ? 300 : 100});
            }
        }
        final LeafTable table = builder.build();
        final List<Finding> findings = new Squeeze()
                .localize(table, table.measureVector("m"), config());

        Assertions.assertEquals(1, findings.size());
        final Finding finding = findings.getFirst();
        Assertions.assertEquals(List.of(new Slice(new int[]{0}, new String[]{"a"})), finding.slices());
        // riskScore carries the generalized potential score (perfect explanation here)
        Assertions.assertEquals(1.0, finding.riskScore(), 1e-9);
        Assertions.assertNull(finding.surprise());
        Assertions.assertEquals(2, finding.leafCount());
        Assertions.assertEquals(200.0, finding.baselineSum(), 1e-9);
        Assertions.assertEquals(600.0, finding.targetSum(), 1e-9);
    }

    @Test
    public void testMultiElementRootCauseInOneFinding() {
        // Two same-magnitude culprits in the same dimension form one cluster and one finding
        // with two slices (Squeeze's element set), unlike RiskLoc's iterative separation.
        // The vocabulary must be broad enough that the auto score weight outweighs the
        // succinctness penalty (n_ele * layer) of the two-element selection.
        final LeafTable.Builder builder = LeafTable.builder(List.of("region", "category"), List.of("m"));
        for(final String region : List.of("a", "b", "c", "d", "e", "f")) {
            for(final String category : List.of("x", "y", "z")) {
                final boolean culprit = "a".equals(region) || "b".equals(region);
                builder.addBaseline(new String[]{region, category}, new double[]{100});
                builder.addTarget(new String[]{region, category}, new double[]{culprit ? 300 : 100});
            }
        }
        final LeafTable table = builder.build();
        final List<Finding> findings = new Squeeze()
                .localize(table, table.measureVector("m"), config());

        Assertions.assertEquals(1, findings.size());
        Assertions.assertEquals(2, findings.getFirst().slices().size());
        Assertions.assertTrue(findings.getFirst().slices().contains(new Slice(new int[]{0}, new String[]{"a"})));
        Assertions.assertTrue(findings.getFirst().slices().contains(new Slice(new int[]{0}, new String[]{"b"})));
    }

    @Test
    public void testNoChangeYieldsNoFindings() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d"), List.of("m"));
        builder.addBaseline(new String[]{"a"}, new double[]{100});
        builder.addTarget(new String[]{"a"}, new double[]{100});
        builder.addBaseline(new String[]{"b"}, new double[]{100});
        builder.addTarget(new String[]{"b"}, new double[]{100});
        final LeafTable table = builder.build();

        Assertions.assertTrue(new Squeeze()
                .localize(table, table.measureVector("m"), config()).isEmpty());
    }

    @Test
    public void testDeterminism() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("region", "category"), List.of("m"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String category : List.of("x", "y", "z")) {
                builder.addBaseline(new String[]{region, category}, new double[]{100});
                builder.addTarget(new String[]{region, category},
                        new double[]{"a".equals(region) ? 320 : ("x".equals(category) ? 104 : 97)});
            }
        }
        final LeafTable table = builder.build();
        final List<Finding> first = new Squeeze().localize(table, table.measureVector("m"), config());
        final List<Finding> second = new Squeeze().localize(table, table.measureVector("m"), config());
        Assertions.assertEquals(first, second);
        Assertions.assertFalse(first.isEmpty());
    }

    @Test
    public void testKneedleFindsKneeOfConcaveCurve() {
        // y = sqrt(x): concave increasing, knee in the lower-x region
        final double[] x = new double[100];
        final double[] y = new double[100];
        for(int i = 0; i < 100; i++) {
            x[i] = i;
            y[i] = Math.sqrt(i);
        }
        final Double knee = Squeeze.kneedle(x, y);
        Assertions.assertNotNull(knee);
        Assertions.assertTrue(knee > 0 && knee < 50, "knee=" + knee);
    }

    @Test
    public void testHistogramAutoEdgesMatchNumpySemantics() {
        // 0..99: IQR = 49.5, FD width = 2*49.5/100^(1/3) ≈ 21.33, sturges = 99/(log2(100)+1) ≈ 12.9
        // -> width = 12.9 -> bins = ceil(99/12.9) = 8
        final double[] values = new double[100];
        for(int i = 0; i < 100; i++) {
            values[i] = i;
        }
        final double[] edges = Squeeze.histogramAutoEdges(values);
        Assertions.assertEquals(9, edges.length);
        Assertions.assertEquals(0.0, edges[0], 1e-9);
        Assertions.assertEquals(99.0, edges[edges.length - 1], 1e-9);
    }
}
