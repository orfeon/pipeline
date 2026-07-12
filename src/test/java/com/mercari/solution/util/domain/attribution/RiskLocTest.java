package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RiskLocTest {

    private static EngineConfig config(final double riskThreshold, final double pepThreshold, final int pruningLayers) {
        return new EngineConfig(
                EngineConfig.Algorithm.riskloc,
                new EngineConfig.RiskLocParams(riskThreshold, pepThreshold, pruningLayers),
                EngineConfig.AdtributorParams.defaults(),
                EngineConfig.Guards.defaults(),
                DerivedAllocation.Method.gre,
                5);
    }

    /** 3x3 grid, baseline 100 per leaf; anomalies injected as target deltas on selected slices. */
    private static LeafTable grid(final double[][] targetDeltas) {
        final List<String> regions = List.of("a", "b", "c");
        final List<String> categories = List.of("x", "y", "z");
        final LeafTable.Builder builder = LeafTable.builder(List.of("region", "category"), List.of("m"));
        for(int r = 0; r < 3; r++) {
            for(int c = 0; c < 3; c++) {
                builder.addBaseline(new String[]{regions.get(r), categories.get(c)}, new double[]{100});
                builder.addTarget(new String[]{regions.get(r), categories.get(c)}, new double[]{100 + targetDeltas[r][c]});
            }
        }
        return builder.build();
    }

    @Test
    public void testSingleLayer1RootCause() {
        // +50 on every region=a leaf
        final LeafTable table = grid(new double[][]{{50, 50, 50}, {0, 0, 0}, {0, 0, 0}});
        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));

        Assertions.assertEquals(1, findings.size());
        final Finding finding = findings.getFirst();
        final Slice slice = finding.slices().getFirst();
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"a"}), slice);
        // r1 = 3*0.4 / (3*0.4 + 1) ≈ 0.545, r2 = 0
        Assertions.assertEquals(1.2 / 2.2, finding.riskScore(), 1e-9);
        Assertions.assertEquals(1.0, finding.explanatoryPower(), 1e-9);
        Assertions.assertEquals(300.0, finding.baselineSum(), 1e-9);
        Assertions.assertEquals(450.0, finding.targetSum(), 1e-9);
        Assertions.assertEquals(3, finding.leafCount());
    }

    @Test
    public void testLayer2RootCause() {
        // +300 on the single leaf (region=a, category=x): neither layer-1 ancestor passes the
        // risk threshold (their normal leaves push r2 up), so it must be found at layer 2
        final LeafTable table = grid(new double[][]{{300, 0, 0}, {0, 0, 0}, {0, 0, 0}});
        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));

        Assertions.assertEquals(1, findings.size());
        final Slice slice = findings.getFirst().slices().getFirst();
        Assertions.assertEquals(new Slice(new int[]{0, 1}, new String[]{"a", "x"}), slice);
        Assertions.assertEquals(2, findings.getFirst().layer());
    }

    @Test
    public void testTwoRootCausesFoundIteratively() {
        // Culprit 1: region=a (+50 each). Culprit 2: the single leaf (b, x) with +800.
        final LeafTable table = grid(new double[][]{{50, 50, 50}, {800, 0, 0}, {0, 0, 0}});
        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));

        Assertions.assertEquals(2, findings.size());
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"a"}),
                findings.get(0).slices().getFirst());
        // After removing region=a, the remaining anomaly is localized exactly at layer 2
        Assertions.assertEquals(new Slice(new int[]{0, 1}, new String[]{"b", "x"}),
                findings.get(1).slices().getFirst());
    }

    @Test
    public void testDecreaseAnomaly() {
        // -60 on every region=b leaf (KPI drop direction); the other leaves carry small noise
        // so the deviation distribution has a proper tail for the cutoff to trim (with all-equal
        // noise-free deviations the reference cutoff logic degenerates by design)
        final LeafTable table = grid(new double[][]{{1, -1, 2}, {-60, -60, -60}, {-2, 3, -3}});
        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));

        Assertions.assertEquals(1, findings.size());
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"b"}),
                findings.getFirst().slices().getFirst());
    }

    @Test
    public void testNoFindingWhenNoChange() {
        final LeafTable table = grid(new double[][]{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}});
        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));
        Assertions.assertTrue(findings.isEmpty());
    }

    @Test
    public void testZeroLeavesAndZeroValuesAreSafe() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d"), List.of("m"));
        builder.addBaseline(new String[]{"a"}, new double[]{0});
        builder.addTarget(new String[]{"a"}, new double[]{0});
        builder.addBaseline(new String[]{"b"}, new double[]{100});
        builder.addTarget(new String[]{"b"}, new double[]{100});
        final LeafTable table = builder.build();

        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));
        Assertions.assertTrue(findings.isEmpty());
    }

    @Test
    public void testNewSegmentWithZeroBaseline() {
        // (a, x) appears only in the target
        final LeafTable.Builder builder = LeafTable.builder(List.of("region", "category"), List.of("m"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String category : List.of("x", "y", "z")) {
                final boolean anomaly = "a".equals(region) && "x".equals(category);
                builder.addBaseline(new String[]{region, category}, new double[]{anomaly ? 0 : 100});
                builder.addTarget(new String[]{region, category}, new double[]{anomaly ? 300 : 100});
            }
        }
        final LeafTable table = builder.build();
        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));

        Assertions.assertFalse(findings.isEmpty());
        Assertions.assertTrue(findings.getFirst().slices().getFirst().contains(new String[]{"a", "x"}));
    }

    @Test
    public void testPruningDoesNotChangeResults() {
        final LeafTable table = grid(new double[][]{{50, 50, 50}, {800, 0, 0}, {0, 0, 0}});
        final List<Finding> pruned = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));
        final List<Finding> unpruned = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 0));
        Assertions.assertEquals(unpruned, pruned);
    }

    @Test
    public void testDeterminism() {
        final LeafTable table = grid(new double[][]{{50, 50, 50}, {800, 0, 0}, {0, 0, 0}});
        final List<Finding> first = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));
        final List<Finding> second = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));
        Assertions.assertEquals(first, second);
    }

    @Test
    public void testHighRiskThresholdSuppressesFindings() {
        final LeafTable table = grid(new double[][]{{50, 50, 50}, {0, 0, 0}, {0, 0, 0}});
        // risk of region=a is ≈ 0.545 < 0.9
        final List<Finding> findings = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.9, 0.02, 1));
        Assertions.assertTrue(findings.isEmpty());
    }

    @Test
    public void testDegenerateGuardRecoversNoiseFreeDecrease() {
        // Noise-free symmetric decrease: the reference cutoff logic classifies nothing as
        // anomalous (the trimmed extremes collapse to zero pointing away from the anomaly).
        // The production guard falls back to a zero cutoff toward the dominant deviation side.
        final LeafTable table = grid(new double[][]{{0, 0, 0}, {-60, -60, -60}, {0, 0, 0}});

        final List<Finding> guarded = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));
        Assertions.assertEquals(1, guarded.size());
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"b"}),
                guarded.getFirst().slices().getFirst());

        // Reference-parity mode (guard disabled): the degenerate behavior is preserved
        final EngineConfig parityConfig = new EngineConfig(
                EngineConfig.Algorithm.riskloc,
                new EngineConfig.RiskLocParams(0.5, 0.02, 1, false),
                EngineConfig.AdtributorParams.defaults(),
                EngineConfig.Guards.defaults(),
                DerivedAllocation.Method.gre,
                5);
        final List<Finding> parity = new RiskLoc()
                .localize(table, table.measureVector("m"), parityConfig);
        Assertions.assertTrue(parity.isEmpty());
    }

    @Test
    public void testSingleLeafRootCauseRiskBoundary() {
        // A single-leaf root cause caps r1 at weight/(weight+1) ≤ 0.5, so its risk score cannot
        // exceed 0.5: it sits exactly on the default threshold (>= comparison, as the reference).
        // Deepest-layer single-element causes are therefore on the detection boundary by design.
        final LeafTable table = grid(new double[][]{{300, 0, 0}, {0, 0, 0}, {0, 0, 0}});

        final List<Finding> atThreshold = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.5, 0.02, 1));
        Assertions.assertEquals(1, atThreshold.size());
        Assertions.assertEquals(0.5, atThreshold.getFirst().riskScore(), 1e-12);

        final List<Finding> aboveThreshold = new RiskLoc()
                .localize(table, table.measureVector("m"), config(0.500001, 0.02, 1));
        Assertions.assertTrue(aboveThreshold.isEmpty());
    }

    @Test
    public void testCutoffTrimsExtremeUniqueDeviations() {
        // 12 unique deviations: 0.0 .. 1.1 -> trim 5 from each side:
        // minVal = 6th smallest = 0.5, maxVal = 6th largest = 0.6
        // chosen = min by abs = 0.5 -> cutoff = -0.5
        final double[] deviations = new double[12];
        for(int i = 0; i < 12; i++) {
            deviations[i] = i * 0.1;
        }
        Assertions.assertEquals(-0.5, RiskLoc.cutoff(deviations), 1e-9);
    }

    @Test
    public void testCutoffWithFewUniqueValues() {
        // Fewer than trim size: both extremes collapse to the overall extremes
        // minVal = max of all = 0.4, maxVal = min of all = -0.4, |equal| -> minVal chosen -> -0.4
        Assertions.assertEquals(-0.4, RiskLoc.cutoff(new double[]{-0.4, 0.4}), 1e-9);
        // Single unique value
        Assertions.assertEquals(-0.3, RiskLoc.cutoff(new double[]{0.3, 0.3}), 1e-9);
    }
}
