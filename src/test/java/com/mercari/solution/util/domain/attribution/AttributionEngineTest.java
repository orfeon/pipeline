package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class AttributionEngineTest {

    private static EngineConfig config(final EngineConfig.Algorithm algorithm) {
        return new EngineConfig(
                algorithm,
                EngineConfig.RiskLocParams.defaults(),
                EngineConfig.AdtributorParams.defaults(),
                EngineConfig.Guards.defaults(),
                DerivedAllocation.Method.gre,
                3);
    }

    private static List<DimensionSpec> flatDimensions(final List<String> names) {
        return names.stream().map(DimensionSpec::flat).toList();
    }

    @Test
    public void testRiskLocRecoversInjectedCulpritsAcrossSeeds() {
        final int dimCount = 3;
        final int cardinality = 8;
        final List<DimensionSpec> dimensions = flatDimensions(SyntheticDataGenerator.dimensionNames(dimCount));
        final List<MeasureSpec> measures = List.of(MeasureSpec.fundamental("m"));

        int hits = 0;
        final int seeds = 20;
        for(long seed = 0; seed < seeds; seed++) {
            // Alternate between layer-1 and layer-2 culprits, position varied by seed
            final Slice culprit;
            final int value = (int) (seed % cardinality);
            if(seed % 2 == 0) {
                culprit = new Slice(new int[]{(int) (seed / 2) % dimCount}, new String[]{"v" + value});
            } else {
                culprit = new Slice(new int[]{0, 1 + (int) (seed / 2) % (dimCount - 1)},
                        new String[]{"v" + value, "v" + ((value + 3) % cardinality)});
            }
            final LeafTable table = SyntheticDataGenerator.generate(seed, dimCount, cardinality, culprit, 2.5);
            final AttributionResult result = AttributionEngine.run(
                    table, dimensions, measures, config(EngineConfig.Algorithm.riskloc), false);

            final List<Slice> predicted = new ArrayList<>();
            for(final Finding finding : result.results().getFirst().findings()) {
                predicted.addAll(finding.slices());
            }
            if(predicted.contains(culprit)) {
                hits++;
            }
        }
        Assertions.assertTrue(hits >= 16,
                "riskloc should recover the injected culprit in most seeded runs, but hit only "
                        + hits + "/" + seeds);
    }

    @Test
    public void testExhaustiveOracleAgreesWithRiskLocOnLayer1Culprit() {
        final int dimCount = 3;
        final int cardinality = 8;
        final List<DimensionSpec> dimensions = flatDimensions(SyntheticDataGenerator.dimensionNames(dimCount));
        final List<MeasureSpec> measures = List.of(MeasureSpec.fundamental("m"));

        for(long seed = 100; seed < 105; seed++) {
            final Slice culprit = new Slice(new int[]{(int) seed % dimCount},
                    new String[]{"v" + (int) (seed % cardinality)});
            final LeafTable table = SyntheticDataGenerator.generate(seed, dimCount, cardinality, culprit, 2.5);

            final AttributionResult exhaustive = AttributionEngine.run(
                    table, dimensions, measures, config(EngineConfig.Algorithm.exhaustive), false);
            final AttributionResult riskloc = AttributionEngine.run(
                    table, dimensions, measures, config(EngineConfig.Algorithm.riskloc), false);

            Assertions.assertEquals(culprit,
                    exhaustive.results().getFirst().findings().getFirst().slices().getFirst(),
                    "exhaustive oracle top finding, seed " + seed);
            Assertions.assertEquals(culprit,
                    riskloc.results().getFirst().findings().getFirst().slices().getFirst(),
                    "riskloc top finding, seed " + seed);
        }
    }

    @Test
    public void testDerivedMeasureSliceValuesRecomputed() {
        // 3x2 grid; cvr of every d=A leaf triples (10/100 -> 30/100)
        final LeafTable.Builder builder = LeafTable.builder(List.of("d", "g"), List.of("orders", "sessions"));
        for(final String d : List.of("A", "B", "C")) {
            for(final String g : List.of("p", "q")) {
                builder.addBaseline(new String[]{d, g}, new double[]{10, 100});
                builder.addTarget(new String[]{d, g}, new double[]{"A".equals(d) ? 30 : 10, 100});
            }
        }
        final MeasureSpec cvr = MeasureSpec.derived("cvr", "orders / sessions", List.of("orders", "sessions"));
        final AttributionResult result = AttributionEngine.run(
                builder.build(), flatDimensions(List.of("d", "g")), List.of(cvr),
                config(EngineConfig.Algorithm.riskloc), false);

        final MeasureResult measureResult = result.results().getFirst();
        Assertions.assertEquals("cvr", measureResult.measure());
        // Totals evaluated as h over global component sums, not pseudo-column sums
        Assertions.assertEquals(60.0 / 600.0, measureResult.baselineTotal(), 1e-9);
        Assertions.assertEquals(100.0 / 600.0, measureResult.targetTotal(), 1e-9);

        Assertions.assertFalse(measureResult.findings().isEmpty());
        final Finding finding = measureResult.findings().getFirst();
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"A"}), finding.slices().getFirst());
        // Slice values are the actual cvr of the slice: 20/200 -> 60/200
        Assertions.assertEquals(0.1, finding.baselineSum(), 1e-9);
        Assertions.assertEquals(0.3, finding.targetSum(), 1e-9);
    }

    @Test
    public void testBinnedDimension() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("price"), List.of("m"));
        for(int price = 1; price <= 8; price++) {
            builder.addBaseline(new String[]{Integer.toString(price)}, new double[]{100});
            builder.addTarget(new String[]{Integer.toString(price)}, new double[]{price <= 4 ? 300 : 100});
        }
        final AttributionResult result = AttributionEngine.run(
                builder.build(),
                List.of(DimensionSpec.binned("price", DimensionSpec.Binning.Method.quantile, 2)),
                List.of(MeasureSpec.fundamental("m")),
                config(EngineConfig.Algorithm.riskloc), false);

        final List<Finding> findings = result.results().getFirst().findings();
        Assertions.assertFalse(findings.isEmpty());
        Assertions.assertEquals("[1,5)", findings.getFirst().slices().getFirst().values()[0]);
    }

    @Test
    public void testSyntheticMarginalFindsInteractionCells() {
        // Perfect anti-diagonal interaction: marginals are uniform, so the marginal baseline is 50
        // everywhere and the total delta is 0 by construction
        final LeafTable.Builder builder = LeafTable.builder(List.of("d1", "d2"), List.of("m"));
        builder.addTarget(new String[]{"a", "x"}, new double[]{90});
        builder.addTarget(new String[]{"a", "y"}, new double[]{10});
        builder.addTarget(new String[]{"b", "x"}, new double[]{10});
        builder.addTarget(new String[]{"b", "y"}, new double[]{90});

        final AttributionResult result = AttributionEngine.run(
                builder.build(),
                flatDimensions(List.of("d1", "d2")),
                List.of(MeasureSpec.fundamental("m")),
                config(EngineConfig.Algorithm.riskloc), true);

        // The marginal baseline preserves totals, so auto must resolve to absoluteDelta
        Assertions.assertEquals(EngineConfig.EpBasis.absoluteDelta, result.results().getFirst().epBasis());
        final List<Finding> findings = result.results().getFirst().findings();
        Assertions.assertEquals(2, findings.size());
        final List<Slice> slices = findings.stream().map(finding -> finding.slices().getFirst()).toList();
        // The two under-performing interaction cells are localized at layer 2
        Assertions.assertTrue(slices.contains(new Slice(new int[]{0, 1}, new String[]{"a", "y"})));
        Assertions.assertTrue(slices.contains(new Slice(new int[]{0, 1}, new String[]{"b", "x"})));
    }

    @Test
    public void testAutoEpBasisFallsBackToAbsoluteDeltaOnMixShift() {
        // Total-constant mix shift: category a gains what category b loses (net delta = 0,
        // churn = 480) — the classic case where netDelta explanatory power is undefined
        final LeafTable.Builder builder = LeafTable.builder(List.of("cat", "g"), List.of("m"));
        for(final String g : List.of("p", "q")) {
            builder.addBaseline(new String[]{"a", g}, new double[]{100});
            builder.addTarget(new String[]{"a", g}, new double[]{160});
            builder.addBaseline(new String[]{"b", g}, new double[]{100});
            builder.addTarget(new String[]{"b", g}, new double[]{40});
        }
        final LeafTable table = builder.build();
        final List<DimensionSpec> dimensions = flatDimensions(List.of("cat", "g"));
        final List<MeasureSpec> measures = List.of(MeasureSpec.fundamental("m"));

        final AttributionResult auto = AttributionEngine.run(
                table, dimensions, measures, config(EngineConfig.Algorithm.riskloc), false);
        Assertions.assertEquals(EngineConfig.EpBasis.absoluteDelta, auto.results().getFirst().epBasis());
        Assertions.assertFalse(auto.results().getFirst().findings().isEmpty());
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"b"}),
                auto.results().getFirst().findings().getFirst().slices().getFirst());

        // Explicit netDelta on the same data: the zero net delta yields all-zero explanatory
        // power and hence no findings — recorded as netDelta so consumers see which basis applied
        final EngineConfig netDeltaConfig = new EngineConfig(
                EngineConfig.Algorithm.riskloc,
                EngineConfig.RiskLocParams.defaults(),
                EngineConfig.AdtributorParams.defaults(),
                EngineConfig.Guards.defaults(),
                DerivedAllocation.Method.gre,
                EngineConfig.EpBasis.netDelta,
                3);
        final AttributionResult netDelta = AttributionEngine.run(
                table, dimensions, measures, netDeltaConfig, false);
        Assertions.assertEquals(EngineConfig.EpBasis.netDelta, netDelta.results().getFirst().epBasis());
        Assertions.assertTrue(netDelta.results().getFirst().findings().isEmpty());
    }

    @Test
    public void testEpBasisStaysNetDeltaOnDirectionalChange() {
        final Slice culprit = new Slice(new int[]{0}, new String[]{"v0"});
        final LeafTable table = SyntheticDataGenerator.generate(42, 3, 8, culprit, 2.5);
        final AttributionResult result = AttributionEngine.run(
                table, flatDimensions(SyntheticDataGenerator.dimensionNames(3)),
                List.of(MeasureSpec.fundamental("m")),
                config(EngineConfig.Algorithm.riskloc), false);
        Assertions.assertEquals(EngineConfig.EpBasis.netDelta, result.results().getFirst().epBasis());
    }

    @Test
    public void testNoChangeYieldsEmptyFindings() {
        final LeafTable.Builder builder = LeafTable.builder(List.of("d"), List.of("m"));
        builder.addBaseline(new String[]{"a"}, new double[]{100});
        builder.addTarget(new String[]{"a"}, new double[]{100});

        for(final EngineConfig.Algorithm algorithm : EngineConfig.Algorithm.values()) {
            final AttributionResult result = AttributionEngine.run(
                    builder.build(), List.of(DimensionSpec.flat("d")),
                    List.of(MeasureSpec.fundamental("m")), config(algorithm), false);
            Assertions.assertTrue(result.results().getFirst().findings().isEmpty(),
                    "algorithm " + algorithm);
        }
    }
}
