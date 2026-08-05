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
        // The two reported under-performing cells each explain 0.25 of the churn; the two
        // over-performing counterparts are not reported, so half the churn stays unexplained
        Assertions.assertEquals(0.5, result.results().getFirst().unexplainedShare(), 1e-9);
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
    public void testDistributionMeasureLocalizesQuantileShift() {
        // Event-level latency: every leaf gets samples 10..100; region=a target leaves have their
        // top value replaced by 500 (tail shift only — the median is untouched). b/c carry ±1
        // noise so the deviation distribution is not degenerate.
        final LeafTable.Builder builder = LeafTable
                .builder(List.of("region", "cat"), List.of(), List.of("latency"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String cat : List.of("x", "y")) {
                final String[] dims = new String[]{region, cat};
                builder.addBaseline(dims, new double[0]);
                builder.addTarget(dims, new double[0]);
                for(int i = 1; i <= 10; i++) {
                    builder.addBaselineSample(dims, 0, i * 10);
                    final double sample = switch (region) {
                        case "a" -> i == 10 ? 500 : i * 10;
                        case "b" -> i == 10 ? 101 : i * 10;
                        default -> i == 10 ? 99 : i * 10;
                    };
                    builder.addTargetSample(dims, 0, sample);
                }
            }
        }
        final MeasureSpec latency = MeasureSpec.distribution("latency", List.of(0.5, 0.99));
        final AttributionResult result = AttributionEngine.run(
                builder.build(), flatDimensions(List.of("region", "cat")), List.of(latency),
                config(EngineConfig.Algorithm.riskloc), false);

        // One MeasureResult per quantile
        Assertions.assertEquals(2, result.results().size());

        final MeasureResult median = result.results().getFirst();
        Assertions.assertEquals("latency", median.measure());
        Assertions.assertEquals(0.5, median.quantile());
        Assertions.assertEquals(EngineConfig.EpBasis.absoluteDelta, median.epBasis());
        Assertions.assertEquals(50.0, median.baselineTotal(), 1e-9);
        Assertions.assertTrue(median.findings().isEmpty(), "median did not shift");

        final MeasureResult p99 = result.results().get(1);
        Assertions.assertEquals(0.99, p99.quantile());
        Assertions.assertEquals(EngineConfig.EpBasis.absoluteDelta, p99.epBasis());
        Assertions.assertEquals(100.0, p99.baselineTotal(), 1e-9);
        Assertions.assertEquals(500.0, p99.targetTotal(), 1e-9);
        Assertions.assertFalse(p99.findings().isEmpty());
        final Finding finding = p99.findings().getFirst();
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"a"}), finding.slices().getFirst());
        // Finding values are quantiles of the merged slice sketches, not sums
        Assertions.assertEquals(100.0, finding.baselineSum(), 1e-9);
        Assertions.assertEquals(500.0, finding.targetSum(), 1e-9);
    }

    @Test
    public void testDistinctMeasureLocalizesCardinalityDrop() {
        // Disjoint identity spaces per leaf: every leaf has 40 baseline users; region=a target
        // leaves drop to 12, b gains one user (asymmetric noise) and c stays flat
        final LeafTable.Builder builder = LeafTable
                .builder(List.of("region", "cat"), List.of(), List.of(), List.of("user"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String cat : List.of("x", "y")) {
                final String[] dims = new String[]{region, cat};
                final String leafPrefix = region + "_" + cat + "_u";
                for(int i = 1; i <= 40; i++) {
                    builder.addBaselineIdentity(dims, 0, leafPrefix + i);
                }
                final int targetUsers = switch (region) {
                    case "a" -> 12;
                    case "b" -> 41;
                    default -> 40;
                };
                for(int i = 1; i <= targetUsers; i++) {
                    builder.addTargetIdentity(dims, 0, leafPrefix + i);
                }
            }
        }
        final AttributionResult result = AttributionEngine.run(
                builder.build(), flatDimensions(List.of("region", "cat")),
                List.of(MeasureSpec.distinct("user")),
                config(EngineConfig.Algorithm.riskloc), false);

        final MeasureResult measureResult = result.results().getFirst();
        Assertions.assertEquals("user", measureResult.measure());
        Assertions.assertNull(measureResult.quantile());
        Assertions.assertEquals(EngineConfig.EpBasis.absoluteDelta, measureResult.epBasis());
        // Identity spaces are disjoint here, so union estimates equal sums (exact mode)
        Assertions.assertEquals(240.0, measureResult.baselineTotal(), 1e-9);
        Assertions.assertEquals(186.0, measureResult.targetTotal(), 1e-9);

        Assertions.assertFalse(measureResult.findings().isEmpty());
        final Finding finding = measureResult.findings().getFirst();
        Assertions.assertEquals(new Slice(new int[]{0}, new String[]{"a"}), finding.slices().getFirst());
        Assertions.assertEquals(80.0, finding.baselineSum(), 1e-9);
        Assertions.assertEquals(24.0, finding.targetSum(), 1e-9);
        // region=a explains 56 of the 58 units of absolute estimate shift (b noise = 2)
        Assertions.assertEquals(2.0 / 58.0, measureResult.unexplainedShare(), 1e-9);
    }

    @Test
    public void testDistinctMeasureTotalsAreUnionsNotSums() {
        // Both leaves contain the same 10 users: the total distinct count is 10, not 20,
        // and the target drop to a shared 8-user subset yields a union of 8
        final LeafTable.Builder builder = LeafTable
                .builder(List.of("d"), List.of(), List.of(), List.of("user"));
        for(final String d : List.of("a", "b")) {
            for(int i = 1; i <= 10; i++) {
                builder.addBaselineIdentity(new String[]{d}, 0, "u" + i);
            }
            for(int i = 1; i <= 8; i++) {
                builder.addTargetIdentity(new String[]{d}, 0, "u" + i);
            }
        }
        final AttributionResult result = AttributionEngine.run(
                builder.build(), flatDimensions(List.of("d")),
                List.of(MeasureSpec.distinct("user")),
                config(EngineConfig.Algorithm.riskloc), false);

        final MeasureResult measureResult = result.results().getFirst();
        Assertions.assertEquals(10.0, measureResult.baselineTotal(), 1e-9);
        Assertions.assertEquals(8.0, measureResult.targetTotal(), 1e-9);
    }

    @Test
    public void testDistinctMeasureRejectsSyntheticMarginal() {
        final LeafTable.Builder builder = LeafTable
                .builder(List.of("d"), List.of(), List.of(), List.of("user"));
        builder.addTargetIdentity(new String[]{"a"}, 0, "u1");
        builder.addTargetIdentity(new String[]{"b"}, 0, "u2");
        final LeafTable table = builder.build();

        Assertions.assertThrows(IllegalArgumentException.class, () -> AttributionEngine.run(
                table, flatDimensions(List.of("d")),
                List.of(MeasureSpec.distinct("user")),
                config(EngineConfig.Algorithm.riskloc), true));
    }

    @Test
    public void testDistributionMeasureRejectsSyntheticMarginal() {
        final LeafTable.Builder builder = LeafTable
                .builder(List.of("d"), List.of(), List.of("latency"));
        builder.addTargetSample(new String[]{"a"}, 0, 10);
        builder.addTargetSample(new String[]{"b"}, 0, 20);
        final LeafTable table = builder.build();

        Assertions.assertThrows(IllegalArgumentException.class, () -> AttributionEngine.run(
                table, flatDimensions(List.of("d")),
                List.of(MeasureSpec.distribution("latency", List.of(0.5))),
                config(EngineConfig.Algorithm.riskloc), true));
    }

    @Test
    public void testExternalRootCauseCandidate() {
        // Uniform +10% on every leaf: the change is real but has no localizable slice
        // (all deviations equal -> nothing passes the risk gate) -> external candidate
        final LeafTable.Builder uniform = LeafTable.builder(List.of("region", "cat"), List.of("m"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String cat : List.of("x", "y")) {
                uniform.addBaseline(new String[]{region, cat}, new double[]{100});
                uniform.addTarget(new String[]{region, cat}, new double[]{110});
            }
        }
        final EngineConfig config = config(EngineConfig.Algorithm.riskloc);
        final AttributionResult uniformResult = AttributionEngine.run(
                uniform.build(), flatDimensions(List.of("region", "cat")),
                List.of(MeasureSpec.fundamental("m")), config, false);
        Assertions.assertTrue(uniformResult.results().getFirst().findings().isEmpty());
        Assertions.assertTrue(AttributionEngine.externalRootCauseCandidate(
                uniformResult.results().getFirst(), config));

        // A cleanly localized culprit is not external
        final LeafTable.Builder localized = LeafTable.builder(List.of("region", "cat"), List.of("m"));
        for(final String region : List.of("a", "b", "c")) {
            for(final String cat : List.of("x", "y")) {
                localized.addBaseline(new String[]{region, cat}, new double[]{100});
                localized.addTarget(new String[]{region, cat}, new double[]{"a".equals(region) ? 300 : 100});
            }
        }
        final AttributionResult localizedResult = AttributionEngine.run(
                localized.build(), flatDimensions(List.of("region", "cat")),
                List.of(MeasureSpec.fundamental("m")), config, false);
        Assertions.assertFalse(localizedResult.results().getFirst().findings().isEmpty());
        Assertions.assertFalse(AttributionEngine.externalRootCauseCandidate(
                localizedResult.results().getFirst(), config));

        // An unchanged measure has no findings but is not external either (nothing moved)
        final LeafTable.Builder flat = LeafTable.builder(List.of("d"), List.of("m"));
        flat.addBaseline(new String[]{"a"}, new double[]{100});
        flat.addTarget(new String[]{"a"}, new double[]{100});
        final AttributionResult flatResult = AttributionEngine.run(
                flat.build(), flatDimensions(List.of("d")),
                List.of(MeasureSpec.fundamental("m")), config, false);
        Assertions.assertFalse(AttributionEngine.externalRootCauseCandidate(
                flatResult.results().getFirst(), config));
    }

    @Test
    public void testExternalRootCauseCandidateJudgesSqueezeOnScores() {
        final EngineConfig config = config(EngineConfig.Algorithm.squeeze);
        final Finding weak = new Finding(
                List.of(new Slice(new int[]{0}, new String[]{"a"})), 0.5, 0.9, null, 100, 200, 1);
        final Finding strong = new Finding(
                List.of(new Slice(new int[]{0}, new String[]{"a"})), 0.95, 0.9, null, 100, 200, 1);

        // The reference judges squeeze on its minimum potential score alone (fallback 0.8)
        Assertions.assertTrue(AttributionEngine.externalRootCauseCandidate(
                new MeasureResult("m", 100, 200, EngineConfig.EpBasis.netDelta, List.of(weak)), config));
        Assertions.assertFalse(AttributionEngine.externalRootCauseCandidate(
                new MeasureResult("m", 100, 200, EngineConfig.EpBasis.netDelta, List.of(strong)), config));
    }

    @Test
    public void testExternalRootCauseCandidateSkipsTruncatedReports() {
        // topK-truncated reports inflate the unexplained share by design: not judged external
        final EngineConfig config = config(EngineConfig.Algorithm.riskloc); // topK = 3
        final Finding partial = new Finding(
                List.of(new Slice(new int[]{0}, new String[]{"a"})), 0.6, 0.2, null, 100, 200, 1);
        Assertions.assertFalse(AttributionEngine.externalRootCauseCandidate(
                new MeasureResult("m", 100, 200, EngineConfig.EpBasis.netDelta,
                        List.of(partial, partial, partial)), config));
        // The same weak explanation without truncation is external
        Assertions.assertTrue(AttributionEngine.externalRootCauseCandidate(
                new MeasureResult("m", 100, 200, EngineConfig.EpBasis.netDelta,
                        List.of(partial)), config));
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
            // No findings -> unexplainedShare is 1 by definition; consumers must read it
            // together with the total delta (here 0: nothing to explain)
            Assertions.assertEquals(1.0, result.results().getFirst().unexplainedShare(), 1e-9);
        }
    }
}
