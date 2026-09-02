package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;

/**
 * Manual scaling benchmark for the attribution core (not run in CI).
 * Measures {@link AttributionEngine#run} wall time against leaf count on the single worker
 * that executes the localization step, to inform the scale guidance and a future
 * small/large execution profile threshold.
 *
 * <p>Run with: {@code mvn test -Dtest=AttributionEngineBenchmarkTest -Dattribution.benchmark=true}</p>
 */
public class AttributionEngineBenchmarkTest {

    @Test
    @EnabledIfSystemProperty(named = "attribution.benchmark", matches = "true")
    public void benchmarkRiskLocScaling() {
        // 3 dimensions; cardinality^3 leaves
        final int[] cardinalities = {10, 22, 47, 100};

        // Warm up JIT on the smallest size
        run(10, 0);

        System.out.println("cardinality | leaves | generate_ms | riskloc_ms | findings");
        for(final int cardinality : cardinalities) {
            final long generateStart = System.nanoTime();
            final LeafTable table = generate(cardinality);
            final long generateMs = (System.nanoTime() - generateStart) / 1_000_000;

            final long runStart = System.nanoTime();
            final AttributionResult result = AttributionEngine.run(
                    table,
                    SyntheticDataGenerator.dimensionNames(3).stream().map(DimensionSpec::flat).toList(),
                    List.of(MeasureSpec.fundamental("m")),
                    config(),
                    false);
            final long runMs = (System.nanoTime() - runStart) / 1_000_000;

            System.out.printf("%11d | %6d | %11d | %10d | %d%n",
                    cardinality, table.leafCount(), generateMs, runMs,
                    result.results().getFirst().findings().size());
        }

        // Squeeze scaling on the same tables: the KDE amplitude filter is O(leaves x 1000)
        // and dominates at scale
        System.out.println("cardinality | leaves | squeeze_ms | findings");
        for(final int cardinality : cardinalities) {
            final LeafTable table = generate(cardinality);
            final long runStart = System.nanoTime();
            final AttributionResult result = AttributionEngine.run(
                    table,
                    SyntheticDataGenerator.dimensionNames(3).stream().map(DimensionSpec::flat).toList(),
                    List.of(MeasureSpec.fundamental("m")),
                    new EngineConfig(
                            EngineConfig.Algorithm.squeeze,
                            EngineConfig.RiskLocParams.defaults(),
                            EngineConfig.AdtributorParams.defaults(),
                            EngineConfig.Guards.defaults(),
                            DerivedAllocation.Method.gre,
                            3),
                    false);
            final long runMs = (System.nanoTime() - runStart) / 1_000_000;
            System.out.printf("%11d | %6d | %10d | %d%n",
                    cardinality, table.leafCount(), runMs,
                    result.results().getFirst().findings().size());
        }

        // Cuboid-count effect: ~1M leaves at growing dimension counts (maxLayer 3:
        // 3 dims = 7 cuboids, 4 dims = 14, 5 dims = 25, 6 dims = 41)
        System.out.println("dims | cardinality | leaves | cuboids<=3 | riskloc_ms");
        final int[][] dimScenarios = {{3, 100, 7}, {4, 32, 14}, {5, 16, 25}, {6, 10, 41}};
        for(final int[] scenario : dimScenarios) {
            final int dims = scenario[0];
            final int cardinality = scenario[1];
            final Slice culprit = new Slice(new int[]{0}, new String[]{"v1"});
            final LeafTable table = SyntheticDataGenerator.generate(42, dims, cardinality, culprit, 2.5);
            final long runStart = System.nanoTime();
            AttributionEngine.run(
                    table,
                    SyntheticDataGenerator.dimensionNames(dims).stream().map(DimensionSpec::flat).toList(),
                    List.of(MeasureSpec.fundamental("m")),
                    config(),
                    false);
            final long runMs = (System.nanoTime() - runStart) / 1_000_000;
            System.out.printf("%4d | %11d | %6d | %10d | %d%n",
                    dims, cardinality, table.leafCount(), scenario[2], runMs);
        }
    }

    private static void run(final int cardinality, final int ignored) {
        AttributionEngine.run(
                generate(cardinality),
                SyntheticDataGenerator.dimensionNames(3).stream().map(DimensionSpec::flat).toList(),
                List.of(MeasureSpec.fundamental("m")),
                config(),
                false);
    }

    private static LeafTable generate(final int cardinality) {
        final Slice culprit = new Slice(new int[]{0}, new String[]{"v1"});
        return SyntheticDataGenerator.generate(42, 3, cardinality, culprit, 2.5);
    }

    private static EngineConfig config() {
        return new EngineConfig(
                EngineConfig.Algorithm.riskloc,
                EngineConfig.RiskLocParams.defaults(),
                EngineConfig.AdtributorParams.defaults(),
                EngineConfig.Guards.defaults(),
                DerivedAllocation.Method.gre,
                3);
    }
}
