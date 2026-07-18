package com.mercari.solution.util.domain.attribution;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Brute-force baseline: ranks every slice of every cuboid up to {@code guards.maxLayer} by
 * explanatory power (surprise as tie-break). Exponential in dimension count — intended for
 * small data, sanity checks, and as a ground-truth oracle in tests.
 */
public class Exhaustive implements AttributionAlgorithm {

    @Override
    public List<Finding> localize(final LeafTable table, final MeasureVector measure, final EngineConfig config) {

        final double[] ep = measure.explanatoryPowers();
        final double[] f = measure.baseline();
        final double[] v = measure.target();
        final double baselineTotal = measure.baselineTotal();
        final double targetTotal = measure.targetTotal();

        final List<Finding> findings = new ArrayList<>();
        for(final int[] cuboid : Cuboids.enumerate(table.dimensionCount(), config.guards().maxLayer())) {
            final Map<List<String>, double[]> elements = new TreeMap<>(Exhaustive::compareValues);
            for(int leaf = 0; leaf < table.leafCount(); leaf++) {
                final List<String> key = new ArrayList<>(cuboid.length);
                for(final int dim : cuboid) {
                    key.add(table.dimValue(leaf, dim));
                }
                final double[] acc = elements.computeIfAbsent(key, k -> new double[4]);
                acc[0] += ep[leaf];
                acc[1] += f[leaf];
                acc[2] += v[leaf];
                acc[3] += 1;
            }
            for(final Map.Entry<List<String>, double[]> entry : elements.entrySet()) {
                final double epSum = entry.getValue()[0];
                if(epSum <= 0) {
                    continue;
                }
                final Slice slice = new Slice(cuboid, entry.getKey().toArray(new String[0]));
                final double surprise = Adtributor.jsDivergence(
                        entry.getValue()[1], entry.getValue()[2], baselineTotal, targetTotal);
                findings.add(new Finding(List.of(slice), null, epSum, surprise,
                        entry.getValue()[1], entry.getValue()[2], (int) entry.getValue()[3]));
            }
        }

        return findings.stream()
                .sorted(Comparator
                        .comparingDouble((Finding finding) -> finding.explanatoryPower()).reversed()
                        .thenComparing((Finding finding) -> finding.slices().getFirst()))
                .limit(config.topK())
                .toList();
    }

    private static int compareValues(final List<String> a, final List<String> b) {
        for(int i = 0; i < Math.min(a.size(), b.size()); i++) {
            final int c = a.get(i).compareTo(b.get(i));
            if(c != 0) {
                return c;
            }
        }
        return Integer.compare(a.size(), b.size());
    }
}
