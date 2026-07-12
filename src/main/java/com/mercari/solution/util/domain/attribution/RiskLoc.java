package com.mercari.solution.util.domain.attribution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * RiskLoc: Kalander, "RiskLoc: Localization of Multi-dimensional Root Causes by Weighted Risk",
 * arXiv:2205.10004. Faithful port of the reference implementation
 * github.com/shaido987/riskloc (algorithms/riskloc.py):
 * deviation score → outlier-trimmed 2-way partitioning → weighting → per-element risk
 * (weighted anomaly coverage r1 minus ripple-effect-normalized penalty r2) → layer-order element
 * search picking the highest-EP element with risk ≥ riskThreshold → iterative removal until the
 * remaining anomalous explanatory power falls below pepThreshold → optional element pruning
 * (a pure speed optimization that never changes results).
 */
public class RiskLoc implements AttributionAlgorithm {

    private static final Logger LOG = LoggerFactory.getLogger(RiskLoc.class);

    /** Number of extreme unique deviation values trimmed on each side when locating the cutoff. */
    private static final int N_REMOVE = 5;

    @Override
    public List<Finding> localize(final LeafTable table, final MeasureVector measure, final EngineConfig config) {

        final int leafCount = table.leafCount();
        if(leafCount == 0) {
            return List.of();
        }

        final double[] f = measure.baseline();
        final double[] v = measure.target();
        final double[] deviations = measure.deviations();
        final double[] ep = measure.explanatoryPowers().clone();

        double cutoff = cutoff(deviations);

        final boolean[] partition = new boolean[leafCount];
        final double[] weight = new double[leafCount];
        computePartitionAndWeights(cutoff, f, v, deviations, partition, weight);

        // Degenerate-cutoff guard (production default; disable for reference-parity testing).
        // With few unique deviations the trimmed cutoff can point away from all the changed
        // leaves, silently classifying nothing as anomalous. Fall back to a forced zero cutoff
        // toward the deviation side carrying more mass — the reference formulas otherwise apply.
        if(config.riskloc().degenerateGuard() && isAllFalse(partition)) {
            double negativeMass = 0;
            double positiveMass = 0;
            for(final double deviation : deviations) {
                if(deviation < 0) {
                    negativeMass -= deviation;
                } else {
                    positiveMass += deviation;
                }
            }
            if(negativeMass + positiveMass > 0) {
                cutoff = positiveMass > negativeMass ? 0.0 : -0.0;
                LOG.warn("riskloc cutoff degenerated (no leaf classified anomalous despite nonzero deviations);"
                        + " falling back to a zero cutoff toward the {} deviation side",
                        positiveMass > negativeMass ? "positive" : "negative");
                computePartitionAndWeights(cutoff, f, v, deviations, partition, weight);
            }
        }

        // Negate all EP values if the sum over the abnormal partition is negative
        double anomalyEpSum = 0;
        for(int i = 0; i < leafCount; i++) {
            if(partition[i]) {
                anomalyEpSum += ep[i];
            }
        }
        if(anomalyEpSum < 0) {
            for(int i = 0; i < leafCount; i++) {
                ep[i] = -ep[i];
            }
            anomalyEpSum = -anomalyEpSum;
        }
        final double adjEpThreshold = anomalyEpSum * config.riskloc().pepThreshold();

        final double[] epZ = new double[leafCount];
        for(int i = 0; i < leafCount; i++) {
            epZ[i] = Math.max(ep[i], 0);
        }

        final int pruningLayers = config.riskloc().pruningLayers();
        final Map<List<Integer>, Set<List<String>>> pruned = pruningLayers > 0 ? new HashMap<>() : null;
        final int maxLayer = Math.min(config.guards().maxLayer(), table.dimensionCount());

        final boolean[] alive = new boolean[leafCount];
        Arrays.fill(alive, true);

        final List<Finding> findings = new ArrayList<>();
        while(findings.size() < config.topK()) {
            double remainingEpSum = 0;
            for(int i = 0; i < leafCount; i++) {
                if(alive[i] && partition[i]) {
                    remainingEpSum += ep[i];
                }
            }
            if(remainingEpSum < adjEpThreshold) {
                break;
            }
            final Finding finding = searchAnomaly(
                    table, f, v, deviations, ep, epZ, partition, weight, alive,
                    pruned, pruningLayers, maxLayer, adjEpThreshold, config.riskloc().riskThreshold());
            if(finding == null) {
                break;
            }
            findings.add(finding);
            final Slice slice = finding.slices().getFirst();
            for(int i = 0; i < leafCount; i++) {
                if(alive[i] && slice.contains(table.dims(i))) {
                    alive[i] = false;
                }
            }
        }
        return findings;
    }

    private Finding searchAnomaly(
            final LeafTable table,
            final double[] f,
            final double[] v,
            final double[] deviations,
            final double[] ep,
            final double[] epZ,
            final boolean[] partition,
            final double[] weight,
            final boolean[] alive,
            final Map<List<Integer>, Set<List<String>>> pruned,
            final int pruningLayers,
            final int maxLayer,
            final double adjEpThreshold,
            final double riskThreshold) {

        for(int layer = 1; layer <= maxLayer; layer++) {
            Finding best = null;
            double epBar = adjEpThreshold;
            for(final int[] cuboid : Cuboids.layer(table.dimensionCount(), layer)) {

                // Group the alive, non-pruned leaves by their cuboid element
                final Map<List<String>, Aggregate> elements = new TreeMap<>(RiskLoc::compareValues);
                for(int i = 0; i < table.leafCount(); i++) {
                    if(!alive[i] || isPruned(pruned, cuboid, table.dims(i))) {
                        continue;
                    }
                    final List<String> key = new ArrayList<>(cuboid.length);
                    for(final int dim : cuboid) {
                        key.add(table.dimValue(i, dim));
                    }
                    final Aggregate aggregate = elements.computeIfAbsent(key, k -> new Aggregate());
                    aggregate.epSum += ep[i];
                    aggregate.epZSum += epZ[i];
                    if(partition[i]) {
                        aggregate.partitionCount++;
                    }
                    aggregate.leaves.add(i);
                }

                if(pruned != null && layer <= pruningLayers) {
                    for(final Map.Entry<List<String>, Aggregate> entry : elements.entrySet()) {
                        if(entry.getValue().epZSum < adjEpThreshold || entry.getValue().partitionCount == 0) {
                            pruned.computeIfAbsent(cuboidKey(cuboid), k -> new HashSet<>()).add(entry.getKey());
                        }
                    }
                }

                // Candidates with at least one abnormal leaf and EP above the current bar, EP-descending
                final List<Map.Entry<List<String>, Aggregate>> candidates = new ArrayList<>();
                for(final Map.Entry<List<String>, Aggregate> entry : elements.entrySet()) {
                    if(entry.getValue().partitionCount > 0 && entry.getValue().epSum > epBar) {
                        candidates.add(entry);
                    }
                }
                candidates.sort(Comparator.comparingDouble(
                        (Map.Entry<List<String>, Aggregate> entry) -> entry.getValue().epSum).reversed());

                for(final Map.Entry<List<String>, Aggregate> entry : candidates) {
                    final List<Integer> leaves = entry.getValue().leaves;
                    final double highRiskScore = highRisk(leaves, partition, weight);
                    final double lowRiskScore = lowRisk(leaves, f, v, deviations);
                    final double riskScore = highRiskScore - lowRiskScore;
                    if(riskScore >= riskThreshold) {
                        final Slice slice = new Slice(cuboid, entry.getKey().toArray(new String[0]));
                        double baselineSum = 0;
                        double targetSum = 0;
                        for(final int leaf : leaves) {
                            baselineSum += f[leaf];
                            targetSum += v[leaf];
                        }
                        best = new Finding(List.of(slice), riskScore, entry.getValue().epSum, null,
                                baselineSum, targetSum, leaves.size());
                        epBar = entry.getValue().epSum;
                        // Candidates are EP-descending: this is the best of this cuboid, continue with the next
                        break;
                    }
                }
            }
            if(best != null) {
                return best;
            }
        }
        return null;
    }

    private static void computePartitionAndWeights(
            final double cutoff,
            final double[] f,
            final double[] v,
            final double[] deviations,
            final boolean[] partition,
            final double[] weight) {

        final boolean anomalyRight = Math.copySign(1.0, cutoff) > 0;
        for(int i = 0; i < deviations.length; i++) {
            partition[i] = anomalyRight ? deviations[i] > cutoff : deviations[i] < cutoff;
            double w;
            if(partition[i]) {
                w = Math.abs(deviations[i]);
            } else {
                w = f[i] == 0 && v[i] == 0 ? 0 : Math.abs(cutoff - deviations[i]);
            }
            weight[i] = Math.min(w, 1.0);
        }
    }

    private static boolean isAllFalse(final boolean[] values) {
        for(final boolean value : values) {
            if(value) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cutoff between the normal and abnormal partitions: trim the {@value #N_REMOVE} largest and
     * smallest unique deviations, then negate whichever remaining extreme is closest to zero.
     */
    static double cutoff(final double[] deviations) {
        final double[] unique = Arrays.stream(deviations).distinct().sorted().toArray();
        final int last = unique.length - 1;
        final double minVal = unique[Math.min(N_REMOVE, last)];
        final double maxVal = unique[Math.max(last - N_REMOVE, 0)];
        final double chosen = Math.abs(minVal) <= Math.abs(maxVal) ? minVal : maxVal;
        return -chosen;
    }

    /** r1: weighted share of the element's leaves that lie in the abnormal partition. */
    static double highRisk(final List<Integer> leaves, final boolean[] partition, final double[] weight) {
        double nAnomaly = 0;
        double nNormal = 1;
        for(final int leaf : leaves) {
            if(partition[leaf]) {
                nAnomaly += weight[leaf];
            } else {
                nNormal += weight[leaf];
            }
        }
        return nAnomaly / (nAnomaly + nNormal);
    }

    /**
     * r2: ripple-effect penalty. Over leaves with both values nonzero, compare the deviation the
     * element would have if it followed the element-level real/predict ratio against its actual
     * deviations; a high ratio means the element's leaves deviate uniformly (expected change),
     * not anomalously.
     */
    static double lowRisk(final List<Integer> leaves, final double[] f, final double[] v, final double[] deviations) {
        final List<Integer> subset = new ArrayList<>();
        double sumF = 0;
        double sumV = 0;
        for(final int leaf : leaves) {
            if(v[leaf] != 0 && f[leaf] != 0) {
                subset.add(leaf);
                sumF += f[leaf];
                sumV += v[leaf];
            }
        }
        if(subset.isEmpty()) {
            return 0.0;
        }
        double devSum = 0;
        double actualDevSum = 0;
        for(final int leaf : subset) {
            final double a = f[leaf] * sumV / sumF;
            double d = 2 * (a - v[leaf]) / (a + v[leaf]);
            if(!Double.isFinite(d)) {
                d = 0.0;
            }
            devSum += Math.abs(d);
            actualDevSum += Math.abs(deviations[leaf]);
        }
        final double w1 = devSum / subset.size();
        final double w2 = actualDevSum / subset.size();
        return w2 == 0 ? 0.0 : w1 / w2;
    }

    private static boolean isPruned(
            final Map<List<Integer>, Set<List<String>>> pruned,
            final int[] cuboid,
            final String[] dims) {

        if(pruned == null || pruned.isEmpty()) {
            return false;
        }
        for(final Map.Entry<List<Integer>, Set<List<String>>> entry : pruned.entrySet()) {
            if(!isSubset(entry.getKey(), cuboid)) {
                continue;
            }
            final List<String> projection = new ArrayList<>(entry.getKey().size());
            for(final int dim : entry.getKey()) {
                projection.add(dims[dim]);
            }
            if(entry.getValue().contains(projection)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSubset(final List<Integer> key, final int[] cuboid) {
        for(final int dim : key) {
            if(Arrays.binarySearch(cuboid, dim) < 0) {
                return false;
            }
        }
        return true;
    }

    private static List<Integer> cuboidKey(final int[] cuboid) {
        return Arrays.stream(cuboid).boxed().toList();
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

    private static class Aggregate {
        private double epSum;
        private double epZSum;
        private int partitionCount;
        private final List<Integer> leaves = new ArrayList<>();
    }
}
