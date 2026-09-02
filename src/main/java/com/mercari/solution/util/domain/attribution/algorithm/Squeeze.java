package com.mercari.solution.util.domain.attribution.algorithm;

import com.mercari.solution.util.domain.attribution.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Squeeze: generic and robust localization of multi-dimensional root causes
 * (Li et al., ISSRE 2019). Faithful port of the reference implementation vendored in
 * <a href="https://github.com/shaido987/riskloc">shaido987/riskloc</a> (NetManAIOps/Squeeze):
 * knee-point amplitude filter (KDE CDF + Kneedle) → density-based 1-d clustering of leaf
 * deviation scores (histogram valleys) → per-cluster bottom-up/top-down search scored by the
 * generalized potential score (GPS), ranked by {@code score·weight − #elements·layer} with the
 * revised auto score weight (NetManAIOps/Squeeze issue #6, as vendored).
 *
 * <p>Unlike RiskLoc's per-slice iteration, each cluster yields one finding whose slice set is
 * the selected element combination. The numeric chain (KDE bandwidth, histogram binning,
 * extrema detection) mirrors numpy/scipy semantics; parity with the Python reference is
 * validated at the F1 level on the NetMan datasets rather than per-case (ADR-9).</p>
 */
public class Squeeze implements AttributionAlgorithm {

    private static final double KDE_POINTS = 1000;

    @Override
    public List<Finding> localize(final LeafTable table, final MeasureVector measure, final EngineConfig config) {
        final EngineConfig.SqueezeParams params = config.squeeze() == null
                ? EngineConfig.SqueezeParams.defaults() : config.squeeze();

        // valid rows: predict > 0 (reference constructor)
        final int total = table.leafCount();
        final List<Integer> validList = new ArrayList<>();
        for(int i = 0; i < total; i++) {
            if(measure.baseline()[i] > 0) {
                validList.add(i);
            }
        }
        if(validList.isEmpty()) {
            return List.of();
        }
        final int n = validList.size();
        final int[] leafIndex = new int[n];      // valid row -> original leaf
        final double[] f = new double[n];
        final double[] v = new double[n];
        for(int i = 0; i < n; i++) {
            leafIndex[i] = validList.get(i);
            f[i] = measure.baseline()[leafIndex[i]];
            v[i] = measure.target()[leafIndex[i]];
        }
        // reference: real -= min(min(real), 0)  ("error in injection" fix)
        double minV = Double.POSITIVE_INFINITY;
        for(final double value : v) {
            minV = Math.min(minV, value);
        }
        if(minV < 0) {
            for(int i = 0; i < n; i++) {
                v[i] -= minV;
            }
        }
        // leaf deviation score: (f - v) / (f + v), NaN -> 0 (f > 0 so no infinities)
        final double[] dev = new double[n];
        for(int i = 0; i < n; i++) {
            final double denom = f[i] + v[i];
            dev[i] = denom == 0 ? 0.0 : (f[i] - v[i]) / denom;
        }

        // attribute names sorted alphabetically (reference); keep mapping to table dim indexes
        final Integer[] dimOrder = new Integer[table.dimensionCount()];
        for(int d = 0; d < dimOrder.length; d++) {
            dimOrder[d] = d;
        }
        Arrays.sort(dimOrder, (a, b) -> table.getDimensionNames().get(a).compareTo(table.getDimensionNames().get(b)));
        final int dims = dimOrder.length;
        final String[][] rowValues = new String[n][dims]; // values in sorted-dim order
        for(int i = 0; i < n; i++) {
            for(int d = 0; d < dims; d++) {
                rowValues[i][d] = table.dimValue(leafIndex[i], dimOrder[d]);
            }
        }

        // amplitude filter: keep rows whose |v - f| exceeds the knee of the KDE CDF
        int[] filtered;
        if(params.enableFilter()) {
            final double[] metrics = new double[n];
            for(int i = 0; i < n; i++) {
                metrics[i] = Math.abs(v[i] - f[i]);
            }
            Double knee = kneeOfKdeCdf(metrics);
            if(knee == null) {
                knee = Arrays.stream(metrics).min().orElse(0);
            }
            final List<Integer> keep = new ArrayList<>();
            for(int i = 0; i < n; i++) {
                if(metrics[i] > knee) {
                    keep.add(i);
                }
            }
            filtered = keep.stream().mapToInt(Integer::intValue).toArray();
        } else {
            filtered = new int[n];
            for(int i = 0; i < n; i++) {
                filtered[i] = i;
            }
        }
        if(filtered.length == 0) {
            return List.of();
        }

        // density-based 1-d clustering on the filtered deviations, then expand each cluster to
        // every row whose deviation falls within the cluster's [min, max] range (reference run())
        final double[] filteredDev = new double[filtered.length];
        for(int i = 0; i < filtered.length; i++) {
            filteredDev[i] = dev[filtered[i]];
        }
        final List<int[]> rawClusters = densityCluster(filteredDev, params.maxNormalDeviation());
        final List<int[]> clusters = new ArrayList<>();
        for(final int[] cluster : rawClusters) {
            double lo = Double.POSITIVE_INFINITY;
            double hi = Double.NEGATIVE_INFINITY;
            for(final int idx : cluster) {
                lo = Math.min(lo, filteredDev[idx]);
                hi = Math.max(hi, filteredDev[idx]);
            }
            final List<Integer> expanded = new ArrayList<>();
            for(int i = 0; i < n; i++) {
                if(dev[i] >= lo && dev[i] <= hi) {
                    expanded.add(i);
                }
            }
            if(!expanded.isEmpty()) {
                clusters.add(expanded.stream().mapToInt(Integer::intValue).toArray());
            }
        }
        if(clusters.isEmpty()) {
            return List.of();
        }

        // normal rows: |dev| < dev[abnormal row with the smallest |dev|] (signed comparison,
        // exactly as the reference — an all-negative anomaly makes this set empty)
        int minAbsAbnormal = -1;
        for(final int[] cluster : clusters) {
            for(final int idx : cluster) {
                if(minAbsAbnormal < 0 || Math.abs(dev[idx]) < Math.abs(dev[minAbsAbnormal])) {
                    minAbsAbnormal = idx;
                }
            }
        }
        final double normalBound = dev[minAbsAbnormal];
        final List<Integer> normalList = new ArrayList<>();
        for(int i = 0; i < n; i++) {
            if(Math.abs(dev[i]) < normalBound) {
                normalList.add(i);
            }
        }
        final int[] normalIndices = normalList.stream().mapToInt(Integer::intValue).toArray();

        // revised auto score weight (vendored fix of NetManAIOps/Squeeze issue #6)
        int clusterSizeSum = 0;
        for(final int[] cluster : clusters) {
            clusterSizeSum += cluster.length;
        }
        int numAttr = 0;
        for(int d = 0; d < dims; d++) {
            final java.util.Set<String> values = new java.util.HashSet<>();
            for(int i = 0; i < n; i++) {
                values.add(rowValues[i][d]);
            }
            numAttr += values.size();
        }
        final double gCluster = Math.log(clusters.size() + 1) / clusters.size();
        final double gAttribute = numAttr / Math.log(numAttr + 1);
        final double gCoverage = -Math.log((double) clusterSizeSum / n);
        final double scoreWeight = gCluster * gAttribute * gCoverage;

        final int maxLayer = Math.min(dims, Math.max(1, config.guards().maxLayer()));
        final Map<String, Map<String, Integer>> descentCache = new HashMap<>();

        final List<Finding> findings = new ArrayList<>();
        for(final int[] cluster : clusters) {
            final CuboidResult best = locateInCluster(
                    cluster, normalIndices, rowValues, f, v, dims, maxLayer,
                    scoreWeight, params, descentCache);
            if(best == null || best.elements.isEmpty()) {
                continue;
            }
            findings.add(toFinding(best, dimOrder, table, measure));
            if(findings.size() >= config.topK()) {
                break;
            }
        }
        return findings;
    }

    private record CuboidResult(int[] cuboid, List<String[]> elements, double score, double rank) {
    }

    private CuboidResult locateInCluster(
            final int[] cluster,
            final int[] normalIndices,
            final String[][] rowValues,
            final double[] f,
            final double[] v,
            final int dims,
            final int maxLayer,
            final double scoreWeight,
            final EngineConfig.SqueezeParams params,
            final Map<String, Map<String, Integer>> descentCache) {

        final List<CuboidResult> results = new ArrayList<>();
        for(int layer = 1; layer <= maxLayer; layer++) {
            for(final int[] cuboid : Cuboids.layer(dims, layer)) {
                final CuboidResult result = locateInCuboid(
                        cuboid, cluster, normalIndices, rowValues, f, v, params, descentCache, scoreWeight, layer);
                if(result != null) {
                    results.add(result);
                }
            }
            boolean earlyExit = false;
            for(final CuboidResult result : results) {
                if(result.score > params.psUpperBound()) {
                    earlyExit = true;
                    break;
                }
            }
            if(earlyExit) {
                break;
            }
        }
        CuboidResult best = null;
        for(final CuboidResult result : results) {
            if(best == null || result.rank > best.rank) {
                best = result;
            }
        }
        return best;
    }

    private CuboidResult locateInCuboid(
            final int[] cuboid,
            final int[] cluster,
            final int[] normalIndices,
            final String[][] rowValues,
            final double[] f,
            final double[] v,
            final EngineConfig.SqueezeParams params,
            final Map<String, Map<String, Integer>> descentCache,
            final double scoreWeight,
            final int layer) {

        // abnormal element combinations with counts, ordered by value tuple (np.unique on AC)
        final TreeMap<String, int[]> elementCounts = new TreeMap<>();
        final Map<String, String[]> elementValues = new LinkedHashMap<>();
        for(final int idx : cluster) {
            final String key = elementKey(rowValues[idx], cuboid);
            elementCounts.computeIfAbsent(key, k -> {
                elementValues.put(k, elementValuesOf(rowValues[idx], cuboid));
                return new int[1];
            })[0]++;
        }

        // descendant counts over all valid rows, cached per cuboid
        final Map<String, Integer> descents = descentCache.computeIfAbsent(
                Arrays.toString(cuboid), key -> {
                    final Map<String, Integer> counts = new HashMap<>();
                    for(final String[] row : rowValues) {
                        counts.merge(elementKey(row, cuboid), 1, Integer::sum);
                    }
                    return counts;
                });

        // sort by descent score descending; ties reversed (np.argsort(score)[::-1] semantics)
        final List<String> keys = new ArrayList<>(elementCounts.keySet());
        final Map<String, Double> descentScore = new HashMap<>();
        for(final String key : keys) {
            descentScore.put(key, elementCounts.get(key)[0] / Math.max(descents.getOrDefault(key, 0), 1e-4));
        }
        final List<String> ascending = new ArrayList<>(keys);
        ascending.sort((a, b) -> Double.compare(descentScore.get(a), descentScore.get(b)));
        final List<String> ordered = new ArrayList<>(ascending.reversed());

        final int maxPartition = Math.min(
                Math.min(ordered.size(), params.maxNumElementsSingleCluster()),
                descents.size() - 1);
        if(maxPartition <= 0) {
            return null;
        }

        // subset = abnormal cluster + normal rows
        final int[] subset = new int[cluster.length + normalIndices.length];
        System.arraycopy(cluster, 0, subset, 0, cluster.length);
        System.arraycopy(normalIndices, 0, subset, cluster.length, normalIndices.length);

        double bestScore = Double.NEGATIVE_INFINITY;
        int bestPartition = -1;
        for(int partition = 1; partition <= maxPartition; partition++) {
            final java.util.Set<String> selection = new java.util.HashSet<>(ordered.subList(0, partition));
            final double score = generalizedPotentialScore(selection, subset, rowValues, f, v, cuboid);
            // np.argsort desc: among ties the largest partition wins
            if(score >= bestScore) {
                bestScore = score;
                bestPartition = partition;
            }
        }
        final List<String[]> elements = new ArrayList<>();
        for(final String key : ordered.subList(0, bestPartition)) {
            elements.add(elementValues.get(key));
        }
        final double rank = bestScore * scoreWeight - elements.size() * layer;
        return new CuboidResult(cuboid, elements, bestScore, rank);
    }

    /** GPS: 1 - (d(v1,a1)/n1 + d(v2,f2)/n2) / (d(v1,f1)/n1 + d(v2,f2)/n2) with L1 distances. */
    private static double generalizedPotentialScore(
            final java.util.Set<String> selection,
            final int[] subset,
            final String[][] rowValues,
            final double[] f,
            final double[] v,
            final int[] cuboid) {

        double sumVp = 0;
        double sumFp = 0;
        int np = 0;
        int nn = 0;
        for(final int idx : subset) {
            if(selection.contains(elementKey(rowValues[idx], cuboid))) {
                sumVp += v[idx];
                sumFp += f[idx];
                np++;
            } else {
                nn++;
            }
        }
        final double ratio = sumFp > 0 ? sumVp / sumFp : 0;
        double dVA = 0;   // |v1 - a1| with a1 = f1 * (Σv1/Σf1)
        double dVF1 = 0;  // |v1 - f1|
        double dVF2 = 0;  // |v2 - f2|
        for(final int idx : subset) {
            if(selection.contains(elementKey(rowValues[idx], cuboid))) {
                dVA += Math.abs(v[idx] - f[idx] * ratio);
                dVF1 += Math.abs(v[idx] - f[idx]);
            } else {
                dVF2 += Math.abs(v[idx] - f[idx]);
            }
        }
        final double numerator = divide(dVA, np) + divide(dVF2, nn);
        final double denominator = divide(dVF1, np) + divide(dVF2, nn);
        return 1 - numerator / denominator;
    }

    private static double divide(final double x, final double y) {
        if(y > 0) {
            return x / y;
        }
        return x == 0 ? 0 : Double.POSITIVE_INFINITY;
    }

    private static String elementKey(final String[] row, final int[] cuboid) {
        final StringBuilder sb = new StringBuilder();
        for(final int d : cuboid) {
            if(!sb.isEmpty()) {
                sb.append(' ');
            }
            sb.append(row[d]);
        }
        return sb.toString();
    }

    private static String[] elementValuesOf(final String[] row, final int[] cuboid) {
        final String[] values = new String[cuboid.length];
        for(int i = 0; i < cuboid.length; i++) {
            values[i] = row[cuboid[i]];
        }
        return values;
    }

    private Finding toFinding(
            final CuboidResult result,
            final Integer[] dimOrder,
            final LeafTable table,
            final MeasureVector measure) {

        final List<Slice> slices = new ArrayList<>();
        for(final String[] values : result.elements) {
            final int[] sliceDims = new int[result.cuboid.length];
            for(int i = 0; i < result.cuboid.length; i++) {
                sliceDims[i] = dimOrder[result.cuboid[i]];
            }
            // Slice dims must be ascending table-dim indexes
            final Integer[] order = new Integer[sliceDims.length];
            for(int i = 0; i < order.length; i++) {
                order[i] = i;
            }
            Arrays.sort(order, (a, b) -> Integer.compare(sliceDims[a], sliceDims[b]));
            final int[] sortedDims = new int[sliceDims.length];
            final String[] sortedValues = new String[sliceDims.length];
            for(int i = 0; i < order.length; i++) {
                sortedDims[i] = sliceDims[order[i]];
                sortedValues[i] = values[order[i]];
            }
            slices.add(new Slice(sortedDims, sortedValues));
        }

        final double[] ep = measure.explanatoryPowers();
        double epSum = 0;
        double baselineSum = 0;
        double targetSum = 0;
        int leafCount = 0;
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            for(final Slice slice : slices) {
                if(slice.contains(table.dims(leaf))) {
                    epSum += ep[leaf];
                    baselineSum += measure.baseline()[leaf];
                    targetSum += measure.target()[leaf];
                    leafCount++;
                    break;
                }
            }
        }
        // riskScore carries the cluster's generalized potential score (the selection confidence,
        // also the input of the external-root-cause judgment)
        return new Finding(slices, result.score, epSum, null, baselineSum, targetSum, leafCount);
    }

    // ------------------------------------------------------------------
    // Amplitude filter: knee of the KDE CDF (scipy gaussian_kde + kneed KneeLocator)
    // ------------------------------------------------------------------

    /** Returns the knee point of the CDF of a Gaussian KDE over the values, or null. */
    static Double kneeOfKdeCdf(final double[] values) {
        final int n = values.length;
        if(n < 2) {
            return null;
        }
        double mean = 0;
        for(final double value : values) {
            mean += value;
        }
        mean /= n;
        double variance = 0;
        for(final double value : values) {
            variance += (value - mean) * (value - mean);
        }
        variance /= (n - 1);
        if(variance <= 0) {
            return null;
        }
        // Scott's rule (scipy default): bandwidth = std * n^(-1/5)
        final double h = Math.sqrt(variance) * Math.pow(n, -0.2);
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for(final double value : values) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        final int points = (int) KDE_POINTS;
        final double[] xs = new double[points];
        final double[] cdf = new double[points];
        final double norm = 1.0 / (n * h * Math.sqrt(2 * Math.PI));
        double cumulative = 0;
        for(int i = 0; i < points; i++) {
            xs[i] = min + (max - min) * i / (points - 1);
            double density = 0;
            for(final double value : values) {
                final double z = (xs[i] - value) / h;
                density += Math.exp(-0.5 * z * z);
            }
            cumulative += density * norm;
            cdf[i] = cumulative;
        }
        return kneedle(xs, cdf);
    }

    /** Kneedle knee detection for a concave increasing curve (kneed defaults, S = 1, offline). */
    static Double kneedle(final double[] x, final double[] y) {
        final int n = x.length;
        if(n < 3) {
            return null;
        }
        final double[] xn = normalize(x);
        final double[] yn = normalize(y);
        final double[] yd = new double[n];
        for(int i = 0; i < n; i++) {
            yd[i] = yn[i] - xn[i];
        }
        final int[] maxima = relExtremaClip(yd, true);
        if(maxima.length == 0) {
            return null;
        }
        final int[] minima = relExtremaClip(yd, false);
        final java.util.Set<Integer> maximaSet = new java.util.HashSet<>();
        for(final int idx : maxima) {
            maximaSet.add(idx);
        }
        final java.util.Set<Integer> minimaSet = new java.util.HashSet<>();
        for(final int idx : minima) {
            minimaSet.add(idx);
        }
        final double s = 1.0 / (n - 1); // S * mean(|diff(x_norm)|)
        double threshold = 0;
        int thresholdIndex = maxima[0];
        int maximaCursor = 0;
        for(int i = maxima[0]; i < n - 1; i++) {
            if(maximaSet.contains(i)) {
                threshold = yd[i] - s;
                thresholdIndex = i;
                maximaCursor++;
            }
            if(minimaSet.contains(i)) {
                threshold = 0;
            }
            if(yd[i + 1] < threshold) {
                return x[thresholdIndex];
            }
        }
        return null;
    }

    private static double[] normalize(final double[] values) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for(final double value : values) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        final double range = max - min;
        final double[] normalized = new double[values.length];
        for(int i = 0; i < values.length; i++) {
            normalized[i] = range == 0 ? 0 : (values[i] - min) / range;
        }
        return normalized;
    }

    /** argrelextrema with order=1, mode='clip': out-of-bounds neighbors clip to the edge value. */
    private static int[] relExtremaClip(final double[] values, final boolean maxima) {
        final List<Integer> indices = new ArrayList<>();
        final int n = values.length;
        for(int i = 0; i < n; i++) {
            final double left = values[Math.max(i - 1, 0)];
            final double right = values[Math.min(i + 1, n - 1)];
            final boolean hit = maxima
                    ? values[i] >= left && values[i] >= right
                    : values[i] <= left && values[i] <= right;
            if(hit) {
                indices.add(i);
            }
        }
        return indices.stream().mapToInt(Integer::intValue).toArray();
    }

    // ------------------------------------------------------------------
    // Density-based 1-d clustering (histogram estimation, valley splitting)
    // ------------------------------------------------------------------

    /** Splits deviation scores into clusters at histogram density valleys (reference defaults). */
    static List<int[]> densityCluster(final double[] array, final double maxNormalDeviation) {
        final double[] edgesCore = histogramAutoEdges(array);
        // extend by five 0.1-wide bins on each side (reference _get_hist)
        final double[] edges = new double[edgesCore.length + 10];
        for(int i = 0; i < 5; i++) {
            edges[i] = edgesCore[0] - 0.1 * (5 - i);
            edges[edges.length - 5 + i] = edgesCore[edgesCore.length - 1] + 0.1 * (i + 1);
        }
        System.arraycopy(edgesCore, 0, edges, 5, edgesCore.length);

        final int bins = edges.length - 1;
        final double[] counts = new double[bins];
        for(final double value : array) {
            int idx = java.util.Arrays.binarySearch(edges, value);
            if(idx < 0) {
                idx = -idx - 2;
            } else if(idx == bins) {
                idx = bins - 1; // right-closed last bin (numpy histogram)
            }
            if(idx >= 0 && idx < bins) {
                counts[idx]++;
            }
        }
        final double[] density = new double[bins];
        final double[] centers = new double[bins];
        for(int i = 0; i < bins; i++) {
            density[i] = counts[i] / (array.length * (edges[i + 1] - edges[i])) / 100.0;
            centers[i] = (edges[i] + edges[i + 1]) / 2;
        }

        // smoothing: moving average, auto window = max(nonzero/10, 1), first window-1 kept raw
        int nonzero = 0;
        for(final double d : density) {
            if(d > 0) {
                nonzero++;
            }
        }
        final int window = Math.max(nonzero / 10, 1);
        final double[] smoothed = new double[bins];
        for(int i = 0; i < bins; i++) {
            if(i < window - 1) {
                smoothed[i] = density[i];
            } else {
                double sum = 0;
                for(int j = i - window + 1; j <= i; j++) {
                    sum += density[j];
                }
                smoothed[i] = sum / window;
            }
        }

        // extrema with order=1, mode='wrap' (neighbors modulo n)
        final List<Integer> peakList = new ArrayList<>();
        final List<Integer> valleyList = new ArrayList<>();
        for(int i = 0; i < bins; i++) {
            final double left = smoothed[Math.floorMod(i - 1, bins)];
            final double right = smoothed[Math.floorMod(i + 1, bins)];
            if(smoothed[i] > left && smoothed[i] > right && smoothed[i] > 0) {
                peakList.add(i);
            }
            if(smoothed[i] <= left && smoothed[i] <= right) {
                valleyList.add(i);
            }
        }

        final double[] boundaries = new double[valleyList.size() + 2];
        boundaries[0] = Double.NEGATIVE_INFINITY;
        for(int i = 0; i < valleyList.size(); i++) {
            boundaries[i + 1] = centers[valleyList.get(i)];
        }
        boundaries[boundaries.length - 1] = Double.POSITIVE_INFINITY;

        final List<int[]> clusters = new ArrayList<>();
        for(final int peak : peakList) {
            final double center = centers[peak];
            final double left = boundaries[searchSortedRight(boundaries, center) - 1];
            final double right = boundaries[searchSortedLeft(boundaries, center)];
            final List<Integer> members = new ArrayList<>();
            double absSum = 0;
            for(int i = 0; i < array.length; i++) {
                if(array[i] >= left && array[i] <= right) {
                    members.add(i);
                    absSum += Math.abs(array[i]);
                }
            }
            if(members.isEmpty() || absSum / members.size() < maxNormalDeviation) {
                continue;
            }
            clusters.add(members.stream().mapToInt(Integer::intValue).toArray());
        }
        return clusters;
    }

    /** numpy histogram_bin_edges(array, 'auto'): max of Freedman-Diaconis and Sturges bin counts. */
    static double[] histogramAutoEdges(final double[] array) {
        final double[] sorted = array.clone();
        Arrays.sort(sorted);
        final int n = sorted.length;
        double first = sorted[0];
        double last = sorted[n - 1];
        if(first == last) {
            first -= 0.5;
            last += 0.5;
        }
        final double range = last - first;
        final double iqr = percentile(sorted, 75) - percentile(sorted, 25);
        final double fdWidth = 2 * iqr * Math.pow(n, -1.0 / 3.0);
        final double sturgesWidth = range / (log2(n) + 1);
        final double width = fdWidth > 0 ? Math.min(fdWidth, sturgesWidth) : sturgesWidth;
        final int binCount = width > 0 ? (int) Math.ceil(range / width) : 1;
        final double[] edges = new double[binCount + 1];
        for(int i = 0; i <= binCount; i++) {
            edges[i] = first + range * i / binCount;
        }
        return edges;
    }

    private static double percentile(final double[] sorted, final double q) {
        final double pos = (sorted.length - 1) * q / 100.0;
        final int lower = (int) Math.floor(pos);
        final int upper = (int) Math.ceil(pos);
        if(lower == upper) {
            return sorted[lower];
        }
        return sorted[lower] + (pos - lower) * (sorted[upper] - sorted[lower]);
    }

    private static double log2(final double value) {
        return Math.log(value) / Math.log(2);
    }

    private static int searchSortedLeft(final double[] sorted, final double value) {
        int lo = 0;
        int hi = sorted.length;
        while(lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if(sorted[mid] < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private static int searchSortedRight(final double[] sorted, final double value) {
        int lo = 0;
        int hi = sorted.length;
        while(lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if(sorted[mid] <= value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
