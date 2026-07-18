package com.mercari.solution.util.domain.attribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Algorithm-neutral data conditioning applied before attribution search:
 * numeric binning of {@code binned} dimensions and cardinality/support guards.
 * All operations relabel dimension values and re-aggregate, so the lattice stays consistent.
 * {@code guards.maxLayer} is not preprocessing — it bounds the cuboid search and is consumed
 * by the algorithms directly.
 */
public final class Preprocess {

    public static final String OTHER_VALUE = "other";

    private Preprocess() {
    }

    /**
     * Bins numeric values of {@code binned} dimensions into interval labels like {@code [lo,hi)}.
     * Quantile binning uses equal leaf-count bins; width binning splits [min,max] evenly.
     * Unparseable values map to {@link #OTHER_VALUE}.
     */
    public static LeafTable bin(final LeafTable table, final List<DimensionSpec> dimensions) {
        final Map<Integer, DimensionSpec> binnedDims = new LinkedHashMap<>();
        for(final DimensionSpec dimension : dimensions) {
            if(DimensionSpec.Type.binned.equals(dimension.type())) {
                binnedDims.put(table.dimensionIndex(dimension.name()), dimension);
            }
        }
        if(binnedDims.isEmpty()) {
            return table;
        }

        final Map<Integer, double[]> edgesPerDim = new HashMap<>();
        for(final Map.Entry<Integer, DimensionSpec> entry : binnedDims.entrySet()) {
            edgesPerDim.put(entry.getKey(), computeEdges(table, entry.getKey(), entry.getValue().binning()));
        }

        return rebuild(table, (leaf, dims) -> {
            final String[] relabeled = dims.clone();
            for(final Map.Entry<Integer, double[]> entry : edgesPerDim.entrySet()) {
                relabeled[entry.getKey()] = binLabel(dims[entry.getKey()], entry.getValue());
            }
            return relabeled;
        });
    }

    /**
     * Applies {@code maxCardinality} (keep the top-N values per dimension by volume + delta,
     * relabel the rest {@code other}) and {@code minSupport} (relabel dimension values whose
     * volume share is below the threshold in every column), then re-aggregates.
     */
    public static LeafTable applyGuards(final LeafTable table, final EngineConfig.Guards guards) {
        final List<Set<String>> keepValues = new ArrayList<>();
        boolean anyRelabel = false;
        for(int dim = 0; dim < table.dimensionCount(); dim++) {
            final Map<String, Double> scores = valueScores(table, dim);
            final Map<String, Double> supports = valueSupports(table, dim);

            Set<String> keep = new HashSet<>(scores.keySet());
            if(guards.maxCardinality() > 0 && keep.size() > guards.maxCardinality()) {
                keep = new HashSet<>(scores.entrySet().stream()
                        .sorted(Map.Entry.<String, Double>comparingByValue().reversed()
                                .thenComparing(Map.Entry.comparingByKey()))
                        .limit(guards.maxCardinality())
                        .map(Map.Entry::getKey)
                        .toList());
            }
            if(guards.minSupport() > 0) {
                keep.removeIf(value -> supports.getOrDefault(value, 0.0) < guards.minSupport());
            }
            if(keep.size() < scores.size()) {
                anyRelabel = true;
            }
            keepValues.add(keep);
        }
        if(!anyRelabel) {
            return table;
        }

        return rebuild(table, (leaf, dims) -> {
            String[] relabeled = null;
            for(int dim = 0; dim < dims.length; dim++) {
                if(!keepValues.get(dim).contains(dims[dim])) {
                    if(relabeled == null) {
                        relabeled = dims.clone();
                    }
                    relabeled[dim] = OTHER_VALUE;
                }
            }
            return relabeled == null ? dims : relabeled;
        });
    }

    private interface Relabeler {
        String[] relabel(int leaf, String[] dims);
    }

    private static LeafTable rebuild(final LeafTable table, final Relabeler relabeler) {
        final LeafTable.Builder builder = LeafTable
                .builder(table.getDimensionNames(), table.getColumnNames());
        final int columnCount = table.columnCount();
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            final String[] dims = relabeler.relabel(leaf, table.dims(leaf));
            final double[] baselineValues = new double[columnCount];
            final double[] targetValues = new double[columnCount];
            for(int c = 0; c < columnCount; c++) {
                baselineValues[c] = table.baselineValue(c, leaf);
                targetValues[c] = table.targetValue(c, leaf);
            }
            builder.addBaseline(dims, baselineValues);
            builder.addTarget(dims, targetValues);
        }
        return builder.build();
    }

    /** Ranking score for maxCardinality: absolute volume plus absolute delta, summed over columns. */
    private static Map<String, Double> valueScores(final LeafTable table, final int dim) {
        final Map<String, Double> scores = new LinkedHashMap<>();
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            double score = 0;
            for(int c = 0; c < table.columnCount(); c++) {
                final double f = table.baselineValue(c, leaf);
                final double v = table.targetValue(c, leaf);
                score += Math.abs(f) + Math.abs(v) + Math.abs(v - f);
            }
            scores.merge(table.dimValue(leaf, dim), score, Double::sum);
        }
        return scores;
    }

    /** Volume share per dimension value: max over columns of (Σ|f|+Σ|v|) / column total volume. */
    private static Map<String, Double> valueSupports(final LeafTable table, final int dim) {
        final int columnCount = table.columnCount();
        final Map<String, double[]> volumes = new LinkedHashMap<>();
        final double[] totals = new double[columnCount];
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            final double[] volume = volumes.computeIfAbsent(
                    table.dimValue(leaf, dim), k -> new double[columnCount]);
            for(int c = 0; c < columnCount; c++) {
                final double abs = Math.abs(table.baselineValue(c, leaf)) + Math.abs(table.targetValue(c, leaf));
                volume[c] += abs;
                totals[c] += abs;
            }
        }
        final Map<String, Double> supports = new LinkedHashMap<>();
        for(final Map.Entry<String, double[]> entry : volumes.entrySet()) {
            double max = 0;
            for(int c = 0; c < columnCount; c++) {
                if(totals[c] > 0) {
                    max = Math.max(max, entry.getValue()[c] / totals[c]);
                }
            }
            supports.put(entry.getKey(), max);
        }
        return supports;
    }

    private static double[] computeEdges(final LeafTable table, final int dim, final DimensionSpec.Binning binning) {
        final List<Double> values = new ArrayList<>();
        for(int leaf = 0; leaf < table.leafCount(); leaf++) {
            final Double value = parse(table.dimValue(leaf, dim));
            if(value != null) {
                values.add(value);
            }
        }
        if(values.isEmpty()) {
            return new double[0];
        }
        final int bins = binning.bins();
        final double[] edges;
        if(DimensionSpec.Binning.Method.quantile.equals(binning.method())) {
            final List<Double> sorted = values.stream().sorted().toList();
            edges = new double[bins + 1];
            for(int i = 0; i <= bins; i++) {
                final int index = Math.min((int) Math.floor((double) i * sorted.size() / bins), sorted.size() - 1);
                edges[i] = sorted.get(index);
            }
            edges[bins] = sorted.getLast();
        } else {
            final double min = values.stream().mapToDouble(Double::doubleValue).min().orElse(0);
            final double max = values.stream().mapToDouble(Double::doubleValue).max().orElse(0);
            edges = new double[bins + 1];
            for(int i = 0; i <= bins; i++) {
                edges[i] = min + (max - min) * i / bins;
            }
        }
        // Collapse duplicate edges (few distinct values) into fewer bins
        return Arrays.stream(edges).distinct().toArray();
    }

    private static String binLabel(final String rawValue, final double[] edges) {
        final Double value = parse(rawValue);
        if(value == null || edges.length == 0) {
            return OTHER_VALUE;
        }
        if(edges.length == 1) {
            return formatBin(edges[0], edges[0], true);
        }
        for(int i = 0; i < edges.length - 1; i++) {
            final boolean last = i == edges.length - 2;
            if(value >= edges[i] && (value < edges[i + 1] || (last && value <= edges[i + 1]))) {
                return formatBin(edges[i], edges[i + 1], last);
            }
        }
        return OTHER_VALUE;
    }

    private static String formatBin(final double lo, final double hi, final boolean inclusive) {
        return "[" + formatNumber(lo) + "," + formatNumber(hi) + (inclusive ? "]" : ")");
    }

    private static String formatNumber(final double value) {
        if(value == Math.floor(value) && !Double.isInfinite(value) && Math.abs(value) < 1e15) {
            return Long.toString((long) value);
        }
        return Double.toString(value);
    }

    private static Double parse(final String value) {
        if(value == null) {
            return null;
        }
        try {
            final double d = Double.parseDouble(value.trim());
            return Double.isFinite(d) ? d : null;
        } catch (final NumberFormatException e) {
            return null;
        }
    }
}
