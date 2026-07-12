package com.mercari.solution.util.domain.attribution;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Column-oriented store of leaf-aggregated rows for attribution analysis.
 * Each leaf is a distinct dimension value tuple with per-column baseline (f) and target (v) sums.
 * The builder sums duplicate tuples per role, so callers may feed unaggregated or multiply
 * bucketed rows (e.g. multiple time buckets per tuple). Missing dimension values are represented
 * as {@code "(null)"}; missing or NaN measure values count as 0.
 */
public final class LeafTable {

    public static final String NULL_VALUE = "(null)";

    private final List<String> dimensionNames;
    private final List<String> columnNames;
    private final String[][] dimValues;   // [leaf][dim]
    private final double[][] baseline;    // [column][leaf]
    private final double[][] target;      // [column][leaf]

    private LeafTable(
            final List<String> dimensionNames,
            final List<String> columnNames,
            final String[][] dimValues,
            final double[][] baseline,
            final double[][] target) {

        this.dimensionNames = dimensionNames;
        this.columnNames = columnNames;
        this.dimValues = dimValues;
        this.baseline = baseline;
        this.target = target;
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public int leafCount() {
        return dimValues.length;
    }

    public int dimensionCount() {
        return dimensionNames.size();
    }

    public int columnCount() {
        return columnNames.size();
    }

    public int columnIndex(final String name) {
        final int index = columnNames.indexOf(name);
        if(index < 0) {
            throw new IllegalArgumentException("column not found: " + name + " in " + columnNames);
        }
        return index;
    }

    public int dimensionIndex(final String name) {
        final int index = dimensionNames.indexOf(name);
        if(index < 0) {
            throw new IllegalArgumentException("dimension not found: " + name + " in " + dimensionNames);
        }
        return index;
    }

    /** Dimension values of a leaf. Callers must not mutate the returned array. */
    public String[] dims(final int leaf) {
        return dimValues[leaf];
    }

    public String dimValue(final int leaf, final int dim) {
        return dimValues[leaf][dim];
    }

    /** Baseline (f) column vector. Callers must not mutate the returned array. */
    public double[] baselineColumn(final int column) {
        return baseline[column];
    }

    /** Target (v) column vector. Callers must not mutate the returned array. */
    public double[] targetColumn(final int column) {
        return target[column];
    }

    public double baselineValue(final int column, final int leaf) {
        return baseline[column][leaf];
    }

    public double targetValue(final int column, final int leaf) {
        return target[column][leaf];
    }

    public double baselineTotal(final int column) {
        return sum(baseline[column]);
    }

    public double targetTotal(final int column) {
        return sum(target[column]);
    }

    public MeasureVector measureVector(final String columnName) {
        final int column = columnIndex(columnName);
        return MeasureVector.of(baseline[column], target[column]);
    }

    /** Distinct values of a dimension in first-appearance order. */
    public Set<String> dimensionValues(final int dim) {
        final Set<String> values = new LinkedHashSet<>();
        for(final String[] dims : dimValues) {
            values.add(dims[dim]);
        }
        return values;
    }

    /** Per dimension-value target sums of a column (marginal totals, used by the synthetic reference). */
    public Map<String, Double> targetMarginals(final int column, final int dim) {
        final Map<String, Double> marginals = new LinkedHashMap<>();
        for(int leaf = 0; leaf < dimValues.length; leaf++) {
            marginals.merge(dimValues[leaf][dim], target[column][leaf], Double::sum);
        }
        return marginals;
    }

    /** Returns a new table sharing dimensions and target columns but with replaced baseline columns. */
    public LeafTable withBaseline(final double[][] newBaseline) {
        if(newBaseline.length != baseline.length) {
            throw new IllegalArgumentException("baseline column count mismatch: "
                    + newBaseline.length + " != " + baseline.length);
        }
        for(final double[] column : newBaseline) {
            if(column.length != dimValues.length) {
                throw new IllegalArgumentException("baseline leaf count mismatch: "
                        + column.length + " != " + dimValues.length);
            }
        }
        return new LeafTable(dimensionNames, columnNames, dimValues, newBaseline, target);
    }

    private static double sum(final double[] values) {
        double sum = 0;
        for(final double value : values) {
            sum += value;
        }
        return sum;
    }

    public static Builder builder(final List<String> dimensionNames, final List<String> columnNames) {
        return new Builder(dimensionNames, columnNames);
    }

    public static class Builder {

        private final List<String> dimensionNames;
        private final List<String> columnNames;
        private final Map<List<String>, double[][]> accumulator = new LinkedHashMap<>();

        private Builder(final List<String> dimensionNames, final List<String> columnNames) {
            if(dimensionNames == null || dimensionNames.isEmpty()) {
                throw new IllegalArgumentException("dimensionNames must not be empty");
            }
            if(columnNames == null || columnNames.isEmpty()) {
                throw new IllegalArgumentException("columnNames must not be empty");
            }
            this.dimensionNames = List.copyOf(dimensionNames);
            this.columnNames = List.copyOf(columnNames);
        }

        public Builder addBaseline(final String[] dims, final double[] values) {
            return add(0, dims, values);
        }

        public Builder addTarget(final String[] dims, final double[] values) {
            return add(1, dims, values);
        }

        private Builder add(final int role, final String[] dims, final double[] values) {
            if(dims.length != dimensionNames.size()) {
                throw new IllegalArgumentException("dimension count mismatch: "
                        + dims.length + " != " + dimensionNames.size());
            }
            if(values.length != columnNames.size()) {
                throw new IllegalArgumentException("column count mismatch: "
                        + values.length + " != " + columnNames.size());
            }
            final List<String> key = new ArrayList<>(dims.length);
            for(final String dim : dims) {
                key.add(dim == null ? NULL_VALUE : dim);
            }
            final double[][] roles = accumulator.computeIfAbsent(
                    key, k -> new double[2][columnNames.size()]);
            for(int c = 0; c < values.length; c++) {
                if(!Double.isNaN(values[c])) {
                    roles[role][c] += values[c];
                }
            }
            return this;
        }

        public boolean isEmpty() {
            return accumulator.isEmpty();
        }

        public LeafTable build() {
            final int leafCount = accumulator.size();
            final int columnCount = columnNames.size();
            final String[][] dimValues = new String[leafCount][];
            final double[][] baseline = new double[columnCount][leafCount];
            final double[][] target = new double[columnCount][leafCount];
            int leaf = 0;
            for(final Map.Entry<List<String>, double[][]> entry : accumulator.entrySet()) {
                dimValues[leaf] = entry.getKey().toArray(new String[0]);
                for(int c = 0; c < columnCount; c++) {
                    baseline[c][leaf] = entry.getValue()[0][c];
                    target[c][leaf] = entry.getValue()[1][c];
                }
                leaf++;
            }
            return new LeafTable(dimensionNames, columnNames, dimValues, baseline, target);
        }
    }
}
