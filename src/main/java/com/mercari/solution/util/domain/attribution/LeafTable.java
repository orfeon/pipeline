package com.mercari.solution.util.domain.attribution;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;

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
 *
 * <p>Distribution columns hold one mergeable KLL sketch per leaf and role instead of a sum.
 * They are fed one sample value at a time ({@code addBaselineSample}/{@code addTargetSample}) or
 * merged wholesale ({@code addBaselineSketch}/{@code addTargetSketch}, used when re-aggregating).
 * A sketch with up to {@value #SKETCH_K} values is exact (and deterministic); beyond that,
 * quantile estimates carry the KLL error bounds.</p>
 *
 * <p>Distinct columns hold one mergeable Theta sketch of identity values per leaf and role.
 * They are fed one identity at a time ({@code addBaselineIdentity}/{@code addTargetIdentity})
 * or merged wholesale ({@code addBaselineDistinct}/{@code addTargetDistinct}). Up to
 * 2^{@value #THETA_LG_K} distinct identities per leaf and role the estimate is exact; beyond
 * that, Theta error bounds apply.</p>
 */
public final class LeafTable {

    public static final String NULL_VALUE = "(null)";

    /** KLL sketch size parameter: ~1.65% max quantile rank error at 99% confidence. */
    public static final int SKETCH_K = 200;

    /** Theta sketch log2 nominal entries: exact up to 4096 identities per leaf and role. */
    public static final int THETA_LG_K = 12;

    private final List<String> dimensionNames;
    private final List<String> columnNames;
    private final List<String> distributionNames;
    private final List<String> distinctNames;
    private final String[][] dimValues;                 // [leaf][dim]
    private final double[][] baseline;                  // [column][leaf]
    private final double[][] target;                    // [column][leaf]
    private final KllDoublesSketch[][] baselineSketches; // [distribution][leaf], entries may be null (no samples)
    private final KllDoublesSketch[][] targetSketches;   // [distribution][leaf]
    private final CompactSketch[][] baselineDistinct;    // [distinct][leaf], entries may be null (no identities)
    private final CompactSketch[][] targetDistinct;      // [distinct][leaf]

    private LeafTable(
            final List<String> dimensionNames,
            final List<String> columnNames,
            final List<String> distributionNames,
            final List<String> distinctNames,
            final String[][] dimValues,
            final double[][] baseline,
            final double[][] target,
            final KllDoublesSketch[][] baselineSketches,
            final KllDoublesSketch[][] targetSketches,
            final CompactSketch[][] baselineDistinct,
            final CompactSketch[][] targetDistinct) {

        this.dimensionNames = dimensionNames;
        this.columnNames = columnNames;
        this.distributionNames = distributionNames;
        this.distinctNames = distinctNames;
        this.dimValues = dimValues;
        this.baseline = baseline;
        this.target = target;
        this.baselineSketches = baselineSketches;
        this.targetSketches = targetSketches;
        this.baselineDistinct = baselineDistinct;
        this.targetDistinct = targetDistinct;
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getDistributionNames() {
        return distributionNames;
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

    public int distributionCount() {
        return distributionNames.size();
    }

    public List<String> getDistinctNames() {
        return distinctNames;
    }

    public int distinctCount() {
        return distinctNames.size();
    }

    public int distinctIndex(final String name) {
        final int index = distinctNames.indexOf(name);
        if(index < 0) {
            throw new IllegalArgumentException("distinct column not found: " + name + " in " + distinctNames);
        }
        return index;
    }

    /** Baseline identity sketch of a leaf, or null when the leaf received no baseline identities. */
    public CompactSketch baselineDistinct(final int distinct, final int leaf) {
        return baselineDistinct[distinct][leaf];
    }

    /** Target identity sketch of a leaf, or null when the leaf received no target identities. */
    public CompactSketch targetDistinct(final int distinct, final int leaf) {
        return targetDistinct[distinct][leaf];
    }

    public int columnIndex(final String name) {
        final int index = columnNames.indexOf(name);
        if(index < 0) {
            throw new IllegalArgumentException("column not found: " + name + " in " + columnNames);
        }
        return index;
    }

    public int distributionIndex(final String name) {
        final int index = distributionNames.indexOf(name);
        if(index < 0) {
            throw new IllegalArgumentException("distribution column not found: " + name + " in " + distributionNames);
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

    /** Baseline sketch of a leaf, or null when the leaf received no baseline samples. Do not mutate. */
    public KllDoublesSketch baselineSketch(final int distribution, final int leaf) {
        return baselineSketches[distribution][leaf];
    }

    /** Target sketch of a leaf, or null when the leaf received no target samples. Do not mutate. */
    public KllDoublesSketch targetSketch(final int distribution, final int leaf) {
        return targetSketches[distribution][leaf];
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
        return new LeafTable(dimensionNames, columnNames, distributionNames, distinctNames,
                dimValues, newBaseline, target, baselineSketches, targetSketches,
                baselineDistinct, targetDistinct);
    }

    private static double sum(final double[] values) {
        double sum = 0;
        for(final double value : values) {
            sum += value;
        }
        return sum;
    }

    public static Builder builder(final List<String> dimensionNames, final List<String> columnNames) {
        return new Builder(dimensionNames, columnNames, List.of(), List.of());
    }

    public static Builder builder(
            final List<String> dimensionNames,
            final List<String> columnNames,
            final List<String> distributionNames) {
        return new Builder(dimensionNames, columnNames, distributionNames, List.of());
    }

    public static Builder builder(
            final List<String> dimensionNames,
            final List<String> columnNames,
            final List<String> distributionNames,
            final List<String> distinctNames) {
        return new Builder(dimensionNames, columnNames, distributionNames, distinctNames);
    }

    public static class Builder {

        private final List<String> dimensionNames;
        private final List<String> columnNames;
        private final List<String> distributionNames;
        private final List<String> distinctNames;
        private final Map<List<String>, Accumulator> accumulator = new LinkedHashMap<>();

        /** Accumulates raw identity updates and wholesale sketch merges into one Theta sketch. */
        private static class ThetaAccumulator {
            private UpdateSketch updates;
            private Union merged;

            void update(final String identity) {
                if(updates == null) {
                    updates = UpdateSketch.builder().setLogNominalEntries(THETA_LG_K).build();
                }
                updates.update(identity);
            }

            void merge(final CompactSketch sketch) {
                if(merged == null) {
                    merged = SetOperation.builder().setLogNominalEntries(THETA_LG_K).buildUnion();
                }
                merged.union(sketch);
            }

            CompactSketch compact() {
                if(merged != null) {
                    if(updates != null) {
                        merged.union(updates.compact());
                    }
                    return merged.getResult();
                }
                return updates == null ? null : updates.compact();
            }
        }

        private static class Accumulator {
            final double[][] sums;                  // [role][column]
            final KllDoublesSketch[][] sketches;    // [role][distribution]
            final ThetaAccumulator[][] distincts;   // [role][distinct]

            Accumulator(final int columnCount, final int distributionCount, final int distinctCount) {
                this.sums = new double[2][columnCount];
                this.sketches = new KllDoublesSketch[2][distributionCount];
                this.distincts = new ThetaAccumulator[2][distinctCount];
            }
        }

        private Builder(
                final List<String> dimensionNames,
                final List<String> columnNames,
                final List<String> distributionNames,
                final List<String> distinctNames) {
            if(dimensionNames == null || dimensionNames.isEmpty()) {
                throw new IllegalArgumentException("dimensionNames must not be empty");
            }
            if((columnNames == null || columnNames.isEmpty())
                    && (distributionNames == null || distributionNames.isEmpty())
                    && (distinctNames == null || distinctNames.isEmpty())) {
                throw new IllegalArgumentException(
                        "columnNames, distributionNames and distinctNames must not all be empty");
            }
            this.dimensionNames = List.copyOf(dimensionNames);
            this.columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
            this.distributionNames = distributionNames == null ? List.of() : List.copyOf(distributionNames);
            this.distinctNames = distinctNames == null ? List.of() : List.copyOf(distinctNames);
        }

        public Builder addBaseline(final String[] dims, final double[] values) {
            return add(0, dims, values);
        }

        public Builder addTarget(final String[] dims, final double[] values) {
            return add(1, dims, values);
        }

        public Builder addBaselineSample(final String[] dims, final int distribution, final double value) {
            return addSample(0, dims, distribution, value);
        }

        public Builder addTargetSample(final String[] dims, final int distribution, final double value) {
            return addSample(1, dims, distribution, value);
        }

        public Builder addBaselineSketch(final String[] dims, final int distribution, final KllDoublesSketch sketch) {
            return addSketch(0, dims, distribution, sketch);
        }

        public Builder addTargetSketch(final String[] dims, final int distribution, final KllDoublesSketch sketch) {
            return addSketch(1, dims, distribution, sketch);
        }

        public Builder addBaselineIdentity(final String[] dims, final int distinct, final String identity) {
            return addIdentity(0, dims, distinct, identity);
        }

        public Builder addTargetIdentity(final String[] dims, final int distinct, final String identity) {
            return addIdentity(1, dims, distinct, identity);
        }

        public Builder addBaselineDistinct(final String[] dims, final int distinct, final CompactSketch sketch) {
            return addDistinct(0, dims, distinct, sketch);
        }

        public Builder addTargetDistinct(final String[] dims, final int distinct, final CompactSketch sketch) {
            return addDistinct(1, dims, distinct, sketch);
        }

        private Builder add(final int role, final String[] dims, final double[] values) {
            if(values.length != columnNames.size()) {
                throw new IllegalArgumentException("column count mismatch: "
                        + values.length + " != " + columnNames.size());
            }
            final Accumulator acc = accumulator(dims);
            for(int c = 0; c < values.length; c++) {
                if(!Double.isNaN(values[c])) {
                    acc.sums[role][c] += values[c];
                }
            }
            return this;
        }

        private Builder addSample(final int role, final String[] dims, final int distribution, final double value) {
            if(Double.isNaN(value)) {
                return this;
            }
            final Accumulator acc = accumulator(dims);
            if(acc.sketches[role][distribution] == null) {
                acc.sketches[role][distribution] = KllDoublesSketch.newHeapInstance(SKETCH_K);
            }
            acc.sketches[role][distribution].update(value);
            return this;
        }

        private Builder addSketch(final int role, final String[] dims, final int distribution, final KllDoublesSketch sketch) {
            if(sketch == null || sketch.isEmpty()) {
                return this;
            }
            final Accumulator acc = accumulator(dims);
            if(acc.sketches[role][distribution] == null) {
                acc.sketches[role][distribution] = KllDoublesSketch.newHeapInstance(SKETCH_K);
            }
            acc.sketches[role][distribution].merge(sketch);
            return this;
        }

        private Builder addIdentity(final int role, final String[] dims, final int distinct, final String identity) {
            if(identity == null) {
                return this;
            }
            final Accumulator acc = accumulator(dims);
            if(acc.distincts[role][distinct] == null) {
                acc.distincts[role][distinct] = new ThetaAccumulator();
            }
            acc.distincts[role][distinct].update(identity);
            return this;
        }

        private Builder addDistinct(final int role, final String[] dims, final int distinct, final CompactSketch sketch) {
            if(sketch == null || sketch.isEmpty()) {
                return this;
            }
            final Accumulator acc = accumulator(dims);
            if(acc.distincts[role][distinct] == null) {
                acc.distincts[role][distinct] = new ThetaAccumulator();
            }
            acc.distincts[role][distinct].merge(sketch);
            return this;
        }

        private Accumulator accumulator(final String[] dims) {
            if(dims.length != dimensionNames.size()) {
                throw new IllegalArgumentException("dimension count mismatch: "
                        + dims.length + " != " + dimensionNames.size());
            }
            final List<String> key = new ArrayList<>(dims.length);
            for(final String dim : dims) {
                key.add(dim == null ? NULL_VALUE : dim);
            }
            return accumulator.computeIfAbsent(
                    key, k -> new Accumulator(columnNames.size(), distributionNames.size(), distinctNames.size()));
        }

        public boolean isEmpty() {
            return accumulator.isEmpty();
        }

        public LeafTable build() {
            final int leafCount = accumulator.size();
            final int columnCount = columnNames.size();
            final int distributionCount = distributionNames.size();
            final int distinctCount = distinctNames.size();
            final String[][] dimValues = new String[leafCount][];
            final double[][] baseline = new double[columnCount][leafCount];
            final double[][] target = new double[columnCount][leafCount];
            final KllDoublesSketch[][] baselineSketches = new KllDoublesSketch[distributionCount][leafCount];
            final KllDoublesSketch[][] targetSketches = new KllDoublesSketch[distributionCount][leafCount];
            final CompactSketch[][] baselineDistinct = new CompactSketch[distinctCount][leafCount];
            final CompactSketch[][] targetDistinct = new CompactSketch[distinctCount][leafCount];
            int leaf = 0;
            for(final Map.Entry<List<String>, Accumulator> entry : accumulator.entrySet()) {
                dimValues[leaf] = entry.getKey().toArray(new String[0]);
                for(int c = 0; c < columnCount; c++) {
                    baseline[c][leaf] = entry.getValue().sums[0][c];
                    target[c][leaf] = entry.getValue().sums[1][c];
                }
                for(int d = 0; d < distributionCount; d++) {
                    baselineSketches[d][leaf] = entry.getValue().sketches[0][d];
                    targetSketches[d][leaf] = entry.getValue().sketches[1][d];
                }
                for(int d = 0; d < distinctCount; d++) {
                    final ThetaAccumulator theta0 = entry.getValue().distincts[0][d];
                    final ThetaAccumulator theta1 = entry.getValue().distincts[1][d];
                    baselineDistinct[d][leaf] = theta0 == null ? null : theta0.compact();
                    targetDistinct[d][leaf] = theta1 == null ? null : theta1.compact();
                }
                leaf++;
            }
            return new LeafTable(dimensionNames, columnNames, distributionNames, distinctNames,
                    dimValues, baseline, target, baselineSketches, targetSketches,
                    baselineDistinct, targetDistinct);
        }
    }
}
