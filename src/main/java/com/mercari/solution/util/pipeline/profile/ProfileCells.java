package com.mercari.solution.util.pipeline.profile;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * The counting pass's state for one group of rows — the whole dataset, a target class, or a group
 * of a comparison axis (profile-engine.md §4): per field the exact row counts over its cells and
 * over its declared edges, the null count and the exact scalars. Every comparison statistic is a
 * function of these arrays.
 *
 * <p>The arrays grow to the largest cell index seen, since the number of cells is only known to
 * the step that located the row; {@link #cells(int, int)} pads them when they are read.
 */
public class ProfileCells implements Serializable {

    /** One row located on the edges: its coerced values and, per field, the cells they fall in. */
    public static class Row implements Serializable {
        public Object[] values;
        public int[] cells;         // per field: a cell index, NULL_CELL or NO_CELL
        public int[] declared;      // per field: a declared-edge bin or NO_CELL; null when no field declares edges
        public int[] pairA;         // per pair: the pooled cell of member a (whole-dataset rows only), else null
        public int[] pairB;
        public byte targetClass;    // 1 positive, 0 negative, -1 none
    }

    private long rows;
    private long targetPositive;
    private long targetNegative;

    private long[] count;
    private long[] nulls;
    private double[] min;
    private double[] max;
    private double[] sum;
    private double[] sumCompensation;
    private double[] mean;
    private double[] m2;
    private long[][] cells;
    private long[][] declared;
    private long[][] pairA;
    private long[][] pairB;

    private ProfileCells() {
    }

    public static ProfileCells of(final int fields) {
        final ProfileCells c = new ProfileCells();
        c.count = new long[fields];
        c.nulls = new long[fields];
        c.min = new double[fields];
        c.max = new double[fields];
        Arrays.fill(c.min, Double.POSITIVE_INFINITY);
        Arrays.fill(c.max, Double.NEGATIVE_INFINITY);
        c.sum = new double[fields];
        c.sumCompensation = new double[fields];
        c.mean = new double[fields];
        c.m2 = new double[fields];
        c.cells = new long[fields][];
        c.declared = new long[fields][];
        return c;
    }

    public void add(final Row row) {
        rows += 1;
        if(row.targetClass == 1) {
            targetPositive += 1;
        } else if(row.targetClass == 0) {
            targetNegative += 1;
        }
        for(int f = 0; f < count.length; f++) {
            final int cell = row.cells[f];
            if(cell == ProfileEdges.NULL_CELL) {
                nulls[f] += 1;
                continue;
            }
            if(cell < 0) {
                continue;
            }
            count[f] += 1;
            cells[f] = increment(cells[f], cell);
            final Double v = ProfileEdges.numeric(row.values[f]);
            if(v != null) {
                addNumeric(f, v);
            }
            if(row.declared != null && row.declared[f] >= 0) {
                declared[f] = increment(declared[f], row.declared[f]);
            }
        }
        if(row.pairA != null) {
            if(pairA == null) {
                pairA = new long[row.pairA.length][];
                pairB = new long[row.pairB.length][];
            }
            for(int p = 0; p < row.pairA.length; p++) {
                if(row.pairA[p] >= 0) {
                    pairA[p] = increment(pairA[p], row.pairA[p]);
                }
                if(row.pairB[p] >= 0) {
                    pairB[p] = increment(pairB[p], row.pairB[p]);
                }
            }
        }
    }

    private void addNumeric(final int f, final double v) {
        if(v < min[f]) {
            min[f] = v;
        }
        if(v > max[f]) {
            max[f] = v;
        }
        final double y = v - sumCompensation[f];
        final double t = sum[f] + y;
        sumCompensation[f] = (t - sum[f]) - y;
        sum[f] = t;
        // count[f] was already incremented for this observation
        final double delta = v - mean[f];
        mean[f] += delta / count[f];
        m2[f] += delta * (v - mean[f]);
    }

    private static long[] increment(final long[] array, final int index) {
        final long[] grown = array == null ? new long[index + 1]
                : array.length <= index ? Arrays.copyOf(array, index + 1) : array;
        grown[index] += 1;
        return grown;
    }

    public ProfileCells merge(final ProfileCells other) {
        rows += other.rows;
        targetPositive += other.targetPositive;
        targetNegative += other.targetNegative;
        for(int f = 0; f < count.length; f++) {
            final double na = count[f];
            final double nb = other.count[f];
            if(nb > 0) {
                if(na == 0) {
                    mean[f] = other.mean[f];
                    m2[f] = other.m2[f];
                } else {
                    final double delta = other.mean[f] - mean[f];
                    m2[f] += other.m2[f] + delta * delta * na * nb / (na + nb);
                    mean[f] += delta * nb / (na + nb);
                }
            }
            count[f] += other.count[f];
            nulls[f] += other.nulls[f];
            min[f] = Math.min(min[f], other.min[f]);
            max[f] = Math.max(max[f], other.max[f]);
            final double y = other.sum[f] - sumCompensation[f];
            final double t = sum[f] + y;
            sumCompensation[f] = (t - sum[f]) - y;
            sum[f] = t;
            cells[f] = plus(cells[f], other.cells[f]);
            declared[f] = plus(declared[f], other.declared[f]);
        }
        if(other.pairA != null) {
            if(pairA == null) {
                pairA = new long[other.pairA.length][];
                pairB = new long[other.pairB.length][];
            }
            for(int p = 0; p < other.pairA.length; p++) {
                pairA[p] = plus(pairA[p], other.pairA[p]);
                pairB[p] = plus(pairB[p], other.pairB[p]);
            }
        }
        return this;
    }

    private static long[] plus(final long[] a, final long[] b) {
        if(b == null) {
            return a;
        }
        if(a == null) {
            return b.clone();
        }
        final long[] out = a.length >= b.length ? a : Arrays.copyOf(a, b.length);
        for(int i = 0; i < b.length; i++) {
            out[i] += b[i];
        }
        return out;
    }

    // ---- reading ----

    public long getRows() {
        return rows;
    }

    public long getTargetPositive() {
        return targetPositive;
    }

    public long getTargetNegative() {
        return targetNegative;
    }

    public long count(final int field) {
        return count[field];
    }

    public long nulls(final int field) {
        return nulls[field];
    }

    /** True when the field has numeric observations in this group. */
    public boolean hasNumeric(final int field) {
        return count[field] > 0 && min[field] <= max[field];
    }

    public double min(final int field) {
        return min[field];
    }

    public double max(final int field) {
        return max[field];
    }

    public double sum(final int field) {
        return sum[field];
    }

    public double mean(final int field) {
        return mean[field];
    }

    /** Sum of squared deviations from the mean. */
    public double m2(final int field) {
        return m2[field];
    }

    /** The field's cell counts padded to {@code size} cells. */
    public long[] cells(final int field, final int size) {
        return padded(cells[field], size);
    }

    /** The field's counts over its declared edges padded to {@code size} bins. */
    public long[] declared(final int field, final int size) {
        return padded(declared[field], size);
    }

    public long[] pairA(final int pair, final int size) {
        return padded(pairA == null ? null : pairA[pair], size);
    }

    public long[] pairB(final int pair, final int size) {
        return padded(pairB == null ? null : pairB[pair], size);
    }

    private static long[] padded(final long[] array, final int size) {
        if(array == null) {
            return new long[size];
        }
        return array.length == size ? array.clone() : Arrays.copyOf(array, Math.max(size, array.length));
    }

    /** Locates a row on the edges, once for every group it is counted in. */
    public static Row locate(
            final ProfileSpec spec,
            final ProfileEdges edges,
            final ProfileRow source,
            final boolean hasDeclared) {

        final List<ProfileSpec.FieldSpec> fields = spec.getFields();
        final Row row = new Row();
        row.values = source.values;
        row.cells = new int[fields.size()];
        row.declared = hasDeclared ? new int[fields.size()] : null;
        for(int f = 0; f < fields.size(); f++) {
            row.cells[f] = edges.cellOf(fields.get(f), f, source.values[f]);
            if(hasDeclared) {
                row.declared[f] = row.cells[f] >= 0 ? edges.declaredCellOf(f, source.values[f]) : ProfileEdges.NO_CELL;
            }
        }
        row.targetClass = -1;
        final ProfileSpec.TargetSpec target = spec.getTarget();
        if(target != null) {
            final Boolean targetClass = target.classOf(source.values[target.fieldIndex]);
            if(targetClass != null) {
                row.targetClass = (byte) (targetClass ? 1 : 0);
            }
        }
        return row;
    }

    /** A copy of {@code row} that also carries the pooled cells of the declared pairs (whole-dataset rows). */
    public static Row withPairs(final ProfileEdges edges, final Row row) {
        if(edges.getPairCount() == 0) {
            return row;
        }
        final Row copy = new Row();
        copy.values = row.values;
        copy.cells = row.cells;
        copy.declared = row.declared;
        copy.targetClass = row.targetClass;
        copy.pairA = new int[edges.getPairCount()];
        copy.pairB = new int[edges.getPairCount()];
        for(int p = 0; p < edges.getPairCount(); p++) {
            final int[] members = edges.getPairFields(p);
            copy.pairA[p] = members[0] < 0 ? ProfileEdges.NO_CELL : edges.pairCellOf(p, row.values[members[0]]);
            copy.pairB[p] = members[1] < 0 ? ProfileEdges.NO_CELL : edges.pairCellOf(p, row.values[members[1]]);
        }
        return copy;
    }

    /** Counts located rows per group; a plain CombineFn so the runner can lift it ahead of the shuffle. */
    public static class Fn extends Combine.CombineFn<Row, ProfileCells, ProfileCells> {

        private final int fields;

        public Fn(final int fields) {
            this.fields = fields;
        }

        @Override
        public ProfileCells createAccumulator() {
            return ProfileCells.of(fields);
        }

        @Override
        public ProfileCells addInput(final ProfileCells accumulator, final Row row) {
            if(row != null && row.values != null) {
                accumulator.add(row);
            }
            return accumulator;
        }

        @Override
        public ProfileCells mergeAccumulators(final Iterable<ProfileCells> accumulators) {
            ProfileCells merged = null;
            for(final ProfileCells accumulator : accumulators) {
                merged = merged == null ? accumulator : merged.merge(accumulator);
            }
            return merged == null ? createAccumulator() : merged;
        }

        @Override
        public ProfileCells extractOutput(final ProfileCells accumulator) {
            return accumulator;
        }

        @Override
        public Coder<ProfileCells> getAccumulatorCoder(final CoderRegistry registry, final Coder<Row> inputCoder) {
            return SerializableCoder.of(ProfileCells.class);
        }

        @Override
        public Coder<ProfileCells> getDefaultOutputCoder(final CoderRegistry registry, final Coder<Row> inputCoder) {
            return SerializableCoder.of(ProfileCells.class);
        }
    }
}
