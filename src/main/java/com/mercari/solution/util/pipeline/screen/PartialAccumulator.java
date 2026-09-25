package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.pipeline.glm.VectorAccumulator;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Element-wise sum of one fixed-length vector over the whole window plus the same vector per period bucket:
 * the partial-test sums {@code [s, b, a(k)]} of one column x transform ({@link ConditioningScorer#partial}),
 * the gaussian residual sums, and the fitted model's per-period gradient / Gram sums under
 * {@link ConditioningScorer#FIT_PERIOD_KEY}. Same shape as {@link ScoreAccumulator} with a variable slot
 * count; each vector sums (and is encoded) like a {@link VectorAccumulator}. An empty accumulator (length 0,
 * no period) is the identity of {@link Fn}.
 */
public final class PartialAccumulator implements Serializable {

    private double[] total;
    private final TreeMap<String, double[]> periods = new TreeMap<>();

    public PartialAccumulator() {
        this.total = new double[0];
    }

    public PartialAccumulator(final double[] total) {
        this.total = total;
    }

    public double[] getTotal() {
        return total;
    }

    public Map<String, double[]> getPeriods() {
        return periods;
    }

    public boolean isEmpty() {
        return total.length == 0;
    }

    /**
     * The window vector for in-place sums (a sparse contribution, the binned block's occupied bins, touches only its
     * entries), allocated at {@code length} on first use.
     */
    double[] total(final int length) {
        if (total.length == 0) {
            total = new double[length];
        } else if (total.length != length) {
            throw new IllegalStateException("partial sums of " + total.length + " and " + length + " values cannot merge");
        }
        return total;
    }

    /** Adds one contribution to the total and, when {@code period} is non-null, to that period. */
    public PartialAccumulator add(final String period, final double[] contribution) {
        total = sum(total, contribution);
        if (period != null) addPeriod(period, contribution);
        return this;
    }

    public PartialAccumulator merge(final PartialAccumulator other) {
        total = sum(total, other.total);
        for (final Map.Entry<String, double[]> e : other.periods.entrySet()) addPeriod(e.getKey(), e.getValue());
        return this;
    }

    /** Sums into the period's slot in place; the contribution is copied only when it opens the slot (never aliased). */
    private void addPeriod(final String period, final double[] contribution) {
        periods.compute(period, (key, slot) -> slot == null ? contribution.clone() : sum(slot, contribution));
    }

    /** {@link VectorAccumulator#add}: in place, an empty vector is the identity. */
    private static double[] sum(final double[] into, final double[] other) {
        return new VectorAccumulator(into).add(other).getValues();
    }

    public static final Coder<PartialAccumulator> CODER = new AccumulatorCoder();

    private static class AccumulatorCoder extends AtomicCoder<PartialAccumulator> {
        private static final Coder<VectorAccumulator> VECTOR = VectorAccumulator.CODER;
        private static final VarIntCoder INT = VarIntCoder.of();
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();

        @Override
        public void encode(final PartialAccumulator value, final OutputStream out) throws CoderException, IOException {
            VECTOR.encode(new VectorAccumulator(value.total), out);
            INT.encode(value.periods.size(), out);
            for (final Map.Entry<String, double[]> e : value.periods.entrySet()) {
                STRING.encode(e.getKey(), out);
                VECTOR.encode(new VectorAccumulator(e.getValue()), out);
            }
        }

        @Override
        public PartialAccumulator decode(final InputStream in) throws CoderException, IOException {
            final PartialAccumulator acc = new PartialAccumulator(VECTOR.decode(in).getValues());
            final int n = INT.decode(in);
            for (int p = 0; p < n; p++) {
                final String key = STRING.decode(in);
                acc.periods.put(key, VECTOR.decode(in).getValues());
            }
            return acc;
        }
    }

    /** Combine function: inputs are partial accumulators produced by the partial pass. */
    public static class Fn extends Combine.CombineFn<PartialAccumulator, PartialAccumulator, PartialAccumulator> {
        @Override
        public PartialAccumulator createAccumulator() {
            return new PartialAccumulator();
        }

        @Override
        public PartialAccumulator addInput(final PartialAccumulator accumulator, final PartialAccumulator input) {
            return accumulator.merge(input);
        }

        @Override
        public PartialAccumulator mergeAccumulators(final Iterable<PartialAccumulator> accumulators) {
            final PartialAccumulator merged = new PartialAccumulator();
            for (final PartialAccumulator a : accumulators) merged.merge(a);
            return merged;
        }

        @Override
        public PartialAccumulator extractOutput(final PartialAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public Coder<PartialAccumulator> getAccumulatorCoder(final CoderRegistry registry, final Coder<PartialAccumulator> inputCoder) {
            return CODER;
        }

        @Override
        public Coder<PartialAccumulator> getDefaultOutputCoder(final CoderRegistry registry, final Coder<PartialAccumulator> inputCoder) {
            return CODER;
        }
    }
}
