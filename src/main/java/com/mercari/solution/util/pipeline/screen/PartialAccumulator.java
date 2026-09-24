package com.mercari.solution.util.pipeline.screen;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * Element-wise sum of one fixed-length vector over the whole window plus the same vector per period bucket:
 * the partial-test sums {@code [s, b, a(k)]} of one column x transform ({@link ConditioningScorer#partial}),
 * the gaussian residual sums, and the fitted model's per-period gradient / Gram sums under
 * {@link ConditioningScorer#FIT_PERIOD_KEY}. Same shape as {@link ScoreAccumulator} with a variable slot
 * count. An empty accumulator (length 0, no period) is the identity of {@link Fn}.
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

    /** Adds one contribution to the total and, when {@code period} is non-null, to that period. */
    public PartialAccumulator add(final String period, final double[] contribution) {
        total = sum(total, contribution);
        if (period != null) periods.merge(period, contribution.clone(), PartialAccumulator::sum);
        return this;
    }

    public PartialAccumulator merge(final PartialAccumulator other) {
        total = sum(total, other.total);
        for (final Map.Entry<String, double[]> e : other.periods.entrySet()) periods.merge(e.getKey(), e.getValue().clone(), PartialAccumulator::sum);
        return this;
    }

    private static double[] sum(final double[] into, final double[] other) {
        if (other.length == 0) return into;
        if (into.length == 0) return Arrays.copyOf(other, other.length);
        if (into.length != other.length) throw new IllegalStateException("vector length mismatch: " + into.length + " vs " + other.length);
        for (int i = 0; i < into.length; i++) into[i] += other[i];
        return into;
    }

    public static final Coder<PartialAccumulator> CODER = new AccumulatorCoder();

    private static class AccumulatorCoder extends AtomicCoder<PartialAccumulator> {
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();

        @Override
        public void encode(final PartialAccumulator value, final OutputStream out) throws CoderException, IOException {
            vector(value.total, out);
            INT.encode(value.periods.size(), out);
            for (final Map.Entry<String, double[]> e : value.periods.entrySet()) {
                STRING.encode(e.getKey(), out);
                vector(e.getValue(), out);
            }
        }

        @Override
        public PartialAccumulator decode(final InputStream in) throws CoderException, IOException {
            final PartialAccumulator acc = new PartialAccumulator(vector(in));
            final int n = INT.decode(in);
            for (int p = 0; p < n; p++) {
                final String key = STRING.decode(in);
                acc.periods.put(key, vector(in));
            }
            return acc;
        }

        private static void vector(final double[] v, final OutputStream out) throws IOException {
            INT.encode(v.length, out);
            for (final double d : v) DOUBLE.encode(d, out);
        }

        private static double[] vector(final InputStream in) throws IOException {
            final int n = INT.decode(in);
            final double[] v = new double[n];
            for (int i = 0; i < n; i++) v[i] = DOUBLE.decode(in);
            return v;
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
