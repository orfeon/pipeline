package com.mercari.solution.util.pipeline.screen;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Element-wise sum of fixed-length vectors (the conditioning passes: column moments, the Newton evaluation
 * {@code [n, ll, g, G]}, the partial-test sums {@code [s, b, a]}). An empty accumulator (length 0) is the
 * identity, so a pass that emits nothing yields an empty result under {@code Combine.globally}.
 */
public final class VectorAccumulator implements Serializable {

    private double[] values;

    public VectorAccumulator() {
        this.values = new double[0];
    }

    public VectorAccumulator(final double[] values) {
        this.values = values;
    }

    public double[] getValues() {
        return values;
    }

    public boolean isEmpty() {
        return values.length == 0;
    }

    public VectorAccumulator add(final double[] other) {
        if (other.length == 0) return this;
        if (values.length == 0) {
            values = Arrays.copyOf(other, other.length);
            return this;
        }
        if (values.length != other.length) throw new IllegalStateException("vector length mismatch: " + values.length + " vs " + other.length);
        for (int i = 0; i < values.length; i++) values[i] += other[i];
        return this;
    }

    public VectorAccumulator merge(final VectorAccumulator other) {
        return add(other.values);
    }

    public static final Coder<VectorAccumulator> CODER = new VectorCoder();

    private static class VectorCoder extends AtomicCoder<VectorAccumulator> {
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();

        @Override
        public void encode(final VectorAccumulator value, final OutputStream out) throws CoderException, IOException {
            INT.encode(value.values.length, out);
            for (final double v : value.values) DOUBLE.encode(v, out);
        }

        @Override
        public VectorAccumulator decode(final InputStream in) throws CoderException, IOException {
            final int n = INT.decode(in);
            final double[] values = new double[n];
            for (int i = 0; i < n; i++) values[i] = DOUBLE.decode(in);
            return new VectorAccumulator(values);
        }
    }

    public static class Fn extends Combine.CombineFn<VectorAccumulator, VectorAccumulator, VectorAccumulator> {
        @Override
        public VectorAccumulator createAccumulator() {
            return new VectorAccumulator();
        }

        @Override
        public VectorAccumulator addInput(final VectorAccumulator accumulator, final VectorAccumulator input) {
            return accumulator.merge(input);
        }

        @Override
        public VectorAccumulator mergeAccumulators(final Iterable<VectorAccumulator> accumulators) {
            final VectorAccumulator merged = new VectorAccumulator();
            for (final VectorAccumulator a : accumulators) merged.merge(a);
            return merged;
        }

        @Override
        public VectorAccumulator extractOutput(final VectorAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public Coder<VectorAccumulator> getAccumulatorCoder(final CoderRegistry registry, final Coder<VectorAccumulator> inputCoder) {
            return CODER;
        }

        @Override
        public Coder<VectorAccumulator> getDefaultOutputCoder(final CoderRegistry registry, final Coder<VectorAccumulator> inputCoder) {
            return CODER;
        }
    }
}
