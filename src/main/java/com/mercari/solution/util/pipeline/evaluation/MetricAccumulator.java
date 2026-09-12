package com.mercari.solution.util.pipeline.evaluation;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Bounded accumulator of one metrics key (split × prediction set × slice value): the weighted sums of the
 * per-unit metrics plus, per bootstrap replicate, the same sums under the replicate's Poisson weights. The
 * same shape carries the run's bookkeeping (row counts, per-split unit counts and time range) under the
 * {@code }-prefixed keys. Combined by {@link Fn} (input = accumulator = output).
 */
public final class MetricAccumulator implements Serializable {

    public static final int SLOTS = 8;
    public static final int N_UNITS = 0, N_ROWS = 1, W = 2, WY = 3, LOG = 4, LOG_BASE = 5, HIT = 6, BRIER = 7;
    /** replicate layout: the six weighted sums W .. BRIER */
    public static final int BOOT_SLOTS = 6;
    public static final int BOOT_FIRST = W;

    /** bookkeeping keys and their slots (the same array, read differently) */
    public static final String ROWS_KEY = "rows";
    public static final String SPLIT_KEY_PREFIX = "split";
    public static final int ROWS_IN = 0, ROWS_INVALID = 1, ROWS_UNASSIGNED = 2, UNITS_SKIPPED = 3, UNITS = 4, ROWS = 5;

    final double[] total = new double[SLOTS];
    double[] boot;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;

    public MetricAccumulator() {
        this.boot = new double[0];
    }

    MetricAccumulator(final int samples) {
        this.boot = new double[samples * BOOT_SLOTS];
    }

    public double[] getTotal() {
        return total;
    }

    /** The replicate sums: {@code boot[b * BOOT_SLOTS + (slot - BOOT_FIRST)]}. */
    public double[] getBoot() {
        return boot;
    }

    public int samples() {
        return boot.length / BOOT_SLOTS;
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public void add(final double[] slots) {
        for (int i = 0; i < SLOTS; i++) total[i] += slots[i];
    }

    /** Adds one unit's metrics under the replicate weights {@code w[b]} (the unit weight folded into {@code slots}). */
    public void addReplicates(final double[] slots, final double[] w) {
        final int b = w.length;
        if (boot.length == 0) boot = new double[b * BOOT_SLOTS];
        for (int r = 0; r < b; r++) {
            final int base = r * BOOT_SLOTS;
            for (int s = 0; s < BOOT_SLOTS; s++) boot[base + s] += w[r] * slots[BOOT_FIRST + s];
        }
    }

    public void time(final long t) {
        if (t < minTime) minTime = t;
        if (t > maxTime) maxTime = t;
    }

    public MetricAccumulator merge(final MetricAccumulator other) {
        add(other.total);
        if (other.boot.length > 0) {
            if (boot.length == 0) {
                boot = other.boot.clone();
            } else {
                if (boot.length != other.boot.length) throw new IllegalStateException("bootstrap length mismatch: " + boot.length + " vs " + other.boot.length);
                for (int i = 0; i < boot.length; i++) boot[i] += other.boot[i];
            }
        }
        if (other.minTime < minTime) minTime = other.minTime;
        if (other.maxTime > maxTime) maxTime = other.maxTime;
        return this;
    }

    public static final Coder<MetricAccumulator> CODER = new AccumulatorCoder();

    private static class AccumulatorCoder extends AtomicCoder<MetricAccumulator> {
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();
        private static final VarLongCoder LONG = VarLongCoder.of();

        @Override
        public void encode(final MetricAccumulator value, final OutputStream out) throws CoderException, IOException {
            for (final double v : value.total) DOUBLE.encode(v, out);
            INT.encode(value.boot.length, out);
            for (final double v : value.boot) DOUBLE.encode(v, out);
            LONG.encode(value.minTime, out);
            LONG.encode(value.maxTime, out);
        }

        @Override
        public MetricAccumulator decode(final InputStream in) throws CoderException, IOException {
            final MetricAccumulator a = new MetricAccumulator();
            for (int i = 0; i < SLOTS; i++) a.total[i] = DOUBLE.decode(in);
            final int n = INT.decode(in);
            a.boot = new double[n];
            for (int i = 0; i < n; i++) a.boot[i] = DOUBLE.decode(in);
            a.minTime = LONG.decode(in);
            a.maxTime = LONG.decode(in);
            return a;
        }
    }

    public static class Fn extends Combine.CombineFn<MetricAccumulator, MetricAccumulator, MetricAccumulator> {
        @Override
        public MetricAccumulator createAccumulator() {
            return new MetricAccumulator();
        }

        @Override
        public MetricAccumulator addInput(final MetricAccumulator accumulator, final MetricAccumulator input) {
            return accumulator.merge(input);
        }

        @Override
        public MetricAccumulator mergeAccumulators(final Iterable<MetricAccumulator> accumulators) {
            final MetricAccumulator merged = new MetricAccumulator();
            for (final MetricAccumulator a : accumulators) merged.merge(a);
            return merged;
        }

        @Override
        public MetricAccumulator extractOutput(final MetricAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public Coder<MetricAccumulator> getAccumulatorCoder(final CoderRegistry registry, final Coder<MetricAccumulator> inputCoder) {
            return CODER;
        }

        @Override
        public Coder<MetricAccumulator> getDefaultOutputCoder(final CoderRegistry registry, final Coder<MetricAccumulator> inputCoder) {
            return CODER;
        }
    }
}
