package com.mercari.solution.util.pipeline.evaluation;

import com.mercari.solution.util.pipeline.feature.FeatureValues;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Bounded accumulator of one metrics key (split × prediction set × slice value): the weighted sums of the
 * per-unit metrics plus, per bootstrap replicate, the same sums under the replicate's Poisson weights. The
 * same shape carries the run's bookkeeping (row counts, per-split unit counts and time range) under the
 * {@link #SEP}-prefixed keys.
 *
 * <p>The replicate sums are expanded lazily: a unit's contribution enters as its slots plus its bootstrap key
 * ({@link #contribute}), and the {@code samples × 7} replicate sums are computed ({@link #expand}) only when
 * the pending contributions exceed {@link Fn#PENDING_MAX} or the output is extracted. A partial accumulator
 * of a few units — what a runner that hands the align step one unit per bundle (the DirectRunner) or one
 * element per accumulator (a lifted combine's first step) encodes — is therefore a few dozen bytes per
 * contribution instead of 7 × samples doubles, on every runner, while an accumulator that has seen many
 * units is the expanded form as before. The Poisson draws are a pure function of (seed, key), so the
 * expansion point does not affect the result.
 */
public final class MetricAccumulator implements Serializable {

    public static final int SLOTS = 9;
    public static final int N_UNITS = 0, N_ROWS = 1, W = 2, WY = 3, LOG = 4, LOG_BASE = 5, HIT = 6, BRIER = 7, UTILITY = 8;
    /** replicate layout: the seven weighted sums W .. UTILITY */
    public static final int BOOT_SLOTS = 7;
    public static final int BOOT_FIRST = W;

    /** separator inside composite keys; a key starting with it is a bookkeeping key, never a metrics cell */
    public static final String SEP = "\u0001";
    /** bookkeeping keys and their slots (the same array, read differently) */
    public static final String ROWS_KEY = SEP + "rows";
    public static final String SPLIT_KEY_PREFIX = SEP + "split" + SEP;
    /** integrity counters (slot 0 = units): a declared slice / a discovery dimension the rows of a unit disagree on, by index */
    public static final String SLICE_VARIES_KEY_PREFIX = SEP + "sliceVaries" + SEP;
    public static final String DIMENSION_VARIES_KEY_PREFIX = SEP + "dimensionVaries" + SEP;
    public static final int ROWS_IN = 0, ROWS_INVALID = 1, ROWS_UNASSIGNED = 2, UNITS_SKIPPED = 3, UNITS = 4, ROWS = 5, ROWS_DUPLICATE = 6;

    final double[] total = new double[SLOTS];
    double[] boot;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    /** contributions whose replicate sums are not expanded yet: the slots and the bootstrap key, pairwise */
    private List<double[]> pendingSlots = new ArrayList<>();
    private List<String> pendingKeys = new ArrayList<>();

    public MetricAccumulator() {
        this.boot = new double[0];
    }

    public double[] getTotal() {
        return total;
    }

    /** The replicate sums: {@code boot[b * BOOT_SLOTS + (slot - BOOT_FIRST)]} (after {@link #expand}). */
    public double[] getBoot() {
        return boot;
    }

    public int samples() {
        return boot.length / BOOT_SLOTS;
    }

    public int pending() {
        return pendingKeys.size();
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

    /**
     * Adds one unit's contribution: the slots into the totals now, and — with a bootstrap key — the replicate
     * sums later ({@link #expand}); a null key (no bootstrap, or a bookkeeping / discovery contribution) has none.
     */
    public void contribute(final double[] slots, final String bootKey) {
        add(slots);
        if (bootKey != null) {
            pendingSlots.add(slots.clone());
            pendingKeys.add(bootKey);
        }
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

    /** Expands every pending contribution into the replicate sums under its Poisson weights ({@code samples} draws from {@code seed} and the key). */
    public void expand(final long seed, final int samples) {
        if (pendingKeys.isEmpty()) return;
        if (samples > 0) {
            for (int i = 0; i < pendingKeys.size(); i++) addReplicates(pendingSlots.get(i), poissonWeights(seed, pendingKeys.get(i), samples));
        }
        pendingSlots = new ArrayList<>();
        pendingKeys = new ArrayList<>();
    }

    /** Poisson(1) replicate weights of a resampling unit: a pure function of (seed, key). */
    public static double[] poissonWeights(final long seed, final String key, final int samples) {
        final double[] w = new double[samples];
        if (samples == 0) return w;
        final SplittableRandom rng = FeatureValues.seededRandom(seed, key + SEP + "bootstrap");
        final double limit = Math.exp(-1d);
        for (int b = 0; b < samples; b++) {
            int c = 0;
            double p = 1d;
            do {
                c++;
                p *= rng.nextDouble();
            } while (p > limit);
            w[b] = c - 1;
        }
        return w;
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
        pendingSlots.addAll(other.pendingSlots);
        pendingKeys.addAll(other.pendingKeys);
        if (other.minTime < minTime) minTime = other.minTime;
        if (other.maxTime > maxTime) maxTime = other.maxTime;
        return this;
    }

    public static final Coder<MetricAccumulator> CODER = new AccumulatorCoder();

    private static class AccumulatorCoder extends AtomicCoder<MetricAccumulator> {
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();
        private static final VarLongCoder LONG = VarLongCoder.of();
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();

        @Override
        public void encode(final MetricAccumulator value, final OutputStream out) throws CoderException, IOException {
            for (final double v : value.total) DOUBLE.encode(v, out);
            INT.encode(value.boot.length, out);
            for (final double v : value.boot) DOUBLE.encode(v, out);
            LONG.encode(value.minTime, out);
            LONG.encode(value.maxTime, out);
            INT.encode(value.pendingKeys.size(), out);
            for (int i = 0; i < value.pendingKeys.size(); i++) {
                STRING.encode(value.pendingKeys.get(i), out);
                for (final double v : value.pendingSlots.get(i)) DOUBLE.encode(v, out);
            }
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
            final int p = INT.decode(in);
            for (int i = 0; i < p; i++) {
                a.pendingKeys.add(STRING.decode(in));
                final double[] slots = new double[SLOTS];
                for (int s = 0; s < SLOTS; s++) slots[s] = DOUBLE.decode(in);
                a.pendingSlots.add(slots);
            }
            return a;
        }
    }

    /**
     * The Combine of the accumulators (input = accumulator = output). Pending contributions are expanded once
     * they exceed {@link #PENDING_MAX} in a merge, and always on extraction, so a merged accumulator's size is
     * bounded by the expanded form plus {@code PENDING_MAX} contributions.
     */
    public static class Fn extends Combine.CombineFn<MetricAccumulator, MetricAccumulator, MetricAccumulator> {
        /** the expanded form (7 × samples doubles) is the break-even of about this many pending contributions at 1000 samples */
        public static final int PENDING_MAX = 32;

        private final long seed;
        private final int samples;

        public Fn(final long seed, final int samples) {
            this.seed = seed;
            this.samples = samples;
        }

        @Override
        public MetricAccumulator createAccumulator() {
            return new MetricAccumulator();
        }

        @Override
        public MetricAccumulator addInput(final MetricAccumulator accumulator, final MetricAccumulator input) {
            return bounded(accumulator.merge(input));
        }

        @Override
        public MetricAccumulator mergeAccumulators(final Iterable<MetricAccumulator> accumulators) {
            final MetricAccumulator merged = new MetricAccumulator();
            for (final MetricAccumulator a : accumulators) bounded(merged.merge(a));
            return merged;
        }

        @Override
        public MetricAccumulator extractOutput(final MetricAccumulator accumulator) {
            accumulator.expand(seed, samples);
            return accumulator;
        }

        private MetricAccumulator bounded(final MetricAccumulator accumulator) {
            if (accumulator.pending() > PENDING_MAX) accumulator.expand(seed, samples);
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
