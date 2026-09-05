package com.mercari.solution.util.pipeline.screen;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Bounded accumulator of one screened column (candidate x transform): the score-test sums over the whole
 * window plus the same sums per period bucket. The same shape carries the run's bookkeeping under
 * {@link #BOOKKEEPING_KEY}. Combined by {@link Fn} (input = accumulator = output).
 */
public final class ScoreAccumulator implements Serializable {

    public static final int SLOTS = 9;
    /** grouped family: sum of weighted group scores */
    public static final int S = 0;
    /** grouped family: sum of weighted group information */
    public static final int H = 1;
    /** rows whose transformed value is finite */
    public static final int N_OBS = 2;
    /** binomial family moment sums (see {@link ScreenReport#stats}) */
    public static final int C1 = 3, C2 = 4, C3 = 5, C4 = 6, C5 = 7;
    /** gaussian family: Σ w r² (the residual variance around the baseline / the label variance) */
    public static final int C6 = 8;

    /** key of the bookkeeping accumulator (never a column index) */
    public static final int BOOKKEEPING_KEY = -1;
    public static final int ROWS_IN = 0, ROWS_TIME_FILTERED = 1, ROWS_INVALID = 2, UNITS_SCORED = 3, UNITS_SKIPPED = 4, ROWS_SCORED = 5;

    final double[] total = new double[SLOTS];
    final TreeMap<String, double[]> periods = new TreeMap<>();
    long maxTime = Long.MIN_VALUE;
    long minTime = Long.MAX_VALUE;

    public ScoreAccumulator() {}

    public double[] getTotal() {
        return total;
    }

    public Map<String, double[]> getPeriods() {
        return periods;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public long getMinTime() {
        return minTime;
    }

    /** Adds one contribution to the total and, when {@code period} is non-null, to that period. */
    public ScoreAccumulator add(final String period, final double[] contribution) {
        for (int i = 0; i < SLOTS; i++) total[i] += contribution[i];
        if (period != null) {
            final double[] slot = periods.computeIfAbsent(period, k -> new double[SLOTS]);
            for (int i = 0; i < SLOTS; i++) slot[i] += contribution[i];
        }
        return this;
    }

    public ScoreAccumulator time(final long millis) {
        if (millis > maxTime) maxTime = millis;
        if (millis < minTime) minTime = millis;
        return this;
    }

    public ScoreAccumulator merge(final ScoreAccumulator other) {
        for (int i = 0; i < SLOTS; i++) total[i] += other.total[i];
        for (final Map.Entry<String, double[]> e : other.periods.entrySet()) {
            final double[] slot = periods.computeIfAbsent(e.getKey(), k -> new double[SLOTS]);
            for (int i = 0; i < SLOTS; i++) slot[i] += e.getValue()[i];
        }
        if (other.maxTime > maxTime) maxTime = other.maxTime;
        if (other.minTime < minTime) minTime = other.minTime;
        return this;
    }

    public static final Coder<ScoreAccumulator> CODER = new AccumulatorCoder();

    private static class AccumulatorCoder extends AtomicCoder<ScoreAccumulator> {
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();
        private static final VarLongCoder LONG = VarLongCoder.of();
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();

        @Override
        public void encode(final ScoreAccumulator value, final OutputStream out) throws CoderException, IOException {
            for (int i = 0; i < SLOTS; i++) DOUBLE.encode(value.total[i], out);
            INT.encode(value.periods.size(), out);
            for (final Map.Entry<String, double[]> e : value.periods.entrySet()) {
                STRING.encode(e.getKey(), out);
                for (int i = 0; i < SLOTS; i++) DOUBLE.encode(e.getValue()[i], out);
            }
            LONG.encode(value.maxTime, out);
            LONG.encode(value.minTime, out);
        }

        @Override
        public ScoreAccumulator decode(final InputStream in) throws CoderException, IOException {
            final ScoreAccumulator acc = new ScoreAccumulator();
            for (int i = 0; i < SLOTS; i++) acc.total[i] = DOUBLE.decode(in);
            final int n = INT.decode(in);
            for (int p = 0; p < n; p++) {
                final String key = STRING.decode(in);
                final double[] slot = new double[SLOTS];
                for (int i = 0; i < SLOTS; i++) slot[i] = DOUBLE.decode(in);
                acc.periods.put(key, slot);
            }
            acc.maxTime = LONG.decode(in);
            acc.minTime = LONG.decode(in);
            return acc;
        }
    }

    /** Combine function: inputs are partial accumulators produced by the scorer. */
    public static class Fn extends Combine.CombineFn<ScoreAccumulator, ScoreAccumulator, ScoreAccumulator> {
        @Override
        public ScoreAccumulator createAccumulator() {
            return new ScoreAccumulator();
        }

        @Override
        public ScoreAccumulator addInput(final ScoreAccumulator accumulator, final ScoreAccumulator input) {
            return accumulator.merge(input);
        }

        @Override
        public ScoreAccumulator mergeAccumulators(final Iterable<ScoreAccumulator> accumulators) {
            final ScoreAccumulator merged = new ScoreAccumulator();
            for (final ScoreAccumulator a : accumulators) merged.merge(a);
            return merged;
        }

        @Override
        public ScoreAccumulator extractOutput(final ScoreAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public Coder<ScoreAccumulator> getAccumulatorCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<ScoreAccumulator> inputCoder) {
            return CODER;
        }

        @Override
        public Coder<ScoreAccumulator> getDefaultOutputCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<ScoreAccumulator> inputCoder) {
            return CODER;
        }
    }
}
