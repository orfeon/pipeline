package com.mercari.solution.util.pipeline.process;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * The mergeable counters shared by every aggregated process output (edges, nodes, variants, handovers,
 * conformance): occurrence frequency, distinct-case count, start / end counts, violation count and a
 * duration distribution (count / sum / min / max plus a KLL quantile sketch for the median and p95). One case
 * contributes one partial per key, so the combine is merge-only.
 */
public final class ProcessStats implements Serializable {

    static final int KLL_K = 200;

    public long frequency;
    public long cases;
    public long starts;
    public long ends;
    public long applicable;
    public long violations;

    public long durationCount;
    public double durationSum;
    public double durationMin = Double.POSITIVE_INFINITY;
    public double durationMax = Double.NEGATIVE_INFINITY;
    private transient KllDoublesSketch kll;

    public ProcessStats() {}

    public void addDuration(final double value) {
        durationCount++;
        durationSum += value;
        if (value < durationMin) durationMin = value;
        if (value > durationMax) durationMax = value;
        if (kll == null) kll = KllDoublesSketch.newHeapInstance(KLL_K);
        kll.update(value);
    }

    public ProcessStats merge(final ProcessStats other) {
        frequency += other.frequency;
        cases += other.cases;
        starts += other.starts;
        ends += other.ends;
        applicable += other.applicable;
        violations += other.violations;
        durationCount += other.durationCount;
        durationSum += other.durationSum;
        if (other.durationMin < durationMin) durationMin = other.durationMin;
        if (other.durationMax > durationMax) durationMax = other.durationMax;
        if (other.kll != null) {
            if (kll == null) kll = KllDoublesSketch.newHeapInstance(KLL_K);
            kll.merge(other.kll);
        }
        return this;
    }

    public boolean hasDurations() {
        return durationCount > 0;
    }

    public Double durationMean() {
        return durationCount == 0 ? null : durationSum / durationCount;
    }

    public Double durationMinOrNull() {
        return durationCount == 0 ? null : durationMin;
    }

    public Double durationMaxOrNull() {
        return durationCount == 0 ? null : durationMax;
    }

    /**
     * Quantiles of the durations at the given ranks (null entries when there are none). Queried on a copy: a KLL
     * query sorts its base level in place, which would count as mutating the combined value downstream.
     */
    public Double[] durationQuantiles(final double... ranks) {
        final Double[] result = new Double[ranks.length];
        if (kll == null || kll.isEmpty()) return result;
        final KllDoublesSketch copy = KllDoublesSketch.heapify(Memory.wrap(kll.toByteArray()));
        for (int i = 0; i < ranks.length; i++) result[i] = copy.getQuantile(ranks[i]);
        return result;
    }

    /** Merge-only combine (a case emits one partial per key). */
    public static final class Fn extends Combine.CombineFn<ProcessStats, ProcessStats, ProcessStats> {
        @Override
        public ProcessStats createAccumulator() {
            return new ProcessStats();
        }

        @Override
        public ProcessStats addInput(final ProcessStats accumulator, final ProcessStats input) {
            return accumulator.merge(input);
        }

        @Override
        public ProcessStats mergeAccumulators(final Iterable<ProcessStats> accumulators) {
            final ProcessStats merged = new ProcessStats();
            for (final ProcessStats a : accumulators) merged.merge(a);
            return merged;
        }

        @Override
        public ProcessStats extractOutput(final ProcessStats accumulator) {
            return accumulator;
        }

        @Override
        public Coder<ProcessStats> getAccumulatorCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<ProcessStats> inputCoder) {
            return StatsCoder.of();
        }

        @Override
        public Coder<ProcessStats> getDefaultOutputCoder(final org.apache.beam.sdk.coders.CoderRegistry registry, final Coder<ProcessStats> inputCoder) {
            return StatsCoder.of();
        }
    }

    public static final class StatsCoder extends AtomicCoder<ProcessStats> {
        private static final StatsCoder INSTANCE = new StatsCoder();
        private static final VarLongCoder LONG = VarLongCoder.of();
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final ByteArrayCoder BYTES = ByteArrayCoder.of();

        public static StatsCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(final ProcessStats value, final OutputStream out) throws CoderException, IOException {
            LONG.encode(value.frequency, out);
            LONG.encode(value.cases, out);
            LONG.encode(value.starts, out);
            LONG.encode(value.ends, out);
            LONG.encode(value.applicable, out);
            LONG.encode(value.violations, out);
            LONG.encode(value.durationCount, out);
            DOUBLE.encode(value.durationSum, out);
            DOUBLE.encode(value.durationMin, out);
            DOUBLE.encode(value.durationMax, out);
            BYTES.encode(value.kll == null ? new byte[0] : value.kll.toByteArray(), out);
        }

        @Override
        public ProcessStats decode(final InputStream in) throws CoderException, IOException {
            final ProcessStats value = new ProcessStats();
            value.frequency = LONG.decode(in);
            value.cases = LONG.decode(in);
            value.starts = LONG.decode(in);
            value.ends = LONG.decode(in);
            value.applicable = LONG.decode(in);
            value.violations = LONG.decode(in);
            value.durationCount = LONG.decode(in);
            value.durationSum = DOUBLE.decode(in);
            value.durationMin = DOUBLE.decode(in);
            value.durationMax = DOUBLE.decode(in);
            final byte[] bytes = BYTES.decode(in);
            value.kll = bytes.length == 0 ? null : KllDoublesSketch.heapify(Memory.wrap(bytes));
            return value;
        }
    }
}
