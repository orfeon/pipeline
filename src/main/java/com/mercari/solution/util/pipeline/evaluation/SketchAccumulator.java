package com.mercari.solution.util.pipeline.evaluation;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A mergeable KLL quantile sketch of one value stream (the quantile-binned calibration tables: one per
 * split × prediction set × table). Serialised as the sketch's own bytes; an empty accumulator is the identity.
 */
public final class SketchAccumulator implements Serializable {

    /** sketch parameter: rank error about 0.8% (the bin boundaries' approximation) */
    public static final int K = 400;

    private transient KllDoublesSketch sketch;

    public SketchAccumulator() {
        this.sketch = KllDoublesSketch.newHeapInstance(K);
    }

    private SketchAccumulator(final KllDoublesSketch sketch) {
        this.sketch = sketch;
    }

    public void update(final double v) {
        if (Double.isFinite(v)) sketch.update(v);
    }

    public boolean isEmpty() {
        return sketch.isEmpty();
    }

    public long count() {
        return sketch.getN();
    }

    public double min() {
        return sketch.getMinItem();
    }

    public double max() {
        return sketch.getMaxItem();
    }

    /** The {@code bins − 1} interior boundaries at ranks i / bins (inclusive search). */
    public double[] edges(final int bins) {
        final double[] edges = new double[bins - 1];
        for (int i = 1; i < bins; i++) edges[i - 1] = sketch.getQuantile((double) i / bins, QuantileSearchCriteria.INCLUSIVE);
        return edges;
    }

    public SketchAccumulator merge(final SketchAccumulator other) {
        if (!other.sketch.isEmpty()) sketch.merge(other.sketch);
        return this;
    }

    private void writeObject(final java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        final byte[] bytes = sketch.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private void readObject(final java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        final byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        sketch = KllDoublesSketch.heapify(Memory.wrap(bytes));
    }

    public static final Coder<SketchAccumulator> CODER = new SketchCoder();

    private static class SketchCoder extends AtomicCoder<SketchAccumulator> {
        private static final ByteArrayCoder BYTES = ByteArrayCoder.of();

        @Override
        public void encode(final SketchAccumulator value, final OutputStream out) throws CoderException, IOException {
            BYTES.encode(value.sketch.toByteArray(), out);
        }

        @Override
        public SketchAccumulator decode(final InputStream in) throws CoderException, IOException {
            return new SketchAccumulator(KllDoublesSketch.heapify(Memory.wrap(BYTES.decode(in))));
        }
    }

    public static class Fn extends Combine.CombineFn<SketchAccumulator, SketchAccumulator, SketchAccumulator> {
        @Override
        public SketchAccumulator createAccumulator() {
            return new SketchAccumulator();
        }

        @Override
        public SketchAccumulator addInput(final SketchAccumulator accumulator, final SketchAccumulator input) {
            return accumulator.merge(input);
        }

        @Override
        public SketchAccumulator mergeAccumulators(final Iterable<SketchAccumulator> accumulators) {
            final SketchAccumulator merged = new SketchAccumulator();
            for (final SketchAccumulator a : accumulators) merged.merge(a);
            return merged;
        }

        @Override
        public SketchAccumulator extractOutput(final SketchAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public Coder<SketchAccumulator> getAccumulatorCoder(final CoderRegistry registry, final Coder<SketchAccumulator> inputCoder) {
            return CODER;
        }

        @Override
        public Coder<SketchAccumulator> getDefaultOutputCoder(final CoderRegistry registry, final Coder<SketchAccumulator> inputCoder) {
            return CODER;
        }
    }
}
