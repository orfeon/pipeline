package com.mercari.solution.util.pipeline.glm;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.DoublesSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A mergeable KLL quantile sketch of one value stream (the evaluation transform's quantile-binned calibration
 * tables and discovery dimensions; the screen transform's window reference of independent-row rank / absdev).
 * Serialised as the sketch's own bytes and the running sum; an empty accumulator is the identity. The sketch's compaction is
 * randomised (datasketches' unseeded generator), so beyond k values a re-run can differ within the rank error.
 */
public final class SketchAccumulator implements Serializable {

    /** default sketch parameter: rank error about 0.8% (the bin boundaries' approximation) */
    public static final int K = 400;

    private transient KllDoublesSketch sketch;
    /** the running sum of the values fed (the mean is read from it, not from the approximate quantiles) */
    private double sum;
    /**
     * The sketch's sorted view, built once on the first read and immutable: a sketch shared through a side input
     * is read from several bundles at once, and the sketch's own lazily cached view must not be built twice
     * concurrently (the concurrent build corrupts the sort). Reset by {@link #update} / {@link #merge}.
     */
    private transient volatile DoublesSortedView view;

    public SketchAccumulator() {
        this(K);
    }

    /** @param k the KLL parameter: a larger k gives finer quantiles (rank error ≈ 1 / k-ish) for a larger sketch */
    public SketchAccumulator(final int k) {
        this.sketch = KllDoublesSketch.newHeapInstance(k);
    }

    public int k() {
        return sketch.getK();
    }

    private SketchAccumulator(final KllDoublesSketch sketch) {
        this.sketch = sketch;
    }

    public void update(final double v) {
        if (Double.isFinite(v)) {
            sketch.update(v);
            sum += v;
            view = null;
        }
    }

    /** The immutable sorted view (built once under the accumulator's lock; the caller checks non-emptiness). */
    private DoublesSortedView view() {
        DoublesSortedView v = view;
        if (v == null) {
            synchronized (this) {
                v = view;
                if (v == null) view = v = sketch.getSortedView();
            }
        }
        return v;
    }

    public boolean isEmpty() {
        return sketch.isEmpty();
    }

    public double min() {
        return sketch.getMinItem();
    }

    public double max() {
        return sketch.getMaxItem();
    }

    /** The number of values fed in. */
    public long count() {
        return sketch.getN();
    }

    /**
     * The mean of the values fed in, from the running sum (not the sketch's approximation), clamped to the exact
     * min / max: a constant stream's mean is its value exactly rather than the sum's rounding residue (0.1 fed ten
     * times sums to 0.9999999999999999), and an overflowing sum stays within the values. NaN on an empty sketch.
     */
    public double mean() {
        if (sketch.isEmpty()) return Double.NaN;
        final double mean = sum / sketch.getN();
        final double min = sketch.getMinItem(), max = sketch.getMaxItem();
        return mean < min ? min : mean > max ? max : mean;
    }

    /**
     * The mid-rank of {@code v} in the sketched stream as a fraction of the count: (values below v + half the
     * values equal to v, v itself included) / n — the mean of the exclusive and inclusive normalized ranks, in
     * (0, 1). NaN for a non-finite value or an empty sketch.
     */
    public double rank(final double v) {
        if (!Double.isFinite(v) || sketch.isEmpty()) return Double.NaN;
        final DoublesSortedView sv = view();
        return 0.5 * (sv.getRank(v, QuantileSearchCriteria.EXCLUSIVE) + sv.getRank(v, QuantileSearchCriteria.INCLUSIVE));
    }

    /** The value at normalized rank {@code q} (inclusive search); NaN on an empty sketch. */
    public double quantile(final double q) {
        if (sketch.isEmpty()) return Double.NaN;
        return view().getQuantile(q, QuantileSearchCriteria.INCLUSIVE);
    }

    /**
     * The median: the mean of the inclusive and exclusive quantiles at 1/2 — below k values the type-7 sample
     * median (the middle value, or the mean of the two middle values of an even count) like
     * {@link StatMath#medianFinite}; NaN on an empty sketch.
     */
    public double median() {
        if (sketch.isEmpty()) return Double.NaN;
        final DoublesSortedView sv = view();
        return 0.5 * (sv.getQuantile(0.5, QuantileSearchCriteria.INCLUSIVE) + sv.getQuantile(0.5, QuantileSearchCriteria.EXCLUSIVE));
    }

    /** The {@code bins − 1} interior boundaries at ranks i / bins (inclusive search). */
    public double[] edges(final int bins) {
        final double[] edges = new double[bins - 1];
        final DoublesSortedView sv = view();
        for (int i = 1; i < bins; i++) edges[i - 1] = sv.getQuantile((double) i / bins, QuantileSearchCriteria.INCLUSIVE);
        return edges;
    }

    /**
     * The median of the values in each bin cut by {@code edges} (bin i = (edge_{i−1}, edge_i], the outer bins open):
     * the quantile at the middle of the bin's inclusive rank interval (rank(edge_{i−1}), rank(edge_i)], so inside its
     * bin whatever the ties — a quantile at (i + 0.5) / bins lands in a neighbouring bin once tied values collapse
     * the edges. An empty bin takes the value at its lower rank. The caller checks non-emptiness.
     */
    public double[] binMedians(final double[] edges) {
        final double[] out = new double[edges.length + 1];
        final DoublesSortedView sv = view();
        double lower = 0d;
        for (int i = 0; i < out.length; i++) {
            final double upper = i < edges.length ? sv.getRank(edges[i], QuantileSearchCriteria.INCLUSIVE) : 1d;
            out[i] = sv.getQuantile(0.5 * (lower + upper), QuantileSearchCriteria.INCLUSIVE);
            lower = upper;
        }
        return out;
    }

    /**
     * Merges another sketch in. A KLL merge keeps this sketch's k, so an empty accumulator (the Combine's
     * identity, created at the default k) adopts the other's sketch instead: a table's declared k survives.
     */
    public SketchAccumulator merge(final SketchAccumulator other) {
        if (other.sketch.isEmpty()) return this;
        if (sketch.isEmpty()) {
            sketch = KllDoublesSketch.heapify(Memory.wrap(other.sketch.toByteArray()));
        } else {
            sketch.merge(other.sketch);
        }
        sum += other.sum;
        view = null;
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
        private static final DoubleCoder DOUBLE = DoubleCoder.of();

        @Override
        public void encode(final SketchAccumulator value, final OutputStream out) throws CoderException, IOException {
            BYTES.encode(value.sketch.toByteArray(), out);
            DOUBLE.encode(value.sum, out);
        }

        @Override
        public SketchAccumulator decode(final InputStream in) throws CoderException, IOException {
            final SketchAccumulator s = new SketchAccumulator(KllDoublesSketch.heapify(Memory.wrap(BYTES.decode(in))));
            s.sum = DOUBLE.decode(in);
            return s;
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

        /** Builds on the first non-empty accumulator (the contract lets a merge modify and return an argument): no copy, and its k. */
        @Override
        public SketchAccumulator mergeAccumulators(final Iterable<SketchAccumulator> accumulators) {
            SketchAccumulator merged = null;
            for (final SketchAccumulator a : accumulators) {
                if (a.isEmpty()) continue;
                if (merged == null) merged = a;
                else merged.merge(a);
            }
            return merged == null ? new SketchAccumulator() : merged;
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
