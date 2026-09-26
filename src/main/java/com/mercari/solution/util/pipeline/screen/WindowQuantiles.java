package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.pipeline.glm.SketchAccumulator;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * The window's quantile sketches, one per candidate column (DSL doc §6): the reference for the {@code rank}
 * and {@code absdev} transforms when the rows are independent (no {@code group}), where "within the unit" would
 * be a single row. Built by one pass over the prepared rows before the score pass (a KLL sketch per column,
 * {@link SketchAccumulator#K}: rank error about 0.8%), combined globally and read as a singleton side input.
 * The same sketches give the value-bin edges of §6.1. An empty instance (no column, or every sketch empty) is
 * the Combine's identity. The KLL compaction is randomised, so beyond k values per column a re-run can move a
 * candidate's rank / absdev within the rank error (the placebo columns stay exactly reproducible).
 */
public final class WindowQuantiles implements Serializable {

    private SketchAccumulator[] sketches;

    public WindowQuantiles(final int columns) {
        this.sketches = emptySketches(columns);
    }

    private static SketchAccumulator[] emptySketches(final int columns) {
        final SketchAccumulator[] sketches = new SketchAccumulator[columns];
        for (int i = 0; i < columns; i++) sketches[i] = new SketchAccumulator();
        return sketches;
    }

    private WindowQuantiles(final SketchAccumulator[] sketches) {
        this.sketches = sketches;
    }

    public int columns() {
        return sketches.length;
    }

    /** Feeds one row's candidate values (non-finite values are skipped). */
    public void update(final double[] x) {
        for (int c = 0; c < sketches.length && c < x.length; c++) sketches[c].update(x[c]);
    }

    public boolean isEmpty() {
        for (final SketchAccumulator s : sketches) if (!s.isEmpty()) return false;
        return true;
    }

    /** Values fed for column {@code c}. */
    public long count(final int c) {
        return sketches[c].count();
    }

    /**
     * Mid-rank of {@code v} in column {@code c} as a fraction of the window's finite values, in (0, 1); NaN when v
     * is not finite or the window has no value (the Combine's column-less default of an empty window included).
     */
    public double rank(final int c, final double v) {
        return c < sketches.length ? sketches[c].rank(v) : Double.NaN;
    }

    /** The window median of column {@code c} (the type-7 median below k values; NaN without a value). */
    public double median(final int c) {
        return c < sketches.length ? sketches[c].median() : Double.NaN;
    }

    /** The smallest / largest finite value of column {@code c} (NaN without a value). */
    public double min(final int c) {
        return sketches[c].isEmpty() ? Double.NaN : sketches[c].min();
    }

    public double max(final int c) {
        return sketches[c].isEmpty() ? Double.NaN : sketches[c].max();
    }

    /** The value of column {@code c} at normalized rank {@code q}. */
    public double quantile(final int c, final double q) {
        return sketches[c].quantile(q);
    }

    /** The {@code bins − 1} interior edges of column {@code c} at ranks i / bins. */
    public double[] edges(final int c, final int bins) {
        return sketches[c].edges(bins);
    }

    /**
     * Merges another window's sketches in (column by column); a column-less side adopts copies of the other's
     * columns (an empty sketch copies what it merges), so {@code other} — a Combine input — is never aliased.
     */
    public WindowQuantiles merge(final WindowQuantiles other) {
        if (other.sketches.length == 0) return this;
        if (sketches.length == 0) sketches = emptySketches(other.sketches.length);
        if (sketches.length != other.sketches.length) {
            throw new IllegalStateException("window quantiles of " + sketches.length + " and " + other.sketches.length + " columns cannot merge");
        }
        for (int c = 0; c < sketches.length; c++) sketches[c].merge(other.sketches[c]);
        return this;
    }

    public static final Coder<WindowQuantiles> CODER = new QuantilesCoder();

    private static class QuantilesCoder extends AtomicCoder<WindowQuantiles> {
        private static final VarIntCoder INT = VarIntCoder.of();

        @Override
        public void encode(final WindowQuantiles value, final OutputStream out) throws CoderException, IOException {
            INT.encode(value.sketches.length, out);
            for (final SketchAccumulator s : value.sketches) SketchAccumulator.CODER.encode(s, out);
        }

        @Override
        public WindowQuantiles decode(final InputStream in) throws CoderException, IOException {
            final SketchAccumulator[] sketches = new SketchAccumulator[INT.decode(in)];
            for (int i = 0; i < sketches.length; i++) sketches[i] = SketchAccumulator.CODER.decode(in);
            return new WindowQuantiles(sketches);
        }
    }

    /** Input = accumulator = output: the pre-pass's per-bundle sketches are merged globally. */
    public static class Fn extends Combine.CombineFn<WindowQuantiles, WindowQuantiles, WindowQuantiles> {
        @Override
        public WindowQuantiles createAccumulator() {
            return new WindowQuantiles(0);
        }

        @Override
        public WindowQuantiles addInput(final WindowQuantiles accumulator, final WindowQuantiles input) {
            return accumulator.merge(input);
        }

        @Override
        public WindowQuantiles mergeAccumulators(final Iterable<WindowQuantiles> accumulators) {
            WindowQuantiles merged = null;
            for (final WindowQuantiles a : accumulators) {
                if (a.sketches.length == 0) continue;
                merged = merged == null ? a : merged.merge(a);
            }
            return merged == null ? new WindowQuantiles(0) : merged;
        }

        @Override
        public WindowQuantiles extractOutput(final WindowQuantiles accumulator) {
            return accumulator;
        }

        @Override
        public Coder<WindowQuantiles> getAccumulatorCoder(final CoderRegistry registry, final Coder<WindowQuantiles> inputCoder) {
            return CODER;
        }

        @Override
        public Coder<WindowQuantiles> getDefaultOutputCoder(final CoderRegistry registry, final Coder<WindowQuantiles> inputCoder) {
            return CODER;
        }
    }
}
