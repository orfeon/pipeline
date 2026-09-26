package com.mercari.solution.util.pipeline.screen;

import com.mercari.solution.util.pipeline.glm.SketchAccumulator;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The window's quantile sketches, one per sketched x column (DSL doc §6): the reference for the {@code rank}
 * and {@code absdev} transforms when the rows are independent (no {@code group}), where "within the unit" would
 * be a single row, the value edges of the binned test and of a pair's 2-D grid. Built by one pass over the prepared
 * rows before the score pass (a KLL sketch per column, {@link SketchAccumulator#K}: rank error about 0.8%),
 * combined globally and read as a singleton side input. The same pass counts the levels of the categorical
 * candidates (DSL doc §6.2): an exact {@code level → count} map per column, from which the report and the scorers
 * take the {@link Levels} dictionary (the most frequent {@code maxLevels} named, the rest folded). An empty
 * instance (no column, or every sketch empty) is the Combine's identity. The KLL compaction is randomised, so
 * beyond k values per column a re-run can move a candidate's rank / absdev within the rank error (the placebo
 * columns stay exactly reproducible).
 */
public final class WindowQuantiles implements Serializable {

    /** the hard cap on the distinct levels one column may show: beyond it the column is not a categorical candidate */
    public static final int LEVELS_HARD_CAP = 20_000;

    private SketchAccumulator[] sketches;
    /** level → count per categorical column (exact; empty arrays without categoricals) */
    private TreeMap<String, Long>[] levels;

    public WindowQuantiles(final int columns) {
        this(columns, 0);
    }

    private static SketchAccumulator[] emptySketches(final int columns) {
        final SketchAccumulator[] sketches = new SketchAccumulator[columns];
        for (int i = 0; i < columns; i++) sketches[i] = new SketchAccumulator();
        return sketches;
    }

    @SuppressWarnings("unchecked")
    private static TreeMap<String, Long>[] emptyLevels(final int categoricals) {
        final TreeMap<String, Long>[] levels = new TreeMap[categoricals];
        for (int i = 0; i < categoricals; i++) levels[i] = new TreeMap<>();
        return levels;
    }

    public WindowQuantiles(final int columns, final int categoricals) {
        this.sketches = emptySketches(columns);
        this.levels = emptyLevels(categoricals);
    }

    private WindowQuantiles(final SketchAccumulator[] sketches, final TreeMap<String, Long>[] levels) {
        this.sketches = sketches;
        this.levels = levels;
    }

    public int columns() {
        return sketches.length;
    }

    public int categoricals() {
        return levels.length;
    }

    /** Feeds one row's candidate values (non-finite values are skipped). */
    public void update(final double[] x) {
        for (int c = 0; c < sketches.length && c < x.length; c++) sketches[c].update(x[c]);
    }

    /** Counts one row's categorical levels (a column above {@link #LEVELS_HARD_CAP} distinct levels fails the step). */
    public void updateLevels(final String[] cat) {
        if (cat == null) return;
        for (int c = 0; c < levels.length && c < cat.length; c++) {
            levels[c].merge(cat[c], 1L, Long::sum);
            if (levels[c].size() > LEVELS_HARD_CAP) {
                throw new IllegalStateException("categorical column " + c + " shows more than " + LEVELS_HARD_CAP + " distinct levels: not a categorical candidate (narrow categorical.include)");
            }
        }
    }

    /** Feeds one row's values of the given columns only (the others' sketches stay empty; non-finite values are skipped). */
    public void update(final double[] x, final int[] columns) {
        for (final int c : columns) if (c < sketches.length && c < x.length) sketches[c].update(x[c]);
    }

    public boolean isEmpty() {
        for (final SketchAccumulator s : sketches) if (!s.isEmpty()) return false;
        for (final TreeMap<String, Long> l : levels) if (!l.isEmpty()) return false;
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

    /** The window mean of column {@code c} (exact, from the sketch's running sum; NaN without a value). */
    public double mean(final int c) {
        return c < sketches.length ? sketches[c].mean() : Double.NaN;
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
     * A categorical column's level dictionary (DSL doc §6.2): the {@code maxLevels} most frequent levels named, by
     * count then name, at indices 0..n − 1; every other level folds into the last index ({@link ScreenSpec#LEVEL_OTHER},
     * present only when a level was folded). {@code index} maps a level to its slot, {@code frequency} gives each
     * slot's share of the rows (the placebo columns' redraw distribution) and {@code cumulative} its running sum.
     */
    public record Levels(List<String> names, Map<String, Integer> index, double[] frequency, double[] cumulative, boolean folded) {
        public int size() {
            return names.size();
        }

        public int indexOf(final String level) {
            final Integer i = index.get(level);
            return i != null ? i : folded ? names.size() - 1 : -1;
        }

        /** The slot a uniform draw {@code u} in [0, 1) falls in under the frequencies (the last one past a rounding shortfall). */
        public int slot(final double u) {
            int lo = 0, hi = cumulative.length - 1;
            while (lo < hi) {
                final int mid = (lo + hi) >>> 1;
                if (u < cumulative[mid]) hi = mid;
                else lo = mid + 1;
            }
            return lo;
        }
    }

    public Levels levels(final int c, final int maxLevels) {
        final TreeMap<String, Long> counts = levels[c];
        final List<Map.Entry<String, Long>> entries = new ArrayList<>(counts.entrySet());
        entries.sort(Comparator.<Map.Entry<String, Long>>comparingLong(e -> -e.getValue()).thenComparing(Map.Entry::getKey));
        final List<String> names = new ArrayList<>();
        final Map<String, Integer> index = new HashMap<>();
        final List<Double> freq = new ArrayList<>();
        double total = 0;
        for (final Map.Entry<String, Long> e : entries) total += e.getValue();
        double other = 0;
        for (final Map.Entry<String, Long> e : entries) {
            if (names.size() < maxLevels) {
                index.put(e.getKey(), names.size());
                names.add(e.getKey());
                freq.add(total > 0 ? e.getValue() / total : 0d);
            } else {
                other += e.getValue();
            }
        }
        final boolean folded = other > 0;
        if (folded) {
            names.add(ScreenSpec.LEVEL_OTHER);
            freq.add(total > 0 ? other / total : 0d);
        }
        final double[] frequency = new double[freq.size()];
        final double[] cumulative = new double[freq.size()];
        double running = 0;
        for (int i = 0; i < frequency.length; i++) {
            frequency[i] = freq.get(i);
            running += frequency[i];
            cumulative[i] = running;
        }
        return new Levels(names, index, frequency, cumulative, folded);
    }

    /**
     * Merges another window's sketches and level counts in (column by column); a column-less side adopts copies of the
     * other's columns (an empty sketch copies what it merges), so {@code other} — a Combine input — is never aliased.
     */
    public WindowQuantiles merge(final WindowQuantiles other) {
        if (other.sketches.length == 0 && other.levels.length == 0) return this;
        if (sketches.length == 0 && levels.length == 0) {
            sketches = emptySketches(other.sketches.length);
            levels = emptyLevels(other.levels.length);
        }
        if (sketches.length != other.sketches.length || levels.length != other.levels.length) {
            throw new IllegalStateException("window quantiles of " + sketches.length + "/" + levels.length + " and " + other.sketches.length + "/" + other.levels.length + " columns cannot merge");
        }
        for (int c = 0; c < sketches.length; c++) sketches[c].merge(other.sketches[c]);
        for (int c = 0; c < levels.length; c++) {
            for (final Map.Entry<String, Long> e : other.levels[c].entrySet()) levels[c].merge(e.getKey(), e.getValue(), Long::sum);
            if (levels[c].size() > LEVELS_HARD_CAP) {
                throw new IllegalStateException("categorical column " + c + " shows more than " + LEVELS_HARD_CAP + " distinct levels: not a categorical candidate (narrow categorical.include)");
            }
        }
        return this;
    }

    public static final Coder<WindowQuantiles> CODER = new QuantilesCoder();

    private static class QuantilesCoder extends AtomicCoder<WindowQuantiles> {
        private static final VarIntCoder INT = VarIntCoder.of();
        private static final VarLongCoder LONG = VarLongCoder.of();
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();

        @Override
        public void encode(final WindowQuantiles value, final OutputStream out) throws CoderException, IOException {
            INT.encode(value.sketches.length, out);
            for (final SketchAccumulator s : value.sketches) SketchAccumulator.CODER.encode(s, out);
            INT.encode(value.levels.length, out);
            for (final TreeMap<String, Long> l : value.levels) {
                INT.encode(l.size(), out);
                for (final Map.Entry<String, Long> e : l.entrySet()) {
                    STRING.encode(e.getKey(), out);
                    LONG.encode(e.getValue(), out);
                }
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public WindowQuantiles decode(final InputStream in) throws CoderException, IOException {
            final SketchAccumulator[] sketches = new SketchAccumulator[INT.decode(in)];
            for (int i = 0; i < sketches.length; i++) sketches[i] = SketchAccumulator.CODER.decode(in);
            final TreeMap<String, Long>[] levels = new TreeMap[INT.decode(in)];
            for (int c = 0; c < levels.length; c++) {
                levels[c] = new TreeMap<>();
                final int n = INT.decode(in);
                for (int i = 0; i < n; i++) levels[c].put(STRING.decode(in), LONG.decode(in));
            }
            return new WindowQuantiles(sketches, levels);
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
                if (a.sketches.length == 0 && a.levels.length == 0) continue;
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
