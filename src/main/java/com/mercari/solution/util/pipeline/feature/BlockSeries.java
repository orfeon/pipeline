package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

/**
 * The {@link Summary} states of one fitted thing over the time blocks of {@code fit.mode: forward}
 * ({@link ForwardBlocks}): one state per observed block, combined on demand into the state a row may read —
 * the blocks in {@code (usable − windowBlocks, usable]} ({@code windowBlocks ≤ 0}: every block up to
 * {@code usable}). This is the fit-side counterpart of the keyed replay: the same family that accumulates a
 * key's past row by row is merged block by block here, so a statistic written once as a {@code Summary} is
 * available under {@code static} (one block, everything), {@code forward} (a prefix of blocks) and a
 * {@code window} (a range of blocks) without a second implementation — only the monoid law is needed, so a
 * non-invertible family (extrema, sorted values) is served by merging the range's parts instead of a prefix
 * difference.
 *
 * <p>A model that must be solved from the state (an eigendecomposition, a ridge system) is fitted once per
 * <em>change point</em> — every observed block and, under a window, the index at which an observed block
 * leaves the window ({@link #changePoints}) — and a row reads the model of the floor change point of its
 * usable block ({@link #lookup}), exactly as {@link JointFit} does for its per-block solutions.
 *
 * @param <S> the summary state
 */
public final class BlockSeries<S extends Serializable> implements Serializable {

    private final Summary<S> family;
    private final TreeMap<Long, S> parts;

    public BlockSeries(final Summary<S> family, final Map<Long, S> parts) {
        this.family = family;
        this.parts = new TreeMap<>(parts);
    }

    /** The blocks that carried data, in order ({@code minBlocks} counts these). */
    public TreeSet<Long> observed() {
        return new TreeSet<>(parts.keySet());
    }

    public boolean isEmpty() {
        return parts.isEmpty();
    }

    /**
     * The merged state of the blocks a usable block reads — {@code (usable − windowBlocks, usable]}, or every
     * block {@code ≤ usable} when {@code windowBlocks ≤ 0} — or null when no observed block lies in that range.
     */
    public S window(final long usable, final int windowBlocks) {
        final NavigableMap<Long, S> range = windowBlocks > 0
                ? parts.subMap(usable - windowBlocks, false, usable, true)
                : parts.headMap(usable, true);
        if (range.isEmpty()) return null;
        final S out = family.create();
        for (final S part : range.values()) family.merge(out, part);
        return out;
    }

    /** The merged state of every block (what a static artifact holds); null when nothing was observed. */
    public S total() {
        return parts.isEmpty() ? null : window(parts.lastKey(), 0);
    }

    /**
     * The block indices at which the readable state changes: every observed block (a block enters) and, under
     * a window, {@code block + windowBlocks} (the block leaves). One model per change point serves every usable
     * block by floor lookup.
     */
    public TreeSet<Long> changePoints(final int windowBlocks) {
        final TreeSet<Long> points = new TreeSet<>(parts.keySet());
        if (windowBlocks > 0) for (final long block : parts.keySet()) points.add(block + windowBlocks);
        return points;
    }

    /**
     * One model per change point, fitted from the window state at that point ({@code fit} receives the family's
     * empty state when the window is empty there, so it can produce its own "nothing fitted" model).
     *
     * <p>Without a window the readable state is a prefix that grows by exactly one block per change point, so the
     * series merges only the entering block into one running state instead of re-merging the whole prefix: B merges
     * rather than B²/2, which is what makes a long forward fit affordable for a family whose part is large (the
     * co-occurrence counts of {@link Spectral}, the gathered values of {@link QuantileTransform}) — the same
     * prefix-scan {@link VarianceComponents#forwardSeries} does per key. {@code fit} therefore reads the state it is
     * given and keeps nothing: every family's fit copies what it needs into its model.
     */
    public <M> TreeMap<Long, M> models(final int windowBlocks, final Function<S, M> fit) {
        final TreeMap<Long, M> models = new TreeMap<>();
        if (windowBlocks > 0) {
            for (final long at : changePoints(windowBlocks)) {
                final S state = window(at, windowBlocks);
                models.put(at, fit.apply(state == null ? family.create() : state));
            }
            return models;
        }
        final S prefix = family.create();
        for (final Map.Entry<Long, S> part : parts.entrySet()) {
            family.merge(prefix, part.getValue());
            models.put(part.getKey(), fit.apply(prefix));
        }
        return models;
    }

    /**
     * The model a row reads: the floor change point of its usable block, or null when none precedes it or fewer
     * than {@code minBlocks} observed blocks lie at or before the usable block.
     */
    public static <M> M lookup(final TreeMap<Long, M> models, final TreeSet<Long> observed, final long usable, final int minBlocks) {
        if (models == null || observed == null) return null;
        final Map.Entry<Long, M> floor = models.floorEntry(usable);
        if (floor == null || observed.headSet(usable, true).size() < minBlocks) return null;
        return floor.getValue();
    }

}
