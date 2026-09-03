package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Exact order statistics over a multiset of doubles that supports insertion AND deletion — the running
 * accumulator behind the {@code quantile} encoding statistic on the incremental replay path (a value is
 * added when its row becomes visible and removed when it leaves a {@code maxAge} window).
 *
 * <p>Sqrt-decomposition: the values are kept sorted in a sequence of sorted blocks of at most
 * {@value #BLOCK} elements, so an insertion or deletion moves at most one block ({@code O(√n)}); a rank
 * query descends a Fenwick tree over the block sizes ({@code O(log #blocks)}, the tree is rebuilt only when
 * a block splits or empties). A plain sorted array would move {@code O(n)} per insertion, which on the
 * global level of a lattice (one key holding every row) turns the linear replay into a quadratic memmove;
 * a sketch would lose determinism and cannot delete. Blocks start small ({@value #INITIAL} values) and
 * double up to {@value #BLOCK} before splitting, so an accumulator with a handful of values — one exists
 * per (column × filter value) — costs a few hundred bytes, not a full block.
 */
public final class OrderStatistics implements Serializable {

    static final int BLOCK = 1024;
    static final int INITIAL = 16;

    private double[][] blocks = new double[4][];
    private int[] sizes = new int[4];
    private int blockCount;
    private int size;
    /** Fenwick tree (1-based) over {@code sizes[0..blockCount)}; null when a structural change invalidated it. */
    private int[] tree;

    public int size() {
        return size;
    }

    public void add(final double v) {
        if (blockCount == 0) {
            blocks[0] = new double[INITIAL];
            blocks[0][0] = v;
            sizes[0] = 1;
            blockCount = 1;
            size = 1;
            tree = null;
            return;
        }
        // the first block whose last element is >= v, else the last block
        int b = blockFor(v);
        if (b == blockCount) b = blockCount - 1;
        double[] block = blocks[b];
        final int n = sizes[b];
        final int at = upperBound(block, n, v);
        if (n == block.length) {
            if (block.length < BLOCK) {
                block = Arrays.copyOf(block, Math.min(BLOCK, block.length * 2));
                blocks[b] = block;
            } else {
                // split: keep the lower half here, move the upper half into a fresh block after it
                final int half = n / 2;
                final double[] upper = new double[BLOCK];
                System.arraycopy(block, half, upper, 0, n - half);
                insertBlock(b + 1, upper, n - half);
                sizes[b] = half;
                if (at > half) {
                    insertAt(b + 1, at - half, v);
                } else {
                    insertAt(b, at, v);
                }
                size++;
                return;
            }
        }
        insertAt(b, at, v);
        size++;
    }

    private void insertAt(final int b, final int at, final double v) {
        final double[] block = blocks[b];
        final int n = sizes[b];
        System.arraycopy(block, at, block, at + 1, n - at);
        block[at] = v;
        sizes[b] = n + 1;
        update(b, 1);
    }

    /** Removes one occurrence of {@code v}; returns false when it is absent. */
    public boolean remove(final double v) {
        final int b = blockFor(v);
        if (b == blockCount) return false;
        final double[] block = blocks[b];
        final int n = sizes[b];
        final int at = lowerBound(block, n, v);
        if (at == n || Double.compare(block[at], v) != 0) return false;
        System.arraycopy(block, at + 1, block, at, n - at - 1);
        if (n == 1) {
            removeBlock(b);
        } else {
            sizes[b] = n - 1;
            update(b, -1);
        }
        size--;
        return true;
    }

    /** The k-th smallest value (0-based). */
    public double select(final int k) {
        if (k < 0 || k >= size) throw new IndexOutOfBoundsException(k + " of " + size);
        final long at = locate(k);
        return blocks[(int) (at >>> 32)][(int) at];
    }

    /**
     * Quantile {@code p} in [0, 1] with linear interpolation between order statistics (R type 7 / numpy
     * default), or null when empty.
     */
    public Double quantile(final double p) {
        if (size == 0) return null;
        final double h = (size - 1) * Math.min(1d, Math.max(0d, p));
        final int lo = (int) Math.floor(h);
        final long at = locate(lo);
        final int b = (int) (at >>> 32), off = (int) at;
        final double x = blocks[b][off];
        final double frac = h - lo;
        if (frac == 0 || lo + 1 >= size) return x;
        // the next order statistic is the neighbour in this block, or the first value of the next block
        final double next = off + 1 < sizes[b] ? blocks[b][off + 1] : blocks[b + 1][0];
        return x + frac * (next - x);
    }

    /** Type-7 quantile of the first {@code n} values of a sorted array. */
    public static double quantile(final double p, final double[] sorted, final int n) {
        final double h = (n - 1) * Math.min(1d, Math.max(0d, p));
        final int lo = (int) Math.floor(h);
        final double x = sorted[lo];
        if (lo + 1 >= n) return x;
        final double frac = h - lo;
        return frac == 0 ? x : x + frac * (sorted[lo + 1] - x);
    }

    // --- block bookkeeping ------------------------------------------------------------------------

    /** (block index << 32 | offset) of the k-th value: Fenwick descent over the block sizes. */
    private long locate(int k) {
        if (tree == null) rebuild();
        int pos = 0;
        for (int step = Integer.highestOneBit(blockCount); step > 0; step >>= 1) {
            final int next = pos + step;
            if (next <= blockCount && tree[next] <= k) {
                pos = next;
                k -= tree[next];
            }
        }
        return ((long) pos << 32) | k;
    }

    private void rebuild() {
        tree = new int[blockCount + 1];
        for (int i = 1; i <= blockCount; i++) {
            tree[i] += sizes[i - 1];
            final int j = i + (i & -i);
            if (j <= blockCount) tree[j] += tree[i];
        }
    }

    private void update(final int b, final int delta) {
        if (tree == null) return;
        for (int i = b + 1; i <= blockCount; i += i & -i) tree[i] += delta;
    }

    private void insertBlock(final int at, final double[] block, final int n) {
        if (blockCount == blocks.length) {
            blocks = Arrays.copyOf(blocks, blocks.length * 2);
            sizes = Arrays.copyOf(sizes, sizes.length * 2);
        }
        System.arraycopy(blocks, at, blocks, at + 1, blockCount - at);
        System.arraycopy(sizes, at, sizes, at + 1, blockCount - at);
        blocks[at] = block;
        sizes[at] = n;
        blockCount++;
        tree = null;
    }

    private void removeBlock(final int at) {
        System.arraycopy(blocks, at + 1, blocks, at, blockCount - at - 1);
        System.arraycopy(sizes, at + 1, sizes, at, blockCount - at - 1);
        blockCount--;
        blocks[blockCount] = null;
        sizes[blockCount] = 0;
        tree = null;
    }

    /** Index of the first block whose last element is >= v (blockCount when none). */
    private int blockFor(final double v) {
        int lo = 0, hi = blockCount;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final double last = blocks[mid][sizes[mid] - 1];
            if (Double.compare(last, v) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static int lowerBound(final double[] a, final int n, final double v) {
        int lo = 0, hi = n;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (Double.compare(a[mid], v) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static int upperBound(final double[] a, final int n, final double v) {
        int lo = 0, hi = n;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (Double.compare(a[mid], v) <= 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

}
