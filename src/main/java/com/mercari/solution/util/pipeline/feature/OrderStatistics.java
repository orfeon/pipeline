package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Exact order statistics over a multiset of doubles that supports insertion AND deletion — the running
 * accumulator behind the {@code quantile} encoding statistic on the incremental replay path (a value is
 * added when its row becomes visible and removed when it leaves a {@code maxAge} window).
 *
 * <p>Sqrt-decomposition: the values are kept sorted in a list of sorted blocks of at most
 * {@value #BLOCK} elements, so an insertion or deletion moves at most one block ({@code O(√n)}) and a
 * rank query walks the block sizes ({@code O(n / BLOCK)}). A plain sorted array would move {@code O(n)}
 * per insertion, which on the global level of a lattice (one key holding every row) turns the linear
 * replay into a quadratic memmove; a sketch would lose determinism and cannot delete.
 */
public final class OrderStatistics implements Serializable {

    static final int BLOCK = 1024;

    private final List<double[]> blocks = new ArrayList<>();
    private final List<Integer> sizes = new ArrayList<>();
    private int size;

    public int size() {
        return size;
    }

    public void add(final double v) {
        if (blocks.isEmpty()) {
            final double[] block = new double[BLOCK];
            block[0] = v;
            blocks.add(block);
            sizes.add(1);
            size = 1;
            return;
        }
        // the first block whose last element is >= v, else the last block
        int b = blockFor(v);
        if (b == blocks.size()) b = blocks.size() - 1;
        final double[] block = blocks.get(b);
        final int n = sizes.get(b);
        final int at = upperBound(block, n, v);
        if (n == block.length) {
            // split: keep the lower half here, move the upper half into a fresh block
            final int half = n / 2;
            final double[] upper = new double[BLOCK];
            System.arraycopy(block, half, upper, 0, n - half);
            Arrays.fill(block, half, n, 0d);
            blocks.add(b + 1, upper);
            sizes.set(b, half);
            sizes.add(b + 1, n - half);
            if (at > half) {
                insertAt(b + 1, at - half, v);
            } else {
                insertAt(b, at, v);
            }
        } else {
            insertAt(b, at, v);
        }
        size++;
    }

    private void insertAt(final int b, final int at, final double v) {
        final double[] block = blocks.get(b);
        final int n = sizes.get(b);
        System.arraycopy(block, at, block, at + 1, n - at);
        block[at] = v;
        sizes.set(b, n + 1);
    }

    /** Removes one occurrence of {@code v}; returns false when it is absent. */
    public boolean remove(final double v) {
        final int b = blockFor(v);
        if (b == blocks.size()) return false;
        final double[] block = blocks.get(b);
        final int n = sizes.get(b);
        final int at = lowerBound(block, n, v);
        if (at == n || Double.compare(block[at], v) != 0) return false;
        System.arraycopy(block, at + 1, block, at, n - at - 1);
        if (n == 1) {
            blocks.remove(b);
            sizes.remove(b);
        } else {
            sizes.set(b, n - 1);
        }
        size--;
        return true;
    }

    /** The k-th smallest value (0-based). */
    public double select(int k) {
        if (k < 0 || k >= size) throw new IndexOutOfBoundsException(k + " of " + size);
        for (int b = 0; b < blocks.size(); b++) {
            final int n = sizes.get(b);
            if (k < n) return blocks.get(b)[k];
            k -= n;
        }
        throw new IllegalStateException();
    }

    /**
     * Quantile {@code p} in [0, 1] with linear interpolation between order statistics (R type 7 / numpy
     * default), or null when empty.
     */
    public Double quantile(final double p) {
        if (size == 0) return null;
        return quantile(p, size, this::select);
    }

    interface Selector {
        double at(int k);
    }

    /** Type-7 quantile over {@code n} sorted values reachable through {@code select}. */
    static double quantile(final double p, final int n, final Selector select) {
        final double h = (n - 1) * Math.min(1d, Math.max(0d, p));
        final int lo = (int) Math.floor(h);
        final double x = select.at(lo);
        if (lo + 1 >= n) return x;
        final double frac = h - lo;
        return frac == 0 ? x : x + frac * (select.at(lo + 1) - x);
    }

    /** Type-7 quantile of a sorted array. */
    public static double quantile(final double p, final double[] sorted, final int n) {
        return quantile(p, n, k -> sorted[k]);
    }

    /** Index of the first block whose last element is >= v (blocks.size() when none). */
    private int blockFor(final double v) {
        int lo = 0, hi = blocks.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final double last = blocks.get(mid)[sizes.get(mid) - 1];
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
