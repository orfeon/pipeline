package com.mercari.solution.util.pipeline.feature;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Rank statistics of the dependence between two series over one window — the {@code regression} funcs
 * {@code spearman}, {@code kendall} and {@code quadrantAsymmetry}. They read the window's pairs as ranks, so a monotone
 * transform of either series changes nothing, and they have no {@link Summary} family: an event entering the window
 * moves every other event's rank, so the window is ranked again at every row (the scan path, bounded by the window's
 * {@code maxEvents} / {@code maxAge}).
 *
 * <ul>
 *   <li>{@link #spearman}: the Pearson correlation of the mid-ranks (ties share the mean of their ranks);</li>
 *   <li>{@link #kendall}: τ_b, the tie-corrected Kendall correlation, in O(n log n) (Knight 1966: sort by x, count the
 *       exchanges a merge sort by y needs);</li>
 *   <li>{@link #quadrantAsymmetry}: {@code (#{u_x < 1 − q ∧ u_y < 1 − q} − #{u_x > q ∧ u_y > q}) / n} over the pairs'
 *       pseudo-observations {@code u = midrank / (n + 1)} — positive when the series fall together more often than they
 *       rise together. Its expectation is 0 under any elliptical copula (normal, t), so it is an asymmetry no correlation
 *       can express, without assuming a family.</li>
 * </ul>
 *
 * Every statistic is null with fewer than two pairs or when a series is constant over them; {@code quadrantAsymmetry}
 * is also null while a corner holds fewer than {@link #MIN_TAIL} pairs in expectation ({@code (1 − q) n < MIN_TAIL}),
 * where it is noise. The values are finite (the evaluator drops a pair with a missing one); ties are numeric equality,
 * so {@code -0.0} and {@code 0.0} tie.
 *
 * <p>{@link #ofCells} is the population counterpart read by the row {@code dependence} op: the same funcs (plus the
 * {@code lowerTail} / {@code upperTail} corners) over a cell distribution map keyed {@code "<x>|<y>"}.
 */
public final class RankDependence {

    public static final String SPEARMAN = "spearman";
    public static final String KENDALL = "kendall";
    public static final String QUADRANT_ASYMMETRY = "quadrantAsymmetry";
    public static final String LOWER_TAIL = "lowerTail";
    public static final String UPPER_TAIL = "upperTail";
    /** The regression funcs computed here (scan-only). */
    public static final List<String> FUNCS = List.of(SPEARMAN, KENDALL, QUADRANT_ASYMMETRY);
    /** The row {@code dependence} funcs over a cell distribution map ({@link #ofCells}). */
    public static final List<String> CELL_FUNCS = List.of(SPEARMAN, KENDALL, QUADRANT_ASYMMETRY, LOWER_TAIL, UPPER_TAIL);
    /** The cell funcs that read the corners, hence {@code q}. */
    public static final List<String> CORNER_FUNCS = List.of(QUADRANT_ASYMMETRY, LOWER_TAIL, UPPER_TAIL);
    public static final double DEFAULT_Q = 0.75;
    /** quadrantAsymmetry is null while {@code (1 − q) n} is below this: a corner of a few pairs is noise. */
    public static final int MIN_TAIL = 5;
    /**
     * The slack of the comparisons against {@code (1 − q)}: a decimal q is not exact in binary ({@code 1 − 0.9} is
     * {@code 0.0999…98}), so a product that equals a whole number or a rank would otherwise round to either side.
     */
    private static final double EPSILON = 1e-9;

    private RankDependence() {
    }

    /** The func over the first {@code n} pairs ({@code x[i]}, {@code y[i]}); {@code q} is read by quadrantAsymmetry only. */
    public static Double of(final String func, final double[] x, final double[] y, final int n, final double q) {
        return switch (func) {
            case SPEARMAN -> spearman(x, y, n);
            case KENDALL -> kendall(x, y, n);
            case QUADRANT_ASYMMETRY -> quadrantAsymmetry(x, y, n, q);
            default -> throw new IllegalArgumentException("not a rank dependence func: " + func);
        };
    }

    public static Double spearman(final double[] x, final double[] y, final int n) {
        if (n < 2) return null;
        final double[] rx = midRanks(x, n), ry = midRanks(y, n);
        // the mean of the mid-ranks is (n + 1) / 2 whatever the ties
        final double mean = (n + 1) / 2d;
        double sxy = 0, sxx = 0, syy = 0;
        for (int i = 0; i < n; i++) {
            final double dx = rx[i] - mean, dy = ry[i] - mean;
            sxy += dx * dy;
            sxx += dx * dx;
            syy += dy * dy;
        }
        if (!(sxx > 0) || !(syy > 0)) return null;
        return sxy / Math.sqrt(sxx * syy);
    }

    public static Double kendall(final double[] x, final double[] y, final int n) {
        if (n < 2) return null;
        // τ reads the order alone: each pair as (rank of x, rank of y) packed into one primitive key, sorted by x then y
        // (no boxed index sort per row; ranks tie by numeric equality, -0.0 with 0.0 included)
        final int[] rx = lowerRanks(x, n), ry = lowerRanks(y, n);
        final long[] keys = new long[n];
        for (int i = 0; i < n; i++) keys[i] = (long) rx[i] * n + ry[i];
        Arrays.sort(keys);
        final long pairs = (long) n * (n - 1) / 2;
        // ties in x (runs of the x rank), and joint ties in (x, y) (runs of the whole key)
        final long tiesX = tiedPairs(keys, n, true), tiesXY = tiedPairs(keys, n, false);
        final int[] ys = new int[n];
        for (int i = 0; i < n; i++) ys[i] = (int) (keys[i] % n);
        // the exchanges a stable merge sort by y needs = the discordant pairs (pairs tied in x are already in y order)
        final long swaps = mergeSort(ys, new int[n], 0, n);
        long tiesY = 0;
        for (int i = 0; i < n; ) {
            int j = i;
            while (j + 1 < n && ys[j + 1] == ys[i]) j++;
            final long t = j - i + 1;
            tiesY += t * (t - 1) / 2;
            i = j + 1;
        }
        final double denominator = Math.sqrt((double) (pairs - tiesX) * (pairs - tiesY));
        if (!(denominator > 0)) return null;
        return (pairs - tiesX - tiesY + tiesXY - 2d * swaps) / denominator;
    }

    public static Double quadrantAsymmetry(final double[] x, final double[] y, final int n, final double q) {
        if (n < 2 || (1 - q) * n < MIN_TAIL - EPSILON) return null;
        final double[] rx = midRanks(x, n), ry = midRanks(y, n);
        if (constant(rx, n) || constant(ry, n)) return null;
        // u < 1 − q ⇔ rank < (1 − q)(n + 1), and u > q ⇔ (n + 1) − rank < (1 − q)(n + 1): one threshold for both
        // corners, so they mirror each other exactly (comparing u with 1 − q and with q apart rounds the two corners
        // differently, e.g. at q = 0.7, and reads an asymmetry into a perfectly symmetric window)
        final double corner = (1 - q) * (n + 1) - EPSILON;
        long lower = 0, upper = 0;
        for (int i = 0; i < n; i++) {
            if (rx[i] < corner && ry[i] < corner) lower++;
            if (n + 1 - rx[i] < corner && n + 1 - ry[i] < corner) upper++;
        }
        return (double) (lower - upper) / n;
    }

    /**
     * A func over a two-dimensional cell distribution: the map a {@code distribution} stat (or a {@code countByValue})
     * holds over a {@code cross} of two ordered codes, keyed {@code "<x>|<y>"} with a share or a count per cell. The map
     * is read as the population of a sample in which a cell is a tie in both series, so each func is the limit of its
     * {@code regression} namesake over that sample:
     *
     * <ul>
     *   <li>{@code kendall}: τ_b of the table, {@code (P_c − P_d) / √((1 − Σ p_x²)(1 − Σ p_y²))}, which is exactly the
     *       sample τ_b of the pairs the counts stand for;</li>
     *   <li>{@code spearman}: the Pearson correlation of the codes' mid-rank shares {@code u = F(v−) + p(v) / 2},
     *       weighted by the cells, which is exactly the sample Spearman of the same pairs;</li>
     *   <li>{@code quadrantAsymmetry}: the mass with both {@code u < 1 − q} minus the mass with both {@code u > q};</li>
     *   <li>{@code lowerTail} / {@code upperTail}: {@code P(u_y < 1 − q | u_x < 1 − q)} and {@code P(u_y > q | u_x > q)},
     *       x the first code of the key (under independence they are the mass of y's corner).</li>
     * </ul>
     *
     * Null when the map is null or empty, when a key is not two numbers joined by {@code |} or a value is not a
     * non-negative number (it is not a cell distribution), when either series takes a single value, and for the
     * corner funcs while a corner they read holds no code (a code's mid-rank share sits in the corner only when the
     * code is rare enough: an extreme bin holding more than {@code 2 (1 − q)} of the mass never does).
     */
    public static Double ofCells(final String func, final Map<?, ?> cells, final double q) {
        final Grid grid = Grid.of(cells);
        if (grid == null) return null;
        return switch (func) {
            case SPEARMAN -> grid.spearman();
            case KENDALL -> grid.kendall();
            case QUADRANT_ASYMMETRY, LOWER_TAIL, UPPER_TAIL -> grid.corner(func, q);
            default -> throw new IllegalArgumentException("not a cell dependence func: " + func);
        };
    }

    /**
     * A cell distribution normalised to shares, held sparse: the cells with mass sorted by x then y code
     * ({@code row[k]} / {@code col[k]} index the sorted distinct x / y codes, {@code mass[k]} is the share) and the two
     * marginals. Memory and time follow the cells (τ_b in O(cells log ny)), not the product of the code counts: a map
     * over two high-cardinality codes (a cross of raw values) costs what its entries do.
     */
    private record Grid(int[] row, int[] col, double[] mass, double[] px, double[] py) {

        static Grid of(final Map<?, ?> cells) {
            if (cells == null || cells.isEmpty()) return null;
            final TreeMap<Double, TreeMap<Double, Double>> byX = new TreeMap<>();
            final TreeMap<Double, Integer> ys = new TreeMap<>();
            double total = 0;
            for (final Map.Entry<?, ?> e : cells.entrySet()) {
                if (e.getKey() == null || e.getValue() == null) continue;
                final Double v = FeatureValues.toDouble(e.getValue());
                if (v == null || !(v >= 0) || Double.isInfinite(v)) return null;
                if (v == 0) continue;
                // keys may be CharSequence after a coder round trip
                final String key = e.getKey().toString();
                final int bar = key.indexOf('|');
                if (bar < 0 || key.indexOf('|', bar + 1) >= 0) return null;
                final double x, y;
                try {
                    x = Double.parseDouble(key.substring(0, bar).trim());
                    y = Double.parseDouble(key.substring(bar + 1).trim());
                } catch (final NumberFormatException notACell) {
                    return null;
                }
                if (!Double.isFinite(x) || !Double.isFinite(y)) return null;
                // + 0.0: -0.0 is the code 0 (TreeMap orders by Double.compare, which tells them apart)
                byX.computeIfAbsent(x + 0.0, k -> new TreeMap<>()).merge(y + 0.0, v, Double::sum);
                ys.put(y + 0.0, 0);
                total += v;
            }
            if (byX.size() < 2 || ys.size() < 2) return null;
            int j = 0;
            for (final Map.Entry<Double, Integer> e : ys.entrySet()) e.setValue(j++);
            int n = 0;
            for (final TreeMap<Double, Double> cellsOfX : byX.values()) n += cellsOfX.size();
            final int[] row = new int[n], col = new int[n];
            final double[] mass = new double[n];
            final double[] px = new double[byX.size()], py = new double[ys.size()];
            int i = 0, k = 0;
            for (final TreeMap<Double, Double> cellsOfX : byX.values()) {
                for (final Map.Entry<Double, Double> e : cellsOfX.entrySet()) {
                    row[k] = i;
                    col[k] = ys.get(e.getKey());
                    mass[k] = e.getValue() / total;
                    px[i] += mass[k];
                    py[col[k]] += mass[k];
                    k++;
                }
                i++;
            }
            return new Grid(row, col, mass, px, py);
        }

        /** The mid-rank shares {@code F(v−) + p(v) / 2} of the codes. */
        private static double[] midShares(final double[] marginal) {
            final double[] u = new double[marginal.length];
            double below = 0;
            for (int i = 0; i < marginal.length; i++) {
                u[i] = below + marginal[i] / 2;
                below += marginal[i];
            }
            return u;
        }

        Double spearman() {
            final double[] ux = midShares(px), uy = midShares(py);
            double mx = 0, my = 0;
            for (int i = 0; i < px.length; i++) mx += px[i] * ux[i];
            for (int j = 0; j < py.length; j++) my += py[j] * uy[j];
            double sxy = 0, sxx = 0, syy = 0;
            for (int i = 0; i < px.length; i++) sxx += px[i] * (ux[i] - mx) * (ux[i] - mx);
            for (int j = 0; j < py.length; j++) syy += py[j] * (uy[j] - my) * (uy[j] - my);
            for (int k = 0; k < mass.length; k++) sxy += mass[k] * (ux[row[k]] - mx) * (uy[col[k]] - my);
            if (!(sxx > 0) || !(syy > 0)) return null;
            return sxy / Math.sqrt(sxx * syy);
        }

        Double kendall() {
            // a Fenwick tree over the y codes holds the mass of the rows after the current one: its mass right of a
            // cell is concordant with it, left of it discordant (each unordered pair of cells counted once; the cells
            // of one x code tie in x, so a row is queried in full before it is added)
            final double[] after = new double[py.length + 1];
            double afterTotal = 0, concordant = 0, discordant = 0;
            for (int end = mass.length; end > 0; ) {
                int start = end - 1;
                while (start > 0 && row[start - 1] == row[end - 1]) start--;
                for (int k = start; k < end; k++) {
                    concordant += mass[k] * (afterTotal - prefix(after, col[k] + 1));
                    discordant += mass[k] * prefix(after, col[k]);
                }
                for (int k = start; k < end; k++) {
                    for (int t = col[k] + 1; t < after.length; t += t & -t) after[t] += mass[k];
                    afterTotal += mass[k];
                }
                end = start;
            }
            double sx = 0, sy = 0;
            for (final double v : px) sx += v * v;
            for (final double v : py) sy += v * v;
            final double denominator = Math.sqrt((1 - sx) * (1 - sy));
            if (!(denominator > 0)) return null;
            // P_c − P_d over ordered pairs = 2 (concordant − discordant)
            return 2 * (concordant - discordant) / denominator;
        }

        /** The mass of the y codes {@code [0, count)} in a Fenwick tree. */
        private static double prefix(final double[] tree, final int count) {
            double sum = 0;
            for (int t = count; t > 0; t -= t & -t) sum += tree[t];
            return sum;
        }

        Double corner(final String func, final double q) {
            final double[] ux = midShares(px), uy = midShares(py);
            // the regression corners on the mid-rank shares, with the same slack (a decimal q is not exact in binary)
            final double corner = 1 - q - EPSILON;
            boolean yLower = false, yUpper = false;
            for (final double u : uy) {
                yLower |= u < corner;
                yUpper |= 1 - u < corner;
            }
            double xLower = 0, xUpper = 0, lower = 0, upper = 0;
            for (int i = 0; i < px.length; i++) {
                if (ux[i] < corner) xLower += px[i];
                if (1 - ux[i] < corner) xUpper += px[i];
            }
            for (int k = 0; k < mass.length; k++) {
                final double x = ux[row[k]], y = uy[col[k]];
                if (x < corner && y < corner) lower += mass[k];
                if (1 - x < corner && 1 - y < corner) upper += mass[k];
            }
            return switch (func) {
                case QUADRANT_ASYMMETRY -> xLower > 0 && xUpper > 0 && yLower && yUpper ? Double.valueOf(lower - upper) : null;
                case LOWER_TAIL -> xLower > 0 && yLower ? Double.valueOf(lower / xLower) : null;
                case UPPER_TAIL -> xUpper > 0 && yUpper ? Double.valueOf(upper / xUpper) : null;
                default -> throw new IllegalArgumentException("not a corner func: " + func);
            };
        }
    }

    /** The mid-ranks (1-based; ties share the mean of their ranks) of the first {@code n} values. */
    static double[] midRanks(final double[] v, final int n) {
        final double[] sorted = Arrays.copyOf(v, n);
        Arrays.sort(sorted);
        final double[] ranks = new double[n];
        // a value's ties occupy the ranks below + 1 .. notAbove
        for (int i = 0; i < n; i++) ranks[i] = (below(sorted, v[i]) + notAbove(sorted, v[i]) + 1) / 2d;
        return ranks;
    }

    /** Each of the first {@code n} values' count of values strictly below it (0-based; ties share it). */
    private static int[] lowerRanks(final double[] v, final int n) {
        final double[] sorted = Arrays.copyOf(v, n);
        Arrays.sort(sorted);
        final int[] ranks = new int[n];
        for (int i = 0; i < n; i++) ranks[i] = below(sorted, v[i]);
        return ranks;
    }

    /** The number of {@code sorted} values numerically below {@code value} ({@code <}: -0.0 is not below 0.0). */
    private static int below(final double[] sorted, final double value) {
        int lo = 0, hi = sorted.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (sorted[mid] < value) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** The number of {@code sorted} values numerically at most {@code value}. */
    private static int notAbove(final double[] sorted, final double value) {
        int lo = 0, hi = sorted.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (sorted[mid] <= value) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** The tied pairs of the sorted {@code (x rank) · n + (y rank)} keys: in x alone, or in both. */
    private static long tiedPairs(final long[] keys, final int n, final boolean xOnly) {
        long ties = 0;
        for (int i = 0; i < keys.length; ) {
            int j = i;
            while (j + 1 < keys.length && (xOnly ? keys[j + 1] / n == keys[i] / n : keys[j + 1] == keys[i])) j++;
            final long t = j - i + 1;
            ties += t * (t - 1) / 2;
            i = j + 1;
        }
        return ties;
    }

    private static boolean constant(final double[] ranks, final int n) {
        for (int i = 1; i < n; i++) if (ranks[i] != ranks[0]) return false;
        return true;
    }

    /** Sorts {@code v[from, to)} ascending, returning the number of exchanges of a bubble sort (strict inversions). */
    private static long mergeSort(final int[] v, final int[] buffer, final int from, final int to) {
        if (to - from < 2) return 0;
        final int mid = (from + to) >>> 1;
        long swaps = mergeSort(v, buffer, from, mid) + mergeSort(v, buffer, mid, to);
        int i = from, j = mid, k = from;
        while (i < mid && j < to) {
            if (v[j] < v[i]) {
                // v[j] jumps every element left in the first half: each is a strict inversion
                swaps += mid - i;
                buffer[k++] = v[j++];
            } else {
                buffer[k++] = v[i++];
            }
        }
        while (i < mid) buffer[k++] = v[i++];
        while (j < to) buffer[k++] = v[j++];
        System.arraycopy(buffer, from, v, from, to - from);
        return swaps;
    }
}
