package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.util.domain.math.MatrixOps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Pure functions over a numeric vector: the vector → vector steps ({@link #slice}, {@link #diff},
 * {@link #normalize}) and the vector → scalar readouts ({@link #read}, {@link #polyfit}) behind the row
 * {@code type: vector} op. Beam-free and stateless; the accepted readout names and their output types live in
 * {@link OperatorCatalog#VECTOR_FUNCS} / {@link OperatorCatalog#vectorOutput}. A readout that is undefined on the
 * vector at hand (too few elements, a zero denominator, a non-finite result) reads null, never NaN.
 */
public final class VectorOps {

    private VectorOps() {}

    /**
     * One {@code vector} column resolved from its coordinates: the steps applied to the array (slice → diff →
     * normalize → resample / pad, in that order) and the readout taken from the result — or, for the {@code vector}
     * readout, the stepped vector itself as an array column.
     */
    public record Plan(String func, Integer from, Integer to, int diff, String normalize, boolean unitPosition,
                       int degree, int coefficient, Integer resample, Integer padLength, String padMode, String padSide) implements Serializable {

        public static Plan of(final Map<String, String> coordinates) {
            return new Plan(coordinates.get("func"),
                    coordinates.containsKey("sliceFrom") ? Integer.valueOf(coordinates.get("sliceFrom")) : null,
                    coordinates.containsKey("sliceTo") ? Integer.valueOf(coordinates.get("sliceTo")) : null,
                    Integer.parseInt(coordinates.getOrDefault("diff", "0")),
                    coordinates.get("normalize"),
                    "unit".equals(coordinates.get("position")),
                    Integer.parseInt(coordinates.getOrDefault("degree", "0")),
                    Integer.parseInt(coordinates.getOrDefault("coefficient", "0")),
                    coordinates.containsKey("resample") ? Integer.valueOf(coordinates.get("resample")) : null,
                    coordinates.containsKey("padLength") ? Integer.valueOf(coordinates.get("padLength")) : null,
                    coordinates.getOrDefault("padMode", "edge"),
                    coordinates.getOrDefault("padSide", "end"));
        }

        /** The column's value for an array field value (null when the array or the readout is undefined). */
        public Object evaluate(final Object array) {
            double[] x = toVector(array);
            if (x == null) return null;
            x = VectorOps.slice(x, from, to);
            x = VectorOps.diff(x, diff);
            if (normalize != null) x = VectorOps.normalize(x, normalize);
            if (x == null) return null;
            if (resample != null) x = VectorOps.resample(x, resample);
            if (padLength != null) x = VectorOps.pad(x, padLength, padMode, padSide);
            if ("vector".equals(func)) {
                // the empty vector has no readout but its length: an array svd would otherwise fix its dimension on it
                if (x.length == 0) return null;
                final List<Double> out = new java.util.ArrayList<>(x.length);
                for (final double v : x) out.add(v);
                return out;
            }
            if ("polyfit".equals(func)) {
                final double[] coefficients = polyfit(x, positions(x.length, unitPosition), degree);
                return coefficients == null ? null : finite(coefficients[coefficient]);
            }
            return read(func, x, positions(x.length, unitPosition));
        }
    }

    /**
     * The vector of an array value ({@code double[]} or a list of numbers), or null when the array is absent or a
     * component is missing (null / NaN / infinite): a vector with a hole has no defined readout.
     */
    public static double[] toVector(final Object value) {
        if (value instanceof double[] a) {
            for (final double d : a) if (!Double.isFinite(d)) return null;
            return a;
        }
        if (!(value instanceof List<?> list)) return null;
        final double[] x = new double[list.size()];
        for (int i = 0; i < x.length; i++) {
            final Double d = FeatureValues.toDouble(list.get(i));
            if (d == null || !Double.isFinite(d)) return null;
            x[i] = d;
        }
        return x;
    }

    /**
     * The elements {@code [from, to)} with the list-slicing convention: a negative index counts from the end, a
     * null bound is open, bounds beyond the vector are clamped (an empty result is a valid, empty vector).
     */
    public static double[] slice(final double[] x, final Integer from, final Integer to) {
        if (from == null && to == null) return x;
        final int n = x.length;
        final int start = clamp(from == null ? 0 : from < 0 ? n + from : from, n);
        final int end = clamp(to == null ? n : to < 0 ? n + to : to, n);
        return start >= end ? new double[0] : java.util.Arrays.copyOfRange(x, start, end);
    }

    private static int clamp(final int index, final int n) {
        return Math.max(0, Math.min(n, index));
    }

    /** Differences of adjacent elements, applied {@code order} times (each pass shortens the vector by one). */
    public static double[] diff(final double[] x, final int order) {
        double[] out = x;
        for (int pass = 0; pass < order && out.length > 0; pass++) {
            final double[] next = new double[out.length - 1];
            for (int i = 0; i < next.length; i++) next[i] = out[i + 1] - out[i];
            out = next;
        }
        return out;
    }

    /**
     * The vector rescaled by one of its own statistics — {@code sum} (shares), {@code mean} (relative to the
     * average), {@code l2} (unit length), {@code zscore} ((x − mean) / std, population std) — or null when the
     * denominator is zero or the vector is empty.
     */
    public static double[] normalize(final double[] x, final String mode) {
        if (x.length == 0) return null;
        final double mean = sum(x) / x.length;
        final double shift = "zscore".equals(mode) ? mean : 0d;
        final double scale = switch (mode) {
            case "sum" -> sum(x);
            case "mean" -> mean;
            case "l2" -> MatrixOps.norm(x);
            case "zscore" -> std(x, mean);
            default -> throw new IllegalArgumentException("unsupported vector normalize: " + mode);
        };
        if (scale == 0 || !Double.isFinite(scale)) return null;
        final double[] out = new double[x.length];
        for (int i = 0; i < x.length; i++) out[i] = (x[i] - shift) / scale;
        return out;
    }

    /**
     * The vector linearly interpolated onto {@code length} equally spaced positions of its own span (the first and the
     * last element kept): the same shape at a fixed length, whether the array was shorter or longer. A single element
     * is repeated; the empty vector stays empty (nothing to interpolate).
     */
    public static double[] resample(final double[] x, final int length) {
        final int m = x.length;
        if (m == 0 || m == length) return x;
        final double[] out = new double[length];
        if (m == 1) {
            java.util.Arrays.fill(out, x[0]);
            return out;
        }
        for (int j = 0; j < length; j++) {
            final double p = length == 1 ? 0d : (double) j * (m - 1) / (length - 1);
            final int i = Math.min(m - 2, (int) Math.floor(p));
            final double t = p - i;
            // the convex form, not x[i] + t (x[i+1] − x[i]): exact at t = 0 / 1, so the first and last element are kept
            out[j] = (1 - t) * x[i] + t * x[i + 1];
        }
        return out;
    }

    /**
     * The vector extended to {@code length} elements — at its {@code end} (default) or its {@code start} — with the
     * nearest element ({@code edge}, default) or zeros ({@code zero}); a vector of that length or longer is returned as
     * is (padding never truncates), the empty vector is padded with zeros (it has no edge).
     */
    public static double[] pad(final double[] x, final int length, final String mode, final String side) {
        final int m = x.length;
        if (m >= length) return x;
        final boolean zero = "zero".equals(mode) || m == 0;
        final boolean start = "start".equals(side);
        final double[] out = new double[length];
        final int offset = start ? length - m : 0;
        System.arraycopy(x, 0, out, offset, m);
        final double fill = zero ? 0d : start ? x[0] : x[m - 1];
        if (start) java.util.Arrays.fill(out, 0, offset, fill);
        else java.util.Arrays.fill(out, m, length, fill);
        return out;
    }

    /** The position of each element: its index, or the index scaled to [0, 1] ({@code unit}; a single element sits at 0). */
    public static double[] positions(final int n, final boolean unit) {
        final double[] p = new double[n];
        for (int i = 0; i < n; i++) p[i] = unit ? (n == 1 ? 0d : i / (double) (n - 1)) : i;
        return p;
    }

    /**
     * A scalar readout of the vector. {@code length} is defined on the empty vector (0); every other readout needs
     * an element, {@code std} and {@code slope} need two. {@code argmin} / {@code argmax} are the index of the first
     * extreme element within the vector the steps produced.
     */
    public static Object read(final String func, final double[] x, final double[] positions) {
        final int n = x.length;
        if ("length".equals(func)) return (long) n;
        if (n == 0) return null;
        return switch (func) {
            case "sum" -> finite(sum(x));
            case "mean" -> finite(sum(x) / n);
            case "std" -> n < 2 ? null : finite(std(x, sum(x) / n));
            case "min" -> x[arg(x, -1)];
            case "max" -> x[arg(x, 1)];
            case "argmin" -> (long) arg(x, -1);
            case "argmax" -> (long) arg(x, 1);
            case "first" -> x[0];
            case "last" -> x[n - 1];
            case "norm" -> finite(MatrixOps.norm(x));
            case "slope" -> slope(x, positions);
            default -> throw new IllegalArgumentException("unsupported vector func: " + func);
        };
    }

    /**
     * Least-squares polynomial coefficients in ascending order ({@code c[0] + c[1] p + … + c[degree] p^degree} over
     * the positions), or null when the vector has fewer than {@code degree + 1} elements.
     */
    public static double[] polyfit(final double[] x, final double[] positions, final int degree) {
        if (x.length < degree + 1) return null;
        return MatrixOps.polyfit(positions, x, degree);
    }

    private static Double slope(final double[] x, final double[] positions) {
        final int n = x.length;
        if (n < 2) return null;
        final double pMean = sum(positions) / n;
        final double xMean = sum(x) / n;
        double num = 0, den = 0;
        for (int i = 0; i < n; i++) {
            num += (positions[i] - pMean) * (x[i] - xMean);
            den += (positions[i] - pMean) * (positions[i] - pMean);
        }
        return den == 0 ? null : finite(num / den);
    }

    private static double sum(final double[] x) {
        double s = 0;
        for (final double d : x) s += d;
        return s;
    }

    /** Population standard deviation (the convention of the sequence / encoding {@code std}). */
    private static double std(final double[] x, final double mean) {
        double ss = 0;
        for (final double d : x) ss += (d - mean) * (d - mean);
        return Math.sqrt(ss / x.length);
    }

    /** Index of the first minimum (sign −1) or maximum (sign +1). */
    private static int arg(final double[] x, final int sign) {
        int best = 0;
        for (int i = 1; i < x.length; i++) if (sign * Double.compare(x[i], x[best]) > 0) best = i;
        return best;
    }

    private static Double finite(final double value) {
        return Double.isFinite(value) ? value : null;
    }

}
