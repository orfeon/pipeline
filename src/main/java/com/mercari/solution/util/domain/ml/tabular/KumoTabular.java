package com.mercari.solution.util.domain.ml.tabular;

import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.pipeline.glm.GlmFit;

import java.util.*;

/**
 * The preprocessing and the postprocessing of Kumo Tabular: the default recipe of structured-data-models
 * ({@code sdm/models/kumo/tabular/recipe.py}) for one estimator, without its random augmentations (FlipSign,
 * ShuffleColumns, ShuffleCategories). Everything is fitted on the context rows, which come first, and applied to all rows.
 */
public final class KumoTabular {

    public static final int MAX_CLASSES = 10;

    private static final int MAX_COLUMNS = 500;
    private static final int COUNT_MIN_CARDINALITY = 50;
    private static final double STANDARDIZE_EPS = 1e-6;
    private static final double CLIP = 100d;
    private static final double SIGMA_THRESHOLD = 4d;
    private static final double MIN_STD = 1e-6;
    private static final double DOUBLE_EPS = Math.ulp(1d);

    /** {@code x[row][column]} and whether each column came from a categorical feature. */
    public record Features(float[][] x, boolean[] categoricalMask) {}

    public record Classes(int[] codes, List<String> labels) {}

    public record Scale(double mean, double scale) {}

    private record Column(double[] values, boolean categorical) {}

    private KumoTabular() {}

    /**
     * @param numerical a column of all the rows per feature: NaN is a missing value
     * @param categorical a column of all the rows per feature: a value is comparable, null is a missing value
     */
    public static Features features(
            final List<double[]> numerical,
            final List<Comparable<?>[]> categorical,
            final int rows,
            final int contextRows) {

        final List<Column> numericalColumns = new ArrayList<>();
        for(final double[] values : numerical) {
            numericalColumns.add(new Column(values.clone(), false));
        }
        final List<Column> columns = new ArrayList<>(chain(numericalColumns, contextRows));
        columns.addAll(chain(categoricalColumns(categorical, rows, contextRows), contextRows));
        final List<Column> selected = columns.subList(0, Math.min(columns.size(), MAX_COLUMNS));

        final float[][] x = new float[rows][selected.size()];
        final boolean[] mask = new boolean[selected.size()];
        for(int c=0; c<selected.size(); c++) {
            final Column column = selected.get(c);
            mask[c] = column.categorical;
            for(int r=0; r<rows; r++) {
                x[r][c] = (float) column.values[r];
            }
        }
        return new Features(x, mask);
    }

    /** The codes of the context rows, by the first appearance of a label. */
    public static Classes classes(final List<String> labels) {
        final Map<String, Integer> codes = new LinkedHashMap<>();
        final int[] values = new int[labels.size()];
        for(int i=0; i<values.length; i++) {
            final int next = codes.size();
            values[i] = codes.computeIfAbsent(labels.get(i), label -> next);
        }
        if(codes.size() > MAX_CLASSES) {
            throw new IllegalArgumentException("kumo takes up to " + MAX_CLASSES + " classes, but got " + codes.size());
        }
        return new Classes(values, List.copyOf(codes.keySet()));
    }

    /** The mean and the population standard deviation of a target, whose scale is 1 if it is constant. */
    public static Scale scale(final double[] values) {
        final double[] stats = meanAndVariance(values, values.length);
        final double mean = stats[0];
        final double variance = stats[1];
        final double count = stats[2];
        final double roundOff = count * mean * DOUBLE_EPS;
        final boolean constant = variance <= count * DOUBLE_EPS * variance + roundOff * roundOff;
        return new Scale(mean, constant ? 1d : Math.sqrt(variance));
    }

    /** Softmax over the first {@code classes} logits of each row. */
    public static double[][] probabilities(final Tensor logits, final int rows, final int classes) {
        final int width = width(logits, rows);
        if(width < classes) {
            throw new IllegalArgumentException("kumo logits have " + width + " values per row, but there are " + classes + " classes: " + logits);
        }
        final double[][] probabilities = new double[rows][classes];
        for(int r=0; r<rows; r++) {
            final double[] row = probabilities[r];
            for(int k=0; k<classes; k++) {
                row[k] = logits.getDouble(r * width + k);
            }
            GlmFit.softmax(row, row);
        }
        return probabilities;
    }

    /** The quantiles of each row in the scale of the target: sorted, in ascending order of their levels. */
    public static double[][] quantiles(final Tensor logits, final int rows, final Scale scale) {
        final int width = width(logits, rows);
        final double[][] quantiles = new double[rows][width];
        for(int r=0; r<rows; r++) {
            for(int q=0; q<width; q++) {
                quantiles[r][q] = logits.getDouble(r * width + q);
            }
            Arrays.sort(quantiles[r]);
            for(int q=0; q<width; q++) {
                quantiles[r][q] = quantiles[r][q] * scale.scale + scale.mean;
            }
        }
        return quantiles;
    }

    private static int width(final Tensor logits, final int rows) {
        final long[] shape = logits.shape();
        if(shape.length != 2 || shape[0] != rows) {
            throw new IllegalArgumentException("kumo logits must have a row per query, " + rows + " rows, but got: " + logits);
        }
        return (int) shape[1];
    }

    // DropConstantColumns, Standardize, Clip, ClipSigma: in order, as sdm does on each block of columns
    private static List<Column> chain(final List<Column> columns, final int contextRows) {
        final List<Column> kept = new ArrayList<>();
        for(final Column column : columns) {
            if(isConstant(column.values, contextRows)) {
                continue;
            }
            standardize(column.values, contextRows);
            clipSigma(column.values, contextRows);
            kept.add(column);
        }
        return kept;
    }

    // a NaN differs from every value, also another NaN: a column with one is only constant if it is all NaN
    private static boolean isConstant(final double[] values, final int contextRows) {
        if(contextRows <= 1) {
            return false;
        }
        boolean different = false;
        boolean allNaN = true;
        for(int i=0; i<contextRows; i++) {
            different |= values[i] != values[0];
            allNaN &= Double.isNaN(values[i]);
        }
        return !different || allNaN;
    }

    private static void standardize(final double[] values, final int contextRows) {
        final double[] stats = meanAndVariance(values, contextRows);
        final double scale = Math.sqrt(stats[1]) + STANDARDIZE_EPS;
        for(int i=0; i<values.length; i++) {
            // a NaN stays NaN, and an infinity becomes the end of the range
            values[i] = Math.max(-CLIP, Math.min(CLIP, (values[i] - stats[0]) / scale));
        }
    }

    // the mean, the population variance and the count of the finite values among the first n
    private static double[] meanAndVariance(final double[] values, final int n) {
        int count = 0;
        double sum = 0;
        for(int i=0; i<n; i++) {
            if(Double.isFinite(values[i])) {
                count++;
                sum += values[i];
            }
        }
        final double mean = count == 0 ? 0 : sum / count;
        double squares = 0;
        for(int i=0; i<n; i++) {
            if(Double.isFinite(values[i])) {
                squares += (values[i] - mean) * (values[i] - mean);
            }
        }
        return new double[]{mean, count == 0 ? 0 : squares / count, count};
    }

    // two stages of bounds on the context rows, the second without the values out of the first; a value goes over a bound
    // only by the logarithm of its size
    private static void clipSigma(final double[] values, final int contextRows) {
        final double[] stats = meanAndVariance(values, contextRows);
        double mean = stats[0];
        double std = Math.max(Math.sqrt(stats[1] * stats[2] / Math.max(stats[2] - 1, 1)), MIN_STD);
        final double firstLower = mean - SIGMA_THRESHOLD * std;
        final double firstUpper = mean + SIGMA_THRESHOLD * std;

        int kept = 0;
        double keptSum = 0;
        for(int i=0; i<contextRows; i++) {
            if(values[i] >= firstLower && values[i] <= firstUpper) {
                kept++;
                keptSum += values[i];
            }
        }
        if(kept > 0) {
            mean = keptSum / kept;
            double keptSquares = 0;
            for(int i=0; i<contextRows; i++) {
                if(values[i] >= firstLower && values[i] <= firstUpper) {
                    keptSquares += (values[i] - mean) * (values[i] - mean);
                }
            }
            std = Math.max(Math.sqrt(keptSquares / Math.max(kept - 1, 1)), MIN_STD);
        }
        final double lower = mean - SIGMA_THRESHOLD * std;
        final double upper = mean + SIGMA_THRESHOLD * std;
        for(int i=0; i<values.length; i++) {
            final double logAbs = Math.log1p(Math.abs(values[i]));
            values[i] = Math.min(logAbs + upper, Math.max(lower - logAbs, values[i]));
        }
    }

    // AlignCategories by value, AddCategoryCounts, ToNumerical: the count columns come before the code columns
    private static List<Column> categoricalColumns(
            final List<Comparable<?>[]> categorical,
            final int rows,
            final int contextRows) {

        final List<Column> counts = new ArrayList<>();
        final List<Column> codes = new ArrayList<>();
        for(final Comparable<?>[] values : categorical) {
            final TreeSet<Comparable<?>> seen = new TreeSet<>();
            for(int i=0; i<contextRows; i++) {
                if(values[i] != null) {
                    seen.add(values[i]);
                }
            }
            final Map<Comparable<?>, Integer> index = new HashMap<>();
            for(final Comparable<?> value : seen) {
                index.put(value, index.size());
            }
            final double[] code = new double[rows];
            for(int i=0; i<rows; i++) {
                code[i] = values[i] == null ? -1 : index.getOrDefault(values[i], -1);
            }
            codes.add(new Column(code, true));
            if(seen.size() > COUNT_MIN_CARDINALITY) {
                counts.add(new Column(logCounts(code, seen.size(), contextRows), false));
            }
        }
        final List<Column> columns = new ArrayList<>(counts);
        columns.addAll(codes);
        return columns;
    }

    // a missing or an unknown value shares one count, in the slot after the categories
    private static double[] logCounts(final double[] code, final int categories, final int contextRows) {
        final int[] tally = new int[categories + 1];
        for(int i=0; i<contextRows; i++) {
            tally[code[i] >= 0 ? (int) code[i] : categories]++;
        }
        final double[] counts = new double[code.length];
        for(int i=0; i<counts.length; i++) {
            counts[i] = Math.log1p(tally[code[i] >= 0 ? (int) code[i] : categories]);
        }
        return counts;
    }

}
