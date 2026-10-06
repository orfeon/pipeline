package com.mercari.solution.util.pipeline.profile;

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * What the first pass hands to the counting pass (profile-engine.md §3, §4): per field the split
 * points of its cells — the equal-frequency cells of the whole dataset for a numeric-like field,
 * the counted values for a string field — the declared edges of the fields that have them, and
 * the pooled cells of every declared field pair. A cell is right-closed: a value equal to a split
 * point belongs to the cell below it.
 */
public class ProfileEdges implements Serializable {

    /** Equal-frequency cells per numeric-like field (fewer where ties collapse split points). */
    public static final int CELLS = 100;

    /** Cell of a null value. */
    public static final int NULL_CELL = -1;
    /** Cell of a value that is not counted (unreadable, NaN, infinite, or a field without cells). */
    public static final int NO_CELL = -2;

    public static final String OTHER = "(other)";

    private final double[][] splits;          // numeric-like: interior split points, strictly increasing
    private final double[][] declared;        // numeric-like: declared split points, or null
    private final String[][] categories;      // string: the counted values; every other value is the last cell
    private final int[][] pairFields;         // [pair] = {field a, field b}
    private final double[][] pairSplits;      // [pair] = pooled split points

    private transient Map<String, Integer>[] categoryIndex;

    private ProfileEdges(
            final double[][] splits,
            final double[][] declared,
            final String[][] categories,
            final int[][] pairFields,
            final double[][] pairSplits) {

        this.splits = splits;
        this.declared = declared;
        this.categories = categories;
        this.pairFields = pairFields;
        this.pairSplits = pairSplits;
        this.categoryIndex = indexCategories(categories);
    }

    /** Rebuilt on arrival rather than on first use: the edges are a side input every worker thread reads. */
    private void readObject(final java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.categoryIndex = indexCategories(categories);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Integer>[] indexCategories(final String[][] categories) {
        final Map<String, Integer>[] indices = new Map[categories.length];
        for(int field = 0; field < categories.length; field++) {
            if(categories[field] != null) {
                final Map<String, Integer> index = new HashMap<>();
                for(int i = 0; i < categories[field].length; i++) {
                    index.put(categories[field][i], i);
                }
                indices[field] = index;
            }
        }
        return indices;
    }

    /**
     * @param declaredEdges field path → declared split points (sorted, strictly increasing)
     * @param pairs declared field pairs (paths)
     * @param categoryLimit max counted values of a string field
     */
    public static ProfileEdges of(
            final ProfileAccumulator accumulator,
            final Map<String, double[]> declaredEdges,
            final List<String[]> pairs,
            final int categoryLimit) {

        final ProfileSpec spec = accumulator.getSpec();
        final int n = spec.getFields().size();
        final double[][] splits = new double[n][];
        final double[][] declared = new double[n][];
        final String[][] categories = new String[n][];
        final Map<String, Integer> indexOfPath = new HashMap<>();
        for(int i = 0; i < n; i++) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
            final ProfileAccumulator.FieldAccumulator field = accumulator.getField(i);
            indexOfPath.put(fieldSpec.path, i);
            switch (fieldSpec.profileType) {
                case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> {
                    final KllDoublesSketch kll = field.getKll();
                    if(field.count > 0 && kll != null && !kll.isEmpty()) {
                        splits[i] = quantileSplits(kll, CELLS);
                        if(declaredEdges != null && declaredEdges.containsKey(fieldSpec.path)) {
                            declared[i] = declaredEdges.get(fieldSpec.path);
                        }
                    }
                }
                case STRING -> {
                    if(field.count > 0) {
                        categories[i] = topValues(field, categoryLimit).toArray(new String[0]);
                    }
                }
                case BOOL -> { }
            }
        }
        final int[][] pairFields = new int[pairs == null ? 0 : pairs.size()][];
        final double[][] pairSplits = new double[pairFields.length][];
        for(int p = 0; p < pairFields.length; p++) {
            final int a = indexOfPath.getOrDefault(pairs.get(p)[0], -1);
            final int b = indexOfPath.getOrDefault(pairs.get(p)[1], -1);
            pairFields[p] = new int[] { a, b };
            final KllDoublesSketch kllA = a < 0 ? null : accumulator.getField(a).getKll();
            final KllDoublesSketch kllB = b < 0 ? null : accumulator.getField(b).getKll();
            if(kllA != null && kllB != null && !kllA.isEmpty() && !kllB.isEmpty()) {
                // merge copies: the accumulator's own sketches must stay as they are
                final KllDoublesSketch pooled = KllDoublesSketch.newHeapInstance(Math.max(kllA.getK(), kllB.getK()));
                pooled.merge(KllDoublesSketch.heapify(org.apache.datasketches.memory.Memory.wrap(kllA.toByteArray())));
                pooled.merge(KllDoublesSketch.heapify(org.apache.datasketches.memory.Memory.wrap(kllB.toByteArray())));
                pairSplits[p] = quantileSplits(pooled, CELLS);
            }
        }
        return new ProfileEdges(splits, declared, categories, pairFields, pairSplits);
    }

    /** The distinct interior quantiles at 1/cells … (cells-1)/cells, below the maximum. */
    static double[] quantileSplits(final KllDoublesSketch kll, final int cells) {
        final double max = kll.getMaxItem();
        final double[] points = new double[cells - 1];
        int n = 0;
        for(int i = 1; i < cells; i++) {
            final double q = kll.getQuantile((double) i / cells, QuantileSearchCriteria.INCLUSIVE);
            // a split at the maximum would leave an empty last cell
            if(q < max && (n == 0 || q > points[n - 1])) {
                points[n++] = q;
            }
        }
        return Arrays.copyOf(points, n);
    }

    /**
     * The values of a string field the counting pass keeps apart, most frequent first: the exact
     * table when the field has one, else the frequent items (the certain ones, or the candidates
     * when no value is frequent enough to be certain of).
     */
    static List<String> topValues(final ProfileAccumulator.FieldAccumulator field, final int limit) {
        final List<String> values = new ArrayList<>();
        final Map<String, Long> table = field.getStringValues();
        if(table != null) {
            table.entrySet().stream()
                    .sorted((a, b) -> {
                        final int c = Long.compare(b.getValue(), a.getValue());
                        return c != 0 ? c : a.getKey().compareTo(b.getKey());
                    })
                    .limit(limit)
                    .forEach(e -> values.add(e.getKey()));
            return values;
        }
        final ItemsSketch<String> fi = field.getFrequentItems();
        if(fi == null) {
            return values;
        }
        ItemsSketch.Row<String>[] rows = fi.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        if(rows.length == 0) {
            rows = fi.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
        }
        for(int i = 0; i < rows.length && i < limit; i++) {
            values.add(rows[i].getItem());
        }
        return values;
    }

    public double[] getSplits(final int field) {
        return splits[field];
    }

    public double[] getDeclared(final int field) {
        return declared[field];
    }

    public String[] getCategories(final int field) {
        return categories[field];
    }

    public int getPairCount() {
        return pairFields.length;
    }

    public int[] getPairFields(final int pair) {
        return pairFields[pair];
    }

    public double[] getPairSplits(final int pair) {
        return pairSplits[pair];
    }

    /** Number of cells of a field: 0 when it has none. */
    public int cellCount(final ProfileSpec.ProfileType profileType, final int field) {
        return switch (profileType) {
            case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> splits[field] == null ? 0 : splits[field].length + 1;
            case STRING -> categories[field] == null ? 0 : categories[field].length + 1;
            case BOOL -> 2;
        };
    }

    /** The cell of a coerced {@link ProfileRow} value: an index, {@link #NULL_CELL} or {@link #NO_CELL}. */
    public int cellOf(final ProfileSpec.FieldSpec fieldSpec, final int field, final Object value) {
        if(value == null) {
            return NULL_CELL;
        }
        if(value == ProfileRow.Marker.ERROR) {
            return NO_CELL;
        }
        switch (fieldSpec.profileType) {
            case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> {
                final Double v = numeric(value);
                return v == null || splits[field] == null ? NO_CELL : locate(splits[field], v);
            }
            case STRING -> {
                if(categories[field] == null || !(value instanceof String s)) {
                    return NO_CELL;
                }
                final Integer index = categoryIndex[field].get(s);
                return index == null ? categories[field].length : index;
            }
            case BOOL -> {
                return value instanceof Boolean b ? (b ? 0 : 1) : NO_CELL;
            }
        }
        return NO_CELL;
    }

    /** The declared-edge bin of a value, or {@link #NO_CELL} when the field declares none or the value is not counted. */
    public int declaredCellOf(final int field, final Object value) {
        if(declared[field] == null) {
            return NO_CELL;
        }
        final Double v = numeric(value);
        return v == null ? NO_CELL : locate(declared[field], v);
    }

    /** The pooled cell of a value of one member of a declared pair. */
    public int pairCellOf(final int pair, final Object value) {
        if(pairSplits[pair] == null) {
            return NO_CELL;
        }
        final Double v = numeric(value);
        return v == null ? NO_CELL : locate(pairSplits[pair], v);
    }

    /** The finite numeric reading of a coerced value, or null. */
    static Double numeric(final Object value) {
        final double v;
        if(value instanceof Double d) {
            v = d;
        } else if(value instanceof Integer i) {
            v = i;
        } else {
            return null;
        }
        return Double.isNaN(v) || Double.isInfinite(v) ? null : v;
    }

    /** Index of the right-closed cell of {@code v} among {@code splits.length + 1} cells. */
    static int locate(final double[] splits, final double v) {
        final int index = Arrays.binarySearch(splits, v);
        return index >= 0 ? index : -index - 1;
    }
}
