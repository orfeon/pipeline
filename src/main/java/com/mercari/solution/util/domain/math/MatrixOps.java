package com.mercari.solution.util.domain.math;

import org.ojalgo.matrix.decomposition.Cholesky;
import org.ojalgo.matrix.decomposition.SingularValue;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.R064Store;

import java.util.ArrayList;
import java.util.List;

/**
 * Linear-algebra core shared by the {@code select} functions
 * ({@code util/pipeline/select/Matrix}) and the {@code query} built-ins
 * ({@code util/pipeline/udf/MatrixFunctions}) — the adapters stay thin and the
 * numerics live here, on ojalgo (pure Java, dependency-free on the workers).
 *
 * <p>Everything operates on plain {@code double[]} / {@code double[][]}:
 * ojalgo stores never cross this class's boundary, so callers (and Beam
 * coders / Calcite codegen) only ever see JDK types. Solves use the singular
 * value decomposition (rank-deficient and overdetermined systems both yield
 * the least-squares / pseudo-inverse solution) except {@link #solveGram},
 * whose normal-equation matrix is symmetric positive (semi-)definite and goes
 * through Cholesky first.
 *
 * <p>The {@code toVector} / {@code toMatrix} coercers accept the value shapes
 * both surfaces produce ({@code List<Number>}, {@code List<List<Number>>},
 * boxed and primitive arrays); {@code null} input returns {@code null},
 * non-numeric or ragged input throws {@link IllegalArgumentException}.
 */
public final class MatrixOps {

    private MatrixOps() {
    }

    public static double dot(final double[] a, final double[] b) {
        checkSameLength("dot", a, b);
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    public static double norm(final double[] a) {
        return Math.sqrt(dot(a, a));
    }

    /** Cosine similarity; {@code null} when either vector has zero norm. */
    public static Double cosineSimilarity(final double[] a, final double[] b) {
        checkSameLength("cosineSimilarity", a, b);
        final double normA = norm(a);
        final double normB = norm(b);
        if (normA == 0 || normB == 0) {
            return null;
        }
        return dot(a, b) / (normA * normB);
    }

    /** Matrix-vector product (a fixed projection matrix × an embedding, etc). */
    public static double[] multiply(final double[][] matrix, final double[] vector) {
        checkMatrix("multiply", matrix);
        if (matrix[0].length != vector.length) {
            throw new IllegalArgumentException("multiply requires matrix columns == vector length,"
                    + " but got " + matrix[0].length + " columns and " + vector.length + " elements");
        }
        final double[] out = new double[matrix.length];
        for (int i = 0; i < matrix.length; i++) {
            out[i] = dot(matrix[i], vector);
        }
        return out;
    }

    /**
     * Solves {@code A x = b} by SVD: the exact solution for a full-rank square
     * system, the least-squares solution for an overdetermined one, the
     * minimum-norm solution when rank-deficient.
     */
    public static double[] solve(final double[][] a, final double[] b) {
        checkMatrix("solve", a);
        if (a.length != b.length) {
            throw new IllegalArgumentException("solve requires matrix rows == rhs length,"
                    + " but got " + a.length + " rows and " + b.length + " elements");
        }
        final R064Store matrix = R064Store.FACTORY.rows(a);
        final R064Store rhs = columnStore(b);
        final SingularValue<Double> svd = SingularValue.R064.make(matrix);
        if (!svd.decompose(matrix)) {
            throw new IllegalStateException("solve failed to decompose the "
                    + a.length + "x" + a[0].length + " matrix");
        }
        return column(svd.getSolution(rhs));
    }

    /** Moore–Penrose pseudo-inverse via SVD (exact inverse when non-singular). */
    public static double[][] inverse(final double[][] a) {
        checkMatrix("inverse", a);
        final R064Store matrix = R064Store.FACTORY.rows(a);
        final SingularValue<Double> svd = SingularValue.R064.make(matrix);
        if (!svd.decompose(matrix)) {
            throw new IllegalStateException("inverse failed to decompose the "
                    + a.length + "x" + a[0].length + " matrix");
        }
        final MatrixStore<Double> inverse = svd.getInverse();
        final double[][] out = new double[(int) inverse.countRows()][(int) inverse.countColumns()];
        for (int i = 0; i < out.length; i++) {
            for (int j = 0; j < out[i].length; j++) {
                out[i][j] = inverse.doubleValue(i, j);
            }
        }
        return out;
    }

    /**
     * Mahalanobis distance {@code sqrt((x-m)^T P (x-m))} with {@code P} the
     * <em>precision</em> (inverse covariance) matrix — invert a covariance
     * matrix once with {@link #inverse} and reuse it. A slightly negative
     * quadratic form from floating-point noise is clamped to zero.
     */
    public static double mahalanobis(final double[] x, final double[] mean, final double[][] precision) {
        checkSameLength("mahalanobis", x, mean);
        checkMatrix("mahalanobis", precision);
        if (precision.length != x.length || precision[0].length != x.length) {
            throw new IllegalArgumentException("mahalanobis requires a " + x.length + "x" + x.length
                    + " precision matrix, but got "
                    + precision.length + "x" + precision[0].length);
        }
        final double[] d = new double[x.length];
        for (int i = 0; i < x.length; i++) {
            d[i] = x[i] - mean[i];
        }
        return Math.sqrt(Math.max(0d, dot(multiply(precision, d), d)));
    }

    /**
     * Least-squares polynomial fit; returns {@code degree + 1} coefficients in
     * ascending order ({@code c[0] + c[1] x + …}). Degree 1 is a straight
     * line: {@code c[1]} is the slope.
     */
    public static double[] polyfit(final double[] x, final double[] y, final int degree) {
        checkSameLength("polyfit", x, y);
        if (degree < 0) {
            throw new IllegalArgumentException("polyfit degree must be >= 0, but was " + degree);
        }
        if (x.length < degree + 1) {
            throw new IllegalArgumentException("polyfit degree " + degree + " requires at least "
                    + (degree + 1) + " points, but got " + x.length);
        }
        final double[][] vandermonde = new double[x.length][degree + 1];
        for (int i = 0; i < x.length; i++) {
            double pow = 1;
            for (int j = 0; j <= degree; j++) {
                vandermonde[i][j] = pow;
                pow *= x[i];
            }
        }
        return solve(vandermonde, y);
    }

    /**
     * Solves the normal equations {@code (X^T X + ridge·I) β = X^T y} from an
     * additively-accumulated Gram matrix — the streaming/mergeable form of
     * multivariate linear regression. Cholesky first (the Gram matrix is
     * symmetric PSD); singular systems fall back to the SVD minimum-norm
     * solution.
     */
    public static double[] solveGram(final double[][] xtx, final double[] xty, final double ridge) {
        checkMatrix("solveGram", xtx);
        if (xtx.length != xtx[0].length || xtx.length != xty.length) {
            throw new IllegalArgumentException("solveGram requires a square matrix matching the"
                    + " rhs length, but got " + xtx.length + "x" + xtx[0].length
                    + " and " + xty.length + " elements");
        }
        if (ridge < 0) {
            throw new IllegalArgumentException("solveGram ridge must be >= 0, but was " + ridge);
        }
        final double[][] regularized;
        if (ridge > 0) {
            regularized = new double[xtx.length][];
            for (int i = 0; i < xtx.length; i++) {
                regularized[i] = xtx[i].clone();
                regularized[i][i] += ridge;
            }
        } else {
            regularized = xtx;
        }
        final R064Store matrix = R064Store.FACTORY.rows(regularized);
        final R064Store rhs = columnStore(xty);
        final Cholesky<Double> cholesky = Cholesky.R064.make(matrix);
        if (cholesky.decompose(matrix) && cholesky.isSolvable()) {
            return column(cholesky.getSolution(rhs));
        }
        return solve(regularized, xty);
    }

    /** Coerces a numeric collection/array value to {@code double[]} ({@code null} in → {@code null} out). */
    public static double[] toVector(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof double[] doubles) {
            return doubles.clone();
        }
        if (value instanceof float[] floats) {
            final double[] out = new double[floats.length];
            for (int i = 0; i < floats.length; i++) {
                out[i] = floats[i];
            }
            return out;
        }
        if (value instanceof long[] longs) {
            final double[] out = new double[longs.length];
            for (int i = 0; i < longs.length; i++) {
                out[i] = longs[i];
            }
            return out;
        }
        if (value instanceof int[] ints) {
            final double[] out = new double[ints.length];
            for (int i = 0; i < ints.length; i++) {
                out[i] = ints[i];
            }
            return out;
        }
        final List<?> values = asCollection("vector", value);
        final double[] out = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            final Object element = values.get(i);
            if (!(element instanceof Number number)) {
                throw new IllegalArgumentException("vector requires numeric elements, but position "
                        + (i + 1) + " holds " + describe(element));
            }
            out[i] = number.doubleValue();
        }
        return out;
    }

    /** Coerces a nested collection/array value (rows of numbers) to {@code double[][]}. */
    public static double[][] toMatrix(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof double[][] doubles) {
            final double[][] out = new double[doubles.length][];
            for (int i = 0; i < doubles.length; i++) {
                out[i] = doubles[i].clone();
            }
            return out;
        }
        final List<?> rows = asCollection("matrix", value);
        final double[][] out = new double[rows.size()][];
        for (int i = 0; i < rows.size(); i++) {
            final double[] row = toVector(rows.get(i));
            if (row == null) {
                throw new IllegalArgumentException("matrix row " + (i + 1) + " is null");
            }
            if (i > 0 && row.length != out[0].length) {
                throw new IllegalArgumentException("matrix rows must have equal lengths, but row "
                        + (i + 1) + " has " + row.length + " elements while row 1 has "
                        + out[0].length);
            }
            out[i] = row;
        }
        return out;
    }

    /** Coerces a flat numeric value to a row-major {@code rows × columns} matrix. */
    public static double[][] toMatrix(final Object value, final int columns) {
        final double[] flat = toVector(value);
        if (flat == null) {
            return null;
        }
        if (columns <= 0 || flat.length % columns != 0) {
            throw new IllegalArgumentException("matrix of " + flat.length
                    + " elements cannot be split into rows of " + columns + " columns");
        }
        final double[][] out = new double[flat.length / columns][columns];
        for (int i = 0; i < flat.length; i++) {
            out[i / columns][i % columns] = flat[i];
        }
        return out;
    }

    public static List<Double> toList(final double[] values) {
        if (values == null) {
            return null;
        }
        final List<Double> out = new ArrayList<>(values.length);
        for (final double value : values) {
            out.add(value);
        }
        return out;
    }

    private static R064Store columnStore(final double[] values) {
        final R064Store store = R064Store.FACTORY.make(values.length, 1);
        for (int i = 0; i < values.length; i++) {
            store.set(i, 0, values[i]);
        }
        return store;
    }

    private static double[] column(final MatrixStore<Double> store) {
        final double[] out = new double[(int) store.countRows()];
        for (int i = 0; i < out.length; i++) {
            out[i] = store.doubleValue(i, 0);
        }
        return out;
    }

    private static List<?> asCollection(final String what, final Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        if (value instanceof Object[] array) {
            return List.of(array);
        }
        throw new IllegalArgumentException(what + " requires an array/collection value,"
                + " but got " + describe(value));
    }

    private static String describe(final Object value) {
        return value == null ? "NULL" : value.getClass().getSimpleName();
    }

    private static void checkSameLength(final String op, final double[] a, final double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(op + " requires vectors of equal length, but got "
                    + a.length + " and " + b.length);
        }
    }

    private static void checkMatrix(final String op, final double[][] matrix) {
        if (matrix.length == 0 || matrix[0].length == 0) {
            throw new IllegalArgumentException(op + " requires a non-empty matrix");
        }
    }
}
