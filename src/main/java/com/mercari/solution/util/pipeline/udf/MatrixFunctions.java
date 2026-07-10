package com.mercari.solution.util.pipeline.udf;

import com.mercari.solution.util.domain.math.MatrixOps;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ImplementableFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScalarFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Linear-algebra built-ins (always registered), thin adapters over
 * {@link MatrixOps} — the same core the {@code select} module's matrix
 * functions use. Vectors are {@code ARRAY<DOUBLE>}-shaped arguments, matrices
 * are nested arrays ({@code ARRAY[ARRAY[…], …]} literals or lookup columns);
 * MATRIX_MULTIPLY / MATRIX_SOLVE / MAHALANOBIS also take a trailing
 * {@code columns} overload for <em>flat row-major</em> matrices — the shape
 * of the project's {@code matrix}-typed fields (ONNX tensors, {@code reshape}
 * outputs), which surface in SQL as flat {@code ARRAY<DOUBLE>} columns.
 * Every function returns NULL when any array argument is NULL.
 *
 * <ul>
 * <li><b>{@code COSINE_SIMILARITY(a, b)}</b> → {@code DOUBLE}: cosine of two
 *     equal-length vectors (embedding similarity); NULL when either norm is
 *     zero.</li>
 * <li><b>{@code MATRIX_MULTIPLY(matrix, vector)}</b> → {@code ARRAY<DOUBLE>}:
 *     matrix-vector product — projecting an embedding through a fixed (PCA /
 *     linear-model) matrix.</li>
 * <li><b>{@code MATRIX_SOLVE(matrix, rhs)}</b> → {@code ARRAY<DOUBLE>}: SVD
 *     solve of {@code A x = b} (least-squares when overdetermined,
 *     minimum-norm when rank-deficient).</li>
 * <li><b>{@code MAHALANOBIS(x, mean, precision)}</b> → {@code DOUBLE}:
 *     Mahalanobis distance with a <em>precision</em> (inverse covariance)
 *     matrix — the anomaly score against a distribution fitted upstream.</li>
 * <li><b>{@code POLY_FIT(xs, ys, degree)}</b> → {@code ARRAY<DOUBLE>}:
 *     least-squares polynomial coefficients in ascending order
 *     ({@code POLY_FIT(...)[2]} of a degree-1 fit is the slope — SQL arrays
 *     are 1-based).</li>
 * <li><b>{@code LINEAR_REG(y, xs)}</b> — aggregate: multivariate linear
 *     regression of {@code y} on the feature vector {@code xs}
 *     ({@code ARRAY[x1, x2, …]}, same length every row), an intercept is
 *     prepended automatically; accumulates the Gram matrix (mergeable) and
 *     solves at result time. Like SEQ_COLLECT the raw result is an opaque
 *     collection — wrap it in {@code AS_DOUBLE_ARRAY} to project it.</li>
 * <li><b>{@code AS_DOUBLE_ARRAY(v)}</b> → {@code ARRAY<DOUBLE>}: re-types an
 *     opaque numeric collection (a LINEAR_REG result, an ARRAY_COMPACT'ed
 *     numeric array) into a projectable double array.</li>
 * </ul>
 */
public class MatrixFunctions {

    private MatrixFunctions() {
    }

    /** Reflective evaluation targets (array arguments arrive as Calcite-internal Lists). */
    public static Double cosineSimilarity(Object a, Object b) {
        final double[] left = vector("COSINE_SIMILARITY", a);
        final double[] right = vector("COSINE_SIMILARITY", b);
        if (left == null || right == null) {
            return null;
        }
        return wrap("COSINE_SIMILARITY", () -> MatrixOps.cosineSimilarity(left, right));
    }

    public static List matrixMultiply(Object matrix, Object vector) {
        final double[][] m = matrix("MATRIX_MULTIPLY", matrix);
        final double[] v = vector("MATRIX_MULTIPLY", vector);
        if (m == null || v == null) {
            return null;
        }
        return MatrixOps.toList(wrap("MATRIX_MULTIPLY", () -> MatrixOps.multiply(m, v)));
    }

    /** Flat-matrix overload: {@code matrix} is a row-major array split into {@code columns}-wide rows. */
    public static List matrixMultiply(Object matrix, Object vector, Object columns) {
        final double[][] m = flatMatrix("MATRIX_MULTIPLY", matrix, columns);
        final double[] v = vector("MATRIX_MULTIPLY", vector);
        if (m == null || v == null) {
            return null;
        }
        return MatrixOps.toList(wrap("MATRIX_MULTIPLY", () -> MatrixOps.multiply(m, v)));
    }

    public static List matrixSolve(Object matrix, Object rhs) {
        final double[][] m = matrix("MATRIX_SOLVE", matrix);
        final double[] b = vector("MATRIX_SOLVE", rhs);
        if (m == null || b == null) {
            return null;
        }
        return MatrixOps.toList(wrap("MATRIX_SOLVE", () -> MatrixOps.solve(m, b)));
    }

    /** Flat-matrix overload: {@code matrix} is a row-major array split into {@code columns}-wide rows. */
    public static List matrixSolve(Object matrix, Object rhs, Object columns) {
        final double[][] m = flatMatrix("MATRIX_SOLVE", matrix, columns);
        final double[] b = vector("MATRIX_SOLVE", rhs);
        if (m == null || b == null) {
            return null;
        }
        return MatrixOps.toList(wrap("MATRIX_SOLVE", () -> MatrixOps.solve(m, b)));
    }

    public static Double mahalanobis(Object x, Object mean, Object precision) {
        final double[] v = vector("MAHALANOBIS", x);
        final double[] m = vector("MAHALANOBIS", mean);
        final double[][] p = matrix("MAHALANOBIS", precision);
        if (v == null || m == null || p == null) {
            return null;
        }
        return wrap("MAHALANOBIS", () -> MatrixOps.mahalanobis(v, m, p));
    }

    /** Flat-matrix overload: {@code precision} is a row-major array split into {@code columns}-wide rows. */
    public static Double mahalanobis(Object x, Object mean, Object precision, Object columns) {
        final double[] v = vector("MAHALANOBIS", x);
        final double[] m = vector("MAHALANOBIS", mean);
        final double[][] p = flatMatrix("MAHALANOBIS", precision, columns);
        if (v == null || m == null || p == null) {
            return null;
        }
        return wrap("MAHALANOBIS", () -> MatrixOps.mahalanobis(v, m, p));
    }

    public static List polyFit(Object xs, Object ys, Object degree) {
        final double[] x = vector("POLY_FIT", xs);
        final double[] y = vector("POLY_FIT", ys);
        if (x == null || y == null) {
            return null;
        }
        if (!(degree instanceof Number d)) {
            throw new IllegalArgumentException("POLY_FIT degree must be a number, but got "
                    + (degree == null ? "NULL" : degree.getClass().getSimpleName()));
        }
        return MatrixOps.toList(wrap("POLY_FIT", () -> MatrixOps.polyfit(x, y, d.intValue())));
    }

    public static List asDoubleArray(Object value) {
        return MatrixOps.toList(vector("AS_DOUBLE_ARRAY", value));
    }

    private static double[] vector(String functionName, Object value) {
        try {
            return MatrixOps.toVector(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(functionName + ": " + e.getMessage());
        }
    }

    private static double[][] matrix(String functionName, Object value) {
        try {
            return MatrixOps.toMatrix(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(functionName + ": " + e.getMessage());
        }
    }

    private static double[][] flatMatrix(String functionName, Object value, Object columns) {
        if (!(columns instanceof Number c)) {
            throw new IllegalArgumentException(functionName + " columns must be a number, but got "
                    + (columns == null ? "NULL" : columns.getClass().getSimpleName()));
        }
        try {
            return MatrixOps.toMatrix(value, c.intValue());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(functionName + ": " + e.getMessage());
        }
    }

    private static <T> T wrap(String functionName, java.util.function.Supplier<T> body) {
        try {
            return body.get();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(functionName + ": " + e.getMessage());
        }
    }

    /**
     * {@code LINEAR_REG(y, xs)} accumulator host: sums {@code X^T X} /
     * {@code X^T y} (with a leading intercept column) so partial accumulators
     * merge additively; the solve happens once, in {@code result}. Rows with a
     * NULL y or NULL xs are skipped; a feature-vector length change mid-group
     * is an error.
     */
    public static class LinearRegHost {

        /** The mutable accumulator; public because generated code manipulates it. */
        public static class Acc {
            public double[][] xtx;
            public double[] xty;
        }

        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object y, Object xs) {
            if (y == null || xs == null) {
                return acc;
            }
            if (!(y instanceof Number number)) {
                throw new IllegalArgumentException("LINEAR_REG requires a numeric y, but got "
                        + y.getClass().getSimpleName());
            }
            final double[] features = vector("LINEAR_REG", xs);
            final double[] row = new double[features.length + 1];
            row[0] = 1d;
            System.arraycopy(features, 0, row, 1, features.length);
            if (acc.xtx == null) {
                acc.xtx = new double[row.length][row.length];
                acc.xty = new double[row.length];
            } else if (acc.xty.length != row.length) {
                throw new IllegalArgumentException("LINEAR_REG feature vectors must have equal"
                        + " lengths, but got " + (acc.xty.length - 1) + " and " + features.length);
            }
            for (int i = 0; i < row.length; i++) {
                for (int j = 0; j < row.length; j++) {
                    acc.xtx[i][j] += row[i] * row[j];
                }
                acc.xty[i] += row[i] * number.doubleValue();
            }
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            if (b.xtx == null) {
                return a;
            }
            if (a.xtx == null) {
                return b;
            }
            if (a.xty.length != b.xty.length) {
                throw new IllegalArgumentException("LINEAR_REG feature vectors must have equal"
                        + " lengths, but got " + (a.xty.length - 1) + " and " + (b.xty.length - 1));
            }
            for (int i = 0; i < a.xty.length; i++) {
                for (int j = 0; j < a.xty.length; j++) {
                    a.xtx[i][j] += b.xtx[i][j];
                }
                a.xty[i] += b.xty[i];
            }
            return a;
        }

        public List<Double> result(Acc acc) {
            if (acc.xtx == null) {
                return null;
            }
            return MatrixOps.toList(MatrixOps.solveGram(acc.xtx, acc.xty, 0d));
        }
    }

    /** The Calcite function objects. */
    static List<Map.Entry<String, Function>> builtIns() {
        final Function linearReg = AggregateFunctionImpl.create(LinearRegHost.class);
        if (linearReg == null) {
            throw new IllegalStateException(
                    "LINEAR_REG host does not match the aggregate convention");
        }
        return List.of(
                Map.entry("COSINE_SIMILARITY",
                        new DoubleFunction("cosineSimilarity", "a", "b")),
                Map.entry("MAHALANOBIS",
                        new DoubleFunction("mahalanobis", "x", "mean", "precision")),
                Map.entry("MAHALANOBIS",
                        new DoubleFunction("mahalanobis", "x", "mean", "precision", "columns")),
                Map.entry("MATRIX_MULTIPLY",
                        new DoubleArrayFunction("matrixMultiply", "matrix", "vector")),
                Map.entry("MATRIX_MULTIPLY",
                        new DoubleArrayFunction("matrixMultiply", "matrix", "vector", "columns")),
                Map.entry("MATRIX_SOLVE",
                        new DoubleArrayFunction("matrixSolve", "matrix", "rhs")),
                Map.entry("MATRIX_SOLVE",
                        new DoubleArrayFunction("matrixSolve", "matrix", "rhs", "columns")),
                Map.entry("POLY_FIT",
                        new DoubleArrayFunction("polyFit", "xs", "ys", "degree")),
                Map.entry("AS_DOUBLE_ARRAY",
                        new DoubleArrayFunction("asDoubleArray", "value")),
                Map.entry("LINEAR_REG", linearReg));
    }

    /** ANY-parameter scalar with an explicit return type (Object-typed reflective targets). */
    private abstract static class AnyParamFunction
            implements ScalarFunction, ImplementableFunction {

        private final Method method;
        private final String[] parameterNames;

        private AnyParamFunction(String methodName, String... parameterNames) {
            final Class<?>[] parameterTypes = new Class<?>[parameterNames.length];
            java.util.Arrays.fill(parameterTypes, Object.class);
            this.method = Types.lookupMethod(MatrixFunctions.class, methodName, parameterTypes);
            this.parameterNames = parameterNames;
        }

        @Override
        public List<FunctionParameter> getParameters() {
            return SequenceFunnelFunctions.parameters(parameterNames, ordinal -> SqlTypeName.ANY);
        }

        @Override
        public CallImplementor getImplementor() {
            return RexImpTable.createImplementor(
                    new ReflectiveCallNotNullImplementor(method), NullPolicy.NONE, false);
        }
    }

    private static final class DoubleFunction extends AnyParamFunction {

        private DoubleFunction(String methodName, String... parameterNames) {
            super(methodName, parameterNames);
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        }
    }

    private static final class DoubleArrayFunction extends AnyParamFunction {

        private DoubleArrayFunction(String methodName, String... parameterNames) {
            super(methodName, parameterNames);
        }

        @Override
        public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
            final RelDataType element = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
            return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(element, -1L), true);
        }
    }
}
