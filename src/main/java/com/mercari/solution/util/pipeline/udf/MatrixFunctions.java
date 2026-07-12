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
 * <li><b>{@code LINEAR_REG(y, xs [, lambda])}</b> — aggregate: multivariate
 *     linear regression of {@code y} on the feature vector {@code xs}
 *     ({@code ARRAY[x1, x2, …]}, same length every row), an intercept is
 *     prepended automatically; accumulates the Gram matrix (mergeable) and
 *     solves at result time. The optional {@code lambda} (a constant ≥ 0)
 *     is a ridge penalty on the non-intercept coefficients — the
 *     per-key-refit form of segment-wise regularized regression. Like
 *     SEQ_COLLECT the raw result is an opaque collection — feed it to
 *     {@code LINREG_PREDICT} or wrap it in {@code AS_DOUBLE_ARRAY} to
 *     project the coefficients ({@code [intercept, b1, …]}).</li>
 * <li><b>{@code LINREG_PREDICT(model, features)}</b> → {@code DOUBLE}: apply
 *     a LINEAR_REG result (or any {@code [intercept, b1, …]} coefficient
 *     array) to a feature vector — train on history and predict the current
 *     element in one statement.</li>
 * <li><b>{@code BAYES_LINREG(y, xs, priorPrecision [, noiseVar])}</b> —
 *     aggregate: Bayesian linear regression with an isotropic Gaussian
 *     prior {@code N(0, priorPrecision⁻¹ I)} over all coefficients
 *     (intercept included). With {@code noiseVar} the posterior is exact;
 *     without it the noise variance is estimated from the ridge-fit
 *     residuals ({@code RSS / max(1, n - k)}). The opaque result carries the
 *     posterior mean and covariance — consume it with {@code BAYES_PREDICT};
 *     the layout is internal.</li>
 * <li><b>{@code BAYES_PREDICT(model, features)}</b> → {@code ARRAY<DOUBLE>}
 *     {@code [mean, sd]}: posterior-predictive mean and standard deviation
 *     (observation noise included) of a BAYES_LINREG model at a feature
 *     vector — uncertainty-aware forecasts for downstream decisions.</li>
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

    /** Applies a {@code [intercept, b1, …]} coefficient vector to a feature vector. */
    public static Double linregPredict(Object model, Object features) {
        final double[] coefs = vector("LINREG_PREDICT", model);
        final double[] x = vector("LINREG_PREDICT", features);
        if (coefs == null || x == null) {
            return null;
        }
        if (coefs.length != x.length + 1) {
            throw new IllegalArgumentException("LINREG_PREDICT model has " + coefs.length
                    + " coefficients (intercept included), which does not fit "
                    + x.length + " features");
        }
        double out = coefs[0];
        for (int i = 0; i < x.length; i++) {
            out += coefs[i + 1] * x[i];
        }
        return out;
    }

    /**
     * Posterior-predictive {@code [mean, sd]} of a {@code BAYES_LINREG} model
     * at a feature vector (observation noise included in the variance).
     */
    public static List bayesPredict(Object model, Object features) {
        final double[] packed = vector("BAYES_PREDICT", model);
        final double[] x = vector("BAYES_PREDICT", features);
        if (packed == null || x == null) {
            return null;
        }
        if (packed.length < 2) {
            throw new IllegalArgumentException(
                    "BAYES_PREDICT model is not a BAYES_LINREG result");
        }
        final int k = (int) packed[0];
        if (k < 1 || packed.length != 2 + k + k * k) {
            throw new IllegalArgumentException(
                    "BAYES_PREDICT model is not a BAYES_LINREG result");
        }
        if (x.length + 1 != k) {
            throw new IllegalArgumentException("BAYES_PREDICT model has " + k
                    + " coefficients (intercept included), which does not fit "
                    + x.length + " features");
        }
        final double noiseVar = packed[1];
        final double[] row = new double[k];
        row[0] = 1d;
        System.arraycopy(x, 0, row, 1, x.length);
        double mean = 0d;
        double quadratic = 0d;
        for (int i = 0; i < k; i++) {
            mean += packed[2 + i] * row[i];
            double covRow = 0d;
            for (int j = 0; j < k; j++) {
                covRow += packed[2 + k + i * k + j] * row[j];
            }
            quadratic += row[i] * covRow;
        }
        final double variance = Math.max(0d, noiseVar + quadratic);
        final List<Double> out = new ArrayList<>(2);
        out.add(mean);
        out.add(Math.sqrt(variance));
        return out;
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
            public double ridge = Double.NaN;
        }

        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object y, Object xs) {
            addRow(acc, y, xs);
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            return mergeAcc(a, b);
        }

        public List<Double> result(Acc acc) {
            return solveAcc(acc);
        }

        /** Accumulates one row; returns whether the row counted (non-NULL). */
        static boolean addRow(Acc acc, Object y, Object xs) {
            if (y == null || xs == null) {
                return false;
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
            return true;
        }

        static <A extends Acc> A mergeAcc(A a, A b) {
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
            if (Double.isNaN(a.ridge)) {
                a.ridge = b.ridge;
            }
            return a;
        }

        /** Solves the normal equations, penalizing the non-intercept diagonal only. */
        static List<Double> solveAcc(Acc acc) {
            if (acc.xtx == null) {
                return null;
            }
            final double ridge = Double.isNaN(acc.ridge) ? 0d : acc.ridge;
            double[][] xtx = acc.xtx;
            if (ridge > 0) {
                xtx = new double[acc.xtx.length][];
                for (int i = 0; i < acc.xtx.length; i++) {
                    xtx[i] = acc.xtx[i].clone();
                    if (i > 0) {
                        xtx[i][i] += ridge;
                    }
                }
            }
            return MatrixOps.toList(MatrixOps.solveGram(xtx, acc.xty, 0d));
        }
    }

    /**
     * {@code LINEAR_REG(y, xs, lambda)} — the ridge overload: {@code lambda}
     * (a constant ≥ 0) penalizes the non-intercept coefficients.
     */
    public static class LinearRegRidgeHost {

        public LinearRegHost.Acc init() {
            return new LinearRegHost.Acc();
        }

        public LinearRegHost.Acc add(LinearRegHost.Acc acc, Object y, Object xs, Object lambda) {
            if (!(lambda instanceof Number l)) {
                throw new IllegalArgumentException("LINEAR_REG lambda must be a number >= 0,"
                        + " but got " + (lambda == null ? "NULL" : lambda.getClass().getSimpleName()));
            }
            final double ridge = l.doubleValue();
            if (ridge < 0) {
                throw new IllegalArgumentException(
                        "LINEAR_REG lambda must be >= 0, but was " + ridge);
            }
            acc.ridge = ridge;
            LinearRegHost.addRow(acc, y, xs);
            return acc;
        }

        public LinearRegHost.Acc merge(LinearRegHost.Acc a, LinearRegHost.Acc b) {
            return LinearRegHost.mergeAcc(a, b);
        }

        public List<Double> result(LinearRegHost.Acc acc) {
            return LinearRegHost.solveAcc(acc);
        }
    }

    /**
     * {@code BAYES_LINREG} accumulator host (see the class javadoc for the
     * model). The opaque result packs {@code [k, noiseVar, mean(k),
     * cov(k×k) row-major]} with {@code k} the coefficient count (intercept
     * first) — {@code BAYES_PREDICT} is the consumer; the layout is internal.
     */
    public static class BayesLinRegHost {

        /** The mutable accumulator; public because generated code manipulates it. */
        public static class Acc extends LinearRegHost.Acc {
            public double yty;
            public long n;
            public double alpha = Double.NaN;
            public double noiseVar = Double.NaN;
        }

        public Acc init() {
            return new Acc();
        }

        public Acc add(Acc acc, Object y, Object xs, Object priorPrecision) {
            setAlpha(acc, priorPrecision);
            accumulate(acc, y, xs);
            return acc;
        }

        public Acc merge(Acc a, Acc b) {
            return mergeBayes(a, b);
        }

        public List<Double> result(Acc acc) {
            return posterior(acc);
        }

        /** Gram merge may keep either instance — fold the other side's scalars into it. */
        static Acc mergeBayes(Acc a, Acc b) {
            final Acc out = LinearRegHost.mergeAcc(a, b);
            final Acc other = out == a ? b : a;
            out.yty += other.yty;
            out.n += other.n;
            if (Double.isNaN(out.alpha)) {
                out.alpha = other.alpha;
            }
            if (Double.isNaN(out.noiseVar)) {
                out.noiseVar = other.noiseVar;
            }
            return out;
        }

        static void setAlpha(Acc acc, Object priorPrecision) {
            if (!(priorPrecision instanceof Number a)) {
                throw new IllegalArgumentException("BAYES_LINREG priorPrecision must be a number"
                        + " > 0, but got " + (priorPrecision == null
                                ? "NULL" : priorPrecision.getClass().getSimpleName()));
            }
            final double alpha = a.doubleValue();
            if (alpha <= 0) {
                throw new IllegalArgumentException(
                        "BAYES_LINREG priorPrecision must be > 0, but was " + alpha);
            }
            acc.alpha = alpha;
        }

        static void accumulate(Acc acc, Object y, Object xs) {
            if (LinearRegHost.addRow(acc, y, xs)) {
                final double value = ((Number) y).doubleValue();
                acc.yty += value * value;
                acc.n++;
            }
        }

        static List<Double> posterior(Acc acc) {
            if (acc.xtx == null) {
                return null;
            }
            final int k = acc.xty.length;
            double noiseVar = acc.noiseVar;
            if (Double.isNaN(noiseVar)) {
                // Estimate the noise variance from the ridge-fit residuals
                // (lambda = alpha as the working penalty).
                final double[] m0 = MatrixOps.solveGram(acc.xtx, acc.xty, acc.alpha);
                final double rss = acc.yty - 2 * MatrixOps.dot(m0, acc.xty)
                        + MatrixOps.dot(m0, MatrixOps.multiply(acc.xtx, m0));
                noiseVar = Math.max(Math.max(rss, 0d) / Math.max(1, acc.n - k), 1e-12);
            }
            final double beta = 1d / noiseVar;
            final double[][] precision = new double[k][k];
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    precision[i][j] = beta * acc.xtx[i][j];
                }
                precision[i][i] += acc.alpha;
            }
            final double[][] cov = MatrixOps.inverse(precision);
            final double[] mean = MatrixOps.multiply(cov, acc.xty);
            final List<Double> out = new ArrayList<>(2 + k + k * k);
            out.add((double) k);
            out.add(noiseVar);
            for (int i = 0; i < k; i++) {
                out.add(beta * mean[i]);
            }
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    out.add(cov[i][j]);
                }
            }
            return out;
        }
    }

    /** {@code BAYES_LINREG(y, xs, priorPrecision, noiseVar)} — the known-noise overload. */
    public static class BayesLinRegNoiseHost {

        public BayesLinRegHost.Acc init() {
            return new BayesLinRegHost.Acc();
        }

        public BayesLinRegHost.Acc add(BayesLinRegHost.Acc acc, Object y, Object xs,
                Object priorPrecision, Object noiseVar) {
            BayesLinRegHost.setAlpha(acc, priorPrecision);
            if (!(noiseVar instanceof Number v)) {
                throw new IllegalArgumentException("BAYES_LINREG noiseVar must be a number > 0,"
                        + " but got " + (noiseVar == null
                                ? "NULL" : noiseVar.getClass().getSimpleName()));
            }
            final double variance = v.doubleValue();
            if (variance <= 0) {
                throw new IllegalArgumentException(
                        "BAYES_LINREG noiseVar must be > 0, but was " + variance);
            }
            acc.noiseVar = variance;
            BayesLinRegHost.accumulate(acc, y, xs);
            return acc;
        }

        public BayesLinRegHost.Acc merge(BayesLinRegHost.Acc a, BayesLinRegHost.Acc b) {
            return BayesLinRegHost.mergeBayes(a, b);
        }

        public List<Double> result(BayesLinRegHost.Acc acc) {
            return BayesLinRegHost.posterior(acc);
        }
    }

    /** The Calcite function objects. */
    static List<Map.Entry<String, Function>> builtIns() {
        final Function linearReg = aggregate("LINEAR_REG", LinearRegHost.class);
        final Function linearRegRidge = aggregate("LINEAR_REG", LinearRegRidgeHost.class);
        final Function bayesLinReg = aggregate("BAYES_LINREG", BayesLinRegHost.class);
        final Function bayesLinRegNoise = aggregate("BAYES_LINREG", BayesLinRegNoiseHost.class);
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
                Map.entry("LINREG_PREDICT",
                        new DoubleFunction("linregPredict", "model", "features")),
                Map.entry("BAYES_PREDICT",
                        new DoubleArrayFunction("bayesPredict", "model", "features")),
                Map.entry("LINEAR_REG", linearReg),
                Map.entry("LINEAR_REG", linearRegRidge),
                Map.entry("BAYES_LINREG", bayesLinReg),
                Map.entry("BAYES_LINREG", bayesLinRegNoise));
    }

    private static Function aggregate(String name, Class<?> host) {
        final Function function = AggregateFunctionImpl.create(host);
        if (function == null) {
            throw new IllegalStateException(
                    name + " host does not match the aggregate convention");
        }
        return function;
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
