package com.mercari.solution.util.domain.ml;

import com.mercari.solution.util.domain.math.MatrixOps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Linear model fitting (OLS / ridge / lasso / PLS) on the shared ojalgo core
 * ({@link MatrixOps}) — formerly commons-math3. OLS and ridge go through the
 * normal-equation solver ({@code MatrixOps.solveGram}: Cholesky with an SVD
 * fallback) instead of the explicit matrix inverse the old implementation
 * used; lasso keeps its iterative-reweighting scheme but with a fixed random
 * seed, so results are reproducible. Coefficient matrices are plain
 * {@code double[][]} in (features × outputs) orientation.
 */
public class LinearModelUtil implements Serializable {

    /** Fixed seed for lasso's random initialization (reproducible fits). */
    private static final long LASSO_SEED = 42L;

    public static LinearModel olsModel(double[][] X, double[][] Y) {
        return LinearModel.of(ols(X, Y));
    }

    public static double[][] ols(double[][] X, double[][] Y) {
        return MatrixOps.solveGram(gram(X), crossProduct(X, Y), 0D);
    }

    public static LinearModel ridgeModel(double[][] X, double[][] Y, double alpha) {
        return LinearModel.of(ridge(X, Y, alpha));
    }

    public static double[][] ridge(double[][] X, double[][] Y, double alpha) {
        return MatrixOps.solveGram(gram(X), crossProduct(X, Y), alpha);
    }

    public static LinearModel lassoModel(double[][] X, double[][] Y,
                                         double alpha, int maxIteration, double tolerance) {
        return LinearModel.of(lasso(X, Y, alpha, maxIteration, tolerance));
    }

    /**
     * Lasso via iterative reweighting: each step solves the normal equations
     * with an L1-derived diagonal penalty {@code alpha / |beta_i|} until the
     * coefficients move less than {@code tolerance}.
     */
    public static double[][] lasso(double[][] X, double[][] Y,
                                   double alpha, int maxIteration, double tolerance) {

        final int features = X[0].length;
        final int outputs = Y[0].length;
        final double[][] xtx = gram(X);
        final double[][] xty = crossProduct(X, Y);

        final Random random = new Random(LASSO_SEED);
        // (outputs × features), matching the historical iteration layout.
        double[][] beta = new double[outputs][features];
        for (int i = 0; i < outputs; i++) {
            for (int j = 0; j < features; j++) {
                beta[i][j] = random.nextDouble();
            }
        }

        for (int iteration = 0; iteration < maxIteration; iteration++) {
            // Pseudo-inverse of diag(|beta[0]|): reciprocal of the non-zero entries.
            final double[][] penalized = new double[features][];
            for (int i = 0; i < features; i++) {
                penalized[i] = xtx[i].clone();
                final double weight = Math.abs(beta[0][i]);
                if (weight > 0) {
                    penalized[i][i] += alpha / weight;
                }
            }
            final double[][] newBeta = MatrixOps.solveGram(penalized, xty, 0D);
            double epsilon = 0;
            for (int i = 0; i < outputs; i++) {
                for (int j = 0; j < features; j++) {
                    final double diff = beta[i][j] - newBeta[j][i];
                    epsilon += diff * diff;
                }
            }
            if (Math.sqrt(epsilon) < tolerance) {
                break;
            }
            beta = transpose(newBeta);
        }

        return transpose(beta);
    }

    public static LogisticModel logisticBinaryModel(double[][] X, double[][] Y,
                                                    double l2, int maxIteration, double tolerance) {
        return LogisticModel.of(logisticRegressionBinary(X, Y, l2, maxIteration, tolerance));
    }

    /**
     * Binary logistic regression by IRLS (Newton-Raphson): each step solves
     * {@code (X^T W X + l2·I) Δβ = X^T (y − p) − l2·β} with
     * {@code W = diag(p(1−p))} through {@link MatrixOps#solveGram}. {@code Y}
     * is n×1 of 0/1 labels; the result is (features × 1). β starts at zero
     * (deterministic); use {@code l2 > 0} on separable data, where the
     * unpenalized MLE diverges.
     */
    public static double[][] logisticRegressionBinary(double[][] X, double[][] Y,
                                                      double l2, int maxIteration, double tolerance) {
        final int n = X.length;
        final int d = X[0].length;
        if (Y.length != n || Y[0].length != 1) {
            throw new IllegalArgumentException("Y must be n x 1 of 0/1 labels, but was "
                    + Y.length + " x " + Y[0].length);
        }
        final double[] y = new double[n];
        for (int i = 0; i < n; i++) {
            if (Y[i][0] != 0D && Y[i][0] != 1D) {
                throw new IllegalArgumentException("Y must contain only 0/1, but row "
                        + i + " holds " + Y[i][0]);
            }
            y[i] = Y[i][0];
        }
        checkPenalty(l2);

        final double[] beta = new double[d];
        for (int iteration = 0; iteration < maxIteration; iteration++) {
            final double[][] hessian = new double[d][d];
            final double[] gradient = new double[d];
            for (int i = 0; i < n; i++) {
                final double p = sigmoid(MatrixOps.dot(X[i], beta));
                final double w = Math.max(p * (1 - p), PROBABILITY_CLAMP);
                final double residual = y[i] - p;
                for (int a = 0; a < d; a++) {
                    gradient[a] += X[i][a] * residual;
                    for (int b = 0; b < d; b++) {
                        hessian[a][b] += X[i][a] * w * X[i][b];
                    }
                }
            }
            for (int a = 0; a < d; a++) {
                gradient[a] -= l2 * beta[a];
                hessian[a][a] += l2;
            }
            final double[] delta = MatrixOps.solveGram(hessian, gradient, 0D);
            double epsilon = 0;
            for (int a = 0; a < d; a++) {
                beta[a] += delta[a];
                epsilon += delta[a] * delta[a];
            }
            if (Math.sqrt(epsilon) < tolerance) {
                break;
            }
        }

        final double[][] out = new double[d][1];
        for (int a = 0; a < d; a++) {
            out[a][0] = beta[a];
        }
        return out;
    }

    public static LogisticModel logisticMultiModel(double[][] X, double[][] Y,
                                                   double l2, int maxIteration, double tolerance) {
        return LogisticModel.of(logisticRegressionMulti(X, Y, l2, maxIteration, tolerance));
    }

    /**
     * Multinomial (softmax) logistic regression by the block-Newton method:
     * {@code Y} is n×K one-hot, the last class is the reference, and each step
     * solves the full (K−1)d Hessian system — blocks
     * {@code H_kl = X^T diag(p_k(δ_kl − p_l)) X + l2·I} — through
     * {@link MatrixOps#solveGram}. The result is (features × K−1); the
     * reference class's coefficients are implicitly zero.
     */
    public static double[][] logisticRegressionMulti(double[][] X, double[][] Y,
                                                     double l2, int maxIteration, double tolerance) {
        final int n = X.length;
        final int d = X[0].length;
        final int numClasses = Y[0].length;
        if (numClasses < 2) {
            throw new IllegalArgumentException("Y must be n x K one-hot with K >= 2, but K was " + numClasses);
        }
        if (Y.length != n) {
            throw new IllegalArgumentException("X and Y row counts must match, but got "
                    + n + " and " + Y.length);
        }
        for (int i = 0; i < n; i++) {
            double sum = 0;
            for (final double value : Y[i]) {
                if (value != 0D && value != 1D) {
                    throw new IllegalArgumentException("Y must be one-hot, but row " + i
                            + " holds " + value);
                }
                sum += value;
            }
            if (sum != 1D) {
                throw new IllegalArgumentException("Y must be one-hot (exactly one 1 per row),"
                        + " but row " + i + " sums to " + sum);
            }
        }
        checkPenalty(l2);

        final int m = numClasses - 1;
        // Stacked coefficients: theta[k * d + a] = beta_k[a].
        final double[] theta = new double[m * d];
        final double[] probabilities = new double[m];
        for (int iteration = 0; iteration < maxIteration; iteration++) {
            final double[][] hessian = new double[m * d][m * d];
            final double[] gradient = new double[m * d];
            for (int i = 0; i < n; i++) {
                softmax(X[i], theta, d, probabilities);
                for (int k = 0; k < m; k++) {
                    final double residual = Y[i][k] - probabilities[k];
                    for (int a = 0; a < d; a++) {
                        gradient[k * d + a] += X[i][a] * residual;
                    }
                    for (int l = 0; l < m; l++) {
                        final double w = k == l
                                ? Math.max(probabilities[k] * (1 - probabilities[k]), PROBABILITY_CLAMP)
                                : -probabilities[k] * probabilities[l];
                        for (int a = 0; a < d; a++) {
                            final double xw = X[i][a] * w;
                            for (int b = 0; b < d; b++) {
                                hessian[k * d + a][l * d + b] += xw * X[i][b];
                            }
                        }
                    }
                }
            }
            for (int j = 0; j < m * d; j++) {
                gradient[j] -= l2 * theta[j];
                hessian[j][j] += l2;
            }
            final double[] delta = MatrixOps.solveGram(hessian, gradient, 0D);
            double epsilon = 0;
            for (int j = 0; j < m * d; j++) {
                theta[j] += delta[j];
                epsilon += delta[j] * delta[j];
            }
            if (Math.sqrt(epsilon) < tolerance) {
                break;
            }
        }

        final double[][] out = new double[d][m];
        for (int k = 0; k < m; k++) {
            for (int a = 0; a < d; a++) {
                out[a][k] = theta[k * d + a];
            }
        }
        return out;
    }

    private static final double PROBABILITY_CLAMP = 1e-10;

    private static double sigmoid(final double z) {
        final double p = 1 / (1 + Math.exp(-z));
        return Math.min(1 - PROBABILITY_CLAMP, Math.max(PROBABILITY_CLAMP, p));
    }

    /** Softmax over the m non-reference scores and the implicit 0 of the reference class. */
    private static void softmax(final double[] x, final double[] theta, final int d, final double[] out) {
        final int m = out.length;
        double max = 0;  // the reference class's score
        for (int k = 0; k < m; k++) {
            double score = 0;
            for (int a = 0; a < d; a++) {
                score += x[a] * theta[k * d + a];
            }
            out[k] = score;
            max = Math.max(max, score);
        }
        double denominator = Math.exp(-max);  // the reference class
        for (int k = 0; k < m; k++) {
            out[k] = Math.exp(out[k] - max);
            denominator += out[k];
        }
        for (int k = 0; k < m; k++) {
            out[k] = Math.min(1 - PROBABILITY_CLAMP, Math.max(PROBABILITY_CLAMP, out[k] / denominator));
        }
    }

    private static void checkPenalty(final double l2) {
        if (l2 < 0) {
            throw new IllegalArgumentException("l2 must be >= 0, but was " + l2);
        }
    }

    public static double[][] pls1(double[][] X, double[] y, int components) {
        final double[][] Y = new double[y.length][1];
        for (int i = 0; i < y.length; i++) {
            Y[i][0] = y[i];
        }

        double[][] matrixX = copy(X);
        double[][] matrixY = Y;

        final int xd = X[0].length;
        final double[][] W = new double[xd][components];
        final double[][] P = new double[xd][components];
        final double[][] D = new double[components][1];

        for (int r = 0; r < components; r++) {
            final double[][] xy = crossProduct(matrixX, matrixY);
            final double norm = frobeniusNorm(xy);
            final double[] w = new double[xd];
            for (int i = 0; i < xd; i++) {
                w[i] = xy[i][0] / norm;
            }
            final double[] t = multiplyVector(matrixX, w);
            final double a = 1 / MatrixOps.dot(t, t);
            final double[] p = scale(multiplyVectorTransposed(matrixX, t), a);
            final double d = MatrixOps.dot(columnOf(matrixY, 0), t) * a;

            matrixX = deflate(matrixX, t, p);
            matrixY = deflate(matrixY, t, new double[]{d});

            setColumn(W, r, w);
            setColumn(P, r, p);
            D[r][0] = d;
        }
        // W (P^T W)^-1 D
        return multiply(W, MatrixOps.solve(multiply(transpose(P), W), D));
    }

    public static LinearModel pls2Model(double[][] X, double[][] Y, int components) {
        return LinearModel.of(pls2(X, Y, components));
    }

    public static double[][] pls2(double[][] X, double[][] Y, int components) {

        double[][] matrixX = copy(X);
        double[][] matrixY = copy(Y);

        final int xd = X[0].length;
        final int yd = Y[0].length;

        final double[][] W = new double[xd][components];
        final double[][] P = new double[xd][components];
        final double[][] Q = new double[yd][components];

        for (int r = 0; r < components; r++) {
            final double[] w = MatrixOps.firstRightSingularVector(
                    multiply(transpose(matrixY), matrixX));
            final double[] t = multiplyVector(matrixX, w);
            final double a = 1 / MatrixOps.dot(t, t);
            final double[] p = scale(multiplyVectorTransposed(matrixX, t), a);
            final double[] q = scale(multiplyVectorTransposed(matrixY, t), a);

            matrixX = deflate(matrixX, t, p);
            matrixY = deflate(matrixY, t, q);

            setColumn(W, r, w);
            setColumn(P, r, p);
            setColumn(Q, r, q);
        }
        // W (P^T W)^-1 Q^T
        return multiply(W, MatrixOps.solve(multiply(transpose(P), W), transpose(Q)));
    }

    public static List<List<Double>> calcStandardizeParams(double[][] data) {
        if(data == null || data.length == 0 || data[0].length == 0) {
            return new ArrayList<>();
        }
        final List<List<Double>> centers = new ArrayList<>();
        final int cols = data[0].length;
        for(int col=0; col<cols; col++) {
            double avg = 0;
            double var = 0;
            double count = 0;
            for(int row=0; row<data.length; row++) {
                double value = data[row][col];
                count += 1;
                double delta = value - avg;
                avg += (delta / count);
                var += (delta * (value - avg));
            }
            final double std = Math.sqrt(var / count);
            final List<Double> center = new ArrayList<>();
            center.add(avg);
            center.add(std);
            center.add(var / count);
            centers.add(center);
        }
        return centers;
    }

    public static double[][] standardize(double[][] data, List<List<Double>> params) {
        return standardize(data, params, false);
    }
    public static double[][] standardize(double[][] data, List<List<Double>> params, boolean skipStd) {

        if(data == null) {
            throw new RuntimeException();
        } else if(data.length == 0 || data[0].length == 0) {
            //return new double[0][];
            throw new RuntimeException();
        }

        final double[][] standardized = new double[data.length][data[0].length];

        final int cols = data[0].length;
        for(int col=0; col<cols; col++) {
            double avg = params.get(col).get(0);
            double std = params.get(col).get(1);
            for(int row=0; row<data.length; row++) {
                double value = data[row][col];
                if(std == 0) {
                    standardized[row][col] = value;
                } else if(skipStd) {
                    standardized[row][col] = (value - avg);
                } else {
                    standardized[row][col] = (value - avg) / std;
                }
            }
        }

        return standardized;
    }

    public static double[][] addIntercept(double[][] data) {

        if(data == null) {
            return null;
        } else if(data.length == 0 || data[0].length == 0) {
            return new double[0][];
        }

        final double[][] standardized = new double[data.length][data[0].length + 1];

        final int cols = data[0].length;
        for(int col=0; col<cols; col++) {
            for(int row=0; row<data.length; row++) {
                standardized[row][col] = data[row][col];
            }
        }
        for(int row=0; row<data.length; row++) {
            standardized[row][cols] = data[row][cols];
        }

        return standardized;
    }

    public static class LinearModel implements Model {

        private final int inputSize;
        private final int outputSize;
        private List<List<Double>> weights;

        public int getInputSize() {
            return inputSize;
        }

        public int getOutputSize() {
            return outputSize;
        }

        public List<List<Double>> getWeights() {
            return weights;
        }

        /** The coefficient matrix in (features × outputs) orientation. */
        public double[][] getBeta() {
            final double[][] beta = new double[inputSize][outputSize];
            for(int output=0; output<outputSize; output++) {
                for(int input=0; input<inputSize; input++) {
                    beta[input][output] = weights.get(output).get(input);
                }
            }
            return beta;
        }

        public void setWeights(List<List<Double>> weights) {
            this.weights = weights;
        }

        public void setWeights(double[][] beta) {
            this.weights.clear();
            for(int i=0; i<beta.length; i++) {
                final List<Double> b = new ArrayList<>();
                for(int j=0; j<beta[0].length; j++) {
                    b.add(beta[i][j]);
                }
                this.weights.add(b);
            }
        }

        LinearModel(final int inputSize, final int outputSize, final List<List<Double>> weights) {
            this.inputSize = inputSize;
            this.outputSize = outputSize;
            this.weights = weights;
        }

        /** From a (features × outputs) coefficient matrix. */
        LinearModel(double[][] beta) {
            this.inputSize = beta.length;
            this.outputSize = beta[0].length;
            this.weights = new ArrayList<>();
            for(int output=0; output<this.outputSize; output++) {
                final List<Double> b = new ArrayList<>();
                for(int input=0; input<this.inputSize; input++) {
                    b.add(beta[input][output]);
                }
                weights.add(b);
            }
        }

        public static LinearModel of(final double[][] beta) {
            return new LinearModel(beta);
        }

        public static LinearModel of(final int inputSize, final int outputSize, final List<List<Double>> weights) {
            final LinearModel model = new LinearModel(inputSize, outputSize, weights);
            return model;
        }

        @Override
        public void update(List<String> fields, Map<String, Double> values) {

        }

        @Override
        public List<Double> inference(double[] x) {
            final List<Double> results = new ArrayList<>();
            for(final List<Double> b : weights) {
                double r = 0;
                for(int i=0; i<x.length; i++) {
                    r += (b.get(i) * x[i]);
                }
                if(b.size() > x.length) {
                    r += b.get(b.size() - 1);
                }
                results.add(r);
            }
            return results;
        }

        @Override
        public List<Double> inference(List<String> fields, Map<String, Double> values) {
            final List<Double> results = new ArrayList<>();
            for(final List<Double> b : weights) {
                double r = 0;
                for(int i=0; i<fields.size(); i++) {
                    final Double v = values.get(fields.get(i));
                    r += (b.get(i) * v);
                }
                if(b.size() > fields.size()) {
                    r += b.get(b.size() - 1);
                }
                results.add(r);
            }
            return results;
        }

        @Override
        public String toString() {
            return "input size: " + inputSize + ", output size: " + outputSize + ", weights: " + weights.toString();
        }

    }

    /**
     * A fitted logistic classifier: holds the (features × K−1) coefficients of
     * {@code logisticRegressionBinary} / {@code logisticRegressionMulti} and —
     * unlike {@link LinearModel}, whose inference is linear — returns
     * <em>class probabilities</em>: softmax over the K−1 scored classes plus
     * the implicit reference class (last element). A binary model yields
     * {@code [P(y=1), P(y=0)]}.
     */
    public static class LogisticModel implements Model {

        private final int inputSize;
        private final int numClasses;
        private final List<List<Double>> weights;

        public int getInputSize() {
            return inputSize;
        }

        public int getNumClasses() {
            return numClasses;
        }

        /** Per non-reference class: {@code weights.get(k)} over inputs. */
        public List<List<Double>> getWeights() {
            return weights;
        }

        /** From a (features × K−1) coefficient matrix. */
        LogisticModel(double[][] beta) {
            this.inputSize = beta.length;
            this.numClasses = beta[0].length + 1;
            this.weights = new ArrayList<>();
            for(int k=0; k<beta[0].length; k++) {
                final List<Double> b = new ArrayList<>();
                for(int input=0; input<this.inputSize; input++) {
                    b.add(beta[input][k]);
                }
                weights.add(b);
            }
        }

        public static LogisticModel of(final double[][] beta) {
            return new LogisticModel(beta);
        }

        @Override
        public void update(List<String> fields, Map<String, Double> values) {

        }

        @Override
        public List<Double> inference(double[] x) {
            final double[] scores = new double[weights.size()];
            for(int k=0; k<weights.size(); k++) {
                final List<Double> b = weights.get(k);
                double score = 0;
                for(int i=0; i<x.length; i++) {
                    score += (b.get(i) * x[i]);
                }
                scores[k] = score;
            }
            return probabilities(scores);
        }

        @Override
        public List<Double> inference(List<String> fields, Map<String, Double> values) {
            final double[] x = new double[fields.size()];
            for(int i=0; i<fields.size(); i++) {
                x[i] = values.get(fields.get(i));
            }
            return inference(x);
        }

        private static List<Double> probabilities(final double[] scores) {
            double max = 0;  // the reference class's score
            for(final double score : scores) {
                max = Math.max(max, score);
            }
            final double[] exps = new double[scores.length];
            double denominator = Math.exp(-max);
            for(int k=0; k<scores.length; k++) {
                exps[k] = Math.exp(scores[k] - max);
                denominator += exps[k];
            }
            final List<Double> results = new ArrayList<>();
            for(int k=0; k<scores.length; k++) {
                results.add(exps[k] / denominator);
            }
            results.add(Math.exp(-max) / denominator);
            return results;
        }

        @Override
        public String toString() {
            return "input size: " + inputSize + ", classes: " + numClasses + ", weights: " + weights.toString();
        }

    }

    public static String toString(double[] values) {
        final List<Double> doubles = new ArrayList<>();
        for(double d : values) {
            doubles.add(d);
        }
        return doubles.toString();
    }

    private static double[][] gram(final double[][] X) {
        return multiply(transpose(X), X);
    }

    /** X^T Y — the cross-product matrix of the normal equations. */
    private static double[][] crossProduct(final double[][] X, final double[][] Y) {
        return multiply(transpose(X), Y);
    }

    private static double[][] multiply(final double[][] a, final double[][] b) {
        final double[][] out = new double[a.length][b[0].length];
        for (int i = 0; i < a.length; i++) {
            for (int k = 0; k < b.length; k++) {
                final double value = a[i][k];
                if (value == 0) {
                    continue;
                }
                for (int j = 0; j < b[0].length; j++) {
                    out[i][j] += value * b[k][j];
                }
            }
        }
        return out;
    }

    private static double[] multiplyVector(final double[][] m, final double[] v) {
        final double[] out = new double[m.length];
        for (int i = 0; i < m.length; i++) {
            out[i] = MatrixOps.dot(m[i], v);
        }
        return out;
    }

    /** {@code m^T v} without materializing the transpose. */
    private static double[] multiplyVectorTransposed(final double[][] m, final double[] v) {
        final double[] out = new double[m[0].length];
        for (int i = 0; i < m.length; i++) {
            for (int j = 0; j < out.length; j++) {
                out[j] += m[i][j] * v[i];
            }
        }
        return out;
    }

    private static double[][] transpose(final double[][] m) {
        final double[][] out = new double[m[0].length][m.length];
        for (int i = 0; i < m.length; i++) {
            for (int j = 0; j < m[0].length; j++) {
                out[j][i] = m[i][j];
            }
        }
        return out;
    }

    /** Rank-one deflation: {@code m - t p^T}. */
    private static double[][] deflate(final double[][] m, final double[] t, final double[] p) {
        final double[][] out = new double[m.length][m[0].length];
        for (int i = 0; i < m.length; i++) {
            for (int j = 0; j < m[0].length; j++) {
                out[i][j] = m[i][j] - t[i] * p[j];
            }
        }
        return out;
    }

    private static double[] scale(final double[] v, final double a) {
        final double[] out = new double[v.length];
        for (int i = 0; i < v.length; i++) {
            out[i] = v[i] * a;
        }
        return out;
    }

    private static double[] columnOf(final double[][] m, final int column) {
        final double[] out = new double[m.length];
        for (int i = 0; i < m.length; i++) {
            out[i] = m[i][column];
        }
        return out;
    }

    private static void setColumn(final double[][] m, final int column, final double[] values) {
        for (int i = 0; i < values.length; i++) {
            m[i][column] = values[i];
        }
    }

    private static double frobeniusNorm(final double[][] m) {
        double sum = 0;
        for (final double[] row : m) {
            for (final double value : row) {
                sum += value * value;
            }
        }
        return Math.sqrt(sum);
    }

    private static double[][] copy(final double[][] m) {
        final double[][] out = new double[m.length][];
        for (int i = 0; i < m.length; i++) {
            out[i] = m[i].clone();
        }
        return out;
    }
}
