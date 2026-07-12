package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL-level tests for the linear-algebra built-ins (COSINE_SIMILARITY /
 * MATRIX_MULTIPLY / MATRIX_SOLVE / MAHALANOBIS / POLY_FIT scalar functions and
 * the LINEAR_REG aggregate + AS_DOUBLE_ARRAY companion), thin adapters over
 * {@code util/domain/math/MatrixOps} — the same core the select module's
 * matrix functions use.
 */
public class Query2MatrixFunctionsTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");
    private static final double DELTA = 1e-9;

    private static Schema vectorSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("emb1", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
                Schema.Field.of("emb2", Schema.FieldType.array(Schema.FieldType.FLOAT64).withNullable(true))));
    }

    private static MElement vectors(String userId, List<Double> emb1, List<Double> emb2) {
        final Map<String, Object> values = new HashMap<>();
        values.put("userId", userId);
        values.put("emb1", emb1);
        values.put("emb2", emb2);
        return MElement.of(values, TIMESTAMP);
    }

    @Test
    public void testCosineSimilarity() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", vectorSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               COSINE_SIMILARITY(i.emb1, i.emb2) AS sim
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    vectors("u1", List.of(1d, 2d, 3d), List.of(2d, 4d, 6d)),
                    vectors("u2", List.of(1d, 0d), List.of(0d, 1d)),
                    vectors("u3", List.of(0d, 0d), List.of(1d, 1d)),
                    vectors("u4", List.of(1d, 1d), null)),
                    TIMESTAMP);
            Assertions.assertEquals(4, outputs.size());
            Assertions.assertEquals(1d, doubleValue(outputs.get(0), "sim"), DELTA);
            Assertions.assertEquals(0d, doubleValue(outputs.get(1), "sim"), DELTA);
            // Zero-norm and NULL both yield NULL.
            Assertions.assertNull(outputs.get(2).getPrimitiveValue("sim"));
            Assertions.assertNull(outputs.get(3).getPrimitiveValue("sim"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testMatrixMultiplyAndSolveWithLiteralMatrix() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", vectorSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               MATRIX_MULTIPLY(ARRAY[ARRAY[1.0, 0.0, 0.0], ARRAY[0.0, 1.0, 0.0]], i.emb1) AS projected,
                               MATRIX_SOLVE(ARRAY[ARRAY[2.0, 1.0], ARRAY[1.0, 3.0]], i.emb2) AS solved
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    // 2x + y = 5, x + 3y = 10 -> [1, 3]
                    vectors("u1", List.of(7d, 8d, 9d), List.of(5d, 10d))),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            assertDoubleArray(List.of(7d, 8d), outputs.get(0).getPrimitiveValue("projected"));
            assertDoubleArray(List.of(1d, 3d), outputs.get(0).getPrimitiveValue("solved"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testMahalanobisAndPolyFit() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", vectorSchema())
                .withSql("""
                        SELECT i.userId AS userId,
                               MAHALANOBIS(i.emb1,
                                 ARRAY[0.0, 0.0],
                                 ARRAY[ARRAY[1.0, 0.0], ARRAY[0.0, 1.0]]) AS score,
                               POLY_FIT(ARRAY[0.0, 1.0, 2.0, 3.0], i.emb2, 1) AS trend
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    // Points on y = 2x + 1.
                    vectors("u1", List.of(3d, 4d), List.of(1d, 3d, 5d, 7d))),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            // Identity precision: euclidean distance of (3, 4).
            Assertions.assertEquals(5d, doubleValue(outputs.get(0), "score"), DELTA);
            assertDoubleArray(List.of(1d, 2d), outputs.get(0).getPrimitiveValue("trend"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testFlatMatrixOverloadsAndMatrixTypedField() {
        // A matrix-typed field (the repo's canonical flat row-major representation
        // with the shape in the schema — ONNX tensors, reshape outputs) surfaces
        // in SQL as a flat ARRAY<DOUBLE>; the trailing-columns overloads read it.
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("mat", Schema.FieldType.matrix(
                        Schema.FieldType.FLOAT64, List.of(2, 2))),
                Schema.Field.of("vec", Schema.FieldType.array(Schema.FieldType.FLOAT64))));
        final Query2 query = Query2.builder()
                .withInput("INPUT", schema)
                .withSql("""
                        SELECT i.userId AS userId,
                               MATRIX_SOLVE(i.mat, i.vec, 2) AS solved,
                               MATRIX_MULTIPLY(ARRAY[1.0, 0.0, 0.0, 1.0], i.vec, 2) AS identity_,
                               MAHALANOBIS(i.vec, ARRAY[0.0, 0.0], i.mat, 2) AS score
                        FROM INPUT AS i
                        """)
                .build();
        query.setup();
        try {
            final Map<String, Object> values = new HashMap<>();
            values.put("userId", "u1");
            // Row-major [[1, 0], [0, 1]] identity, so solve(I, b) = b.
            values.put("mat", List.of(1d, 0d, 0d, 1d));
            values.put("vec", List.of(3d, 4d));
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(values, TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            assertDoubleArray(List.of(3d, 4d), outputs.get(0).getPrimitiveValue("solved"));
            assertDoubleArray(List.of(3d, 4d), outputs.get(0).getPrimitiveValue("identity_"));
            Assertions.assertEquals(5d, doubleValue(outputs.get(0), "score"), DELTA);
        } finally {
            query.teardown();
        }
    }

    private static Schema samplesSchema() {
        return Schema.of(List.of(
                Schema.Field.of("groupId", Schema.FieldType.STRING),
                Schema.Field.of("samples", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("y", Schema.FieldType.FLOAT64.withNullable(true)),
                        Schema.Field.of("x1", Schema.FieldType.FLOAT64),
                        Schema.Field.of("x2", Schema.FieldType.FLOAT64)))))));
    }

    @Test
    public void testLinearRegOverUnnestedRows() {
        // Exact samples of y = 1 + 2 x1 + 3 x2: LINEAR_REG accumulates the
        // Gram matrix over the unnested rows and solves per group; the opaque
        // result is re-typed to a projectable ARRAY<DOUBLE> by AS_DOUBLE_ARRAY.
        final List<Map<String, Object>> samples = new java.util.ArrayList<>();
        final double[][] xs = {{0, 0}, {1, 0}, {0, 1}, {1, 1}, {2, 1}, {1, 2}};
        for (final double[] x : xs) {
            samples.add(Map.of("y", 1 + 2 * x[0] + 3 * x[1], "x1", x[0], "x2", x[1]));
        }
        // A NULL y row must be skipped, not break the fit.
        final Map<String, Object> nullRow = new HashMap<>();
        nullRow.put("y", null);
        nullRow.put("x1", 9d);
        nullRow.put("x2", 9d);
        samples.add(nullRow);

        final Query2 query = Query2.builder()
                .withInput("INPUT", samplesSchema())
                .withSql("""
                        SELECT i.groupId AS groupId,
                               AS_DOUBLE_ARRAY(LINEAR_REG(s.y, ARRAY[s.x1, s.x2])) AS coef
                        FROM INPUT AS i, UNNEST(i.samples) AS s
                        GROUP BY i.groupId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("groupId", "g1", "samples", samples), TIMESTAMP)),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            assertDoubleArray(List.of(1d, 2d, 3d), outputs.get(0).getPrimitiveValue("coef"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLinearRegRidgeOverload() {
        // Exact samples of y = 2 x1: lambda = 0 matches OLS; lambda = 1
        // penalizes the slope but not the intercept:
        // (X'X + diag(0, 1)) b = X'y with X'X = [[3,6],[6,14]], X'y = [12,28]
        // -> b = [4/3, 4/3].
        final List<Map<String, Object>> samples = List.of(
                Map.of("y", 2d, "x1", 1d, "x2", 0d),
                Map.of("y", 4d, "x1", 2d, "x2", 0d),
                Map.of("y", 6d, "x1", 3d, "x2", 0d));
        final Query2 query = Query2.builder()
                .withInput("INPUT", samplesSchema())
                .withSql("""
                        SELECT i.groupId AS groupId,
                               AS_DOUBLE_ARRAY(LINEAR_REG(s.y, ARRAY[s.x1], 0.0)) AS ols,
                               AS_DOUBLE_ARRAY(LINEAR_REG(s.y, ARRAY[s.x1], 1.0)) AS ridge
                        FROM INPUT AS i, UNNEST(i.samples) AS s
                        GROUP BY i.groupId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("groupId", "g1", "samples", samples), TIMESTAMP)),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            assertDoubleArray(List.of(0d, 2d), outputs.get(0).getPrimitiveValue("ols"));
            assertDoubleArray(List.of(4d / 3d, 4d / 3d), outputs.get(0).getPrimitiveValue("ridge"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLinregPredictTrainAndPredictInOneStatement() {
        // Fit y = 1 + 2 x1 + 3 x2 on the history rows and apply the model to a
        // fixed feature vector in the same statement: 1 + 2*2 + 3*1 = 8.
        final List<Map<String, Object>> samples = new java.util.ArrayList<>();
        final double[][] xs = {{0, 0}, {1, 0}, {0, 1}, {1, 1}, {2, 1}, {1, 2}};
        for (final double[] x : xs) {
            samples.add(Map.of("y", 1 + 2 * x[0] + 3 * x[1], "x1", x[0], "x2", x[1]));
        }
        final Query2 query = Query2.builder()
                .withInput("INPUT", samplesSchema())
                .withSql("""
                        SELECT i.groupId AS groupId,
                               LINREG_PREDICT(LINEAR_REG(s.y, ARRAY[s.x1, s.x2]),
                                              ARRAY[2.0, 1.0]) AS forecast
                        FROM INPUT AS i, UNNEST(i.samples) AS s
                        GROUP BY i.groupId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("groupId", "g1", "samples", samples), TIMESTAMP)),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(8d, doubleValue(outputs.get(0), "forecast"), DELTA);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testBayesLinRegWithKnownNoise() {
        // y = 2x samples (1,2), (2,4), (3,6); alpha = 1, noiseVar = 1.
        // Posterior precision A = I + X'X = [[4,6],[6,15]], S = A^-1,
        // m = S X'y = [0.5, 5/3]; at x' = [1,2]: mean = 23/6,
        // var = 1 + x' S x' = 1 + 7/24.
        final List<Map<String, Object>> samples = List.of(
                Map.of("y", 2d, "x1", 1d, "x2", 0d),
                Map.of("y", 4d, "x1", 2d, "x2", 0d),
                Map.of("y", 6d, "x1", 3d, "x2", 0d));
        final Query2 query = Query2.builder()
                .withInput("INPUT", samplesSchema())
                .withSql("""
                        SELECT i.groupId AS groupId,
                               BAYES_PREDICT(BAYES_LINREG(s.y, ARRAY[s.x1], 1.0, 1.0),
                                             ARRAY[2.0]) AS prediction
                        FROM INPUT AS i, UNNEST(i.samples) AS s
                        GROUP BY i.groupId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("groupId", "g1", "samples", samples), TIMESTAMP)),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            assertDoubleArray(List.of(23d / 6d, Math.sqrt(1d + 7d / 24d)),
                    outputs.get(0).getPrimitiveValue("prediction"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testBayesLinRegWithEstimatedNoise() {
        // Near-exact data with a weak prior: the estimated noise variance is
        // tiny, so the predictive mean tracks y = 2x closely and the sd stays
        // small but positive.
        final List<Map<String, Object>> samples = List.of(
                Map.of("y", 2d, "x1", 1d, "x2", 0d),
                Map.of("y", 4d, "x1", 2d, "x2", 0d),
                Map.of("y", 6d, "x1", 3d, "x2", 0d));
        final Query2 query = Query2.builder()
                .withInput("INPUT", samplesSchema())
                .withSql("""
                        SELECT i.groupId AS groupId,
                               BAYES_PREDICT(BAYES_LINREG(s.y, ARRAY[s.x1], 0.01),
                                             ARRAY[2.0]) AS prediction
                        FROM INPUT AS i, UNNEST(i.samples) AS s
                        GROUP BY i.groupId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("groupId", "g1", "samples", samples), TIMESTAMP)),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            final Object value = outputs.get(0).getPrimitiveValue("prediction");
            Assertions.assertInstanceOf(List.class, value);
            final List<?> prediction = (List<?>) value;
            final double mean = ((Number) prediction.get(0)).doubleValue();
            final double sd = ((Number) prediction.get(1)).doubleValue();
            Assertions.assertEquals(4d, mean, 0.05);
            Assertions.assertTrue(sd > 0 && sd < 0.5, "sd out of range: " + sd);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testModelAggregatesOverAllNullGroupYieldNull() {
        // All-NULL y rows: the fit yields NULL and both predictors pass the
        // NULL through instead of failing.
        final Map<String, Object> nullRow = new HashMap<>();
        nullRow.put("y", null);
        nullRow.put("x1", 1d);
        nullRow.put("x2", 2d);
        final Query2 query = Query2.builder()
                .withInput("INPUT", samplesSchema())
                .withSql("""
                        SELECT i.groupId AS groupId,
                               LINREG_PREDICT(LINEAR_REG(s.y, ARRAY[s.x1, s.x2]),
                                              ARRAY[1.0, 1.0]) AS forecast,
                               BAYES_PREDICT(BAYES_LINREG(s.y, ARRAY[s.x1], 1.0, 1.0),
                                             ARRAY[1.0]) AS prediction
                        FROM INPUT AS i, UNNEST(i.samples) AS s
                        GROUP BY i.groupId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("groupId", "g1", "samples", List.of(nullRow)), TIMESTAMP)),
                    TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertNull(outputs.get(0).getPrimitiveValue("forecast"));
            Assertions.assertNull(outputs.get(0).getPrimitiveValue("prediction"));
        } finally {
            query.teardown();
        }
    }

    private static double doubleValue(MElement element, String field) {
        return ((Number) element.getPrimitiveValue(field)).doubleValue();
    }

    private static void assertDoubleArray(List<Double> expected, Object actual) {
        Assertions.assertInstanceOf(List.class, actual);
        final List<?> values = (List<?>) actual;
        Assertions.assertEquals(expected.size(), values.size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), ((Number) values.get(i)).doubleValue(), DELTA);
        }
    }
}
