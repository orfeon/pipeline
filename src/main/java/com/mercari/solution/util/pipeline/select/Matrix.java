package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.MatrixOps;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Linear-algebra select functions, thin adapters over {@link MatrixOps} — the
 * same core behind the {@code query} module's COSINE_SIMILARITY /
 * MATRIX_MULTIPLY / MATRIX_SOLVE / MAHALANOBIS / POLY_FIT built-ins. Vector
 * arguments are {@code array<float64>}-shaped input fields; matrix arguments
 * are either a constant (a 2D JSON array in the config — a fixed projection /
 * precision matrix) or a field: a {@code matrix}-typed field (the repo's
 * canonical flat row-major representation, e.g. an ONNX tensor or a
 * {@code reshape} output — the 2D shape comes from the schema), a flat array
 * field with an explicit {@code columns} parameter, or an array-of-arrays
 * field. A NULL argument yields NULL.
 *
 * <pre>{@code
 * { "name": "sim",   "func": "cosine_similarity", "left": "embedding1", "right": "embedding2" },
 * { "name": "proj",  "func": "matrix_multiply",   "matrix": [[...],[...]], "field": "embedding" },
 * { "name": "coef",  "func": "matrix_solve",      "matrixField": "designMatrix", "field": "target" },
 * { "name": "score", "func": "mahalanobis",       "field": "features", "mean": [...], "covariance": [[...],[...]] },
 * { "name": "trend", "func": "poly_fit",          "x": "timestamps", "y": "prices", "degree": 1 }
 * }</pre>
 */
public class Matrix implements SelectFunction {

    public enum Op implements Serializable {
        cosine_similarity,
        matrix_multiply,
        matrix_solve,
        mahalanobis,
        poly_fit
    }

    /** A vector argument: a constant JSON array or an input field. */
    private record VectorSpec(double[] constant, String field) implements Serializable {
        private double[] resolve(final Map<String, Object> input) {
            return constant != null ? constant : MatrixOps.toVector(input.get(field));
        }
    }

    /**
     * A matrix argument: a constant 2D JSON array, or an input field —
     * a {@code matrix}-typed field (flat row-major values; {@code columns}
     * derived from the schema shape), a flat array field with an explicit
     * {@code columns} parameter, or an array-of-arrays field
     * ({@code columns} = 0 → nested interpretation).
     */
    private record MatrixSpec(double[][] constant, String field, int columns) implements Serializable {
        private double[][] resolve(final Map<String, Object> input) {
            if (constant != null) {
                return constant;
            }
            final Object value = input.get(field);
            return columns > 0 ? MatrixOps.toMatrix(value, columns) : MatrixOps.toMatrix(value);
        }
    }

    private final String name;
    private final Op op;

    private final VectorSpec left;
    private final VectorSpec right;
    private final MatrixSpec matrix;
    private final VectorSpec mean;
    private final MatrixSpec precision;
    private final boolean invertMatrixArg;
    private final Integer degree;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient double[][] constantPrecision;

    private Matrix(
            final String name,
            final Op op,
            final VectorSpec left,
            final VectorSpec right,
            final MatrixSpec matrix,
            final VectorSpec mean,
            final MatrixSpec precision,
            final boolean invertMatrixArg,
            final Integer degree,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.op = op;
        this.left = left;
        this.right = right;
        this.matrix = matrix;
        this.mean = mean;
        this.precision = precision;
        this.invertMatrixArg = invertMatrixArg;
        this.degree = degree;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Matrix of(String name, JsonObject jsonObject, List<Schema.Field> allInputFields, Op op, boolean ignore) {

        final Set<String> referencedFields = new LinkedHashSet<>();
        VectorSpec left = null;
        VectorSpec right = null;
        MatrixSpec matrix = null;
        VectorSpec mean = null;
        MatrixSpec precision = null;
        boolean invertMatrixArg = false;
        Integer degree = null;

        switch (op) {
            case cosine_similarity -> {
                left = vectorSpec(name, jsonObject, null, "left", referencedFields, true);
                right = vectorSpec(name, jsonObject, null, "right", referencedFields, true);
            }
            case matrix_multiply, matrix_solve -> {
                matrix = matrixSpec(name, jsonObject, allInputFields, "matrix", "matrixField", referencedFields, true);
                left = vectorSpec(name, jsonObject, null, "field", referencedFields, true);
            }
            case mahalanobis -> {
                left = vectorSpec(name, jsonObject, null, "field", referencedFields, true);
                mean = vectorSpec(name, jsonObject, "mean", "meanField", referencedFields, true);
                if (jsonObject.has("precision") || jsonObject.has("precisionField")) {
                    precision = matrixSpec(name, jsonObject, allInputFields, "precision", "precisionField", referencedFields, true);
                } else {
                    precision = matrixSpec(name, jsonObject, allInputFields, "covariance", "covarianceField", referencedFields, true);
                    invertMatrixArg = true;
                }
            }
            case poly_fit -> {
                right = vectorSpec(name, jsonObject, null, "y", referencedFields, true);
                left = vectorSpec(name, jsonObject, null, "x", referencedFields, false);
                if (jsonObject.has("degree")) {
                    degree = jsonObject.get("degree").getAsInt();
                    if (degree < 0) {
                        throw new IllegalArgumentException("SelectField " + op + ": " + name
                                + ".degree must be >= 0, but was " + degree);
                    }
                } else {
                    degree = 1;
                }
            }
        }

        final List<Schema.Field> inputFields = new ArrayList<>();
        for (final String field : referencedFields) {
            inputFields.add(Schema.Field.of(field, ElementSchemaUtil.getInputFieldType(field, allInputFields)));
        }

        final Schema.FieldType outputFieldType = switch (op) {
            case cosine_similarity, mahalanobis -> Schema.FieldType.FLOAT64.withNullable(true);
            case matrix_multiply, matrix_solve, poly_fit ->
                    Schema.FieldType.array(Schema.FieldType.FLOAT64).withNullable(true);
        };

        return new Matrix(name, op, left, right, matrix, mean, precision,
                invertMatrixArg, degree, inputFields, outputFieldType, ignore);
    }

    private static VectorSpec vectorSpec(
            final String name,
            final JsonObject jsonObject,
            final String constantParameter,
            final String fieldParameter,
            final Set<String> referencedFields,
            final boolean required) {

        if (constantParameter != null && jsonObject.has(constantParameter)
                && jsonObject.get(constantParameter).isJsonArray()) {
            return new VectorSpec(parseVector(name, constantParameter,
                    jsonObject.get(constantParameter).getAsJsonArray()), null);
        }
        if (jsonObject.has(fieldParameter)) {
            final String field = jsonObject.get(fieldParameter).getAsString();
            referencedFields.add(field);
            return new VectorSpec(null, field);
        }
        if (required) {
            throw new IllegalArgumentException("SelectField matrix func: " + name + " requires "
                    + (constantParameter == null
                            ? fieldParameter + " parameter"
                            : constantParameter + " or " + fieldParameter + " parameter"));
        }
        return null;
    }

    private static MatrixSpec matrixSpec(
            final String name,
            final JsonObject jsonObject,
            final List<Schema.Field> allInputFields,
            final String constantParameter,
            final String fieldParameter,
            final Set<String> referencedFields,
            final boolean required) {

        if (jsonObject.has(constantParameter) && jsonObject.get(constantParameter).isJsonArray()) {
            return new MatrixSpec(parseMatrix(name, constantParameter,
                    jsonObject.get(constantParameter).getAsJsonArray()), null, 0);
        }
        if (jsonObject.has(fieldParameter)) {
            final String field = jsonObject.get(fieldParameter).getAsString();
            referencedFields.add(field);
            return new MatrixSpec(null, field,
                    matrixColumns(name, jsonObject, field, allInputFields));
        }
        if (required) {
            throw new IllegalArgumentException("SelectField matrix func: " + name + " requires "
                    + constantParameter + " or " + fieldParameter + " parameter");
        }
        return null;
    }

    /**
     * How to read a matrix field's flat/nested value: a {@code matrix}-typed
     * field carries its 2D shape in the schema; a flat array field needs the
     * {@code columns} parameter; an array-of-arrays field is read nested (0).
     */
    private static int matrixColumns(
            final String name,
            final JsonObject jsonObject,
            final String field,
            final List<Schema.Field> allInputFields) {

        final Schema.FieldType fieldType = ElementSchemaUtil.getInputFieldType(field, allInputFields);
        if (Schema.Type.matrix.equals(fieldType.getType())) {
            final List<Integer> shape = fieldType.getShape();
            if (shape == null || shape.size() != 2) {
                throw new IllegalArgumentException("SelectField matrix func: " + name + " field: "
                        + field + " must be a 2D matrix, but its shape is " + shape);
            }
            return shape.get(1);
        }
        if (jsonObject.has("columns")) {
            final int columns = jsonObject.get("columns").getAsInt();
            if (columns <= 0) {
                throw new IllegalArgumentException("SelectField matrix func: " + name
                        + ".columns must be > 0, but was " + columns);
            }
            return columns;
        }
        return 0;
    }

    private static double[] parseVector(final String name, final String parameter, final JsonArray array) {
        final double[] out = new double[array.size()];
        for (int i = 0; i < array.size(); i++) {
            final JsonElement element = array.get(i);
            if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("SelectField matrix func: " + name + "."
                        + parameter + " must be a numeric array");
            }
            out[i] = element.getAsDouble();
        }
        return out;
    }

    private static double[][] parseMatrix(final String name, final String parameter, final JsonArray array) {
        final double[][] out = new double[array.size()][];
        for (int i = 0; i < array.size(); i++) {
            final JsonElement row = array.get(i);
            if (!row.isJsonArray()) {
                throw new IllegalArgumentException("SelectField matrix func: " + name + "."
                        + parameter + " must be a 2D numeric array");
            }
            out[i] = parseVector(name, parameter, row.getAsJsonArray());
            if (i > 0 && out[i].length != out[0].length) {
                throw new IllegalArgumentException("SelectField matrix func: " + name + "."
                        + parameter + " rows must have equal lengths");
            }
        }
        return out;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {
        // A constant covariance matrix is inverted once per worker.
        if (Op.mahalanobis.equals(op) && invertMatrixArg && precision.constant() != null) {
            this.constantPrecision = MatrixOps.inverse(precision.constant());
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        try {
            return switch (op) {
                case cosine_similarity -> {
                    final double[] a = left.resolve(input);
                    final double[] b = right.resolve(input);
                    yield a == null || b == null ? null : MatrixOps.cosineSimilarity(a, b);
                }
                case matrix_multiply -> {
                    final double[][] m = matrix.resolve(input);
                    final double[] v = left.resolve(input);
                    yield m == null || v == null ? null : MatrixOps.toList(MatrixOps.multiply(m, v));
                }
                case matrix_solve -> {
                    final double[][] m = matrix.resolve(input);
                    final double[] b = left.resolve(input);
                    yield m == null || b == null ? null : MatrixOps.toList(MatrixOps.solve(m, b));
                }
                case mahalanobis -> {
                    final double[] x = left.resolve(input);
                    final double[] m = mean.resolve(input);
                    final double[][] p = resolvePrecision(input);
                    yield x == null || m == null || p == null ? null : MatrixOps.mahalanobis(x, m, p);
                }
                case poly_fit -> {
                    final double[] y = right.resolve(input);
                    if (y == null) {
                        yield null;
                    }
                    final double[] x;
                    if (left == null) {
                        x = new double[y.length];
                        for (int i = 0; i < y.length; i++) {
                            x[i] = i;
                        }
                    } else {
                        x = left.resolve(input);
                        if (x == null) {
                            yield null;
                        }
                    }
                    yield MatrixOps.toList(MatrixOps.polyfit(x, y, degree));
                }
            };
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("SelectField " + op + ": " + name + " failed: "
                    + e.getMessage(), e);
        }
    }

    private double[][] resolvePrecision(final Map<String, Object> input) {
        if (constantPrecision != null) {
            return constantPrecision;
        }
        final double[][] resolved = precision.resolve(input);
        if (resolved == null) {
            return null;
        }
        return invertMatrixArg ? MatrixOps.inverse(resolved) : resolved;
    }
}
