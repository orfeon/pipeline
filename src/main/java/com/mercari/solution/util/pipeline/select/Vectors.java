package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Select functions on numeric vectors, typically a model output. A vector is an {@code array} field, a {@code matrix}
 * field with at most one dimension other than 1, or a Tensor of the same shape inside the onnx transform. Results are
 * record values (lists), except that {@code squeeze} keeps a Tensor as a Tensor without a copy. A NULL field yields NULL.
 *
 * <pre>{@code
 * { "name": "embedding", "func": "vector_normalize", "field": "sentence_embedding", "size": 256 },
 * { "name": "last",      "func": "squeeze",          "field": "logits", "axis": 0 },
 * { "name": "probs",     "func": "softmax",          "field": "logits" },
 * { "name": "prob",      "func": "sigmoid",          "field": "logit" },
 * { "name": "best",      "func": "argmax",           "field": "probs" },
 * { "name": "top",       "func": "topk",             "field": "probs", "k": 5 },
 * { "name": "scores",    "func": "labels",           "field": "probs", "labels": ["a", "b"] }
 * }</pre>
 */
public class Vectors implements SelectFunction {

    public enum Op implements Serializable {
        vector_normalize,
        squeeze,
        softmax,
        sigmoid,
        argmax,
        topk,
        labels
    }

    private final String name;
    private final Op op;
    private final String field;
    private final Integer size;
    private final Integer k;
    private final int[] axis;
    private final List<String> labels;

    private final Schema.FieldType inputFieldType;
    private final boolean outputDouble;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private Vectors(
            final String name,
            final Op op,
            final String field,
            final Integer size,
            final Integer k,
            final int[] axis,
            final List<String> labels,
            final Schema.FieldType inputFieldType,
            final boolean outputDouble,
            final Schema.Field inputField,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.op = op;
        this.field = field;
        this.size = size;
        this.k = k;
        this.axis = axis;
        this.labels = labels;
        this.inputFieldType = inputFieldType;
        this.outputDouble = outputDouble;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Vectors of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final Op op, final boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires field parameter");
        }
        final String field = jsonObject.get("field").getAsString();
        final String inputFieldName = ElementSchemaUtil.getInputFieldName(field, inputFields);
        final Schema.Field inputField = Schema.Field.of(inputFieldName, ElementSchemaUtil.getInputFieldType(inputFieldName, inputFields));
        final Schema.FieldType inputFieldType = ElementSchemaUtil.getInputFieldType(field, inputFields);

        final Integer size = jsonObject.has("size") ? Integer.valueOf(jsonObject.get("size").getAsInt()) : null;
        if(size != null && size < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " size must be over than zero: " + size);
        }
        final Integer k = jsonObject.has("k") ? Integer.valueOf(jsonObject.get("k").getAsInt()) : null;

        final boolean scalar = isNumeric(inputFieldType);
        final boolean vector = isVector(inputFieldType);
        final Schema.FieldType valueType = valueType(inputFieldType);
        final boolean outputDouble = valueType != null && valueType.getType() == Schema.Type.float64;
        final Schema.FieldType realType = (outputDouble ? Schema.FieldType.FLOAT64 : Schema.FieldType.FLOAT32).withNullable(false);

        int[] axis = null;
        List<String> labels = null;
        final Schema.FieldType outputFieldType;
        switch (op) {
            case vector_normalize, softmax -> {
                requireVector(name, op, inputFieldType, vector);
                outputFieldType = Schema.FieldType.array(realType);
            }
            case sigmoid -> {
                if(!scalar) {
                    requireVector(name, op, inputFieldType, vector);
                }
                outputFieldType = scalar ? realType : Schema.FieldType.array(realType);
            }
            case argmax -> {
                requireVector(name, op, inputFieldType, vector);
                outputFieldType = Schema.FieldType.INT64;
            }
            case topk -> {
                requireVector(name, op, inputFieldType, vector);
                if(k == null || k < 1) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: topk requires k over than zero");
                }
                outputFieldType = Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("index", Schema.FieldType.INT64),
                        Schema.Field.of("value", realType))));
            }
            case labels -> {
                if(!jsonObject.has("labels") || !jsonObject.get("labels").isJsonArray() || jsonObject.getAsJsonArray("labels").isEmpty()) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: labels requires labels parameter");
                }
                labels = new ArrayList<>();
                for(final JsonElement label : jsonObject.getAsJsonArray("labels")) {
                    labels.add(label.getAsString());
                }
                if(scalar) {
                    outputFieldType = Schema.FieldType.STRING;
                } else {
                    requireVector(name, op, inputFieldType, vector);
                    outputFieldType = Schema.FieldType.array(Schema.FieldType.element(List.of(
                            Schema.Field.of("label", Schema.FieldType.STRING),
                            Schema.Field.of("value", realType))));
                }
            }
            case squeeze -> {
                if(jsonObject.has("axis")) {
                    final JsonElement axisElement = jsonObject.get("axis");
                    axis = axisElement.isJsonArray()
                            ? axisElement.getAsJsonArray().asList().stream().mapToInt(JsonElement::getAsInt).toArray()
                            : new int[]{axisElement.getAsInt()};
                }
                outputFieldType = squeezeType(name, inputFieldType, axis);
            }
            default -> throw new IllegalArgumentException("Not supported vector func: " + op);
        }
        return new Vectors(name, op, field, size, k, axis, labels, inputFieldType, outputDouble, inputField, outputFieldType, ignore);
    }

    private static void requireVector(final String name, final Op op, final Schema.FieldType type, final boolean vector) {
        if(!vector) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires a numeric array, or a matrix with one dimension other than 1, but got: " + type);
        }
    }

    private static boolean isNumeric(final Schema.FieldType type) {
        return switch (type.getType()) {
            case int8, int16, int32, int64, float16, float32, float64 -> true;
            default -> false;
        };
    }

    private static boolean isVector(final Schema.FieldType type) {
        return switch (type.getType()) {
            case array -> isNumeric(type.getArrayValueType());
            case matrix -> type.getShape().stream().filter(dim -> dim != 1).count() <= 1;
            default -> false;
        };
    }

    private static Schema.FieldType valueType(final Schema.FieldType type) {
        return switch (type.getType()) {
            case array -> type.getArrayValueType();
            case matrix -> type.getMatrixValueType();
            default -> isNumeric(type) ? type : null;
        };
    }

    // a dynamic dimension is only known to be 1 when axis says so
    private static Schema.FieldType squeezeType(final String name, final Schema.FieldType type, final int[] axis) {
        final List<Integer> shape = switch (type.getType()) {
            case array -> List.of(-1);
            case matrix -> type.getShape();
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: squeeze requires an array or a matrix, but got: " + type);
        };
        final Schema.FieldType valueType = valueType(type);
        final List<Integer> squeezed = new ArrayList<>();
        for(int d=0; d<shape.size(); d++) {
            final int dim = shape.get(d);
            final boolean removed;
            if(axis == null) {
                removed = dim == 1;
            } else {
                removed = contains(axis, d, shape.size());
                if(removed && dim != 1 && dim != -1) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: squeeze axis: " + d + " has size " + dim + " in shape " + shape);
                }
            }
            if(!removed) {
                squeezed.add(dim);
            }
        }
        return switch (squeezed.size()) {
            case 0 -> valueType;
            case 1 -> Schema.FieldType.array(valueType);
            default -> Schema.FieldType.matrix(valueType, squeezed);
        };
    }

    private static boolean contains(final int[] axis, final int dim, final int rank) {
        for(final int a : axis) {
            if(a == dim || a + rank == dim) {
                return true;
            }
        }
        return false;
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
    public boolean acceptsTensor() {
        return true;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        final Object value = ElementSchemaUtil.getValue(input, field);
        if(value == null) {
            return null;
        }
        return switch (op) {
            case vector_normalize -> values(normalize(vector(value)));
            case softmax -> values(softmax(vector(value)));
            case sigmoid -> {
                if(value instanceof Number number) {
                    yield real(sigmoid(number.doubleValue()));
                }
                final double[] vector = vector(value);
                for(int i=0; i<vector.length; i++) {
                    vector[i] = sigmoid(vector[i]);
                }
                yield values(vector);
            }
            case argmax -> {
                final int index = argmax(vector(value));
                yield index < 0 ? null : (Object) (long) index;
            }
            case topk -> topk(vector(value));
            case labels -> {
                if(value instanceof Number number) {
                    final long index = number.longValue();
                    if(index < 0 || index >= labels.size()) {
                        throw new IllegalArgumentException("SelectField: " + name + " index: " + index + " is out of labels: " + labels);
                    }
                    yield labels.get((int) index);
                }
                yield labeled(vector(value));
            }
            case squeeze -> squeeze(value);
        };
    }

    private double[] vector(final Object value) {
        final double[] vector = switch (value) {
            case Tensor tensor -> {
                final long[] shape = tensor.shape();
                if(Arrays.stream(shape).filter(dim -> dim != 1).count() > 1) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires a vector, but got: " + tensor);
                }
                final double[] values = new double[tensor.size()];
                for(int i=0; i<values.length; i++) {
                    values[i] = tensor.getDouble(i);
                }
                yield values;
            }
            case List<?> list -> {
                final double[] values = new double[list.size()];
                for(int i=0; i<values.length; i++) {
                    values[i] = ((Number) list.get(i)).doubleValue();
                }
                yield values;
            }
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires a vector, but got: " + value);
        };
        if(size == null) {
            return vector;
        }
        if(size > vector.length) {
            throw new IllegalArgumentException("SelectField: " + name + " size: " + size + " is over the vector length: " + vector.length);
        }
        return Arrays.copyOf(vector, size);
    }

    private static double[] normalize(final double[] vector) {
        double squared = 0;
        for(final double v : vector) {
            squared += v * v;
        }
        final double norm = Math.sqrt(squared);
        if(norm == 0) {
            return vector;
        }
        final double[] normalized = new double[vector.length];
        for(int i=0; i<vector.length; i++) {
            normalized[i] = vector[i] / norm;
        }
        return normalized;
    }

    private static double[] softmax(final double[] logits) {
        double max = Double.NEGATIVE_INFINITY;
        for(final double logit : logits) {
            max = Math.max(max, logit);
        }
        // relative to the largest, so that large logits do not overflow
        final double[] probabilities = new double[logits.length];
        double sum = 0;
        for(int i=0; i<logits.length; i++) {
            probabilities[i] = Math.exp(logits[i] - max);
            sum += probabilities[i];
        }
        for(int i=0; i<logits.length; i++) {
            probabilities[i] /= sum;
        }
        return probabilities;
    }

    private static double sigmoid(final double x) {
        return 1 / (1 + Math.exp(-x));
    }

    // NaN is never the largest; -1 if there is no other value
    private static int argmax(final double[] vector) {
        int best = -1;
        for(int i=0; i<vector.length; i++) {
            if(!Double.isNaN(vector[i]) && (best < 0 || vector[i] > vector[best])) {
                best = i;
            }
        }
        return best;
    }

    private List<Map<String, Object>> topk(final double[] vector) {
        final List<Integer> indices = new ArrayList<>(vector.length);
        for(int i=0; i<vector.length; i++) {
            indices.add(i);
        }
        // stable: the same values keep the order of their indices. Double.compare puts NaN above every value, so NaN goes last explicitly
        indices.sort((a, b) -> {
            final boolean nanA = Double.isNaN(vector[a]);
            final boolean nanB = Double.isNaN(vector[b]);
            if(nanA || nanB) {
                return Boolean.compare(nanA, nanB);
            }
            return Double.compare(vector[b], vector[a]);
        });
        final List<Map<String, Object>> top = new ArrayList<>();
        for(final int index : indices.subList(0, Math.min(k, indices.size()))) {
            top.add(Map.of("index", (long) index, "value", real(vector[index])));
        }
        return top;
    }

    private List<Map<String, Object>> labeled(final double[] vector) {
        if(vector.length != labels.size()) {
            throw new IllegalArgumentException("SelectField: " + name + " has " + labels.size() + " labels for a vector of length " + vector.length);
        }
        final List<Map<String, Object>> labeled = new ArrayList<>();
        for(int i=0; i<vector.length; i++) {
            labeled.add(Map.of("label", labels.get(i), "value", real(vector[i])));
        }
        return labeled;
    }

    private Object squeeze(final Object value) {
        return switch (value) {
            case Tensor tensor -> {
                final long[] shape = tensor.shape();
                final List<Long> squeezed = new ArrayList<>();
                for(int d=0; d<shape.length; d++) {
                    final boolean removed = axis == null ? shape[d] == 1 : contains(axis, d, shape.length);
                    if(removed && shape[d] != 1) {
                        throw new IllegalArgumentException("SelectField: " + name + " func: squeeze axis: " + d + " has size " + shape[d] + " in: " + tensor);
                    }
                    if(!removed) {
                        squeezed.add(shape[d]);
                    }
                }
                yield tensor.reshaped(squeezed.stream().mapToLong(Long::longValue).toArray());
            }
            // a list is flat: only the type changes, or the list of one value is the value
            case List<?> list -> switch (outputFieldType.getType()) {
                case array, matrix -> list;
                default -> {
                    if(list.size() != 1) {
                        throw new IllegalArgumentException("SelectField: " + name + " func: squeeze of " + inputFieldType + " requires one value, but got " + list.size());
                    }
                    yield list.getFirst();
                }
            };
            default -> value;
        };
    }

    private Object real(final double value) {
        return outputDouble ? (Object) value : (Object) (float) value;
    }

    private List<Object> values(final double[] vector) {
        final List<Object> values = new ArrayList<>(vector.length);
        for(final double value : vector) {
            values.add(real(value));
        }
        return values;
    }

}
