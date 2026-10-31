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
import java.util.SplittableRandom;
import java.util.stream.IntStream;

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
 * { "name": "scores",    "func": "labels",           "field": "probs", "labels": ["a", "b"] },
 * { "name": "choices",   "func": "group_softmax",    "field": "logits", "groups": "options" },
 * { "name": "last",      "func": "slice",            "field": "logits", "axis": 0, "index": -1 },
 * { "name": "token",     "func": "argmax",           "field": "logits", "position": "last" },
 * { "name": "token",     "func": "sample",           "field": "logits", "position": "last", "temperature": 0.7, "topP": 0.9, "seed": 0 }
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
        labels,
        group_softmax,
        slice,
        sample
    }

    // the variable of a loop of the onnx transform, which makes a seeded sample differ from one iteration to the next
    public static final String ITERATION = "_iteration";
    private static final long SEED_STEP = 0x9E3779B97F4A7C15L;

    private record Slice(int axis, Long index, Long start, Long end) implements Serializable {}

    private record Sampling(double temperature, Integer topK, double topP, Long seed) implements Serializable {}

    private final String name;
    private final Op op;
    private final String field;
    private final Integer size;
    private final Integer k;
    private final int[] axis;
    private final List<String> labels;
    private final String groups;
    private final double temperature;
    private final boolean last;
    private final Slice slice;
    private final Sampling sampling;

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
            final String groups,
            final double temperature,
            final boolean last,
            final Slice slice,
            final Sampling sampling,
            final Schema.FieldType inputFieldType,
            final boolean outputDouble,
            final Schema.Field inputField,
            final Schema.Field groupsField,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.op = op;
        this.field = field;
        this.size = size;
        this.k = k;
        this.axis = axis;
        this.labels = labels;
        this.groups = groups;
        this.temperature = temperature;
        this.last = last;
        this.slice = slice;
        this.sampling = sampling;
        this.inputFieldType = inputFieldType;
        this.outputDouble = outputDouble;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        if(groupsField != null) {
            this.inputFields.add(groupsField);
        }
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

        final boolean last = position(name, jsonObject, inputFieldType, op);
        final boolean scalar = isNumeric(inputFieldType);
        final boolean vector = last || isVector(inputFieldType);
        final Schema.FieldType valueType = valueType(inputFieldType);
        final boolean outputDouble = valueType != null && valueType.getType() == Schema.Type.float64;
        final Schema.FieldType realType = (outputDouble ? Schema.FieldType.FLOAT64 : Schema.FieldType.FLOAT32).withNullable(false);

        int[] axis = null;
        List<String> labels = null;
        String groups = null;
        double temperature = 1d;
        Schema.Field groupsField = null;
        Slice slice = null;
        Sampling sampling = null;
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
                outputFieldType = scalar ? realType.withNullable(true) : Schema.FieldType.array(realType);
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
                // the dimensions the type fixes to 1: a dynamic one stays whatever its length, so that the value follows the type
                if(axis == null) {
                    final List<Integer> shape = shapeOf(name, Op.squeeze, inputFieldType);
                    axis = IntStream.range(0, shape.size()).filter(d -> shape.get(d) == 1).toArray();
                }
            }
            case group_softmax -> {
                requireVector(name, op, inputFieldType, vector);
                if(!jsonObject.has("groups")) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: group_softmax requires groups parameter");
                }
                groups = jsonObject.get("groups").getAsString();
                final Schema.FieldType groupsType = ElementSchemaUtil.getInputFieldType(groups, inputFields);
                if(groupsType.getType() != Schema.Type.array || groupsType.getArrayValueType().getType() != Schema.Type.array
                        || !isInteger(groupsType.getArrayValueType().getArrayValueType())) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: group_softmax groups: " + groups + " must be an array of arrays of integer ids, but got: " + groupsType);
                }
                final String groupsFieldName = ElementSchemaUtil.getInputFieldName(groups, inputFields);
                groupsField = Schema.Field.of(groupsFieldName, ElementSchemaUtil.getInputFieldType(groupsFieldName, inputFields));
                if(jsonObject.has("temperature")) {
                    temperature = jsonObject.get("temperature").getAsDouble();
                    if(!(temperature > 0)) {
                        throw new IllegalArgumentException("SelectField: " + name + " func: group_softmax temperature must be over than zero: " + temperature);
                    }
                }
                outputFieldType = Schema.FieldType.array(realType);
            }
            case slice -> {
                slice = parseSlice(name, jsonObject, inputFieldType);
                outputFieldType = sliceType(name, inputFieldType, slice);
            }
            case sample -> {
                requireVector(name, op, inputFieldType, vector);
                sampling = parseSampling(name, jsonObject);
                outputFieldType = Schema.FieldType.INT64;
            }
            default -> throw new IllegalArgumentException("Not supported vector func: " + op);
        }
        return new Vectors(name, op, field, size, k, axis, labels, groups, temperature, last, slice, sampling, inputFieldType, outputDouble, inputField, groupsField, outputFieldType, ignore);
    }

    // position: last, for a model that gives the values of every position of a sequence
    private static boolean position(final String name, final JsonObject jsonObject, final Schema.FieldType type, final Op op) {
        if(!jsonObject.has("position")) {
            return false;
        }
        if(op != Op.argmax && op != Op.topk && op != Op.sample) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " does not take position");
        }
        final String position = jsonObject.get("position").getAsString();
        if(!"last".equals(position)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " position must be last, but got: " + position);
        }
        if(type.getType() != Schema.Type.matrix || type.getShape().size() != 2 || !isNumeric(type.getMatrixValueType())) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " position: last requires a numeric matrix of two dimensions, but got: " + type);
        }
        return true;
    }

    private static Slice parseSlice(final String name, final JsonObject jsonObject, final Schema.FieldType type) {
        final int rank = shapeOf(name, Op.slice, type).size();
        final int axis = jsonObject.has("axis") ? jsonObject.get("axis").getAsInt() : 0;
        if(axis < -rank || axis >= rank) {
            throw new IllegalArgumentException("SelectField: " + name + " func: slice axis: " + axis + " is out of rank " + rank);
        }
        final Long index = jsonObject.has("index") ? Long.valueOf(jsonObject.get("index").getAsLong()) : null;
        final Long start = jsonObject.has("start") ? Long.valueOf(jsonObject.get("start").getAsLong()) : null;
        final Long end = jsonObject.has("end") ? Long.valueOf(jsonObject.get("end").getAsLong()) : null;
        if((index == null) == (start == null && end == null)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: slice requires either index, or start and end");
        }
        return new Slice(axis < 0 ? axis + rank : axis, index, start, end);
    }

    private static Schema.FieldType sliceType(final String name, final Schema.FieldType type, final Slice slice) {
        final Schema.FieldType valueType = valueType(type);
        final List<Integer> shape = new ArrayList<>(shapeOf(name, Op.slice, type));
        if(slice.index != null) {
            shape.remove(slice.axis);
        } else {
            final int length = shape.get(slice.axis);
            final long start = slice.start == null ? 0 : slice.start;
            final long end = slice.end == null ? length : slice.end;
            // the length is only known if the range does not depend on a dynamic length
            final boolean known = length >= 0 && start >= 0 && end >= 0;
            shape.set(slice.axis, known ? (int) Math.max(0, Math.min(end, length) - Math.min(start, length)) : -1);
        }
        return switch (shape.size()) {
            case 0 -> valueType.withNullable(true);
            case 1 -> Schema.FieldType.array(valueType);
            default -> Schema.FieldType.matrix(valueType, shape);
        };
    }

    private static Sampling parseSampling(final String name, final JsonObject jsonObject) {
        final double temperature = jsonObject.has("temperature") ? jsonObject.get("temperature").getAsDouble() : 1d;
        if(temperature < 0) {
            throw new IllegalArgumentException("SelectField: " + name + " func: sample temperature must not be negative: " + temperature);
        }
        final Integer topK = jsonObject.has("topK") ? Integer.valueOf(jsonObject.get("topK").getAsInt()) : null;
        if(topK != null && topK < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " func: sample topK must be over than zero: " + topK);
        }
        final double topP = jsonObject.has("topP") ? jsonObject.get("topP").getAsDouble() : 1d;
        if(topP <= 0 || topP > 1) {
            throw new IllegalArgumentException("SelectField: " + name + " func: sample topP must be over zero, and at most 1: " + topP);
        }
        final Long seed = jsonObject.has("seed") ? Long.valueOf(jsonObject.get("seed").getAsLong()) : null;
        return new Sampling(temperature, topK, topP, seed);
    }

    private static void requireVector(final String name, final Op op, final Schema.FieldType type, final boolean vector) {
        if(!vector) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires a numeric array, or a matrix with one dimension other than 1, but got: " + type);
        }
    }

    static boolean isNumeric(final Schema.FieldType type) {
        return switch (type.getType()) {
            case int8, int16, int32, int64, float16, float32, float64 -> true;
            default -> false;
        };
    }

    private static boolean isInteger(final Schema.FieldType type) {
        return switch (type.getType()) {
            case int8, int16, int32, int64 -> true;
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

    private static List<Integer> shapeOf(final String name, final Op op, final Schema.FieldType type) {
        return switch (type.getType()) {
            case array -> List.of(-1);
            case matrix -> type.getShape();
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires an array or a matrix, but got: " + type);
        };
    }

    // a dynamic dimension is only known to be 1 when axis says so
    private static Schema.FieldType squeezeType(final String name, final Schema.FieldType type, final int[] axis) {
        final List<Integer> shape = shapeOf(name, Op.squeeze, type);
        if(axis != null) {
            for(final int a : axis) {
                if(a < -shape.size() || a >= shape.size()) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: squeeze axis: " + a + " is out of shape " + shape);
                }
            }
        }
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
            case 0 -> valueType.withNullable(true);
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
        final Object value = last ? lastRow(ElementSchemaUtil.getValue(input, field)) : ElementSchemaUtil.getValue(input, field);
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
            case group_softmax -> {
                final Object ids = ElementSchemaUtil.getValue(input, groups);
                if(ids == null) {
                    yield null;
                }
                final double[] maxima = groupMaxima(vector(value), ids);
                for(int g=0; g<maxima.length; g++) {
                    maxima[g] /= temperature;
                }
                yield values(softmax(maxima));
            }
            case slice -> slice(value);
            case sample -> {
                final int index = sample(vector(value), input.get(ITERATION));
                yield index < 0 ? null : (Object) (long) index;
            }
        };
    }

    // the row is a view: only that row is read of the values of a whole sequence
    private Object lastRow(final Object value) {
        final Tensor tensor = tensor(value);
        return tensor == null ? null : tensor.row(Math.toIntExact(tensor.shape()[0]) - 1);
    }

    private Tensor tensor(final Object value) {
        return switch (value) {
            case null -> null;
            case Tensor tensor -> tensor;
            // a matrix is flat in a record, with one dynamic dimension at most
            case List<?> list -> Tensor.of(tensorType(valueType(inputFieldType)), list,
                    Tensor.flatShape("SelectField: " + name + " func: " + op, shapeOf(name, op, inputFieldType), list.size()));
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires an array or a matrix, but got: " + value);
        };
    }

    private static Tensor.Type tensorType(final Schema.FieldType type) {
        return switch (type.getType()) {
            case float16, float32 -> Tensor.Type.FLOAT;
            case float64 -> Tensor.Type.DOUBLE;
            case int64 -> Tensor.Type.INT64;
            case int32 -> Tensor.Type.INT32;
            case int16 -> Tensor.Type.INT16;
            case int8 -> Tensor.Type.INT8;
            case bool -> Tensor.Type.BOOL;
            case string -> Tensor.Type.STRING;
            default -> throw new IllegalArgumentException("Not supported matrix value type: " + type);
        };
    }

    private Object slice(final Object value) {
        // an array of a record keeps its values, whatever their type: only a matrix needs the shape of a Tensor
        if(value instanceof List<?> list && inputFieldType.getType() == Schema.Type.array) {
            if(slice.index != null) {
                return list.get(Math.toIntExact(sliceIndex(list.size())));
            }
            final int start = (int) bound(slice.start, 0, list.size());
            final int end = (int) bound(slice.end, list.size(), list.size());
            return new ArrayList<>(list.subList(start, Math.max(start, end)));
        }
        final Tensor tensor = tensor(value);
        final long length = tensor.shape()[slice.axis];
        final Tensor sliced;
        if(slice.index != null) {
            final long index = sliceIndex(length);
            final long[] shape = tensor.shape();
            final long[] removed = new long[shape.length - 1];
            for(int d=0; d<removed.length; d++) {
                removed[d] = shape[d < slice.axis ? d : d + 1];
            }
            sliced = tensor.slice(slice.axis, index, index + 1).reshaped(removed);
        } else {
            final long start = bound(slice.start, 0, length);
            final long end = bound(slice.end, length, length);
            sliced = tensor.slice(slice.axis, start, Math.max(start, end));
        }
        if(sliced.rank() == 0) {
            return sliced.get(0);
        }
        return value instanceof Tensor ? sliced : sliced.toList();
    }

    private long sliceIndex(final long length) {
        final long index = slice.index < 0 ? slice.index + length : slice.index;
        if(index < 0 || index >= length) {
            throw new IllegalArgumentException("SelectField: " + name + " func: slice index: " + slice.index + " is out of a length of " + length);
        }
        return index;
    }

    private static long bound(final Long value, final long defaultValue, final long length) {
        if(value == null) {
            return defaultValue;
        }
        return Math.max(0, Math.min(length, value < 0 ? value + length : value));
    }

    // temperature, then top k, then top p, as is usual. NaN is never chosen; -1 if there is no other value
    private int sample(final double[] logits, final Object iteration) {
        if(sampling.temperature == 0) {
            return argmax(logits);
        }
        final List<Integer> indices = new ArrayList<>(logits.length);
        for(int i=0; i<logits.length; i++) {
            if(!Double.isNaN(logits[i])) {
                indices.add(i);
            }
        }
        if(indices.isEmpty()) {
            return -1;
        }
        indices.sort((a, b) -> Double.compare(logits[b], logits[a]));
        final List<Integer> top = indices.subList(0, sampling.topK == null ? indices.size() : Math.min(sampling.topK, indices.size()));
        final double[] scaled = new double[top.size()];
        for(int i=0; i<scaled.length; i++) {
            scaled[i] = logits[top.get(i)] / sampling.temperature;
        }
        final double[] probabilities = softmax(scaled);
        int kept = 0;
        double total = 0;
        while(kept < probabilities.length && (kept == 0 || total < sampling.topP)) {
            total += probabilities[kept++];
        }
        double remaining = random(iteration).nextDouble() * total;
        for(int i=0; i<kept; i++) {
            remaining -= probabilities[i];
            if(remaining < 0) {
                return top.get(i);
            }
        }
        return top.get(kept - 1);
    }

    // with a seed the result only depends on the values and the iteration, so that a retried record gets the same one
    private SplittableRandom random(final Object iteration) {
        if(sampling.seed == null) {
            return new SplittableRandom();
        }
        final long step = iteration instanceof Number number ? number.longValue() : 0L;
        return new SplittableRandom(sampling.seed + step * SEED_STEP);
    }

    // the logit of a group is that of its most likely token
    private double[] groupMaxima(final double[] logits, final Object groups) {
        if(!(groups instanceof List<?> list)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: group_softmax groups must be an array of arrays of ids, but got: " + groups);
        }
        final double[] maxima = new double[list.size()];
        for(int g=0; g<maxima.length; g++) {
            if(!(list.get(g) instanceof List<?> ids) || ids.isEmpty()) {
                throw new IllegalArgumentException("SelectField: " + name + " func: group_softmax group " + g + " must be an array of ids, but got: " + list.get(g));
            }
            maxima[g] = Double.NEGATIVE_INFINITY;
            for(final Object id : ids) {
                if(!(id instanceof Number number)) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: group_softmax group " + g + " must be an array of ids, but got: " + ids);
                }
                final long index = number.longValue();
                if(index < 0 || index >= logits.length) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: group_softmax id: " + index + " is out of a vector of length " + logits.length);
                }
                maxima[g] = Math.max(maxima[g], logits[(int) index]);
            }
        }
        return maxima;
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
    static int argmax(final double[] vector) {
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
                    final boolean removed = contains(axis, d, shape.length);
                    if(removed && shape[d] != 1) {
                        throw new IllegalArgumentException("SelectField: " + name + " func: squeeze axis: " + d + " has size " + shape[d] + " in: " + tensor);
                    }
                    if(!removed) {
                        squeezed.add(shape[d]);
                    }
                }
                final Tensor reshaped = tensor.reshaped(squeezed.stream().mapToLong(Long::longValue).toArray());
                // no dimension left: the value itself, as a model output of rank 1 gives, which the next function can tell from a vector
                yield reshaped.rank() == 0 ? reshaped.get(0) : reshaped;
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
