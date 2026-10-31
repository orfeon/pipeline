package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

/**
 * The {@code inputs} parameter of the onnx transform: a select list evaluated per record, whose items named as a model
 * input feed that input; the others are intermediate values. An item may carry the onnx keys {@code pad},
 * {@code empty} and {@code fill}, which are taken out before the select functions are built.
 *
 * <p>A value one rank below the model input is stacked on a new dim0, so each record is a row of the batch. A value of
 * the rank of the model input is the input as it is, which takes a batch of one record. With both, dim0 of the model
 * outputs is still that batch of one record (text tokens stacked next to {@code image_features [N, 512]}).
 */
public class OnnxInputs implements Serializable {

    private record Pad(double value, boolean left, int multiple) implements Serializable {}

    /** {@code empty} / {@code fill}: a value for the whole batch. A null shape comes from the model input. */
    private record Constant(long[] shape, double value) implements Serializable {}

    record ModelInput(
            OnnxGraphInfo.ValueInfo info,
            Pad pad,
            Constant constant,
            Schema.FieldType fieldType,
            int rank,
            boolean stacked) implements Serializable {}

    private final Select select;
    private final List<ModelInput> modelInputs;
    private final boolean batched;
    private final boolean singleRecord;

    private OnnxInputs(final Select select, final List<ModelInput> modelInputs, final boolean batched, final boolean singleRecord) {
        this.select = select;
        this.modelInputs = modelInputs;
        this.batched = batched;
        this.singleRecord = singleRecord;
    }

    public static OnnxInputs of(
            final JsonArray inputs,
            final Map<String, OnnxGraphInfo.ValueInfo> modelInputInfos,
            final List<Schema.Field> inputFields) {

        final JsonArray selects = new JsonArray();
        final Map<String, Pad> pads = new HashMap<>();
        final Map<String, Constant> constants = new LinkedHashMap<>();
        for(final JsonElement element : inputs) {
            if(!element.isJsonObject() || !element.getAsJsonObject().has("name")) {
                throw new IllegalArgumentException("onnx inputs item requires name: " + element);
            }
            final JsonObject item = element.getAsJsonObject();
            for(final String name : expandName(item.get("name").getAsString(), modelInputInfos.keySet())) {
                final JsonObject select = item.deepCopy();
                select.addProperty("name", name);
                final Pad pad = parsePad(name, select.remove("pad"));
                final Constant constant = parseConstant("onnx inputs", name, select);
                if(pad != null) {
                    if(constant != null) {
                        throw new IllegalArgumentException("onnx inputs: " + name + " pad can not be used with empty or fill");
                    }
                    if(!modelInputInfos.containsKey(name)) {
                        throw new IllegalArgumentException("onnx inputs: " + name + " has pad, but it is not a model input: " + modelInputInfos.keySet());
                    }
                    pads.put(name, pad);
                }
                if(constant != null) {
                    if(!modelInputInfos.containsKey(name)) {
                        throw new IllegalArgumentException("onnx inputs: " + name + " has empty or fill, but it is not a model input: " + modelInputInfos.keySet());
                    }
                    constants.put(name, constant);
                } else {
                    selects.add(select);
                }
            }
        }

        final Select select = Select.of(selects, inputFields);
        final Map<String, Schema.FieldType> valueTypes = new HashMap<>();
        for(final SelectFunction function : select.getSelectFunctions()) {
            valueTypes.put(function.getName(), function.getOutputFieldType());
        }

        final List<String> missing = new ArrayList<>();
        final List<ModelInput> modelInputs = new ArrayList<>();
        boolean stacked = false;
        boolean asIs = false;
        for(final OnnxGraphInfo.ValueInfo info : modelInputInfos.values()) {
            final String name = info.name();
            if(!info.isTensor()) {
                throw new IllegalArgumentException("onnx model input: " + info + " is not supported: only tensors can be given");
            }
            final Constant constant = constants.get(name);
            final Schema.FieldType valueType = valueTypes.get(name);
            if(constant != null && valueType != null) {
                throw new IllegalArgumentException("onnx inputs: " + name + " is given both a value and empty or fill");
            }
            if(constant != null) {
                validateConstant("onnx inputs", name, constant, info);
                modelInputs.add(new ModelInput(info, null, constant, null, 0, false));
            } else if(valueType == null) {
                missing.add(info.toString());
            } else {
                final int rank = rank(name, valueType);
                if(rank == info.rank() - 1) {
                    stacked = true;
                    validatePad(name, pads.get(name), info);
                } else if(rank == info.rank()) {
                    if(pads.containsKey(name)) {
                        throw new IllegalArgumentException("onnx inputs: " + name + " has pad, but a value of type " + valueType
                                + " is the model input " + info + " as it is, with no batch to pad");
                    }
                    asIs = true;
                } else {
                    throw new IllegalArgumentException("onnx inputs: " + name + " of type " + valueType
                            + " has rank " + rank + ", but the model input " + info + " takes a value of rank " + (info.rank() - 1)
                            + " (a row of the batch) or " + info.rank() + " (the input as it is)");
                }
                modelInputs.add(new ModelInput(info, pads.get(name), null, valueType, rank, rank == info.rank() - 1));
            }
        }
        if(!missing.isEmpty()) {
            throw new IllegalArgumentException("onnx model inputs are not given by inputs: " + missing);
        }
        return new OnnxInputs(select, modelInputs, stacked, asIs || !stacked);
    }

    /**
     * Expands a name with variables ({@code %d} digits, {@code %s} alphanumerics) to the matching model inputs. A pattern
     * that matches nothing is an error: it would be a setting copied from another model.
     */
    static List<String> expandName(final String name, final Collection<String> modelInputNames) {
        if(!hasVariables(name)) {
            return List.of(name);
        }
        final Pattern pattern = namePattern(name);
        final List<String> names = modelInputNames.stream().filter(n -> pattern.matcher(n).matches()).toList();
        if(names.isEmpty()) {
            throw new IllegalArgumentException("onnx inputs name: " + name + " matches no model input: " + modelInputNames);
        }
        return names;
    }

    static boolean hasVariables(final String name) {
        return name.contains("%d") || name.contains("%s");
    }

    // a group per variable, in the order they appear
    static Pattern namePattern(final String name) {
        return Pattern.compile(Arrays.stream(name.split("((?=%[ds])|(?<=%[ds]))"))
                .map(part -> switch (part) {
                    case "%d" -> "([0-9]+)";
                    case "%s" -> "([A-Za-z0-9_]+)";
                    default -> Pattern.quote(part);
                })
                .reduce("", String::concat));
    }

    // naming one that is not used only costs a little more memory, while the opposite would give a null value silently,
    // so that any mention counts (also in an expression or a template)
    public static Set<String> mentioned(final String text, final Collection<String> names) {
        final Set<String> mentioned = new LinkedHashSet<>();
        for(final String name : names) {
            if(Pattern.compile("(?<![A-Za-z0-9_])" + Pattern.quote(name) + "(?![A-Za-z0-9_])").matcher(text).find()) {
                mentioned.add(name);
            }
        }
        return mentioned;
    }

    private static void validatePad(final String name, final Pad pad, final OnnxGraphInfo.ValueInfo info) {
        if(pad == null) {
            return;
        }
        validateValue(name, "pad", pad.value, info);
        if(pad.multiple > 1 && dynamicRowDimensions(info).isEmpty()) {
            throw new IllegalArgumentException("onnx inputs: " + name + " has pad.multiple, but the model input " + info + " has no dynamic dimension to pad");
        }
    }

    // in an integer tensor a NaN would silently become 0, and an infinity the largest integer
    private static void validateValue(final String name, final String key, final double value, final OnnxGraphInfo.ValueInfo info) {
        final Tensor.Type type = OnnxRuntimeUtil.tensorType(info.type());
        if(!Double.isFinite(value) && type != Tensor.Type.FLOAT && type != Tensor.Type.DOUBLE) {
            throw new IllegalArgumentException("onnx inputs: " + name + " " + key + " is " + value + ", but the model input " + info + " is not a float");
        }
    }

    private static List<Integer> dynamicRowDimensions(final OnnxGraphInfo.ValueInfo info) {
        final long[] shape = info.shape();
        final List<Integer> dimensions = new ArrayList<>();
        for(int d=1; d<shape.length; d++) {
            if(shape[d] < 0) {
                dimensions.add(d - 1);
            }
        }
        return dimensions;
    }

    private static Pad parsePad(final String name, final JsonElement pad) {
        if(pad == null || pad.isJsonNull()) {
            return null;
        }
        if(pad.isJsonPrimitive()) {
            return new Pad(pad.getAsDouble(), false, 1);
        }
        final JsonObject params = pad.getAsJsonObject();
        if(!params.has("value")) {
            throw new IllegalArgumentException("onnx inputs: " + name + " pad requires value: " + pad);
        }
        final String side = params.has("side") ? params.get("side").getAsString() : "right";
        if(!side.equals("left") && !side.equals("right")) {
            throw new IllegalArgumentException("onnx inputs: " + name + " pad.side must be left or right: " + side);
        }
        final int multiple = params.has("multiple") ? params.get("multiple").getAsInt() : 1;
        if(multiple < 1) {
            throw new IllegalArgumentException("onnx inputs: " + name + " pad.multiple must be over than zero: " + multiple);
        }
        return new Pad(params.get("value").getAsDouble(), side.equals("left"), multiple);
    }

    private static void validateConstant(final String where, final String name, final Constant constant, final OnnxGraphInfo.ValueInfo info) {
        if(constant.shape != null && constant.shape.length != info.rank()) {
            throw new IllegalArgumentException(where + ": " + name + " has rank " + constant.shape.length
                    + " in its empty or fill, but the model input is " + info);
        }
        validateValue(name, "fill", constant.value, info);
    }

    private static Constant parseConstant(final String where, final String name, final JsonObject select) {
        final JsonElement empty = select.remove("empty");
        final JsonElement fill = select.remove("fill");
        final JsonElement shape = select.remove("shape");
        if(empty == null && fill == null) {
            if(shape != null) {
                select.add("shape", shape);
            }
            return null;
        }
        if(empty != null && fill != null) {
            throw new IllegalArgumentException(where + ": " + name + " must not have both empty and fill");
        }
        if(select.size() != 1) {
            throw new IllegalArgumentException(where + ": " + name + " empty or fill can not be used with a select function: " + select);
        }
        if(empty != null) {
            if(shape != null) {
                throw new IllegalArgumentException(where + ": " + name + " empty is the shape: shape is not needed");
            }
            final long[] emptyShape = parseShape(where, name, empty);
            if(Arrays.stream(emptyShape).noneMatch(dim -> dim == 0)) {
                throw new IllegalArgumentException(where + ": " + name + " empty shape must have a dimension of 0: " + empty);
            }
            return new Constant(emptyShape, 0);
        }
        return new Constant(shape == null ? null : parseShape(where, name, shape), fill.getAsDouble());
    }

    private static long[] parseShape(final String where, final String name, final JsonElement shape) {
        if(!shape.isJsonArray()) {
            throw new IllegalArgumentException(where + ": " + name + " shape must be an array: " + shape);
        }
        final long[] dims = new long[shape.getAsJsonArray().size()];
        for(int i=0; i<dims.length; i++) {
            dims[i] = shape.getAsJsonArray().get(i).getAsLong();
            if(dims[i] < 0) {
                throw new IllegalArgumentException(where + ": " + name + " shape must not have a negative dimension: " + shape);
            }
        }
        return dims;
    }

    private static int rank(final String name, final Schema.FieldType fieldType) {
        return switch (fieldType.getType()) {
            case array -> 1 + rank(name, fieldType.getArrayValueType());
            case matrix -> fieldType.getShape().size();
            case element, map, json -> throw new IllegalArgumentException("onnx inputs: " + name + " of type " + fieldType + " can not be a model input");
            default -> 0;
        };
    }

    public void setup() {
        select.setup();
    }

    /** The model input {@code name} when a later value of {@code fieldType} replaces what {@code inputs} gives it, at the same rank. */
    ModelInput replacement(final String name, final Schema.FieldType fieldType) {
        final ModelInput input = modelInputs.stream().filter(i -> i.info.name().equals(name)).findFirst().orElseThrow();
        final int rank = rank(name, fieldType);
        // a constant has no rank of its own: the value follows what the other inputs do
        final boolean stacked = input.constant == null ? input.stacked : batched && rank == input.info.rank() - 1;
        final int expected = stacked ? input.info.rank() - 1 : input.info.rank();
        if(rank != expected) {
            throw new IllegalArgumentException("onnx loop.next: " + name + " of type " + fieldType + " has rank " + rank
                    + ", but the model input " + input.info + " takes a value of rank " + expected);
        }
        return new ModelInput(input.info, input.pad, null, fieldType, rank, stacked);
    }

    /**
     * The model input {@code name} when {@code empty} or {@code fill} of {@code item} replaces what {@code inputs} gives it,
     * or null if {@code item} has neither, in which case it is left as it was.
     */
    static ModelInput constantReplacement(final String name, final JsonObject item, final Map<String, OnnxGraphInfo.ValueInfo> modelInputInfos) {
        final Constant constant = parseConstant("onnx loop.next", name, item);
        if(constant == null) {
            return null;
        }
        final OnnxGraphInfo.ValueInfo info = modelInputInfos.get(name);
        if(info == null) {
            throw new IllegalArgumentException("onnx loop.next: " + name + " has empty or fill, but it is not a model input: " + modelInputInfos.keySet());
        }
        validateConstant("onnx loop.next", name, constant, info);
        return new ModelInput(info, null, constant, null, 0, false);
    }

    /** dim0 of the model outputs is the batch if a value was stacked on a new dim0; otherwise an output is the value of the record. */
    public boolean isBatched() {
        return batched;
    }

    /** A value is the model input as it is: a batch can only have one record. */
    public boolean isSingleRecord() {
        return singleRecord;
    }

    public Select getSelect() {
        return select;
    }

    /** Evaluates the select list on {@code values}, which then also hold the results. */
    public void evaluate(final Map<String, Object> values, final Instant timestamp) {
        apply(select, values, timestamp);
    }

    /** Whether a record can have no model outputs, as {@link #skipsModel} leaves nothing to ask the model for it. */
    public boolean canSkipModel() {
        return select.getSelectFunctions().stream().anyMatch(SelectFunction::canSkipModel);
    }

    /** Whether a value of the evaluated {@code values} leaves nothing to ask the model for the record. */
    public boolean skipsModel(final Map<String, Object> values) {
        return select.getSelectFunctions().stream().anyMatch(function -> function.skipsModel(values.get(function.getName())));
    }

    /**
     * Evaluates a select list in order on {@code values}, which also receive the results. Functions that do not take a
     * Tensor find every value as a record field would hold it.
     */
    public static Map<String, Object> apply(final Select select, final Map<String, Object> values, final Instant timestamp) {
        final Map<String, Object> results = new HashMap<>();
        // the values stay Tensors for the next function, and a list only boxes what a function reads of it
        Map<String, Object> recordValues = null;
        for(final SelectFunction function : select.getSelectFunctions()) {
            if(function.ignore()) {
                continue;
            }
            if(recordValues == null && !function.acceptsTensor()) {
                recordValues = new HashMap<>(values);
                recordValues.replaceAll((name, value) -> recordValue(value));
            }
            final Object result = function.apply(function.acceptsTensor() ? values : recordValues, timestamp);
            values.put(function.getName(), result);
            results.put(function.getName(), result);
            if(recordValues != null) {
                recordValues.put(function.getName(), recordValue(result));
            }
        }
        return results;
    }

    private static Object recordValue(final Object value) {
        return value instanceof Tensor tensor && tensor.rank() > 0 ? tensor.asList() : Tensor.materialize(value);
    }

    /** The tensors of a batch from the evaluated values of its records. The caller closes them. */
    public Map<String, OnnxTensor> tensors(final OrtEnvironment environment, final List<Map<String, Object>> records) throws OrtException {
        final Map<String, OnnxTensor> tensors = new HashMap<>();
        try {
            for(final ModelInput input : modelInputs) {
                tensors.put(input.info.name(), tensor(environment, input, records));
            }
        } catch (final RuntimeException | OrtException e) {
            // the caller only closes the tensors it receives
            tensors.values().forEach(OnnxTensor::close);
            throw e;
        }
        return tensors;
    }

    static OnnxTensor tensor(final OrtEnvironment environment, final ModelInput input, final List<Map<String, Object>> records) throws OrtException {
        final String name = input.info.name();
        final Tensor.Type type = OnnxRuntimeUtil.tensorType(input.info.type());
        final Tensor tensor;
        if(input.constant != null) {
            tensor = Tensor.fill(type, constantShape(input, records.size()), input.constant.value);
        } else if(input.stacked) {
            final List<Tensor> rows = new ArrayList<>(records.size());
            for(final Map<String, Object> record : records) {
                rows.add(toTensor(name, record.get(name), input, type));
            }
            tensor = input.pad == null
                    ? Tensor.stack(rows, null, false)
                    : Tensor.stack(rows, input.pad.value, input.pad.left, multiples(input));
        } else {
            if(records.size() != 1) {
                throw new IllegalArgumentException("onnx inputs: " + name + " is the model input as it is, which takes one record at a time, but got " + records.size());
            }
            tensor = toTensor(name, records.getFirst().get(name), input, type);
        }
        return OnnxRuntimeUtil.toOnnxTensor(environment, tensor, input.info.type());
    }

    // a fixed dimension of the model input keeps its length
    private static long[] multiples(final ModelInput input) {
        final long[] multiples = new long[input.rank];
        Arrays.fill(multiples, 1L);
        for(final int dimension : dynamicRowDimensions(input.info)) {
            multiples[dimension] = input.pad.multiple;
        }
        return multiples;
    }

    private static long[] constantShape(final ModelInput input, final int batchSize) {
        if(input.constant.shape != null) {
            return input.constant.shape;
        }
        final long[] shape = input.info.shape().clone();
        for(int d=0; d<shape.length; d++) {
            if(shape[d] < 0) {
                shape[d] = d == 0 ? batchSize : 0;
            }
        }
        return shape;
    }

    private static Tensor toTensor(final String name, final Object value, final ModelInput input, final Tensor.Type type) {
        final Tensor tensor = switch (value) {
            case null -> throw new IllegalArgumentException("onnx model input: " + name + " is null");
            case Tensor t -> t.as(type);
            case List<?> list -> {
                if(input.fieldType.getType() == Schema.Type.matrix) {
                    yield Tensor.of(type, list, Tensor.flatShape("onnx model input: " + name, input.fieldType.getShape(), list.size()));
                }
                final List<Object> flat = new ArrayList<>();
                final List<Long> shape = new ArrayList<>();
                flatten(name, list, 0, shape, flat);
                yield Tensor.of(type, flat, shape.stream().mapToLong(Long::longValue).toArray());
            }
            default -> Tensor.of(type, List.of(value), new long[0]);
        };
        if(tensor.rank() != input.rank) {
            throw new IllegalArgumentException("onnx model input: " + name + " must have rank " + input.rank + ", but got " + tensor);
        }
        return tensor;
    }

    private static void flatten(final String name, final List<?> list, final int dim, final List<Long> shape, final List<Object> flat) {
        if(shape.size() == dim) {
            shape.add((long) list.size());
        } else if(shape.get(dim) != list.size()) {
            throw new IllegalArgumentException("onnx model input: " + name + " has arrays of different lengths at dimension " + dim + ": " + shape.get(dim) + " and " + list.size());
        }
        for(final Object value : list) {
            if(value instanceof List<?> child) {
                flatten(name, child, dim + 1, shape, flat);
            } else {
                flat.add(value);
            }
        }
    }

}
