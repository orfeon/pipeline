package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.pipeline.select.Vectors;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import com.mercari.solution.util.schema.converter.OnnxToElementConverter;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;

/**
 * The {@code loop} parameter of the onnx transform: runs the model again with what the last run gave, for one record.
 *
 * <p>After each run {@code next} is evaluated in order, then {@code until} is checked: it stops the loop when it holds, as
 * in the http source, but after {@code next} here, as what it checks is typically a token chosen from the outputs. An
 * item of {@code next} whose value is the name of a model output gives that tensor to the next run as it is, without a copy
 * and keeping its type, so that a KV cache does not cost more than the model does. An item whose value is a select
 * function is evaluated like an item of {@code inputs}, and {@code empty} or {@code fill} gives a model input that constant
 * from the second run on. A model input that is not in {@code next} keeps its tensor.
 */
public class OnnxLoop implements Serializable {

    /** Number of the last run, from 0, in {@code next}, {@code until} and {@code outputs}. */
    public static final String ITERATION = Vectors.ITERATION;

    private final Select vars;
    private final Select next;
    // model input to the model output that feeds it
    private final Map<String, String> feeds;
    private final Map<String, OnnxInputs.ModelInput> replacements;
    private final String untilJson;
    private final int maxIterations;
    private final Set<String> valueOutputs;
    private final List<Schema.Field> fields;

    private transient Filter.ConditionNode until;

    private OnnxLoop(
            final Select vars,
            final Select next,
            final Map<String, String> feeds,
            final Map<String, OnnxInputs.ModelInput> replacements,
            final String untilJson,
            final int maxIterations,
            final Set<String> valueOutputs,
            final List<Schema.Field> fields) {

        this.vars = vars;
        this.next = next;
        this.feeds = feeds;
        this.replacements = replacements;
        this.untilJson = untilJson;
        this.maxIterations = maxIterations;
        this.valueOutputs = valueOutputs;
        this.fields = fields;
    }

    /**
     * @param inputFields what {@code vars} can refer to: the fields of the record and the items of {@code inputs}
     * @param outputFields the model outputs, which {@code next} and {@code until} can refer to as well
     */
    public static OnnxLoop of(
            final JsonObject loop,
            final OnnxGraphInfo graph,
            final OnnxInputs inputs,
            final List<Schema.Field> inputFields,
            final List<Schema.Field> outputFields) {

        if(!loop.has("next") || !loop.get("next").isJsonObject() || loop.getAsJsonObject("next").isEmpty()) {
            throw new IllegalArgumentException("onnx loop.next must not be empty");
        }
        if(!loop.has("until") || loop.get("until").isJsonNull()) {
            throw new IllegalArgumentException("onnx loop.until must not be null");
        }
        // required, unlike the http source: a loop that does not stop holds a worker with a model
        if(!loop.has("maxIterations") || loop.get("maxIterations").isJsonNull() || loop.get("maxIterations").getAsInt() < 1) {
            throw new IllegalArgumentException("onnx loop.maxIterations is required, and must be over than zero");
        }

        final Set<String> outputNames = new HashSet<>();
        outputFields.forEach(field -> outputNames.add(field.getName()));

        final JsonArray varItems = new JsonArray();
        if(loop.has("vars")) {
            for(final Map.Entry<String, JsonElement> entry : objectOf("vars", loop.get("vars")).entrySet()) {
                varItems.add(item("vars", entry.getKey(), entry.getValue(), outputNames));
            }
        }
        final Select vars = Select.of(varItems, inputFields);

        // a model output, then a loop variable or an item of inputs, as the values are looked up
        final Map<String, Schema.Field> context = new LinkedHashMap<>();
        for(final Schema.Field field : inputFields) {
            context.put(field.getName(), field);
        }
        for(final SelectFunction function : vars.getSelectFunctions()) {
            context.put(function.getName(), Schema.Field.of(function.getName(), function.getOutputFieldType()));
        }
        for(final Schema.Field field : outputFields) {
            context.put(field.getName(), field);
        }
        context.put(ITERATION, Schema.Field.of(ITERATION, Schema.FieldType.INT64));

        final Map<String, String> feeds = new LinkedHashMap<>();
        final Map<String, OnnxInputs.ModelInput> constants = new LinkedHashMap<>();
        final JsonArray nextItems = new JsonArray();
        for(final Map.Entry<String, JsonElement> entry : loop.getAsJsonObject("next").entrySet()) {
            final String key = entry.getKey();
            final JsonElement value = entry.getValue();
            if(value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
                putFeeds(feeds, key, value.getAsString(), graph);
            } else {
                final JsonObject item = item("next", key, value, outputNames);
                final OnnxInputs.ModelInput constant = OnnxInputs.constantReplacement(key, item, graph.inputs());
                if(constant != null) {
                    constants.put(key, constant);
                } else {
                    nextItems.add(item);
                }
            }
        }
        final Select next = Select.of(nextItems, new ArrayList<>(context.values()));

        final Map<String, OnnxInputs.ModelInput> replacements = new LinkedHashMap<>();
        for(final SelectFunction function : next.getSelectFunctions()) {
            context.put(function.getName(), Schema.Field.of(function.getName(), function.getOutputFieldType()));
            if(graph.inputs().containsKey(function.getName())) {
                if(feeds.containsKey(function.getName())) {
                    throw new IllegalArgumentException("onnx loop.next: " + function.getName() + " is given both a model output and a select function");
                }
                replacements.put(function.getName(), inputs.replacement(function.getName(), function.getOutputFieldType()));
            }
        }
        for(final Map.Entry<String, OnnxInputs.ModelInput> constant : constants.entrySet()) {
            if(feeds.containsKey(constant.getKey())) {
                throw new IllegalArgumentException("onnx loop.next: " + constant.getKey() + " is given both a model output and empty or fill");
            }
            replacements.put(constant.getKey(), constant.getValue());
        }

        final String untilJson = loop.get("until").toString();
        final Set<String> untilVariables = Filter.parse(untilJson).getRequiredVariables();
        // a filter without a condition does not hold, and every record would run up to maxIterations: false says that on purpose
        final JsonElement untilElement = loop.get("until");
        if(untilVariables.isEmpty() && !(untilElement.isJsonPrimitive() && untilElement.getAsJsonPrimitive().isBoolean())) {
            throw new IllegalArgumentException("onnx loop.until has no condition: " + untilJson);
        }
        validateUntil(untilVariables, new ArrayList<>(context.values()));

        // what the functions read: the outputs that only feed the next run are not copied
        final Set<String> valueOutputs = OnnxInputs.mentioned(nextItems + untilJson, outputNames);

        final List<Schema.Field> fields = new ArrayList<>();
        for(final SelectFunction function : vars.getSelectFunctions()) {
            fields.add(Schema.Field.of(function.getName(), function.getOutputFieldType()));
        }
        for(final SelectFunction function : next.getSelectFunctions()) {
            fields.add(Schema.Field.of(function.getName(), function.getOutputFieldType()));
        }
        fields.add(context.get(ITERATION));

        return new OnnxLoop(vars, next, feeds, replacements, untilJson, loop.get("maxIterations").getAsInt(), valueOutputs, fields);
    }

    // a filter compares one value: an array would never match, and every record would run up to maxIterations
    private static void validateUntil(final Set<String> variables, final List<Schema.Field> fields) {
        final Set<String> unknown = new TreeSet<>();
        for(final String variable : variables) {
            final Schema.FieldType type;
            try {
                type = ElementSchemaUtil.getInputFieldType(variable, fields);
            } catch (final IllegalArgumentException e) {
                unknown.add(variable);
                continue;
            }
            if(type.getType() == Schema.Type.array || type.getType() == Schema.Type.matrix) {
                throw new IllegalArgumentException("onnx loop.until: " + variable + " is an array or a matrix, which a filter can not compare: take one value of it with a select function in next");
            }
        }
        if(!unknown.isEmpty()) {
            throw new IllegalArgumentException("onnx loop.until refers to what the loop does not have: " + unknown);
        }
    }

    private static JsonObject objectOf(final String parameter, final JsonElement element) {
        if(!element.isJsonObject()) {
            throw new IllegalArgumentException("onnx loop." + parameter + " must be a map of names: " + element);
        }
        return element.getAsJsonObject();
    }

    private static JsonObject item(final String parameter, final String name, final JsonElement value, final Set<String> outputNames) {
        if(!value.isJsonObject()) {
            throw new IllegalArgumentException("onnx loop." + parameter + ": " + name + " must be a select function"
                    + ("next".equals(parameter) ? " or the name of a model output: " : ": ") + value);
        }
        if(OnnxInputs.hasVariables(name)) {
            throw new IllegalArgumentException("onnx loop." + parameter + ": " + name + " with name variables must be the name of a model output with them");
        }
        if(ITERATION.equals(name)) {
            throw new IllegalArgumentException("onnx loop." + parameter + ": " + ITERATION + " is the number of the last run");
        }
        // the value of a run would replace it, or the other way round, depending on the step
        if(outputNames.contains(name)) {
            throw new IllegalArgumentException("onnx loop." + parameter + ": " + name + " has the name of a model output");
        }
        final JsonObject item = value.getAsJsonObject().deepCopy();
        item.addProperty("name", name);
        return item;
    }

    // the variables of the name of the input fill those of the name of the output, in the order they appear
    private static void putFeeds(final Map<String, String> feeds, final String input, final String output, final OnnxGraphInfo graph) {
        final List<String> names = OnnxInputs.expandName(input, graph.inputs().keySet());
        for(final String name : names) {
            final OnnxGraphInfo.ValueInfo inputInfo = graph.inputs().get(name);
            if(inputInfo == null) {
                throw new IllegalArgumentException("onnx loop.next: " + name + " has the name of a model output as its value, but it is not a model input: " + graph.inputs().keySet());
            }
            final String outputName = outputName(input, output, name);
            final OnnxGraphInfo.ValueInfo outputInfo = graph.outputs().get(outputName);
            if(outputInfo == null || !outputInfo.isTensor()) {
                throw new IllegalArgumentException("onnx loop.next: " + name + " is given " + outputName + ", which is not a model output: " + graph.outputs().keySet());
            }
            // it goes to the model as it is, without a conversion
            if(inputInfo.type() != outputInfo.type() || inputInfo.rank() != outputInfo.rank()) {
                throw new IllegalArgumentException("onnx loop.next: the model input " + inputInfo + " can not take the model output " + outputInfo);
            }
            if(feeds.put(name, outputName) != null) {
                throw new IllegalArgumentException("onnx loop.next: " + name + " is given more than once");
            }
        }
    }

    private static String outputName(final String input, final String output, final String name) {
        final Matcher matcher = OnnxInputs.namePattern(input).matcher(name);
        if(!matcher.matches()) {
            throw new IllegalStateException(name + " does not match " + input);
        }
        final StringBuilder outputName = new StringBuilder();
        int position = 0;
        for(int group=1; group<=matcher.groupCount(); group++) {
            final int variable = nextVariable(output, position);
            if(variable < 0) {
                throw new IllegalArgumentException("onnx loop.next: " + input + " has more name variables than " + output);
            }
            outputName.append(output, position, variable).append(matcher.group(group));
            position = variable + 2;
        }
        if(nextVariable(output, position) >= 0) {
            throw new IllegalArgumentException("onnx loop.next: " + output + " has more name variables than " + input);
        }
        return outputName.append(output.substring(position)).toString();
    }

    private static int nextVariable(final String name, final int from) {
        final int digits = name.indexOf("%d", from);
        final int letters = name.indexOf("%s", from);
        return digits < 0 ? letters : letters < 0 ? digits : Math.min(digits, letters);
    }

    /** The model outputs that {@code next} and {@code until} read as values. */
    public Set<String> getValueOutputs() {
        return valueOutputs;
    }

    /** The model outputs that only feed the next run: they are requested, but not copied. */
    public Set<String> getFeedOutputs() {
        return new LinkedHashSet<>(feeds.values());
    }

    /** What {@code outputs} can refer to besides the fields of the record, the items of {@code inputs} and the model outputs. */
    public List<Schema.Field> getFields() {
        return fields;
    }

    public void setup() {
        this.vars.setup();
        this.next.setup();
        this.until = Filter.parse(untilJson);
    }

    /**
     * Runs the model until the loop stops. {@code record}, the evaluated values of the record, then has the variables, the
     * last values of {@code next} and the values of the last run of the outputs that {@code next}, {@code until} and
     * {@code readOutputs} read.
     */
    public void run(
            final OnnxModel model,
            final OnnxInputs inputs,
            final Set<String> requestedOutputs,
            final Set<String> readOutputs,
            final Map<String, Object> record,
            final Instant timestamp) throws OrtException {

        OnnxInputs.apply(vars, record, timestamp);

        final OrtEnvironment environment = OrtEnvironment.getEnvironment();
        final List<Map<String, Object>> records = List.of(record);
        // the tensors made here, which stay for the runs that do not replace them
        final Map<String, OnnxTensor> owned = inputs.tensors(environment, records);
        final Map<String, OnnxTensor> tensors = new HashMap<>(owned);
        // the outputs that only the outputs select list reads are only copied from the run that stops the loop
        final Set<String> lastRunOutputs = new HashSet<>(readOutputs);
        lastRunOutputs.removeAll(valueOutputs);
        OrtSession.Result last = null;
        try {
            for(int iteration=0; ; iteration++) {
                final OrtSession.Result result = model.run(tensors, null, requestedOutputs);
                // the tensors of the last run fed this one, which is over: a KV cache is only held until here
                if(last != null) {
                    last.close();
                }
                last = result;

                record.putAll(OnnxToElementConverter.convert(result, 1, inputs.isBatched(), valueOutputs).getFirst());
                record.put(ITERATION, (long) iteration);
                OnnxInputs.apply(next, record, timestamp);
                if(iteration + 1 >= maxIterations || Filter.filter(until, record)) {
                    record.putAll(OnnxToElementConverter.convert(result, 1, inputs.isBatched(), lastRunOutputs).getFirst());
                    return;
                }

                for(final Map.Entry<String, String> feed : feeds.entrySet()) {
                    close(owned.remove(feed.getKey()));
                    tensors.put(feed.getKey(), (OnnxTensor) result.get(feed.getValue()).orElseThrow());
                }
                for(final Map.Entry<String, OnnxInputs.ModelInput> replacement : replacements.entrySet()) {
                    // a constant only has to be made once
                    if(iteration > 0 && replacement.getValue().constant() != null) {
                        continue;
                    }
                    close(owned.remove(replacement.getKey()));
                    final OnnxTensor tensor = OnnxInputs.tensor(environment, replacement.getValue(), records);
                    owned.put(replacement.getKey(), tensor);
                    tensors.put(replacement.getKey(), tensor);
                }
            }
        } finally {
            owned.values().forEach(OnnxTensor::close);
            if(last != null) {
                last.close();
            }
        }
    }

    private static void close(final OnnxTensor tensor) {
        if(tensor != null) {
            tensor.close();
        }
    }

}
