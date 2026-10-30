package com.mercari.solution.module.transform;

import ai.onnxruntime.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.domain.ml.onnx.OnnxGraphInfo;
import com.mercari.solution.util.domain.ml.onnx.OnnxInputs;
import com.mercari.solution.util.domain.ml.onnx.OnnxModel;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.OnnxToElementConverter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;


@Transform.Module(name="onnx")
public class OnnxTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(OnnxTransform.class);

    private static class Parameters {

        private OnnxModel.Config model;
        private JsonArray inputs;
        private JsonArray outputs;
        private Integer bufferSize;

        private JsonElement filter;

        private DataType outputType;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(model == null) {
                errorMessages.add("parameters.model must not be null");
            } else {
                errorMessages.addAll(model.validate());
            }
            if(inputs == null || inputs.isEmpty()) {
                errorMessages.add("parameters.inputs must not be empty");
            }
            if(outputs == null || outputs.isEmpty()) {
                errorMessages.add("parameters.outputs must not be empty");
            }

            if(bufferSize != null && bufferSize < 1) {
                errorMessages.add("parameters.bufferSize must be over than zero");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(bufferSize == null) {
                bufferSize = 1;
            }
            if(outputType == null) {
                outputType = DataType.AVRO;
            }
        }
    }

    // the model outputs named in the outputs select list: naming one that is not used only costs a little more memory,
    // while the opposite would give a null value silently, so that any mention counts (also in an expression or a template)
    private static Set<String> referencedOutputs(final JsonArray outputs, final OnnxGraphInfo graph) {
        final String text = outputs.toString();
        final Set<String> names = new LinkedHashSet<>();
        for(final OnnxGraphInfo.ValueInfo output : graph.outputs().values()) {
            // only a tensor can be read: a name such as "field" is in the text of every select item, and requesting a value
            // that is not a tensor would fail every record
            if(output.isTensor() && Pattern.compile("(?<![A-Za-z0-9_])" + Pattern.quote(output.name()) + "(?![A-Za-z0-9_])").matcher(text).find()) {
                names.add(output.name());
            }
        }
        if(names.isEmpty()) {
            throw new IllegalModuleException("parameters.outputs must refer to at least one of the model outputs: " + graph.outputs().keySet());
        }
        return names;
    }

    private static Schema createOutputSchema(
            final Schema inputSchema,
            final Select outputs) {

        // a later field replaces one of the same name, as its value does in the record
        final Map<String, Schema.Field> fields = new LinkedHashMap<>();
        for(final Schema.Field field : inputSchema.getFields()) {
            fields.put(field.getName(), field.copy());
        }
        for(final SelectFunction function : outputs.getSelectFunctions()) {
            fields.put(function.getName(), Schema.Field.of(function.getName(), function.getOutputFieldType()));
        }
        // from the fields only: Schema.builder(inputSchema) would keep the Avro schema of the input (BigQuery, Avro files, ...),
        // and the records would be written with it, without the added fields
        return Schema.builder()
                .withFields(new ArrayList<>(fields.values()))
                .withType(DataType.ELEMENT)
                .build();
    }

    // what the outputs select list can refer to; a model output comes last, as its value does when they are evaluated
    private static List<Schema.Field> outputsContextFields(
            final Schema inputSchema,
            final OnnxInputs inputs,
            final OnnxGraphInfo graph) {

        final Map<String, Schema.Field> fields = new LinkedHashMap<>();
        for(final Schema.Field field : inputSchema.getFields()) {
            fields.put(field.getName(), field.copy());
        }
        for(final SelectFunction function : inputs.getSelect().getSelectFunctions()) {
            fields.put(function.getName(), Schema.Field.of(function.getName(), function.getOutputFieldType()));
        }
        for(final OnnxGraphInfo.ValueInfo output : graph.outputs().values()) {
            if(output.isTensor()) {
                fields.put(output.name(), Schema.Field.of(output.name(), OnnxToElementConverter.convertFieldType(output, inputs.isBatched())));
            }
        }
        return new ArrayList<>(fields.values());
    }

    @NonNull
    @Override
    public MCollectionTuple expand(
            @NonNull MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.model.setDefaults();

        final OnnxModel model = OnnxModel.create(parameters.model);
        final OnnxGraphInfo graph = model.readGraphInfo();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final OnnxInputs onnxInputs = OnnxInputs.of(parameters.inputs, graph.inputs(), inputSchema.getFields());
        if(onnxInputs.isSingleRecord() && parameters.bufferSize > 1) {
            throw new IllegalModuleException("parameters.bufferSize must be 1: a value of inputs is the model input as it is, without a batch dimension");
        }
        final Set<String> requestedOutputs = referencedOutputs(parameters.outputs, graph);
        final Select outputSelect = Select.of(parameters.outputs, outputsContextFields(inputSchema, onnxInputs, graph));

        final Schema outputSchema = createOutputSchema(inputSchema, outputSelect);

        final Filter filter = Filter.of(parameters.filter);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Inference", ParDo.of(new InferenceDoFn(
                                model, filter, onnxInputs, outputSelect,
                                requestedOutputs, parameters.bufferSize,
                                outputSchema, getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema.withType(DataType.AVRO));
    }

    private static class InferenceDoFn extends DoFn<MElement, MElement> {

        private final OnnxModel model;
        private final OnnxInputs inputs;
        private final Select outputs;
        private final Set<String> requestedOutputs;
        private final Integer bufferSize;

        private final Filter filter;

        private final Schema outputSchema;

        private final TupleTag<BadRecord> failureTag;
        private final boolean failFast;

        // buffered elements per window: a batch is flushed by the element that fills it, which shares its window
        private transient Map<BoundedWindow, List<Buffered>> buffers;

        // values: the fields of the input record, evaluated: those with the values of the inputs select list
        private record Buffered(MElement element, Map<String, Object> values, Map<String, Object> evaluated, Instant timestamp) {}

        private interface Emitter {
            void output(MElement element, Instant timestamp);
            void failure(BadRecord badRecord, Instant timestamp);
        }

        public InferenceDoFn(
                final OnnxModel model,
                final Filter filter,
                final OnnxInputs inputs,
                final Select outputs,
                final Set<String> requestedOutputs,
                final Integer bufferSize,
                final Schema outputSchema,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.filter = filter;

            this.model = model;
            this.inputs = inputs;
            this.outputs = outputs;
            this.requestedOutputs = requestedOutputs;
            this.bufferSize = bufferSize;

            this.outputSchema = outputSchema;

            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.buffers = new LinkedHashMap<>();
            this.model.setup();
            this.filter.setup();
            this.inputs.setup();
            this.outputs.setup();
            this.outputSchema.setup();
        }

        @StartBundle
        public void startBundle(final StartBundleContext c) {
            this.buffers.clear();
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            for(final Map.Entry<BoundedWindow, List<Buffered>> entry : buffers.entrySet()) {
                final BoundedWindow window = entry.getKey();
                flush(entry.getValue(), new Emitter() {
                    @Override
                    public void output(final MElement element, final Instant timestamp) {
                        c.output(element, timestamp, window);
                    }
                    @Override
                    public void failure(final BadRecord badRecord, final Instant timestamp) {
                        c.output(failureTag, badRecord, timestamp, window);
                    }
                });
            }
            buffers.clear();
        }

        // buffered elements are emitted at their own event time, which may precede the element that flushes the batch
        @Override
        public Duration getAllowedTimestampSkew() {
            return Duration.millis(Long.MAX_VALUE);
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            final Map<String, Object> values;
            final Map<String, Object> evaluated;
            try {
                if(!filter.filter(input)) {
                    return;
                }
                values = input.asPrimitiveMap();
                evaluated = new HashMap<>(values);
                inputs.evaluate(evaluated, c.timestamp());
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to preprocess onnx input", input, e, failFast);
                c.output(failureTag, badRecord);
                return;
            }

            final Emitter emitter = new Emitter() {
                @Override
                public void output(final MElement element, final Instant timestamp) {
                    c.outputWithTimestamp(element, timestamp);
                }
                @Override
                public void failure(final BadRecord badRecord, final Instant timestamp) {
                    c.outputWithTimestamp(failureTag, badRecord, timestamp);
                }
            };
            final Buffered buffered = new Buffered(input, values, evaluated, c.timestamp());
            if(inputs.skipsModel(evaluated)) {
                emit(buffered, new HashMap<>(), emitter);
                return;
            }

            final List<Buffered> buffer = buffers.computeIfAbsent(window, w -> new ArrayList<>());
            buffer.add(buffered);
            if(buffer.size() >= bufferSize) {
                buffers.remove(window);
                flush(buffer, emitter);
            }
        }

        private void flush(final List<Buffered> buffer, final Emitter emitter) {
            final List<Map<String, Object>> modelOutputs;
            try {
                modelOutputs = inference(buffer);
            } catch (final Throwable e) {
                if(buffer.size() > 1) {
                    // one record can fail a whole batch: only the records that fail by themselves are failures
                    LOG.warn("Failed to inference a batch of {} records, retrying them one by one: {}", buffer.size(), e.getMessage());
                    for(final Buffered buffered : buffer) {
                        flush(List.of(buffered), emitter);
                    }
                    return;
                }
                final Buffered buffered = buffer.getFirst();
                final BadRecord badRecord = processError("Failed to inference onnx", buffered.element, e, failFast);
                emitter.failure(badRecord, buffered.timestamp);
                return;
            }

            for(int i=0; i<buffer.size(); i++) {
                emit(buffer.get(i), modelOutputs.get(i), emitter);
            }
        }

        private void emit(final Buffered buffered, final Map<String, Object> modelOutputs, final Emitter emitter) {
            final MElement output;
            try {
                output = createOutput(buffered, modelOutputs);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to postprocess onnx output", buffered.element, e, failFast);
                emitter.failure(badRecord, buffered.timestamp);
                return;
            }
            // outside the try: an error thrown by a fused downstream step is not a failure of this record
            emitter.output(output, buffered.timestamp);
        }

        private MElement createOutput(final Buffered buffered, final Map<String, Object> modelOutputs) {
            final Map<String, Object> context = buffered.evaluated;
            context.putAll(modelOutputs);

            final Map<String, Object> record = new HashMap<>(buffered.values);
            record.putAll(OnnxInputs.apply(outputs, context, buffered.timestamp));
            record.replaceAll((name, value) -> Tensor.materialize(value));
            return MElement.of(record, buffered.timestamp)
                    .convert(outputSchema.withType(DataType.AVRO));
        }

        private List<Map<String, Object>> inference(final List<Buffered> buffer) throws OrtException {
            final List<Map<String, Object>> records = new ArrayList<>(buffer.size());
            for(final Buffered buffered : buffer) {
                records.add(buffered.evaluated);
            }
            final Map<String, OnnxTensor> tensors = inputs.tensors(OrtEnvironment.getEnvironment(), records);
            try(final OrtSession.Result result = model.run(tensors, null, requestedOutputs)) {
                return OnnxToElementConverter.convert(result, buffer.size(), inputs.isBatched());
            } finally {
                for(final OnnxTensor tensor : tensors.values()) {
                    tensor.close();
                }
            }
        }

    }

}
