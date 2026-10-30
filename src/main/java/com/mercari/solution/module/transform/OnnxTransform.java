package com.mercari.solution.module.transform;

import ai.onnxruntime.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.domain.ml.onnx.OnnxModel;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToOnnxConverter;
import com.mercari.solution.util.schema.converter.OnnxToElementConverter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;


@Transform.Module(name="onnx")
public class OnnxTransform extends Transform {

    private static class Parameters {

        private OnnxModel.Config model;
        private MappingConfig mapping;
        private List<MappingConfig> mappings;
        private Integer bufferSize;

        private JsonElement filter;
        private JsonArray preprocesses;
        private JsonArray postprocesses;

        private DataType outputType;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(model == null) {
                errorMessages.add("parameters.model must not be null");
            } else {
                errorMessages.addAll(model.validate());
            }
            if(mapping == null && (mappings == null || mappings.isEmpty())) {
                errorMessages.add("parameters.mappings must not be empty");
            } else if(mapping != null && mappings != null && !mappings.isEmpty()) {
                errorMessages.add("parameters.mapping and parameters.mappings must not be specified together");
            }

            if(bufferSize != null && bufferSize < 1) {
                errorMessages.add("parameters.bufferSize must be over than zero");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            model.setDefaults();
            if(mappings == null || mappings.isEmpty()) {
                mappings = new ArrayList<>();
                mappings.add(mapping);
            }
            for(final MappingConfig m : mappings) {
                m.setDefaults();
            }

            if(bufferSize == null) {
                bufferSize = 1;
            }
            if(outputType == null) {
                outputType = DataType.AVRO;
            }
        }

    }

    public static class MappingConfig implements Serializable {

        private Map<String, String> inputs;
        private Map<String, String> outputs;

        public void setDefaults() {
            if(inputs == null) {
                inputs = new HashMap<>();
            }
            if(outputs == null) {
                outputs = new HashMap<>();
            }
        }

        // model output names to request; null requests every output of the model
        private Set<String> requestedOutputs() {
            return outputs.isEmpty() ? null : outputs.keySet();
        }

        private String outputFieldName(final String outputName) {
            return outputs.getOrDefault(outputName, outputName);
        }
    }

    private static Schema mergeSchema(
            final Schema inputSchema,
            final Select preprocesses,
            final OnnxModel model,
            final List<MappingConfig> mappings) {

        final Schema.Builder builder = Schema.builder(inputSchema);
        if(preprocesses.useSelect()) {
            for(final Schema.Field field : Select.createOutputSchema(preprocesses).getFields()) {
                builder.withField(field);
            }
        }
        final Map<String, NodeInfo> outputInfo = model.getOutputInfo();
        for(final MappingConfig mapping : mappings) {
            final List<String> outputNames = mapping.outputs.isEmpty()
                    ? new ArrayList<>(outputInfo.keySet()) : new ArrayList<>(mapping.outputs.keySet());
            final Schema onnxOutputSchema = OnnxToElementConverter.convertOutputSchema(outputInfo, outputNames);
            for(final Schema.Field field : onnxOutputSchema.getFields()) {
                builder.withField(mapping.outputFieldName(field.getName()), field.getFieldType());
            }
        }
        return builder.withType(DataType.ELEMENT).build();
    }

    @NonNull
    @Override
    public MCollectionTuple expand(
            @NonNull MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Select preprocessors = Select.of(parameters.preprocesses, inputSchema.getFields());

        final OnnxModel model = OnnxModel.create(parameters.model);

        Schema outputSchema = mergeSchema(inputSchema, preprocessors, model, parameters.mappings);
        final Select postprocessors = Select.of(parameters.postprocesses, outputSchema.getFields());
        if(postprocessors.useSelect()) {
            outputSchema = Select.createOutputSchema(postprocessors);
        }

        final Filter filter = Filter.of(parameters.filter);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Inference", ParDo.of(new InferenceDoFn(
                                model, filter, preprocessors, postprocessors,
                                parameters.mappings, parameters.bufferSize,
                                outputSchema, getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema.withType(DataType.AVRO));
    }

    private static class InferenceDoFn extends DoFn<MElement, MElement> {

        private final OnnxModel model;
        private final List<MappingConfig> mappings;
        private final Integer bufferSize;

        private final Filter filter;
        private final Select preprocessors;
        private final Select postprocessors;

        private final Schema outputSchema;

        private final TupleTag<BadRecord> failureTag;
        private final boolean failFast;

        // buffered elements per window: a batch is flushed by the element that fills it, which shares its window
        private transient Map<BoundedWindow, List<Buffered>> buffers;

        private record Buffered(MElement element, Map<String, Object> values, Instant timestamp, BoundedWindow window) {}

        private interface Emitter {
            void output(MElement element, Instant timestamp, BoundedWindow window);
            void failure(BadRecord badRecord, Instant timestamp, BoundedWindow window);
        }

        public InferenceDoFn(
                final OnnxModel model,
                final Filter filter,
                final Select preprocessors,
                final Select postprocessors,
                final List<MappingConfig> mappings,
                final Integer bufferSize,
                final Schema outputSchema,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.filter = filter;

            this.model = model;
            this.preprocessors = preprocessors;
            this.postprocessors = postprocessors;
            this.mappings = mappings;
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
            this.preprocessors.setup();
            this.postprocessors.setup();
            this.outputSchema.setup();
        }

        @StartBundle
        public void startBundle(final StartBundleContext c) {
            this.buffers.clear();
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            final Emitter emitter = new Emitter() {
                @Override
                public void output(final MElement element, final Instant timestamp, final BoundedWindow window) {
                    c.output(element, timestamp, window);
                }
                @Override
                public void failure(final BadRecord badRecord, final Instant timestamp, final BoundedWindow window) {
                    c.output(failureTag, badRecord, timestamp, window);
                }
            };
            for(final List<Buffered> buffer : buffers.values()) {
                flush(buffer, emitter);
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
            try {
                if(!filter.filter(input)) {
                    return;
                }
                values = input.asPrimitiveMap();
                values.putAll(preprocessors.select(input, c.timestamp()));
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to preprocess onnx input", input, e, failFast);
                c.output(failureTag, badRecord);
                return;
            }

            final List<Buffered> buffer = buffers.computeIfAbsent(window, w -> new ArrayList<>());
            buffer.add(new Buffered(input, values, c.timestamp(), window));
            if(buffer.size() >= bufferSize) {
                buffers.remove(window);
                flush(buffer, new Emitter() {
                    @Override
                    public void output(final MElement element, final Instant timestamp, final BoundedWindow w) {
                        c.outputWithTimestamp(element, timestamp);
                    }
                    @Override
                    public void failure(final BadRecord badRecord, final Instant timestamp, final BoundedWindow w) {
                        c.outputWithTimestamp(failureTag, badRecord, timestamp);
                    }
                });
            }
        }

        private void flush(final List<Buffered> buffer, final Emitter emitter) {
            if(buffer.isEmpty()) {
                return;
            }
            final List<Map<String, Object>> outputs;
            try {
                final List<Map<String, Object>> inputs = new ArrayList<>();
                for(final Buffered buffered : buffer) {
                    inputs.add(buffered.values);
                }
                outputs = inference(inputs);
            } catch (final Throwable e) {
                for(final Buffered buffered : buffer) {
                    final BadRecord badRecord = processError("Failed to inference onnx", buffered.element, e, failFast);
                    emitter.failure(badRecord, buffered.timestamp, buffered.window);
                }
                return;
            }

            for(int i=0; i<buffer.size(); i++) {
                final Buffered buffered = buffer.get(i);
                try {
                    final Map<String, Object> values = postprocessors.select(outputs.get(i), buffered.timestamp);
                    final MElement output = MElement.of(values, buffered.timestamp)
                            .convert(outputSchema.withType(DataType.AVRO));
                    emitter.output(output, buffered.timestamp, buffered.window);
                } catch (final Throwable e) {
                    final BadRecord badRecord = processError("Failed to postprocess onnx output", buffered.element, e, failFast);
                    emitter.failure(badRecord, buffered.timestamp, buffered.window);
                }
            }
        }

        private List<Map<String,Object>> inference(final List<Map<String,Object>> inputs) throws OrtException {
            for(final MappingConfig mapping : mappings) {
                final List<Map<String,Object>> inputValuesList = new ArrayList<>();
                for(final Map<String, Object> input : inputs) {
                    final Map<String, Object> inputValues = new HashMap<>();
                    for(final Map.Entry<String, String> inputEntry : mapping.inputs.entrySet()) {
                        inputValues.put(inputEntry.getValue(), ElementSchemaUtil.getValue(input, inputEntry.getKey()));
                    }
                    inputValuesList.add(inputValues);
                }
                final Map<String, OnnxTensor> tensors = ElementToOnnxConverter
                        .convert_(OrtEnvironment.getEnvironment(), model.getInputInfo(), inputValuesList);
                try(final OrtSession.Result result = model.run(tensors, null, mapping.requestedOutputs())) {
                    final List<Map<String,Object>> results = OnnxToElementConverter.convert(result, inputs.size());
                    for(int i=0; i<inputs.size(); i++) {
                        for(final Map.Entry<String, Object> outputEntry : results.get(i).entrySet()) {
                            inputs.get(i).put(mapping.outputFieldName(outputEntry.getKey()), outputEntry.getValue());
                        }
                    }
                } finally {
                    for(final OnnxTensor tensor : tensors.values()) {
                        tensor.close();
                    }
                }
            }
            return inputs;
        }

    }

}
