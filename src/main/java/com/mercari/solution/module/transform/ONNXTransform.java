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
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;
import java.util.*;


@Transform.Module(name="onnx")
public class ONNXTransform extends Transform {

    private static class Parameters {

        private OnnxModel.Config model;
        private MappingConfig mapping;
        private List<MappingConfig> mappings;
        private Integer bufferSize;

        private JsonElement filter;
        private JsonArray preprocesses;
        private JsonArray postprocesses;

        private List<String> groupFields;

        private DataType outputType;

        public void validate(final Map<String, Schema> inputSchemas) {
            final List<String> errorMessages = new ArrayList<>();
            if(model == null) {
                errorMessages.add("parameters.model must not be null");
            } else {
                errorMessages.addAll(model.validate());
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
            if(mappings == null) {

            } else {

            }
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
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

        public MappingConfig create(Map<String, String> inputs, Map<String, String> outputs) {
            final MappingConfig config = new MappingConfig();
            config.inputs = inputs;
            config.outputs = outputs;
            return config;
        }

        public void setDefaults() {
            if(inputs == null) {
                inputs = new HashMap<>();
            }
            if(outputs == null) {
                outputs = new HashMap<>();
            }
        }
    }

    private static Schema mergeSchema(
            final Schema inputSchema,
            final Select preprocesses,
            final Schema onnxOutputSchema) {

        final Schema.Builder builder = Schema.builder(inputSchema);
        final Schema selectOutputSchema = Select.createOutputSchema(preprocesses);
        for(final Schema.Field field : selectOutputSchema.getFields()) {
            builder.withField(field);
        }
        for(final Schema.Field field : onnxOutputSchema.getFields()) {
            builder.withField(field);
        }
        return builder.withType(DataType.ELEMENT).build();
    }

    @NonNull
    @Override
    public MCollectionTuple expand(
            @NonNull MCollectionTuple inputs,
            MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(new HashMap<>()); // TODO
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Select preprocessors = Select.of(parameters.preprocesses, inputSchema.getFields());

        final OnnxModel model = OnnxModel.create(parameters.model);
        final Schema onnxOutputSchema = model.outputSchema();

        Schema outputSchema = mergeSchema(inputSchema, preprocessors, onnxOutputSchema);
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

        private transient List<MElement> buffer;

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
            this.buffer = new ArrayList<>();
            this.model.setup();
            this.filter.setup();
            this.preprocessors.setup();
            this.postprocessors.setup();
            this.outputSchema.setup();
        }

        @StartBundle
        public void startBundle(final StartBundleContext c) {
            this.buffer.clear();
        }

        /*
        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            if(!buffer.isEmpty()) {
                inference(buffer);
                buffer.clear();
            }
        }
         */

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                if(!filter.filter(input)) {
                    return;
                }

                buffer.add(input);
                if(buffer.size() >= bufferSize) {
                    processBuffer(c);
                    buffer.clear();
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to inference onnx", input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        private void processBuffer(ProcessContext c) throws OrtException {
            final List<Map<String,Object>> inputs = new ArrayList<>();
            for(final MElement element : buffer) {
                final Map<String,Object> values = element.asPrimitiveMap();
                final Map<String,Object> values_ = preprocessors.select(element, c.timestamp());
                values.putAll(values_);
                inputs.add(values);
            }
            final List<Map<String,Object>> outputs = inference(inputs);
            for(Map<String, Object> values : outputs) {
                values = postprocessors.select(values, c.timestamp());
                MElement output = MElement.of(values, c.timestamp());
                output = output.convert(outputSchema.withType(DataType.AVRO));
                c.output(output);
            }
        }

        private List<Map<String,Object>> inference(final List<Map<String,Object>> inputs) throws OrtException {
            for(final MappingConfig mapping : mappings) {
                final List<Map<String,Object>> inputValuesList = new ArrayList<>();
                for(int i=0; i<inputs.size(); i++) {
                    final Map<String, Object> inputValues = createMap(inputs.get(i), mapping.inputs.keySet());
                    for(final Map.Entry<String, String> inputEntry : mapping.inputs.entrySet()) {
                        inputValues.put(inputEntry.getValue(), inputValues.remove(inputEntry.getKey()));
                    }
                    inputValuesList.add(inputValues);
                }
                final Map<String, OnnxTensor> values = ElementToOnnxConverter
                        .convert_(OrtEnvironment.getEnvironment(), model.getInputInfo(), inputValuesList);

                try(final OrtSession.Result result = model.run(values, null, mapping.outputs.keySet())) {
                    final List<Map<String,Object>> results = OnnxToElementConverter.convert(result);
                    for(int i=0; i<inputs.size(); i++) {
                        final Map<String,Object> outputValues = results.get(i);
                        for(final Map.Entry<String, String> outputEntry : mapping.outputs.entrySet()) {
                            outputValues.put(outputEntry.getValue(), outputValues.remove(outputEntry.getKey()));
                        }
                        inputs.get(i).putAll(outputValues);
                    }
                }
            }
            return inputs;
        }

        private static Map<String, Object> createMap(
                final Map<String, Object> map,
                final Collection<String> fields) {
            return updateMap(new HashMap<>(), map, fields);
        }

        private static Map<String, Object> updateMap(
                final Map<String, Object> newMap,
                final Map<String, Object> map,
                final Collection<String> fields) {
            for(final String field : fields) {
                final Object value = ElementSchemaUtil.getValue(map, field);
                newMap.put(field, value);
            }
            return newMap;
        }
    }

}
