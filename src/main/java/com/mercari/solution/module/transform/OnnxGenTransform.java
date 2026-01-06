package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.util.domain.ml.onnx.OnnxGenModel;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Transform.Module(name="onnx_gen")
public class OnnxGenTransform extends Transform {

    private static class Parameters implements Serializable {

        private String model;
        private String prompt;
        private Map<String, Double> searchOptions;
        private Map<String, Boolean> searchFlags;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();

            if(this.model == null) {
                errorMessages.add("parameters.model must not be null.");
            }
            if(this.prompt == null) {
                errorMessages.add("parameters.prompt must not be null.");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(searchOptions == null) {
                searchOptions = new HashMap<>();
            }
            if(searchFlags == null) {
                searchFlags = new HashMap<>();
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        final Schema inputSchema = Union.createUnionSchema(inputs);

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Generate", ParDo
                        .of(new GenerateDoFn(parameters, inputSchema, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        return MCollectionTuple.done(PDone.in(inputs.getPipeline()));
    }

    private static class GenerateDoFn extends DoFn<MElement, MElement> {

        private final Parameters parameters;
        private final Schema inputSchema;

        private final OnnxGenModel gen;

        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;


        GenerateDoFn(
                final Parameters parameters,
                final Schema inputSchema,
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.parameters = parameters;
            this.gen = OnnxGenModel.of(
                    parameters.model, parameters.prompt, parameters.searchOptions, parameters.searchFlags, inputSchema);
            this.inputSchema = inputSchema;

            this.logs = Logging.map(logging);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() throws Exception {
            final long currentMillis = Instant.now().getMillis();
            gen.setup();
            final long millis = Instant.now().getMillis() - currentMillis;
            LOG.info("setup millis: {}", millis);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                Logging.log(LOG, logs, "input", input);

                final long currentMillis = Instant.now().getMillis();
                final String response = gen.process(input);
                final long millis = Instant.now().getMillis() - currentMillis;
                LOG.info("response: {}, duration millis: {}", response, millis);
            }  catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to process http module", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        @Teardown
        public void teardown() {
            //gen.teardown();
        }
    }


}
