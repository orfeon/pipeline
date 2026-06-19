package com.mercari.solution.module.failure;

import com.mercari.solution.module.*;
import com.mercari.solution.module.sink.StorageSink;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@FailureSink.Module(name="storage")
public class StorageFailureSink extends FailureSink {

    @Override
    public PDone expand(PCollection<BadRecord> input) {
        final StorageSink.Parameters parameters = getParameters(StorageSink.Parameters.class)
                .validate()
                .setDefaults();
        final org.apache.avro.Schema badRecordAvroSchema = FailureUtil.createBadRecordSchema();
        final Schema outputSchema = Schema.of(badRecordAvroSchema);

        final PCollection<MElement> elements = input
                .apply("ToElement", ParDo.of(new OutputDoFn(jobName, moduleName)))
                .setCoder(ElementCoder.of(outputSchema));

        final PCollection<MElement> outputFiles = StorageSink.expand(
                getName(), parameters, elements, outputSchema, outputSchema, MErrorHandler.empty());

        return PDone.in(input.getPipeline());
    }

    private static class OutputDoFn extends DoFn<BadRecord, MElement> {

        private final String jobName;
        private final String moduleName;

        private transient org.apache.avro.Schema badRecordAvroSchema;

        OutputDoFn(final String jobName, final String moduleName) {
            this.jobName = jobName;
            this.moduleName = moduleName;
        }

        @Setup
        public void setup() {
            this.badRecordAvroSchema = FailureUtil.createBadRecordSchema();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final BadRecord badRecord = c.element();
            if(badRecord == null) {
                return;
            }
            try {
                final GenericRecord record = convertToAvro(
                        badRecordAvroSchema, badRecord, jobName, moduleName, c.timestamp());
                final MElement output = MElement.of(record, c.timestamp());
                c.output(output);
            } catch (final Throwable e) {
                FAILURE_ERROR_COUNTER.inc();
                LOG.error("Failed to send bad record: {} to pubsub cause: {}", badRecord, e.getMessage());
            }
        }
    }

}