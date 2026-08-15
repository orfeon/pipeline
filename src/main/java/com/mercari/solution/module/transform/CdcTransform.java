package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.module.Transform.Module;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.cdc.ChangeRecord;
import com.mercari.solution.util.pipeline.cdc.SpannerChangeCapture;
import com.mercari.solution.util.pipeline.cdc.TiCdcChangeCapture;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Normalizes provider-specific change data capture records into the unified
 * {@link ChangeRecord} envelope. The same normalization applies to live streams
 * (spanner source changeDataCapture mode, kafka) and to records replayed from
 * files archived on GCS/S3 (storage/files source), so archive-then-batch-apply
 * pipelines reuse the exact conversion of the streaming path.
 */
@Module(name="cdc")
public class CdcTransform extends Transform {

    private static class Parameters implements Serializable {

        private Format format;
        private String field;
        private Boolean accumulate;

        private void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(format == null) {
                errorMessages.add("cdc transform module[" + name + "] requires 'format' parameter. one of: " + java.util.Arrays.toString(Format.values()));
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(accumulate == null) {
                accumulate = false;
            }
        }
    }

    private enum Format {
        spanner,
        ticdc
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema outputSchema = ChangeRecord.schema();

        final String jsonField;
        final boolean jsonFieldIsBytes;
        if(Format.ticdc.equals(parameters.format)) {
            jsonField = resolveJsonField(parameters, inputSchema);
            jsonFieldIsBytes = inputSchema.hasField(jsonField)
                    && Schema.Type.bytes.equals(inputSchema.getField(jsonField).getFieldType().getType());
        } else {
            jsonField = null;
            jsonFieldIsBytes = false;
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Normalize", ParDo
                        .of(new NormalizeDoFn(parameters.format, jsonField, jsonFieldIsBytes, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        PCollection<MElement> output = outputs.get(outputTag)
                .setCoder(ElementCoder.of(outputSchema));

        if(parameters.accumulate) {
            output = output
                    .apply("WithTableKeys", ParDo.of(new WithTableKeysDoFn()))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(outputSchema)))
                    .apply("GroupByTableKeys", GroupByKey.create())
                    .apply("SelectLatest", ParDo.of(new SelectLatestDoFn()))
                    .setCoder(ElementCoder.of(outputSchema));
        }

        return MCollectionTuple
                .of(output, outputSchema);
    }

    private String resolveJsonField(final Parameters parameters, final Schema inputSchema) {
        if(parameters.field != null) {
            if(!inputSchema.hasField(parameters.field)) {
                throw new IllegalModuleException(
                        "cdc transform module[" + getName() + "].field: " + parameters.field + " does not exist in input schema");
            }
            return parameters.field;
        }
        // kafka/pubsub message payload, files source content
        for(final String candidate : List.of("payload", "content")) {
            if(inputSchema.hasField(candidate)) {
                return candidate;
            }
        }
        throw new IllegalModuleException(
                "cdc transform module[" + getName() + "] with format 'ticdc' requires 'field' parameter (the input field carrying canal-json event text)");
    }

    private static class NormalizeDoFn extends DoFn<MElement, MElement> {

        private final Format format;
        private final String jsonField;
        private final boolean jsonFieldIsBytes;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        NormalizeDoFn(
                final Format format,
                final String jsonField,
                final boolean jsonFieldIsBytes,
                final List<Logging> loggings,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.format = format;
            this.jsonField = jsonField;
            this.jsonFieldIsBytes = jsonFieldIsBytes;
            this.logs = Logging.map(loggings);
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                Logging.log(LOG, logs, "input", input);
                final List<Map<String, Object>> envelopes = switch (format) {
                    case spanner -> SpannerChangeCapture.normalize(input);
                    case ticdc -> normalizeTiCdc(input);
                };
                for(final Map<String, Object> envelope : envelopes) {
                    final MElement output = MElement.of(envelope, c.timestamp());
                    c.output(output);
                    Logging.log(LOG, logs, "output", output);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to normalize change record", input, e, failFast);
                c.output(failureTag, badRecord);
            }
        }

        private List<Map<String, Object>> normalizeTiCdc(final MElement input) {
            final String content = extractText(input);
            final List<Map<String, Object>> envelopes = new ArrayList<>();
            if(content == null) {
                return envelopes;
            }
            // TiCDC storage sink files are newline-delimited events; a Kafka payload is a single line
            for(final String line : content.split("\n")) {
                if(line.isBlank()) {
                    continue;
                }
                envelopes.addAll(TiCdcChangeCapture.normalize(line.trim()));
            }
            return envelopes;
        }

        private String extractText(final MElement input) {
            if(jsonFieldIsBytes) {
                final ByteBuffer byteBuffer = input.getAsBytes(jsonField);
                if(byteBuffer == null) {
                    return null;
                }
                final byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.asReadOnlyBuffer().get(bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return input.getAsString(jsonField);
        }
    }

    private static class WithTableKeysDoFn extends DoFn<MElement, KV<String, MElement>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final String key = element.getAsString(ChangeRecord.FIELD_TABLE)
                    + "#" + element.getAsString(ChangeRecord.FIELD_KEYS);
            c.output(KV.of(key, element));
        }
    }

    private static class SelectLatestDoFn extends DoFn<KV<String, Iterable<MElement>>, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final KV<String, Iterable<MElement>> kv = c.element();
            if(kv == null || kv.getValue() == null) {
                return;
            }
            MElement latest = null;
            String latestSequence = null;
            for(final MElement element : kv.getValue()) {
                final String sequence = element.getAsString(ChangeRecord.FIELD_SEQUENCE);
                if(latest == null || ChangeRecord.compareSequence(sequence, latestSequence) > 0) {
                    latest = element;
                    latestSequence = sequence;
                }
            }
            if(latest != null) {
                c.output(latest);
            }
        }
    }

}
