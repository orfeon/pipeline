package com.mercari.solution.module.source;

import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Module;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.ByteRecordReader;
import com.mercari.solution.util.schema.converter.CsvToElementConverter;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import com.mercari.solution.util.schema.fwf.FwfDecoder;
import com.mercari.solution.util.schema.fwf.FwfLayout;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The storage source's byte-record read path (work_fixedwidth.md §5): files are matched and opened
 * through FileIO and cut into records as bytes ({@link ByteRecordReader}), instead of TextIO's
 * UTF-8 lines. It serves {@code format: fwf} and, for csv / json, the options TextIO can not
 * provide ({@code additionalFields}). Everything file-scoped ({@code skipHeaderLines}, record
 * numbers) restarts per file, and a record that can not be decoded goes to the failure output.
 */
final class StorageRecordReader {

    enum Format { csv, json, fwf }

    enum RecordSplit { line, length }

    private static final List<String> ADDITIONAL_FIELD_KEYS = List.of("resource", "entry", "line", "lastModified");

    private StorageRecordReader() {}

    /** What to read and how to cut it; resolved from the module parameters at assembly time. */
    static class Spec implements Serializable {
        Format format;
        List<String> inputs;
        String compression;
        Integer skipHeaderLines;
        String filterPrefix;
        String delimiter;
        RecordSplit recordSplit;
        // projection: top-level field names (fwf only)
        List<String> fields;
        // metadata key (resource / entry / line / lastModified) -> output field name
        Map<String, String> additionalFields;
    }

    static MCollectionTuple expand(
            final PBegin begin,
            final String name,
            final Spec spec,
            final Schema schema,
            final String timestampAttribute,
            final boolean failFast,
            final MErrorHandler errorHandler) {

        final List<String> errors = new ArrayList<>();
        if(schema == null) {
            errors.add("parameters.schema is required for format " + spec.format);
        }
        final Compression compression = parseCompression(spec.compression, errors);
        if(spec.recordSplit == RecordSplit.length && spec.format != Format.fwf) {
            errors.add("parameters.recordSplit: length is only supported for format fwf");
        }
        if(spec.recordSplit == RecordSplit.length && spec.delimiter != null) {
            errors.add("parameters.delimiter can not be used with recordSplit: length");
        }
        if(spec.delimiter != null && spec.delimiter.isEmpty()) {
            errors.add("parameters.delimiter must not be empty");
        }
        if(spec.skipHeaderLines != null && spec.skipHeaderLines < 0) {
            errors.add("parameters.skipHeaderLines must not be negative");
        }
        if(!errors.isEmpty()) {
            throw new IllegalModuleException(errors);
        }

        final FwfDecoder decoder;
        final Charset charset;
        final List<Schema.Field> fields;
        if(spec.format == Format.fwf) {
            if(schema.getFwfLayout() == null) {
                throw new IllegalModuleException("format fwf requires parameters.schema with encoding.format: fwf and a layout in reference (uri or inline)");
            }
            try {
                decoder = FwfDecoder.of(schema, spec.fields);
            } catch (final IllegalArgumentException e) {
                throw new IllegalModuleException("parameters.fields: " + e.getMessage());
            }
            charset = decoder.getOptions().getCharset();
            final FwfLayout layout = decoder.getLayout();
            if(spec.recordSplit == RecordSplit.length && layout.getRecordLength() == null) {
                throw new IllegalModuleException("parameters.recordSplit: length requires recordLength in the fwf layout");
            }
            if(spec.recordSplit != RecordSplit.length && isWideCharset(charset)) {
                throw new IllegalModuleException("charset " + charset.name() + " can not be split into lines by a single-byte separator; use recordSplit: length");
            }
            fields = new ArrayList<>(layout.toSchemaFields());
        } else {
            if(schema.getFwfLayout() != null) {
                throw new IllegalModuleException("parameters.format: " + spec.format + " differs from schema.encoding.format: fwf");
            }
            decoder = null;
            charset = StandardCharsets.UTF_8;
            fields = new ArrayList<>(schema.getFields());
        }

        final Map<String, String> additionalFields = new LinkedHashMap<>();
        if(spec.additionalFields != null) {
            // appended in a fixed order, whatever the order in the config
            for(final String key : ADDITIONAL_FIELD_KEYS) {
                if(spec.additionalFields.containsKey(key)) {
                    additionalFields.put(key, spec.additionalFields.get(key));
                }
            }
            for(final String key : spec.additionalFields.keySet()) {
                if(!ADDITIONAL_FIELD_KEYS.contains(key)) {
                    errors.add("parameters.additionalFields." + key + " is not supported. supported keys: " + ADDITIONAL_FIELD_KEYS);
                }
            }
        }
        for(final Map.Entry<String, String> entry : additionalFields.entrySet()) {
            final String fieldName = entry.getValue();
            if(fieldName == null || fieldName.isBlank()) {
                errors.add("parameters.additionalFields." + entry.getKey() + " must be an output field name");
            } else if(Schema.hasField(fields, fieldName)) {
                errors.add("parameters.additionalFields." + entry.getKey() + ": the output field name " + fieldName + " is already used");
            } else {
                fields.add(Schema.Field.of(fieldName, switch (entry.getKey()) {
                    case "line" -> Schema.FieldType.INT64;
                    case "lastModified" -> Schema.FieldType.TIMESTAMP;
                    default -> Schema.FieldType.STRING;
                }));
            }
        }
        if(!errors.isEmpty()) {
            throw new IllegalModuleException(errors);
        }
        final Schema outputSchema = Schema.of(fields);

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<BadRecord> failureTag = new TupleTag<>(){};
        final PCollectionTuple outputs = begin
                .apply("Patterns", Create.of(spec.inputs).withCoder(StringUtf8Coder.of()))
                .apply("MatchFiles", FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
                // the bytes are decompressed in the DoFn: Beam's AUTO would hide the file name based
                // detection and, for .zip, concatenate the entries before this reader can see them
                .apply("ReadMatches", FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
                .apply("ReadRecords", ParDo
                        .of(new ReadRecordsDoFn(name, spec, compression, schema, decoder, charset.name(),
                                additionalFields, timestampAttribute, failFast, failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple.of(
                outputs.get(outputTag).setCoder(ElementCoder.of(outputSchema)),
                outputSchema);
    }

    // null: detect from the file name
    private static Compression parseCompression(final String compression, final List<String> errors) {
        if(compression == null) {
            return null;
        }
        try {
            final Compression value = Compression.valueOf(compression.trim().toUpperCase(Locale.ROOT));
            return Compression.AUTO.equals(value) ? null : value;
        } catch (final IllegalArgumentException e) {
            errors.add("parameters.compression: " + compression + " is not supported");
            return null;
        }
    }

    // UTF-16 / UTF-32: the bytes of a line feed appear inside other characters
    private static boolean isWideCharset(final Charset charset) {
        final String name = charset.name().toUpperCase(Locale.ROOT);
        return name.startsWith("UTF-16") || name.startsWith("UTF-32");
    }

    private static class ReadRecordsDoFn extends DoFn<FileIO.ReadableFile, MElement> {

        private final String name;
        private final Format format;
        private final Compression compression;
        private final int skipHeaderLines;
        private final byte[] filterPrefix;
        private final byte[] delimiter;
        private final boolean fixedLength;
        private final Schema schema;
        private final FwfDecoder decoder;
        private final String charsetName;
        private final Map<String, String> additionalFields;
        private final String timestampAttribute;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        private transient Charset charset;

        ReadRecordsDoFn(
                final String name,
                final Spec spec,
                final Compression compression,
                final Schema schema,
                final FwfDecoder decoder,
                final String charsetName,
                final Map<String, String> additionalFields,
                final String timestampAttribute,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            final Charset charset = Charset.forName(charsetName);
            this.name = name;
            this.format = spec.format;
            this.compression = compression;
            this.skipHeaderLines = spec.skipHeaderLines == null ? 0 : spec.skipHeaderLines;
            this.filterPrefix = spec.filterPrefix == null || spec.filterPrefix.isEmpty() ? null : spec.filterPrefix.getBytes(charset);
            this.delimiter = spec.delimiter == null ? null : spec.delimiter.getBytes(charset);
            this.fixedLength = spec.recordSplit == RecordSplit.length;
            this.schema = schema;
            this.decoder = decoder;
            this.charsetName = charsetName;
            this.additionalFields = new HashMap<>(additionalFields);
            this.timestampAttribute = timestampAttribute;
            this.failFast = failFast;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() {
            this.charset = Charset.forName(charsetName);
            if(decoder == null) {
                this.schema.setup();
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final FileIO.ReadableFile file = c.element();
            if(file == null) {
                return;
            }
            final MatchResult.Metadata metadata = file.getMetadata();
            final String resource = metadata.resourceId().toString();
            long number = 0;
            try(final ByteRecordReader reader = open(file)) {
                while(reader.next()) {
                    number = reader.number();
                    if(number <= skipHeaderLines) {
                        continue;
                    }
                    final byte[] buffer = reader.buffer();
                    final int length = reader.length();
                    // blank lines (and the CRLF / LF mix they come from) carry no record
                    if(length == 0 && !fixedLength) {
                        continue;
                    }
                    if(filterPrefix != null && startsWith(buffer, length, filterPrefix)) {
                        continue;
                    }
                    final Map<String, Object> values;
                    try {
                        values = decode(buffer, length);
                    } catch (final RuntimeException e) {
                        c.output(failureTag, failure("Failed to decode " + format + " record", resource, number, buffer, length, e));
                        continue;
                    }
                    for(final Map.Entry<String, String> entry : additionalFields.entrySet()) {
                        values.put(entry.getValue(), switch (entry.getKey()) {
                            case "resource" -> resource;
                            case "line" -> number;
                            case "lastModified" -> metadata.lastModifiedMillis() * 1000L;
                            // entry: the archive entry name; a plain file has none
                            default -> null;
                        });
                    }
                    output(c, values);
                }
            } catch (final IOException e) {
                c.output(failureTag, failure("Failed to read file", resource, number, null, 0, e));
            }
        }

        private ByteRecordReader open(final FileIO.ReadableFile file) throws IOException {
            final Compression fileCompression = compression != null
                    ? compression
                    : Compression.detect(file.getMetadata().resourceId().getFilename());
            final ReadableByteChannel channel = fileCompression.readDecompressed(file.open());
            if(fixedLength) {
                return ByteRecordReader.fixed(Channels.newInputStream(channel), decoder.getLayout().getRecordLength());
            } else if(delimiter != null) {
                return ByteRecordReader.delimited(Channels.newInputStream(channel), delimiter);
            } else {
                return ByteRecordReader.lines(Channels.newInputStream(channel));
            }
        }

        private Map<String, Object> decode(final byte[] buffer, final int length) {
            final Map<String, Object> values = switch (format) {
                case fwf -> decoder.decode(buffer, 0, length);
                case csv -> CsvToElementConverter.convert(schema.getFields(), new String(buffer, 0, length, charset));
                case json -> JsonToElementConverter.convert(schema.getFields(), new String(buffer, 0, length, charset));
            };
            if(values == null) {
                throw new IllegalArgumentException("the record is not a single " + format + " record");
            }
            return values;
        }

        private void output(final ProcessContext c, final Map<String, Object> values) {
            if(timestampAttribute == null) {
                c.output(MElement.of(values, c.timestamp()));
                return;
            }
            final long eventTimeEpochMillis = switch (values.get(timestampAttribute)) {
                case Number n -> n.longValue() / 1000L;
                case String s -> DateTimeUtil.toEpochMicroSecond(s) / 1000L;
                case null, default -> c.timestamp().getMillis();
            };
            c.outputWithTimestamp(MElement.of(values, eventTimeEpochMillis), Instant.ofEpochMilli(eventTimeEpochMillis));
        }

        private BadRecord failure(
                final String message,
                final String resource,
                final long number,
                final byte[] buffer,
                final int length,
                final Throwable e) {

            final Map<String, Object> input = new HashMap<>();
            input.put("name", name);
            input.put("resource", resource);
            input.put("line", number);
            if(buffer != null) {
                // for diagnosis only: undecodable bytes show up as U+FFFD here
                input.put("record", new String(buffer, 0, length, charset));
            }
            return Module.processError(message, input, e, failFast);
        }

        private static boolean startsWith(final byte[] buffer, final int length, final byte[] prefix) {
            if(length < prefix.length) {
                return false;
            }
            for(int i = 0; i < prefix.length; i++) {
                if(buffer[i] != prefix[i]) {
                    return false;
                }
            }
            return true;
        }

    }

}
