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
import com.mercari.solution.util.schema.fwf.FwfOptions;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MetadataCoderV2;
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
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
    private static final byte[] UTF8_BOM = { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };
    private static final long MILLIS_PER_DAY = 86_400_000L;

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

        // StorageSource has already rejected recordSplit for the other formats and a format that
        // contradicts schema.encoding.format
        final FwfDecoder decoder;
        final Charset charset;
        final List<Schema.Field> fields;
        String description = schema.getDescription();
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
            if(spec.recordSplit == RecordSplit.length) {
                if(layout.getRecordLength() == null) {
                    throw new IllegalModuleException("parameters.recordSplit: length requires recordLength in the fwf layout");
                }
                // the file is cut by bytes, but with unit: char the layout recordLength counts characters
                if(decoder.getOptions().getUnit() != FwfOptions.Unit.byte_ && !isSingleByteCharset(charset)) {
                    throw new IllegalModuleException("parameters.recordSplit: length requires schema.encoding.unit: byte for charset "
                            + charset.name() + ": the file is cut every recordLength bytes, but with unit: char recordLength counts characters");
                }
            } else if(isWideCharset(charset)) {
                throw new IllegalModuleException("charset " + charset.name() + " can not be split into lines by a single-byte separator; use recordSplit: length");
            }
            fields = new ArrayList<>(layout.toSchemaFields());
            if(description == null) {
                // a schema with declared fields does not take over the description of the layout
                description = layout.getDescription();
            }
        } else {
            decoder = null;
            charset = StandardCharsets.UTF_8;
            fields = new ArrayList<>(schema.getFields());
        }

        // filterPrefix and delimiter are compared with the record bytes
        for(final Map.Entry<String, String> text : textParameters(spec).entrySet()) {
            final String reason = unmatchable(charset);
            if(reason != null) {
                errors.add("parameters." + text.getKey() + " can not be used with charset " + charset.name() + ": " + reason);
            }
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
        final Schema outputSchema = Schema.builder()
                .withFields(fields)
                .withDescription(description)
                .build();

        // the event time field: a date value is an epoch day, any other number is epoch micros
        final Schema.Field timestampField = timestampAttribute == null ? null : Schema.getField(fields, timestampAttribute);
        final boolean timestampIsDate = timestampField != null
                && timestampField.getFieldType().getType() == Schema.Type.date;

        // Only MetadataCoderV2 carries lastModifiedMillis: with the default Metadata coder it is lost
        // at the reshuffle inside matchAll (the files source registers the coder the same way)
        begin.getPipeline().getCoderRegistry().registerCoderForClass(MatchResult.Metadata.class, MetadataCoderV2.of());

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<BadRecord> failureTag = new TupleTag<>(){};
        final PCollectionTuple outputs = begin
                .apply("Patterns", Create.of(spec.inputs).withCoder(StringUtf8Coder.of()))
                .apply("MatchFiles", FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
                // the bytes are decompressed in the DoFn: Beam's AUTO would hide the file name based
                // detection and, for .zip, concatenate the entries before this reader can see them
                .apply("ReadMatches", FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
                .setCoder(ReadableFileCoder.of(MetadataCoderV2.of()))
                .apply("ReadRecords", ParDo
                        .of(new ReadRecordsDoFn(name, spec, compression, schema, decoder, charset.name(),
                                additionalFields, timestampAttribute, timestampIsDate, failFast, failureTag))
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

    // UTF-16 / UTF-32 and their variants: the bytes of a line feed appear inside other characters.
    // Asked of the charset itself (an ASCII letter does not fit in one byte) and not of its name:
    // the canonical names share no prefix (x-UTF-16LE-BOM, X-UTF-32BE-BOM ...)
    private static boolean isWideCharset(final Charset charset) {
        return charset.canEncode() && "A".getBytes(charset).length != 1;
    }

    private static Map<String, String> textParameters(final Spec spec) {
        final Map<String, String> parameters = new LinkedHashMap<>();
        if(spec.filterPrefix != null && !spec.filterPrefix.isEmpty()) {
            parameters.put("filterPrefix", spec.filterPrefix);
        }
        if(spec.delimiter != null && !spec.delimiter.isEmpty()) {
            parameters.put("delimiter", spec.delimiter);
        }
        return parameters;
    }

    // Why a text can not be matched against record bytes in this charset; null when it can.
    // A decode-only charset can not produce the bytes at all, and one that writes a byte order mark
    // (UTF-16, UTF-32 without an explicit byte order) would put the mark in front of the text.
    private static String unmatchable(final Charset charset) {
        if(!charset.canEncode()) {
            return "the charset can not encode text";
        }
        if("AA".getBytes(charset).length != 2 * "A".getBytes(charset).length) {
            return "it writes a byte order mark; use the charset with an explicit byte order (e.g. UTF-16LE)";
        }
        return null;
    }

    // every character is one byte, so a length in characters is a length in bytes
    private static boolean isSingleByteCharset(final Charset charset) {
        return charset.canEncode() && charset.newEncoder().maxBytesPerChar() == 1.0f;
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
        private final boolean timestampIsDate;
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
                final boolean timestampIsDate,
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
            // csv / json only: the fwf decoder carries its own (projected) layout
            this.schema = decoder == null ? schema : null;
            this.decoder = decoder;
            this.charsetName = charsetName;
            this.additionalFields = new HashMap<>(additionalFields);
            this.timestampAttribute = timestampAttribute;
            this.timestampIsDate = timestampIsDate;
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
                    final Instant eventTime;
                    try {
                        values = decode(buffer, length);
                        for(final Map.Entry<String, String> entry : additionalFields.entrySet()) {
                            values.put(entry.getValue(), switch (entry.getKey()) {
                                case "resource" -> resource;
                                case "line" -> number;
                                case "lastModified" -> metadata.lastModifiedMillis() * 1000L;
                                // entry: the archive entry name; a plain file has none
                                default -> null;
                            });
                        }
                        // a value that is not a time is a failure of the record as well
                        eventTime = eventTime(values, c.timestamp());
                    } catch (final RuntimeException e) {
                        c.output(failureTag, failure("Failed to decode " + format + " record", resource, number, buffer, length, e));
                        continue;
                    }
                    c.outputWithTimestamp(MElement.of(values, eventTime), eventTime);
                }
            } catch (final IOException e) {
                // Not a record error: the file can not be read on from here. With failFast: false the
                // records already read stay in the output, so the failure says how far the file was read.
                c.output(failureTag, failure("Failed to read file after record " + number
                        + "; the rest of the file is not read", resource, number, null, 0, e));
            }
        }

        private ByteRecordReader open(final FileIO.ReadableFile file) throws IOException {
            final Compression fileCompression = compression != null
                    ? compression
                    : Compression.detect(file.getMetadata().resourceId().getFilename());
            final ReadableByteChannel channel = file.open();
            try {
                InputStream input = Channels.newInputStream(fileCompression.readDecompressed(channel));
                if(StandardCharsets.UTF_8.equals(charset)) {
                    input = skipByteOrderMark(input);
                }
                if(fixedLength) {
                    return ByteRecordReader.fixed(input, decoder.getLayout().getRecordLength());
                } else if(delimiter != null) {
                    return ByteRecordReader.delimited(input, delimiter);
                } else {
                    return ByteRecordReader.lines(input);
                }
            } catch (final IOException | RuntimeException e) {
                // nothing owns the channel yet
                try {
                    channel.close();
                } catch (final IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        }

        // A byte order mark at the head of a UTF-8 file is not data (TextIO drops it as well): left in,
        // it becomes part of the first value of a csv line and shifts every field of a fwf record.
        private static InputStream skipByteOrderMark(final InputStream input) throws IOException {
            final PushbackInputStream pushback = new PushbackInputStream(input, UTF8_BOM.length);
            final byte[] head = pushback.readNBytes(UTF8_BOM.length);
            if(!Arrays.equals(head, UTF8_BOM)) {
                pushback.unread(head);
            }
            return pushback;
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

        // The event time of a record: the value of timestampAttribute, or the given default without
        // the attribute or the value.
        private Instant eventTime(final Map<String, Object> values, final Instant defaultTime) {
            if(timestampAttribute == null) {
                return defaultTime;
            }
            return switch (values.get(timestampAttribute)) {
                case Number n when timestampIsDate -> Instant.ofEpochMilli(Math.multiplyExact(n.longValue(), MILLIS_PER_DAY));
                case Number n -> Instant.ofEpochMilli(n.longValue() / 1000L);
                case String s -> Instant.ofEpochMilli(DateTimeUtil.toEpochMicroSecond(s) / 1000L);
                case null, default -> defaultTime;
            };
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
