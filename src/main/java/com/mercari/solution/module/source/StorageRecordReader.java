package com.mercari.solution.module.source;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.MErrorHandler;
import com.mercari.solution.module.Module;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.ArchiveReader;
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
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The storage source's byte-record read path (work_fixedwidth.md §5): files are matched and opened
 * through FileIO and cut into records as bytes ({@link ByteRecordReader}), instead of TextIO's
 * UTF-8 lines. It serves {@code format: fwf} and, for csv / json, the options TextIO can not
 * provide ({@code additionalFields}, {@code archive}, {@code partitions}). Everything file-scoped
 * ({@code skipHeaderLines}, record numbers) restarts per file, and a record that can not be decoded
 * goes to the failure output.
 *
 * <p>With {@code archive} (work_fixedwidth.md §6) a matched file is a zip / tar archive and each
 * selected entry is read as a file of its own: "per file" above becomes "per entry".
 *
 * <p>With {@code partitions} (§6.4) the entries of an archive go to named outputs, each with its
 * own way of reading ({@link Partition}: format, schema, header lines ...). An entry is read by the
 * first partition whose patterns match it and is not read at all when none does. Without
 * partitions there is one unnamed {@link Partition}: the module's own output.
 */
final class StorageRecordReader {

    enum Format { csv, json, fwf }

    enum RecordSplit { line, length }

    private static final List<String> ADDITIONAL_FIELD_KEYS = List.of("resource", "entry", "line", "lastModified");
    private static final List<String> PARTITION_KEYS = List.of(
            "name", "entries", "exclude", "format", "schema", "fields",
            "skipHeaderLines", "filterPrefix", "delimiter", "recordSplit");
    private static final byte[] UTF8_BOM = { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private StorageRecordReader() {}

    /** What to read; resolved from the module parameters at assembly time. */
    static class Spec implements Serializable {
        List<String> inputs;
        String compression;
        // metadata key (resource / entry / line / lastModified) -> output field name
        Map<String, String> additionalFields;
        // null: the matched files are read as they are
        Archive archive;
        // the outputs: one unnamed partition (the module's own output), or the named partitions
        List<Partition> partitions;
    }

    /** parameters.archive: the matched files are archives whose entries are the files to read. */
    static class Archive implements Serializable {
        // zip | tar; null: told from the file name
        String type;
        // globs on the entry path; empty: every file entry
        List<String> entries;
        List<String> exclude;
        // charset of the entry names (zip entries flagged as UTF-8 are always UTF-8)
        String nameCharset;
    }

    /** One output and how its files (or archive entries) are read. */
    static class Partition implements Serializable {
        // null: the single unnamed output of the module
        String name;
        // globs on the archive entry path that route entries to this partition (named partitions)
        List<String> entries;
        List<String> exclude;
        Format format;
        Schema schema;
        // projection: top-level field names (fwf only)
        List<String> fields;
        Integer skipHeaderLines;
        String filterPrefix;
        String delimiter;
        RecordSplit recordSplit;

        // where the partition is declared, for error messages
        private String where() {
            return name == null ? "parameters" : "parameters.partitions(" + name + ")";
        }

        /**
         * parameters.partitions: every key that is omitted is taken from the module-level
         * parameters ({@code defaults}); the format of a partition whose schema declares
         * {@code encoding.format: fwf} is fwf.
         */
        static List<Partition> parse(final JsonElement partitionsElement, final Partition defaults) {
            final List<String> errors = new ArrayList<>();
            final List<Partition> partitions = new ArrayList<>();
            if(!partitionsElement.isJsonArray() || partitionsElement.getAsJsonArray().isEmpty()) {
                throw new IllegalModuleException("parameters.partitions must be a non-empty array");
            }
            final Set<String> names = new HashSet<>();
            int index = 0;
            for(final JsonElement element : partitionsElement.getAsJsonArray()) {
                final String where = "parameters.partitions[" + index + "]";
                index++;
                if(!element.isJsonObject()) {
                    errors.add(where + " must be an object. but: " + element);
                    continue;
                }
                final JsonObject object = element.getAsJsonObject();
                for(final String key : object.keySet()) {
                    if(!PARTITION_KEYS.contains(key)) {
                        errors.add(where + "." + key + " is not supported. supported keys: " + PARTITION_KEYS);
                    }
                }
                final Partition partition = new Partition();
                partition.name = text(object, "name", where, errors);
                if(partition.name == null || partition.name.isBlank()) {
                    errors.add(where + ".name is required");
                    continue;
                } else if(!names.add(partition.name)) {
                    errors.add(where + ".name: " + partition.name + " is duplicated");
                }
                partition.entries = texts(object, "entries", where, errors);
                partition.exclude = texts(object, "exclude", where, errors);
                if(partition.entries == null || partition.entries.isEmpty()) {
                    errors.add(where + ".entries is required (the entries that go to this partition)");
                }
                if(hasBlank(partition.entries)) {
                    errors.add(where + ".entries must not contain an empty pattern");
                }
                if(hasBlank(partition.exclude)) {
                    errors.add(where + ".exclude must not contain an empty pattern");
                }

                partition.schema = defaults.schema;
                if(object.has("schema")) {
                    try {
                        partition.schema = Schema.parse(object.get("schema"));
                        if(partition.schema == null) {
                            errors.add(where + ".schema must be an object");
                        } else {
                            partition.schema.setup();
                        }
                    } catch (final RuntimeException e) {
                        errors.add(where + ".schema: " + e.getMessage());
                    }
                }
                final boolean fwfSchema = partition.schema != null && partition.schema.getFwfLayout() != null;
                final String format = text(object, "format", where, errors);
                if(format != null) {
                    try {
                        partition.format = Format.valueOf(format.trim());
                    } catch (final IllegalArgumentException e) {
                        errors.add(where + ".format: " + format + " is not supported. supported formats: " + Arrays.toString(Format.values()));
                    }
                } else {
                    partition.format = fwfSchema ? Format.fwf : defaults.format;
                    if(partition.format == null) {
                        errors.add(where + ".format is required (or parameters.format for all partitions)");
                    }
                }
                if(partition.format != null && fwfSchema && partition.format != Format.fwf) {
                    errors.add(where + ".format: " + partition.format + " differs from schema.encoding.format: fwf");
                }

                final List<String> fields = texts(object, "fields", where, errors);
                partition.fields = object.has("fields") ? fields : defaults.fields;
                partition.filterPrefix = object.has("filterPrefix") ? text(object, "filterPrefix", where, errors) : defaults.filterPrefix;
                partition.delimiter = object.has("delimiter") ? text(object, "delimiter", where, errors) : defaults.delimiter;
                partition.skipHeaderLines = defaults.skipHeaderLines;
                if(object.has("skipHeaderLines")) {
                    try {
                        partition.skipHeaderLines = object.get("skipHeaderLines").getAsInt();
                    } catch (final RuntimeException e) {
                        errors.add(where + ".skipHeaderLines must be an integer. but: " + object.get("skipHeaderLines"));
                    }
                }
                partition.recordSplit = defaults.recordSplit;
                if(object.has("recordSplit")) {
                    final String recordSplit = text(object, "recordSplit", where, errors);
                    try {
                        partition.recordSplit = recordSplit == null ? null : RecordSplit.valueOf(recordSplit.trim());
                    } catch (final IllegalArgumentException e) {
                        errors.add(where + ".recordSplit must be line or length. but: " + recordSplit);
                    }
                }
                if(partition.format != Format.fwf) {
                    if(object.has("recordSplit")) {
                        errors.add(where + ".recordSplit is only supported for format fwf");
                    }
                    // module-level fwf options do not apply to a csv / json partition
                    partition.recordSplit = null;
                    partition.fields = null;
                }
                partitions.add(partition);
            }
            if(!errors.isEmpty()) {
                throw new IllegalModuleException(errors);
            }
            return partitions;
        }

        private static String text(final JsonObject object, final String key, final String where, final List<String> errors) {
            if(!object.has(key) || object.get(key).isJsonNull()) {
                return null;
            }
            if(!object.get(key).isJsonPrimitive()) {
                errors.add(where + "." + key + " must be a primitive value. but: " + object.get(key));
                return null;
            }
            return object.get(key).getAsString();
        }

        private static List<String> texts(final JsonObject object, final String key, final String where, final List<String> errors) {
            if(!object.has(key) || object.get(key).isJsonNull()) {
                return null;
            }
            final List<String> values = new ArrayList<>();
            final JsonElement element = object.get(key);
            if(element.isJsonArray()) {
                for(final JsonElement value : element.getAsJsonArray()) {
                    if(value.isJsonPrimitive()) {
                        values.add(value.getAsString());
                    } else {
                        errors.add(where + "." + key + " must be an array of strings. but: " + element);
                        break;
                    }
                }
            } else if(element.isJsonPrimitive()) {
                values.add(element.getAsString());
            } else {
                errors.add(where + "." + key + " must be an array of strings. but: " + element);
            }
            return values;
        }
    }

    static MCollectionTuple expand(
            final PBegin begin,
            final String name,
            final Spec spec,
            final String timestampAttribute,
            final boolean failFast,
            final MErrorHandler errorHandler) {

        final List<String> errors = new ArrayList<>();
        final Compression compression = parseCompression(spec.compression, errors);
        ArchiveReader.Type archiveType = null;
        Charset archiveNameCharset = StandardCharsets.UTF_8;
        if(spec.archive != null) {
            if(spec.archive.type != null) {
                try {
                    archiveType = ArchiveReader.Type.valueOf(spec.archive.type.trim().toLowerCase(Locale.ROOT));
                } catch (final IllegalArgumentException e) {
                    errors.add("parameters.archive.type must be zip or tar. but: " + spec.archive.type);
                }
            }
            if(spec.archive.nameCharset != null) {
                try {
                    archiveNameCharset = Charset.forName(spec.archive.nameCharset.trim());
                } catch (final IllegalArgumentException e) {
                    errors.add("parameters.archive.nameCharset: " + spec.archive.nameCharset + " is not a supported charset");
                }
            }
            // Beam's ZIP "compression" concatenates the entries of a zip into one stream: it is the
            // legacy way to read a zip, and applying it first would leave no archive to read
            if(Compression.ZIP.equals(compression)) {
                errors.add("parameters.compression: ZIP can not be used with parameters.archive (the archive reader opens the zip itself)");
            }
            if(hasBlank(spec.archive.entries)) {
                errors.add("parameters.archive.entries must not contain an empty pattern");
            }
            if(hasBlank(spec.archive.exclude)) {
                errors.add("parameters.archive.exclude must not contain an empty pattern");
            }
        }
        final boolean named = spec.partitions.stream().anyMatch(p -> p.name != null);
        if(named && spec.archive == null) {
            // partitions route archive entries by their path
            errors.add("parameters.partitions requires parameters.archive");
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
            for(final Map.Entry<String, String> entry : additionalFields.entrySet()) {
                if(entry.getValue() == null || entry.getValue().isBlank()) {
                    errors.add("parameters.additionalFields." + entry.getKey() + " must be an output field name");
                }
            }
        }
        if(!errors.isEmpty()) {
            throw new IllegalModuleException(errors);
        }

        final List<Plan> plans = new ArrayList<>();
        final List<Schema> outputSchemas = new ArrayList<>();
        for(final Partition partition : spec.partitions) {
            final List<Schema.Field> fields = new ArrayList<>();
            final Plan plan = Plan.of(plans.size(), partition, additionalFields, timestampAttribute, fields, errors);
            if(plan == null) {
                continue;
            }
            plans.add(plan);
            outputSchemas.add(Schema.builder()
                    .withFields(fields)
                    .withDescription(plan.description)
                    .build());
        }
        if(!errors.isEmpty()) {
            throw new IllegalModuleException(errors);
        }

        // Only MetadataCoderV2 carries lastModifiedMillis: with the default Metadata coder it is lost
        // at the reshuffle inside matchAll (the files source registers the coder the same way)
        begin.getPipeline().getCoderRegistry().registerCoderForClass(MatchResult.Metadata.class, MetadataCoderV2.of());

        final TupleTag<BadRecord> failureTag = new TupleTag<>(){};
        TupleTagList otherTags = TupleTagList.of(failureTag);
        for(final Plan plan : plans.subList(1, plans.size())) {
            otherTags = otherTags.and(plan.tag);
        }
        final PCollectionTuple outputs = begin
                .apply("Patterns", Create.of(spec.inputs).withCoder(StringUtf8Coder.of()))
                .apply("MatchFiles", FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
                // the bytes are decompressed in the DoFn: Beam's AUTO would hide the file name based
                // detection and, for .zip, concatenate the entries before this reader can see them
                .apply("ReadMatches", FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
                .setCoder(ReadableFileCoder.of(MetadataCoderV2.of()))
                .apply("ReadRecords", ParDo
                        .of(new ReadRecordsDoFn(name, compression, plans, spec.archive, archiveType,
                                archiveNameCharset.name(), timestampAttribute, failFast, failureTag))
                        .withOutputTags(plans.getFirst().tag, otherTags));

        errorHandler.addError(outputs.get(failureTag));

        if(!named) {
            return MCollectionTuple.of(
                    outputs.get(plans.getFirst().tag).setCoder(ElementCoder.of(outputSchemas.getFirst())),
                    outputSchemas.getFirst());
        }
        MCollectionTuple tuple = MCollectionTuple.empty(begin.getPipeline());
        for(int i = 0; i < plans.size(); i++) {
            tuple = tuple.and(
                    spec.partitions.get(i).name,
                    outputs.get(plans.get(i).tag).setCoder(ElementCoder.of(outputSchemas.get(i))),
                    outputSchemas.get(i));
        }
        return tuple;
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

    private static boolean hasBlank(final List<String> globs) {
        return globs != null && globs.stream().anyMatch(glob -> glob == null || glob.isBlank());
    }

    // UTF-16 / UTF-32 and their variants: the bytes of a line feed appear inside other characters.
    // Asked of the charset itself (an ASCII letter does not fit in one byte) and not of its name:
    // the canonical names share no prefix (x-UTF-16LE-BOM, X-UTF-32BE-BOM ...)
    private static boolean isWideCharset(final Charset charset) {
        return charset.canEncode() && "A".getBytes(charset).length != 1;
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

    /** A {@link Partition} resolved for the workers: how the records of its files are cut and decoded. */
    private static final class Plan implements Serializable {

        // an explicit id: the partitions are built at one call site
        private TupleTag<MElement> tag;
        private Format format;
        // csv / json only: the fwf decoder carries its own (projected) layout
        private Schema schema;
        private FwfDecoder decoder;
        private String charsetName;
        private int skipHeaderLines;
        private byte[] filterPrefix;
        private byte[] delimiter;
        private boolean fixedLength;
        // metadata key -> output field name
        private Map<String, String> additionalFields;
        // the event time field is a date: its value is an epoch day, any other number is epoch micros
        private boolean timestampIsDate;
        private List<String> entries;
        private List<String> exclude;
        private String description;

        private transient Charset charset;
        private transient Predicate<String> entryFilter;

        // null (with the reasons added to errors) when the partition can not be read as declared
        static Plan of(
                final int index,
                final Partition partition,
                final Map<String, String> additionalFields,
                final String timestampAttribute,
                final List<Schema.Field> fields,
                final List<String> errors) {

            final String where = partition.where();
            final int errorCount = errors.size();
            final Schema schema = partition.schema;
            if(schema == null) {
                errors.add(where + ".schema is required for format " + partition.format);
                return null;
            }
            if(partition.recordSplit == RecordSplit.length && partition.delimiter != null) {
                errors.add(where + ".delimiter can not be used with recordSplit: length");
            }
            if(partition.delimiter != null && partition.delimiter.isEmpty()) {
                errors.add(where + ".delimiter must not be empty");
            }
            if(partition.skipHeaderLines != null && partition.skipHeaderLines < 0) {
                errors.add(where + ".skipHeaderLines must not be negative");
            }

            final Plan plan = new Plan();
            plan.tag = new TupleTag<>("partition" + index);
            plan.format = partition.format;
            plan.description = schema.getDescription();
            final Charset charset;
            if(partition.format == Format.fwf) {
                if(schema.getFwfLayout() == null) {
                    errors.add("format fwf requires " + where + ".schema with encoding.format: fwf and a layout in reference (uri or inline)");
                    return null;
                }
                try {
                    plan.decoder = FwfDecoder.of(schema, partition.fields);
                } catch (final IllegalArgumentException e) {
                    errors.add(where + ".fields: " + e.getMessage());
                    return null;
                }
                charset = plan.decoder.getOptions().getCharset();
                final FwfLayout layout = plan.decoder.getLayout();
                if(partition.recordSplit == RecordSplit.length) {
                    if(layout.getRecordLength() == null) {
                        errors.add(where + ".recordSplit: length requires recordLength in the fwf layout");
                    }
                    // the file is cut by bytes, but with unit: char the layout recordLength counts characters
                    if(plan.decoder.getOptions().getUnit() != FwfOptions.Unit.byte_ && !isSingleByteCharset(charset)) {
                        errors.add(where + ".recordSplit: length requires schema.encoding.unit: byte for charset "
                                + charset.name() + ": the file is cut every recordLength bytes, but with unit: char recordLength counts characters");
                    }
                } else if(isWideCharset(charset)) {
                    errors.add("charset " + charset.name() + " can not be split into lines by a single-byte separator; use recordSplit: length (" + where + ")");
                }
                fields.addAll(layout.toSchemaFields());
                if(plan.description == null) {
                    // a schema with declared fields does not take over the description of the layout
                    plan.description = layout.getDescription();
                }
            } else {
                if(schema.getFwfLayout() != null) {
                    errors.add(where + ".format: " + partition.format + " differs from schema.encoding.format: fwf");
                    return null;
                }
                plan.schema = schema;
                charset = StandardCharsets.UTF_8;
                fields.addAll(schema.getFields());
            }
            plan.charsetName = charset.name();
            plan.skipHeaderLines = partition.skipHeaderLines == null ? 0 : partition.skipHeaderLines;
            plan.fixedLength = partition.recordSplit == RecordSplit.length;

            // filterPrefix and delimiter are compared with the record bytes
            final boolean filterPrefix = partition.filterPrefix != null && !partition.filterPrefix.isEmpty();
            final boolean delimiter = partition.delimiter != null && !partition.delimiter.isEmpty();
            final String reason = filterPrefix || delimiter ? unmatchable(charset) : null;
            if(reason != null) {
                if(filterPrefix) {
                    errors.add(where + ".filterPrefix can not be used with charset " + charset.name() + ": " + reason);
                }
                if(delimiter) {
                    errors.add(where + ".delimiter can not be used with charset " + charset.name() + ": " + reason);
                }
            } else {
                plan.filterPrefix = filterPrefix ? partition.filterPrefix.getBytes(charset) : null;
                plan.delimiter = delimiter ? partition.delimiter.getBytes(charset) : null;
            }

            plan.additionalFields = new HashMap<>(additionalFields);
            for(final Map.Entry<String, String> entry : additionalFields.entrySet()) {
                final String fieldName = entry.getValue();
                if(Schema.hasField(fields, fieldName)) {
                    errors.add("parameters.additionalFields." + entry.getKey() + ": the output field name " + fieldName + " is already used"
                            + (partition.name == null ? "" : " in partition " + partition.name));
                } else {
                    fields.add(Schema.Field.of(fieldName, switch (entry.getKey()) {
                        case "line" -> Schema.FieldType.INT64;
                        case "lastModified" -> Schema.FieldType.TIMESTAMP;
                        default -> Schema.FieldType.STRING;
                    }));
                }
            }

            final Schema.Field timestampField = timestampAttribute == null ? null : Schema.getField(fields, timestampAttribute);
            plan.timestampIsDate = timestampField != null
                    && timestampField.getFieldType().getType() == Schema.Type.date;
            plan.entries = partition.entries == null ? List.of() : new ArrayList<>(partition.entries);
            plan.exclude = partition.exclude == null ? List.of() : new ArrayList<>(partition.exclude);
            return errors.size() == errorCount ? plan : null;
        }

        void setup() {
            this.charset = Charset.forName(charsetName);
            this.entryFilter = ArchiveReader.filter(entries, exclude);
            if(schema != null) {
                schema.setup();
            }
        }

        ByteRecordReader reader(final InputStream input) {
            if(fixedLength) {
                return ByteRecordReader.fixed(input, decoder.getLayout().getRecordLength());
            } else if(delimiter != null) {
                return ByteRecordReader.delimited(input, delimiter);
            } else {
                return ByteRecordReader.lines(input);
            }
        }

        Map<String, Object> decode(final byte[] buffer, final int length) {
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

    }

    private static class ReadRecordsDoFn extends DoFn<FileIO.ReadableFile, MElement> {

        private final String name;
        private final Compression compression;
        private final List<Plan> plans;
        private final boolean archive;
        // null: told from the file name
        private final ArchiveReader.Type archiveType;
        private final List<String> archiveEntries;
        private final List<String> archiveExclude;
        private final String archiveNameCharsetName;
        private final String timestampAttribute;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;
        private final Counter entriesRead;
        private final Counter entriesSkipped;

        private transient Predicate<String> archiveFilter;
        // the plan of the archive entry the reader stands on
        private transient int routed;

        ReadRecordsDoFn(
                final String name,
                final Compression compression,
                final List<Plan> plans,
                final Archive archive,
                final ArchiveReader.Type archiveType,
                final String archiveNameCharsetName,
                final String timestampAttribute,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.name = name;
            this.compression = compression;
            this.plans = plans;
            this.archive = archive != null;
            this.archiveType = archiveType;
            this.archiveEntries = archive == null || archive.entries == null ? List.of() : new ArrayList<>(archive.entries);
            this.archiveExclude = archive == null || archive.exclude == null ? List.of() : new ArrayList<>(archive.exclude);
            this.archiveNameCharsetName = archiveNameCharsetName;
            this.timestampAttribute = timestampAttribute;
            this.failFast = failFast;
            this.failureTag = failureTag;
            this.entriesRead = Metrics.counter(name, "archive_entries_read");
            this.entriesSkipped = Metrics.counter(name, "archive_entries_skipped");
        }

        @Setup
        public void setup() {
            this.archiveFilter = ArchiveReader.filter(archiveEntries, archiveExclude);
            for(final Plan plan : plans) {
                plan.setup();
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
            // where the read stands, for the failure of an I/O error
            final Position position = new Position(plans.size());
            try {
                if(archive) {
                    try(final ArchiveReader reader = openArchive(file)) {
                        while(reader.next(this::route)) {
                            position.plan = plans.get(routed);
                            position.planIndex = routed;
                            position.entry = reader.name();
                            position.number = 0;
                            // closed entry by entry: the decompressor of a compressed entry holds
                            // native memory (closing the stream of an entry does not close the archive)
                            try(final InputStream entry = decompressEntry(reader.name(), reader.stream())) {
                                readRecords(c, metadata, resource, entry, position);
                            }
                        }
                    }
                } else {
                    position.plan = plans.getFirst();
                    try(final InputStream input = openFile(file, fileCompression(file))) {
                        readRecords(c, metadata, resource, input, position);
                    }
                }
            } catch (final IOException e) {
                // Not a record error: the file can not be read on from here. With failFast: false the
                // records already read stay in the output, so the failure says how far the file was read.
                c.output(failureTag, failure("Failed to read file after record " + position.number
                        + (position.entry == null ? "" : " of entry " + position.entry)
                        + "; the rest of the file is not read", resource, position, null, 0, e));
            }
        }

        // The partition an archive entry goes to: the first one whose patterns match it. An entry
        // that parameters.archive leaves out, or that no partition takes, is not read at all.
        private boolean route(final String entryName) {
            if(archiveFilter.test(entryName)) {
                for(int i = 0; i < plans.size(); i++) {
                    if(plans.get(i).entryFilter.test(entryName)) {
                        this.routed = i;
                        entriesRead.inc();
                        return true;
                    }
                }
            }
            entriesSkipped.inc();
            return false;
        }

        // One file, or one entry of an archive: records are numbered from 1 and the header lines are
        // skipped for each of them. The stream is left open for the caller.
        private void readRecords(
                final ProcessContext c,
                final MatchResult.Metadata metadata,
                final String resource,
                final InputStream stream,
                final Position position) throws IOException {

            final Plan plan = position.plan;
            final InputStream input = StandardCharsets.UTF_8.equals(plan.charset) ? skipByteOrderMark(stream) : stream;
            // one reader (and its buffers) per partition for all the entries of an archive
            ByteRecordReader reader = position.readers[position.planIndex];
            if(reader == null) {
                reader = plan.reader(input);
                position.readers[position.planIndex] = reader;
            } else {
                reader.reset(input);
            }
            while(reader.next()) {
                final long number = reader.number();
                position.number = number;
                if(number <= plan.skipHeaderLines) {
                    continue;
                }
                final byte[] buffer = reader.buffer();
                final int length = reader.length();
                // blank lines (and the CRLF / LF mix they come from) carry no record
                if(length == 0 && !plan.fixedLength) {
                    continue;
                }
                if(plan.filterPrefix != null && startsWith(buffer, length, plan.filterPrefix)) {
                    continue;
                }
                final Map<String, Object> values;
                final Instant eventTime;
                try {
                    values = plan.decode(buffer, length);
                    for(final Map.Entry<String, String> entry : plan.additionalFields.entrySet()) {
                        values.put(entry.getValue(), switch (entry.getKey()) {
                            case "resource" -> resource;
                            case "line" -> number;
                            case "lastModified" -> metadata.lastModifiedMillis() * 1000L;
                            // entry: the archive entry name; null for a plain file
                            default -> position.entry;
                        });
                    }
                    // a value that is not a time is a failure of the record as well
                    eventTime = eventTime(plan, values, c.timestamp());
                } catch (final RuntimeException e) {
                    c.output(failureTag, failure("Failed to decode " + plan.format + " record", resource, position, buffer, length, e));
                    continue;
                }
                c.outputWithTimestamp(plan.tag, MElement.of(values, eventTime), eventTime);
            }
        }

        // the compression of the file itself: the parameter, or what the file name says
        private Compression fileCompression(final FileIO.ReadableFile file) {
            return compression != null
                    ? compression
                    : Compression.detect(file.getMetadata().resourceId().getFilename());
        }

        private static InputStream openFile(final FileIO.ReadableFile file, final Compression fileCompression) throws IOException {
            final ReadableByteChannel channel = file.open();
            try {
                return Channels.newInputStream(fileCompression.readDecompressed(channel));
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

        private ArchiveReader openArchive(final FileIO.ReadableFile file) throws IOException {
            final MatchResult.Metadata metadata = file.getMetadata();
            final String fileName = metadata.resourceId().getFilename();
            final ArchiveReader.Type type = archiveType != null ? archiveType : ArchiveReader.Type.detect(fileName);
            if(type == null) {
                throw new IOException("can not tell the archive type of " + fileName + " from its name; set parameters.archive.type");
            }
            if(compression == null) {
                rejectUnsupportedCompression(fileName);
            }
            final Compression outer = compression != null ? compression : archiveCompression(fileName);
            final Charset nameCharset = Charset.forName(archiveNameCharsetName);
            if(type == ArchiveReader.Type.zip && Compression.UNCOMPRESSED.equals(outer) && metadata.isReadSeekEfficient()) {
                // only the central directory and the selected entries are read
                final SeekableByteChannel channel = file.openSeekable();
                try {
                    return ArchiveReader.zip(channel, nameCharset);
                } catch (final IOException | RuntimeException e) {
                    try {
                        channel.close();
                    } catch (final IOException suppressed) {
                        e.addSuppressed(suppressed);
                    }
                    throw e;
                }
            }
            final InputStream input = openFile(file, outer);
            return type == ArchiveReader.Type.zip
                    ? ArchiveReader.zipStream(input, nameCharset)
                    : ArchiveReader.tar(input, nameCharset);
        }

        // The compression around an archive, from its file name (.tar.gz, .tgz, .zip.gz ...). A plain
        // .zip is opened as it is: Beam calls it ZIP "compression" and would concatenate its entries.
        private static Compression archiveCompression(final String fileName) {
            // .tgz is .tar.gz: the same reading of the name as the archive type detection
            final Compression detected = Compression.detect(ArchiveReader.expandShortSuffix(fileName));
            return Compression.ZIP.equals(detected) ? Compression.UNCOMPRESSED : detected;
        }

        // An entry that is a compressed file itself (logs.tar of *.gz) is decompressed by its name.
        // An archive inside the archive is not opened: it is an ordinary entry.
        private static InputStream decompressEntry(final String entryName, final InputStream stream) throws IOException {
            rejectUnsupportedCompression(entryName);
            final Compression entryCompression = Compression.detect(entryName);
            if(Compression.UNCOMPRESSED.equals(entryCompression) || Compression.ZIP.equals(entryCompression)) {
                return stream;
            }
            return Channels.newInputStream(entryCompression.readDecompressed(Channels.newChannel(stream)));
        }

        // xz, lz4, compress ...: read as it is, such a file is garbage (a .tar.xz may even pass as an
        // empty tar and give no record at all), so it is a failure by its name
        private static void rejectUnsupportedCompression(final String name) throws IOException {
            final String suffix = ArchiveReader.unsupportedCompression(name);
            if(suffix != null) {
                throw new IOException(name + " is compressed with a format that is not supported (" + suffix
                        + "). supported: gz, bz2, zst, lzo, deflate, snappy");
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

        // The event time of a record: the value of timestampAttribute, or the given default without
        // the attribute or the value.
        private Instant eventTime(final Plan plan, final Map<String, Object> values, final Instant defaultTime) {
            if(timestampAttribute == null) {
                return defaultTime;
            }
            return switch (values.get(timestampAttribute)) {
                case Number n when plan.timestampIsDate -> Instant.ofEpochMilli(Math.multiplyExact(n.longValue(), MILLIS_PER_DAY));
                case Number n -> Instant.ofEpochMilli(n.longValue() / 1000L);
                case String s -> Instant.ofEpochMilli(DateTimeUtil.toEpochMicroSecond(s) / 1000L);
                case null, default -> defaultTime;
            };
        }

        private BadRecord failure(
                final String message,
                final String resource,
                final Position position,
                final byte[] buffer,
                final int length,
                final Throwable e) {

            final Map<String, Object> input = new HashMap<>();
            input.put("name", name);
            input.put("resource", resource);
            if(position.entry != null) {
                input.put("entry", position.entry);
            }
            input.put("line", position.number);
            if(buffer != null && position.plan != null) {
                // for diagnosis only: undecodable bytes show up as U+FFFD here
                input.put("record", new String(buffer, 0, length, position.plan.charset));
            }
            return Module.processError(message, input, e, failFast);
        }

        // the entry (of an archive) and the record being read, with the readers shared by the entries
        private static final class Position {
            Plan plan;
            int planIndex;
            String entry;
            long number;
            final ByteRecordReader[] readers;

            Position(final int plans) {
                this.readers = new ByteRecordReader[plans];
            }
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
