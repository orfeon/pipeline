package com.mercari.solution.module.source;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.FileSchemaUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.CsvToElementConverter;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;


@Source.Module(name="storage", schema=true)
public class StorageSource extends Source {

    private static class Parameters implements Serializable {

        private String input;
        private List<String> inputs;
        private Format format;
        private String compression;

        // for parquet
        private List<String> fields;

        // for csv, json, fwf
        private String filterPrefix;
        private Integer skipHeaderLines;
        private String delimiter;

        // for fwf: line | length. Held as text and parsed in validate: Gson turns an unknown enum
        // value into null, which would silently read the file with the default
        private String recordSplit;

        // metadata (resource / entry / line / lastModified) -> output field name
        private Map<String, String> additionalFields;

        // the matched files are zip / tar archives whose entries are the files to read
        private StorageRecordReader.Archive archive;

        // named outputs for the entries of an archive, each with its own format / schema
        private JsonElement partitions;

        private transient StorageRecordReader.RecordSplit split;

        /**
         * @param declaredFormat the text of parameters.format in the config; Gson turns an unknown
         *                       enum value into null, which must not pass for an omitted format
         */
        public void validate(final Schema schema, final String declaredFormat) {
            final List<String> errorMessages = new ArrayList<>();
            // partitions: null is an omitted parameter (Gson keeps an explicit null as JsonNull)
            if(partitions != null && partitions.isJsonNull()) {
                partitions = null;
            }
            if((inputs == null || inputs.isEmpty()) && input == null) {
                errorMessages.add("parameters.input or inputs is required");
            }
            if(this.format == null && declaredFormat != null) {
                throw new IllegalModuleException("parameters.format: " + declaredFormat + " is not supported. supported formats: "
                        + Arrays.toString(Format.values()));
            }
            // the format is derived from schema.encoding.format when omitted (work_fixedwidth.md §4.6)
            final boolean fwfSchema = schema != null && schema.getFwfLayout() != null;
            if(this.format == null) {
                if(fwfSchema) {
                    this.format = Format.fwf;
                } else if(partitions == null) {
                    // with partitions the format may be declared by each of them
                    errorMessages.add("parameters.format must not be null");
                }
            } else if(fwfSchema && !Format.fwf.equals(this.format)) {
                errorMessages.add("parameters.format: " + this.format + " differs from schema.encoding.format: fwf");
            }
            if(additionalFields != null && !additionalFields.isEmpty()
                    && (Format.avro.equals(format) || Format.parquet.equals(format))) {
                errorMessages.add("parameters.additionalFields is not supported for format " + format + " yet (csv, json, fwf only)");
            }
            if(archive != null && (Format.avro.equals(format) || Format.parquet.equals(format))) {
                errorMessages.add("parameters.archive is not supported for format " + format + " yet (csv, json, fwf only)");
            }
            if(partitions != null && (Format.avro.equals(format) || Format.parquet.equals(format))) {
                errorMessages.add("parameters.partitions is not supported for format " + format + " (csv, json, fwf only)");
            }
            if(recordSplit != null) {
                // with partitions it is a default for the fwf ones
                if(!Format.fwf.equals(format) && partitions == null) {
                    errorMessages.add("parameters.recordSplit is only supported for format fwf");
                }
                try {
                    this.split = StorageRecordReader.RecordSplit.valueOf(recordSplit.trim());
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters.recordSplit must be line or length. but: " + recordSplit);
                }
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(inputs == null) {
                inputs = new ArrayList<>();
            }
            if(inputs.isEmpty() && input != null) {
                inputs.add(input);
            } else if(input == null && !inputs.isEmpty()) {
                input = inputs.getFirst();
            }
            if(fields == null) {
                fields = new ArrayList<>();
            }
        }
    }

    public enum Format implements Serializable {
        avro,
        parquet,
        csv,
        json,
        fwf
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getSchema(), declaredFormat());
        parameters.setDefaults();

        // fwf, and the csv / json options TextIO can not provide, read files as byte records
        final boolean additionalFields = parameters.additionalFields != null && !parameters.additionalFields.isEmpty();
        if(Format.fwf.equals(parameters.format) || additionalFields || parameters.archive != null || parameters.partitions != null) {
            // the module-level way of reading: the single output, or the defaults of the partitions
            final StorageRecordReader.Partition reading = new StorageRecordReader.Partition();
            // As a default for partitions only a format that the config declares: the one derived from
            // the module-level schema belongs to that schema, and a partition with its own schema must
            // not inherit it (a partition that uses the module-level fwf schema is fwf by that schema).
            final boolean inherited = parameters.partitions == null || declaredFormat() != null;
            reading.format = parameters.format == null || !inherited
                    ? null : StorageRecordReader.Format.valueOf(parameters.format.name());
            reading.schema = getSchema();
            reading.skipHeaderLines = parameters.skipHeaderLines;
            reading.filterPrefix = parameters.filterPrefix;
            reading.delimiter = parameters.delimiter;
            reading.recordSplit = parameters.split;
            reading.fields = Format.fwf.equals(parameters.format) || parameters.partitions != null ? parameters.fields : null;

            final StorageRecordReader.Spec spec = new StorageRecordReader.Spec();
            spec.inputs = parameters.inputs;
            spec.compression = parameters.compression;
            spec.additionalFields = parameters.additionalFields;
            spec.archive = parameters.archive;
            spec.partitions = parameters.partitions == null
                    ? List.of(reading)
                    : StorageRecordReader.Partition.parse(parameters.partitions, reading);
            return StorageRecordReader.expand(
                    begin, getName(), spec, getTimestampAttribute(), getFailFast(), errorHandler);
        }

        return switch (parameters.format) {
            case avro, parquet -> {
                final PCollection<GenericRecord> record;
                final org.apache.avro.Schema outputAvroSchema;
                switch (parameters.format) {
                    case avro -> {
                        final org.apache.avro.Schema readSchema = getAvroSchema(
                                parameters.input, getSchema());
                        // fields projection (schema-redesign.md P3): the projected reader schema makes
                        // Avro schema resolution skip unlisted writer fields during decode
                        outputAvroSchema = createProjectionSchema(readSchema, parameters.fields);
                        if(parameters.inputs.size() > 1) {
                            PCollectionList<GenericRecord> list = PCollectionList.empty(begin.getPipeline());
                            int i = 0;
                            for(final String input : parameters.inputs) {
                                final PCollection<GenericRecord> p = begin
                                        .apply("ReadAvro" + i, AvroIO
                                                .readGenericRecords(outputAvroSchema)
                                                .from(input))
                                        .setCoder(AvroCoder.of(outputAvroSchema));
                                list = list.and(p);
                                i++;
                            }
                            record = list.apply("Flatten", Flatten.pCollections());
                        } else {
                            record = begin
                                    .apply("ReadAvro", AvroIO
                                            .readGenericRecords(outputAvroSchema)
                                            .from(parameters.input))
                                    .setCoder(AvroCoder.of(outputAvroSchema));
                        }
                    }
                    case parquet -> {
                        final org.apache.avro.Schema inputAvroSchema = getParquetSchema(
                                parameters.input, getSchema());
                        if(parameters.fields.isEmpty()) {
                            outputAvroSchema = inputAvroSchema;
                        } else {
                            validateProjectionFields(inputAvroSchema, parameters.fields);
                            // parquet keeps the legacy output shape: all fields, non-selected ones
                            // forced nullable (alignment with the avro subset-only output shape is a
                            // Phase 5 item — changing it would alter existing output schemas)
                            outputAvroSchema = createNullableSchema(inputAvroSchema, parameters.fields);
                        }
                        if (parameters.inputs.size() > 1) {
                            PCollectionList<GenericRecord> list = PCollectionList.empty(begin.getPipeline());
                            int i = 0;
                            for(final String input : parameters.inputs) {
                                final PCollection<GenericRecord> p = begin
                                        .apply("ReadAvro" + i, createParquetRead(input, inputAvroSchema, parameters))
                                        .setCoder(AvroCoder.of(outputAvroSchema));
                                list = list.and(p);
                                i++;
                            }
                            record = list.apply("Flatten", Flatten.pCollections());
                        } else {
                            record = begin
                                    .apply("ReadParquet", createParquetRead(parameters.input, inputAvroSchema, parameters))
                                    .setCoder(AvroCoder.of(outputAvroSchema));
                        }
                    }
                    default -> throw new IllegalArgumentException("Storage module not support format: " + parameters.format);
                }

                final PCollection<MElement> output = record
                        .apply("Format", ParDo
                                .of(new AvroFormatDoFn(getTimestampAttribute())));
                final Schema outputSchema = Schema.of(outputAvroSchema);

                yield MCollectionTuple
                        .of(output, outputSchema);
            }
            case csv, json -> {
                final String format = parameters.format.name().toUpperCase();
                final String readName = String.format("Read%sLine", format);

                PCollection<String> record;
                final Schema inputSchema = getSchema();
                if(parameters.inputs.size() > 1) {
                    PCollectionList<String> list = PCollectionList.empty(begin.getPipeline());
                    int i = 0;
                    for(final String input : parameters.inputs) {
                        final PCollection<String> lines = begin
                                .apply(readName + i, createTextRead(parameters, input))
                                .setCoder(StringUtf8Coder.of());
                        list = list.and(lines);
                        i++;
                    }
                    record = list.apply("Flatten", Flatten.pCollections());
                } else {
                    record = begin
                            .apply(readName, createTextRead(parameters, parameters.inputs.getFirst()))
                            .setCoder(StringUtf8Coder.of());
                }

                if (parameters.filterPrefix != null) {
                    final String filterPrefix = parameters.filterPrefix;
                    record = record
                            .apply("FilterPrefix", Filter.by(s -> s != null &&  !s.startsWith(filterPrefix)));
                }

                final PCollection<MElement> output = record
                        .apply("Convert",ParDo
                                .of(new TextFormatDoFn(getName(), inputSchema, parameters.format, getTimestampAttribute(), getSchema() == null)))
                        .setCoder(ElementCoder.of(inputSchema));

                yield MCollectionTuple
                        .of(output, inputSchema);
            }
            case fwf -> throw new IllegalStateException("format fwf is read by StorageRecordReader");
        };
    }

    // the text of parameters.format as written in the config; null when omitted
    private String declaredFormat() {
        final JsonElement parameters = JsonParser.parseString(getParametersText());
        if(!parameters.isJsonObject() || !parameters.getAsJsonObject().has("format")) {
            return null;
        }
        final JsonElement format = parameters.getAsJsonObject().get("format");
        return format.isJsonNull() ? null : format.isJsonPrimitive() ? format.getAsString() : format.toString();
    }

    private static ParquetIO.Read createParquetRead(
            final String input,
            final org.apache.avro.Schema readSchema,
            final Parameters parameters) {

        ParquetIO.Read read = ParquetIO
                .read(readSchema)
                .from(input);

        if(!parameters.fields.isEmpty()) {
            final org.apache.avro.Schema projectionSchema = AvroSchemaUtil.toBuilder(readSchema, parameters.fields).endRecord();
            final org.apache.avro.Schema encodeSchema = createNullableSchema(readSchema, parameters.fields);
            read = read.withProjection(projectionSchema, encodeSchema);
        }
        return read;
    }

    private static TextIO.Read createTextRead(
            final Parameters parameters,
            final String input) {
        TextIO.Read read = TextIO.read().from(input);
        if(parameters.compression != null) {
            read = read.withCompression(Compression
                    .valueOf(parameters.compression.trim().toUpperCase()));
        }
        if(parameters.skipHeaderLines != null) {
            read = read.withSkipHeaderLines(parameters.skipHeaderLines);
        }
        if(parameters.delimiter != null) {
            read = read.withDelimiter(parameters.delimiter.getBytes(StandardCharsets.UTF_8));
        }
        return read;
    }

    private static org.apache.avro.Schema createProjectionSchema(
            final org.apache.avro.Schema readSchema,
            final List<String> fields) {

        if(fields.isEmpty()) {
            return readSchema;
        }
        validateProjectionFields(readSchema, fields);
        return AvroSchemaUtil.toBuilder(readSchema, fields).endRecord();
    }

    // projection names must exist in the input schema (schema-redesign.md P3:
    // a missing field is an assembly-time error, never a silent drop)
    private static void validateProjectionFields(
            final org.apache.avro.Schema schema,
            final List<String> fields) {

        final List<String> missing = fields.stream()
                .filter(f -> schema.getField(f) == null)
                .toList();
        if(!missing.isEmpty()) {
            throw new IllegalModuleException(
                    "parameters.fields " + missing + " are not present in the input schema. available fields: "
                            + schema.getFields().stream().map(org.apache.avro.Schema.Field::name).toList());
        }
    }

    private static org.apache.avro.Schema createNullableSchema(
            final org.apache.avro.Schema schema,
            final List<String> fields) {
        final SchemaBuilder.FieldAssembler<org.apache.avro.Schema> builder = SchemaBuilder
                .record(Optional.ofNullable(schema.getName()).orElse("root"))
                .namespace(schema.getNamespace())
                .fields();
        for(org.apache.avro.Schema.Field field : schema.getFields()) {
            if(fields.contains(field.name())) {
                builder.name(field.name()).type(field.schema()).noDefault();
            } else {
                builder.name(field.name()).type(AvroSchemaUtil.toNullable(field.schema())).noDefault();
            }
        }
        return builder.endRecord();
    }

    private static class AvroFormatDoFn extends DoFn<GenericRecord, MElement> {

        private final String timestampAttribute;

        AvroFormatDoFn(final String timestampAttribute) {
            this.timestampAttribute = timestampAttribute;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement output = MElement.of(c.element(), c.timestamp());
            if(timestampAttribute != null) {
                final Instant eventTime = output.getAsJodaInstant(timestampAttribute);
                final MElement outputWithTimestamp = output.withEventTime(eventTime);
                c.outputWithTimestamp(outputWithTimestamp, eventTime);
            } else {
                c.output(output);
            }
        }
    }


    private static class TextFormatDoFn extends DoFn<String, MElement> {

        private final String name;
        private final Schema inputSchema;
        private final Format format;
        private final String timestampAttribute;
        // a date value is an epoch day, any other number is epoch micros
        private final boolean timestampIsDate;
        private final boolean rawSchema;

        TextFormatDoFn(
                final String name,
                final Schema inputSchema,
                final Format format,
                final String timestampAttribute,
                final boolean rawSchema) {

            this.name = name;
            this.inputSchema = inputSchema;
            this.format = format;
            this.timestampAttribute = timestampAttribute;
            this.timestampIsDate = timestampAttribute != null && inputSchema != null
                    && inputSchema.hasField(timestampAttribute)
                    && Schema.Type.date.equals(inputSchema.getField(timestampAttribute).getFieldType().getType());
            this.rawSchema = rawSchema;
        }

        @Setup
        public void setup() {
            this.inputSchema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Map<String, Object> values;
            if(rawSchema) {
                values = new HashMap<>();
                values.put("text", c.element());
                values.put("name", name);
                values.put("timestamp", DateTimeUtil.toEpochMicroSecond(java.time.Instant.now()));
            } else if(Format.csv.equals(format)) {
                values = CsvToElementConverter.convert(inputSchema.getFields(), c.element());
            } else {
                values = JsonToElementConverter.convert(inputSchema.getFields(), c.element());
            }

            if(timestampAttribute != null) {
                final long eventTimeEpochMillis;
                if(values == null || !values.containsKey(timestampAttribute)) {
                    eventTimeEpochMillis = c.timestamp().getMillis();
                } else {
                    eventTimeEpochMillis = switch (values.get(timestampAttribute)) {
                        case Number n when timestampIsDate -> Math.multiplyExact(n.longValue(), 86_400_000L);
                        case Number n -> n.longValue() / 1000L;
                        case String s -> DateTimeUtil.toEpochMicroSecond(s) / 1000L;
                        case null, default -> c.timestamp().getMillis();
                    };
                }
                final MElement output = MElement.of(values, eventTimeEpochMillis);
                c.outputWithTimestamp(output, Instant.ofEpochMilli(eventTimeEpochMillis));
            } else {
                final MElement output = MElement.of(values, c.timestamp());
                c.output(output);
            }
        }

    }

    // Sampling goes through Beam FileSystems (FileSchemaUtil), so path/glob resolution and
    // credentials (options.aws for s3://) match the runtime IO exactly.
    private static org.apache.avro.Schema getAvroSchema(
            final String input,
            final Schema inputSchema) {

        if(inputSchema != null) {
            return inputSchema.getAvroSchema();
        }
        return FileSchemaUtil.getAvroSchema(input);
    }

    private static org.apache.avro.Schema getParquetSchema(
            final String input,
            final Schema inputSchema) {

        if(inputSchema != null) {
            return inputSchema.getAvroSchema();
        }
        return FileSchemaUtil.getParquetSchema(input);
    }

}
