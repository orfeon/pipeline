package com.mercari.solution.module.sink;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.StorageUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import com.mercari.solution.util.schema.converter.ElementToCsvConverter;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import com.mercari.solution.util.schema.converter.MapToJsonConverter;
import freemarker.template.Template;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Sink.Module(name="files")
public class FilesSink extends Sink {

    private static class Parameters implements Serializable {

        private String output;
        private ContentParameters content;
        private Map<String, String> attributes;

        private Boolean reshuffle;

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(output == null) {
                errorMessages.add("parameters.output must not be empty");
            }
            if(content == null) {
                errorMessages.add("parameters.content must not be empty");
            } else {
                errorMessages.addAll(content.validate());
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults(Schema inputSchema) {
            if(content != null) {
                content.setDefaults(inputSchema);
            }
            if(attributes == null) {
                attributes = new HashMap<>();
            }
            if(reshuffle == null) {
                reshuffle = false;
            }
        }
    }

    private static class ContentParameters implements Serializable {

        public Format format;
        private String field;
        private String text;

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(field != null && text != null) {
                errorMessages.add("");
            }
            return errorMessages;
        }

        private void setDefaults(final Schema inputSchema) {
            if(format == null) {
                if(text != null) {
                    format = Format.text;
                } else if(field != null) {
                    final Schema.Field inputField = ElementSchemaUtil.getInputField(field, inputSchema.getFields());
                    format = switch (inputField.getFieldType().getType()) {
                        case json, element, array, map -> Format.json;
                        case bytes -> Format.bytes;
                        case string -> Format.text;
                        default -> Format.text;
                    };
                } else {
                    format = Format.json;
                }
            }
        }
    }

    private enum Format {
        csv,
        json,
        avro,
        text,
        bytes
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();

        PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        parameters.setDefaults(inputSchema);

        if(parameters.reshuffle) {
            input = input.apply("Reshuffle", Reshuffle.viaRandomKey());
        }

        final TupleTag<MElement> outputTag = new TupleTag<>(){};
        final TupleTag<BadRecord> failureTag = new TupleTag<>(){};

        final PCollectionTuple outputs = input
                .apply("WriteFile", ParDo
                        .of(new WriteDoFn(inputSchema, parameters, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), createOutputSchema());
    }

    private static class WriteDoFn extends DoFn<MElement, MElement> {

        private final Schema inputSchema;

        private final String output;
        private final Map<String, String> attributes;
        private final ContentParameters content;

        private final Map<String, Logging> logging;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        private transient Template templateOutput;
        private transient Map<String,Template> templateAttributes;
        private transient Template templateContent;

        private transient Set<String> templateArgs;

        WriteDoFn(
                final Schema inputSchema,
                final Parameters parameters,
                final List<Logging> logging,
                final Boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.inputSchema = inputSchema;

            this.output = parameters.output;
            this.attributes = parameters.attributes;
            this.content = parameters.content;

            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            inputSchema.setup();
            this.templateArgs = new HashSet<>();

            this.templateOutput = TemplateUtil.createStrictTemplate("output", output);
            this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(output, inputSchema));

            this.templateAttributes = new HashMap<>();
            for(final Map.Entry<String, String> entry : attributes.entrySet()) {
                this.templateAttributes.put(entry.getKey(), TemplateUtil.createStrictTemplate(entry.getKey(), entry.getValue()));
                this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(entry.getValue(), inputSchema));
            }
            if(content.text != null) {
                this.templateContent = TemplateUtil.createStrictTemplate("content", content.text);
                this.templateArgs.addAll(TemplateUtil.extractTemplateArgs(content.text, inputSchema));
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            String output = null;
            try {
                final Map<String, Object> templateValues = input.asStandardMap(inputSchema, null);
                output = TemplateUtil.executeStrictTemplate(templateOutput, templateValues);
                final Object object = createContent(inputSchema, templateValues);
                if(output.startsWith("gs://")) {
                    writeGcs(output, object, templateValues, content.format);
                } else {
                    writeLocal(output, object);
                }
                final MElement result = createOutput(output, c.timestamp());
                c.output(result);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to write file to " + output, input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        private Object createContent(final Schema inputSchema, final Map<String, Object> input) {
            if(content.text == null && content.field == null) {
                return switch (content.format) {
                    case csv -> ElementToCsvConverter.convert(inputSchema, input, inputSchema.getFields().stream().map(Schema.Field::getName).toList());
                    case json -> ElementToJsonConverter.convert(inputSchema, input);
                    case avro -> ElementToAvroConverter.convert(inputSchema, input);
                    default -> throw new IllegalArgumentException("");
                };
            } else if(content.field != null) {
                final Object fieldValue = ElementSchemaUtil.getValue(input, content.field);
                if(fieldValue == null) {
                    return null;
                }
                return switch (content.format) {
                    case csv, text -> fieldValue.toString();
                    case json -> {
                        final Schema.Field field = ElementSchemaUtil.getInputField(content.field, inputSchema.getFields());
                        yield switch (field.getFieldType().getType()) {
                            case string, json -> fieldValue;
                            case map -> MapToJsonConverter.convertObject((Map) fieldValue).toString();
                            case element -> ElementToJsonConverter.convert(field.getFieldType().getElementSchema(), (Map) fieldValue);
                            case null -> null;
                            default -> fieldValue.toString();
                        };
                    }
                    case avro -> {
                        final Schema.Field field = ElementSchemaUtil.getInputField(content.field, inputSchema.getFields());
                        yield switch (field.getFieldType().getType()) {
                            case element -> ElementToAvroConverter.convert(field.getFieldType().getElementSchema(), (Map) fieldValue);
                            default -> throw new IllegalArgumentException("not supported avro compatible fieldType: " + field.getFieldType());
                        };
                    }
                    case bytes -> {
                        final Schema.Field field = ElementSchemaUtil.getInputField(content.field, inputSchema.getFields());
                        yield switch (field.getFieldType().getType()) {
                            case bytes -> fieldValue;
                            default -> throw new IllegalArgumentException("not supported avro compatible fieldType: " + field.getFieldType());
                        };
                    }
                };
            } else {
                return TemplateUtil.executeStrictTemplate(templateContent, input);
            }
        }

        private void writeLocal(final String output, final Object content) {
            final Path path = Path.of(output);
            try {
                Files.createDirectories(path.getParent());
                switch (content) {
                    case byte[] bytes -> Files.write(path, bytes);
                    case String string -> Files.writeString(path, string, StandardCharsets.UTF_8);
                    case Utf8 utf8 -> Files.writeString(path, utf8.toString(), StandardCharsets.UTF_8);
                    case Number number -> Files.writeString(path, number.toString(), StandardCharsets.UTF_8);
                    case ByteBuffer byteBuffer -> Files.write(path, byteBuffer.array());
                    case null -> {

                    }
                    default -> Files.writeString(path, content.toString(), StandardCharsets.UTF_8);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void writeGcs(final String output, final Object content, final Map<String, Object> templateValues, Format format) {
            final StorageObject object = new StorageObject();
            final String[] gcsPaths = StorageUtil.parseGcsPath(output);
            object.setBucket(gcsPaths[0]);
            object.setName(gcsPaths[1]);
            final Map<String, Object> attributeValues = new HashMap<>();
            for(final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), templateValues);
                object.set(entry.getKey(), value);
                attributeValues.put(entry.getKey(), value);
            }
            final String contentType = switch (format) {
                case csv -> "text/csv";
                case text -> "text/plain";
                case json -> "application/json";
                case avro -> "avro/binary";
                default -> "application/octet-stream";
            };
            if(object.getContentType() == null) {
                object.setContentType(contentType);
            }

            try {
                Storage storage = StorageUtil.storage();
                switch (content) {
                    case byte[] bytes -> StorageUtil.writeObject(storage, object, bytes);
                    case String string -> StorageUtil.writeString(output, string, object.getContentType(), attributeValues, new HashMap<>());
                    case Utf8 utf8 -> StorageUtil.writeString(output, utf8.toString(), object.getContentType(), attributeValues, new HashMap<>());
                    case ByteBuffer byteBuffer -> StorageUtil.writeObject(storage, object, byteBuffer.array());
                    case null -> {}
                    default -> StorageUtil.writeString(output, content.toString(), object.getContentType(), attributeValues, new HashMap<>());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        /*
        private void writeS3(final String s3DestinationPath, final byte[] bytes, final Map<String, Object> templateValues) {
            final Map<String, Object> attributes = new HashMap<>();
            for(final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), templateValues);
                attributes.put(entry.getKey(), value);
            }
            final String contentType;
            if(templateAttributes.containsKey("contentType")) {
                contentType = TemplateUtil.executeStrictTemplate(templateAttributes.get("contentType"), templateValues);
            } else {
                contentType = "application/octet-stream";
            }
            S3Util.writeBytes(s3, s3DestinationPath, bytes, contentType, attributes, new HashMap<>());
        }
         */
    }

    private static MElement createOutput(String output, Instant instant) {
        return MElement.of(Map
                .of("output", output, "timestamp", Instant.now().getMillis() * 1000L),
                instant);
    }

    private static Schema createOutputSchema() {
        return Schema.builder()
                .withField("output", Schema.FieldType.STRING.withNullable(true))
                .withField("timestamp", Schema.FieldType.TIMESTAMP.withNullable(true))
                .build();
    }

}
