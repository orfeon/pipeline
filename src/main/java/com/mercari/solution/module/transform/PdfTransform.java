package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.domain.pdf.*;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.outbound.AuthProvider;
import com.mercari.solution.util.pipeline.outbound.HttpTransport;
import com.mercari.solution.util.pipeline.outbound.OutboundRequest;
import com.mercari.solution.util.pipeline.outbound.ResponsePolicy;
import com.mercari.solution.util.pipeline.outbound.SyncCaller;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Reads, renders, splits, generates and edits PDF documents.
 * Design: repo-root work_pdf.md.
 */
@Transform.Module(name="pdf")
public class PdfTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(PdfTransform.class);

    public enum Operation {
        extract,
        render,
        split,
        generate,
        edit;

        boolean writes() {
            return this == generate || this == edit;
        }
    }

    public enum Unit {
        document,
        page,
        image,
        attachment
    }

    public enum Include {
        text,
        metadata,
        lines,
        words,
        forms,
        outline,
        links,
        signatures
    }

    private static final Set<Include> DOCUMENT_ONLY = EnumSet.of(Include.forms, Include.outline, Include.signatures);

    public static class SplitParameters implements Serializable {
        public Integer pagesPerChunk;
    }

    /** {@code generate}: the document to start from; {@code steps} then change it as in {@code edit}. */
    public static class GenerateParameters extends PdfGenerate.ImagesOptions {
        public PdfGenerate.Type type;
        // form
        public String template;
        public Map<String, String> values;
        public Boolean flatten;
        public String font;
    }

    private static class Parameters implements Serializable {

        private Operation operation;
        private String field;
        private Unit unit;
        private List<Include> include;
        private String pages;
        private String password;
        private String prefix;
        private Boolean keepSource;
        private PdfText.Options text;
        private PdfRender.Options render;
        private PdfImages.Options images;
        private SplitParameters split;
        private GenerateParameters generate;
        private List<PdfEdit.Step> steps;
        private List<PdfFonts.Definition> fonts;
        private AuthProvider.Parameters auth;
        private HttpTransport.TimeoutParameters timeout;
        private HttpTransport.Parameters http;
        private JsonArray select;

        private Parameters validate(final Schema inputSchema, final JsonObject json) {
            final List<String> errorMessages = new ArrayList<>();
            // gson reads an unknown enum name as null, which setDefaults would silently replace
            checkEnum(errorMessages, json, "operation", operation, Operation.values());
            checkEnum(errorMessages, json, "unit", unit, Unit.values());
            if(render != null && json.get("render") instanceof JsonObject renderJson) {
                checkEnum(errorMessages, renderJson, "render.format", render.format, PdfRender.Format.values());
                checkEnum(errorMessages, renderJson, "render.color", render.color, PdfRender.Color.values());
                checkEnum(errorMessages, renderJson, "render.when", render.when, PdfRender.When.values());
            }
            if(images != null && json.get("images") instanceof JsonObject imagesJson) {
                checkEnum(errorMessages, imagesJson, "images.format", images.format, PdfImages.Format.values());
            }
            if(generate != null && json.get("generate") instanceof JsonObject generateJson) {
                checkEnum(errorMessages, generateJson, "generate.type", generate.type, PdfGenerate.Type.values());
                checkEnum(errorMessages, generateJson, "generate.pageSize", generate.pageSize, PdfGenerate.PageSize.values());
                checkEnum(errorMessages, generateJson, "generate.orientation", generate.orientation, PdfGenerate.Orientation.values());
            }
            if(steps != null && json.get("steps") instanceof JsonArray stepsJson) {
                for(int i = 0; i < steps.size() && i < stepsJson.size(); i++) {
                    if(steps.get(i) == null || !(stepsJson.get(i) instanceof JsonObject stepJson)) {
                        continue;
                    }
                    final PdfEdit.Step step = steps.get(i);
                    checkEnum(errorMessages, stepJson, "steps[" + i + "].type", step.type, PdfEdit.StepType.values());
                    checkEnum(errorMessages, stepJson, "steps[" + i + "].layer", step.layer, PdfEdit.Layer.values());
                    if(step.items != null && stepJson.get("items") instanceof JsonArray itemsJson) {
                        for(int j = 0; j < step.items.size() && j < itemsJson.size(); j++) {
                            if(step.items.get(j) != null && itemsJson.get(j) instanceof JsonObject itemJson) {
                                checkEnum(errorMessages, itemJson, "steps[" + i + "].items[" + j + "].position", step.items.get(j).position, PdfEdit.Position.values());
                            }
                        }
                    }
                }
            }
            final Operation op = operation == null ? Operation.extract : operation;
            final boolean generateForm = Operation.generate.equals(op) && generate != null && PdfGenerate.Type.form.equals(generate.type);
            if(generateForm) {
                if(field != null) {
                    errorMessages.add("parameters.field is not used by generate.type form (the form comes from generate.template)");
                }
            } else if(field == null) {
                errorMessages.add("parameters.field must not be null");
            } else {
                // edit merges an array of pdfs; generate images makes a page of each image
                errorMessages.addAll(PdfEdit.validateResourceField(inputSchema, field, "parameters.field", op.writes()));
            }
            if(Operation.generate.equals(op)) {
                if(generate == null || generate.type == null) {
                    errorMessages.add("parameters.generate.type is required for operation generate, available: " + Arrays.toString(PdfGenerate.Type.values()));
                } else {
                    errorMessages.addAll(generate.validate("parameters.generate"));
                    if(generateForm && (generate.template == null || generate.template.isBlank())) {
                        errorMessages.add("parameters.generate.template is required for generate.type form");
                    }
                    if(generate.font != null && !fontNames().contains(generate.font)) {
                        errorMessages.add("parameters.generate.font: " + generate.font + " is not a Standard 14 font nor in parameters.fonts");
                    }
                    // the options of the other type would be silently ignored
                    if(generateForm && (generate.pageSize != null || generate.orientation != null || generate.dpi != null || generate.margin != null)) {
                        errorMessages.add("parameters.generate: pageSize, orientation, dpi and margin are only for generate.type images");
                    } else if(!generateForm && (generate.template != null || generate.values != null || generate.flatten != null || generate.font != null)) {
                        errorMessages.add("parameters.generate: template, values, flatten and font are only for generate.type form");
                    }
                }
            } else if(generate != null) {
                errorMessages.add("parameters.generate is only for operation generate");
            }
            if(op.writes()) {
                if(pages != null) {
                    errorMessages.add("parameters.pages is not used by operation " + op + "; set pages in the steps");
                }
                if(include != null) {
                    errorMessages.add("parameters.include is not available for operation " + op);
                }
                if(fonts != null) {
                    final Set<String> fontNames = new HashSet<>();
                    for(int i = 0; i < fonts.size(); i++) {
                        if(fonts.get(i) == null) {
                            errorMessages.add("parameters.fonts[" + i + "] must not be null");
                        } else {
                            errorMessages.addAll(fonts.get(i).validate("parameters.fonts[" + i + "]"));
                            // a later definition would silently replace the earlier one
                            if(fonts.get(i).name != null && !fontNames.add(fonts.get(i).name)) {
                                errorMessages.add("parameters.fonts[" + i + "].name: " + fonts.get(i).name + " is defined more than once");
                            }
                        }
                    }
                }
                errorMessages.addAll(PdfEdit.validate(steps, "parameters.steps", inputSchema, fontNames()));
                if(Operation.edit.equals(op) && (steps == null || steps.isEmpty())) {
                    errorMessages.add("parameters.steps must not be empty for operation edit");
                }
            } else {
                if(steps != null) {
                    errorMessages.add("parameters.steps is only for operations generate and edit");
                }
                if(fonts != null) {
                    errorMessages.add("parameters.fonts is only for operations generate and edit");
                }
            }
            if(unit != null && !Operation.extract.equals(op)) {
                errorMessages.add("parameters.unit is only for operation extract");
            }
            if(include != null && !op.writes()) {
                if(include.stream().anyMatch(Objects::isNull)) {
                    errorMessages.add("parameters.include has an unknown value, available: " + Arrays.toString(Include.values()));
                }
                final Unit u = unit == null ? Unit.document : unit;
                if(Operation.split.equals(op) || (Operation.extract.equals(op) && (Unit.image.equals(u) || Unit.attachment.equals(u)))) {
                    errorMessages.add("parameters.include is not available for " + (Operation.split.equals(op) ? "operation split" : "unit " + u));
                } else if(!(Operation.extract.equals(op) && Unit.document.equals(u))) {
                    for(final Include i : include) {
                        if(DOCUMENT_ONLY.contains(i)) {
                            errorMessages.add("parameters.include: " + i + " is only for unit document");
                        }
                    }
                }
            }
            if(render != null && !Operation.render.equals(op)) {
                errorMessages.add("parameters.render is only for operation render");
            }
            if(render != null) {
                if(render.dpi != null && render.dpi <= 0) {
                    errorMessages.add("parameters.render.dpi must be positive");
                }
                if(render.quality != null && (render.quality < 0 || render.quality > 1)) {
                    errorMessages.add("parameters.render.quality must be between 0 and 1");
                }
            }
            if(split != null && split.pagesPerChunk != null && split.pagesPerChunk < 1) {
                errorMessages.add("parameters.split.pagesPerChunk must be positive");
            }
            if(pages != null) {
                try {
                    PdfPages.parse(pages);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add("parameters." + e.getMessage());
                }
            }
            if(auth != null) {
                errorMessages.addAll(auth.validate("parameters.auth"));
                if(!auth.isNone() && (http == null || http.allowedHosts == null || http.allowedHosts.isEmpty())) {
                    errorMessages.add("parameters.http.allowedHosts is required with parameters.auth, so that credentials are only sent to the hosts you trust");
                }
                if(AuthProvider.Type.gcpOidc.equals(auth.type) && auth.audience == null) {
                    // the urls come from the data, so there is no static origin to default the audience to
                    errorMessages.add("parameters.auth.audience is required for auth.type gcpOidc");
                }
            }
            if(timeout != null) {
                errorMessages.addAll(timeout.validate("parameters.timeout"));
            }
            if(http != null) {
                errorMessages.addAll(http.validate("parameters.http"));
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
            return this;
        }

        private Set<String> fontNames() {
            final Set<String> names = new HashSet<>(PdfFonts.standardNames());
            if(fonts != null) {
                fonts.stream().filter(Objects::nonNull).map(f -> f.name).filter(Objects::nonNull).forEach(names::add);
            }
            return names;
        }

        private static void checkEnum(
                final List<String> errorMessages, final JsonObject json, final String path, final Enum<?> value, final Enum<?>[] values) {

            final String name = path.substring(path.lastIndexOf('.') + 1);
            if(value == null && json != null && json.has(name) && !json.get(name).isJsonNull()) {
                errorMessages.add("parameters." + path + ": " + json.get(name) + " is unknown, available: " + Arrays.toString(values));
            }
        }

        private Parameters setDefaults() {
            if(operation == null) {
                operation = Operation.extract;
            }
            if(unit == null && Operation.extract.equals(operation)) {
                unit = Unit.document;
            }
            if(include == null) {
                include = switch (operation) {
                    case extract -> switch (unit) {
                        case document -> List.of(Include.text, Include.metadata);
                        case page -> List.of(Include.text);
                        default -> List.of();
                    };
                    default -> List.of();
                };
            }
            if(prefix == null) {
                prefix = "";
            }
            if(keepSource == null) {
                keepSource = false;
            }
            if(text == null) {
                text = new PdfText.Options();
            }
            text.setDefaults();
            if(render == null) {
                render = new PdfRender.Options();
            }
            render.setDefaults();
            if(images == null) {
                images = new PdfImages.Options();
            }
            images.setDefaults();
            if(split == null) {
                split = new SplitParameters();
            }
            if(split.pagesPerChunk == null) {
                split.pagesPerChunk = 1;
            }
            if(generate != null) {
                generate.setDefaults();
            }
            if(steps == null) {
                steps = new ArrayList<>();
            }
            if(timeout == null) {
                timeout = new HttpTransport.TimeoutParameters();
            }
            timeout.setDefaults();
            if(http == null) {
                http = new HttpTransport.Parameters();
            }
            http.setDefaults();
            return this;
        }

        private boolean isPageRows() {
            return Operation.render.equals(operation) || (Operation.extract.equals(operation) && Unit.page.equals(unit));
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Parameters parameters = getParameters(Parameters.class)
                .validate(inputSchema, Config.convertConfigJson(getParametersText(), Config.Format.json))
                .setDefaults();

        if(isGenerateForm(parameters)) {
            checkTemplate(parameters);
        }
        final boolean dropSource = parameters.field != null && isBinary(inputSchema.getField(parameters.field).getFieldType())
                && !parameters.keepSource;

        final List<Schema.Field> pdfFields = createPdfFields(parameters).stream()
                .map(f -> Schema.Field.of(parameters.prefix + f.getName(), f.getFieldType()))
                .toList();
        final List<Schema.Field> passFields = inputSchema.getFields().stream()
                .filter(f -> !(dropSource && f.getName().equals(parameters.field)))
                .toList();
        final Set<String> passNames = new HashSet<>();
        passFields.forEach(f -> passNames.add(f.getName()));
        final List<String> collisions = pdfFields.stream().map(Schema.Field::getName).filter(passNames::contains).toList();
        if(!collisions.isEmpty()) {
            throw new IllegalModuleException("pdf module: " + getName() + " output fields " + collisions
                    + " collide with input fields; set parameters.prefix (e.g. \"pdf_\")");
        }
        final Schema pdfSchema = Schema.builder().withFields(passFields).withFields(pdfFields).build();

        PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final boolean streaming = OptionUtil.isStreaming(input);
        if(!streaming) {
            input = input.apply("Reshuffle", Reshuffle.viaRandomKey());
        }

        final List<SelectFunction> selectFunctions;
        final Schema outputSchema;
        if(parameters.select != null && parameters.select.isJsonArray()) {
            selectFunctions = SelectFunction.of(parameters.select, pdfSchema.getFields());
            outputSchema = SelectFunction.createSchema(selectFunctions);
        } else {
            selectFunctions = new ArrayList<>();
            outputSchema = pdfSchema;
        }
        parameters.select = null; // gson JsonArray is not serializable; the select functions carry it

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Pdf", ParDo
                        .of(new PdfDoFn(parameters, inputSchema, dropSource, selectFunctions,
                                getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        PCollection<MElement> output = outputs.get(outputTag).setCoder(ElementCoder.of(outputSchema));
        if(Operation.split.equals(parameters.operation) && !streaming) {
            // spread the chunks so that a following render / extract runs page-parallel
            output = output.apply("ReshuffleChunks", Reshuffle.viaRandomKey());
        }
        return MCollectionTuple.of(output, outputSchema);
    }

    private static boolean isGenerateForm(final Parameters parameters) {
        return Operation.generate.equals(parameters.operation) && PdfGenerate.Type.form.equals(parameters.generate.type);
    }

    private static boolean setsSecurity(final Parameters parameters) {
        return parameters.steps.stream()
                .anyMatch(s -> PdfEdit.StepType.encrypt.equals(s.type) || PdfEdit.StepType.decrypt.equals(s.type));
    }

    /**
     * Reads the form template when the pipeline is built: a misspelled field name or a protected template would
     * fail every record, and a failing worker setup is retried forever in streaming.
     */
    private static void checkTemplate(final Parameters parameters) {
        final String path = parameters.generate.template;
        final String password = parameters.password == null ? "" : PdfEdit.resolve(parameters.password);
        final byte[] bytes;
        try {
            bytes = ResourceUtil.readBytes(path);
        } catch (final RuntimeException e) {
            throw new IllegalModuleException("parameters.generate.template: failed to read " + path + ": " + e.getMessage());
        }
        try(final PDDocument document = Loader.loadPDF(bytes, password)) {
            if(document.isEncrypted() && !setsSecurity(parameters)) {
                throw new IllegalModuleException("parameters.generate.template: " + path
                        + " is encrypted: add a decrypt step to save the forms without encryption, or an encrypt step to protect them again");
            }
            final List<String> names = PdfForm.fieldNames(document);
            if(parameters.generate.values != null) {
                final List<String> unknown = parameters.generate.values.keySet().stream().filter(n -> !names.contains(n)).toList();
                if(!unknown.isEmpty()) {
                    throw new IllegalModuleException("parameters.generate.values: form fields " + unknown
                            + " are not in the template: " + path + ", available: " + names);
                }
            }
        } catch (final IOException e) {
            throw new IllegalModuleException("parameters.generate.template: " + path + " is not a readable pdf: " + e.getMessage());
        }
    }

    private static boolean isBinary(final Schema.FieldType fieldType) {
        return Schema.Type.bytes.equals(fieldType.getType())
                || (Schema.Type.array.equals(fieldType.getType()) && Schema.Type.bytes.equals(fieldType.getArrayValueType().getType()));
    }

    // schema

    private static final Schema LINE_SCHEMA = Schema.builder()
            .withField("page", Schema.FieldType.INT64)
            .withField("text", Schema.FieldType.STRING)
            .withField("x0", Schema.FieldType.FLOAT64)
            .withField("y0", Schema.FieldType.FLOAT64)
            .withField("x1", Schema.FieldType.FLOAT64)
            .withField("y1", Schema.FieldType.FLOAT64)
            .withField("fontSize", Schema.FieldType.FLOAT64)
            .build();

    private static final Schema WORD_SCHEMA = Schema.builder()
            .withField("page", Schema.FieldType.INT64)
            .withField("text", Schema.FieldType.STRING)
            .withField("x0", Schema.FieldType.FLOAT64)
            .withField("y0", Schema.FieldType.FLOAT64)
            .withField("x1", Schema.FieldType.FLOAT64)
            .withField("y1", Schema.FieldType.FLOAT64)
            .withField("fontName", Schema.FieldType.STRING.withNullable(true))
            .withField("fontSize", Schema.FieldType.FLOAT64)
            .build();

    private static final Schema FORM_SCHEMA = Schema.builder()
            .withField("name", Schema.FieldType.STRING.withNullable(true))
            .withField("type", Schema.FieldType.STRING.withNullable(true))
            .withField("value", Schema.FieldType.STRING.withNullable(true))
            .build();

    private static final Schema OUTLINE_SCHEMA = Schema.builder()
            .withField("level", Schema.FieldType.INT64)
            .withField("title", Schema.FieldType.STRING.withNullable(true))
            .withField("page", Schema.FieldType.INT64.withNullable(true))
            .build();

    private static final Schema LINK_SCHEMA = Schema.builder()
            .withField("page", Schema.FieldType.INT64)
            .withField("uri", Schema.FieldType.STRING.withNullable(true))
            .withField("targetPage", Schema.FieldType.INT64.withNullable(true))
            .withField("x0", Schema.FieldType.FLOAT64.withNullable(true))
            .withField("y0", Schema.FieldType.FLOAT64.withNullable(true))
            .withField("x1", Schema.FieldType.FLOAT64.withNullable(true))
            .withField("y1", Schema.FieldType.FLOAT64.withNullable(true))
            .build();

    private static final Schema SIGNATURE_SCHEMA = Schema.builder()
            .withField("name", Schema.FieldType.STRING.withNullable(true))
            .withField("signDate", Schema.FieldType.TIMESTAMP.withNullable(true))
            .withField("reason", Schema.FieldType.STRING.withNullable(true))
            .withField("location", Schema.FieldType.STRING.withNullable(true))
            .withField("contactInfo", Schema.FieldType.STRING.withNullable(true))
            .withField("subFilter", Schema.FieldType.STRING.withNullable(true))
            .build();

    private static List<Schema.Field> createPdfFields(final Parameters parameters) {
        final Schema.Builder builder = Schema.builder();
        switch (parameters.operation) {
            case generate, edit -> builder
                    .withField("content", Schema.FieldType.BYTES)
                    .withField("pageCount", Schema.FieldType.INT64);
            case split -> builder
                    .withField("content", Schema.FieldType.BYTES)
                    .withField("startPage", Schema.FieldType.INT64)
                    .withField("endPage", Schema.FieldType.INT64)
                    .withField("pageCount", Schema.FieldType.INT64);
            case render, extract -> {
                switch (parameters.operation.equals(Operation.render) ? Unit.page : parameters.unit) {
                    case document -> builder.withField("pageCount", Schema.FieldType.INT64);
                    case page -> builder
                            .withField("page", Schema.FieldType.INT64)
                            .withField("pageCount", Schema.FieldType.INT64)
                            .withField("width", Schema.FieldType.FLOAT64)
                            .withField("height", Schema.FieldType.FLOAT64)
                            .withField("rotation", Schema.FieldType.INT64);
                    case image -> builder
                            .withField("page", Schema.FieldType.INT64)
                            .withField("pageCount", Schema.FieldType.INT64)
                            .withField("index", Schema.FieldType.INT64)
                            .withField("format", Schema.FieldType.STRING)
                            .withField("width", Schema.FieldType.INT64)
                            .withField("height", Schema.FieldType.INT64)
                            .withField("bitsPerComponent", Schema.FieldType.INT64)
                            .withField("colorSpace", Schema.FieldType.STRING.withNullable(true))
                            .withField("x0", Schema.FieldType.FLOAT64)
                            .withField("y0", Schema.FieldType.FLOAT64)
                            .withField("x1", Schema.FieldType.FLOAT64)
                            .withField("y1", Schema.FieldType.FLOAT64)
                            .withField("content", Schema.FieldType.BYTES);
                    case attachment -> builder
                            .withField("page", Schema.FieldType.INT64.withNullable(true))
                            .withField("pageCount", Schema.FieldType.INT64)
                            .withField("name", Schema.FieldType.STRING.withNullable(true))
                            .withField("description", Schema.FieldType.STRING.withNullable(true))
                            .withField("mimeType", Schema.FieldType.STRING.withNullable(true))
                            .withField("size", Schema.FieldType.INT64)
                            .withField("creationDate", Schema.FieldType.TIMESTAMP.withNullable(true))
                            .withField("modificationDate", Schema.FieldType.TIMESTAMP.withNullable(true))
                            .withField("content", Schema.FieldType.BYTES);
                }
                if(Operation.render.equals(parameters.operation)) {
                    builder
                            .withField("hasText", Schema.FieldType.BOOLEAN)
                            .withField("image", Schema.FieldType.BYTES.withNullable(true))
                            .withField("imageWidth", Schema.FieldType.INT64.withNullable(true))
                            .withField("imageHeight", Schema.FieldType.INT64.withNullable(true))
                            .withField("dpi", Schema.FieldType.FLOAT64.withNullable(true));
                }
                for(final Include include : new LinkedHashSet<>(parameters.include)) {
                    switch (include) {
                        case text -> builder.withField("text", Schema.FieldType.STRING);
                        case metadata -> builder
                                .withField("fileSize", Schema.FieldType.INT64)
                                .withField("version", Schema.FieldType.STRING.withNullable(true))
                                .withField("encrypted", Schema.FieldType.BOOLEAN)
                                .withField("title", Schema.FieldType.STRING.withNullable(true))
                                .withField("author", Schema.FieldType.STRING.withNullable(true))
                                .withField("subject", Schema.FieldType.STRING.withNullable(true))
                                .withField("keywords", Schema.FieldType.STRING.withNullable(true))
                                .withField("creator", Schema.FieldType.STRING.withNullable(true))
                                .withField("producer", Schema.FieldType.STRING.withNullable(true))
                                .withField("creationDate", Schema.FieldType.TIMESTAMP.withNullable(true))
                                .withField("modificationDate", Schema.FieldType.TIMESTAMP.withNullable(true))
                                .withField("trapped", Schema.FieldType.STRING.withNullable(true));
                        case lines -> builder.withField("lines", Schema.FieldType.array(Schema.FieldType.element(LINE_SCHEMA)));
                        case words -> builder.withField("words", Schema.FieldType.array(Schema.FieldType.element(WORD_SCHEMA)));
                        case forms -> builder.withField("forms", Schema.FieldType.array(Schema.FieldType.element(FORM_SCHEMA)));
                        case outline -> builder.withField("outline", Schema.FieldType.array(Schema.FieldType.element(OUTLINE_SCHEMA)));
                        case links -> builder.withField("links", Schema.FieldType.array(Schema.FieldType.element(LINK_SCHEMA)));
                        case signatures -> builder.withField("signatures", Schema.FieldType.array(Schema.FieldType.element(SIGNATURE_SCHEMA)));
                    }
                }
            }
        }
        return builder.build().getFields();
    }

    // processing

    private static class PdfDoFn extends DoFn<MElement, MElement> {

        private final Parameters parameters;
        private final Schema inputSchema;
        private final boolean dropSource;
        private final List<SelectFunction> selectFunctions;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        private final PdfPages pageSelection;
        private final Set<Include> include;

        private transient PdfText pdfText;
        private transient String password;
        private transient HttpTransport transport;
        private transient ResponsePolicy policy;

        // generate / edit
        private final String resourceOwner = UUID.randomUUID().toString();
        private transient PdfResources resources;
        private transient PdfFonts fonts;
        private transient List<PdfEdit.Compiled> steps;
        private transient byte[] template;
        private transient boolean setsSecurity;
        private transient PdfEdit.FormStep templateForm;

        PdfDoFn(
                final Parameters parameters,
                final Schema inputSchema,
                final boolean dropSource,
                final List<SelectFunction> selectFunctions,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.dropSource = dropSource;
            this.selectFunctions = selectFunctions;
            this.logs = Logging.map(logs);
            this.failFast = failFast;
            this.failureTag = failureTag;
            this.pageSelection = PdfPages.parse(parameters.pages);
            this.include = parameters.include.isEmpty() ? EnumSet.noneOf(Include.class) : EnumSet.copyOf(parameters.include);
        }

        @Setup
        public void setup() throws IOException {
            this.pdfText = new PdfText(parameters.text);
            // e.g. ${utils.secrets.get(...)}, resolved once
            this.password = parameters.password == null ? "" : PdfEdit.resolve(parameters.password);
            // the http transport (an HttpClient with its own threads) is created on the first http(s) path
            final ResponsePolicy.Parameters response = new ResponsePolicy.Parameters();
            response.format = ResponsePolicy.Format.bytes;
            response.setDefaults();
            this.policy = new ResponsePolicy(response);
            this.policy.setup();
            if(parameters.operation.writes()) {
                // fonts, the template and overlay images are read once for all threads of the worker
                this.resources = new PdfResources(resourceOwner);
                this.fonts = PdfFonts.load(parameters.fonts, resources);
                this.steps = PdfEdit.compile(parameters.steps, "pdf", resources);
                this.setsSecurity = setsSecurity(parameters);
                if(isGenerateForm(parameters)) {
                    // checked at assembly (checkTemplate)
                    this.template = resources.read(parameters.generate.template);
                    this.templateForm = new PdfEdit.FormStep(
                            parameters.generate.values, parameters.generate.flatten, parameters.generate.font, "pdf.generate");
                }
            }
            for(final SelectFunction selectFunction : selectFunctions) {
                selectFunction.setup();
            }
        }

        @Teardown
        public void teardown() throws IOException {
            if(transport != null) {
                transport.close();
            }
            if(fonts != null) {
                fonts.close();
            }
            if(resources != null) {
                resources.release();
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            Logging.log(LOG, logs, "input", input);
            if(parameters.operation.writes()) {
                write(c, input);
                return;
            }
            final byte[] bytes;
            final PDDocument document;
            try {
                bytes = readResource(input.getPrimitiveValue(parameters.field), parameters.field);
                document = Loader.loadPDF(bytes, password);
            } catch (final InterruptedException e) {
                // the worker is shutting down: fail the bundle (it is retried) rather than routing the pdf as a failure
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while reading pdf from field: " + parameters.field, e);
            } catch (final Throwable e) {
                c.output(failureTag, processError("Failed to read pdf from field: " + parameters.field, input, e, failFast));
                return;
            }
            try(document) {
                final Map<String, Object> base = base(input);
                final int pageCount = document.getNumberOfPages();
                final List<Integer> pages = pageSelection.resolve(pageCount);
                final PdfStructure.PageIndex pageIndex = new PdfStructure.PageIndex(document);
                switch (parameters.operation) {
                    case split -> {
                        for(final PdfSplit.Chunk chunk : PdfSplit.split(document, pages, parameters.split.pagesPerChunk)) {
                            final Map<String, Object> values = new HashMap<>();
                            values.put("content", ByteBuffer.wrap(chunk.bytes()));
                            values.put("startPage", (long) chunk.startPage());
                            values.put("endPage", (long) chunk.endPage());
                            values.put("pageCount", (long) pageCount);
                            output(c, base, values);
                        }
                    }
                    case render -> {
                        final PDFRenderer renderer = new PDFRenderer(document);
                        for(final int page : pages) {
                            try {
                                output(c, base, pageRow(document, pageIndex, renderer, bytes.length, page, pageCount));
                            } catch (final Throwable e) {
                                pageFailure(c, "Failed to render pdf page: " + page, input, e);
                            }
                        }
                    }
                    case extract -> {
                        switch (parameters.unit) {
                            case document -> output(c, base, documentRow(document, pageIndex, bytes.length, pages, pageCount));
                            case page -> {
                                for(final int page : pages) {
                                    try {
                                        output(c, base, pageRow(document, pageIndex, null, bytes.length, page, pageCount));
                                    } catch (final Throwable e) {
                                        pageFailure(c, "Failed to extract pdf page: " + page, input, e);
                                    }
                                }
                            }
                            case image -> {
                                for(final int page : pages) {
                                    try {
                                        for(final Map<String, Object> values : PdfImages.extract(document.getPage(page - 1), page, parameters.images)) {
                                            values.put("pageCount", (long) pageCount);
                                            output(c, base, values);
                                        }
                                    } catch (final Throwable e) {
                                        pageFailure(c, "Failed to extract images of pdf page: " + page, input, e);
                                    }
                                }
                            }
                            case attachment -> {
                                for(final Map<String, Object> values : PdfStructure.attachments(document, pages)) {
                                    values.put("pageCount", (long) pageCount);
                                    output(c, base, values);
                                }
                            }
                        }
                    }
                }
            } catch (final Throwable e) {
                c.output(failureTag, processError("Failed to process pdf with operation: " + parameters.operation, input, e, failFast));
            }
        }

        /**
         * A failing page is routed on its own, so that the other pages are still output. Under failFast it is
         * thrown to the document-level catch instead, which reports it once.
         */
        private void pageFailure(final ProcessContext c, final String message, final MElement input, final Throwable e) {
            if(failFast) {
                throw new IllegalStateException(message, e);
            }
            c.output(failureTag, processError(message, input, e, false));
        }

        private Map<String, Object> documentRow(
                final PDDocument document, final PdfStructure.PageIndex pageIndex,
                final long fileSize, final List<Integer> pages, final int pageCount) throws IOException {

            final Map<String, Object> values = new HashMap<>();
            values.put("pageCount", (long) pageCount);
            if(include.contains(Include.metadata)) {
                values.putAll(PdfStructure.metadata(document, fileSize));
            }
            final boolean collectWords = include.contains(Include.words) || include.contains(Include.lines);
            if(include.contains(Include.text) || collectWords) {
                final StringBuilder text = new StringBuilder();
                final List<PdfText.Word> words = new ArrayList<>();
                for(final int page : pages) {
                    final PdfText.PageText pageText;
                    try {
                        pageText = pdfText.extract(document, page, collectWords);
                    } catch (final IOException | RuntimeException e) {
                        throw new IllegalStateException("Failed to extract text of page: " + page, e);
                    }
                    if(!text.isEmpty()) {
                        text.append(parameters.text.pageSeparator);
                    }
                    text.append(pageText.text());
                    words.addAll(pageText.words());
                }
                putText(values, text.toString(), words);
            }
            if(include.contains(Include.links)) {
                final List<Map<String, Object>> links = new ArrayList<>();
                for(final int page : pages) {
                    links.addAll(PdfStructure.links(document, pageIndex, document.getPage(page - 1), page));
                }
                values.put("links", links);
            }
            if(include.contains(Include.forms)) {
                values.put("forms", PdfStructure.forms(document));
            }
            if(include.contains(Include.outline)) {
                values.put("outline", PdfStructure.outline(document, pageIndex));
            }
            if(include.contains(Include.signatures)) {
                values.put("signatures", PdfStructure.signatures(document));
            }
            return values;
        }

        private Map<String, Object> pageRow(
                final PDDocument document, final PdfStructure.PageIndex pageIndex, final PDFRenderer renderer,
                final long fileSize, final int page, final int pageCount) throws IOException {

            final PDPage pdPage = document.getPage(page - 1);
            final Map<String, Object> values = PdfStructure.pageValues(pdPage, page);
            values.put("pageCount", (long) pageCount);
            if(include.contains(Include.metadata)) {
                values.putAll(PdfStructure.metadata(document, fileSize));
            }
            final boolean collectWords = include.contains(Include.words) || include.contains(Include.lines);
            final boolean needText = include.contains(Include.text) || collectWords
                    || (renderer != null && PdfRender.When.noText.equals(parameters.render.when));
            String text = null;
            if(needText) {
                final PdfText.PageText pageText = pdfText.extract(document, page, collectWords);
                text = pageText.text();
                if(include.contains(Include.text) || collectWords) {
                    putText(values, text, pageText.words());
                }
            }
            if(include.contains(Include.links)) {
                values.put("links", PdfStructure.links(document, pageIndex, pdPage, page));
            }
            if(renderer != null) {
                final boolean hasText = text == null
                        ? hasText(document, page)
                        : text.strip().length() >= parameters.render.minTextLength;
                values.put("hasText", hasText);
                if(PdfRender.When.always.equals(parameters.render.when) || !hasText) {
                    final PdfRender.Rendered rendered = PdfRender.render(renderer, pdPage, page - 1, parameters.render);
                    values.put("image", ByteBuffer.wrap(rendered.bytes()));
                    values.put("imageWidth", (long) rendered.width());
                    values.put("imageHeight", (long) rendered.height());
                    values.put("dpi", (double) rendered.dpi());
                } else {
                    values.put("image", null);
                    values.put("imageWidth", null);
                    values.put("imageHeight", null);
                    values.put("dpi", null);
                }
            }
            return values;
        }

        private boolean hasText(final PDDocument document, final int page) throws IOException {
            final String text = pdfText.extract(document, page, false).text();
            return text != null && text.strip().length() >= parameters.render.minTextLength;
        }

        private void putText(final Map<String, Object> values, final String text, final List<PdfText.Word> words) {
            if(include.contains(Include.text)) {
                values.put("text", text);
            }
            if(include.contains(Include.words)) {
                values.put("words", words.stream().map(PdfText::wordValues).toList());
            }
            if(include.contains(Include.lines)) {
                values.put("lines", PdfText.lineValues(words));
            }
        }

        private void output(final ProcessContext c, final Map<String, Object> base, final Map<String, Object> pdfValues) {
            final Map<String, Object> values = new HashMap<>(base);
            for(final Map.Entry<String, Object> entry : pdfValues.entrySet()) {
                values.put(parameters.prefix + entry.getKey(), entry.getValue());
            }
            final Instant timestamp = c.timestamp();
            final MElement output;
            if(selectFunctions.isEmpty()) {
                output = MElement.of(values, timestamp);
            } else {
                output = MElement.of(SelectFunction.apply(selectFunctions, values, timestamp), timestamp);
            }
            c.output(output);
            Logging.log(LOG, logs, "output", output);
        }

        /** Bytes of a bytes value, or of the file a string value points to (gs:// / s3:// / local / http(s)://). */
        private byte[] readResource(final Object value, final String name) throws InterruptedException {
            if(value == null) {
                throw new IllegalArgumentException("field: " + name + " is null");
            }
            if(value instanceof ByteBuffer buffer) {
                return ElementSchemaUtil.toBytes(buffer);
            }
            if(value instanceof byte[] bytes) {
                return bytes;
            }
            final String path = value.toString();
            if(path.isBlank()) {
                throw new IllegalArgumentException("path field: " + name + " is empty");
            }
            if(path.startsWith("https://") || path.startsWith("http://")) {
                if(transport == null) {
                    transport = new HttpTransport(parameters.http, parameters.timeout, AuthProvider.create(parameters.auth, null));
                }
                final OutboundRequest request = new OutboundRequest(path, "GET", Map.of("Accept", "application/pdf, */*"), null, 1);
                final SyncCaller.Result result = SyncCaller.call("pdf", transport, policy, request);
                if(!result.succeeded()) {
                    throw new IllegalStateException("Failed to download " + path + ": " + result.error());
                }
                return result.parsed().bytes();
            }
            // a path from the data is read as is: not matched as a glob ('[', '*', '?' are legal in object names)
            return ResourceUtil.readBytesExact(path);
        }

        // generate / edit

        /** Creates (generate) or loads (edit) the document, applies the steps and outputs the saved pdf. */
        private void write(final ProcessContext c, final MElement input) {
            final List<PDDocument> opened = new ArrayList<>();
            try {
                final PDDocument document = switch (parameters.operation) {
                    case generate -> switch (parameters.generate.type) {
                        case form -> load(opened, template, null);
                        case images -> {
                            final List<byte[]> images = new ArrayList<>();
                            for(final Object value : PdfEdit.values(input.getPrimitiveValue(parameters.field))) {
                                images.add(readResource(value, parameters.field));
                            }
                            final PDDocument created = open(opened, new PDDocument());
                            PdfGenerate.images(created, images, parameters.generate);
                            yield created;
                        }
                    };
                    case edit -> {
                        final List<?> sources = PdfEdit.values(input.getPrimitiveValue(parameters.field));
                        if(sources.isEmpty()) {
                            throw new IllegalArgumentException("field: " + parameters.field + " holds no pdf");
                        }
                        final PDDocument base = load(opened, readResource(sources.getFirst(), parameters.field), null);
                        final PDFMergerUtility merger = new PDFMergerUtility();
                        for(final Object source : sources.subList(1, sources.size())) {
                            merger.appendDocument(base, load(opened, readResource(source, parameters.field), null));
                        }
                        yield base;
                    }
                    default -> throw new IllegalStateException("not a writing operation: " + parameters.operation);
                };
                if(document.isEncrypted() && !setsSecurity) {
                    // PDFBox cannot save it with its original encryption
                    throw new IllegalArgumentException("pdf is encrypted: add a decrypt step to save it without encryption, or an encrypt step to protect it again");
                }
                final Context context = new Context(document, input, opened);
                if(templateForm != null) {
                    templateForm.apply(context);
                }
                for(final PdfEdit.Compiled step : steps) {
                    step.apply(context);
                }
                final int pageCount = document.getNumberOfPages();
                final Map<String, Object> values = new HashMap<>();
                try(final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                    document.save(bytes);
                    values.put("content", ByteBuffer.wrap(bytes.toByteArray()));
                }
                values.put("pageCount", (long) pageCount);
                output(c, base(input), values);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while reading a resource of pdf operation: " + parameters.operation, e);
            } catch (final Throwable e) {
                c.output(failureTag, processError("Failed to " + parameters.operation + " pdf", input, e, failFast));
            } finally {
                // the merged documents are read until the result is saved
                for(final PDDocument document : opened.reversed()) {
                    try {
                        document.close();
                    } catch (final IOException e) {
                        LOG.warn("Failed to close pdf document: {}", e.getMessage());
                    }
                }
            }
        }

        private Map<String, Object> base(final MElement input) {
            final Map<String, Object> base = new HashMap<>(input.asPrimitiveMap());
            if(dropSource) {
                base.remove(parameters.field);
            }
            return base;
        }

        private static PDDocument open(final List<PDDocument> opened, final PDDocument document) {
            opened.add(document);
            return document;
        }

        private PDDocument load(final List<PDDocument> opened, final byte[] bytes, final String password) throws IOException {
            final String pw = password != null ? password : this.password;
            return open(opened, Loader.loadPDF(bytes, pw));
        }

        /** What the steps read: the record, its resources and the fonts, for one document. */
        private final class Context implements PdfEdit.Context {

            private final PDDocument document;
            private final MElement input;
            private final List<PDDocument> opened;
            private Map<String, Object> recordValues;
            private Map<String, Object> templateValues;
            private PdfFonts.Resolver resolver;

            private Context(final PDDocument document, final MElement input, final List<PDDocument> opened) {
                this.document = document;
                this.input = input;
                this.opened = opened;
            }

            @Override
            public PDDocument document() {
                return document;
            }

            @Override
            public Map<String, Object> recordValues() {
                if(recordValues == null) {
                    recordValues = input.asStandardMap(inputSchema);
                }
                return recordValues;
            }

            @Override
            public Map<String, Object> templateValues() {
                if(templateValues == null) {
                    templateValues = new HashMap<>(recordValues());
                    TemplateUtil.setFunctions(templateValues);
                }
                return templateValues;
            }

            @Override
            public Object fieldValue(final String field) {
                return input.getPrimitiveValue(field);
            }

            @Override
            public byte[] read(final Object value) throws InterruptedException {
                return readResource(value, "resource");
            }

            @Override
            public PDDocument load(final byte[] bytes, final String password) throws IOException {
                return PdfDoFn.this.load(opened, bytes, password);
            }

            @Override
            public PdfFonts.Resolver fonts() {
                if(resolver == null) {
                    resolver = fonts.forDocument(document);
                }
                return resolver;
            }
        }

    }

}
