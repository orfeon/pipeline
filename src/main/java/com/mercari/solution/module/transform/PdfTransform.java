package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.mercari.solution.module.*;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Reads, renders, splits PDF documents (and, in later phases, generates and edits them).
 * Design: repo-root work_pdf.md.
 */
@Transform.Module(name="pdf")
public class PdfTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(PdfTransform.class);

    public enum Operation {
        extract,
        render,
        split
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
        private AuthProvider.Parameters auth;
        private HttpTransport.TimeoutParameters timeout;
        private HttpTransport.Parameters http;
        private JsonArray select;

        private Parameters validate(final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(field == null) {
                errorMessages.add("parameters.field must not be null");
            } else if(!inputSchema.hasField(field)) {
                errorMessages.add("parameters.field: " + field + " is not in the input schema: " + inputSchema.getFields().stream().map(Schema.Field::getName).toList());
            } else {
                final Schema.Type type = inputSchema.getField(field).getFieldType().getType();
                if(!Schema.Type.bytes.equals(type) && !Schema.Type.string.equals(type)) {
                    errorMessages.add("parameters.field: " + field + " must be bytes or string but: " + type);
                }
            }
            final Operation op = operation == null ? Operation.extract : operation;
            if(unit != null && !Operation.extract.equals(op)) {
                errorMessages.add("parameters.unit is only for operation extract");
            }
            if(include != null) {
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
        final Parameters parameters = getParameters(Parameters.class).validate(inputSchema).setDefaults();

        final boolean isSourceBytes = Schema.Type.bytes.equals(inputSchema.getField(parameters.field).getFieldType().getType());
        final boolean dropSource = isSourceBytes && !parameters.keepSource;

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
                        .of(new PdfDoFn(parameters, isSourceBytes, dropSource, selectFunctions,
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
        private final boolean isSourceBytes;
        private final boolean dropSource;
        private final List<SelectFunction> selectFunctions;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failureTag;

        private final PdfPages pageSelection;
        private final Set<Include> include;

        private transient PdfText pdfText;
        private transient HttpTransport transport;
        private transient ResponsePolicy policy;

        PdfDoFn(
                final Parameters parameters,
                final boolean isSourceBytes,
                final boolean dropSource,
                final List<SelectFunction> selectFunctions,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failureTag) {

            this.parameters = parameters;
            this.isSourceBytes = isSourceBytes;
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
            if(!isSourceBytes) {
                final ResponsePolicy.Parameters response = new ResponsePolicy.Parameters();
                response.format = ResponsePolicy.Format.bytes;
                response.setDefaults();
                this.policy = new ResponsePolicy(response);
                this.policy.setup();
                this.transport = new HttpTransport(parameters.http, parameters.timeout, AuthProvider.create(parameters.auth, null));
            }
            for(final SelectFunction selectFunction : selectFunctions) {
                selectFunction.setup();
            }
        }

        @Teardown
        public void teardown() {
            if(transport != null) {
                transport.close();
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            Logging.log(LOG, logs, "input", input);
            final byte[] bytes;
            final PDDocument document;
            try {
                bytes = readContent(input);
                document = Loader.loadPDF(bytes, parameters.password == null ? "" : parameters.password);
            } catch (final Throwable e) {
                c.output(failureTag, processError("Failed to read pdf from field: " + parameters.field, input, e, failFast));
                return;
            }
            try(document) {
                final Map<String, Object> base = new HashMap<>(input.asPrimitiveMap());
                if(dropSource) {
                    base.remove(parameters.field);
                }
                final int pageCount = document.getNumberOfPages();
                final List<Integer> pages = pageSelection.resolve(pageCount);
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
                                output(c, base, pageRow(document, renderer, bytes.length, page, pageCount));
                            } catch (final Throwable e) {
                                c.output(failureTag, processError("Failed to render pdf page: " + page, input, e, failFast));
                            }
                        }
                    }
                    case extract -> {
                        switch (parameters.unit) {
                            case document -> output(c, base, documentRow(document, bytes.length, pages, pageCount));
                            case page -> {
                                for(final int page : pages) {
                                    try {
                                        output(c, base, pageRow(document, null, bytes.length, page, pageCount));
                                    } catch (final Throwable e) {
                                        c.output(failureTag, processError("Failed to extract pdf page: " + page, input, e, failFast));
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
                                        c.output(failureTag, processError("Failed to extract images of pdf page: " + page, input, e, failFast));
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

        private Map<String, Object> documentRow(
                final PDDocument document, final long fileSize, final List<Integer> pages, final int pageCount) throws IOException {

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
                    links.addAll(PdfStructure.links(document, document.getPage(page - 1), page));
                }
                values.put("links", links);
            }
            if(include.contains(Include.forms)) {
                values.put("forms", PdfStructure.forms(document));
            }
            if(include.contains(Include.outline)) {
                values.put("outline", PdfStructure.outline(document));
            }
            if(include.contains(Include.signatures)) {
                values.put("signatures", PdfStructure.signatures(document));
            }
            return values;
        }

        private Map<String, Object> pageRow(
                final PDDocument document, final PDFRenderer renderer, final long fileSize, final int page, final int pageCount) throws IOException {

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
                values.put("links", PdfStructure.links(document, pdPage, page));
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

        private byte[] readContent(final MElement input) throws InterruptedException {
            if(isSourceBytes) {
                final ByteBuffer buffer = input.getAsBytes(parameters.field);
                if(buffer == null) {
                    throw new IllegalArgumentException("pdf content field: " + parameters.field + " is null");
                }
                final ByteBuffer duplicate = buffer.duplicate();
                final byte[] bytes = new byte[duplicate.remaining()];
                duplicate.get(bytes);
                return bytes;
            }
            final String path = input.getAsString(parameters.field);
            if(path == null || path.isBlank()) {
                throw new IllegalArgumentException("pdf path field: " + parameters.field + " is null or empty");
            }
            if(path.startsWith("https://") || path.startsWith("http://")) {
                final OutboundRequest request = new OutboundRequest(path, "GET", Map.of("Accept", "application/pdf, */*"), null, 1);
                final SyncCaller.Result result = SyncCaller.call("pdf", transport, policy, request);
                if(!result.succeeded()) {
                    throw new IllegalStateException("Failed to download pdf from " + path + ": " + result.error());
                }
                return result.parsed().bytes();
            }
            return ResourceUtil.readBytes(path);
        }

    }

}
