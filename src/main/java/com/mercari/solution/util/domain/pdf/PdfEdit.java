package com.mercari.solution.util.domain.pdf;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.domain.file.ResourceUtil;
import freemarker.template.Template;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDPageTree;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.StandardProtectionPolicy;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDFontDescriptor;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.pdmodel.graphics.state.PDExtendedGraphicsState;
import org.apache.pdfbox.util.Matrix;

import java.awt.Color;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

/**
 * The {@code steps} of the pdf transform's {@code generate} and {@code edit} operations: each step changes the
 * document in place, in the order given.
 *
 * <p>Steps are configured as serializable {@link Step}s and {@link #compile compiled} on the worker
 * (templates, static images), as FreeMarker templates are not serializable.
 */
public class PdfEdit {

    public enum StepType {
        form,
        merge,
        pages,
        rotate,
        overlay,
        metadata,
        encrypt,
        decrypt
    }

    public enum Layer {
        foreground,
        background
    }

    public enum Position {
        topLeft,
        top,
        topRight,
        left,
        center,
        right,
        bottomLeft,
        bottom,
        bottomRight
    }

    public static class Permissions implements Serializable {
        public Boolean print;
        public Boolean printHighQuality;
        public Boolean modify;
        public Boolean copy;
        public Boolean annotate;
        public Boolean fillForms;
        public Boolean extractForAccessibility;
        public Boolean assemble;
    }

    public static class Item implements Serializable {
        public String text;
        public String image;
        public String imageField;
        public Position position;
        public Float margin;
        public Float x;
        public Float y;
        public String font;
        public Float fontSize;
        public Float lineHeight;
        public String color;
        public Float opacity;
        public Float rotate;
        public Float width;
        public Float height;
    }

    public static class Step implements Serializable {
        public StepType type;
        public String pages;
        // rotate
        public Integer angle;
        // merge
        public String field;
        public String password;
        // overlay
        public Layer layer;
        public List<Item> items;
        // form
        public Map<String, String> values;
        public Boolean flatten;
        public String font;
        // metadata
        public String title;
        public String author;
        public String subject;
        public String keywords;
        public String creator;
        public String producer;
        // encrypt
        public String ownerPassword;
        public String userPassword;
        public Integer keyLength;
        public Permissions permissions;
    }

    /** What a step reads besides the document: the record and the resources it points to. */
    public interface Context {
        PDDocument document();
        /** Values of the record for templates (standard values, with the template functions). */
        Map<String, Object> templateValues();
        /** Values of the record for form fields (standard values). */
        Map<String, Object> recordValues();
        /** Primitive value of a record field. */
        Object fieldValue(String field);
        /** Bytes of a bytes value, or of the file a string value points to. */
        byte[] read(Object value) throws Exception;
        /** Loads a pdf that stays open until the document is saved. */
        PDDocument load(byte[] bytes, String password) throws IOException;
        PdfFonts.Resolver fonts();
    }

    public interface Compiled {
        void apply(Context context) throws Exception;
    }

    private static final Pattern COLOR = Pattern.compile("#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})");
    private static final Set<Integer> KEY_LENGTHS = Set.of(40, 128, 256);
    private static final Set<StepType> PAGE_STEPS = EnumSet.of(StepType.pages, StepType.rotate, StepType.overlay);

    private PdfEdit() {}

    // validation

    public static List<String> validate(
            final List<Step> steps, final String path, final Schema inputSchema, final Set<String> fontNames) {

        final List<String> errorMessages = new ArrayList<>();
        if(steps == null) {
            return errorMessages;
        }
        for(int i = 0; i < steps.size(); i++) {
            final Step step = steps.get(i);
            final String p = path + "[" + i + "]";
            if(step == null || step.type == null) {
                errorMessages.add(p + ".type must not be null, available: " + Arrays.toString(StepType.values()));
                continue;
            }
            if(step.pages != null) {
                if(!PAGE_STEPS.contains(step.type)) {
                    // the step works on the whole document: a page selection would be silently ignored
                    errorMessages.add(p + ".pages is not used by step type " + step.type + ", only by " + PAGE_STEPS);
                }
                try {
                    PdfPages.parse(step.pages);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(p + "." + e.getMessage());
                }
            }
            switch (step.type) {
                case form -> {
                    if(step.font != null && !fontNames.contains(step.font)) {
                        errorMessages.add(p + ".font: " + step.font + " is not a Standard 14 font nor in parameters.fonts");
                    }
                }
                case merge -> {
                    if(step.field == null) {
                        errorMessages.add(p + ".field must not be null");
                    } else {
                        errorMessages.addAll(validateResourceField(inputSchema, step.field, p + ".field", true));
                    }
                }
                case pages -> {
                    if(step.pages == null || step.pages.isBlank()) {
                        errorMessages.add(p + ".pages must not be empty");
                    }
                }
                case rotate -> {
                    if(step.angle == null || step.angle % 90 != 0) {
                        errorMessages.add(p + ".angle must be a multiple of 90");
                    }
                }
                case overlay -> {
                    if(step.items == null || step.items.isEmpty()) {
                        errorMessages.add(p + ".items must not be empty");
                    } else {
                        for(int j = 0; j < step.items.size(); j++) {
                            errorMessages.addAll(validateItem(step.items.get(j), p + ".items[" + j + "]", inputSchema, fontNames));
                        }
                    }
                }
                case metadata -> {
                    if(step.title == null && step.author == null && step.subject == null && step.keywords == null
                            && step.creator == null && step.producer == null) {
                        errorMessages.add(p + " sets none of title, author, subject, keywords, creator, producer");
                    }
                }
                case encrypt -> {
                    if(step.ownerPassword == null && step.userPassword == null) {
                        errorMessages.add(p + " needs ownerPassword or userPassword");
                    }
                    if(step.keyLength != null && !KEY_LENGTHS.contains(step.keyLength)) {
                        errorMessages.add(p + ".keyLength must be one of " + KEY_LENGTHS);
                    }
                }
                case decrypt -> {}
            }
        }
        return errorMessages;
    }

    private static List<String> validateItem(final Item item, final String path, final Schema inputSchema, final Set<String> fontNames) {
        final List<String> errorMessages = new ArrayList<>();
        if(item == null) {
            errorMessages.add(path + " must not be null");
            return errorMessages;
        }
        final int kinds = (item.text != null ? 1 : 0) + (item.image != null ? 1 : 0) + (item.imageField != null ? 1 : 0);
        if(kinds != 1) {
            errorMessages.add(path + " needs exactly one of text, image, imageField");
        }
        if(item.imageField != null) {
            if(!inputSchema.hasField(item.imageField)) {
                errorMessages.add(path + ".imageField: " + item.imageField + " is not in the input schema");
            } else if(!Schema.Type.bytes.equals(inputSchema.getField(item.imageField).getFieldType().getType())) {
                errorMessages.add(path + ".imageField: " + item.imageField + " must be a bytes field");
            }
        }
        if(item.font != null && !fontNames.contains(item.font)) {
            errorMessages.add(path + ".font: " + item.font + " is not a Standard 14 font nor in parameters.fonts");
        }
        if(item.fontSize != null && item.fontSize <= 0) {
            errorMessages.add(path + ".fontSize must be positive");
        }
        if(item.lineHeight != null && item.lineHeight <= 0) {
            errorMessages.add(path + ".lineHeight must be positive");
        }
        if(item.color != null && !COLOR.matcher(item.color).matches()) {
            errorMessages.add(path + ".color must be #RRGGBB or #RGB: " + item.color);
        }
        if(item.opacity != null && (item.opacity < 0 || item.opacity > 1)) {
            errorMessages.add(path + ".opacity must be between 0 and 1");
        }
        if((item.width != null && item.width <= 0) || (item.height != null && item.height <= 0)) {
            errorMessages.add(path + ".width and .height must be positive");
        }
        return errorMessages;
    }

    /** A field holding a pdf / image: bytes, a string path, or an array of them. */
    public static List<String> validateResourceField(final Schema inputSchema, final String field, final String path, final boolean allowArray) {
        final List<String> errorMessages = new ArrayList<>();
        if(!inputSchema.hasField(field)) {
            errorMessages.add(path + ": " + field + " is not in the input schema: " + inputSchema.getFields().stream().map(Schema.Field::getName).toList());
            return errorMessages;
        }
        Schema.FieldType fieldType = inputSchema.getField(field).getFieldType();
        if(allowArray && Schema.Type.array.equals(fieldType.getType())) {
            fieldType = fieldType.getArrayValueType();
        }
        if(!Schema.Type.bytes.equals(fieldType.getType()) && !Schema.Type.string.equals(fieldType.getType())) {
            errorMessages.add(path + ": " + field + " must be bytes or string" + (allowArray ? " (or an array of them)" : "")
                    + " but: " + inputSchema.getField(field).getFieldType().getType());
        }
        return errorMessages;
    }

    // compile

    public static List<Compiled> compile(final List<Step> steps, final String name) {
        final List<Compiled> compiled = new ArrayList<>();
        if(steps == null) {
            return compiled;
        }
        for(int i = 0; i < steps.size(); i++) {
            final Step step = steps.get(i);
            final String n = name + ".steps[" + i + "]";
            compiled.add(switch (step.type) {
                case form -> new FormStep(step, n);
                case merge -> new MergeStep(step);
                case pages -> new PagesStep(step);
                case rotate -> new RotateStep(step);
                case overlay -> new OverlayStep(step, n);
                case metadata -> new MetadataStep(step, n);
                case encrypt -> new EncryptStep(step, n);
                case decrypt -> context -> context.document().setAllSecurityToBeRemoved(true);
            });
        }
        return compiled;
    }

    /** A text that is a FreeMarker template when it contains {@code ${...}}, else a constant. */
    public static final class Text {

        private final String constant;
        private final Template template;

        public Text(final String name, final String text) {
            if(text != null && TemplateUtil.isTemplateText(text)) {
                this.constant = null;
                this.template = TemplateUtil.createStrictTemplate(name, text);
            } else {
                this.constant = text;
                this.template = null;
            }
        }

        public String render(final Map<String, Object> values) {
            return template == null ? constant : TemplateUtil.executeStrictTemplate(template, values);
        }
    }

    /** Fills form fields: {@code values} (field name → text) or, if omitted, the record fields named like the form fields. */
    public static final class FormStep implements Compiled {

        private final Map<String, Text> values;
        private final boolean flatten;
        private final String font;

        public FormStep(final Map<String, String> values, final Boolean flatten, final String font, final String name) {
            if(values == null) {
                this.values = null;
            } else {
                this.values = new LinkedHashMap<>();
                for(final Map.Entry<String, String> entry : values.entrySet()) {
                    this.values.put(entry.getKey(), new Text(name + ".values." + entry.getKey(), entry.getValue()));
                }
            }
            this.flatten = flatten != null && flatten;
            this.font = font;
        }

        FormStep(final Step step, final String name) {
            this(step.values, step.flatten, step.font, name);
        }

        public Set<String> fieldNames() {
            return values == null ? null : values.keySet();
        }

        @Override
        public void apply(final Context context) throws IOException {
            final Map<String, Object> fieldValues = new LinkedHashMap<>();
            if(values == null) {
                final Map<String, Object> record = context.recordValues();
                for(final String name : PdfForm.fieldNames(context.document())) {
                    if(record.containsKey(name)) {
                        fieldValues.put(name, record.get(name));
                    }
                }
            } else {
                final Map<String, Object> templateValues = context.templateValues();
                for(final Map.Entry<String, Text> entry : values.entrySet()) {
                    fieldValues.put(entry.getKey(), entry.getValue().render(templateValues));
                }
            }
            PdfForm.fill(context.document(), fieldValues, font, context.fonts(), flatten);
        }
    }

    private record MergeStep(String field, String password) implements Compiled {

        MergeStep(final Step step) {
            // resolved once per worker, as the module password
            this(step.field, resolve(step.password));
        }

        @Override
        public void apply(final Context context) throws Exception {
            final PDFMergerUtility merger = new PDFMergerUtility();
            for(final Object v : values(context.fieldValue(field))) {
                merger.appendDocument(context.document(), context.load(context.read(v), password));
            }
        }
    }

    private record PagesStep(PdfPages pages) implements Compiled {

        PagesStep(final Step step) {
            this(PdfPages.parse(step.pages));
        }

        @Override
        public void apply(final Context context) {
            final PDDocument document = context.document();
            final List<Integer> order = pages.resolveInOrder(document.getNumberOfPages());
            if(order.isEmpty()) {
                throw new IllegalArgumentException("pages selects no page of the " + document.getNumberOfPages() + " pages");
            }
            if(new HashSet<>(order).size() != order.size()) {
                throw new IllegalArgumentException("pages selects a page more than once: " + order);
            }
            final List<PDPage> selected = new ArrayList<>();
            for(final int page : order) {
                final PDPage pdPage = document.getPage(page - 1);
                // attributes inherited from the old page tree nodes are lost when the page moves to a new tree
                pdPage.setResources(pdPage.getResources());
                pdPage.setMediaBox(pdPage.getMediaBox());
                pdPage.setCropBox(pdPage.getCropBox());
                pdPage.setRotation(pdPage.getRotation());
                selected.add(pdPage);
            }
            final PDPageTree tree = new PDPageTree();
            selected.forEach(tree::add);
            document.getDocumentCatalog().getCOSObject().setItem(COSName.PAGES, tree.getCOSObject());
        }
    }

    private record RotateStep(PdfPages pages, int angle) implements Compiled {

        RotateStep(final Step step) {
            this(PdfPages.parse(step.pages), step.angle);
        }

        @Override
        public void apply(final Context context) {
            final PDDocument document = context.document();
            for(final int page : pages.resolve(document.getNumberOfPages())) {
                final PDPage pdPage = document.getPage(page - 1);
                pdPage.setRotation(Math.floorMod(pdPage.getRotation() + angle, 360));
            }
        }
    }

    private static final class MetadataStep implements Compiled {

        private final Text title, author, subject, keywords, creator, producer;

        MetadataStep(final Step step, final String name) {
            this.title = step.title == null ? null : new Text(name + ".title", step.title);
            this.author = step.author == null ? null : new Text(name + ".author", step.author);
            this.subject = step.subject == null ? null : new Text(name + ".subject", step.subject);
            this.keywords = step.keywords == null ? null : new Text(name + ".keywords", step.keywords);
            this.creator = step.creator == null ? null : new Text(name + ".creator", step.creator);
            this.producer = step.producer == null ? null : new Text(name + ".producer", step.producer);
        }

        @Override
        public void apply(final Context context) {
            final Map<String, Object> values = context.templateValues();
            final PDDocumentInformation info = context.document().getDocumentInformation();
            if(title != null) info.setTitle(title.render(values));
            if(author != null) info.setAuthor(author.render(values));
            if(subject != null) info.setSubject(subject.render(values));
            if(keywords != null) info.setKeywords(keywords.render(values));
            if(creator != null) info.setCreator(creator.render(values));
            if(producer != null) info.setProducer(producer.render(values));
        }
    }

    private static final class EncryptStep implements Compiled {

        private final Text ownerPassword;
        private final Text userPassword;
        private final int keyLength;
        private final Permissions permissions;

        EncryptStep(final Step step, final String name) {
            this.ownerPassword = step.ownerPassword == null ? null : new Text(name + ".ownerPassword", step.ownerPassword);
            this.userPassword = step.userPassword == null ? null : new Text(name + ".userPassword", step.userPassword);
            this.keyLength = step.keyLength == null ? 256 : step.keyLength;
            this.permissions = step.permissions == null ? new Permissions() : step.permissions;
        }

        @Override
        public void apply(final Context context) throws IOException {
            final Map<String, Object> values = context.templateValues();
            // without an owner password nobody can lift the permissions
            final String owner = ownerPassword == null ? UUID.randomUUID().toString() : ownerPassword.render(values);
            final String user = userPassword == null ? "" : userPassword.render(values);
            final AccessPermission access = new AccessPermission();
            access.setCanPrint(allowed(permissions.print));
            access.setCanPrintFaithful(allowed(permissions.printHighQuality));
            access.setCanModify(allowed(permissions.modify));
            access.setCanExtractContent(allowed(permissions.copy));
            access.setCanModifyAnnotations(allowed(permissions.annotate));
            access.setCanFillInForm(allowed(permissions.fillForms));
            access.setCanExtractForAccessibility(allowed(permissions.extractForAccessibility));
            access.setCanAssembleDocument(allowed(permissions.assemble));
            final StandardProtectionPolicy policy = new StandardProtectionPolicy(owner, user, access);
            policy.setEncryptionKeyLength(keyLength);
            // PDFBox defaults to RC4 for 128-bit keys: use AES-128 (40-bit keys are RC4 only, 256-bit always AES)
            policy.setPreferAES(true);
            context.document().protect(policy);
        }

        private static boolean allowed(final Boolean permission) {
            return permission == null || permission;
        }
    }

    /** Draws text and images on pages, positioned in the page as displayed (crop box, /Rotate applied). */
    private static final class OverlayStep implements Compiled {

        private final PdfPages pages;
        private final boolean background;
        private final List<Item> items;
        private final List<Text> texts;
        private final List<byte[]> images;

        OverlayStep(final Step step, final String name) {
            this.pages = PdfPages.parse(step.pages);
            this.background = Layer.background.equals(step.layer);
            this.items = step.items;
            this.texts = new ArrayList<>();
            this.images = new ArrayList<>();
            for(int i = 0; i < items.size(); i++) {
                final Item item = items.get(i);
                texts.add(item.text == null ? null : new Text(name + ".items[" + i + "].text", item.text));
                images.add(item.image == null ? null : ResourceUtil.readBytes(item.image));
            }
        }

        @Override
        public void apply(final Context context) throws Exception {
            final PDDocument document = context.document();
            final int pageCount = document.getNumberOfPages();
            // an image is embedded once and drawn on every page
            final List<PDImageXObject> xObjects = new ArrayList<>();
            for(int i = 0; i < items.size(); i++) {
                final Item item = items.get(i);
                byte[] bytes = images.get(i);
                if(item.imageField != null) {
                    final Object value = context.fieldValue(item.imageField);
                    bytes = value == null ? null : context.read(value);
                }
                xObjects.add(bytes == null ? null : PdfGenerate.createImage(document, bytes));
            }
            // one copy of the record values per document; only page changes from page to page
            final Map<String, Object> values = new HashMap<>(context.templateValues());
            values.put("pageCount", pageCount);
            for(final int page : pages.resolve(pageCount)) {
                final PDPage pdPage = document.getPage(page - 1);
                values.put("page", page);
                final PDRectangle box = pdPage.getCropBox();
                final int rotation = Math.floorMod(pdPage.getRotation(), 360);
                final boolean swap = rotation == 90 || rotation == 270;
                final float width = swap ? box.getHeight() : box.getWidth();
                final float height = swap ? box.getWidth() : box.getHeight();
                final PDPageContentStream.AppendMode mode = background
                        ? PDPageContentStream.AppendMode.PREPEND
                        : PDPageContentStream.AppendMode.APPEND;
                try(final PDPageContentStream content = new PDPageContentStream(document, pdPage, mode, true, true)) {
                    content.saveGraphicsState();
                    content.transform(displayMatrix(box, rotation));
                    for(int i = 0; i < items.size(); i++) {
                        final Item item = items.get(i);
                        if(texts.get(i) != null) {
                            drawText(content, context.fonts(), item, texts.get(i).render(values), width, height);
                        } else if(xObjects.get(i) != null) {
                            drawImage(content, item, xObjects.get(i), width, height);
                        }
                    }
                    content.restoreGraphicsState();
                }
            }
        }

        /**
         * Maps the displayed page (origin at its lower left, as rotated by /Rotate clockwise) to the user space.
         */
        static Matrix displayMatrix(final PDRectangle box, final int rotation) {
            final float x = box.getLowerLeftX();
            final float y = box.getLowerLeftY();
            final float w = box.getWidth();
            final float h = box.getHeight();
            return switch (rotation) {
                case 90 -> new Matrix(0, 1, -1, 0, x + w, y);
                case 180 -> new Matrix(-1, 0, 0, -1, x + w, y + h);
                case 270 -> new Matrix(0, -1, 1, 0, x, y + h);
                default -> new Matrix(1, 0, 0, 1, x, y);
            };
        }

        private static void drawText(
                final PDPageContentStream content, final PdfFonts.Resolver fonts, final Item item,
                final String text, final float pageWidth, final float pageHeight) throws IOException {

            if(text == null) {
                return;
            }
            // any line break (\n, \r\n, \r); trailing line breaks (e.g. of a YAML block scalar) are dropped, as an
            // empty last line would only shift a bottom / centered box
            final String[] lines = text.split("\\R");
            if(lines.length == 0 || (lines.length == 1 && lines[0].isEmpty())) {
                return;
            }
            final PDFont font = fonts.get(item.font, true);
            final float size = item.fontSize == null ? 12F : item.fontSize;
            final float leading = size * (item.lineHeight == null ? 1.2F : item.lineHeight);
            final PDFontDescriptor descriptor = font.getFontDescriptor();
            final float ascent = size * (descriptor == null || descriptor.getAscent() == 0 ? 800F : descriptor.getAscent()) / 1000F;
            final float descent = size * (descriptor == null || descriptor.getDescent() == 0 ? 200F : Math.abs(descriptor.getDescent())) / 1000F;
            final float[] lineWidths = new float[lines.length];
            float boxWidth = 0;
            for(int i = 0; i < lines.length; i++) {
                try {
                    lineWidths[i] = font.getStringWidth(lines[i]) / 1000F * size;
                } catch (final IllegalArgumentException e) {
                    throw new IllegalArgumentException("font: " + font.getName() + " cannot draw the text: '" + lines[i]
                            + "' (" + e.getMessage() + "); set a TrueType font covering it in parameters.fonts", e);
                }
                boxWidth = Math.max(boxWidth, lineWidths[i]);
            }
            final float boxHeight = ascent + descent + leading * (lines.length - 1);
            final float[] origin = layout(item, boxWidth, boxHeight, pageWidth, pageHeight);

            content.saveGraphicsState();
            applyStyle(content, item, origin, boxWidth, boxHeight);
            final Color color = item.color == null ? Color.BLACK : parseColor(item.color);
            content.setNonStrokingColor(color);
            content.beginText();
            content.setFont(font, size);
            final Position position = item.position == null ? Position.topLeft : item.position;
            for(int i = 0; i < lines.length; i++) {
                final float offset = switch (position) {
                    case top, center, bottom -> (boxWidth - lineWidths[i]) / 2;
                    case topRight, right, bottomRight -> boxWidth - lineWidths[i];
                    default -> 0;
                };
                final float baseline = origin[1] + boxHeight - ascent - leading * i;
                content.setTextMatrix(Matrix.getTranslateInstance(origin[0] + offset, baseline));
                content.showText(lines[i]);
            }
            content.endText();
            content.restoreGraphicsState();
        }

        private static void drawImage(
                final PDPageContentStream content, final Item item, final PDImageXObject image,
                final float pageWidth, final float pageHeight) throws IOException {

            final float width;
            final float height;
            if(item.width != null && item.height != null) {
                width = item.width;
                height = item.height;
            } else if(item.width != null) {
                width = item.width;
                height = item.width * image.getHeight() / image.getWidth();
            } else if(item.height != null) {
                height = item.height;
                width = item.height * image.getWidth() / image.getHeight();
            } else {
                width = image.getWidth();
                height = image.getHeight();
            }
            final float[] origin = layout(item, width, height, pageWidth, pageHeight);
            content.saveGraphicsState();
            applyStyle(content, item, origin, width, height);
            content.drawImage(image, origin[0], origin[1], width, height);
            content.restoreGraphicsState();
        }

        /** Lower left corner of the item's box in the displayed page (bottom-up coordinates). */
        private static float[] layout(final Item item, final float width, final float height, final float pageWidth, final float pageHeight) {
            final Position position = item.position == null ? Position.topLeft : item.position;
            final float margin = item.margin == null ? 0F : item.margin;
            final float left = switch (position) {
                case topLeft, left, bottomLeft -> margin;
                case top, center, bottom -> (pageWidth - width) / 2;
                case topRight, right, bottomRight -> pageWidth - margin - width;
            };
            final float top = switch (position) {
                case topLeft, top, topRight -> margin;
                case left, center, right -> (pageHeight - height) / 2;
                case bottomLeft, bottom, bottomRight -> pageHeight - margin - height;
            };
            final float x = left + (item.x == null ? 0F : item.x);
            final float y = top + (item.y == null ? 0F : item.y);
            return new float[]{x, pageHeight - y - height};
        }

        private static void applyStyle(
                final PDPageContentStream content, final Item item, final float[] origin, final float width, final float height) throws IOException {

            if(item.opacity != null && item.opacity < 1F) {
                final PDExtendedGraphicsState state = new PDExtendedGraphicsState();
                state.setNonStrokingAlphaConstant(item.opacity);
                state.setStrokingAlphaConstant(item.opacity);
                content.setGraphicsStateParameters(state);
            }
            if(item.rotate != null && item.rotate % 360 != 0) {
                final float cx = origin[0] + width / 2;
                final float cy = origin[1] + height / 2;
                content.transform(Matrix.getTranslateInstance(cx, cy));
                content.transform(Matrix.getRotateInstance(Math.toRadians(item.rotate), 0, 0));
                content.transform(Matrix.getTranslateInstance(-cx, -cy));
            }
        }

        private static Color parseColor(final String color) {
            String hex = color.substring(1);
            if(hex.length() == 3) {
                hex = "" + hex.charAt(0) + hex.charAt(0) + hex.charAt(1) + hex.charAt(1) + hex.charAt(2) + hex.charAt(2);
            }
            return new Color(Integer.parseInt(hex, 16));
        }
    }

    /** Resolves a template of the config that uses only the template functions (e.g. {@code ${utils.secrets.get(...)}}). */
    public static String resolve(final String text) {
        final Map<String, Object> values = new HashMap<>();
        TemplateUtil.setFunctions(values);
        return TemplateUtil.executeStrictTemplateIfNeeded(text, values);
    }

    /** The non-null values of a field value that holds one resource or an array of them. */
    public static List<?> values(final Object value) {
        if(value == null) {
            return List.of();
        }
        if(value instanceof List<?> list) {
            return list.stream().filter(Objects::nonNull).toList();
        }
        return List.of(value);
    }

}
