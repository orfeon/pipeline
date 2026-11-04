package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.pdf.PdfText;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.pdfbox.pdmodel.graphics.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAppearanceCharacteristicsDictionary;
import org.apache.pdfbox.pdmodel.interactive.form.PDAcroForm;
import org.apache.pdfbox.pdmodel.interactive.form.PDCheckBox;
import org.apache.pdfbox.pdmodel.interactive.form.PDTextField;
import org.apache.pdfbox.text.PDFTextStripper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** generate / edit operations of the pdf transform. */
public class PdfTransformWriteTest {

    private static final String DIR = "target/pdf-transform-write-test";
    private static final String DOC = DIR + "/doc.pdf";
    private static final String ENCRYPTED = DIR + "/encrypted.pdf";
    private static final String FORM = DIR + "/form.pdf";
    private static final String PNG = DIR + "/image.png";
    private static final String JPEG = DIR + "/image.jpg";
    private static final String FONT = DIR + "/LiberationSans-Regular.ttf";

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @BeforeAll
    public static void createFixtures() throws IOException {
        Files.createDirectories(Path.of(DIR));
        Files.write(Path.of(DOC), PdfTransformTest.createDocument(null));
        Files.write(Path.of(ENCRYPTED), PdfTransformTest.createDocument("secret"));
        Files.write(Path.of(FORM), createForm());
        ImageIO.write(image(200, 100, Color.RED), "png", Path.of(PNG).toFile());
        ImageIO.write(image(60, 120, Color.BLUE), "jpg", Path.of(JPEG).toFile());
        // PDFBox ships a TrueType font for its font fallback
        try(final InputStream is = PDFont.class.getResourceAsStream("/org/apache/pdfbox/resources/ttf/LiberationSans-Regular.ttf")) {
            Files.write(Path.of(FONT), is.readAllBytes());
        }
    }

    /** One page with the text fields "name" and "date" and the check box "agree". */
    private static byte[] createForm() throws IOException {
        try(final PDDocument document = new PDDocument()) {
            final PDPage page = new PDPage(PDRectangle.A4);
            document.addPage(page);
            final PDAcroForm form = new PDAcroForm(document);
            document.getDocumentCatalog().setAcroForm(form);
            final PDResources resources = new PDResources();
            resources.put(COSName.getPDFName("Helv"), new PDType1Font(Standard14Fonts.FontName.HELVETICA));
            resources.put(COSName.getPDFName("ZaDb"), new PDType1Font(Standard14Fonts.FontName.ZAPF_DINGBATS));
            form.setDefaultResources(resources);
            form.setDefaultAppearance("/Helv 12 Tf 0 g");
            float y = 700;
            for(final String name : List.of("name", "date")) {
                final PDTextField field = new PDTextField(form);
                field.setPartialName(name);
                form.getFields().add(field);
                field.getWidgets().getFirst().setRectangle(new PDRectangle(72, y, 200, 20));
                field.getWidgets().getFirst().setPage(page);
                page.getAnnotations().add(field.getWidgets().getFirst());
                y -= 40;
            }
            final PDCheckBox checkBox = new PDCheckBox(form);
            checkBox.setPartialName("agree");
            form.getFields().add(checkBox);
            final var widget = checkBox.getWidgets().getFirst();
            widget.setRectangle(new PDRectangle(72, y, 20, 20));
            widget.setPage(page);
            final PDAppearanceCharacteristicsDictionary characteristics = new PDAppearanceCharacteristicsDictionary(new org.apache.pdfbox.cos.COSDictionary());
            characteristics.setNormalCaption("4");
            widget.setAppearanceCharacteristics(characteristics);
            // an "on" appearance named Yes, so that check() has a state to switch to
            final var appearance = new org.apache.pdfbox.pdmodel.interactive.annotation.PDAppearanceDictionary();
            final org.apache.pdfbox.cos.COSDictionary normal = new org.apache.pdfbox.cos.COSDictionary();
            normal.setItem(COSName.getPDFName("Yes"), new org.apache.pdfbox.pdmodel.interactive.annotation.PDAppearanceStream(document).getCOSObject());
            normal.setItem(COSName.Off, new org.apache.pdfbox.pdmodel.interactive.annotation.PDAppearanceStream(document).getCOSObject());
            appearance.getCOSObject().setItem(COSName.N, normal);
            widget.setAppearance(appearance);
            widget.setAppearanceState("Off");
            page.getAnnotations().add(widget);
            try(final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                document.save(bytes);
                return bytes.toByteArray();
            }
        }
    }

    private static BufferedImage image(final int width, final int height, final Color color) {
        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        final Graphics2D g = image.createGraphics();
        g.setColor(color);
        g.fillRect(0, 0, width, height);
        g.dispose();
        return image;
    }

    /** A create source with one element {id, name, agree, path, paths, images} feeding a pdf transform. */
    private static String config(final String parameters) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - id: doc1
                          name: Alice
                          agree: true
                          path: "%s"
                          paths: ["%s", "%s"]
                          images: ["%s", "%s"]
                      schema:
                        fields:
                          - { name: id, type: string }
                          - { name: name, type: string }
                          - { name: agree, type: boolean }
                          - { name: path, type: string }
                          - { name: paths, type: string, mode: repeated }
                          - { name: images, type: string, mode: repeated }
                transforms:
                  - name: pdf
                    module: pdf
                    inputs: [create]
                    failFast: false
                    parameters:
                %s
                """.formatted(DOC, DOC, DOC, PNG, JPEG, parameters.indent(6));
    }

    private Map<String, MCollection> apply(final String yaml) {
        try {
            return MPipeline.apply(pipeline, Config.load(yaml));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static MElement single(final Iterable<MElement> elements) {
        final List<MElement> list = new ArrayList<>();
        elements.forEach(list::add);
        Assertions.assertEquals(1, list.size());
        return list.getFirst();
    }

    private static byte[] content(final MElement element) {
        final ByteBuffer buffer = element.getAsBytes("content").duplicate();
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static String text(final PDDocument document) throws IOException {
        return new PDFTextStripper().getText(document);
    }

    private static String rootMessage(final Throwable e) {
        Throwable t = e;
        final StringBuilder sb = new StringBuilder();
        while(t != null) {
            sb.append(t.getMessage()).append(" / ");
            t = t.getCause();
        }
        return sb.toString();
    }

    @Test
    public void testGenerateFormWithValues() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: generate
                generate:
                  type: form
                  template: "%s"
                  values:
                    name: "${name} (${id})"
                    date: "2026-11-04"
                  flatten: true
                steps:
                  - type: metadata
                    title: "Form of ${name}"
                """.formatted(FORM)));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final MElement element = single(elements);
            Assertions.assertEquals(1L, element.getAsLong("pageCount"));
            Assertions.assertEquals("doc1", element.getAsString("id"));
            try(final PDDocument document = Loader.loadPDF(content(element))) {
                final PDAcroForm form = document.getDocumentCatalog().getAcroForm();
                Assertions.assertTrue(form == null || form.getFields().isEmpty(), "flattened");
                final String text = text(document);
                Assertions.assertTrue(text.contains("Alice (doc1)"), text);
                Assertions.assertTrue(text.contains("2026-11-04"), text);
                Assertions.assertEquals("Form of Alice", document.getDocumentInformation().getTitle());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testGenerateFormFromSameNamedFields() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: generate
                generate:
                  type: form
                  template: "%s"
                """.formatted(FORM)));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            try(final PDDocument document = Loader.loadPDF(content(single(elements)))) {
                final PDAcroForm form = document.getDocumentCatalog().getAcroForm();
                Assertions.assertEquals("Alice", form.getField("name").getValueAsString());
                Assertions.assertEquals("", form.getField("date").getValueAsString());
                Assertions.assertTrue(((PDCheckBox) form.getField("agree")).isChecked());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testGenerateFormUnknownFieldFailsSetup() {
        apply(config("""
                operation: generate
                generate:
                  type: form
                  template: "%s"
                  values:
                    nmae: "${name}"
                """.formatted(FORM)));
        final Exception e = Assertions.assertThrows(Exception.class, pipeline::run);
        Assertions.assertTrue(rootMessage(e).contains("nmae"), rootMessage(e));
    }

    @Test
    public void testGenerateImages() {
        final String yaml = config("""
                operation: generate
                field: images
                generate:
                  type: images
                  pageSize: A4
                  margin: 36
                """);
        final Map<String, MCollection> outputs = apply(yaml);
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final MElement element = single(elements);
            Assertions.assertEquals(2L, element.getAsLong("pageCount"));
            try(final PDDocument document = Loader.loadPDF(content(element))) {
                // the wide png goes on a landscape page, the tall jpeg on a portrait page
                final PDRectangle first = document.getPage(0).getMediaBox();
                Assertions.assertEquals(PDRectangle.A4.getHeight(), first.getWidth(), 0.01);
                final PDRectangle second = document.getPage(1).getMediaBox();
                Assertions.assertEquals(PDRectangle.A4.getWidth(), second.getWidth(), 0.01);
                final PDImageXObject jpeg = image(document.getPage(1));
                Assertions.assertEquals("jpg", jpeg.getSuffix());
                Assertions.assertEquals(60, jpeg.getWidth());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testGenerateImagesAtImageSize() {
        final String yaml = config("""
                operation: generate
                field: path
                generate:
                  type: images
                  dpi: 72
                """).replace("path: \"" + DOC + "\"", "path: \"" + PNG + "\"");
        final Map<String, MCollection> outputs = apply(yaml);
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            try(final PDDocument document = Loader.loadPDF(content(single(elements)))) {
                final PDRectangle box = document.getPage(0).getMediaBox();
                Assertions.assertEquals(200, box.getWidth(), 0.01);
                Assertions.assertEquals(100, box.getHeight(), 0.01);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    private static PDImageXObject image(final PDPage page) throws IOException {
        for(final COSName name : page.getResources().getXObjectNames()) {
            final PDXObject xObject = page.getResources().getXObject(name);
            if(xObject instanceof PDImageXObject image) {
                return image;
            }
        }
        throw new AssertionError("no image on the page");
    }

    @Test
    public void testEditMergePagesRotate() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: edit
                field: paths
                steps:
                  - type: merge
                    field: path
                  - type: pages
                    pages: "4,9,1"
                  - type: rotate
                    angle: 90
                    pages: "1"
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final MElement element = single(elements);
            Assertions.assertEquals(3L, element.getAsLong("pageCount"));
            // a string path array is not dropped
            Assertions.assertNotNull(element.getPrimitiveValue("paths"));
            try(final PDDocument document = Loader.loadPDF(content(element))) {
                Assertions.assertEquals(3, document.getNumberOfPages());
                Assertions.assertEquals(90, document.getPage(0).getRotation());
                Assertions.assertEquals(0, document.getPage(1).getRotation());
                final PDFTextStripper stripper = new PDFTextStripper();
                stripper.setStartPage(1);
                stripper.setEndPage(1);
                // the rotated page shows its text vertically, one character per line
                Assertions.assertTrue(stripper.getText(document).replaceAll("\\s", "").contains("HelloPDFpageone"));
                stripper.setStartPage(2);
                stripper.setEndPage(2);
                // page 9 of the merged 9 pages is an image-only page
                Assertions.assertTrue(stripper.getText(document).isBlank());
                stripper.setStartPage(3);
                stripper.setEndPage(3);
                Assertions.assertTrue(stripper.getText(document).contains("Hello PDF page one"));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEditOverlay() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: edit
                field: path
                fonts:
                  - name: liberation
                    path: "%s"
                steps:
                  - type: rotate
                    angle: 90
                    pages: "2"
                  - type: overlay
                    items:
                      - text: "Page ${page} of ${pageCount} ${id}"
                        position: bottom
                        margin: 20
                        fontSize: 10
                      - text: "TOPLEFT"
                        font: liberation
                        margin: 30
                      - text: "DRAFT"
                        position: center
                        fontSize: 60
                        rotate: 45
                        opacity: 0.3
                        color: "#ff0000"
                      - image: "%s"
                        position: topRight
                        width: 50
                        margin: 10
                """.formatted(FONT, PNG)));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            try(final PDDocument document = Loader.loadPDF(content(single(elements)))) {
                final String text = text(document);
                Assertions.assertTrue(text.contains("Page 1 of 3 doc1"), text);
                Assertions.assertTrue(text.contains("Page 3 of 3 doc1"), text);
                Assertions.assertTrue(text.contains("DRAFT"), text);
                final PdfText.Options options = new PdfText.Options();
                options.setDefaults();
                final PdfText pdfText = new PdfText(options);
                // on the rotated page 2 the text is placed in the page as displayed (842 x 595)
                for(final int page : List.of(1, 2)) {
                    final List<PdfText.Word> words = pdfText.extract(document, page, true).words();
                    final PdfText.Word word = words.stream()
                            .filter(w -> w.text().equals("TOPLEFT"))
                            .findFirst().orElseThrow(() -> new AssertionError("page " + page + ": " + words));
                    Assertions.assertEquals(30, word.x0(), 1.0, "page " + page);
                    // the box top is at the margin; the glyphs start below it, by the ascent above the cap height
                    Assertions.assertTrue(word.y0() >= 30 && word.y0() < 36, "page " + page + ": " + word);
                    Assertions.assertTrue(word.fontName().contains("LiberationSans"), word.fontName());
                }
                final List<PdfText.Word> words = pdfText.extract(document, 2, true).words();
                final PdfText.Word footer = words.stream()
                        .filter(w -> w.text().startsWith("Page 2 of 3"))
                        .findFirst().orElseThrow(() -> new AssertionError(words.toString()));
                Assertions.assertTrue(footer.y1() > 595 - 25 && footer.y1() <= 595 - 18, "footer at the displayed bottom: " + footer);
                Assertions.assertEquals(842 / 2.0, (footer.x0() + footer.x1()) / 2, 1.0, "footer centered: " + footer);
                Assertions.assertNotNull(image(document.getPage(0)));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testGenerateThenEditBytes() {
        // the generated content (bytes) is the edit input and dropped from its output; pageCount needs a prefix
        final String yaml = config("""
                operation: generate
                field: images
                generate:
                  type: images
                """) + """
                  - name: edit
                    module: pdf
                    inputs: [pdf]
                    parameters:
                      operation: edit
                      field: content
                      prefix: edited_
                      steps:
                        - type: merge
                          field: path
                        - type: overlay
                          pages: "1"
                          items:
                            - text: "${id}"
                """;
        final Map<String, MCollection> outputs = apply(yaml);
        Assertions.assertFalse(outputs.get("edit").getSchema().hasField("content"));
        Assertions.assertTrue(outputs.get("edit").getSchema().hasField("pageCount"));
        PAssert.that(outputs.get("edit").getCollection()).satisfies(elements -> {
            final MElement element = single(elements);
            Assertions.assertEquals(2L, element.getAsLong("pageCount"));
            Assertions.assertEquals(5L, element.getAsLong("edited_pageCount"));
            final ByteBuffer buffer = element.getAsBytes("edited_content").duplicate();
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            try(final PDDocument document = Loader.loadPDF(bytes)) {
                final PDFTextStripper stripper = new PDFTextStripper();
                stripper.setEndPage(1);
                Assertions.assertTrue(stripper.getText(document).contains("doc1"));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEditEncrypt() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: edit
                field: path
                steps:
                  - type: encrypt
                    userPassword: "${id}"
                    ownerPassword: owner
                    permissions:
                      print: false
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final byte[] bytes = content(single(elements));
            Assertions.assertThrows(InvalidPasswordException.class, () -> Loader.loadPDF(bytes).close());
            try(final PDDocument document = Loader.loadPDF(bytes, "doc1")) {
                Assertions.assertTrue(document.isEncrypted());
                Assertions.assertFalse(document.getCurrentAccessPermission().canPrint());
                Assertions.assertTrue(document.getCurrentAccessPermission().canExtractContent());
                Assertions.assertEquals(256, document.getEncryption().getLength());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEditDecrypt() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: edit
                field: path
                password: secret
                steps:
                  - type: decrypt
                """).replace("path: \"" + DOC + "\"", "path: \"" + ENCRYPTED + "\""));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            try(final PDDocument document = Loader.loadPDF(content(single(elements)))) {
                Assertions.assertFalse(document.isEncrypted());
                Assertions.assertEquals(3, document.getNumberOfPages());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEditEncryptedNeedsSecurityStep() {
        // without decrypt / encrypt the pdf cannot be saved: the record fails
        final Map<String, MCollection> outputs = apply(config("""
                operation: edit
                field: path
                password: secret
                steps:
                  - type: rotate
                    angle: 180
                """).replace("path: \"" + DOC + "\"", "path: \"" + ENCRYPTED + "\""));
        PAssert.that(outputs.get("pdf").getCollection()).empty();
        pipeline.run();
    }

    @Test
    public void testFormFont() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: generate
                fonts:
                  - name: liberation
                    path: "%s"
                generate:
                  type: form
                  template: "%s"
                  values:
                    name: "${name}"
                  font: liberation
                """.formatted(FONT, FORM)));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            try(final PDDocument document = Loader.loadPDF(content(single(elements)))) {
                final PDTextField field = (PDTextField) document.getDocumentCatalog().getAcroForm().getField("name");
                Assertions.assertEquals("Alice", field.getValueAsString());
                final String da = field.getDefaultAppearance();
                final COSName fontName = COSName.getPDFName(da.substring(1, da.indexOf(' ')));
                final PDFont font = document.getDocumentCatalog().getAcroForm().getDefaultResources().getFont(fontName);
                Assertions.assertTrue(font.getName().contains("LiberationSans"), font.getName());
                // an editable field keeps the whole font
                Assertions.assertFalse(font.getName().contains("+"), font.getName());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testTextOutsideStandardFontIsFailure() {
        final Map<String, MCollection> outputs = apply(config("""
                operation: edit
                field: path
                steps:
                  - type: overlay
                    items:
                      - text: "あ"
                """));
        PAssert.that(outputs.get("pdf").getCollection()).empty();
        pipeline.run();
    }

    @Test
    public void testValidation() {
        final Map<String, String> cases = Map.of(
                "parameters.steps must not be empty", """
                        operation: edit
                        field: path
                        """,
                "steps[0].type", """
                        operation: edit
                        field: path
                        steps:
                          - type: stamp
                        """,
                "angle must be a multiple of 90", """
                        operation: edit
                        field: path
                        steps:
                          - type: rotate
                            angle: 45
                        """,
                "needs exactly one of text, image, imageField", """
                        operation: edit
                        field: path
                        steps:
                          - type: overlay
                            items:
                              - text: a
                                image: b.png
                        """,
                "is not used by generate.type form", """
                        operation: generate
                        field: path
                        generate:
                          type: form
                          template: t.pdf
                        """,
                "set pages in the steps", """
                        operation: edit
                        field: path
                        pages: "1"
                        steps:
                          - type: decrypt
                        """,
                "nor in parameters.fonts", """
                        operation: edit
                        field: path
                        steps:
                          - type: overlay
                            items:
                              - text: a
                                font: Gothic
                        """,
                "steps is only for operations generate and edit", """
                        field: path
                        steps:
                          - type: decrypt
                        """);
        for(final Map.Entry<String, String> entry : cases.entrySet()) {
            final Exception e = Assertions.assertThrows(Exception.class, () -> apply(config(entry.getValue())), entry.getKey());
            Assertions.assertTrue(rootMessage(e).contains(entry.getKey()), entry.getKey() + " / " + rootMessage(e));
        }
    }

}
