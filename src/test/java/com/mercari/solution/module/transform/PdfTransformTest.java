package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.PDDocumentNameDictionary;
import org.apache.pdfbox.pdmodel.PDEmbeddedFilesNameTreeNode;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.common.filespecification.PDComplexFileSpecification;
import org.apache.pdfbox.pdmodel.common.filespecification.PDEmbeddedFile;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.StandardProtectionPolicy;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.pdfbox.pdmodel.graphics.image.JPEGFactory;
import org.apache.pdfbox.pdmodel.graphics.image.LosslessFactory;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionURI;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationLink;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDPageFitDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDDocumentOutline;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineItem;
import org.apache.pdfbox.pdmodel.interactive.form.PDAcroForm;
import org.apache.pdfbox.pdmodel.interactive.form.PDTextField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class PdfTransformTest {

    private static final String DIR = "target/pdf-transform-test";
    private static final String DOC = DIR + "/doc.pdf";
    private static final String ENCRYPTED = DIR + "/encrypted.pdf";
    private static final String BROKEN = DIR + "/broken.pdf";
    private static final String TILED = DIR + "/tiled.pdf";

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @BeforeAll
    public static void createFixtures() throws IOException {
        Files.createDirectories(Path.of(DIR));
        Files.write(Path.of(DOC), createDocument(null));
        Files.write(Path.of(ENCRYPTED), createDocument("secret"));
        Files.write(Path.of(BROKEN), "this is not a pdf".getBytes(StandardCharsets.UTF_8));
        Files.write(Path.of(TILED), createTiledDocument());
    }

    /**
     * Page 1: a picture stored as 4 tiles of 20 x 10 pixels (red | green over blue | yellow), each drawn at 40 x 20
     * points, and a separate jpeg. Page 2: a scan stored as layers, a white background and a stencil mask painted red
     * on its left half, both 50 x 50 pixels drawn at 100 x 100 points.
     */
    static byte[] createTiledDocument() throws IOException {
        try(final PDDocument document = new PDDocument()) {
            final PDPage page1 = new PDPage(PDRectangle.A4);
            final PDPage page2 = new PDPage(PDRectangle.A4);
            document.addPage(page1);
            document.addPage(page2);
            try(final PDPageContentStream cs = new PDPageContentStream(document, page1)) {
                cs.drawImage(LosslessFactory.createFromImage(document, image(20, 10, Color.RED)), 100, 620, 40, 20);
                cs.drawImage(LosslessFactory.createFromImage(document, image(20, 10, Color.GREEN)), 140, 620, 40, 20);
                cs.drawImage(LosslessFactory.createFromImage(document, image(20, 10, Color.BLUE)), 100, 600, 40, 20);
                cs.drawImage(LosslessFactory.createFromImage(document, image(20, 10, Color.YELLOW)), 140, 600, 40, 20);
                cs.drawImage(JPEGFactory.createFromImage(document, image(30, 30, Color.GRAY)), 400, 100, 60, 60);
            }
            final BufferedImage mask = new BufferedImage(50, 50, BufferedImage.TYPE_BYTE_BINARY);
            final Graphics2D g = mask.createGraphics();
            g.setColor(Color.WHITE);
            g.fillRect(0, 0, 50, 50);
            // a stencil mask paints its 0 samples
            g.setColor(Color.BLACK);
            g.fillRect(0, 0, 25, 50);
            g.dispose();
            final PDImageXObject stencil = LosslessFactory.createFromImage(document, mask);
            stencil.setStencil(true);
            try(final PDPageContentStream cs = new PDPageContentStream(document, page2)) {
                cs.drawImage(LosslessFactory.createFromImage(document, image(50, 50, Color.WHITE)), 100, 100, 100, 100);
                cs.setNonStrokingColor(Color.RED);
                cs.drawImage(stencil, 100, 100, 100, 100);
            }
            try(final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                document.save(bytes);
                return bytes.toByteArray();
            }
        }
    }

    /**
     * 3 pages: page 1 text + a png image + a URI link, page 2 text, page 3 only a jpeg image (no text).
     * An AcroForm text field, a 2-item outline, an embedded file, and document information.
     */
    static byte[] createDocument(final String password) throws IOException {
        try(final PDDocument document = new PDDocument()) {
            final PDType1Font font = new PDType1Font(Standard14Fonts.FontName.HELVETICA);
            final PDPage page1 = new PDPage(PDRectangle.A4);
            final PDPage page2 = new PDPage(PDRectangle.A4);
            final PDPage page3 = new PDPage(PDRectangle.A4);
            document.addPage(page1);
            document.addPage(page2);
            document.addPage(page3);

            final PDImageXObject png = LosslessFactory.createFromImage(document, image(40, 30, Color.RED));
            try(final PDPageContentStream cs = new PDPageContentStream(document, page1)) {
                text(cs, font, 72, 750, "Hello PDF page one");
                text(cs, font, 72, 730, "Second line here");
                cs.drawImage(png, 100, 500, 80, 60);
            }
            try(final PDPageContentStream cs = new PDPageContentStream(document, page2)) {
                text(cs, font, 72, 750, "Second page text");
            }
            final PDImageXObject jpeg = JPEGFactory.createFromImage(document, image(64, 48, Color.BLUE));
            try(final PDPageContentStream cs = new PDPageContentStream(document, page3)) {
                cs.drawImage(jpeg, 50, 50, 128, 96);
            }

            final PDAnnotationLink link = new PDAnnotationLink();
            link.setRectangle(new PDRectangle(72, 700, 100, 20));
            final PDActionURI uri = new PDActionURI();
            uri.setURI("https://example.com/doc");
            link.setAction(uri);
            page1.getAnnotations().add(link);

            final PDAcroForm form = new PDAcroForm(document);
            document.getDocumentCatalog().setAcroForm(form);
            final PDResources resources = new PDResources();
            resources.put(org.apache.pdfbox.cos.COSName.getPDFName("Helv"), font);
            form.setDefaultResources(resources);
            form.setDefaultAppearance("/Helv 0 Tf 0 g");
            final PDTextField field = new PDTextField(form);
            field.setPartialName("applicant");
            form.getFields().add(field);
            field.getWidgets().getFirst().setRectangle(new PDRectangle(72, 600, 200, 20));
            field.getWidgets().getFirst().setPage(page2);
            page2.getAnnotations().add(field.getWidgets().getFirst());
            field.setValue("Alice");

            final PDDocumentOutline outline = new PDDocumentOutline();
            document.getDocumentCatalog().setDocumentOutline(outline);
            outline.addLast(outlineItem("Chapter 1", page1));
            outline.addLast(outlineItem("Chapter 2", page2));

            final PDComplexFileSpecification spec = new PDComplexFileSpecification();
            spec.setFile("data.xml");
            final PDEmbeddedFile embedded = new PDEmbeddedFile(document, new ByteArrayInputStream("<a>1</a>".getBytes(StandardCharsets.UTF_8)));
            embedded.setSubtype("application/xml");
            spec.setEmbeddedFile(embedded);
            final PDEmbeddedFilesNameTreeNode tree = new PDEmbeddedFilesNameTreeNode();
            tree.setNames(Map.of("data.xml", spec));
            final PDDocumentNameDictionary names = new PDDocumentNameDictionary(document.getDocumentCatalog());
            names.setEmbeddedFiles(tree);
            document.getDocumentCatalog().setNames(names);

            final PDDocumentInformation info = new PDDocumentInformation();
            info.setTitle("Test Doc");
            info.setAuthor("Tester");
            document.setDocumentInformation(info);

            if(password != null) {
                final StandardProtectionPolicy policy = new StandardProtectionPolicy("owner", password, new AccessPermission());
                policy.setEncryptionKeyLength(128);
                document.protect(policy);
            }
            try(final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                document.save(bytes);
                return bytes.toByteArray();
            }
        }
    }

    private static void text(final PDPageContentStream cs, final PDType1Font font, final float x, final float y, final String text) throws IOException {
        cs.beginText();
        cs.setFont(font, 12);
        cs.newLineAtOffset(x, y);
        cs.showText(text);
        cs.endText();
    }

    private static PDOutlineItem outlineItem(final String title, final PDPage page) {
        final PDOutlineItem item = new PDOutlineItem();
        item.setTitle(title);
        final PDPageFitDestination destination = new PDPageFitDestination();
        destination.setPage(page);
        item.setDestination(destination);
        return item;
    }

    private static BufferedImage image(final int width, final int height, final Color color) {
        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        final Graphics2D g = image.createGraphics();
        g.setColor(color);
        g.fillRect(0, 0, width, height);
        g.dispose();
        return image;
    }

    private static String config(final String path, final String parameters) {
        return config(path, parameters, "");
    }

    private static String config(final String path, final String parameters, final String moduleOptions) {
        return """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - id: doc1
                          path: "%s"
                      schema:
                        fields:
                          - name: id
                            type: string
                          - name: path
                            type: string
                transforms:
                  - name: pdf
                    module: pdf
                    inputs: [create]
                %s    parameters:
                      field: path
                %s
                """.formatted(path, moduleOptions, parameters.indent(6));
    }

    private Map<String, MCollection> apply(final String yaml) {
        try {
            return MPipeline.apply(pipeline, Config.load(yaml));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<MElement> list(final Iterable<MElement> elements) {
        final List<MElement> list = new ArrayList<>();
        elements.forEach(list::add);
        return list;
    }

    private static byte[] bytes(final MElement element, final String field) {
        final ByteBuffer buffer = element.getAsBytes(field).duplicate();
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> structs(final MElement element, final String field) {
        return (List<Map<String, Object>>) element.getPrimitiveValue(field);
    }

    @Test
    public void testExtractDocument() {
        final Map<String, MCollection> outputs = apply(config(DOC, ""));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(1, list.size());
            final MElement e = list.getFirst();
            Assertions.assertEquals("doc1", e.getAsString("id"));
            Assertions.assertEquals(DOC, e.getAsString("path"));
            Assertions.assertEquals(3L, e.getAsLong("pageCount"));
            Assertions.assertEquals("Test Doc", e.getAsString("title"));
            Assertions.assertEquals("Tester", e.getAsString("author"));
            Assertions.assertFalse((Boolean) e.getPrimitiveValue("encrypted"));
            final String text = e.getAsString("text");
            Assertions.assertTrue(text.contains("Hello PDF page one"), text);
            Assertions.assertTrue(text.contains("Second line here"), text);
            Assertions.assertTrue(text.contains("Second page text"), text);
            Assertions.assertTrue(text.indexOf("Hello") < text.indexOf("Second page"), text);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testExtractDocumentStructures() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                include: [forms, outline, links, signatures]
                prefix: "pdf_"
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final MElement e = list(elements).getFirst();
            Assertions.assertNull(e.getPrimitiveValue("pdf_text"));
            final List<Map<String, Object>> forms = structs(e, "pdf_forms");
            Assertions.assertEquals(1, forms.size());
            Assertions.assertEquals("applicant", forms.getFirst().get("name"));
            Assertions.assertEquals("Tx", forms.getFirst().get("type"));
            Assertions.assertEquals("Alice", forms.getFirst().get("value"));
            final List<Map<String, Object>> outline = structs(e, "pdf_outline");
            Assertions.assertEquals(2, outline.size());
            Assertions.assertEquals("Chapter 2", outline.get(1).get("title"));
            Assertions.assertEquals(2L, outline.get(1).get("page"));
            Assertions.assertEquals(1L, outline.get(1).get("level"));
            final List<Map<String, Object>> links = structs(e, "pdf_links");
            Assertions.assertEquals(1, links.size());
            Assertions.assertEquals("https://example.com/doc", links.getFirst().get("uri"));
            Assertions.assertEquals(1L, links.getFirst().get("page"));
            // A4 height 841.89: rect y 700..720 -> top-left origin 121.89..141.89
            Assertions.assertEquals(841.89 - 720, (Double) links.getFirst().get("y0"), 0.01);
            Assertions.assertTrue(structs(e, "pdf_signatures").isEmpty());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testExtractPages() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                unit: page
                include: [text, lines, words]
                pages: "1-2"
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(2, list.size());
            final MElement page1 = list.stream().filter(e -> e.getAsLong("page") == 1L).findFirst().orElseThrow();
            Assertions.assertEquals(3L, page1.getAsLong("pageCount"));
            Assertions.assertEquals(595.27, page1.getAsDouble("width"), 0.01);
            Assertions.assertTrue(page1.getAsString("text").contains("Hello PDF page one"));
            Assertions.assertFalse(page1.getAsString("text").contains("Second page text"));
            final List<Map<String, Object>> lines = structs(page1, "lines");
            Assertions.assertEquals(2, lines.size());
            Assertions.assertEquals("Hello PDF page one", lines.getFirst().get("text"));
            Assertions.assertEquals(72.0, (Double) lines.getFirst().get("x0"), 0.5);
            Assertions.assertTrue((Double) lines.getFirst().get("y1") < (Double) lines.get(1).get("y1"));
            Assertions.assertEquals(12.0, (Double) lines.getFirst().get("fontSize"), 0.01);
            final List<Map<String, Object>> words = structs(page1, "words");
            Assertions.assertFalse(words.isEmpty());
            Assertions.assertEquals("Helvetica", words.getFirst().get("fontName"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testExtractImages() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                unit: image
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(2, list.size());
            final MElement png = list.stream().filter(e -> e.getAsLong("page") == 1L).findFirst().orElseThrow();
            Assertions.assertEquals("png", png.getAsString("format"));
            Assertions.assertEquals(40L, png.getAsLong("width"));
            Assertions.assertEquals(30L, png.getAsLong("height"));
            Assertions.assertEquals(100.0, png.getAsDouble("x0"), 0.01);
            Assertions.assertEquals(180.0, png.getAsDouble("x1"), 0.01);
            Assertions.assertEquals(841.89 - 560, png.getAsDouble("y0"), 0.01);
            assertImage(bytes(png, "content"), 40, 30);
            final MElement jpeg = list.stream().filter(e -> e.getAsLong("page") == 3L).findFirst().orElseThrow();
            Assertions.assertEquals("jpeg", jpeg.getAsString("format"));
            final byte[] jpegBytes = bytes(jpeg, "content");
            Assertions.assertEquals((byte) 0xFF, jpegBytes[0]);
            Assertions.assertEquals((byte) 0xD8, jpegBytes[1]);
            assertImage(jpegBytes, 64, 48);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testExtractImagesMergeAdjacent() {
        final Map<String, MCollection> outputs = apply(config(TILED, """
                unit: image
                images:
                  merge: adjacent
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(3, list.size());

            // the 4 tiles as one picture, at the pixels of the tiles
            final MElement tiles = list.stream().filter(e -> e.getAsLong("page") == 1L && e.getAsLong("parts") == 4L).findFirst().orElseThrow();
            Assertions.assertEquals(0L, tiles.getAsLong("index"));
            Assertions.assertEquals("png", tiles.getAsString("format"));
            Assertions.assertEquals(40L, tiles.getAsLong("width"));
            Assertions.assertEquals(20L, tiles.getAsLong("height"));
            Assertions.assertEquals(100.0, tiles.getAsDouble("x0"), 0.01);
            Assertions.assertEquals(180.0, tiles.getAsDouble("x1"), 0.01);
            Assertions.assertEquals(841.89 - 640, tiles.getAsDouble("y0"), 0.01);
            Assertions.assertEquals(841.89 - 600, tiles.getAsDouble("y1"), 0.01);
            final BufferedImage picture = read(bytes(tiles, "content"));
            Assertions.assertEquals(40, picture.getWidth());
            Assertions.assertEquals(20, picture.getHeight());
            Assertions.assertEquals(Color.RED.getRGB(), picture.getRGB(0, 0));
            Assertions.assertEquals(Color.RED.getRGB(), picture.getRGB(19, 9));
            Assertions.assertEquals(Color.GREEN.getRGB(), picture.getRGB(20, 0));
            Assertions.assertEquals(Color.BLUE.getRGB(), picture.getRGB(0, 10));
            Assertions.assertEquals(Color.YELLOW.getRGB(), picture.getRGB(39, 19));

            // a separate image is output as before
            final MElement jpeg = list.stream().filter(e -> e.getAsLong("page") == 1L && e.getAsLong("parts") == 1L).findFirst().orElseThrow();
            Assertions.assertEquals(1L, jpeg.getAsLong("index"));
            Assertions.assertEquals("jpeg", jpeg.getAsString("format"));
            Assertions.assertEquals(30L, jpeg.getAsLong("width"));

            // the layers of the scan: the stencil is painted in its fill color over the background
            final MElement scan = list.stream().filter(e -> e.getAsLong("page") == 2L).findFirst().orElseThrow();
            Assertions.assertEquals(2L, scan.getAsLong("parts"));
            Assertions.assertEquals(50L, scan.getAsLong("width"));
            final BufferedImage layers = read(bytes(scan, "content"));
            Assertions.assertEquals(Color.RED.getRGB(), layers.getRGB(5, 25));
            Assertions.assertEquals(Color.WHITE.getRGB(), layers.getRGB(45, 25));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testExtractImagesMergeGap() {
        // with a gap wider than the distance to the jpeg, the jpeg joins the tiles; the uncovered area is transparent
        final Map<String, MCollection> outputs = apply(config(TILED, """
                unit: image
                pages: "1"
                images:
                  merge: adjacent
                  mergeGap: 500
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(1, list.size());
            final MElement merged = list.getFirst();
            Assertions.assertEquals(5L, merged.getAsLong("parts"));
            Assertions.assertEquals(100.0, merged.getAsDouble("x0"), 0.01);
            Assertions.assertEquals(460.0, merged.getAsDouble("x1"), 0.01);
            final BufferedImage picture = read(bytes(merged, "content"));
            // 0.5 pixels per point, the resolution of all parts
            Assertions.assertEquals(180, picture.getWidth());
            Assertions.assertEquals(270, picture.getHeight());
            Assertions.assertEquals(Color.RED.getRGB(), picture.getRGB(0, 0));
            Assertions.assertEquals(0, picture.getRGB(100, 100) >>> 24);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testExtractImagesWithoutMerge() {
        final Map<String, MCollection> outputs = apply(config(TILED, """
                unit: image
                """));
        // the output schema is unchanged without merge
        Assertions.assertFalse(outputs.get("pdf").getSchema().hasField("parts"));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            // the 4 tiles, the jpeg and the 2 layers
            Assertions.assertEquals(7, list(elements).size());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testExtractAttachments() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                unit: attachment
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(1, list.size());
            final MElement e = list.getFirst();
            Assertions.assertEquals("data.xml", e.getAsString("name"));
            Assertions.assertEquals("application/xml", e.getAsString("mimeType"));
            Assertions.assertNull(e.getPrimitiveValue("page"));
            Assertions.assertEquals("<a>1</a>", new String(bytes(e, "content"), StandardCharsets.UTF_8));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testRenderNoText() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                operation: render
                include: [text]
                render:
                  dpi: 72
                  format: jpeg
                  color: gray
                  when: noText
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(3, list.size());
            for(final MElement e : list) {
                final long page = e.getAsLong("page");
                if(page == 3L) {
                    Assertions.assertFalse((Boolean) e.getPrimitiveValue("hasText"));
                    Assertions.assertEquals(595L, e.getAsLong("imageWidth"), 1);
                    Assertions.assertEquals(842L, e.getAsLong("imageHeight"), 1);
                    assertImage(bytes(e, "image"), e.getAsLong("imageWidth").intValue(), e.getAsLong("imageHeight").intValue());
                } else {
                    Assertions.assertTrue((Boolean) e.getPrimitiveValue("hasText"));
                    Assertions.assertNull(e.getPrimitiveValue("image"));
                    Assertions.assertFalse(e.getAsString("text").isBlank());
                }
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testRenderMaxPixels() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                operation: render
                pages: "3"
                render:
                  dpi: 300
                  maxPixels: 100000
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(1, list.size());
            final MElement e = list.getFirst();
            final long pixels = e.getAsLong("imageWidth") * e.getAsLong("imageHeight");
            Assertions.assertTrue(pixels <= 101_000, "pixels: " + pixels);
            Assertions.assertTrue(e.getAsDouble("dpi") < 300);
            final byte[] png = bytes(e, "image");
            Assertions.assertEquals((byte) 0x89, png[0]);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testSplit() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                operation: split
                split:
                  pagesPerChunk: 2
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(2, list.size());
            for(final MElement e : list) {
                Assertions.assertEquals(3L, e.getAsLong("pageCount"));
                try(final PDDocument chunk = Loader.loadPDF(bytes(e, "content"))) {
                    final long expected = e.getAsLong("endPage") - e.getAsLong("startPage") + 1;
                    Assertions.assertEquals(expected, chunk.getNumberOfPages());
                    if(e.getAsLong("startPage") == 1L) {
                        Assertions.assertEquals(2L, e.getAsLong("endPage"));
                        final String text = new org.apache.pdfbox.text.PDFTextStripper().getText(chunk);
                        Assertions.assertTrue(text.contains("Second page text"), text);
                    } else {
                        Assertions.assertEquals(3L, e.getAsLong("startPage"));
                    }
                } catch (final IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testSplitChunkHoldsOnlyItsPages() {
        // page 2 holds a form widget whose /P points to the source page: importing the page as is would
        // pull the source page tree, and so every page of the document, into the chunk
        final Map<String, MCollection> outputs = apply(config(DOC, """
                operation: split
                pages: "2"
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(1, list.size());
            try(final PDDocument chunk = Loader.loadPDF(bytes(list.getFirst(), "content"))) {
                Assertions.assertEquals(1, chunk.getNumberOfPages());
                Assertions.assertEquals(1, chunk.getDocument().getObjectsByType(org.apache.pdfbox.cos.COSName.PAGE).size());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testPathIsNotGlob() throws IOException {
        // on gs:// a '[' makes the path a glob ("glob[1].pdf" would match "glob1.pdf"); the local filesystem
        // prefers an existing file, so this only guards that the exact read handles such a name
        final String path = DIR + "/glob[1].pdf";
        Files.copy(Path.of(DOC), Path.of(path), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        final Map<String, MCollection> outputs = apply(config(path, ""));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(1, list.size());
            Assertions.assertEquals(3L, list.getFirst().getAsLong("pageCount"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testUnknownEnumValue() {
        final Exception e = Assertions.assertThrows(Exception.class, () -> apply(config(DOC, """
                operation: Render
                """)));
        Assertions.assertTrue(rootMessage(e).contains("parameters.operation"), rootMessage(e));
        final Exception r = Assertions.assertThrows(Exception.class, () -> apply(config(DOC, """
                operation: render
                render:
                  format: jpg
                """)));
        Assertions.assertTrue(rootMessage(r).contains("parameters.render.format"), rootMessage(r));
        final Exception m = Assertions.assertThrows(Exception.class, () -> apply(config(DOC, """
                unit: image
                images:
                  merge: tiles
                """)));
        Assertions.assertTrue(rootMessage(m).contains("parameters.images.merge"), rootMessage(m));
        final Exception g = Assertions.assertThrows(Exception.class, () -> apply(config(DOC, """
                unit: image
                images:
                  merge: adjacent
                  mergeGap: -1
                """)));
        Assertions.assertTrue(rootMessage(g).contains("parameters.images.mergeGap must not be negative"), rootMessage(g));
    }

    @Test
    public void testBytesFieldAndPassword() throws IOException {
        final String base64 = Base64.getEncoder().encodeToString(Files.readAllBytes(Path.of(ENCRYPTED)));
        final String yaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - id: doc1
                          data: "%s"
                      schema:
                        fields:
                          - name: id
                            type: string
                          - name: data
                            type: string
                transforms:
                  - name: decode
                    module: select
                    inputs: [create]
                    parameters:
                      select:
                        - name: id
                        - name: content
                          func: base64_decode
                          field: data
                          type: bytes
                  - name: pdf
                    module: pdf
                    inputs: [decode]
                    parameters:
                      field: content
                      password: secret
                      unit: page
                      pages: "2-"
                """.formatted(base64);
        final Map<String, MCollection> outputs = apply(yaml);
        Assertions.assertFalse(outputs.get("pdf").getSchema().hasField("content"));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(2, list.size());
            final MElement page2 = list.stream().filter(e -> e.getAsLong("page") == 2L).findFirst().orElseThrow();
            Assertions.assertEquals("doc1", page2.getAsString("id"));
            Assertions.assertTrue(page2.getAsString("text").contains("Second page text"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testBrokenPdfIsFailure() {
        final Map<String, MCollection> outputs = apply(config(BROKEN, "", "    failFast: false\n"));
        PAssert.that(outputs.get("pdf").getCollection()).empty();
        pipeline.run();
    }

    @Test
    public void testFieldCollision() {
        final String yaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - page: 1
                          path: "%s"
                      schema:
                        fields:
                          - name: page
                            type: int64
                          - name: path
                            type: string
                transforms:
                  - name: pdf
                    module: pdf
                    inputs: [create]
                    parameters:
                      field: path
                      unit: page
                """.formatted(DOC);
        final Exception e = Assertions.assertThrows(Exception.class, () -> apply(yaml));
        Assertions.assertTrue(rootMessage(e).contains("prefix"), rootMessage(e));
    }

    @Test
    public void testSelect() {
        final Map<String, MCollection> outputs = apply(config(DOC, """
                unit: page
                select:
                  - name: id
                  - name: page
                """));
        PAssert.that(outputs.get("pdf").getCollection()).satisfies(elements -> {
            final List<MElement> list = list(elements);
            Assertions.assertEquals(3, list.size());
            for(final MElement e : list) {
                Assertions.assertNull(e.getPrimitiveValue("text"));
                Assertions.assertEquals("doc1", e.getAsString("id"));
            }
            return null;
        });
        pipeline.run();
    }

    private static BufferedImage read(final byte[] bytes) {
        try {
            return ImageIO.read(new ByteArrayInputStream(bytes));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertImage(final byte[] bytes, final int width, final int height) {
        try {
            final BufferedImage image = ImageIO.read(new ByteArrayInputStream(bytes));
            Assertions.assertNotNull(image);
            Assertions.assertEquals(width, image.getWidth());
            Assertions.assertEquals(height, image.getHeight());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String rootMessage(final Throwable e) {
        final StringBuilder sb = new StringBuilder();
        Throwable t = e;
        while(t != null) {
            sb.append(t.getMessage()).append(" / ");
            t = t.getCause();
        }
        return sb.toString();
    }

}
