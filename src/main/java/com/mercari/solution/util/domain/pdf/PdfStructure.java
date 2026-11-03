package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.PDDocumentNameDictionary;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDNameTreeNode;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.common.filespecification.PDComplexFileSpecification;
import org.apache.pdfbox.pdmodel.common.filespecification.PDEmbeddedFile;
import org.apache.pdfbox.pdmodel.common.filespecification.PDFileSpecification;
import org.apache.pdfbox.pdmodel.interactive.action.PDAction;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionGoTo;
import org.apache.pdfbox.pdmodel.interactive.action.PDActionURI;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotation;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationFileAttachment;
import org.apache.pdfbox.pdmodel.interactive.annotation.PDAnnotationLink;
import org.apache.pdfbox.pdmodel.interactive.digitalsignature.PDSignature;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDPageDestination;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineItem;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineNode;
import org.apache.pdfbox.pdmodel.interactive.form.PDAcroForm;
import org.apache.pdfbox.pdmodel.interactive.form.PDField;
import org.apache.pdfbox.pdmodel.interactive.form.PDTerminalField;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Document-level structures of a PDF as primitive values (timestamps in epoch micros, boxes in points
 * with the origin at the top-left of the page's crop box).
 */
public class PdfStructure {

    private static final int MAX_OUTLINE_ITEMS = 10_000;

    private PdfStructure() {}

    public static Map<String, Object> metadata(final PDDocument document, final long fileSize) {
        final Map<String, Object> values = new HashMap<>();
        final PDDocumentInformation info = document.getDocumentInformation();
        values.put("fileSize", fileSize);
        values.put("version", Float.toString(document.getVersion()));
        values.put("encrypted", document.isEncrypted());
        values.put("title", info.getTitle());
        values.put("author", info.getAuthor());
        values.put("subject", info.getSubject());
        values.put("keywords", info.getKeywords());
        values.put("creator", info.getCreator());
        values.put("producer", info.getProducer());
        values.put("creationDate", micros(info.getCreationDate()));
        values.put("modificationDate", micros(info.getModificationDate()));
        values.put("trapped", info.getTrapped());
        return values;
    }

    public static Map<String, Object> pageValues(final PDPage page, final int pageNumber) {
        final Map<String, Object> values = new HashMap<>();
        final PDRectangle box = page.getCropBox();
        values.put("page", (long) pageNumber);
        values.put("width", (double) box.getWidth());
        values.put("height", (double) box.getHeight());
        values.put("rotation", (long) page.getRotation());
        return values;
    }

    public static List<Map<String, Object>> forms(final PDDocument document) {
        final List<Map<String, Object>> values = new ArrayList<>();
        final PDAcroForm form = document.getDocumentCatalog().getAcroForm(null);
        if(form == null) {
            return values;
        }
        for(final PDField field : form.getFieldTree()) {
            if(!(field instanceof PDTerminalField)) {
                continue;
            }
            final Map<String, Object> value = new HashMap<>();
            value.put("name", field.getFullyQualifiedName());
            value.put("type", field.getFieldType());
            value.put("value", field.getValueAsString());
            values.add(value);
        }
        return values;
    }

    public static List<Map<String, Object>> outline(final PDDocument document) throws IOException {
        final List<Map<String, Object>> values = new ArrayList<>();
        final PDOutlineNode root = document.getDocumentCatalog().getDocumentOutline();
        if(root != null) {
            outline(document, root, 1, values);
        }
        return values;
    }

    private static void outline(final PDDocument document, final PDOutlineNode node, final int level, final List<Map<String, Object>> values) throws IOException {
        for(final PDOutlineItem item : node.children()) {
            if(values.size() >= MAX_OUTLINE_ITEMS) {
                return;
            }
            final Map<String, Object> value = new HashMap<>();
            value.put("level", (long) level);
            value.put("title", item.getTitle());
            value.put("page", pageNumber(document, item.findDestinationPage(document)));
            values.add(value);
            outline(document, item, level + 1, values);
        }
    }

    public static List<Map<String, Object>> links(final PDDocument document, final PDPage page, final int pageNumber) throws IOException {
        final List<Map<String, Object>> values = new ArrayList<>();
        for(final PDAnnotation annotation : page.getAnnotations()) {
            if(!(annotation instanceof PDAnnotationLink link)) {
                continue;
            }
            String uri = null;
            Long targetPage = null;
            final PDAction action = link.getAction();
            if(action instanceof PDActionURI actionURI) {
                uri = actionURI.getURI();
            } else if(action instanceof PDActionGoTo goTo) {
                targetPage = targetPage(document, goTo.getDestination());
            } else if(action == null) {
                targetPage = targetPage(document, link.getDestination());
            }
            if(uri == null && targetPage == null) {
                continue;
            }
            final Map<String, Object> value = box(page, link.getRectangle());
            value.put("page", (long) pageNumber);
            value.put("uri", uri);
            value.put("targetPage", targetPage);
            values.add(value);
        }
        return values;
    }

    public static List<Map<String, Object>> signatures(final PDDocument document) {
        final List<Map<String, Object>> values = new ArrayList<>();
        for(final PDSignature signature : document.getSignatureDictionaries()) {
            final Map<String, Object> value = new HashMap<>();
            value.put("name", signature.getName());
            value.put("signDate", micros(signature.getSignDate()));
            value.put("reason", signature.getReason());
            value.put("location", signature.getLocation());
            value.put("contactInfo", signature.getContactInfo());
            value.put("subFilter", signature.getSubFilter());
            values.add(value);
        }
        return values;
    }

    /** Embedded files of the document's name tree (page = null) and of file attachment annotations on the selected pages. */
    public static List<Map<String, Object>> attachments(final PDDocument document, final List<Integer> pages) throws IOException {
        final List<Map<String, Object>> values = new ArrayList<>();
        final PDDocumentCatalog catalog = document.getDocumentCatalog();
        final PDDocumentNameDictionary names = catalog.getNames();
        if(names != null && names.getEmbeddedFiles() != null) {
            collectEmbeddedFiles(names.getEmbeddedFiles(), values);
        }
        for(final int pageNumber : pages) {
            final PDPage page = document.getPage(pageNumber - 1);
            for(final PDAnnotation annotation : page.getAnnotations()) {
                if(annotation instanceof PDAnnotationFileAttachment attachment) {
                    final Map<String, Object> value = attachment(attachment.getFile(), null);
                    if(value != null) {
                        value.put("page", (long) pageNumber);
                        values.add(value);
                    }
                }
            }
        }
        return values;
    }

    private static void collectEmbeddedFiles(final PDNameTreeNode<PDComplexFileSpecification> node, final List<Map<String, Object>> values) throws IOException {
        final Map<String, PDComplexFileSpecification> entries = node.getNames();
        if(entries != null) {
            for(final Map.Entry<String, PDComplexFileSpecification> entry : entries.entrySet()) {
                final Map<String, Object> value = attachment(entry.getValue(), entry.getKey());
                if(value != null) {
                    value.put("page", null);
                    values.add(value);
                }
            }
        }
        final List<PDNameTreeNode<PDComplexFileSpecification>> kids = node.getKids();
        if(kids != null) {
            for(final PDNameTreeNode<PDComplexFileSpecification> kid : kids) {
                collectEmbeddedFiles(kid, values);
            }
        }
    }

    private static Map<String, Object> attachment(final PDFileSpecification specification, final String key) throws IOException {
        if(!(specification instanceof PDComplexFileSpecification complex)) {
            return null;
        }
        PDEmbeddedFile file = complex.getEmbeddedFileUnicode();
        if(file == null) file = complex.getEmbeddedFileDos();
        if(file == null) file = complex.getEmbeddedFileMac();
        if(file == null) file = complex.getEmbeddedFileUnix();
        if(file == null) file = complex.getEmbeddedFile();
        if(file == null) {
            return null;
        }
        final byte[] bytes = file.toByteArray();
        final Map<String, Object> value = new HashMap<>();
        String name = complex.getFilename();
        if(name == null) {
            name = key;
        }
        value.put("name", name);
        value.put("description", complex.getFileDescription());
        value.put("mimeType", file.getSubtype());
        value.put("size", (long) bytes.length);
        value.put("creationDate", micros(file.getCreationDate()));
        value.put("modificationDate", micros(file.getModDate()));
        value.put("content", ByteBuffer.wrap(bytes));
        return value;
    }

    /** A box in PDF user space converted to top-left-origin crop-box coordinates. */
    static Map<String, Object> box(final PDPage page, final PDRectangle rect) {
        final PDRectangle crop = page.getCropBox();
        final Map<String, Object> value = new HashMap<>();
        if(rect == null) {
            value.put("x0", null);
            value.put("y0", null);
            value.put("x1", null);
            value.put("y1", null);
            return value;
        }
        value.put("x0", (double) (rect.getLowerLeftX() - crop.getLowerLeftX()));
        value.put("x1", (double) (rect.getUpperRightX() - crop.getLowerLeftX()));
        value.put("y0", (double) (crop.getUpperRightY() - rect.getUpperRightY()));
        value.put("y1", (double) (crop.getUpperRightY() - rect.getLowerLeftY()));
        return value;
    }

    private static Long targetPage(final PDDocument document, final PDDestination destination) throws IOException {
        if(destination instanceof PDPageDestination pageDestination) {
            if(pageDestination.getPage() != null) {
                return pageNumber(document, pageDestination.getPage());
            }
            final int index = pageDestination.getPageNumber();
            return index >= 0 ? (long) index + 1 : null;
        }
        if(destination != null) {
            // named destination
            final PDPageDestination resolved = document.getDocumentCatalog().findNamedDestinationPage(
                    (org.apache.pdfbox.pdmodel.interactive.documentnavigation.destination.PDNamedDestination) destination);
            return resolved == null ? null : targetPage(document, resolved);
        }
        return null;
    }

    private static Long pageNumber(final PDDocument document, final PDPage page) {
        if(page == null) {
            return null;
        }
        final int index = document.getPages().indexOf(page);
        return index < 0 ? null : (long) index + 1;
    }

    static Long micros(final Calendar calendar) {
        return calendar == null ? null : calendar.toInstant().toEpochMilli() * 1000L;
    }

}
