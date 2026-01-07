package com.mercari.solution.util.domain.file;

import com.mercari.solution.module.Schema;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class PdfUtil implements Serializable {

    private static final String FIELD_NAME_CONTENT = "Content";
    private static final String FIELD_NAME_FILESIZE = "FileByteSize";
    private static final String FIELD_NAME_PAGE = "Page";
    private static final String FIELD_NAME_VERSION = "Version";
    private static final String FIELD_NAME_ENCRYPTED = "Encrypted";
    private static final String FIELD_NAME_TITLE = "Title";
    private static final String FIELD_NAME_AUTHOR = "Author";
    private static final String FIELD_NAME_SUBJECT = "Subject";
    private static final String FIELD_NAME_KEYWORDS = "Keywords";
    private static final String FIELD_NAME_CREATOR = "Creator";
    private static final String FIELD_NAME_PRODUCER = "Producer";
    private static final String FIELD_NAME_CREATIONDATE = "CreationDate";
    private static final String FIELD_NAME_MODIFICATIONDATE = "ModificationDate";
    private static final String FIELD_NAME_TRAPPED = "Trapped";
    private static final String FIELD_NAME_FAILED = "Failed";
    private static final String FIELD_NAME_ERROR_PAGE = "ErrorPageCount";
    private static final String FIELD_NAME_ERROR_MESSAGE = "ErrorMessage";

    public static Schema createPdfSchema(String prefix) {
        return Schema.builder()
                .withField(prefix + FIELD_NAME_CONTENT, Schema.FieldType.STRING.withNullable(false))
                .withField(prefix + FIELD_NAME_FILESIZE, Schema.FieldType.INT64.withNullable(true))
                .withField(prefix + FIELD_NAME_PAGE, Schema.FieldType.INT64.withNullable(true))
                .withField(prefix + FIELD_NAME_VERSION, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_ENCRYPTED, Schema.FieldType.BOOLEAN.withNullable(true))
                .withField(prefix + FIELD_NAME_TITLE, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_AUTHOR, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_SUBJECT, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_KEYWORDS, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_CREATOR, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_PRODUCER, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_CREATIONDATE, Schema.FieldType.TIMESTAMP.withNullable(true))
                .withField(prefix + FIELD_NAME_MODIFICATIONDATE, Schema.FieldType.TIMESTAMP.withNullable(true))
                .withField(prefix + FIELD_NAME_TRAPPED, Schema.FieldType.STRING.withNullable(true))
                .withField(prefix + FIELD_NAME_FAILED, Schema.FieldType.BOOLEAN.withNullable(false))
                .withField(prefix + FIELD_NAME_ERROR_PAGE, Schema.FieldType.INT64.withNullable(false))
                .withField(prefix + FIELD_NAME_ERROR_MESSAGE, Schema.FieldType.STRING.withNullable(true))
                .build();
    }

    public static String extractText(final PDDocument document) {
        final PDFTextStripper stripper = new PDFTextStripper();
        stripper.setStartPage(1);
        stripper.setEndPage(document.getNumberOfPages());
        stripper.setSortByPosition(true);
        stripper.setLineSeparator("");
        stripper.setAddMoreFormatting(true);
        stripper.setSuppressDuplicateOverlappingText(true);
        stripper.setShouldSeparateByBeads(true);
        try {
            return stripper.getText(document);
        } catch (Exception e) {
            throw new RuntimeException("failed extract text", e);
        }
    }

    public static String extractText(
            final PDDocument document,
            int page) {

        final PDFTextStripper stripper = new PDFTextStripper();
        stripper.setStartPage(page);
        stripper.setEndPage(page);
        stripper.setSortByPosition(true);
        stripper.setLineSeparator("");
        stripper.setAddMoreFormatting(true);
        stripper.setSuppressDuplicateOverlappingText(true);
        stripper.setShouldSeparateByBeads(true);
        try {
            return stripper.getText(document);
        } catch (Exception e) {
            throw new RuntimeException("failed page: " + page, e);
        }
    }

    public byte[] convertImage(
            final PDFRenderer renderer,
            final int page,
            final float dpi,
            final ImageType imageType) {

        try {
            final BufferedImage image = renderer.renderImageWithDPI(page, dpi, imageType);
            try(final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                final boolean res = ImageIO.write(image, "JPEG", byteArrayOutputStream);
                return byteArrayOutputStream.toByteArray();
            }

        } catch (IOException e) {
            return null;
        }
    }

    public Map<String, Object> extractPDF(
            final PDFTextStripper stripper,
            final String prefix,
            final byte[] bytes) {
        if(bytes == null) {
            return createEmpty(bytes,  prefix,"content is null");
        }

        final Map<String, Object> values = new HashMap<>();
        final List<String> pageErrorMessages = new ArrayList<>();
        try(final PDDocument document = Loader.loadPDF(bytes)) {

            PDFRenderer renderer = new PDFRenderer(document);

            final List<String> textContents = new ArrayList<>();


            int pageCount = document.getPages().getCount();
            long errorPageCount = 0;
            for (int page = 0; page <= pageCount; page++) {
                stripper.setStartPage(page);
                stripper.setEndPage(page);
                stripper.setSortByPosition(true);
                stripper.setLineSeparator("");
                stripper.setAddMoreFormatting(true);
                stripper.setSuppressDuplicateOverlappingText(true);
                stripper.setShouldSeparateByBeads(true);
                try {
                    final String text = stripper.getText(document);
                    textContents.add(text);
                } catch (Exception e) {
                    final String errorMessage = "page: " + page + ", error: " + e.getMessage();
                    pageErrorMessages.add(errorMessage);
                    //LOG.error(errorMessage);
                    textContents.add(" ");
                    errorPageCount += 1;
                }
            }

            final String content = String.join("", textContents);

            values.put(prefix + FIELD_NAME_CONTENT, content);
            values.put(prefix + FIELD_NAME_FILESIZE, Integer.valueOf(bytes.length).longValue());
            values.put(prefix + FIELD_NAME_PAGE, Integer.valueOf(document.getNumberOfPages()).longValue());
            values.put(prefix + FIELD_NAME_VERSION, Float.valueOf(document.getVersion()).toString());
            values.put(prefix + FIELD_NAME_ENCRYPTED, document.isEncrypted());
            values.put(prefix + FIELD_NAME_TITLE, document.getDocumentInformation().getTitle());
            values.put(prefix + FIELD_NAME_AUTHOR, document.getDocumentInformation().getAuthor());
            values.put(prefix + FIELD_NAME_SUBJECT, document.getDocumentInformation().getSubject());
            values.put(prefix + FIELD_NAME_KEYWORDS, document.getDocumentInformation().getKeywords());
            values.put(prefix + FIELD_NAME_CREATOR, document.getDocumentInformation().getCreator());
            values.put(prefix + FIELD_NAME_PRODUCER, document.getDocumentInformation().getProducer());

            final Calendar creationDate = document.getDocumentInformation().getCreationDate();
            if(creationDate != null) {
                values.put(prefix + FIELD_NAME_CREATIONDATE, creationDate.toInstant().toEpochMilli() * 1000L);
            } else  {
                values.put(prefix + FIELD_NAME_CREATIONDATE, null);
            }

            final Calendar modificationDate = document.getDocumentInformation().getModificationDate();
            if(modificationDate != null) {
                values.put(prefix + FIELD_NAME_MODIFICATIONDATE, modificationDate.toInstant().toEpochMilli() * 1000L);
            } else  {
                values.put(prefix + FIELD_NAME_MODIFICATIONDATE, null);
            }

            values.put(prefix + FIELD_NAME_TRAPPED, document.getDocumentInformation().getTrapped());
            values.put(prefix + FIELD_NAME_FAILED, false);
            values.put(prefix + FIELD_NAME_ERROR_PAGE, errorPageCount);

            if(!pageErrorMessages.isEmpty()) {
                values.put(prefix + FIELD_NAME_ERROR_MESSAGE, String.join(", ", pageErrorMessages));
            }

            return values;
        } catch (final Exception e) {
            if(HtmlUtil.isZip(bytes)) {
                try {
                    HtmlUtil.EPUBDocument document = HtmlUtil.readEPUB(bytes);
                    values.put(prefix + FIELD_NAME_CONTENT, document.getContent());
                    values.put(prefix + FIELD_NAME_PAGE, document.getPage());
                    values.put(prefix + FIELD_NAME_FILESIZE, Integer.valueOf(bytes.length).longValue());
                    values.put(prefix + FIELD_NAME_FAILED, false);
                    values.put(prefix + FIELD_NAME_ERROR_PAGE, 0L);
                    values.put(prefix + FIELD_NAME_ERROR_MESSAGE, e.getMessage());
                    return values;
                } catch (Exception ee) {
                    //LOG.error("Failed to parse epub cause: {}", ee.getMessage());
                    return createEmpty(bytes, prefix, ee.getMessage());
                }
            } else {
                //LOG.error("Failed to parse pdf cause: {}", e.getMessage());
                return createEmpty(bytes, prefix, e.getMessage());
            }
        }
    }

    private static Map<String, Object> createEmpty(
            final byte[] bytes,
            final String prefix,
            final String message) {
        final Map<String, Object> values = new HashMap<>();
        values.put(prefix + FIELD_NAME_CONTENT, "");
        values.put(prefix + FIELD_NAME_PAGE, 0L);
        values.put(prefix + FIELD_NAME_FILESIZE, bytes == null ? 0L : Integer.valueOf(bytes.length).longValue());
        values.put(prefix + FIELD_NAME_FAILED, true);
        values.put(prefix + FIELD_NAME_ERROR_PAGE, 0L);
        values.put(prefix + FIELD_NAME_ERROR_MESSAGE, message);
        return values;
    }


}
