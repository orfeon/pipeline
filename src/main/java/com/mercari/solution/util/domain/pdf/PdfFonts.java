package com.mercari.solution.util.domain.pdf;

import org.apache.fontbox.ttf.TTFParser;
import org.apache.fontbox.ttf.TrueTypeCollection;
import org.apache.fontbox.ttf.TrueTypeFont;
import org.apache.pdfbox.io.RandomAccessReadBuffer;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType0Font;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Fonts for the text the pdf transform writes (overlay text, form field values).
 *
 * <p>The Standard 14 fonts ({@code Helvetica}, {@code Times-Roman}, {@code Courier}, ...) need no file but
 * only cover WinAnsi characters. Other scripts (e.g. Japanese) need a TrueType font from {@code fonts}:
 * the file is read and parsed once per worker and embedded into each document that uses it.
 */
public class PdfFonts implements Closeable {

    public static final String DEFAULT_FONT = Standard14Fonts.FontName.HELVETICA.getName();

    public static class Definition implements Serializable {

        public String name;
        public String path;
        /** For a TrueType collection (.ttc): the PostScript name of the font to use; the first font by default. */
        public String fontName;

        public List<String> validate(final String path) {
            final List<String> errorMessages = new ArrayList<>();
            if(name == null || name.isBlank()) {
                errorMessages.add(path + ".name must not be empty");
            } else if(isStandard(name)) {
                errorMessages.add(path + ".name: " + name + " is a Standard 14 font name; use another name");
            }
            if(this.path == null || this.path.isBlank()) {
                errorMessages.add(path + ".path must not be empty");
            }
            return errorMessages;
        }
    }

    private final Map<String, TrueTypeFont> fonts;
    private final Set<String> collectionFonts;
    private final List<Closeable> closeables;

    private PdfFonts(final Map<String, TrueTypeFont> fonts, final Set<String> collectionFonts, final List<Closeable> closeables) {
        this.fonts = fonts;
        this.collectionFonts = collectionFonts;
        this.closeables = closeables;
    }

    public static boolean isStandard(final String name) {
        return standardName(name) != null;
    }

    public static List<String> standardNames() {
        return Arrays.stream(Standard14Fonts.FontName.values()).map(Standard14Fonts.FontName::getName).toList();
    }

    private static Standard14Fonts.FontName standardName(final String name) {
        for(final Standard14Fonts.FontName fontName : Standard14Fonts.FontName.values()) {
            if(fontName.getName().equals(name)) {
                return fontName;
            }
        }
        return null;
    }

    /** Parses the font files, read by {@code resources}. */
    public static PdfFonts load(final List<Definition> definitions, final PdfResources resources) throws IOException {
        final Map<String, TrueTypeFont> fonts = new HashMap<>();
        final Set<String> collectionFonts = new HashSet<>();
        final List<Closeable> closeables = new ArrayList<>();
        if(definitions != null) {
            for(final Definition definition : definitions) {
                // throws for a missing file
                final byte[] bytes = resources.read(definition.path);
                final TrueTypeFont font;
                if(isCollection(bytes)) {
                    final TrueTypeCollection collection = new TrueTypeCollection(new ByteArrayInputStream(bytes));
                    closeables.add(collection);
                    font = definition.fontName == null ? first(collection) : collection.getFontByName(definition.fontName);
                    if(font == null) {
                        throw new IOException("font: " + definition.fontName + " is not in the collection: " + definition.path);
                    }
                    collectionFonts.add(definition.name);
                } else {
                    if(isPostScriptOpenType(bytes)) {
                        throw new IOException("font: " + definition.path + " is an OpenType font with CFF outlines, which cannot be embedded; use a font with TrueType outlines (.ttf / .ttc)");
                    }
                    font = new TTFParser().parse(new RandomAccessReadBuffer(bytes));
                    closeables.add(font);
                }
                fonts.put(definition.name, font);
            }
        }
        return new PdfFonts(fonts, collectionFonts, closeables);
    }

    private static boolean isCollection(final byte[] bytes) {
        return bytes.length >= 4 && bytes[0] == 't' && bytes[1] == 't' && bytes[2] == 'c' && bytes[3] == 'f';
    }

    private static boolean isPostScriptOpenType(final byte[] bytes) {
        return bytes.length >= 4 && bytes[0] == 'O' && bytes[1] == 'T' && bytes[2] == 'T' && bytes[3] == 'O';
    }

    private static TrueTypeFont first(final TrueTypeCollection collection) throws IOException {
        final TrueTypeFont[] first = new TrueTypeFont[1];
        collection.processAllFonts(font -> {
            if(first[0] == null) {
                first[0] = font;
            }
        });
        return first[0];
    }

    /** Fonts bound to one document: each font is loaded into the document once, on first use. */
    public Resolver forDocument(final PDDocument document) {
        return new Resolver(document);
    }

    public final class Resolver {

        private final PDDocument document;
        private final Map<String, PDFont> loaded = new HashMap<>();

        private Resolver(final PDDocument document) {
            this.document = document;
        }

        /**
         * @param embedSubset embed only the glyphs used. A form field that stays editable needs the whole font,
         *                    as a viewer regenerates its appearance from the font when the value is changed.
         */
        public PDFont get(final String name, final boolean embedSubset) throws IOException {
            final String fontName = name == null ? DEFAULT_FONT : name;
            final String key = fontName + (embedSubset ? "#subset" : "#full");
            final PDFont cached = loaded.get(key);
            if(cached != null) {
                return cached;
            }
            final PDFont font;
            final Standard14Fonts.FontName standard = standardName(fontName);
            if(standard != null) {
                font = new PDType1Font(standard);
            } else {
                final TrueTypeFont ttf = fonts.get(fontName);
                if(ttf == null) {
                    throw new IllegalArgumentException("font: " + fontName + " is not defined in parameters.fonts");
                }
                if(!embedSubset && collectionFonts.contains(fontName)) {
                    // PDFBox embeds a font of a collection only as a subset
                    throw new IllegalArgumentException("font: " + fontName + " is from a TrueType collection (.ttc), which can only be embedded as a subset: "
                            + "flatten the form (flatten: true) or use a .ttf font");
                }
                // the TrueTypeFont stays open (owned by PdfFonts), so it is reused across documents
                font = PDType0Font.load(document, ttf, embedSubset);
            }
            loaded.put(key, font);
            return font;
        }
    }

    @Override
    public void close() throws IOException {
        for(final Closeable closeable : closeables) {
            closeable.close();
        }
    }

}
