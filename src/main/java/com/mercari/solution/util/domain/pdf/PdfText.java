package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageTree;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-page text extraction with optional word / line boxes. One instance per DoFn (not thread-safe).
 * Coordinates are in points with the origin at the top-left of the page's crop box
 * (PDFBox's direction-adjusted text positions).
 */
public class PdfText {

    public static class Options implements Serializable {
        public Boolean sortByPosition;
        public String lineSeparator;
        public String pageSeparator;

        public void setDefaults() {
            if(sortByPosition == null) {
                sortByPosition = true;
            }
            if(lineSeparator == null) {
                lineSeparator = "\n";
            }
            if(pageSeparator == null) {
                pageSeparator = "\n";
            }
        }
    }

    public record Word(
            int page, int line, String text,
            double x0, double y0, double x1, double y1,
            String fontName, double fontSize) {}

    public record PageText(String text, List<Word> words) {}

    private final CollectingStripper stripper;

    public PdfText(final Options options) throws IOException {
        this.stripper = new CollectingStripper();
        this.stripper.setSortByPosition(options.sortByPosition);
        this.stripper.setLineSeparator(options.lineSeparator);
        this.stripper.setSuppressDuplicateOverlappingText(true);
        this.stripper.setShouldSeparateByBeads(true);
        // the stripper processes only the requested page (see CollectingStripper.processPages), which it counts as page 1
        this.stripper.setStartPage(1);
        this.stripper.setEndPage(1);
    }

    /** Text of one page (1-based); words are collected only when {@code collectWords}. */
    public PageText extract(final PDDocument document, final int page, final boolean collectWords) throws IOException {
        stripper.begin(page, collectWords);
        final String text = stripper.getText(document);
        return new PageText(text, stripper.words());
    }

    public static Map<String, Object> wordValues(final Word word) {
        final Map<String, Object> values = new HashMap<>();
        values.put("page", (long) word.page());
        values.put("text", word.text());
        values.put("x0", word.x0());
        values.put("y0", word.y0());
        values.put("x1", word.x1());
        values.put("y1", word.y1());
        values.put("fontName", word.fontName());
        values.put("fontSize", word.fontSize());
        return values;
    }

    /** Groups words by line: text joined with a space, box = union, font size = max. */
    public static List<Map<String, Object>> lineValues(final List<Word> words) {
        final Map<String, List<Word>> lines = new LinkedHashMap<>();
        for(final Word word : words) {
            lines.computeIfAbsent(word.page() + ":" + word.line(), k -> new ArrayList<>()).add(word);
        }
        final List<Map<String, Object>> values = new ArrayList<>();
        for(final List<Word> line : lines.values()) {
            final StringBuilder text = new StringBuilder();
            double x0 = Double.MAX_VALUE, y0 = Double.MAX_VALUE, x1 = -Double.MAX_VALUE, y1 = -Double.MAX_VALUE, fontSize = 0;
            for(final Word word : line) {
                if(!text.isEmpty()) {
                    text.append(' ');
                }
                text.append(word.text());
                x0 = Math.min(x0, word.x0());
                y0 = Math.min(y0, word.y0());
                x1 = Math.max(x1, word.x1());
                y1 = Math.max(y1, word.y1());
                fontSize = Math.max(fontSize, word.fontSize());
            }
            final Map<String, Object> value = new HashMap<>();
            value.put("page", (long) line.getFirst().page());
            value.put("text", text.toString());
            value.put("x0", x0);
            value.put("y0", y0);
            value.put("x1", x1);
            value.put("y1", y1);
            value.put("fontSize", fontSize);
            values.add(value);
        }
        return values;
    }

    /** Strips the subset tag ({@code ABCDEF+}) of an embedded font name. */
    static String fontName(final PDFont font) {
        if(font == null || font.getName() == null) {
            return null;
        }
        final String name = font.getName();
        if(name.length() > 7 && name.charAt(6) == '+' && name.substring(0, 6).chars().allMatch(Character::isUpperCase)) {
            return name.substring(7);
        }
        return name;
    }

    private static class CollectingStripper extends PDFTextStripper {

        private int page;
        private int line;
        private boolean collect;
        private List<Word> words = new ArrayList<>();

        CollectingStripper() throws IOException {
            super();
        }

        void begin(final int page, final boolean collect) {
            this.page = page;
            this.line = 0;
            this.collect = collect;
            this.words = new ArrayList<>();
        }

        List<Word> words() {
            return words;
        }

        /**
         * PDFTextStripper walks the whole page tree on every getText call and filters by start / end page,
         * so reading a document page by page would cost O(pages²). Go to the requested page directly.
         */
        @Override
        protected void processPages(final PDPageTree pages) throws IOException {
            final PDPage target = pages.get(page - 1);
            if(target.hasContents()) {
                processPage(target);
            }
        }

        @Override
        protected void writeString(final String text, final List<TextPosition> positions) throws IOException {
            super.writeString(text, positions);
            if(!collect || text == null || text.isBlank() || positions == null || positions.isEmpty()) {
                return;
            }
            double x0 = Double.MAX_VALUE, y0 = Double.MAX_VALUE, x1 = -Double.MAX_VALUE, y1 = -Double.MAX_VALUE, fontSize = 0;
            for(final TextPosition p : positions) {
                x0 = Math.min(x0, p.getXDirAdj());
                x1 = Math.max(x1, p.getXDirAdj() + p.getWidthDirAdj());
                y0 = Math.min(y0, p.getYDirAdj() - p.getHeightDir());
                y1 = Math.max(y1, p.getYDirAdj());
                fontSize = Math.max(fontSize, p.getFontSizeInPt());
            }
            words.add(new Word(page, line, text, x0, y0, x1, y1, fontName(positions.getFirst().getFont()), fontSize));
        }

        @Override
        protected void writeLineSeparator() throws IOException {
            super.writeLineSeparator();
            line++;
        }

        @Override
        protected void writeParagraphEnd() throws IOException {
            super.writeParagraphEnd();
            line++;
        }
    }

}
