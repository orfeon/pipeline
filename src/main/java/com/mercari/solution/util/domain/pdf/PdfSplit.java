package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Splits a document into chunks of the selected pages, each saved as its own PDF.
 *
 * <p>Built on PDFBox's {@link Splitter}, which clones the annotations of the imported pages and
 * fixes their destinations and the structure tree. Importing pages with {@code importPage} alone
 * keeps the back-references of annotations ({@code /P} of widgets, link destinations), which pull
 * the source page and through its {@code /Parent} the whole source document into every chunk.
 */
public class PdfSplit {

    public record Chunk(byte[] bytes, int startPage, int endPage) {}

    private PdfSplit() {}

    public static List<Chunk> split(final PDDocument source, final List<Integer> pages, final int pagesPerChunk) throws IOException {
        final List<Chunk> chunks = new ArrayList<>();
        for(int from = 0; from < pages.size(); from += pagesPerChunk) {
            final List<Integer> chunkPages = pages.subList(from, Math.min(pages.size(), from + pagesPerChunk));
            final List<PDDocument> documents = new ChunkSplitter(chunkPages).split(source);
            try {
                if(documents.size() != 1) {
                    throw new IllegalStateException("pdf split produced " + documents.size() + " documents for pages " + chunkPages);
                }
                try(final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                    documents.getFirst().save(bytes);
                    chunks.add(new Chunk(bytes.toByteArray(), chunkPages.getFirst(), chunkPages.getLast()));
                }
            } finally {
                for(final PDDocument document : documents) {
                    document.close();
                }
            }
        }
        return chunks;
    }

    /** One output document holding the given (ascending, possibly non-contiguous) 1-based pages. */
    private static final class ChunkSplitter extends Splitter {

        private final Set<Integer> selected;
        private int pageNumber;

        ChunkSplitter(final List<Integer> pages) {
            this.selected = new HashSet<>(pages);
            this.pageNumber = pages.getFirst() - 1;
            setStartPage(pages.getFirst());
            setEndPage(pages.getLast());
        }

        @Override
        protected boolean splitAtPage(final int pageNumber) {
            return false;
        }

        @Override
        protected void processPage(final PDPage page) throws IOException {
            // called for every page from startPage to endPage, in order
            pageNumber++;
            if(selected.contains(pageNumber)) {
                super.processPage(page);
            }
        }
    }

}
