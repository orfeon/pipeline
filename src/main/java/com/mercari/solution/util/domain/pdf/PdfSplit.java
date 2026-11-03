package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Splits a document into chunks of the selected pages, each saved as its own PDF. */
public class PdfSplit {

    public record Chunk(byte[] bytes, int startPage, int endPage, int size) {}

    private PdfSplit() {}

    public static List<Chunk> split(final PDDocument source, final List<Integer> pages, final int pagesPerChunk) throws IOException {
        final List<Chunk> chunks = new ArrayList<>();
        for(int from = 0; from < pages.size(); from += pagesPerChunk) {
            final List<Integer> chunkPages = pages.subList(from, Math.min(pages.size(), from + pagesPerChunk));
            try(final PDDocument target = new PDDocument()) {
                target.setVersion(source.getVersion());
                target.setDocumentInformation(source.getDocumentInformation());
                target.getDocumentCatalog().setViewerPreferences(source.getDocumentCatalog().getViewerPreferences());
                for(final int pageNumber : chunkPages) {
                    final PDPage page = source.getPage(pageNumber - 1);
                    final PDPage imported = target.importPage(page);
                    // inherited attributes are not carried by importPage
                    imported.setResources(page.getResources());
                    imported.setMediaBox(page.getMediaBox());
                    imported.setCropBox(page.getCropBox());
                    imported.setRotation(page.getRotation());
                }
                try(final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                    target.save(bytes);
                    chunks.add(new Chunk(bytes.toByteArray(), chunkPages.getFirst(), chunkPages.getLast(), chunkPages.size()));
                }
            }
        }
        return chunks;
    }

}
