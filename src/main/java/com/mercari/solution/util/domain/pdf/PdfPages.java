package com.mercari.solution.util.domain.pdf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Page selection spec of the pdf transform: comma separated 1-based pages and ranges,
 * {@code "1-3,5,10-"} ({@code N-} runs to the last page). Pages beyond the document are ignored.
 */
public class PdfPages implements Serializable {

    private final List<int[]> ranges;

    private PdfPages(final List<int[]> ranges) {
        this.ranges = ranges;
    }

    public static PdfPages all() {
        return new PdfPages(List.of(new int[]{1, Integer.MAX_VALUE}));
    }

    public static PdfPages parse(final String spec) {
        if(spec == null || spec.isBlank()) {
            return all();
        }
        final List<int[]> ranges = new ArrayList<>();
        for(final String part : spec.split(",")) {
            final String p = part.trim();
            if(p.isEmpty()) {
                continue;
            }
            final int dash = p.indexOf('-');
            try {
                if(dash < 0) {
                    final int page = Integer.parseInt(p);
                    ranges.add(new int[]{page, page});
                } else {
                    final String from = p.substring(0, dash).trim();
                    final String to = p.substring(dash + 1).trim();
                    final int start = from.isEmpty() ? 1 : Integer.parseInt(from);
                    final int end = to.isEmpty() ? Integer.MAX_VALUE : Integer.parseInt(to);
                    ranges.add(new int[]{start, end});
                }
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("pages: '" + spec + "' has an illegal part: '" + p + "'");
            }
            final int[] last = ranges.getLast();
            if(last[0] < 1 || last[1] < last[0]) {
                throw new IllegalArgumentException("pages: '" + spec + "' has an illegal range: '" + p + "' (pages start at 1)");
            }
        }
        if(ranges.isEmpty()) {
            return all();
        }
        return new PdfPages(ranges);
    }

    /** Selected 1-based page numbers that exist in a document of {@code pageCount} pages, ascending. */
    public List<Integer> resolve(final int pageCount) {
        final TreeSet<Integer> pages = new TreeSet<>();
        for(final int[] range : ranges) {
            final int end = Math.min(range[1], pageCount);
            for(int page = range[0]; page <= end; page++) {
                pages.add(page);
            }
        }
        return new ArrayList<>(pages);
    }

    /**
     * Selected 1-based page numbers in the order they are written ({@code "3,1-2"} gives 3, 1, 2), with
     * repetitions kept; pages beyond the document are ignored.
     */
    public List<Integer> resolveInOrder(final int pageCount) {
        final List<Integer> pages = new ArrayList<>();
        for(final int[] range : ranges) {
            final int end = Math.min(range[1], pageCount);
            for(int page = range[0]; page <= end; page++) {
                pages.add(page);
            }
        }
        return pages;
    }

}
