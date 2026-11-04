package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.contentstream.PDFGraphicsStreamEngine;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.color.PDColor;
import org.apache.pdfbox.pdmodel.graphics.color.PDColorSpace;
import org.apache.pdfbox.pdmodel.graphics.color.PDDeviceGray;
import org.apache.pdfbox.pdmodel.graphics.color.PDDeviceRGB;
import org.apache.pdfbox.pdmodel.graphics.image.PDImage;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.util.Matrix;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.awt.image.DirectColorModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Images drawn on a page (image XObjects, also inside form XObjects, and inline images) with the box
 * they are placed in (points, top-left origin of the crop box). The same XObject drawn twice on a page
 * is emitted once.
 *
 * <p>With {@link Merge#adjacent}, images whose boxes touch or overlap are composed into one PNG, as they
 * are drawn on the page: a picture stored as tiles or strips, or a scan stored as layers (a background
 * image and a foreground mask), comes out as one image.
 */
public class PdfImages {

    public enum Format {
        original,
        png
    }

    public enum Merge {
        none,
        adjacent
    }

    public static class Options implements Serializable {
        public Integer minWidth;
        public Integer minHeight;
        public Format format;
        public Merge merge;
        public Float mergeGap;

        public List<String> validate(final String path) {
            final List<String> errorMessages = new ArrayList<>();
            if(mergeGap != null && mergeGap < 0) {
                errorMessages.add(path + ".mergeGap must not be negative");
            }
            // it would be silently ignored
            if(mergeGap != null && !merges()) {
                errorMessages.add(path + ".mergeGap is only for merge: adjacent");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(minWidth == null) {
                minWidth = 1;
            }
            if(minHeight == null) {
                minHeight = 1;
            }
            if(format == null) {
                format = Format.original;
            }
            if(merge == null) {
                merge = Merge.none;
            }
            if(mergeGap == null) {
                mergeGap = 1F;
            }
        }

        public boolean merges() {
            return merge != null && !Merge.none.equals(merge);
        }
    }

    /** Largest merged image (width x height); a larger group is composed at a lower resolution. */
    static final long MAX_MERGED_PIXELS = 25_000_000L;

    private static final List<String> JPEG_FILTERS = List.of(COSName.DCT_DECODE.getName(), COSName.DCT_DECODE_ABBREVIATION.getName());
    private static final List<String> JPX_FILTERS = List.of(COSName.JPX_DECODE.getName());

    private PdfImages() {}

    public static List<Map<String, Object>> extract(final PDPage page, final int pageNumber, final Options options) throws IOException {
        final Collector collector = new Collector(page, options.merges());
        collector.processPage(page);
        final PDRectangle crop = page.getCropBox();

        final List<List<Drawn>> groups = options.merges()
                ? group(collector.drawn, options.mergeGap)
                : collector.drawn.stream().map(List::of).toList();

        final List<Map<String, Object>> values = new ArrayList<>();
        final Set<COSBase> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        int index = 0;
        for(final List<Drawn> group : groups) {
            final Map<String, Object> value;
            if(group.size() == 1) {
                final Drawn drawn = group.getFirst();
                if(drawn.key != null && !seen.add(drawn.key)) {
                    continue;
                }
                value = single(drawn, options);
            } else {
                value = composed(group, options);
            }
            if(value == null) {
                continue;
            }
            value.putAll(PdfStructure.box(crop, minX(group), minY(group), maxX(group), maxY(group)));
            value.put("page", (long) pageNumber);
            value.put("index", (long) index++);
            if(options.merges()) {
                value.put("parts", (long) group.size());
            }
            values.add(value);
        }
        return values;
    }

    private static Map<String, Object> single(final Drawn drawn, final Options options) throws IOException {
        final PDImage image = drawn.image;
        if(image.getWidth() < options.minWidth || image.getHeight() < options.minHeight) {
            return null;
        }
        final Encoded encoded = encode(image, options.format);
        if(encoded == null) {
            return null;
        }
        final Map<String, Object> value = new HashMap<>();
        value.put("format", encoded.format);
        value.put("width", (long) image.getWidth());
        value.put("height", (long) image.getHeight());
        value.put("bitsPerComponent", (long) image.getBitsPerComponent());
        value.put("colorSpace", colorSpaceName(image));
        value.put("content", ByteBuffer.wrap(encoded.bytes));
        return value;
    }

    private static Map<String, Object> composed(final List<Drawn> group, final Options options) throws IOException {
        final BufferedImage image = compose(group);
        if(image == null || image.getWidth() < options.minWidth || image.getHeight() < options.minHeight) {
            return null;
        }
        final Map<String, Object> value = new HashMap<>();
        value.put("format", "png");
        value.put("width", (long) image.getWidth());
        value.put("height", (long) image.getHeight());
        value.put("bitsPerComponent", 8L);
        value.put("colorSpace", PDDeviceRGB.INSTANCE.getName());
        value.put("content", ByteBuffer.wrap(PdfRender.encode(image, PdfRender.Format.png, 1.0f)));
        return value;
    }

    /**
     * Groups the drawn images whose boxes touch, overlap or are at most gap points apart (transitively),
     * in the order of their first image on the page.
     */
    static List<List<Drawn>> group(final List<Drawn> drawn, final double gap) {
        final int n = drawn.size();
        final int[] parent = new int[n];
        for(int i = 0; i < n; i++) {
            parent[i] = i;
        }
        // sweep in the order of minX: only the images that start at most gap after one ends can be near it
        final int[] order = IntStream.range(0, n).boxed()
                .sorted(Comparator.comparingDouble(i -> drawn.get(i).minX))
                .mapToInt(Integer::intValue)
                .toArray();
        for(int x = 0; x < n; x++) {
            final Drawn first = drawn.get(order[x]);
            for(int y = x + 1; y < n && drawn.get(order[y]).minX <= first.maxX + gap; y++) {
                if(near(first, drawn.get(order[y]), gap)) {
                    final int a = root(parent, order[x]);
                    final int b = root(parent, order[y]);
                    // the smaller index stays the root, so groups keep the order of their first image
                    parent[Math.max(a, b)] = Math.min(a, b);
                }
            }
        }
        final Map<Integer, List<Drawn>> groups = new LinkedHashMap<>();
        for(int i = 0; i < n; i++) {
            groups.computeIfAbsent(root(parent, i), k -> new ArrayList<>()).add(drawn.get(i));
        }
        return new ArrayList<>(groups.values());
    }

    private static int root(final int[] parent, int i) {
        while(parent[i] != i) {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        return i;
    }

    private static boolean near(final Drawn a, final Drawn b, final double gap) {
        return a.minX <= b.maxX + gap && b.minX <= a.maxX + gap
                && a.minY <= b.maxY + gap && b.minY <= a.maxY + gap;
    }

    /**
     * Draws the images of a group in page order onto one canvas covering their boxes, at the resolution of
     * the finest image (pixels per point), so that tiles keep their pixels. Areas no image covers are transparent.
     */
    static BufferedImage compose(final List<Drawn> group) throws IOException {
        final double minX = minX(group), minY = minY(group), maxX = maxX(group), maxY = maxY(group);
        // the resolution comes from the sizes the images declare, so that they are decoded one at a time below
        double scale = 0;
        for(final Drawn drawn : group) {
            final double area = Math.abs(drawn.ctm.createAffineTransform().getDeterminant());
            if(area > 0) {
                scale = Math.max(scale, Math.sqrt((double) drawn.image.getWidth() * drawn.image.getHeight() / area));
            }
        }
        if(scale <= 0) {
            return null;
        }
        scale *= shrink((maxX - minX) * scale, (maxY - minY) * scale);
        // a little slack, so that float noise does not add a column of empty pixels
        final int width = Math.max(1, (int) Math.ceil((maxX - minX) * scale - 0.01));
        final int height = Math.max(1, (int) Math.ceil((maxY - minY) * scale - 0.01));

        final BufferedImage canvas = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        boolean painted = false;
        final Graphics2D g = canvas.createGraphics();
        try {
            final AffineTransform page = new AffineTransform(scale, 0, 0, -scale, -minX * scale, maxY * scale);
            for(final Drawn drawn : group) {
                final BufferedImage image = drawn.image.isStencil()
                        ? drawn.image.getStencilImage(drawn.paint)
                        : drawn.image.getImage();
                if(image == null) {
                    continue;
                }
                final AffineTransform transform = new AffineTransform(page);
                transform.concatenate(drawn.ctm.createAffineTransform());
                // image pixels (top-left origin) to the unit square the image is drawn in
                transform.concatenate(new AffineTransform(1.0 / image.getWidth(), 0, 0, -1.0 / image.getHeight(), 0, 1));
                // images whose pixels fall one to one on canvas pixels are copied pixel by pixel, others are resampled
                g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, pixelForPixel(transform)
                        ? RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR
                        : RenderingHints.VALUE_INTERPOLATION_BILINEAR);
                g.drawImage(image, transform, null);
                painted = true;
            }
        } finally {
            g.dispose();
        }
        if(!painted) {
            return null;
        }
        return opaque(canvas) ? toRgb(canvas) : canvas;
    }

    /**
     * The factor that brings a canvas of width x height pixels within {@link #MAX_MERGED_PIXELS}, counting a side
     * of less than a pixel as the one pixel it is drawn on.
     */
    private static double shrink(final double width, final double height) {
        double factor = 1;
        if(width * height > MAX_MERGED_PIXELS) {
            factor = Math.sqrt(MAX_MERGED_PIXELS / (width * height));
        }
        if(height * factor < 1 && width * factor > MAX_MERGED_PIXELS) {
            factor = MAX_MERGED_PIXELS / width;
        }
        if(width * factor < 1 && height * factor > MAX_MERGED_PIXELS) {
            factor = MAX_MERGED_PIXELS / height;
        }
        return factor;
    }

    /** Whether the transform maps image pixels onto canvas pixels one to one (also turned by quarter turns or mirrored). */
    private static boolean pixelForPixel(final AffineTransform t) {
        final double e = 0.01;
        final boolean straight = Math.abs(t.getShearX()) < e && Math.abs(t.getShearY()) < e
                && Math.abs(Math.abs(t.getScaleX()) - 1) < e && Math.abs(Math.abs(t.getScaleY()) - 1) < e;
        final boolean turned = Math.abs(t.getScaleX()) < e && Math.abs(t.getScaleY()) < e
                && Math.abs(Math.abs(t.getShearX()) - 1) < e && Math.abs(Math.abs(t.getShearY()) - 1) < e;
        return straight || turned;
    }

    private static boolean opaque(final BufferedImage image) {
        final int[] row = new int[image.getWidth()];
        for(int y = 0; y < image.getHeight(); y++) {
            image.getRGB(0, y, row.length, 1, row, 0, row.length);
            for(final int argb : row) {
                if((argb >>> 24) != 0xFF) {
                    return false;
                }
            }
        }
        return true;
    }

    /** The pixels of an ARGB image without its alpha band, sharing its data (a canvas can be 25M pixels). */
    private static BufferedImage toRgb(final BufferedImage argb) {
        final WritableRaster rgb = argb.getRaster().createWritableChild(
                0, 0, argb.getWidth(), argb.getHeight(), 0, 0, new int[]{0, 1, 2});
        return new BufferedImage(new DirectColorModel(24, 0xFF0000, 0xFF00, 0xFF), rgb, false, null);
    }

    private static double minX(final List<Drawn> group) {
        return group.stream().mapToDouble(d -> d.minX).min().orElse(0);
    }

    private static double minY(final List<Drawn> group) {
        return group.stream().mapToDouble(d -> d.minY).min().orElse(0);
    }

    private static double maxX(final List<Drawn> group) {
        return group.stream().mapToDouble(d -> d.maxX).max().orElse(0);
    }

    private static double maxY(final List<Drawn> group) {
        return group.stream().mapToDouble(d -> d.maxY).max().orElse(0);
    }

    private record Encoded(String format, byte[] bytes) {}

    private static Encoded encode(final PDImage image, final Format format) throws IOException {
        if(Format.original.equals(format) && image instanceof PDImageXObject xobject) {
            final String suffix = xobject.getSuffix();
            if("jpg".equals(suffix) && isDeviceGrayOrRGB(image)) {
                try(final InputStream is = xobject.createInputStream(JPEG_FILTERS)) {
                    return new Encoded("jpeg", is.readAllBytes());
                }
            }
            if("jpx".equals(suffix)) {
                try(final InputStream is = xobject.createInputStream(JPX_FILTERS)) {
                    return new Encoded("jp2", is.readAllBytes());
                }
            }
        }
        final BufferedImage bufferedImage = image.getImage();
        if(bufferedImage == null) {
            return null;
        }
        return new Encoded("png", PdfRender.encode(bufferedImage, PdfRender.Format.png, 1.0f));
    }

    private static boolean isDeviceGrayOrRGB(final PDImage image) {
        try {
            final PDColorSpace colorSpace = image.getColorSpace();
            return colorSpace instanceof PDDeviceGray || colorSpace instanceof PDDeviceRGB;
        } catch (final IOException e) {
            return false;
        }
    }

    private static String colorSpaceName(final PDImage image) {
        try {
            final PDColorSpace colorSpace = image.getColorSpace();
            return colorSpace == null ? null : colorSpace.getName();
        } catch (final IOException e) {
            return null;
        }
    }

    /**
     * An image as drawn: its transformation (the unit square to user space), its box in user space,
     * the XObject it is (null for inline images) and, for a stencil mask, the color it is painted in.
     */
    record Drawn(PDImage image, Matrix ctm, COSBase key, Color paint,
                 double minX, double minY, double maxX, double maxY) {}

    private static class Collector extends PDFGraphicsStreamEngine {

        private final boolean paints;
        private final List<Drawn> drawn = new ArrayList<>();
        private final Map<COSBase, Set<Matrix>> placed = new IdentityHashMap<>();

        /** paints: keep the fill color of stencil masks, which only composing uses. */
        Collector(final PDPage page, final boolean paints) {
            super(page);
            this.paints = paints;
        }

        @Override
        public void drawImage(final PDImage image) {
            final Matrix ctm = getGraphicsState().getCurrentTransformationMatrix().clone();
            final COSBase key = image instanceof PDImageXObject xobject ? xobject.getCOSObject() : null;
            // the same image drawn again at the same place adds nothing (and would merge with itself)
            if(key != null && !placed.computeIfAbsent(key, k -> new HashSet<>()).add(ctm)) {
                return;
            }
            double minX = Double.MAX_VALUE, minY = Double.MAX_VALUE, maxX = -Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
            for(final float[] corner : new float[][]{{0, 0}, {1, 0}, {0, 1}, {1, 1}}) {
                final Point2D.Float p = ctm.transformPoint(corner[0], corner[1]);
                minX = Math.min(minX, p.x);
                maxX = Math.max(maxX, p.x);
                minY = Math.min(minY, p.y);
                maxY = Math.max(maxY, p.y);
            }
            final Color paint = paints && image.isStencil() ? fillColor() : null;
            drawn.add(new Drawn(image, ctm, key, paint, minX, minY, maxX, maxY));
        }

        private Color fillColor() {
            try {
                final PDColor color = getGraphicsState().getNonStrokingColor();
                return new Color(color.toRGB());
            } catch (final IOException | RuntimeException e) {
                // e.g. a pattern: paint the mask black
                return Color.BLACK;
            }
        }

        @Override public void appendRectangle(Point2D p0, Point2D p1, Point2D p2, Point2D p3) {}
        @Override public void clip(int windingRule) {}
        @Override public void moveTo(float x, float y) {}
        @Override public void lineTo(float x, float y) {}
        @Override public void curveTo(float x1, float y1, float x2, float y2, float x3, float y3) {}
        @Override public Point2D getCurrentPoint() { return new Point2D.Float(0, 0); }
        @Override public void closePath() {}
        @Override public void endPath() {}
        @Override public void strokePath() {}
        @Override public void fillPath(int windingRule) {}
        @Override public void fillAndStrokePath(int windingRule) {}
        @Override public void shadingFill(COSName shadingName) {}
    }

}
