package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.contentstream.PDFGraphicsStreamEngine;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.color.PDColorSpace;
import org.apache.pdfbox.pdmodel.graphics.color.PDDeviceGray;
import org.apache.pdfbox.pdmodel.graphics.color.PDDeviceRGB;
import org.apache.pdfbox.pdmodel.graphics.image.PDImage;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.util.Matrix;

import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Images drawn on a page (image XObjects, also inside form XObjects, and inline images) with the box
 * they are placed in (points, top-left origin of the crop box). The same XObject drawn twice on a page
 * is emitted once.
 */
public class PdfImages {

    public enum Format {
        original,
        png
    }

    public static class Options implements Serializable {
        public Integer minWidth;
        public Integer minHeight;
        public Format format;

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
        }
    }

    private static final List<String> JPEG_FILTERS = List.of(COSName.DCT_DECODE.getName(), COSName.DCT_DECODE_ABBREVIATION.getName());
    private static final List<String> JPX_FILTERS = List.of(COSName.JPX_DECODE.getName());

    private PdfImages() {}

    public static List<Map<String, Object>> extract(final PDPage page, final int pageNumber, final Options options) throws IOException {
        final Collector collector = new Collector(page);
        collector.processPage(page);
        final List<Map<String, Object>> values = new ArrayList<>();
        int index = 0;
        for(final Drawn drawn : collector.drawn) {
            final PDImage image = drawn.image;
            if(image.getWidth() < options.minWidth || image.getHeight() < options.minHeight) {
                continue;
            }
            final Encoded encoded = encode(image, options.format);
            if(encoded == null) {
                continue;
            }
            final Map<String, Object> value = new HashMap<>(drawn.box);
            value.put("page", (long) pageNumber);
            value.put("index", (long) index++);
            value.put("format", encoded.format);
            value.put("width", (long) image.getWidth());
            value.put("height", (long) image.getHeight());
            value.put("bitsPerComponent", (long) image.getBitsPerComponent());
            value.put("colorSpace", colorSpaceName(image));
            value.put("content", ByteBuffer.wrap(encoded.bytes));
            values.add(value);
        }
        return values;
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

    private record Drawn(PDImage image, Map<String, Object> box) {}

    private static class Collector extends PDFGraphicsStreamEngine {

        private final PDRectangle crop;
        private final List<Drawn> drawn = new ArrayList<>();
        private final Set<COSBase> seen = Collections.newSetFromMap(new IdentityHashMap<>());

        Collector(final PDPage page) {
            super(page);
            this.crop = page.getCropBox();
        }

        @Override
        public void drawImage(final PDImage image) {
            if(image instanceof PDImageXObject xobject && !seen.add(xobject.getCOSObject())) {
                return;
            }
            final Matrix ctm = getGraphicsState().getCurrentTransformationMatrix();
            double minX = Double.MAX_VALUE, minY = Double.MAX_VALUE, maxX = -Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
            for(final float[] corner : new float[][]{{0, 0}, {1, 0}, {0, 1}, {1, 1}}) {
                final Point2D.Float p = ctm.transformPoint(corner[0], corner[1]);
                minX = Math.min(minX, p.x);
                maxX = Math.max(maxX, p.x);
                minY = Math.min(minY, p.y);
                maxY = Math.max(maxY, p.y);
            }
            drawn.add(new Drawn(image, PdfStructure.box(crop, minX, minY, maxX, maxY)));
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
