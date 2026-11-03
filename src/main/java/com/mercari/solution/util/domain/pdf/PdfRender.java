package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/** Renders pages to PNG / JPEG, capping the pixel count by lowering the dpi. */
public class PdfRender {

    public enum Format {
        png,
        jpeg
    }

    public enum Color {
        rgb,
        gray,
        binary
    }

    public enum When {
        always,
        noText
    }

    public static class Options implements Serializable {
        public Float dpi;
        public Format format;
        public Color color;
        public Float quality;
        public Long maxPixels;
        public When when;
        public Integer minTextLength;

        public void setDefaults() {
            if(dpi == null) {
                dpi = 150f;
            }
            if(format == null) {
                format = Format.png;
            }
            if(color == null) {
                color = Color.rgb;
            }
            if(quality == null) {
                quality = 0.9f;
            }
            if(maxPixels == null) {
                maxPixels = 25_000_000L;
            }
            if(when == null) {
                when = When.always;
            }
            if(minTextLength == null) {
                minTextLength = 1;
            }
        }
    }

    public record Rendered(byte[] bytes, int width, int height, float dpi) {}

    private PdfRender() {}

    static {
        if(System.getProperty("java.awt.headless") == null) {
            System.setProperty("java.awt.headless", "true");
        }
    }

    /** {@code pageIndex} is 0-based. */
    public static Rendered render(final PDFRenderer renderer, final PDPage page, final int pageIndex, final Options options) throws IOException {
        final float dpi = effectiveDpi(page, options.dpi, options.maxPixels);
        final ImageType imageType = switch (options.color) {
            case rgb -> ImageType.RGB;
            case gray -> ImageType.GRAY;
            case binary -> ImageType.BINARY;
        };
        final BufferedImage image = renderer.renderImageWithDPI(pageIndex, dpi, imageType);
        return new Rendered(encode(image, options.format, options.quality), image.getWidth(), image.getHeight(), dpi);
    }

    static float effectiveDpi(final PDPage page, final float dpi, final long maxPixels) {
        final PDRectangle box = page.getCropBox();
        final double width = box.getWidth() * dpi / 72.0;
        final double height = box.getHeight() * dpi / 72.0;
        final double pixels = width * height;
        if(maxPixels <= 0 || pixels <= maxPixels) {
            return dpi;
        }
        return (float) (dpi * Math.sqrt(maxPixels / pixels));
    }

    public static byte[] encode(final BufferedImage image, final Format format, final float quality) throws IOException {
        try(final ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            switch (format) {
                case png -> {
                    if(!ImageIO.write(image, "png", bytes)) {
                        throw new IOException("no png image writer");
                    }
                }
                case jpeg -> {
                    final BufferedImage rgb = toJpegCompatible(image);
                    final Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
                    if(!writers.hasNext()) {
                        throw new IOException("no jpeg image writer");
                    }
                    final ImageWriter writer = writers.next();
                    try(final ImageOutputStream ios = ImageIO.createImageOutputStream(bytes)) {
                        writer.setOutput(ios);
                        final ImageWriteParam param = writer.getDefaultWriteParam();
                        param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                        param.setCompressionQuality(Math.max(0f, Math.min(1f, quality)));
                        writer.write(null, new IIOImage(rgb, null, null), param);
                    } finally {
                        writer.dispose();
                    }
                }
            }
            return bytes.toByteArray();
        }
    }

    private static BufferedImage toJpegCompatible(final BufferedImage image) {
        if(image.getType() == BufferedImage.TYPE_INT_RGB
                || image.getType() == BufferedImage.TYPE_3BYTE_BGR
                || image.getType() == BufferedImage.TYPE_BYTE_GRAY) {
            return image;
        }
        final BufferedImage rgb = new BufferedImage(image.getWidth(), image.getHeight(),
                image.getType() == BufferedImage.TYPE_BYTE_BINARY ? BufferedImage.TYPE_BYTE_GRAY : BufferedImage.TYPE_INT_RGB);
        final java.awt.Graphics2D g = rgb.createGraphics();
        try {
            g.setColor(java.awt.Color.WHITE);
            g.fillRect(0, 0, image.getWidth(), image.getHeight());
            g.drawImage(image, 0, 0, null);
        } finally {
            g.dispose();
        }
        return rgb;
    }

}
