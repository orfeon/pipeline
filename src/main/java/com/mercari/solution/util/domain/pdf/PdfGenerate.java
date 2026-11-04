package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.image.JPEGFactory;
import org.apache.pdfbox.pdmodel.graphics.image.LosslessFactory;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.util.Matrix;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Creates documents: one page per image (JPEG EXIF orientation applied without re-encoding). */
public class PdfGenerate {

    public enum Type {
        form,
        images
    }

    public enum PageSize {
        image(null),
        A3(PDRectangle.A3),
        A4(PDRectangle.A4),
        A5(PDRectangle.A5),
        letter(PDRectangle.LETTER),
        legal(PDRectangle.LEGAL);

        private final PDRectangle rectangle;

        PageSize(final PDRectangle rectangle) {
            this.rectangle = rectangle;
        }
    }

    public enum Orientation {
        auto,
        portrait,
        landscape
    }

    public static class ImagesOptions implements Serializable {

        public PageSize pageSize;
        public Orientation orientation;
        public Float dpi;
        public Float margin;

        public List<String> validate(final String path) {
            final List<String> errorMessages = new ArrayList<>();
            if(dpi != null && dpi <= 0) {
                errorMessages.add(path + ".dpi must be positive");
            }
            if(margin != null && margin < 0) {
                errorMessages.add(path + ".margin must not be negative");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(pageSize == null) {
                pageSize = PageSize.image;
            }
            if(orientation == null) {
                orientation = Orientation.auto;
            }
            if(dpi == null) {
                dpi = 96F;
            }
            if(margin == null) {
                margin = 0F;
            }
        }
    }

    private PdfGenerate() {}

    /**
     * An image with the orientation its JPEG EXIF data asks for (1 = as stored, 2 to 8 = mirrored and / or rotated,
     * as phone cameras store photos). The pixels are kept as stored and turned by the drawing matrix.
     */
    public record Image(PDImageXObject xObject, int orientation) {

        /** Whether the image is shown rotated by 90 or 270 degrees, so that width and height swap. */
        private boolean transposed() {
            return orientation >= 5 && orientation <= 8;
        }

        /** Width in pixels as displayed. */
        public int width() {
            return transposed() ? xObject.getHeight() : xObject.getWidth();
        }

        /** Height in pixels as displayed. */
        public int height() {
            return transposed() ? xObject.getWidth() : xObject.getHeight();
        }

        /** Draws the image, as displayed, into the box (x, y, width, height). */
        public void draw(final PDPageContentStream content, final float x, final float y, final float width, final float height) throws IOException {
            // maps the image's unit square (u, v) to the box's (s, t), then scales to the box
            final float[] m = switch (orientation) {
                case 2 -> new float[]{-1, 0, 0, 1, 1, 0};
                case 3 -> new float[]{-1, 0, 0, -1, 1, 1};
                case 4 -> new float[]{1, 0, 0, -1, 0, 1};
                case 5 -> new float[]{0, -1, -1, 0, 1, 1};
                case 6 -> new float[]{0, -1, 1, 0, 0, 1};
                case 7 -> new float[]{0, 1, 1, 0, 0, 0};
                case 8 -> new float[]{0, 1, -1, 0, 1, 0};
                default -> new float[]{1, 0, 0, 1, 0, 0};
            };
            content.drawImage(xObject, new Matrix(
                    width * m[0], height * m[1], width * m[2], height * m[3], x + width * m[4], y + height * m[5]));
        }
    }

    /** Adds one page per image (JPEG is embedded as is, other formats losslessly). */
    public static void images(final PDDocument document, final List<byte[]> images, final ImagesOptions options) throws IOException {
        if(images.isEmpty()) {
            throw new IllegalArgumentException("no images to generate a pdf from");
        }
        int index = 0;
        for(final byte[] bytes : images) {
            final Image image;
            try {
                image = createImage(document, bytes);
            } catch (final IOException | RuntimeException e) {
                throw new IllegalArgumentException("Failed to read image: " + index, e);
            }
            addPage(document, image, options);
            index++;
        }
    }

    public static Image createImage(final PDDocument document, final byte[] bytes) throws IOException {
        if(bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("image is empty");
        }
        if(bytes.length > 2 && (bytes[0] & 0xFF) == 0xFF && (bytes[1] & 0xFF) == 0xD8) {
            return new Image(JPEGFactory.createFromByteArray(document, bytes), jpegOrientation(bytes));
        }
        return new Image(createXObject(document, bytes), 1);
    }

    private static PDImageXObject createXObject(final PDDocument document, final byte[] bytes) throws IOException {
        try {
            return PDImageXObject.createFromByteArray(document, bytes, "image");
        } catch (final IllegalArgumentException | IOException e) {
            // formats PDFBox does not detect itself (e.g. a TIFF that is not CCITT compressed) go through ImageIO
            final BufferedImage buffered = ImageIO.read(new ByteArrayInputStream(bytes));
            if(buffered == null) {
                throw new IllegalArgumentException("unsupported image format", e);
            }
            return LosslessFactory.createFromImage(document, buffered);
        }
    }

    /**
     * The EXIF orientation (tag 0x0112 of IFD0 in the APP1 segment) of a JPEG, 1 when absent or unreadable.
     */
    static int jpegOrientation(final byte[] bytes) {
        int offset = 2;
        while(offset + 4 <= bytes.length && (bytes[offset] & 0xFF) == 0xFF) {
            final int marker = bytes[offset + 1] & 0xFF;
            if(marker == 0xDA || marker == 0xD9) {
                // start of scan / end of image: no more metadata
                return 1;
            }
            final int length = ((bytes[offset + 2] & 0xFF) << 8) | (bytes[offset + 3] & 0xFF);
            final int start = offset + 4;
            if(length < 2 || start + length - 2 > bytes.length) {
                return 1;
            }
            if(marker == 0xE1 && length >= 8 && bytes[start] == 'E' && bytes[start + 1] == 'x' && bytes[start + 2] == 'i'
                    && bytes[start + 3] == 'f' && bytes[start + 4] == 0 && bytes[start + 5] == 0) {
                final int orientation = tiffOrientation(bytes, start + 6, start + length - 2);
                return orientation >= 1 && orientation <= 8 ? orientation : 1;
            }
            offset = start + length - 2;
        }
        return 1;
    }

    private static int tiffOrientation(final byte[] bytes, final int tiff, final int end) {
        if(tiff + 8 > end) {
            return 1;
        }
        final boolean little = bytes[tiff] == 'I' && bytes[tiff + 1] == 'I';
        if(!little && !(bytes[tiff] == 'M' && bytes[tiff + 1] == 'M')) {
            return 1;
        }
        final long ifd = tiff + readInt(bytes, tiff + 4, little);
        if(ifd + 2 > end) {
            return 1;
        }
        final int count = readShort(bytes, (int) ifd, little);
        for(int i = 0; i < count; i++) {
            final int entry = (int) ifd + 2 + i * 12;
            if(entry + 12 > end) {
                return 1;
            }
            if(readShort(bytes, entry, little) == 0x0112) {
                // type SHORT, count 1: the value is in the first two bytes of the value field
                return readShort(bytes, entry + 8, little);
            }
        }
        return 1;
    }

    private static int readShort(final byte[] bytes, final int offset, final boolean little) {
        final int b0 = bytes[offset] & 0xFF;
        final int b1 = bytes[offset + 1] & 0xFF;
        return little ? (b1 << 8) | b0 : (b0 << 8) | b1;
    }

    private static long readInt(final byte[] bytes, final int offset, final boolean little) {
        long value = 0;
        for(int i = 0; i < 4; i++) {
            final int b = bytes[offset + (little ? 3 - i : i)] & 0xFF;
            value = (value << 8) | b;
        }
        return value;
    }

    private static void addPage(final PDDocument document, final Image image, final ImagesOptions options) throws IOException {
        final float margin = options.margin;
        final float imageWidth = image.width() * 72F / options.dpi;
        final float imageHeight = image.height() * 72F / options.dpi;
        final PDRectangle size;
        if(options.pageSize.rectangle == null) {
            size = new PDRectangle(imageWidth + 2 * margin, imageHeight + 2 * margin);
        } else {
            final PDRectangle base = options.pageSize.rectangle;
            final boolean landscape = switch (options.orientation) {
                case portrait -> false;
                case landscape -> true;
                case auto -> image.width() > image.height();
            };
            size = landscape
                    ? new PDRectangle(Math.max(base.getWidth(), base.getHeight()), Math.min(base.getWidth(), base.getHeight()))
                    : new PDRectangle(Math.min(base.getWidth(), base.getHeight()), Math.max(base.getWidth(), base.getHeight()));
        }
        final float areaWidth = size.getWidth() - 2 * margin;
        final float areaHeight = size.getHeight() - 2 * margin;
        if(areaWidth <= 0 || areaHeight <= 0) {
            throw new IllegalArgumentException("margin: " + margin + " leaves no room on the page");
        }
        final float scale = options.pageSize.rectangle == null
                ? 1F
                : Math.min(areaWidth / imageWidth, areaHeight / imageHeight);
        final float width = imageWidth * scale;
        final float height = imageHeight * scale;

        final PDPage page = new PDPage(size);
        document.addPage(page);
        try(final PDPageContentStream content = new PDPageContentStream(document, page)) {
            image.draw(content, margin + (areaWidth - width) / 2, margin + (areaHeight - height) / 2, width, height);
        }
    }

}
