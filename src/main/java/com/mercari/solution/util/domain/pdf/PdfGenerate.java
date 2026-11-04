package com.mercari.solution.util.domain.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.image.JPEGFactory;
import org.apache.pdfbox.pdmodel.graphics.image.LosslessFactory;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Creates documents: one page per image. */
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

    /** Adds one page per image (JPEG is embedded as is, other formats losslessly). */
    public static void images(final PDDocument document, final List<byte[]> images, final ImagesOptions options) throws IOException {
        if(images.isEmpty()) {
            throw new IllegalArgumentException("no images to generate a pdf from");
        }
        int index = 0;
        for(final byte[] bytes : images) {
            final PDImageXObject image;
            try {
                image = createImage(document, bytes);
            } catch (final IOException | RuntimeException e) {
                throw new IllegalArgumentException("Failed to read image: " + index, e);
            }
            addPage(document, image, options);
            index++;
        }
    }

    public static PDImageXObject createImage(final PDDocument document, final byte[] bytes) throws IOException {
        if(bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("image is empty");
        }
        if(bytes.length > 2 && (bytes[0] & 0xFF) == 0xFF && (bytes[1] & 0xFF) == 0xD8) {
            return JPEGFactory.createFromByteArray(document, bytes);
        }
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

    private static void addPage(final PDDocument document, final PDImageXObject image, final ImagesOptions options) throws IOException {
        final float margin = options.margin;
        final float imageWidth = image.getWidth() * 72F / options.dpi;
        final float imageHeight = image.getHeight() * 72F / options.dpi;
        final PDRectangle size;
        if(options.pageSize.rectangle == null) {
            size = new PDRectangle(imageWidth + 2 * margin, imageHeight + 2 * margin);
        } else {
            final PDRectangle base = options.pageSize.rectangle;
            final boolean landscape = switch (options.orientation) {
                case portrait -> false;
                case landscape -> true;
                case auto -> image.getWidth() > image.getHeight();
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
            content.drawImage(image, margin + (areaWidth - width) / 2, margin + (areaHeight - height) / 2, width, height);
        }
    }

}
