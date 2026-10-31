package com.mercari.solution.util.domain.image;

import com.mercari.solution.util.domain.math.Tensor;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.IndexColorModel;
import java.awt.image.WritableRaster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Images as Tensors, in pure Java. An image is {@code [H, W]} or {@code [H, W, C]}: of integers from 0 to 255, which is what
 * decoding gives (int16) and what resizing rounds to, or of floats with any values. A result keeps the type of its image.
 * {@link #resize} follows the rules of cv2.resize for the size of the cells and the rounding, without its fixed point
 * arithmetic: its values can differ by 1.
 *
 * <p>{@link #cropLine}, {@link #isDark} and {@link #textLines} are ported from {@code indra_ocr/rec_preproc.py} and
 * {@code indra_ocr/det_post.py} of datalab-to/indra (Apache-2.0), changed to work on Tensors.
 */
public final class ImageTensors {

    public enum Interpolation {
        /** area when both dimensions shrink, or else linear. */
        auto,
        area,
        linear
    }

    /** A connected component: its bounding box and its number of pixels. */
    public record Component(int x, int y, int width, int height, int area) {}

    /** A box of a line of text with its mean probability. */
    public record Line(float x0, float y0, float x1, float y1, float score) {}

    private static final int MAX_VALUE = 255;
    // as cv2.cvtColor does, in 14 bits
    private static final int GRAY_R = 4899;
    private static final int GRAY_G = 9617;
    private static final int GRAY_B = 1868;
    private static final int GRAY_SHIFT = 14;

    private ImageTensors() {}

    /**
     * {@code [H, W, 1]} for a grayscale image, whose values are kept as they are, or {@code [H, W, 3]} in RGB. An alpha channel
     * is dropped. An image of more than {@code maxPixels} is read with rows and columns skipped, by the same step.
     */
    public static Tensor decode(final byte[] bytes, final long maxPixels) {
        try(final ImageInputStream stream = new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes))) {
            final Iterator<ImageReader> readers = ImageIO.getImageReaders(stream);
            if(!readers.hasNext()) {
                throw new IllegalArgumentException("Not a readable image of " + bytes.length + " bytes");
            }
            final ImageReader reader = readers.next();
            try {
                reader.setInput(stream, true, true);
                final int width = reader.getWidth(0);
                final int height = reader.getHeight(0);
                final ImageReadParam param = reader.getDefaultReadParam();
                if((long) width * height > maxPixels) {
                    final int step = skipStep(width, height, maxPixels);
                    param.setSourceSubsampling(step, step, 0, 0);
                }
                return fromImage(reader.read(0, param));
            } finally {
                reader.dispose();
            }
        } catch (final IOException e) {
            throw new IllegalArgumentException("Failed to read an image of " + bytes.length + " bytes", e);
        }
    }

    // the square root is not enough for a long and thin image: a side shorter than the step still keeps one row or column
    private static int skipStep(final int width, final int height, final long maxPixels) {
        int step = (int) Math.ceil(Math.sqrt((double) width * height / maxPixels));
        while(step < Math.max(width, height) && (long) Math.ceilDiv(width, step) * Math.ceilDiv(height, step) > maxPixels) {
            step++;
        }
        return step;
    }

    private static Tensor fromImage(final BufferedImage image) {
        final int height = image.getHeight();
        final int width = image.getWidth();
        // getRGB would apply a gamma to the values of a gray image: those of the raster are the ones of the file
        final boolean gray = !(image.getColorModel() instanceof IndexColorModel)
                && image.getColorModel().getColorSpace().getType() == ColorSpace.TYPE_GRAY;
        if(gray) {
            final int shift = image.getRaster().getSampleModel().getSampleSize(0) - 8;
            final short[] data = new short[height * width];
            final int[] row = new int[width];
            for(int y=0; y<height; y++) {
                image.getRaster().getSamples(0, y, width, 1, 0, row);
                for(int x=0; x<width; x++) {
                    data[y * width + x] = (short) (shift > 0 ? row[x] >> shift : row[x]);
                }
            }
            return Tensor.of(data, new long[]{height, width, 1});
        }
        final short[] data = new short[height * width * 3];
        final int[] row = new int[width];
        for(int y=0; y<height; y++) {
            image.getRGB(0, y, width, 1, row, 0, width);
            for(int x=0; x<width; x++) {
                final int position = (y * width + x) * 3;
                data[position] = (short) (row[x] >> 16 & 0xFF);
                data[position + 1] = (short) (row[x] >> 8 & 0xFF);
                data[position + 2] = (short) (row[x] & 0xFF);
            }
        }
        return Tensor.of(data, new long[]{height, width, 3});
    }

    /** PNG of an image of 1 or 3 channels, with its values rounded and limited to 0 to 255. */
    public static byte[] encode(final Tensor image) {
        final int height = height(image);
        final int width = width(image);
        final int channels = channels(image);
        if(channels != 1 && channels != 3) {
            throw new IllegalArgumentException("An image to encode must have 1 or 3 channels, but got: " + image);
        }
        final float[] values = floats(image);
        final BufferedImage buffered = new BufferedImage(width, height, channels == 1 ? BufferedImage.TYPE_BYTE_GRAY : BufferedImage.TYPE_INT_RGB);
        final WritableRaster raster = buffered.getRaster();
        final int[] pixel = new int[channels];
        for(int y=0; y<height; y++) {
            for(int x=0; x<width; x++) {
                for(int c=0; c<channels; c++) {
                    pixel[c] = limit(values[(y * width + x) * channels + c]);
                }
                raster.setPixel(x, y, pixel);
            }
        }
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final MemoryCacheImageOutputStream stream = new MemoryCacheImageOutputStream(bytes)) {
            if(!ImageIO.write(buffered, "png", stream)) {
                throw new IllegalStateException("No PNG writer");
            }
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to write PNG", e);
        }
        return bytes.toByteArray();
    }

    /** {@code [H, W]} of an image. Three channels are 0.299 R + 0.587 G + 0.114 B, as cv2 does. */
    public static Tensor gray(final Tensor image) {
        if(image.rank() == 2) {
            return image;
        }
        final int channels = channels(image);
        if(channels == 1) {
            return image.reshaped(new long[]{height(image), width(image)});
        }
        if(channels != 3) {
            throw new IllegalArgumentException("A gray image is made of 1 or 3 channels, but got: " + image);
        }
        final int size = height(image) * width(image);
        final long[] shape = {height(image), width(image)};
        if(image.type() == Tensor.Type.FLOAT || image.type() == Tensor.Type.DOUBLE) {
            final float[] values = floats(image);
            final float[] gray = new float[size];
            for(int i=0; i<size; i++) {
                gray[i] = 0.299f * values[i * 3] + 0.587f * values[i * 3 + 1] + 0.114f * values[i * 3 + 2];
            }
            return withType(image, gray, shape);
        }
        // from the values as they are: a page of floats would take twice the memory of the page
        final Tensor pixels = image.as(Tensor.Type.INT16);
        final short[] values = (short[]) pixels.data();
        final int offset = pixels.offset();
        final short[] gray = new short[size];
        for(int i=0; i<size; i++) {
            final int p = offset + i * 3;
            gray[i] = (short) ((values[p] * GRAY_R + values[p + 1] * GRAY_G + values[p + 2] * GRAY_B + (1 << (GRAY_SHIFT - 1))) >> GRAY_SHIFT);
        }
        return Tensor.of(gray, shape).as(image.type());
    }

    /** {@code [H, W, channels]} of 1 or 3 channels: three of the same value for a gray image, or the gray of a color one. */
    public static Tensor withChannels(final Tensor image, final int channels) {
        final int current = channels(image);
        if(image.rank() == 3 && current == channels) {
            return image;
        }
        final long height = height(image);
        final long width = width(image);
        if(channels == 1) {
            return gray(image).reshaped(new long[]{height, width, 1});
        }
        if(channels != 3 || current != 1) {
            throw new IllegalArgumentException("An image of 1 channel can have 3, and one of 3 channels can have 1, but got " + channels + " for: " + image);
        }
        final List<Object> values = image.asList();
        final List<Object> repeated = new AbstractList<>() {
            @Override
            public Object get(final int index) {
                return values.get(index / 3);
            }
            @Override
            public int size() {
                return values.size() * 3;
            }
        };
        return Tensor.of(image.type(), repeated, new long[]{height, width, 3});
    }

    /** The size that fits a long side or a short side, rounding as Python does: {@code [height, width]}. */
    public static int[] fittedSize(final int height, final int width, final Integer longSide, final Integer shortSide) {
        final double scale = longSide != null
                ? (double) longSide / Math.max(height, width)
                : (double) shortSide / Math.min(height, width);
        return new int[]{(int) Math.max(1, Math.rint(height * scale)), (int) Math.max(1, Math.rint(width * scale))};
    }

    /** An image of the given size. An image of that size is returned as it is. */
    public static Tensor resize(final Tensor image, final int newHeight, final int newWidth, final Interpolation interpolation) {
        final int height = height(image);
        final int width = width(image);
        if(height == newHeight && width == newWidth) {
            return image;
        }
        final int channels = channels(image);
        final boolean shrinks = newHeight <= height && newWidth <= width;
        final Interpolation used = interpolation == Interpolation.auto ? (shrinks ? Interpolation.area : Interpolation.linear) : interpolation;
        final Weights rows = used == Interpolation.area && shrinks ? Weights.area(height, newHeight) : Weights.linear(height, newHeight, used == Interpolation.area);
        final Weights columns = used == Interpolation.area && shrinks ? Weights.area(width, newWidth) : Weights.linear(width, newWidth, used == Interpolation.area);

        final float[] source = floats(image);
        final float[] horizontal = new float[height * newWidth * channels];
        for(int y=0; y<height; y++) {
            final int from = y * width * channels;
            final int to = y * newWidth * channels;
            for(int x=0; x<newWidth; x++) {
                for(int w=columns.starts[x]; w<columns.starts[x + 1]; w++) {
                    final float weight = columns.weights[w];
                    final int index = columns.indices[w] * channels;
                    for(int c=0; c<channels; c++) {
                        horizontal[to + x * channels + c] += source[from + index + c] * weight;
                    }
                }
            }
        }
        final int rowSize = newWidth * channels;
        final float[] resized = new float[newHeight * rowSize];
        for(int y=0; y<newHeight; y++) {
            final int to = y * rowSize;
            for(int w=rows.starts[y]; w<rows.starts[y + 1]; w++) {
                final float weight = rows.weights[w];
                final int from = rows.indices[w] * rowSize;
                for(int i=0; i<rowSize; i++) {
                    resized[to + i] += horizontal[from + i] * weight;
                }
            }
        }
        return withType(image, resized, shape(image, newHeight, newWidth));
    }

    /** Pads at the bottom and the right: to at least the given size, then up to a multiple of {@code multiple}. */
    public static Tensor pad(final Tensor image, final int minHeight, final int minWidth, final int multiple, final float value) {
        final int height = height(image);
        final int width = width(image);
        final int channels = channels(image);
        final int newHeight = Math.ceilDiv(Math.max(height, minHeight), multiple) * multiple;
        final int newWidth = Math.ceilDiv(Math.max(width, minWidth), multiple) * multiple;
        if(newHeight == height && newWidth == width) {
            return image;
        }
        final float[] source = floats(image);
        final float[] padded = new float[newHeight * newWidth * channels];
        Arrays.fill(padded, value);
        for(int y=0; y<height; y++) {
            System.arraycopy(source, y * width * channels, padded, y * newWidth * channels, width * channels);
        }
        return withType(image, padded, shape(image, newHeight, newWidth));
    }

    /**
     * The float values a model takes: {@code value * scale}, then {@code 1 - value} if {@code invert}, then
     * {@code (value - mean) / std}, with a mean and a std of one value or one per channel. {@code channelFirst} gives
     * {@code [C, H, W]}, or else {@code [H, W, C]}.
     */
    public static Tensor toModel(final Tensor image, final float scale, final boolean invert, final float[] mean, final float[] std, final boolean channelFirst) {
        final int height = height(image);
        final int width = width(image);
        final int channels = channels(image);
        for(final float[] values : new float[][]{mean, std}) {
            if(values != null && values.length != 1 && values.length != channels) {
                throw new IllegalArgumentException("mean and std must have one value or one per channel, but got " + values.length + " for " + image);
            }
        }
        final float[] source = floats(image);
        final float[] data = new float[height * width * channels];
        for(int p=0; p<height * width; p++) {
            for(int c=0; c<channels; c++) {
                float value = source[p * channels + c] * scale;
                if(invert) {
                    value = 1f - value;
                }
                if(mean != null) {
                    value -= mean[mean.length == 1 ? 0 : c];
                }
                if(std != null) {
                    value /= std[std.length == 1 ? 0 : c];
                }
                data[channelFirst ? c * height * width + p : p * channels + c] = value;
            }
        }
        return Tensor.of(data, channelFirst ? new long[]{channels, height, width} : new long[]{height, width, channels});
    }

    /**
     * A line of a page, {@code height} pixels high, keeping its aspect ratio: the box grows by {@code padY} and {@code padX}
     * times its height on each side, and what is outside the page is {@code fill}. With {@code invertDark} the values of
     * a line of light text on a dark ground are turned over, so that every line is dark text on light paper.
     */
    public static Tensor cropLine(
            final Tensor page,
            final double x0,
            final double y0,
            final double x1,
            final double y1,
            final int height,
            final double padX,
            final double padY,
            final int minWidth,
            final int maxWidth,
            final float fill,
            final boolean invertDark) {

        return cropLines(page, List.of(new double[]{x0, y0, x1, y1}), height, padX, padY, minWidth, maxWidth, fill, invertDark).getFirst();
    }

    /** {@link #cropLine} of each box of x0, y0, x1 and y1, reading the page once. */
    public static List<Tensor> cropLines(
            final Tensor page,
            final List<double[]> boxes,
            final int height,
            final double padX,
            final double padY,
            final int minWidth,
            final int maxWidth,
            final float fill,
            final boolean invertDark) {

        final float[] source = floats(page);
        final List<Tensor> crops = new ArrayList<>(boxes.size());
        for(final double[] box : boxes) {
            crops.add(cropLine(page, source, box[0], box[1], box[2], box[3], height, padX, padY, minWidth, maxWidth, fill, invertDark));
        }
        return crops;
    }

    private static Tensor cropLine(
            final Tensor page,
            final float[] source,
            final double x0,
            final double y0,
            final double x1,
            final double y1,
            final int height,
            final double padX,
            final double padY,
            final int minWidth,
            final int maxWidth,
            final float fill,
            final boolean invertDark) {

        final boolean dark = invertDark && isDark(page, source, x0, y0, x1, y1);
        final double boxHeight = Math.max(1d, y1 - y0);
        final int left = (int) Math.floor(x0 - padX * boxHeight);
        final int top = (int) Math.floor(y0 - padY * boxHeight);
        final int cropWidth = Math.max(1, (int) Math.ceil(x1 + padX * boxHeight) - left);
        final int cropHeight = Math.max(1, (int) Math.ceil(y1 + padY * boxHeight) - top);

        final int pageHeight = height(page);
        final int pageWidth = width(page);
        final int channels = channels(page);
        final float[] crop = new float[cropHeight * cropWidth * channels];
        Arrays.fill(crop, dark ? MAX_VALUE - fill : fill);
        final int fromX = Math.max(0, left);
        final int toX = Math.min(pageWidth, left + cropWidth);
        for(int y=Math.max(0, top); y<Math.min(pageHeight, top + cropHeight) && toX > fromX; y++) {
            System.arraycopy(source, (y * pageWidth + fromX) * channels, crop, ((y - top) * cropWidth + fromX - left) * channels, (toX - fromX) * channels);
        }
        final Tensor cropped = withType(page, crop, shape(page, cropHeight, cropWidth));

        final int width = Math.max(minWidth, Math.min(maxWidth, (int) Math.rint((double) cropWidth * height / cropHeight)));
        final Tensor resized = resize(cropped, height, width, cropHeight > height ? Interpolation.area : Interpolation.linear);
        if(!dark) {
            return resized;
        }
        final float[] values = floats(resized);
        final float[] inverted = new float[values.length];
        for(int i=0; i<values.length; i++) {
            inverted[i] = MAX_VALUE - values[i];
        }
        return withType(resized, inverted, resized.shape());
    }

    /**
     * Whether a box has light text on a dark ground: the ground, the median of the box, is nearer the dark end than the text
     * is, so that the bright tail is the longer one.
     */
    public static boolean isDark(final Tensor page, final double x0, final double y0, final double x1, final double y1) {
        return isDark(page, floats(page), x0, y0, x1, y1);
    }

    private static boolean isDark(final Tensor page, final float[] source, final double x0, final double y0, final double x1, final double y1) {
        final int pageHeight = height(page);
        final int pageWidth = width(page);
        final int channels = channels(page);
        if(channels != 1) {
            throw new IllegalArgumentException("Dark lines are found on a gray image, but got: " + page);
        }
        final int fromX = Math.max(0, (int) x0);
        final int toX = Math.min(pageWidth, (int) x1 + 1);
        final int fromY = Math.max(0, (int) y0);
        final int toY = Math.min(pageHeight, (int) y1 + 1);
        if(fromX >= toX || fromY >= toY) {
            return false;
        }
        final int[] cumulative = new int[MAX_VALUE + 1];
        for(int y=fromY; y<toY; y++) {
            for(int x=fromX; x<toX; x++) {
                cumulative[limit(source[y * pageWidth + x])]++;
            }
        }
        for(int v=1; v<=MAX_VALUE; v++) {
            cumulative[v] += cumulative[v - 1];
        }
        final int count = (toY - fromY) * (toX - fromX);
        final double low = percentile(cumulative, count, 0.02);
        final double median = percentile(cumulative, count, 0.5);
        final double high = percentile(cumulative, count, 0.98);
        return high - median > 2 * (median - low) + 8;
    }

    // np.percentile (linear) from a histogram
    private static double percentile(final int[] cumulative, final int count, final double q) {
        final double position = q * (count - 1);
        final int floor = (int) Math.floor(position);
        final double fraction = position - floor;
        final double low = valueAt(cumulative, floor);
        final double high = valueAt(cumulative, Math.min(floor + 1, count - 1));
        final double difference = high - low;
        return fraction >= 0.5 ? high - difference * (1 - fraction) : low + difference * fraction;
    }

    private static int valueAt(final int[] cumulative, final int index) {
        for(int v=0; v<cumulative.length; v++) {
            if(cumulative[v] > index) {
                return v;
            }
        }
        return cumulative.length - 1;
    }

    /** The connected components of the pixels over {@code threshold} of {@code [H, W]}, in the order a scan first meets them. */
    public static List<Component> connectedComponents(final Tensor image, final double threshold, final int connectivity) {
        if(image.rank() != 2) {
            throw new IllegalArgumentException("connected components are found in [H, W], but got: " + image);
        }
        return label(floats(image), height(image), width(image), threshold, connectivity).components;
    }

    /**
     * The boxes of the lines of a page from a model that gives, for {@code [H / stride, W / stride]}, the logit of the band in
     * the middle of a line, and the distances from each pixel of it up to the top of the line and down to its bottom. The
     * bands over {@code threshold} in probability are the lines, of 4-connected pixels; the top and the bottom of a line are the
     * means over its pixels, and its ends are those of the band, which is shorter by {@code xShrink} times the height
     * of the line on each side. The boxes are in pixels of the input of the model.
     *
     * @param output {@code [C, h, w]}, whose channels 0, 1 and 2 are the logit, the distance to the top and that to the bottom
     */
    public static List<Line> textLines(final Tensor output, final double threshold, final int minArea, final int stride, final double xShrink) {
        final long[] shape = output.shape();
        if(shape.length != 3 || shape[0] < 3) {
            throw new IllegalArgumentException("text lines are found in [C, h, w] of at least 3 channels, but got: " + output);
        }
        final int height = Math.toIntExact(shape[1]);
        final int width = Math.toIntExact(shape[2]);
        final int size = height * width;
        final float[] values = floats(output);
        final float[] probabilities = new float[size];
        for(int i=0; i<size; i++) {
            probabilities[i] = (float) (1d / (1d + Math.exp(-values[i])));
        }
        final Labels labels = label(probabilities, height, width, threshold, 4);
        final int count = labels.components.size();
        final double[] tops = new double[count];
        final double[] bottoms = new double[count];
        final double[] scores = new double[count];
        for(int y=0; y<height; y++) {
            final double center = (y + 0.5) * stride;
            for(int x=0; x<width; x++) {
                final int p = y * width + x;
                final int label = labels.labels[p] - 1;
                if(label >= 0) {
                    tops[label] += center - values[size + p];
                    bottoms[label] += center + values[size * 2 + p];
                    scores[label] += probabilities[p];
                }
            }
        }
        final List<Line> lines = new ArrayList<>();
        for(int i=0; i<count; i++) {
            final Component component = labels.components.get(i);
            if(component.area < minArea) {
                continue;
            }
            final double top = tops[i] / component.area;
            final double bottom = bottoms[i] / component.area;
            final double extension = Math.min(xShrink, 0.5) * Math.max(1d, bottom - top);
            lines.add(new Line(
                    (float) (component.x * stride - extension),
                    (float) top,
                    (float) ((component.x + component.width) * stride + extension),
                    (float) bottom,
                    (float) (scores[i] / component.area)));
        }
        return lines;
    }

    private record Labels(int[] labels, List<Component> components) {}

    // from 1 for each component, with 0 for the other pixels
    private static Labels label(final float[] values, final int height, final int width, final double threshold, final int connectivity) {
        final int[] labels = new int[height * width];
        final int[] stack = new int[height * width];
        final List<Component> components = new ArrayList<>();
        for(int start=0; start<labels.length; start++) {
            if(labels[start] != 0 || !(values[start] > threshold)) {
                continue;
            }
            final int label = components.size() + 1;
            int minX = width, maxX = -1, minY = height, maxY = -1, area = 0;
            int top = 0;
            stack[top++] = start;
            labels[start] = label;
            while(top > 0) {
                final int p = stack[--top];
                final int y = p / width;
                final int x = p - y * width;
                area++;
                minX = Math.min(minX, x);
                maxX = Math.max(maxX, x);
                minY = Math.min(minY, y);
                maxY = Math.max(maxY, y);
                for(int dy=-1; dy<=1; dy++) {
                    for(int dx=-1; dx<=1; dx++) {
                        if(dx == 0 && dy == 0 || connectivity == 4 && dx != 0 && dy != 0) {
                            continue;
                        }
                        final int ny = y + dy;
                        final int nx = x + dx;
                        if(ny < 0 || ny >= height || nx < 0 || nx >= width) {
                            continue;
                        }
                        final int n = ny * width + nx;
                        if(labels[n] == 0 && values[n] > threshold) {
                            labels[n] = label;
                            stack[top++] = n;
                        }
                    }
                }
            }
            components.add(new Component(minX, minY, maxX - minX + 1, maxY - minY + 1, area));
        }
        return new Labels(labels, components);
    }

    // which source values make each value of a dimension, in the order of cv2's tables
    private record Weights(int[] starts, int[] indices, float[] weights) {

        // the overlap of the cell of a new value with the old ones
        static Weights area(final int size, final int newSize) {
            final double scale = 1d / ((double) newSize / size);
            final List<int[]> indices = new ArrayList<>(newSize);
            final List<float[]> weights = new ArrayList<>(newSize);
            int total = 0;
            for(int d=0; d<newSize; d++) {
                final double from = d * scale;
                final double to = from + scale;
                final double cell = Math.min(scale, size - from);
                final int last = Math.min((int) Math.floor(to), size - 1);
                final int first = Math.min((int) Math.ceil(from), last);
                final int capacity = last - first + 2;
                final int[] index = new int[capacity];
                final float[] weight = new float[capacity];
                int n = 0;
                if(first - from > 1e-3) {
                    index[n] = first - 1;
                    weight[n++] = (float) ((first - from) / cell);
                }
                for(int s=first; s<last; s++) {
                    index[n] = s;
                    weight[n++] = (float) (1d / cell);
                }
                if(to - last > 1e-3) {
                    index[n] = last;
                    weight[n++] = (float) (Math.min(Math.min(to - last, 1d), cell) / cell);
                }
                indices.add(Arrays.copyOf(index, n));
                weights.add(Arrays.copyOf(weight, n));
                total += n;
            }
            return flatten(indices, weights, total);
        }

        // two old values; areaMode is what cv2 does for INTER_AREA when a dimension grows
        static Weights linear(final int size, final int newSize, final boolean areaMode) {
            final double inverse = (double) newSize / size;
            final double scale = 1d / inverse;
            final List<int[]> indices = new ArrayList<>(newSize);
            final List<float[]> weights = new ArrayList<>(newSize);
            int total = 0;
            for(int d=0; d<newSize; d++) {
                int source;
                float fraction;
                if(areaMode) {
                    source = (int) Math.floor(d * scale);
                    final float position = (float) ((d + 1) - (source + 1) * inverse);
                    fraction = position <= 0 ? 0f : position - (float) Math.floor(position);
                } else {
                    final float position = (float) ((d + 0.5) * scale - 0.5);
                    source = (int) Math.floor(position);
                    fraction = position - source;
                }
                if(source < 0) {
                    source = 0;
                    fraction = 0f;
                }
                if(source >= size - 1) {
                    source = size - 1;
                    fraction = 0f;
                }
                if(fraction == 0f) {
                    indices.add(new int[]{source});
                    weights.add(new float[]{1f});
                    total += 1;
                } else {
                    indices.add(new int[]{source, source + 1});
                    weights.add(new float[]{1f - fraction, fraction});
                    total += 2;
                }
            }
            return flatten(indices, weights, total);
        }

        private static Weights flatten(final List<int[]> indices, final List<float[]> weights, final int total) {
            final int[] starts = new int[indices.size() + 1];
            final int[] flatIndices = new int[total];
            final float[] flatWeights = new float[total];
            for(int d=0; d<indices.size(); d++) {
                final int length = indices.get(d).length;
                System.arraycopy(indices.get(d), 0, flatIndices, starts[d], length);
                System.arraycopy(weights.get(d), 0, flatWeights, starts[d], length);
                starts[d + 1] = starts[d] + length;
            }
            return new Weights(starts, flatIndices, flatWeights);
        }
    }

    private static float[] floats(final Tensor tensor) {
        final float[] values = new float[tensor.size()];
        if(tensor.data() instanceof float[] data) {
            System.arraycopy(data, tensor.offset(), values, 0, values.length);
        } else if(tensor.data() instanceof short[] data) {
            for(int i=0; i<values.length; i++) {
                values[i] = data[tensor.offset() + i];
            }
        } else {
            for(int i=0; i<values.length; i++) {
                values[i] = (float) tensor.getDouble(i);
            }
        }
        return values;
    }

    // of the type of the image: integers are rounded as cv2 does, half to even, and limited to the range of a pixel
    private static Tensor withType(final Tensor image, final float[] values, final long[] shape) {
        if(image.type() == Tensor.Type.FLOAT || image.type() == Tensor.Type.DOUBLE) {
            return Tensor.of(values, shape).as(image.type());
        }
        final short[] data = new short[values.length];
        for(int i=0; i<values.length; i++) {
            data[i] = (short) limit(values[i]);
        }
        return Tensor.of(data, shape).as(image.type());
    }

    private static int limit(final float value) {
        return (int) Math.max(0d, Math.min(MAX_VALUE, Math.rint(value)));
    }

    private static long[] shape(final Tensor image, final int height, final int width) {
        return image.rank() == 2 ? new long[]{height, width} : new long[]{height, width, channels(image)};
    }

    public static int height(final Tensor image) {
        requireImage(image);
        return Math.toIntExact(image.shape()[0]);
    }

    public static int width(final Tensor image) {
        requireImage(image);
        return Math.toIntExact(image.shape()[1]);
    }

    public static int channels(final Tensor image) {
        requireImage(image);
        return image.rank() == 2 ? 1 : Math.toIntExact(image.shape()[2]);
    }

    private static void requireImage(final Tensor image) {
        if(image.rank() != 2 && image.rank() != 3) {
            throw new IllegalArgumentException("An image must be [H, W] or [H, W, C], but got: " + image);
        }
    }

}
