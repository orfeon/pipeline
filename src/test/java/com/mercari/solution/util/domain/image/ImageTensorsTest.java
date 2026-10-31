package com.mercari.solution.util.domain.image;

import com.mercari.solution.util.domain.math.Tensor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ImageTensorsTest {

    private static Tensor image(final int height, final int width, final int... values) {
        final short[] data = new short[values.length];
        for(int i=0; i<values.length; i++) {
            data[i] = (short) values[i];
        }
        return Tensor.of(data, new long[]{height, width});
    }

    private static Tensor fill(final int height, final int width, final int value) {
        final int[] values = new int[height * width];
        Arrays.fill(values, value);
        return image(height, width, values);
    }

    private static List<Object> values(final Tensor tensor) {
        return tensor.toList();
    }

    @Test
    public void testResizeArea() {
        // each new value is the mean of its cell
        final Tensor even = ImageTensors.resize(image(2, 4, 0, 10, 20, 30, 40, 50, 60, 70), 1, 2, ImageTensors.Interpolation.area);
        Assertions.assertArrayEquals(new long[]{1, 2}, even.shape());
        Assertions.assertEquals(List.of(25, 45), values(even));

        // cells over part of a value: (0 + 30 * 0.5) / 1.5 and (30 * 0.5 + 60) / 1.5
        final Tensor odd = ImageTensors.resize(image(1, 3, 0, 30, 60), 1, 2, ImageTensors.Interpolation.area);
        Assertions.assertEquals(List.of(10, 50), values(odd));
    }

    @Test
    public void testResizeLinear() {
        // the centers of the new pixels on the old ones: an end repeats its value
        final Tensor resized = ImageTensors.resize(image(1, 2, 0, 100), 1, 4, ImageTensors.Interpolation.linear);
        Assertions.assertEquals(List.of(0, 25, 75, 100), values(resized));
    }

    @Test
    public void testResizeAuto() {
        final Tensor page = image(1, 4, 0, 100, 0, 100);
        Assertions.assertEquals(List.of(50, 50), values(ImageTensors.resize(page, 1, 2, ImageTensors.Interpolation.auto)), "area when it shrinks");
        Assertions.assertEquals(List.of(0, 25, 75, 100), values(ImageTensors.resize(image(1, 2, 0, 100), 1, 4, ImageTensors.Interpolation.auto)), "linear when it grows");
        Assertions.assertSame(page, ImageTensors.resize(page, 1, 4, ImageTensors.Interpolation.auto), "the same size is the same image");
    }

    @Test
    public void testResizeAreaWhenADimensionGrows() {
        // not a cell any more, as cv2 goes on with weights from the position of the next pixel
        final Tensor resized = ImageTensors.resize(image(2, 2, 0, 100, 0, 100), 1, 4, ImageTensors.Interpolation.area);
        Assertions.assertArrayEquals(new long[]{1, 4}, resized.shape());
        Assertions.assertEquals(List.of(0, 0, 100, 100), values(resized));
    }

    @Test
    public void testResizeKeepsChannelsAndType() {
        final Tensor rgb = Tensor.of(new short[]{0, 10, 20, 100, 110, 120}, new long[]{1, 2, 3});
        final Tensor resized = ImageTensors.resize(rgb, 1, 1, ImageTensors.Interpolation.area);
        Assertions.assertArrayEquals(new long[]{1, 1, 3}, resized.shape());
        Assertions.assertEquals(List.of(50, 60, 70), values(resized));

        final Tensor real = ImageTensors.resize(Tensor.of(new float[]{0f, 1f}, new long[]{1, 2}), 1, 1, ImageTensors.Interpolation.area);
        Assertions.assertEquals(Tensor.Type.FLOAT, real.type());
        Assertions.assertEquals(List.of(0.5f), values(real), "a float image is not rounded");
    }

    @Test
    public void testOtherTypesKeepTheirType() {
        // floats of 64 bits are floats: not rounded to integers, as the output of a model can have values below 1
        final Tensor real = Tensor.of(new double[]{0.25d, 0.75d, 0.5d, 0.5d}, new long[]{2, 2});
        final Tensor resized = ImageTensors.resize(real, 1, 1, ImageTensors.Interpolation.area);
        Assertions.assertEquals(Tensor.Type.DOUBLE, resized.type());
        Assertions.assertEquals(0.5d, (double) resized.get(0), 1e-6);
        final Tensor padded = ImageTensors.pad(real, 2, 3, 1, 0.5f);
        Assertions.assertEquals(Tensor.Type.DOUBLE, padded.type());
        Assertions.assertEquals(List.of(0.25d, 0.75d, 0.5d, 0.5d, 0.5d, 0.5d), values(padded));
        final Tensor gray = ImageTensors.gray(Tensor.of(new double[]{0.5d, 0.5d, 0.5d}, new long[]{1, 1, 3}));
        Assertions.assertEquals(Tensor.Type.DOUBLE, gray.type());
        Assertions.assertEquals(0.5d, (double) gray.get(0), 1e-6);

        // integers are of a pixel, in their own type: a record field of int64 holds a long
        final Tensor longs = ImageTensors.resize(Tensor.of(new long[]{0L, 100L}, new long[]{1, 2}), 1, 1, ImageTensors.Interpolation.area);
        Assertions.assertEquals(Tensor.Type.INT64, longs.type());
        Assertions.assertEquals(List.of(50L), values(longs));
        final Tensor ints = ImageTensors.gray(Tensor.of(new int[]{255, 0, 0}, new long[]{1, 1, 3}));
        Assertions.assertEquals(Tensor.Type.INT32, ints.type());
        Assertions.assertEquals(List.of(76), values(ints));
    }

    @Test
    public void testGrayOfARowOfABatch() {
        final Tensor batch = Tensor.of(new short[]{9, 9, 9, 255, 0, 0}, new long[]{2, 1, 1, 3});
        Assertions.assertEquals(List.of(76), values(ImageTensors.gray(batch.row(1))));
    }

    @Test
    public void testFittedSize() {
        Assertions.assertArrayEquals(new int[]{512, 1024}, ImageTensors.fittedSize(1000, 2000, 1024, null));
        Assertions.assertArrayEquals(new int[]{1024, 2048}, ImageTensors.fittedSize(1000, 2000, null, 1024));
        // as Python rounds: 2.5 to 2, 3.5 to 4
        Assertions.assertArrayEquals(new int[]{2, 10}, ImageTensors.fittedSize(5, 20, 10, null));
        Assertions.assertArrayEquals(new int[]{4, 10}, ImageTensors.fittedSize(7, 20, 10, null));
        Assertions.assertArrayEquals(new int[]{1, 10}, ImageTensors.fittedSize(1, 1000, 10, null), "at least 1");
    }

    @Test
    public void testGray() {
        // the values of cv2 for pure colors
        final Tensor gray = ImageTensors.gray(Tensor.of(new short[]{255, 0, 0, 0, 255, 0, 0, 0, 255}, new long[]{1, 3, 3}));
        Assertions.assertArrayEquals(new long[]{1, 3}, gray.shape());
        Assertions.assertEquals(List.of(76, 150, 29), values(gray));

        final Tensor one = ImageTensors.gray(Tensor.of(new short[]{7, 8}, new long[]{1, 2, 1}));
        Assertions.assertArrayEquals(new long[]{1, 2}, one.shape());
        Assertions.assertEquals(List.of(7, 8), values(one));
    }

    @Test
    public void testWithChannels() {
        final Tensor one = Tensor.of(new short[]{7, 8}, new long[]{1, 2, 1});
        final Tensor three = ImageTensors.withChannels(one, 3);
        Assertions.assertArrayEquals(new long[]{1, 2, 3}, three.shape());
        Assertions.assertEquals(List.of(7, 7, 7, 8, 8, 8), values(three));
        Assertions.assertEquals(Tensor.Type.INT16, three.type());
        Assertions.assertSame(one, ImageTensors.withChannels(one, 1));

        final Tensor color = Tensor.of(new short[]{255, 0, 0}, new long[]{1, 1, 3});
        Assertions.assertSame(color, ImageTensors.withChannels(color, 3));
        final Tensor gray = ImageTensors.withChannels(color, 1);
        Assertions.assertArrayEquals(new long[]{1, 1, 1}, gray.shape());
        Assertions.assertEquals(List.of(76), values(gray));

        Assertions.assertArrayEquals(new long[]{1, 2, 3}, ImageTensors.withChannels(image(1, 2, 7, 8), 3).shape(), "of [H, W]");
        Assertions.assertArrayEquals(new long[]{1, 2, 1}, ImageTensors.withChannels(image(1, 2, 7, 8), 1).shape(), "of [H, W]");
        Assertions.assertThrows(IllegalArgumentException.class, () -> ImageTensors.withChannels(Tensor.of(new short[]{1, 2}, new long[]{1, 1, 2}), 3));
    }

    @Test
    public void testPad() {
        final Tensor padded = ImageTensors.pad(image(1, 2, 1, 2), 1, 1, 4, 255f);
        Assertions.assertArrayEquals(new long[]{4, 4}, padded.shape());
        Assertions.assertEquals(List.of(1, 2, 255, 255), values(padded).subList(0, 4));
        Assertions.assertEquals(List.of(255, 255, 255, 255), values(padded).subList(12, 16));

        Assertions.assertArrayEquals(new long[]{3, 5}, ImageTensors.pad(image(1, 2, 1, 2), 3, 5, 1, 0f).shape(), "at least a size");
        final Tensor full = image(2, 2, 1, 2, 3, 4);
        Assertions.assertSame(full, ImageTensors.pad(full, 1, 1, 2, 0f));
    }

    @Test
    public void testToModel() {
        // the ink of a page: 1 - value / 255, in one channel first
        final Tensor ink = ImageTensors.toModel(image(1, 2, 0, 255), 1f / 255f, true, null, null, true);
        Assertions.assertArrayEquals(new long[]{1, 1, 2}, ink.shape());
        Assertions.assertEquals(List.of(1f, 0f), values(ink));

        final Tensor rgb = Tensor.of(new short[]{1, 2, 3, 4, 5, 6}, new long[]{1, 2, 3});
        final Tensor chw = ImageTensors.toModel(rgb, 1f, false, new float[]{1f, 2f, 3f}, new float[]{2f}, true);
        Assertions.assertArrayEquals(new long[]{3, 1, 2}, chw.shape());
        Assertions.assertEquals(List.of(0f, 1.5f, 0f, 1.5f, 0f, 1.5f), values(chw));
        final Tensor hwc = ImageTensors.toModel(rgb, 1f, false, null, null, false);
        Assertions.assertArrayEquals(new long[]{1, 2, 3}, hwc.shape());
        Assertions.assertEquals(List.of(1f, 2f, 3f, 4f, 5f, 6f), values(hwc));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ImageTensors.toModel(rgb, 1f, false, new float[]{1f, 2f}, null, true));
    }

    @Test
    public void testEncodeDecode() {
        // the values of a gray image stay as they are: Java's color conversion would apply a gamma to them
        final Tensor gray = image(2, 3, 0, 1, 64, 128, 200, 255);
        final Tensor decodedGray = ImageTensors.decode(ImageTensors.encode(gray), 100);
        Assertions.assertArrayEquals(new long[]{2, 3, 1}, decodedGray.shape());
        Assertions.assertEquals(values(gray), values(decodedGray));

        final Tensor rgb = Tensor.of(new short[]{255, 0, 1, 2, 128, 254}, new long[]{1, 2, 3});
        final Tensor decodedRgb = ImageTensors.decode(ImageTensors.encode(rgb), 100);
        Assertions.assertArrayEquals(new long[]{1, 2, 3}, decodedRgb.shape());
        Assertions.assertEquals(values(rgb), values(decodedRgb));

        Assertions.assertEquals(List.of(0, 255, 3), values(ImageTensors.gray(ImageTensors.decode(
                ImageTensors.encode(Tensor.of(new float[]{-5f, 300f, 2.6f}, new long[]{1, 3})), 100))), "limited and rounded");
    }

    @Test
    public void testDecodeSkipsAboveMaxPixels() {
        final Tensor decoded = ImageTensors.decode(ImageTensors.encode(fill(40, 60, 9)), 100);
        // 2400 pixels for at most 100: a step of 5
        Assertions.assertArrayEquals(new long[]{8, 12, 1}, decoded.shape());

        // a step of 10 from the square root would leave 100 of them: the single row stays whatever the step
        final Tensor thin = ImageTensors.decode(ImageTensors.encode(fill(1, 1000, 9)), 10);
        Assertions.assertArrayEquals(new long[]{1, 10, 1}, thin.shape());
        Assertions.assertArrayEquals(new long[]{1, 1, 1}, ImageTensors.decode(ImageTensors.encode(fill(1, 1000, 9)), 1).shape());
    }

    @Test
    public void testDecodeOtherImages() throws IOException {
        final BufferedImage indexed = new BufferedImage(2, 1, BufferedImage.TYPE_BYTE_BINARY);
        indexed.setRGB(1, 0, 0xFFFFFF);
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Assertions.assertTrue(ImageIO.write(indexed, "png", bytes));
        Assertions.assertEquals(List.of(0, 255), values(ImageTensors.gray(ImageTensors.decode(bytes.toByteArray(), 100))), "two colors");

        final BufferedImage alpha = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB);
        alpha.setRGB(0, 0, 0x80102030);
        final ByteArrayOutputStream alphaBytes = new ByteArrayOutputStream();
        Assertions.assertTrue(ImageIO.write(alpha, "png", alphaBytes));
        Assertions.assertArrayEquals(new long[]{1, 1, 3}, ImageTensors.decode(alphaBytes.toByteArray(), 100).shape(), "the alpha is dropped");

        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ImageTensors.decode(new byte[]{1, 2, 3}, 100));
        Assertions.assertTrue(e.getMessage().contains("Not a readable image"), e.getMessage());
    }

    @Test
    public void testConnectedComponents() {
        final Tensor image = Tensor.of(new float[]{
                1, 0, 0, 1,
                0, 1, 0, 1,
                0, 0, 0, 0,
                1, 1, 1, 0}, new long[]{4, 4});
        Assertions.assertEquals(List.of(
                new ImageTensors.Component(0, 0, 1, 1, 1),
                new ImageTensors.Component(3, 0, 1, 2, 2),
                new ImageTensors.Component(1, 1, 1, 1, 1),
                new ImageTensors.Component(0, 3, 3, 1, 3)), ImageTensors.connectedComponents(image, 0.5, 4), "in the order of a scan");
        Assertions.assertEquals(List.of(
                new ImageTensors.Component(0, 0, 2, 2, 2),
                new ImageTensors.Component(3, 0, 1, 2, 2),
                new ImageTensors.Component(0, 3, 3, 1, 3)), ImageTensors.connectedComponents(image, 0.5, 8), "diagonals connect");
        Assertions.assertEquals(List.of(), ImageTensors.connectedComponents(image, 1, 8), "over the threshold, not at it");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ImageTensors.connectedComponents(Tensor.of(new float[]{1f}, new long[]{1, 1, 1}), 0.5, 8));
    }

    // a band in rows 1 and 2, columns 1 to 4, with channels [3, 4, 6]
    private static Tensor detection() {
        final int height = 4;
        final int width = 6;
        final float[] data = new float[3 * height * width];
        Arrays.fill(data, 0, height * width, -10f);
        for(int y=1; y<=2; y++) {
            for(int x=1; x<=4; x++) {
                final int p = y * width + x;
                data[p] = 10f;
                data[height * width + p] = 3f;
                data[2 * height * width + p] = 5f;
            }
        }
        return Tensor.of(data, new long[]{3, height, width});
    }

    @Test
    public void testTextLines() {
        final List<ImageTensors.Line> lines = ImageTensors.textLines(detection(), 0.5, 2, 2, 0.15);
        Assertions.assertEquals(1, lines.size());
        final ImageTensors.Line line = lines.getFirst();
        // the rows are centered at 3 and 5: the top is the mean of 0 and 2, and the bottom that of 8 and 10
        Assertions.assertEquals(1f, line.y0(), 1e-5f);
        Assertions.assertEquals(9f, line.y1(), 1e-5f);
        // the band is 8 wide from 2: the line goes on by 0.15 of its height of 8 on each side
        Assertions.assertEquals(2f - 1.2f, line.x0(), 1e-5f);
        Assertions.assertEquals(10f + 1.2f, line.x1(), 1e-5f);
        Assertions.assertEquals(1d / (1d + Math.exp(-10d)), line.score(), 1e-5);

        Assertions.assertEquals(List.of(), ImageTensors.textLines(detection(), 0.5, 9, 2, 0.15), "smaller than minArea");
        Assertions.assertEquals(List.of(), ImageTensors.textLines(detection(), 1d, 2, 2, 0.15), "nothing over the threshold");
    }

    @Test
    public void testTextLinesOfARowOfABatch() {
        // what a batch gives: the row of [B, C, h, w] shares its array, from an offset
        final Tensor detection = detection();
        final float[] data = new float[detection.size() * 2];
        Arrays.fill(data, 0, detection.size(), -10f);
        System.arraycopy(detection.data(), 0, data, detection.size(), detection.size());
        final Tensor batch = Tensor.of(data, new long[]{2, 3, 4, 6});
        Assertions.assertEquals(ImageTensors.textLines(detection, 0.5, 2, 2, 0.15), ImageTensors.textLines(batch.row(1), 0.5, 2, 2, 0.15));
        Assertions.assertEquals(List.of(), ImageTensors.textLines(batch.row(0), 0.5, 2, 2, 0.15));
    }

    @Test
    public void testCropLine() {
        final Tensor page = fill(40, 60, 200);
        // a box of 10 x 40 with padding of 2 and 3: the crop is 14 high, 46 wide, and 32 high keeps its ratio: 105.14
        final Tensor crop = ImageTensors.cropLine(page, 10, 10, 50, 20, 32, 0.3, 0.2, 8, 4096, 255f, false);
        Assertions.assertArrayEquals(new long[]{32, 105}, crop.shape());
        Assertions.assertEquals(200, crop.get(0));

        // the same box in a tall window keeps the scale of the text: padding of 16, so 42 high and 96 / 42 * 46
        final Tensor tall = ImageTensors.cropLine(page, 10, 10, 50, 20, 96, 0.3, 1.6, 8, 4096, 255f, false);
        Assertions.assertArrayEquals(new long[]{96, 105}, tall.shape());

        Assertions.assertArrayEquals(new long[]{32, 8}, ImageTensors.cropLine(page, 10, 10, 10.5, 100, 32, 0, 0, 8, 4096, 255f, false).shape(), "minWidth");
        Assertions.assertArrayEquals(new long[]{32, 50}, ImageTensors.cropLine(page, 0, 10, 60, 11, 32, 0, 0, 8, 50, 255f, false).shape(), "maxWidth");
    }

    @Test
    public void testCropLineOutsideThePage() {
        final Tensor page = fill(4, 4, 100);
        // the box starts 2 before the page: the first 2 of 6 are the fill
        final Tensor crop = ImageTensors.cropLine(page, -2, 0, 4, 4, 4, 0, 0, 1, 100, 255f, false);
        Assertions.assertEquals(List.of(255, 255, 100, 100, 100, 100), values(crop).subList(0, 6));

        final Tensor gone = ImageTensors.cropLine(page, 10, 10, 14, 14, 4, 0, 0, 1, 100, 255f, false);
        Assertions.assertEquals(255, gone.get(0), "all of it");
    }

    @Test
    public void testDarkLines() {
        // light text on a dark ground: more than the last 2 percent of the box are bright
        final int[] values = new int[100];
        Arrays.fill(values, 20);
        for(int i=90; i<100; i++) {
            values[i] = 240;
        }
        final Tensor dark = image(10, 10, values);
        Assertions.assertTrue(ImageTensors.isDark(dark, 0, 0, 10, 10));

        // the crop is dark text on light paper, and what is outside the page is paper
        final Tensor crop = ImageTensors.cropLine(dark, 0, 0, 10, 10, 10, 0, 0, 1, 100, 255f, true);
        Assertions.assertEquals(235, crop.get(0));
        Assertions.assertEquals(15, crop.get(99));
        final Tensor wider = ImageTensors.cropLine(dark, -2, 0, 10, 10, 10, 0, 0, 1, 100, 255f, true);
        Assertions.assertEquals(255, wider.get(0));

        Assertions.assertEquals(20, ImageTensors.cropLine(dark, 0, 0, 10, 10, 10, 0, 0, 1, 100, 255f, false).get(0), "only when asked");

        // dark text on light paper, and a shadowed line of it
        final int[] light = new int[100];
        for(int i=0; i<100; i++) {
            light[i] = i < 90 ? 230 : 10;
        }
        Assertions.assertFalse(ImageTensors.isDark(image(10, 10, light), 0, 0, 10, 10));
        for(int i=0; i<100; i++) {
            light[i] = i < 90 ? 90 : 10;
        }
        Assertions.assertFalse(ImageTensors.isDark(image(10, 10, light), 0, 0, 10, 10));
        Assertions.assertFalse(ImageTensors.isDark(dark, 20, 20, 30, 30), "a box outside the page");
    }

}
