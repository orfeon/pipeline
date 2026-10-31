package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.image.ImageTensors;
import com.mercari.solution.util.domain.math.Tensor;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImageFunctionsTest {

    private static final Schema.FieldType FLOAT32 = Schema.FieldType.FLOAT32.withNullable(false);
    private static final Schema.FieldType INT16 = Schema.FieldType.type(Schema.Type.int16).withNullable(false);

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("content", Schema.FieldType.BYTES),
            Schema.Field.of("out", Schema.FieldType.matrix(FLOAT32, List.of(-1, -1, -1))),
            Schema.Field.of("page", Schema.FieldType.matrix(INT16, List.of(-1, -1))),
            Schema.Field.of("small", Schema.FieldType.matrix(INT16, List.of(-1, -1))));

    private static ImageFunctions function(final String json, final ImageFunctions.Op op) {
        return ImageFunctions.of("f", JsonParser.parseString(json).getAsJsonObject(), FIELDS, op, false);
    }

    // a band in rows 1 and 2 of [3, 4, 6], columns 1 to 4: a line from 1 to 9 and from 0.8 to 11.2 in the model input
    private static Tensor output() {
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

    private static Tensor empty(final int height, final int width) {
        return Tensor.of(new short[height * width], new long[]{height, width});
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> box(final Tensor page, final Tensor small) {
        final ImageFunctions function = function("{ field: out, imageField: page, resizedField: small, stride: 2, xShrink: 0.15 }", ImageFunctions.Op.text_line_boxes);
        final Map<String, Object> values = new HashMap<>();
        values.put("out", output());
        values.put("page", page);
        values.put("small", small);
        final List<Map<String, Object>> boxes = (List<Map<String, Object>>) function.apply(values, Instant.EPOCH);
        Assertions.assertEquals(1, boxes.size());
        return boxes.getFirst();
    }

    private static void assertBox(final Map<String, Object> box, final float x0, final float y0, final float x1, final float y1) {
        Assertions.assertEquals(x0, (float) box.get("x0"), 1e-4f);
        Assertions.assertEquals(y0, (float) box.get("y0"), 1e-4f);
        Assertions.assertEquals(x1, (float) box.get("x1"), 1e-4f);
        Assertions.assertEquals(y1, (float) box.get("y1"), 1e-4f);
    }

    @Test
    public void testBoxesInThePixelsOfTheImage() {
        assertBox(box(empty(40, 60), empty(40, 60)), 0.8f, 1f, 11.2f, 9f);
        // half the size in both dimensions, however it was asked for
        assertBox(box(empty(40, 60), empty(20, 30)), 1.6f, 2f, 22.4f, 18f);
        // a size that does not keep the ratio: the rows by 4, the columns by 2
        assertBox(box(empty(40, 60), empty(10, 30)), 1.6f, 4f, 22.4f, 36f);
        // the height is rounded from 3.5 to 4: the ratio is kept, with the scale of the long side and not 4 / 7
        assertBox(box(empty(7, 20), empty(4, 10)), 1.6f, 2f, 20f, 7f);
        // limited by the image
        assertBox(box(empty(20, 15), empty(10, 5)), 2.4f, 2f, 15f, 18f);
    }

    @Test
    public void testNullImageGivesNoBoxes() {
        final ImageFunctions function = function("{ field: out, imageField: page, resizedField: small, stride: 2, xShrink: 0.15 }", ImageFunctions.Op.text_line_boxes);
        final Map<String, Object> values = new HashMap<>();
        values.put("out", output());
        values.put("page", empty(4, 4));
        Assertions.assertNull(function.apply(values, Instant.EPOCH));
    }

    @Test
    public void testDecodeChannels() {
        final byte[] gray = ImageTensors.encode(Tensor.of(new short[]{7, 8}, new long[]{1, 2}));
        final Map<String, Object> values = new HashMap<>();
        values.put("content", ByteBuffer.wrap(gray));

        final Tensor file = (Tensor) function("{ field: content }", ImageFunctions.Op.image_decode).apply(values, Instant.EPOCH);
        Assertions.assertArrayEquals(new long[]{1, 2, 1}, file.shape(), "what the file has");

        final ImageFunctions three = function("{ field: content, channels: 3 }", ImageFunctions.Op.image_decode);
        Assertions.assertEquals(List.of(-1, -1, 3), three.getOutputFieldType().getShape(), "known when the pipeline is built");
        final Tensor rgb = (Tensor) three.apply(values, Instant.EPOCH);
        Assertions.assertArrayEquals(new long[]{1, 2, 3}, rgb.shape());
        Assertions.assertEquals(List.of(7, 7, 7, 8, 8, 8), rgb.toList());

        // what a model of RGB takes, from a grayscale file, with the mean and the std of each channel
        final List<Schema.Field> fields = List.of(Schema.Field.of("decoded", three.getOutputFieldType()));
        final ImageFunctions tensor = ImageFunctions.of("t", JsonParser.parseString("{ field: decoded, scale: 1, mean: [1, 2, 3], std: [1, 2, 4] }").getAsJsonObject(), fields, ImageFunctions.Op.image_tensor, false);
        final Map<String, Object> decoded = new HashMap<>();
        decoded.put("decoded", rgb);
        final Tensor pixels = (Tensor) tensor.apply(decoded, Instant.EPOCH);
        Assertions.assertArrayEquals(new long[]{3, 1, 2}, pixels.shape());
        Assertions.assertEquals(List.of(6f, 7f, 2.5f, 3f, 1f, 1.25f), pixels.toList());

        final byte[] color = ImageTensors.encode(Tensor.of(new short[]{255, 0, 0}, new long[]{1, 1, 3}));
        values.put("content", ByteBuffer.wrap(color));
        final Tensor one = (Tensor) function("{ field: content, channels: 1 }", ImageFunctions.Op.image_decode).apply(values, Instant.EPOCH);
        Assertions.assertArrayEquals(new long[]{1, 1, 1}, one.shape());
        Assertions.assertEquals(List.of(76), one.toList());
    }

}
