package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.image.ImageTensors;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Select functions on images, for the inputs and the outputs of the onnx transform: an image is a Tensor, which only lives
 * there. It leaves as bytes, with {@code image_encode} or {@code image_crop}. A NULL field gives NULL.
 *
 * <pre>{@code
 * { "name": "image",  "func": "image_decode", "field": "content" },
 * { "name": "gray",   "func": "image_gray",   "field": "image" },
 * { "name": "small",  "func": "image_resize", "field": "gray", "longSide": 1024 },
 * { "name": "padded", "func": "image_pad",    "field": "small", "multiple": 32, "value": 255 },
 * { "name": "x",      "func": "image_tensor", "field": "padded", "invert": true },
 * { "name": "boxes",  "func": "text_line_boxes", "field": "output", "imageField": "gray", "longSide": 1024 },
 * { "name": "lines",  "func": "image_crop",   "field": "gray", "boxes": "boxes", "height": 32, "invertDark": true }
 * }</pre>
 */
public class ImageFunctions implements SelectFunction {

    public enum Op implements Serializable {
        image_decode,
        image_encode,
        image_gray,
        image_resize,
        image_pad,
        image_crop,
        image_tensor,
        image_size,
        connected_components,
        text_line_boxes
    }

    private static final long DEFAULT_MAX_PIXELS = 40_000_000L;

    private record Options(
            long maxPixels,
            Integer width,
            Integer height,
            Integer longSide,
            Integer shortSide,
            ImageTensors.Interpolation interpolation,
            int multiple,
            float value,
            String boxes,
            double padX,
            double padY,
            int minWidth,
            int maxWidth,
            boolean invertDark,
            float scale,
            boolean invert,
            float[] mean,
            float[] std,
            boolean channelFirst,
            double threshold,
            int connectivity,
            int minArea,
            int stride,
            double xShrink,
            String imageField) implements Serializable {}

    private final String name;
    private final Op op;
    private final String field;
    private final Options options;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private ImageFunctions(
            final String name,
            final Op op,
            final String field,
            final Options options,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.op = op;
        this.field = field;
        this.options = options;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static ImageFunctions of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final Op op, final boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " requires field parameter");
        }
        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(op == Op.image_decode) {
            if(type.getType() != Schema.Type.bytes) {
                throw new IllegalArgumentException("SelectField: " + name + " func: image_decode field: " + field + " must be bytes, but got: " + type);
            }
        } else if(op == Op.text_line_boxes) {
            requireMatrix(name, op, field, type, 3);
        } else if(op == Op.connected_components) {
            requireMatrix(name, op, field, type, 2);
        } else {
            requireMatrix(name, op, field, type, 2, 3);
        }

        final Map<String, Schema.Field> parents = new LinkedHashMap<>();
        ArrayFunctions.putParent(parents, field, inputFields);

        final Options options = parseOptions(name, jsonObject, op, type, inputFields, parents);
        return new ImageFunctions(name, op, field, options, new ArrayList<>(parents.values()), outputType(op, type), ignore);
    }

    private static Options parseOptions(
            final String name,
            final JsonObject json,
            final Op op,
            final Schema.FieldType type,
            final List<Schema.Field> inputFields,
            final Map<String, Schema.Field> parents) {

        final long maxPixels = json.has("maxPixels") ? json.get("maxPixels").getAsLong() : DEFAULT_MAX_PIXELS;
        if(maxPixels < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " maxPixels must be over than zero: " + maxPixels);
        }
        final Integer width = positive(name, op, json, "width");
        final Integer height = positive(name, op, json, "height");
        final Integer longSide = positive(name, op, json, "longSide");
        final Integer shortSide = positive(name, op, json, "shortSide");
        final ImageTensors.Interpolation interpolation = parseInterpolation(name, op, json);
        final Integer multiple = positive(name, op, json, "multiple");
        final float value = json.has("value") ? json.get("value").getAsFloat() : op == Op.image_crop ? 255f : 0f;
        final Integer minWidthParameter = positive(name, op, json, "minWidth");
        final Integer maxWidthParameter = positive(name, op, json, "maxWidth");
        final int minWidth = minWidthParameter == null ? 8 : minWidthParameter;
        final int maxWidth = maxWidthParameter == null ? 4096 : maxWidthParameter;
        final double padX = json.has("padX") ? json.get("padX").getAsDouble() : 0d;
        final double padY = json.has("padY") ? json.get("padY").getAsDouble() : 0d;
        final boolean invertDark = json.has("invertDark") && json.get("invertDark").getAsBoolean();
        final float scale = json.has("scale") ? json.get("scale").getAsFloat() : 1f / 255f;
        final boolean invert = json.has("invert") && json.get("invert").getAsBoolean();
        final float[] mean = floats(name, json, "mean");
        final float[] std = floats(name, json, "std");
        final boolean channelFirst = !json.has("layout") || parseLayout(name, json.get("layout").getAsString());
        final double threshold = json.has("threshold") ? json.get("threshold").getAsDouble() : 0.5d;
        final int connectivity = json.has("connectivity") ? json.get("connectivity").getAsInt() : 8;
        if(connectivity != 4 && connectivity != 8) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " connectivity must be 4 or 8: " + connectivity);
        }
        final Integer minArea = positive(name, op, json, "minArea");
        final Integer stride = positive(name, op, json, "stride");
        final double xShrink = json.has("xShrink") ? json.get("xShrink").getAsDouble() : 0.15d;

        String boxes = null;
        String imageField = null;
        switch (op) {
            case image_resize -> {
                final int sizes = (longSide != null ? 1 : 0) + (shortSide != null ? 1 : 0) + (width != null || height != null ? 1 : 0);
                if(sizes != 1) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_resize requires one of longSide, shortSide, or width and height");
                }
                if((width == null) != (height == null)) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_resize requires both of width and height");
                }
            }
            case image_pad -> {
                if(multiple == null && width == null && height == null) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_pad requires multiple, width or height");
                }
            }
            case image_crop -> {
                if(height == null) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_crop requires height");
                }
                if(minWidth > maxWidth) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_crop minWidth: " + minWidth + " must not be over maxWidth: " + maxWidth);
                }
                if(!json.has("boxes")) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_crop requires boxes parameter");
                }
                boxes = json.get("boxes").getAsString();
                final Schema.FieldType boxType = ElementSchemaUtil.getInputFieldType(boxes, inputFields);
                if(boxType.getType() != Schema.Type.array || boxType.getArrayValueType().getType() != Schema.Type.element) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_crop boxes: " + boxes + " must be an array of x0, y0, x1, y1, but got: " + boxType);
                }
                final Set<String> names = new HashSet<>();
                boxType.getArrayValueType().getElementSchema().getFields().forEach(f -> names.add(f.getName()));
                if(!names.containsAll(List.of("x0", "y0", "x1", "y1"))) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_crop boxes: " + boxes + " must have x0, y0, x1 and y1, but got: " + names);
                }
                if(invertDark && type.getShape().size() != 2) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_crop invertDark requires a gray image of [H, W], but got: " + type);
                }
                ArrayFunctions.putParent(parents, boxes, inputFields);
            }
            case image_tensor -> validateChannels(name, type, mean, std);
            case text_line_boxes -> {
                if(!json.has("imageField") || longSide == null) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: text_line_boxes requires imageField and longSide, the image before it was resized and the long side it was resized to");
                }
                imageField = json.get("imageField").getAsString();
                requireMatrix(name, op, imageField, ElementSchemaUtil.getInputFieldType(imageField, inputFields), 2, 3);
                ArrayFunctions.putParent(parents, imageField, inputFields);
            }
            default -> {}
        }
        return new Options(maxPixels, width, height, longSide, shortSide, interpolation,
                multiple == null ? 1 : multiple, value, boxes, padX, padY,
                minWidth, maxWidth, invertDark,
                scale, invert, mean, std, channelFirst, threshold, connectivity,
                minArea == null ? (op == Op.text_line_boxes ? 2 : 1) : minArea,
                stride == null ? 2 : stride, xShrink, imageField);
    }

    private static ImageTensors.Interpolation parseInterpolation(final String name, final Op op, final JsonObject json) {
        if(!json.has("interpolation")) {
            return ImageTensors.Interpolation.auto;
        }
        final String interpolation = json.get("interpolation").getAsString();
        try {
            return ImageTensors.Interpolation.valueOf(interpolation);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " interpolation must be one of " + Arrays.toString(ImageTensors.Interpolation.values()) + ": " + interpolation);
        }
    }

    // here as far as the channels are known, rather than on every record. Those of a decoded image are only known then
    private static void validateChannels(final String name, final Schema.FieldType type, final float[] mean, final float[] std) {
        final int channels = type.getShape().size() == 2 ? 1 : type.getShape().get(2);
        for(final float[] values : new float[][]{mean, std}) {
            if(values != null && values.length != 1 && channels > 0 && values.length != channels) {
                throw new IllegalArgumentException("SelectField: " + name + " func: image_tensor mean and std must have one value or one per channel, but got " + values.length + " for: " + type);
            }
        }
        if(std != null) {
            for(final float value : std) {
                if(value == 0f) {
                    throw new IllegalArgumentException("SelectField: " + name + " func: image_tensor std must not be zero");
                }
            }
        }
    }

    private static boolean parseLayout(final String name, final String layout) {
        return switch (layout) {
            case "chw" -> true;
            case "hwc" -> false;
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: image_tensor layout must be chw or hwc: " + layout);
        };
    }

    private static Integer positive(final String name, final Op op, final JsonObject json, final String parameter) {
        if(!json.has(parameter)) {
            return null;
        }
        final int value = json.get(parameter).getAsInt();
        if(value < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " " + parameter + " must be over than zero: " + value);
        }
        return value;
    }

    private static float[] floats(final String name, final JsonObject json, final String parameter) {
        if(!json.has(parameter)) {
            return null;
        }
        final JsonElement element = json.get(parameter);
        if(element.isJsonPrimitive()) {
            return new float[]{element.getAsFloat()};
        }
        if(!element.isJsonArray() || element.getAsJsonArray().isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: image_tensor " + parameter + " must be a number or an array of numbers: " + element);
        }
        final JsonArray array = element.getAsJsonArray();
        final float[] values = new float[array.size()];
        for(int i=0; i<values.length; i++) {
            values[i] = array.get(i).getAsFloat();
        }
        return values;
    }

    private static void requireMatrix(final String name, final Op op, final String field, final Schema.FieldType type, final int... ranks) {
        if(type.getType() != Schema.Type.matrix || Arrays.stream(ranks).noneMatch(rank -> rank == type.getShape().size())) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " field: " + field + " must be a matrix of rank " + Arrays.toString(ranks) + ", but got: " + type);
        }
        // a boolean mask of a model would otherwise fail every record
        if(!Vectors.isNumeric(type.getMatrixValueType())) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " field: " + field + " must be a matrix of numbers, but got values of type: " + type.getMatrixValueType().getType());
        }
    }

    private static Schema.FieldType outputType(final Op op, final Schema.FieldType type) {
        final Schema.FieldType int16 = Schema.FieldType.type(Schema.Type.int16).withNullable(false);
        return switch (op) {
            case image_decode -> Schema.FieldType.matrix(int16, List.of(-1, -1, -1));
            case image_encode -> Schema.FieldType.BYTES;
            case image_gray -> Schema.FieldType.matrix(type.getMatrixValueType(), List.of(-1, -1));
            case image_resize, image_pad -> Schema.FieldType.matrix(type.getMatrixValueType(), dynamicShape(type));
            case image_crop -> Schema.FieldType.array(Schema.FieldType.BYTES.withNullable(false));
            case image_tensor -> Schema.FieldType.matrix(Schema.FieldType.FLOAT32.withNullable(false), List.of(-1, -1, -1));
            case image_size -> Schema.FieldType.element(List.of(
                    Schema.Field.of("height", Schema.FieldType.INT64),
                    Schema.Field.of("width", Schema.FieldType.INT64)));
            case connected_components -> Schema.FieldType.array(Schema.FieldType.element(List.of(
                    Schema.Field.of("x", Schema.FieldType.INT32),
                    Schema.Field.of("y", Schema.FieldType.INT32),
                    Schema.Field.of("width", Schema.FieldType.INT32),
                    Schema.Field.of("height", Schema.FieldType.INT32),
                    Schema.Field.of("area", Schema.FieldType.INT32))));
            case text_line_boxes -> Schema.FieldType.array(Schema.FieldType.element(List.of(
                    Schema.Field.of("x0", Schema.FieldType.FLOAT32),
                    Schema.Field.of("y0", Schema.FieldType.FLOAT32),
                    Schema.Field.of("x1", Schema.FieldType.FLOAT32),
                    Schema.Field.of("y1", Schema.FieldType.FLOAT32),
                    Schema.Field.of("score", Schema.FieldType.FLOAT32))));
        };
    }

    private static List<Integer> dynamicShape(final Schema.FieldType type) {
        final List<Integer> shape = new ArrayList<>(type.getShape());
        shape.set(0, -1);
        shape.set(1, -1);
        return shape;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public boolean acceptsTensor() {
        return true;
    }

    @Override
    public boolean onnxOnly() {
        return true;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        final Object value = ElementSchemaUtil.getValue(input, field);
        if(value == null) {
            return null;
        }
        return switch (op) {
            case image_decode -> ImageTensors.decode(bytes(value), options.maxPixels);
            case image_encode -> ByteBuffer.wrap(ImageTensors.encode(tensor(field, value)));
            case image_gray -> ImageTensors.gray(tensor(field, value));
            case image_resize -> resize(tensor(field, value));
            case image_pad -> ImageTensors.pad(tensor(field, value),
                    options.height == null ? 1 : options.height,
                    options.width == null ? 1 : options.width,
                    options.multiple, options.value);
            case image_crop -> crop(tensor(field, value), ElementSchemaUtil.getValue(input, options.boxes));
            case image_tensor -> ImageTensors.toModel(tensor(field, value), options.scale, options.invert, options.mean, options.std, options.channelFirst);
            case image_size -> {
                final Tensor image = tensor(field, value);
                final Map<String, Object> size = new HashMap<>();
                size.put("height", (long) ImageTensors.height(image));
                size.put("width", (long) ImageTensors.width(image));
                yield size;
            }
            case connected_components -> components(tensor(field, value));
            case text_line_boxes -> lines(tensor(field, value), ElementSchemaUtil.getValue(input, options.imageField));
        };
    }

    private Tensor resize(final Tensor image) {
        if(options.width != null) {
            return ImageTensors.resize(image, options.height, options.width, options.interpolation);
        }
        final int[] size = ImageTensors.fittedSize(ImageTensors.height(image), ImageTensors.width(image), options.longSide, options.shortSide);
        return ImageTensors.resize(image, size[0], size[1], options.interpolation);
    }

    private List<ByteBuffer> crop(final Tensor image, final Object boxes) {
        if(boxes == null) {
            return null;
        }
        final List<double[]> positions = new ArrayList<>();
        for(final Object box : (List<?>) boxes) {
            final Map<?, ?> values = (Map<?, ?>) box;
            positions.add(new double[]{number(values, "x0"), number(values, "y0"), number(values, "x1"), number(values, "y1")});
        }
        final List<ByteBuffer> crops = new ArrayList<>();
        for(final Tensor crop : ImageTensors.cropLines(image, positions, options.height, options.padX, options.padY, options.minWidth, options.maxWidth, options.value, options.invertDark)) {
            crops.add(ByteBuffer.wrap(ImageTensors.encode(crop)));
        }
        return crops;
    }

    private double number(final Map<?, ?> box, final String key) {
        if(!(box.get(key) instanceof Number number)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " box must have a number of " + key + ", but got: " + box);
        }
        return number.doubleValue();
    }

    private List<Map<String, Object>> components(final Tensor image) {
        final List<Map<String, Object>> components = new ArrayList<>();
        for(final ImageTensors.Component component : ImageTensors.connectedComponents(image, options.threshold, options.connectivity)) {
            if(component.area() < options.minArea) {
                continue;
            }
            final Map<String, Object> values = new HashMap<>();
            values.put("x", component.x());
            values.put("y", component.y());
            values.put("width", component.width());
            values.put("height", component.height());
            values.put("area", component.area());
            components.add(values);
        }
        return components;
    }

    // in pixels of the image before it was resized to the long side the model took
    private List<Map<String, Object>> lines(final Tensor output, final Object imageValue) {
        if(imageValue == null) {
            return null;
        }
        final Tensor image = tensor(options.imageField, imageValue);
        final int height = ImageTensors.height(image);
        final int width = ImageTensors.width(image);
        final double scale = (double) options.longSide / Math.max(height, width);
        final List<Map<String, Object>> lines = new ArrayList<>();
        for(final ImageTensors.Line line : ImageTensors.textLines(output, options.threshold, options.minArea, options.stride, options.xShrink)) {
            final Map<String, Object> values = new HashMap<>();
            values.put("x0", clip(line.x0() / scale, width));
            values.put("y0", clip(line.y0() / scale, height));
            values.put("x1", clip(line.x1() / scale, width));
            values.put("y1", clip(line.y1() / scale, height));
            values.put("score", line.score());
            lines.add(values);
        }
        return lines;
    }

    private static float clip(final double value, final int limit) {
        return (float) Math.max(0d, Math.min(limit, value));
    }

    private byte[] bytes(final Object value) {
        return switch (value) {
            case ByteBuffer buffer -> ElementSchemaUtil.toBytes(buffer);
            case byte[] bytes -> bytes;
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " field: " + field + " must be bytes, but got: " + value.getClass());
        };
    }

    private Tensor tensor(final String fieldName, final Object value) {
        if(value instanceof Tensor tensor) {
            return tensor;
        }
        throw new IllegalArgumentException("SelectField: " + name + " func: " + op + " field: " + fieldName + " must be an image in the inputs or the outputs of the onnx transform, but got: " + value.getClass());
    }

}
