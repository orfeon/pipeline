package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.*;
import ai.onnxruntime.platform.Fp16Conversions;
import com.mercari.solution.util.domain.math.Tensor;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OnnxRuntimeUtil {

    private static final Logger LOG = LoggerFactory.getLogger(OnnxRuntimeUtil.class);

    /** The Tensor element type that holds values of an onnx type. */
    public static Tensor.Type tensorType(final OnnxJavaType type) {
        return switch (type) {
            case FLOAT, FLOAT16, BFLOAT16 -> Tensor.Type.FLOAT;
            case DOUBLE -> Tensor.Type.DOUBLE;
            case INT64 -> Tensor.Type.INT64;
            case INT32 -> Tensor.Type.INT32;
            case INT16 -> Tensor.Type.INT16;
            case INT8, UINT8 -> Tensor.Type.INT8;
            case BOOL -> Tensor.Type.BOOL;
            case STRING -> Tensor.Type.STRING;
            default -> throw new IllegalArgumentException("Not supported onnx tensor type: " + type);
        };
    }

    /** {@code tensor} holds values of {@link #tensorType(OnnxJavaType)} of {@code type}. */
    public static OnnxTensor toOnnxTensor(final OrtEnvironment environment, final Tensor tensor, final OnnxJavaType type) throws OrtException {
        final long[] shape = tensor.shape();
        final int offset = tensor.offset();
        final int size = tensor.size();
        return switch (tensor.data()) {
            case float[] a -> {
                final FloatBuffer buffer = FloatBuffer.wrap(a, offset, size);
                yield switch (type) {
                    case FLOAT16 -> OnnxTensor.createTensor(environment, Fp16Conversions.convertFloatBufferToFp16Buffer(buffer), shape, type);
                    case BFLOAT16 -> OnnxTensor.createTensor(environment, Fp16Conversions.convertFloatBufferToBf16Buffer(buffer), shape, type);
                    default -> OnnxTensor.createTensor(environment, buffer, shape);
                };
            }
            case double[] a -> OnnxTensor.createTensor(environment, DoubleBuffer.wrap(a, offset, size), shape);
            case long[] a -> OnnxTensor.createTensor(environment, LongBuffer.wrap(a, offset, size), shape);
            case int[] a -> OnnxTensor.createTensor(environment, IntBuffer.wrap(a, offset, size), shape);
            case short[] a -> OnnxTensor.createTensor(environment, ShortBuffer.wrap(a, offset, size), shape);
            case byte[] a -> OnnxTensor.createTensor(environment, ByteBuffer.wrap(a, offset, size), shape, type);
            case boolean[] a -> {
                final byte[] bytes = new byte[size];
                for(int i=0; i<size; i++) {
                    bytes[i] = a[offset + i] ? (byte) 1 : (byte) 0;
                }
                yield OnnxTensor.createTensor(environment, ByteBuffer.wrap(bytes), shape, OnnxJavaType.BOOL);
            }
            case String[] a -> OnnxTensor.createTensor(environment, Arrays.copyOfRange(a, offset, offset + size), shape);
            default -> throw new IllegalArgumentException("Not supported tensor: " + tensor);
        };
    }

    /** Copies a model output to a Tensor, which outlives the {@code OrtSession.Result}: uint8 widens to int32, float16 to float. */
    public static Tensor fromOnnxTensor(final OnnxTensor tensor) throws OrtException {
        final long[] shape = tensor.getInfo().getShape();
        final int size = Math.toIntExact(tensor.getInfo().getNumElements());
        return switch (tensor.getInfo().type) {
            case FLOAT, FLOAT16, BFLOAT16 -> {
                final float[] values = new float[size];
                tensor.getFloatBuffer().get(values);
                yield Tensor.of(values, shape);
            }
            case DOUBLE -> {
                final double[] values = new double[size];
                tensor.getDoubleBuffer().get(values);
                yield Tensor.of(values, shape);
            }
            case INT64 -> {
                final long[] values = new long[size];
                tensor.getLongBuffer().get(values);
                yield Tensor.of(values, shape);
            }
            case INT32 -> {
                final int[] values = new int[size];
                tensor.getIntBuffer().get(values);
                yield Tensor.of(values, shape);
            }
            case INT16 -> {
                final short[] values = new short[size];
                tensor.getShortBuffer().get(values);
                yield Tensor.of(values, shape);
            }
            case INT8 -> {
                final byte[] values = new byte[size];
                tensor.getByteBuffer().get(values);
                yield Tensor.of(values, shape);
            }
            case UINT8 -> {
                final ByteBuffer buffer = tensor.getByteBuffer();
                final int[] values = new int[size];
                for(int i=0; i<size; i++) {
                    values[i] = buffer.get(i) & 0xFF;
                }
                yield Tensor.of(values, shape);
            }
            case BOOL -> {
                final ByteBuffer buffer = tensor.getByteBuffer();
                final boolean[] values = new boolean[size];
                for(int i=0; i<size; i++) {
                    values[i] = buffer.get(i) != 0;
                }
                yield Tensor.of(values, shape);
            }
            case STRING -> {
                final List<String> values = new ArrayList<>(size);
                flattenStrings(tensor.getValue(), values);
                yield Tensor.of(values.toArray(new String[0]), shape);
            }
            default -> throw new IllegalArgumentException("Not supported tensor type: " + tensor.getInfo().type);
        };
    }

    private static void flattenStrings(final Object value, final List<String> values) {
        if(value instanceof String[] strings) {
            values.addAll(Arrays.asList(strings));
        } else if(value instanceof Object[] array) {
            for(final Object child : array) {
                flattenStrings(child, values);
            }
        } else {
            values.add((String) value);
        }
    }

    public static List<?> convertMatrix(final OnnxValue onnxValue) throws OrtException {
        switch (onnxValue.getType()) {
            case ONNX_TYPE_TENSOR -> {
                return switch (onnxValue.getValue()) {
                    case Float f -> List.of(f);
                    case float[] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { list.add(v); }
                        yield list;
                    }
                    case float[][] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { list.add(vv); }}
                        yield list;
                    }
                    case float[][][] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) {list.add(vvv); }}}
                        yield list;
                    }
                    case float[][][][] f -> {
                        final List<Float> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) { for(var vvvv : vvv) {list.add(vvvv); }}}}
                        yield list;
                    }
                    case Double f -> List.of(f);
                    case double[] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { list.add(v); }
                        yield list;
                    }
                    case double[][] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { list.add(vv); }}
                        yield list;
                    }
                    case double[][][] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) {list.add(vvv); }}}
                        yield list;
                    }
                    case double[][][][] f -> {
                        final List<Double> list = new ArrayList<>();
                        for(var v : f) { for(var vv : v) { for(var vvv : vv) { for(var vvvv : vvv) {list.add(vvvv); }}}}
                        yield list;
                    }
                    default -> new ArrayList<>();
                };
            }
            default -> throw new RuntimeException();
        }
    }

    public static List<Object> flatten(final List<Object> list, final int rank) {
        Stream<Object> stream = list.stream();
        for(int i=1; i<rank; i++) {
            stream = stream.flatMap(v -> ((List<Object>)v).stream());
        }
        return stream.collect(Collectors.toList());
    }

    public static String getExtensionsLibraryPath() {
        try {
            final Object result = Class.forName("ai.onnxruntime.extensions.OrtxPackage")
                    .getMethod("getLibraryPath")
                    .invoke(null);
            return (String) result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to refer onnxruntime extensions package", e);
        }

    }

    public static KV<Map<String, NodeInfo>, Map<String, NodeInfo>> getNodesInfo(final String model) {
        try(final OrtEnvironment environment = OrtEnvironment.getEnvironment();
            final OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions()) {

            sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT);
            try {
                final String extensionsLibraryPath = getExtensionsLibraryPath();
                sessionOptions.registerCustomOpLibrary(extensionsLibraryPath);
            } catch (Throwable e) {
                LOG.warn("Skip to load extensions library");
            }

            final byte[] bytes = ResourceUtil.readBytes(model);
            try(OrtSession session = environment.createSession(bytes, sessionOptions)) {
                return KV.of(session.getInputInfo(), session.getOutputInfo());
            }
        } catch (OrtException e) {
            throw new RuntimeException("Failed to load model: " + model, e);
        }
    }

    public static String getModelDescription(final OrtSession session) {
        try {
            final JsonObject message = new JsonObject();
            message.addProperty("numInputs", session.getNumInputs());
            message.addProperty("numOutputs", session.getNumOutputs());

            final JsonObject inputInfo = new JsonObject();
            for (final Map.Entry<String, NodeInfo> entry : session.getInputInfo().entrySet()) {
                final JsonObject nodeInfo = new JsonObject();
                nodeInfo.addProperty("name", entry.getValue().getName());
                nodeInfo.addProperty("info", entry.getValue().getInfo().toString());
                inputInfo.add(entry.getKey(), nodeInfo);
            }
            message.add("inputInfo", inputInfo);

            final JsonObject outputInfo = new JsonObject();
            for (final Map.Entry<String, NodeInfo> entry : session.getOutputInfo().entrySet()) {
                final JsonObject nodeInfo = new JsonObject();
                nodeInfo.addProperty("name", entry.getValue().getName());
                nodeInfo.addProperty("info", entry.getValue().getInfo().toString());
                outputInfo.add(entry.getKey(), nodeInfo);
            }
            message.add("outputInfo", outputInfo);

            final JsonObject metadata = new JsonObject();
            metadata.addProperty("domain", session.getMetadata().getDomain());
            metadata.addProperty("description", session.getMetadata().getDescription());
            metadata.addProperty("graphName", session.getMetadata().getGraphName());
            metadata.addProperty("graphDescription", session.getMetadata().getGraphDescription());
            metadata.addProperty("producerName", session.getMetadata().getProducerName());
            metadata.addProperty("version", session.getMetadata().getVersion());
            final JsonObject customMetadata = new JsonObject();
            for (final Map.Entry<String, String> entry : session.getMetadata().getCustomMetadata().entrySet()) {
                customMetadata.addProperty(entry.getKey(), entry.getValue());
            }
            metadata.add("customMetadata", customMetadata);

            message.add("metadata", metadata);

            return message.toString();
        } catch (OrtException e) {
            throw new IllegalStateException(e);
        }

    }

}
