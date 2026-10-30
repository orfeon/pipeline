package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.*;
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

    public static long[] convertShape(
            final ValueInfo valueInfo,
            final List<Object> values) {

        return switch (valueInfo) {
            case TensorInfo tensorInfo -> {
                final long[] shape = new long[tensorInfo.getShape().length];
                for(int idx = 0; idx < shape.length; idx++) {
                    if(tensorInfo.getShape()[idx] < 0) {
                        if(idx == 0) {
                            shape[idx] = values.size();
                        } else {
                            if(values.isEmpty()) {
                                throw new IllegalArgumentException("values must not be empty to resolve the dynamic dimension " + idx);
                            }
                            // the dynamic dimension idx is the length of the list nested idx levels deep (dim0 is the batch)
                            Object nested = values.getFirst();
                            for(int depth = 1; depth < idx; depth++) {
                                nested = nested instanceof List<?> list && !list.isEmpty() ? list.getFirst() : null;
                            }
                            shape[idx] = switch (nested) {
                                case Object[] o -> o.length;
                                case Collection<?> c -> c.size();
                                case null, default -> throw new IllegalArgumentException("value for tensor dimension " + idx
                                        + " must be an array, but: " + nested);
                            };
                        }
                    } else {
                        shape[idx] = tensorInfo.getShape()[idx];
                    }
                }
                yield shape;
            }
            case MapInfo mapInfo -> {
                throw new IllegalArgumentException("not supported mapInfo: " + mapInfo);
            }
            case SequenceInfo sequenceInfo -> {
                throw new IllegalArgumentException("not supported sequenceInfo: " + sequenceInfo);
            }
            default -> {
                throw new IllegalArgumentException("not supported valueInfo: " + valueInfo);
            }
        };

    }

    public static OnnxTensor convertTensor(
            final OrtEnvironment environment, final TensorInfo tensorInfo, final List<Object> values)
            throws OrtException {

        final long[] shape = convertShape(tensorInfo, values);

        final List<Object> flattenValues = flatten(values, shape.length);
        long size = 1;
        for(final long dim : shape) {
            size *= dim;
        }
        if(flattenValues.size() != size) {
            throw new IllegalArgumentException("values for tensor shape " + Arrays.toString(shape) + " must have " + size
                    + " elements, but got " + flattenValues.size() + " (records in a batch must have arrays of the same length)");
        }
        return switch (tensorInfo.type) {
            case STRING -> {
                final String[] stringValues = new String[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    stringValues[i] = (String) flattenValues.get(i);
                }
                yield OnnxTensor.createTensor(environment, stringValues, shape);
            }
            case BOOL -> {
                final byte[] boolValues = new byte[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    boolValues[i] = (Boolean) flattenValues.get(i) ? (byte) 1 : (byte) 0;
                }
                yield OnnxTensor.createTensor(environment, ByteBuffer.wrap(boolValues), shape, OnnxJavaType.BOOL);
            }
            case INT8 -> {
                final byte[] byteValues = new byte[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    byteValues[i] = ((Number) flattenValues.get(i)).byteValue();
                }
                yield OnnxTensor.createTensor(environment, ByteBuffer.wrap(byteValues), shape);
            }
            case INT16 -> {
                final short[] shortValues = new short[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    shortValues[i] = ((Number) flattenValues.get(i)).shortValue();
                }
                yield OnnxTensor.createTensor(environment, ShortBuffer.wrap(shortValues), shape);
            }
            case INT32 -> {
                final int[] intValues = new int[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    intValues[i] = ((Number) flattenValues.get(i)).intValue();
                }
                yield OnnxTensor.createTensor(environment, IntBuffer.wrap(intValues), shape);
            }
            case INT64 -> {
                final long[] longValues = new long[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    longValues[i] = ((Number) flattenValues.get(i)).longValue();
                }
                yield OnnxTensor.createTensor(environment, LongBuffer.wrap(longValues), shape);
            }
            case FLOAT16, FLOAT -> {
                final float[] floatValues = new float[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    floatValues[i] = ((Number) flattenValues.get(i)).floatValue();
                }
                yield OnnxTensor.createTensor(environment, FloatBuffer.wrap(floatValues), shape);
            }
            case DOUBLE -> {
                final double[] doubleValues = new double[flattenValues.size()];
                for (int i = 0; i < flattenValues.size(); i++) {
                    doubleValues[i] = ((Number) flattenValues.get(i)).doubleValue();
                }
                yield OnnxTensor.createTensor(environment, DoubleBuffer.wrap(doubleValues), shape);
            }
            default -> throw new IllegalArgumentException("Not supported tensor type: " + tensorInfo.type);
        };
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
