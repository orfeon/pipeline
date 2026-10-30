package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxJavaType;
import ai.onnxruntime.TensorInfo;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

/**
 * The inputs and outputs of an ONNX model, read from the graph of the {@code .onnx} file without creating a session:
 * pipeline assembly needs neither the native runtime nor the weights ({@code *.onnx_data} is never read).
 * Names, types and shapes are those {@code OrtSession.getInputInfo} / {@code getOutputInfo} give, as long as the graph
 * declares them: see {@link #isComplete()}.
 */
public record OnnxGraphInfo(Map<String, ValueInfo> inputs, Map<String, ValueInfo> outputs) implements Serializable {

    /**
     * @param type  element type of a tensor, null for another value (sequence, map, optional, sparse tensor) or a tensor
     *              of a type that is not supported (complex)
     * @param shape dimensions, -1 for a dynamic one. Null if the graph does not declare them: {@link #rank()} needs a shape
     */
    public record ValueInfo(String name, OnnxJavaType type, long[] shape) implements Serializable {

        public boolean isTensor() {
            return type != null;
        }

        public int rank() {
            return shape.length;
        }

        @Override
        public String toString() {
            return name + ":" + (isTensor() ? type + Arrays.toString(shape) : "non-tensor");
        }
    }

    /** Whether every value declares its type and shape. A session infers what is left out, this reader does not. */
    public boolean isComplete() {
        return Stream.concat(inputs.values().stream(), outputs.values().stream()).allMatch(info -> info.shape() != null);
    }

    /** From what a session reports: for a graph that is not {@link #isComplete() complete}. */
    public static OnnxGraphInfo of(final Map<String, NodeInfo> inputs, final Map<String, NodeInfo> outputs) {
        return new OnnxGraphInfo(valueInfos(inputs), valueInfos(outputs));
    }

    private static Map<String, ValueInfo> valueInfos(final Map<String, NodeInfo> nodes) {
        final Map<String, ValueInfo> infos = new LinkedHashMap<>();
        for(final Map.Entry<String, NodeInfo> node : nodes.entrySet()) {
            final String name = node.getKey();
            infos.put(name, node.getValue().getInfo() instanceof TensorInfo tensor && tensor.type != OnnxJavaType.UNKNOWN
                    ? new ValueInfo(name, tensor.type, tensor.getShape())
                    : new ValueInfo(name, null, new long[0]));
        }
        return Collections.unmodifiableMap(infos);
    }

    // field numbers of onnx.proto
    private static final int MODEL_GRAPH = 7;
    private static final int GRAPH_INITIALIZER = 5;
    private static final int GRAPH_INPUT = 11;
    private static final int GRAPH_OUTPUT = 12;
    private static final int GRAPH_SPARSE_INITIALIZER = 15;
    private static final int TENSOR_NAME = 8;
    private static final int SPARSE_TENSOR_VALUES = 1;
    private static final int VALUE_INFO_NAME = 1;
    private static final int VALUE_INFO_TYPE = 2;
    private static final int TYPE_TENSOR = 1;
    private static final int TENSOR_TYPE_ELEM_TYPE = 1;
    private static final int TENSOR_TYPE_SHAPE = 2;
    private static final int SHAPE_DIM = 1;
    private static final int DIM_VALUE = 1;

    /** Reads the stream as a stream: the nodes and initializers embedded in the model are skipped, not held in memory. */
    public static OnnxGraphInfo read(final InputStream stream) throws IOException {
        final CodedInputStream input = CodedInputStream.newInstance(stream);
        input.setSizeLimit(Integer.MAX_VALUE);
        OnnxGraphInfo info = null;
        int tag;
        while((tag = input.readTag()) != 0) {
            if(WireFormat.getTagFieldNumber(tag) == MODEL_GRAPH) {
                final int limit = input.pushLimit(input.readRawVarint32());
                info = readGraph(input);
                input.popLimit(limit);
            } else {
                input.skipField(tag);
            }
        }
        if(info == null) {
            throw new IOException("onnx model file has no graph");
        }
        return info;
    }

    private static OnnxGraphInfo readGraph(final CodedInputStream input) throws IOException {
        final Map<String, ValueInfo> inputs = new LinkedHashMap<>();
        final Map<String, ValueInfo> outputs = new LinkedHashMap<>();
        final Set<String> initializers = new HashSet<>();
        int tag;
        while((tag = input.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case GRAPH_INPUT -> {
                    final ValueInfo info = readValueInfo(input);
                    inputs.put(info.name(), info);
                }
                case GRAPH_OUTPUT -> {
                    final ValueInfo info = readValueInfo(input);
                    outputs.put(info.name(), info);
                }
                case GRAPH_INITIALIZER -> initializers.add(readTensorName(input));
                case GRAPH_SPARSE_INITIALIZER -> {
                    final int limit = input.pushLimit(input.readRawVarint32());
                    int sparseTag;
                    while((sparseTag = input.readTag()) != 0) {
                        if(WireFormat.getTagFieldNumber(sparseTag) == SPARSE_TENSOR_VALUES) {
                            initializers.add(readTensorName(input));
                        } else {
                            input.skipField(sparseTag);
                        }
                    }
                    input.popLimit(limit);
                }
                default -> input.skipField(tag);
            }
        }
        // models of an old IR version list their initializers as inputs too: those are not inputs to give
        inputs.keySet().removeAll(initializers);
        return new OnnxGraphInfo(Collections.unmodifiableMap(inputs), Collections.unmodifiableMap(outputs));
    }

    private static String readTensorName(final CodedInputStream input) throws IOException {
        final int limit = input.pushLimit(input.readRawVarint32());
        String name = "";
        int tag;
        while((tag = input.readTag()) != 0) {
            if(WireFormat.getTagFieldNumber(tag) == TENSOR_NAME) {
                name = input.readString();
            } else {
                input.skipField(tag);
            }
        }
        input.popLimit(limit);
        return name;
    }

    private static ValueInfo readValueInfo(final CodedInputStream input) throws IOException {
        final int limit = input.pushLimit(input.readRawVarint32());
        String name = "";
        OnnxJavaType type = null;
        long[] shape = null;
        boolean typed = false;
        boolean tensor = false;
        int tag;
        while((tag = input.readTag()) != 0) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case VALUE_INFO_NAME -> name = input.readString();
                case VALUE_INFO_TYPE -> {
                    typed = true;
                    final int typeLimit = input.pushLimit(input.readRawVarint32());
                    int typeTag;
                    while((typeTag = input.readTag()) != 0) {
                        if(WireFormat.getTagFieldNumber(typeTag) == TYPE_TENSOR) {
                            tensor = true;
                            final int tensorLimit = input.pushLimit(input.readRawVarint32());
                            int tensorTag;
                            while((tensorTag = input.readTag()) != 0) {
                                switch (WireFormat.getTagFieldNumber(tensorTag)) {
                                    case TENSOR_TYPE_ELEM_TYPE -> type = javaType(input.readInt32());
                                    case TENSOR_TYPE_SHAPE -> shape = readShape(input);
                                    default -> input.skipField(tensorTag);
                                }
                            }
                            input.popLimit(tensorLimit);
                        } else {
                            input.skipField(typeTag);
                        }
                    }
                    input.popLimit(typeLimit);
                }
                default -> input.skipField(tag);
            }
        }
        input.popLimit(limit);
        // a value that is not a tensor has no shape to declare
        return new ValueInfo(name, type, typed && !tensor ? new long[0] : shape);
    }

    private static long[] readShape(final CodedInputStream input) throws IOException {
        final int limit = input.pushLimit(input.readRawVarint32());
        final List<Long> dims = new ArrayList<>();
        int tag;
        while((tag = input.readTag()) != 0) {
            if(WireFormat.getTagFieldNumber(tag) == SHAPE_DIM) {
                dims.add(readDim(input));
            } else {
                input.skipField(tag);
            }
        }
        input.popLimit(limit);
        return dims.stream().mapToLong(Long::longValue).toArray();
    }

    // a dim_param or a dim without a value is dynamic
    private static long readDim(final CodedInputStream input) throws IOException {
        final int limit = input.pushLimit(input.readRawVarint32());
        long dim = -1;
        int tag;
        while((tag = input.readTag()) != 0) {
            if(WireFormat.getTagFieldNumber(tag) == DIM_VALUE) {
                dim = input.readInt64();
            } else {
                input.skipField(tag);
            }
        }
        input.popLimit(limit);
        return dim;
    }

    // TensorProto.DataType, mapped as OnnxJavaType.mapFromOnnxTensorType does; null for what that gives UNKNOWN
    private static OnnxJavaType javaType(final int elemType) {
        return switch (elemType) {
            case 1 -> OnnxJavaType.FLOAT;
            case 2 -> OnnxJavaType.UINT8;
            case 3 -> OnnxJavaType.INT8;
            case 4, 5 -> OnnxJavaType.INT16;
            case 6, 12 -> OnnxJavaType.INT32;
            case 7, 13 -> OnnxJavaType.INT64;
            case 8 -> OnnxJavaType.STRING;
            case 9 -> OnnxJavaType.BOOL;
            case 10 -> OnnxJavaType.FLOAT16;
            case 11 -> OnnxJavaType.DOUBLE;
            case 16 -> OnnxJavaType.BFLOAT16;
            default -> null;
        };
    }

}
