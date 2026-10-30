package com.mercari.solution.util.domain.ml.onnx;

import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Tiny ONNX models for tests, serialized as ModelProto by hand (field numbers of onnx.proto) so that no model file
 * or Python tooling is needed. Dimensions given as a String are symbolic (dynamic), a Long is fixed.
 */
public class OnnxTestModels {

    // TensorProto.DataType
    public static final int FLOAT = 1;
    public static final int UINT8 = 2;
    public static final int INT8 = 3;
    public static final int INT16 = 5;
    public static final int INT64 = 7;
    public static final int BOOL = 9;
    public static final int FLOAT16 = 10;
    public static final int DOUBLE = 11;
    public static final int BFLOAT16 = 16;

    private static final int IR_VERSION = 8;
    private static final int OPSET = 13;

    /**
     * {@code x float[batch, 3]} with outputs {@code y = x W + b float[batch, 2]}, {@code s = ReduceSum(x, 1, keepdims)
     * float[batch, 1]} and {@code y3 = Reshape(y, [-1, 2, 1]) float[batch, 2, 1]}. W = [[1, 2], [3, 4], [5, 6]], b = [0.5, -0.5].
     */
    public static byte[] linear() {
        final Graph graph = new Graph("linear");
        graph.input("x", FLOAT, "batch", 3L);
        graph.initializer("W", FLOAT, new long[]{3, 2}, new float[]{1, 2, 3, 4, 5, 6});
        graph.initializer("b", FLOAT, new long[]{2}, new float[]{0.5f, -0.5f});
        graph.initializer("axes", INT64, new long[]{1}, new long[]{1});
        graph.initializer("shape3", INT64, new long[]{3}, new long[]{-1, 2, 1});
        graph.node("MatMul", List.of("x", "W"), List.of("xw"));
        graph.node("Add", List.of("xw", "b"), List.of("y"));
        graph.node("ReduceSum", List.of("x", "axes"), List.of("s"), "keepdims", 1L);
        graph.node("Reshape", List.of("y", "shape3"), List.of("y3"));
        graph.output("y", FLOAT, "batch", 2L);
        graph.output("s", FLOAT, "batch", 1L);
        graph.output("y3", FLOAT, "batch", 2L, 1L);
        return graph.model();
    }

    /**
     * {@code ids int64[batch, length]} and {@code attention_mask int64[batch, length]} with outputs
     * {@code total = sum(ids * mask) float[batch]} and {@code count = sum(mask) int64[batch]}.
     */
    public static byte[] tokens() {
        final Graph graph = new Graph("tokens");
        graph.input("ids", INT64, "batch", "length");
        graph.input("attention_mask", INT64, "batch", "length");
        graph.initializer("axes", INT64, new long[]{1}, new long[]{1});
        graph.node("Mul", List.of("ids", "attention_mask"), List.of("masked"));
        graph.node("Cast", List.of("masked"), List.of("masked_float"), "to", (long) FLOAT);
        graph.node("ReduceSum", List.of("masked_float", "axes"), List.of("total"), "keepdims", 0L);
        graph.node("ReduceSum", List.of("attention_mask", "axes"), List.of("count"), "keepdims", 0L);
        graph.output("total", FLOAT, "batch");
        graph.output("count", INT64, "batch");
        return graph.model();
    }

    /** {@code x <elemType>[batch]} with the output {@code y = Cast(x, float) float[batch]}. */
    public static byte[] cast(final int elemType) {
        final Graph graph = new Graph("cast");
        graph.input("x", elemType, "batch");
        graph.node("Cast", List.of("x"), List.of("y"), "to", (long) FLOAT);
        graph.output("y", FLOAT, "batch");
        return graph.model();
    }

    /** {@code x float[batch]} with the rank-0 output {@code total = ReduceSum(x) float[]}: no batch dimension. */
    public static byte[] scalarOutput() {
        final Graph graph = new Graph("scalar");
        graph.input("x", FLOAT, "batch");
        graph.node("ReduceSum", List.of("x"), List.of("total"), "keepdims", 0L);
        graph.output("total", FLOAT);
        return graph.model();
    }

    /** {@code x float[batch, length]} with the output {@code y = Reshape(x, [0, -1, 2]) float[batch, half, 2]}. */
    public static byte[] dynamicMatrix() {
        final Graph graph = new Graph("dynamic_matrix");
        graph.input("x", FLOAT, "batch", "length");
        graph.initializer("shape", INT64, new long[]{3}, new long[]{0, -1, 2});
        graph.node("Reshape", List.of("x", "shape"), List.of("y"));
        graph.output("y", FLOAT, "batch", "half", 2L);
        return graph.model();
    }

    public static Path write(final Path path, final byte[] model) {
        try {
            Files.createDirectories(path.getParent());
            Files.write(path, model);
            return path;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A path for configs: forward slashes, also on Windows. */
    public static String configPath(final Path path) {
        return path.toAbsolutePath().toString().replace('\\', '/');
    }

    private static class Graph {

        private final String name;
        private final List<byte[]> nodes = new ArrayList<>();
        private final List<byte[]> initializers = new ArrayList<>();
        private final List<byte[]> inputs = new ArrayList<>();
        private final List<byte[]> outputs = new ArrayList<>();

        Graph(final String name) {
            this.name = name;
        }

        void input(final String name, final int elemType, final Object... dims) {
            inputs.add(valueInfo(name, elemType, dims));
        }

        void output(final String name, final int elemType, final Object... dims) {
            outputs.add(valueInfo(name, elemType, dims));
        }

        void initializer(final String name, final int dataType, final long[] dims, final Object data) {
            initializers.add(message(out -> {
                for(final long dim : dims) {
                    out.writeInt64(1, dim);
                }
                out.writeInt32(2, dataType);
                switch (data) {
                    case float[] values -> out.writeByteArray(4, message(o -> { for(final float v : values) o.writeFloatNoTag(v); }));
                    case long[] values -> out.writeByteArray(7, message(o -> { for(final long v : values) o.writeInt64NoTag(v); }));
                    default -> throw new IllegalArgumentException("not supported initializer data: " + data);
                }
                out.writeString(8, name);
            }));
        }

        void node(final String opType, final List<String> nodeInputs, final List<String> nodeOutputs) {
            node(opType, nodeInputs, nodeOutputs, null, null);
        }

        void node(final String opType, final List<String> nodeInputs, final List<String> nodeOutputs,
                  final String attributeName, final Long attributeValue) {
            final int index = nodes.size();
            nodes.add(message(out -> {
                for(final String input : nodeInputs) {
                    out.writeString(1, input);
                }
                for(final String output : nodeOutputs) {
                    out.writeString(2, output);
                }
                out.writeString(3, opType + "_" + index);
                out.writeString(4, opType);
                if(attributeName != null) {
                    // AttributeProto: name = 1, i = 3, type = 20 (INT = 2)
                    out.writeByteArray(5, message(a -> {
                        a.writeString(1, attributeName);
                        a.writeInt64(3, attributeValue);
                        a.writeInt32(20, 2);
                    }));
                }
            }));
        }

        byte[] model() {
            final byte[] graph = message(out -> {
                for(final byte[] node : nodes) out.writeByteArray(1, node);
                out.writeString(2, name);
                for(final byte[] initializer : initializers) out.writeByteArray(5, initializer);
                for(final byte[] input : inputs) out.writeByteArray(11, input);
                for(final byte[] output : outputs) out.writeByteArray(12, output);
            });
            return message(out -> {
                out.writeInt64(1, IR_VERSION);
                out.writeString(2, "mercari-pipeline-test");
                out.writeByteArray(7, graph);
                out.writeByteArray(8, message(o -> {
                    o.writeString(1, "");
                    o.writeInt64(2, OPSET);
                }));
            });
        }

        // ValueInfoProto { name = 1, type = 2 { tensor_type = 1 { elem_type = 1, shape = 2 { dim = 1 { dim_value = 1 | dim_param = 2 }}}}}
        private static byte[] valueInfo(final String name, final int elemType, final Object... dims) {
            final byte[] shape = message(s -> {
                for(final Object dim : dims) {
                    s.writeByteArray(1, message(d -> {
                        switch (dim) {
                            case Long value -> d.writeInt64(1, value);
                            case String param -> d.writeString(2, param);
                            default -> throw new IllegalArgumentException("not supported dim: " + dim);
                        }
                    }));
                }
            });
            final byte[] tensorType = message(t -> {
                t.writeInt32(1, elemType);
                t.writeByteArray(2, shape);
            });
            return message(out -> {
                out.writeString(1, name);
                out.writeByteArray(2, message(t -> t.writeByteArray(1, tensorType)));
            });
        }
    }

    private interface Writer {
        void write(CodedOutputStream out) throws IOException;
    }

    private static byte[] message(final Writer writer) {
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            final CodedOutputStream out = CodedOutputStream.newInstance(bytes);
            writer.write(out);
            out.flush();
            return bytes.toByteArray();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
