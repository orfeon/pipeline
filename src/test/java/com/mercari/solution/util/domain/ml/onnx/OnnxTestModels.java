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
    public static final int INT32 = 6;
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

    /**
     * A text embedding model with an input it does not use: {@code input_ids int64[batch, length]}, {@code attention_mask int64[batch, length]}
     * and {@code image_features float[n, 4]}. The output {@code sentence_embedding float[batch, 2]} is
     * the sum of the attended ids and their number.
     */
    public static byte[] textEmbedding() {
        final Graph graph = new Graph("text_embedding");
        graph.input("input_ids", INT64, "batch", "length");
        graph.input("attention_mask", INT64, "batch", "length");
        graph.input("image_features", FLOAT, "n", 4L);
        graph.initializer("axes", INT64, new long[]{1}, new long[]{1});
        graph.node("Cast", List.of("input_ids"), List.of("ids_float"), "to", (long) FLOAT);
        graph.node("Cast", List.of("attention_mask"), List.of("mask_float"), "to", (long) FLOAT);
        graph.node("Mul", List.of("ids_float", "mask_float"), List.of("masked"));
        graph.node("ReduceSum", List.of("masked", "axes"), List.of("sum"), "keepdims", 1L);
        graph.node("ReduceSum", List.of("mask_float", "axes"), List.of("count"), "keepdims", 1L);
        graph.node("Concat", List.of("sum", "count"), List.of("sentence_embedding"), "axis", 1L);
        graph.output("sentence_embedding", FLOAT, "batch", 2L);
        return graph.model();
    }

    /** {@code x <elemType>[dims...]} with the output {@code y = x}. */
    public static byte[] identity(final int elemType, final Object... dims) {
        final Graph graph = new Graph("identity");
        graph.input("x", elemType, dims);
        graph.node("Identity", List.of("x"), List.of("y"));
        graph.output("y", elemType, dims);
        return graph.model();
    }

    /**
     * A model that decides by the logits of a token: {@code input_ids int64[batch, length]}, {@code attention_mask int64[batch, length]}
     * and {@code past_conv.0 float[batch, 2, n]}, which is not used. The output {@code logits float[batch, 1, 7]} is
     * [0, 0, 0, 1, 2, 3, -1] times the sum of the attended ids.
     */
    public static byte[] decision() {
        final Graph graph = new Graph("decision");
        graph.input("input_ids", INT64, "batch", "length");
        graph.input("attention_mask", INT64, "batch", "length");
        graph.input("past_conv.0", FLOAT, "batch", 2L, "n");
        graph.initializer("axes", INT64, new long[]{1}, new long[]{1});
        graph.initializer("base", FLOAT, new long[]{7}, new float[]{0, 0, 0, 1, 2, 3, -1});
        graph.node("Mul", List.of("input_ids", "attention_mask"), List.of("masked"));
        graph.node("Cast", List.of("masked"), List.of("masked_float"), "to", (long) FLOAT);
        graph.node("ReduceSum", List.of("masked_float", "axes"), List.of("sum"), "keepdims", 1L);
        graph.node("Unsqueeze", List.of("sum", "axes"), List.of("expanded"));
        graph.node("Mul", List.of("expanded", "base"), List.of("logits"));
        graph.output("logits", FLOAT, "batch", 1L, 7L);
        return graph.model();
    }

    /**
     * A forecaster that takes NaN as missing: {@code context float[batch, length]} with the outputs {@code seen = context} and
     * {@code forecasts float[batch, 3, 4]}: the sum of the values that are not NaN times [[1, 2, 3, 4], [10, 20, 30, 40], [100, 200, 300, 400]].
     */
    public static byte[] forecaster() {
        final Graph graph = new Graph("forecaster");
        graph.input("context", FLOAT, "batch", "length");
        graph.initializer("zero", FLOAT, new long[0], new float[]{0});
        graph.initializer("axes", INT64, new long[]{1}, new long[]{1});
        graph.initializer("base", FLOAT, new long[]{3, 4}, new float[]{1, 2, 3, 4, 10, 20, 30, 40, 100, 200, 300, 400});
        graph.node("IsNaN", List.of("context"), List.of("missing"));
        graph.node("Where", List.of("missing", "zero", "context"), List.of("filled"));
        graph.node("ReduceSum", List.of("filled", "axes"), List.of("sum"), "keepdims", 1L);
        graph.node("Unsqueeze", List.of("sum", "axes"), List.of("expanded"));
        graph.node("Mul", List.of("expanded", "base"), List.of("forecasts"));
        graph.node("Identity", List.of("context"), List.of("seen"));
        graph.output("seen", FLOAT, "batch", "length");
        graph.output("forecasts", FLOAT, "batch", 3L, 4L);
        return graph.model();
    }

    /**
     * A model of the shape of a tabular model that learns in context: {@code x float[rows, columns]} with the context rows
     * first, {@code y_context [context]} and {@code categorical_mask bool[columns]}. The output is
     * {@code logits = ReduceSum(query rows of x, 1, keepdims) weights float[query, width]} and the scalar
     * {@code total = ReduceSum(weights)}.
     */
    public static byte[] tabular(final int targetType, final float... weights) {
        final Graph graph = new Graph("tabular");
        graph.input("x", FLOAT, "rows", "columns");
        graph.input("y_context", targetType, "context");
        graph.input("categorical_mask", BOOL, "columns");
        graph.initializer("end", INT64, new long[]{1}, new long[]{Long.MAX_VALUE});
        graph.initializer("rowAxis", INT64, new long[]{1}, new long[]{0});
        graph.initializer("columnAxis", INT64, new long[]{1}, new long[]{1});
        graph.initializer("weights", FLOAT, new long[]{1, weights.length}, weights);
        graph.node("Shape", List.of("y_context"), List.of("start"));
        graph.node("Slice", List.of("x", "start", "end", "rowAxis"), List.of("query"));
        graph.node("ReduceSum", List.of("query", "columnAxis"), List.of("sum"), "keepdims", 1L);
        graph.node("MatMul", List.of("sum", "weights"), List.of("logits"));
        graph.node("ReduceSum", List.of("weights"), List.of("total"), "keepdims", 0L);
        graph.output("logits", FLOAT, "query", (long) weights.length);
        graph.output("total", FLOAT);
        return graph.model();
    }

    /**
     * {@code ids float[batch, length]} and {@code features float[n, 2]} with the outputs
     * {@code pooled = ReduceSum(ids, 1, keepdims) float[batch, 1]} and {@code rows = features float[n, 2]}.
     */
    public static byte[] mixed() {
        final Graph graph = new Graph("mixed");
        graph.input("ids", FLOAT, "batch", "length");
        graph.input("features", FLOAT, "n", 2L);
        graph.initializer("axes", INT64, new long[]{1}, new long[]{1});
        graph.node("ReduceSum", List.of("ids", "axes"), List.of("pooled"), "keepdims", 1L);
        graph.node("Identity", List.of("features"), List.of("rows"));
        graph.output("pooled", FLOAT, "batch", 1L);
        graph.output("rows", FLOAT, "n", 2L);
        return graph.model();
    }

    /**
     * {@code x float[batch]} with {@code y = x}, {@code past.0 float[batch, length]} with {@code out.0 = past.0},
     * {@code filled float[-, 2]} with {@code out.filled = filled} and {@code features float[n, 2]}, which is not used.
     * {@code default_input} is an initializer listed as an input as well, as in models of an old IR version.
     */
    public static byte[] constants() {
        final Graph graph = new Graph("constants");
        graph.input("x", FLOAT, "batch");
        graph.input("past.0", FLOAT, "batch", "length");
        graph.input("filled", FLOAT, "batch", 2L);
        graph.input("features", FLOAT, "n", 2L);
        graph.input("default_input", FLOAT, 1L);
        graph.initializer("default_input", FLOAT, new long[]{1}, new float[]{1});
        graph.node("Identity", List.of("x"), List.of("y"));
        graph.node("Identity", List.of("past.0"), List.of("out.0"));
        graph.node("Identity", List.of("filled"), List.of("out.filled"));
        graph.output("y", FLOAT, "batch");
        graph.output("out.0", FLOAT, "batch", "length");
        graph.output("out.filled", FLOAT, "batch", 2L);
        return graph.model();
    }

    /** {@code x float[batch, 2]} with the output {@code y = x}, declared without a shape: only a session infers it. */
    public static byte[] undeclaredOutputShape() {
        final Graph graph = new Graph("undeclared");
        graph.input("x", FLOAT, "batch", 2L);
        graph.node("Identity", List.of("x"), List.of("y"));
        graph.outputWithoutShape("y", FLOAT);
        return graph.model();
    }

    /** {@code x float[batch]} with {@code y = x} and the sequence output {@code field}, named as a key of a select item. */
    public static byte[] sequenceOutput() {
        final Graph graph = new Graph("sequence");
        graph.input("x", FLOAT, "batch");
        graph.node("Identity", List.of("x"), List.of("y"));
        graph.node("SequenceConstruct", List.of("x"), List.of("field"));
        graph.output("y", FLOAT, "batch");
        graph.outputSequence("field", FLOAT);
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

        void outputWithoutShape(final String name, final int elemType) {
            final byte[] tensorType = message(t -> t.writeInt32(1, elemType));
            outputs.add(message(out -> {
                out.writeString(1, name);
                out.writeByteArray(2, message(t -> t.writeByteArray(1, tensorType)));
            }));
        }

        // TypeProto { sequence_type = 4 { elem_type = 1 { tensor_type = 1 { elem_type = 1 }}}}
        void outputSequence(final String name, final int elemType) {
            final byte[] tensorType = message(t -> t.writeInt32(1, elemType));
            final byte[] element = message(t -> t.writeByteArray(1, tensorType));
            final byte[] sequence = message(s -> s.writeByteArray(1, element));
            outputs.add(message(out -> {
                out.writeString(1, name);
                out.writeByteArray(2, message(t -> t.writeByteArray(4, sequence)));
            }));
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
