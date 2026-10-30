package com.mercari.solution.util.schema.converter;

import ai.onnxruntime.*;
import com.mercari.solution.module.Schema;

import java.lang.reflect.Array;
import java.nio.*;
import java.util.*;

/**
 * Converts model outputs to record values. dim0 of every output is the batch: an output {@code [B]} gives each record
 * a scalar, {@code [B, n]} an array and {@code [B, d1, ..., dk]} a matrix of shape {@code [d1, ..., dk]} (row-major).
 */
public class OnnxToElementConverter {

    public static Schema convertOutputSchema(final Map<String, NodeInfo> outputsInfo, final List<String> outputs) {
        final List<String> outputNames;
        if(outputs == null || outputs.isEmpty()) {
            outputNames = new ArrayList<>(outputsInfo.keySet());
        } else {
            outputNames = outputs;
        }

        final Schema.Builder builder = Schema.builder();
        for(final String outputName : outputNames) {
            final NodeInfo nodeInfo = outputsInfo.get(outputName);
            if(nodeInfo == null) {
                throw new IllegalArgumentException("Not found output name: " + outputName + " in outputs info: " + outputsInfo);
            }

            switch (nodeInfo.getInfo()) {
                case TensorInfo tensorInfo -> {
                    final Schema.FieldType elementType = switch (tensorInfo.type) {
                        case BOOL -> Schema.FieldType.BOOLEAN.withNullable(false);
                        case STRING -> Schema.FieldType.STRING.withNullable(false);
                        case INT8, UINT8, INT16, INT32 -> Schema.FieldType.INT32.withNullable(false);
                        case INT64 -> Schema.FieldType.INT64.withNullable(false);
                        case FLOAT, FLOAT16, BFLOAT16 -> Schema.FieldType.FLOAT32.withNullable(false);
                        case DOUBLE -> Schema.FieldType.FLOAT64.withNullable(false);
                        default -> throw new IllegalArgumentException("Not supported output type: " + tensorInfo.type);
                    };
                    final long[] shape = tensorInfo.getShape();
                    final Schema.FieldType fieldType;
                    if(shape.length <= 1) {
                        fieldType = elementType;
                    } else if(shape.length == 2) {
                        fieldType = Schema.FieldType.array(elementType);
                    } else {
                        fieldType = Schema.FieldType.matrix(elementType, Arrays.copyOfRange(shape, 1, shape.length));
                    }
                    builder.withField(outputName, fieldType);
                }
                default -> throw new IllegalArgumentException("Not supported node type: " + nodeInfo.getInfo());
            }
        }
        return builder.build();
    }

    /**
     * Splits each output tensor by dim0 into {@code batchSize} record values. A rank-0 output (no batch dimension)
     * is given to every record.
     */
    public static List<Map<String,Object>> convert(final OrtSession.Result result, final int batchSize) {
        final List<Map<String,Object>> outputs = new ArrayList<>();
        for(int i=0; i<batchSize; i++) {
            outputs.add(new HashMap<>());
        }
        for (final Map.Entry<String, OnnxValue> entry : result) {
            if (!(entry.getValue() instanceof OnnxTensor tensor)) {
                throw new IllegalArgumentException("Not supported onnx output: " + entry.getKey() + " type: " + entry.getValue().getInfo());
            }
            final long[] shape = tensor.getInfo().getShape();
            final List<Object> values = flatValues(tensor);
            if(shape.length == 0) {
                for(final Map<String, Object> output : outputs) {
                    output.put(entry.getKey(), values.getFirst());
                }
                continue;
            }
            if(shape[0] != batchSize) {
                throw new IllegalStateException("onnx output: " + entry.getKey() + " has dim0 " + shape[0]
                        + " but the batch has " + batchSize + " records (outputs without a batch dimension are not supported)");
            }
            final int rowSize = batchSize == 0 ? 0 : values.size() / batchSize;
            for(int i=0; i<batchSize; i++) {
                final Object rowValue;
                if(shape.length == 1) {
                    rowValue = values.get(i);
                } else {
                    rowValue = new ArrayList<>(values.subList(i * rowSize, (i + 1) * rowSize));
                }
                outputs.get(i).put(entry.getKey(), rowValue);
            }
        }
        return outputs;
    }

    private static List<Object> flatValues(final OnnxTensor tensor) {
        final int size = Math.toIntExact(tensor.getInfo().getNumElements());
        final List<Object> values = new ArrayList<>(size);
        switch (tensor.getInfo().type) {
            case BOOL -> {
                final ByteBuffer buffer = tensor.getByteBuffer();
                for(int i=0; i<size; i++) {
                    values.add(buffer.get(i) != 0);
                }
            }
            case INT8 -> {
                final ByteBuffer buffer = tensor.getByteBuffer();
                for(int i=0; i<size; i++) {
                    values.add((int) buffer.get(i));
                }
            }
            case UINT8 -> {
                final ByteBuffer buffer = tensor.getByteBuffer();
                for(int i=0; i<size; i++) {
                    values.add(buffer.get(i) & 0xFF);
                }
            }
            case INT16 -> {
                final ShortBuffer buffer = tensor.getShortBuffer();
                for(int i=0; i<size; i++) {
                    values.add((int) buffer.get(i));
                }
            }
            case INT32 -> {
                final IntBuffer buffer = tensor.getIntBuffer();
                for(int i=0; i<size; i++) {
                    values.add(buffer.get(i));
                }
            }
            case INT64 -> {
                final LongBuffer buffer = tensor.getLongBuffer();
                for(int i=0; i<size; i++) {
                    values.add(buffer.get(i));
                }
            }
            case FLOAT, FLOAT16, BFLOAT16 -> {
                // getFloatBuffer widens float16 / bfloat16 to float
                final FloatBuffer buffer = tensor.getFloatBuffer();
                for(int i=0; i<size; i++) {
                    values.add(buffer.get(i));
                }
            }
            case DOUBLE -> {
                final DoubleBuffer buffer = tensor.getDoubleBuffer();
                for(int i=0; i<size; i++) {
                    values.add(buffer.get(i));
                }
            }
            case STRING -> {
                try {
                    flattenArray(tensor.getValue(), values);
                } catch (final OrtException e) {
                    throw new IllegalStateException("Failed to read string tensor", e);
                }
            }
            default -> throw new IllegalArgumentException("Not supported tensor info type: " + tensor.getInfo().type);
        }
        return values;
    }

    private static void flattenArray(final Object value, final List<Object> values) {
        if(value != null && value.getClass().isArray()) {
            for(int i=0; i<Array.getLength(value); i++) {
                flattenArray(Array.get(value, i), values);
            }
        } else {
            values.add(value);
        }
    }

}
