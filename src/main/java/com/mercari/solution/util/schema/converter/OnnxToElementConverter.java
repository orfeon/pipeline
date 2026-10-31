package com.mercari.solution.util.schema.converter;

import ai.onnxruntime.*;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.domain.ml.onnx.OnnxGraphInfo;
import com.mercari.solution.util.domain.ml.onnx.OnnxRuntimeUtil;

import java.util.*;

/**
 * Converts model outputs to record values. dim0 of an output is the batch if the inputs were stacked on one: an output
 * {@code [B]} gives each record a scalar, {@code [B, n]} an array and {@code [B, d1, ..., dk]} a matrix of shape
 * {@code [d1, ..., dk]} (row-major). Otherwise the whole output is the value of the one record, by the same rule.
 */
public class OnnxToElementConverter {

    /** The record field type of a model output. */
    public static Schema.FieldType convertFieldType(final OnnxGraphInfo.ValueInfo info, final boolean batched) {
        final Schema.FieldType elementType = switch (info.type()) {
            case BOOL -> Schema.FieldType.BOOLEAN.withNullable(false);
            case STRING -> Schema.FieldType.STRING.withNullable(false);
            case INT8, UINT8, INT16, INT32 -> Schema.FieldType.INT32.withNullable(false);
            case INT64 -> Schema.FieldType.INT64.withNullable(false);
            case FLOAT, FLOAT16, BFLOAT16 -> Schema.FieldType.FLOAT32.withNullable(false);
            case DOUBLE -> Schema.FieldType.FLOAT64.withNullable(false);
            case null, default -> throw new IllegalArgumentException("Not supported output type: " + info);
        };
        final long[] shape = info.shape();
        final long[] valueShape = batched && shape.length > 0 ? Arrays.copyOfRange(shape, 1, shape.length) : shape;
        return switch (valueShape.length) {
            case 0 -> elementType;
            case 1 -> Schema.FieldType.array(elementType);
            default -> Schema.FieldType.matrix(elementType, valueShape);
        };
    }

    /**
     * The values of each record of the batch: a scalar of a rank-1 output is the value itself, the others are Tensors
     * that share the array of the output. A rank-0 output (no batch dimension) is given to every record.
     */
    public static List<Map<String,Object>> convert(final OrtSession.Result result, final int batchSize, final boolean batched) throws OrtException {
        return convert(result, batchSize, batched, null);
    }

    /** {@link #convert(OrtSession.Result, int, boolean)} of the outputs in {@code names}, or all of them if null. The others are not copied. */
    public static List<Map<String,Object>> convert(final OrtSession.Result result, final int batchSize, final boolean batched, final Set<String> names) throws OrtException {
        final List<Map<String,Object>> outputs = new ArrayList<>();
        for(int i=0; i<batchSize; i++) {
            outputs.add(new HashMap<>());
        }
        for (final Map.Entry<String, OnnxValue> entry : result) {
            if(names != null && !names.contains(entry.getKey())) {
                continue;
            }
            if (!(entry.getValue() instanceof OnnxTensor onnxTensor)) {
                throw new IllegalArgumentException("Not supported onnx output: " + entry.getKey() + " type: " + entry.getValue().getInfo());
            }
            final Tensor tensor = OnnxRuntimeUtil.fromOnnxTensor(onnxTensor);
            if(tensor.rank() == 0 || !batched) {
                for(final Map<String, Object> output : outputs) {
                    output.put(entry.getKey(), tensor.rank() == 0 ? tensor.get(0) : tensor);
                }
                continue;
            }
            final long dim0 = tensor.shape()[0];
            if(dim0 != batchSize) {
                throw new IllegalStateException("onnx output: " + entry.getKey() + " has dim0 " + dim0
                        + " but the batch has " + batchSize + " records: a stacked input makes dim0 of every output the batch."
                        + " To take the whole output, give every input as it is, at the rank of the model input");
            }
            for(int i=0; i<batchSize; i++) {
                final Tensor row = tensor.row(i);
                outputs.get(i).put(entry.getKey(), row.rank() == 0 ? row.get(0) : row);
            }
        }
        return outputs;
    }

}
