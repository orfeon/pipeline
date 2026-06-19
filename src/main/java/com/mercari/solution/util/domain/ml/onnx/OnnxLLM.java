package com.mercari.solution.util.domain.ml.onnx;

import ai.onnxruntime.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.nio.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OnnxLLM implements Serializable {

    public SimpleConfig config;
    public ModelConfig modelConfig;

    public static OnnxLLM create(String json) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        return create(jsonObject);
    }

    public static OnnxLLM create(JsonObject jsonObject) {
        final OnnxLLM llm = new OnnxLLM();
        llm.modelConfig = ModelConfig.parse(jsonObject);
        return llm;
    }

    public static OnnxLLM create2(String json, OrtSession session) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        return create2(jsonObject, session);
    }

    public static OnnxLLM create2(JsonObject jsonObject, OrtSession session) {
        final OnnxLLM llm = new OnnxLLM();
        llm.config = SimpleConfig.parse(jsonObject);
        llm.config.setDefaults(session);
        return llm;
    }

    public long[] process(
            final OrtEnvironment environment,
            final OrtSession session,
            final long[] input_ids_,
            final long[] attention_mask_) throws OrtException {

        final List<Long> outputs = new ArrayList<>();

        OnnxTensor input_ids = OnnxTensor.createTensor(environment, LongBuffer.wrap(input_ids_), new long[]{1, input_ids_.length});
        OnnxTensor attention_mask = OnnxTensor.createTensor(environment, LongBuffer.wrap(attention_mask_), new long[]{1, attention_mask_.length});

        final List<OnnxTensor> kv_keys = new ArrayList<>();
        final List<OnnxTensor> kv_values = new ArrayList<>();
        long[] shape_init = new long[]{1, modelConfig.decoder.num_key_value_heads, 0, modelConfig.decoder.head_size};
        for(int i=0; i<modelConfig.decoder.num_hidden_layers; i++) {
            kv_keys.add(OnnxTensor.createTensor(environment, FloatBuffer.wrap(new float[0]), shape_init));
            kv_values.add(OnnxTensor.createTensor(environment, FloatBuffer.wrap(new float[0]), shape_init));
        }

        for(int j=0; j<580; j++) {
            final Map<String, OnnxTensor> values = new HashMap<>();
            values.put(modelConfig.decoder.inputs.input_ids, input_ids);
            values.put(modelConfig.decoder.inputs.attention_mask, attention_mask);
            for(int i=0; i<modelConfig.decoder.num_hidden_layers; i++) {
                final String keyName = String.format(modelConfig.decoder.inputs.past_key_names, i);
                final String valueName = String.format(modelConfig.decoder.inputs.past_value_names, i);
                values.put(keyName, kv_keys.get(i));
                values.put(valueName, kv_values.get(i));
            }

            try(final OrtSession.Result result = session.run(values)) {
                int token_id = OnnxModel.argmax((OnnxTensor)result.get(modelConfig.decoder.outputs.logits).get());
                if(modelConfig.eos_token_id.contains(token_id)) {
                    break;
                }
                outputs.add(Integer.valueOf(token_id).longValue());

                for(int i=0; i<modelConfig.decoder.num_hidden_layers; i++) {
                    final String keyName = String.format(modelConfig.decoder.outputs.present_key_names, i);
                    final String valueName = String.format(modelConfig.decoder.outputs.present_value_names, i);
                    final OnnxTensor k = (OnnxTensor) result.get(keyName).get();
                    final OnnxTensor v = (OnnxTensor) result.get(valueName).get();

                    final long[] shape_next = new long[]{1, modelConfig.decoder.num_key_value_heads, toList(k.getInfo().getShape()).get(2), modelConfig.decoder.head_size};

                    kv_keys.set(i, OnnxTensor.createTensor(environment, FloatBuffer.wrap(k.getFloatBuffer().array()), shape_next));
                    kv_values.set(i, OnnxTensor.createTensor(environment, FloatBuffer.wrap(v.getFloatBuffer().array()), shape_next));
                }

                long[] attention_mask_values = attention_mask.getLongBuffer().array();
                attention_mask_values = append(attention_mask_values, attention_mask_values[0]);

                input_ids = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{token_id}), new long[]{1, 1});
                attention_mask = OnnxTensor.createTensor(environment, LongBuffer.wrap(attention_mask_values), new long[]{1, attention_mask_values.length});
            }
        }

        return outputs.stream().mapToLong(l -> l).toArray();
    }

    public long[] process_(
            final OrtEnvironment environment,
            final OrtSession session,
            final long[] input_ids_,
            final long[] attention_mask_) throws OrtException {

        final List<Long> outputs = new ArrayList<>();

        OnnxTensor input_ids = OnnxTensor.createTensor(environment, LongBuffer.wrap(input_ids_), new long[]{1, input_ids_.length});
        OnnxTensor attention_mask = OnnxTensor.createTensor(environment, LongBuffer.wrap(attention_mask_), new long[]{1, attention_mask_.length});
        OnnxTensor position_ids = sequence(environment, input_ids_.length);

        final Map<String, OnnxTensor> inputs = new HashMap<>();
        for(final Map.Entry<String, String> entry : config.inputs.feeds.entrySet()) {
            final TensorInfo tensorInfo = config.inputs.tensorInfos.get(entry.getValue());
            final OnnxTensor tensor = zeros(environment, tensorInfo);
            inputs.put(entry.getValue(), tensor);
        }

        final boolean usePositionIds = session.getInputInfo().containsKey(config.inputs.position_ids);
        final boolean useNumLogitsToKeep = session.getInputInfo().containsKey(config.inputs.num_logits_to_keep);

        int pos = input_ids_.length;
        for(int j=0; j<580; j++) {
            final Map<String, OnnxTensor> values = new HashMap<>();
            values.put(config.inputs.input_ids, input_ids);
            values.put(config.inputs.attention_mask, attention_mask);
            if(usePositionIds) {
                values.put(config.inputs.position_ids, position_ids);
            }
            if(useNumLogitsToKeep) {
                OnnxTensor num_logits_to_keep = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{1L}), new long[]{});
                values.put(config.inputs.num_logits_to_keep, num_logits_to_keep);
            }
            values.putAll(inputs);

            try(final OrtSession.Result result = session.run(values)) {
                final int token_id = OnnxModel.argmax((OnnxTensor)result.get(config.outputs.logits).get());
                if(config.eos_token_id.contains(token_id)) {
                    System.out.println("finished");
                    break;
                } else {
                    System.out.println("n: " + token_id);
                }
                outputs.add(Integer.valueOf(token_id).longValue());

                for(final Map.Entry<String,String> entry : config.inputs.feeds.entrySet()) {
                    final OnnxTensor k = (OnnxTensor) result.get(entry.getKey()).get();
                    inputs.put(entry.getValue(), copy(environment, k));
                }

                long[] attention_mask_values = attention_mask.getLongBuffer().array();
                attention_mask_values = append(attention_mask_values, attention_mask_values[0]);

                input_ids = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{token_id}), new long[]{1, 1});
                attention_mask = OnnxTensor.createTensor(environment, LongBuffer.wrap(attention_mask_values), new long[]{1, attention_mask_values.length});
                position_ids = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{pos}), new long[]{1, 1});
                pos = pos + 1;
            }
        }

        return outputs.stream().mapToLong(l -> l).toArray();
    }

    private static long[] append(long[] array, long value) {
        int size = array.length + 1;
        long[] newArray = new long[size];
        for(int i=0; i<size; i++) {
            if(i >= array.length) {
                newArray[i] = value;
            } else {
                newArray[i] = array[i];
            }
        }
        return newArray;
    }

    public static class SimpleConfig implements Serializable {

        public List<Integer> eos_token_id;
        public InputsConfig inputs;
        public OutputsConfig outputs;

        public static class InputsConfig implements Serializable {
            public String input_ids;
            public String attention_mask;
            public String position_ids;
            public String num_logits_to_keep;
            public Map<String, String> feeds;

            public transient Map<String, TensorInfo> tensorInfos;

            public void setDefaults(OrtSession session) {
                if(input_ids == null) {
                    input_ids = "input_ids";
                }
                if(attention_mask == null) {
                    attention_mask = "attention_mask";
                }
                if(position_ids == null) {
                    position_ids = "position_ids";
                }
                if(num_logits_to_keep == null) {
                    num_logits_to_keep = "num_logits_to_keep";
                }
                if(feeds == null) {
                    feeds = new HashMap<>();
                }
                if(tensorInfos == null) {
                    tensorInfos = new HashMap<>();
                }

                try {
                    final Map<String, String> mappings = new HashMap<>();
                    final Map<Pattern, String> mappings_ = new HashMap<>();
                    for(final Map.Entry<String,String> feedEntry : feeds.entrySet()) {
                        final String onnxInputFieldName = feedEntry.getKey()
                                .replaceAll("\\.", "\\\\.")
                                .replaceAll("%d", "(\\\\d+)");
                        final Pattern p = Pattern.compile(onnxInputFieldName);
                        mappings_.put(p, feedEntry.getValue());
                    }
                    for(final Map.Entry<Pattern, String> entry : mappings_.entrySet()) {
                        for (final Map.Entry<String, NodeInfo> inputInfoEntry : session.getInputInfo().entrySet()) {
                            final String onnxInputFieldName = inputInfoEntry.getKey();
                            final TensorInfo tensorInfo = ((TensorInfo)inputInfoEntry.getValue().getInfo());
                            tensorInfos.put(onnxInputFieldName, tensorInfo);
                            final Matcher m = entry.getKey().matcher(onnxInputFieldName);
                            if(m.find()) {
                                final String onnxOutputFieldName;
                                if(m.groupCount() > 0) {
                                    int d = Integer.parseInt(m.group(1));
                                    onnxOutputFieldName = String.format(entry.getValue(), d);
                                } else {
                                    onnxOutputFieldName = m.group();
                                }
                                mappings.put(onnxOutputFieldName, onnxInputFieldName);
                            }
                        }
                    }

                    this.feeds = mappings;
                } catch (OrtException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public static class OutputsConfig implements Serializable {
            public String logits;

            public void setDefaults() {
                if(logits == null) {
                    logits = "logits";
                }
            }
        }

        public void setDefaults(OrtSession session) {
            if(eos_token_id == null) {
                eos_token_id = new ArrayList<>();
            }

            if(inputs == null) {
                inputs = new InputsConfig();
            }
            inputs.setDefaults(session);

            if(outputs == null) {
                outputs = new OutputsConfig();
            }
            outputs.setDefaults();
        }

        public static SimpleConfig parse(final JsonObject jsonObject) {
            return new Gson().fromJson(jsonObject, SimpleConfig.class);
        }

    }

    public static class ModelConfig implements Serializable {

        public Integer bos_token_id;
        public Integer context_length;
        public DecoderConfig decoder;
        public List<Integer> eos_token_id;
        public Integer pad_token_id;
        public String type;
        public Integer vocab_size;

        public static class DecoderConfig implements Serializable {

            public Integer head_size;
            public Integer hidden_size;
            public InputsConfig inputs;
            public OutputsConfig outputs;
            public Integer num_attention_heads;
            public Integer num_hidden_layers;
            public Integer num_key_value_heads;

            public static class InputsConfig implements Serializable {
                public String input_ids;
                public String attention_mask;
                public String past_key_names;
                public String past_value_names;
                public List<String> other_names;
            }

            public static class OutputsConfig implements Serializable {
                public String logits;
                public String present_key_names;
                public String present_value_names;
                public List<String> other_names;
            }
        }

        public static ModelConfig parse(final JsonObject jsonObject) {
            return new Gson().fromJson(jsonObject, ModelConfig.class);
        }

    }

    static List<Long> toList(long[] values) {
        final List<Long> list = new ArrayList<>(values.length);
        Arrays.stream(values).forEachOrdered(list::add);
        return list;
    }

    static OnnxTensor copy(
            final OrtEnvironment environment,
            final OnnxTensor tensor) throws OrtException {

        return switch (tensor.getInfo().type) {
            case INT8 -> OnnxTensor.createTensor(environment, ByteBuffer.wrap(tensor.getByteBuffer().array()), tensor.getInfo().getShape());
            case INT16 -> OnnxTensor.createTensor(environment, ShortBuffer.wrap(tensor.getShortBuffer().array()), tensor.getInfo().getShape());
            case INT32 -> OnnxTensor.createTensor(environment, IntBuffer.wrap(tensor.getIntBuffer().array()), tensor.getInfo().getShape());
            case INT64 -> OnnxTensor.createTensor(environment, LongBuffer.wrap(tensor.getLongBuffer().array()), tensor.getInfo().getShape());
            case FLOAT -> OnnxTensor.createTensor(environment, FloatBuffer.wrap(tensor.getFloatBuffer().array()), tensor.getInfo().getShape());
            case DOUBLE -> OnnxTensor.createTensor(environment, DoubleBuffer.wrap(tensor.getDoubleBuffer().array()), tensor.getInfo().getShape());
            default -> throw new IllegalArgumentException();
        };
    }

    static OnnxTensor zeros(OrtEnvironment environment, TensorInfo tensorInfo) throws OrtException {
        long[] shape = new long[tensorInfo.getShape().length];
        shape[0] = 1;
        for(int i=1; i<shape.length; i++) {
            shape[i] = tensorInfo.getShape()[i] >= 0 ? tensorInfo.getShape()[i] : 0;
        }
        final int elementCount = Long.valueOf(OrtUtil.elementCount(shape)).intValue();
        return switch (tensorInfo.type) {
            case INT8 -> OnnxTensor.createTensor(environment, ByteBuffer.wrap(new byte[elementCount]), shape);
            case INT16 -> OnnxTensor.createTensor(environment, ShortBuffer.wrap(new short[elementCount]), tensorInfo.getShape());
            case INT32 -> OnnxTensor.createTensor(environment, IntBuffer.wrap(new int[elementCount]), tensorInfo.getShape());
            case INT64 -> OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[elementCount]), shape);
            case FLOAT -> OnnxTensor.createTensor(environment, FloatBuffer.wrap(new float[elementCount]), shape);
            case DOUBLE -> OnnxTensor.createTensor(environment, DoubleBuffer.wrap(new double[elementCount]), tensorInfo.getShape());
            default -> throw new IllegalArgumentException();
        };
    }

    static OnnxTensor sequence(OrtEnvironment environment, int length) throws OrtException {
        long[] ls = new long[length];
        for(int l=0; l<length; l++) {
            ls[l] = l;
        }
        long[] shape = new long[]{1L, length};
        return OnnxTensor.createTensor(environment, LongBuffer.wrap(ls), shape);
    }

}
