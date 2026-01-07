package com.mercari.solution.util.domain.ml.onnx;

import java.io.Serializable;
import java.util.List;

/*
 * https://github.com/huggingface/transformers/blob/main/src/transformers/configuration_utils.py#L53
 */
public class PretrainedConfig implements Serializable {

    public List<String> architectures;
    public Boolean attention_bias;
    public Double attention_dropout;
    public Long bos_token_id;
    public Long eos_token_id;
    public Integer head_dim;
    public String hidden_activation;
    public Double initializer_range;
    public Integer intermediate_size;
    public List<String> layer_types;
    public Integer max_position_embeddings;
    public String model_type;
    public Integer num_attention_heads;
    public Integer num_key_value_heads;
    public Integer pad_token_id;
    public Integer query_pre_attn_scalar;
    public Double rms_norm_eps;
    public Double rope_local_base_freq;
    public Double rope_theta;
    public Integer sliding_window;
    public String transformers_version;
    public Boolean use_bidirectional_attention;
    public Boolean use_cache;

    public Integer vocab_size;
    public Integer hidden_size;
    public Integer num_hidden_layers;




}
