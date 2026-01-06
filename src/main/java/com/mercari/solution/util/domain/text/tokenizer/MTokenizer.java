package com.mercari.solution.util.domain.text.tokenizer;

import com.mercari.solution.module.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface MTokenizer {

    List<String> validate();
    void setup();
    List<Schema.Field> getOutputFields();

    Map<String, Object> encode(String text);
    String decode(long[] ids);
    List<Map<String, Object>> batchEncode(List<String> text);
    List<String> batchDecode(long[][] ids);
    List<String> tokenize(String text);

    enum TokenizerName {
        huggingface,
        none
    }

    static List<Long> toList(long[] values) {
        final List<Long> list = new ArrayList<>(values.length);
        Arrays.stream(values).forEachOrdered(list::add);
        return list;
    }

    static List<Integer> toList(int[] values) {
        final List<Integer> list = new ArrayList<>(values.length);
        Arrays.stream(values).forEachOrdered(list::add);
        return list;
    }

    static List<Float> toList(float[] values) {
        final List<Float> list = new ArrayList<>(values.length);
        double[] dd = new double[values.length];
        for(int i=0; i<values.length; i++) {
            dd[i] = values[i];
        }
        Arrays.stream(dd).forEachOrdered(f -> list.add(Double.valueOf(f).floatValue()));
        return list;
    }

    static List<String> toList(String[] values) {
        final List<String> list = new ArrayList<>(values.length);
        list.addAll(Arrays.asList(values));
        return list;
    }

}
