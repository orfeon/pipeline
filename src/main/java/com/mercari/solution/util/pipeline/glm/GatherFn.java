package com.mercari.solution.util.pipeline.glm;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.util.ArrayList;
import java.util.List;

/**
 * Gathers the (few) combined accumulators of a run into one list for a finalize step. In the global window
 * the default empty list still fires, so a finalize step runs (and a summary is emitted) on an empty input.
 */
public class GatherFn<T> extends Combine.CombineFn<T, List<T>, List<T>> {

    private final Coder<List<T>> coder;

    public GatherFn(final Coder<T> elementCoder) {
        this.coder = ListCoder.of(elementCoder);
    }

    @Override
    public List<T> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<T> addInput(final List<T> acc, final T input) {
        acc.add(input);
        return acc;
    }

    @Override
    public List<T> mergeAccumulators(final Iterable<List<T>> accs) {
        final List<T> merged = new ArrayList<>();
        for (final List<T> a : accs) merged.addAll(a);
        return merged;
    }

    @Override
    public List<T> extractOutput(final List<T> acc) {
        return acc;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(final CoderRegistry registry, final Coder<T> inputCoder) {
        return coder;
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(final CoderRegistry registry, final Coder<T> inputCoder) {
        return coder;
    }
}
