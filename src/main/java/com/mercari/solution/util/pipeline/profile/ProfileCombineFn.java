package com.mercari.solution.util.pipeline.profile;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Single CombineFn computing the whole dataset profile in one shuffle
 * (field sketch sets + correlation co-moments + row sample) over pre-extracted {@link ProfileRow}s.
 */
public class ProfileCombineFn extends Combine.CombineFn<ProfileRow, ProfileAccumulator, ProfileAccumulator> {

    private final ProfileSpec spec;

    public ProfileCombineFn(final ProfileSpec spec) {
        this.spec = spec;
    }

    @Override
    public ProfileAccumulator createAccumulator() {
        return ProfileAccumulator.of(spec);
    }

    @Override
    public ProfileAccumulator addInput(final ProfileAccumulator accumulator, final ProfileRow row) {
        if(row == null || row.values == null) {
            return accumulator;
        }
        accumulator.add(row.values, spec.isSampleEnabled() ? row.sampleJson : null);
        if(row.isFailed()) {
            accumulator.countError();
        }
        return accumulator;
    }

    @Override
    public ProfileAccumulator mergeAccumulators(final Iterable<ProfileAccumulator> accumulators) {
        ProfileAccumulator merged = null;
        for(final ProfileAccumulator accumulator : accumulators) {
            if(merged == null) {
                merged = accumulator;
            } else {
                merged = merged.merge(accumulator);
            }
        }
        return merged == null ? createAccumulator() : merged;
    }

    @Override
    public ProfileAccumulator extractOutput(final ProfileAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public Coder<ProfileAccumulator> getAccumulatorCoder(final CoderRegistry registry, final Coder<ProfileRow> inputCoder) {
        return SerializableCoder.of(ProfileAccumulator.class);
    }

    @Override
    public Coder<ProfileAccumulator> getDefaultOutputCoder(final CoderRegistry registry, final Coder<ProfileRow> inputCoder) {
        return SerializableCoder.of(ProfileAccumulator.class);
    }
}
