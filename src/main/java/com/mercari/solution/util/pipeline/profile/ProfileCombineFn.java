package com.mercari.solution.util.pipeline.profile;

import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Single CombineFn computing the whole dataset profile in one shuffle
 * (field sketch sets + correlation co-moments + row sample).
 */
public class ProfileCombineFn extends Combine.CombineFn<MElement, ProfileAccumulator, ProfileAccumulator> {

    private final ProfileSpec spec;

    public ProfileCombineFn(final ProfileSpec spec) {
        this.spec = spec;
    }

    @Override
    public ProfileAccumulator createAccumulator() {
        return ProfileAccumulator.of(spec);
    }

    @Override
    public ProfileAccumulator addInput(final ProfileAccumulator accumulator, final MElement element) {
        if(element == null) {
            return accumulator;
        }
        try {
            final Map<String, Object> primitives = element.asPrimitiveMap();
            final List<ProfileSpec.FieldSpec> fieldSpecs = spec.getFields();
            final Object[] values = new Object[fieldSpecs.size()];
            for(int i = 0; i < fieldSpecs.size(); i++) {
                values[i] = ProfileSpec.getValue(primitives, fieldSpecs.get(i).path);
            }
            final String sampleJson = spec.isSampleEnabled() ? toSampleJson(fieldSpecs, values) : null;
            accumulator.add(values, sampleJson);
        } catch (final Throwable e) {
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
    public Coder<ProfileAccumulator> getAccumulatorCoder(final CoderRegistry registry, final Coder<MElement> inputCoder) {
        return SerializableCoder.of(ProfileAccumulator.class);
    }

    @Override
    public Coder<ProfileAccumulator> getDefaultOutputCoder(final CoderRegistry registry, final Coder<MElement> inputCoder) {
        return SerializableCoder.of(ProfileAccumulator.class);
    }

    /** Renders one row as a compact JSON string for the VarOpt sample (display values, profiled fields only). */
    private static String toSampleJson(final List<ProfileSpec.FieldSpec> fieldSpecs, final Object[] values) {
        final JsonObject json = new JsonObject();
        for(int i = 0; i < fieldSpecs.size(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = fieldSpecs.get(i);
            final Object value = values[i];
            if(value == null) {
                json.add(fieldSpec.path, null);
                continue;
            }
            try {
                switch (fieldSpec.profileType) {
                    case NUMERIC -> json.addProperty(fieldSpec.path, ProfileSpec.toDouble(value));
                    case BOOL -> json.addProperty(fieldSpec.path, ProfileSpec.toBoolean(value));
                    case TIMESTAMP -> {
                        final Double ms = ProfileSpec.toEpochMillis(value, fieldSpec.sourceType);
                        json.addProperty(fieldSpec.path, ms == null ? null : Instant.ofEpochMilli(ms.longValue()).toString());
                    }
                    case ARRAY_LENGTH -> {
                        if(value instanceof java.util.Collection<?> c) {
                            json.addProperty(fieldSpec.path, c.size());
                        }
                    }
                    default -> {
                        final String s = ProfileSpec.toStringValue(value, fieldSpec.symbols);
                        json.addProperty(fieldSpec.path, s != null && s.length() > 256 ? s.substring(0, 256) : s);
                    }
                }
            } catch (final Throwable e) {
                json.add(fieldSpec.path, null);
            }
        }
        return json.toString();
    }
}
