package com.mercari.solution.util.domain.attribution;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Test-only synthetic data generator following the approach of the RiskLoc reference repository:
 * uniform baseline volumes with multiplicative noise, and root causes injected as a multiplicative
 * factor on the target values of a chosen slice.
 */
final class SyntheticDataGenerator {

    private SyntheticDataGenerator() {
    }

    /** Dimension names {@code d0..d(n-1)}, values {@code v0..v(c-1)} per dimension. */
    static List<String> dimensionNames(final int dimCount) {
        final List<String> names = new ArrayList<>();
        for(int i = 0; i < dimCount; i++) {
            names.add("d" + i);
        }
        return names;
    }

    static LeafTable generate(
            final long seed,
            final int dimCount,
            final int cardinality,
            final Slice culprit,
            final double factor) {

        final Random random = new Random(seed);
        final LeafTable.Builder builder = LeafTable.builder(dimensionNames(dimCount), List.of("m"));
        final int[] tuple = new int[dimCount];
        final String[] dims = new String[dimCount];
        while(true) {
            for(int i = 0; i < dimCount; i++) {
                dims[i] = "v" + tuple[i];
            }
            final double baseline = 50 + 50 * random.nextDouble();
            final double noise = 1 + (random.nextDouble() - 0.5) * 0.04;
            double target = baseline * noise;
            if(culprit.contains(dims)) {
                target *= factor;
            }
            builder.addBaseline(dims.clone(), new double[]{baseline});
            builder.addTarget(dims.clone(), new double[]{target});

            int position = dimCount - 1;
            while(position >= 0 && tuple[position] == cardinality - 1) {
                tuple[position] = 0;
                position--;
            }
            if(position < 0) {
                break;
            }
            tuple[position]++;
        }
        return builder.build();
    }
}
