package com.mercari.solution.util.domain.attribution;

import java.util.ArrayList;
import java.util.List;

/**
 * Enumerates cuboids (dimension index combinations) of the attribution search lattice
 * in layer order, lexicographically within a layer.
 */
public final class Cuboids {

    private Cuboids() {
    }

    public static List<int[]> enumerate(final int dimCount, final int maxLayer) {
        final List<int[]> cuboids = new ArrayList<>();
        final int max = Math.min(dimCount, maxLayer);
        for(int layer = 1; layer <= max; layer++) {
            combinations(dimCount, layer, cuboids);
        }
        return cuboids;
    }

    public static List<int[]> layer(final int dimCount, final int layer) {
        final List<int[]> cuboids = new ArrayList<>();
        if(layer >= 1 && layer <= dimCount) {
            combinations(dimCount, layer, cuboids);
        }
        return cuboids;
    }

    private static void combinations(final int n, final int k, final List<int[]> results) {
        final int[] indexes = new int[k];
        for(int i = 0; i < k; i++) {
            indexes[i] = i;
        }
        while(true) {
            results.add(indexes.clone());
            int i = k - 1;
            while(i >= 0 && indexes[i] == n - k + i) {
                i--;
            }
            if(i < 0) {
                return;
            }
            indexes[i]++;
            for(int j = i + 1; j < k; j++) {
                indexes[j] = indexes[j - 1] + 1;
            }
        }
    }
}
