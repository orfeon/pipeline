package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FactorizationTest {

    /** y = 1 exactly when (a, b) ∈ {(a1,b1), (a2,b2)}: no main effects, a pure pairwise interaction. */
    private static List<Factorization.Example> xor(final int repeats) {
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int i = 0; i < repeats; i++) {
            for (final String a : List.of("a1", "a2")) {
                for (final String b : List.of("b1", "b2")) {
                    final double y = a.substring(1).equals(b.substring(1)) ? 1 : 0;
                    examples.add(new Factorization.Example(new String[]{a, b, "c" + (i % 3)}, y));
                }
            }
        }
        return examples;
    }

    @Test
    public void testAlsRecoversInteraction() {
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), false, 2, 40, 0.01, 7L);
        final Factorization.Model model = Factorization.fit(options, xor(5));
        double sse = 0;
        for (final Factorization.Example e : xor(1)) {
            final double p = model.predict(e.values());
            sse += (p - e.y()) * (p - e.y());
        }
        Assertions.assertTrue(Math.sqrt(sse / 4) < 0.15, "rmse too high: " + Math.sqrt(sse / 4));
        // the a×b pair score separates matching from mismatching combinations
        Assertions.assertTrue(model.pair(new String[]{"a1", "b1", null}, 0, 1) > model.pair(new String[]{"a1", "b2", null}, 0, 1));
        Assertions.assertTrue(model.pair(new String[]{"a2", "b2", null}, 0, 1) > model.pair(new String[]{"a2", "b1", null}, 0, 1));
        // unknown values yield no pair score, embeddings have the latent dimension
        Assertions.assertNull(model.pair(new String[]{"a9", "b1", null}, 0, 1));
        Assertions.assertEquals(2, model.embedding(new String[]{"a1", "b1", null}, 0, 5).length);
        // deterministic for a seed
        final Factorization.Model again = Factorization.fit(options, xor(5));
        Assertions.assertEquals(model.predict(new String[]{"a1", "b1", "c0"}), again.predict(new String[]{"a1", "b1", "c0"}), 1e-12);
    }

    @Test
    public void testFieldWeightedVariantRanksPairs() {
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), true, 2, 40, 0.01, 3L);
        final Factorization.Model model = Factorization.fit(options, xor(5));
        // r_ab carries the interaction; r_ac / r_bc have nothing to explain
        Assertions.assertTrue(Math.abs(model.r[0][1]) > Math.abs(model.r[0][2]) || Math.abs(model.r[0][1]) > Math.abs(model.r[1][2]),
                "r=" + java.util.Arrays.deepToString(model.r));
    }

    @Test
    public void testArtifactRoundTrip() {
        final String dir = "target/feature-artifacts/" + UUID.randomUUID();
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), true, 3, 20, 0.05, 1L);
        final Factorization.Model model = Factorization.fit(options, xor(4));
        Factorization.write(dir, "hash1", "fm", model);
        Assertions.assertTrue(Factorization.exists(dir, "hash1", "fm"));
        Assertions.assertFalse(Factorization.exists(dir, "hash2", "fm"));
        final Factorization.Model loaded = Factorization.read(dir, "hash1", "fm", options.fields(), true, 3);
        Assertions.assertEquals(model.w0, loaded.w0, 1e-12);
        Assertions.assertEquals(model.r[0][1], loaded.r[0][1], 1e-12);
        for (final Factorization.Example e : xor(1)) {
            Assertions.assertEquals(model.predict(e.values()), loaded.predict(e.values()), 1e-12);
            Assertions.assertEquals(model.pair(e.values(), 0, 1), loaded.pair(e.values(), 0, 1), 1e-12);
        }
    }

}
