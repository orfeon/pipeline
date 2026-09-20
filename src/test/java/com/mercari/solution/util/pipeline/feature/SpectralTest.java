package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

public class SpectralTest {

    private static Spectral.PairCounts counts(final Object... pairsAndCounts) {
        final Spectral.PairCounts state = new Spectral.PairCounts();
        for (int i = 0; i < pairsAndCounts.length; i += 3) {
            state.add((String) pairsAndCounts[i], (String) pairsAndCounts[i + 1], ((Number) pairsAndCounts[i + 2]).longValue());
        }
        return state;
    }

    private static double cosine(final double[] a, final double[] b) {
        double ab = 0, aa = 0, bb = 0;
        for (int i = 0; i < a.length; i++) {
            ab += a[i] * b[i];
            aa += a[i] * a[i];
            bb += b[i] * b[i];
        }
        return ab / Math.sqrt(aa * bb);
    }

    @Test
    public void testPairCountsFamily() {
        final Spectral.PairCounts state = Spectral.SUMMARY.create();
        // a row's value followed by the earlier values in its window: (good, fair), (good, good)
        Spectral.SUMMARY.update(state, new String[]{"good", "fair", "good"}, 1);
        Spectral.SUMMARY.update(state, new String[]{"fair", "good"}, 1);
        Spectral.SUMMARY.update(state, new String[]{"poor"}, 1); // a first event has no pair
        Assertions.assertEquals(3, state.pairs);
        Assertions.assertEquals(3.0, Spectral.SUMMARY.count(state));
        // unordered: (good, fair) and (fair, good) are one cell, keyed in string order
        Assertions.assertEquals(2L, state.counts.get("fair").get("good"));
        Assertions.assertEquals(1L, state.counts.get("good").get("good"));
        Assertions.assertNull(state.counts.get("poor"));
        // a monoid: merging does not touch the other side, and the identity changes nothing
        final Spectral.PairCounts other = counts("fair", "poor", 4);
        final Spectral.PairCounts merged = Spectral.SUMMARY.create();
        Spectral.SUMMARY.merge(merged, state);
        Spectral.SUMMARY.merge(merged, other);
        Spectral.SUMMARY.merge(merged, Spectral.SUMMARY.create());
        Assertions.assertEquals(7, merged.pairs);
        Assertions.assertEquals(4L, merged.counts.get("fair").get("poor"));
        Assertions.assertEquals(2L, merged.counts.get("fair").get("good"));
        Assertions.assertEquals(4, other.pairs);
        Assertions.assertEquals(3, state.pairs);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> Spectral.SUMMARY.update(state, new String[]{"a", "b"}, -1));
    }

    /** Two values that only ever follow each other: PPMI = [[0, ln 2], [ln 2, 0]], eigenvalues ±ln 2. */
    @Test
    public void testTwoValuesByHand() {
        final Spectral spectral = Spectral.fit(counts("a", "b", 10), 2, 256, true);
        Assertions.assertArrayEquals(new String[]{"a", "b"}, spectral.vocabulary);
        Assertions.assertEquals(2, spectral.rank());
        Assertions.assertEquals(Math.log(2), Math.abs(spectral.eigenvalues[0]), 1e-12);
        Assertions.assertEquals(Math.log(2), Math.abs(spectral.eigenvalues[1]), 1e-12);
        Assertions.assertEquals(0, spectral.eigenvalues[0] + spectral.eigenvalues[1], 1e-12);
        final double[] a = spectral.embed("a"), b = spectral.embed("b");
        // |coordinates| = sqrt(ln 2 / 2); the signed reconstruction Σ sign(λ_r) a_r b_r gives the matrix back
        for (int r = 0; r < 2; r++) {
            Assertions.assertEquals(Math.sqrt(Math.log(2) / 2), Math.abs(a[r]), 1e-12);
            Assertions.assertTrue(a[r] > 0, "the first value carries the largest loading of a tie: positive");
        }
        double ab = 0, aa = 0;
        for (int r = 0; r < 2; r++) {
            ab += Math.signum(spectral.eigenvalues[r]) * a[r] * b[r];
            aa += Math.signum(spectral.eigenvalues[r]) * a[r] * a[r];
        }
        Assertions.assertEquals(Math.log(2), ab, 1e-12);
        Assertions.assertEquals(0, aa, 1e-12);
        Assertions.assertNull(spectral.embed("c"));
        Assertions.assertNull(spectral.embed(null));
        // the apply path reads a row index and its coordinates one by one; embed hands out a copy, never the model's row
        Assertions.assertNull(spectral.indexOf("c"));
        Assertions.assertEquals(Integer.valueOf(0), spectral.indexOf("a"));
        Assertions.assertEquals(a[1], spectral.coordinate(spectral.indexOf("a"), 1), 0);
        Assertions.assertNull(spectral.coordinate(0, spectral.rank()));
        a[0] = 42;
        Assertions.assertNotEquals(42d, spectral.embed("a")[0]);
    }

    /** At full rank the signed factorisation reproduces the PPMI matrix computed by hand from the counts. */
    @Test
    public void testFactorisationReproducesPpmi() {
        final Spectral.PairCounts state = counts("a", "b", 8, "a", "c", 2, "b", "c", 5, "a", "a", 3, "c", "d", 7);
        final Spectral spectral = Spectral.fit(state, 4, 256, true);
        final String[] values = {"a", "b", "c", "d"};
        final double[][] c = new double[4][4];
        final Object[][] cells = {{0, 1, 8}, {0, 2, 2}, {1, 2, 5}, {0, 0, 3}, {2, 3, 7}};
        for (final Object[] cell : cells) {
            c[(int) cell[0]][(int) cell[1]] += (int) cell[2];
            c[(int) cell[1]][(int) cell[0]] += (int) cell[2];
        }
        final double[] row = new double[4];
        double total = 0;
        for (int i = 0; i < 4; i++) for (int j = 0; j < 4; j++) {
            row[i] += c[i][j];
            total += c[i][j];
        }
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                final double expected = c[i][j] > 0 ? Math.max(0, Math.log(c[i][j] * total / (row[i] * row[j]))) : 0;
                double actual = 0;
                for (int r = 0; r < 4; r++) actual += Math.signum(spectral.eigenvalues[r]) * spectral.embed(values[i])[r] * spectral.embed(values[j])[r];
                Assertions.assertEquals(expected, actual, 1e-9, values[i] + "/" + values[j]);
            }
        }
        // components by decreasing |eigenvalue|
        for (int r = 1; r < 4; r++) Assertions.assertTrue(Math.abs(spectral.eigenvalues[r - 1]) >= Math.abs(spectral.eigenvalues[r]) - 1e-12);
    }

    /** Values that share neighbourhoods land together: two groups that transition within themselves, rarely across. */
    @Test
    public void testNeighbourhoodsSeparate() {
        final Spectral.PairCounts state = new Spectral.PairCounts();
        final String[][] groups = {{"a1", "a2", "a3"}, {"b1", "b2", "b3"}};
        final Random random = new Random(5);
        for (final String[] group : groups) {
            for (int i = 0; i < 3; i++) for (int j = i + 1; j < 3; j++) state.add(group[i], group[j], 40 + random.nextInt(20));
        }
        state.add("a1", "b1", 2);
        final Spectral spectral = Spectral.fit(state, 2, 256, true);
        Assertions.assertEquals(6, spectral.vocabulary.length);
        Assertions.assertTrue(cosine(spectral.embed("a1"), spectral.embed("a2")) > 0.95);
        Assertions.assertTrue(cosine(spectral.embed("b2"), spectral.embed("b3")) > 0.95);
        Assertions.assertTrue(Math.abs(cosine(spectral.embed("a2"), spectral.embed("b2"))) < 0.05);
        // the same counts inserted in another order give the same columns bit for bit
        final Spectral.PairCounts reversed = new Spectral.PairCounts();
        reversed.add("b1", "a1", 2);
        for (int g = 1; g >= 0; g--) {
            for (int i = 2; i >= 0; i--) for (int j = 2; j > i; j--) reversed.add(groups[g][j], groups[g][i], state.counts.get(groups[g][i]).get(groups[g][j]));
        }
        final Spectral again = Spectral.fit(reversed, 2, 256, true);
        for (final String value : spectral.vocabulary) Assertions.assertArrayEquals(spectral.embed(value), again.embed(value), 0);
    }

    @Test
    public void testVocabularyCapAndDegenerateInputs() {
        // c co-occurs least: with room for two values it is left out and reads null
        final Spectral capped = Spectral.fit(counts("a", "b", 10, "a", "c", 1), 2, 2, false);
        Assertions.assertArrayEquals(new String[]{"a", "b"}, capped.vocabulary);
        Assertions.assertEquals(1, capped.dropped);
        Assertions.assertNull(capped.embed("c"));
        Assertions.assertNotNull(capped.embed("b"));
        // a value the cap keeps but whose every partner it dropped has no co-occurrence row: it reads null, not the
        // origin of the space (all four values tie on mass, so the string order keeps a, b, x and drops y — leaving x
        // with no partner at all)
        final Spectral isolated = Spectral.fit(counts("a", "b", 10, "x", "y", 10), 2, 3, false);
        Assertions.assertArrayEquals(new String[]{"a", "b"}, isolated.vocabulary);
        Assertions.assertEquals(2, isolated.dropped);
        Assertions.assertNull(isolated.embed("x"));
        Assertions.assertNull(isolated.embed("y"));
        // nothing co-occurring survives the cap (b's only partner is beyond it, a only repeats): no embedding at all
        Assertions.assertTrue(Spectral.fit(counts("a", "a", 9, "b", "c", 1), 2, 2, false).isEmpty());
        // rank beyond the vocabulary is capped
        Assertions.assertEquals(2, Spectral.fit(counts("a", "b", 10), 8, 256, false).rank());
        // nothing, or a single value repeating: no embedding
        Assertions.assertTrue(Spectral.fit(new Spectral.PairCounts(), 2, 256, false).isEmpty());
        final Spectral single = Spectral.fit(counts("a", "a", 9), 2, 256, false);
        Assertions.assertTrue(single.isEmpty());
        Assertions.assertNull(single.embed("a"));
        Assertions.assertEquals(9, single.pairs);
    }

    @Test
    public void testMergedTimeBlocksAndArtifactJson() {
        final Map<Long, Spectral.PairCounts> parts = new LinkedHashMap<>();
        parts.put(0L, counts("a", "b", 4, "b", "c", 1));
        parts.put(1L, counts("a", "b", 2, "a", "c", 6));
        parts.put(3L, counts("c", "d", 5));
        final BlockSeries<Spectral.PairCounts> series = new BlockSeries<>(Spectral.SUMMARY, parts);
        final Spectral whole = Spectral.fit(counts("a", "b", 6, "b", "c", 1, "a", "c", 6, "c", "d", 5), 3, 256, true);
        final Spectral total = Spectral.fit(series.total(), 3, 256, true);
        for (final String value : whole.vocabulary) Assertions.assertArrayEquals(whole.embed(value), total.embed(value), 1e-12);
        // a forward row behind block 1 reads blocks 0 and 1 only: d has not been seen yet
        final Spectral prefix = series.models(0, s -> Spectral.fit(s, 3, 256, false)).get(1L);
        Assertions.assertNull(prefix.embed("d"));
        Assertions.assertArrayEquals(Spectral.fit(counts("a", "b", 6, "b", "c", 1, "a", "c", 6), 3, 256, true).embed("a"), prefix.embed("a"), 1e-12);

        final Spectral back = Spectral.fromJson(com.google.gson.JsonParser.parseString(whole.toJson().toString()).getAsJsonObject());
        Assertions.assertArrayEquals(whole.vocabulary, back.vocabulary);
        Assertions.assertArrayEquals(whole.eigenvalues, back.eigenvalues, 0);
        Assertions.assertEquals(whole.pairs, back.pairs);
        for (final String value : whole.vocabulary) Assertions.assertArrayEquals(whole.embed(value), back.embed(value), 0);
        Assertions.assertTrue(Spectral.fromJson(com.google.gson.JsonParser.parseString(Spectral.fit(new Spectral.PairCounts(), 2, 256, false).toJson().toString()).getAsJsonObject()).isEmpty());
    }

}
