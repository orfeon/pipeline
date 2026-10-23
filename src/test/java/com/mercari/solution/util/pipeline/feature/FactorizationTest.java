package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
            for (int f = 0; f < 3; f++) Assertions.assertEquals(model.count(e.values(), f), loaded.count(e.values(), f));
        }
    }

    /** The training examples of a value are counted: 0 for a value the fit never saw, null for a missing one. */
    @Test
    public void testCount() {
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), false, 2, 5, 0.01, 7L);
        final Factorization.Model model = Factorization.fit(options, xor(5));
        Assertions.assertEquals(10L, model.count(new String[]{"a1", "b1", "c0"}, 0));
        Assertions.assertEquals(8L, model.count(new String[]{"a1", "b1", "c0"}, 2)); // i % 3 == 0 for i = 0, 3 → 2 repeats × 4 rows
        Assertions.assertEquals(0L, model.count(new String[]{"a9", "b1", "c0"}, 0));
        Assertions.assertNull(model.count(new String[]{null, "b1", "c0"}, 0));
    }

    /** The fit is a function of the example set: the order the runner delivers them in does not reach the model. */
    @Test
    public void testFitIsOrderIndependent() {
        for (final boolean fieldWeighted : List.of(false, true)) {
            final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), fieldWeighted, 3, 15, 0.05, 11L);
            final List<Factorization.Example> examples = noisy(200, 5);
            final Factorization.Model model = Factorization.fit(options, examples);
            final List<Factorization.Example> shuffled = new ArrayList<>(examples);
            java.util.Collections.shuffle(shuffled, new java.util.Random(3));
            final Factorization.Model again = Factorization.fit(options, shuffled);
            for (final Factorization.Example e : examples) {
                Assertions.assertEquals(model.predict(e.values()), again.predict(e.values()), 0d);
            }
        }
    }

    /** fm (the cached-slope path) fits main effects plus an a × b interaction. */
    @Test
    public void testFmFitsAnInteraction() {
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), false, 4, 60, 0.001, 5L);
        final List<Factorization.Example> examples = noisy(400, 9);
        final Factorization.Model model = Factorization.fit(options, examples);
        double sse = 0, sst = 0, mean = 0;
        for (final Factorization.Example e : examples) mean += e.y() / examples.size();
        for (final Factorization.Example e : examples) {
            final double p = model.predict(e.values());
            sse += (p - e.y()) * (p - e.y());
            sst += (e.y() - mean) * (e.y() - mean);
        }
        Assertions.assertTrue(sse / sst < 0.2, "R² too low: " + (1 - sse / sst));
    }

    /** Rotating every factor by one orthogonal map leaves every pair score and prediction, and moves the embeddings. */
    @Test
    public void testAlignmentKeepsPairsAndPredictions() {
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), true, 3, 15, 0.05, 2L);
        final List<Factorization.Example> examples = noisy(200, 5);
        final Factorization.Model reference = Factorization.fit(options, examples);
        // another seed: the same fit up to the gauge, roughly — aligned into the reference's coordinates
        final Factorization.Model other = Factorization.fit(new Factorization.Options(List.of("a", "b", "c"), true, 3, 15, 0.05, 99L), examples);
        final Map<String, Double> before = new java.util.HashMap<>();
        for (final Factorization.Example e : examples) before.put(String.join("|", e.values()), other.predict(e.values()));
        final double[] embeddingBefore = other.embedding(examples.get(0).values(), 0, 3);
        final double distanceBefore = distance(embeddingBefore, reference.embedding(examples.get(0).values(), 0, 3));
        final Factorization.Model aligned = other.alignTo(reference, Alignment.PROCRUSTES);
        for (final Factorization.Example e : examples) {
            Assertions.assertEquals(before.get(String.join("|", e.values())), aligned.predict(e.values()), 1e-9);
        }
        Assertions.assertTrue(distance(aligned.embedding(examples.get(0).values(), 0, 3), reference.embedding(examples.get(0).values(), 0, 3)) <= distanceBefore + 1e-12);
        // the input model is untouched; none: no rotation
        Assertions.assertArrayEquals(embeddingBefore, other.embedding(examples.get(0).values(), 0, 3), 0d);
        Assertions.assertSame(other, other.alignTo(reference, Alignment.NONE));
    }

    /**
     * fit.mode forward: one model per change point over the blocks up to it; a block's targets reach only the change
     * points at or after it, a window drops the blocks behind it, and a window below minRows is not solved.
     */
    @Test
    public void testForwardModelsReadEarlierBlocksOnly() {
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), false, 2, 10, 0.05, 4L);
        final List<Factorization.Example> examples = tagged(noisy(300, 5), 3);
        final Factorization.Plan plan = new Factorization.Plan(0, true, 0, 0, Alignment.PROCRUSTES, true, 5);
        final Factorization.Fits fits = Factorization.fitAll(options, examples, plan);
        Assertions.assertEquals(java.util.Set.of(0L, 1L, 2L), fits.blocks.keySet());
        Assertions.assertNull(fits.modelFor(null, -1L, 1));
        Assertions.assertSame(fits.blocks.get(1L), fits.modelFor(null, 1L, 1));
        Assertions.assertNull(fits.modelFor(null, 1L, 3), "minBlocks: two observed blocks are fewer than three");
        Assertions.assertSame(fits.blocks.get(2L), fits.total, "without a window the last change point reads every block");

        // a later block's targets do not move the earlier models (bit for bit)
        final List<Factorization.Example> changed = new ArrayList<>();
        for (final Factorization.Example e : examples) changed.add(e.tag() == 2 ? new Factorization.Example(e.values(), e.y() + 5, e.tag()) : e);
        final Factorization.Fits again = Factorization.fitAll(options, changed, plan);
        for (final Factorization.Example e : examples) {
            Assertions.assertEquals(fits.blocks.get(0L).predict(e.values()), again.blocks.get(0L).predict(e.values()), 0d);
            Assertions.assertEquals(fits.blocks.get(1L).predict(e.values()), again.blocks.get(1L).predict(e.values()), 0d);
            Assertions.assertEquals(fits.blocks.get(1L).embedding(e.values(), 0, 2)[0], again.blocks.get(1L).embedding(e.values(), 0, 2)[0], 0d);
        }
        Assertions.assertNotEquals(fits.blocks.get(2L).predict(examples.get(0).values()), again.blocks.get(2L).predict(examples.get(0).values()));

        // the change point 1 model counts the rows of blocks 0 and 1 only
        long rows = 0;
        for (final Factorization.Example e : examples) if (e.tag() <= 1 && e.values()[2].equals("c0")) rows++;
        Assertions.assertEquals(rows, fits.blocks.get(1L).count(new String[]{null, null, "c0"}, 2));

        // window of one block: every change point reads its own block alone, and one past the last block reads nothing
        final Factorization.Fits windowed = Factorization.fitAll(options, examples, new Factorization.Plan(0, true, 1, 0, Alignment.PROCRUSTES, false, 5));
        Assertions.assertEquals(java.util.Set.of(0L, 1L, 2L, 3L), windowed.blocks.keySet());
        Assertions.assertNull(windowed.blocks.get(3L));
        long block1 = 0;
        for (final Factorization.Example e : examples) if (e.tag() == 1 && e.values()[2].equals("c0")) block1++;
        Assertions.assertEquals(block1, windowed.blocks.get(1L).count(new String[]{null, null, "c0"}, 2));
        // without a warm start the windowed fit of block 1 is a plain fit over block 1 (up to the gauge)
        final List<Factorization.Example> only1 = new ArrayList<>();
        for (final Factorization.Example e : examples) if (e.tag() == 1) only1.add(e);
        final Factorization.Model plain = Factorization.fit(options, only1);
        Assertions.assertEquals(plain.predict(only1.get(0).values()), windowed.blocks.get(1L).predict(only1.get(0).values()), 1e-9);
        Assertions.assertNotNull(windowed.total);

        // minRows above a block's examples: those change points are not solved
        final Factorization.Fits floor = Factorization.fitAll(options, examples, new Factorization.Plan(0, true, 1, 10_000, null, true, 5));
        Assertions.assertNull(floor.blocks.get(1L));
        Assertions.assertNull(floor.total);
    }

    /**
     * fit.mode fold: a fold's model is the plain fit over every other fold (up to the gauge it is rotated by), so
     * changing a fold's own targets never moves the model its rows read.
     */
    @Test
    public void testFoldModelsAreOutOfFold() {
        final Factorization.Options options = new Factorization.Options(List.of("a", "b", "c"), true, 2, 10, 0.05, 4L);
        final List<Factorization.Example> examples = tagged(noisy(300, 5), 3);
        final Factorization.Plan plan = new Factorization.Plan(3, false, 0, 0, Alignment.PROCRUSTES, false, 0);
        final Factorization.Fits fits = Factorization.fitAll(options, examples, plan);
        Assertions.assertEquals(3, fits.folds.length);
        Assertions.assertNull(fits.modelFor(null, null, 1), "a row without a fold reads nothing");
        for (int f = 0; f < 3; f++) {
            final Factorization.Model plain = Factorization.fit(options, Factorization.outOfFold(examples, f));
            for (final Factorization.Example e : examples) Assertions.assertEquals(plain.predict(e.values()), fits.modelFor(f, null, 1).predict(e.values()), 1e-9);
        }
        final List<Factorization.Example> changed = new ArrayList<>();
        for (final Factorization.Example e : examples) changed.add(e.tag() == 0 ? new Factorization.Example(e.values(), e.y() + 5, e.tag()) : e);
        final Factorization.Fits again = Factorization.fitAll(options, changed, plan);
        for (final Factorization.Example e : examples) {
            Assertions.assertEquals(fits.folds[0].predict(e.values()), again.folds[0].predict(e.values()), 1e-9);
        }
        Assertions.assertNotEquals(fits.folds[1].predict(examples.get(0).values()), again.folds[1].predict(examples.get(0).values()));
    }

    /** Noisy targets with main effects and an a × b interaction over a few values per field. */
    private static List<Factorization.Example> noisy(final int n, final int values) {
        final java.util.Random random = new java.util.Random(17);
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int a = random.nextInt(values), b = random.nextInt(values), c = random.nextInt(3);
            final double y = 0.3 * a - 0.2 * b + ((a + b) % 2 == 0 ? 1 : -1) * 0.8 + 0.1 * random.nextGaussian();
            examples.add(new Factorization.Example(new String[]{"a" + a, "b" + b, "c" + c}, y));
        }
        return examples;
    }

    /** The examples spread over {@code tags} tags in order (blocks / folds). */
    private static List<Factorization.Example> tagged(final List<Factorization.Example> examples, final int tags) {
        final List<Factorization.Example> out = new ArrayList<>();
        for (int i = 0; i < examples.size(); i++) {
            final Factorization.Example e = examples.get(i);
            out.add(new Factorization.Example(e.values(), e.y(), (long) i * tags / examples.size()));
        }
        return out;
    }

    private static double distance(final double[] a, final double[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) s += (a[i] - b[i]) * (a[i] - b[i]);
        return Math.sqrt(s);
    }

}
