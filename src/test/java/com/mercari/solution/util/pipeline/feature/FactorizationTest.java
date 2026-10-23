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

    /** between: the sum of the known cross-side pairs; an unknown member drops out, nothing known reads null. */
    @Test
    public void testBetween() {
        final Factorization.Model model = Factorization.fit(new Factorization.Options(List.of("a", "b", "c"), true, 2, 10, 0.01, 7L), noisy(200, 5));
        final String[] row = {"a1", "b2", "c0"};
        Assertions.assertEquals(model.pair(row, 0, 2) + model.pair(row, 1, 2), model.between(row, null, new int[]{0, 1}, new int[]{2}), 1e-12);
        final String[] unseen = {"a99", "b2", "c0"};
        Assertions.assertEquals(model.pair(unseen, 1, 2), model.between(unseen, null, new int[]{0, 1}, new int[]{2}), 1e-12);
        Assertions.assertNull(model.between(new String[]{"a99", null, "c0"}, null, new int[]{0, 1}, new int[]{2}));
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

    /**
     * The Plackett–Luce log-likelihood by hand: places (1, 2, 2) choose the winner from all three, and the tied last
     * pair is no choice (they finish there in some order); places (1, 2, 2, 4) choose each tied second from the pool of
     * three (Breslow); equal targets throughout choose nothing. With top 1 only the winners are choices.
     */
    @Test
    public void testPlackettLuceLikelihood() {
        final List<Factorization.Example> examples = List.of(
                new Factorization.Example(new String[]{"p1"}, 1, 0, 0, "c1"),
                new Factorization.Example(new String[]{"p2"}, 2, 0, 0, "c1"),
                new Factorization.Example(new String[]{"p3"}, 2, 0, 0, "c1"),
                new Factorization.Example(new String[]{"p4"}, 5, 0, 0, "c2"),   // alone: no information
                new Factorization.Example(new String[]{"p5"}, 3, 0, 0, null),   // no contest
                new Factorization.Example(new String[]{"p1"}, 1, 0, 0, "c3"),
                new Factorization.Example(new String[]{"p2"}, 2, 0, 0, "c3"),
                new Factorization.Example(new String[]{"p3"}, 2, 0, 0, "c3"),
                new Factorization.Example(new String[]{"p4"}, 3, 0, 0, "c3"),
                new Factorization.Example(new String[]{"p1"}, 0, 0, 0, "c4"),   // all tied: no information
                new Factorization.Example(new String[]{"p2"}, 0, 0, 0, "c4"));
        final double[] s = {0.3, -0.2, 0.5, 2.0, 1.0, 0.1, 0.4, -0.6, 0.8, 1.5, -1.5};
        final Factorization.PlackettLuce full = new Factorization.PlackettLuce(examples, s.clone(), 0, false);
        full.refresh();
        final double all = Math.log(Math.exp(0.3) + Math.exp(-0.2) + Math.exp(0.5));
        final double all3 = Math.log(Math.exp(0.1) + Math.exp(0.4) + Math.exp(-0.6) + Math.exp(0.8));
        final double second3 = Math.log(Math.exp(0.4) + Math.exp(-0.6) + Math.exp(0.8));
        Assertions.assertEquals(0.3 - all + (0.1 - all3) + (0.4 - second3) + (-0.6 - second3), full.logLikelihood(), 1e-12);
        final Factorization.PlackettLuce winner = new Factorization.PlackettLuce(examples, s.clone(), 1, false);
        winner.refresh();
        Assertions.assertEquals(0.3 - all + (0.1 - all3), winner.logLikelihood(), 1e-12);
        // descending: the larger target is the better place (p2 / p3 tie for the win, p1 is last and chooses nothing)
        final Factorization.PlackettLuce descending = new Factorization.PlackettLuce(examples, s.clone(), 0, true);
        descending.refresh();
        final double rest3 = Math.log(Math.exp(0.1) + Math.exp(0.4) + Math.exp(-0.6));
        Assertions.assertEquals((-0.2 - all) + (0.5 - all) + (0.8 - all3) + (0.4 - rest3) + (-0.6 - rest3),
                descending.logLikelihood(), 1e-12);
    }

    /** The coordinate step's gradient and curvature are those of the log-likelihood along the slope direction. */
    @Test
    public void testLossDerivativesMatchFiniteDifferences() {
        final java.util.Random random = new java.util.Random(5);
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int c = 0; c < 6; c++) {
            for (int j = 0; j < 5; j++) {
                // integer targets with ties; a probability for the logistic loss
                examples.add(new Factorization.Example(new String[]{"p" + j}, random.nextInt(3), 0, 0, "c" + c));
            }
        }
        final int n = examples.size();
        final double[] s = new double[n];
        final double[] h = new double[n];
        final int[] rows = new int[n];
        for (int i = 0; i < n; i++) {
            s[i] = random.nextGaussian();
            h[i] = random.nextInt(3) == 0 ? 0 : random.nextGaussian();
            rows[i] = i;
        }
        final List<Factorization.Example> rates = new ArrayList<>();
        for (final Factorization.Example e : examples) rates.add(new Factorization.Example(e.values(), e.y() / 2, 0, 0, null));
        for (final int top : List.of(0, 1, 2)) {
            assertDerivatives(s, h, rows, t -> new Factorization.PlackettLuce(examples, t, top, top == 2));
        }
        assertDerivatives(s, h, rows, t -> new Factorization.Logistic(rates, t));
        assertDerivatives(s, h, rows, t -> new Factorization.Squared(rates, t));
    }

    private static void assertDerivatives(final double[] s, final double[] h, final int[] rows,
                                          final java.util.function.Function<double[], Factorization.Objective> objective) {
        final double eps = 1e-4;
        final java.util.function.DoubleUnaryOperator ll = t -> {
            final double[] moved = s.clone();
            for (int i = 0; i < moved.length; i++) moved[i] += t * h[i];
            final Factorization.Objective o = objective.apply(moved);
            o.refresh();
            return o.logLikelihood();
        };
        final Factorization.Objective at = objective.apply(s.clone());
        at.refresh();
        final double[] gh = at.derivatives(rows, 0, rows.length, h);
        final double f0 = ll.applyAsDouble(0), plus = ll.applyAsDouble(eps), minus = ll.applyAsDouble(-eps);
        Assertions.assertEquals((plus - minus) / (2 * eps), gh[0], 1e-6, "gradient");
        Assertions.assertEquals(-(plus - 2 * f0 + minus) / (eps * eps), gh[1], 1e-3, "curvature");
        // shift keeps the cached pool sums: the likelihood after a move equals a fresh evaluation at the moved scores
        at.shift(rows, 0, rows.length, h, 0.3);
        Assertions.assertEquals(ll.applyAsDouble(0.3), at.logLikelihood(), 1e-9, "shift");
    }

    /**
     * Plackett–Luce recovers player strengths from contest orders alone, and a field constant within every contest (the
     * venue) gets no main effect: contests only see score differences. Its interaction with the player is identified.
     */
    @Test
    public void testPlackettLuceRecoversStrengths() {
        final java.util.Random random = new java.util.Random(23);
        final double[] strength = new double[10];
        for (int p = 0; p < strength.length; p++) strength[p] = (p - 4.5) / 3;
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int c = 0; c < 400; c++) {
            final String venue = "v" + (c % 2);
            final List<Integer> players = new ArrayList<>();
            for (int p = 0; p < strength.length; p++) players.add(p);
            java.util.Collections.shuffle(players, random);
            final List<Integer> entries = players.subList(0, 5);
            // performance = strength + a venue aptitude (even players like v0) + Gumbel noise: a Plackett–Luce draw
            final double[] performance = new double[entries.size()];
            for (int j = 0; j < entries.size(); j++) {
                final int p = entries.get(j);
                final double aptitude = (p % 2 == 0) == (c % 2 == 0) ? 0.8 : -0.8;
                performance[j] = strength[p] + aptitude - Math.log(-Math.log(random.nextDouble()));
            }
            for (int j = 0; j < entries.size(); j++) {
                int place = 1;
                for (final double other : performance) if (other > performance[j]) place++;
                examples.add(new Factorization.Example(new String[]{"p" + entries.get(j), venue}, place, 0, 0, "c" + c));
            }
        }
        final Factorization.Options options = new Factorization.Options(List.of("player", "venue"), false, 2, 30, 0.1, 3L,
                Factorization.Loss.plackettLuce, 0, false, true);
        final Factorization.Model model = Factorization.fit(options, examples);
        Assertions.assertEquals("plackettLuce", model.loss);
        Assertions.assertEquals(0, model.w0, 0d);
        double sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
        for (int p = 0; p < strength.length; p++) {
            final double w = model.params.get(0).get("p" + p)[0];
            sx += strength[p];
            sy += w;
            sxx += strength[p] * strength[p];
            syy += w * w;
            sxy += strength[p] * w;
        }
        final int m = strength.length;
        final double corr = (sxy - sx * sy / m) / Math.sqrt((sxx - sx * sx / m) * (syy - sy * sy / m));
        Assertions.assertTrue(corr > 0.95, "strength correlation " + corr);
        for (final String venue : List.of("v0", "v1")) Assertions.assertEquals(0, model.params.get(1).get(venue)[0], 1e-9, venue);
        // the aptitude: an even player is better at v0 than at v1, an odd one the other way round
        final double even = model.pair(new String[]{"p2", "v0"}, 0, 1) - model.pair(new String[]{"p2", "v1"}, 0, 1);
        final double odd = model.pair(new String[]{"p3", "v0"}, 0, 1) - model.pair(new String[]{"p3", "v1"}, 0, 1);
        Assertions.assertTrue(even > 0.5 && odd < -0.5, "even " + even + " odd " + odd);
    }

    /**
     * Plackett–Luce: a lone entry or a contest of equal targets carries no information — it counts towards neither
     * minRows nor a value's count.
     */
    @Test
    public void testPlackettLuceLoneEntriesTakeNoPart() {
        final List<Factorization.Example> examples = List.of(
                new Factorization.Example(new String[]{"p1", "v"}, 1, 0, 0, "c1"),
                new Factorization.Example(new String[]{"p2", "v"}, 2, 0, 0, "c1"),
                new Factorization.Example(new String[]{"p3", "v"}, 1, 0, 0, "c2"),    // alone
                new Factorization.Example(new String[]{"p4", "v"}, 0, 0, 0, "c3"),    // all tied (nobody sold)
                new Factorization.Example(new String[]{"p1", "v"}, 0, 0, 0, "c3"));
        final Factorization.Options options = new Factorization.Options(List.of("player", "venue"), false, 2, 5, 0.1, 1L,
                Factorization.Loss.plackettLuce, 0, false, true);
        final Factorization.Model model = Factorization.fit(options, examples);
        Assertions.assertEquals(0L, model.count(new String[]{"p3", "v"}, 0));
        Assertions.assertEquals(0L, model.count(new String[]{"p4", "v"}, 0));
        Assertions.assertEquals(1L, model.count(new String[]{"p1", "v"}, 0));
        Assertions.assertEquals(2L, model.count(new String[]{"p1", "v"}, 1));
        Assertions.assertNull(Factorization.fit(options, examples, 3));
        Assertions.assertNotNull(Factorization.fit(options, examples, 2));
    }

    /** Logistic loss: the fitted probability of a cell follows its observed rate; linear: false fits no per-value weight. */
    @Test
    public void testLogisticAndInteractionsOnly() {
        final List<Factorization.Example> examples = new ArrayList<>();
        // rate 0.8 for (a1, b1) and (a2, b2), 0.2 for the crossed cells
        for (int i = 0; i < 50; i++) {
            for (final String a : List.of("a1", "a2")) {
                for (final String b : List.of("b1", "b2")) {
                    final boolean match = a.substring(1).equals(b.substring(1));
                    examples.add(new Factorization.Example(new String[]{a, b}, (i % 5 < (match ? 4 : 1)) ? 1 : 0));
                }
            }
        }
        final Factorization.Options options = new Factorization.Options(List.of("a", "b"), false, 2, 40, 0.01, 1L,
                Factorization.Loss.logistic, 0, false, true);
        final Factorization.Model model = Factorization.fit(options, examples);
        Assertions.assertEquals(0.8, Factorization.Logistic.sigmoid(model.predict(new String[]{"a1", "b1"})), 0.02);
        Assertions.assertEquals(0.2, Factorization.Logistic.sigmoid(model.predict(new String[]{"a1", "b2"})), 0.02);

        final Factorization.Model interactions = Factorization.fit(new Factorization.Options(List.of("a", "b"), false, 2, 40, 0.01, 1L,
                Factorization.Loss.logistic, 0, false, false), examples);
        for (final Map<String, double[]> values : interactions.params) for (final double[] p : values.values()) Assertions.assertEquals(0, p[0], 0d);
        Assertions.assertTrue(interactions.pair(new String[]{"a1", "b1"}, 0, 1) > interactions.pair(new String[]{"a1", "b2"}, 0, 1));
    }

    /**
     * A numeric field has one vector, scaled by its standardised value: a slope that differs by category is the pair
     * of the category with the numeric field. The standardisation is the fit's own and travels with the artifact.
     */
    @Test
    public void testNumericField() {
        final java.util.Random random = new java.util.Random(31);
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            final String a = "a" + random.nextInt(3);
            final double x = 50 + 10 * random.nextGaussian();
            // the slope of x is +2 (per sd) for a0, 0 for a1, -2 for a2
            final double slope = a.equals("a0") ? 2 : a.equals("a1") ? 0 : -2;
            final double y = slope * (x - 50) / 10 + 0.05 * random.nextGaussian();
            examples.add(new Factorization.Example(new String[]{a, Factorization.NUMERIC}, new double[]{Double.NaN, x}, y, 0, 0, null));
        }
        final java.util.ArrayList<String> scales = new java.util.ArrayList<>(java.util.Arrays.asList(null, "zscore"));
        final Factorization.Options options = new Factorization.Options(List.of("a", "x"), false, 2, 60, 0.001, 9L,
                Factorization.Loss.squared, 0, false, true, scales);
        final Factorization.Model model = Factorization.fit(options, examples);
        Assertions.assertArrayEquals(new boolean[]{false, true}, model.numeric);
        Assertions.assertEquals(50, model.mean[1], 1.5);
        Assertions.assertEquals(10, model.scale[1], 1.0);
        // the per-unit slope of x by category, in units of the fitted standard deviation
        final double[] slopes = new double[3];
        for (int c = 0; c < 3; c++) {
            final String[] values = {"a" + c, Factorization.NUMERIC};
            slopes[c] = model.pair(values, null, 0, 1, true) + model.params.get(1).get(Factorization.NUMERIC)[0];
        }
        Assertions.assertEquals(2, slopes[0] * 10 / model.scale[1], 0.3, java.util.Arrays.toString(slopes));
        Assertions.assertEquals(0, slopes[1] * 10 / model.scale[1], 0.3, java.util.Arrays.toString(slopes));
        Assertions.assertEquals(-2, slopes[2] * 10 / model.scale[1], 0.3, java.util.Arrays.toString(slopes));
        // the pair at a row's value is the slope times its standardised value; a missing value contributes nothing
        final double[] at60 = {Double.NaN, 60};
        final String[] row = {"a0", Factorization.NUMERIC};
        Assertions.assertEquals(model.pair(row, null, 0, 1, true) * (60 - model.mean[1]) / model.scale[1], model.pair(row, at60, 0, 1, false), 1e-12);
        Assertions.assertNull(model.pair(new String[]{"a0", null}, new double[]{Double.NaN, Double.NaN}, 0, 1, false));
        Assertions.assertEquals(model.w0 + model.params.get(0).get("a0")[0], model.predict(new String[]{"a0", null}, new double[]{Double.NaN, Double.NaN}), 1e-12);
        // the slope per unit does not depend on the row's value, a missing one included
        Assertions.assertEquals(model.pair(row, null, 0, 1, true), model.pair(new String[]{"a0", null}, new double[]{Double.NaN, Double.NaN}, 0, 1, true));
        Assertions.assertEquals(600L, model.count(row, 1));
        // between reads the numeric side at the row's value; a missing value leaves no known pair; no values = refused
        Assertions.assertEquals(model.pair(row, at60, 0, 1, false), model.between(row, at60, new int[]{0}, new int[]{1}), 1e-12);
        Assertions.assertNull(model.between(new String[]{"a0", null}, new double[]{Double.NaN, Double.NaN}, new int[]{0}, new int[]{1}));
        Assertions.assertThrows(IllegalStateException.class, () -> model.between(row, null, new int[]{0}, new int[]{1}));

        final String dir = "target/feature-artifacts/" + UUID.randomUUID();
        Factorization.write(dir, "hash1", "fm", model);
        final Factorization.Model loaded = Factorization.read(dir, "hash1", "fm", List.of("a", "x"), new boolean[]{false, true}, false, 2);
        Assertions.assertEquals(model.mean[1], loaded.mean[1], 0d);
        Assertions.assertEquals(model.scale[1], loaded.scale[1], 0d);
        Assertions.assertEquals(model.predict(row, at60), loaded.predict(row, at60), 1e-12);
    }

    /**
     * Numeric fields under fwfm (the slope through r and the per-pair update), the standardisation of a value far from 0
     * with a small spread (one-pass Σx² / n − mean² cancels away there), and a numeric value that is not finite read as
     * missing whatever the example's text says.
     */
    @Test
    public void testNumericFieldStandardisation() {
        final java.util.Random random = new java.util.Random(43);
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            final String a = "a" + random.nextInt(3);
            final double z = random.nextGaussian();
            final double slope = a.equals("a0") ? 2 : a.equals("a1") ? 0 : -2;
            examples.add(new Factorization.Example(new String[]{a, Factorization.NUMERIC}, new double[]{Double.NaN, 1e9 + 10 * z},
                    slope * z + 0.05 * random.nextGaussian(), 0, 0, null));
        }
        // present by its text, but without a finite value: missing
        examples.add(new Factorization.Example(new String[]{"a0", Factorization.NUMERIC}, new double[]{Double.NaN, Double.NaN}, 5, 0, 0, null));
        final java.util.ArrayList<String> scales = new java.util.ArrayList<>(java.util.Arrays.asList(null, "zscore"));
        final Factorization.Model model = Factorization.fit(new Factorization.Options(List.of("a", "x"), true, 2, 60, 0.001, 9L,
                Factorization.Loss.squared, 0, false, true, scales), examples);
        Assertions.assertEquals(1e9, model.mean[1], 2);
        Assertions.assertEquals(10, model.scale[1], 1.0);
        Assertions.assertTrue(Double.isFinite(model.w0));
        Assertions.assertEquals(600L, model.count(new String[]{"a0", Factorization.NUMERIC}, 1));
        for (int c = 0; c < 3; c++) {
            final double expected = c == 0 ? 2 : c == 1 ? 0 : -2;
            final double slope = model.pair(new String[]{"a" + c, null}, null, 0, 1, true) + model.params.get(1).get(Factorization.NUMERIC)[0];
            Assertions.assertEquals(expected, slope * 10 / model.scale[1], 0.3, "a" + c);
        }
    }

    /**
     * A warm start carries a numeric field's slope per raw unit over a change of scale: block 1 widens the spread four
     * times (the centre stays), and the change point 1 model started from change point 0's — with no epoch of its own —
     * predicts every raw value as change point 0 did. The categorical-only readouts refuse a model with a numeric field.
     */
    @Test
    public void testNumericWarmStartKeepsRawSlope() {
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            final long block = i < 100 ? 0 : 1;
            final String a = "a" + i % 3;
            final double x = 50 + (i % 2 == 0 ? 1 : -1) * (block == 0 ? 10 : 40);
            final double slope = a.equals("a0") ? 2 : a.equals("a1") ? 0 : -2;
            examples.add(new Factorization.Example(new String[]{a, Factorization.NUMERIC}, new double[]{Double.NaN, x},
                    slope * (x - 50) / 10, 0, block, null));
        }
        final java.util.ArrayList<String> scales = new java.util.ArrayList<>(java.util.Arrays.asList(null, "zscore"));
        final Factorization.Options options = new Factorization.Options(List.of("a", "x"), false, 2, 30, 0.01, 5L,
                Factorization.Loss.squared, 0, false, true, scales);
        final Factorization.Fits fits = Factorization.fitAll(options, examples, new Factorization.Plan(0, true, 0, 0, Alignment.PROCRUSTES, true, 0));
        final Factorization.Model first = fits.blocks.get(0L), second = fits.blocks.get(1L);
        Assertions.assertEquals(10, first.scale[1], 1e-9);
        Assertions.assertEquals(Math.sqrt((100 + 1600) / 2d), second.scale[1], 1e-9);
        for (final String a : List.of("a0", "a1", "a2")) {
            for (final double x : new double[]{10, 45, 70, 90}) {
                final String[] values = {a, Factorization.NUMERIC};
                final double[] raw = {Double.NaN, x};
                Assertions.assertEquals(first.predict(values, raw), second.predict(values, raw), 1e-9, a + " at " + x);
            }
        }
        Assertions.assertThrows(IllegalStateException.class, () -> first.predict(new String[]{"a0", Factorization.NUMERIC}));
        Assertions.assertThrows(IllegalStateException.class, () -> first.pair(new String[]{"a0", Factorization.NUMERIC}, 0, 1));
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
