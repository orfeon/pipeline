package com.mercari.solution.util.pipeline.feature;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * How a factorization fit is served by the engine: the coders of its examples, models and fits, the streaming
 * forward fit behind the change-point join, and the index a row finds its change point with.
 */
public class FactorizationServeTest {

    private static final Factorization.Options OPTIONS = new Factorization.Options(List.of("seller", "category", "grade"), false, 3, 8, 0.05, 7L);

    /** Three categorical fields over a few values, tagged with one of {@code blocks} time blocks. */
    private static List<Factorization.Example> examples(final int n, final int blocks) {
        final Random random = new Random(11);
        final List<Factorization.Example> examples = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final String seller = "s" + random.nextInt(6);
            final String category = "c" + random.nextInt(4);
            final String grade = random.nextInt(10) == 0 ? null : "g" + random.nextInt(3);
            final double y = (seller.hashCode() % 3) * 0.3 + (category.equals("c1") ? 0.5 : 0) + random.nextGaussian() * 0.1;
            examples.add(new Factorization.Example(new String[]{seller, category, grade}, y, i % blocks));
        }
        return examples;
    }

    private static <T> T roundTrip(final Coder<T> coder, final T value) throws Exception {
        return CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, value));
    }

    private static void assertSameModel(final Factorization.Model expected, final Factorization.Model actual) {
        if (expected == null) {
            Assertions.assertNull(actual);
            return;
        }
        Assertions.assertEquals(expected.fields, actual.fields);
        Assertions.assertEquals(expected.fieldWeighted, actual.fieldWeighted);
        Assertions.assertEquals(expected.k, actual.k);
        Assertions.assertEquals(expected.w0, actual.w0, 0d);
        Assertions.assertEquals(expected.loss, actual.loss);
        Assertions.assertArrayEquals(expected.numeric, actual.numeric);
        Assertions.assertArrayEquals(expected.mean, actual.mean, 0d);
        Assertions.assertArrayEquals(expected.scale, actual.scale, 0d);
        for (int f = 0; f < expected.fields.size(); f++) {
            Assertions.assertArrayEquals(expected.r[f], actual.r[f], 0d);
            final Map<String, double[]> params = expected.params.get(f);
            Assertions.assertEquals(params.keySet(), actual.params.get(f).keySet());
            for (final Map.Entry<String, double[]> e : params.entrySet()) {
                Assertions.assertArrayEquals(e.getValue(), actual.params.get(f).get(e.getKey()), 0d);
            }
        }
    }

    @Test
    public void testExampleCoderRoundTrip() throws Exception {
        final Factorization.Example plain = new Factorization.Example(new String[]{"s1", null, "g2"}, 0.25, 3L);
        final Factorization.Example full = new Factorization.Example(new String[]{"s1", Factorization.NUMERIC, "ü"},
                new double[]{Double.NaN, 4.5, 0d}, -1.5, Math.log(0.2), -1L, "{\"__contestTime\":1}");
        for (final Factorization.Example e : List.of(plain, full)) {
            final Factorization.Example decoded = roundTrip(FactorizationCoders.ExampleCoder.of(), e);
            Assertions.assertArrayEquals(e.values(), decoded.values());
            Assertions.assertArrayEquals(e.x(), decoded.x());
            Assertions.assertEquals(e.y(), decoded.y(), 0d);
            Assertions.assertEquals(e.offset(), decoded.offset(), 0d);
            Assertions.assertEquals(e.tag(), decoded.tag());
            Assertions.assertEquals(e.contest(), decoded.contest());
            Assertions.assertEquals(0, Factorization.ORDER.compare(e, decoded));
        }
    }

    @Test
    public void testModelAndFitsCoderRoundTrip() throws Exception {
        final List<Factorization.Example> examples = examples(400, 3);
        final Factorization.Model model = Factorization.fit(OPTIONS, examples);
        assertSameModel(model, roundTrip(FactorizationCoders.ModelCoder.of(), model));

        final Factorization.Fits forward = Factorization.fitAll(OPTIONS, examples,
                new Factorization.Plan(0, true, 1, 0, Alignment.PROCRUSTES, true, 4));
        final Factorization.Fits forwardDecoded = roundTrip(FactorizationCoders.FitsCoder.of(), forward);
        assertSameModel(forward.total, forwardDecoded.total);
        Assertions.assertEquals(forward.blocks.keySet(), forwardDecoded.blocks.keySet());
        for (final long at : forward.blocks.keySet()) assertSameModel(forward.blocks.get(at), forwardDecoded.blocks.get(at));
        Assertions.assertEquals(forward.observedBlocks, forwardDecoded.observedBlocks);
        Assertions.assertNull(forwardDecoded.folds);

        final List<Factorization.Example> folded = new ArrayList<>();
        for (final Factorization.Example e : examples) folded.add(new Factorization.Example(e.values(), e.y(), e.tag() % 2));
        final Factorization.Fits folds = Factorization.fitAll(OPTIONS, folded, new Factorization.Plan(2, false, 0, 0, Alignment.PROCRUSTES, false, 0));
        // what rows read leaves the whole-input model out
        final Factorization.Fits served = roundTrip(FactorizationCoders.FitsCoder.of(), folds.served());
        Assertions.assertNull(served.total);
        Assertions.assertEquals(2, served.folds.length);
        for (int f = 0; f < 2; f++) assertSameModel(folds.folds[f], served.folds[f]);
        Assertions.assertNull(served.blocks);
        final Factorization.Fits statik = Factorization.Fits.ofTotal(model);
        Assertions.assertSame(statik, statik.served());
    }

    /** The streaming forward fit hands out, change point by change point, the very models fitAll collects. */
    @Test
    public void testStreamingForwardMatchesFitAll() {
        final List<Factorization.Example> examples = examples(600, 5);
        for (final int window : new int[]{0, 2}) {
            final Factorization.Plan plan = new Factorization.Plan(0, true, window, 90, Alignment.PROCRUSTES, true, 4);
            final Factorization.Fits fits = Factorization.fitAll(OPTIONS, examples, plan);
            final TreeMap<Long, Factorization.Model> streamed = new TreeMap<>();
            final List<Long> order = new ArrayList<>();
            final Factorization.Model total = Factorization.fitForward(OPTIONS, examples, plan, (at, model) -> {
                order.add(at);
                streamed.put(at, model);
            });
            Assertions.assertEquals(new ArrayList<>(fits.blocks.keySet()), order, "in time order, every change point once");
            for (final long at : fits.blocks.keySet()) assertSameModel(fits.blocks.get(at), streamed.get(at));
            assertSameModel(fits.total, total);
            Assertions.assertEquals(fits.observedBlocks, Factorization.observedBlocks(examples));
        }
    }

    /** The change point a row is keyed by is the one whose model the side-input lookup would serve it. */
    @Test
    public void testIndexFindsTheLookupChangePoint() {
        final TreeSet<Long> observed = new TreeSet<>(List.of(2L, 3L, 5L, 9L));
        final TreeSet<Long> changePoints = new TreeSet<>(observed);
        for (final long block : observed) changePoints.add(block + 2);
        final TreeMap<Long, Long> models = new TreeMap<>();
        for (final long at : changePoints) models.put(at, at);
        final FeatureStages.FmIndex index = new FeatureStages.FmIndex(changePoints, observed);
        for (int minBlocks = 1; minBlocks <= 3; minBlocks++) {
            for (long usable = 0; usable <= 13; usable++) {
                Assertions.assertEquals(BlockSeries.lookup(models, observed, usable, minBlocks), index.changePointOf(usable, minBlocks),
                        "usable " + usable + ", minBlocks " + minBlocks);
            }
        }
        Assertions.assertNull(index.changePointOf(null, 1));
    }
}
