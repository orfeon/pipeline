package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The log-signature family: the Lyndon basis, a known Lévy area, Chen's identity (concatenation = tensor product, the
 * {@code merge} of a monoid) and the inverse = the reversed path.
 */
public class SignatureTest {

    private static final long T0 = 1_700_000_000_000L;

    @Test
    public void testLyndonWordsCountByWittsFormula() {
        Assertions.assertEquals(5, Signature.lyndonWords(2, 3).size());   // a b ab aab abb
        Assertions.assertEquals(8, Signature.lyndonWords(2, 4).size());   // + aaab aabb abbb
        Assertions.assertEquals(14, Signature.lyndonWords(3, 3).size());  // 3 + 3 + 8
        Assertions.assertEquals(1, Signature.lyndonWords(1, 4).size());   // a single channel: its increment only
        Assertions.assertEquals(List.of("a", "b", "ab", "aab", "abb"), Signature.lyndonWords(2, 3).stream().map(Signature::wordName).toList());
    }

    private static SequenceEvaluator.Past past(final long millis, final double x, final double y) {
        final Map<String, Object> values = new HashMap<>();
        values.put("x", x);
        values.put("y", y);
        return new SequenceEvaluator.Past(millis, values);
    }

    private static Signature.State fold(final Signature family, final List<SequenceEvaluator.Past> path) {
        final Signature.State s = family.create();
        for (final SequenceEvaluator.Past p : path) family.update(s, family.event(p), 1);
        return s;
    }

    @Test
    public void testKnownLevyArea() {
        // (0,0) → (1,0) → (1,1): the increment in x then in y encloses an area of 1/2 with the chord
        final Signature family = new Signature(List.of("x", "y"), false, 2, true, null);
        final Signature.State s = fold(family, List.of(past(T0, 0, 0), past(T0 + 1, 1, 0), past(T0 + 2, 1, 1)));
        final List<String> words = family.words().stream().map(Signature::wordName).toList();
        Assertions.assertEquals(1.0, (Double) family.read(s, Summary.Readout.of("word", words.indexOf("a"))), 1e-12);
        Assertions.assertEquals(1.0, (Double) family.read(s, Summary.Readout.of("word", words.indexOf("b"))), 1e-12);
        Assertions.assertEquals(0.5, (Double) family.read(s, Summary.Readout.of("word", words.indexOf("ab"))), 1e-12);
        // the other way round the area changes sign; a straight path has none
        final Signature.State back = fold(family, List.of(past(T0, 0, 0), past(T0 + 1, 0, 1), past(T0 + 2, 1, 1)));
        Assertions.assertEquals(-0.5, (Double) family.read(back, Summary.Readout.of("word", words.indexOf("ab"))), 1e-12);
        final Signature.State line = fold(family, List.of(past(T0, 0, 0), past(T0 + 1, 0.5, 0.5), past(T0 + 2, 1, 1)));
        Assertions.assertEquals(0.0, (Double) family.read(line, Summary.Readout.of("word", words.indexOf("ab"))), 1e-12);
        // fewer than two points: no path
        Assertions.assertNull(family.read(fold(family, List.of(past(T0, 3, 4))), Summary.Readout.of("word", 0)));
    }

    private static List<SequenceEvaluator.Past> randomPath(final Random random, final int n) {
        final List<SequenceEvaluator.Past> path = new ArrayList<>();
        long millis = T0;
        double x = 0, y = 0;
        for (int i = 0; i < n; i++) {
            millis += (long) (random.nextDouble() * 3 * 86_400_000L);
            x += random.nextGaussian();
            y += random.nextGaussian() * 0.5;
            path.add(past(millis, x, y));
        }
        return path;
    }

    @Test
    public void testChenIdentityAndInverse() {
        final Random random = new Random(9);
        for (final boolean byEvents : new boolean[]{true, false}) {
            final Signature family = new Signature(List.of("x", "y"), true, 4, byEvents, null);
            final List<SequenceEvaluator.Past> path = randomPath(random, 40);
            final Signature.State whole = fold(family, path);
            // Chen: the concatenation of two pieces (joined by the increment between them) is the product of their signatures
            final Signature.State first = fold(family, path.subList(0, 17)), second = fold(family, path.subList(17, 40));
            family.merge(first, family.create());
            family.merge(first, second);
            for (int i = 0; i < whole.tensor.length; i++) {
                Assertions.assertEquals(whole.tensor[i], first.tensor[i], 1e-9 * Math.max(1, Math.abs(whole.tensor[i])), byEvents + "@" + i);
            }
            for (int w = 0; w < family.words().size(); w++) {
                Assertions.assertEquals((Double) family.read(whole, Summary.Readout.of("word", w)), (Double) family.read(first, Summary.Readout.of("word", w)), 1e-8);
            }
            // the inverse is the signature of the reversed path, and multiplies to the identity
            final double[] inverse = family.inverse(whole.tensor);
            final double[] product = family.multiply(whole.tensor, inverse);
            final double[] identity = family.identity();
            for (int i = 0; i < product.length; i++) Assertions.assertEquals(identity[i], product[i], 1e-7 * Math.max(1, Math.abs(whole.tensor[i])), "S ⊗ S⁻¹ @" + i);
            if (byEvents) continue; // on the events clock the reversed path's time channel would run backwards by construction
            final List<SequenceEvaluator.Past> reversed = new ArrayList<>(path);
            java.util.Collections.reverse(reversed);
            // reversing time too: the time channel of the reversed path must run backwards as well, so compare without it
            final Signature plain = new Signature(List.of("x", "y"), false, 4, false, null);
            final double[] forward = fold(plain, path).tensor, backward = fold(plain, reversed).tensor, inv = plain.inverse(forward);
            for (int i = 0; i < inv.length; i++) Assertions.assertEquals(backward[i], inv[i], 1e-8 * Math.max(1, Math.abs(backward[i])), "reversed @" + i);
        }
    }

    @Test
    public void testLogOfOneIncrementIsTheIncrement() {
        final Signature family = new Signature(List.of("x", "y"), false, 3, true, null);
        final double[] log = family.log(family.exp(new double[]{0.7, -1.3}));
        Assertions.assertEquals(0.7, log[family.index(new int[]{0})], 1e-12);
        Assertions.assertEquals(-1.3, log[family.index(new int[]{1})], 1e-12);
        for (int i = family.index(new int[]{0, 0}); i < log.length; i++) Assertions.assertEquals(0, log[i], 1e-12, "level ≥ 2 @" + i);
    }
}
