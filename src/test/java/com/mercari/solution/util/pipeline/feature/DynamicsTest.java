package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The {@code lti} dynamics: the running state (propagated, evicted, merged) against the direct projection of the
 * window it summarises, and {@code ewma} against the formula it had before it became the order-0 exponential measure.
 */
public class DynamicsTest {

    private static final long T0 = 1_700_000_000_000L;
    private static final double DAY = 86_400_000d;

    private record Case(String name, Dynamics family, int dimension) {}

    private static List<Case> cases() {
        final List<Case> cases = new ArrayList<>();
        for (final boolean byTime : new boolean[]{false, true}) {
            final String clock = byTime ? "time" : "events";
            cases.add(new Case("exponential0/" + clock, new Dynamics(Dynamics.Measure.exponential, 0, 3d, null, byTime), 1));
            cases.add(new Case("exponential6/" + clock, new Dynamics(Dynamics.Measure.exponential, 6, 2.5, null, byTime), 7));
            cases.add(new Case("fourier3/" + clock, new Dynamics(Dynamics.Measure.fourier, 3, null, 7d, byTime), 7));
            cases.add(new Case("fourier2damped/" + clock, new Dynamics(Dynamics.Measure.fourier, 2, 5d, 11d, byTime), 5));
            cases.add(new Case("legendre8/" + clock, new Dynamics(Dynamics.Measure.legendre, 8, null, null, byTime), 9));
        }
        return cases;
    }

    /** A random path: irregular spacing (minutes to days), repeated timestamps, missing values. */
    private static List<SequenceEvaluator.Past> path(final Random random, final int size) {
        final List<SequenceEvaluator.Past> path = new ArrayList<>();
        long millis = T0;
        for (int i = 0; i < size; i++) {
            if (random.nextDouble() > 0.1) millis += (long) Math.pow(10, 4 + random.nextDouble() * 4.5);
            final Map<String, Object> values = new HashMap<>();
            values.put("x", random.nextInt(9) == 0 ? null : 50 + random.nextGaussian() * 20);
            path.add(new SequenceEvaluator.Past(millis, values));
        }
        return path;
    }

    /** The event of a row, or null for a row without a value (which the evaluator never folds). */
    private static Dynamics.Event event(final Dynamics family, final SequenceEvaluator.Past p) {
        return family.event(p.millis(), SequenceEvaluator.finite(p.values().get("x")));
    }

    private static void fold(final Dynamics family, final Dynamics.State state, final SequenceEvaluator.Past p, final int sign) {
        final Dynamics.Event e = event(family, p);
        if (e != null) family.update(state, e, sign);
    }

    private static void assertClose(final String at, final Object expected, final Object actual, final double tolerance) {
        if (expected == null || actual == null) {
            Assertions.assertEquals(expected, actual, at);
            return;
        }
        final double e = ((Number) expected).doubleValue(), a = ((Number) actual).doubleValue();
        Assertions.assertEquals(e, a, Math.max(tolerance, Math.abs(e) * tolerance), at);
    }

    /**
     * Folding events one by one (propagation between them) and reading at a later time equals the direct projection
     * of the same events — for every measure, both clocks, every component.
     */
    @Test
    public void testRunningStateMatchesDirectProjection() {
        final Random random = new Random(5);
        for (final Case c : cases()) {
            final List<SequenceEvaluator.Past> path = path(random, 120);
            final Dynamics.State state = c.family().create();
            for (int i = 0; i < path.size(); i++) {
                fold(c.family(), state, path.get(i), 1);
                final long now = path.get(i).millis() + 3_600_000L * (1 + random.nextInt(48));
                final List<SequenceEvaluator.Past> window = path.subList(0, i + 1);
                for (int j = 0; j < c.dimension(); j++) {
                    assertClose(c.name() + "@" + i + "/" + j, c.family().project(window, "x", now, j),
                            c.family().readAt(state, Summary.Readout.of("component", j), now), 1e-9);
                }
            }
        }
    }

    /**
     * A sliding window evicts its oldest event per step for many steps: the running state keeps matching the direct
     * projection of the window without a periodic re-fold (the propagators are contractive or orthogonal), and an
     * emptied window reads null.
     */
    @Test
    public void testLongEvictionRunNeedsNoRefold() {
        final Random random = new Random(17);
        for (final Case c : cases()) {
            if (!c.family().invertible()) continue;
            final List<SequenceEvaluator.Past> path = path(random, 20_000);
            final Dynamics.State state = c.family().create();
            final long maxAge = 5 * 86_400_000L;
            int evict = 0;
            for (int i = 0; i < path.size(); i++) {
                final long now = path.get(i).millis() + 60_000L;
                fold(c.family(), state, path.get(i), 1);
                while (path.get(evict).millis() < now - maxAge) fold(c.family(), state, path.get(evict++), -1);
                if (i % 997 != 0 && i != path.size() - 1) continue;
                final List<SequenceEvaluator.Past> window = path.subList(evict, i + 1);
                for (int j = 0; j < c.dimension(); j++) {
                    assertClose(c.name() + "@" + i + "/" + j, c.family().project(window, "x", now, j),
                            c.family().readAt(state, Summary.Readout.of("component", j), now), 1e-8);
                }
            }
            // evicting everything empties the state exactly
            while (evict < path.size()) fold(c.family(), state, path.get(evict++), -1);
            Assertions.assertNull(c.family().readAt(state, Summary.Readout.of("component", 0), T0), c.name());
            Assertions.assertEquals(0, c.family().count(state), c.name());
        }
    }

    /** Merging the states of consecutive segments equals folding the whole path (the fit-stage use of a monoid). */
    @Test
    public void testMergeOfSegmentsEqualsWholePath() {
        final Random random = new Random(23);
        for (final Case c : cases()) {
            final List<SequenceEvaluator.Past> path = path(random, 90);
            final Dynamics.State whole = c.family().create(), first = c.family().create(), second = c.family().create(), third = c.family().create();
            for (int i = 0; i < path.size(); i++) {
                fold(c.family(), whole, path.get(i), 1);
                fold(c.family(), i < 30 ? first : i < 55 ? second : third, path.get(i), 1);
            }
            // associativity: (first ⊕ second) ⊕ third, and the identity on either side
            c.family().merge(first, c.family().create());
            c.family().merge(first, second);
            c.family().merge(first, third);
            final long now = path.get(path.size() - 1).millis() + 86_400_000L;
            for (int j = 0; j < c.dimension(); j++) {
                assertClose(c.name() + "/" + j, c.family().readAt(whole, Summary.Readout.of("component", j), now),
                        c.family().readAt(first, Summary.Readout.of("component", j), now), 1e-9);
            }
            Assertions.assertEquals(c.family().count(whole), c.family().count(first), c.name());
        }
    }

    /** {@code ewma} is the order-0 exponential measure: the new state equals the formula the op was evaluated with before. */
    @Test
    public void testEwmaMatchesTheFormerFormula() {
        final Random random = new Random(31);
        for (final boolean byTime : new boolean[]{false, true}) {
            for (final double halflife : new double[]{0.5, 2, 10}) {
                final Dynamics family = new Dynamics(Dynamics.Measure.exponential, 0, halflife, null, byTime);
                final List<SequenceEvaluator.Past> path = path(random, 300);
                final Dynamics.State state = family.create();
                for (int i = 0; i < path.size(); i++) {
                    fold(family, state, path.get(i), 1);
                    final long now = path.get(i).millis() + 3_600_000L;
                    final Double former = formerEwma(path.subList(0, i + 1), halflife, byTime, now);
                    assertClose("h" + halflife + "/" + byTime + "@" + i, former, family.readAt(state, Summary.Readout.of("component", 0), now), 1e-12);
                    assertClose("scan h" + halflife + "/" + byTime + "@" + i, former, family.project(path.subList(0, i + 1), "x", now, 0), 1e-12);
                }
            }
        }
    }

    /**
     * The scan formula of {@code ewma} before PR-U4a: weights 0.5^(steps / halflife) from the current row — the events
     * clock counting the valued rows after each one (a row without a value is no event of the channel).
     */
    private static Double formerEwma(final List<SequenceEvaluator.Past> window, final double halflife, final boolean byTime, final long now) {
        double num = 0, den = 0;
        int valued = 0;
        for (final SequenceEvaluator.Past p : window) if (FeatureValues.toDouble(p.values().get("x")) != null) valued++;
        int seen = 0;
        for (int i = 0; i < window.size(); i++) {
            final Double v = FeatureValues.toDouble(window.get(i).values().get("x"));
            if (v == null) continue;
            seen++;
            final double steps = byTime ? (now - window.get(i).millis()) / DAY : (valued - seen);
            final double w = Math.pow(0.5, steps / halflife);
            num += w * v;
            den += w;
        }
        return den == 0 ? null : num / den;
    }

    /**
     * Known values: the Laguerre kernel of an event u older than the newest is e^(−u) L_j(u) with L_1 = 1 − u,
     * L_2 = 1 − 2u + u²/2 and the newest event's is 1 (the readout divides by the mass 1 + e^(−u)), read at the newest
     * event also on the time clock — the gap since it does not move the readout; the Legendre components of a linear path x = a + b·u
     * over its own span are a + b/2 (j = 0) and the discrete Legendre moment b · mean(u·P_1(2u − 1)) + a · mean(P_1)
     * (j = 1); a fourier harmonic reads cos / sin of the event's phase.
     */
    @Test
    public void testKnownValues() {
        final Dynamics laguerre = new Dynamics(Dynamics.Measure.exponential, 2, 1d, null, true);
        final Dynamics.State two = laguerre.create();
        laguerre.update(two, laguerre.event(T0, 4d), 1);
        laguerre.update(two, laguerre.event(T0 + 3 * 86_400_000L, 0d), 1);
        final double u = Math.log(2) * 3; // three days at a one-day halflife
        final double w = Math.exp(-u);
        for (final long now : new long[]{T0 + 3 * 86_400_000L, T0 + 180 * 86_400_000L}) {
            assertClose("L0", 4 * w / (1 + w), laguerre.readAt(two, Summary.Readout.of("component", 0), now), 1e-12);
            assertClose("L1", 4 * w * (1 - u) / (1 + w), laguerre.readAt(two, Summary.Readout.of("component", 1), now), 1e-12);
            assertClose("L2", 4 * w * (1 - 2 * u + u * u / 2) / (1 + w), laguerre.readAt(two, Summary.Readout.of("component", 2), now), 1e-12);
        }
        // a stale entity: every component of a single event stays its value (L_j(0) = 1), not ~(θ·gap)^j / j!
        final Dynamics high = new Dynamics(Dynamics.Measure.exponential, 16, 7d, null, true);
        final Dynamics.State stale = high.create();
        high.update(stale, high.event(T0, 4d), 1);
        final long later = T0 + 180 * 86_400_000L;
        final List<SequenceEvaluator.Past> single = List.of(new SequenceEvaluator.Past(T0, Map.of("x", 4d)));
        for (int j = 0; j <= 16; j++) {
            assertClose("stale L" + j, 4d, high.readAt(stale, Summary.Readout.of("component", j), later), 1e-12);
            assertClose("stale scan L" + j, 4d, high.project(single, "x", later, j), 1e-12);
        }

        // events clock: x_i = 10 + 2·i at ordinals 0..4, u_i = i / 4
        final Dynamics legendre = new Dynamics(Dynamics.Measure.legendre, 1, null, null, false);
        final Dynamics.State line = legendre.create();
        double p1 = 0;
        for (int i = 0; i < 5; i++) {
            legendre.update(line, legendre.event(T0 + i, 10d + 2 * i), 1);
            p1 += (10d + 2 * i) * (2 * (i / 4d) - 1);
        }
        assertClose("P0", 14d, legendre.readAt(line, Summary.Readout.of("component", 0), T0 + 10), 1e-12);
        assertClose("P1", p1 / 5, legendre.readAt(line, Summary.Readout.of("component", 1), T0 + 10), 1e-12);

        final Dynamics fourier = new Dynamics(Dynamics.Measure.fourier, 1, null, 4d, false);
        final Dynamics.State wave = fourier.create();
        fourier.update(wave, fourier.event(T0, 2d), 1);
        Assertions.assertNull(fourier.event(T0 + 1, null), "a missing value is no event of the channel");
        fourier.update(wave, fourier.event(T0 + 2, 0d), 1);
        // the value is one event old: phase 2π / 4
        assertClose("c0", 1d, fourier.readAt(wave, Summary.Readout.of("component", 0), T0 + 2), 1e-12);
        assertClose("c1", 2 * Math.cos(Math.PI / 2) / 2, fourier.readAt(wave, Summary.Readout.of("component", 1), T0 + 2), 1e-12);
        assertClose("s1", 2 * Math.sin(Math.PI / 2) / 2, fourier.readAt(wave, Summary.Readout.of("component", 2), T0 + 2), 1e-12);
        Assertions.assertEquals("s1", Dynamics.componentName(Dynamics.Measure.fourier, 2));
        Assertions.assertEquals("c1", Dynamics.componentName(Dynamics.Measure.fourier, 1));
    }

    /**
     * A row without a value is no event of the channel: on the time clock it does not move the read position — the
     * exponential components of a channel whose newest rows are missing stay those read at its newest value, instead
     * of growing like (θ·gap)^j / j! over the gap between that value and the missing row (the divergence the read
     * position at the newest event was meant to rule out) — and on the events clock it is not counted: folding
     * [x, missing, y] equals folding [x, y]. The scan path agrees on both.
     */
    @Test
    public void testMissingValueIsNoEventOfTheChannel() {
        final long day = 86_400_000L;
        final Dynamics high = new Dynamics(Dynamics.Measure.exponential, 8, 7d, null, true);
        final Dynamics.State state = high.create();
        final List<SequenceEvaluator.Past> path = new ArrayList<>();
        final double[] values = {30, 45, 80, 35};
        for (int i = 0; i < values.length; i++) {
            path.add(new SequenceEvaluator.Past(T0 + i * 3 * day, Map.of("x", values[i])));
            fold(high, state, path.get(path.size() - 1), 1);
        }
        final double[] before = new double[9];
        for (int j = 0; j <= 8; j++) before[j] = (Double) high.readAt(state, Summary.Readout.of("component", j), T0 + 12 * day);
        // two missing rows, the last one 200 days after the newest value
        final Map<String, Object> missing = new HashMap<>();
        missing.put("x", null);
        path.add(new SequenceEvaluator.Past(T0 + 100 * day, missing));
        path.add(new SequenceEvaluator.Past(T0 + 209 * day, missing));
        fold(high, state, path.get(4), 1);
        fold(high, state, path.get(5), 1);
        final long now = T0 + 210 * day;
        for (int j = 0; j <= 8; j++) {
            final Object read = high.readAt(state, Summary.Readout.of("component", j), now);
            assertClose("L" + j + " unchanged by missing rows", before[j], read, 1e-12);
            assertClose("scan L" + j, before[j], high.project(path, "x", now, j), 1e-9);
            Assertions.assertTrue(Math.abs(((Double) read)) <= 80 * 1.5, "bounded by the values' scale: " + read);
        }
        Assertions.assertEquals(4, high.count(state), 0d);

        // the events clock counts the channel's valued events only
        for (final Dynamics family : List.of(new Dynamics(Dynamics.Measure.exponential, 3, 2d, null, false),
                new Dynamics(Dynamics.Measure.fourier, 2, null, 5d, false), new Dynamics(Dynamics.Measure.legendre, 3, null, null, false))) {
            final Dynamics.State withGap = family.create(), without = family.create();
            fold(family, withGap, path.get(0), 1);
            fold(family, without, path.get(0), 1);
            fold(family, withGap, path.get(4), 1);           // missing
            fold(family, withGap, path.get(1), 1);
            fold(family, without, path.get(1), 1);
            fold(family, withGap, path.get(2), 1);
            fold(family, without, path.get(2), 1);
            final List<SequenceEvaluator.Past> gapped = List.of(path.get(0), path.get(4), path.get(1), path.get(2));
            for (int j = 0; j < 3; j++) {
                final Object expected = family.readAt(without, Summary.Readout.of("component", j), now);
                assertClose("events " + j, expected, family.readAt(withGap, Summary.Readout.of("component", j), now), 1e-12);
                assertClose("events scan " + j, expected, family.project(gapped, "x", now, j), 1e-12);
            }
        }
    }

    /** The two Legendre evaluations the paths use agree: the monomial coefficients (running state) and Bonnet's recurrence (scan). */
    @Test
    public void testShiftedLegendreCoefficients() {
        final double[][] c = Dynamics.shiftedLegendre(8);
        for (int j = 0; j <= 8; j++) {
            for (double u = 0; u <= 1.0001; u += 0.05) {
                double monomial = 0, power = 1;
                for (int k = 0; k <= j; k++) {
                    monomial += c[j][k] * power;
                    power *= u;
                }
                Assertions.assertEquals(Dynamics.legendre(2 * u - 1, j), monomial, 1e-9, "P" + j + "(" + u + ")");
            }
        }
    }
}
