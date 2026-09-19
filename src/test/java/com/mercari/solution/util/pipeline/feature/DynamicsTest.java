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

    private static Dynamics.Event event(final Dynamics family, final SequenceEvaluator.Past p) {
        return family.event(p.millis(), SequenceEvaluator.finite(p.values().get("x")));
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
                c.family().update(state, event(c.family(), path.get(i)), 1);
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
                c.family().update(state, event(c.family(), path.get(i)), 1);
                while (path.get(evict).millis() < now - maxAge) c.family().update(state, event(c.family(), path.get(evict++)), -1);
                if (i % 997 != 0 && i != path.size() - 1) continue;
                final List<SequenceEvaluator.Past> window = path.subList(evict, i + 1);
                for (int j = 0; j < c.dimension(); j++) {
                    assertClose(c.name() + "@" + i + "/" + j, c.family().project(window, "x", now, j),
                            c.family().readAt(state, Summary.Readout.of("component", j), now), 1e-8);
                }
            }
            // evicting everything empties the state exactly
            while (evict < path.size()) c.family().update(state, event(c.family(), path.get(evict++)), -1);
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
                final Dynamics.Event e = event(c.family(), path.get(i));
                c.family().update(whole, e, 1);
                c.family().update(i < 30 ? first : i < 55 ? second : third, e, 1);
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
                    family.update(state, event(family, path.get(i)), 1);
                    final long now = path.get(i).millis() + 3_600_000L;
                    final Double former = formerEwma(path.subList(0, i + 1), halflife, byTime, now);
                    assertClose("h" + halflife + "/" + byTime + "@" + i, former, family.readAt(state, Summary.Readout.of("component", 0), now), 1e-12);
                    assertClose("scan h" + halflife + "/" + byTime + "@" + i, former, family.project(path.subList(0, i + 1), "x", now, 0), 1e-12);
                }
            }
        }
    }

    /** The scan formula of {@code ewma} before PR-U4a: weights 0.5^(steps / halflife) from the current row. */
    private static Double formerEwma(final List<SequenceEvaluator.Past> window, final double halflife, final boolean byTime, final long now) {
        double num = 0, den = 0;
        for (int i = 0; i < window.size(); i++) {
            final Double v = FeatureValues.toDouble(window.get(i).values().get("x"));
            if (v == null) continue;
            final double steps = byTime ? (now - window.get(i).millis()) / DAY : (window.size() - 1 - i);
            final double w = Math.pow(0.5, steps / halflife);
            num += w * v;
            den += w;
        }
        return den == 0 ? null : num / den;
    }

    /**
     * Known values: the Laguerre kernel of one event is e^(−u) L_j(u) with L_1 = 1 − u, L_2 = 1 − 2u + u²/2 (the
     * readout divides by the mass e^(−u), leaving L_j(u) · x); the Legendre components of a linear path x = a + b·u
     * over its own span are a + b/2 (j = 0) and the discrete Legendre moment b · mean(u·P_1(2u − 1)) + a · mean(P_1)
     * (j = 1); a fourier harmonic reads cos / sin of the event's phase.
     */
    @Test
    public void testKnownValues() {
        final Dynamics laguerre = new Dynamics(Dynamics.Measure.exponential, 2, 1d, null, true);
        final Dynamics.State one = laguerre.create();
        laguerre.update(one, laguerre.event(T0, 4d), 1);
        final double u = Math.log(2) * 3; // three days at a one-day halflife
        final long now = T0 + 3 * 86_400_000L;
        assertClose("L0", 4d, laguerre.readAt(one, Summary.Readout.of("component", 0), now), 1e-12);
        assertClose("L1", 4 * (1 - u), laguerre.readAt(one, Summary.Readout.of("component", 1), now), 1e-12);
        assertClose("L2", 4 * (1 - 2 * u + u * u / 2), laguerre.readAt(one, Summary.Readout.of("component", 2), now), 1e-12);

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
        fourier.update(wave, fourier.event(T0 + 1, null), 1); // a missing value still advances the events clock
        // the value is one event old: phase 2π / 4
        assertClose("c0", 2d, fourier.readAt(wave, Summary.Readout.of("component", 0), T0 + 1), 1e-12);
        assertClose("c1", 2 * Math.cos(Math.PI / 2), fourier.readAt(wave, Summary.Readout.of("component", 1), T0 + 1), 1e-12);
        assertClose("s1", 2 * Math.sin(Math.PI / 2), fourier.readAt(wave, Summary.Readout.of("component", 2), T0 + 1), 1e-12);
        Assertions.assertEquals("s1", Dynamics.componentName(Dynamics.Measure.fourier, 2));
        Assertions.assertEquals("c1", Dynamics.componentName(Dynamics.Measure.fourier, 1));
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
