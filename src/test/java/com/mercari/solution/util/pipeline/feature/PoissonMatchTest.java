package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * {@link PoissonMatch}: the result probabilities of two Poisson scores against grid sums of the joint law (numpy over a
 * 120 × 120 grid; 400 × 400 for the large rates), independent, Dixon–Coles and bivariate Poisson.
 */
public class PoissonMatchTest {

    private static void assertReadouts(final PoissonMatch m, final double expected0, final double expected1, final double win0, final double draw,
                                       final double win1, final double over0p5, final double over2p5, final double over3p5, final double bothScore) {
        Assertions.assertEquals(expected0, m.expected(0), 1e-12);
        Assertions.assertEquals(expected1, m.expected(1), 1e-12);
        Assertions.assertEquals(win0, m.win(0), 1e-12);
        Assertions.assertEquals(draw, m.draw(), 1e-12);
        Assertions.assertEquals(win1, m.win(1), 1e-12);
        Assertions.assertEquals(m.win(1), m.lose(0), 0d);
        Assertions.assertEquals(m.win(0), m.lose(1), 0d);
        Assertions.assertEquals(over0p5, m.over(0.5), 1e-12);
        Assertions.assertEquals(over2p5, m.over(2.5), 1e-12);
        Assertions.assertEquals(over3p5, m.over(3.5), 1e-12);
        Assertions.assertEquals(bothScore, m.bothScore(), 1e-12);
        Assertions.assertEquals(1d, m.win(0) + m.draw() + m.win(1), 1e-12, "a result is one of three");
    }

    @Test
    public void testHandValues() {
        // independent scores at 1.6 / 1.1
        final PoissonMatch plain = PoissonMatch.of(1.6, 1.1, 0d, 0d);
        assertReadouts(plain, 1.6, 1.1, 0.48957353712886176, 0.24891098064143483, 0.26151548222970367,
                0.93279448726025049, 0.50637550892653815, 0.28590782438378903, 0.53243791104701499);
        // Dixon-Coles rho -0.1: 0-0 and 1-1 gain, 0-1 and 1-0 lose the same mass; the marginals (and the expected scores) unchanged
        final PoissonMatch dc = PoissonMatch.of(1.6, 1.1, -0.1, 0d);
        assertReadouts(dc, 1.6, 1.1, 0.4777453668866658, 0.27256732112582677, 0.24968731198750768,
                0.92096631701805443, 0.50637550892653815, 0.28590782438378903, 0.54426608128921095);
        final double tilt = 1.6 * 1.1 * -0.1 * Math.exp(-2.7);
        Assertions.assertEquals(plain.draw() - 2 * tilt, dc.draw(), 1e-15);
        Assertions.assertEquals(plain.win(0) + tilt, dc.win(0), 1e-15);
        Assertions.assertEquals(plain.over(1.5) - tilt, dc.over(1.5), 1e-15, "totals 0 and 1 hold three of the four cells");
        Assertions.assertEquals(plain.over(2.5), dc.over(2.5), 0d, "from a total of 2 on the four cells cancel");
        // bivariate Poisson, a common part of mean 0.3: the difference of the scores is the independent one, the total and the expectations carry it
        final PoissonMatch bp = PoissonMatch.of(1.6, 1.1, 0d, 0.3);
        assertReadouts(bp, 1.9, 1.4, 0.4895735371288617, 0.24891098064143483, 0.26151548222970361,
                0.95021293163213616, 0.61937786232768033, 0.4157238591689329, 0.65362148520362262);
        Assertions.assertEquals(plain.win(0), bp.win(0), 0d);
        Assertions.assertEquals(plain.draw(), bp.draw(), 0d);
        Assertions.assertEquals(0.81578784703890372, bp.over(1.5), 1e-12);
        Assertions.assertEquals(0.24879590933009479, bp.over(4.5), 1e-12);
        // the prior of a poisson rating: both rates 1
        assertReadouts(PoissonMatch.of(1d, 1d, 0d, 0d), 1d, 1d, 0.3457458387231645, 0.30850832255367111, 0.34574583872316444,
                0.8646647167633873, 0.32332358381693654, 0.14287653950145296, 0.39957640089372815);
        assertReadouts(PoissonMatch.of(1d, 1d, -0.1, 0d), 1d, 1d, 0.33221231039950322, 0.33557537920099367, 0.33221231039950322,
                0.85113118843972602, 0.32332358381693654, 0.14287653950145296, 0.41310992921738943);
        // a rho that makes a tau non-positive at these rates (0.5 at 3 x 1: tau of 0-0 = -0.5) is left out: the independent model
        assertReadouts(PoissonMatch.of(3d, 1d, 0.5, 0d), 3d, 1d, 0.77501529120969725, 0.13112159537380774, 0.093863113416495253,
                0.98168436111126622, 0.76189669444645591, 0.56652987963329127, 0.60064912934942816);
        Assertions.assertEquals(PoissonMatch.of(3d, 1d, 0d, 0d).draw(), PoissonMatch.of(3d, 1d, 0.5, 0d).draw(), 0d);
        // large rates (points, not goals): the sums in logs keep every digit
        final PoissonMatch points = PoissonMatch.of(100d, 95d, 0d, 0d);
        Assertions.assertEquals(0.62636668627025138, points.win(0), 1e-9);
        Assertions.assertEquals(0.026816333808901291, points.draw(), 1e-9);
        Assertions.assertEquals(0.34681697992087451, points.win(1), 1e-9);
        Assertions.assertEquals(0.50952321823029156, points.over(194.5), 1e-9);
        Assertions.assertEquals(1d, points.bothScore(), 1e-12);
        Assertions.assertEquals(1d, points.win(0) + points.draw() + points.win(1), 1e-12);
    }

    @Test
    public void testProperties() {
        // the sides are symmetric: swapping the rates swaps win and lose, keeps the draw, the total and both-score
        final PoissonMatch a = PoissonMatch.of(2.3, 0.7, -0.08, 0d), b = PoissonMatch.of(0.7, 2.3, -0.08, 0d);
        Assertions.assertEquals(a.win(0), b.win(1), 1e-15);
        Assertions.assertEquals(a.lose(0), b.lose(1), 1e-15);
        Assertions.assertEquals(a.draw(), b.draw(), 1e-15);
        Assertions.assertEquals(a.over(2.5), b.over(2.5), 1e-15);
        Assertions.assertEquals(a.bothScore(), b.bothScore(), 1e-15);
        // equal rates: the two sides are one law bit for bit (the two sums walk different paths), whichever row is side 0
        for (final double rate : new double[]{1d, 0.37, 2.5, 140d}) {
            final PoissonMatch even = PoissonMatch.of(rate, rate, -0.1, 0d);
            Assertions.assertEquals(even.win(0), even.win(1), 0d, "rate " + rate);
            Assertions.assertEquals(even.lose(0), even.win(0), 0d);
            Assertions.assertEquals(1d, 2 * even.win(0) + even.draw(), 1e-12);
        }
        // the total's law: over is 1 below 0, falls with the line, steps only at integers
        Assertions.assertEquals(1d, a.over(-1), 0d);
        Assertions.assertEquals(a.over(2.5), a.over(2.0), 0d);
        Assertions.assertEquals(a.over(2.5), a.over(2.99), 0d);
        Assertions.assertTrue(a.over(0.5) > a.over(1.5) && a.over(1.5) > a.over(2.5) && a.over(2.5) > a.over(5.5));
        // over(0.5) = 1 - P(0-0): independent e^(-λ0-λ1), under rho the 0-0 cell has lost the tilt (negative here: a likelier 0-0)
        final PoissonMatch plain = PoissonMatch.of(2.3, 0.7, 0d, 0d);
        Assertions.assertEquals(1 - Math.exp(-3.0), plain.over(0.5), 1e-12);
        Assertions.assertEquals(plain.over(0.5) + 2.3 * 0.7 * -0.08 * Math.exp(-3.0), a.over(0.5), 1e-15);
        Assertions.assertTrue(a.over(0.5) < plain.over(0.5));
        // bivariate Poisson: expected = own rate + common part, over(0.5) = 1 - P(no goal at all) = 1 - e^(-λ0-λ1-c)
        final PoissonMatch bp = PoissonMatch.of(1.2, 0.9, 0d, 0.4);
        Assertions.assertEquals(1.6, bp.expected(0), 1e-15);
        Assertions.assertEquals(1 - Math.exp(-2.5), bp.over(0.5), 1e-12);
        Assertions.assertEquals(1 - Math.exp(-1.6) - Math.exp(-1.3) + Math.exp(-2.5), bp.bothScore(), 1e-12);
        // a common part of 0 is the independent model bit for bit
        final PoissonMatch zero = PoissonMatch.of(1.2, 0.9, 0d, 0d);
        Assertions.assertEquals(zero.over(2.5), PoissonMatch.of(1.2, 0.9, 0d, 0d).over(2.5), 0d);
        Assertions.assertEquals(1 - Math.exp(-2.1), zero.over(0.5), 1e-12);
        // no model: a rate that is not positive, infinite, past the bound; a rho outside (-1, 1); a negative covariance
        Assertions.assertNull(PoissonMatch.of(0d, 1d, 0d, 0d));
        Assertions.assertNull(PoissonMatch.of(1d, Double.NaN, 0d, 0d));
        Assertions.assertNull(PoissonMatch.of(1d, Double.POSITIVE_INFINITY, 0d, 0d));
        Assertions.assertNull(PoissonMatch.of(PoissonMatch.MAX_RATE * 1.01, 1d, 0d, 0d));
        Assertions.assertNotNull(PoissonMatch.of(PoissonMatch.MAX_RATE, 1d, 0d, 0d));
        Assertions.assertNull(PoissonMatch.of(1d, 1d, 1d, 0d));
        Assertions.assertNull(PoissonMatch.of(1d, 1d, 0d, -0.1));
        // at the bound the three results still sum to one
        final PoissonMatch bound = PoissonMatch.of(PoissonMatch.MAX_RATE, PoissonMatch.MAX_RATE * 0.98, 0d, 0d);
        Assertions.assertEquals(1d, bound.win(0) + bound.draw() + bound.win(1), 1e-9);
        Assertions.assertTrue(bound.win(0) > 0.9, "200 points ahead at sd ~ 141");
    }
}
