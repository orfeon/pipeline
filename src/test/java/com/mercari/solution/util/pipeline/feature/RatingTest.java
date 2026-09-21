package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The sequence {@code rating} op: the update rules against hand values, the order independence of a contest, the
 * compile contract (pooled stage key, window shift, diagnostics) and the equality of the running state with a
 * from-scratch replay — over the untrimmed and the trimmed history.
 */
public class RatingTest {

    private static final String SOURCES = """
            sources:
              - name: listings
                eventTime: session_time
                keys: [session_id, seller_id]
                fields:
                  - {name: session_id, type: string}
                  - {name: seller_id, type: string}
                  - {name: category, type: string}
                  - {name: start_price, type: float64}
                  - {name: final_price, type: float64, availableAt: after(event), kind: outcome}
                settlementLag: PT30M
                ingestionLag: P2D
            """;

    private static final String SPEC = """
            lineage:
              - {fields: [session_id, seller_id, category, start_price, final_price], from: listings}
            time: {field: session_time, orderTieBreak: [session_id]}
            predictAt: "event_time - PT10M"
            entities:
              - {name: seller, keys: [seller_id], minInterval: P30D}
            contexts:
              - {name: session, keys: [session_id]}
            features:
              - name: skill
                scope: sequence
                entity: seller
                ops:
                  - {type: rating, field: final_price, context: session, order: descending, funcs: [mu, sigma, count, delta]}
                  - {type: rating, field: final_price, context: session, order: descending, method: elo, as: elo, funcs: [mu, count, delta]}
                  - {type: rating, field: final_price, context: session, order: descending, method: bradleyTerry, as: bt, tau: 0.5}
              - name: past
                scope: sequence
                entity: seller
                ops:
                  - {type: aggregate, field: start_price, funcs: [mean]}
            """;

    private static FeaturePlan compile(final String spec) {
        final JsonObject sources = Config.convertConfigJson(SOURCES, Config.Format.yaml);
        return FeaturePlanCompiler.compile(sources, Config.convertConfigJson(spec, Config.Format.yaml), null);
    }

    private static boolean hasCode(final FeaturePlan plan, final String code) {
        return plan.getDiagnostics().getMessages().stream().anyMatch(m -> m.code().equals(code));
    }

    private static Rating rating(final Rating.Method method, final boolean ascending, final Double tau) {
        return Rating.of(method, ascending, null, null, null, tau, null, null, List.of("p"), List.of("c"), "y");
    }

    private static Rating.Entry entry(final String player, final double outcome) {
        return new Rating.Entry(player, outcome);
    }

    @Test
    public void testEloHandValues() {
        final Rating elo = rating(Rating.Method.elo, true, null);
        final Rating.State state = new Rating.State();
        // three equal players ranked 1, 2, 3: (0.5 + 0.5) / 2 opponents * 32 = +16, 0, -16
        elo.update(state, List.of(entry("a", 1), entry("b", 2), entry("c", 3)));
        Assertions.assertEquals(1516d, (Double) elo.read(state, "a", "mu"), 1e-12);
        Assertions.assertEquals(1500d, (Double) elo.read(state, "b", "mu"), 1e-12);
        Assertions.assertEquals(1484d, (Double) elo.read(state, "c", "mu"), 1e-12);
        Assertions.assertEquals(16d, (Double) elo.read(state, "a", "delta"), 1e-12);
        Assertions.assertEquals(1L, elo.read(state, "a", "count"));
        // the upset: c (1484) beats a (1516); a's expected score is 1 / (1 + 10^(-32 / 400))
        elo.update(state, List.of(entry("a", 2), entry("c", 1)));
        final double expected = 1d / (1d + Math.pow(10d, (1484d - 1516d) / 400d));
        Assertions.assertEquals(1516d - 32d * expected, (Double) elo.read(state, "a", "mu"), 1e-9);
        Assertions.assertEquals(1484d + 32d * expected, (Double) elo.read(state, "c", "mu"), 1e-9);
        Assertions.assertEquals(2L, elo.read(state, "a", "count"));
        Assertions.assertEquals(1L, elo.read(state, "b", "count"));
        // a player never rated reads the prior, and no change
        Assertions.assertEquals(1500d, elo.read(state, "z", "mu"));
        Assertions.assertEquals(0L, elo.read(state, "z", "count"));
        Assertions.assertNull(elo.read(state, "z", "delta"));
        Assertions.assertNull(elo.read(state, null, "mu"));
        Assertions.assertEquals(1500d, elo.read(null, "a", "mu"));
    }

    /** Two equal players under the Weng-Lin updates without drift: the published two-player values (both rules agree). */
    @Test
    public void testWengLinTwoPlayers() {
        for (final Rating.Method method : List.of(Rating.Method.bradleyTerry, Rating.Method.plackettLuce)) {
            final Rating rating = rating(method, true, 0d);
            final Rating.State state = new Rating.State();
            rating.update(state, List.of(entry("winner", 1), entry("loser", 2)));
            Assertions.assertEquals(27.63523138347365, (Double) rating.read(state, "winner", "mu"), 1e-9, method.name());
            Assertions.assertEquals(22.36476861652635, (Double) rating.read(state, "loser", "mu"), 1e-9, method.name());
            Assertions.assertEquals(8.065506316323548, (Double) rating.read(state, "winner", "sigma"), 1e-9, method.name());
            Assertions.assertEquals(8.065506316323548, (Double) rating.read(state, "loser", "sigma"), 1e-9, method.name());
        }
        // the drift widens a participant's uncertainty before the contest: the same result moves the rating further
        final Rating drifting = rating(Rating.Method.plackettLuce, true, 2d);
        final Rating.State state = new Rating.State();
        drifting.update(state, List.of(entry("winner", 1), entry("loser", 2)));
        Assertions.assertTrue((Double) drifting.read(state, "winner", "mu") > 27.63523138347365);
    }

    /**
     * What the field size does to the two Bayesian updates under the default parameters — the table of the module
     * documentation ("Field size decides what the uncertainty is worth"), pinned here so the two cannot drift apart.
     * One contest of k fresh players without ties: bradleyTerry adds up each player's k − 1 pairs (the winner's move
     * and the collapse of sigma grow with the field), plackettLuce's normaliser grows with the field (sigma barely
     * shrinks; the winner's is the largest of the contest, the last player's the smallest).
     *
     * <p>The documented moves are rounded to one decimal, so the tolerance is a little wider than half that digit
     * (two of them land within 0.005 of the rounding boundary).
     */
    @Test
    public void testFieldSizeUnderTheDefaults() {
        // k → {bradleyTerry winner move, sigma, plackettLuce winner move, winner's sigma, last player's sigma}
        final Map<Integer, double[]> documented = Map.of(
                2, new double[]{2.6, 8.07, 2.6, 8.07, 8.07},
                4, new double[]{7.9, 7.50, 2.8, 8.26, 8.08},
                8, new double[]{18.4, 6.22, 2.3, 8.32, 8.18},
                16, new double[]{39.5, 1.89, 1.7, 8.33, 8.25});
        for (final Map.Entry<Integer, double[]> e : documented.entrySet()) {
            final int k = e.getKey();
            final List<Rating.Entry> contest = freshContest(k);
            final Rating bt = rating(Rating.Method.bradleyTerry, true, null), pl = rating(Rating.Method.plackettLuce, true, null);
            final Rating.State btState = new Rating.State(), plState = new Rating.State();
            bt.update(btState, contest);
            pl.update(plState, contest);
            final double[] d = e.getValue();
            Assertions.assertEquals(d[0], (Double) bt.read(btState, "p1", "delta"), 0.06, "bradleyTerry winner, k = " + k);
            for (int i = 1; i <= k; i++) Assertions.assertEquals(d[1], (Double) bt.read(btState, "p" + i, "sigma"), 0.006, "bradleyTerry sigma, k = " + k);
            Assertions.assertEquals(d[2], (Double) pl.read(plState, "p1", "delta"), 0.06, "plackettLuce winner, k = " + k);
            Assertions.assertEquals(d[3], (Double) pl.read(plState, "p1", "sigma"), 0.006, "plackettLuce winner's sigma, k = " + k);
            Assertions.assertEquals(d[4], (Double) pl.read(plState, "p" + k, "sigma"), 0.006, "plackettLuce last player's sigma, k = " + k);
        }
        // a larger beta softens bradleyTerry's collapse at k = 16 (sigma 7.8) and halves the move (+19.8) without
        // curing it: the move is still 2.4 prior standard deviations
        final List<Rating.Entry> field = freshContest(16);
        final double sigma = Rating.defaultSigma(Rating.defaultMu(Rating.Method.bradleyTerry));
        final Rating wide = Rating.of(Rating.Method.bradleyTerry, true, null, null, 2 * sigma, null, null, null, List.of("p"), List.of("c"), "y");
        final Rating.State wideState = new Rating.State();
        wide.update(wideState, field);
        Assertions.assertEquals(7.8, (Double) wide.read(wideState, "p1", "sigma"), 0.05);
        Assertions.assertEquals(19.8, (Double) wide.read(wideState, "p1", "delta"), 0.06, "the move is halved, not cured");
        Assertions.assertTrue((Double) wide.read(wideState, "p1", "delta") > 2 * sigma, "still more than two prior standard deviations");
        // and no beta makes plackettLuce's sigma shrink there: the shrink is largest as beta → 0 (the normaliser is
        // dominated by k · sigma²), and even there the last player keeps 8.22 of the 8.33 it started with
        final Rating narrow = Rating.of(Rating.Method.plackettLuce, true, null, null, sigma / 100, null, null, null, List.of("p"), List.of("c"), "y");
        final Rating.State narrowState = new Rating.State();
        narrow.update(narrowState, field);
        Assertions.assertEquals(8.22, (Double) narrow.read(narrowState, "p16", "sigma"), 0.01, "within 1.5% of the prior after a contest, whatever beta");
    }

    /** One contest of k fresh players, p1 first (ascending outcomes: the smaller the better). */
    private static List<Rating.Entry> freshContest(final int k) {
        final List<Rating.Entry> contest = new ArrayList<>();
        for (int i = 1; i <= k; i++) contest.add(entry("p" + i, i));
        return contest;
    }

    private static final long DAY = Duration.ofDays(1).toMillis();

    private static Rating timed(final Rating.Method method, final double tau, final long tauPerMillis) {
        return Rating.of(method, true, null, null, null, tau, null, null, tauPerMillis, null, List.of("p"), List.of("c"), "y");
    }

    /**
     * {@code tauPer}: the drift runs on the time since the player's last contest, not per contest. A first contest
     * adds nothing (the prior is the whole uncertainty), an absence reopens the variance by tau² · Δt / tauPer — for
     * the next contest and for a read alike — and two contests of one time drift nothing between them.
     */
    @Test
    public void testDriftInTime() {
        for (final Rating.Method method : List.of(Rating.Method.bradleyTerry, Rating.Method.plackettLuce)) {
            final Rating rating = timed(method, 2d, 30 * DAY);
            final Rating.State state = new Rating.State();
            final long t0 = 1_700_000_000_000L;
            rating.update(state, List.of(entry("a", 1), entry("b", 2)), t0);
            // the first contest of both: the driftless two-player values
            Assertions.assertEquals(27.63523138347365, (Double) rating.read(state, "a", "mu", t0), 1e-9, method.name());
            final double sigma0 = 8.065506316323548;
            Assertions.assertEquals(sigma0, (Double) rating.read(state, "a", "sigma", t0), 1e-9, method.name());
            // read 300 days later: ten periods of drift on the variance, the rating itself untouched
            final long later = t0 + 300 * DAY;
            Assertions.assertEquals(Math.sqrt(sigma0 * sigma0 + 4d * 10), (Double) rating.read(state, "a", "sigma", later), 1e-9, method.name());
            Assertions.assertEquals(27.63523138347365, (Double) rating.read(state, "a", "mu", later), 1e-9, method.name());
            Assertions.assertEquals(sigma0, (Double) rating.read(state, "a", "sigma"), 1e-9, "a read without a time is the state itself");
            // a player never rated reads the prior whenever it is read
            Assertions.assertEquals(25d / 3, (Double) rating.read(state, "z", "sigma", later), 0d, method.name());

            // the contest held then enters with that variance: the update of a per-contest rating whose tau² is the
            // same 40 on the same ratings
            final Rating reference = rating(method, true, Math.sqrt(40d));
            final Rating.State copy = new Rating.State();
            for (final String name : List.of("a", "b")) {
                final Rating.Player from = state.players.get(name), to = new Rating.Player();
                to.mu = from.mu;
                to.sigma = from.sigma;
                copy.players.put(name, to);
            }
            reference.update(copy, List.of(entry("a", 2), entry("b", 1)));
            rating.update(state, List.of(entry("a", 2), entry("b", 1)), later);
            for (final String name : List.of("a", "b")) {
                Assertions.assertEquals(copy.players.get(name).mu, state.players.get(name).mu, 1e-12, method.name());
                Assertions.assertEquals(copy.players.get(name).sigma, state.players.get(name).sigma, 1e-12, method.name());
            }
            // no time has passed: a second contest at the same time drifts nothing, and the read is the state
            final double before = state.players.get("a").sigma;
            Assertions.assertEquals(before, (Double) rating.read(state, "a", "sigma", later), 1e-12, method.name());
            final Rating still = rating(method, true, 0d);
            final Rating.State stillCopy = new Rating.State();
            for (final String name : List.of("a", "b")) {
                final Rating.Player from = state.players.get(name), to = new Rating.Player();
                to.mu = from.mu;
                to.sigma = from.sigma;
                stillCopy.players.put(name, to);
            }
            still.update(stillCopy, List.of(entry("a", 1), entry("b", 2)));
            rating.update(state, List.of(entry("a", 1), entry("b", 2)), later);
            Assertions.assertEquals(stillCopy.players.get("a").sigma, state.players.get("a").sigma, 1e-12, method.name());
            // the longer the absence, the wider: a rested player is read wider than a busy one
            Assertions.assertTrue((Double) rating.read(state, "a", "sigma", later + 200 * DAY) > (Double) rating.read(state, "a", "sigma", later + 20 * DAY));
            // a drift in time needs the time
            Assertions.assertThrows(IllegalStateException.class, () -> rating.update(state, List.of(entry("a", 1), entry("b", 2))));
        }
    }

    /**
     * bradleyTerry's pairings on a field of sixteen fresh players: {@code mean} weighs the contest like one game — the
     * winner's move and everyone's sigma are those of a two-player contest — {@code adjacent} pairs the rank
     * neighbours (the ends have one, the others a better and a worse one: no move between equals, twice the
     * shrinkage), and ties are neighbours of each other and of both sides, whatever the order of the entries.
     */
    @Test
    public void testBradleyTerryPairs() {
        final java.util.function.Function<Rating.Pairs, Rating> bt = pairs ->
                Rating.of(Rating.Method.bradleyTerry, true, null, null, null, null, null, null, null, pairs, List.of("p"), List.of("c"), "y");
        final Rating.State two = new Rating.State();
        bt.apply(Rating.Pairs.all).update(two, List.of(entry("w", 1), entry("l", 2)));
        final double move = (Double) bt.apply(null).read(two, "w", "delta"), sigma = (Double) bt.apply(null).read(two, "w", "sigma");
        final double prior = 25d / 3, v = prior * prior + Math.pow(prior / 100, 2), shrinkOfOne = 1 - sigma * sigma / v;

        final List<Rating.Entry> field = new ArrayList<>();
        for (int i = 1; i <= 16; i++) field.add(entry("p" + i, i));
        final Rating.State mean = new Rating.State(), adjacent = new Rating.State(), all = new Rating.State();
        bt.apply(Rating.Pairs.mean).update(mean, field);
        bt.apply(Rating.Pairs.adjacent).update(adjacent, field);
        bt.apply(Rating.Pairs.all).update(all, field);
        // the numbers the module documentation quotes for k = 16: winner +2.6 / sigma 8.07, adjacent's middle 0 / 7.79
        Assertions.assertEquals(2.6, move, 0.05);
        Assertions.assertEquals(8.07, sigma, 0.005);
        Assertions.assertEquals(7.79, adjacent.players.get("p8").sigma, 0.005);
        Assertions.assertEquals(move, mean.players.get("p1").delta, 1e-12);
        Assertions.assertEquals(-move, mean.players.get("p16").delta, 1e-12);
        Assertions.assertEquals(15 * move, all.players.get("p1").delta, 1e-9, "the full-pair update is fifteen games");
        for (int i = 1; i <= 16; i++) Assertions.assertEquals(sigma, mean.players.get("p" + i).sigma, 1e-12, "p" + i);
        Assertions.assertEquals(move, adjacent.players.get("p1").delta, 1e-12);
        Assertions.assertEquals(-move, adjacent.players.get("p16").delta, 1e-12);
        Assertions.assertEquals(sigma, adjacent.players.get("p1").sigma, 1e-12);
        Assertions.assertEquals(0d, adjacent.players.get("p8").delta, 1e-12, "one better and one worse neighbour of equal strength");
        Assertions.assertEquals(Math.sqrt(v * (1 - 2 * shrinkOfOne)), adjacent.players.get("p8").sigma, 1e-12);

        // ties: 1, 2, 2, 3, 5 — the winner meets both players at 2, a player at 2 the other one (a draw), the winner and
        // the player at 3; the player at 5 meets the player at 3 alone (the nearest better outcome, not the next rank)
        final List<Rating.Entry> tied = List.of(entry("a", 1), entry("b", 2), entry("c", 2), entry("d", 3), entry("e", 5));
        final Rating.State tiedState = new Rating.State();
        bt.apply(Rating.Pairs.adjacent).update(tiedState, tied);
        Assertions.assertEquals(2 * move, tiedState.players.get("a").delta, 1e-12);
        Assertions.assertEquals(0d, tiedState.players.get("b").delta, 1e-12, "a win, a draw and a loss among equals");
        Assertions.assertEquals(tiedState.players.get("b").sigma, tiedState.players.get("c").sigma, 0d);
        Assertions.assertEquals(Math.sqrt(v * (1 - 3 * shrinkOfOne)), tiedState.players.get("b").sigma, 1e-12);
        Assertions.assertEquals(-move, tiedState.players.get("d").delta, 1e-12, "two losses against the players at 2, one win");
        Assertions.assertEquals(-move, tiedState.players.get("e").delta, 1e-12);
        // the entries arrive in any order, and a player never meets its own rows
        for (final Rating.Pairs pairs : Rating.Pairs.values()) {
            final List<Rating.Entry> rows = new ArrayList<>(tied);
            rows.add(entry("a", 4));
            final Rating.State ordered = new Rating.State(), shuffled = new Rating.State();
            bt.apply(pairs).update(ordered, rows);
            final List<Rating.Entry> other = new ArrayList<>(rows);
            Collections.shuffle(other, new Random(5));
            bt.apply(pairs).update(shuffled, other);
            for (final String name : ordered.players.keySet()) {
                Assertions.assertEquals(ordered.players.get(name).mu, shuffled.players.get(name).mu, 0d, pairs + " " + name);
                Assertions.assertEquals(ordered.players.get(name).sigma, shuffled.players.get(name).sigma, 0d, pairs + " " + name);
            }
        }
    }


    /**
     * A rating without a team is the arithmetic it was before teams existed, to the last bit: 400 seeded contests —
     * ties, players holding several rows, drift per contest and in time, every method and pairing — folded by the
     * rating and by {@link PlayersOnly}, the update as it stood before a row could be a team, frozen here. A team of one
     * shares its change by v / v = 1, starts its sums from the member (not from 0) and clamps after the share, so
     * nothing may move — not by an ulp.
     *
     * <p>The two run side by side in one JVM on purpose. {@code Math.exp} / {@code Math.pow} are specified to within
     * an ulp, not to a bit, so a constant recorded on one machine (as this test first did: the hash of every bit
     * pattern, taken from the implementation that knew players only, and met by the team-aware one) would hold a
     * platform's libm rather than the property. An oracle reads the same {@code Math} as the code it judges.
     */
    @Test
    public void testPlayerArithmeticIsUnchangedByTeams() {
        int compared = 0;
        for (final Rating.Method method : Rating.Method.values()) {
            for (final Rating.Pairs pairs : method == Rating.Method.bradleyTerry ? Rating.Pairs.values() : new Rating.Pairs[]{null}) {
                for (final Long tauPer : method == Rating.Method.elo ? new Long[]{null} : new Long[]{null, 3 * DAY}) {
                    final Double tau = method == Rating.Method.elo ? null : 1.5;
                    final Rating rating = Rating.of(method, true, null, null, null, tau, null, null, tauPer, pairs, List.of("p"), List.of("c"), "y");
                    final PlayersOnly before = new PlayersOnly(method, tau == null ? 0 : tau, tauPer == null ? 0L : tauPer, pairs == null ? Rating.Pairs.all : pairs);
                    final Rating.State state = new Rating.State(), expected = new Rating.State();
                    final Random random = new Random(20260921);
                    long millis = 1_700_000_000_000L;
                    for (int contest = 0; contest < 400; contest++) {
                        millis += random.nextInt(10) * DAY / 2;
                        final List<Rating.Entry> entries = new ArrayList<>();
                        final int size = 2 + random.nextInt(9);
                        for (int i = 0; i < size; i++) entries.add(entry("p" + random.nextInt(30), random.nextInt(6)));
                        rating.update(state, entries, millis);
                        before.update(expected, entries, millis);
                    }
                    Assertions.assertEquals(expected.players.keySet(), state.players.keySet());
                    for (final String name : expected.players.keySet()) {
                        final Rating.Player was = expected.players.get(name), is = state.players.get(name);
                        final String where = method + " " + pairs + " tauPer " + tauPer + " " + name;
                        Assertions.assertEquals(Double.doubleToLongBits(was.mu), Double.doubleToLongBits(is.mu), where + " mu " + was.mu + " / " + is.mu);
                        Assertions.assertEquals(Double.doubleToLongBits(was.sigma), Double.doubleToLongBits(is.sigma), where + " sigma " + was.sigma + " / " + is.sigma);
                        Assertions.assertEquals(Double.doubleToLongBits(was.delta), Double.doubleToLongBits(is.delta), where + " delta");
                        Assertions.assertEquals(was.count, is.count, where);
                        Assertions.assertEquals(was.lastMillis, is.lastMillis, where);
                        compared++;
                    }
                }
            }
        }
        Assertions.assertTrue(compared >= 8 * 25, "every configuration rated its players: " + compared);
    }

    /**
     * The rating update as it stood before a row could be a team (players only, default prior and beta): the oracle of
     * {@link #testPlayerArithmeticIsUnchangedByTeams}. FROZEN — it is not kept in step with {@link Rating}; a deliberate
     * change of a player's arithmetic changes this copy in the same commit, and says so.
     */
    private static final class PlayersOnly {
        private static final double KAPPA = 1e-4, MU = 25d, SIGMA = MU / 3, BETA = SIGMA / 2, K_FACTOR = 32d, SCALE = 400d, ELO_MU = 1500d;
        private final Rating.Method method;
        private final double tau;
        private final long tauPerMillis;
        private final Rating.Pairs pairs;

        PlayersOnly(final Rating.Method method, final double tau, final long tauPerMillis, final Rating.Pairs pairs) {
            this.method = method;
            this.tau = tau;
            this.tauPerMillis = tauPerMillis;
            this.pairs = pairs;
        }

        private double mu() {
            return method == Rating.Method.elo ? ELO_MU : MU;
        }

        private double sigma() {
            return method == Rating.Method.elo ? ELO_MU / 3 : SIGMA;
        }

        private double drifted(final Rating.Player p, final long millis) {
            final double s = p == null ? sigma() : p.sigma;
            if (tauPerMillis <= 0) return s * s + tau * tau;
            if (p == null) return s * s;
            final long absence = millis > p.lastMillis ? millis - p.lastMillis : 0L;
            return s * s + tau * tau * absence / (double) tauPerMillis;
        }

        void update(final Rating.State state, final List<Rating.Entry> contest, final long millis) {
            final List<Rating.Entry> entries = new ArrayList<>(contest);
            entries.sort(java.util.Comparator.comparing(Rating.Entry::player).thenComparingDouble(Rating.Entry::outcome));
            final int n = entries.size();
            if (n < 2 || entries.get(0).player().equals(entries.get(n - 1).player())) return;
            final double[] m = new double[n], v = new double[n];
            for (int i = 0; i < n; i++) {
                final Rating.Player p = state.players.get(entries.get(i).player());
                m[i] = p == null ? mu() : p.mu;
                v[i] = drifted(p, millis);
            }
            final double[] dMu = new double[n], shrink = new double[n];
            java.util.Arrays.fill(shrink, 1d);
            switch (method) {
                case elo -> elo(entries, m, dMu);
                case bradleyTerry -> bradleyTerry(entries, m, v, dMu, shrink);
                case plackettLuce -> plackettLuce(entries, m, v, dMu, shrink);
            }
            int i = 0;
            while (i < n) {
                final String name = entries.get(i).player();
                double change = 0, factor = 1;
                int j = i;
                for (; j < n && entries.get(j).player().equals(name); j++) {
                    change += dMu[j];
                    factor *= shrink[j];
                }
                final Rating.Player p = state.players.computeIfAbsent(name, key -> {
                    final Rating.Player created = new Rating.Player();
                    created.mu = mu();
                    created.sigma = sigma();
                    return created;
                });
                p.mu = m[i] + change;
                if (method != Rating.Method.elo) p.sigma = Math.sqrt(v[i] * factor);
                p.delta = change;
                p.count++;
                p.lastMillis = millis;
                i = j;
            }
        }

        private static double score(final double a, final double b) {
            if (a == b) return 0.5;
            return Double.compare(a, b) < 0 ? 1d : 0d;   // ascending: the smaller outcome is the better one
        }

        private void elo(final List<Rating.Entry> entries, final double[] m, final double[] dMu) {
            final int n = entries.size();
            for (int i = 0; i < n; i++) {
                double sum = 0;
                int opponents = 0;
                for (int j = 0; j < n; j++) {
                    if (entries.get(j).player().equals(entries.get(i).player())) continue;
                    final double expected = 1d / (1d + Math.pow(10d, (m[j] - m[i]) / SCALE));
                    sum += score(entries.get(i).outcome(), entries.get(j).outcome()) - expected;
                    opponents++;
                }
                if (opponents > 0) dMu[i] = K_FACTOR * sum / opponents;
            }
        }

        private void bradleyTerry(final List<Rating.Entry> entries, final double[] m, final double[] v, final double[] dMu, final double[] shrink) {
            final int n = entries.size();
            for (int i = 0; i < n; i++) {
                final Rating.Entry self = entries.get(i);
                double better = Double.NaN, worse = Double.NaN;
                if (pairs == Rating.Pairs.adjacent) {
                    for (int q = 0; q < n; q++) {
                        final Rating.Entry other = entries.get(q);
                        if (other.player().equals(self.player())) continue;
                        final double sc = score(other.outcome(), self.outcome());
                        if (sc == 1d && (Double.isNaN(better) || score(other.outcome(), better) == 0d)) better = other.outcome();
                        if (sc == 0d && (Double.isNaN(worse) || score(other.outcome(), worse) == 1d)) worse = other.outcome();
                    }
                }
                double omega = 0, delta = 0;
                int opponents = 0;
                for (int q = 0; q < n; q++) {
                    final Rating.Entry other = entries.get(q);
                    if (other.player().equals(self.player())) continue;
                    if (pairs == Rating.Pairs.adjacent && other.outcome() != self.outcome() && other.outcome() != better && other.outcome() != worse) continue;
                    final double c = Math.sqrt(v[i] + v[q] + 2 * BETA * BETA);
                    final double p = 1d / (1d + Math.exp((m[q] - m[i]) / c));
                    omega += v[i] / c * (score(self.outcome(), other.outcome()) - p);
                    delta += Math.sqrt(v[i]) / c * (v[i] / (c * c)) * p * (1 - p);
                    opponents++;
                }
                if (pairs == Rating.Pairs.mean && opponents > 0) {
                    omega /= opponents;
                    delta /= opponents;
                }
                dMu[i] = omega;
                shrink[i] = Math.max(1 - delta, KAPPA);
            }
        }

        private void plackettLuce(final List<Rating.Entry> entries, final double[] m, final double[] v, final double[] dMu, final double[] shrink) {
            final int n = entries.size();
            double c2 = 0, top = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                c2 += v[i] + BETA * BETA;
                top = Math.max(top, m[i]);
            }
            final double c = Math.sqrt(c2);
            final double[] e = new double[n];
            for (int i = 0; i < n; i++) e[i] = Math.exp((m[i] - top) / c);
            final double[] remaining = new double[n];
            final int[] ties = new int[n];
            for (int q = 0; q < n; q++) {
                for (int t = 0; t < n; t++) {
                    final double sc = score(entries.get(t).outcome(), entries.get(q).outcome());
                    if (sc <= 0.5) remaining[q] += e[t];
                    if (sc == 0.5) ties[q]++;
                }
            }
            for (int i = 0; i < n; i++) {
                double omega = 0, delta = 0;
                for (int q = 0; q < n; q++) {
                    if (q != i && score(entries.get(q).outcome(), entries.get(i).outcome()) < 0.5) continue;
                    final double quotient = e[i] / remaining[q];
                    omega += (q == i ? 1 - quotient : -quotient) / ties[q];
                    delta += quotient * (1 - quotient) / ties[q];
                }
                dMu[i] = v[i] / c * omega;
                shrink[i] = Math.max(1 - Math.sqrt(v[i]) / c * (v[i] / c2) * delta, KAPPA);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // teams: a row rated as the sum of its members
    // ------------------------------------------------------------------------------------------

    private static final double PRIOR_SIGMA = 25d / 3;

    /** A rating of teams of two: the seller (pool "seller", the rating's own prior) and an agent with the prior given. */
    private static Rating duo(final Rating.Method method, final Double tau, final Long tauPerMillis, final double agentMu, final double agentSigma) {
        final Rating solo = Rating.of(method, true, null, null, null, tau, null, null, tauPerMillis, null, List.of("seller_id"), List.of("c"), "y");
        return solo.withTeam("seller", List.of(new Rating.Member("agent", List.of("agent_id"), agentMu, agentSigma, tau == null ? Rating.defaultTau(PRIOR_SIGMA) : tau)));
    }

    private static Rating.Entry team(final String seller, final String agent, final double outcome) {
        return new Rating.Entry(List.of("seller\u0001" + seller, "agent\u0001" + agent), outcome);
    }

    private static Rating.Player player(final double mu, final double sigma, final long lastMillis) {
        final Rating.Player player = new Rating.Player();
        player.mu = mu;
        player.sigma = sigma;
        player.lastMillis = lastMillis;
        return player;
    }

    /**
     * The team form by hand (no drift, two teams — where both Bayesian rules agree). Two fresh members a side share
     * the team's change in halves; next to a settled member (sigma 2) a fresh one holds 94.6% of the team's variance
     * and takes that part of the update: the known member hardly moves.
     */
    @Test
    public void testTeamUpdateIsSharedByVariance() {
        for (final Rating.Method method : List.of(Rating.Method.bradleyTerry, Rating.Method.plackettLuce)) {
            final Rating rating = duo(method, 0d, null, 25d, PRIOR_SIGMA);
            final Rating.State fresh = new Rating.State();
            rating.update(fresh, List.of(team("s1", "a1", 1), team("s2", "a2", 2)));
            for (final String winner : List.of("seller\u0001s1", "agent\u0001a1")) {
                Assertions.assertEquals(26.964185503295965, fresh.players.get(winner).mu, 1e-9, method + " " + winner);
                Assertions.assertEquals(8.17755635771097, fresh.players.get(winner).sigma, 1e-9, method + " " + winner);
                Assertions.assertEquals(1L, fresh.players.get(winner).count);
            }
            Assertions.assertEquals(23.035814496704035, fresh.players.get("agent\u0001a2").mu, 1e-9, method.name());
            // by hand: v = 2 · sigma², c² = 2v + 2β², Ω = v / c / 2, Δ = sqrt(v) / c · v / c² / 4, a half each
            final double v = 2 * PRIOR_SIGMA * PRIOR_SIGMA, beta = PRIOR_SIGMA / 2, c = Math.sqrt(2 * v + 2 * beta * beta);
            Assertions.assertEquals(25 + 0.5 * (v / c / 2), fresh.players.get("seller\u0001s1").mu, 1e-12);
            Assertions.assertEquals(PRIOR_SIGMA * Math.sqrt(1 - 0.5 * (Math.sqrt(v) / c * v / (c * c) / 4)), fresh.players.get("seller\u0001s1").sigma, 1e-12);

            // a settled seller with a fresh agent, on both sides
            final Rating.State settled = new Rating.State();
            for (final String seller : List.of("s1", "s2")) settled.players.put("seller\u0001" + seller, player(25, 2.0, 0));
            rating.update(settled, List.of(team("s1", "a1", 1), team("s2", "a2", 2)));
            Assertions.assertEquals(25.148408504215848, settled.players.get("seller\u0001s1").mu, 1e-9, method.name());
            Assertions.assertEquals(1.9964953348276537, settled.players.get("seller\u0001s1").sigma, 1e-9, method.name());
            Assertions.assertEquals(27.57653653152513, settled.players.get("agent\u0001a1").mu, 1e-9, method.name());
            Assertions.assertEquals(8.07606386511803, settled.players.get("agent\u0001a1").sigma, 1e-9, method.name());
            final double sellerMove = settled.players.get("seller\u0001s1").delta, agentMove = settled.players.get("agent\u0001a1").delta;
            Assertions.assertEquals(4.0 / (4.0 + PRIOR_SIGMA * PRIOR_SIGMA), sellerMove / (sellerMove + agentMove), 1e-12, "the share is the part of the variance");

            // the shares add up to the team's change: the team moves as ONE player of its summed strength moves
            final Rating one = Rating.of(method, true, 50d, Math.sqrt(4.0 + PRIOR_SIGMA * PRIOR_SIGMA), beta, 0d, null, null, List.of("p"), List.of("c"), "y");
            final Rating.State single = new Rating.State();
            one.update(single, List.of(entry("t1", 1), entry("t2", 2)));
            Assertions.assertEquals((Double) one.read(single, "t1", "delta"), sellerMove + agentMove, 1e-12, method.name());
            Assertions.assertEquals((Double) one.read(single, "t1", "mu"), rating.readTeam(settled, List.of("seller\u0001s1", "agent\u0001a1"), "mu", Long.MIN_VALUE), 1e-12);
        }
    }

    /**
     * A member of several teams of one contest receives the sum of its shares, the rows of one team are not compared
     * with each other, and nothing depends on the order the rows arrive in — with ties, a shared agent, a shared seller
     * and a team holding two rows, under every method and pairing, to the last bit.
     */
    @Test
    public void testTeamContestIsOrderFree() {
        final List<Rating.Entry> contest = List.of(team("s1", "a1", 1), team("s2", "a1", 2), team("s3", "a2", 2), team("s1", "a2", 4),
                team("s4", "a3", 5), team("s4", "a3", 3), team("s5", "a4", 6));
        for (final Rating.Method method : List.of(Rating.Method.bradleyTerry, Rating.Method.plackettLuce)) {
            for (final Rating.Pairs pairs : method == Rating.Method.bradleyTerry ? Rating.Pairs.values() : new Rating.Pairs[]{null}) {
                final Rating rating = Rating.of(method, true, null, null, null, null, null, null, null, pairs, List.of("seller_id"), List.of("c"), "y")
                        .withTeam("seller", List.of(new Rating.Member("agent", List.of("agent_id"), 0d, 3d, 0.05)));
                final Rating.State ordered = new Rating.State();
                rating.update(ordered, contest);
                for (int seed = 0; seed < 5; seed++) {
                    final List<Rating.Entry> rows = new ArrayList<>(contest);
                    Collections.shuffle(rows, new Random(seed));
                    final Rating.State shuffled = new Rating.State();
                    rating.update(shuffled, rows);
                    Assertions.assertEquals(ordered.players.keySet(), shuffled.players.keySet());
                    for (final String name : ordered.players.keySet()) {
                        Assertions.assertEquals(ordered.players.get(name).mu, shuffled.players.get(name).mu, 0d, method + " " + pairs + " " + name);
                        Assertions.assertEquals(ordered.players.get(name).sigma, shuffled.players.get(name).sigma, 0d, method + " " + pairs + " " + name);
                    }
                }
                // one contest, one count — for the agent of two teams and the team of two rows alike
                for (final Rating.Player player : ordered.players.values()) Assertions.assertEquals(1L, player.count);
                Assertions.assertEquals(9, ordered.players.size(), "five sellers and four agents, in their own pools");
                // an agent reads its own prior until it is rated, a seller the rating's
                Assertions.assertEquals(0d, (Double) rating.read(ordered, 1, "agent\u0001never", "mu", Long.MIN_VALUE), 0d);
                Assertions.assertEquals(3d, (Double) rating.read(ordered, 1, "agent\u0001never", "sigma", Long.MIN_VALUE), 0d);
                Assertions.assertEquals(25d, (Double) rating.read(ordered, "seller\u0001never", "mu"), 0d);
            }
        }
        // a contest of one team (its two rows) is no contest
        final Rating rating = duo(Rating.Method.plackettLuce, 0d, null, 0d, 3d);
        final Rating.State none = new Rating.State();
        rating.update(none, List.of(team("s4", "a3", 5), team("s4", "a3", 3)));
        Assertions.assertTrue(none.players.isEmpty());
    }

    /**
     * Under a drift in time every member carries the absence of its own: the one that stayed away enters the contest
     * wider, holds the larger part of the team's variance and takes the larger part of the update — and the team's
     * sigma a row reads is that of its members as of the row.
     */
    @Test
    public void testTeamMembersDriftOnTheirOwn() {
        final Rating rating = duo(Rating.Method.plackettLuce, 2d, 30 * DAY, 25d, PRIOR_SIGMA);
        final long now = 1_700_000_000_000L;
        final Rating.State state = new Rating.State();
        // both members of both teams settled at sigma 2; the sellers competed yesterday, agent a1 300 days ago
        for (final String seller : List.of("s1", "s2")) state.players.put("seller\u0001" + seller, player(25, 2, now - DAY));
        state.players.put("agent\u0001a1", player(25, 2, now - 300 * DAY));
        state.players.put("agent\u0001a2", player(25, 2, now - DAY));
        final List<String> rested = List.of("seller\u0001s1", "agent\u0001a1");
        Assertions.assertEquals(Math.sqrt((4 + 4d / 30) + (4 + 4d * 10)), rating.readTeam(state, rested, "sigma", now), 1e-12);
        Assertions.assertEquals(Math.sqrt(8d), rating.readTeam(state, rested, "sigma", Long.MIN_VALUE), 1e-12, "without a time: the state itself");
        Assertions.assertEquals(50d, rating.readTeam(state, rested, "mu", now), 0d);
        rating.update(state, List.of(team("s1", "a1", 1), team("s2", "a2", 2)), now);
        final double seller = state.players.get("seller\u0001s1").delta, agent = state.players.get("agent\u0001a1").delta;
        Assertions.assertEquals((4 + 4d * 10) / (4 + 4d / 30), agent / seller, 1e-9, "the shares are the drifted variances");
        Assertions.assertTrue(agent > 10 * seller);
        Assertions.assertEquals(now, state.players.get("agent\u0001a1").lastMillis);
        // in the other team nobody rested: equal shares
        Assertions.assertEquals(state.players.get("seller\u0001s2").delta, state.players.get("agent\u0001a2").delta, 1e-12);
    }

    /** Rows to teams: the pools keep a seller and an agent of one id apart, and a row missing a member joins no contest. */
    @Test
    public void testTeamsFromRows() {
        final Rating rating = duo(Rating.Method.bradleyTerry, 0d, null, 0d, 3d);
        Assertions.assertEquals(2, rating.members().size());
        final java.util.function.Function<String[], SequenceEvaluator.Past> row = f -> {
            final Map<String, Object> values = new HashMap<>();
            values.put("c", f[0]);
            values.put("seller_id", f[1]);
            values.put("agent_id", f[2]);
            values.put("y", Double.valueOf(f[3]));
            return new SequenceEvaluator.Past(1_000L, values);
        };
        final Rating.State state = new Rating.State();
        // the id "x" is a seller in one row and an agent in another; the last row has no agent
        rating.fold(state, List.of(row.apply(new String[]{"c1", "x", "a1", "1"}), row.apply(new String[]{"c1", "s2", "x", "2"}),
                row.apply(new String[]{"c1", "s3", null, "3"})));
        final Map<String, Object> first = row.apply(new String[]{"c1", "x", "a1", "1"}).values(), second = row.apply(new String[]{"c1", "s2", "x", "2"}).values();
        final String sellerX = rating.memberKey(first, 0), agentX = rating.memberKey(second, 1);
        Assertions.assertTrue(sellerX.startsWith("seller\u0001") && agentX.startsWith("agent\u0001"), sellerX + " " + agentX);
        Assertions.assertEquals(sellerX.substring("seller".length()), agentX.substring("agent".length()), "the same id, apart by the pool alone");
        Assertions.assertEquals(java.util.Set.of(sellerX, rating.memberKey(first, 1), rating.memberKey(second, 0), agentX), state.players.keySet());
        Assertions.assertTrue(state.players.get(sellerX).mu > 25 && state.players.get(agentX).mu < 0);
        Assertions.assertEquals(List.of(sellerX, rating.memberKey(first, 1)), rating.teamOf(first));
        final Map<String, Object> incomplete = row.apply(new String[]{"c2", "x", null, "1"}).values();
        Assertions.assertNull(rating.teamOf(incomplete));
        Assertions.assertNull(rating.readTeam(state, rating.teamOf(incomplete), "mu", 2_000L));
        Assertions.assertEquals(sellerX, rating.player(incomplete), "the seller still reads its rating");
        Assertions.assertTrue((Double) rating.read(state, rating.player(incomplete), "mu") > 25);
        Assertions.assertNull(rating.memberKey(incomplete, 1));
        Assertions.assertNull(rating.read(state, 1, rating.memberKey(incomplete, 1), "mu", 2_000L));
        // a team of members never seen reads the priors: 25 + 0, sqrt(sigma² + 3²)
        final Map<String, Object> unseen = row.apply(new String[]{"c2", "new", "new", "1"}).values();
        Assertions.assertEquals(25d, rating.readTeam(state, rating.teamOf(unseen), "mu", 2_000L), 0d);
        Assertions.assertEquals(Math.sqrt(PRIOR_SIGMA * PRIOR_SIGMA + 9), rating.readTeam(state, rating.teamOf(unseen), "sigma", 2_000L), 1e-12);
        Assertions.assertEquals(25d, rating.readTeam(null, rating.teamOf(unseen), "mu", 2_000L), 0d, "nothing folded yet");

        // what a team cannot be
        final Rating solo = rating(Rating.Method.plackettLuce, true, 0d);
        final List<Rating.Member> agent = List.of(new Rating.Member("agent", List.of("agent_id"), 0d, 3d, 0d));
        Assertions.assertThrows(IllegalArgumentException.class, () -> rating(Rating.Method.elo, true, null).withTeam("seller", agent), "elo has no variance to share by");
        Assertions.assertThrows(IllegalArgumentException.class, () -> solo.withTeam("agent", agent), "one pool twice");
        Assertions.assertThrows(IllegalArgumentException.class, () -> solo.withTeam(null, agent));
        Assertions.assertThrows(IllegalArgumentException.class, () -> solo.withTeam("seller", List.of(new Rating.Member("agent", List.of("agent_id"), 0d, 0d, 0d))), "sigma 0");
        Assertions.assertThrows(IllegalArgumentException.class, () -> solo.withTeam("seller", List.of(new Rating.Member("agent", List.of(), 0d, 3d, 0d))), "no key fields: every row would be one member");
        Assertions.assertThrows(IllegalArgumentException.class, () -> solo.withTeam("seller", List.of(new Rating.Member("agent", null, 0d, 3d, 0d))), "no key fields");
        Assertions.assertThrows(IllegalArgumentException.class, () -> solo.withTeam("seller", List.of()), "a team needs a member besides the rated player");
        Assertions.assertThrows(IllegalArgumentException.class, () -> solo.withTeam("sel\u0001ler", agent), "a separator inside a pool would let two pools meet on one state key");
        Assertions.assertThrows(IllegalArgumentException.class, () -> rating.withTeam("seller", agent), "the whole team is declared at once");
        Assertions.assertThrows(IllegalArgumentException.class, () -> rating.update(new Rating.State(), List.of(entry("a", 1), entry("b", 2))), "a player in a rating of teams");
        Assertions.assertThrows(IllegalArgumentException.class, () -> rating.readTeam(state, rating.teamOf(unseen), "count", 0L));
        Assertions.assertThrows(IllegalArgumentException.class, () -> rating.readTeam(state, rating.teamOf(unseen), null, 0L), "no readout named");
        Assertions.assertThrows(IllegalArgumentException.class, () -> rating.readTeam(state, List.of(sellerX), "mu", 0L), "a team of one in a rating of teams of two");
    }

    /**
     * What the team form is for. Sellers (skill sd 4) work with agents (skill sd 2); an agent keeps a small stable of
     * sellers and a seller leaves it for a random agent one time in four. The performance of a row is the sum of both
     * skills and noise. Rated alone, an agent's rating is its skill PLUS the quality of its stable — the company it
     * keeps; rated as a member of the team, the seller's part goes to the seller. The correlation of the agents' mu with
     * their true skill tells the two apart.
     */
    @Test
    public void testTeamRatingSeparatesAMemberFromItsCompany() {
        for (int seed = 1; seed <= 10; seed++) separates(seed);
    }

    private static void separates(final int seed) {
        final Random random = new Random(seed);
        final int sellers = 200, agents = 20;
        final double[] sellerSkill = new double[sellers], agentSkill = new double[agents];
        final int[] stable = new int[sellers];
        for (int i = 0; i < sellers; i++) {
            sellerSkill[i] = 4 * random.nextGaussian();
            stable[i] = random.nextInt(agents);
        }
        for (int j = 0; j < agents; j++) agentSkill[j] = 2 * random.nextGaussian();

        final Rating teams = Rating.of(Rating.Method.plackettLuce, false, null, null, null, null, null, null, List.of("seller_id"), List.of("c"), "y")
                .withTeam("seller", List.of(new Rating.Member("agent", List.of("agent_id"), 0d, 4d, 0.04)));
        final Rating alone = Rating.of(Rating.Method.plackettLuce, false, 0d, 4d, null, 0.04, null, null, List.of("agent_id"), List.of("c"), "y");
        final Rating.State teamState = new Rating.State(), aloneState = new Rating.State();
        for (int contest = 0; contest < 1500; contest++) {
            final List<Rating.Entry> asTeams = new ArrayList<>(), asAgents = new ArrayList<>();
            final java.util.Set<Integer> drawn = new java.util.HashSet<>();
            while (drawn.size() < 8) drawn.add(random.nextInt(sellers));
            for (final int seller : drawn) {
                final int agent = random.nextInt(4) == 0 ? random.nextInt(agents) : stable[seller];
                final double performance = sellerSkill[seller] + agentSkill[agent] + 4 * random.nextGaussian();
                asTeams.add(team("s" + seller, "a" + agent, performance));
                asAgents.add(entry("a" + agent, performance));
            }
            teams.update(teamState, asTeams);
            alone.update(aloneState, asAgents);
        }
        final double[] inTeam = new double[agents], onItsOwn = new double[agents];
        for (int j = 0; j < agents; j++) {
            inTeam[j] = (Double) teams.read(teamState, 1, "agent\u0001a" + j, "mu", Long.MIN_VALUE);
            onItsOwn[j] = (Double) alone.read(aloneState, "a" + j, "mu");
        }
        final double team = correlation(inTeam, agentSkill), solo = correlation(onItsOwn, agentSkill);
        Assertions.assertTrue(team > TEAM_CORRELATION && team > solo + TEAM_MARGIN, "seed " + seed + ": team " + team + " vs alone " + solo);
    }

    private static double correlation(final double[] a, final double[] b) {
        double ma = 0, mb = 0;
        for (int i = 0; i < a.length; i++) {
            ma += a[i] / a.length;
            mb += b[i] / a.length;
        }
        double sab = 0, saa = 0, sbb = 0;
        for (int i = 0; i < a.length; i++) {
            sab += (a[i] - ma) * (b[i] - mb);
            saa += (a[i] - ma) * (a[i] - ma);
            sbb += (b[i] - mb) * (b[i] - mb);
        }
        return sab / Math.sqrt(saa * sbb);
    }

    /** Measured over the seeds 1..10: in the team 0.918 to 0.990, alone 0.683 to 0.919 (always lower, by 0.060 at least) - the bounds leave room. */
    private static final double TEAM_CORRELATION = 0.9, TEAM_MARGIN = 0.04;

    // ------------------------------------------------------------------------------------------
    // teams through the DSL: with / team on the rating op
    // ------------------------------------------------------------------------------------------

    /** The spec with an agent: a second entity of the same row, and one rating op in place of the three of {@link #SPEC}. */
    private static String teamSpec(final String op) {
        final int from = SPEC.indexOf("      - {type: rating"), to = SPEC.indexOf("  - name: past");
        return (SPEC.substring(0, from) + op + "\n" + SPEC.substring(to))
                .replace("fields: [session_id, seller_id, category,", "fields: [session_id, seller_id, agent_id, category,")
                .replace("  - {name: seller, keys: [seller_id], minInterval: P30D}\n", "  - {name: seller, keys: [seller_id], minInterval: P30D}\n  - {name: agent, keys: [agent_id]}\n");
    }

    private static FeaturePlan compileTeam(final String op) {
        return compileTeamSpec(teamSpec(op));
    }

    private static FeaturePlan compileTeamSpec(final String spec) {
        final JsonObject sources = Config.convertConfigJson(SOURCES.replace("      - {name: seller_id, type: string}\n",
                "      - {name: seller_id, type: string}\n      - {name: agent_id, type: string}\n"), Config.Format.yaml);
        return FeaturePlanCompiler.compile(sources, Config.convertConfigJson(spec, Config.Format.yaml), null);
    }

    private static final String DUO = "      - {type: rating, field: final_price, context: session, order: descending, as: duo, tau: 0.5,"
            + " with: [{entity: agent, mu: 0, sigma: 4}], funcs: [mu, sigma, count], team: [mu, sigma]}";

    @Test
    public void testCompileTeam() {
        final FeaturePlan plan = compileTeam(DUO);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "sequence.rating.with"), plan::describe);
        // the rated player keeps the names and the readout of a rating of players; a member reads under its entity
        // name, the team under `team`
        final OutputColumn seller = plan.getColumn("skill_all_duo_mu"), agent = plan.getColumn("skill_all_duo_agent_sigma"), team = plan.getColumn("skill_all_duo_team_mu");
        for (final String name : List.of("skill_all_duo_mu", "skill_all_duo_sigma", "skill_all_duo_count", "skill_all_duo_agent_mu", "skill_all_duo_agent_sigma",
                "skill_all_duo_agent_count", "skill_all_duo_team_mu", "skill_all_duo_team_sigma")) {
            Assertions.assertNotNull(plan.getColumn(name), () -> name + "\n" + plan.describe());
        }
        Assertions.assertEquals(8, plan.getColumns().stream().filter(c -> "rating".equals(c.getOperator())).count());
        Assertions.assertEquals(com.mercari.solution.module.Schema.Type.int64, plan.getColumn("skill_all_duo_agent_count").getFieldType().getType());
        Assertions.assertNull(seller.getCoordinates().get("readout"));
        Assertions.assertEquals("member", agent.getCoordinates().get("readout"));
        Assertions.assertEquals("agent", agent.getCoordinates().get("member"));
        Assertions.assertEquals("1", agent.getCoordinates().get("memberIndex"));
        Assertions.assertEquals("team", team.getCoordinates().get("readout"));
        // one state behind all of them: the same team, the same stage (the global key), the same shift
        for (final OutputColumn c : List.of(seller, agent, team)) {
            Assertions.assertEquals("seller", c.getCoordinates().get("teamPool"));
            Assertions.assertEquals("agent|agent_id|0.0|4.0|0.5", c.getCoordinates().get("teamMembers"));
            Assertions.assertEquals("skill_all_duo", c.getCoordinates().get("stateKey"));
            Assertions.assertEquals("", c.getCoordinates().get("stageKeys"));
            Assertions.assertTrue(c.getPastInputs().containsAll(List.of("final_price", "seller_id", "agent_id", "session_id")), c.getPastInputs().toString());
            Assertions.assertEquals(OutputColumn.Status.windowShift, c.getStatus());
            Assertions.assertEquals(2, Rating.of(c.getCoordinates()).members().size());
        }
        // the lineage of every column names both entities' keys (the contests it folds are made of them)
        for (final OutputColumn c : List.of(seller, agent, team)) Assertions.assertTrue(c.getInputs().containsAll(List.of("seller_id", "agent_id")), c.getInputs().toString());
        final Rating.Member member = Rating.of(team.getCoordinates()).members().get(1);
        Assertions.assertEquals(new Rating.Member("agent", List.of("agent_id"), 0d, 4d, 0.5), member);

        // a bare entity name: the op's prior and drift
        final FeaturePlan bare = compileTeam(DUO.replace("with: [{entity: agent, mu: 0, sigma: 4}]", "with: [agent]"));
        Assertions.assertFalse(bare.getDiagnostics().hasErrors(), bare::describe);
        Assertions.assertEquals("agent|agent_id|25.0|" + 25d / 3 + "|0.5", bare.getColumn("skill_all_duo_mu").getCoordinates().get("teamMembers"));

        // a rating without `with` is untouched by all this: not a coordinate differs
        final OutputColumn before = compile(SPEC).getColumn("skill_all_final_price_rating_mu");
        Assertions.assertNull(before.getCoordinates().get("teamMembers"));
        Assertions.assertNull(before.getCoordinates().get("teamPool"));
        Assertions.assertNull(before.getCoordinates().get("readout"));
        Assertions.assertEquals(1, Rating.of(before.getCoordinates()).members().size());
        // ... and the pools of a named window split a team's contests as they split a player's
        final FeaturePlan pooled = compileTeamSpec(teamSpec(DUO).replace("    entity: seller\n    ops:\n      - {type: rating",
                "    entity: seller\n    windows: [{filter: \"category = $self.category\"}]\n    ops:\n      - {type: rating"));
        Assertions.assertFalse(pooled.getDiagnostics().hasErrors(), pooled::describe);
        Assertions.assertEquals("category", pooled.getColumn("skill_all_duo_team_mu").getCoordinates().get("stageKeys"));
    }

    @Test
    public void testCompileTeamErrors() {
        final Map<String, String> cases = new java.util.LinkedHashMap<>();
        cases.put(DUO.replace("entity: agent", "entity: nobody"), "an unknown entity");
        cases.put(DUO.replace("entity: agent", "entity: seller"), "the block's own entity");
        cases.put(DUO.replace("with: [{entity: agent, mu: 0, sigma: 4}]", "with: [agent, agent]"), "a member twice");
        cases.put(DUO.replace("as: duo, tau: 0.5,", "as: duo, method: elo,"), "elo keeps no variance to share by");
        cases.put(DUO.replace("as: duo, ", ""), "a team needs a name");
        cases.put(DUO.replace(" with: [{entity: agent, mu: 0, sigma: 4}],", ""), "team readouts without a team");
        cases.put(DUO.replace("team: [mu, sigma]", "team: [mu, count]"), "count is no team readout");
        cases.put(DUO.replace("sigma: 4", "sigma: 0"), "a member's sigma");
        cases.put(DUO.replace("sigma: 4", "sigma: 4, beta: 2"), "beta is the team's, not a member's");
        cases.put(DUO.replace("with: [{entity: agent, mu: 0, sigma: 4}]", "with: [3]"), "neither a name nor a member");
        for (final Map.Entry<String, String> e : cases.entrySet()) {
            final FeaturePlan plan = compileTeam(e.getKey());
            Assertions.assertTrue(plan.getDiagnostics().hasErrors() && hasCode(plan, "sequence.rating.with"), () -> e.getValue() + "\n" + plan.describe());
            Assertions.assertTrue(plan.getColumns().stream().noneMatch(c -> "rating".equals(c.getOperator())), e.getValue());
        }
        // two ops of one name with different teams would share one state
        Assertions.assertTrue(hasCode(compileTeam(DUO + "\n" + DUO.replace("sigma: 4", "sigma: 2").replace(", team: [mu, sigma]", "").replace("funcs: [mu, sigma, count]", "funcs: [delta]")),
                "sequence.rating.as"));
    }

    /**
     * The engine contract over teams: the running state (the fold pointer) equals a from-scratch replay, over the whole
     * history and over the trimmed one, to the last bit — member and team readouts, a drift in time per member, rows
     * without an agent (no contest for them, no member / team readout, the seller still read).
     */
    @Test
    public void testIncrementalMatchesScanAndTrimmedWithTeams() {
        final String ops = "      - {type: rating, field: final_price, context: session, order: descending, as: duo, tau: 1.5, tauPer: P1D,"
                + " with: [{entity: agent, mu: 0, sigma: 4, tau: 0.5}], funcs: [mu, sigma, count, delta], team: [mu, sigma]}\n"
                + "      - {type: rating, field: final_price, context: session, order: descending, method: bradleyTerry, pairs: adjacent, as: adj, with: [agent]}";
        final FeaturePlan plan = compileTeam(ops);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        assertIncrementalMatchesScanAndTrimmed(plan, 4 + 4 + 2 + 2 + 2);
    }

    @Test
    public void testContestProperties() {
        for (final Rating.Method method : Rating.Method.values()) {
            final Rating rating = rating(method, true, null);
            final List<Rating.Entry> contest = List.of(entry("a", 1), entry("b", 2), entry("c", 2), entry("d", 4), entry("e", 5));
            final Rating.State state = new Rating.State();
            rating.update(state, contest);
            rating.update(state, List.of(entry("a", 3), entry("d", 1), entry("e", 2)));
            final double a = (Double) rating.read(state, "a", "mu"), b = (Double) rating.read(state, "b", "mu"), c = (Double) rating.read(state, "c", "mu");
            // tied players with equal priors move alike; the ranking of the first contest is kept among those not seen again
            Assertions.assertEquals(b, c, 1e-12, method.name());
            Assertions.assertTrue(Double.isFinite(a), method.name());
            if (method != Rating.Method.elo) {
                Assertions.assertTrue((Double) rating.read(state, "a", "sigma") < (Double) rating.read(state, "b", "sigma"), "two contests tell more than one");
            }

            // the rows of a contest arrive in any order: the state is the same to the last bit
            final Random random = new Random(5);
            for (int i = 0; i < 20; i++) {
                final List<Rating.Entry> shuffled = new ArrayList<>(contest);
                Collections.shuffle(shuffled, random);
                final Rating.State other = new Rating.State();
                rating.update(other, shuffled);
                rating.update(other, List.of(entry("e", 2), entry("a", 3), entry("d", 1)));
                for (final String player : List.of("a", "b", "c", "d", "e")) {
                    Assertions.assertEquals(rating.read(state, player, "mu"), rating.read(other, player, "mu"), method + " " + player);
                }
            }

            // descending reads a score: the mirrored outcomes give the same ratings
            final Rating descending = rating(method, false, null);
            final Rating.State mirrored = new Rating.State();
            descending.update(mirrored, contest.stream().map(e -> entry(e.player(), -e.outcome())).toList());
            final Rating.State once = new Rating.State();
            rating.update(once, contest);
            for (final String player : List.of("a", "b", "c", "d", "e")) {
                Assertions.assertEquals((Double) rating.read(once, player, "mu"), (Double) rating.read(mirrored, player, "mu"), 1e-12, method + " " + player);
            }
            // from equal priors a strict ranking is kept: every place ends above the next
            final Rating.State ranked = new Rating.State();
            rating.update(ranked, List.of(entry("a", 1), entry("b", 2), entry("c", 3), entry("d", 4), entry("e", 5)));
            final List<String> players = List.of("a", "b", "c", "d", "e");
            for (int i = 1; i < players.size(); i++) {
                Assertions.assertTrue((Double) rating.read(ranked, players.get(i - 1), "mu") > (Double) rating.read(ranked, players.get(i), "mu"), method + " place " + i);
            }
            Assertions.assertTrue((Double) rating.read(ranked, "a", "delta") > 0 && (Double) rating.read(ranked, "e", "delta") < 0, method.name());

            // a contest needs two distinct players
            final Rating.State lonely = new Rating.State();
            rating.update(lonely, List.of(entry("a", 1)));
            rating.update(lonely, List.of(entry("a", 1), entry("a", 2)));
            Assertions.assertTrue(lonely.players.isEmpty(), method.name());
            // a player with two rows takes part twice and is updated once (its rows are adjacent after the sort)
            final Rating.State twice = new Rating.State();
            rating.update(twice, List.of(entry("a", 1), entry("b", 2), entry("a", 3)));
            Assertions.assertEquals(1L, rating.read(twice, "a", "count"), method.name());
            Assertions.assertTrue(Double.isFinite((Double) rating.read(twice, "a", "mu")), method.name());
        }
    }

    private static SequenceEvaluator.Past past(final String contest, final String player, final double outcome) {
        final Map<String, Object> values = new HashMap<>();
        values.put("c", contest);
        values.put("p", player);
        values.put("y", outcome);
        return new SequenceEvaluator.Past(1_000L, values);
    }

    /**
     * Several contests may share one event time, and the replay does not fix the row order inside a timestamp: they
     * are folded in context-key order, so a player taking part in two of them ends at the same rating every run.
     */
    @Test
    public void testContestsOfOneTimeAreFoldedInKeyOrder() {
        final Rating rating = rating(Rating.Method.plackettLuce, true, null);
        // the two contests do not commute: a meets b in one and c in the other, from whatever rating the first left
        final Rating.State first = new Rating.State(), second = new Rating.State();
        rating.update(first, List.of(entry("a", 1), entry("b", 2)));
        rating.update(first, List.of(entry("a", 1), entry("c", 2)));
        rating.update(second, List.of(entry("a", 1), entry("c", 2)));
        rating.update(second, List.of(entry("a", 1), entry("b", 2)));
        Assertions.assertNotEquals(rating.read(first, "b", "mu"), rating.read(second, "b", "mu"));

        final List<SequenceEvaluator.Past> run = List.of(
                past("c1", "a", 1), past("c1", "b", 2), past("c2", "a", 1), past("c2", "c", 2));
        final Rating.State reference = new Rating.State();
        rating.fold(reference, run);
        final Random random = new Random(29);
        for (int i = 0; i < 20; i++) {
            final List<SequenceEvaluator.Past> shuffled = new ArrayList<>(run);
            Collections.shuffle(shuffled, random);
            final Rating.State other = new Rating.State();
            rating.fold(other, shuffled);
            for (final String player : List.of("a", "b", "c")) {
                final String key = rating.player(Map.of("p", player));
                Assertions.assertEquals(rating.read(reference, key, "mu"), rating.read(other, key, "mu"), player);
                Assertions.assertEquals(rating.read(reference, key, "sigma"), rating.read(other, key, "sigma"), player);
            }
        }
        // c1 sorts before c2: the key order, not the row order
        Assertions.assertEquals(rating.read(first, "b", "mu"), rating.read(reference, rating.player(Map.of("p", "b")), "mu"));
    }

    @Test
    public void testCompile() {
        final FeaturePlan plan = compile(SPEC);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final OutputColumn mu = plan.getColumn("skill_all_final_price_rating_mu");
        Assertions.assertNotNull(mu, plan::describe);
        Assertions.assertEquals("plackettLuce", mu.getCoordinates().get("method"));
        Assertions.assertEquals("descending", mu.getCoordinates().get("order"));
        Assertions.assertEquals("seller_id", mu.getCoordinates().get("playerKeys"));
        Assertions.assertEquals("session_id", mu.getCoordinates().get("contestKeys"));
        Assertions.assertEquals("", mu.getCoordinates().get("stageKeys"));
        Assertions.assertEquals("skill_all_final_price_rating", mu.getCoordinates().get("stateKey"));
        Assertions.assertEquals(Double.toString(25d / 3), mu.getCoordinates().get("sigma"));
        Assertions.assertTrue(mu.getPastInputs().containsAll(List.of("final_price", "seller_id", "session_id")), mu.getPastInputs().toString());
        // the outcome is known 2 days 30 minutes after the contest, the row is computed 10 minutes before its own: the
        // window is shifted, and the seller's minInterval (30 days) does not absorb it — other sellers' contests count
        Assertions.assertEquals(OutputColumn.Status.windowShift, mu.getStatus(), plan::describe);
        Assertions.assertEquals(Duration.parse("P2DT40M"), mu.getWindowShift());
        Assertions.assertNull(mu.getCoordinates().get("minInterval"));
        Assertions.assertEquals(com.mercari.solution.module.Schema.Type.int64, plan.getColumn("skill_all_elo_count").getFieldType().getType());
        Assertions.assertNull(plan.getColumn("skill_all_elo_mu").getCoordinates().get("sigma"));
        Assertions.assertEquals("32.0", plan.getColumn("skill_all_elo_mu").getCoordinates().get("kFactor"));
        Assertions.assertNotNull(plan.getColumn("skill_all_bt_sigma"), plan::describe);
        Assertions.assertEquals("0.5", plan.getColumn("skill_all_bt_mu").getCoordinates().get("tau"));
        // running state: nothing is kept for the whole history
        for (final OutputColumn c : plan.getColumns()) {
            if ("rating".equals(c.getOperator())) Assertions.assertNull(SequenceEvaluator.unboundedReason(c), c.getCanonicalName());
        }

        // one replay under the global key, with its own hint
        final FeaturePlan.Stage stage = plan.getStages().stream().filter(s -> s.columnNames().contains(mu.getCanonicalName())).findFirst().orElseThrow();
        Assertions.assertTrue(stage.runsUnderSingleKey(), plan::describe);
        Assertions.assertTrue(hasCode(plan, "sequence.rating.globalKey"), plan::describe);
        Assertions.assertFalse(hasCode(plan, "encoding.globalKey"), plan::describe);

        // a pre-event equality filter splits the contests into pools: it is the partition key
        final FeaturePlan pooled = compile(SPEC.replace("    entity: seller\n    ops:", "    entity: seller\n    windows: [{filter: \"category = $self.category\"}]\n    ops:"));
        Assertions.assertFalse(pooled.getDiagnostics().hasErrors(), pooled::describe);
        final OutputColumn pooledMu = pooled.getColumn("skill_all_final_price_rating_mu");
        Assertions.assertNotNull(pooledMu, pooled::describe);
        Assertions.assertEquals("category", pooledMu.getCoordinates().get("stageKeys"));
        Assertions.assertNull(pooledMu.getCoordinates().get("filter"));
        Assertions.assertEquals(List.of("category"), pooled.getStages().stream().filter(s -> s.columnNames().contains(pooledMu.getCanonicalName())).findFirst().orElseThrow().keys());
        Assertions.assertFalse(hasCode(pooled, "sequence.rating.globalKey"), pooled::describe);

        // the rating over everything next to the one per pool, in one block (their gap is the usual feature): a filter
        // has no token, so both windows are `all` until the pooled one is named — two states, two stages
        final String windows = "    entity: seller\n    windows: [{}, {filter: \"category = $self.category\"%s}]\n    ops:";
        Assertions.assertTrue(hasCode(compile(SPEC.replace("    entity: seller\n    ops:", windows.formatted(""))), "column.duplicate"));
        final FeaturePlan both = compile(SPEC.replace("    entity: seller\n    ops:", windows.formatted(", as: byCategory")));
        Assertions.assertFalse(both.getDiagnostics().hasErrors(), both::describe);
        final OutputColumn whole = both.getColumn("skill_all_final_price_rating_mu"), perPool = both.getColumn("skill_byCategory_final_price_rating_mu");
        Assertions.assertNotNull(perPool, both::describe);
        Assertions.assertEquals("", whole.getCoordinates().get("stageKeys"));
        Assertions.assertEquals("category", perPool.getCoordinates().get("stageKeys"));
        Assertions.assertEquals("skill_byCategory_final_price_rating", perPool.getCoordinates().get("stateKey"));
        Assertions.assertNotEquals(whole.getCoordinates().get("stateKey"), perPool.getCoordinates().get("stateKey"));
        Assertions.assertNotNull(both.getColumn("skill_byCategory_elo_mu"), both::describe);
    }

    @Test
    public void testCompileErrors() {
        final String op = "      - {type: rating, field: final_price, context: session, order: descending, funcs: [mu, sigma, count, delta]}";
        Assertions.assertTrue(SPEC.contains(op));
        final Map<String, String> cases = new java.util.LinkedHashMap<>();
        cases.put("      - {type: rating, field: final_price, order: descending}", "sequence.rating.context");
        cases.put("      - {type: rating, field: final_price, context: nowhere}", "sequence.rating.context");
        cases.put("      - {type: rating, field: final_price, context: session, method: glicko}", "sequence.rating.method");
        cases.put("      - {type: rating, field: final_price, context: session, order: best}", "sequence.rating.order");
        cases.put("      - {type: rating, field: final_price, context: session, funcs: [mean]}", "sequence.rating.func");
        cases.put("      - {type: rating, field: final_price, context: session, method: elo, funcs: [sigma]}", "sequence.rating.func");
        cases.put("      - {type: rating, field: final_price, context: session, method: elo, beta: 2}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, kFactor: 24}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, sigma: -1}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, mu: 0}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: category, context: session}", "sequence.op.type");
        cases.put("      - {type: rating, field: final_price, context: session, pairs: mean}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, method: elo, pairs: mean}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, method: bradleyTerry, pairs: nearest}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, method: elo, tauPer: P30D}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, tauPer: P30D}", "sequence.rating.parameter");
        cases.put("      - {type: rating, field: final_price, context: session, tau: 2, tauPer: PT0S}", "sequence.rating.parameter");
        // two ops of one block on the same segment with different parameters: they would share one running state
        cases.put("      - {type: rating, field: final_price, context: session, funcs: [mu]}\n"
                + "      - {type: rating, field: final_price, context: session, method: elo, funcs: [count]}", "sequence.rating.as");
        for (final Map.Entry<String, String> e : cases.entrySet()) {
            final FeaturePlan plan = compile(SPEC.replace(op, e.getKey()));
            Assertions.assertTrue(hasCode(plan, e.getValue()), () -> e.getKey() + "\n" + plan.describe());
        }
        // a rating has no bounded window, no general filter and no future direction
        for (final String windows : List.of("[{maxAge: P365D}]", "[{maxEvents: 10}]", "[{filter: \"start_price > 10\"}]", "[{filter: \"final_price = $self.final_price\"}]")) {
            final FeaturePlan plan = compile(SPEC.replace("    entity: seller\n    ops:", "    entity: seller\n    windows: " + windows + "\n    ops:"));
            Assertions.assertTrue(hasCode(plan, "sequence.rating.window"), () -> windows + "\n" + plan.describe());
        }
        final FeaturePlan future = compile(SPEC.replace("    entity: seller\n    ops:", "    entity: seller\n    direction: future\n    windows: [{maxAge: P5D}]\n    ops:"));
        Assertions.assertTrue(hasCode(future, "sequence.direction.op"), future::describe);
    }

    /**
     * The running state against the from-scratch replay, and the trimmed history against the untrimmed one: sessions of
     * two to six sellers (a seller may list twice, two sessions may share a time), steps from seconds to weeks so the
     * window shift (2 days 40 minutes) holds contests back.
     */
    @Test
    public void testIncrementalMatchesScanAndTrimmed() {
        assertIncrementalMatchesScanAndTrimmed(SPEC, 9);
    }

    /** The drift in time (the state keeps each player's last contest time; a read adds the drift up to the row) and the pairings. */
    @Test
    public void testIncrementalMatchesScanAndTrimmedWithDriftAndPairs() {
        final String ops = "      - {type: rating, field: final_price, context: session, order: descending, tau: 2, tauPer: P1D, as: pl, funcs: [mu, sigma, count, delta]}\n"
                + "      - {type: rating, field: final_price, context: session, order: descending, method: bradleyTerry, pairs: adjacent, as: adj}\n"
                + "      - {type: rating, field: final_price, context: session, order: descending, method: bradleyTerry, pairs: mean, tau: 1, tauPer: PT6H, as: mean}\n";
        final int from = SPEC.indexOf("      - {type: rating"), to = SPEC.indexOf("  - name: past");
        final FeaturePlan plan = compile(SPEC.substring(0, from) + ops + SPEC.substring(to));
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        Assertions.assertEquals(Long.toString(DAY), plan.getColumn("skill_all_pl_sigma").getCoordinates().get("tauPerMillis"));
        Assertions.assertNull(plan.getColumn("skill_all_pl_sigma").getCoordinates().get("pairs"));
        Assertions.assertEquals("adjacent", plan.getColumn("skill_all_adj_mu").getCoordinates().get("pairs"));
        Assertions.assertNull(plan.getColumn("skill_all_adj_mu").getCoordinates().get("tauPerMillis"));
        assertIncrementalMatchesScanAndTrimmed(SPEC.substring(0, from) + ops + SPEC.substring(to), 8);
    }

    private static void assertIncrementalMatchesScanAndTrimmed(final String spec, final int expectedColumns) {
        assertIncrementalMatchesScanAndTrimmed(compile(spec), expectedColumns);
    }

    private static void assertIncrementalMatchesScanAndTrimmed(final FeaturePlan plan, final int expectedColumns) {
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> columns = plan.getColumns().stream().filter(c -> "rating".equals(c.getOperator())).toList();
        Assertions.assertEquals(expectedColumns, columns.size(), plan::describe);
        final SequenceEvaluator evaluator = new SequenceEvaluator(columns), trimmedEvaluator = new SequenceEvaluator(columns);
        evaluator.setup();
        trimmedEvaluator.setup();
        Assertions.assertTrue(trimmedEvaluator.unboundedColumns().isEmpty(), () -> trimmedEvaluator.unboundedColumns().toString());
        final SequenceEvaluator.Watermarks watermarks = new SequenceEvaluator.Watermarks(trimmedEvaluator.bufferedFields());
        trimmedEvaluator.register(watermarks);

        final Random random = new Random(17);
        long millis = 1_700_000_000_000L;
        final List<SequenceEvaluator.Past> history = new ArrayList<>();
        final SequenceEvaluator.History trimmed = new SequenceEvaluator.History();
        final List<SequenceEvaluator.Past> pending = new ArrayList<>();
        final SequenceEvaluator.KeyState state = new SequenceEvaluator.KeyState(), trimmedState = new SequenceEvaluator.KeyState();
        int compared = 0, rated = 0, session = 0;
        for (int step = 0; step < 300; step++) {
            millis += (long) Math.pow(10, 3 + random.nextDouble() * 6);
            history.addAll(pending);
            for (final SequenceEvaluator.Past p : pending) trimmed.add(new SequenceEvaluator.Past(p.millis(), new HashMap<>(p.values())));
            pending.clear();
            final int sessions = random.nextInt(5) == 0 ? 2 : 1;
            for (int s = 0; s < sessions; s++) {
                final String sessionId = "session" + session++;
                final int size = 2 + random.nextInt(5);
                for (int i = 0; i < size; i++) {
                    final Map<String, Object> row = new HashMap<>();
                    row.put("session_id", random.nextInt(40) == 0 ? null : sessionId);
                    row.put("seller_id", random.nextInt(40) == 0 ? null : "seller" + random.nextInt(8));
                    row.put("final_price", random.nextInt(15) == 0 ? null : (double) random.nextInt(6));
                    row.put("agent_id", random.nextInt(25) == 0 ? null : "agent" + random.nextInt(5));
                    final Map<String, Object> trimmedRow = new HashMap<>(row);
                    for (final OutputColumn c : columns) {
                        final Object incremental = evaluator.evaluateColumn(c, row, millis, history, state);
                        final Object scan = evaluator.evaluateColumn(c, row, millis, history, null);
                        // the same contests folded in the same order: equal to the last bit
                        Assertions.assertEquals(scan, incremental, c.getCanonicalName() + "@" + step);
                        Assertions.assertEquals(scan, trimmedEvaluator.evaluateColumn(c, trimmedRow, millis, trimmed, trimmedState), c.getCanonicalName() + "@" + step + " (trimmed)");
                        // a column reads null when the row lacks what it reads: the seller (the player's columns), the agent
                        // (a member's), either (the team's)
                        final String readout = c.getCoordinates().get("readout");
                        final boolean missing = "team".equals(readout) ? row.get("seller_id") == null || row.get("agent_id") == null
                                : "member".equals(readout) ? row.get("agent_id") == null : row.get("seller_id") == null;
                        if (missing) Assertions.assertNull(scan, c.getCanonicalName());
                        if (c.getCanonicalName().endsWith("_count") && scan != null && (Long) scan > 0) rated++;
                        compared++;
                    }
                    watermarks.reset(trimmed.size());
                    trimmedEvaluator.retainInto(trimmedState, millis, trimmed, watermarks);
                    trimmed.trim(watermarks);
                    pending.add(new SequenceEvaluator.Past(millis, row));
                }
            }
        }
        Assertions.assertTrue(compared > 5000 && rated > 1000, "compared=" + compared + " rated=" + rated);
        // what stays is the contests the shift still holds back, not the pool's past (the trim itself is amortised)
        Assertions.assertTrue(trimmed.base() > 0, "history was never trimmed: " + trimmed.retained() + " of " + history.size());
    }

    /**
     * The scan path over a trimmed history. A rating replays every contest of its window from the window's first
     * entry — it has no bounded tail — so it counts as an unbounded column and pins the key's history; and the window
     * the selection hands it holds only entries the history still has, so the replay cannot read a trimmed index.
     */
    @Test
    public void testScanPathReadsTheHeldWindow() {
        final FeaturePlan plan = compile(SPEC);
        Assertions.assertFalse(plan.getDiagnostics().hasErrors(), plan::describe);
        final List<OutputColumn> columns = plan.getColumns().stream().filter(c -> "rating".equals(c.getOperator())).toList();
        final SequenceEvaluator scan = new SequenceEvaluator(columns, true); // forceScan: the replay, not the running state
        scan.setup();
        Assertions.assertEquals(columns.size(), scan.unboundedColumns().size(), () -> scan.unboundedColumns().toString());
        final SequenceEvaluator.Watermarks watermarks = new SequenceEvaluator.Watermarks(scan.bufferedFields());
        scan.register(watermarks);
        watermarks.reset(1_000);
        Assertions.assertEquals(0, watermarks.all(), "a scan-path rating must pin the history at 0");

        // a trimmed history all the same: the window is what it still holds, read without an index below the base
        final long start = 1_700_000_000_000L, day = 86_400_000L;
        final SequenceEvaluator.History trimmed = new SequenceEvaluator.History();
        final List<SequenceEvaluator.Past> held = new ArrayList<>();
        for (int s = 0; s < 10; s++) {
            for (int i = 0; i < 2; i++) {
                final Map<String, Object> values = new HashMap<>();
                values.put("session_id", "session" + s);
                values.put("seller_id", "seller" + ((s + i) % 4));
                values.put("final_price", (double) i);
                trimmed.add(new SequenceEvaluator.Past(start + s * day, values));
                if (s >= 7) held.add(new SequenceEvaluator.Past(start + s * day, new HashMap<>(values)));
            }
        }
        trimmed.trimBefore(14);
        Assertions.assertEquals(14, trimmed.base());
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> trimmed.get(0));
        final Map<String, Object> row = Map.of("seller_id", "seller1", "session_id", "session99");
        final long now = start + 100 * day;
        for (final OutputColumn c : columns) {
            Assertions.assertEquals(scan.evaluateColumn(c, row, now, held, null),
                    scan.evaluateColumn(c, row, now, trimmed, null), c.getCanonicalName());
        }
        // the held window really rates the row's seller (sessions 8 and 9), so the equality above is not null == null
        Assertions.assertEquals(2L, scan.evaluateColumn(plan.getColumn("skill_all_final_price_rating_count"), row, now, trimmed, null));
    }

    /**
     * Neither path implements a window that evicts a contest: a rating column carrying one is a compile layer that
     * relaxed {@code sequence.rating.window} without implementing it, and the evaluator says so instead of replaying a
     * window it cannot honour. The check belongs to {@code setup()} — the compile layer reads the same column through
     * {@code unboundedReason}, where a throw would replace a diagnostic with a crash of the compiler.
     */
    @Test
    public void testWindowThatEvictsIsRejectedByTheEvaluator() {
        for (final Map.Entry<String, String> coordinate : Map.of("maxEvents", "5", "maxAge", "P7D", "filter", "category = $self.category").entrySet()) {
            final FeaturePlan plan = compile(SPEC);
            final OutputColumn c = plan.getColumn("skill_all_final_price_rating_mu");
            c.coordinates.put(coordinate.getKey(), coordinate.getValue());
            final SequenceEvaluator evaluator = new SequenceEvaluator(List.of(c));
            final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, evaluator::setup, coordinate.getKey());
            Assertions.assertTrue(e.getMessage().contains("sequence.rating.window"), e.getMessage());
            // describing the column stays total: the compile layer reports, it does not run the window
            Assertions.assertDoesNotThrow(() -> SequenceEvaluator.unboundedReason(c), coordinate.getKey());
        }
    }

}
