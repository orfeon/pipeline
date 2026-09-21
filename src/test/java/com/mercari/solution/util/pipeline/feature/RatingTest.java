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
     * One contest of k fresh players without ties: bradleyTerry adds up all k − 1 pairs (the winner's move and the
     * collapse of sigma grow with the field), plackettLuce's normaliser grows with the field (sigma barely shrinks;
     * the winner's is the largest of the contest, the last player's the smallest).
     */
    @Test
    public void testFieldSizeUnderTheDefaults() {
        // k → {bradleyTerry winner move, sigma, plackettLuce winner move, largest sigma, smallest sigma}
        final Map<Integer, double[]> documented = Map.of(
                2, new double[]{2.6, 8.07, 2.6, 8.07, 8.07},
                4, new double[]{7.9, 7.50, 2.8, 8.26, 8.08},
                8, new double[]{18.4, 6.22, 2.3, 8.32, 8.18},
                16, new double[]{39.5, 1.89, 1.7, 8.33, 8.25});
        for (final Map.Entry<Integer, double[]> e : documented.entrySet()) {
            final int k = e.getKey();
            final List<Rating.Entry> contest = new ArrayList<>();
            for (int i = 1; i <= k; i++) contest.add(entry("p" + i, i));
            final Rating bt = rating(Rating.Method.bradleyTerry, true, null), pl = rating(Rating.Method.plackettLuce, true, null);
            final Rating.State btState = new Rating.State(), plState = new Rating.State();
            bt.update(btState, contest);
            pl.update(plState, contest);
            final double[] d = e.getValue();
            Assertions.assertEquals(d[0], (Double) bt.read(btState, "p1", "delta"), 0.05, "bradleyTerry winner, k = " + k);
            for (int i = 1; i <= k; i++) Assertions.assertEquals(d[1], (Double) bt.read(btState, "p" + i, "sigma"), 0.006, "bradleyTerry sigma, k = " + k);
            Assertions.assertEquals(d[2], (Double) pl.read(plState, "p1", "delta"), 0.05, "plackettLuce winner, k = " + k);
            Assertions.assertEquals(d[3], (Double) pl.read(plState, "p1", "sigma"), 0.006, "plackettLuce winner's sigma, k = " + k);
            Assertions.assertEquals(d[4], (Double) pl.read(plState, "p" + k, "sigma"), 0.006, "plackettLuce last player's sigma, k = " + k);
        }
        // a larger beta softens bradleyTerry's collapse at k = 16 (sigma 7.8), not the size of the move — and no beta
        // makes plackettLuce's sigma shrink there
        final List<Rating.Entry> field = new ArrayList<>();
        for (int i = 1; i <= 16; i++) field.add(entry("p" + i, i));
        final double sigma = Rating.defaultSigma(Rating.defaultMu(Rating.Method.bradleyTerry));
        final Rating wide = Rating.of(Rating.Method.bradleyTerry, true, null, null, 2 * sigma, null, null, null, List.of("p"), List.of("c"), "y");
        final Rating.State wideState = new Rating.State();
        wide.update(wideState, field);
        Assertions.assertEquals(7.8, (Double) wide.read(wideState, "p1", "sigma"), 0.05);
        Assertions.assertTrue((Double) wide.read(wideState, "p1", "delta") > 2 * sigma, "still more than two prior standard deviations");
        final Rating narrow = Rating.of(Rating.Method.plackettLuce, true, null, null, sigma / 8, null, null, null, List.of("p"), List.of("c"), "y");
        final Rating.State narrowState = new Rating.State();
        narrow.update(narrowState, field);
        Assertions.assertTrue((Double) narrow.read(narrowState, "p16", "sigma") > 0.98 * sigma, "within 2% of the prior after a contest, whatever beta");
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
        final FeaturePlan plan = compile(spec);
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
                    final Map<String, Object> trimmedRow = new HashMap<>(row);
                    for (final OutputColumn c : columns) {
                        final Object incremental = evaluator.evaluateColumn(c, row, millis, history, state);
                        final Object scan = evaluator.evaluateColumn(c, row, millis, history, null);
                        // the same contests folded in the same order: equal to the last bit
                        Assertions.assertEquals(scan, incremental, c.getCanonicalName() + "@" + step);
                        Assertions.assertEquals(scan, trimmedEvaluator.evaluateColumn(c, trimmedRow, millis, trimmed, trimmedState), c.getCanonicalName() + "@" + step + " (trimmed)");
                        if (row.get("seller_id") == null) Assertions.assertNull(scan, c.getCanonicalName());
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
