package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Sequential ratings from multi-player contests (the sequence {@code rating} op): every contest — the rows of one
 * context group sharing an event time — moves the strength of all its players at once, so a player's rating reflects
 * the strength of the opponents it met, which a per-entity aggregate of the outcome cannot.
 *
 * <p>Methods: {@code elo} (the pairwise logistic update, {@code kFactor} shared over the opponents) and the two
 * closed-form Bayesian approximations of Weng &amp; Lin (JMLR 2011) over a Gaussian strength {@code (mu, sigma)}:
 * {@code bradleyTerry} (full pairwise) and {@code plackettLuce} (the ranking likelihood). Before a contest every
 * participant's variance grows by {@code tau²} (strengths drift).
 *
 * <p>Unlike a {@link Summary} the state is <b>not mergeable</b>: an update reads the ratings the earlier contests
 * left, so the contests of a pool must be folded in time order by one replay — the op runs under the stage's one key
 * (global, or the partition a reduced window filter names) and has no per-block / prefix-scan form. Within a contest
 * the update is order-independent: every change is computed from the pre-contest ratings over the entries sorted by
 * player, then applied. A player with several rows in one contest takes part once per row (its own rows are not
 * compared with each other in the pairwise methods) and receives the sum of their changes.
 *
 * <p>The row order the replay hands over is not fixed inside one event time (the sorter orders by event time alone),
 * so the contests held at one event time are folded in the order of their context key, not the order their rows
 * happen to arrive in: two contests of the same timestamp sharing a player would otherwise leave a different state
 * per run.
 */
public final class Rating implements Serializable {

    public enum Method { elo, bradleyTerry, plackettLuce }

    public static final List<String> METHODS = List.of("elo", "bradleyTerry", "plackettLuce");

    /** Readouts: the rating, its uncertainty (not for elo), the contests rated so far, the rating's last change. */
    public static final List<String> FUNCS = List.of("mu", "sigma", "count", "delta");

    public static final List<String> ORDERS = List.of("ascending", "descending");

    /** Lower bound of the factor a contest shrinks a variance by (Weng &amp; Lin's κ). */
    private static final double KAPPA = 1e-4;

    /** One player's running rating. */
    public static final class Player implements Serializable {
        public double mu;
        public double sigma;
        public long count;
        public double delta;
    }

    /** The ratings of one pool: player key → rating (players never seen read the prior). */
    public static final class State implements Serializable {
        public final Map<String, Player> players = new HashMap<>();
    }

    /** One row of a contest: the player and its outcome. */
    public record Entry(String player, double outcome) {}

    private final Method method;
    /** Whether a smaller outcome is the better one (a rank / finishing position) rather than a larger one (a score). */
    private final boolean ascending;
    private final double mu, sigma, beta, tau, kFactor, scale;
    private final List<String> playerKeys, contestKeys;
    private final String field;

    private Rating(final Method method, final boolean ascending, final double mu, final double sigma, final double beta,
                   final double tau, final double kFactor, final double scale,
                   final List<String> playerKeys, final List<String> contestKeys, final String field) {
        this.method = method;
        this.ascending = ascending;
        this.mu = mu;
        this.sigma = sigma;
        this.beta = beta;
        this.tau = tau;
        this.kFactor = kFactor;
        this.scale = scale;
        this.playerKeys = playerKeys;
        this.contestKeys = contestKeys;
        this.field = field;
    }

    /** The prior rating of a method when the spec declares none. */
    public static double defaultMu(final Method method) {
        return method == Method.elo ? 1500d : 25d;
    }

    /** The defaults that derive from the prior: {@code sigma = |mu| / 3}, {@code beta = sigma / 2}, {@code tau = sigma / 100}. */
    public static double defaultSigma(final double mu) {
        return Math.abs(mu) / 3d;
    }

    public static double defaultBeta(final double sigma) {
        return sigma / 2d;
    }

    public static double defaultTau(final double sigma) {
        return sigma / 100d;
    }

    public static final double DEFAULT_K_FACTOR = 32d;
    public static final double DEFAULT_SCALE = 400d;

    /** A rating from explicit parameters (tests, and the compiler's defaults resolution goes through the coordinates). */
    public static Rating of(final Method method, final boolean ascending, final Double mu, final Double sigma, final Double beta,
                            final Double tau, final Double kFactor, final Double scale,
                            final List<String> playerKeys, final List<String> contestKeys, final String field) {
        final double m = mu != null ? mu : defaultMu(method);
        final double s = sigma != null ? sigma : defaultSigma(m);
        return new Rating(method, ascending, m, s, beta != null ? beta : defaultBeta(s), tau != null ? tau : defaultTau(s),
                kFactor != null ? kFactor : DEFAULT_K_FACTOR, scale != null ? scale : DEFAULT_SCALE, playerKeys, contestKeys, field);
    }

    /** The rating a column's coordinates describe (written by {@code FeaturePlanCompiler}, defaults resolved there). */
    public static Rating of(final Map<String, String> coordinates) {
        return of(Method.valueOf(coordinates.get("method")), !"descending".equals(coordinates.get("order")),
                number(coordinates, "mu"), number(coordinates, "sigma"), number(coordinates, "beta"), number(coordinates, "tau"),
                number(coordinates, "kFactor"), number(coordinates, "scale"),
                keys(coordinates.get("playerKeys")), keys(coordinates.get("contestKeys")), coordinates.get("field"));
    }

    private static Double number(final Map<String, String> coordinates, final String key) {
        final String text = coordinates.get(key);
        return text == null ? null : Double.valueOf(text);
    }

    private static List<String> keys(final String joined) {
        return joined == null || joined.isEmpty() ? List.of() : List.of(joined.split(","));
    }

    /** The player a row is rated as (null when a key field is missing: the row reads null and joins no contest). */
    public String player(final Map<String, Object> row) {
        return FeatureValues.key(row, playerKeys);
    }

    /**
     * Folds the rows of ONE event time into the state: they are split into contests by the context keys (a row
     * without them, without a player or without a finite outcome takes no part), and every contest with at least two
     * distinct players updates its players. The contests are applied in context-key order so the arbitrary row order
     * inside a timestamp cannot reach the state (two contests of one event time may share a player).
     */
    public void fold(final State state, final List<SequenceEvaluator.Past> run) {
        final Map<String, List<Entry>> contests = new TreeMap<>();
        for (final SequenceEvaluator.Past p : run) {
            final String contest = FeatureValues.key(p.values(), contestKeys);
            final String player = player(p.values());
            final Double outcome = SequenceEvaluator.finite(p.values().get(field));
            if (contest == null || player == null || outcome == null) continue;
            contests.computeIfAbsent(contest, k -> new ArrayList<>()).add(new Entry(player, outcome));
        }
        for (final List<Entry> entries : contests.values()) update(state, entries);
    }

    /** Folds a time-ordered window from scratch (the scan reference of the running state). */
    public State replay(final List<SequenceEvaluator.Past> window) {
        final State state = new State();
        int from = 0;
        while (from < window.size()) {
            int to = from;
            while (to < window.size() && window.get(to).millis() == window.get(from).millis()) to++;
            fold(state, window.subList(from, to));
            from = to;
        }
        return state;
    }

    /** Applies one contest. The entries' order does not matter (they are sorted by player, then outcome). */
    public void update(final State state, final List<Entry> contest) {
        final List<Entry> entries = new ArrayList<>(contest);
        entries.sort(Comparator.comparing(Entry::player).thenComparingDouble(Entry::outcome));
        final int n = entries.size();
        // a contest needs two distinct players; the entries are sorted by player, so the ends decide it
        if (n < 2 || entries.get(0).player().equals(entries.get(n - 1).player())) return;

        // pre-contest ratings per entry (the variance already carries the drift)
        final double[] m = new double[n], v = new double[n];
        for (int i = 0; i < n; i++) {
            final Player p = state.players.get(entries.get(i).player());
            m[i] = p == null ? mu : p.mu;
            final double s = p == null ? sigma : p.sigma;
            v[i] = s * s + tau * tau;
        }
        final double[] dMu = new double[n], shrink = new double[n];
        Arrays.fill(shrink, 1d);
        switch (method) {
            case elo -> elo(entries, m, dMu);
            case bradleyTerry -> bradleyTerry(entries, m, v, dMu, shrink);
            case plackettLuce -> plackettLuce(entries, m, v, dMu, shrink);
        }

        // a player's entries are adjacent (sorted): one change per player
        int i = 0;
        while (i < n) {
            final String name = entries.get(i).player();
            double change = 0, factor = 1;
            int j = i;
            for (; j < n && entries.get(j).player().equals(name); j++) {
                change += dMu[j];
                factor *= shrink[j];
            }
            final Player p = state.players.computeIfAbsent(name, k -> {
                final Player created = new Player();
                created.mu = mu;
                created.sigma = sigma;
                return created;
            });
            p.mu = m[i] + change;
            if (method != Method.elo) p.sigma = Math.sqrt(v[i] * factor);
            p.delta = change;
            p.count++;
            i = j;
        }
    }

    /** 1 when entry i did better than entry j, 0.5 on equal outcomes, else 0. */
    private double score(final Entry i, final Entry j) {
        final int c = Double.compare(i.outcome(), j.outcome());
        if (c == 0) return 0.5;
        return (c < 0) == ascending ? 1d : 0d;
    }

    private void elo(final List<Entry> entries, final double[] m, final double[] dMu) {
        final int n = entries.size();
        for (int i = 0; i < n; i++) {
            double sum = 0;
            int opponents = 0;
            for (int j = 0; j < n; j++) {
                if (entries.get(j).player().equals(entries.get(i).player())) continue;
                final double expected = 1d / (1d + Math.pow(10d, (m[j] - m[i]) / scale));
                sum += score(entries.get(i), entries.get(j)) - expected;
                opponents++;
            }
            // kFactor is the change of a whole contest: shared over the opponents so it does not grow with the field
            if (opponents > 0) dMu[i] = kFactor * sum / opponents;
        }
    }

    private void bradleyTerry(final List<Entry> entries, final double[] m, final double[] v, final double[] dMu, final double[] shrink) {
        final int n = entries.size();
        for (int i = 0; i < n; i++) {
            double omega = 0, delta = 0;
            for (int q = 0; q < n; q++) {
                if (entries.get(q).player().equals(entries.get(i).player())) continue;
                final double c = Math.sqrt(v[i] + v[q] + 2 * beta * beta);
                final double p = 1d / (1d + Math.exp((m[q] - m[i]) / c));
                omega += v[i] / c * (score(entries.get(i), entries.get(q)) - p);
                delta += Math.sqrt(v[i]) / c * (v[i] / (c * c)) * p * (1 - p);
            }
            dMu[i] = omega;
            shrink[i] = Math.max(1 - delta, KAPPA);
        }
    }

    private void plackettLuce(final List<Entry> entries, final double[] m, final double[] v, final double[] dMu, final double[] shrink) {
        final int n = entries.size();
        double c2 = 0, top = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < n; i++) {
            c2 += v[i] + beta * beta;
            top = Math.max(top, m[i]);
        }
        final double c = Math.sqrt(c2);
        // exp((mu − max) / c): the ratios below are shift-invariant, the shift keeps exp in range
        final double[] e = new double[n];
        for (int i = 0; i < n; i++) e[i] = Math.exp((m[i] - top) / c);
        // per entry q: the strength still in the race when q's place is decided (q and everything not better), and its ties
        final double[] remaining = new double[n];
        final int[] ties = new int[n];
        for (int q = 0; q < n; q++) {
            for (int s = 0; s < n; s++) {
                final double sc = score(entries.get(s), entries.get(q));
                if (sc <= 0.5) remaining[q] += e[s];
                if (sc == 0.5) ties[q]++;
            }
        }
        for (int i = 0; i < n; i++) {
            double omega = 0, delta = 0;
            for (int q = 0; q < n; q++) {
                // the places decided while i was still in the race: q's own and every better one
                if (q != i && score(entries.get(q), entries.get(i)) < 0.5) continue;
                final double quotient = e[i] / remaining[q];
                omega += (q == i ? 1 - quotient : -quotient) / ties[q];
                delta += quotient * (1 - quotient) / ties[q];
            }
            dMu[i] = v[i] / c * omega;
            shrink[i] = Math.max(1 - Math.sqrt(v[i]) / c * (v[i] / c2) * delta, KAPPA);
        }
    }

    /**
     * A readout of a player's rating as the state holds it ({@code state} may be null: nothing folded yet). A player
     * never rated reads the prior {@code mu} / {@code sigma}, count 0 and a null {@code delta}.
     */
    public Object read(final State state, final String player, final String func) {
        if (player == null) return null;
        final Player p = state == null ? null : state.players.get(player);
        return switch (func) {
            case "mu" -> p == null ? mu : p.mu;
            case "sigma" -> p == null ? sigma : p.sigma;
            case "count" -> p == null ? 0L : p.count;
            case "delta" -> p == null ? null : (Object) p.delta;
            default -> throw new IllegalArgumentException("unknown rating readout: " + func);
        };
    }

}
