package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Sequential ratings from multi-player contests (the sequence {@code rating} op): every contest — the rows of one
 * context group sharing an event time — moves the strength of all its players at once, so a player's rating reflects
 * the strength of the opponents it met, which a per-entity aggregate of the outcome cannot.
 *
 * <p>Methods: {@code elo} (the pairwise logistic update, {@code kFactor} shared over the opponents) and the two
 * closed-form Bayesian approximations of Weng &amp; Lin (JMLR 2011) over a Gaussian strength {@code (mu, sigma)}:
 * {@code bradleyTerry} (pairwise — every opponent, the rank neighbours only, or the mean over the opponents:
 * {@link Pairs}) and {@code plackettLuce} (the ranking likelihood). Before a contest every participant's variance
 * grows by {@code tau²} (strengths drift) — per contest, or, with {@code tauPer}, in proportion to the time since the
 * player's previous contest, so that an absence reopens the uncertainty and a busy stretch does not.
 *
 * <p>Unlike a {@link Summary} the state is <b>not mergeable</b>: an update reads the ratings the earlier contests
 * left, so the contests of a pool must be folded in time order by one replay — the op runs under the stage's one key
 * (global, or the partition a reduced window filter names) and has no per-block / prefix-scan form. Within a contest
 * the update is order-independent: every change is computed from the pre-contest ratings over the entries sorted by
 * player, then applied. A player with several rows in one contest takes part once per row (its own rows are not
 * compared with each other in the pairwise methods) and receives the sum of their changes.
 *
 * <p><b>Teams</b> ({@link #withTeam}). A row may be rated as a team: the rated player together with other entities of
 * the same row (its {@link Member}s), each with a rating of its own. The team's strength is the sum of its members'
 * — {@code mu = Σ mu_j}, {@code sigma² = Σ sigma_j²}, the performance noise {@code beta} once per team — the update
 * rules run on the teams exactly as they run on players, and a team's change is shared among its members by their
 * part of its variance: {@code mu_j += (v_j / v) · Ω}, {@code sigma_j² = v_j · max(1 − (v_j / v) · Δ, κ)} (Weng &amp;
 * Lin's team form). The well-known member hardly moves and the uncertain one takes the update, which is what lets a
 * member's contribution be told apart from that of the company it keeps. A team of one member has the share 1:
 * the arithmetic of a player, to the last bit. A member of several teams of one contest receives the sum of its
 * shares, as a player of several rows does.
 *
 * <p>The row order the replay hands over is not fixed inside one event time (the sorter orders by event time alone),
 * so the contests held at one event time are folded in the order of their context key, not the order their rows
 * happen to arrive in: two contests of the same timestamp sharing a player would otherwise leave a different state
 * per run.
 */
public final class Rating implements Serializable {

    public enum Method { elo, bradleyTerry, plackettLuce }

    /**
     * The opponents a {@code bradleyTerry} player is paired with. {@code all}: every opponent, the sums growing with
     * the field (the paper's full-pair update — in a field of sixteen one contest is fifteen games). {@code adjacent}:
     * the rank neighbours only (the paper's partial-pair update) — the opponents sharing the player's outcome and those
     * at the nearest better and the nearest worse outcome, a set the outcomes alone decide. {@code mean}: every
     * opponent, the sums divided by their number — a contest weighs like one game whatever the field, the
     * normalisation {@code elo} applies to {@code kFactor}.
     *
     * <p>{@code all} and {@code mean} pair every player with every opponent, so a pair is always read from both
     * sides. {@code adjacent} is symmetric too, <b>except</b> where a player holds several rows in one contest: a
     * neighbour is the nearest outcome among that row's opponents, and the player's own other rows are none of
     * them, so a row may meet an opponent whose own neighbours the row is not one of. The pairing stays a function
     * of the outcomes either way, which is what keeps a contest with ties order-free.
     */
    public enum Pairs { all, adjacent, mean }

    public static final List<String> PAIRS = Arrays.stream(Pairs.values()).map(Pairs::name).toList();

    /** The method names as the DSL spells them, in declaration order (derived so it cannot drift from the enum). */
    public static final List<String> METHODS = Arrays.stream(Method.values()).map(Method::name).toList();

    /**
     * Readouts: the rating, its uncertainty (not for elo), the contests rated so far, the rating's last change, the
     * rating net of the prior ({@code deviation}: what the contests added), and the rating standardised within its
     * pool ({@code z}: against the mean and sd of {@code mu} over the pool's rated players — a member's level is
     * identified up to a shift of its pool, which this reads it free of).
     */
    public static final List<String> FUNCS = List.of("mu", "sigma", "count", "delta", "deviation", "z");

    public static final List<String> ORDERS = List.of("ascending", "descending");

    /** Lower bound of the factor a contest shrinks a variance by (Weng &amp; Lin's κ). */
    private static final double KAPPA = 1e-4;

    /** One player's running rating. */
    public static final class Player implements Serializable {
        public double mu;
        public double sigma;
        public long count;
        public double delta;
        /** The event time of the player's last contest (the clock of a time drift). */
        public long lastMillis;
    }

    /**
     * The running moments of {@code mu} over the rated players of one pool (a member entity, or the single pool of a
     * rating without a team). The sums are taken of {@code mu − shift}, the shift being the pool's prior {@code mu}
     * (every player of a pool shares one): the deviations are on the scale of the spread, so a large prior next to a
     * small spread does not cancel the variance away in {@code Σx² − n·mean²}. Below a relative floor the sd reads 0.
     */
    public static final class Pool implements Serializable {
        public long players;
        public double shift;
        public double sum, sumSq;

        /** The sd below which the pool has no spread to standardise by (relative to the deviations' scale). */
        private static final double RELATIVE_FLOOR = 1e-7;

        void add(final double mu, final double prior) {
            if (players == 0) shift = prior;
            players++;
            sum += mu - shift;
            sumSq += (mu - shift) * (mu - shift);
        }

        void move(final double from, final double to) {
            sum += to - from;
            sumSq += (to - shift) * (to - shift) - (from - shift) * (from - shift);
        }

        /** The mean of {@code mu} over the pool's rated players. */
        public double mean() {
            return shift + sum / players;
        }

        /** The population sd of {@code mu} over the pool's rated players; 0 when it is below rounding of the deviations. */
        public double sd() {
            final double meanDeviation = sum / players, meanSquare = sumSq / players;
            final double variance = meanSquare - meanDeviation * meanDeviation;
            if (!(variance > RELATIVE_FLOOR * RELATIVE_FLOOR * Math.max(meanSquare, Double.MIN_NORMAL))) return 0d;
            return Math.sqrt(variance);
        }
    }

    /**
     * The ratings of one pool: player key → rating (players never seen read the prior). Beside them, what a readout
     * needs and the players do not hold: per team the contests it ran ({@code teams}, teams of two or more members
     * only, and only when the rating counts teams — {@link #withTeam(String, List, boolean)}: one entry per distinct
     * team is a cost a spec that reads no {@code team: [count]} does not pay), per member pool the moments of
     * {@code mu} over its rated players ({@code pools}, keyed by the pool name, {@code ""} for a rating without a team).
     */
    public static final class State implements Serializable {
        public final Map<String, Player> players = new HashMap<>();
        public final Map<String, Long> teams = new HashMap<>();
        public final Map<String, Pool> pools = new HashMap<>();
    }

    /**
     * One row of a contest: the team it is rated as — the state keys of its members ({@link #teamOf}), the rated
     * player first — and its outcome. A row of one member is a player: every rating without a team.
     */
    public record Entry(List<String> members, double outcome) {
        public Entry(final String player, final double outcome) {
            this(List.of(player), outcome);
        }

        /** The rated player: the first member. */
        public String player() {
            return members.get(0);
        }
    }

    /**
     * A member of the team a row is rated as: the fields of the row that name it, its prior and its drift. {@code pool}
     * is the namespace of its keys in the state — members of different entities live in one {@link State}, and a seller
     * and an agent may well share an id — or null: the keys themselves, for the single player of a rating without a team,
     * whose state is keyed as it always was.
     */
    public record Member(String pool, List<String> keys, double mu, double sigma, double tau) implements Serializable {}

    /**
     * The team readouts: the sum of the members' ratings, the uncertainty of that sum, the contests this very team
     * (all its members together) has run, and the sum net of the members' priors.
     */
    public static final List<String> TEAM_FUNCS = List.of("mu", "sigma", "count", "deviation");

    /**
     * Between a pool and a key in a state key ({@link #memberKey}), and between the members in a team's identity
     * ({@link #id}). A key from {@link FeatureValues#key} carries {@code U+0001} itself (it terminates every
     * component) but is length-prefixed, so a concatenation of keys stays unambiguous whatever the data holds. What
     * could make two pools meet on one state key is a separator inside a <b>pool</b>, which {@link #withTeam} rejects.
     */
    private static final char POOL_SEPARATOR = '\u0001', MEMBER_SEPARATOR = '\u0002';

    private final Method method;
    /** Whether a smaller outcome is the better one (a rank / finishing position) rather than a larger one (a score). */
    private final boolean ascending;
    private final double beta, kFactor, scale;
    /** The time {@code tau} is the drift of, or 0: {@code tau} is the drift of one contest. */
    private final long tauPerMillis;
    private final Pairs pairs;
    private final List<String> contestKeys;
    private final String field;
    /**
     * The team a row is rated as, the rated player first; one member = a player. The rated player's prior, drift and
     * key fields live here and nowhere else (the rating's {@code mu} / {@code sigma} / {@code tau} are its first member's).
     */
    private final List<Member> members;
    /** Whether {@link #update} keeps the contests per team ({@code State.teams}, the {@code team: [count]} readout). */
    private final boolean countTeams;

    private Rating(final Method method, final boolean ascending, final double beta, final double kFactor, final double scale,
                   final long tauPerMillis, final Pairs pairs, final List<String> contestKeys, final String field, final List<Member> members,
                   final boolean countTeams) {
        this.members = members;
        this.countTeams = countTeams;
        this.tauPerMillis = tauPerMillis;
        this.pairs = pairs;
        this.method = method;
        this.ascending = ascending;
        this.beta = beta;
        this.kFactor = kFactor;
        this.scale = scale;
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
        return of(method, ascending, mu, sigma, beta, tau, kFactor, scale, null, null, playerKeys, contestKeys, field);
    }

    /**
     * @param tauPerMillis the time {@code tau} is the drift of (null / 0: {@code tau} is the drift of one contest)
     * @param pairs        bradleyTerry's pairing (null: all)
     */
    public static Rating of(final Method method, final boolean ascending, final Double mu, final Double sigma, final Double beta,
                            final Double tau, final Double kFactor, final Double scale, final Long tauPerMillis, final Pairs pairs,
                            final List<String> playerKeys, final List<String> contestKeys, final String field) {
        final double m = mu != null ? mu : defaultMu(method);
        final double s = sigma != null ? sigma : defaultSigma(m);
        // a rating without a team: one member under no pool, its state keyed by the bare keys as it always was
        return new Rating(method, ascending, beta != null ? beta : defaultBeta(s),
                kFactor != null ? kFactor : DEFAULT_K_FACTOR, scale != null ? scale : DEFAULT_SCALE,
                tauPerMillis == null ? 0L : tauPerMillis, pairs == null ? Pairs.all : pairs, contestKeys, field,
                List.of(new Member(null, playerKeys, m, s, tau != null ? tau : defaultTau(s))), false);
    }

    /**
     * The rating a column's coordinates describe (written by {@code FeaturePlanCompiler}, defaults resolved there). A
     * team is the two coordinates {@code teamPool} (the rated player's pool) and {@code teamMembers}
     * ({@link #encodeMembers}); without them the rating is one of players, as it always was. {@code teamCounts} (an
     * op coordinate, so every column of the op agrees) keeps the contests per team for a {@code team: [count]} column.
     */
    public static Rating of(final Map<String, String> coordinates) {
        final Rating players = playersOf(coordinates);
        final String members = coordinates.get("teamMembers");
        return members == null ? players : players.withTeam(coordinates.get("teamPool"), decodeMembers(members),
                "true".equals(coordinates.get("teamCounts")));
    }

    /** {@code pool|key,key|mu|sigma|tau} per member, joined by {@code ;} (names are checked for the separators at compile time). */
    public static String encodeMembers(final List<Member> members) {
        final List<String> parts = new ArrayList<>();
        for (final Member m : members) parts.add(m.pool() + "|" + String.join(",", m.keys()) + "|" + m.mu() + "|" + m.sigma() + "|" + m.tau());
        return String.join(";", parts);
    }

    static List<Member> decodeMembers(final String text) {
        final List<Member> members = new ArrayList<>();
        for (final String part : text.split(";")) {
            final String[] f = part.split("\\|");
            if (f.length != 5) throw new IllegalArgumentException("not a team member (pool|keys|mu|sigma|tau): " + part);
            members.add(new Member(f[0], List.of(f[1].split(",")), Double.parseDouble(f[2]), Double.parseDouble(f[3]), Double.parseDouble(f[4])));
        }
        return members;
    }

    private static Rating playersOf(final Map<String, String> coordinates) {
        return of(Method.valueOf(coordinates.get("method")), !"descending".equals(coordinates.get("order")),
                number(coordinates, "mu"), number(coordinates, "sigma"), number(coordinates, "beta"), number(coordinates, "tau"),
                number(coordinates, "kFactor"), number(coordinates, "scale"),
                coordinates.get("tauPerMillis") == null ? null : Long.valueOf(coordinates.get("tauPerMillis")),
                coordinates.get("pairs") == null ? null : Pairs.valueOf(coordinates.get("pairs")),
                FeaturePlanCompiler.keyList(coordinates.get("playerKeys")), FeaturePlanCompiler.keyList(coordinates.get("contestKeys")),
                coordinates.get("field"));
    }

    private static Double number(final Map<String, String> coordinates, final String key) {
        final String text = coordinates.get(key);
        return text == null ? null : Double.valueOf(text);
    }

    /**
     * This rating over teams: the rated player — its keys now in the namespace {@code pool} — together with the members
     * {@code with}, each of a pool of its own. The rating's {@code mu} / {@code sigma} / {@code tau} stay the player's;
     * {@code beta}, the pairing and the clock of a drift in time are the team's. {@code elo} keeps no uncertainty to
     * share a team's change by, so a team is rated by the two Bayesian methods only. The contests per team are kept
     * ({@code team: [count]}); {@link #withTeam(String, List, boolean)} drops them.
     */
    public Rating withTeam(final String pool, final List<Member> with) {
        return withTeam(pool, with, true);
    }

    /**
     * {@link #withTeam(String, List)}, keeping the contests per team ({@code State.teams}, read by {@code team:
     * [count]}) only when {@code countTeams}: one entry per distinct team, held for the whole replay of the pool.
     */
    public Rating withTeam(final String pool, final List<Member> with, final boolean countTeams) {
        if (method == Method.elo) {
            throw new IllegalArgumentException("elo keeps no uncertainty to share a team's update by: a team is rated by bradleyTerry / plackettLuce");
        }
        // the whole team is declared at once: a second call would otherwise drop the members the first one added
        if (members.size() > 1) {
            throw new IllegalArgumentException("this rating already has a team of " + members.size() + " members: withTeam declares the whole team at once");
        }
        if (with == null || with.isEmpty()) {
            throw new IllegalArgumentException("a team needs at least one member besides the rated player");
        }
        final Member player = members.get(0);
        final List<Member> team = new ArrayList<>();
        team.add(new Member(pool, player.keys(), player.mu(), player.sigma(), player.tau()));
        team.addAll(with);
        final Set<String> pools = new HashSet<>();
        for (final Member member : team) {
            if (member == null) throw new IllegalArgumentException("a team holds no null member");
            if (member.pool() == null || member.pool().isEmpty()) throw new IllegalArgumentException("every member of a team needs a pool (the namespace of its keys)");
            if (member.pool().indexOf(POOL_SEPARATOR) >= 0 || member.pool().indexOf(MEMBER_SEPARATOR) >= 0) {
                throw new IllegalArgumentException("a pool cannot hold the key separators U+0001 / U+0002 (two pools would meet on one state key): '" + member.pool() + "'");
            }
            if (!pools.add(member.pool())) throw new IllegalArgumentException("two members of one team share the pool '" + member.pool() + "'");
            // an empty key list would name every row the same member; a null one fails per row, deep in the fold
            if (member.keys() == null || member.keys().isEmpty()) {
                throw new IllegalArgumentException("a member needs the key fields that name it: " + member);
            }
            if (!(member.sigma() > 0) || !(member.tau() >= 0) || !Double.isFinite(member.mu())) {
                throw new IllegalArgumentException("a member needs a finite mu, sigma > 0 and tau >= 0: " + member);
            }
        }
        return new Rating(method, ascending, beta, kFactor, scale, tauPerMillis, pairs, contestKeys, field, List.copyOf(team), countTeams);
    }

    /** The members of the team a row is rated as (one: a player). */
    public List<Member> members() {
        return members;
    }

    /** The player a row is rated as (null when a key field is missing: the row reads null and joins no contest). */
    public String player(final Map<String, Object> row) {
        return memberKey(row, 0);
    }

    /** The state key of a row's {@code member}-th team member, or null when one of its key fields is missing. */
    public String memberKey(final Map<String, Object> row, final int member) {
        final Member m = members.get(member);
        final String key = FeatureValues.key(row, m.keys());
        return key == null || m.pool() == null ? key : m.pool() + POOL_SEPARATOR + key;
    }

    /** The team a row is rated as — the state keys of its members — or null when a member is missing: the row joins no contest. */
    public List<String> teamOf(final Map<String, Object> row) {
        final String[] team = new String[members.size()];
        for (int j = 0; j < team.length; j++) {
            final String key = memberKey(row, j);
            if (key == null) return null;
            team[j] = key;
        }
        return List.of(team);
    }

    /** What makes two rows the same team: all their members (a player's own rows, in a rating without a team). */
    private static String id(final Entry entry) {
        return teamId(entry.members());
    }

    private static String teamId(final List<String> members) {
        return members.size() == 1 ? members.get(0) : String.join(String.valueOf(MEMBER_SEPARATOR), members);
    }

    /**
     * Folds the rows of ONE event time into the state: they are split into contests by the context keys (a row
     * without them, without a player — or a member of its team — or without a finite outcome takes no part), and every
     * contest with at least two distinct players (teams) updates them. The contests are applied in context-key order so the arbitrary row order
     * inside a timestamp cannot reach the state (two contests of one event time may share a player).
     */
    public void fold(final State state, final List<SequenceEvaluator.Past> run) {
        final Map<String, List<Entry>> contests = new TreeMap<>();
        // the run's event time dates every contest it holds (the clock of a drift in time): a run of mixed times
        // would date them all by its first row, so the precondition is checked rather than trusted
        final long millis = run.isEmpty() ? 0L : run.get(0).millis();
        for (final SequenceEvaluator.Past p : run) {
            if (p.millis() != millis) {
                throw new IllegalArgumentException("fold takes the rows of ONE event time: " + millis + " and " + p.millis()
                        + " (the caller must slice the history by event time - SequenceEvaluator.advanceRating / Rating.replay)");
            }
            final String contest = FeatureValues.key(p.values(), contestKeys);
            final List<String> team = teamOf(p.values());
            final Double outcome = SequenceEvaluator.finite(p.values().get(field));
            if (contest == null || team == null || outcome == null) continue;
            contests.computeIfAbsent(contest, k -> new ArrayList<>()).add(new Entry(team, outcome));
        }
        for (final List<Entry> entries : contests.values()) update(state, entries, millis);
    }

    /**
     * Folds a time-ordered window from scratch (the scan reference of the running state). Every contest of the window
     * is read, from its first entry: a rating has no bounded tail, which is why a rating evaluated on the scan path
     * counts as an unbounded column and pins the key's history there ({@code SequenceEvaluator.unbounded}). A stage
     * takes that path only under the equivalence tests' {@code forceScan} — in production the fold pointer serves the
     * column, and its watermark is that pointer.
     */
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

    /** Applies one contest of a rating whose drift is per contest (a drift in time needs the contest's time). */
    public void update(final State state, final List<Entry> contest) {
        if (tauPerMillis > 0) throw new IllegalStateException("a rating with tauPer drifts in time: update(state, contest, millis)");
        update(state, contest, 0L);
    }

    /**
     * The variance a player enters a contest — or is read — with at {@code millis}: the rating's, grown by the drift.
     * Per contest that is {@code tau²}, added when a contest is held (a read adds nothing: no contest has happened).
     * In time it is {@code tau² · Δt / tauPer} over the time since the player's last contest, for a contest and a read
     * alike — the read of a returning player shows the absence it comes back from — and nothing for a player never
     * rated, whose prior is the whole uncertainty already.
     */
    private double drifted(final Member member, final Player p, final long millis, final boolean contest) {
        final double s = p == null ? member.sigma() : p.sigma, tau = member.tau();
        if (tauPerMillis <= 0) return s * s + (contest ? tau * tau : 0d);
        if (p == null) return s * s;
        // the absence, compared before it is subtracted: `millis - lastMillis` would wrap to a huge positive
        // difference for an extreme millis, where Math.max of the wrapped value cannot clamp it back to none
        final long absence = millis > p.lastMillis ? millis - p.lastMillis : 0L;
        return s * s + tau * tau * absence / (double) tauPerMillis;
    }

    /** What a contest does to one member: the sum of its shares, from the rating it entered the contest with. */
    private static final class Change {
        final Member member;
        final double mu, variance;
        double change, factor = 1;

        Change(final Member member, final double mu, final double variance) {
            this.member = member;
            this.mu = mu;
            this.variance = variance;
        }
    }

    /** Applies one contest held at {@code millis}. The entries' order does not matter (they are sorted by team, then outcome). */
    public void update(final State state, final List<Entry> contest, final long millis) {
        final List<Entry> entries = new ArrayList<>(contest);
        entries.sort(Comparator.comparing(Rating::id).thenComparingDouble(Entry::outcome));
        final int n = entries.size(), k = members.size();
        final String[] ids = new String[n];
        for (int i = 0; i < n; i++) {
            if (entries.get(i).members().size() != k) {
                throw new IllegalArgumentException("an entry of " + entries.get(i).members().size() + " member(s) in a rating of teams of " + k);
            }
            ids[i] = id(entries.get(i));
        }
        // a contest needs two distinct players (teams); the entries are sorted by them, so the ends decide it
        if (n < 2 || ids[0].equals(ids[n - 1])) return;

        // pre-contest ratings per entry (the variance already carries the drift): a team is the sum of its members.
        // The per-member values are held flat (index i * k + j): one array rather than one per entry
        final double[] mus = new double[n * k], variances = new double[n * k];
        final double[] m = new double[n], v = new double[n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < k; j++) {
                final Player p = state.players.get(entries.get(i).members().get(j));
                mus[i * k + j] = p == null ? members.get(j).mu() : p.mu;
                variances[i * k + j] = drifted(members.get(j), p, millis, true);
                // the first member starts the sums (not 0 +): a team of one is its member to the last bit
                m[i] = j == 0 ? mus[i * k + j] : m[i] + mus[i * k + j];
                v[i] = j == 0 ? variances[i * k + j] : v[i] + variances[i * k + j];
            }
        }
        // Ω and Δ per entry: the change of its mu, and the part of its variance the contest takes away
        final double[] dMu = new double[n], deltas = new double[n];
        switch (method) {
            case elo -> elo(entries, ids, m, dMu);
            case bradleyTerry -> bradleyTerry(entries, ids, m, v, dMu, deltas);
            case plackettLuce -> plackettLuce(entries, m, v, dMu, deltas);
        }

        // a team's change is shared among its members by their part of its variance (1 for a player). The entries are
        // walked in their sorted order, so the sums of a member of several entries — a player of several rows, a member
        // of several teams — are taken in an order the contest decides, not the order its rows arrived in
        final Map<String, Change> changes = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < k; j++) {
                final String key = entries.get(i).members().get(j);
                Change c = changes.get(key);
                if (c == null) {
                    c = new Change(members.get(j), mus[i * k + j], variances[i * k + j]);
                    changes.put(key, c);
                }
                final double share = variances[i * k + j] / v[i];
                c.change += share * dMu[i];
                c.factor *= Math.max(1 - share * deltas[i], KAPPA);
            }
        }
        for (final Map.Entry<String, Change> e : changes.entrySet()) {
            final Change c = e.getValue();
            Player p = state.players.get(e.getKey());
            final boolean created = p == null;
            if (created) {
                p = new Player();
                p.mu = c.member.mu();
                p.sigma = c.member.sigma();
                state.players.put(e.getKey(), p);
            }
            final double before = p.mu;
            p.mu = c.mu + c.change;
            if (method != Method.elo) p.sigma = Math.sqrt(c.variance * c.factor);
            p.delta = c.change;
            p.count++;
            p.lastMillis = millis;
            // the pool's moments of mu follow the player (a bookkeeping beside the players: their numbers are untouched)
            final Pool pool = state.pools.computeIfAbsent(c.member.pool() == null ? "" : c.member.pool(), key -> new Pool());
            if (created) pool.add(p.mu, c.member.mu()); else pool.move(before, p.mu);
        }
        if (countTeams) {
            // the contests a team ran: once per contest, whatever the number of rows it held in it (the entries are
            // sorted by team, so a team's rows are adjacent)
            for (int i = 0; i < n; i++) {
                if (i == 0 || !ids[i].equals(ids[i - 1])) state.teams.merge(ids[i], 1L, Long::sum);
            }
        }
    }

    /** 1 when entry i did better than entry j, 0.5 on equal outcomes, else 0. */
    private double score(final Entry i, final Entry j) {
        return score(i.outcome(), j.outcome());
    }

    /** 1 when outcome a is better than outcome b, 0.5 when they are equal, else 0. */
    private double score(final double a, final double b) {
        // numeric equality, not Double.compare: -0.0 and 0.0 are the same outcome (a tie), which compare denies
        if (a == b) return 0.5;
        return (Double.compare(a, b) < 0) == ascending ? 1d : 0d;
    }

    private void elo(final List<Entry> entries, final String[] ids, final double[] m, final double[] dMu) {
        final int n = entries.size();
        for (int i = 0; i < n; i++) {
            final Entry self = entries.get(i);
            double sum = 0;
            int opponents = 0;
            for (int j = 0; j < n; j++) {
                final Entry other = entries.get(j);
                if (ids[j].equals(ids[i])) continue;
                final double expected = 1d / (1d + Math.pow(10d, (m[j] - m[i]) / scale));
                sum += score(self, other) - expected;
                opponents++;
            }
            // kFactor is the change of a whole contest: shared over the opponents so it does not grow with the field
            if (opponents > 0) dMu[i] = kFactor * sum / opponents;
        }
    }

    private void bradleyTerry(final List<Entry> entries, final String[] ids, final double[] m, final double[] v, final double[] dMu, final double[] deltas) {
        final int n = entries.size();
        for (int i = 0; i < n; i++) {
            final Entry self = entries.get(i);
            // adjacent: the nearest better and the nearest worse outcome among the OPPONENTS (the player's own other
            // rows are no opponents), which with the player's own outcome name its rank neighbours — a set the
            // outcomes decide, not the order of the entries, so tied neighbours all count
            double better = Double.NaN, worse = Double.NaN;
            if (pairs == Pairs.adjacent) {
                for (int q = 0; q < n; q++) {
                    final Entry other = entries.get(q);
                    if (ids[q].equals(ids[i])) continue;
                    final double sc = score(other, self);
                    if (sc == 1d && (Double.isNaN(better) || score(other.outcome(), better) == 0d)) better = other.outcome();
                    if (sc == 0d && (Double.isNaN(worse) || score(other.outcome(), worse) == 1d)) worse = other.outcome();
                }
            }
            double omega = 0, delta = 0;
            int opponents = 0;
            for (int q = 0; q < n; q++) {
                final Entry other = entries.get(q);
                if (ids[q].equals(ids[i])) continue;
                if (pairs == Pairs.adjacent && other.outcome() != self.outcome() && other.outcome() != better && other.outcome() != worse) continue;
                final double c = Math.sqrt(v[i] + v[q] + 2 * beta * beta);
                final double p = 1d / (1d + Math.exp((m[q] - m[i]) / c));
                omega += v[i] / c * (score(self, other) - p);
                delta += Math.sqrt(v[i]) / c * (v[i] / (c * c)) * p * (1 - p);
                opponents++;
            }
            if (pairs == Pairs.mean && opponents > 0) {
                omega /= opponents;
                delta /= opponents;
            }
            dMu[i] = omega;
            deltas[i] = delta;
        }
    }

    private void plackettLuce(final List<Entry> entries, final double[] m, final double[] v, final double[] dMu, final double[] deltas) {
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
            final Entry place = entries.get(q);
            for (int s = 0; s < n; s++) {
                final double sc = score(entries.get(s), place);
                if (sc <= 0.5) remaining[q] += e[s];
                if (sc == 0.5) ties[q]++;
            }
        }
        for (int i = 0; i < n; i++) {
            final Entry self = entries.get(i);
            double omega = 0, delta = 0;
            for (int q = 0; q < n; q++) {
                // the places decided while i was still in the race: q's own and every better one
                if (q != i && score(entries.get(q), self) < 0.5) continue;
                final double quotient = e[i] / remaining[q];
                omega += (q == i ? 1 - quotient : -quotient) / ties[q];
                delta += quotient * (1 - quotient) / ties[q];
            }
            dMu[i] = v[i] / c * omega;
            deltas[i] = Math.sqrt(v[i]) / c * (v[i] / c2) * delta;
        }
    }

    /**
     * A readout of a player's rating as the state holds it ({@code state} may be null: nothing folded yet). A player
     * never rated reads the prior {@code mu} / {@code sigma}, count 0, a null {@code delta}, {@code deviation} 0 and
     * the {@code z} of its prior's place in the pool.
     *
     * <p>Under a drift in time ({@code tauPer}) this reads the uncertainty of the player's <b>last contest</b>, not the
     * one it carries now: every path that serves a row must pass the row's time
     * ({@link #read(State, String, String, long)}), as {@code SequenceEvaluator} does on the fold-pointer and the scan
     * path alike. This overload is the state's own view (tests, and a rating whose drift is per contest).
     */
    public Object read(final State state, final String player, final String func) {
        return read(state, player, func, Long.MIN_VALUE);
    }

    /**
     * A readout at {@code nowMillis}: under a drift in time {@code sigma} carries the drift since the player's last
     * contest ({@link #drifted}) — what is known of the player now, not what was known when it last competed.
     */
    public Object read(final State state, final String player, final String func, final long nowMillis) {
        return read(state, 0, player, func, nowMillis);
    }

    /**
     * A readout of the {@code member}-th member of a row's team ({@code key}: its state key, {@link #memberKey}); a
     * member never rated reads its own prior.
     */
    public Object read(final State state, final int member, final String key, final String func, final long nowMillis) {
        if (key == null) return null;
        final Member m = members.get(member);
        final Player p = state == null ? null : state.players.get(key);
        return switch (func) {
            case "mu" -> p == null ? m.mu() : p.mu;
            case "sigma" -> p == null ? m.sigma() : sigmaAt(m, p, nowMillis);
            case "count" -> p == null ? 0L : p.count;
            case "delta" -> p == null ? null : (Object) p.delta;
            case "deviation" -> p == null ? 0d : p.mu - m.mu();
            case "z" -> poolZ(state, m, p == null ? m.mu() : p.mu);
            default -> throw new IllegalArgumentException("unknown rating readout: " + func);
        };
    }

    /**
     * {@code mu} standardised within the member's pool: against the mean and sd of {@code mu} over the pool's rated
     * players (the prior of a player never rated is where it stands). Null until the pool holds two rated players
     * with different ratings — before that there is no scale to read against.
     */
    private static Double poolZ(final State state, final Member m, final double mu) {
        final Pool pool = state == null ? null : state.pools.get(m.pool() == null ? "" : m.pool());
        if (pool == null || pool.players < 2) return null;
        final double sd = pool.sd();
        return sd > 0 ? (mu - pool.mean()) / sd : null;
    }

    /**
     * A readout of the team a row is rated as ({@code team}: {@link #teamOf}, null = a member is missing → null): the
     * strength the contest will see — {@code mu} the sum of the members' ratings, {@code sigma} the uncertainty of that
     * sum, under a drift in time as of {@code nowMillis} — and beside it {@code count}, the contests this very team
     * has run (every member together; 0 before the first), and {@code deviation}, the sum net of the members' priors.
     * A member never rated counts with its prior, so a known player
     * in new company still reads a team. The sum is what the contests identify: the members' levels may shift against
     * each other over a long replay (every player up, every agent down changes no expectation), their sum does not.
     */
    public Object readTeam(final State state, final List<String> team, final String func, final long nowMillis) {
        if (team == null) return null;
        // func null first: TEAM_FUNCS is an immutable list, whose contains(null) throws
        if (func == null || !TEAM_FUNCS.contains(func)) {
            throw new IllegalArgumentException("unknown team readout: " + func + " (available: " + TEAM_FUNCS + ")");
        }
        if (team.size() != members.size()) {
            throw new IllegalArgumentException("a team of " + team.size() + " read from a rating of teams of " + members.size());
        }
        if ("count".equals(func)) {
            if (!countTeams) {
                throw new IllegalStateException("this rating keeps no contests per team: team: [count] needs withTeam(pool, with, true)"
                        + " (the coordinate teamCounts)");
            }
            return state == null ? 0L : state.teams.getOrDefault(teamId(team), 0L);
        }
        double sum = 0;
        for (int j = 0; j < members.size(); j++) {
            final Member m = members.get(j);
            final Player p = state == null ? null : state.players.get(team.get(j));
            switch (func) {
                case "mu" -> sum += p == null ? m.mu() : p.mu;
                case "deviation" -> sum += p == null ? 0d : p.mu - m.mu();
                default -> {
                    final double s = p == null ? m.sigma() : sigmaAt(m, p, nowMillis);
                    sum += s * s;
                }
            }
        }
        return "sigma".equals(func) ? Math.sqrt(sum) : sum;
    }

    /**
     * The state after a replay, one line per pool — for the run log, so the warm-up of a pool can be judged: how many
     * players were rated, the median of their contest counts, and the spread of {@code mu} the contests produced
     * (the scale a {@code z} readout standardises by). A pool of few contests per player is still near its prior.
     */
    public String describe(final State state) {
        final Map<String, List<Long>> counts = new TreeMap<>();
        for (final Map.Entry<String, Player> e : state.players.entrySet()) {
            final int at = members.get(0).pool() == null ? -1 : e.getKey().indexOf(POOL_SEPARATOR);
            counts.computeIfAbsent(at < 0 ? "" : e.getKey().substring(0, at), k -> new ArrayList<>()).add(e.getValue().count);
        }
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, List<Long>> e : counts.entrySet()) {
            final List<Long> sorted = e.getValue();
            sorted.sort(null);
            final Pool pool = state.pools.get(e.getKey());
            if (sb.length() > 0) sb.append("; ");
            sb.append("pool ").append(e.getKey().isEmpty() ? "<players>" : e.getKey())
                    .append(": players=").append(sorted.size())
                    .append(" contests/player median=").append(sorted.get(sorted.size() / 2))
                    .append(" max=").append(sorted.get(sorted.size() - 1));
            if (pool != null && pool.players > 0) {
                sb.append(String.format(java.util.Locale.ROOT, " mu mean=%.3f sd=%.3f", pool.mean(), pool.sd()));
            }
        }
        return sb.length() == 0 ? "no player rated" : sb.toString();
    }

    /**
     * The uncertainty a row at {@code nowMillis} reads: the state's, carrying the drift of the absence since the
     * player's last contest. {@code Long.MIN_VALUE} (a read without a time) is the state's own value, bit for bit.
     */
    private double sigmaAt(final Member member, final Player p, final long nowMillis) {
        if (tauPerMillis <= 0 || nowMillis == Long.MIN_VALUE) return p.sigma;
        return Math.sqrt(drifted(member, p, nowMillis, false));
    }

}
