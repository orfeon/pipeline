package com.mercari.solution.util.pipeline.feature;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The state of a rating pool after a replay, persisted like a fit artifact ({@code fit.artifact} on the rating's
 * sequence block): {@code <uri>/<planHash>/<block>.rating/<stateKey>.<key hash>.json}, one file per rating state and
 * stage key (a pool). A run whose input starts after the snapshot's last folded contest starts every pool from it and
 * folds only the contests after {@link Rating.State#foldedUntilMillis} — the serving form of a rating, whose full
 * replay is otherwise one thread over the whole history. A run whose input <b>reaches back</b> to or before that time
 * (a full-history backfill, a retried attempt of the key that wrote the file, a run over an overlapping range) replays
 * from scratch and rewrites the snapshot: what a full replay produces is the snapshot to continue from, and reading a
 * state that already holds the contests about to be served would leak. That rule needs no mark of the run that wrote
 * a file: a retry sees the same first row its earlier attempt saw. {@code refit: true} replays from scratch whatever
 * the input; {@code require: true} fails a pool whose snapshot is missing (a serving run must never fall back to a
 * replay of its short input from the prior). The plan hash strips {@code fit.artifact}, so the training and the serving
 * config share the directory.
 *
 * <p>Streamed JSON (one {@link JsonWriter} / {@link JsonReader} pass over the players): a pool of millions of players
 * is written and read without a tree or a string of the whole state on the heap.
 */
public final class RatingSnapshot {

    private static final Logger LOG = LoggerFactory.getLogger(RatingSnapshot.class);

    private RatingSnapshot() {}

    /**
     * One rating state to snapshot: its state key (one running state per rating op), block, artifact location, whether
     * to replay afresh whatever the input, and whether a missing snapshot fails the pool instead of replaying it.
     */
    public record Spec(String stateKey, String block, String uri, String version, boolean refit, boolean required) implements Serializable {}

    /** The snapshot specs of a stage's columns: one per rating state whose coordinates name an artifact. */
    public static List<Spec> specsOf(final List<OutputColumn> columns, final String version) {
        final List<Spec> specs = new ArrayList<>();
        final Set<String> seen = new HashSet<>();
        for (final OutputColumn c : columns) {
            if (!"rating".equals(c.getOperator())) continue;
            final String uri = c.getCoordinates().get("artifact");
            final String stateKey = c.getCoordinates().get("stateKey");
            if (uri == null || stateKey == null || !seen.add(stateKey)) continue;
            specs.add(new Spec(stateKey, c.getBlock(), uri, version,
                    "true".equals(c.getCoordinates().get("artifactRefit")), "true".equals(c.getCoordinates().get("artifactRequired"))));
        }
        return specs;
    }

    public static String directory(final String uri, final String version, final String block) {
        return FitArtifact.directory(uri, version) + "/" + block + ".rating";
    }

    /** The file of a stage key: the key text carries separators and length prefixes, so its hash names the file. */
    public static String path(final Spec spec, final String key) {
        return directory(spec.uri(), spec.version(), spec.block()) + "/" + spec.stateKey() + "." + FeaturePlanCompiler.sha256(key) + ".json";
    }

    public static boolean exists(final Spec spec, final String key) {
        return ResourceUtil.exists(path(spec, key));
    }

    /** Writes the key's state (overwriting an earlier snapshot of the pool). */
    public static void write(final Spec spec, final String key, final Rating.State state) {
        final String path = path(spec, key);
        try {
            final ResourceId resource = FileSystems.matchNewResource(path, false);
            try (JsonWriter out = new JsonWriter(new OutputStreamWriter(Channels.newOutputStream(FileSystems.create(resource, MimeTypes.TEXT)), StandardCharsets.UTF_8))) {
                out.beginObject();
                out.name("planHash").value(spec.version());
                out.name("block").value(spec.block());
                out.name("createdAt").value(Instant.now().toString());
                out.name("stateKey").value(spec.stateKey());
                out.name("key").value(key);
                out.name("state").beginObject();
                out.name("foldedUntilMillis").value(state.foldedUntilMillis);
                out.name("players").beginObject();
                for (final java.util.Map.Entry<String, Rating.Player> e : state.players.entrySet()) {
                    final Rating.Player p = e.getValue();
                    out.name(e.getKey()).beginObject();
                    out.name("mu").value(p.mu);
                    out.name("sigma").value(p.sigma);
                    out.name("count").value(p.count);
                    out.name("delta").value(p.delta);
                    out.name("lastMillis").value(p.lastMillis);
                    out.endObject();
                }
                out.endObject();
                out.name("teams").beginObject();
                for (final java.util.Map.Entry<String, Long> e : state.teams.entrySet()) out.name(e.getKey()).value(e.getValue());
                out.endObject();
                out.name("pools").beginObject();
                for (final java.util.Map.Entry<String, Rating.Pool> e : state.pools.entrySet()) {
                    final Rating.Pool pool = e.getValue();
                    out.name(e.getKey()).beginObject();
                    out.name("players").value(pool.players);
                    out.name("shift").value(pool.shift);
                    out.name("sum").value(pool.sum);
                    out.name("sumSq").value(pool.sumSq);
                    out.endObject();
                }
                out.endObject();
                out.endObject();
                out.endObject();
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to write the rating snapshot " + path, e);
        }
        LOG.info("wrote rating snapshot {} ({} players, contests folded until {})", path, state.players.size(), state.foldedUntilMillis);
    }

    /** The key's snapshot, or null when there is none. */
    public static Rating.State read(final Spec spec, final String key) {
        final String path = path(spec, key);
        if (!ResourceUtil.exists(path)) return null;
        final Rating.State state = new Rating.State();
        boolean found = false;
        try {
            final ResourceId resource = FileSystems.matchSingleFileSpec(path).resourceId();
            try (JsonReader in = new JsonReader(new InputStreamReader(Channels.newInputStream(FileSystems.open(resource)), StandardCharsets.UTF_8))) {
                in.beginObject();
                while (in.hasNext()) {
                    if ("state".equals(in.nextName())) {
                        readState(in, state);
                        found = true;
                    } else {
                        in.skipValue();
                    }
                }
                in.endObject();
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to read the rating snapshot " + path, e);
        }
        if (!found) throw new IllegalStateException("rating snapshot " + path + " holds no state");
        LOG.info("loaded rating snapshot {} ({} players, contests folded until {})", path, state.players.size(), state.foldedUntilMillis);
        return state;
    }

    private static void readState(final JsonReader in, final Rating.State state) throws IOException {
        in.beginObject();
        while (in.hasNext()) {
            switch (in.nextName()) {
                case "foldedUntilMillis" -> state.foldedUntilMillis = in.nextLong();
                case "players" -> {
                    in.beginObject();
                    while (in.hasNext()) {
                        final String player = in.nextName();
                        final Rating.Player p = new Rating.Player();
                        in.beginObject();
                        while (in.hasNext()) {
                            switch (in.nextName()) {
                                case "mu" -> p.mu = in.nextDouble();
                                case "sigma" -> p.sigma = in.nextDouble();
                                case "count" -> p.count = in.nextLong();
                                case "delta" -> p.delta = in.nextDouble();
                                case "lastMillis" -> p.lastMillis = in.nextLong();
                                default -> in.skipValue();
                            }
                        }
                        in.endObject();
                        state.players.put(player, p);
                    }
                    in.endObject();
                }
                case "teams" -> {
                    in.beginObject();
                    while (in.hasNext()) state.teams.put(in.nextName(), in.nextLong());
                    in.endObject();
                }
                case "pools" -> {
                    in.beginObject();
                    while (in.hasNext()) {
                        final String name = in.nextName();
                        final Rating.Pool pool = new Rating.Pool();
                        in.beginObject();
                        while (in.hasNext()) {
                            switch (in.nextName()) {
                                case "players" -> pool.players = in.nextLong();
                                case "shift" -> pool.shift = in.nextDouble();
                                case "sum" -> pool.sum = in.nextDouble();
                                case "sumSq" -> pool.sumSq = in.nextDouble();
                                default -> in.skipValue();
                            }
                        }
                        in.endObject();
                        state.pools.put(name, pool);
                    }
                    in.endObject();
                }
                default -> {
                    if (in.peek() == JsonToken.NULL) in.nextNull(); else in.skipValue();
                }
            }
        }
        in.endObject();
    }
}
