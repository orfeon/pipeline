package com.mercari.solution.util.pipeline.feature;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The state of a rating pool after a replay, persisted like a fit artifact ({@code fit.artifact} on the rating's
 * sequence block, or the top-level one): {@code <uri>/<planHash>/<block>.rating/<stateKey>.<key hash>.json}, one file
 * per rating state and stage key (a pool). A run that finds the snapshot ({@code refit: false}) starts every pool from
 * it and folds only the contests after {@link Rating.State#foldedUntilMillis} — the serving form of a rating, whose
 * full replay is otherwise one thread over the whole history; a run that finds none (or {@code refit: true}) replays
 * from scratch and writes it. The plan hash strips {@code fit.artifact}, so the training and the serving config share
 * the directory. A JSON file, because the state is a map of small records and a snapshot is read once per pool.
 */
public final class RatingSnapshot {

    private static final Logger LOG = LoggerFactory.getLogger(RatingSnapshot.class);
    private static final Gson GSON = new Gson();

    private RatingSnapshot() {}

    /** One rating state to snapshot: its state key (one running state per rating op), block, artifact location and whether to replay afresh. */
    public record Spec(String stateKey, String block, String uri, String version, boolean refit) implements Serializable {}

    /** The snapshot specs of a stage's columns: one per rating state whose coordinates name an artifact. */
    public static List<Spec> specsOf(final List<OutputColumn> columns, final String version) {
        final List<Spec> specs = new ArrayList<>();
        final Set<String> seen = new HashSet<>();
        for (final OutputColumn c : columns) {
            if (!"rating".equals(c.getOperator())) continue;
            final String uri = c.getCoordinates().get("artifact");
            final String stateKey = c.getCoordinates().get("stateKey");
            if (uri == null || stateKey == null || !seen.add(stateKey)) continue;
            specs.add(new Spec(stateKey, c.getBlock(), uri, version, "true".equals(c.getCoordinates().get("artifactRefit"))));
        }
        return specs;
    }

    public static String directory(final String uri, final String version, final String block) {
        return FitArtifact.directory(uri, version) + "/" + block + ".rating";
    }

    public static String path(final Spec spec, final String key) {
        return directory(spec.uri(), spec.version(), spec.block()) + "/" + spec.stateKey() + "." + hash(key) + ".json";
    }

    /** A file name for a stage key: the key text carries separators and length prefixes, so its hash names the file. */
    static String hash(final String key) {
        try {
            final byte[] digest = MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 16; i++) sb.append(String.format("%02x", digest[i]));
            return sb.toString();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean exists(final Spec spec, final String key) {
        return ResourceUtil.exists(path(spec, key));
    }

    public static void write(final Spec spec, final String key, final Rating.State state) {
        final String path = path(spec, key);
        final JsonObject json = FitArtifact.manifest(spec.version(), spec.block());
        json.addProperty("stateKey", spec.stateKey());
        json.addProperty("key", key);
        json.add("state", GSON.toJsonTree(state));
        ResourceUtil.writeString(path, json.toString());
        LOG.info("wrote rating snapshot {} ({} players, contests folded until {})", path, state.players.size(), state.foldedUntilMillis);
    }

    public static Rating.State read(final Spec spec, final String key) {
        final String path = path(spec, key);
        final JsonObject json = JsonParser.parseString(ResourceUtil.readString(path)).getAsJsonObject();
        final Rating.State state = GSON.fromJson(json.get("state"), Rating.State.class);
        state.rowsBeforeSnapshot = 0;
        LOG.info("loaded rating snapshot {} ({} players, contests folded until {}): the replay continues after it", path, state.players.size(), state.foldedUntilMillis);
        return state;
    }
}
