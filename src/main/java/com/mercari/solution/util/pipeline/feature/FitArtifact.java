package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * Persisted fit result of a {@code fit.mode: static} encoding block (work-feature.md §7 train/serve 整合):
 * the per-key sufficient statistics of every lattice level, stored as one Avro file per block under a
 * content-addressed directory {@code <artifactUri>/<planHash>/}. The plan hash covers the spec and the
 * sources contract, so a change in either invalidates the artifact; a {@code manifest.json} next to the
 * file records what was fitted.
 *
 * <p>Workers cache loaded artifacts per path for the lifetime of the JVM. Because paths are
 * content-addressed (plan hash or {@code fit.artifact.id}), a changed spec always misses the cache; but
 * overwriting the SAME path (e.g. {@code refit: true} against a pinned {@code id}) is only picked up by a
 * fresh JVM — long-lived runners (server-launched runs, Flink / Spark sessions) keep serving the old
 * statistics until restarted.
 */
public final class FitArtifact {

    private static final Logger LOG = LoggerFactory.getLogger(FitArtifact.class);
    private static final char SEPARATOR = (char) 1;

    public static final Schema SCHEMA = new Schema.Parser().parse("""
            {"type": "record", "name": "FeatureFitStats", "namespace": "com.mercari.solution.feature",
             "fields": [
               {"name": "level", "type": "string"},
               {"name": "key", "type": "string"},
               {"name": "n", "type": "double"},
               {"name": "sum", "type": "double"},
               {"name": "sumSq", "type": "double"}
             ]}
            """);

    private FitArtifact() {}

    public static String directory(final String artifactUri, final String planHash) {
        final String base = artifactUri.endsWith("/") ? artifactUri.substring(0, artifactUri.length() - 1) : artifactUri;
        return base + "/" + planHash;
    }

    public static String statsPath(final String artifactUri, final String planHash, final String block) {
        return directory(artifactUri, planHash) + "/" + block + ".avro";
    }

    public static String manifestPath(final String artifactUri, final String planHash, final String block) {
        return directory(artifactUri, planHash) + "/" + block + ".manifest.json";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String block) {
        return ResourceUtil.exists(statsPath(artifactUri, planHash, block));
    }

    /** Composite map key used in memory and in side inputs: {@code level + (char) 1 + key}. */
    public static String entryKey(final String level, final String key) {
        return level + SEPARATOR + key;
    }

    static String levelOf(final String entryKey) {
        return entryKey.substring(0, entryKey.indexOf(SEPARATOR));
    }

    public static void write(final String artifactUri, final String planHash, final String block,
                             final Map<String, VarianceComponents.KeyStats> stats, final List<String> levels) {
        final String path = statsPath(artifactUri, planHash, block);
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
                writer.create(SCHEMA, bytes);
                final List<String> keys = new ArrayList<>(stats.keySet());
                Collections.sort(keys);
                for (final String entry : keys) {
                    final VarianceComponents.KeyStats s = stats.get(entry);
                    final int sep = entry.indexOf(SEPARATOR);
                    final GenericRecord record = new GenericData.Record(SCHEMA);
                    record.put("level", entry.substring(0, sep));
                    record.put("key", entry.substring(sep + 1));
                    record.put("n", s.n);
                    record.put("sum", s.sum);
                    record.put("sumSq", s.sumSq);
                    writer.append(record);
                }
            }
            ResourceUtil.writeBytes(path, bytes.toByteArray());
            final JsonObject manifest = new JsonObject();
            manifest.addProperty("planHash", planHash);
            manifest.addProperty("block", block);
            manifest.addProperty("entries", stats.size());
            manifest.addProperty("createdAt", Instant.now().toString());
            final JsonArray levelArray = new JsonArray();
            levels.forEach(levelArray::add);
            manifest.add("levels", levelArray);
            ResourceUtil.writeString(manifestPath(artifactUri, planHash, block), manifest.toString());
            LOG.info("wrote fit artifact {} ({} entries)", path, stats.size());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write fit artifact: " + path, e);
        }
    }

    public static Map<String, VarianceComponents.KeyStats> read(final String artifactUri, final String planHash, final String block) {
        final String path = statsPath(artifactUri, planHash, block);
        final Map<String, VarianceComponents.KeyStats> stats = new HashMap<>();
        try (final DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(ResourceUtil.readBytes(path)), new GenericDatumReader<>(SCHEMA))) {
            while (reader.hasNext()) {
                final GenericRecord record = reader.next();
                final VarianceComponents.KeyStats s = new VarianceComponents.KeyStats();
                s.n = (Double) record.get("n");
                s.sum = (Double) record.get("sum");
                s.sumSq = (Double) record.get("sumSq");
                stats.put(entryKey(record.get("level").toString(), record.get("key").toString()), s);
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read fit artifact: " + path, e);
        }
        LOG.info("loaded fit artifact {} ({} entries)", path, stats.size());
        return stats;
    }

}
