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
import java.io.Serializable;
import java.util.*;

/**
 * Factorization machine over categorical fields (docs/design/feature-dsl.md §4.4):
 *
 * <pre>
 *   ŷ = w0 + Σ_f w_f[x_f] + Σ_{f&lt;g} r_fg · ⟨v_f[x_f], v_g[x_g]⟩
 * </pre>
 *
 * {@code fm}: r ≡ 1; {@code fwfm}: one scalar r per field pair (its fitted values are a field-pair
 * importance ranking, exported with the artifact manifest). Training is alternating least squares: the
 * model is linear in every single parameter, so each parameter has a closed-form ridge update; residuals
 * are cached so one sweep over all parameters costs O(N · F · k). The fit is deterministic for a seed.
 */
public final class Factorization {

    private static final Logger LOG = LoggerFactory.getLogger(Factorization.class);
    private static final char SEPARATOR = (char) 1;

    private Factorization() {}

    /** One training example: the field values (null = missing) and the (offset-adjusted) target. */
    public record Example(String[] values, double y) implements Serializable {}

    public static final class Model implements Serializable {
        public final List<String> fields;
        public final boolean fieldWeighted;
        public final int k;
        public double w0;
        /** field index → value → [w, v_1 .. v_k] */
        public final List<Map<String, double[]>> params;
        /** pair weights r[f][g] (symmetric; 1 for fm) */
        public final double[][] r;

        Model(final List<String> fields, final boolean fieldWeighted, final int k) {
            this.fields = fields;
            this.fieldWeighted = fieldWeighted;
            this.k = k;
            this.params = new ArrayList<>();
            for (int f = 0; f < fields.size(); f++) params.add(new HashMap<>());
            this.r = new double[fields.size()][fields.size()];
            for (final double[] row : r) Arrays.fill(row, 1d);
        }

        double[] param(final int f, final String value) {
            return value == null ? null : params.get(f).get(value);
        }

        /** ⟨v_f[x_f], v_g[x_g]⟩ scaled by r_fg, or null when either value is unknown. */
        public Double pair(final String[] values, final int f, final int g) {
            final double[] a = param(f, values[f]);
            final double[] b = param(g, values[g]);
            if (a == null || b == null) return null;
            double dot = 0;
            for (int d = 1; d <= k; d++) dot += a[d] * b[d];
            return r[f][g] * dot;
        }

        public double[] embedding(final String[] values, final int f, final int dims) {
            final double[] p = param(f, values[f]);
            if (p == null) return null;
            return Arrays.copyOfRange(p, 1, 1 + Math.min(dims, k));
        }

        /** Full linear predictor (without the offset). */
        public double predict(final String[] values) {
            double y = w0;
            for (int f = 0; f < fields.size(); f++) {
                final double[] p = param(f, values[f]);
                if (p != null) y += p[0];
            }
            for (int f = 0; f < fields.size(); f++) {
                for (int g = f + 1; g < fields.size(); g++) {
                    final Double pair = pair(values, f, g);
                    if (pair != null) y += pair;
                }
            }
            return y;
        }

        public int size() {
            int n = 0;
            for (final Map<String, double[]> m : params) n += m.size();
            return n;
        }
    }

    public record Options(List<String> fields, boolean fieldWeighted, int latentDim, int epochs, double reg, long seed) implements Serializable {}

    public static Model fit(final Options options, final List<Example> examples) {
        final int F = options.fields().size();
        final int k = options.latentDim();
        final Model model = new Model(options.fields(), options.fieldWeighted(), k);
        final Random random = new Random(options.seed());
        // parameters initialised with small random factors; index the examples by (field, value)
        final List<Map<String, List<Integer>>> index = new ArrayList<>();
        for (int f = 0; f < F; f++) index.add(new HashMap<>());
        for (int i = 0; i < examples.size(); i++) {
            final String[] x = examples.get(i).values();
            for (int f = 0; f < F; f++) {
                if (x[f] == null) continue;
                index.get(f).computeIfAbsent(x[f], v -> new ArrayList<>()).add(i);
                model.params.get(f).computeIfAbsent(x[f], v -> {
                    final double[] p = new double[k + 1];
                    for (int d = 1; d <= k; d++) p[d] = random.nextGaussian() * 0.1;
                    return p;
                });
            }
        }
        final double[] e = new double[examples.size()];
        model.w0 = 0;
        for (int i = 0; i < examples.size(); i++) e[i] = examples.get(i).y() - model.predict(examples.get(i).values());
        final double reg = options.reg();

        for (int epoch = 0; epoch < options.epochs(); epoch++) {
            // global bias
            {
                double num = 0;
                for (int i = 0; i < e.length; i++) num += e[i] + model.w0;
                final double w0 = num / (e.length + reg);
                final double delta = w0 - model.w0;
                for (int i = 0; i < e.length; i++) e[i] -= delta;
                model.w0 = w0;
            }
            // linear weights
            for (int f = 0; f < F; f++) {
                for (final Map.Entry<String, List<Integer>> entry : index.get(f).entrySet()) {
                    final double[] p = model.params.get(f).get(entry.getKey());
                    double num = 0;
                    for (final int i : entry.getValue()) num += e[i] + p[0];
                    final double w = num / (entry.getValue().size() + reg);
                    final double delta = w - p[0];
                    for (final int i : entry.getValue()) e[i] -= delta;
                    p[0] = w;
                }
            }
            // factors: ŷ is linear in v_f[x][d] with slope h = Σ_{g≠f} r_fg v_g[x_g][d]
            for (int f = 0; f < F; f++) {
                for (final Map.Entry<String, List<Integer>> entry : index.get(f).entrySet()) {
                    final double[] p = model.params.get(f).get(entry.getKey());
                    for (int d = 1; d <= k; d++) {
                        double num = 0, den = reg;
                        final double[] h = new double[entry.getValue().size()];
                        int j = 0;
                        for (final int i : entry.getValue()) {
                            h[j] = slope(model, examples.get(i).values(), f, d);
                            num += h[j] * (e[i] + p[d] * h[j]);
                            den += h[j] * h[j];
                            j++;
                        }
                        final double v = den == 0 ? 0 : num / den;
                        final double delta = v - p[d];
                        j = 0;
                        for (final int i : entry.getValue()) e[i] -= delta * h[j++];
                        p[d] = v;
                    }
                }
            }
            // field-pair weights (fwfm): ŷ is linear in r_fg with slope ⟨v_f, v_g⟩
            if (options.fieldWeighted()) {
                for (int f = 0; f < F; f++) {
                    for (int g = f + 1; g < F; g++) {
                        double num = 0, den = reg;
                        final double[] dots = new double[examples.size()];
                        for (int i = 0; i < examples.size(); i++) {
                            final Double pair = model.pair(examples.get(i).values(), f, g);
                            final double dot = pair == null ? 0 : pair / model.r[f][g];
                            dots[i] = dot;
                            num += dot * (e[i] + model.r[f][g] * dot);
                            den += dot * dot;
                        }
                        final double rNew = den == 0 ? model.r[f][g] : num / den;
                        final double delta = rNew - model.r[f][g];
                        for (int i = 0; i < examples.size(); i++) e[i] -= delta * dots[i];
                        model.r[f][g] = rNew;
                        model.r[g][f] = rNew;
                    }
                }
            }
            double sse = 0;
            for (final double v : e) sse += v * v;
            LOG.info("factorization epoch {}: rmse={}", epoch + 1, Math.sqrt(sse / Math.max(1, e.length)));
        }
        return model;
    }

    private static double slope(final Model model, final String[] x, final int f, final int d) {
        double h = 0;
        for (int g = 0; g < model.fields.size(); g++) {
            if (g == f) continue;
            final double[] q = model.param(g, x[g]);
            if (q != null) h += model.r[f][g] * q[d];
        }
        return h;
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static final Schema SCHEMA = new Schema.Parser().parse("""
            {"type": "record", "name": "FeatureFmParam", "namespace": "com.mercari.solution.feature",
             "fields": [
               {"name": "field", "type": "string"},
               {"name": "value", "type": "string"},
               {"name": "w", "type": "double"},
               {"name": "v", "type": "bytes"}
             ]}
            """);

    public static String artifactPath(final String artifactUri, final String planHash, final String block) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + block + ".fm.avro";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String block) {
        return ResourceUtil.exists(artifactPath(artifactUri, planHash, block));
    }

    public static void write(final String artifactUri, final String planHash, final String block, final Model model) {
        final String path = artifactPath(artifactUri, planHash, block);
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
                writer.create(SCHEMA, bytes);
                writer.append(record("__global", "", model.w0, new double[0]));
                for (int f = 0; f < model.fields.size(); f++) {
                    for (int g = f + 1; g < model.fields.size(); g++) {
                        writer.append(record("__r", model.fields.get(f) + SEPARATOR + model.fields.get(g), model.r[f][g], new double[0]));
                    }
                }
                for (int f = 0; f < model.fields.size(); f++) {
                    final List<String> values = new ArrayList<>(model.params.get(f).keySet());
                    Collections.sort(values);
                    for (final String value : values) {
                        final double[] p = model.params.get(f).get(value);
                        writer.append(record(model.fields.get(f), value, p[0], Arrays.copyOfRange(p, 1, p.length)));
                    }
                }
            }
            ResourceUtil.writeBytes(path, bytes.toByteArray());
            final JsonObject manifest = FitArtifact.manifest(planHash, block);
            manifest.addProperty("variant", model.fieldWeighted ? "fwfm" : "fm");
            manifest.addProperty("latentDim", model.k);
            manifest.addProperty("parameters", model.size());
            final JsonArray fields = new JsonArray();
            model.fields.forEach(fields::add);
            manifest.add("fields", fields);
            // fwfm: the field-pair weights rank the interactions (§4.4, lineage feedback for the generative DSL)
            final JsonArray pairs = new JsonArray();
            for (int f = 0; f < model.fields.size(); f++) {
                for (int g = f + 1; g < model.fields.size(); g++) {
                    final JsonObject pair = new JsonObject();
                    pair.addProperty("fields", model.fields.get(f) + "," + model.fields.get(g));
                    pair.addProperty("r", model.r[f][g]);
                    pairs.add(pair);
                }
            }
            manifest.add("pairWeights", pairs);
            ResourceUtil.writeString(FitArtifact.directory(artifactUri, planHash) + "/" + block + ".fm.manifest.json", manifest.toString());
            LOG.info("wrote factorization artifact {} ({} parameters)", path, model.size());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write factorization artifact: " + path, e);
        }
    }

    private static GenericRecord record(final String field, final String value, final double w, final double[] v) {
        final GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("field", field);
        record.put("value", value);
        record.put("w", w);
        // latent vector as big-endian doubles: array<double> round-trips with float precision on this Avro version
        final java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(8 * v.length);
        for (final double d : v) buffer.putDouble(d);
        buffer.flip();
        record.put("v", buffer);
        return record;
    }

    public static Model read(final String artifactUri, final String planHash, final String block,
                             final List<String> fields, final boolean fieldWeighted, final int k) {
        final String path = artifactPath(artifactUri, planHash, block);
        final Model model = new Model(fields, fieldWeighted, k);
        try (final DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(ResourceUtil.readBytes(path)), new GenericDatumReader<>(SCHEMA))) {
            while (reader.hasNext()) {
                final GenericRecord record = reader.next();
                final String field = record.get("field").toString();
                final String value = record.get("value").toString();
                final double w = (Double) record.get("w");
                if ("__global".equals(field)) {
                    model.w0 = w;
                } else if ("__r".equals(field)) {
                    final int sep = value.indexOf(SEPARATOR);
                    final int f = fields.indexOf(value.substring(0, sep));
                    final int g = fields.indexOf(value.substring(sep + 1));
                    if (f >= 0 && g >= 0) {
                        model.r[f][g] = w;
                        model.r[g][f] = w;
                    }
                } else {
                    final int f = fields.indexOf(field);
                    if (f < 0) continue;
                    final java.nio.ByteBuffer v = (java.nio.ByteBuffer) record.get("v");
                    final double[] p = new double[k + 1];
                    p[0] = w;
                    for (int d = 0; d < k && v.remaining() >= 8; d++) p[d + 1] = v.getDouble();
                    model.params.get(f).put(value, p);
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read factorization artifact: " + path, e);
        }
        LOG.info("loaded factorization artifact {} ({} parameters)", path, model.size());
        return model;
    }

}
