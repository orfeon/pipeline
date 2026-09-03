package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Fitted discretization of a numeric field (docs/design/feature-dsl.md §4.4, {@code type: discretize}): bin edges
 * learned from the whole input in a static fit and applied by lookup, so the bins can key an encoding.
 *
 * <p>Bin numbering: {@code -1} = missing (null / NaN), {@code 0} = below the fitted minimum,
 * {@code 1..B} = the fitted bins (B = interior edges + 1, at most {@code bins}), {@code B + 1} = above
 * the fitted maximum. The out-of-range and missing bins are dedicated as the spec requires, so a serving
 * value the fit never saw is a category of its own (and a drift signal) rather than a silent edge bin.
 *
 * <p>{@code method: quantile}: the interior edges are the type-7 quantiles {@code i / B} of the fitted
 * values, {@code B = min(bins (default 10), n / minSamplesPerBin)}; duplicate edges (ties) and edges at the
 * extremes are dropped, so B can be smaller than requested on discrete data. Values equal to an edge fall
 * in the upper bin ({@code edge <= v < next}). A fit that saw no value (n = 0) maps every non-missing
 * value to bin 1 — the artifact is still written so a later run can load it.
 */
public final class Discretization implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Discretization.class);

    public final String method;
    /** Strictly increasing interior edges. */
    public final double[] edges;
    public final double min;
    public final double max;
    public final long n;

    Discretization(final String method, final double[] edges, final double min, final double max, final long n) {
        this.method = method;
        this.edges = edges;
        this.min = min;
        this.max = max;
        this.n = n;
    }

    /** Number of fitted bins (B). */
    public int bins() {
        return edges.length + 1;
    }

    public Long bin(final Double v) {
        if (v == null || v.isNaN()) return -1L;
        if (n == 0) return 1L;
        if (v < min) return 0L;
        if (v > max) return (long) bins() + 1;
        // first edge > v: the number of edges <= v is the 0-based bin
        int lo = 0, hi = edges.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (edges[mid] <= v) lo = mid + 1;
            else hi = mid;
        }
        return lo + 1L;
    }

    /** Quantile edges of the first {@code count} values (the input is not modified: Beam forbids mutating DoFn inputs). */
    public static Discretization fitQuantile(final double[] input, final int count, final Integer bins, final Integer minSamplesPerBin) {
        if (count == 0) {
            LOG.warn("discretize: no non-null values to fit; every value maps to bin 1");
            return new Discretization("quantile", new double[0], Double.NaN, Double.NaN, 0);
        }
        final double[] values = Arrays.copyOf(input, count);
        Arrays.sort(values);
        // B = min(bins (default 10), n / minSamplesPerBin): the default caps a sample-driven bin count too
        int b = bins == null ? 10 : bins;
        if (minSamplesPerBin != null) b = Math.min(b, Math.max(1, count / minSamplesPerBin));
        final double min = values[0];
        final double max = values[count - 1];
        final double[] candidates = new double[Math.max(0, b - 1)];
        int m = 0;
        for (int i = 1; i < b; i++) {
            final double edge = OrderStatistics.quantile((double) i / b, values, count);
            // drop ties and edges at the extremes: they would create empty bins
            if (edge <= min || edge >= max) continue;
            if (m > 0 && edge <= candidates[m - 1]) continue;
            candidates[m++] = edge;
        }
        final Discretization d = new Discretization("quantile", Arrays.copyOf(candidates, m), min, max, count);
        if (d.bins() < b) LOG.info("discretize: {} bins requested, {} distinct edges fitted (ties / extremes dropped)", b, d.bins());
        return d;
    }

    // --- artifact ------------------------------------------------------------------------------------

    public static String artifactPath(final String artifactUri, final String planHash, final String block) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + block + ".bins.json";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String block) {
        return ResourceUtil.exists(artifactPath(artifactUri, planHash, block));
    }

    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("method", method);
        final JsonArray array = new JsonArray();
        for (final double e : edges) array.add(e);
        json.add("edges", array);
        json.addProperty("bins", bins());
        json.addProperty("min", min);
        json.addProperty("max", max);
        json.addProperty("n", n);
        return json;
    }

    public static Discretization fromJson(final JsonObject json) {
        final JsonArray array = json.getAsJsonArray("edges");
        final double[] edges = new double[array.size()];
        for (int i = 0; i < edges.length; i++) edges[i] = array.get(i).getAsDouble();
        // n decides whether the edges apply at all (n = 0 maps everything to bin 1): never default it
        final JsonElement n = json.get("n");
        if (n == null || !n.isJsonPrimitive()) throw new IllegalStateException("discretization artifact lacks 'n': " + json);
        return new Discretization(json.get("method").getAsString(), edges,
                json.get("min").getAsDouble(), json.get("max").getAsDouble(), n.getAsLong());
    }

    public static void write(final String artifactUri, final String planHash, final String block, final Discretization d) {
        final String path = artifactPath(artifactUri, planHash, block);
        final JsonObject json = FitArtifact.manifest(planHash, block);
        for (final Map.Entry<String, JsonElement> e : d.toJson().entrySet()) json.add(e.getKey(), e.getValue());
        ResourceUtil.writeString(path, json.toString());
        LOG.info("wrote discretization artifact {} ({} bins)", path, d.bins());
    }

    public static Discretization read(final String artifactUri, final String planHash, final String block) {
        final String path = artifactPath(artifactUri, planHash, block);
        final Discretization d = fromJson(JsonParser.parseString(ResourceUtil.readString(path)).getAsJsonObject());
        LOG.info("loaded discretization artifact {} ({} bins)", path, d.bins());
        return d;
    }

}
