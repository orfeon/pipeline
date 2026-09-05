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
 * Fitted quantile transform of a numeric field (docs/design/feature-dsl.md §4.4, {@code type: quantileTransform}):
 * the empirical distribution of the whole input is summarised by {@code bins + 1} knots — the type-7 quantiles at
 * {@code 0, 1/B, ..., 1} — and a value is mapped to its position in that distribution, {@code F(v) ∈ [0, 1]}
 * (linear interpolation between knots; {@code distribution: normal} applies the probit Φ⁻¹ on top, so the output
 * is a normal score). Rank-based normalisation: robust to outliers and to the scale of the field, monotone, and
 * the same map at training and serving time (the knots are the artifact).
 *
 * <p>Out-of-range behaviour: a value at or below the fitted minimum maps to 0, at or above the maximum to 1
 * (a normal score is clamped at {@code ±Φ⁻¹(1 − 1e-6)}); a value equal to a run of tied knots (a mass point of
 * the distribution) maps to the middle of the run's probability range, so ties do not depend on the search
 * direction. Missing (null / NaN) maps to null. A fit that saw no value (n = 0) maps everything to null and is
 * still written as an artifact.
 */
public final class QuantileTransform implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(QuantileTransform.class);

    public static final int DEFAULT_BINS = 100;
    public static final String UNIFORM = "uniform";
    public static final String NORMAL = "normal";
    /** Probability clamp of the normal score (the transform never returns ±∞). */
    static final double NORMAL_EPSILON = 1e-6;

    /** Nondecreasing knots: the quantiles at i / bins, i = 0..bins (length bins + 1); empty when n = 0. */
    public final double[] knots;
    public final long n;
    public final String distribution;

    QuantileTransform(final double[] knots, final long n, final String distribution) {
        this.knots = knots;
        this.n = n;
        this.distribution = distribution;
    }

    /** Number of intervals between knots (B). */
    public int bins() {
        return Math.max(0, knots.length - 1);
    }

    /** Fits the knots on the first {@code count} values (the input is not modified: Beam forbids mutating DoFn inputs). */
    public static QuantileTransform fit(final double[] input, final int count, final int bins, final String distribution) {
        if (count == 0) {
            LOG.warn("quantileTransform: no non-null values to fit; every value maps to null");
            return new QuantileTransform(new double[0], 0, distribution);
        }
        final double[] values = Arrays.copyOf(input, count);
        Arrays.sort(values);
        final double[] knots = new double[bins + 1];
        for (int i = 0; i <= bins; i++) knots[i] = OrderStatistics.quantile((double) i / bins, values, count);
        return new QuantileTransform(knots, count, distribution);
    }

    /** The empirical CDF position of a value (or its normal score); null for missing values and an empty fit. */
    public Double transform(final Double v) {
        if (v == null || v.isNaN() || n == 0) return null;
        final double p = position(v);
        return NORMAL.equals(distribution) ? probit(Math.min(1 - NORMAL_EPSILON, Math.max(NORMAL_EPSILON, p))) : p;
    }

    /** F(v) in [0, 1] by linear interpolation between knots; a tied run of knots maps to the middle of its range. */
    double position(final double v) {
        final int bins = bins();
        if (v <= knots[0]) return 0d;
        if (v >= knots[bins]) return 1d;
        // first knot >= v and last knot <= v
        int lo = 0, hi = bins;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (knots[mid] < v) lo = mid + 1;
            else hi = mid;
        }
        final int first = lo;
        if (knots[first] == v) {
            int last = first;
            while (last + 1 <= bins && knots[last + 1] == v) last++;
            return (first + last) / (2d * bins);
        }
        // knots[first - 1] < v < knots[first]
        final int i = first - 1;
        return (i + (v - knots[i]) / (knots[i + 1] - knots[i])) / bins;
    }

    /** Inverse of the standard normal CDF (Acklam's rational approximation, relative error below 1.2e-9). Defined on (0, 1). */
    static double probit(final double p) {
        final double[] a = {-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00};
        final double[] b = {-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01};
        final double[] c = {-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00};
        final double[] d = {7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00};
        final double low = 0.02425, high = 1 - low;
        double x;
        if (p < low) {
            final double q = Math.sqrt(-2 * Math.log(p));
            x = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
        } else if (p <= high) {
            final double q = p - 0.5, r = q * q;
            x = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
        } else {
            final double q = Math.sqrt(-2 * Math.log(1 - p));
            x = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
        }
        return x;
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static String artifactPath(final String artifactUri, final String planHash, final String block) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + block + ".quantiles.json";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String block) {
        return ResourceUtil.exists(artifactPath(artifactUri, planHash, block));
    }

    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        final JsonArray array = new JsonArray();
        for (final double k : knots) array.add(k);
        json.add("knots", array);
        json.addProperty("bins", bins());
        json.addProperty("n", n);
        json.addProperty("distribution", distribution);
        return json;
    }

    public static QuantileTransform fromJson(final JsonObject json) {
        final JsonArray array = json.getAsJsonArray("knots");
        final double[] knots = new double[array.size()];
        for (int i = 0; i < knots.length; i++) knots[i] = array.get(i).getAsDouble();
        final JsonElement n = json.get("n");
        if (n == null || !n.isJsonPrimitive()) throw new IllegalStateException("quantile transform artifact lacks 'n': " + json);
        final JsonElement distribution = json.get("distribution");
        return new QuantileTransform(knots, n.getAsLong(), distribution == null ? UNIFORM : distribution.getAsString());
    }

    public static void write(final String artifactUri, final String planHash, final String block, final QuantileTransform q) {
        final String path = artifactPath(artifactUri, planHash, block);
        final JsonObject json = FitArtifact.manifest(planHash, block);
        for (final Map.Entry<String, JsonElement> e : q.toJson().entrySet()) json.add(e.getKey(), e.getValue());
        ResourceUtil.writeString(path, json.toString());
        LOG.info("wrote quantile transform artifact {} ({} knots, n={})", path, q.knots.length, q.n);
    }

    public static QuantileTransform read(final String artifactUri, final String planHash, final String block) {
        final String path = artifactPath(artifactUri, planHash, block);
        final QuantileTransform q = fromJson(JsonParser.parseString(ResourceUtil.readString(path)).getAsJsonObject());
        LOG.info("loaded quantile transform artifact {} ({} knots, n={})", path, q.knots.length, q.n);
        return q;
    }

}
