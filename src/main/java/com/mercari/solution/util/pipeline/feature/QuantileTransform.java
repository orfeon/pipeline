package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.math.NormalDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Fitted quantile transform of a numeric field (docs/design/feature-dsl.md §4.4, {@code type: quantileTransform}):
 * the empirical distribution of the whole input is summarised by {@code bins + 1} knots — the type-7 quantiles at
 * {@code 0, 1/B, ..., 1} — and a value is mapped to its position in that distribution, {@code F(v) ∈ [0, 1]}
 * (linear interpolation between knots; {@code distribution: normal} applies the probit Φ⁻¹ on top, so the output
 * is a normal score). Rank-based normalisation: robust to outliers and to the scale of the field, monotone, and
 * the same map at training and serving time (the knots are the artifact).
 *
 * <p>Out-of-range behaviour: a value below the fitted minimum maps to 0, above the maximum to 1 (a normal score
 * is clamped at {@code ±Φ⁻¹(1 − clip)}, {@code clip} = 1e-6 by default so the transform never returns ±∞; a larger
 * {@code clip} such as 0.001 caps the score of the extreme rows at ±3.09 — the fitted minimum / maximum otherwise
 * read ±4.75 whatever n, which turns them into outliers of a downstream linear combination or svd); a value equal
 * to a run of tied knots (a mass point of the distribution,
 * also when the run sits at the minimum or the maximum) maps to the middle of the run's probability range, so
 * ties do not depend on the search direction. Missing (null / NaN) maps to null. A fit that saw no value (n = 0)
 * maps everything to null and is still written as an artifact.
 */
public final class QuantileTransform implements Serializable, FitArtifact.Model {

    private static final Logger LOG = LoggerFactory.getLogger(QuantileTransform.class);

    public static final int DEFAULT_BINS = 100;
    public static final String UNIFORM = "uniform";
    public static final String NORMAL = "normal";
    /** Default probability clamp of the normal score (the transform never returns ±∞). */
    public static final double DEFAULT_CLIP = 1e-6;

    /** Nondecreasing knots: the quantiles at i / bins, i = 0..bins (length bins + 1); empty when n = 0. */
    public final double[] knots;
    public final long n;
    public final String distribution;
    /** Probability clamp of the normal score: p is clamped to [clip, 1 − clip] before the probit (0 < clip < 0.5). */
    public final double clip;

    QuantileTransform(final double[] knots, final long n, final String distribution, final double clip) {
        this.knots = knots;
        this.n = n;
        this.distribution = distribution;
        this.clip = clip;
    }

    /** Number of intervals between knots (B). */
    public int bins() {
        return Math.max(0, knots.length - 1);
    }

    /** Fits the knots on the first {@code count} values (the input is not modified: Beam forbids mutating DoFn inputs). */
    public static QuantileTransform fit(final double[] input, final int count, final int bins, final String distribution) {
        return fit(input, count, bins, distribution, DEFAULT_CLIP);
    }

    /** {@link #fit(double[], int, int, String)} with the probability clamp of the normal score. */
    public static QuantileTransform fit(final double[] input, final int count, final int bins, final String distribution, final double clip) {
        if (count == 0) {
            LOG.warn("quantileTransform: no non-null values to fit; every value maps to null");
            return new QuantileTransform(new double[0], 0, distribution, clip);
        }
        final double[] values = Arrays.copyOf(input, count);
        Arrays.sort(values);
        final double[] knots = new double[bins + 1];
        for (int i = 0; i <= bins; i++) knots[i] = OrderStatistics.quantile((double) i / bins, values, count);
        return new QuantileTransform(knots, count, distribution, clip);
    }

    /** Fits the knots on gathered {@link Values}; {@code warn} = report an empty fit (a forward window without values is normal, a whole input is not). */
    public static QuantileTransform fit(final Values values, final int bins, final String distribution, final double clip, final boolean warn) {
        if (values.size == 0 && !warn) return new QuantileTransform(new double[0], 0, distribution, clip);
        return fit(values.values, values.size, bins, distribution, clip);
    }

    /**
     * The values a fit reads — of the whole input, or of one time block under {@code fit.mode: forward}: a growable
     * buffer, exact (the knots are order statistics of the very values, not of a sketch). What crosses a worker
     * boundary is exactly {@code size} doubles.
     */
    public static final class Values implements Serializable {
        double[] values = new double[16];
        int size;

        void add(final double v) {
            // a state serialized empty comes back with a zero-length buffer: doubling 0 would stay 0
            if (size == values.length) values = Arrays.copyOf(values, Math.max(16, values.length * 2));
            values[size++] = v;
        }

        void append(final Values other) {
            if (size + other.size > values.length) values = Arrays.copyOf(values, Math.max(size + other.size, values.length * 2));
            System.arraycopy(other.values, 0, values, size, other.size);
            size += other.size;
        }

        public int size() {
            return size;
        }

        private void writeObject(final java.io.ObjectOutputStream out) throws java.io.IOException {
            if (values.length != size) values = Arrays.copyOf(values, size);
            out.defaultWriteObject();
        }
    }

    /**
     * {@link Values} as a {@link Summary} family — a monoid (merge = concatenation) that cannot remove, so a
     * {@link BlockSeries} serves a window by merging the range's blocks. The state is the data itself: the whole
     * training column lands on the worker that fits, as it always did for this type (8 bytes per row).
     */
    public static final Summary<Values> VALUES = new ValuesSummary();

    static final class ValuesSummary implements Summary<Values> {
        @Override
        public Values create() {
            return new Values();
        }

        @Override
        public void update(final Values state, final Object contribution, final int sign) {
            if (sign < 0) throw new UnsupportedOperationException("gathered values are not invertible");
            state.add(((Number) contribution).doubleValue());
        }

        @Override
        public boolean invertible() {
            return false;
        }

        @Override
        public void merge(final Values into, final Values other) {
            into.append(other);
        }

        @Override
        public Object read(final Values state, final Readout readout) {
            if ("count".equals(readout.name())) return (long) state.size;
            throw new IllegalArgumentException("gathered values cannot read " + readout.name());
        }

        @Override
        public double count(final Values state) {
            return state.size;
        }
    }

    /** Whether the fit saw no value: every value maps to null (a serving config loading such an artifact reads null everywhere). */
    @Override
    public boolean isEmpty() {
        return n == 0;
    }

    @Override
    public String describe() {
        return knots.length + " knots, n=" + n;
    }

    /**
     * The same knots with another probability clamp. The clamp is an apply-time parameter (the knots are the fit), so a
     * loaded artifact takes the clip of the config that applies it — also one pinned by {@code fit.artifact.id} or
     * fitted before the clip existed.
     */
    public QuantileTransform withClip(final double clip) {
        return clip == this.clip ? this : new QuantileTransform(knots, n, distribution, clip);
    }

    /** The empirical CDF position of a value (or its normal score); null for missing values and an empty fit. */
    public Double transform(final Double v) {
        if (v == null || v.isNaN() || isEmpty()) return null;
        final double p = position(v);
        return NORMAL.equals(distribution) ? probit(Math.min(1 - clip, Math.max(clip, p))) : p;
    }

    /**
     * F(v) in [0, 1] by linear interpolation between knots; a tied run of knots maps to the middle of its range —
     * including a run at either end (a zero-inflated field's mass point at the minimum), hence the strict bounds.
     */
    double position(final double v) {
        final int bins = bins();
        if (v < knots[0]) return 0d;
        if (v > knots[bins]) return 1d;
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

    /** Inverse of the standard normal CDF on (0, 1) — the shared {@link NormalDistribution#inverseNormal}. */
    static double probit(final double p) {
        return NormalDistribution.inverseNormal(p);
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static final FitArtifact.Json<QuantileTransform> ARTIFACT = new FitArtifact.Json<>("quantile transform", "quantiles",
            QuantileTransform::fromJson, "the column", "on an input that has values");

    @Override
    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        final JsonArray array = new JsonArray();
        for (final double k : knots) array.add(k);
        json.add("knots", array);
        json.addProperty("bins", bins());
        json.addProperty("n", n);
        json.addProperty("distribution", distribution);
        if (NORMAL.equals(distribution)) json.addProperty("clip", clip);
        return json;
    }

    public static QuantileTransform fromJson(final JsonObject json) {
        final JsonArray array = json.getAsJsonArray("knots");
        final double[] knots = new double[array.size()];
        for (int i = 0; i < knots.length; i++) knots[i] = array.get(i).getAsDouble();
        final JsonElement n = json.get("n");
        if (n == null || !n.isJsonPrimitive()) throw new IllegalStateException("quantile transform artifact lacks 'n': " + json);
        final JsonElement distribution = json.get("distribution");
        final JsonElement clip = json.get("clip");
        return new QuantileTransform(knots, n.getAsLong(), distribution == null ? UNIFORM : distribution.getAsString(),
                clip == null || !clip.isJsonPrimitive() ? DEFAULT_CLIP : clip.getAsDouble());
    }

}
