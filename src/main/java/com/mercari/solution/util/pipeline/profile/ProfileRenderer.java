package com.mercari.solution.util.pipeline.profile;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.sampling.VarOptItemsSamples;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Renders a {@link ProfileAccumulator} into the single-file HTML report.
 *
 * <p>The report embeds three versioned JSON blocks ({@code profile-payload} for the view,
 * {@code profile-manifest} for run metadata, {@code profile-sketches} for the base64 sketch
 * binaries) into the bundled template. When the embedded total exceeds the size limit the
 * renderer degrades in a fixed order (sketches → sample rows → histogram resolution) and
 * records what was dropped in the manifest.
 */
public class ProfileRenderer {

    public static final int PAYLOAD_FORMAT_VERSION = 1;

    private static final String TEMPLATE_RESOURCE = "/profile/report_template.html";
    private static final Gson GSON = new Gson();
    private static final Pattern KEY_NAME = Pattern.compile(".*(_|^)(id|key|code|uuid)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TARGET_NAME = Pattern.compile("^(is_.*|has_.*|.*_flag|flag|label|target)$", Pattern.CASE_INSENSITIVE);

    private static final double[] QUANTILE_RANKS = { 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99 };
    private static final String[] QUANTILE_NAMES = { "p1", "p5", "p25", "p50", "p75", "p95", "p99" };

    public static class Config implements Serializable {
        public String title;
        public boolean showValues = true;
        public String jobName;
        public String moduleName;
        public List<String> inputNames;
        public String expandedParametersJson;   // JSON text (JsonObject is not Serializable)
        public long embedLimitBytes = 25_000_000L;
        public int histogramBins = 256;
        public int timestampBins = 64;
        public int overlayBins = 64;            // per-group histogram resolution (shared edges)
        public int overlayTopK = 10;            // per-group string comparison labels
        public int timeGroupLimit = 60;         // max time buckets kept in the report
        public int topKShow = 20;
        public int sampleRowsInPayload = 1_000;
        public boolean embedSketches = true;
        public String sketchesOutput;
        public List<ProfileAxis> axes = List.of();
        public List<String[]> comparePairs = List.of();   // declared numeric field pairs
        public String compareWithSource;                  // uri of the past report (loaded at runtime)
    }

    /** The embedded blocks of a past report loaded for {@code compareWith}. */
    public static class PastReport implements Serializable {
        public String source;
        public String payloadJson;
        public String manifestJson;
        public String sketchesJson;

        /** Extracts the three embedded JSON blocks out of a past report's HTML text. */
        public static PastReport parse(final String source, final String html) {
            final PastReport past = new PastReport();
            past.source = source;
            past.payloadJson = extractBlock(html, "profile-payload");
            past.manifestJson = extractBlock(html, "profile-manifest");
            past.sketchesJson = extractBlock(html, "profile-sketches");
            if(past.payloadJson == null) {
                throw new IllegalArgumentException("compareWith target has no profile-payload block: " + source);
            }
            return past;
        }

        private static String extractBlock(final String html, final String id) {
            final String marker = "<script type=\"application/json\" id=\"" + id + "\">";
            final int start = html.indexOf(marker);
            if(start < 0) {
                return null;
            }
            final int end = html.indexOf("</script>", start);
            return end < 0 ? null : html.substring(start + marker.length(), end);
        }
    }

    public static class Result {
        public String html;
        public String payloadJson;
        public String manifestJson;
        public String sketchesJson;   // null when not embedded
        public List<String> degradations = new ArrayList<>();
    }

    public static Result render(final ProfileAccumulator accumulator, final Config config) {
        return render(accumulator, java.util.Map.of(), null, config);
    }

    public static Result render(
            final ProfileAccumulator accumulator,
            final java.util.Map<String, ProfileAccumulator> subProfiles,
            final Config config) {
        return render(accumulator, subProfiles, null, config);
    }

    public static Result render(
            final ProfileAccumulator accumulator,
            final java.util.Map<String, ProfileAccumulator> subProfiles,
            final PastReport past,
            final Config config) {
        return render(accumulator, subProfiles, past, config, null);
    }

    /**
     * @param groupTotals total distinct groups per axis id (from the pipeline, when the groups were
     *                    bounded before the shuffle); null derives the count from {@code subProfiles}
     */
    public static Result render(
            final ProfileAccumulator accumulator,
            final java.util.Map<String, ProfileAccumulator> subProfiles,
            final PastReport past,
            final Config config,
            final java.util.Map<String, Long> groupTotals) {

        final Result result = new Result();
        int bins = config.histogramBins;
        int sampleRows = config.sampleRowsInPayload;
        int groupLimit = Integer.MAX_VALUE;
        boolean embedSketches = config.embedSketches;

        String payload = buildPayload(accumulator, subProfiles, past, config, bins, sampleRows, groupLimit, groupTotals).toString();
        String sketches = embedSketches ? buildSketches(accumulator, config).toString() : null;

        // degradation order: sketches → sample rows → histogram resolution → top segments only
        if(sketches != null && tooLarge(payload, sketches, config.embedLimitBytes)) {
            sketches = null;
            result.degradations.add("sketch binaries not embedded (size limit exceeded)");
        }
        if(tooLarge(payload, sketches, config.embedLimitBytes) && sampleRows > 100) {
            sampleRows = 100;
            payload = buildPayload(accumulator, subProfiles, past, config, bins, sampleRows, groupLimit, groupTotals).toString();
            result.degradations.add("sample rows in payload reduced to 100 (size limit exceeded)");
        }
        if(tooLarge(payload, sketches, config.embedLimitBytes) && bins > 64) {
            bins = 64;
            payload = buildPayload(accumulator, subProfiles, past, config, bins, sampleRows, groupLimit, groupTotals).toString();
            result.degradations.add("histogram resolution reduced to 64 bins (size limit exceeded)");
        }
        if(tooLarge(payload, sketches, config.embedLimitBytes) && !config.axes.isEmpty()) {
            groupLimit = 5;
            payload = buildPayload(accumulator, subProfiles, past, config, bins, sampleRows, groupLimit, groupTotals).toString();
            result.degradations.add("comparison groups limited to the top 5 per axis (size limit exceeded)");
        }

        final String manifest = buildManifest(accumulator, config, result.degradations).toString();

        final String template = loadTemplate();
        result.html = template
                .replace("__PROFILE_TITLE__", escapeHtml(config.title))
                .replace("__PROFILE_STATIC__", buildStaticTable(accumulator))
                .replace("__PROFILE_PAYLOAD__", embedJson(payload))
                .replace("__PROFILE_MANIFEST__", embedJson(manifest))
                .replace("__PROFILE_SKETCHES__", sketches == null ? "{}" : embedJson(sketches));
        result.payloadJson = payload;
        result.manifestJson = manifest;
        result.sketchesJson = sketches;
        return result;
    }

    /**
     * Makes JSON text safe to splice into a {@code <script type="application/json">} block: a data
     * value containing {@code </script>} (or {@code <!--}) would otherwise terminate the block early
     * and hand the rest of the value to the HTML parser. {@code <} only ever occurs inside JSON
     * strings, and {@code <} is the same string to JSON.parse, so the payload is unchanged.
     */
    static String embedJson(final String json) {
        return json.replace("<", "\\u003c");
    }

    /**
     * Strictly increasing equal-width edges from {@code min} to {@code max} (at most {@code bins}
     * intervals). Consecutive points that collide in double precision (large magnitude, narrow
     * range) are dropped, since the sketch PMF/CDF queries require monotonically increasing split
     * points. Returns {@code [min, max]} when the range is empty.
     */
    static double[] equalWidthEdges(final double min, final double max, final int bins) {
        if(!(max > min)) {
            return new double[] { min, max };
        }
        final double[] edges = new double[bins + 1];
        int n = 0;
        edges[n++] = min;
        for(int i = 1; i < bins; i++) {
            final double edge = min + (max - min) * i / bins;
            if(edge > edges[n - 1] && edge < max) {
                edges[n++] = edge;
            }
        }
        edges[n++] = max;
        return n == edges.length ? edges : java.util.Arrays.copyOf(edges, n);
    }

    /** The interior points of {@code edges}: the split points for PMF/CDF queries over those bins. */
    static double[] interiorPoints(final double[] edges) {
        final double[] points = new double[Math.max(0, edges.length - 2)];
        System.arraycopy(edges, 1, points, 0, points.length);
        return points;
    }

    /** Bin shares of {@code kll} over {@code edges} (edges.length - 1 values); a single bin needs no query. */
    static double[] pmfOverEdges(final KllDoublesSketch kll, final double[] edges) {
        if(edges.length < 3) {
            return new double[] { 1d };
        }
        return kll.getPMF(interiorPoints(edges), QuantileSearchCriteria.INCLUSIVE);
    }

    /** Cumulative ranks of {@code kll} at each interior edge plus 1.0 at the last (edges.length - 1 values). */
    static double[] cdfOverEdges(final KllDoublesSketch kll, final double[] edges) {
        if(edges.length < 3) {
            return new double[] { 1d };
        }
        return kll.getCDF(interiorPoints(edges), QuantileSearchCriteria.INCLUSIVE);
    }

    private static boolean tooLarge(final String payload, final String sketches, final long limit) {
        long size = payload.length();
        if(sketches != null) {
            size += sketches.length();
        }
        return size > limit;
    }

    private static String loadTemplate() {
        try(final InputStream is = ProfileRenderer.class.getResourceAsStream(TEMPLATE_RESOURCE)) {
            if(is == null) {
                throw new IllegalStateException("profile report template not found: " + TEMPLATE_RESOURCE);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new IllegalStateException("failed to load profile report template", e);
        }
    }

    // ---- payload ----

    private static JsonObject buildPayload(
            final ProfileAccumulator accumulator,
            final java.util.Map<String, ProfileAccumulator> subProfiles,
            final PastReport past,
            final Config config,
            final int bins,
            final int sampleRows,
            final int groupLimit,
            final java.util.Map<String, Long> groupTotals) {

        final ProfileSpec spec = accumulator.getSpec();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();

        final JsonObject payload = new JsonObject();
        payload.addProperty("formatVersion", PAYLOAD_FORMAT_VERSION);
        payload.addProperty("title", config.title);
        payload.addProperty("values", config.showValues ? "show" : "hide");
        payload.addProperty("rows", accumulator.getRowCount());
        payload.addProperty("errorRows", accumulator.getErrorCount());
        payload.addProperty("topKShow", config.topKShow);

        final boolean withTarget = spec.getTarget() != null && spec.getTarget().fieldStats;
        final boolean withOverlay = !config.axes.isEmpty() || withTarget;
        final JsonArray fields = new JsonArray();
        for(int i = 0; i < accumulator.getFieldCount(); i++) {
            final JsonObject field = buildField(spec.getFields().get(i), accumulator.getField(i), accumulator, params, config, bins);
            if(withOverlay) {
                final JsonObject overlay = buildOverlayMeta(spec.getFields().get(i), accumulator.getField(i), config);
                if(overlay != null) {
                    field.add("overlay", overlay);
                }
            }
            fields.add(field);
        }
        payload.add("fields", fields);

        if(withTarget) {
            payload.add("target", buildTarget(accumulator, spec, config, fields));
        }
        if(withOverlay) {
            final JsonArray comparisons = buildComparisons(accumulator, subProfiles, spec, config, groupLimit, groupTotals);
            annotateBaselineDrift(comparisons, fields, config);
            payload.add("comparisons", comparisons);
        }
        if(!config.comparePairs.isEmpty()) {
            payload.add("fieldPairs", buildFieldPairs(accumulator, spec, config));
        }
        if(past != null) {
            payload.add("compareWith", buildCompareWith(accumulator, past, spec, config));
        }

        final JsonArray skipped = new JsonArray();
        for(final ProfileSpec.SkippedField skip : spec.getSkipped()) {
            final JsonObject o = new JsonObject();
            o.addProperty("path", skip.path);
            o.addProperty("sourceType", skip.sourceType);
            o.addProperty("reason", skip.reason);
            skipped.add(o);
        }
        payload.add("skippedFields", skipped);

        if(spec.isCorrelationEnabled()) {
            payload.add("correlations", buildCorrelations(accumulator, spec));
        }
        if(!spec.getKeyFieldIndices().isEmpty()) {
            payload.add("keys", buildKeys(accumulator, spec, params));
        }
        if(config.showValues && spec.isSampleEnabled()) {
            final JsonObject sample = buildSample(accumulator, spec, sampleRows);
            if(sample != null) {
                payload.add("sample", sample);
            }
        }
        payload.add("suggestions", buildSuggestions(accumulator, spec));
        return payload;
    }

    private static JsonObject buildField(
            final ProfileSpec.FieldSpec fieldSpec,
            final ProfileAccumulator.FieldAccumulator field,
            final ProfileAccumulator accumulator,
            final ProfileSpec.SketchParameters params,
            final Config config,
            final int bins) {

        final JsonObject o = new JsonObject();
        o.addProperty("path", fieldSpec.path);
        o.addProperty("type", fieldSpec.profileType.name().toLowerCase());
        o.addProperty("sourceType", fieldSpec.sourceType);
        o.addProperty("isKey", fieldSpec.isKey);
        o.addProperty("count", field.count);
        o.addProperty("nullCount", field.nullCount);
        o.addProperty("errorCount", field.errorCount);
        final long total = field.count + field.nullCount + field.errorCount + field.nanCount + field.infCount;
        final double nullRate = total == 0 ? 0d : (double) field.nullCount / total;
        o.addProperty("nullRate", nullRate);

        Double distinctEstimate = null;
        final CpcSketch cpc = field.cpcResult(params);
        if(cpc != null && field.count > 0) {
            final JsonObject distinct = new JsonObject();
            distinctEstimate = cpc.getEstimate();
            distinct.addProperty("estimate", distinctEstimate);
            distinct.addProperty("lower", cpc.getLowerBound(2));
            distinct.addProperty("upper", cpc.getUpperBound(2));
            o.add("distinct", distinct);
        }

        Double top1Ratio = null;
        switch (fieldSpec.profileType) {
            case NUMERIC, ARRAY_LENGTH -> o.add("numeric", buildNumeric(field, bins));
            case STRING -> {
                final JsonObject string = new JsonObject();
                string.addProperty("emptyCount", field.emptyCount);
                final KllDoublesSketch lengthKll = field.getKll();
                if(lengthKll != null && !lengthKll.isEmpty()) {
                    final JsonObject length = new JsonObject();
                    length.addProperty("min", lengthKll.getMinItem());
                    length.addProperty("max", lengthKll.getMaxItem());
                    length.addProperty("p50", lengthKll.getQuantile(0.5, QuantileSearchCriteria.INCLUSIVE));
                    length.addProperty("p95", lengthKll.getQuantile(0.95, QuantileSearchCriteria.INCLUSIVE));
                    string.add("length", length);
                }
                final ItemsSketch<String> fi = field.getFrequentItems();
                if(fi != null && field.count > 0) {
                    final ItemsSketch.Row<String>[] rows = fi.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
                    final JsonArray topK = new JsonArray();
                    for(int r = 0; r < rows.length && r < params.topKKeep; r++) {
                        final JsonObject item = new JsonObject();
                        if(config.showValues) {
                            final String value = rows[r].getItem();
                            item.addProperty("value", value.length() > 256 ? value.substring(0, 256) : value);
                        }
                        item.addProperty("count", rows[r].getEstimate());
                        item.addProperty("lower", rows[r].getLowerBound());
                        item.addProperty("upper", rows[r].getUpperBound());
                        topK.add(item);
                        if(r == 0) {
                            top1Ratio = (double) rows[r].getEstimate() / field.count;
                        }
                    }
                    string.add("topK", topK);
                    string.addProperty("maximumError", fi.getMaximumError());
                }
                o.add("string", string);
            }
            case BOOL -> {
                final JsonObject bool = new JsonObject();
                bool.addProperty("trueCount", field.trueCount);
                bool.addProperty("falseCount", field.falseCount);
                o.add("bool", bool);
            }
            case TIMESTAMP -> {
                final JsonObject ts = new JsonObject();
                if(field.count > 0) {
                    ts.addProperty("min", Instant.ofEpochMilli((long) field.min).toString());
                    ts.addProperty("max", Instant.ofEpochMilli((long) field.max).toString());
                    final KllDoublesSketch kll = field.getKll();
                    if(kll != null && !kll.isEmpty()) {
                        ts.add("histogram", buildHistogram(kll, field.min, field.max, config.timestampBins, field.count));
                    }
                }
                o.add("timestamp", ts);
            }
        }

        o.add("notable", buildNotable(fieldSpec, field, accumulator, distinctEstimate, top1Ratio, nullRate));
        return o;
    }

    private static JsonObject buildNumeric(final ProfileAccumulator.FieldAccumulator field, final int bins) {
        final JsonObject numeric = new JsonObject();
        numeric.addProperty("zeroCount", field.zeroCount);
        numeric.addProperty("nanCount", field.nanCount);
        numeric.addProperty("infCount", field.infCount);
        if(field.count == 0) {
            return numeric;
        }
        numeric.addProperty("min", field.min);
        numeric.addProperty("max", field.max);
        numeric.addProperty("mean", field.mean);
        final double n = field.count;
        if(n > 1) {
            final double variance = field.m2 / (n - 1);
            numeric.addProperty("stddev", Math.sqrt(variance));
            if(field.m2 > 0) {
                numeric.addProperty("skewness", Math.sqrt(n) * field.m3 / Math.pow(field.m2, 1.5));
            }
        }
        final KllDoublesSketch kll = field.getKll();
        if(kll != null && !kll.isEmpty()) {
            final JsonObject quantiles = new JsonObject();
            for(int i = 0; i < QUANTILE_RANKS.length; i++) {
                quantiles.addProperty(QUANTILE_NAMES[i], kll.getQuantile(QUANTILE_RANKS[i], QuantileSearchCriteria.INCLUSIVE));
            }
            numeric.add("quantiles", quantiles);
            numeric.addProperty("rankError", kll.getNormalizedRankError(false));
            numeric.add("histogram", buildHistogram(kll, field.min, field.max, bins, field.count));
            numeric.add("cdf", buildCdf(kll, field.min, field.max, bins));
        }
        return numeric;
    }

    /** Equal-width PMF histogram: {@code edges} has one more entry than {@code counts} (at most bins+1). */
    private static JsonObject buildHistogram(
            final KllDoublesSketch kll, final double min, final double max, final int bins, final long count) {

        final JsonObject histogram = new JsonObject();
        final double[] edges = equalWidthEdges(min, max, bins);
        final JsonArray counts = new JsonArray();
        if(edges.length < 3) {
            counts.add(count);
        } else {
            for(final double p : pmfOverEdges(kll, edges)) {
                counts.add(Math.round(p * count));
            }
        }
        histogram.add("edges", toJsonArray(edges));
        histogram.add("counts", counts);
        return histogram;
    }

    /** CDF at up to bins+1 equally spaced points from min to max. */
    private static JsonObject buildCdf(final KllDoublesSketch kll, final double min, final double max, final int bins) {
        final JsonObject cdf = new JsonObject();
        final JsonArray points = new JsonArray();
        final JsonArray ranks = new JsonArray();
        if(!(max > min)) {
            points.add(min);
            ranks.add(1.0);
        } else {
            // the query points include min and max themselves, so dedupe them as one edge list
            final double[] queryPoints = equalWidthEdges(min, max, bins);
            final double[] cdfValues = kll.getCDF(queryPoints, QuantileSearchCriteria.INCLUSIVE);
            for(int i = 0; i < queryPoints.length; i++) {
                points.add(queryPoints[i]);
                ranks.add(cdfValues[i]);
            }
        }
        cdf.add("points", points);
        cdf.add("ranks", ranks);
        return cdf;
    }

    private static JsonArray buildNotable(
            final ProfileSpec.FieldSpec fieldSpec,
            final ProfileAccumulator.FieldAccumulator field,
            final ProfileAccumulator accumulator,
            final Double distinctEstimate,
            final Double top1Ratio,
            final double nullRate) {

        final JsonArray notable = new JsonArray();
        if(field.count == 0 && field.nullCount > 0) {
            notable.add("all_null");
        } else {
            if(nullRate > 0.5) {
                notable.add("high_null");
            }
            if(field.count > 0) {
                switch (fieldSpec.profileType) {
                    case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> {
                        if(field.min == field.max) {
                            notable.add("constant");
                        }
                    }
                    case STRING -> {
                        if(distinctEstimate != null && distinctEstimate <= 1.5) {
                            notable.add("constant");
                        }
                    }
                    default -> { }
                }
                if(top1Ratio != null && top1Ratio > 0.9 && distinctEstimate != null && distinctEstimate > 1.5) {
                    notable.add("dominant_value");
                }
                if(distinctEstimate != null && field.count > 100 && distinctEstimate >= 0.95 * field.count) {
                    notable.add("unique_like");
                }
                if(ProfileSpec.ProfileType.NUMERIC.equals(fieldSpec.profileType) && field.count > 2 && field.m2 > 0) {
                    final double skewness = Math.sqrt(field.count) * field.m3 / Math.pow(field.m2, 1.5);
                    if(Math.abs(skewness) > 2) {
                        notable.add("skewed");
                    }
                }
            }
        }
        return notable;
    }

    private static JsonObject buildCorrelations(final ProfileAccumulator accumulator, final ProfileSpec spec) {
        final List<Integer> numericIndices = spec.getNumericFieldIndices();
        final JsonObject correlations = new JsonObject();
        final JsonArray fieldPaths = new JsonArray();
        for(final Integer index : numericIndices) {
            fieldPaths.add(spec.getFields().get(index).path);
        }
        correlations.add("fields", fieldPaths);
        final JsonArray matrix = new JsonArray();
        final JsonArray counts = new JsonArray();
        for(int i = 0; i < numericIndices.size(); i++) {
            final JsonArray row = new JsonArray();
            final JsonArray countRow = new JsonArray();
            for(int j = 0; j < numericIndices.size(); j++) {
                final Double correlation = accumulator.correlation(i, j);
                row.add(correlation == null || !Double.isFinite(correlation) ? null : correlation);
                final Double pairCount = accumulator.pairCount(i, j);
                countRow.add(pairCount == null ? null : pairCount.longValue());
            }
            matrix.add(row);
            counts.add(countRow);
        }
        correlations.add("matrix", matrix);
        correlations.add("counts", counts);
        return correlations;
    }

    private static JsonObject buildKeys(
            final ProfileAccumulator accumulator,
            final ProfileSpec spec,
            final ProfileSpec.SketchParameters params) {

        final List<Integer> keyIndices = spec.getKeyFieldIndices();
        final JsonObject keys = new JsonObject();
        final JsonArray fields = new JsonArray();
        final List<CompactSketch> sketches = new ArrayList<>();
        for(final Integer index : keyIndices) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(index);
            final ProfileAccumulator.FieldAccumulator field = accumulator.getField(index);
            final CompactSketch theta = field.thetaResult(params);
            sketches.add(theta);
            final JsonObject o = new JsonObject();
            o.addProperty("path", fieldSpec.path);
            if(theta != null && field.count > 0) {
                o.addProperty("distinct", theta.getEstimate());
                o.addProperty("distinctLower", theta.getLowerBound(2));
                o.addProperty("distinctUpper", theta.getUpperBound(2));
                o.addProperty("keyness", accumulator.getRowCount() == 0
                        ? 0d : theta.getEstimate() / accumulator.getRowCount());
            }
            fields.add(o);
        }
        keys.add("fields", fields);

        // pairwise intersection estimates; containment[i][j] = |Ai ∩ Aj| / |Ai|
        final Double[][] intersections = new Double[sketches.size()][sketches.size()];
        for(int i = 0; i < sketches.size(); i++) {
            for(int j = i + 1; j < sketches.size(); j++) {
                final CompactSketch a = sketches.get(i);
                final CompactSketch b = sketches.get(j);
                if(a == null || b == null) {
                    continue;
                }
                final Intersection intersection = SetOperation.builder().buildIntersection();
                intersection.intersect(a);
                intersection.intersect(b);
                intersections[i][j] = intersection.getResult().getEstimate();
                intersections[j][i] = intersections[i][j];
            }
        }
        final JsonArray containment = new JsonArray();
        for(int i = 0; i < sketches.size(); i++) {
            final JsonArray row = new JsonArray();
            for(int j = 0; j < sketches.size(); j++) {
                if(i == j) {
                    row.add(1.0);
                } else if(intersections[i][j] == null
                        || sketches.get(i) == null || sketches.get(i).getEstimate() == 0d) {
                    row.add((JsonElement) null);
                } else {
                    row.add(intersections[i][j] / sketches.get(i).getEstimate());
                }
            }
            containment.add(row);
        }
        keys.add("containment", containment);

        // proportional venn (2-3 sets, rendered as SVG by the client)
        if(sketches.size() >= 2 && sketches.size() <= 3
                && sketches.stream().allMatch(s -> s != null && s.getEstimate() > 0d)) {
            final JsonObject venn = new JsonObject();
            final JsonArray labels = new JsonArray();
            final JsonArray sizes = new JsonArray();
            for(int i = 0; i < keyIndices.size(); i++) {
                labels.add(spec.getFields().get(keyIndices.get(i)).path);
                sizes.add(sketches.get(i).getEstimate());
            }
            venn.add("labels", labels);
            venn.add("sizes", sizes);
            final JsonArray pairs = new JsonArray();
            for(int i = 0; i < sketches.size(); i++) {
                for(int j = i + 1; j < sketches.size(); j++) {
                    final JsonObject pair = new JsonObject();
                    pair.addProperty("a", i);
                    pair.addProperty("b", j);
                    pair.addProperty("size", intersections[i][j] == null ? 0d : intersections[i][j]);
                    pairs.add(pair);
                }
            }
            venn.add("pairs", pairs);
            if(sketches.size() == 3) {
                final Intersection triple = SetOperation.builder().buildIntersection();
                triple.intersect(sketches.get(0));
                triple.intersect(sketches.get(1));
                triple.intersect(sketches.get(2));
                venn.addProperty("triple", triple.getResult().getEstimate());
            }
            keys.add("venn", venn);
        }
        return keys;
    }

    // ---- comparisons (segments / time axes) ----

    /** Shared comparison metadata per field: histogram edges (numeric-like) or top-value labels (string). */
    private static JsonObject buildOverlayMeta(
            final ProfileSpec.FieldSpec fieldSpec,
            final ProfileAccumulator.FieldAccumulator field,
            final Config config) {

        switch (fieldSpec.profileType) {
            case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> {
                final double[] edges = overlayEdges(field, config.overlayBins);
                if(edges == null) {
                    return null;
                }
                final JsonObject overlay = new JsonObject();
                final JsonArray edgesArray = new JsonArray();
                for(final double edge : edges) {
                    edgesArray.add(edge);
                }
                overlay.add("edges", edgesArray);
                return overlay;
            }
            case STRING -> {
                final List<String> labels = overlayLabels(field, config.overlayTopK);
                if(labels == null || labels.isEmpty()) {
                    return null;
                }
                final JsonObject overlay = new JsonObject();
                final JsonArray labelsArray = new JsonArray();
                for(int i = 0; i < labels.size(); i++) {
                    labelsArray.add(config.showValues ? labels.get(i) : "#" + (i + 1));
                }
                overlay.add("labels", labelsArray);
                return overlay;
            }
            default -> {
                return null;
            }
        }
    }

    /** Equal-width bin edges over the global value range ({@code bins + 1} entries), or null when undefined. */
    private static double[] overlayEdges(final ProfileAccumulator.FieldAccumulator field, final int bins) {
        if(field.count == 0 || !Double.isFinite(field.min) || !Double.isFinite(field.max)) {
            return null;
        }
        return equalWidthEdges(field.min, field.max, bins);
    }

    /** The global top values used as the comparison categories for string fields (actual values). */
    private static List<String> overlayLabels(final ProfileAccumulator.FieldAccumulator field, final int k) {
        final ItemsSketch<String> fi = field.getFrequentItems();
        if(fi == null || field.count == 0) {
            return null;
        }
        final ItemsSketch.Row<String>[] rows = fi.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        final List<String> labels = new ArrayList<>();
        for(int i = 0; i < rows.length && i < k; i++) {
            labels.add(rows[i].getItem());
        }
        return labels;
    }

    /**
     * Per-axis, per-group comparison data. Group histograms/top-K counts are aligned to the shared
     * edges/labels from {@link #buildOverlayMeta}, so the client can overlay them without rebinning.
     */
    private static JsonArray buildComparisons(
            final ProfileAccumulator accumulator,
            final java.util.Map<String, ProfileAccumulator> subProfiles,
            final ProfileSpec spec,
            final Config config,
            final int groupLimit,
            final java.util.Map<String, Long> groupTotals) {

        final JsonArray axes = new JsonArray();
        for(final ProfileAxis axis : config.axes) {
            final JsonObject axisJson = new JsonObject();
            axisJson.addProperty("kind", axis.kind.name());
            axisJson.addProperty("field", axis.field);
            if(ProfileAxis.Kind.time.equals(axis.kind)) {
                axisJson.addProperty("granularity", axis.granularity);
            }
            if(ProfileAxis.Kind.inputs.equals(axis.kind) && axis.baseline != null) {
                axisJson.addProperty("baseline", axis.baseline);
            }

            // collect this axis's groups from the keyed sub-profiles
            final List<java.util.Map.Entry<String, ProfileAccumulator>> groups = new ArrayList<>();
            for(final java.util.Map.Entry<String, ProfileAccumulator> entry : subProfiles.entrySet()) {
                if(axis.groupOfKey(entry.getKey()) != null) {
                    groups.add(entry);
                }
            }
            final int limit;
            if(ProfileAxis.Kind.inputs.equals(axis.kind)) {
                // declared input order; never truncated (input count is small by construction)
                groups.sort(java.util.Comparator.comparingInt(entry -> {
                    final int i = axis.inputNames.indexOf(axis.groupOfKey(entry.getKey()));
                    return i < 0 ? Integer.MAX_VALUE : i;
                }));
            } else if(ProfileAxis.Kind.time.equals(axis.kind)) {
                // chronological; keep the most recent buckets when over the limit
                groups.sort(java.util.Map.Entry.comparingByKey());
                limit = Math.min(config.timeGroupLimit, groupLimit);
                if(groups.size() > limit) {
                    groups.subList(0, groups.size() - limit).clear();
                }
            } else {
                // largest groups first
                groups.sort((a, b) -> Long.compare(b.getValue().getRowCount(), a.getValue().getRowCount()));
                limit = Math.min(axis.topK, groupLimit);
                if(groups.size() > limit) {
                    groups.subList(limit, groups.size()).clear();
                }
            }
            // groups bounded before the shuffle report their true total via groupTotals
            final long totalGroups = groupTotals != null && groupTotals.containsKey(axis.id())
                    ? groupTotals.get(axis.id())
                    : countGroups(subProfiles, axis);
            axisJson.addProperty("truncatedGroups", Math.max(0L, totalGroups - groups.size()));

            final JsonArray groupsJson = new JsonArray();
            for(int g = 0; g < groups.size(); g++) {
                final String value = axis.groupOfKey(groups.get(g).getKey());
                final ProfileAccumulator group = groups.get(g).getValue();
                final JsonObject groupJson = new JsonObject();
                groupJson.addProperty("value", groupLabel(axis, value, g, config.showValues));
                groupJson.addProperty("rows", group.getRowCount());
                if(spec.getTarget() != null && group.getTargetRate() != null) {
                    // per-group target rate (the group spec counts the class totals)
                    groupJson.addProperty("targetPositive", group.getTargetPositiveRows());
                    groupJson.addProperty("targetRate", group.getTargetRate());
                }
                groupJson.add("fields", buildGroupFields(accumulator, group, spec, config));
                groupsJson.add(groupJson);
            }
            axisJson.add("groups", groupsJson);
            axes.add(axisJson);
        }
        if(spec.getTarget() != null && spec.getTarget().fieldStats) {
            axes.add(buildTargetAxis(accumulator, spec, config));
        }
        return axes;
    }

    // ---- target (binary outcome) ----

    /** Information value above which a field separates the classes suspiciously well (possible leakage). */
    private static final double IV_LEAK_THRESHOLD = 0.5;

    /**
     * The target as a synthetic comparison axis with the two classes as groups, so the compare bar
     * can overlay positive-vs-negative distributions on every field card with the same client
     * code as segments. Built from the global accumulator's class split, not from sub-profiles.
     */
    private static JsonObject buildTargetAxis(
            final ProfileAccumulator accumulator, final ProfileSpec spec, final Config config) {

        final ProfileSpec.TargetSpec target = spec.getTarget();
        final JsonObject axisJson = new JsonObject();
        axisJson.addProperty("kind", "target");
        axisJson.addProperty("field", target.path);
        axisJson.addProperty("positive", target.positiveLabel());
        axisJson.addProperty("truncatedGroups", 0);
        final JsonArray groups = new JsonArray();
        for(final boolean positiveClass : new boolean[] { true, false }) {
            final JsonObject groupJson = new JsonObject();
            groupJson.addProperty("value", positiveClass ? "positive" : "negative");
            groupJson.addProperty("rows", positiveClass ? accumulator.getTargetPositiveRows() : accumulator.getTargetNegativeRows());
            final JsonObject fields = new JsonObject();
            for(int i = 0; i < spec.getFields().size(); i++) {
                final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
                final ProfileAccumulator.FieldAccumulator globalField = accumulator.getField(i);
                final ProfileAccumulator.TargetStats stats = globalField.getTarget();
                if(stats == null) {
                    continue;   // the target field itself
                }
                final JsonObject o = new JsonObject();
                final long count = positiveClass ? stats.positiveCount : stats.negativeCount;
                o.addProperty("count", count);
                o.addProperty("nulls", positiveClass ? stats.nullPositive : stats.nullNegative);
                switch (fieldSpec.profileType) {
                    case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> {
                        final ProfileAccumulator.Moments moments = positiveClass ? stats.positive : stats.negative;
                        final KllDoublesSketch kll = positiveClass ? stats.getKllPositive() : stats.getKllNegative();
                        if(count > 0 && moments.n > 0) {
                            o.addProperty("mean", moments.mean);
                        }
                        final double[] edges = overlayEdges(globalField, config.overlayBins);
                        if(edges != null && edges.length > 2 && kll != null && !kll.isEmpty()) {
                            o.add("hist", scaledCounts(pmfOverEdges(kll, edges), count));
                        }
                    }
                    case STRING -> {
                        final List<String> labels = overlayLabels(globalField, config.overlayTopK);
                        final ItemsSketch<String> fi = positiveClass ? stats.getFiPositive() : stats.getFiNegative();
                        if(labels != null && fi != null) {
                            final JsonArray topK = new JsonArray();
                            for(final String label : labels) {
                                topK.add(fi.getEstimate(label));
                            }
                            o.add("topK", topK);
                        }
                    }
                    case BOOL -> {
                        final long trues = positiveClass ? stats.truePositive : stats.trueNegative;
                        o.addProperty("trueCount", trues);
                        o.addProperty("falseCount", count - trues);
                    }
                }
                fields.add(fieldSpec.path, o);
            }
            groupJson.add("fields", fields);
            groups.add(groupJson);
        }
        axisJson.add("groups", groups);
        return axisJson;
    }

    /**
     * A one-class target is almost always a wrong {@code positive} value (case, quoting, type):
     * the per-field statistics need both classes, so say so instead of rendering an empty analysis.
     */
    public static String targetWarning(final ProfileAccumulator accumulator, final ProfileSpec.TargetSpec target) {
        if(accumulator.getRowCount() == 0 || accumulator.getTargetPositiveRows() + accumulator.getTargetNegativeRows() == 0) {
            return accumulator.getRowCount() == 0 ? null
                    : "every row has a null or unreadable " + target.path + " value: no target analysis was possible";
        }
        if(accumulator.getTargetPositiveRows() == 0) {
            return "no row matched the positive value `" + target.positiveLabel() + "` of " + target.path
                    + " — check parameters.target.positive (case, quoting); every row was classed negative and the per-field statistics are skipped";
        }
        if(accumulator.getTargetNegativeRows() == 0) {
            return "every row matched the positive value `" + target.positiveLabel() + "` of " + target.path
                    + " — there is no negative class to compare against and the per-field statistics are skipped";
        }
        return null;
    }

    private static JsonArray scaledCounts(final double[] shares, final long count) {
        final JsonArray counts = new JsonArray();
        for(final double share : shares) {
            counts.add(Math.round(share * count));
        }
        return counts;
    }

    /**
     * The target block: class totals plus, per field, the positive rate over the field's bins
     * (shared equal-width edges for numeric-like fields, global top-K labels + other for strings,
     * true/false for bools), the information value (PSI between the positive and negative
     * distributions), the binned KS separation, the point-biserial correlation for numeric-like
     * fields and the positive rate among the field's null rows. The per-field IV is also
     * annotated on the top-level field objects for the stat strips.
     */
    private static JsonObject buildTarget(
            final ProfileAccumulator accumulator,
            final ProfileSpec spec,
            final Config config,
            final JsonArray fieldsJson) {

        final ProfileSpec.TargetSpec target = spec.getTarget();
        final JsonObject result = new JsonObject();
        result.addProperty("field", target.path);
        result.addProperty("positive", target.positiveLabel());
        result.addProperty("positiveRows", accumulator.getTargetPositiveRows());
        result.addProperty("negativeRows", accumulator.getTargetNegativeRows());
        result.addProperty("nullRows", accumulator.getTargetNullRows());
        final Double rate = accumulator.getTargetRate();
        if(rate != null) {
            result.addProperty("rate", rate);
        }
        final String warning = targetWarning(accumulator, target);
        if(warning != null) {
            result.addProperty("warning", warning);
        }

        final java.util.Map<String, JsonObject> fieldMap = new java.util.HashMap<>();
        for(final JsonElement field : fieldsJson) {
            fieldMap.put(field.getAsJsonObject().get("path").getAsString(), field.getAsJsonObject());
        }

        final JsonArray fields = new JsonArray();
        for(int i = 0; i < spec.getFields().size(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
            final ProfileAccumulator.FieldAccumulator field = accumulator.getField(i);
            final ProfileAccumulator.TargetStats stats = field.getTarget();
            if(stats == null) {
                continue;
            }
            final JsonObject o = new JsonObject();
            o.addProperty("path", fieldSpec.path);
            o.addProperty("type", fieldSpec.profileType.name().toLowerCase());
            o.addProperty("count", stats.positiveCount + stats.negativeCount);
            final long nulls = stats.nullPositive + stats.nullNegative;
            if(nulls > 0) {
                final JsonObject nullRate = new JsonObject();
                nullRate.addProperty("rows", nulls);
                nullRate.addProperty("rate", (double) stats.nullPositive / nulls);
                o.add("nulls", nullRate);
            }

            double[] positiveShares = null;
            double[] negativeShares = null;
            if(stats.positiveCount > 0 && stats.negativeCount > 0) {
                switch (fieldSpec.profileType) {
                    case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> {
                        final double[] edges = overlayEdges(field, config.overlayBins);
                        final KllDoublesSketch kllPositive = stats.getKllPositive();
                        final KllDoublesSketch kllNegative = stats.getKllNegative();
                        if(edges != null && kllPositive != null && !kllPositive.isEmpty()
                                && kllNegative != null && !kllNegative.isEmpty()) {
                            positiveShares = pmfOverEdges(kllPositive, edges);
                            negativeShares = pmfOverEdges(kllNegative, edges);
                            o.add("edges", toJsonArray(edges));
                        }
                        // point-biserial correlation: standardized mean difference between the classes
                        final ProfileAccumulator.Moments p = stats.positive;
                        final ProfileAccumulator.Moments q = stats.negative;
                        final double n = p.n + q.n;
                        if(p.n > 0 && q.n > 0) {
                            final double mean = (p.n * p.mean + q.n * q.mean) / n;
                            final double m2 = p.m2 + q.m2 + p.n * (p.mean - mean) * (p.mean - mean) + q.n * (q.mean - mean) * (q.mean - mean);
                            final double sd = Math.sqrt(m2 / n);
                            if(sd > 0 && Double.isFinite(sd)) {
                                // class shares in double arithmetic (p.n * q.n would overflow long past ~3e9 rows each)
                                final double r = (p.mean - q.mean) / sd * Math.sqrt((p.n / n) * (q.n / n));
                                if(Double.isFinite(r)) {
                                    o.addProperty("pointBiserial", r);
                                }
                            }
                            o.addProperty("meanPositive", p.mean);
                            o.addProperty("meanNegative", q.mean);
                        }
                    }
                    case STRING -> {
                        final List<String> labels = overlayLabels(field, config.overlayTopK);
                        final ItemsSketch<String> fiPositive = stats.getFiPositive();
                        final ItemsSketch<String> fiNegative = stats.getFiNegative();
                        if(labels != null && !labels.isEmpty() && fiPositive != null && fiNegative != null) {
                            final JsonArray labelsJson = new JsonArray();
                            positiveShares = new double[labels.size() + 1];
                            negativeShares = new double[labels.size() + 1];
                            double positiveSum = 0d;
                            double negativeSum = 0d;
                            for(int l = 0; l < labels.size(); l++) {
                                labelsJson.add(config.showValues ? labels.get(l) : "#" + (l + 1));
                                positiveShares[l] = (double) fiPositive.getEstimate(labels.get(l)) / stats.positiveCount;
                                negativeShares[l] = (double) fiNegative.getEstimate(labels.get(l)) / stats.negativeCount;
                                positiveSum += positiveShares[l];
                                negativeSum += negativeShares[l];
                            }
                            labelsJson.add("(other)");
                            positiveShares[labels.size()] = Math.max(0d, 1d - positiveSum);
                            negativeShares[labels.size()] = Math.max(0d, 1d - negativeSum);
                            o.add("labels", labelsJson);
                        }
                    }
                    case BOOL -> {
                        final JsonArray labelsJson = new JsonArray();
                        labelsJson.add("true");
                        labelsJson.add("false");
                        o.add("labels", labelsJson);
                        positiveShares = new double[] {
                                (double) stats.truePositive / stats.positiveCount,
                                1d - (double) stats.truePositive / stats.positiveCount };
                        negativeShares = new double[] {
                                (double) stats.trueNegative / stats.negativeCount,
                                1d - (double) stats.trueNegative / stats.negativeCount };
                    }
                }
            }
            if(positiveShares != null) {
                // aligned class counts per bin: the per-bin positive rate is positive / (positive + negative)
                o.add("positive", scaledCounts(positiveShares, stats.positiveCount));
                o.add("negative", scaledCounts(negativeShares, stats.negativeCount));
                final double iv = psi(positiveShares, negativeShares);
                o.addProperty("iv", iv);
                if(o.has("edges")) {
                    o.addProperty("ks", binnedKs(positiveShares, negativeShares));
                } else {
                    // categories have no order: total variation distance instead of KS
                    o.addProperty("tvd", totalVariation(positiveShares, negativeShares));
                }
                if(iv > IV_LEAK_THRESHOLD) {
                    o.addProperty("leak", true);
                }
                final JsonObject fieldJson = fieldMap.get(fieldSpec.path);
                if(fieldJson != null) {
                    final JsonObject annotation = new JsonObject();
                    annotation.addProperty("iv", iv);
                    fieldJson.add("target", annotation);
                }
            }
            fields.add(o);
        }
        result.add("fields", fields);
        return result;
    }

    private static int countGroups(final java.util.Map<String, ProfileAccumulator> subProfiles, final ProfileAxis axis) {
        int count = 0;
        for(final String key : subProfiles.keySet()) {
            if(axis.groupOfKey(key) != null) {
                count += 1;
            }
        }
        return count;
    }

    /** With {@code values: hide}, segment group labels are raw values and must be masked; time buckets and input names are not. */
    private static String groupLabel(final ProfileAxis axis, final String value, final int index, final boolean showValues) {
        if(showValues || !ProfileAxis.Kind.segments.equals(axis.kind) || ProfileAxis.NULL_GROUP.equals(value)) {
            return value;
        }
        return "group #" + (index + 1);
    }

    private static JsonObject buildGroupFields(
            final ProfileAccumulator global,
            final ProfileAccumulator group,
            final ProfileSpec spec,
            final Config config) {

        final JsonObject fields = new JsonObject();
        for(int i = 0; i < spec.getFields().size(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
            final ProfileAccumulator.FieldAccumulator globalField = global.getField(i);
            final ProfileAccumulator.FieldAccumulator groupField = group.getField(i);
            final JsonObject o = new JsonObject();
            o.addProperty("count", groupField.count);
            o.addProperty("nulls", groupField.nullCount);
            switch (fieldSpec.profileType) {
                case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> {
                    if(groupField.count > 0) {
                        o.addProperty("mean", groupField.mean);
                        final double[] edges = overlayEdges(globalField, config.overlayBins);
                        final KllDoublesSketch kll = groupField.getKll();
                        if(edges != null && edges.length > 2 && kll != null && !kll.isEmpty()) {
                            final double[] pmf = pmfOverEdges(kll, edges);
                            final JsonArray hist = new JsonArray();
                            for(final double p : pmf) {
                                hist.add(Math.round(p * groupField.count));
                            }
                            o.add("hist", hist);
                        }
                    }
                }
                case STRING -> {
                    final CpcSketch cpc = groupField.cpcResult(spec.getSketchParameters());
                    if(cpc != null && groupField.count > 0) {
                        o.addProperty("distinct", cpc.getEstimate());
                    }
                    final List<String> labels = overlayLabels(globalField, config.overlayTopK);
                    final ItemsSketch<String> fi = groupField.getFrequentItems();
                    if(labels != null && fi != null) {
                        final JsonArray topK = new JsonArray();
                        for(final String label : labels) {
                            topK.add(fi.getEstimate(label));
                        }
                        o.add("topK", topK);
                    }
                }
                case BOOL -> {
                    o.addProperty("trueCount", groupField.trueCount);
                    o.addProperty("falseCount", groupField.falseCount);
                }
            }
            fields.add(fieldSpec.path, o);
        }
        return fields;
    }

    // ---- drift metrics (baseline inputs / field pairs / past report) ----

    private static final double PSI_EPSILON = 1e-6;

    /** Population stability index over two aligned share distributions (smoothed). */
    private static double psi(final double[] sharesA, final double[] sharesB) {
        double psi = 0d;
        for(int i = 0; i < sharesA.length; i++) {
            final double a = Math.max(sharesA[i], PSI_EPSILON);
            final double b = Math.max(sharesB[i], PSI_EPSILON);
            psi += (a - b) * Math.log(a / b);
        }
        return psi;
    }

    private static double[] sharesOf(final JsonArray counts, final double total) {
        final double[] shares = new double[counts.size()];
        if(total <= 0) {
            return shares;
        }
        for(int i = 0; i < counts.size(); i++) {
            shares[i] = counts.get(i).getAsDouble() / total;
        }
        return shares;
    }

    /** Half the L1 distance between two aligned share distributions (total variation distance, in [0, 1]). */
    private static double totalVariation(final double[] sharesA, final double[] sharesB) {
        double sum = 0d;
        for(int i = 0; i < sharesA.length; i++) {
            sum += Math.abs(sharesA[i] - sharesB[i]);
        }
        return sum / 2;
    }

    /** Max |cumulative share A − cumulative share B| over aligned bins (binned KS statistic). */
    private static double binnedKs(final double[] sharesA, final double[] sharesB) {
        double ks = 0d;
        double cumA = 0d;
        double cumB = 0d;
        for(int i = 0; i < sharesA.length; i++) {
            cumA += sharesA[i];
            cumB += sharesB[i];
            ks = Math.max(ks, Math.abs(cumA - cumB));
        }
        return ks;
    }

    /** Linear interpolation of a stored CDF ({@code points} ascending, {@code ranks} in [0,1]). */
    private static double interpolateCdf(final double[] points, final double[] ranks, final double x) {
        if(points.length == 0) {
            return 0d;
        }
        if(x <= points[0]) {
            return x < points[0] ? 0d : ranks[0];
        }
        if(x >= points[points.length - 1]) {
            return 1d;
        }
        int low = 0;
        int high = points.length - 1;
        while(low + 1 < high) {
            final int mid = (low + high) >>> 1;
            if(points[mid] <= x) {
                low = mid;
            } else {
                high = mid;
            }
        }
        final double span = points[high] - points[low];
        final double t = span <= 0 ? 0d : (x - points[low]) / span;
        return ranks[low] + t * (ranks[high] - ranks[low]);
    }

    /**
     * PSI/KS of every non-baseline group against the baseline group, written into the group field
     * entries; the per-field maximum PSI is also annotated on the top-level field objects
     * ({@code drift}) for the stat strips.
     */
    private static void annotateBaselineDrift(final JsonArray comparisons, final JsonArray fields, final Config config) {
        final java.util.Map<String, JsonObject> fieldMap = new java.util.HashMap<>();
        for(final JsonElement field : fields) {
            fieldMap.put(field.getAsJsonObject().get("path").getAsString(), field.getAsJsonObject());
        }
        for(final JsonElement axisElement : comparisons) {
            final JsonObject axis = axisElement.getAsJsonObject();
            if(!axis.has("baseline")) {
                continue;
            }
            final String baseline = axis.get("baseline").getAsString();
            JsonObject baselineFields = null;
            for(final JsonElement group : axis.getAsJsonArray("groups")) {
                if(baseline.equals(group.getAsJsonObject().get("value").getAsString())) {
                    baselineFields = group.getAsJsonObject().getAsJsonObject("fields");
                }
            }
            if(baselineFields == null) {
                continue;
            }
            for(final JsonElement groupElement : axis.getAsJsonArray("groups")) {
                final JsonObject group = groupElement.getAsJsonObject();
                if(baseline.equals(group.get("value").getAsString())) {
                    continue;
                }
                for(final String path : group.getAsJsonObject("fields").keySet()) {
                    final JsonObject target = group.getAsJsonObject("fields").getAsJsonObject(path);
                    final JsonObject base = baselineFields.getAsJsonObject(path);
                    if(base == null) {
                        continue;
                    }
                    final Double psi = groupPsi(base, target);
                    if(psi == null) {
                        continue;
                    }
                    target.addProperty("psi", psi);
                    if(target.has("hist") && base.has("hist")) {
                        target.addProperty("ks", binnedKs(
                                sharesOf(base.getAsJsonArray("hist"), base.get("count").getAsDouble()),
                                sharesOf(target.getAsJsonArray("hist"), target.get("count").getAsDouble())));
                    }
                    final JsonObject field = fieldMap.get(path);
                    if(field != null) {
                        final double previous = field.has("drift")
                                ? field.getAsJsonObject("drift").get("psi").getAsDouble()
                                : -1d;
                        if(psi > previous) {
                            final JsonObject drift = new JsonObject();
                            drift.addProperty("psi", psi);
                            drift.addProperty("vs", group.get("value").getAsString());
                            field.add("drift", drift);
                        }
                    }
                }
            }
        }
    }

    /** PSI between a baseline and a target group entry, from whichever aligned distribution both carry. */
    private static Double groupPsi(final JsonObject base, final JsonObject target) {
        if(base.has("hist") && target.has("hist")
                && base.getAsJsonArray("hist").size() == target.getAsJsonArray("hist").size()) {
            return psi(
                    sharesOf(target.getAsJsonArray("hist"), target.get("count").getAsDouble()),
                    sharesOf(base.getAsJsonArray("hist"), base.get("count").getAsDouble()));
        }
        if(base.has("topK") && target.has("topK")
                && base.getAsJsonArray("topK").size() == target.getAsJsonArray("topK").size()) {
            // top-K shares + an "other" bucket so the tail is represented
            final double[] a = withOtherBucket(sharesOf(target.getAsJsonArray("topK"), target.get("count").getAsDouble()));
            final double[] b = withOtherBucket(sharesOf(base.getAsJsonArray("topK"), base.get("count").getAsDouble()));
            return psi(a, b);
        }
        if(base.has("trueCount") && target.has("trueCount")) {
            final double baseTotal = base.get("trueCount").getAsDouble() + base.get("falseCount").getAsDouble();
            final double targetTotal = target.get("trueCount").getAsDouble() + target.get("falseCount").getAsDouble();
            if(baseTotal == 0 || targetTotal == 0) {
                return null;
            }
            return psi(
                    new double[] { target.get("trueCount").getAsDouble() / targetTotal, target.get("falseCount").getAsDouble() / targetTotal },
                    new double[] { base.get("trueCount").getAsDouble() / baseTotal, base.get("falseCount").getAsDouble() / baseTotal });
        }
        return null;
    }

    private static double[] withOtherBucket(final double[] shares) {
        final double[] out = new double[shares.length + 1];
        double sum = 0d;
        for(int i = 0; i < shares.length; i++) {
            out[i] = shares[i];
            sum += shares[i];
        }
        out[shares.length] = Math.max(0d, 1d - sum);
        return out;
    }

    // ---- declared field pairs (compare) ----

    private static final double[] QQ_RANKS;
    static {
        QQ_RANKS = new double[49];
        for(int i = 0; i < QQ_RANKS.length; i++) {
            QQ_RANKS[i] = 0.02 * (i + 1);
        }
    }

    private static JsonArray buildFieldPairs(
            final ProfileAccumulator accumulator, final ProfileSpec spec, final Config config) {

        final JsonArray pairs = new JsonArray();
        for(final String[] pair : config.comparePairs) {
            final JsonObject o = new JsonObject();
            o.addProperty("a", pair[0]);
            o.addProperty("b", pair[1]);
            final ProfileAccumulator.FieldAccumulator fieldA = fieldByPath(accumulator, spec, pair[0]);
            final ProfileAccumulator.FieldAccumulator fieldB = fieldByPath(accumulator, spec, pair[1]);
            if(fieldA == null || fieldB == null
                    || fieldA.count == 0 || fieldB.count == 0
                    || fieldA.getKll() == null || fieldA.getKll().isEmpty()
                    || fieldB.getKll() == null || fieldB.getKll().isEmpty()) {
                o.addProperty("error", "both fields need numeric observations to compare");
                pairs.add(o);
                continue;
            }
            final double min = Math.min(fieldA.min, fieldB.min);
            final double max = Math.max(fieldA.max, fieldB.max);
            final double[] edges = equalWidthEdges(min, max, config.overlayBins);
            final double[] sharesA = pmfOverEdges(fieldA.getKll(), edges);
            final double[] sharesB = pmfOverEdges(fieldB.getKll(), edges);
            final double[] cdfA = cdfOverEdges(fieldA.getKll(), edges);
            final double[] cdfB = cdfOverEdges(fieldB.getKll(), edges);
            o.add("edges", toJsonArray(edges));
            o.add("sharesA", toJsonArray(sharesA));
            o.add("sharesB", toJsonArray(sharesB));
            o.addProperty("countA", fieldA.count);
            o.addProperty("countB", fieldB.count);
            o.addProperty("psi", psi(sharesA, sharesB));
            double ks = 0d;
            for(int i = 0; i < cdfA.length; i++) {
                ks = Math.max(ks, Math.abs(cdfA[i] - cdfB[i]));
            }
            o.addProperty("ks", ks);
            final JsonArray qq = new JsonArray();
            for(final double rank : QQ_RANKS) {
                final JsonArray point = new JsonArray();
                point.add(fieldA.getKll().getQuantile(rank, QuantileSearchCriteria.INCLUSIVE));
                point.add(fieldB.getKll().getQuantile(rank, QuantileSearchCriteria.INCLUSIVE));
                qq.add(point);
            }
            o.add("qq", qq);
            pairs.add(o);
        }
        return pairs;
    }

    private static ProfileAccumulator.FieldAccumulator fieldByPath(
            final ProfileAccumulator accumulator, final ProfileSpec spec, final String path) {
        for(int i = 0; i < spec.getFields().size(); i++) {
            if(spec.getFields().get(i).path.equals(path)) {
                return accumulator.getField(i);
            }
        }
        return null;
    }

    private static JsonArray toJsonArray(final double[] values) {
        final JsonArray array = new JsonArray();
        for(final double value : values) {
            array.add(value);
        }
        return array;
    }

    // ---- compareWith (past report artifact) ----

    /**
     * Payload-level comparison against a past report's embedded blocks, plus Theta-sketch key
     * overlap when both reports embed sketch binaries (§6.3: v0 payloads must stay readable).
     */
    private static JsonObject buildCompareWith(
            final ProfileAccumulator accumulator,
            final PastReport past,
            final ProfileSpec spec,
            final Config config) {

        final JsonObject result = new JsonObject();
        result.addProperty("source", past.source);
        final List<String> warnings = new ArrayList<>();

        final JsonObject oldPayload = JsonParser.parseString(past.payloadJson).getAsJsonObject();
        final int oldVersion = oldPayload.has("formatVersion") ? oldPayload.get("formatVersion").getAsInt() : 0;
        if(oldVersion > PAYLOAD_FORMAT_VERSION) {
            warnings.add("past report has a newer payload format (v" + oldVersion + " > v" + PAYLOAD_FORMAT_VERSION + "); some comparisons may be incomplete");
        }
        if(past.manifestJson != null) {
            final JsonObject oldManifest = JsonParser.parseString(past.manifestJson).getAsJsonObject();
            if(oldManifest.has("job") && oldManifest.getAsJsonObject("job").has("generatedAt")) {
                result.addProperty("generatedAtOld", oldManifest.getAsJsonObject("job").get("generatedAt").getAsString());
            }
            if(oldManifest.has("sketchParameters")) {
                final JsonObject oldParams = oldManifest.getAsJsonObject("sketchParameters");
                final ProfileSpec.SketchParameters params = spec.getSketchParameters();
                if(oldParams.has("kllK") && oldParams.get("kllK").getAsInt() != params.kllK
                        || oldParams.has("cpcLgK") && oldParams.get("cpcLgK").getAsInt() != params.cpcLgK) {
                    warnings.add("sketch parameters differ between runs; comparison accuracy is limited by the coarser side");
                }
            }
        }
        final boolean oldHidden = oldPayload.has("values") && "hide".equals(oldPayload.get("values").getAsString());
        result.addProperty("rowsOld", oldPayload.get("rows").getAsLong());
        result.addProperty("rowsNew", accumulator.getRowCount());

        final java.util.Map<String, JsonObject> oldFields = new java.util.LinkedHashMap<>();
        for(final JsonElement field : oldPayload.getAsJsonArray("fields")) {
            oldFields.put(field.getAsJsonObject().get("path").getAsString(), field.getAsJsonObject());
        }
        final JsonObject oldSketchFields = past.sketchesJson == null ? null
                : JsonParser.parseString(past.sketchesJson).getAsJsonObject().has("fields")
                        ? JsonParser.parseString(past.sketchesJson).getAsJsonObject().getAsJsonObject("fields")
                        : null;

        final JsonArray fields = new JsonArray();
        final java.util.Set<String> seen = new java.util.HashSet<>();
        for(int i = 0; i < spec.getFields().size(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
            final ProfileAccumulator.FieldAccumulator field = accumulator.getField(i);
            seen.add(fieldSpec.path);
            final JsonObject o = new JsonObject();
            o.addProperty("path", fieldSpec.path);
            o.addProperty("type", fieldSpec.profileType.name().toLowerCase());
            final JsonObject old = oldFields.get(fieldSpec.path);
            if(old == null) {
                o.addProperty("status", "added");
                fields.add(o);
                continue;
            }
            if(!fieldSpec.profileType.name().toLowerCase().equals(old.get("type").getAsString())) {
                o.addProperty("status", "type_changed");
                o.addProperty("typeOld", old.get("type").getAsString());
                fields.add(o);
                continue;
            }
            o.addProperty("status", "common");
            compareCommonField(o, fieldSpec, field, old, accumulator, spec, config, oldHidden);
            if(fieldSpec.isKey && oldSketchFields != null && oldSketchFields.has(fieldSpec.path)) {
                final JsonObject oldSketch = oldSketchFields.getAsJsonObject(fieldSpec.path);
                if(oldSketch.has("theta")) {
                    try {
                        o.add("keyOverlap", compareKeySketches(field, oldSketch.get("theta").getAsString(), spec));
                    } catch (final Throwable e) {
                        warnings.add("failed to compare key sketches for " + fieldSpec.path + ": " + e.getMessage());
                    }
                }
            }
            fields.add(o);
        }
        for(final java.util.Map.Entry<String, JsonObject> entry : oldFields.entrySet()) {
            if(!seen.contains(entry.getKey())) {
                final JsonObject o = new JsonObject();
                o.addProperty("path", entry.getKey());
                o.addProperty("type", entry.getValue().get("type").getAsString());
                o.addProperty("status", "removed");
                fields.add(o);
            }
        }
        result.add("fields", fields);
        final JsonArray warningsArray = new JsonArray();
        for(final String warning : warnings) {
            warningsArray.add(warning);
        }
        result.add("warnings", warningsArray);
        return result;
    }

    private static void compareCommonField(
            final JsonObject o,
            final ProfileSpec.FieldSpec fieldSpec,
            final ProfileAccumulator.FieldAccumulator field,
            final JsonObject old,
            final ProfileAccumulator accumulator,
            final ProfileSpec spec,
            final Config config,
            final boolean oldHidden) {

        o.addProperty("nullRateOld", old.get("nullRate").getAsDouble());
        final long total = field.count + field.nullCount + field.errorCount + field.nanCount + field.infCount;
        o.addProperty("nullRateNew", total == 0 ? 0d : (double) field.nullCount / total);
        if(old.has("distinct")) {
            o.addProperty("distinctOld", old.getAsJsonObject("distinct").get("estimate").getAsDouble());
        }
        final CpcSketch cpc = field.cpcResult(spec.getSketchParameters());
        if(cpc != null && field.count > 0) {
            o.addProperty("distinctNew", cpc.getEstimate());
        }

        switch (fieldSpec.profileType) {
            case NUMERIC, ARRAY_LENGTH -> {
                final JsonObject oldNumeric = old.has("numeric") ? old.getAsJsonObject("numeric") : null;
                final KllDoublesSketch kll = field.getKll();
                if(oldNumeric == null || !oldNumeric.has("cdf") || kll == null || kll.isEmpty() || field.count == 0) {
                    return;
                }
                final JsonObject oldCdf = oldNumeric.getAsJsonObject("cdf");
                final double[] oldPoints = toDoubleArray(oldCdf.getAsJsonArray("points"));
                final double[] oldRanks = toDoubleArray(oldCdf.getAsJsonArray("ranks"));
                final double min = Math.min(field.min, oldNumeric.get("min").getAsDouble());
                final double max = Math.max(field.max, oldNumeric.get("max").getAsDouble());
                if(!(max > min) || oldPoints.length == 0) {
                    return;
                }
                final double[] edges = equalWidthEdges(min, max, config.overlayBins);
                final int bins = edges.length - 1;
                final double[] newShares = pmfOverEdges(kll, edges);
                final double[] newCdf = cdfOverEdges(kll, edges);
                final double[] oldShares = new double[bins];
                final double[] oldCdfAtEdges = new double[bins];
                double previous = 0d;
                for(int i = 0; i < bins; i++) {
                    final double rank = interpolateCdf(oldPoints, oldRanks, edges[i + 1]);
                    oldShares[i] = Math.max(0d, rank - previous);
                    oldCdfAtEdges[i] = rank;
                    previous = rank;
                }
                o.addProperty("psi", psi(newShares, oldShares));
                double ks = 0d;
                for(int i = 0; i < bins; i++) {
                    ks = Math.max(ks, Math.abs(newCdf[i] - oldCdfAtEdges[i]));
                }
                o.addProperty("ks", ks);
                if(oldNumeric.has("quantiles")) {
                    o.addProperty("p50Old", oldNumeric.getAsJsonObject("quantiles").get("p50").getAsDouble());
                    o.addProperty("p50New", kll.getQuantile(0.5, QuantileSearchCriteria.INCLUSIVE));
                }
                // both CDFs on the shared edges for the client-side overlay
                final JsonObject cdf = new JsonObject();
                cdf.add("edges", toJsonArray(edges));
                cdf.add("oldRanks", toJsonArray(oldCdfAtEdges));
                final double[] newCdfTrimmed = new double[bins];
                System.arraycopy(newCdf, 0, newCdfTrimmed, 0, bins);
                cdf.add("newRanks", toJsonArray(newCdfTrimmed));
                o.add("cdf", cdf);
            }
            case STRING -> {
                if(oldHidden || !config.showValues || !old.has("string")) {
                    return;
                }
                final JsonObject oldString = old.getAsJsonObject("string");
                final ItemsSketch<String> fi = field.getFrequentItems();
                if(!oldString.has("topK") || fi == null || field.count == 0) {
                    return;
                }
                // PSI over the past top-K labels' shares (+ other), estimated on both sides
                final JsonArray oldTopK = oldString.getAsJsonArray("topK");
                final long oldCount = old.get("count").getAsLong();
                if(oldCount == 0 || oldTopK.isEmpty()) {
                    return;
                }
                final List<Double> oldShares = new ArrayList<>();
                final List<Double> newShares = new ArrayList<>();
                for(final JsonElement item : oldTopK) {
                    final JsonObject itemObject = item.getAsJsonObject();
                    if(!itemObject.has("value")) {
                        return;   // past report was value-masked
                    }
                    oldShares.add(itemObject.get("count").getAsDouble() / oldCount);
                    newShares.add((double) fi.getEstimate(itemObject.get("value").getAsString()) / field.count);
                }
                final double[] a = withOtherBucket(newShares.stream().mapToDouble(Double::doubleValue).toArray());
                final double[] b = withOtherBucket(oldShares.stream().mapToDouble(Double::doubleValue).toArray());
                o.addProperty("psi", psi(a, b));
            }
            case BOOL -> {
                if(!old.has("bool") || field.count == 0) {
                    return;
                }
                final JsonObject oldBool = old.getAsJsonObject("bool");
                final double oldTotal = oldBool.get("trueCount").getAsDouble() + oldBool.get("falseCount").getAsDouble();
                if(oldTotal == 0) {
                    return;
                }
                o.addProperty("trueShareOld", oldBool.get("trueCount").getAsDouble() / oldTotal);
                o.addProperty("trueShareNew", (double) field.trueCount / field.count);
            }
        }
    }

    /** Theta set operations between the current key sketch and a past report's embedded one. */
    private static JsonObject compareKeySketches(
            final ProfileAccumulator.FieldAccumulator field,
            final String oldThetaBase64,
            final ProfileSpec spec) {

        final CompactSketch oldTheta = CompactSketch.heapify(
                org.apache.datasketches.memory.Memory.wrap(Base64.getDecoder().decode(oldThetaBase64)));
        final CompactSketch newTheta = field.thetaResult(spec.getSketchParameters());
        final JsonObject o = new JsonObject();
        o.addProperty("distinctOld", oldTheta.getEstimate());
        o.addProperty("distinctNew", newTheta == null ? 0d : newTheta.getEstimate());
        if(newTheta == null || newTheta.getEstimate() == 0d || oldTheta.getEstimate() == 0d) {
            return o;
        }
        final Intersection intersection = SetOperation.builder().buildIntersection();
        intersection.intersect(oldTheta);
        intersection.intersect(newTheta);
        final double inter = intersection.getResult().getEstimate();
        o.addProperty("intersection", inter);
        o.addProperty("retainedShare", inter / oldTheta.getEstimate());   // |old ∩ new| / |old|
        o.addProperty("newShare", 1d - inter / newTheta.getEstimate());   // share of new keys unseen before
        return o;
    }

    private static double[] toDoubleArray(final JsonArray array) {
        final double[] values = new double[array.size()];
        for(int i = 0; i < array.size(); i++) {
            values[i] = array.get(i).getAsDouble();
        }
        return values;
    }

    /** Gson's lenient parser accepts bare NaN/Infinity; they are not JSON and would break JSON.parse in the report. */
    private static JsonElement finiteOrNull(final JsonElement element) {
        if(element != null && element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()) {
            final double d = element.getAsDouble();
            if(Double.isNaN(d) || Double.isInfinite(d)) {
                return JsonNull.INSTANCE;
            }
        }
        return element;
    }

    private static JsonObject buildSample(
            final ProfileAccumulator accumulator, final ProfileSpec spec, final int maxRows) {

        final VarOptItemsSketch<String> sample = accumulator.sampleResult();
        if(sample == null || sample.getNumSamples() == 0) {
            return null;
        }
        final JsonObject result = new JsonObject();
        final JsonArray fields = new JsonArray();
        for(final ProfileSpec.FieldSpec fieldSpec : spec.getFields()) {
            fields.add(fieldSpec.path);
        }
        result.add("fields", fields);
        final JsonArray rows = new JsonArray();
        final VarOptItemsSamples<String> samples = sample.getSketchSamples();
        int added = 0;
        for(final VarOptItemsSamples<String>.WeightedSample weighted : samples) {
            if(added >= maxRows) {
                break;
            }
            try {
                final JsonObject row = JsonParser.parseString(weighted.getItem()).getAsJsonObject();
                final JsonArray values = new JsonArray();
                for(final ProfileSpec.FieldSpec fieldSpec : spec.getFields()) {
                    values.add(finiteOrNull(row.get(fieldSpec.path)));
                }
                rows.add(values);
                added += 1;
            } catch (final Throwable e) {
                // skip malformed sample rows
            }
        }
        result.add("rows", rows);
        result.addProperty("sampledFrom", sample.getN());
        return result;
    }

    private static JsonArray buildSuggestions(final ProfileAccumulator accumulator, final ProfileSpec spec) {
        final JsonArray suggestions = new JsonArray();
        final long rows = accumulator.getRowCount();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();

        // keys: distinct ≈ rows and ID-like naming
        final List<String> keyCandidates = new ArrayList<>();
        // segments: low-cardinality strings
        final List<String> segmentCandidates = new ArrayList<>();
        // time: single timestamp field
        final List<String> timestampFields = new ArrayList<>();
        // target: boolean or flag-like naming
        final List<String> targetCandidates = new ArrayList<>();

        for(int i = 0; i < accumulator.getFieldCount(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
            final ProfileAccumulator.FieldAccumulator field = accumulator.getField(i);
            switch (fieldSpec.profileType) {
                case NUMERIC, STRING -> {
                    final CpcSketch cpc = field.cpcResult(params);
                    if(cpc != null && rows > 100 && !fieldSpec.isKey
                            && cpc.getEstimate() >= 0.95 * rows
                            && KEY_NAME.matcher(lastName(fieldSpec.path)).matches()) {
                        keyCandidates.add(fieldSpec.path);
                    }
                    if(ProfileSpec.ProfileType.STRING.equals(fieldSpec.profileType)
                            && cpc != null && field.count > 0) {
                        final double distinct = cpc.getEstimate();
                        if(distinct >= 2 && distinct <= 50) {
                            segmentCandidates.add(fieldSpec.path);
                        }
                    }
                    if(TARGET_NAME.matcher(lastName(fieldSpec.path)).matches()) {
                        targetCandidates.add(fieldSpec.path);
                    }
                }
                case TIMESTAMP -> timestampFields.add(fieldSpec.path);
                case BOOL -> {
                    if(TARGET_NAME.matcher(lastName(fieldSpec.path)).matches()) {
                        targetCandidates.add(fieldSpec.path);
                    }
                }
                default -> { }
            }
        }

        if(!keyCandidates.isEmpty() && spec.getKeyFieldIndices().isEmpty()) {
            suggestions.add(suggestion("keys", keyCandidates,
                    "distinct count is close to the row count and the name looks like an identifier",
                    "keys: [" + String.join(", ", keyCandidates) + "]"));
        }
        for(int i = 0; i < segmentCandidates.size() && i < 3; i++) {
            final String path = segmentCandidates.get(i);
            suggestions.add(suggestion("segments", List.of(path),
                    "low-cardinality categorical field (2-50 distinct values)",
                    "segments: [" + path + "]"));
        }
        if(timestampFields.size() == 1) {
            suggestions.add(suggestion("time", timestampFields,
                    "the only timestamp field in the dataset",
                    "time: " + timestampFields.getFirst()));
        }
        if(!targetCandidates.isEmpty() && spec.getTarget() == null) {
            suggestions.add(suggestion("target", List.of(targetCandidates.getFirst()),
                    "flag-like field name",
                    "target: " + targetCandidates.getFirst()));
        }
        return suggestions;
    }

    private static JsonObject suggestion(
            final String kind, final List<String> fields, final String reason, final String yaml) {
        final JsonObject o = new JsonObject();
        o.addProperty("kind", kind);
        final JsonArray fieldArray = new JsonArray();
        for(final String field : fields) {
            fieldArray.add(field);
        }
        o.add("fields", fieldArray);
        o.addProperty("reason", reason);
        o.addProperty("yaml", yaml);
        return o;
    }

    private static String lastName(final String path) {
        final int index = path.lastIndexOf('.');
        return index < 0 ? path : path.substring(index + 1);
    }

    // ---- manifest ----

    private static JsonObject buildManifest(
            final ProfileAccumulator accumulator, final Config config, final List<String> degradations) {

        final ProfileSpec spec = accumulator.getSpec();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();

        final JsonObject manifest = new JsonObject();
        manifest.addProperty("formatVersion", PAYLOAD_FORMAT_VERSION);

        final JsonObject generator = new JsonObject();
        generator.addProperty("name", "mercari-pipeline profile sink");
        generator.addProperty("sketchLibrary", "org.apache.datasketches:datasketches-java:6.2.0");
        manifest.add("generator", generator);

        final JsonObject job = new JsonObject();
        job.addProperty("jobName", config.jobName);
        job.addProperty("moduleName", config.moduleName);
        final JsonArray inputs = new JsonArray();
        if(config.inputNames != null) {
            for(final String input : config.inputNames) {
                inputs.add(input);
            }
        }
        job.add("inputs", inputs);
        job.addProperty("generatedAt", Instant.now().toString());
        manifest.add("job", job);

        manifest.addProperty("rows", accumulator.getRowCount());
        manifest.addProperty("errorRows", accumulator.getErrorCount());

        final JsonObject sketchParameters = new JsonObject();
        sketchParameters.addProperty("kllK", params.kllK);
        sketchParameters.addProperty("cpcLgK", params.cpcLgK);
        sketchParameters.addProperty("fiMaxMapSize", params.fiMaxMapSize);
        sketchParameters.addProperty("thetaLgK", params.thetaLgK);
        sketchParameters.addProperty("sampleK", params.sampleK);
        sketchParameters.addProperty("topKKeep", params.topKKeep);
        manifest.add("sketchParameters", sketchParameters);

        if(config.expandedParametersJson != null) {
            manifest.add("expandedParameters", JsonParser.parseString(config.expandedParametersJson));
        }

        final JsonArray schemaSnapshot = new JsonArray();
        for(final ProfileSpec.FieldSpec fieldSpec : spec.getFields()) {
            final JsonObject o = new JsonObject();
            o.addProperty("path", fieldSpec.path);
            o.addProperty("sourceType", fieldSpec.sourceType);
            o.addProperty("profileType", fieldSpec.profileType.name().toLowerCase());
            schemaSnapshot.add(o);
        }
        manifest.add("schemaSnapshot", schemaSnapshot);

        final JsonArray degradationArray = new JsonArray();
        for(final String degradation : degradations) {
            degradationArray.add(degradation);
        }
        manifest.add("degradations", degradationArray);
        if(config.sketchesOutput != null) {
            manifest.addProperty("sketchesOutput", config.sketchesOutput);
        }
        if(config.compareWithSource != null) {
            manifest.addProperty("compareWith", config.compareWithSource);
        }
        return manifest;
    }

    // ---- sketches ----

    /**
     * Base64 sketch binaries per field. With {@code values: hide}, item-bearing sketches
     * (frequent items, sample) are excluded because their binaries contain raw values.
     */
    public static JsonObject buildSketches(final ProfileAccumulator accumulator, final Config config) {
        final ProfileSpec spec = accumulator.getSpec();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();
        final Base64.Encoder encoder = Base64.getEncoder();

        final JsonObject sketches = new JsonObject();
        sketches.addProperty("formatVersion", PAYLOAD_FORMAT_VERSION);
        final JsonObject fields = new JsonObject();
        for(int i = 0; i < accumulator.getFieldCount(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
            final ProfileAccumulator.FieldAccumulator field = accumulator.getField(i);
            final JsonObject o = new JsonObject();
            final KllDoublesSketch kll = field.getKll();
            if(kll != null && !kll.isEmpty()) {
                o.addProperty("kll", encoder.encodeToString(kll.toByteArray()));
            }
            final CpcSketch cpc = field.cpcResult(params);
            if(cpc != null && field.count > 0) {
                o.addProperty("cpc", encoder.encodeToString(cpc.toByteArray()));
            }
            if(config.showValues) {
                final ItemsSketch<String> fi = field.getFrequentItems();
                if(fi != null && field.count > 0) {
                    o.addProperty("fi", encoder.encodeToString(fi.toByteArray(new org.apache.datasketches.common.ArrayOfStringsSerDe())));
                }
            }
            final CompactSketch theta = field.thetaResult(params);
            if(theta != null) {
                o.addProperty("theta", encoder.encodeToString(theta.toByteArray()));
            }
            if(!o.keySet().isEmpty()) {
                fields.add(fieldSpec.path, o);
            }
        }
        sketches.add("fields", fields);
        if(config.showValues && spec.isSampleEnabled()) {
            final VarOptItemsSketch<String> sample = accumulator.sampleResult();
            if(sample != null && sample.getNumSamples() > 0) {
                sketches.addProperty("sample", encoder.encodeToString(
                        sample.toByteArray(new org.apache.datasketches.common.ArrayOfStringsSerDe())));
            }
        }
        return sketches;
    }

    // ---- static fallback (readable without JavaScript) ----

    private static String buildStaticTable(final ProfileAccumulator accumulator) {
        final ProfileSpec spec = accumulator.getSpec();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();
        final StringBuilder sb = new StringBuilder();
        sb.append("<table><thead><tr>")
                .append("<th>field</th><th>type</th><th>count</th><th>null</th><th>distinct&asymp;</th><th>min</th><th>max</th><th>mean</th>")
                .append("</tr></thead><tbody>");
        for(int i = 0; i < accumulator.getFieldCount(); i++) {
            final ProfileSpec.FieldSpec fieldSpec = spec.getFields().get(i);
            final ProfileAccumulator.FieldAccumulator field = accumulator.getField(i);
            sb.append("<tr><td>").append(escapeHtml(fieldSpec.path)).append("</td>")
                    .append("<td>").append(fieldSpec.profileType.name().toLowerCase()).append("</td>")
                    .append("<td>").append(field.count).append("</td>")
                    .append("<td>").append(field.nullCount).append("</td>");
            final CpcSketch cpc = field.cpcResult(params);
            sb.append("<td>").append(cpc != null && field.count > 0 ? String.format("%.0f", cpc.getEstimate()) : "-").append("</td>");
            if(field.count > 0 && Double.isFinite(field.min)) {
                sb.append("<td>").append(field.min).append("</td>")
                        .append("<td>").append(field.max).append("</td>")
                        .append("<td>").append(String.format("%.4g", field.mean)).append("</td>");
            } else {
                sb.append("<td>-</td><td>-</td><td>-</td>");
            }
            sb.append("</tr>");
        }
        sb.append("</tbody></table>");
        return sb.toString();
    }

    private static String escapeHtml(final String text) {
        if(text == null) {
            return "";
        }
        return text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
