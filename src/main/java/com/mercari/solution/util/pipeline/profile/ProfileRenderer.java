package com.mercari.solution.util.pipeline.profile;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.sampling.VarOptItemsSamples;
import org.apache.datasketches.sampling.VarOptItemsSketch;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Renders a {@link ProfileReport.Result} into the report's payload and manifest JSON and the
 * single-file HTML report (profile-dsl.md §10). Every statistic comes from the model; the renderer
 * only adds what is drawn and not recorded — the whole-dataset histogram and CDF, the correlation
 * matrix, the sample rows, the suggestions. When the payload exceeds the size limit it sheds, in a
 * fixed order, sample rows → the correlation matrix (down to its strongest pairs) → histogram
 * resolution → comparison groups → comparison resolution, and records each step.
 */
public class ProfileRenderer {

    private static final String TEMPLATE_RESOURCE = "/profile/report_template.html";
    private static final Pattern KEY_NAME = Pattern.compile(".*(_|^)(id|key|code|uuid)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TARGET_NAME = Pattern.compile("^(is_.*|has_.*|.*_flag|flag|label|target)$", Pattern.CASE_INSENSITIVE);

    private static final String[] PAYLOAD_QUANTILE_NAMES = { "p1", "p5", "p25", "p50", "p75", "p95", "p99" };
    private static final int VALUES_IN_PAYLOAD = 50;
    private static final int CORRELATION_TOP_PAIRS = 500;

    private static final double[] QQ_RANKS;
    static {
        QQ_RANKS = new double[49];
        for(int i = 0; i < QQ_RANKS.length; i++) {
            QQ_RANKS[i] = 0.02 * (i + 1);
        }
    }

    public static class Rendered {
        public String html;
        public String payloadJson;
        public String manifestJson;
        public List<String> degradations = new ArrayList<>();
    }

    /** How much of the drawn data one payload build carries: the steps of the size ladder. */
    private static class Detail {
        int sampleRows;
        int histogramBins;
        boolean correlationMatrix = true;
        int groupLimit = Integer.MAX_VALUE;
        boolean overlayCells = true;        // false: group distributions over the bins instead of the cells
        boolean overlayDistributions = true;
    }

    public static Rendered render(
            final ProfileReport.Result result,
            final ProfileAccumulator accumulator,
            final ProfileReport.Config config) {

        final Rendered rendered = new Rendered();
        final Detail detail = new Detail();
        detail.sampleRows = config.sampleRowsInPayload;
        detail.histogramBins = config.histogramBins;

        final long limit = config.embedLimitBytes;
        String payload = buildPayload(result, accumulator, config, detail).toString();
        if(payload.length() > limit && detail.sampleRows > 100) {
            detail.sampleRows = 100;
            payload = buildPayload(result, accumulator, config, detail).toString();
            rendered.degradations.add("sample rows in the report reduced to 100 (size limit exceeded)");
        }
        if(payload.length() > limit && accumulator.getSpec().isCorrelationEnabled()) {
            detail.correlationMatrix = false;
            payload = buildPayload(result, accumulator, config, detail).toString();
            rendered.degradations.add("correlation matrix reduced to its " + CORRELATION_TOP_PAIRS + " strongest pairs (size limit exceeded)");
        }
        if(payload.length() > limit && detail.histogramBins > 64) {
            detail.histogramBins = 64;
            payload = buildPayload(result, accumulator, config, detail).toString();
            rendered.degradations.add("histogram resolution reduced to 64 bins (size limit exceeded)");
        }
        final boolean withComparisons = !result.axes.isEmpty() || (result.target != null && result.target.positiveGroup != null);
        if(payload.length() > limit && !result.axes.isEmpty()) {
            detail.groupLimit = 5;
            payload = buildPayload(result, accumulator, config, detail).toString();
            rendered.degradations.add("comparison groups in the report limited to 5 per axis (size limit exceeded)");
        }
        if(payload.length() > limit && withComparisons) {
            detail.overlayCells = false;
            payload = buildPayload(result, accumulator, config, detail).toString();
            rendered.degradations.add("comparison distributions in the report reduced to the bins (size limit exceeded)");
        }
        if(payload.length() > limit && withComparisons) {
            detail.overlayDistributions = false;
            payload = buildPayload(result, accumulator, config, detail).toString();
            rendered.degradations.add("comparison distributions dropped from the report, statistics kept (size limit exceeded)");
        }
        if(payload.length() > limit) {
            rendered.degradations.add("report payload still exceeds the size limit by "
                    + (payload.length() - limit) + " bytes after every reduction");
        }

        final String manifest = buildManifest(result, accumulator, config, rendered.degradations).toString();
        rendered.html = loadTemplate()
                .replace("__PROFILE_TITLE__", escapeHtml(config.title))
                .replace("__PROFILE_STATIC__", buildStaticTable(result))
                .replace("__PROFILE_PAYLOAD__", embedJson(payload))
                .replace("__PROFILE_MANIFEST__", embedJson(manifest));
        rendered.payloadJson = payload;
        rendered.manifestJson = manifest;
        return rendered;
    }

    /** The payload and the manifest as one JSON document ({@code output.payload}, profile-dsl.md §10.2). */
    public static String payloadFile(final Rendered rendered) {
        final JsonObject file = new JsonObject();
        file.addProperty("formatVersion", ProfileReport.FORMAT_VERSION);
        file.add("payload", JsonParser.parseString(rendered.payloadJson));
        file.add("manifest", JsonParser.parseString(rendered.manifestJson));
        return file.toString();
    }

    /**
     * Makes JSON text safe to splice into a {@code <script type="application/json">} block: a data
     * value containing {@code </script>} (or {@code <!--}) would otherwise terminate the block early
     * and hand the rest of the value to the HTML parser. {@code <} only ever occurs inside JSON
     * strings, and the escaped form is the same string to JSON.parse, so the payload is unchanged.
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

    private static double[] interiorPoints(final double[] edges) {
        final double[] points = new double[Math.max(0, edges.length - 2)];
        System.arraycopy(edges, 1, points, 0, points.length);
        return points;
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
            final ProfileReport.Result result,
            final ProfileAccumulator accumulator,
            final ProfileReport.Config config,
            final Detail detail) {

        final ProfileSpec spec = accumulator.getSpec();
        final JsonObject payload = new JsonObject();
        payload.addProperty("formatVersion", ProfileReport.FORMAT_VERSION);
        payload.addProperty("title", config.title);
        payload.addProperty("values", config.showValues ? "show" : "hide");
        payload.addProperty("rows", result.rows);
        payload.addProperty("errorRows", result.errorRows);
        payload.addProperty("topKShow", config.topKShow);
        payload.addProperty("bins", config.binsCount);

        final JsonObject notableCounts = new JsonObject();
        for(final Map.Entry<String, Long> entry : result.notableCounts().entrySet()) {
            notableCounts.addProperty(entry.getKey(), entry.getValue());
        }
        payload.add("notableCounts", notableCounts);

        final JsonArray fields = new JsonArray();
        for(final ProfileReport.FieldResult field : result.fields) {
            fields.add(buildField(field, accumulator.getField(field.index), config, detail));
        }
        payload.add("fields", fields);

        if(result.target != null) {
            payload.add("target", buildTarget(result, config, detail));
        }
        final JsonArray comparisons = new JsonArray();
        for(final ProfileReport.AxisResult axis : result.axes) {
            comparisons.add(buildAxis(axis, result, config, detail));
        }
        if(result.target != null && result.target.positiveGroup != null) {
            comparisons.add(buildTargetAxis(result, config, detail));
        }
        if(!comparisons.isEmpty()) {
            payload.add("comparisons", comparisons);
        }
        if(!result.pairs.isEmpty()) {
            payload.add("fieldPairs", buildPairs(result, accumulator));
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
            payload.add("correlations", buildCorrelations(accumulator, spec, detail.correlationMatrix));
        }
        if(!result.keys.isEmpty()) {
            final JsonObject keys = new JsonObject();
            final JsonArray keyFields = new JsonArray();
            for(final ProfileReport.KeyResult key : result.keys) {
                final JsonObject o = new JsonObject();
                o.addProperty("path", key.key);
                o.addProperty("distinct", key.distinct);
                o.addProperty("distinctLower", key.distinctLower);
                o.addProperty("distinctUpper", key.distinctUpper);
                o.addProperty("keyness", key.keyness);
                o.addProperty("nullKeys", key.nullKeys);
                keyFields.add(o);
            }
            keys.add("fields", keyFields);
            payload.add("keys", keys);
        }
        if(config.showValues && spec.isSampleEnabled()) {
            final JsonObject sample = buildSample(accumulator, spec, detail.sampleRows);
            if(sample != null) {
                payload.add("sample", sample);
            }
        }
        payload.add("suggestions", buildSuggestions(accumulator, spec));
        return payload;
    }

    private static JsonObject buildField(
            final ProfileReport.FieldResult r,
            final ProfileAccumulator.FieldAccumulator field,
            final ProfileReport.Config config,
            final Detail detail) {

        final JsonObject o = new JsonObject();
        o.addProperty("path", r.path);
        o.addProperty("type", r.type);
        o.addProperty("sourceType", r.sourceType);
        o.addProperty("isKey", r.isKey);
        o.addProperty("count", r.count);
        o.addProperty("nullCount", r.nulls);
        o.addProperty("errorCount", r.errors);
        o.addProperty("nullRate", r.nullRate);
        if(r.distinct != null) {
            final JsonObject distinct = new JsonObject();
            distinct.addProperty("estimate", r.distinct);
            distinct.addProperty("lower", r.distinctLower);
            distinct.addProperty("upper", r.distinctUpper);
            distinct.addProperty("exact", r.distinctExact);
            o.add("distinct", distinct);
        }

        switch (r.type) {
            case "numeric", "array" -> {
                final JsonObject numeric = new JsonObject();
                numeric.addProperty("zeroCount", r.zeros);
                numeric.addProperty("nanCount", r.nans);
                numeric.addProperty("infCount", r.infs);
                if(r.count > 0) {
                    numeric.addProperty("min", r.min);
                    numeric.addProperty("max", r.max);
                    numeric.addProperty("sum", r.sum);
                    numeric.addProperty("mean", r.mean);
                    numeric.addProperty("stddev", r.stddev);
                    numeric.addProperty("skewness", r.skewness);
                    if(r.quantiles != null) {
                        final JsonObject quantiles = new JsonObject();
                        for(int q = 0; q < PAYLOAD_QUANTILE_NAMES.length; q++) {
                            quantiles.addProperty(PAYLOAD_QUANTILE_NAMES[q], r.quantiles[q]);
                        }
                        numeric.add("quantiles", quantiles);
                        numeric.addProperty("rankError", r.rankError);
                    }
                    final KllDoublesSketch kll = field.getKll();
                    if(kll != null && !kll.isEmpty()) {
                        numeric.add("histogram", buildHistogram(kll, field.min, field.max, detail.histogramBins, field.count));
                        numeric.add("cdf", buildCdf(kll, field.min, field.max, detail.histogramBins));
                    }
                    if("exact".equals(r.valuesKind) && config.showValues) {
                        // a discrete numeric field: its values are readable as a table, not as histogram bars
                        // (left out with values: hide, as the values output writes them as ranks)
                        numeric.add("values", valuesJson(r, true, VALUES_IN_PAYLOAD));
                        numeric.addProperty("valuesTotal", r.values.size());
                    }
                }
                o.add("numeric", numeric);
            }
            case "string" -> {
                final JsonObject string = new JsonObject();
                string.addProperty("emptyCount", r.empties);
                string.addProperty("blankCount", r.blanks);
                string.addProperty("nullLikeCount", r.nullLike);
                string.addProperty("nullLikeRate", r.nullLikeRate);
                if(r.lengthMin != null) {
                    final JsonObject length = new JsonObject();
                    length.addProperty("min", r.lengthMin);
                    length.addProperty("max", r.lengthMax);
                    length.addProperty("p50", r.lengthP50);
                    length.addProperty("p95", r.lengthP95);
                    string.add("length", length);
                }
                if(r.count > 0) {
                    string.add("topK", valuesJson(r, config.showValues, VALUES_IN_PAYLOAD));
                    string.addProperty("topKKind", r.valuesKind);
                    string.addProperty("valuesTotal", r.values.size());
                    if(r.valuesMaximumError != null) {
                        string.addProperty("maximumError", r.valuesMaximumError);
                    }
                }
                o.add("string", string);
            }
            case "bool" -> {
                final JsonObject bool = new JsonObject();
                bool.addProperty("trueCount", r.trues);
                bool.addProperty("falseCount", r.falses);
                o.add("bool", bool);
            }
            default -> {
                final JsonObject ts = new JsonObject();
                if(r.count > 0) {
                    ts.addProperty("min", Instant.ofEpochMilli(r.min.longValue()).toString());
                    ts.addProperty("max", Instant.ofEpochMilli(r.max.longValue()).toString());
                    final KllDoublesSketch kll = field.getKll();
                    if(kll != null && !kll.isEmpty()) {
                        ts.add("histogram", buildHistogram(kll, field.min, field.max, config.timestampBins, field.count));
                    }
                }
                o.add("timestamp", ts);
            }
        }

        final JsonArray notable = new JsonArray();
        r.notable.forEach(notable::add);
        o.add("notable", notable);

        if(r.cells != null && detail.overlayDistributions) {
            final JsonObject overlay = new JsonObject();
            if(r.numericLike()) {
                overlay.add("edges", toJsonArray(detail.overlayCells ? r.cellEdges : finiteBinEdges(r)));
                overlay.add("all", toJsonArray(detail.overlayCells ? r.cells : r.bins));
            } else if("string".equals(r.type)) {
                // the "(other)" cell is kept out of the overlay: it would dwarf the named values
                final JsonArray labels = new JsonArray();
                final JsonArray counts = new JsonArray();
                for(int c = 0; c < r.cellLabels.length - 1; c++) {
                    labels.add(ProfileReport.binLabel(r, c, config.showValues));
                    counts.add(r.cells[c]);
                }
                overlay.add("labels", labels);
                overlay.add("all", counts);
            }
            if(!overlay.keySet().isEmpty()) {
                o.add("overlay", overlay);
            }
        }
        if(r.association != null) {
            final JsonObject target = new JsonObject();
            target.addProperty("association", r.association);
            target.addProperty("kind", r.associationKind);
            o.add("target", target);
        }
        if(r.drift != null || r.nullShift != null) {
            final JsonObject drift = new JsonObject();
            drift.addProperty("value", r.drift);
            drift.addProperty("kind", r.driftKind);
            drift.addProperty("vs", r.driftVs);
            drift.addProperty("nullShift", r.nullShift);
            o.add("drift", drift);
        }
        return o;
    }

    private static JsonArray valuesJson(final ProfileReport.FieldResult r, final boolean showValues, final int limit) {
        final JsonArray values = new JsonArray();
        for(int i = 0; i < r.values.size() && i < limit; i++) {
            final ProfileReport.ValueRow row = r.values.get(i);
            final JsonObject item = new JsonObject();
            if(showValues) {
                item.addProperty("value", ProfileReport.shorten(row.value));
            }
            item.addProperty("count", row.count);
            item.addProperty("lower", row.lower);
            item.addProperty("upper", row.upper);
            if(ProfileReport.isNullLike(row.value) && "string".equals(r.type)) {
                item.addProperty("nullLike", true);
            }
            values.add(item);
        }
        return values;
    }

    /** The bin edges of a numeric-like field as finite numbers: the open ends of declared edges become the observed range. */
    private static double[] finiteBinEdges(final ProfileReport.FieldResult r) {
        final double min = r.cellEdges[0];
        final double max = r.cellEdges[r.cellEdges.length - 1];
        final double[] edges = new double[r.bins.length + 1];
        for(int b = 0; b < r.bins.length; b++) {
            edges[b] = r.binLower[b] == null ? Math.min(min, r.binUpper[b] == null ? min : r.binUpper[b]) : r.binLower[b];
        }
        final Double last = r.binUpper[r.bins.length - 1];
        edges[r.bins.length] = last == null ? Math.max(max, edges[r.bins.length - 1]) : last;
        return edges;
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
            for(final double p : kll.getPMF(interiorPoints(edges), QuantileSearchCriteria.INCLUSIVE)) {
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

    // ---- comparisons ----

    /** One field of one group as the report draws it: totals, the drift statistics and the aligned distribution. */
    private static JsonObject groupFieldJson(
            final ProfileReport.FieldResult field, final ProfileReport.GroupField g, final Detail detail) {

        final JsonObject o = new JsonObject();
        o.addProperty("count", g.count);
        o.addProperty("nulls", g.nulls);
        final boolean time = "timestamp".equals(field.type);
        if(!time) {
            o.addProperty("mean", g.mean);
            o.addProperty("min", g.min);
            o.addProperty("max", g.max);
            o.addProperty("p50", g.p50);
        }
        o.addProperty("ks", g.ks);
        o.addProperty("tvd", g.tvd);
        o.addProperty("psi", g.psi);
        o.addProperty("nullShift", g.nullShift);
        o.addProperty("noiseKs", g.noiseKs);
        o.addProperty("noisePsi", g.noisePsi);
        if("bool".equals(field.type)) {
            o.addProperty("trueCount", g.cells[0]);
            o.addProperty("falseCount", g.cells[1]);
        } else if(detail.overlayDistributions) {
            if(field.numericLike()) {
                o.add("hist", toJsonArray(detail.overlayCells ? g.cells : g.bins));
            } else {
                final JsonArray topK = new JsonArray();
                for(int c = 0; c < g.cells.length - 1; c++) {
                    topK.add(g.cells[c]);
                }
                o.add("topK", topK);
            }
        }
        return o;
    }

    private static JsonObject groupJson(
            final ProfileReport.GroupResult group,
            final String label,
            final ProfileReport.Result result,
            final Detail detail) {

        final JsonObject groupJson = new JsonObject();
        groupJson.addProperty("value", label);
        groupJson.addProperty("rows", group.rows);
        if(group.targetRate != null) {
            groupJson.addProperty("targetPositive", group.targetPositive);
            groupJson.addProperty("targetRate", group.targetRate);
        }
        final JsonObject fields = new JsonObject();
        for(final ProfileReport.FieldResult field : result.fields) {
            final ProfileReport.GroupField g = group.fields[field.index];
            if(g != null) {
                fields.add(field.path, groupFieldJson(field, g, detail));
            }
        }
        groupJson.add("fields", fields);
        return groupJson;
    }

    private static JsonObject buildAxis(
            final ProfileReport.AxisResult axis,
            final ProfileReport.Result result,
            final ProfileReport.Config config,
            final Detail detail) {

        final JsonObject axisJson = new JsonObject();
        axisJson.addProperty("kind", axis.axis.kind.name());
        axisJson.addProperty("field", axis.axis.field);
        if(ProfileAxis.Kind.time.equals(axis.axis.kind)) {
            axisJson.addProperty("granularity", axis.axis.granularity);
        }
        if(ProfileAxis.Kind.inputs.equals(axis.axis.kind) && axis.axis.baseline != null) {
            axisJson.addProperty("baseline", axis.axis.baseline);
        }
        // the size ladder keeps the first groups (the largest segments, every input) or the most recent buckets
        int from = 0;
        int to = axis.groups.size();
        if(to > detail.groupLimit && !ProfileAxis.Kind.inputs.equals(axis.axis.kind)) {
            if(ProfileAxis.Kind.time.equals(axis.axis.kind)) {
                from = to - detail.groupLimit;
            } else {
                to = detail.groupLimit;
            }
        }
        axisJson.addProperty("truncatedGroups", axis.truncatedGroups + (axis.groups.size() - (to - from)));
        final JsonArray groups = new JsonArray();
        for(int g = from; g < to; g++) {
            groups.add(groupJson(axis.groups.get(g),
                    ProfileReport.groupLabel(axis.axis, axis.groups.get(g).value, g, config.showValues), result, detail));
        }
        axisJson.add("groups", groups);
        return axisJson;
    }

    /** The two target classes as a comparison axis, so the compare bar overlays them like any other groups. */
    private static JsonObject buildTargetAxis(
            final ProfileReport.Result result, final ProfileReport.Config config, final Detail detail) {

        final JsonObject axisJson = new JsonObject();
        axisJson.addProperty("kind", "target");
        axisJson.addProperty("field", result.target.field);
        axisJson.addProperty("positive", result.target.positive);
        axisJson.addProperty("truncatedGroups", 0);
        final JsonArray groups = new JsonArray();
        groups.add(groupJson(result.target.positiveGroup, "positive", result, detail));
        groups.add(groupJson(result.target.negativeGroup, "negative", result, detail));
        axisJson.add("groups", groups);
        return axisJson;
    }

    private static JsonObject buildTarget(
            final ProfileReport.Result result, final ProfileReport.Config config, final Detail detail) {

        final ProfileReport.TargetResult t = result.target;
        final JsonObject target = new JsonObject();
        target.addProperty("field", t.field);
        target.addProperty("positive", t.positive);
        target.addProperty("positiveRows", t.positiveRows);
        target.addProperty("negativeRows", t.negativeRows);
        target.addProperty("nullRows", t.nullRows);
        target.addProperty("rate", t.rate);
        if(t.warning != null) {
            target.addProperty("warning", t.warning);
        }
        final JsonArray fields = new JsonArray();
        for(final ProfileReport.TargetField f : t.fields) {
            final ProfileReport.FieldResult field = result.fields.get(f.index);
            final JsonObject o = new JsonObject();
            o.addProperty("path", field.path);
            o.addProperty("type", field.type);
            o.addProperty("count", f.count);
            if(f.nullRows > 0) {
                final JsonObject nulls = new JsonObject();
                nulls.addProperty("rows", f.nullRows);
                nulls.addProperty("rate", f.rateWhenNull);
                o.add("nulls", nulls);
            }
            o.addProperty("ks", f.ks);
            o.addProperty("tvd", f.tvd);
            o.addProperty("iv", f.iv);
            o.addProperty("pointBiserial", f.pointBiserial);
            if(!"timestamp".equals(field.type)) {
                o.addProperty("meanPositive", f.meanPositive);
                o.addProperty("meanNegative", f.meanNegative);
            }
            o.addProperty("edgesKind", field.edgesKind);
            if(f.iv != null && detail.overlayDistributions) {
                if(field.numericLike()) {
                    o.add("edges", toJsonArray(finiteBinEdges(field)));
                } else {
                    final JsonArray labels = new JsonArray();
                    for(int b = 0; b < field.binLabels.length; b++) {
                        labels.add(ProfileReport.binLabel(field, b, config.showValues));
                    }
                    o.add("labels", labels);
                }
                o.add("positive", toJsonArray(f.positiveBins));
                o.add("negative", toJsonArray(f.negativeBins));
            }
            fields.add(o);
        }
        target.add("fields", fields);
        return target;
    }

    private static JsonArray buildPairs(final ProfileReport.Result result, final ProfileAccumulator accumulator) {
        final ProfileSpec spec = accumulator.getSpec();
        final JsonArray pairs = new JsonArray();
        for(final ProfileReport.PairResult pair : result.pairs) {
            final JsonObject o = new JsonObject();
            o.addProperty("a", pair.a);
            o.addProperty("b", pair.b);
            if(pair.error != null) {
                o.addProperty("error", pair.error);
                pairs.add(o);
                continue;
            }
            o.add("edges", toJsonArray(pair.edges));
            o.add("sharesA", sharesJson(pair.binsA));
            o.add("sharesB", sharesJson(pair.binsB));
            o.addProperty("countA", pair.countA);
            o.addProperty("countB", pair.countB);
            o.addProperty("psi", pair.psi);
            o.addProperty("ks", pair.ks);
            o.addProperty("noiseKs", pair.noiseKs);
            o.addProperty("noisePsi", pair.noisePsi);
            // the Q-Q plot is drawn from the two quantile sketches (profile-dsl.md §5.12)
            final KllDoublesSketch kllA = sketchOf(accumulator, spec, pair.a);
            final KllDoublesSketch kllB = sketchOf(accumulator, spec, pair.b);
            if(kllA != null && kllB != null) {
                final JsonArray qq = new JsonArray();
                for(final double rank : QQ_RANKS) {
                    final JsonArray point = new JsonArray();
                    point.add(kllA.getQuantile(rank, QuantileSearchCriteria.INCLUSIVE));
                    point.add(kllB.getQuantile(rank, QuantileSearchCriteria.INCLUSIVE));
                    qq.add(point);
                }
                o.add("qq", qq);
            }
            pairs.add(o);
        }
        return pairs;
    }

    private static KllDoublesSketch sketchOf(final ProfileAccumulator accumulator, final ProfileSpec spec, final String path) {
        for(int i = 0; i < spec.getFields().size(); i++) {
            if(spec.getFields().get(i).path.equals(path)) {
                final KllDoublesSketch kll = accumulator.getField(i).getKll();
                return kll == null || kll.isEmpty() ? null : kll;
            }
        }
        return null;
    }

    private static JsonArray sharesJson(final long[] counts) {
        final long total = ProfileReport.total(counts);
        final JsonArray shares = new JsonArray();
        for(final long count : counts) {
            shares.add(total == 0 ? 0d : (double) count / total);
        }
        return shares;
    }

    private static JsonArray toJsonArray(final double[] values) {
        final JsonArray array = new JsonArray();
        for(final double value : values) {
            array.add(value);
        }
        return array;
    }

    private static JsonArray toJsonArray(final long[] values) {
        final JsonArray array = new JsonArray();
        for(final long value : values) {
            array.add(value);
        }
        return array;
    }

    // ---- correlations / sample / suggestions ----

    private static JsonObject buildCorrelations(
            final ProfileAccumulator accumulator, final ProfileSpec spec, final boolean matrixForm) {

        final List<Integer> numericIndices = spec.getNumericFieldIndices();
        final JsonObject correlations = new JsonObject();
        final JsonArray fieldPaths = new JsonArray();
        for(final Integer index : numericIndices) {
            fieldPaths.add(spec.getFields().get(index).path);
        }
        correlations.add("fields", fieldPaths);
        if(!matrixForm) {
            // the strongest pairs only: [i, j, correlation, rows]
            final List<double[]> pairs = new ArrayList<>();
            for(int i = 0; i < numericIndices.size(); i++) {
                for(int j = i + 1; j < numericIndices.size(); j++) {
                    final Double correlation = accumulator.correlation(i, j);
                    if(correlation != null && Double.isFinite(correlation)) {
                        final Double pairCount = accumulator.pairCount(i, j);
                        pairs.add(new double[] { i, j, correlation, pairCount == null ? 0 : pairCount });
                    }
                }
            }
            pairs.sort((a, b) -> Double.compare(Math.abs(b[2]), Math.abs(a[2])));
            final JsonArray top = new JsonArray();
            for(int p = 0; p < pairs.size() && p < CORRELATION_TOP_PAIRS; p++) {
                final JsonArray pair = new JsonArray();
                pair.add((int) pairs.get(p)[0]);
                pair.add((int) pairs.get(p)[1]);
                pair.add(pairs.get(p)[2]);
                pair.add((long) pairs.get(p)[3]);
                top.add(pair);
            }
            correlations.add("top", top);
            return correlations;
        }
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

    /** Declarations the run could have used, inferred from names and cardinalities (profile-dsl.md §10.1). */
    private static JsonArray buildSuggestions(final ProfileAccumulator accumulator, final ProfileSpec spec) {
        final JsonArray suggestions = new JsonArray();
        final long rows = accumulator.getRowCount();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();

        final List<String> keyCandidates = new ArrayList<>();
        final List<String> segmentCandidates = new ArrayList<>();
        final List<String> timestampFields = new ArrayList<>();
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
            final ProfileReport.Result result,
            final ProfileAccumulator accumulator,
            final ProfileReport.Config config,
            final List<String> degradations) {

        final ProfileSpec spec = accumulator.getSpec();
        final ProfileSpec.SketchParameters params = spec.getSketchParameters();

        final JsonObject manifest = new JsonObject();
        manifest.addProperty("formatVersion", ProfileReport.FORMAT_VERSION);

        final JsonObject generator = new JsonObject();
        generator.addProperty("name", "mercari-pipeline profile transform");
        generator.addProperty("sketchLibrary", "org.apache.datasketches:datasketches-java:6.2.0");
        manifest.add("generator", generator);

        final JsonObject job = new JsonObject();
        job.addProperty("jobName", config.jobName);
        job.addProperty("moduleName", config.moduleName);
        job.addProperty("runId", config.runId);
        job.addProperty("dataset", config.dataset);
        job.addProperty("partition", config.partition);
        final JsonArray inputs = new JsonArray();
        for(final String input : config.inputNames) {
            inputs.add(input);
        }
        job.add("inputs", inputs);
        job.addProperty("generatedAt", result.generatedAt.toString());
        manifest.add("job", job);

        manifest.addProperty("rows", result.rows);
        manifest.addProperty("errorRows", result.errorRows);

        final JsonObject sketchParameters = new JsonObject();
        sketchParameters.addProperty("kllK", params.kllK);
        sketchParameters.addProperty("cpcLgK", params.cpcLgK);
        sketchParameters.addProperty("fiMaxMapSize", params.fiMaxMapSize);
        sketchParameters.addProperty("thetaLgK", params.thetaLgK);
        sketchParameters.addProperty("sampleK", params.sampleK);
        sketchParameters.addProperty("topKKeep", params.topKKeep);
        sketchParameters.addProperty("valueTableLimit", params.valueTableLimit);
        manifest.add("sketchParameters", sketchParameters);

        final JsonObject counting = new JsonObject();
        counting.addProperty("pass", config.countingPass);
        counting.addProperty("cells", ProfileEdges.CELLS);
        counting.addProperty("bins", config.binsCount);
        manifest.add("counting", counting);

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
        return manifest;
    }

    // ---- static fallback (readable without JavaScript) ----

    private static String buildStaticTable(final ProfileReport.Result result) {
        final StringBuilder sb = new StringBuilder();
        sb.append("<table><thead><tr>")
                .append("<th>field</th><th>type</th><th>count</th><th>null</th><th>distinct&asymp;</th><th>min</th><th>max</th><th>mean</th>")
                .append("</tr></thead><tbody>");
        for(final ProfileReport.FieldResult field : result.fields) {
            sb.append("<tr><td>").append(escapeHtml(field.path)).append("</td>")
                    .append("<td>").append(field.type).append("</td>")
                    .append("<td>").append(field.count).append("</td>")
                    .append("<td>").append(field.nulls).append("</td>")
                    .append("<td>").append(field.distinct == null ? "-" : String.format("%.0f", field.distinct)).append("</td>");
            if(field.min != null && !"timestamp".equals(field.type)) {
                sb.append("<td>").append(field.min).append("</td>")
                        .append("<td>").append(field.max).append("</td>")
                        .append("<td>").append(field.mean == null ? "-" : String.format("%.4g", field.mean)).append("</td>");
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
