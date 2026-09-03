package com.mercari.solution.module.sink;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.profile.ProfileAccumulator;
import com.mercari.solution.util.pipeline.profile.ProfileCombineFn;
import com.mercari.solution.util.pipeline.profile.ProfileRenderer;
import com.mercari.solution.util.pipeline.profile.ProfileRow;
import com.mercari.solution.util.pipeline.profile.ProfileSpec;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProfileSinkTest {

    private static final int ROWS = 500;

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @TempDir
    Path tempDir;

    @Test
    public void testProfileReport() throws Exception {

        final Path reportPath = tempDir.resolve("report.html");
        final String output = reportPath.toString().replace('\\', '/');

        final StringBuilder elements = new StringBuilder();
        for(int i = 0; i < ROWS; i++) {
            if(i > 0) {
                elements.append(",");
            }
            elements.append(String.format(
                    "{ \"id\": %d, \"price\": %s, \"category\": \"cat%d\", \"active\": %b, \"created_at\": \"%s\" }",
                    i, i * 1.5, i % 5, i % 2 == 0,
                    Instant.parse("2025-01-01T00:00:00Z").plusSeconds(i * 3600L)));
        }

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [%s]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "long" },
                          { "name": "price", "type": "double" },
                          { "name": "category", "type": "string" },
                          { "name": "active", "type": "boolean" },
                          { "name": "created_at", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "created_at"
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s",
                        "keys": ["id"],
                        "segments": ["category"],
                        "time": { "field": "created_at", "granularity": "day" },
                        "report": { "title": "profile test" }
                      }
                    }
                  ]
                }
                """.formatted(elements, output);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("profile").getCollection()).satisfies(results -> {
            int count = 0;
            for(final MElement result : results) {
                Assertions.assertEquals(output, result.getAsString("output"));
                Assertions.assertEquals(ROWS, result.getAsLong("rows"));
                Assertions.assertEquals(5L, result.getAsLong("fields"));
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();

        // the report file exists and its embedded payload JSON reflects the input dataset
        Assertions.assertTrue(Files.exists(reportPath), "report file not written: " + reportPath);
        final String html = Files.readString(reportPath);
        final JsonObject payload = extractJsonBlock(html, "profile-payload");
        final JsonObject manifest = extractJsonBlock(html, "profile-manifest");
        final JsonObject sketches = extractJsonBlock(html, "profile-sketches");

        Assertions.assertEquals(1, payload.get("formatVersion").getAsInt());
        Assertions.assertEquals(ROWS, payload.get("rows").getAsLong());
        Assertions.assertEquals("profile test", payload.get("title").getAsString());

        final Map<String, JsonObject> fields = new HashMap<>();
        for(final var field : payload.getAsJsonArray("fields")) {
            fields.put(field.getAsJsonObject().get("path").getAsString(), field.getAsJsonObject());
        }
        Assertions.assertEquals(5, fields.size());

        // id: unique numeric key
        final JsonObject id = fields.get("id");
        Assertions.assertEquals(ROWS, id.get("count").getAsLong());
        Assertions.assertEquals(0, id.get("nullCount").getAsLong());
        final double idDistinct = id.getAsJsonObject("distinct").get("estimate").getAsDouble();
        Assertions.assertTrue(Math.abs(idDistinct - ROWS) < ROWS * 0.03,
                "id distinct estimate out of bounds: " + idDistinct);
        Assertions.assertTrue(id.get("isKey").getAsBoolean());

        // price: quantiles and moments (price = id * 1.5)
        final JsonObject price = fields.get("price").getAsJsonObject("numeric");
        Assertions.assertEquals(0d, price.get("min").getAsDouble());
        Assertions.assertEquals((ROWS - 1) * 1.5, price.get("max").getAsDouble());
        final double expectedMean = (ROWS - 1) * 1.5 / 2;
        Assertions.assertEquals(expectedMean, price.get("mean").getAsDouble(), 1e-6);
        final double p50 = price.getAsJsonObject("quantiles").get("p50").getAsDouble();
        Assertions.assertTrue(Math.abs(p50 - expectedMean) < ROWS * 1.5 * 0.05, "p50 out of bounds: " + p50);
        Assertions.assertEquals(256, price.getAsJsonObject("histogram").getAsJsonArray("counts").size());
        Assertions.assertEquals(257, price.getAsJsonObject("cdf").getAsJsonArray("points").size());

        // category: 5 even top-K values
        final JsonArray topK = fields.get("category").getAsJsonObject("string").getAsJsonArray("topK");
        Assertions.assertEquals(5, topK.size());
        for(final var item : topK) {
            Assertions.assertEquals(ROWS / 5, item.getAsJsonObject().get("count").getAsLong());
        }

        // active: exact bool counts
        final JsonObject bool = fields.get("active").getAsJsonObject("bool");
        Assertions.assertEquals(ROWS / 2, bool.get("trueCount").getAsLong());
        Assertions.assertEquals(ROWS / 2, bool.get("falseCount").getAsLong());

        // created_at: range
        final JsonObject timestamp = fields.get("created_at").getAsJsonObject("timestamp");
        Assertions.assertEquals("2025-01-01T00:00:00Z", timestamp.get("min").getAsString());

        // correlations: price is a perfect linear function of id
        final JsonObject correlations = payload.getAsJsonObject("correlations");
        final JsonArray correlationFields = correlations.getAsJsonArray("fields");
        Assertions.assertEquals(2, correlationFields.size());
        final double correlation = correlations.getAsJsonArray("matrix")
                .get(0).getAsJsonArray().get(1).getAsDouble();
        Assertions.assertEquals(1.0, correlation, 1e-9);

        // keys: keyness ≈ 1
        final JsonObject keyField = payload.getAsJsonObject("keys")
                .getAsJsonArray("fields").get(0).getAsJsonObject();
        Assertions.assertEquals("id", keyField.get("path").getAsString());
        Assertions.assertTrue(Math.abs(keyField.get("keyness").getAsDouble() - 1.0) < 0.03);

        // comparisons: segments axis with 5 groups of 100 rows, per-group histograms aligned to shared edges
        final JsonArray comparisons = payload.getAsJsonArray("comparisons");
        Assertions.assertEquals(2, comparisons.size());
        final JsonObject segmentsAxis = comparisons.get(0).getAsJsonObject();
        Assertions.assertEquals("segments", segmentsAxis.get("kind").getAsString());
        Assertions.assertEquals("category", segmentsAxis.get("field").getAsString());
        Assertions.assertEquals(0, segmentsAxis.get("truncatedGroups").getAsInt());
        final JsonArray segmentGroups = segmentsAxis.getAsJsonArray("groups");
        Assertions.assertEquals(5, segmentGroups.size());
        long segmentRows = 0;
        for(final var group : segmentGroups) {
            final JsonObject g = group.getAsJsonObject();
            Assertions.assertEquals(ROWS / 5, g.get("rows").getAsLong());
            segmentRows += g.get("rows").getAsLong();
            final JsonObject priceGroup = g.getAsJsonObject("fields").getAsJsonObject("price");
            Assertions.assertEquals(ROWS / 5, priceGroup.get("count").getAsLong());
            Assertions.assertEquals(64, priceGroup.getAsJsonArray("hist").size());
            final JsonObject activeGroup = g.getAsJsonObject("fields").getAsJsonObject("active");
            Assertions.assertEquals(ROWS / 5,
                    activeGroup.get("trueCount").getAsLong() + activeGroup.get("falseCount").getAsLong());
        }
        Assertions.assertEquals(ROWS, segmentRows);
        // shared overlay edges present on the numeric field
        Assertions.assertEquals(65, fields.get("price").getAsJsonObject("overlay").getAsJsonArray("edges").size());
        Assertions.assertTrue(fields.get("category").getAsJsonObject("overlay").getAsJsonArray("labels").size() >= 5);

        // time axis: 500 hourly rows from 2025-01-01 → 21 daily buckets, chronological
        final JsonObject timeAxis = comparisons.get(1).getAsJsonObject();
        Assertions.assertEquals("time", timeAxis.get("kind").getAsString());
        Assertions.assertEquals("day", timeAxis.get("granularity").getAsString());
        final JsonArray timeGroups = timeAxis.getAsJsonArray("groups");
        Assertions.assertEquals(21, timeGroups.size());
        Assertions.assertEquals("2025-01-01", timeGroups.get(0).getAsJsonObject().get("value").getAsString());
        Assertions.assertEquals(24, timeGroups.get(0).getAsJsonObject().get("rows").getAsLong());
        long timeRows = 0;
        for(final var group : timeGroups) {
            timeRows += group.getAsJsonObject().get("rows").getAsLong();
        }
        Assertions.assertEquals(ROWS, timeRows);

        // sample and suggestions exist
        Assertions.assertTrue(payload.getAsJsonObject("sample").getAsJsonArray("rows").size() > 0);
        Assertions.assertFalse(payload.getAsJsonArray("suggestions").isEmpty());

        // manifest and sketches blocks
        Assertions.assertEquals(ROWS, manifest.get("rows").getAsLong());
        Assertions.assertTrue(manifest.getAsJsonArray("degradations").isEmpty());
        Assertions.assertTrue(sketches.getAsJsonObject("fields").getAsJsonObject("price").has("kll"));
        Assertions.assertTrue(sketches.getAsJsonObject("fields").getAsJsonObject("id").has("theta"));
    }

    @Test
    public void testValuesHide() throws Exception {

        final Path reportPath = tempDir.resolve("report_hide.html");
        final String output = reportPath.toString().replace('\\', '/');

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "name": "secret1", "score": 1.0 },
                          { "name": "secret2", "score": 2.0 },
                          { "name": "secret2", "score": 3.0 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "name", "type": "string" },
                          { "name": "score", "type": "double" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s",
                        "values": "hide"
                      }
                    }
                  ]
                }
                """.formatted(output);

        final Config config = Config.load(configJson);
        MPipeline.apply(pipeline, config);
        pipeline.run();

        final String html = Files.readString(reportPath);
        final JsonObject payload = extractJsonBlock(html, "profile-payload");
        Assertions.assertEquals("hide", payload.get("values").getAsString());
        Assertions.assertFalse(payload.has("sample"));
        Assertions.assertFalse(html.contains("secret1"), "raw value leaked into hidden report");

        for(final var field : payload.getAsJsonArray("fields")) {
            final JsonObject o = field.getAsJsonObject();
            if(o.has("string")) {
                for(final var item : o.getAsJsonObject("string").getAsJsonArray("topK")) {
                    Assertions.assertFalse(item.getAsJsonObject().has("value"));
                }
            }
        }
    }

    @Test
    public void testCompareModeAndCompareWith() throws Exception {

        // stage 1: generate the past report (ids 0..399, price uniform)
        final Path pastPath = tempDir.resolve("past.html");
        final String pastOutput = pastPath.toString().replace('\\', '/');
        final TestPipeline pastPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
        final String pastConfig = """
                {
                  "sources": [
                    {
                      "name": "items",
                      "module": "create",
                      "parameters": { "type": "element", "elements": [%s] },
                      "schema": { "fields": [
                        { "name": "id", "type": "long" },
                        { "name": "price", "type": "double" }
                      ] }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["items"],
                      "parameters": { "output": "%s", "keys": ["id"] }
                    }
                  ]
                }
                """.formatted(elementsJson(0, 400, 1.0), pastOutput);
        MPipeline.apply(pastPipeline, Config.load(pastConfig));
        pastPipeline.run();
        Assertions.assertTrue(Files.exists(pastPath));

        // stage 2: two inputs compared against a baseline, a declared pair, and compareWith the past report
        final Path reportPath = tempDir.resolve("compare.html");
        final String output = reportPath.toString().replace('\\', '/');
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "a",
                      "module": "create",
                      "parameters": { "type": "element", "elements": [%s] },
                      "schema": { "fields": [
                        { "name": "id", "type": "long" },
                        { "name": "user_id", "type": "long" },
                        { "name": "price", "type": "double" },
                        { "name": "discounted", "type": "double" }
                      ] }
                    },
                    {
                      "name": "b",
                      "module": "create",
                      "parameters": { "type": "element", "elements": [%s] },
                      "schema": { "fields": [
                        { "name": "id", "type": "long" },
                        { "name": "user_id", "type": "long" },
                        { "name": "price", "type": "double" },
                        { "name": "discounted", "type": "double" }
                      ] }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["a", "b"],
                      "parameters": {
                        "output": "%s",
                        "mode": "compare",
                        "baseline": "a",
                        "keys": ["id", "user_id"],
                        "compare": [["price", "discounted"]],
                        "compareWith": "%s"
                      }
                    }
                  ]
                }
                """.formatted(
                        compareElementsJson(0, 300, 1.0),
                        compareElementsJson(300, 600, 2.0),
                        output, pastOutput);
        MPipeline.apply(pipeline, Config.load(configJson));
        pipeline.run();

        final String html = Files.readString(reportPath);
        final JsonObject payload = extractJsonBlock(html, "profile-payload");

        // inputs axis: declared order, baseline recorded, drift metrics on the non-baseline group
        final JsonObject inputsAxis = payload.getAsJsonArray("comparisons").get(0).getAsJsonObject();
        Assertions.assertEquals("inputs", inputsAxis.get("kind").getAsString());
        Assertions.assertEquals("a", inputsAxis.get("baseline").getAsString());
        final JsonArray inputGroups = inputsAxis.getAsJsonArray("groups");
        Assertions.assertEquals(2, inputGroups.size());
        Assertions.assertEquals("a", inputGroups.get(0).getAsJsonObject().get("value").getAsString());
        Assertions.assertEquals("b", inputGroups.get(1).getAsJsonObject().get("value").getAsString());
        Assertions.assertEquals(300, inputGroups.get(0).getAsJsonObject().get("rows").getAsLong());
        final JsonObject bPrice = inputGroups.get(1).getAsJsonObject()
                .getAsJsonObject("fields").getAsJsonObject("price");
        Assertions.assertTrue(bPrice.has("psi"), "psi missing on non-baseline group");
        Assertions.assertTrue(bPrice.get("psi").getAsDouble() > 0.2,
                "price distributions differ strongly, psi=" + bPrice.get("psi"));
        Assertions.assertTrue(bPrice.has("ks"));
        // baseline group carries no drift metrics
        Assertions.assertFalse(inputGroups.get(0).getAsJsonObject()
                .getAsJsonObject("fields").getAsJsonObject("price").has("psi"));
        // stat-strip annotation on the top-level field
        for(final var field : payload.getAsJsonArray("fields")) {
            if("price".equals(field.getAsJsonObject().get("path").getAsString())) {
                Assertions.assertEquals("b", field.getAsJsonObject().getAsJsonObject("drift").get("vs").getAsString());
            }
        }

        // declared field pair: shared edges, psi > 0 (discounted is half of price), 49 Q-Q points
        final JsonObject pair = payload.getAsJsonArray("fieldPairs").get(0).getAsJsonObject();
        Assertions.assertEquals("price", pair.get("a").getAsString());
        Assertions.assertEquals("discounted", pair.get("b").getAsString());
        Assertions.assertEquals(65, pair.getAsJsonArray("edges").size());
        Assertions.assertTrue(pair.get("psi").getAsDouble() > 0);
        Assertions.assertEquals(49, pair.getAsJsonArray("qq").size());

        // venn for the two declared keys
        final JsonObject venn = payload.getAsJsonObject("keys").getAsJsonObject("venn");
        Assertions.assertEquals(2, venn.getAsJsonArray("labels").size());
        Assertions.assertEquals(1, venn.getAsJsonArray("pairs").size());

        // compareWith: rows, common numeric field with psi/ks/cdf, key overlap via theta sketches
        final JsonObject compareWith = payload.getAsJsonObject("compareWith");
        Assertions.assertEquals(400, compareWith.get("rowsOld").getAsLong());
        Assertions.assertEquals(600, compareWith.get("rowsNew").getAsLong());
        JsonObject idField = null;
        JsonObject priceField = null;
        JsonObject userIdField = null;
        for(final var field : compareWith.getAsJsonArray("fields")) {
            switch (field.getAsJsonObject().get("path").getAsString()) {
                case "id" -> idField = field.getAsJsonObject();
                case "price" -> priceField = field.getAsJsonObject();
                case "user_id" -> userIdField = field.getAsJsonObject();
                default -> { }
            }
        }
        Assertions.assertEquals("common", priceField.get("status").getAsString());
        Assertions.assertTrue(priceField.has("psi"));
        Assertions.assertTrue(priceField.has("ks"));
        Assertions.assertEquals(65, priceField.getAsJsonObject("cdf").getAsJsonArray("edges").size());
        Assertions.assertEquals("added", userIdField.get("status").getAsString());
        // old ids 0..399 are all present in new ids 0..599 → retained ≈ 1.0, new-key share ≈ 1/3
        final JsonObject keyOverlap = idField.getAsJsonObject("keyOverlap");
        Assertions.assertEquals(1.0, keyOverlap.get("retainedShare").getAsDouble(), 0.05);
        Assertions.assertEquals(1.0 / 3, keyOverlap.get("newShare").getAsDouble(), 0.05);
    }

    private static String elementsJson(final int from, final int to, final double priceFactor) {
        final StringBuilder sb = new StringBuilder();
        for(int i = from; i < to; i++) {
            if(sb.length() > 0) {
                sb.append(",");
            }
            sb.append(String.format("{ \"id\": %d, \"price\": %s }", i, i * priceFactor));
        }
        return sb.toString();
    }

    private static String compareElementsJson(final int from, final int to, final double priceFactor) {
        final StringBuilder sb = new StringBuilder();
        for(int i = from; i < to; i++) {
            if(sb.length() > 0) {
                sb.append(",");
            }
            sb.append(String.format("{ \"id\": %d, \"user_id\": %d, \"price\": %s, \"discounted\": %s }",
                    i, i % 100, i * priceFactor, i * priceFactor * 0.5));
        }
        return sb.toString();
    }

    @Test
    public void testAccumulatorMergeAndSerialization() throws Exception {

        final Schema schema = Schema.builder()
                .withField("x", Schema.FieldType.FLOAT64)
                .withField("y", Schema.FieldType.FLOAT64)
                .withField("s", Schema.FieldType.STRING)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", true, true);
        final ProfileCombineFn fn = new ProfileCombineFn(spec);

        // single accumulator over all rows vs merged split accumulators (with a serialization
        // round trip in between, as Beam does at shuffle boundaries)
        ProfileAccumulator single = fn.createAccumulator();
        ProfileAccumulator first = fn.createAccumulator();
        ProfileAccumulator second = fn.createAccumulator();
        for(int i = 0; i < 1000; i++) {
            final ProfileRow element = ProfileRow.of(spec, element(i));
            single = fn.addInput(single, element);
            if(i < 500) {
                first = fn.addInput(first, element);
            } else {
                second = fn.addInput(second, element);
            }
        }
        first = roundTrip(first);
        ProfileAccumulator merged = fn.mergeAccumulators(List.of(first, second));

        // adding inputs after a merge must keep working (Beam may do this)
        merged = fn.addInput(merged, ProfileRow.of(spec, element(1000)));
        single = fn.addInput(single, ProfileRow.of(spec, element(1000)));
        merged = roundTrip(merged);

        Assertions.assertEquals(single.getRowCount(), merged.getRowCount());
        for(int f = 0; f < spec.getFields().size(); f++) {
            Assertions.assertEquals(single.getField(f).count, merged.getField(f).count,
                    "count mismatch for field " + spec.getFields().get(f).path);
            Assertions.assertEquals(single.getField(f).mean, merged.getField(f).mean, 1e-6);
            Assertions.assertEquals(single.getField(f).m2, merged.getField(f).m2, 1e-3);
        }
        // x-y correlation identical between merged and single paths
        final Double singleCorrelation = single.correlation(0, 1);
        final Double mergedCorrelation = merged.correlation(0, 1);
        Assertions.assertNotNull(singleCorrelation);
        Assertions.assertNotNull(mergedCorrelation);
        Assertions.assertEquals(singleCorrelation, mergedCorrelation, 1e-9);

        // distinct estimates survive serialization and merging
        final double distinct = merged.getField(2).cpcResult(spec.getSketchParameters()).getEstimate();
        Assertions.assertTrue(Math.abs(distinct - 101) < 10, "distinct estimate out of bounds: " + distinct);

        // sample survives
        Assertions.assertTrue(merged.sampleResult().getNumSamples() > 0);
    }

    @Test
    public void testEmptyInputRendersEmptyReport() {
        final Schema schema = Schema.builder()
                .withField("x", Schema.FieldType.FLOAT64)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", true, true);
        final ProfileAccumulator empty = ProfileAccumulator.of(spec);

        final ProfileRenderer.Config config = new ProfileRenderer.Config();
        config.title = "empty";
        final ProfileRenderer.Result result = ProfileRenderer.render(empty, config);
        final JsonObject payload = JsonParser.parseString(result.payloadJson).getAsJsonObject();
        Assertions.assertEquals(0, payload.get("rows").getAsLong());
        Assertions.assertTrue(result.html.contains("profile-payload"));
    }

    private static MElement element(final int i) {
        final Map<String, Object> values = new HashMap<>();
        values.put("x", (double) i);
        values.put("y", i * 2.0 + 1.0);
        values.put("s", "value" + (i % 101));
        return MElement.of(values, java.time.Instant.parse("2025-01-01T00:00:00Z").toEpochMilli());
    }

    private static ProfileAccumulator roundTrip(final ProfileAccumulator accumulator) throws Exception {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try(final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(accumulator);
        }
        try(final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (ProfileAccumulator) in.readObject();
        }
    }

    @Test
    public void testScriptTagInValueCannotBreakOutOfJsonBlock() {
        final Schema schema = Schema.builder()
                .withField("s", Schema.FieldType.STRING)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", true, false);
        final ProfileCombineFn fn = new ProfileCombineFn(spec);
        final String hostile = "</script><script>alert(document.cookie)</script>";
        ProfileAccumulator acc = fn.createAccumulator();
        for(int i = 0; i < 50; i++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("s", i % 2 == 0 ? hostile : "plain");
            acc = fn.addInput(acc, ProfileRow.of(spec, MElement.of(values, 0L)));
        }
        final ProfileRenderer.Config config = new ProfileRenderer.Config();
        config.title = "hostile";
        final ProfileRenderer.Result result = ProfileRenderer.render(acc, config);

        // the hostile value never appears verbatim in the html (top-K label and sample rows)
        Assertions.assertFalse(result.html.contains(hostile), "raw </script> value leaked into the html");
        // yet the embedded blocks still parse to the original value
        final JsonObject payload = extractJsonBlock(result.html, "profile-payload");
        final JsonArray topK = payload.getAsJsonArray("fields").get(0).getAsJsonObject()
                .getAsJsonObject("string").getAsJsonArray("topK");
        boolean found = false;
        for(final JsonElement item : topK) {
            found |= hostile.equals(item.getAsJsonObject().get("value").getAsString());
        }
        Assertions.assertTrue(found, "top-K label lost after escaping");
        // a report with such values can still be read back for compareWith
        final ProfileRenderer.PastReport past = ProfileRenderer.PastReport.parse("past.html", result.html);
        Assertions.assertEquals(payload, JsonParser.parseString(past.payloadJson).getAsJsonObject());
        extractJsonBlock(result.html, "profile-manifest");
        extractJsonBlock(result.html, "profile-sketches");
    }

    @Test
    public void testNonFiniteSampleValuesBecomeNull() {
        final Schema schema = Schema.builder()
                .withField("x", Schema.FieldType.FLOAT64)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", true, false);
        final ProfileCombineFn fn = new ProfileCombineFn(spec);
        ProfileAccumulator acc = fn.createAccumulator();
        for(int i = 0; i < 20; i++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("x", i % 3 == 0 ? Double.NaN : (i % 3 == 1 ? Double.POSITIVE_INFINITY : (double) i));
            acc = fn.addInput(acc, ProfileRow.of(spec, MElement.of(values, 0L)));
        }
        final ProfileRenderer.Config config = new ProfileRenderer.Config();
        config.title = "nan";
        final ProfileRenderer.Result result = ProfileRenderer.render(acc, config);

        Assertions.assertFalse(result.payloadJson.contains("NaN"), "bare NaN in payload: " + result.payloadJson);
        Assertions.assertFalse(result.payloadJson.contains("Infinity"), "bare Infinity in payload");
        final JsonObject payload = JsonParser.parseString(result.payloadJson).getAsJsonObject();
        final JsonObject numeric = payload.getAsJsonArray("fields").get(0).getAsJsonObject().getAsJsonObject("numeric");
        Assertions.assertEquals(7, numeric.get("nanCount").getAsLong());
        Assertions.assertEquals(7, numeric.get("infCount").getAsLong());
        int nulls = 0;
        for(final JsonElement row : payload.getAsJsonObject("sample").getAsJsonArray("rows")) {
            if(row.getAsJsonArray().get(0).isJsonNull()) {
                nulls += 1;
            }
        }
        Assertions.assertEquals(14, nulls);
    }

    @Test
    public void testNarrowRangeAtLargeMagnitudeRendersHistogram() {
        // consecutive equal-width split points collide in double precision here (spacing < ulp)
        final Schema schema = Schema.builder()
                .withField("seq", Schema.FieldType.INT64)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", false, false);
        final ProfileCombineFn fn = new ProfileCombineFn(spec);
        ProfileAccumulator acc = fn.createAccumulator();
        for(int i = 0; i < 1000; i++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("seq", 100_000_000_000_000_000L + i);
            acc = fn.addInput(acc, ProfileRow.of(spec, MElement.of(values, 0L)));
        }
        final ProfileRenderer.Config config = new ProfileRenderer.Config();
        config.title = "narrow";
        config.axes = List.of();
        config.comparePairs = List.<String[]>of(new String[] { "seq", "seq" });
        final ProfileRenderer.Result result = ProfileRenderer.render(acc, config);

        final JsonObject payload = JsonParser.parseString(result.payloadJson).getAsJsonObject();
        final JsonObject numeric = payload.getAsJsonArray("fields").get(0).getAsJsonObject().getAsJsonObject("numeric");
        final JsonArray edges = numeric.getAsJsonObject("histogram").getAsJsonArray("edges");
        final JsonArray counts = numeric.getAsJsonObject("histogram").getAsJsonArray("counts");
        Assertions.assertTrue(edges.size() >= 2 && edges.size() < 257, "edges: " + edges.size());
        Assertions.assertEquals(edges.size() - 1, counts.size());
        for(int i = 1; i < edges.size(); i++) {
            Assertions.assertTrue(edges.get(i).getAsDouble() > edges.get(i - 1).getAsDouble(), "edges not increasing");
        }
        long total = 0;
        for(final JsonElement c : counts) {
            total += c.getAsLong();
        }
        Assertions.assertEquals(1000, total);
        final JsonArray cdfPoints = numeric.getAsJsonObject("cdf").getAsJsonArray("points");
        for(int i = 1; i < cdfPoints.size(); i++) {
            Assertions.assertTrue(cdfPoints.get(i).getAsDouble() > cdfPoints.get(i - 1).getAsDouble(), "cdf points not increasing");
        }
        // declared pair on the same narrow field also renders
        Assertions.assertFalse(payload.getAsJsonArray("fieldPairs").get(0).getAsJsonObject().has("error"));
    }

    @Test
    public void testDecimalBytesAreProfiledAsNumbers() {
        // Avro decimal logical type: unscaled two's-complement big-endian bytes
        final java.math.BigDecimal decimal = new java.math.BigDecimal("1234.567890000");
        final byte[] unscaled = decimal.unscaledValue().toByteArray();
        Assertions.assertEquals(1234.56789, ProfileSpec.toDouble(java.nio.ByteBuffer.wrap(unscaled), 9), 1e-9);
        Assertions.assertEquals(1234.56789, ProfileSpec.toDouble(unscaled, 9), 1e-9);
        Assertions.assertEquals(-0.5, ProfileSpec.toDouble(new java.math.BigDecimal("-0.5")), 1e-12);

        final Schema schema = Schema.builder()
                .withField("price", Schema.FieldType.decimal(38, 9))
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", false, false);
        Assertions.assertEquals(9, spec.getFields().get(0).scale);
        final Map<String, Object> values = new HashMap<>();
        values.put("price", java.nio.ByteBuffer.wrap(unscaled));
        final ProfileRow row = ProfileRow.of(spec, MElement.of(values, 0L));
        Assertions.assertEquals(1234.56789, (Double) row.values[0], 1e-9);
        Assertions.assertFalse(row.isFailed());
    }

    @Test
    public void testNestedStructAsJsonStringIsNavigated() {
        // Spanner STRUCT columns arrive as a JSON string from the primitive accessor
        final Schema schema = Schema.builder()
                .withField("id", Schema.FieldType.INT64)
                .withField("address", Schema.FieldType.element(Schema.builder()
                        .withField("city", Schema.FieldType.STRING)
                        .withField("zip", Schema.FieldType.INT64)
                        .withField("moved_at", Schema.FieldType.TIMESTAMP)
                        .build()))
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", false, false);
        Assertions.assertEquals(List.of("id", "address.city", "address.zip", "address.moved_at"),
                spec.getFields().stream().map(f -> f.path).toList());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 1L);
        values.put("address", "{\"city\":\"Tokyo\",\"zip\":1000001,\"moved_at\":\"2025-03-01T00:00:00Z\"}");
        final ProfileRow row = ProfileRow.of(spec, MElement.of(values, 0L));
        Assertions.assertEquals("Tokyo", row.values[1]);
        Assertions.assertEquals(1000001d, (Double) row.values[2], 0d);
        Assertions.assertEquals((double) Instant.parse("2025-03-01T00:00:00Z").toEpochMilli(), (Double) row.values[3], 0d);
        Assertions.assertFalse(row.isFailed());

        // a nested map (every other data type) takes the same path
        values.put("address", Map.of("city", "Osaka", "zip", 5300001L));
        final ProfileRow mapRow = ProfileRow.of(spec, MElement.of(values, 0L));
        Assertions.assertEquals("Osaka", mapRow.values[1]);
        Assertions.assertNull(mapRow.values[3]);
    }

    @Test
    public void testUnconvertibleValueIsAFieldErrorNotARowFailure() {
        final Schema schema = Schema.builder()
                .withField("x", Schema.FieldType.FLOAT64)
                .withField("s", Schema.FieldType.STRING)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", false, false);
        final Map<String, Object> values = new HashMap<>();
        values.put("x", Map.of("not", "a number"));
        values.put("s", "fine");
        final ProfileRow row = ProfileRow.of(spec, MElement.of(values, 0L));
        Assertions.assertSame(ProfileRow.Marker.ERROR, row.values[0]);
        Assertions.assertEquals("fine", row.values[1]);
        Assertions.assertFalse(row.isFailed());

        final ProfileCombineFn fn = new ProfileCombineFn(spec);
        final ProfileAccumulator acc = fn.addInput(fn.createAccumulator(), row);
        Assertions.assertEquals(1, acc.getRowCount());
        Assertions.assertEquals(0, acc.getErrorCount());
        Assertions.assertEquals(1, acc.getField(0).errorCount);
        Assertions.assertEquals(1, acc.getField(1).count);
    }

    @Test
    public void testNonGlobalWindowIsRejected() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [{ "id": 1, "created_at": "2025-01-01T00:00:00Z" }]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "long" },
                          { "name": "created_at", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "created_at"
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["create"],
                      "strategy": { "window": { "type": "fixed", "unit": "day", "size": 1, "offset": 0 } },
                      "parameters": { "output": "%s" }
                    }
                  ]
                }
                """.formatted(tempDir.resolve("windowed.html").toString().replace('\\', '/'));
        final Config config = Config.load(configJson);
        final Throwable e = Assertions.assertThrows(Throwable.class, () -> MPipeline.apply(pipeline, config));
        boolean found = false;
        for(Throwable t = e; t != null; t = t.getCause()) {
            found |= t.getMessage() != null && t.getMessage().contains("requires the global window");
        }
        Assertions.assertTrue(found, "unexpected error: " + e);
    }

    @Test
    public void testSegmentGroupsAreBoundedInPipeline() throws Exception {
        final Path reportPath = tempDir.resolve("report_segments.html");
        final String output = reportPath.toString().replace('\\', '/');
        final int groups = 50;
        final StringBuilder elements = new StringBuilder();
        // 50 groups of 8 rows, then 10 extra rows for each of the last 5 groups (18 rows each)
        for(int i = 0; i < 450; i++) {
            if(i > 0) {
                elements.append(",");
            }
            final int group = i < 400 ? i % groups : groups - 5 + (i - 400) % 5;
            elements.append(String.format("{ \"id\": %d, \"category\": \"cat%02d\" }", i, group));
        }
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": { "type": "element", "elements": [%s] },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "long" },
                          { "name": "category", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s",
                        "segments": [{ "field": "category", "topK": 5 }]
                      }
                    }
                  ]
                }
                """.formatted(elements, output);
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("profile").getCollection()).satisfies(results -> {
            int count = 0;
            for(final MElement result : results) {
                Assertions.assertEquals(450L, result.getAsLong("rows"));
                Assertions.assertEquals(0L, result.getAsLong("errorRows"));
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();

        final JsonObject payload = extractJsonBlock(Files.readString(reportPath), "profile-payload");
        final JsonObject axis = payload.getAsJsonArray("comparisons").get(0).getAsJsonObject();
        final JsonArray kept = axis.getAsJsonArray("groups");
        Assertions.assertEquals(5, kept.size());
        Assertions.assertEquals(groups - 5, axis.get("truncatedGroups").getAsInt());
        // only the largest groups survive the in-pipeline bound
        for(final JsonElement g : kept) {
            Assertions.assertEquals(18L, g.getAsJsonObject().get("rows").getAsLong(), "a small group was kept: " + g);
        }
    }

    @Test
    public void testTargetAssociation() throws Exception {
        final Path reportPath = tempDir.resolve("report_target.html");
        final String output = reportPath.toString().replace('\\', '/');

        // sold_flag is positive exactly for cat0/cat1 rows (200 of 500); score is high on positive rows
        final StringBuilder elements = new StringBuilder();
        for(int i = 0; i < ROWS; i++) {
            if(i > 0) {
                elements.append(",");
            }
            final boolean positive = i % 5 < 2;
            final boolean nullFlag = i % 50 == 49;   // 10 rows with a null target
            elements.append(String.format(
                    "{ \"id\": %d, \"score\": %d, \"category\": \"cat%d\", \"sold_flag\": %s, \"created_at\": \"%s\" }",
                    i, (positive ? 100 : 0) + i % 50, i % 5, nullFlag ? "null" : String.valueOf(positive),
                    Instant.parse("2025-01-01T00:00:00Z").plusSeconds(i * 3600L)));
        }
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": { "type": "element", "elements": [%s] },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "long" },
                          { "name": "score", "type": "long" },
                          { "name": "category", "type": "string" },
                          { "name": "sold_flag", "type": "boolean" },
                          { "name": "created_at", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["create"],
                      "parameters": {
                        "output": "%s",
                        "target": "sold_flag",
                        "segments": ["category"]
                      }
                    }
                  ]
                }
                """.formatted(elements, output);
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        PAssert.that(outputs.get("profile").getCollection()).satisfies(results -> {
            int count = 0;
            for(final MElement result : results) {
                Assertions.assertEquals((long) ROWS, result.getAsLong("rows"));
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();

        final String html = Files.readString(reportPath);
        final JsonObject payload = extractJsonBlock(html, "profile-payload");
        final JsonObject manifest = extractJsonBlock(html, "profile-manifest");

        // class totals: 10 null-target rows are excluded from the rate
        final JsonObject target = payload.getAsJsonObject("target");
        Assertions.assertEquals("sold_flag", target.get("field").getAsString());
        Assertions.assertEquals("true", target.get("positive").getAsString());
        final long positiveRows = target.get("positiveRows").getAsLong();
        final long negativeRows = target.get("negativeRows").getAsLong();
        Assertions.assertEquals(10L, target.get("nullRows").getAsLong());
        Assertions.assertEquals(ROWS - 10, positiveRows + negativeRows);
        Assertions.assertEquals(200L, positiveRows);   // the null-target rows (i % 50 == 49) are all cat4, i.e. negative
        Assertions.assertEquals(positiveRows / (double) (positiveRows + negativeRows), target.get("rate").getAsDouble(), 1e-9);

        final Map<String, JsonObject> targetFields = new HashMap<>();
        for(final JsonElement f : target.getAsJsonArray("fields")) {
            targetFields.put(f.getAsJsonObject().get("path").getAsString(), f.getAsJsonObject());
        }
        Assertions.assertFalse(targetFields.containsKey("sold_flag"), "the target field must not relate to itself");
        Assertions.assertEquals(java.util.Set.of("id", "score", "category", "created_at"), targetFields.keySet());

        // category: cat0/cat1 are 100% positive, the rest 0% → perfectly separating (leak-level IV)
        final JsonObject category = targetFields.get("category");
        final JsonArray labels = category.getAsJsonArray("labels");
        Assertions.assertEquals("(other)", labels.get(labels.size() - 1).getAsString());
        final JsonArray categoryPositive = category.getAsJsonArray("positive");
        final JsonArray categoryNegative = category.getAsJsonArray("negative");
        for(int l = 0; l < labels.size() - 1; l++) {
            final String label = labels.get(l).getAsString();
            final long expectedPositive = "cat0".equals(label) || "cat1".equals(label) ? 100L : 0L;
            Assertions.assertEquals(expectedPositive, categoryPositive.get(l).getAsLong(), "positive count for " + label);
            Assertions.assertEquals(expectedPositive == 0 ? ("cat4".equals(label) ? 90L : 100L) : 0L,
                    categoryNegative.get(l).getAsLong(), "negative count for " + label);
        }
        Assertions.assertTrue(category.get("iv").getAsDouble() > 0.5, "iv: " + category.get("iv"));
        Assertions.assertTrue(category.get("leak").getAsBoolean());
        Assertions.assertEquals(1.0, category.get("tvd").getAsDouble(), 1e-9);
        Assertions.assertFalse(category.has("ks"));

        // score: 100-149 on positive rows, 0-49 on negative rows → strongly positive point-biserial r
        final JsonObject score = targetFields.get("score");
        Assertions.assertEquals(65, score.getAsJsonArray("edges").size());
        Assertions.assertEquals(64, score.getAsJsonArray("positive").size());
        Assertions.assertTrue(score.get("pointBiserial").getAsDouble() > 0.9, "point-biserial: " + score.get("pointBiserial"));
        Assertions.assertTrue(score.get("ks").getAsDouble() > 0.99, "ks: " + score.get("ks"));
        Assertions.assertTrue(score.get("meanPositive").getAsDouble() > score.get("meanNegative").getAsDouble() + 90);
        Assertions.assertTrue(score.get("iv").getAsDouble() > 0.5);
        long scorePositive = 0;
        for(final JsonElement c : score.getAsJsonArray("positive")) {
            scorePositive += c.getAsLong();
        }
        Assertions.assertEquals(positiveRows, scorePositive, 3);

        // stat-strip annotation and the suppressed target suggestion
        final Map<String, JsonObject> fields = new HashMap<>();
        for(final JsonElement field : payload.getAsJsonArray("fields")) {
            fields.put(field.getAsJsonObject().get("path").getAsString(), field.getAsJsonObject());
        }
        Assertions.assertTrue(fields.get("category").getAsJsonObject("target").get("iv").getAsDouble() > 0.5);
        Assertions.assertFalse(fields.get("sold_flag").has("target"));
        for(final JsonElement suggestion : payload.getAsJsonArray("suggestions")) {
            Assertions.assertNotEquals("target", suggestion.getAsJsonObject().get("kind").getAsString());
        }

        // the target appears as a comparison axis (positive / negative groups over the shared overlay edges)
        final JsonArray comparisons = payload.getAsJsonArray("comparisons");
        Assertions.assertEquals(2, comparisons.size());
        final JsonObject segmentsAxis = comparisons.get(0).getAsJsonObject();
        final JsonObject targetAxis = comparisons.get(1).getAsJsonObject();
        Assertions.assertEquals("target", targetAxis.get("kind").getAsString());
        Assertions.assertEquals("sold_flag", targetAxis.get("field").getAsString());
        final JsonObject positiveGroup = targetAxis.getAsJsonArray("groups").get(0).getAsJsonObject();
        final JsonObject negativeGroup = targetAxis.getAsJsonArray("groups").get(1).getAsJsonObject();
        Assertions.assertEquals("positive", positiveGroup.get("value").getAsString());
        Assertions.assertEquals(positiveRows, positiveGroup.get("rows").getAsLong());
        Assertions.assertEquals(negativeRows, negativeGroup.get("rows").getAsLong());
        final JsonObject positiveScore = positiveGroup.getAsJsonObject("fields").getAsJsonObject("score");
        Assertions.assertEquals(positiveRows, positiveScore.get("count").getAsLong());
        Assertions.assertEquals(64, positiveScore.getAsJsonArray("hist").size());
        Assertions.assertFalse(positiveGroup.getAsJsonObject("fields").has("sold_flag"));
        Assertions.assertTrue(fields.get("score").has("overlay"));

        // per-segment target rate from the group sub-profiles
        Assertions.assertEquals("segments", segmentsAxis.get("kind").getAsString());
        for(final JsonElement g : segmentsAxis.getAsJsonArray("groups")) {
            final JsonObject group = g.getAsJsonObject();
            final String value = group.get("value").getAsString();
            final double expected = "cat0".equals(value) || "cat1".equals(value) ? 1.0 : 0.0;
            Assertions.assertEquals(expected, group.get("targetRate").getAsDouble(), 1e-9, "target rate of " + value);
        }

        Assertions.assertEquals("sold_flag", manifest.getAsJsonObject("expandedParameters").get("target").getAsString());
    }

    @Test
    public void testNumericTargetWithPositiveValueMergesAndSerializes() throws Exception {
        final Schema schema = Schema.builder()
                .withField("x", Schema.FieldType.FLOAT64)
                .withField("label", Schema.FieldType.INT64)
                .withField("s", Schema.FieldType.STRING)
                .withField("b", Schema.FieldType.BOOLEAN)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", false, false)
                .withTarget("label", 1.0);
        Assertions.assertEquals("1", spec.getTarget().positiveLabel());
        final ProfileCombineFn fn = new ProfileCombineFn(spec);

        ProfileAccumulator single = fn.createAccumulator();
        ProfileAccumulator first = fn.createAccumulator();
        ProfileAccumulator second = fn.createAccumulator();
        for(int i = 0; i < 900; i++) {
            final ProfileRow row = ProfileRow.of(spec, targetElement(i));
            single = fn.addInput(single, row);
            if(i < 450) {
                first = fn.addInput(first, row);
            } else {
                second = fn.addInput(second, row);
            }
        }
        first = roundTrip(first);
        ProfileAccumulator merged = fn.mergeAccumulators(List.of(first, second));
        merged = fn.addInput(merged, ProfileRow.of(spec, targetElement(900)));
        single = fn.addInput(single, ProfileRow.of(spec, targetElement(900)));
        merged = roundTrip(merged);

        Assertions.assertEquals(single.getTargetPositiveRows(), merged.getTargetPositiveRows());
        Assertions.assertEquals(single.getTargetNegativeRows(), merged.getTargetNegativeRows());
        Assertions.assertEquals(301L, single.getTargetPositiveRows());   // i % 3 == 0
        Assertions.assertEquals(0L, single.getTargetNullRows());
        for(final ProfileAccumulator acc : List.of(single, merged)) {
            final ProfileAccumulator.TargetStats x = acc.getField(0).getTarget();
            Assertions.assertNull(acc.getField(1).getTarget(), "the target field has no split");
            Assertions.assertEquals(301L, x.positiveCount);
            Assertions.assertEquals(600L, x.negativeCount);
            Assertions.assertEquals(x.positiveCount, x.getKllPositive().getN());
            Assertions.assertEquals(x.negativeCount, x.getKllNegative().getN());
            Assertions.assertEquals(single.getField(0).getTarget().positive.mean, x.positive.mean, 1e-9);
            final ProfileAccumulator.TargetStats s = acc.getField(2).getTarget();
            Assertions.assertEquals(151L, s.getFiPositive().getEstimate("v0"));   // even i with i % 3 == 0
            Assertions.assertEquals(0L, s.getFiNegative().getEstimate("v0"));
            final ProfileAccumulator.TargetStats b = acc.getField(3).getTarget();
            Assertions.assertEquals(301L, b.truePositive);
            Assertions.assertEquals(0L, b.trueNegative);
        }

        final ProfileRenderer.Config config = new ProfileRenderer.Config();
        config.title = "target";
        final ProfileRenderer.Result result = ProfileRenderer.render(merged, config);
        final JsonObject payload = JsonParser.parseString(result.payloadJson).getAsJsonObject();
        final JsonObject target = payload.getAsJsonObject("target");
        Assertions.assertEquals("1", target.get("positive").getAsString());
        final Map<String, JsonObject> targetFields = new HashMap<>();
        for(final JsonElement f : target.getAsJsonArray("fields")) {
            targetFields.put(f.getAsJsonObject().get("path").getAsString(), f.getAsJsonObject());
        }
        // classes 10..19 vs 0..9 with p = 1/3: r = 10 / sqrt(8.25 + 100 * 2/9) * sqrt(2/9) ≈ 0.85
        Assertions.assertEquals(0.853, targetFields.get("x").get("pointBiserial").getAsDouble(), 0.01);
        Assertions.assertTrue(targetFields.get("s").get("iv").getAsDouble() > 0.5);
        Assertions.assertEquals(1.0, targetFields.get("s").get("tvd").getAsDouble(), 1e-9);
        final JsonObject b = targetFields.get("b");
        Assertions.assertEquals("true", b.getAsJsonArray("labels").get(0).getAsString());
        Assertions.assertEquals(301L, b.getAsJsonArray("positive").get(0).getAsLong());
        Assertions.assertEquals(0L, b.getAsJsonArray("negative").get(0).getAsLong());
        Assertions.assertTrue(result.html.contains("\"kind\":\"target\""));
        Assertions.assertFalse(target.has("warning"));

        // a positive value no row matches is reported instead of rendering an empty analysis
        final ProfileSpec unmatched = ProfileSpec.of(schema, null, null, null, "default", false, false)
                .withTarget("label", "5");
        final ProfileCombineFn unmatchedFn = new ProfileCombineFn(unmatched);
        ProfileAccumulator acc = unmatchedFn.createAccumulator();
        for(int i = 0; i < 30; i++) {
            acc = unmatchedFn.addInput(acc, ProfileRow.of(unmatched, targetElement(i)));
        }
        final JsonObject unmatchedTarget = JsonParser.parseString(ProfileRenderer.render(acc, config).payloadJson)
                .getAsJsonObject().getAsJsonObject("target");
        Assertions.assertEquals(0L, unmatchedTarget.get("positiveRows").getAsLong());
        Assertions.assertTrue(unmatchedTarget.get("warning").getAsString().contains("no row matched the positive value `5`"),
                "warning: " + unmatchedTarget.get("warning"));
        Assertions.assertTrue(unmatchedTarget.getAsJsonArray("fields").get(0).getAsJsonObject().has("count"));
        Assertions.assertFalse(unmatchedTarget.getAsJsonArray("fields").get(0).getAsJsonObject().has("iv"));
    }

    @Test
    public void testSizeDegradationLadderCoversComparisons() throws Exception {
        final Schema schema = Schema.builder()
                .withField("x", Schema.FieldType.FLOAT64)
                .withField("label", Schema.FieldType.INT64)
                .withField("s", Schema.FieldType.STRING)
                .withField("b", Schema.FieldType.BOOLEAN)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", true, true)
                .withTarget("label", "1");
        final ProfileCombineFn fn = new ProfileCombineFn(spec);
        final ProfileCombineFn groupFn = new ProfileCombineFn(spec.groupSpec());
        final com.mercari.solution.util.pipeline.profile.ProfileAxis axis = new com.mercari.solution.util.pipeline.profile.ProfileAxis();
        axis.kind = com.mercari.solution.util.pipeline.profile.ProfileAxis.Kind.segments;
        axis.field = "s";
        axis.fieldIndex = 2;
        axis.sourceType = "string";
        ProfileAccumulator acc = fn.createAccumulator();
        final Map<String, ProfileAccumulator> subProfiles = new HashMap<>();
        for(int i = 0; i < 300; i++) {
            final ProfileRow row = ProfileRow.of(spec, targetElement(i));
            acc = fn.addInput(acc, row);
            final String key = axis.groupKey(axis.groupValue(row.values[2]));
            subProfiles.put(key, groupFn.addInput(subProfiles.computeIfAbsent(key, k -> groupFn.createAccumulator()), row));
        }
        final ProfileRenderer.Config config = new ProfileRenderer.Config();
        config.title = "ladder";
        config.axes = List.of(axis);
        config.embedLimitBytes = 1;   // nothing fits: every step must fire, in order, and the shortfall is recorded

        final ProfileRenderer.Result result = ProfileRenderer.render(acc, subProfiles, config);
        final List<String> steps = result.degradations;
        Assertions.assertEquals(7, steps.size(), String.join("\n", steps));
        Assertions.assertTrue(steps.get(0).startsWith("sketch binaries not embedded"), steps.get(0));
        Assertions.assertTrue(steps.get(1).startsWith("sample rows in payload reduced to 100"), steps.get(1));
        Assertions.assertTrue(steps.get(2).startsWith("histogram resolution reduced to 64 bins"), steps.get(2));
        Assertions.assertTrue(steps.get(3).startsWith("comparison groups limited to the top 5"), steps.get(3));
        Assertions.assertTrue(steps.get(4).startsWith("comparison resolution reduced to 16 bins / top 5 labels"), steps.get(4));
        Assertions.assertTrue(steps.get(5).startsWith("comparison distributions dropped"), steps.get(5));
        Assertions.assertTrue(steps.get(6).contains("still exceeds the size limit by"), steps.get(6));
        Assertions.assertNull(result.sketchesJson);

        // the degraded payload keeps totals and statistics but no per-bin arrays
        final JsonObject payload = JsonParser.parseString(result.payloadJson).getAsJsonObject();
        for(final JsonElement field : payload.getAsJsonArray("fields")) {
            Assertions.assertFalse(field.getAsJsonObject().has("overlay"), field.toString());
        }
        final JsonArray comparisons = payload.getAsJsonArray("comparisons");
        Assertions.assertEquals(2, comparisons.size());   // segments axis + target axis
        for(final JsonElement axisJson : comparisons) {
            for(final JsonElement group : axisJson.getAsJsonObject().getAsJsonArray("groups")) {
                final JsonObject fields = group.getAsJsonObject().getAsJsonObject("fields");
                for(final String path : fields.keySet()) {
                    Assertions.assertTrue(fields.getAsJsonObject(path).has("count"));
                    Assertions.assertFalse(fields.getAsJsonObject(path).has("hist"), path);
                    Assertions.assertFalse(fields.getAsJsonObject(path).has("topK"), path);
                }
            }
        }
        final JsonObject target = payload.getAsJsonObject("target");
        for(final JsonElement f : target.getAsJsonArray("fields")) {
            final JsonObject o = f.getAsJsonObject();
            Assertions.assertTrue(o.has("iv"), o.toString());
            Assertions.assertFalse(o.has("positive"), o.toString());
        }
        Assertions.assertEquals(7, JsonParser.parseString(result.manifestJson).getAsJsonObject().getAsJsonArray("degradations").size());

        // the same data under the default limit degrades nothing and keeps the full arrays
        config.embedLimitBytes = 25_000_000L;
        final ProfileRenderer.Result full = ProfileRenderer.render(acc, subProfiles, config);
        Assertions.assertTrue(full.degradations.isEmpty(), String.join("\n", full.degradations));
        final JsonObject fullTarget = JsonParser.parseString(full.payloadJson).getAsJsonObject().getAsJsonObject("target");
        Assertions.assertEquals(64, fullTarget.getAsJsonArray("fields").get(0).getAsJsonObject().getAsJsonArray("positive").size());
    }

    @Test
    public void testInformationValueIgnoresSparseTailNoise() {
        // 20 bins, 10,000 rows: identical classes except a few tail rows that sketch error moves around
        final long[] positive = new long[20];
        final long[] negative = new long[20];
        java.util.Arrays.fill(positive, 0, 5, 1000L);
        java.util.Arrays.fill(negative, 0, 5, 1000L);
        positive[17] = 3;
        negative[19] = 3;
        final long[] shifted = negative.clone();
        shifted[19] = 0;
        shifted[16] = 3;
        Assertions.assertTrue(ProfileRenderer.informationValue(positive, negative, true) < 0.001);
        Assertions.assertTrue(ProfileRenderer.informationValue(positive, shifted, true) < 0.001);
        // categories are not merged: a class missing from a frequent category is a real signal
        Assertions.assertTrue(ProfileRenderer.informationValue(new long[] { 100, 0 }, new long[] { 0, 100 }, false) > 1);
        // perfectly separating ordered bins keep a leak-level value
        Assertions.assertTrue(ProfileRenderer.informationValue(new long[] { 500, 500, 0, 0 }, new long[] { 0, 0, 500, 500 }, true) > 1);
    }

    private static MElement targetElement(final int i) {
        final boolean positive = i % 3 == 0;
        final Map<String, Object> values = new HashMap<>();
        values.put("x", (positive ? 10d : 0d) + i % 10);
        values.put("label", positive ? 1L : 0L);
        values.put("s", "v" + (positive ? i % 2 : 2 + i % 2));
        values.put("b", positive);
        return MElement.of(values, java.time.Instant.parse("2025-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testTargetDeclarationIsValidated() throws Exception {
        final Schema schema = Schema.builder()
                .withField("flag", Schema.FieldType.BOOLEAN)
                .withField("label", Schema.FieldType.INT64)
                .withField("status", Schema.FieldType.STRING)
                .withField("created_at", Schema.FieldType.TIMESTAMP)
                .build();
        final java.util.function.Supplier<ProfileSpec> spec = () -> ProfileSpec.of(schema, null, null, null, "default", false, false);

        Assertions.assertEquals(Boolean.TRUE, spec.get().withTarget("flag", null).getTarget().positive);
        Assertions.assertEquals(Boolean.FALSE, spec.get().withTarget("flag", "false").getTarget().positive);
        Assertions.assertEquals(Boolean.FALSE, spec.get().withTarget("flag", "false").getTarget().classOf(Boolean.TRUE));
        Assertions.assertEquals("sold", spec.get().withTarget("status", "sold").getTarget().positive);
        Assertions.assertNull(spec.get().withTarget("status", "sold").getTarget().classOf(null));
        Assertions.assertNull(spec.get().withTarget("status", "sold").getTarget().classOf(ProfileRow.Marker.ERROR));
        Assertions.assertEquals(1.0, spec.get().withTarget("label", 1L).getTarget().positive);
        // the sink passes number literals as text: each field type reads them its own way
        Assertions.assertEquals(1.0, spec.get().withTarget("label", "1").getTarget().positive);
        Assertions.assertEquals("1", spec.get().withTarget("status", "1").getTarget().positive);
        Assertions.assertEquals("1", spec.get().withTarget("status", 1.0).getTarget().positive);
        Assertions.assertEquals(Boolean.TRUE, spec.get().withTarget("flag", "1").getTarget().positive);
        Assertions.assertEquals(Boolean.FALSE, spec.get().withTarget("flag", "0").getTarget().positive);
        // non-finite numeric target values are neither class (the field statistics exclude them too)
        Assertions.assertNull(spec.get().withTarget("label", "1").getTarget().classOf(Double.NaN));
        Assertions.assertNull(spec.get().withTarget("label", "1").getTarget().classOf(Double.POSITIVE_INFINITY));
        Assertions.assertEquals(Boolean.TRUE, spec.get().withTarget("label", "1").getTarget().classOf(1.0));

        assertMessage(() -> spec.get().withTarget("label", null), "target.positive is required for the numeric field label");
        assertMessage(() -> spec.get().withTarget("label", "sold"), "must be a number");
        assertMessage(() -> spec.get().withTarget("label", true), "must be a number");
        assertMessage(() -> spec.get().withTarget("status", null), "target.positive is required for the string field status");
        assertMessage(() -> spec.get().withTarget("created_at", null), "must be a bool, numeric or string field");
        assertMessage(() -> spec.get().withTarget("missing", null), "target field not found");
        assertMessage(() -> spec.get().withTarget("flag", "yes"), "must be true or false");

        // the same rule surfaces from the sink configuration
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": { "type": "element", "elements": [{ "label": 1 }] },
                      "schema": { "fields": [{ "name": "label", "type": "long" }] }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["create"],
                      "parameters": { "output": "%s", "target": "label" }
                    }
                  ]
                }
                """.formatted(tempDir.resolve("target_invalid.html").toString().replace('\\', '/'));
        final Config config = Config.load(configJson);
        final Throwable e = Assertions.assertThrows(Throwable.class, () -> MPipeline.apply(pipeline, config));
        boolean found = false;
        for(Throwable t = e; t != null; t = t.getCause()) {
            found |= t.getMessage() != null && t.getMessage().contains("parameters.target: target.positive is required");
        }
        Assertions.assertTrue(found, "unexpected error: " + e);
    }

    private static void assertMessage(final org.junit.jupiter.api.function.Executable executable, final String expected) {
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, executable);
        Assertions.assertTrue(e.getMessage().contains(expected), "unexpected message: " + e.getMessage());
    }

    private static JsonObject extractJsonBlock(final String html, final String id) {
        final String marker = "<script type=\"application/json\" id=\"" + id + "\">";
        final int start = html.indexOf(marker);
        Assertions.assertTrue(start >= 0, "missing block: " + id);
        final int end = html.indexOf("</script>", start);
        return JsonParser.parseString(html.substring(start + marker.length(), end)).getAsJsonObject();
    }
}
