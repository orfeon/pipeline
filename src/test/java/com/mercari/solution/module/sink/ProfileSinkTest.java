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

    private static JsonObject extractJsonBlock(final String html, final String id) {
        final String marker = "<script type=\"application/json\" id=\"" + id + "\">";
        final int start = html.indexOf(marker);
        Assertions.assertTrue(start >= 0, "missing block: " + id);
        final int end = html.indexOf("</script>", start);
        return JsonParser.parseString(html.substring(start + marker.length(), end)).getAsJsonObject();
    }
}
