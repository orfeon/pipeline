package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Config-driven e2e tests of the profile transform on a synthetic online-auction dataset: every
 * output's records, the report and payload files read back from {@code target/}, and the property
 * the transform exists for — two samples of one distribution compare as equal.
 */
public class ProfileTransformTest {

    private static final int ROWS = 2000;

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static String outputPath(final String name) throws Exception {
        final Path dir = Paths.get("target", "profile-transform-test");
        Files.createDirectories(dir);
        final Path path = dir.resolve(name);
        Files.deleteIfExists(path);
        return path.toAbsolutePath().toString().replace('\\', '/');
    }

    private static final String ITEM_SCHEMA = """
            "schema": {
              "fields": [
                { "name": "id", "type": "long" },
                { "name": "price", "type": "double" },
                { "name": "cost", "type": "double" },
                { "name": "qty", "type": "long" },
                { "name": "category", "type": "string" },
                { "name": "status", "type": "string" },
                { "name": "memo", "type": "string" },
                { "name": "active", "type": "boolean" },
                { "name": "sold", "type": "boolean" },
                { "name": "created_at", "type": "timestamp" }
              ]
            }
            """;

    /**
     * Items: {@code price} = 1.5 · id, {@code cost} the same values in another order, {@code qty} in
     * {1, 2, 3}, five even categories, {@code status} with the string 'null' on 30% of the rows,
     * {@code memo} always null, {@code sold} true above the median price.
     */
    private static String itemsSource(final String name) {
        final StringBuilder elements = new StringBuilder();
        for(int i = 0; i < ROWS; i++) {
            if(i > 0) {
                elements.append(",");
            }
            elements.append(String.format(Locale.ROOT,
                    "{ \"id\": %d, \"price\": %s, \"cost\": %s, \"qty\": %d, \"category\": \"cat%d\", \"status\": \"%s\", \"active\": %b, \"sold\": %b, \"created_at\": \"%s\" }",
                    i, i * 1.5, ((i * 7) % ROWS) * 1.5, 1 + i % 3, i % 5, i % 10 < 3 ? "null" : "listed", i % 2 == 0, i >= ROWS / 2,
                    Instant.parse("2025-01-01T00:00:00Z").plusSeconds(i * 3600L)));
        }
        return """
                {
                  "name": "%s",
                  "module": "create",
                  "parameters": { "type": "element", "elements": [%s] },
                  %s
                }
                """.formatted(name, elements, ITEM_SCHEMA);
    }

    private static Map<String, MElement> byField(final Iterable<MElement> records) {
        final Map<String, MElement> map = new HashMap<>();
        for(final MElement record : records) {
            map.put(record.getAsString("field"), record);
        }
        return map;
    }

    private static JsonObject extractJsonBlock(final String html, final String id) {
        final String marker = "<script type=\"application/json\" id=\"" + id + "\">";
        final int start = html.indexOf(marker);
        Assertions.assertTrue(start >= 0, "block not found: " + id);
        final int end = html.indexOf("</script>", start);
        return JsonParser.parseString(html.substring(start + marker.length(), end)).getAsJsonObject();
    }

    @Test
    public void testOutputsAndReport() throws Exception {
        final String report = outputPath("items.html");
        final String payload = outputPath("items.json");
        final String configJson = """
                {
                  "sources": [%s],
                  "transforms": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["items"],
                      "parameters": {
                        "output": { "report": "%s", "payload": "%s" },
                        "run": { "id": "run-1", "dataset": "items", "partition": "2025-01" },
                        "keys": ["id"],
                        "segments": ["category"],
                        "time": { "field": "created_at", "granularity": "month" },
                        "target": "sold",
                        "compare": [["price", "cost"]],
                        "report": { "title": "items profile" }
                      }
                    }
                  ]
                }
                """.formatted(itemsSource("items"), report, payload);

        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        for(final String name : List.of("profile", "profile.groups", "profile.values", "profile.bins", "profile.pairs",
                "profile.target", "profile.keys", "profile.summary")) {
            Assertions.assertNotNull(outputs.get(name), "output not found: " + name);
        }

        PAssert.that(outputs.get("profile").getCollection()).satisfies(records -> {
            final Map<String, MElement> fields = byField(records);
            Assertions.assertEquals(10, fields.size());
            final MElement price = fields.get("price");
            Assertions.assertEquals("run-1", price.getAsString("runId"));
            Assertions.assertEquals("items", price.getAsString("dataset"));
            Assertions.assertEquals("2025-01", price.getAsString("partition"));
            Assertions.assertEquals(2L, price.getAsLong("formatVersion"));
            Assertions.assertNotNull(price.getPrimitiveValue("generatedAt"));
            Assertions.assertEquals("numeric", price.getAsString("type"));
            Assertions.assertEquals((long) ROWS, price.getAsLong("rows"));
            Assertions.assertEquals((long) ROWS, price.getAsLong("count"));
            Assertions.assertEquals(0d, price.getAsDouble("min"));
            Assertions.assertEquals((ROWS - 1) * 1.5, price.getAsDouble("max"));
            Assertions.assertEquals(1.5 * ROWS * (ROWS - 1) / 2d, price.getAsDouble("sum"), 1e-6);
            Assertions.assertEquals((ROWS - 1) * 1.5 / 2, price.getAsDouble("mean"), 1e-6);
            Assertions.assertEquals((ROWS - 1) * 1.5 / 2, price.getAsDouble("p50"), ROWS * 1.5 * 0.05);
            // the target splits price at its median: the classes do not overlap
            Assertions.assertEquals("ks", price.getAsString("associationKind"));
            Assertions.assertEquals(1d, price.getAsDouble("association"), 0.02);
            // no drift axis without mode: compare
            Assertions.assertNull(price.getAsDouble("drift"));

            // qty: three values, tabulated exactly
            final MElement qty = fields.get("qty");
            Assertions.assertEquals(Boolean.TRUE, qty.getPrimitiveValue("distinctExact"));
            Assertions.assertEquals(3d, qty.getAsDouble("distinct"));
            Assertions.assertEquals(3d, qty.getAsDouble("distinct_lo"));

            // status: the string 'null' on 30% of the rows
            final MElement status = fields.get("status");
            Assertions.assertEquals(600L, status.getAsLong("nullLike"));
            Assertions.assertEquals(0.3, status.getAsDouble("nullLikeRate"), 1e-9);
            Assertions.assertEquals(0d, status.getAsDouble("nullRate"), 1e-9);
            Assertions.assertTrue(((List<?>) status.getPrimitiveValue("notable")).contains("null_like_value"));
            Assertions.assertEquals("listed", status.getAsString("top"));

            // memo: never set
            final MElement memo = fields.get("memo");
            Assertions.assertEquals(List.of("all_null"), memo.getPrimitiveValue("notable"));
            Assertions.assertEquals(1d, memo.getAsDouble("nullRate"), 1e-9);

            // id: an integer key, nearly unique; price is continuous and is not flagged for it
            Assertions.assertTrue(((List<?>) fields.get("id").getPrimitiveValue("notable")).contains("unique_like"));
            Assertions.assertFalse(((List<?>) price.getPrimitiveValue("notable")).contains("unique_like"));

            // timestamps report their range as timestamps
            Assertions.assertNotNull(fields.get("created_at").getPrimitiveValue("minTime"));
            Assertions.assertNull(fields.get("created_at").getAsDouble("min"));
            return null;
        });

        PAssert.that(outputs.get("profile.values").getCollection()).satisfies(records -> {
            final Map<String, Long> counts = new HashMap<>();
            for(final MElement record : records) {
                counts.put(record.getAsString("field") + "=" + record.getAsString("value"), record.getAsLong("count"));
                if("qty".equals(record.getAsString("field"))) {
                    Assertions.assertEquals(Boolean.TRUE, record.getPrimitiveValue("exact"));
                    Assertions.assertEquals(record.getAsLong("count"), record.getAsLong("count_lo"));
                }
            }
            Assertions.assertEquals(667L, counts.get("qty=1"));
            Assertions.assertEquals(667L, counts.get("qty=2"));
            Assertions.assertEquals(666L, counts.get("qty=3"));
            Assertions.assertEquals(400L, counts.get("category=cat0"));
            Assertions.assertEquals(600L, counts.get("status=null"));
            Assertions.assertEquals(1400L, counts.get("status=listed"));
            Assertions.assertEquals(1000L, counts.get("active=true"));
            Assertions.assertFalse(counts.keySet().stream().anyMatch(key -> key.startsWith("price=")));
            return null;
        });

        PAssert.that(outputs.get("profile.bins").getCollection()).satisfies(records -> {
            long priceAll = 0;
            int priceBins = 0;
            long priceInCat0 = 0;
            long pricePositive = 0;
            long memoNull = 0;
            for(final MElement record : records) {
                final String field = record.getAsString("field");
                final String axis = record.getAsString("axis");
                if("price".equals(field) && axis == null) {
                    priceAll += record.getAsLong("count");
                    priceBins++;
                    Assertions.assertEquals("quantile", record.getAsString("edgesKind"));
                    Assertions.assertEquals(Boolean.TRUE, record.getPrimitiveValue("exact"));
                    Assertions.assertEquals(0.1, record.getAsDouble("share"), 0.03);
                    Assertions.assertTrue(record.getAsDouble("lower") < record.getAsDouble("upper"));
                    // the classes split at the median: a bin is all positive or all negative, but for the one that holds it
                    final double rate = record.getAsDouble("rate");
                    Assertions.assertTrue(rate >= 0 && rate <= 1);
                } else if("price".equals(field) && "segments:category".equals(axis) && "cat0".equals(record.getAsString("group"))) {
                    priceInCat0 += record.getAsLong("count");
                } else if("price".equals(field) && "target".equals(axis) && "positive".equals(record.getAsString("group"))) {
                    pricePositive += record.getAsLong("count");
                } else if("memo".equals(field)) {
                    memoNull += record.getAsLong("count");
                }
            }
            Assertions.assertEquals(ROWS, priceAll);
            Assertions.assertEquals(10, priceBins);
            Assertions.assertEquals(ROWS / 5, priceInCat0);
            Assertions.assertEquals(ROWS / 2, pricePositive);
            // a field with no value has no cells: nothing to bin
            Assertions.assertEquals(0, memoNull);
            return null;
        });

        PAssert.that(outputs.get("profile.groups").getCollection()).satisfies(records -> {
            int categoryPrice = 0;
            int months = 0;
            for(final MElement record : records) {
                if("segments:category".equals(record.getAsString("axis")) && "price".equals(record.getAsString("field"))) {
                    categoryPrice++;
                    Assertions.assertEquals(400L, record.getAsLong("groupRows"));
                    Assertions.assertEquals(400L, record.getAsLong("count"));
                    Assertions.assertEquals(Boolean.FALSE, record.getPrimitiveValue("baseline"));
                    // every fifth row: the same distribution as the rest
                    Assertions.assertTrue(record.getAsDouble("ks") < 0.02, "ks: " + record.getAsDouble("ks"));
                    Assertions.assertTrue(record.getAsDouble("psi") < 0.01, "psi: " + record.getAsDouble("psi"));
                    Assertions.assertEquals(0d, record.getAsDouble("nullShift"), 1e-12);
                    Assertions.assertTrue(record.getAsDouble("noiseKs") > record.getAsDouble("ks"));
                    Assertions.assertEquals((ROWS - 1) * 1.5 / 2, record.getAsDouble("p50"), ROWS * 1.5 * 0.02);
                    Assertions.assertNull(record.getAsDouble("tvd"));
                }
                if("time:created_at".equals(record.getAsString("axis")) && "price".equals(record.getAsString("field"))) {
                    months++;
                    // price grows with time: a month is far from the rest
                    Assertions.assertTrue(record.getAsDouble("ks") > 0.4, "ks: " + record.getAsDouble("ks"));
                }
                if("segments:category".equals(record.getAsString("axis")) && "status".equals(record.getAsString("field"))) {
                    Assertions.assertNotNull(record.getAsDouble("tvd"));
                    Assertions.assertNull(record.getAsDouble("ks"));
                }
            }
            Assertions.assertEquals(5, categoryPrice);
            Assertions.assertEquals(3, months);
            return null;
        });

        PAssert.that(outputs.get("profile.target").getCollection()).satisfies(records -> {
            final Map<String, MElement> fields = byField(records);
            Assertions.assertFalse(fields.containsKey("sold"));
            Assertions.assertEquals(1d, fields.get("price").getAsDouble("ks"), 0.02);
            Assertions.assertTrue(fields.get("price").getAsDouble("iv") > 3);
            Assertions.assertTrue(fields.get("price").getAsDouble("pointBiserial") > 0.8);
            Assertions.assertTrue(fields.get("category").getAsDouble("tvd") < 0.01);
            Assertions.assertNull(fields.get("category").getAsDouble("ks"));
            // memo is never set: the positive rate among its null rows is the overall rate
            Assertions.assertFalse(fields.containsKey("memo"));
            return null;
        });

        PAssert.that(outputs.get("profile.pairs").getCollection()).satisfies(records -> {
            int count = 0;
            for(final MElement record : records) {
                count++;
                Assertions.assertEquals("price", record.getAsString("a"));
                Assertions.assertEquals("cost", record.getAsString("b"));
                // the same values in another order
                Assertions.assertEquals(0d, record.getAsDouble("ks"), 1e-12);
                Assertions.assertEquals(0d, record.getAsDouble("psi"), 1e-12);
                Assertions.assertEquals((long) ROWS, record.getAsLong("countA"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputs.get("profile.keys").getCollection()).satisfies(records -> {
            int count = 0;
            for(final MElement record : records) {
                count++;
                Assertions.assertEquals("key", record.getAsString("kind"));
                Assertions.assertEquals("id", record.getAsString("key"));
                Assertions.assertEquals(ROWS, record.getAsDouble("distinct"), ROWS * 0.05);
                Assertions.assertTrue(record.getAsDouble("keyness") <= 1d);
                Assertions.assertEquals(0L, record.getAsLong("nullKeys"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputs.get("profile.summary").getCollection()).satisfies(records -> {
            int count = 0;
            for(final MElement record : records) {
                count++;
                Assertions.assertEquals((long) ROWS, record.getAsLong("rows"));
                Assertions.assertEquals(0L, record.getAsLong("errorRows"));
                Assertions.assertEquals(10L, record.getAsLong("fields"));
                Assertions.assertEquals(report, record.getAsString("report"));
                Assertions.assertEquals(payload, record.getAsString("payload"));
                Assertions.assertTrue(record.getAsLong("reportBytes") > 10_000);
                Assertions.assertEquals(1L, record.getAsLong("allNull"));
                Assertions.assertEquals(1L, record.getAsLong("nullLikeValue"));
                Assertions.assertEquals(0.5, record.getAsDouble("targetRate"), 1e-9);
                Assertions.assertNull(record.getAsString("targetWarning"));
                Assertions.assertEquals(List.of(), record.getPrimitiveValue("degradations"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();

        // the report embeds the payload the records were built from
        final String html = Files.readString(Paths.get(report));
        Assertions.assertFalse(html.contains("__PROFILE_"), "unreplaced template placeholder");
        final JsonObject embedded = extractJsonBlock(html, "profile-payload");
        final JsonObject manifest = extractJsonBlock(html, "profile-manifest");
        Assertions.assertEquals(2, embedded.get("formatVersion").getAsInt());
        Assertions.assertEquals(ROWS, embedded.get("rows").getAsLong());
        Assertions.assertEquals("items profile", embedded.get("title").getAsString());
        Assertions.assertEquals(1, embedded.getAsJsonObject("notableCounts").get("all_null").getAsInt());
        Assertions.assertEquals("run-1", manifest.getAsJsonObject("job").get("runId").getAsString());
        Assertions.assertTrue(manifest.getAsJsonObject("counting").get("pass").getAsBoolean());

        final Map<String, JsonObject> fields = new HashMap<>();
        for(final JsonElement field : embedded.getAsJsonArray("fields")) {
            fields.put(field.getAsJsonObject().get("path").getAsString(), field.getAsJsonObject());
        }
        final JsonObject price = fields.get("price");
        Assertions.assertEquals(256, price.getAsJsonObject("numeric").getAsJsonObject("histogram").getAsJsonArray("counts").size());
        // the overlay carries the cells: edges one longer than the counts, counts adding up to the rows
        final JsonArray overlayEdges = price.getAsJsonObject("overlay").getAsJsonArray("edges");
        final JsonArray overlayAll = price.getAsJsonObject("overlay").getAsJsonArray("all");
        Assertions.assertEquals(overlayAll.size() + 1, overlayEdges.size());
        long overlayRows = 0;
        for(final JsonElement count : overlayAll) {
            overlayRows += count.getAsLong();
        }
        Assertions.assertEquals(ROWS, overlayRows);
        // the discrete numeric field carries its value table
        Assertions.assertEquals(3, fields.get("qty").getAsJsonObject("numeric").getAsJsonArray("values").size());
        Assertions.assertEquals("exact", fields.get("status").getAsJsonObject("string").get("topKKind").getAsString());

        // comparisons: the two axes and the target classes, each group aligned to the overlay
        final JsonArray comparisons = embedded.getAsJsonArray("comparisons");
        Assertions.assertEquals(3, comparisons.size());
        final JsonObject segments = comparisons.get(0).getAsJsonObject();
        Assertions.assertEquals("segments", segments.get("kind").getAsString());
        Assertions.assertEquals(5, segments.getAsJsonArray("groups").size());
        final JsonObject cat0Price = segments.getAsJsonArray("groups").get(0).getAsJsonObject()
                .getAsJsonObject("fields").getAsJsonObject("price");
        Assertions.assertEquals(overlayAll.size(), cat0Price.getAsJsonArray("hist").size());
        Assertions.assertEquals("target", comparisons.get(2).getAsJsonObject().get("kind").getAsString());

        final JsonObject targetPrice = embedded.getAsJsonObject("target").getAsJsonArray("fields").get(1).getAsJsonObject();
        Assertions.assertEquals("price", targetPrice.get("path").getAsString());
        Assertions.assertEquals(10, targetPrice.getAsJsonArray("positive").size());
        Assertions.assertEquals(11, targetPrice.getAsJsonArray("edges").size());
        Assertions.assertEquals(1, embedded.getAsJsonArray("fieldPairs").size());
        Assertions.assertEquals(1, embedded.getAsJsonObject("keys").getAsJsonArray("fields").size());

        // the payload file is the same payload and manifest in one document
        final JsonObject payloadFile = JsonParser.parseString(Files.readString(Paths.get(payload))).getAsJsonObject();
        Assertions.assertEquals(2, payloadFile.get("formatVersion").getAsInt());
        Assertions.assertEquals(embedded, payloadFile.getAsJsonObject("payload"));
        Assertions.assertEquals(manifest, payloadFile.getAsJsonObject("manifest"));
    }

    /** Two inputs drawn from one distribution, the second with 10% of {@code price} missing. */
    private static String sampleSource(final String name, final long seed, final int rows, final double missing) {
        final Random random = new Random(seed);
        final StringBuilder elements = new StringBuilder();
        for(int i = 0; i < rows; i++) {
            if(i > 0) {
                elements.append(",");
            }
            final double price = Math.exp(random.nextGaussian());
            final String category = "cat" + (int) Math.min(4, Math.floor(-Math.log(random.nextDouble()) * 1.5));
            if(random.nextDouble() < missing) {
                elements.append(String.format(Locale.ROOT, "{ \"id\": %d, \"category\": \"%s\" }", i, category));
            } else {
                elements.append(String.format(Locale.ROOT, "{ \"id\": %d, \"price\": %.6f, \"category\": \"%s\" }", i, price, category));
            }
        }
        return """
                {
                  "name": "%s",
                  "module": "create",
                  "parameters": { "type": "element", "elements": [%s] },
                  "schema": {
                    "fields": [
                      { "name": "id", "type": "long" },
                      { "name": "price", "type": "double" },
                      { "name": "category", "type": "string" }
                    ]
                  }
                }
                """.formatted(name, elements);
    }

    /** The regression the transform exists for (profile-engine.md §7): one distribution compares as one. */
    @Test
    public void testSameDistributionComparesAsEqual() throws Exception {
        final int rows = 5000;
        final String configJson = """
                {
                  "sources": [%s, %s],
                  "transforms": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["before", "after"],
                      "parameters": {
                        "mode": "compare",
                        "baseline": "before",
                        "drift": { "exclude": ["id"] },
                        "outputs": []
                      }
                    }
                  ]
                }
                """.formatted(sampleSource("before", 1, rows, 0), sampleSource("after", 2, rows, 0.1));

        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));

        PAssert.that(outputs.get("profile.groups").getCollection()).satisfies(records -> {
            int checked = 0;
            for(final MElement record : records) {
                Assertions.assertEquals("inputs", record.getAsString("axis"));
                final boolean baseline = Boolean.TRUE.equals(record.getPrimitiveValue("baseline"));
                Assertions.assertEquals("before".equals(record.getAsString("group")), baseline);
                if(baseline) {
                    // the reference has nothing to be compared with
                    Assertions.assertNull(record.getAsDouble("ks"));
                    Assertions.assertNull(record.getAsDouble("psi"));
                    Assertions.assertNull(record.getAsDouble("nullShift"));
                    continue;
                }
                if("price".equals(record.getAsString("field"))) {
                    checked++;
                    Assertions.assertTrue(record.getAsDouble("psi") < 0.01, "psi: " + record.getAsDouble("psi"));
                    Assertions.assertTrue(record.getAsDouble("ks") < 2 * record.getAsDouble("noiseKs"),
                            "ks " + record.getAsDouble("ks") + " noise " + record.getAsDouble("noiseKs"));
                    // the values are the same; what moved is how often there is one
                    Assertions.assertEquals(0.1, record.getAsDouble("nullShift"), 0.02);
                    Assertions.assertEquals(9 * (1d / record.getAsLong("count") + 1d / rows), record.getAsDouble("noisePsi"), 1e-9);
                }
                if("category".equals(record.getAsString("field"))) {
                    checked++;
                    Assertions.assertTrue(record.getAsDouble("tvd") < 0.03, "tvd: " + record.getAsDouble("tvd"));
                    Assertions.assertTrue(record.getAsDouble("psi") < 0.01, "psi: " + record.getAsDouble("psi"));
                }
            }
            Assertions.assertEquals(2, checked);
            return null;
        });

        PAssert.that(outputs.get("profile").getCollection()).satisfies(records -> {
            final Map<String, MElement> fields = byField(records);
            final MElement price = fields.get("price");
            Assertions.assertEquals("ks", price.getAsString("driftKind"));
            Assertions.assertEquals("after", price.getAsString("driftVs"));
            Assertions.assertTrue(price.getAsDouble("drift") < 0.05);
            Assertions.assertEquals(0.1, price.getAsDouble("nullShift"), 0.02);
            Assertions.assertEquals("tvd", fields.get("category").getAsString("driftKind"));
            // excluded from the drift ranking, still profiled
            Assertions.assertNull(fields.get("id").getAsDouble("drift"));
            Assertions.assertEquals(2L * rows, fields.get("id").getAsLong("count"));
            return null;
        });

        // `outputs: []`: the heavy outputs are declared and empty
        PAssert.that(outputs.get("profile.bins").getCollection()).empty();
        PAssert.that(outputs.get("profile.values").getCollection()).empty();
        PAssert.that(outputs.get("profile.summary").getCollection()).satisfies(records -> {
            for(final MElement record : records) {
                Assertions.assertNull(record.getAsString("report"));
                Assertions.assertNull(record.getAsLong("reportBytes"));
                Assertions.assertEquals(2L * rows, record.getAsLong("rows"));
            }
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testDeclaredEdgesAndHiddenValues() throws Exception {
        final String report = outputPath("hidden.html");
        final String configJson = """
                {
                  "sources": [%s],
                  "transforms": [
                    {
                      "name": "profile",
                      "module": "profile",
                      "inputs": ["items"],
                      "parameters": {
                        "output": "%s",
                        "values": "hide",
                        "segments": ["category", "qty"],
                        "drift": { "axis": "segments:category" },
                        "bins": { "count": 5, "edges": { "price": [300, 1500] } }
                      }
                    }
                  ]
                }
                """.formatted(itemsSource("items"), report);

        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));

        PAssert.that(outputs.get("profile.bins").getCollection()).satisfies(records -> {
            final Map<Long, Long> priceBands = new HashMap<>();
            int costBins = 0;
            for(final MElement record : records) {
                if(record.getAsString("axis") != null) {
                    // segment values are raw data: masked
                    Assertions.assertTrue(record.getAsString("group").startsWith("group #"), record.getAsString("group"));
                    continue;
                }
                if("price".equals(record.getAsString("field"))) {
                    Assertions.assertEquals("declared", record.getAsString("edgesKind"));
                    priceBands.put(record.getAsLong("bin"), record.getAsLong("count"));
                } else if("cost".equals(record.getAsString("field"))) {
                    costBins++;
                } else if("category".equals(record.getAsString("field"))) {
                    Assertions.assertTrue(record.getAsString("value").startsWith("#") || "(other)".equals(record.getAsString("value")));
                }
            }
            // right-closed bands over price = 1.5 · id: (.., 300], (300, 1500], (1500, ..)
            Assertions.assertEquals(Map.of(0L, 201L, 1L, 800L, 2L, 999L), priceBands);
            Assertions.assertEquals(5, costBins);
            return null;
        });

        PAssert.that(outputs.get("profile.values").getCollection()).satisfies(records -> {
            for(final MElement record : records) {
                if("status".equals(record.getAsString("field"))) {
                    Assertions.assertTrue(record.getAsString("value").startsWith("#"));
                }
                if("active".equals(record.getAsString("field"))) {
                    // true / false are not raw values
                    Assertions.assertTrue(List.of("true", "false").contains(record.getAsString("value")));
                }
            }
            return null;
        });

        PAssert.that(outputs.get("profile").getCollection()).satisfies(records -> {
            final Map<String, MElement> fields = byField(records);
            Assertions.assertNull(fields.get("status").getAsString("top"));
            // the drift columns name the group as the groups output does: masked
            final MElement status = fields.get("status");
            Assertions.assertEquals("tvd", status.getAsString("driftKind"));
            Assertions.assertTrue(status.getAsString("driftVs").startsWith("group #"), status.getAsString("driftVs"));
            Assertions.assertTrue(status.getAsDouble("drift") > 0.2);
            // the axis field differs from the rest of its own groups by construction: no drift columns
            final MElement category = fields.get("category");
            Assertions.assertNull(category.getAsDouble("drift"));
            Assertions.assertNull(category.getAsString("driftVs"));
            Assertions.assertNull(category.getAsDouble("nullShift"));
            // a segment axis that is not the drift axis feeds nothing into these columns
            Assertions.assertNotNull(fields.get("qty").getAsDouble("drift"));
            return null;
        });

        PAssert.that(outputs.get("profile.groups").getCollection()).satisfies(records -> {
            int qtyGroups = 0;
            for(final MElement record : records) {
                // numeric segment values are raw values too
                Assertions.assertTrue(record.getAsString("group").startsWith("group #"), record.getAsString("group"));
                if("segments:qty".equals(record.getAsString("axis")) && "price".equals(record.getAsString("field"))) {
                    qtyGroups++;
                }
            }
            Assertions.assertEquals(3, qtyGroups);
            return null;
        });

        pipeline.run();

        final String html = Files.readString(Paths.get(report));
        Assertions.assertFalse(html.contains("listed"), "a raw value leaked into a values: hide report");
        Assertions.assertFalse(html.contains("cat0"), "a raw segment value leaked into a values: hide report");
        final JsonObject embedded = extractJsonBlock(html, "profile-payload");
        Assertions.assertEquals("hide", embedded.get("values").getAsString());
        Assertions.assertFalse(embedded.has("sample"));
        for(final JsonElement element : embedded.getAsJsonArray("fields")) {
            final JsonObject field = element.getAsJsonObject();
            // the exact value table of a discrete numeric field is a list of raw values
            if(field.has("numeric")) {
                Assertions.assertFalse(field.getAsJsonObject("numeric").has("values"), field.get("path").getAsString());
            }
            if(field.has("drift") && !field.getAsJsonObject("drift").get("vs").isJsonNull()) {
                Assertions.assertTrue(field.getAsJsonObject("drift").get("vs").getAsString().startsWith("group #"));
            }
        }
    }

    /** No parameters at all: one record per field, no second pass, no file. */
    @Test
    public void testNoParameters() throws Exception {
        final String configJson = """
                {
                  "sources": [%s],
                  "transforms": [
                    { "name": "profile", "module": "profile", "inputs": ["before"] }
                  ]
                }
                """.formatted(sampleSource("before", 3, 300, 0.2));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        PAssert.that(outputs.get("profile").getCollection()).satisfies(records -> {
            final Map<String, MElement> fields = byField(records);
            Assertions.assertEquals(3, fields.size());
            Assertions.assertEquals(300L, fields.get("id").getAsLong("count"));
            Assertions.assertEquals(0.2, fields.get("price").getAsDouble("nullRate"), 0.08);
            Assertions.assertEquals("profile", fields.get("price").getAsString("dataset"));
            Assertions.assertNull(fields.get("price").getAsString("partition"));
            return null;
        });
        // the default outputs include the bins, so the counting pass ran
        PAssert.that(outputs.get("profile.bins").getCollection()).satisfies(records -> {
            long priceRows = 0;
            for(final MElement record : records) {
                if("price".equals(record.getAsString("field")) && !"(null)".equals(record.getAsString("value"))) {
                    priceRows += record.getAsLong("count");
                    Assertions.assertNull(record.getAsLong("positives"));
                }
            }
            Assertions.assertTrue(priceRows > 200 && priceRows < 280, "price rows: " + priceRows);
            return null;
        });
        PAssert.that(outputs.get("profile.groups").getCollection()).empty();
        PAssert.that(outputs.get("profile.target").getCollection()).empty();
        PAssert.that(outputs.get("profile.pairs").getCollection()).empty();
        PAssert.that(outputs.get("profile.keys").getCollection()).empty();
        pipeline.run();
    }

    @Test
    public void testRejectedParameters() {
        final Map<String, String> cases = new HashMap<>(Map.of(
                "\"segments\": [\"category\", { \"field\": \"category\", \"topK\": 3 }]", "lists the field more than once",
                "\"segments\": [{ \"field\": \"category\", \"topK\": 0 }]", "topK must be positive"));
        cases.putAll(Map.of(
                "\"previous\": { \"report\": \"x.html\" }", "parameters.previous is not implemented yet",
                "\"expectations\": []", "parameters.expectations is not implemented yet",
                "\"compareWith\": \"x.html\"", "removed profile sink",
                "\"output\": { \"report\": \"x.html\", \"sketches\": \"x.json\" }", "parameters.output.sketches",
                "\"outputs\": [\"sketches\"]", "parameters.outputs accepts",
                "\"bins\": { \"mode\": \"sketch\" }", "bins.mode: sketch is not implemented yet",
                "\"bins\": { \"edges\": { \"price\": [3, 1] } }", "strictly increasing",
                "\"bins\": { \"edges\": { \"category\": [1] } }", "must be numeric",
                "\"drift\": { \"axis\": \"segments:category\" }", "parameters.drift.axis must be one of the declared axes",
                "\"segmentz\": [\"category\"]", "unknown parameter: parameters.segmentz"));
        for(final Map.Entry<String, String> entry : cases.entrySet()) {
            final String configJson = """
                    {
                      "sources": [%s],
                      "transforms": [
                        { "name": "profile", "module": "profile", "inputs": ["before"], "parameters": { %s } }
                      ]
                    }
                    """.formatted(sampleSource("before", 1, 5, 0), entry.getKey());
            final TestPipeline rejected = TestPipeline.create().enableAbandonedNodeEnforcement(false);
            final Throwable error = Assertions.assertThrows(Throwable.class,
                    () -> MPipeline.apply(rejected, load(configJson)), entry.getKey());
            Assertions.assertTrue(messages(error).contains(entry.getValue()),
                    "expected `" + entry.getValue() + "` for " + entry.getKey() + " but was: " + messages(error));
        }
    }

    private static Config load(final String configJson) {
        try {
            return Config.load(configJson);
        } catch (final java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String messages(final Throwable error) {
        final StringBuilder sb = new StringBuilder();
        for(Throwable e = error; e != null; e = e.getCause()) {
            sb.append(e.getMessage()).append(" | ");
        }
        return sb.toString();
    }
}
