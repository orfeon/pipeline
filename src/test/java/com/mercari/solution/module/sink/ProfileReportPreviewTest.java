package com.mercari.solution.module.sink;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.profile.ProfileAccumulator;
import com.mercari.solution.util.pipeline.profile.ProfileAxis;
import com.mercari.solution.util.pipeline.profile.ProfileCombineFn;
import com.mercari.solution.util.pipeline.profile.ProfileRenderer;
import com.mercari.solution.util.pipeline.profile.ProfileSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Manual preview generator (not part of the regular suite): renders a realistic sample report
 * for visual inspection in a browser.
 * Run: mvn test -Dtest=ProfileReportPreviewTest -Dprofile.preview.dir=/path/to/dir
 */
public class ProfileReportPreviewTest {

    @Test
    @EnabledIfSystemProperty(named = "profile.preview.dir", matches = ".+")
    public void generatePreviewReport() throws Exception {

        final Schema schema = Schema.builder()
                .withField("user_id", Schema.FieldType.STRING)
                .withField("item_id", Schema.FieldType.STRING)
                .withField("category", Schema.FieldType.STRING)
                .withField("brand", Schema.FieldType.STRING)
                .withField("list_price", Schema.FieldType.FLOAT64)
                .withField("sold_price", Schema.FieldType.FLOAT64)
                .withField("weight_g", Schema.FieldType.FLOAT64)
                .withField("like_count", Schema.FieldType.INT64)
                .withField("sold_flag", Schema.FieldType.BOOLEAN)
                .withField("description", Schema.FieldType.STRING)
                .withField("created_at", Schema.FieldType.TIMESTAMP)
                .build();

        final ProfileSpec spec = ProfileSpec.of(
                schema, null, null, Set.of("user_id", "item_id"), "default", true, true);
        final ProfileCombineFn fn = new ProfileCombineFn(spec);

        final ProfileSpec groupSpec = ProfileSpec.of(schema, null, null, null, "default", false, false);
        final ProfileCombineFn groupFn = new ProfileCombineFn(groupSpec);
        final ProfileAxis segmentsAxis = new ProfileAxis();
        segmentsAxis.kind = ProfileAxis.Kind.segments;
        segmentsAxis.field = "category";
        segmentsAxis.fieldIndex = 2;
        segmentsAxis.sourceType = "string";
        final ProfileAxis timeAxis = new ProfileAxis();
        timeAxis.kind = ProfileAxis.Kind.time;
        timeAxis.field = "created_at";
        timeAxis.fieldIndex = 10;
        timeAxis.sourceType = "timestamp";
        timeAxis.granularity = "month";
        final List<ProfileAxis> axes = List.of(segmentsAxis, timeAxis);
        final Map<String, ProfileAccumulator> subProfiles = new HashMap<>();
        final Random random = new Random(42);
        final String[] categories = {"fashion", "electronics", "books", "toys", "sports", "beauty", "food"};
        final String[] brands = new String[100];
        for(int i = 0; i < brands.length; i++) {
            brands[i] = "brand_" + i;
        }

        ProfileAccumulator acc = fn.createAccumulator();
        final long base = 1735689600_000_000L; // 2025-01-01 (micros)
        for(int i = 0; i < 20_000; i++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("user_id", "u" + (10000 + random.nextInt(8000)));
            values.put("item_id", "m" + (100000 + i));
            values.put("category", categories[weightedIndex(random, categories.length)]);
            values.put("brand", random.nextDouble() < 0.3 ? null : brands[random.nextInt(brands.length)]);
            final double listPrice = Math.exp(6 + random.nextGaussian() * 1.2);
            values.put("list_price", Math.round(listPrice));
            values.put("sold_price", random.nextDouble() < 0.4 ? null : Math.round(listPrice * (0.7 + random.nextDouble() * 0.3)));
            values.put("weight_g", random.nextDouble() < 0.05 ? null : 50 + random.nextDouble() * 2000);
            values.put("like_count", (long) Math.max(0, (int) Math.exp(random.nextGaussian() * 1.5)));
            values.put("sold_flag", random.nextDouble() < 0.35);
            values.put("description", random.nextDouble() < 0.6 ? "A nice item number " + i : "");
            values.put("created_at", base + (long) (random.nextDouble() * 180) * 86400_000_000L);
            final MElement element = MElement.of(values, 1735689600000L);
            acc = fn.addInput(acc, element);
            for(final ProfileAxis axis : axes) {
                final String group = axis.groupValue(values.get(axis.field));
                if(group != null) {
                    final String key = axis.groupKey(group);
                    subProfiles.put(key, groupFn.addInput(
                            subProfiles.computeIfAbsent(key, k -> groupFn.createAccumulator()), element));
                }
            }
        }

        final ProfileRenderer.Config config = new ProfileRenderer.Config();
        config.title = "items dataset profile (preview)";
        config.jobName = "preview";
        config.moduleName = "profile";
        config.inputNames = List.of("items");
        config.axes = axes;
        config.comparePairs = List.<String[]>of(new String[] { "list_price", "sold_price" });
        final ProfileRenderer.Result first = ProfileRenderer.render(acc, subProfiles, config);

        // second render compares against the first render's artifact (exercises the Compare tab)
        config.compareWithSource = "profile_preview_past.html";
        final ProfileRenderer.PastReport past = ProfileRenderer.PastReport.parse("profile_preview_past.html", first.html);
        final ProfileRenderer.Result result = ProfileRenderer.render(acc, subProfiles, past, config);

        final Path dir = Paths.get(System.getProperty("profile.preview.dir"));
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("profile_preview_past.html"), first.html);
        final Path path = dir.resolve("profile_preview.html");
        Files.writeString(path, result.html);
        System.out.println("preview report written to: " + path);
    }

    /** skewed category distribution */
    private static int weightedIndex(final Random random, final int size) {
        final double r = random.nextDouble();
        return Math.min(size - 1, (int) (r * r * size));
    }
}
