package com.mercari.solution.util.pipeline.profile;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Pure tests of the profile's statistics and of the two passes without a pipeline: the functions of
 * exact counts against brute force, the counting pass against a direct count, and the first pass's
 * exact value table and null sentinels.
 */
public class ProfileReportTest {

    // ---- statistics ----

    @Test
    public void testKsTvdPsi() {
        final long[] a = { 10, 20, 30, 40 };
        final long[] b = { 40, 30, 20, 10 };
        // cumulative shares: a 0.1 0.3 0.6 1.0 / b 0.4 0.7 0.9 1.0
        Assertions.assertEquals(0.4, ProfileReport.ks(a, b), 1e-12);
        Assertions.assertEquals(0.4, ProfileReport.tvd(a, b), 1e-12);
        Assertions.assertEquals(0d, ProfileReport.ks(a, a), 1e-12);
        Assertions.assertEquals(0d, ProfileReport.psi(a, a), 1e-12);
        // the smoothed PSI by hand
        double expected = 0;
        for(int i = 0; i < 4; i++) {
            final double pa = (a[i] + 0.5) / 102d;
            final double pb = (b[i] + 0.5) / 102d;
            expected += (pa - pb) * Math.log(pa / pb);
        }
        Assertions.assertEquals(expected, ProfileReport.psi(a, b), 1e-12);
        // an empty side has no statistic
        Assertions.assertNull(ProfileReport.ks(a, new long[4]));
        Assertions.assertNull(ProfileReport.tvd(new long[4], b));
        Assertions.assertNull(ProfileReport.psi(a, new long[4]));
        // a bin empty on one side is a finite term, not an epsilon blow-up
        final double oneSided = ProfileReport.psi(new long[] { 0, 100 }, new long[] { 50, 50 });
        Assertions.assertTrue(Double.isFinite(oneSided) && oneSided > 0);
    }

    @Test
    public void testNoise() {
        Assertions.assertEquals(1.36 * Math.sqrt(1d / 303 + 1d / 900000), ProfileReport.noiseKs(303, 900000), 1e-12);
        Assertions.assertEquals(9 * (1d / 303 + 1d / 900000), ProfileReport.noisePsi(10, 303, 900000), 1e-12);
        Assertions.assertNull(ProfileReport.noiseKs(0, 10));
        Assertions.assertNull(ProfileReport.noisePsi(1, 10, 10));
    }

    @Test
    public void testMergeBins() {
        // 100 equal cells → 10 bins of 10 cells
        final long[] cells = new long[100];
        Arrays.fill(cells, 7);
        final int[] binOfCell = ProfileReport.mergeBins(cells, 10);
        for(int i = 0; i < 100; i++) {
            Assertions.assertEquals(i / 10, binOfCell[i]);
        }
        Assertions.assertEquals(10, ProfileReport.binCount(binOfCell));
        final long[] bins = ProfileReport.toBins(cells, binOfCell);
        for(final long bin : bins) {
            Assertions.assertEquals(70, bin);
        }

        // one heavy cell passes several targets at once: bins stay dense and no bin is empty
        final long[] heavy = { 1, 1, 96, 1, 1 };
        final int[] heavyBins = ProfileReport.mergeBins(heavy, 10);
        Assertions.assertArrayEquals(new int[] { 0, 0, 0, 1, 1 }, heavyBins);
        for(final long bin : ProfileReport.toBins(heavy, heavyBins)) {
            Assertions.assertTrue(bin > 0);
        }

        // fewer cells than bins: one bin per non-trailing cell
        Assertions.assertArrayEquals(new int[] { 0, 1, 2 }, ProfileReport.mergeBins(new long[] { 5, 5, 5 }, 10));
        // no rows: a single bin
        Assertions.assertArrayEquals(new int[] { 0, 0, 0 }, ProfileReport.mergeBins(new long[3], 10));
        // every assignment is non-decreasing and counts are preserved
        final Random random = new Random(7);
        for(int trial = 0; trial < 50; trial++) {
            final long[] counts = new long[1 + random.nextInt(100)];
            for(int i = 0; i < counts.length; i++) {
                counts[i] = random.nextInt(4) == 0 ? 0 : random.nextInt(1000);
            }
            final int[] assignment = ProfileReport.mergeBins(counts, 10);
            for(int i = 1; i < assignment.length; i++) {
                Assertions.assertTrue(assignment[i] == assignment[i - 1] || assignment[i] == assignment[i - 1] + 1);
            }
            Assertions.assertTrue(ProfileReport.binCount(assignment) <= 10);
            Assertions.assertEquals(ProfileReport.total(counts), ProfileReport.total(ProfileReport.toBins(counts, assignment)));
        }
    }

    @Test
    public void testMedianAndWilson() {
        // cells [0,10] (10,20] (20,30]: the median of 10 / 20 / 10 rows is the middle of the second cell
        final double[] edges = { 0, 10, 20, 30 };
        Assertions.assertEquals(15d, ProfileReport.median(new long[] { 10, 20, 10 }, edges, 0, 30), 1e-12);
        // the group's own range bounds the interpolation
        Assertions.assertEquals(12d, ProfileReport.median(new long[] { 0, 8, 0 }, edges, 11, 13), 1e-12);
        Assertions.assertNull(ProfileReport.median(new long[3], edges, 0, 30));

        final double[] interval = ProfileReport.wilson(50, 100);
        Assertions.assertTrue(interval[0] < 0.5 && interval[1] > 0.5);
        Assertions.assertEquals(0.404, interval[0], 0.002);
        Assertions.assertEquals(0.596, interval[1], 0.002);
        Assertions.assertEquals(0d, ProfileReport.wilson(0, 10)[0], 1e-12);
        Assertions.assertNull(ProfileReport.wilson(0, 0));
    }

    @Test
    public void testNullLikeAndNames() {
        for(final String value : List.of("", " ", "　", "　　 ", "null", "NULL", " Null ", "None", "NaN", "N/A", "na", "-")) {
            Assertions.assertTrue(ProfileReport.isNullLike(value), "expected null-like: [" + value + "]");
        }
        for(final String value : List.of("0", "nullable", "none of them", "a", "--", "x-")) {
            Assertions.assertFalse(ProfileReport.isNullLike(value), "expected a value: [" + value + "]");
        }
        Assertions.assertFalse(ProfileReport.isNullLike(null));
        Assertions.assertEquals("allNull", ProfileReport.notableColumn("all_null"));
        Assertions.assertEquals("nullLikeOnly", ProfileReport.notableColumn("null_like_only"));
        Assertions.assertEquals("skewed", ProfileReport.notableColumn("skewed"));
    }

    // ---- the two passes without a pipeline ----

    private static Schema schema() {
        return Schema.builder()
                .withField("price", Schema.FieldType.FLOAT64)
                .withField("cost", Schema.FieldType.FLOAT64)
                .withField("qty", Schema.FieldType.INT64)
                .withField("category", Schema.FieldType.STRING)
                .withField("active", Schema.FieldType.BOOLEAN)
                .withField("sold", Schema.FieldType.BOOLEAN)
                .build();
    }

    private static ProfileSpec spec() {
        return ProfileSpec.of(schema(), null, null, null, "default", false, false).withTarget("sold", null);
    }

    private static List<MElement> elements(final int rows, final long seed) {
        final Random random = new Random(seed);
        final List<MElement> elements = new ArrayList<>();
        for(int i = 0; i < rows; i++) {
            final double price = Math.exp(random.nextGaussian());
            final MElement.Builder builder = MElement.builder()
                    .withFloat64("price", i % 50 == 0 ? null : (Double) price)
                    .withFloat64("cost", Math.exp(random.nextGaussian()))
                    .withInt64("qty", (long) (1 + random.nextInt(3)))
                    .withString("category", "cat" + random.nextInt(4))
                    .withBool("active", random.nextBoolean())
                    .withBool("sold", i % 97 == 0 ? null : (Boolean) (price + 0.5 * random.nextGaussian() > 1.2));
            elements.add(builder.build());
        }
        return elements;
    }

    private record Passes(ProfileSpec spec, ProfileAccumulator accumulator, ProfileEdges edges, Map<String, ProfileCells> cells, List<ProfileRow> rows) {}

    /** Runs both passes in memory: every row is counted in the whole dataset and in its target class. */
    private static Passes passes(final int rows, final long seed, final Map<String, double[]> declared, final List<String[]> pairs) {
        final ProfileSpec spec = spec();
        final ProfileAccumulator accumulator = ProfileAccumulator.of(spec);
        final List<ProfileRow> profileRows = new ArrayList<>();
        for(final MElement element : elements(rows, seed)) {
            final ProfileRow row = ProfileRow.of(spec, element);
            profileRows.add(row);
            accumulator.add(row.values, null);
        }
        return secondPass(spec, accumulator, profileRows, declared, pairs);
    }

    /** The counting pass over the edges of a given first pass (the same sketch state gives the same edges). */
    private static Passes secondPass(
            final ProfileSpec spec,
            final ProfileAccumulator accumulator,
            final List<ProfileRow> profileRows,
            final Map<String, double[]> declared,
            final List<String[]> pairs) {

        final ProfileEdges edges = ProfileEdges.of(accumulator.copy(), declared, pairs, 50);
        final Map<String, ProfileCells> cells = new HashMap<>();
        for(final ProfileRow row : profileRows) {
            final ProfileCells.Row located = ProfileCells.locate(spec, edges, row, declared != null && !declared.isEmpty());
            cells.computeIfAbsent(ProfileReport.ALL_KEY, k -> ProfileCells.of(spec.getFields().size()))
                    .add(ProfileCells.withPairs(edges, located));
            if(located.targetClass >= 0) {
                cells.computeIfAbsent(located.targetClass == 1 ? ProfileReport.TARGET_POSITIVE_KEY : ProfileReport.TARGET_NEGATIVE_KEY,
                        k -> ProfileCells.of(spec.getFields().size())).add(located);
            }
        }
        return new Passes(spec, accumulator, edges, cells, profileRows);
    }

    private static ProfileReport.Config config(final Map<String, double[]> declared, final List<String[]> pairs) {
        final ProfileReport.Config config = new ProfileReport.Config();
        config.runId = "run";
        config.dataset = "items";
        config.declaredEdges = declared == null ? Map.of() : declared;
        config.comparePairs = pairs == null ? List.of() : pairs;
        return config;
    }

    @Test
    public void testCellsAreExact() {
        final Passes passes = passes(5000, 11, null, null);
        final ProfileCells all = passes.cells.get(ProfileReport.ALL_KEY);
        final int price = 0;
        final double[] splits = passes.edges.getSplits(price);
        Assertions.assertTrue(splits.length > 90, "expected about 99 split points: " + splits.length);

        // brute force: right-closed cells over the same split points
        final long[] expected = new long[splits.length + 1];
        long nulls = 0;
        double sum = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for(final ProfileRow row : passes.rows) {
            final Double value = (Double) row.values[price];
            if(value == null) {
                nulls++;
                continue;
            }
            int cell = 0;
            while(cell < splits.length && value > splits[cell]) {
                cell++;
            }
            expected[cell]++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        Assertions.assertArrayEquals(expected, all.cells(price, splits.length + 1));
        Assertions.assertEquals(nulls, all.nulls(price));
        Assertions.assertEquals(ProfileReport.total(expected), all.count(price));
        Assertions.assertEquals(sum, all.sum(price), 1e-6);
        Assertions.assertEquals(min, all.min(price));
        Assertions.assertEquals(max, all.max(price));
        // equal-frequency: no cell holds much more than 1 / 100 of the rows beyond the sketch's rank error
        for(final long count : expected) {
            Assertions.assertTrue(count < 5000 * 0.05, "cell far from equal frequency: " + count);
        }

        // categorical cells: the counted values and "(other)"
        final int category = 3;
        final String[] categories = passes.edges.getCategories(category);
        Assertions.assertEquals(4, categories.length);
        final long[] categoryCells = all.cells(category, categories.length + 1);
        Assertions.assertEquals(5000, ProfileReport.total(categoryCells));
        Assertions.assertEquals(0, categoryCells[categories.length]);

        // bool cells: true, false
        final long[] activeCells = all.cells(4, 2);
        Assertions.assertEquals(5000, activeCells[0] + activeCells[1]);
    }

    @Test
    public void testCellsMerge() {
        final Passes passes = passes(3000, 5, Map.of("price", new double[] { 0.5, 1, 2 }), List.<String[]>of(new String[] { "price", "cost" }));
        final int fields = passes.spec.getFields().size();
        final ProfileCells whole = ProfileCells.of(fields);
        final ProfileCells left = ProfileCells.of(fields);
        final ProfileCells right = ProfileCells.of(fields);
        int i = 0;
        for(final ProfileRow row : passes.rows) {
            final ProfileCells.Row located = ProfileCells.withPairs(passes.edges, ProfileCells.locate(passes.spec, passes.edges, row, true));
            whole.add(located);
            (i++ % 3 == 0 ? left : right).add(located);
        }
        final ProfileCells merged = ProfileCells.of(fields).merge(left).merge(right);
        for(int f = 0; f < fields; f++) {
            final int size = passes.edges.cellCount(passes.spec.getFields().get(f).profileType, f);
            Assertions.assertArrayEquals(whole.cells(f, size), merged.cells(f, size));
            Assertions.assertEquals(whole.count(f), merged.count(f));
            Assertions.assertEquals(whole.nulls(f), merged.nulls(f));
            if(whole.hasNumeric(f)) {
                Assertions.assertEquals(whole.mean(f), merged.mean(f), 1e-9);
                Assertions.assertEquals(whole.m2(f), merged.m2(f), 1e-6);
                Assertions.assertEquals(whole.sum(f), merged.sum(f), 1e-6);
                Assertions.assertEquals(whole.min(f), merged.min(f));
            }
        }
        Assertions.assertArrayEquals(whole.declared(0, 4), merged.declared(0, 4));
        final int pairSize = passes.edges.getPairSplits(0).length + 1;
        Assertions.assertArrayEquals(whole.pairA(0, pairSize), merged.pairA(0, pairSize));
        Assertions.assertArrayEquals(whole.pairB(0, pairSize), merged.pairB(0, pairSize));
        Assertions.assertEquals(whole.getRows(), merged.getRows());
        Assertions.assertEquals(whole.getTargetPositive(), merged.getTargetPositive());
    }

    @Test
    public void testResultFromBothPasses() {
        final Passes passes = passes(5000, 23, null, List.<String[]>of(new String[] { "price", "cost" }));
        final ProfileReport.Config config = config(null, List.<String[]>of(new String[] { "price", "cost" }));
        final ProfileReport.Result result = ProfileReport.build(passes.accumulator, passes.cells, passes.edges, config, null);

        Assertions.assertEquals(5000, result.rows);
        final ProfileReport.FieldResult price = result.fields.get(0);
        Assertions.assertEquals("quantile", price.edgesKind);
        Assertions.assertEquals(10, price.bins.length);
        Assertions.assertEquals(price.count, ProfileReport.total(price.bins));
        // equal-frequency bins: each holds about a tenth of the rows
        for(final long bin : price.bins) {
            Assertions.assertEquals(0.1, (double) bin / price.count, 0.03);
        }
        // bins tile the observed range
        Assertions.assertEquals(price.min, price.binLower[0]);
        Assertions.assertEquals(price.max, price.binUpper[9]);
        for(int b = 1; b < 10; b++) {
            Assertions.assertEquals(price.binUpper[b - 1], price.binLower[b]);
        }
        Assertions.assertEquals(100, price.nulls);
        Assertions.assertEquals(passes.accumulator.getField(0).sum, price.sum, 1e-6);

        // qty has three distinct values: an exact table, most frequent first
        final ProfileReport.FieldResult qty = result.fields.get(2);
        Assertions.assertTrue(qty.distinctExact);
        Assertions.assertEquals(3d, qty.distinct);
        Assertions.assertEquals("exact", qty.valuesKind);
        Assertions.assertEquals(3, qty.values.size());
        Assertions.assertEquals(5000, qty.values.stream().mapToLong(v -> v.count).sum());
        Assertions.assertTrue(qty.values.get(0).count >= qty.values.get(1).count);
        Assertions.assertTrue(Set.of("1", "2", "3").contains(qty.values.get(0).value));
        // a continuous field keeps no table
        Assertions.assertFalse(price.distinctExact);
        Assertions.assertNull(price.valuesKind);

        // the target: sold depends on price and not on cost
        final ProfileReport.TargetResult target = result.target;
        Assertions.assertNotNull(target.positiveGroup);
        Assertions.assertEquals(5000, target.positiveRows + target.negativeRows + target.nullRows);
        final Map<String, ProfileReport.TargetField> targetFields = new HashMap<>();
        for(final ProfileReport.TargetField field : target.fields) {
            targetFields.put(result.fields.get(field.index).path, field);
        }
        Assertions.assertFalse(targetFields.containsKey("sold"));
        Assertions.assertTrue(targetFields.get("price").ks > 0.5, "price ks: " + targetFields.get("price").ks);
        Assertions.assertTrue(targetFields.get("cost").ks < 0.06, "cost ks: " + targetFields.get("cost").ks);
        Assertions.assertTrue(targetFields.get("price").iv > targetFields.get("cost").iv);
        Assertions.assertTrue(targetFields.get("price").pointBiserial > 0.3);
        Assertions.assertNotNull(targetFields.get("category").tvd);
        Assertions.assertNull(targetFields.get("category").ks);
        Assertions.assertEquals("ks", price.associationKind);
        Assertions.assertEquals(targetFields.get("price").ks, price.association);
        // per-bin class counts add up to the classed rows of the field
        final ProfileReport.TargetField priceTarget = targetFields.get("price");
        Assertions.assertEquals(priceTarget.count,
                ProfileReport.total(priceTarget.positiveBins) + ProfileReport.total(priceTarget.negativeBins));
        // the positive rate rises with price
        final double lowRate = (double) priceTarget.positiveBins[0] / (priceTarget.positiveBins[0] + priceTarget.negativeBins[0]);
        final double highRate = (double) priceTarget.positiveBins[9] / (priceTarget.positiveBins[9] + priceTarget.negativeBins[9]);
        Assertions.assertTrue(lowRate < 0.1 && highRate > 0.9, lowRate + " / " + highRate);

        // the pair: two independent samples of one distribution
        final ProfileReport.PairResult pair = result.pairs.getFirst();
        Assertions.assertNull(pair.error);
        Assertions.assertEquals(4900, pair.countA);
        Assertions.assertEquals(5000, pair.countB);
        Assertions.assertTrue(pair.ks < 2 * pair.noiseKs, "pair ks " + pair.ks + " noise " + pair.noiseKs);
        Assertions.assertTrue(pair.psi < 0.01, "pair psi: " + pair.psi);
        Assertions.assertEquals(11, pair.edges.length);

        // records carry the identity and the same numbers
        final List<MElement> fieldRecords = ProfileReport.fieldRecords(result, config);
        Assertions.assertEquals(6, fieldRecords.size());
        final MElement priceRecord = fieldRecords.getFirst();
        Assertions.assertEquals("run", priceRecord.getAsString("runId"));
        Assertions.assertEquals("items", priceRecord.getAsString("dataset"));
        Assertions.assertEquals(2L, priceRecord.getAsLong("formatVersion"));
        Assertions.assertEquals(price.sum, priceRecord.getAsDouble("sum"));
        final List<MElement> binRecords = ProfileReport.binRecords(result, config);
        long priceBins = 0;
        long priceNullBins = 0;
        for(final MElement record : binRecords) {
            if("price".equals(record.getAsString("field")) && record.getAsString("axis") == null) {
                if(ProfileReport.NULL_BIN.equals(record.getAsString("value"))) {
                    priceNullBins += record.getAsLong("count");
                } else {
                    priceBins += record.getAsLong("count");
                    Assertions.assertNotNull(record.getAsDouble("rate"));
                    Assertions.assertTrue(record.getAsDouble("rate_lo") <= record.getAsDouble("rate"));
                    Assertions.assertTrue(record.getAsDouble("rate_hi") >= record.getAsDouble("rate"));
                }
            }
        }
        Assertions.assertEquals(price.count, priceBins);
        Assertions.assertEquals(100, priceNullBins);
    }

    @Test
    public void testDeclaredEdgesKeepTheCellGrid() {
        final Map<String, double[]> declared = Map.of("price", new double[] { 0.5, 1, 2 });
        // one first pass, counted twice: the sketch draws at random, so two first passes over the same rows
        // would not give the same edges (profile-dsl.md §6.2)
        final Passes quantile = passes(4000, 31, null, null);
        final Passes withDeclared = secondPass(quantile.spec, quantile.accumulator, quantile.rows, declared, null);
        final ProfileReport.Result plain = ProfileReport.build(quantile.accumulator, quantile.cells, quantile.edges, config(null, null), null);
        final ProfileReport.Result banded = ProfileReport.build(withDeclared.accumulator, withDeclared.cells, withDeclared.edges, config(declared, null), null);

        final ProfileReport.FieldResult price = banded.fields.get(0);
        Assertions.assertEquals("declared", price.edgesKind);
        Assertions.assertEquals(4, price.bins.length);
        Assertions.assertNull(price.binLower[0]);
        Assertions.assertEquals(0.5, price.binUpper[0]);
        Assertions.assertEquals(2d, price.binLower[3]);
        Assertions.assertNull(price.binUpper[3]);
        // the declared bins are exact counts over right-closed bands
        final long[] expected = new long[4];
        for(final ProfileRow row : withDeclared.rows) {
            final Double value = (Double) row.values[0];
            if(value != null) {
                expected[value <= 0.5 ? 0 : value <= 1 ? 1 : value <= 2 ? 2 : 3]++;
            }
        }
        Assertions.assertArrayEquals(expected, price.bins);

        // KS still reads the 100-cell grid: the same value as without declared edges
        final ProfileReport.TargetField plainTarget = plain.target.fields.stream().filter(f -> f.index == 0).findFirst().orElseThrow();
        final ProfileReport.TargetField bandedTarget = banded.target.fields.stream().filter(f -> f.index == 0).findFirst().orElseThrow();
        Assertions.assertEquals(plainTarget.ks, bandedTarget.ks);
        // the information value follows the field's bins
        Assertions.assertEquals(4, bandedTarget.positiveBins.length);
        Assertions.assertNotEquals(plainTarget.iv, bandedTarget.iv);
    }

    @Test
    public void testValueTableHoldsValuesWhole() {
        final Schema schema = Schema.builder()
                .withField("note", Schema.FieldType.STRING)
                .withField("code", Schema.FieldType.STRING)
                .withField("flag", Schema.FieldType.STRING)
                .build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", false, false);
        final ProfileAccumulator accumulator = ProfileAccumulator.of(spec);
        final String prefix = "x".repeat(256);
        for(int i = 0; i < 200; i++) {
            final MElement element = MElement.builder()
                    // two long values that share their first 256 characters
                    .withString("note", prefix + (i % 2 == 0 ? "a" : "b"))
                    .withString("code", i % 4 == 0 ? "null" : i % 4 == 1 ? "　" : "A" + (i % 2))
                    .withString("flag", i < 150 ? "null" : " ")
                    .build();
            accumulator.add(ProfileRow.of(spec, element).values, null);
        }
        final ProfileReport.Result result = ProfileReport.build(accumulator, null, null, config(null, null), null);

        // a table that shortened its keys would report one value and call it exact
        final ProfileReport.FieldResult note = result.fields.get(0);
        Assertions.assertFalse(note.distinctExact);
        Assertions.assertFalse(note.notable.contains("constant"));
        Assertions.assertEquals(2d, note.distinct, 0.2);

        final ProfileReport.FieldResult code = result.fields.get(1);
        Assertions.assertTrue(code.distinctExact);
        Assertions.assertEquals(4d, code.distinct);
        Assertions.assertEquals(100, code.nullLike);
        Assertions.assertEquals(50, code.blanks);
        Assertions.assertEquals(0.5, code.nullLikeRate, 1e-12);
        Assertions.assertTrue(code.notable.contains("null_like_value"));
        Assertions.assertFalse(code.notable.contains("null_like_only"));

        // every value is a sentinel: the field is empty in all but name
        final ProfileReport.FieldResult flag = result.fields.get(2);
        Assertions.assertTrue(flag.notable.contains("null_like_only"));
        Assertions.assertFalse(flag.notable.contains("null_like_value"));
        Assertions.assertEquals("null", flag.top);
        Assertions.assertEquals(1L, result.notableCounts().get("null_like_only"));
        Assertions.assertEquals(0L, result.notableCounts().get("all_null"));

        // without a counting pass there are no bins and no groups
        Assertions.assertNull(code.bins);
        Assertions.assertTrue(result.axes.isEmpty());
        Assertions.assertTrue(ProfileReport.binRecords(result, config(null, null)).isEmpty());
        Assertions.assertEquals(2 + 4 + 2, ProfileReport.valueRecords(result, config(null, null)).size());
    }

    @Test
    public void testMergedValueTables() {
        final Schema schema = Schema.builder().withField("qty", Schema.FieldType.INT64).build();
        final ProfileSpec spec = ProfileSpec.of(schema, null, null, null, "default", false, false);
        final ProfileAccumulator small = ProfileAccumulator.of(spec);
        final ProfileAccumulator other = ProfileAccumulator.of(spec);
        final ProfileAccumulator wide = ProfileAccumulator.of(spec);
        for(int i = 0; i < 600; i++) {
            small.add(new Object[] { (double) (i % 3) }, null);
            other.add(new Object[] { (double) (i % 5) }, null);
            wide.add(new Object[] { (double) (1000 + i) }, null);
        }
        Assertions.assertEquals(5, small.merge(other).getField(0).getNumericValues().size());
        Assertions.assertEquals(600L + 600L, small.getField(0).getNumericValues().values().stream().mapToLong(Long::longValue).sum());
        Assertions.assertEquals(600d + 1200d, small.getField(0).sum, 1e-9);
        // the merged table passes the limit: dropped, and it stays dropped
        final ProfileAccumulator wider = ProfileAccumulator.of(spec);
        for(int i = 0; i < 600; i++) {
            wider.add(new Object[] { (double) (5000 + i) }, null);
        }
        Assertions.assertNull(wide.merge(wider).getField(0).getNumericValues());
        Assertions.assertNull(small.merge(wide).getField(0).getNumericValues());
        Assertions.assertNull(small.copy().getField(0).getNumericValues());
    }
}
