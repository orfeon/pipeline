package com.mercari.solution.util.domain.ml.tabular;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/** {@code tabular_encode} and {@code tabular_decode} of the kumo preset against the values of structured-data-models. */
public class KumoTabularTest {

    // a key that keeps the order of the rows of the fixture, on which the numbers of the classes depend
    private static final String KEY = "__row";

    // KUMO_FIXTURES_DIR: all the tables of the source of the fixtures, which are too large to keep here
    private static List<Path> fixtures() throws IOException {
        final String all = System.getenv("KUMO_FIXTURES_DIR");
        try(final Stream<Path> dirs = Files.list(all == null ? KumoFixture.RESOURCES : Path.of(all))) {
            return dirs.filter(Files::isDirectory).sorted().toList();
        }
    }

    private static List<Schema.Field> rowFields(final KumoFixture fixture) {
        final List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of(KEY, Schema.FieldType.INT64));
        for(final String column : fixture.header()) {
            fields.add(Schema.Field.of(column, fixture.isNumberColumn(column) ? Schema.FieldType.FLOAT64 : Schema.FieldType.STRING));
        }
        return fields;
    }

    private static List<Map<String, Object>> rows(final KumoFixture fixture) {
        final List<Map<String, Object>> rows = new ArrayList<>();
        for(int r=0; r<fixture.rows().size(); r++) {
            final Map<String, Object> row = new HashMap<>();
            row.put(KEY, (long) r);
            for(final String column : fixture.header()) {
                row.put(column, column.equals(fixture.target()) && r >= fixture.contextRows() ? null : fixture.value(r, column));
            }
            rows.add(row);
        }
        return rows;
    }

    private static SelectFunction encoder(final KumoFixture fixture) {
        final JsonObject json = new JsonObject();
        json.addProperty("name", "tab");
        json.addProperty("func", "tabular_encode");
        json.addProperty("preset", "kumo");
        json.addProperty("task", fixture.meta().get("task").getAsString());
        json.addProperty("field", "rows");
        json.addProperty("key", KEY);
        json.addProperty("target", fixture.target());
        json.addProperty("maxContextRows", 100000);
        json.add("numerical", new Gson().toJsonTree(fixture.columns("numerical")).getAsJsonArray());
        json.add("categorical", new Gson().toJsonTree(fixture.columns("categorical")).getAsJsonArray());
        return SelectFunction.of(json, List.of(Schema.Field.of("rows", Schema.FieldType.array(Schema.FieldType.element(rowFields(fixture))))));
    }

    @Test
    public void testEncodeAsStructuredDataModels() throws IOException {
        for(final Path dir : fixtures()) {
            final KumoFixture fixture = KumoFixture.load(dir);
            final Map<String, Object> input = new HashMap<>();
            input.put("rows", rows(fixture));
            final SelectFunction encoder = encoder(fixture);
            encoder.setup();
            final Map<?, ?> encoded = (Map<?, ?>) encoder.apply(input, Instant.EPOCH);

            final List<?> x = (List<?>) encoded.get("x");
            final int columns = ((List<?>) x.getFirst()).size();
            final JsonArray shape = fixture.meta().getAsJsonArray("x");
            Assertions.assertEquals(shape.get(0).getAsInt(), x.size(), fixture.name());
            Assertions.assertEquals(shape.get(1).getAsInt(), columns, fixture.name());
            final float[] expected = fixture.floats("x.bin");
            for(int i=0; i<expected.length; i++) {
                final float value = (Float) ((List<?>) x.get(i / columns)).get(i % columns);
                Assertions.assertEquals(expected[i], value, 1e-5f, fixture.name() + " x[" + (i / columns) + "][" + (i % columns) + "]");
            }

            final byte[] mask = fixture.bytes("mask.bin");
            final List<?> categoricalMask = (List<?>) encoded.get("categoricalMask");
            Assertions.assertEquals(mask.length, categoricalMask.size(), fixture.name());
            for(int i=0; i<mask.length; i++) {
                Assertions.assertEquals(mask[i] != 0, categoricalMask.get(i), fixture.name() + " mask " + i);
            }

            final List<?> yContext = (List<?>) encoded.get("yContext");
            if(fixture.classification()) {
                Assertions.assertEquals(fixture.classes(), encoded.get("classes"), fixture.name());
                final int[] y = fixture.ints("y.bin");
                Assertions.assertEquals(y.length, yContext.size(), fixture.name());
                for(int i=0; i<y.length; i++) {
                    Assertions.assertEquals(y[i], yContext.get(i), fixture.name() + " y " + i);
                }
            } else {
                final float[] y = fixture.floats("y.bin");
                Assertions.assertEquals(y.length, yContext.size(), fixture.name());
                for(int i=0; i<y.length; i++) {
                    Assertions.assertEquals(y[i], (Float) yContext.get(i), 1e-5f, fixture.name() + " y " + i);
                }
            }
            Assertions.assertEquals(fixture.rows().size() - fixture.contextRows(), ((List<?>) encoded.get("queryKeys")).size(), fixture.name());
        }
    }

    @Test
    public void testDecodeAsStructuredDataModels() throws IOException {
        for(final Path dir : fixtures()) {
            final KumoFixture fixture = KumoFixture.load(dir);
            final float[] logits = fixture.floats("logits.bin");
            final float[] predictions = fixture.floats("pred.bin");
            final int width = fixture.meta().getAsJsonArray("logits").get(1).getAsInt();
            final int rows = logits.length / width;
            final Tensor tensor = Tensor.of(logits, new long[]{rows, width});
            if(fixture.classification()) {
                final int classes = fixture.classes().size();
                final double[][] probabilities = KumoTabular.probabilities(tensor, rows, classes);
                for(int r=0; r<rows; r++) {
                    for(int k=0; k<classes; k++) {
                        Assertions.assertEquals(predictions[r * classes + k], probabilities[r][k], 1e-5, fixture.name());
                    }
                }
            } else {
                final float[] y = fixture.floats("y.bin");
                final Map<String, Object> input = new HashMap<>();
                input.put("rows", rows(fixture));
                final Map<?, ?> encoded = (Map<?, ?>) encoder(fixture).apply(input, Instant.EPOCH);
                Assertions.assertEquals(y.length, ((List<?>) encoded.get("yContext")).size());
                final KumoTabular.Scale scale = new KumoTabular.Scale((Double) encoded.get("yMean"), (Double) encoded.get("yScale"));
                final double[][] quantiles = KumoTabular.quantiles(tensor, rows, scale);
                for(int r=0; r<rows; r++) {
                    final double mean = Arrays.stream(quantiles[r]).average().orElseThrow();
                    Assertions.assertEquals(predictions[r], mean, 1e-4 * Math.max(1, Math.abs(predictions[r])), fixture.name());
                }
            }
        }
    }

    @Test
    public void testScale() {
        Assertions.assertEquals(new KumoTabular.Scale(2d, 1d), KumoTabular.scale(new double[]{1, 3}));
        // a constant target has a unit scale, also when its variance is only round-off
        Assertions.assertEquals(new KumoTabular.Scale(5d, 1d), KumoTabular.scale(new double[]{5, 5, 5}));
        final KumoTabular.Scale roundOff = KumoTabular.scale(new double[]{0.1, 0.1, 0.1});
        Assertions.assertEquals(1d, roundOff.scale());
    }

    @Test
    public void testClasses() {
        final KumoTabular.Classes classes = KumoTabular.classes(List.of("b", "a", "b", "c"));
        Assertions.assertEquals(List.of("b", "a", "c"), classes.labels());
        Assertions.assertArrayEquals(new int[]{0, 1, 0, 2}, classes.codes());
        final List<String> tooMany = new ArrayList<>();
        for(int i=0; i<=KumoTabular.MAX_CLASSES; i++) {
            tooMany.add("c" + i);
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> KumoTabular.classes(tooMany));
    }

    // a category: sorted by value, unknown and missing are -1, a count column only over 50 categories, and it comes first
    @Test
    public void testCategories() {
        final List<Comparable<?>[]> few = new ArrayList<>();
        few.add(new Comparable<?>[]{10L, 9L, 10L, 9L, null, 7L});
        final KumoTabular.Features codes = KumoTabular.features(List.of(), few, 6, 4);
        Assertions.assertEquals(1, codes.categoricalMask().length);
        Assertions.assertTrue(codes.categoricalMask()[0]);
        // 9 comes before 10: the codes are 1, 0, 1, 0, and then the same value for the missing and the unknown value
        Assertions.assertTrue(codes.x()[0][0] > codes.x()[1][0]);
        Assertions.assertEquals(codes.x()[0][0], codes.x()[2][0]);
        Assertions.assertEquals(codes.x()[4][0], codes.x()[5][0]);
        Assertions.assertTrue(codes.x()[4][0] < codes.x()[1][0]);

        final int rows = 51;
        final Comparable<?>[] many = new Comparable<?>[rows + 1];
        for(int i=0; i<rows; i++) {
            many[i] = "v" + i;
        }
        // a frequent value, so that the count column is not constant
        many[1] = "v0";
        final List<Comparable<?>[]> columns = new ArrayList<>();
        columns.add(many);
        final KumoTabular.Features fifty = KumoTabular.features(List.of(), columns, rows + 1, rows);
        Assertions.assertEquals(1, fifty.categoricalMask().length, "50 categories have no count column");

        final Comparable<?>[] frequent = new Comparable<?>[rows + 2];
        for(int i=0; i<rows; i++) {
            frequent[i] = "v" + i;
        }
        frequent[rows] = "v0";
        final List<Comparable<?>[]> counted = new ArrayList<>();
        counted.add(frequent);
        final KumoTabular.Features features = KumoTabular.features(List.of(), counted, rows + 2, rows + 1);
        Assertions.assertArrayEquals(new boolean[]{false, true}, features.categoricalMask());
    }

    // a constant column and an all-NaN column go, and a column with a NaN stays with its NaN
    @Test
    public void testColumns() {
        final double nan = Double.NaN;
        final List<double[]> numerical = List.of(
                new double[]{1, 1, 1, 9},
                new double[]{nan, nan, nan, 9},
                new double[]{1, nan, 3, 2},
                new double[]{1, 2, 3, Double.POSITIVE_INFINITY});
        final KumoTabular.Features features = KumoTabular.features(numerical, List.of(), 4, 3);
        Assertions.assertEquals(2, features.categoricalMask().length);
        Assertions.assertTrue(Float.isNaN(features.x()[1][0]));
        Assertions.assertFalse(Float.isNaN(features.x()[0][0]));
        // an infinity is clipped to a finite value
        Assertions.assertTrue(Float.isFinite(features.x()[3][1]));
        // one context row: nothing is known to be constant
        Assertions.assertEquals(1, KumoTabular.features(List.of(new double[]{1, 2}), List.of(), 2, 1).categoricalMask().length);
    }

    @Test
    public void testColumnLimit() {
        final List<double[]> numerical = new ArrayList<>();
        for(int c=0; c<501; c++) {
            numerical.add(new double[]{c, c + 1});
        }
        Assertions.assertEquals(500, KumoTabular.features(numerical, List.of(), 2, 2).categoricalMask().length);
    }

    @Test
    public void testLogitsShape() {
        final Tensor logits = Tensor.of(new float[]{1, 2, 3, 4}, new long[]{2, 2});
        Assertions.assertThrows(IllegalArgumentException.class, () -> KumoTabular.probabilities(logits, 3, 2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> KumoTabular.probabilities(logits, 2, 3));
    }

}
