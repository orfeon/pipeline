package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TimeseriesTest {

    private static final long HOUR = 3_600_000_000L;

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("points", Schema.FieldType.array(Schema.FieldType.element(List.of(
                    Schema.Field.of("ts", Schema.FieldType.TIMESTAMP),
                    Schema.Field.of("day", Schema.FieldType.DATE),
                    Schema.Field.of("v", Schema.FieldType.FLOAT32),
                    Schema.Field.of("label", Schema.FieldType.STRING),
                    Schema.Field.of("meta", Schema.FieldType.element(List.of(
                            Schema.Field.of("at", Schema.FieldType.TIMESTAMP),
                            Schema.Field.of("value", Schema.FieldType.FLOAT64)))))))),
            Schema.Field.of("text", Schema.FieldType.STRING),
            Schema.Field.of("start", Schema.FieldType.TIMESTAMP),
            Schema.Field.of("startDay", Schema.FieldType.DATE),
            Schema.Field.of("q", Schema.FieldType.matrix(Schema.FieldType.FLOAT32, List.of(3, -1))),
            Schema.Field.of("qd", Schema.FieldType.matrix(Schema.FieldType.FLOAT64, List.of(-1, -1))),
            Schema.Field.of("v", Schema.FieldType.array(Schema.FieldType.FLOAT32)));

    private static SelectFunction function(final String json) {
        final SelectFunction function = SelectFunction.of(new Gson().fromJson(json, JsonObject.class), FIELDS);
        function.setup();
        return function;
    }

    private static Map<String, Object> point(final long hour, final Object value) {
        final Map<String, Object> point = new HashMap<>();
        point.put("ts", hour * HOUR);
        point.put("v", value);
        return point;
    }

    private static Map<?, ?> grid(final String options, final List<?> points) {
        final String json = "{ \"name\": \"g\", \"func\": \"timeseries_grid\", \"field\": \"points\", \"timestampField\": \"ts\", \"valueField\": \"v\", \"interval\": \"1h\", \"length\": 5" + options + " }";
        final Map<String, Object> input = new HashMap<>();
        input.put("points", points);
        return (Map<?, ?>) function(json).apply(input, Instant.EPOCH);
    }

    // the points come in any order, and a time without a value stays in the grid as NaN
    @Test
    public void testGridOrderAndGaps() {
        final Map<?, ?> grid = grid("", List.of(point(13, 4f), point(10, 1f), point(11, 2f)));
        Assertions.assertEquals(Arrays.asList(1d, 2d, Double.NaN, 4d), grid.get("values"));
        Assertions.assertEquals(List.of(10 * HOUR, 11 * HOUR, 12 * HOUR, 13 * HOUR), grid.get("timestamps"));
        Assertions.assertEquals(13 * HOUR, grid.get("lastTimestamp"));
        Assertions.assertEquals(List.of(true, true, false, true), grid.get("observed"));
    }

    // the latest length values, however long the gaps before them
    @Test
    public void testGridKeepsTheLatest() {
        final Map<?, ?> grid = grid("", List.of(point(0, 9f), point(100, 1f), point(102, 3f), point(103, 4f), point(104, 5f), point(105, 6f)));
        Assertions.assertEquals(Arrays.asList(Double.NaN, 3d, 4d, 5d, 6d), grid.get("values"));
        Assertions.assertEquals(101 * HOUR, ((List<?>) grid.get("timestamps")).getFirst());
    }

    // a time is floored to the interval from the epoch, also before it
    @Test
    public void testGridFloorsTimes() {
        final List<Map<String, Object>> points = new ArrayList<>();
        final Map<String, Object> inside = new HashMap<>();
        inside.put("ts", -HOUR / 2);
        inside.put("v", 1f);
        points.add(inside);
        final Map<String, Object> next = new HashMap<>();
        next.put("ts", HOUR / 2);
        next.put("v", 2f);
        points.add(next);
        final Map<?, ?> grid = grid("", points);
        Assertions.assertEquals(List.of(-HOUR, 0L), grid.get("timestamps"));
        Assertions.assertEquals(List.of(1d, 2d), grid.get("values"));
    }

    @Test
    public void testGridAggregation() {
        final Map<String, Object> early = point(1, 1f);
        final Map<String, Object> late = new HashMap<>();
        late.put("ts", HOUR + 60_000_000L);
        late.put("v", 3f);
        final List<Map<String, Object>> points = List.of(late, early);
        // last is by time, not by the place in the array
        Assertions.assertEquals(List.of(3d), grid("", points).get("values"));
        Assertions.assertEquals(List.of(3d), grid(", \"aggregation\": \"last\"", points).get("values"));
        Assertions.assertEquals(List.of(2d), grid(", \"aggregation\": \"mean\"", points).get("values"));
        Assertions.assertEquals(List.of(4d), grid(", \"aggregation\": \"sum\"", points).get("values"));
    }

    // a value that is null or NaN is not observed, and does not hide another of the same time
    @Test
    public void testGridMissingValues() {
        final Map<?, ?> grid = grid(", \"aggregation\": \"sum\"", List.of(point(1, 2f), point(1, null), point(2, Float.NaN), point(2, null)));
        Assertions.assertEquals(Arrays.asList(2d, Double.NaN), grid.get("values"));
        Assertions.assertEquals(List.of(true, false), grid.get("observed"));
        Assertions.assertEquals(2 * HOUR, grid.get("lastTimestamp"));
    }

    // a nested path is read as its type was looked up: not a grid of NaN or nothing
    @Test
    public void testGridOfNestedFields() {
        final String json = "{ \"name\": \"g\", \"func\": \"timeseries_grid\", \"field\": \"points\", \"timestampField\": \"meta.at\", \"valueField\": \"meta.value\", \"interval\": \"1h\", \"length\": 5 }";
        final Map<String, Object> input = new HashMap<>();
        input.put("points", List.of(
                Map.of("meta", Map.of("at", 3 * HOUR, "value", 1d)),
                Map.of("meta", Map.of("at", 5 * HOUR, "value", 2d))));
        final Map<?, ?> grid = (Map<?, ?>) function(json).apply(input, Instant.EPOCH);
        Assertions.assertEquals(Arrays.asList(1d, Double.NaN, 2d), grid.get("values"));
        Assertions.assertEquals(5 * HOUR, grid.get("lastTimestamp"));
    }

    @Test
    public void testGridOfNothingIsNull() {
        Assertions.assertNull(grid("", null));
        Assertions.assertNull(grid("", List.of()));
    }

    @Test
    public void testGridOfDates() {
        final String json = "{ \"name\": \"g\", \"func\": \"timeseries_grid\", \"field\": \"points\", \"timestampField\": \"day\", \"valueField\": \"v\", \"interval\": \"1d\", \"length\": 5 }";
        final Map<String, Object> input = new HashMap<>();
        input.put("points", List.of(Map.of("day", 2, "v", 1f), Map.of("day", 4, "v", 2f)));
        final Map<?, ?> grid = (Map<?, ?>) function(json).apply(input, Instant.EPOCH);
        Assertions.assertEquals(Arrays.asList(1d, Double.NaN, 2d), grid.get("values"));
        Assertions.assertEquals(4 * 24 * HOUR, grid.get("lastTimestamp"));
    }

    @Test
    public void testGridConfigErrors() {
        final String base = "\"name\": \"g\", \"func\": \"timeseries_grid\", ";
        final String ok = "\"timestampField\": \"ts\", \"valueField\": \"v\", \"interval\": \"1h\", \"length\": 5";
        // not an array of elements, a required parameter, a field of a wrong type, a bad interval, a bad length
        for(final String json : List.of(
                "\"field\": \"v\", " + ok,
                "\"field\": \"points\", \"valueField\": \"v\", \"interval\": \"1h\", \"length\": 5",
                "\"field\": \"points\", \"timestampField\": \"label\", \"valueField\": \"v\", \"interval\": \"1h\", \"length\": 5",
                "\"field\": \"points\", \"timestampField\": \"ts\", \"valueField\": \"label\", \"interval\": \"1h\", \"length\": 5",
                "\"field\": \"points\", \"timestampField\": \"nothing\", \"valueField\": \"v\", \"interval\": \"1h\", \"length\": 5",
                "\"field\": \"points\", \"timestampField\": \"ts\", \"valueField\": \"v\", \"interval\": \"0h\", \"length\": 5",
                "\"field\": \"points\", \"timestampField\": \"ts\", \"valueField\": \"v\", \"interval\": \"1mo\", \"length\": 5",
                "\"field\": \"points\", \"timestampField\": \"ts\", \"valueField\": \"v\", \"interval\": \"1h\", \"length\": 0",
                "\"field\": \"points\", \"aggregation\": \"median\", " + ok)) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ " + base + json + " }"), json);
        }
    }

    private static List<?> quantiles(final String options, final Object value) {
        final String json = "{ \"name\": \"f\", \"func\": \"timeseries_quantiles\", \"field\": \"q\", \"levels\": [0.1, 0.5, 0.9], \"start\": \"start\", \"interval\": \"1h\"" + options + " }";
        final Map<String, Object> input = new HashMap<>();
        input.put("q", value);
        input.put("start", 10 * HOUR);
        return (List<?>) function(json).apply(input, Instant.EPOCH);
    }

    private static final float[] VALUES = {1f, 2f, 10f, 20f, 100f, 200f};

    // the first step is one interval after start; a Tensor and a flat list give the same
    @Test
    public void testQuantiles() {
        final List<?> forecast = quantiles("", Tensor.of(VALUES, new long[]{3, 2}));
        Assertions.assertEquals(2, forecast.size());
        Assertions.assertEquals(Map.of("timestamp", 11 * HOUR, "q10", 1f, "q50", 10f, "q90", 100f), forecast.get(0));
        Assertions.assertEquals(Map.of("timestamp", 12 * HOUR, "q10", 2f, "q50", 20f, "q90", 200f), forecast.get(1));
        Assertions.assertEquals(forecast, quantiles("", List.of(1f, 2f, 10f, 20f, 100f, 200f)));
    }

    @Test
    public void testQuantilesSelect() {
        final SelectFunction function = function("{ \"name\": \"f\", \"func\": \"timeseries_quantiles\", \"field\": \"q\", \"levels\": [0.1, 0.5, 0.9], \"start\": \"start\", \"interval\": \"1h\", \"select\": [0.90, 0.1] }");
        Assertions.assertEquals(List.of("timestamp", "q90", "q10"), function.getOutputFieldType().getArrayValueType().getElementSchema().getFields().stream().map(Schema.Field::getName).toList());
        final List<?> forecast = quantiles(", \"select\": [0.90, 0.1]", Tensor.of(VALUES, new long[]{3, 2}));
        Assertions.assertEquals(Map.of("timestamp", 11 * HOUR, "q10", 1f, "q90", 100f), forecast.getFirst());
    }

    @Test
    public void testQuantilesAxis() {
        // [steps, quantiles]: step 0 is 1, 2, 3
        final float[] values = {1f, 2f, 3f, 4f, 5f, 6f};
        final String json = "{ \"name\": \"f\", \"func\": \"timeseries_quantiles\", \"field\": \"qd\", \"levels\": [0.1, 0.5, 0.9], \"start\": \"start\", \"interval\": \"30m\", \"axis\": \"step_quantile\" }";
        final Map<String, Object> input = new HashMap<>();
        input.put("qd", Tensor.of(values, new long[]{2, 3}));
        input.put("start", 0L);
        final List<?> forecast = (List<?>) function(json).apply(input, Instant.EPOCH);
        Assertions.assertEquals(2, forecast.size());
        // float64 by the type of the field
        Assertions.assertEquals(Map.of("timestamp", HOUR / 2, "q10", 1d, "q50", 2d, "q90", 3d), forecast.get(0));
        Assertions.assertEquals(Map.of("timestamp", HOUR, "q10", 4d, "q50", 5d, "q90", 6d), forecast.get(1));
    }

    @Test
    public void testQuantileNames() {
        final SelectFunction function = function("{ \"name\": \"f\", \"func\": \"timeseries_quantiles\", \"field\": \"qd\", \"levels\": [0.05, 0.1, 0.50, 0.975], \"start\": \"start\", \"interval\": \"1h\" }");
        Assertions.assertEquals(List.of("timestamp", "q05", "q10", "q50", "q975"), function.getOutputFieldType().getArrayValueType().getElementSchema().getFields().stream().map(Schema.Field::getName).toList());
    }

    @Test
    public void testQuantilesStartOfDate() {
        final String json = "{ \"name\": \"f\", \"func\": \"timeseries_quantiles\", \"field\": \"qd\", \"levels\": [0.5], \"start\": \"startDay\", \"interval\": \"1d\" }";
        final Map<String, Object> input = new HashMap<>();
        input.put("qd", List.of(1d));
        input.put("startDay", 2);
        Assertions.assertEquals(List.of(Map.of("timestamp", 3 * 24 * HOUR, "q50", 1d)), function(json).apply(input, Instant.EPOCH));
    }

    @Test
    public void testQuantilesNulls() {
        Assertions.assertNull(quantiles("", null));
        final String json = "{ \"name\": \"f\", \"func\": \"timeseries_quantiles\", \"field\": \"q\", \"levels\": [0.1, 0.5, 0.9], \"start\": \"start\", \"interval\": \"1h\" }";
        final Map<String, Object> input = new HashMap<>();
        input.put("q", List.of(1f, 2f, 3f));
        Assertions.assertNull(function(json).apply(input, Instant.EPOCH));
    }

    // the levels must fit the value: a Tensor of another shape, a flat list that can not be divided
    @Test
    public void testQuantilesOfAnotherShape() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> quantiles("", Tensor.of(VALUES, new long[]{2, 3})));
        Assertions.assertThrows(IllegalArgumentException.class, () -> quantiles("", List.of(1f, 2f)));
    }

    @Test
    public void testQuantilesConfigErrors() {
        final String ok = "\"start\": \"start\", \"interval\": \"1h\"";
        for(final String json : List.of(
                // not a matrix, no levels, a level out of range, twice the same, a shape of other levels, select out of levels
                "\"field\": \"v\", \"levels\": [0.5], " + ok,
                "\"field\": \"q\", \"levels\": [], " + ok,
                "\"field\": \"q\", \"levels\": [0.1, 0.5, 1.0], " + ok,
                "\"field\": \"q\", \"levels\": [0.1, 0.5, 0.50], " + ok,
                "\"field\": \"q\", \"levels\": [0.1, 0.5], " + ok,
                "\"field\": \"q\", \"levels\": [0.1, 0.5, 0.9], \"select\": [0.2], " + ok,
                // start of a wrong type, without start, a bad axis
                "\"field\": \"q\", \"levels\": [0.1, 0.5, 0.9], \"start\": \"text\", \"interval\": \"1h\"",
                "\"field\": \"q\", \"levels\": [0.1, 0.5, 0.9], \"interval\": \"1h\"",
                "\"field\": \"q\", \"levels\": [0.1, 0.5, 0.9], \"axis\": \"other\", " + ok)) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"f\", \"func\": \"timeseries_quantiles\", " + json + " }"), json);
        }
    }

}
