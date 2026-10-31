package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class VectorsTest {

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("v", Schema.FieldType.array(Schema.FieldType.FLOAT32)),
            Schema.Field.of("d", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
            Schema.Field.of("s", Schema.FieldType.FLOAT32),
            Schema.Field.of("i", Schema.FieldType.INT64),
            Schema.Field.of("text", Schema.FieldType.STRING),
            Schema.Field.of("m1", Schema.FieldType.matrix(Schema.FieldType.FLOAT32, List.of(1, 3))),
            Schema.Field.of("m2", Schema.FieldType.matrix(Schema.FieldType.FLOAT32, List.of(2, 3))),
            Schema.Field.of("md", Schema.FieldType.matrix(Schema.FieldType.FLOAT32, List.of(-1, 3))),
            Schema.Field.of("groups", Schema.FieldType.array(Schema.FieldType.array(Schema.FieldType.INT64))),
            Schema.Field.of("floatGroups", Schema.FieldType.array(Schema.FieldType.array(Schema.FieldType.FLOAT32))),
            Schema.Field.of("longs", Schema.FieldType.array(Schema.FieldType.INT64)));

    private static SelectFunction function(final String json) {
        final SelectFunction function = SelectFunction.of(new Gson().fromJson(json, JsonObject.class), FIELDS);
        function.setup();
        return function;
    }

    private static Object apply(final String json, final Object value) {
        final Map<String, Object> input = new HashMap<>();
        input.put(new Gson().fromJson(json, JsonObject.class).get("field").getAsString(), value);
        return function(json).apply(input, Instant.EPOCH);
    }

    private static List<Double> doubles(final Object value) {
        return ((List<?>) value).stream().map(v -> ((Number) v).doubleValue()).toList();
    }

    private static void assertClose(final List<Double> expected, final Object actual) {
        final List<Double> values = doubles(actual);
        Assertions.assertEquals(expected.size(), values.size());
        for(int i=0; i<values.size(); i++) {
            Assertions.assertEquals(expected.get(i), values.get(i), 1e-6);
        }
    }

    @Test
    public void testVectorNormalize() {
        final String json = "{ \"name\": \"o\", \"func\": \"vector_normalize\", \"field\": \"v\" }";
        assertClose(List.of(0.6, 0.8), apply(json, List.of(3f, 4f)));
        // a Tensor gives the same, and a zero vector stays
        assertClose(List.of(0.6, 0.8), apply(json, Tensor.of(new float[]{3f, 4f}, new long[]{2})));
        assertClose(List.of(0d, 0d), apply(json, List.of(0f, 0f)));
        Assertions.assertNull(apply(json, null));
        // truncate, then normalize
        final String sized = "{ \"name\": \"o\", \"func\": \"vector_normalize\", \"field\": \"v\", \"size\": 2 }";
        assertClose(List.of(0.6, 0.8), apply(sized, List.of(3f, 4f, 100f)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> apply(sized, List.of(3f)));
        // the type of the values follows the input
        Assertions.assertEquals(Schema.Type.float32, function(json).getOutputFieldType().getArrayValueType().getType());
        Assertions.assertEquals(Schema.Type.float64, function("{ \"name\": \"o\", \"func\": \"vector_normalize\", \"field\": \"d\" }").getOutputFieldType().getArrayValueType().getType());
        Assertions.assertInstanceOf(Double.class, ((List<?>) apply("{ \"name\": \"o\", \"func\": \"vector_normalize\", \"field\": \"d\" }", List.of(1d))).getFirst());
    }

    @Test
    public void testSoftmax() {
        final String json = "{ \"name\": \"o\", \"func\": \"softmax\", \"field\": \"v\" }";
        assertClose(List.of(0.26894142, 0.73105858), apply(json, List.of(0f, 1f)));
        // large logits do not overflow
        assertClose(List.of(0.26894142, 0.73105858), apply(json, List.of(1000f, 1001f)));
        // only the head
        assertClose(List.of(0.26894142, 0.73105858), apply("{ \"name\": \"o\", \"func\": \"softmax\", \"field\": \"v\", \"size\": 2 }", List.of(0f, 1f, 50f)));
    }

    @Test
    public void testSigmoid() {
        final String json = "{ \"name\": \"o\", \"func\": \"sigmoid\", \"field\": \"v\" }";
        assertClose(List.of(0.5, 0.73105858), apply(json, List.of(0f, 1f)));
        final String scalar = "{ \"name\": \"o\", \"func\": \"sigmoid\", \"field\": \"s\" }";
        Assertions.assertEquals(0.5f, apply(scalar, 0f));
        Assertions.assertEquals(Schema.Type.float32, function(scalar).getOutputFieldType().getType());
    }

    @Test
    public void testArgmax() {
        final String json = "{ \"name\": \"o\", \"func\": \"argmax\", \"field\": \"v\" }";
        Assertions.assertEquals(1L, apply(json, List.of(1f, 3f, 2f)));
        // the first of the largest, NaN never
        Assertions.assertEquals(0L, apply(json, List.of(3f, 3f)));
        Assertions.assertEquals(1L, apply(json, List.of(Float.NaN, 1f)));
        Assertions.assertNull(apply(json, List.of(Float.NaN)));
        Assertions.assertNull(apply(json, List.of()));
    }

    @Test
    public void testTopk() {
        final String json = "{ \"name\": \"o\", \"func\": \"topk\", \"field\": \"v\", \"k\": 2 }";
        Assertions.assertEquals(List.of(Map.of("index", 1L, "value", 3f), Map.of("index", 2L, "value", 2f)), apply(json, List.of(1f, 3f, 2f)));
        // k over the length, and NaN last
        Assertions.assertEquals(List.of(Map.of("index", 1L, "value", 1f)), apply("{ \"name\": \"o\", \"func\": \"topk\", \"field\": \"v\", \"k\": 1 }", List.of(Float.NaN, 1f)));
        Assertions.assertEquals(1, ((List<?>) apply(json, List.of(1f))).size());
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"topk\", \"field\": \"v\" }"));
    }

    @Test
    public void testLabels() {
        final String json = "{ \"name\": \"o\", \"func\": \"labels\", \"field\": \"i\", \"labels\": [\"a\", \"b\"] }";
        Assertions.assertEquals("b", apply(json, 1L));
        Assertions.assertThrows(IllegalArgumentException.class, () -> apply(json, 2L));
        final String vector = "{ \"name\": \"o\", \"func\": \"labels\", \"field\": \"v\", \"labels\": [\"a\", \"b\"] }";
        Assertions.assertEquals(List.of(Map.of("label", "a", "value", 0.25f), Map.of("label", "b", "value", 0.75f)), apply(vector, List.of(0.25f, 0.75f)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> apply(vector, List.of(1f)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"labels\", \"field\": \"v\" }"));
    }

    @Test
    public void testSqueeze() {
        // a fixed dimension of 1 goes
        final SelectFunction fixed = function("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"m1\" }");
        Assertions.assertEquals(Schema.Type.array, fixed.getOutputFieldType().getType());
        // the list is flat: only the type changes
        final List<Float> flat = List.of(1f, 2f, 3f);
        Assertions.assertEquals(flat, apply("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"m1\" }", flat));

        // a Tensor stays a Tensor on the same array
        final Tensor tensor = Tensor.of(new float[]{1f, 2f, 3f}, new long[]{1, 3});
        final Object squeezed = apply("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"m1\" }", tensor);
        Assertions.assertArrayEquals(new long[]{3}, ((Tensor) squeezed).shape());
        Assertions.assertSame(tensor.data(), ((Tensor) squeezed).data());

        // a dynamic dimension stays in the type, unless axis says it goes
        Assertions.assertEquals(Schema.Type.matrix, function("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"md\" }").getOutputFieldType().getType());
        final String axis = "{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"md\", \"axis\": 0 }";
        Assertions.assertEquals(Schema.Type.array, function(axis).getOutputFieldType().getType());
        Assertions.assertArrayEquals(new long[]{3}, ((Tensor) apply(axis, tensor)).shape());
        // the dimension is not 1 at run time
        Assertions.assertThrows(IllegalArgumentException.class, () -> apply(axis, Tensor.of(new float[6], new long[]{2, 3})));
        // or in the schema
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"m2\", \"axis\": 0 }"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"text\" }"));
        // an axis the value does not have
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"m1\", \"axis\": 2 }"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"m1\", \"axis\": -3 }"));
        Assertions.assertArrayEquals(new long[]{3}, ((Tensor) apply("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"m1\", \"axis\": -2 }", tensor)).shape());
    }

    // without axis, a dynamic dimension that happens to be 1 stays, as it does in the type
    @Test
    public void testSqueezeFollowsTheType() {
        final Tensor one = Tensor.of(new float[]{1f}, new long[]{1});
        final String array = "{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"v\" }";
        Assertions.assertEquals(Schema.Type.array, function(array).getOutputFieldType().getType());
        Assertions.assertArrayEquals(new long[]{1}, ((Tensor) apply(array, one)).shape());
        final Tensor row = Tensor.of(new float[]{1f, 2f, 3f}, new long[]{1, 3});
        Assertions.assertArrayEquals(new long[]{1, 3}, ((Tensor) apply("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"md\" }", row)).shape());
    }

    // no dimension left: a record value, for the functions that tell a scalar from a vector
    @Test
    public void testSqueezeToScalar() {
        final String json = "{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"v\", \"axis\": 0 }";
        Assertions.assertEquals(Schema.Type.float32, function(json).getOutputFieldType().getType());
        Assertions.assertEquals(2f, apply(json, Tensor.of(new float[]{2f}, new long[]{1})));
        Assertions.assertEquals(2f, apply(json, List.of(2f)));
    }

    // a NULL field yields NULL: the types of a value without an array around it allow it
    @Test
    public void testScalarOutputsAreNullable() {
        Assertions.assertNotEquals(Boolean.FALSE, function("{ \"name\": \"o\", \"func\": \"sigmoid\", \"field\": \"s\" }").getOutputFieldType().getNullable());
        final List<Schema.Field> fields = List.of(Schema.Field.of("r", Schema.FieldType.array(Schema.FieldType.FLOAT32.withNullable(false))));
        final SelectFunction squeeze = SelectFunction.of(new Gson().fromJson("{ \"name\": \"o\", \"func\": \"squeeze\", \"field\": \"r\", \"axis\": 0 }", JsonObject.class), fields);
        Assertions.assertNotEquals(Boolean.FALSE, squeeze.getOutputFieldType().getNullable());
    }

    // a later item of a name replaces the earlier one for the type the items after it see, as it does for the value
    @Test
    public void testLaterItemReplacesTheType() {
        final List<SelectFunction> functions = SelectFunction.of(new Gson().fromJson("""
                [
                  { "name": "v", "func": "squeeze", "field": "m1" },
                  { "name": "x", "field": "s" },
                  { "name": "s", "func": "argmax", "field": "v" },
                  { "name": "y", "field": "s" }
                ]""", JsonArray.class), FIELDS);
        Assertions.assertEquals(Schema.Type.float32, functions.get(1).getOutputFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, functions.get(3).getOutputFieldType().getType());
    }

    // a matrix is a vector only with one dimension other than 1
    @Test
    public void testVectorRequired() {
        Assertions.assertEquals(2L, apply("{ \"name\": \"o\", \"func\": \"argmax\", \"field\": \"m1\" }", List.of(1f, 2f, 3f)));
        Assertions.assertEquals(2L, apply("{ \"name\": \"o\", \"func\": \"argmax\", \"field\": \"m1\" }", Tensor.of(new float[]{1f, 2f, 3f}, new long[]{1, 3})));
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"argmax\", \"field\": \"m2\" }"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> function("{ \"name\": \"o\", \"func\": \"softmax\", \"field\": \"s\" }"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> apply("{ \"name\": \"o\", \"func\": \"argmax\", \"field\": \"v\" }", Tensor.of(new float[6], new long[]{2, 3})));
    }


    private static Object groupSoftmax(final Object logits, final Object groups) {
        final Map<String, Object> input = new HashMap<>();
        input.put("v", logits);
        input.put("groups", groups);
        return function("{ \"name\": \"o\", \"func\": \"group_softmax\", \"field\": \"v\", \"groups\": \"groups\" }").apply(input, Instant.EPOCH);
    }

    // the softmax of the largest logit of each group
    @Test
    public void testGroupSoftmax() {
        final List<Float> logits = List.of(9f, 1f, 2f, 0f, 3f);
        final double high = Math.exp(3) / (Math.exp(3) + Math.exp(2));
        assertClose(List.of(1 - high, high), groupSoftmax(logits, List.of(List.of(1L, 2L), List.of(3L, 4L))));
        assertClose(List.of(1 - high, high), groupSoftmax(Tensor.of(new float[]{9, 1, 2, 0, 3}, new long[]{5}), List.of(List.of(1L, 2L), List.of(3L, 4L))));
        // a Tensor of a dimension of 1 is a vector as well
        assertClose(List.of(1 - high, high), groupSoftmax(Tensor.of(new float[]{9, 1, 2, 0, 3}, new long[]{1, 5}), List.of(List.of(1L, 2L), List.of(3L, 4L))));
        Assertions.assertNull(groupSoftmax(null, List.of(List.of(1L))));
        Assertions.assertNull(groupSoftmax(logits, null));
    }

    @Test
    public void testGroupSoftmaxOfIllegalGroups() {
        final List<Float> logits = List.of(1f, 2f);
        Assertions.assertThrows(IllegalArgumentException.class, () -> groupSoftmax(logits, List.of(List.of(2L))));
        Assertions.assertThrows(IllegalArgumentException.class, () -> groupSoftmax(logits, List.of(List.of(-1L))));
        Assertions.assertThrows(IllegalArgumentException.class, () -> groupSoftmax(logits, List.of(List.of())));
        Assertions.assertThrows(IllegalArgumentException.class, () -> groupSoftmax(logits, List.of(Arrays.asList(1L, null))));
        // the type of groups, its absence and the type of the vector are config errors: an id is an integer
        for(final String json : List.of(
                "{ \"name\": \"o\", \"func\": \"group_softmax\", \"field\": \"v\", \"groups\": \"v\" }",
                "{ \"name\": \"o\", \"func\": \"group_softmax\", \"field\": \"v\", \"groups\": \"floatGroups\" }",
                "{ \"name\": \"o\", \"func\": \"group_softmax\", \"field\": \"v\" }",
                "{ \"name\": \"o\", \"func\": \"group_softmax\", \"field\": \"m2\", \"groups\": \"groups\" }")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function(json), json);
        }
    }


    @Test
    public void testPositionLast() {
        final Tensor logits = Tensor.of(new float[]{9, 0, 0, 0, 7, 8}, new long[]{2, 3});
        Assertions.assertEquals(2L, apply("{\"name\": \"o\", \"func\": \"argmax\", \"field\": \"md\", \"position\": \"last\"}", logits));
        final List<?> top = (List<?>) apply("{\"name\": \"o\", \"func\": \"topk\", \"field\": \"md\", \"position\": \"last\", \"k\": 1}", logits);
        Assertions.assertEquals(2L, ((Map<?, ?>) top.getFirst()).get("index"));
        // a matrix is flat in a record
        Assertions.assertEquals(2L, apply("{\"name\": \"o\", \"func\": \"argmax\", \"field\": \"md\", \"position\": \"last\"}", List.of(9f, 0f, 0f, 0f, 7f, 8f)));
        Assertions.assertNull(apply("{\"name\": \"o\", \"func\": \"argmax\", \"field\": \"md\", \"position\": \"last\"}", null));

        for(final String json : List.of(
                "{\"name\": \"o\", \"func\": \"argmax\", \"field\": \"md\", \"position\": \"first\"}",
                "{\"name\": \"o\", \"func\": \"argmax\", \"field\": \"v\", \"position\": \"last\"}",
                "{\"name\": \"o\", \"func\": \"softmax\", \"field\": \"md\", \"position\": \"last\"}")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function(json), json);
        }
    }

    @Test
    public void testSlice() {
        final Tensor tensor = Tensor.of(new float[]{0, 1, 2, 3, 4, 5}, new long[]{2, 3});
        final String row = "{\"name\": \"o\", \"func\": \"slice\", \"field\": \"m2\", \"index\": -1}";
        Assertions.assertEquals(Schema.Type.array, function(row).getOutputFieldType().getType());
        final Tensor last = (Tensor) apply(row, tensor);
        Assertions.assertSame(tensor.data(), last.data());
        Assertions.assertEquals(List.of(3f, 4f, 5f), last.toList());
        Assertions.assertEquals(List.of(3f, 4f, 5f), apply(row, List.of(0f, 1f, 2f, 3f, 4f, 5f)), "a record value stays one");

        final String column = "{\"name\": \"o\", \"func\": \"slice\", \"field\": \"m2\", \"axis\": -1, \"start\": 1}";
        final Schema.FieldType columnType = function(column).getOutputFieldType();
        Assertions.assertEquals(List.of(2, 2), columnType.getShape());
        Assertions.assertEquals(List.of(1f, 2f, 4f, 5f), ((Tensor) apply(column, tensor)).toList());

        final String part = "{\"name\": \"o\", \"func\": \"slice\", \"field\": \"v\", \"start\": -2, \"end\": 99}";
        Assertions.assertEquals(List.of(2f, 3f), apply(part, List.of(1f, 2f, 3f)));
        Assertions.assertEquals(2f, apply("{\"name\": \"o\", \"func\": \"slice\", \"field\": \"v\", \"index\": 1}", List.of(1f, 2f, 3f)));
        Assertions.assertEquals(List.of(), apply("{\"name\": \"o\", \"func\": \"slice\", \"field\": \"v\", \"start\": 2, \"end\": 1}", List.of(1f, 2f, 3f)));
        // integers stay integers
        Assertions.assertEquals(List.of(2L), apply("{\"name\": \"o\", \"func\": \"slice\", \"field\": \"longs\", \"start\": 1}", List.of(1L, 2L)));
        Assertions.assertNull(apply(row, null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> apply("{\"name\": \"o\", \"func\": \"slice\", \"field\": \"v\", \"index\": 3}", List.of(1f, 2f, 3f)));

        for(final String json : List.of(
                "{\"name\": \"o\", \"func\": \"slice\", \"field\": \"v\"}",
                "{\"name\": \"o\", \"func\": \"slice\", \"field\": \"v\", \"index\": 0, \"start\": 1}",
                "{\"name\": \"o\", \"func\": \"slice\", \"field\": \"v\", \"axis\": 1, \"index\": 0}",
                "{\"name\": \"o\", \"func\": \"slice\", \"field\": \"s\", \"index\": 0}")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function(json), json);
        }
    }

    // with a seed the result depends on the logits and on the iteration only, and each index comes as often as its probability says
    @Test
    public void testSample() {
        final String seeded = "{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"seed\": 7}";
        final List<Float> logits = List.of((float) Math.log(0.2), (float) Math.log(0.8));
        final int[] counts = new int[2];
        final Set<Object> draws = new HashSet<>();
        for(long iteration=0; iteration<2000; iteration++) {
            final Map<String, Object> input = new HashMap<>();
            input.put("v", logits);
            input.put("_iteration", iteration);
            final SelectFunction function = function(seeded);
            final Object index = function.apply(input, Instant.EPOCH);
            Assertions.assertEquals(index, function.apply(input, Instant.EPOCH), "a retried record gets the same one");
            counts[((Number) index).intValue()]++;
            draws.add(index);
        }
        Assertions.assertEquals(2, draws.size());
        Assertions.assertEquals(0.8, counts[1] / 2000d, 0.03);

        // temperature 0 is the most likely, and so are topK 1 and a small topP
        final List<Float> three = List.of(0f, 2f, 1f);
        Assertions.assertEquals(1L, apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"temperature\": 0}", three));
        Assertions.assertEquals(1L, apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topK\": 1}", three));
        Assertions.assertEquals(1L, apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topP\": 0.01}", three));
        // top k and top p only keep the largest
        for(int seed=0; seed<200; seed++) {
            final Object topK = apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topK\": 2, \"seed\": " + seed + "}", three);
            Assertions.assertNotEquals(0L, topK);
            final Object topP = apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topP\": 0.7, \"seed\": " + seed + "}", three);
            Assertions.assertEquals(1L, apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topP\": 0.6, \"seed\": " + seed + "}", three));
            Assertions.assertNotEquals(0L, topP);
        }
        // NaN is never chosen
        Assertions.assertEquals(1L, apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"seed\": 1}", List.of(Float.NaN, 0f)));
        Assertions.assertNull(apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\"}", List.of(Float.NaN)));
        Assertions.assertEquals(2L, apply("{\"name\": \"o\", \"func\": \"sample\", \"field\": \"md\", \"position\": \"last\", \"temperature\": 0}", Tensor.of(new float[]{9, 0, 0, 0, 7, 8}, new long[]{2, 3})));

        for(final String json : List.of(
                "{\"name\": \"o\", \"func\": \"sample\", \"field\": \"s\"}",
                "{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"temperature\": -1}",
                "{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topK\": 0}",
                "{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topP\": 0}",
                "{\"name\": \"o\", \"func\": \"sample\", \"field\": \"v\", \"topP\": 1.5}")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> function(json), json);
        }
    }
}
