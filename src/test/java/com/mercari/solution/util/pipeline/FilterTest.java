package com.mercari.solution.util.pipeline;

import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.StructToMapConverter;
import org.apache.avro.util.Utf8;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;


public class FilterTest {

    @Test
    public void testFilter() {
        final String config1 = """
                [
                  { "key": "stringField", "op": "=", "value": "stringValue" },
                  { "key": "longField", "op": ">=", "value": 100 }
                ]
                """;

        final Filter.ConditionNode node1 = Filter.parse(config1);

        final Schema schema = Schema.builder()
                .withField("stringField", Schema.FieldType.STRING)
                .withField("longField", Schema.FieldType.INT64)
                .build();

        MElement element1 = MElement.builder()
                .withString("stringField", "stringValue")
                .withInt64("longField", 100L)
                .build();
        Assertions.assertTrue(Filter.filter(node1, schema, element1));

        element1 = MElement.builder()
                .withString("stringField", "stringValue")
                .withInt64("longField", 99L)
                .build();
        Assertions.assertFalse(Filter.filter(node1, schema, element1));

        element1 = MElement.builder()
                .withString("stringField", "stringValue_")
                .withInt64("longField", 99L)
                .build();
        Assertions.assertFalse(Filter.filter(node1, schema, element1));

        element1 = MElement.builder()
                .withString("stringField", "stringValue_")
                .withInt64("longField", 100L)
                .build();
        Assertions.assertFalse(Filter.filter(node1, schema, element1));

    }

    @Test
    public void testLeafCompare() {

        // Number
        var leaf1 = new Filter.ConditionLeaf();
        leaf1.setKey("");
        leaf1.setValue(new Gson().fromJson("1", JsonElement.class));

        leaf1.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.NOT_EQUAL);
        Assertions.assertFalse(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.NOT_EQUAL);
        Assertions.assertFalse(Filter.is(null, leaf1));

        leaf1.setOp(Filter.Op.GREATER);
        Assertions.assertFalse(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assertions.assertTrue(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.GREATER);
        Assertions.assertTrue(Filter.is(10, leaf1));
        leaf1.setOp(Filter.Op.GREATER);
        Assertions.assertTrue(Filter.is(12.312, leaf1));
        leaf1.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assertions.assertTrue(Filter.is(2212310.12221, leaf1));
        leaf1.setOp(Filter.Op.GREATER);
        Assertions.assertFalse(Filter.is(-10, leaf1));
        leaf1.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assertions.assertFalse(Filter.is(-10, leaf1));

        leaf1.setOp(Filter.Op.LESSER);
        Assertions.assertFalse(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assertions.assertTrue(Filter.is(1, leaf1));
        leaf1.setOp(Filter.Op.LESSER);
        Assertions.assertFalse(Filter.is(10, leaf1));
        leaf1.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assertions.assertFalse(Filter.is(10, leaf1));
        leaf1.setOp(Filter.Op.LESSER);
        Assertions.assertTrue(Filter.is(-10, leaf1));
        leaf1.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assertions.assertTrue(Filter.is(-10, leaf1));

        // Number in, notin
        var leaf2 = new Filter.ConditionLeaf();
        leaf2.setKey("");
        leaf2.setValue(new Gson().fromJson("[1,2,3]", JsonArray.class));

        leaf2.setOp(Filter.Op.IN);
        Assertions.assertTrue(Filter.is(1, leaf2));
        Assertions.assertTrue(Filter.is(2, leaf2));
        Assertions.assertTrue(Filter.is(3, leaf2));
        Assertions.assertFalse(Filter.is(4, leaf2));
        Assertions.assertFalse(Filter.is(-3, leaf2));
        Assertions.assertFalse(Filter.is(-4.12, leaf2));

        leaf2.setOp(Filter.Op.NOT_IN);
        Assertions.assertFalse(Filter.is(1, leaf2));
        Assertions.assertFalse(Filter.is(2, leaf2));
        Assertions.assertFalse(Filter.is(3, leaf2));
        Assertions.assertTrue(Filter.is(-100, leaf2));

        // String
        var leaf3 = new Filter.ConditionLeaf();
        leaf3.setKey("");
        leaf3.setValue(new Gson().fromJson("a", JsonElement.class));

        leaf3.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is("a", leaf3));
        Assertions.assertFalse(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.NOT_EQUAL);
        Assertions.assertFalse(Filter.is("a", leaf3));
        Assertions.assertTrue(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.GREATER);
        Assertions.assertFalse(Filter.is("a", leaf3));
        Assertions.assertTrue(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assertions.assertTrue(Filter.is("a", leaf3));
        Assertions.assertTrue(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.LESSER);
        Assertions.assertFalse(Filter.is("a", leaf3));
        Assertions.assertFalse(Filter.is("b", leaf3));
        leaf3.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assertions.assertTrue(Filter.is("a", leaf3));
        Assertions.assertFalse(Filter.is("b", leaf3));

        // String in, notin
        var leaf4 = new Filter.ConditionLeaf();
        leaf4.setKey("");
        leaf4.setValue(new Gson().fromJson("['a','b','c']", JsonArray.class));

        leaf4.setOp(Filter.Op.IN);
        Assertions.assertTrue(Filter.is("a", leaf4));
        Assertions.assertTrue(Filter.is("b", leaf4));
        Assertions.assertTrue(Filter.is("c", leaf4));
        Assertions.assertFalse(Filter.is("d", leaf4));
        Assertions.assertFalse(Filter.is("dsafa", leaf4));
        Assertions.assertFalse(Filter.is("A", leaf4));

        leaf4.setOp(Filter.Op.NOT_IN);
        Assertions.assertFalse(Filter.is("a", leaf4));
        Assertions.assertFalse(Filter.is("b", leaf4));
        Assertions.assertFalse(Filter.is("c", leaf4));
        Assertions.assertTrue(Filter.is("dfa", leaf4));

        // Null
        var leaf5 = new Filter.ConditionLeaf();
        leaf5.setKey("");
        leaf5.setValue(new Gson().fromJson("null", JsonElement.class));

        leaf5.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is(null, leaf5));
        Assertions.assertFalse(Filter.is("b", leaf5));
        leaf5.setOp(Filter.Op.NOT_EQUAL);
        Assertions.assertFalse(Filter.is(null, leaf5));
        Assertions.assertTrue(Filter.is("b", leaf5));

        // Date
        var leaf6 = new Filter.ConditionLeaf();
        leaf6.setKey("");
        leaf6.setValue(new Gson().fromJson("2021-08-21", JsonElement.class));

        leaf6.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.NOT_EQUAL);
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.GREATER);
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.LESSER);
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 22), leaf6));
        leaf6.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 20), leaf6));
        Assertions.assertTrue(Filter.is(LocalDate.of(2021, 8, 21), leaf6));
        Assertions.assertFalse(Filter.is(LocalDate.of(2021, 8, 22), leaf6));

        // Timestamp
        var leaf7 = new Filter.ConditionLeaf();
        leaf7.setKey("");
        leaf7.setValue(new Gson().fromJson("\"2021-08-21T10:30:45Z\"", JsonElement.class));

        leaf7.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.NOT_EQUAL);
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.GREATER);
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.GREATER_OR_EQUAL);
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.LESSER);
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));
        leaf7.setOp(Filter.Op.LESSER_OR_EQUAL);
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-20T10:30:45Z"), leaf7));
        Assertions.assertTrue(Filter.is(Instant.parse("2021-08-21T10:30:45Z"), leaf7));
        Assertions.assertFalse(Filter.is(Instant.parse("2021-08-22T10:30:45Z"), leaf7));

    }

    @Test
    public void testNodeCompare() {
        var leaf1 = new Filter.ConditionLeaf();
        leaf1.setKey("field1");
        leaf1.setValue(new Gson().fromJson("a", JsonElement.class));
        leaf1.setOp(Filter.Op.EQUAL);

        var leaf2 = new Filter.ConditionLeaf();
        leaf2.setKey("field2");
        leaf2.setValue(new Gson().fromJson("1", JsonElement.class));
        leaf2.setOp(Filter.Op.EQUAL);

        // AND
        var node1 = new Filter.ConditionNode();
        node1.setType(Filter.Type.AND);
        node1.setLeaves(Arrays.asList(leaf1, leaf2));

        Schema schema = Schema.builder()
                .withField("field1", Schema.FieldType.STRING)
                .withField("field2", Schema.FieldType.INT64)
                .build();

        Struct struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assertions.assertFalse(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node1, schema, MElement.of(struct, Instant.now())));

        // OR
        var node2 = new Filter.ConditionNode();
        node2.setType(Filter.Type.OR);
        node2.setLeaves(Arrays.asList(leaf1, leaf2));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assertions.assertTrue(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node2, schema, MElement.of(struct, Instant.now())));

        // NEST AND(AND,OR)
        var node3 = new Filter.ConditionNode();
        node3.setType(Filter.Type.AND);
        node3.setNodes(Arrays.asList(node1, node2));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assertions.assertFalse(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node3, schema, MElement.of(struct, Instant.now())));

        // NEST OR(AND,OR)
        var node4 = new Filter.ConditionNode();
        node4.setType(Filter.Type.OR);
        node4.setNodes(Arrays.asList(node1, node2));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assertions.assertTrue(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node4, schema, MElement.of(struct, Instant.now())));

        // NEST OR(AND,OR) with Leaves
        var leaf3 = new Filter.ConditionLeaf();
        leaf3.setKey("field2");
        leaf3.setValue(new Gson().fromJson("1", JsonElement.class));
        leaf3.setOp(Filter.Op.NOT_EQUAL);

        var node5 = new Filter.ConditionNode();
        node5.setType(Filter.Type.OR);
        node5.setNodes(Arrays.asList(node1, node2));
        node5.setLeaves(Arrays.asList(leaf3));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assertions.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assertions.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assertions.assertTrue(Filter.filter(node5, schema, MElement.of(struct, Instant.now())));

        // NEST AND(AND,OR) with Leaves
        var node6 = new Filter.ConditionNode();
        node6.setType(Filter.Type.AND);
        node6.setNodes(Arrays.asList(node1, node2));
        node6.setLeaves(Arrays.asList(leaf3));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(1)
                .build();
        Assertions.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("a")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(1)
                .build();
        Assertions.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to("b")
                .set("field2").to(2)
                .build();
        Assertions.assertFalse(Filter.filter(node6, schema, MElement.of(struct, Instant.now())));
    }

    @Test
    public void testNodeParseElement() {

        final String filter1String =
                "{\n" +
                        "  \"or\": [\n" +
                        "    { \"key\": \"field1\", \"op\": \"=\", \"value\": 1 },\n" +
                        "      {\n" +
                        "        \"and\": [\n" +
                        "          { \"key\": \"field2\", \"op\": \"=\", \"value\": 2 },\n" +
                        "          { \"key\": \"field3\", \"op\": \"=\", \"value\": 3 }\n" +
                        "        ]\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}";

        Schema schema = Schema.builder()
                .withField("field1", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .build();

        final JsonElement filter1 = new Gson().fromJson(filter1String, JsonElement.class);
        Struct struct = Struct.newBuilder()
                .set("field1").to(1L)
                .set("field2").to(2L)
                .set("field3").to(3L)
                .build();
        Assertions.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Assertions.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(4)
                .build();
        Assertions.assertFalse(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));


        // Simple case
        final String filter2String = "[\n" +
                "{ \"key\": \"field1\", \"op\": \"=\", \"value\": 1 },\n" +
                "{ \"key\": \"field2\", \"op\": \"=\", \"value\": 2 },\n" +
                "{ \"key\": \"field3\", \"op\": \"=\", \"value\": 3 }\n" +
                "]\n";

        final JsonElement filter2 = new Gson().fromJson(filter2String, JsonElement.class);
        struct = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Assertions.assertTrue(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Assertions.assertFalse(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct, Instant.now())));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(4)
                .build();
        Assertions.assertFalse(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct, Instant.now())));
    }

    @Test
    public void testNodeParseObject() {

        final String filter1String =
                "{\n" +
                        "  \"or\": [\n" +
                        "    { \"key\": \"field1\", \"op\": \"=\", \"value\": 1 },\n" +
                        "      {\n" +
                        "        \"and\": [\n" +
                        "          { \"key\": \"field2\", \"op\": \"=\", \"value\": 2 },\n" +
                        "          { \"key\": \"field3\", \"op\": \"=\", \"value\": 3 }\n" +
                        "        ]\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}";

        Schema schema = Schema.builder()
                .withField("field1", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .withField("field2", Schema.FieldType.INT64)
                .build();

        final JsonObject filter1 = new Gson().fromJson(filter1String, JsonObject.class);
        Struct struct = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        Map<String,Object> values = StructToMapConverter.convert(struct);
        Assertions.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));
        Assertions.assertTrue(Filter.filter(Filter.parse(filter1), values));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        values = StructToMapConverter.convert(struct);
        Assertions.assertTrue(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));
        Assertions.assertTrue(Filter.filter(Filter.parse(filter1), values));

        struct = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(2)
                .set("field3").to(4)
                .build();
        values = StructToMapConverter.convert(struct);
        Assertions.assertFalse(Filter.filter(Filter.parse(filter1), schema, MElement.of(struct, Instant.now())));
        Assertions.assertFalse(Filter.filter(Filter.parse(filter1), values));

        final String filter2String =
                "{ \"key\": \"field1\", \"op\": \"=\", \"value\": 1 }";

        final JsonObject filter2 = new Gson().fromJson(filter2String, JsonObject.class);
        Struct struct2 = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        values = StructToMapConverter.convert(struct2);
        Assertions.assertTrue(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct2, Instant.now())));
        Assertions.assertTrue(Filter.filter(Filter.parse(filter2), values));

        struct2 = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(1)
                .set("field3").to(1)
                .build();
        values = StructToMapConverter.convert(struct2);
        Assertions.assertFalse(Filter.filter(Filter.parse(filter2), schema, MElement.of(struct2, Instant.now())));
        Assertions.assertFalse(Filter.filter(Filter.parse(filter2), values));


        final String filter3String =
                "{ \"expression\": \"(field1 / field2) - field3\", \"op\": \">\", \"value\": 0 }";

        final JsonObject filter3 = new Gson().fromJson(filter3String, JsonObject.class);
        Struct struct3 = Struct.newBuilder()
                .set("field1").to(1)
                .set("field2").to(2)
                .set("field3").to(3)
                .build();
        values = StructToMapConverter.convert(struct3);
        Assertions.assertFalse(Filter.filter(Filter.parse(filter3), schema, MElement.of(struct3, Instant.now())));
        Assertions.assertFalse(Filter.filter(Filter.parse(filter3), values));

        struct3 = Struct.newBuilder()
                .set("field1").to(2)
                .set("field2").to(1)
                .set("field3").to(1)
                .build();
        values = StructToMapConverter.convert(struct3);
        Assertions.assertTrue(Filter.filter(Filter.parse(filter3), schema, MElement.of(struct3, Instant.now())));
        Assertions.assertTrue(Filter.filter(Filter.parse(filter3), values));

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.DATE)
                    .withField("field2", Schema.FieldType.DATE)
                    .build();

            final String filterString =
                    "{ \"expression\": \"field1 - field2\", \"op\": \">\", \"value\": 0 }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Date.parseDate("2023-12-31"))
                    .set("field2").to(com.google.cloud.Date.parseDate("2022-12-31"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), values));

            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.TIMESTAMP)
                    .withField("field2", Schema.FieldType.TIMESTAMP)
                    .build();

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2023-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.TIMESTAMP)
                    .withField("field2", Schema.FieldType.TIMESTAMP)
                    .build();

            final String filterString =
                    "{ \"expression\": \"timestamp_diff_hour(field1, field2)\", \"op\": \"<\", \"value\": 24 }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T00:00:00.000Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2023-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.TIMESTAMP)
                    .withField("field2", Schema.FieldType.TIMESTAMP)
                    .build();

            final String filterString =
                    "{ \"expression\": \"timestamp_diff_day(field1, field2)\", \"op\": \"=\", \"value\": 365 }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2023-12-31T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.998Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T23:59:59.999Z"))
                    .set("field2").to(com.google.cloud.Timestamp.parseTimestamp("2022-12-31T23:59:59.999Z"))
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.INT64.withNullable(true))
                    .build();

            final String filterString =
                    "{ \"key\": \"field1\", \"op\": \"=\", \"value\": null }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to((Long) null)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), values));

            struct = Struct.newBuilder()
                    .set("field1").to(-10D)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.INT64.withNullable(true))
                    .build();

            final String filterString =
                    "{ \"key\": \"field1\", \"op\": \"!=\", \"value\": null }";

            final JsonObject filter = new Gson().fromJson(filterString, JsonObject.class);
            struct = Struct.newBuilder()
                    .set("field1").to((Long) null)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertFalse(Filter.filter(Filter.parse(filter), values));

            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.FLOAT64.withNullable(true))
                    .build();

            struct = Struct.newBuilder()
                    .set("field1").to(10D)
                    .build();
            values = StructToMapConverter.convert(struct);
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), values));
        }

        {
            schema = Schema.builder()
                    .withField("field1", Schema.FieldType.STRING.withNullable(true))
                    .build();

            final String filterString = """
                    [
                      { "key": "field1", "op": "match", "value": "/2024-11-(0[1-9]|[12][0-9]|3[01])/" },
                      { "key": "field1", "op": "match", "value": "\\\\.pdf$" }
                    ]
                    """;


            final JsonElement filter = new Gson().fromJson(filterString, JsonElement.class);
            struct = Struct.newBuilder()
                    .set("field1").to("gs://mybucket/2024-11-11/myfile.pdf")
                    .build();
            values = StructToMapConverter.convert(struct);
            System.out.println(Filter.parse(filter));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), schema, MElement.of(struct, Instant.now())));
            Assertions.assertTrue(Filter.filter(Filter.parse(filter), values));
        }

    }

    @Test
    public void testOpOf() {
        Assertions.assertEquals(Filter.Op.EQUAL, Filter.Op.of("="));
        Assertions.assertEquals(Filter.Op.NOT_EQUAL, Filter.Op.of("!="));
        Assertions.assertEquals(Filter.Op.GREATER, Filter.Op.of(">"));
        Assertions.assertEquals(Filter.Op.GREATER_OR_EQUAL, Filter.Op.of(">="));
        Assertions.assertEquals(Filter.Op.LESSER, Filter.Op.of("<"));
        Assertions.assertEquals(Filter.Op.LESSER_OR_EQUAL, Filter.Op.of("<="));
        Assertions.assertEquals(Filter.Op.IN, Filter.Op.of("in"));
        Assertions.assertEquals(Filter.Op.NOT_IN, Filter.Op.of("not in"));
        Assertions.assertEquals(Filter.Op.MATCH, Filter.Op.of("match"));
        // case insensitive and trimmed
        Assertions.assertEquals(Filter.Op.IN, Filter.Op.of(" IN "));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.Op.of("=="));
    }

    @Test
    public void testFilterLifecycle() {
        // null filter always matches
        final Filter nullFilter = Filter.of((String) null);
        nullFilter.setup();
        final MElement element = MElement.builder()
                .withString("stringField", "a")
                .build();
        Assertions.assertTrue(nullFilter.filter(element));
        Assertions.assertTrue(nullFilter.toJson().isJsonNull());

        final Filter filter = Filter.of(new Gson().fromJson("""
                [ { "key": "stringField", "op": "=", "value": "a" } ]
                """, JsonElement.class));
        filter.setup();
        Assertions.assertTrue(filter.filter(element));
        Assertions.assertTrue(filter.toJson().isJsonArray());

        final MElement notMatched = MElement.builder()
                .withString("stringField", "b")
                .build();
        Assertions.assertFalse(filter.filter(notMatched));

        // filter with field list
        final Schema schema = Schema.builder()
                .withField("stringField", Schema.FieldType.STRING)
                .build();
        Assertions.assertTrue(filter.filter(schema.getFields(), element));
        Assertions.assertFalse(filter.filter(schema.getFields(), notMatched));
    }

    @Test
    public void testParseErrors() {
        // primitive json
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.parse(new Gson().fromJson("1", JsonElement.class)));
        // array containing and/or object
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.parse(new Gson().fromJson("""
                        [ { "or": [ { "key": "a", "op": "=", "value": 1 } ] } ]
                        """, JsonElement.class)));
        // array containing non object
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.parse(new Gson().fromJson("[1]", JsonElement.class)));
        // both and / or at top level
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.parse(new Gson().fromJson("""
                        { "and": [ { "key": "a", "op": "=", "value": 1 } ], "or": [ { "key": "a", "op": "=", "value": 1 } ] }
                        """, JsonObject.class)));
        // and parameter is not array
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.parse(new Gson().fromJson("""
                        { "and": { "key": "a", "op": "=", "value": 1 } }
                        """, JsonObject.class)));
        // leaf without key/op/value
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.parse(new Gson().fromJson("""
                        [ { "key": "a", "op": "=" } ]
                        """, JsonElement.class)));
        // expression must be string
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.parse(new Gson().fromJson("""
                        [ { "expression": true, "op": "=", "value": 1 } ]
                        """, JsonElement.class)));

        // null json produces a TRUE type node
        Assertions.assertEquals(Filter.Type.TRUE, Filter.parse((JsonElement) null).getType());
    }

    @Test
    public void testLeafInErrorAndMatch() {
        // in op requires array value
        final var inLeaf = new Filter.ConditionLeaf();
        inLeaf.setKey("");
        inLeaf.setOp(Filter.Op.IN);
        inLeaf.setValue(new Gson().fromJson("1", JsonElement.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Filter.is(1, inLeaf));

        // match op via parse
        final Filter.ConditionNode node = Filter.parse("""
                [ { "key": "stringField", "op": "match", "value": "^ab.*z$" } ]
                """);
        final Map<String, Object> matched = Map.of("stringField", "abcz");
        final Map<String, Object> notMatched = Map.of("stringField", "abc");
        Assertions.assertTrue(Filter.filter(node, matched));
        Assertions.assertFalse(Filter.filter(node, notMatched));
    }

    @Test
    public void testLeafCompareValueTypes() {
        final var leaf = new Filter.ConditionLeaf();
        leaf.setKey("");
        leaf.setValue(new Gson().fromJson("1", JsonElement.class));
        leaf.setOp(Filter.Op.EQUAL);

        Assertions.assertTrue(Filter.is((byte) 1, leaf));
        Assertions.assertTrue(Filter.is((short) 1, leaf));
        Assertions.assertTrue(Filter.is(1L, leaf));
        Assertions.assertTrue(Filter.is(1F, leaf));
        Assertions.assertTrue(Filter.is(1D, leaf));
        Assertions.assertTrue(Filter.is(BigInteger.ONE, leaf));
        Assertions.assertTrue(Filter.is(BigDecimal.ONE, leaf));
        Assertions.assertFalse(Filter.is((byte) 2, leaf));
        Assertions.assertFalse(Filter.is(BigInteger.TWO, leaf));

        // NaN and Infinity never match any comparison
        Assertions.assertFalse(Filter.is(Double.NaN, leaf));
        Assertions.assertFalse(Filter.is(Float.NaN, leaf));
        Assertions.assertFalse(Filter.is(Double.POSITIVE_INFINITY, leaf));
        Assertions.assertFalse(Filter.is(Float.NEGATIVE_INFINITY, leaf));

        // TRUE / FALSE ops
        leaf.setOp(Filter.Op.TRUE);
        Assertions.assertTrue(Filter.is(1, leaf));
        leaf.setOp(Filter.Op.FALSE);
        Assertions.assertFalse(Filter.is(1, leaf));

        // boolean
        final var boolLeaf = new Filter.ConditionLeaf();
        boolLeaf.setKey("");
        boolLeaf.setValue(new Gson().fromJson("true", JsonElement.class));
        boolLeaf.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is(true, boolLeaf));
        Assertions.assertFalse(Filter.is(false, boolLeaf));

        // Utf8
        final var stringLeaf = new Filter.ConditionLeaf();
        stringLeaf.setKey("");
        stringLeaf.setValue(new Gson().fromJson("\"a\"", JsonElement.class));
        stringLeaf.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is(new Utf8("a"), stringLeaf));
        Assertions.assertFalse(Filter.is(new Utf8("b"), stringLeaf));

        // LocalTime
        final var timeLeaf = new Filter.ConditionLeaf();
        timeLeaf.setKey("");
        timeLeaf.setValue(new Gson().fromJson("\"10:30:00\"", JsonElement.class));
        timeLeaf.setOp(Filter.Op.LESSER);
        Assertions.assertTrue(Filter.is(LocalTime.of(10, 0, 0), timeLeaf));
        Assertions.assertFalse(Filter.is(LocalTime.of(11, 0, 0), timeLeaf));

        // java.time.Instant
        final var instantLeaf = new Filter.ConditionLeaf();
        instantLeaf.setKey("");
        instantLeaf.setValue(new Gson().fromJson("\"2021-08-21T10:30:45Z\"", JsonElement.class));
        instantLeaf.setOp(Filter.Op.EQUAL);
        Assertions.assertTrue(Filter.is(java.time.Instant.parse("2021-08-21T10:30:45Z"), instantLeaf));
        Assertions.assertFalse(Filter.is(java.time.Instant.parse("2021-08-22T10:30:45Z"), instantLeaf));
    }

    @Test
    public void testExpressionVariableMissing() {
        final Filter.ConditionNode node = Filter.parse("""
                [ { "expression": "field1 + field2", "op": ">", "value": 1 } ]
                """);
        Assertions.assertEquals(Set.of("field1", "field2"), node.getRequiredVariables());
        // missing expression variable throws
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Filter.filter(node, Map.of("field1", 1L)));
        Assertions.assertTrue(Filter.filter(node, Map.of("field1", 1L, "field2", 1L)));
    }

    @Test
    public void testEmptyConditionNode() {
        // node without leaves and nodes never matches
        final var node = new Filter.ConditionNode();
        node.setType(Filter.Type.AND);
        Assertions.assertFalse(Filter.filter(node, Map.of("field1", 1L)));

        // null condition always matches
        Assertions.assertTrue(Filter.filter((Filter.ConditionNode) null, Map.of("field1", 1L)));
    }

    @Test
    public void testConditionNodeToString() {
        final Filter.ConditionNode node = Filter.parse("""
                { "or": [
                  { "key": "field1", "op": "=", "value": 1 },
                  { "and": [
                    { "key": "field2", "op": ">", "value": 2 }
                  ] }
                ] }
                """);
        final String text = node.toString();
        Assertions.assertTrue(text.contains("OR"));
        Assertions.assertTrue(text.contains("field1"));
        Assertions.assertTrue(text.contains("field2"));
        Assertions.assertEquals(Set.of("field1", "field2"), node.getRequiredVariables());
    }

    @Test
    public void testGetFieldNames() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [
                    { "name": "field1", "type": "string" },
                    { "name": "field2", "type": "struct", "fields": [
                      { "name": "child", "type": "string" }
                    ] }
                  ]
                }
                """);
        final Set<String> names1 = Filter.ConditionNode.getFieldNames(null, schema.getField("field1"));
        Assertions.assertEquals(Set.of("field1"), names1);
        final Set<String> names2 = Filter.ConditionNode.getFieldNames(null, schema.getField("field2"));
        Assertions.assertEquals(Set.of("field2", "field2.child"), names2);
    }

    @Test
    public void testValidateError() {
        final Schema schema = Schema.builder()
                .withField("field1", Schema.FieldType.STRING)
                .build();
        final Filter.ConditionNode node = Filter.parse("""
                [ { "key": "missingField", "op": "=", "value": "a" } ]
                """);
        final var errorMessages = node.validate(schema.getFields());
        Assertions.assertEquals(1, errorMessages.size());
        Assertions.assertTrue(errorMessages.get(0).contains("missingField"));
    }

    @Test
    public void testValidate() {
        final String schemaString = """
                {
                  "fields": [
                    { "name": "field1", "type": "string" },
                    { "name": "field2", "type": "struct", "fields": [
                      { "name": "field2A", "type": "struct", "fields": [
                        { "name": "field2A1", "type": "string" },
                        { "name": "field2A2", "type": "string" }
                      ] }
                    ] },
                    { "name": "field3", "type": "int64" },
                    { "name": "field4", "type": "timestamp" }
                  ]
                }
                """;

        final String filterString = """
                    [
                      { "key": "field1", "op": "=", "value": "a" },
                      { "key": "field2.field2A.field2A2", "op": "=", "value": "okok" }
                    ]
                    """;

        final Schema schema = Schema.parse(schemaString);
        final Filter.ConditionNode node = Filter.parse(filterString);
        var r = node.validate(schema.getFields());
        Assertions.assertTrue(r.isEmpty());
    }

}