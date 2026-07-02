package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionTest {

    @Test
    public void test() {
        final List<Schema.Field> inputFields = List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("nestedField", Schema.FieldType.element(Schema.of(List.of(
                        Schema.Field.of("stringField", Schema.FieldType.STRING)
                ))))
        );

        final String partitionJsonArray = """
                [
                  {
                    "name": "partition1",
                    "filter": [
                      { "key": "nestedField.stringField", "op": "!=", "value": "" }
                    ],
                    "select": [
                      { "name": "stringField" },
                      { "name": "longField", "value": 1, "type": "int64" }
                    ]
                  }
                ]
                """;

        final String json = """
                {
                  "stringField": "",
                  "nestedField": {
                    "stringField": "a"
                  }
                }
                """;


        final JsonArray partitionJson = new Gson().fromJson(partitionJsonArray, JsonArray.class);
        final List<Partition> partitions = Partition.of(partitionJson, Schema.of(inputFields));
        for(final Partition partition : partitions) {
            partition.setup();
        }

        final MElement input1 = MElement.of(JsonToElementConverter.convert(inputFields, json), Instant.parse("2025-01-01T00:00:00Z"));

        for(final Partition partition : partitions) {
            final List<MElement> outputs = partition.execute(input1, Instant.parse("2025-01-01T00:00:00Z"));
            System.out.println(partition.getName() + " : " + outputs);
        }

    }

    private static Schema createSimpleSchema() {
        return Schema.of(List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("arrayField", Schema.FieldType.array(Schema.FieldType.INT64))));
    }

    private static MElement createSimpleElement(String s, long l, List<Long> array) {
        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", s);
        values.put("longField", l);
        values.put("arrayField", array);
        return MElement.of(values, Instant.parse("2025-01-01T00:00:00Z"));
    }

    @Test
    public void testPassThroughPartition() {
        final Schema inputSchema = createSimpleSchema();
        final JsonObject partitionJson = new Gson().fromJson("""
                { "name": "partition0" }
                """, JsonObject.class);

        final Partition partition = Partition.of(partitionJson, inputSchema);
        partition.setup();

        Assertions.assertEquals("partition0", partition.getName());
        Assertions.assertNotNull(partition.getOutputTag());
        // no select/unnest: output schema is input schema as-is
        Assertions.assertSame(inputSchema, partition.getOutputSchema());

        final MElement input = createSimpleElement("a", 1L, List.of(1L, 2L));
        // no filter: always matches
        Assertions.assertTrue(partition.match(input));

        final List<MElement> outputs = partition.execute(input, Instant.parse("2025-01-01T00:00:00Z"));
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertSame(input, outputs.get(0));

        partition.teardown();
    }

    @Test
    public void testMatch() {
        final Schema inputSchema = createSimpleSchema();
        final JsonObject partitionJson = new Gson().fromJson("""
                {
                  "name": "partition1",
                  "filter": [
                    { "key": "longField", "op": ">", "value": 1 }
                  ]
                }
                """, JsonObject.class);

        final Partition partition = Partition.of(partitionJson, inputSchema);
        partition.setup();

        Assertions.assertTrue(partition.match(createSimpleElement("a", 2L, List.of())));
        Assertions.assertFalse(partition.match(createSimpleElement("a", 1L, List.of())));
    }

    @Test
    public void testUnnestOnlyPartition() {
        final Schema inputSchema = createSimpleSchema();
        final JsonObject partitionJson = new Gson().fromJson("""
                { "name": "partition1", "flattenField": "arrayField" }
                """, JsonObject.class);

        final Partition partition = Partition.of(partitionJson, inputSchema);
        partition.setup();

        // flatten field type is unnested in the output schema
        Assertions.assertEquals(
                Schema.Type.int64,
                partition.getOutputSchema().getField("arrayField").getFieldType().getType());

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final List<MElement> outputs = partition.execute(createSimpleElement("a", 1L, List.of(10L, 20L)), timestamp);
        Assertions.assertEquals(2, outputs.size());
        Assertions.assertEquals(10L, outputs.get(0).getPrimitiveValue("arrayField"));
        Assertions.assertEquals(20L, outputs.get(1).getPrimitiveValue("arrayField"));
    }

    @Test
    public void testSelectAndUnnestPartition() {
        final Schema inputSchema = createSimpleSchema();
        final Partition partition = Partition.of(
                "partition1",
                new Gson().fromJson("""
                        [ { "key": "stringField", "op": "=", "value": "a" } ]
                        """, JsonArray.class),
                new Gson().fromJson("""
                        [
                          { "name": "stringField" },
                          { "name": "arrayField" }
                        ]
                        """, JsonArray.class),
                "arrayField",
                inputSchema);
        partition.setup();

        Assertions.assertTrue(partition.match(createSimpleElement("a", 1L, List.of())));
        Assertions.assertFalse(partition.match(createSimpleElement("b", 1L, List.of())));

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final List<MElement> outputs = partition.execute(createSimpleElement("a", 1L, List.of(10L, 20L)), timestamp);
        Assertions.assertEquals(2, outputs.size());
        Assertions.assertEquals("a", outputs.get(0).getPrimitiveValue("stringField"));
        Assertions.assertEquals(10L, outputs.get(0).getPrimitiveValue("arrayField"));
        Assertions.assertEquals(20L, outputs.get(1).getPrimitiveValue("arrayField"));
    }

    @Test
    public void testSqlPartition() {
        final Schema inputSchema = Schema.of(List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64)));

        // Partition registers the query table under the partition name.
        // The single element execute() path feeds the table named "INPUT",
        // so only a partition named INPUT receives the input element there.
        final JsonObject partitionJson = new Gson().fromJson("""
                {
                  "name": "INPUT",
                  "sql": "SELECT stringField, longField * 2 AS doubled FROM INPUT"
                }
                """, JsonObject.class);

        final Partition partition = Partition.of(partitionJson, inputSchema);
        partition.setup();

        Assertions.assertTrue(partition.getOutputSchema().hasField("doubled"));

        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", "a");
        values.put("longField", 3L);
        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final MElement input = MElement.of(values, timestamp);

        Assertions.assertTrue(partition.match(input));
        final List<MElement> outputs = partition.execute(input, timestamp);
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertEquals(6L, ((Number) outputs.get(0).getPrimitiveValue("doubled")).longValue());

        // executeStateful with explicit table name keyed inputs
        final Map<String, List<MElement>> inputsMap = new HashMap<>();
        inputsMap.put("INPUT", List.of(input));
        final List<MElement> statefulOutputs = partition.executeStateful(inputsMap, timestamp);
        Assertions.assertEquals(1, statefulOutputs.size());
        Assertions.assertEquals(6L, ((Number) statefulOutputs.get(0).getPrimitiveValue("doubled")).longValue());

        partition.teardown();

        // a sql partition whose name does not match the fed table name
        // must not throw and produces no rows for the single element execute() path
        final JsonObject otherJson = new Gson().fromJson("""
                {
                  "name": "mytable",
                  "sql": "SELECT stringField FROM mytable"
                }
                """, JsonObject.class);
        final Partition other = Partition.of(otherJson, inputSchema);
        other.setup();
        Assertions.assertTrue(other.execute(input, timestamp).isEmpty());
        other.teardown();
    }

    @Test
    public void testExecuteStateful() {
        final Schema inputSchema = createSimpleSchema();
        final JsonObject partitionJson = new Gson().fromJson("""
                {
                  "name": "partition1",
                  "select": [
                    { "name": "stringField" },
                    { "name": "longField" }
                  ]
                }
                """, JsonObject.class);

        final Partition partition = Partition.of(partitionJson, inputSchema);
        partition.setup();

        final Instant timestamp = Instant.parse("2025-01-01T00:00:10Z");
        final Map<String, Object> values1 = new HashMap<>();
        values1.put("stringField", "a");
        values1.put("longField", 1L);
        values1.put("arrayField", List.of());
        final Map<String, Object> values2 = new HashMap<>();
        values2.put("stringField", "b");
        values2.put("longField", 2L);
        values2.put("arrayField", List.of());
        final List<MElement> inputs = List.of(
                MElement.of(values1, Instant.parse("2025-01-01T00:00:01Z")),
                MElement.of(values2, Instant.parse("2025-01-01T00:00:02Z")));

        // list overload: stateless functions select the latest element
        final List<MElement> outputs = partition.executeStateful(inputs, timestamp);
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertEquals("b", outputs.get(0).getPrimitiveValue("stringField"));

        // map overload with single input
        final Map<String, List<MElement>> inputsMap = new HashMap<>();
        inputsMap.put("input1", inputs);
        final List<MElement> outputsFromMap = partition.executeStateful(inputsMap, timestamp);
        Assertions.assertEquals(1, outputsFromMap.size());
        Assertions.assertEquals("b", outputsFromMap.get(0).getPrimitiveValue("stringField"));
    }

    @Test
    public void testOfJsonArray() {
        final Schema inputSchema = createSimpleSchema();

        // null array
        Assertions.assertTrue(Partition.of((JsonArray) null, inputSchema).isEmpty());

        // non object elements are skipped
        final JsonArray array = new Gson().fromJson("""
                [ "notAnObject", { "name": "p1" } ]
                """, JsonArray.class);
        final List<Partition> partitions = Partition.of(array, inputSchema);
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("p1", partitions.get(0).getName());

        // partition without name
        final JsonArray noName = new Gson().fromJson("""
                [ { "filter": [ { "key": "longField", "op": "=", "value": 1 } ] } ]
                """, JsonArray.class);
        final List<Partition> noNamePartitions = Partition.of(noName, inputSchema);
        Assertions.assertEquals(1, noNamePartitions.size());
        Assertions.assertNull(noNamePartitions.get(0).getName());
    }

}
