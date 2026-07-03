package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessorTest {

    private static Schema createSchema() {
        return Schema.of(List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64),
                Schema.Field.of("arrayField", Schema.FieldType.array(Schema.FieldType.INT64))));
    }

    private static Map<String, Object> createValues(String s, long l, List<Long> array) {
        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", s);
        values.put("longField", l);
        values.put("arrayField", array);
        return values;
    }

    @Test
    public void testParametersToJson() {
        final String parametersJson = """
                {
                  "filter": [
                    { "key": "longField", "op": ">", "value": 0 }
                  ],
                  "select": [
                    { "name": "stringField" },
                    { "name": "doubled", "expression": "longField * 2", "type": "float64" }
                  ],
                  "flatten": "arrayField"
                }
                """;
        final Processor.Parameters parameters = new Gson().fromJson(parametersJson, Processor.Parameters.class);
        Assertions.assertTrue(parameters.validate().isEmpty());
        parameters.setDefaults();

        final JsonObject json = parameters.toJson();
        Assertions.assertTrue(json.has("filter"));
        Assertions.assertTrue(json.has("select"));
        Assertions.assertEquals("arrayField", json.get("flatten").getAsString());

        // empty parameters produce empty json
        final Processor.Parameters empty = new Gson().fromJson("{}", Processor.Parameters.class);
        Assertions.assertEquals(0, empty.toJson().size());
    }

    @Test
    public void testProcessFilterSelectFlatten() {
        final String parametersJson = """
                {
                  "filter": [
                    { "key": "longField", "op": ">", "value": 0 }
                  ],
                  "select": [
                    { "name": "stringField" },
                    { "name": "doubled", "expression": "longField * 2", "type": "float64" },
                    { "name": "arrayField" }
                  ],
                  "flatten": "arrayField"
                }
                """;
        final Processor.Parameters parameters = new Gson().fromJson(parametersJson, Processor.Parameters.class);
        final Processor processor = parameters.create(createSchema()).setup(createSchema());

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");

        // matched: select applied then flattened
        final List<Map<String, Object>> outputs = processor
                .process(createValues("a", 5L, List.of(1L, 2L)), timestamp);
        Assertions.assertEquals(2, outputs.size());
        for(final Map<String, Object> output : outputs) {
            Assertions.assertEquals("a", output.get("stringField"));
            Assertions.assertEquals(10.0D, output.get("doubled"));
        }
        Assertions.assertEquals(1L, outputs.get(0).get("arrayField"));
        Assertions.assertEquals(2L, outputs.get(1).get("arrayField"));

        // filtered out
        final List<Map<String, Object>> filtered = processor
                .process(createValues("a", -1L, List.of(1L)), timestamp);
        Assertions.assertTrue(filtered.isEmpty());

        // MElement overload
        final MElement element = MElement.of(createValues("b", 3L, List.of(7L)), timestamp);
        final List<Map<String, Object>> elementOutputs = processor.process(element);
        Assertions.assertEquals(1, elementOutputs.size());
        Assertions.assertEquals(6.0D, elementOutputs.get(0).get("doubled"));
        Assertions.assertEquals(7L, elementOutputs.get(0).get("arrayField"));

        final MElement filteredElement = MElement.of(createValues("b", 0L, List.of(7L)), timestamp);
        Assertions.assertTrue(processor.process(filteredElement).isEmpty());
    }

    @Test
    public void testProcessPassThrough() {
        final Processor.Parameters parameters = new Gson().fromJson("{}", Processor.Parameters.class);
        final Processor processor = parameters.create(createSchema()).setup(createSchema());

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final Map<String, Object> values = createValues("a", 1L, List.of(1L, 2L));

        final List<Map<String, Object>> outputs = processor.process(values, timestamp);
        Assertions.assertEquals(1, outputs.size());
        Assertions.assertEquals(values, outputs.get(0));

        final MElement element = MElement.of(values, timestamp);
        final List<Map<String, Object>> elementOutputs = processor.process(element);
        Assertions.assertEquals(1, elementOutputs.size());
        Assertions.assertEquals("a", elementOutputs.get(0).get("stringField"));

        // copy constructor
        final Processor copied = new Processor(processor);
        Assertions.assertEquals(1, copied.process(values, timestamp).size());
    }

}
