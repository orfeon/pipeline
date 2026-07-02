package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Tests for com.mercari.solution.util.pipeline.Select (facade over select functions)
public class SelectFacadeTest {

    private static List<Schema.Field> createFields() {
        return List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64));
    }

    private static Map<String, Object> createValues(String s, long l) {
        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", s);
        values.put("longField", l);
        return values;
    }

    private static Select createSelect() {
        final String selectJson = """
                [
                  { "name": "stringField" },
                  { "name": "renamed", "field": "stringField" },
                  { "name": "constant", "type": "int64", "value": 5 },
                  { "name": "doubled", "expression": "longField * 2", "type": "float64" }
                ]
                """;
        final JsonArray selectArray = new Gson().fromJson(selectJson, JsonArray.class);
        return Select.of(selectArray, createFields());
    }

    @Test
    public void testSelect() {
        final Select select = createSelect();
        Assertions.assertTrue(select.useSelect());
        select.setup();

        final Schema outputSchema = Select.createOutputSchema(select);
        Assertions.assertTrue(outputSchema.hasField("stringField"));
        Assertions.assertTrue(outputSchema.hasField("renamed"));
        Assertions.assertTrue(outputSchema.hasField("constant"));
        Assertions.assertTrue(outputSchema.hasField("doubled"));

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final MElement element = MElement.of(createValues("a", 3L), timestamp);

        final Map<String, Object> selected = select.select(element, timestamp);
        Assertions.assertEquals("a", selected.get("stringField"));
        Assertions.assertEquals("a", selected.get("renamed"));
        Assertions.assertEquals(5L, selected.get("constant"));
        Assertions.assertEquals(6.0D, selected.get("doubled"));

        // primitive map overload
        final Map<String, Object> selected2 = select.select(createValues("b", 4L), timestamp);
        Assertions.assertEquals("b", selected2.get("renamed"));
        Assertions.assertEquals(8.0D, selected2.get("doubled"));

        // static apply
        final Map<String, Object> applied = Select.apply(
                select.getSelectFunctions(), createValues("c", 5L), timestamp);
        Assertions.assertEquals("c", applied.get("renamed"));
        Assertions.assertEquals(10.0D, applied.get("doubled"));

        // toJson keeps the original select definition
        Assertions.assertTrue(select.toJson().isJsonArray());
        Assertions.assertEquals(4, select.toJson().getAsJsonArray().size());
    }

    @Test
    public void testEmptySelect() {
        final Select select = Select.of(new JsonArray(), createFields());
        Assertions.assertFalse(select.useSelect());
        select.setup();

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final MElement element = MElement.of(createValues("a", 1L), timestamp);
        // pass through
        final Map<String, Object> selected = select.select(element, timestamp);
        Assertions.assertEquals("a", selected.get("stringField"));

        final Map<String, Object> values = createValues("b", 2L);
        Assertions.assertEquals(values, select.select(values, timestamp));
    }

    @Test
    public void testStatefulSelect() {
        final Select select = createSelect();
        select.setup();

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final MElement element1 = MElement.of(createValues("a", 1L), Instant.parse("2025-01-01T00:00:01Z"));
        final MElement element2 = MElement.of(createValues("b", 2L), Instant.parse("2025-01-01T00:00:02Z"));

        // with stateless functions the latest element is selected
        final Map<String, Object> selected = select.select(List.of(element1, element2), timestamp);
        Assertions.assertEquals("b", selected.get("renamed"));
        Assertions.assertEquals(4.0D, selected.get("doubled"));

        // empty buffer
        Assertions.assertTrue(select.select(List.of(), timestamp).isEmpty());
    }

    @Test
    public void testGetInputFieldNames() {
        final Select select = createSelect();
        final List<String> inputFieldNames = Select.getInputFieldNames(select.getSelectFunctions());
        Assertions.assertTrue(inputFieldNames.contains("stringField"));
        Assertions.assertTrue(inputFieldNames.contains("longField"));
        // no duplicates
        Assertions.assertEquals(inputFieldNames.size(), inputFieldNames.stream().distinct().count());
    }

    @Test
    public void testCreateStateAvroSchema() {
        final Select select = createSelect();
        final org.apache.avro.Schema stateSchema = Select
                .createStateAvroSchema(Schema.of(createFields()), select.getSelectFunctions());
        Assertions.assertNotNull(stateSchema.getField("stringField"));
        Assertions.assertNotNull(stateSchema.getField("longField"));
    }

}
