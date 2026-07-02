package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnnestTest {

    private static List<Schema.Field> createFields() {
        return List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("arrayField", Schema.FieldType.array(Schema.FieldType.INT64)));
    }

    @Test
    public void testOf() {
        final Unnest noop = Unnest.of(null);
        Assertions.assertFalse(noop.useUnnest());
        Assertions.assertNull(noop.getFlattenField());
        Assertions.assertNull(noop.getOutputField());

        final Unnest simple = Unnest.of("arrayField");
        simple.setup();
        Assertions.assertTrue(simple.useUnnest());
        Assertions.assertEquals("arrayField", simple.getFlattenField());
        Assertions.assertEquals("arrayField", simple.getOutputField());

        final Unnest nested = Unnest.of("parent.child");
        Assertions.assertEquals("parent.child", nested.getFlattenField());
        Assertions.assertEquals("child", nested.getOutputField());

        // trailing dot keeps the entire text as output field
        final Unnest trailing = Unnest.of("parent.");
        Assertions.assertEquals("parent.", trailing.getOutputField());
    }

    @Test
    public void testCreateSchema() {
        final Schema schema = Unnest.createSchema(createFields(), "arrayField");
        Assertions.assertEquals(Schema.Type.int64, schema.getField("arrayField").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.string, schema.getField("stringField").getFieldType().getType());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Unnest.createSchema(createFields(), "missingField"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> Unnest.createSchema(createFields(), "stringField"));
    }

    @Test
    public void testUnnest() {
        final Unnest unnest = Unnest.of("arrayField");

        final Map<String, Object> values = new HashMap<>();
        values.put("stringField", "a");
        values.put("arrayField", List.of(1L, 2L, 3L));

        final List<Map<String, Object>> outputs = unnest.unnest(values);
        Assertions.assertEquals(3, outputs.size());
        for(int i = 0; i < 3; i++) {
            Assertions.assertEquals("a", outputs.get(i).get("stringField"));
            Assertions.assertEquals(i + 1L, outputs.get(i).get("arrayField"));
        }

        // empty array produces single output with null value
        final Map<String, Object> emptyValues = new HashMap<>();
        emptyValues.put("stringField", "a");
        emptyValues.put("arrayField", new ArrayList<>());
        final List<Map<String, Object>> emptyOutputs = unnest.unnest(emptyValues);
        Assertions.assertEquals(1, emptyOutputs.size());
        Assertions.assertNull(emptyOutputs.get(0).get("arrayField"));

        // null input element
        Assertions.assertTrue(unnest.unnest((MElement) null).isEmpty());

        // MElement input
        final Map<String, Object> elementValues = new HashMap<>();
        elementValues.put("stringField", "b");
        elementValues.put("arrayField", List.of(5L));
        final MElement element = MElement.of(elementValues, Instant.parse("2025-01-01T00:00:00Z"));
        final List<Map<String, Object>> elementOutputs = unnest.unnest(element);
        Assertions.assertEquals(1, elementOutputs.size());
        Assertions.assertEquals(5L, elementOutputs.get(0).get("arrayField"));

        // no flatten field configured: pass through
        final Unnest noop = Unnest.of(null);
        final List<Map<String, Object>> noopOutputs = noop.unnest(values);
        Assertions.assertEquals(1, noopOutputs.size());
        Assertions.assertEquals(values, noopOutputs.get(0));
    }

}
