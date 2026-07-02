package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputTest {

    @Test
    public void testParameters() {
        final String parametersJson = """
                {
                  "name": "myInput",
                  "filter": [
                    { "key": "longField", "op": ">=", "value": 10 }
                  ]
                }
                """;
        final Input.Parameters parameters = new Gson().fromJson(parametersJson, Input.Parameters.class);
        Assertions.assertEquals("myInput", parameters.getName());
        Assertions.assertTrue(parameters.validate().isEmpty());
        parameters.setDefaults();

        final JsonObject json = parameters.toJson();
        Assertions.assertEquals("myInput", json.get("name").getAsString());
        Assertions.assertTrue(json.has("filter"));

        final Schema schema = Schema.of(List.of(
                Schema.Field.of("longField", Schema.FieldType.INT64)));
        final Input input = parameters.create(schema).setup(schema);
        Assertions.assertEquals("myInput", input.getName());

        final Instant timestamp = Instant.parse("2025-01-01T00:00:00Z");
        final Map<String, Object> matched = new HashMap<>();
        matched.put("longField", 10L);
        Assertions.assertEquals(1, input.process(matched, timestamp).size());

        final Map<String, Object> notMatched = new HashMap<>();
        notMatched.put("longField", 9L);
        Assertions.assertTrue(input.process(notMatched, timestamp).isEmpty());
    }

}
