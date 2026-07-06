package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class CreateSourceTest {

    private static final double DELTA = 1e-15;

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testDestinationReferenceRejectedOnAnySource() throws Exception {
        // schema-redesign.md Phase 4: the Source base rejects destination references
        // uniformly — a destination points at a write target and can never define a source
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0],
                        "schema": { "reference": { "destination": true } }
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("not applicable to source modules"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testCreateRange() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "from": 1,
                        "to": 100,
                        "select": [
                          { "name": "a", "field": "value" },
                          { "name": "b", "expression": "value % 10", "type": "int64" },
                          { "name": "c", "func": "hash", "field": "b" }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("create");

        // Assert output schema
        final Schema outputSchema = output.getSchema();
        for(final Schema.Field field : outputSchema.getFields()) {
            switch (field.getName()) {
                case "a", "b" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.int64);
                case "c" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.string);
            }
        }

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                final Long a = row.getAsLong("a");
                final Long b = row.getAsLong("b");
                Assertions.assertEquals(a % 10, b, DELTA);
                count++;
            }
            Assertions.assertEquals(100, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testCreateElements() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "stringField": "a", "intField": 1, "floatField": 0.15, "boolField": true, "timestampField": "2024-10-10T00:00:00Z" },
                          { "stringField": "b", "intField": 2, "floatField": 1.15, "boolField": false, "timestampField": "2024-10-20T00:00:00Z" },
                          { "stringField": "c", "intField": 3, "floatField": 2.15, "boolField": true, "timestampField": "2024-10-30T00:00:00Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "stringField", "type": "string" },
                          { "name": "intField", "type": "int" },
                          { "name": "floatField", "type": "float" },
                          { "name": "boolField", "type": "boolean" },
                          { "name": "timestampField", "type": "timestamp" }
                        ]
                      },
                      "timestampAttribute": "timestampField"
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("create");

        // Assert output schema
        final Schema outputSchema = output.getSchema();
        //Assertions.assertEquals(12, outputSchema.countFields());
        for(final Schema.Field field : outputSchema.getFields()) {
            switch (field.getName()) {
                case "stringField" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.string);
                case "intField" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.int32);
                case "longField" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.int64);
                case "floatField" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.float32);
                case "doubleField" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.float64);
                case "boolField" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.bool);
                case "timestampField" -> Assertions.assertEquals(field.getFieldType().getType(), Schema.Type.timestamp);
            }
        }

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                switch (row.getPrimitiveValue("stringField").toString()) {
                    case "a" -> {
                        Assertions.assertEquals(true, row.getPrimitiveValue("boolField"));
                        Assertions.assertEquals(1, row.getPrimitiveValue("intField"));
                        Assertions.assertEquals(0.15F, row.getPrimitiveValue("floatField"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-10T00:00:00.000Z"), row.getPrimitiveValue("timestampField"));
                    }
                    case "b" -> {
                        Assertions.assertEquals(false, row.getPrimitiveValue("boolField"));
                        Assertions.assertEquals(2, row.getPrimitiveValue("intField"));
                        Assertions.assertEquals(1.15F, row.getPrimitiveValue("floatField"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-20T00:00:00.000Z"), row.getPrimitiveValue("timestampField"));
                    }
                    case "c" -> {
                        Assertions.assertEquals(true, row.getPrimitiveValue("boolField"));
                        Assertions.assertEquals(3, row.getPrimitiveValue("intField"));
                        Assertions.assertEquals(2.15F, row.getPrimitiveValue("floatField"));
                        Assertions.assertEquals(DateTimeUtil.toEpochMicroSecond("2024-10-30T00:00:00.000Z"), row.getPrimitiveValue("timestampField"));
                    }
                }
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

}
