package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.cdc.ChangeRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class CdcTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final Gson GSON = new Gson();

    @Test
    public void testTiCdcNormalize() throws Exception {

        final String insertEvent = "{\"database\":\"testdb\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"data\":[{\"id\":\"1\",\"name\":\"alice\"}]}";
        final String deleteEvent = "{\"database\":\"testdb\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"DELETE\",\"es\":1723190500000,\"data\":[{\"id\":\"2\",\"name\":\"bob\"}]}";
        final String ddlEvent = "{\"database\":\"testdb\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"sql\":\"ALTER TABLE users ADD c INT\"}";

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "payload": %s },
                          { "payload": %s },
                          { "payload": %s }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "payload", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "cdc",
                      "module": "cdc",
                      "inputs": ["events"],
                      "parameters": {
                        "format": "ticdc"
                      }
                    }
                  ]
                }
                """.formatted(GSON.toJson(insertEvent), GSON.toJson(deleteEvent), GSON.toJson(ddlEvent));

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("cdc");
        Assertions.assertNotNull(output);

        final Schema outputSchema = output.getSchema();
        Assertions.assertTrue(outputSchema.hasField(ChangeRecord.FIELD_TABLE));
        Assertions.assertTrue(outputSchema.hasField(ChangeRecord.FIELD_OP));
        Assertions.assertTrue(outputSchema.hasField(ChangeRecord.FIELD_KEYS));
        Assertions.assertTrue(outputSchema.hasField(ChangeRecord.FIELD_SEQUENCE));

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, MElement> byKeys = new HashMap<>();
            int count = 0;
            for(final MElement element : elements) {
                byKeys.put(element.getAsString(ChangeRecord.FIELD_KEYS), element);
                count++;
            }
            // the DDL event must be skipped
            Assertions.assertEquals(2, count);

            final MElement inserted = byKeys.get("{\"id\":\"1\"}");
            Assertions.assertNotNull(inserted);
            Assertions.assertEquals("users", inserted.getAsString(ChangeRecord.FIELD_TABLE));
            Assertions.assertEquals(ChangeRecord.Op.INSERT, ChangeRecord.getOp(inserted));

            final MElement deleted = byKeys.get("{\"id\":\"2\"}");
            Assertions.assertNotNull(deleted);
            Assertions.assertEquals(ChangeRecord.Op.DELETE, ChangeRecord.getOp(deleted));
            Assertions.assertNull(deleted.asPrimitiveMap().get(ChangeRecord.FIELD_AFTER));
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testTiCdcAccumulate() throws Exception {

        // three changes on the same key: INSERT -> UPDATE -> UPDATE; accumulate must keep only the latest
        final String event1 = "{\"database\":\"testdb\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"data\":[{\"id\":\"1\",\"name\":\"alice\"}]}";
        final String event2 = "{\"database\":\"testdb\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"UPDATE\",\"es\":1723190500000,\"data\":[{\"id\":\"1\",\"name\":\"bob\"}],\"old\":[{\"name\":\"alice\"}]}";
        final String event3 = "{\"database\":\"testdb\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"UPDATE\",\"es\":1723190600000,\"data\":[{\"id\":\"1\",\"name\":\"carol\"}],\"old\":[{\"name\":\"bob\"}]}";
        final String otherKey = "{\"database\":\"testdb\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"data\":[{\"id\":\"2\",\"name\":\"dave\"}]}";

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "payload": %s },
                          { "payload": %s },
                          { "payload": %s },
                          { "payload": %s }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "payload", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "cdc",
                      "module": "cdc",
                      "inputs": ["events"],
                      "parameters": {
                        "format": "ticdc",
                        "accumulate": true
                      }
                    }
                  ]
                }
                """.formatted(GSON.toJson(event1), GSON.toJson(event2), GSON.toJson(event3), GSON.toJson(otherKey));

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("cdc");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, MElement> byKeys = new HashMap<>();
            int count = 0;
            for(final MElement element : elements) {
                byKeys.put(element.getAsString(ChangeRecord.FIELD_KEYS), element);
                count++;
            }
            Assertions.assertEquals(2, count);
            final MElement latest = byKeys.get("{\"id\":\"1\"}");
            Assertions.assertNotNull(latest);
            Assertions.assertEquals("{\"id\":\"1\",\"name\":\"carol\"}", latest.getAsString(ChangeRecord.FIELD_AFTER));
            return null;
        });

        pipeline.run();
    }

}
