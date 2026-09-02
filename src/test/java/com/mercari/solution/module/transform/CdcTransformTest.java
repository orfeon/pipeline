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

import com.mercari.solution.util.pipeline.cdc.ChangeSchema;

import java.util.HashMap;
import java.util.List;
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
            // the DDL event becomes a SCHEMA control record (null keys)
            Assertions.assertEquals(3, count);
            final MElement schemaChange = byKeys.get(null);
            Assertions.assertNotNull(schemaChange);
            Assertions.assertEquals(ChangeRecord.Op.SCHEMA, ChangeRecord.getOp(schemaChange));
            Assertions.assertEquals("ALTER TABLE users ADD c INT", schemaChange.getAsString(ChangeRecord.FIELD_STATEMENT));

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


    private static String config(final boolean accumulate, final String... payloads) {
        final StringBuilder elements = new StringBuilder();
        for(final String payload : payloads) {
            if(!elements.isEmpty()) {
                elements.append(",");
            }
            elements.append("{ \"payload\": ").append(GSON.toJson(payload)).append(" }");
        }
        return """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ %s ]
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
                        "format": "ticdc"%s%s
                      }
                    }
                  ]
                }
                """.formatted(elements, accumulate ? ", \"accumulate\": true" : "", EXTRA_PARAMETERS.get());
    }

    // extra transform parameters (json fragment starting with a comma) for the next config() call
    private static final ThreadLocal<String> EXTRA_PARAMETERS = ThreadLocal.withInitial(() -> "");


    @Test
    public void testSchemaDriftSynthesizesSchemaRecord() throws Exception {
        // two events of the same table in one newline-delimited payload (TiCDC storage sink file):
        // processed by one DoFn call, so the worker-local schema cache sees both in order
        final String v1 = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(64)\"},\"data\":[{\"id\":\"1\",\"name\":\"alice\"}]}";
        final String v2 = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190500000,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(64)\",\"age\":\"int(11)\"},\"data\":[{\"id\":\"2\",\"name\":\"bob\",\"age\":\"20\"}]}";
        final Config config = Config.load(config(false, v1 + "\n" + v2));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("cdc");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int rows = 0;
            MElement schemaChange = null;
            for(final MElement element : elements) {
                if(ChangeRecord.Op.SCHEMA.equals(ChangeRecord.getOp(element))) {
                    schemaChange = element;
                } else {
                    rows++;
                    Assertions.assertNull(element.asPrimitiveMap().get(ChangeRecord.FIELD_SCHEMA));
                }
            }
            Assertions.assertEquals(2, rows);
            // the first observation never reports; the drift on the second event does
            Assertions.assertNotNull(schemaChange);
            Assertions.assertEquals("users", schemaChange.getAsString(ChangeRecord.FIELD_TABLE));
            Assertions.assertNull(schemaChange.getAsString(ChangeRecord.FIELD_STATEMENT));
            final List<ChangeSchema.Column> columns = ChangeSchema.fromJson(schemaChange.getAsString(ChangeRecord.FIELD_SCHEMA));
            Assertions.assertEquals(3, columns.size());
            Assertions.assertEquals(new ChangeSchema.Column("age", ChangeSchema.TYPE_INT64, false), columns.get(2));
            // ordered before the rows of the second event
            Assertions.assertTrue(ChangeRecord.compareSequence(
                    schemaChange.getAsString(ChangeRecord.FIELD_SEQUENCE), ChangeRecord.sequence(1723190500000000L, 0L)) < 0);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testPrimaryKeyChangeIsSplit() throws Exception {
        final String update = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"UPDATE\",\"es\":1723190400000,\"data\":[{\"id\":\"2\",\"name\":\"alice\"}],\"old\":[{\"id\":\"1\"}]}";
        final Config config = Config.load(config(false, update));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("cdc");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, MElement> byKeys = new HashMap<>();
            for(final MElement element : elements) {
                byKeys.put(element.getAsString(ChangeRecord.FIELD_KEYS), element);
            }
            Assertions.assertEquals(2, byKeys.size());
            final MElement delete = byKeys.get("{\"id\":\"1\"}");
            Assertions.assertEquals(ChangeRecord.Op.DELETE, ChangeRecord.getOp(delete));
            Assertions.assertNull(delete.asPrimitiveMap().get(ChangeRecord.FIELD_AFTER));
            final MElement insert = byKeys.get("{\"id\":\"2\"}");
            Assertions.assertEquals(ChangeRecord.Op.INSERT, ChangeRecord.getOp(insert));
            Assertions.assertEquals("{\"id\":\"2\",\"name\":\"alice\"}", insert.getAsString(ChangeRecord.FIELD_AFTER));
            Assertions.assertTrue(ChangeRecord.compareSequence(
                    delete.getAsString(ChangeRecord.FIELD_SEQUENCE), insert.getAsString(ChangeRecord.FIELD_SEQUENCE)) < 0);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testAccumulateTruncateBarrier() throws Exception {
        final String before = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"data\":[{\"id\":\"1\",\"name\":\"alice\"}]}";
        final String truncate = "{\"database\":\"d\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"es\":1723190500000,\"sql\":\"TRUNCATE TABLE users\"}";
        final String after = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190600000,\"data\":[{\"id\":\"2\",\"name\":\"bob\"}]}";
        final String otherTable = "{\"database\":\"d\",\"table\":\"orders\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"data\":[{\"id\":\"9\"}]}";
        final Config config = Config.load(config(true, before, truncate, after, otherTable));
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("cdc");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, MElement> rows = new HashMap<>();
            int controls = 0;
            for(final MElement element : elements) {
                if(ChangeRecord.getOp(element).isControl()) {
                    controls++;
                    Assertions.assertEquals(ChangeRecord.Op.TRUNCATE, ChangeRecord.getOp(element));
                } else {
                    rows.put(element.getAsString(ChangeRecord.FIELD_TABLE) + "#" + element.getAsString(ChangeRecord.FIELD_KEYS), element);
                }
            }
            Assertions.assertEquals(1, controls);
            // the row sequenced before the TRUNCATE of its table is dropped, other tables are untouched
            Assertions.assertEquals(2, rows.size());
            Assertions.assertNull(rows.get("users#{\"id\":\"1\"}"));
            Assertions.assertNotNull(rows.get("users#{\"id\":\"2\"}"));
            Assertions.assertNotNull(rows.get("orders#{\"id\":\"9\"}"));
            return null;
        });
        pipeline.run();
    }


    @Test
    public void testSchemaChangesGenerateDestinationDdl() throws Exception {
        final String v1 = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(64)\"},\"data\":[{\"id\":\"1\",\"name\":\"alice\"}]}";
        final String ddl = "{\"database\":\"d\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"es\":1723190450000,\"sql\":\"ALTER TABLE users ADD COLUMN age INT\"}";
        final String v2 = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190500000,\"mysqlType\":{\"id\":\"int(11)\",\"name\":\"varchar(64)\",\"age\":\"int(11)\"},\"data\":[{\"id\":\"2\",\"name\":\"bob\",\"age\":\"20\"}]}";
        final String truncate = "{\"database\":\"d\",\"table\":\"users\",\"isDdl\":true,\"type\":\"QUERY\",\"es\":1723190600000,\"sql\":\"TRUNCATE TABLE users\"}";
        EXTRA_PARAMETERS.set(", \"schemaChanges\": { \"dialect\": \"bigquery\", \"table\": \"p.d.${table}\", \"baseline\": \"none\" }");
        final String configJson = config(false, v1 + "\n" + ddl + "\n" + v2 + "\n" + truncate);
        EXTRA_PARAMETERS.set("");
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("cdc");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            MElement providerDdl = null;
            MElement synthesized = null;
            MElement truncated = null;
            int rows = 0;
            for(final MElement element : elements) {
                switch (ChangeRecord.getOp(element)) {
                    case SCHEMA -> {
                        if(element.getAsString(ChangeRecord.FIELD_SCHEMA) == null) {
                            providerDdl = element;
                        } else {
                            synthesized = element;
                        }
                    }
                    case TRUNCATE -> truncated = element;
                    default -> rows++;
                }
            }
            Assertions.assertEquals(2, rows);
            // provider DDL text moves to source.metadata.ddl; statement only carries the dialect
            Assertions.assertNotNull(providerDdl);
            Assertions.assertNull(providerDdl.getAsString(ChangeRecord.FIELD_STATEMENT));
            Assertions.assertTrue(providerDdl.getAsString(ChangeRecord.FIELD_SOURCE + "." + ChangeRecord.FIELD_SOURCE_METADATA).contains("ALTER TABLE users ADD COLUMN age INT"));
            // the synthesized SCHEMA carries the additive destination DDL
            Assertions.assertNotNull(synthesized);
            Assertions.assertEquals("ALTER TABLE `p.d.users` ADD COLUMN IF NOT EXISTS `age` INT64;", synthesized.getAsString(ChangeRecord.FIELD_STATEMENT));
            Assertions.assertNotNull(truncated);
            Assertions.assertEquals("TRUNCATE TABLE `p.d.users`;", truncated.getAsString(ChangeRecord.FIELD_STATEMENT));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEnvelopeFormatFromFailureRecordJson() throws Exception {
        // bigquery sink cdc failure record: the envelope json sits in record.json
        final String envelopeJson = "{\"table\":\"users\",\"op\":\"UPDATE\",\"keys\":\"{\\\"id\\\":1}\",\"after\":\"{\\\"id\\\":1,\\\"name\\\":\\\"a\\\"}\",\"commitTimestamp\":1723190400000000,\"sequence\":\"ff/1\",\"source\":{\"provider\":\"postgres\"}}";
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "failures",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "module": "bq", "record": { "json": %s } }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "module", "type": "string" },
                          { "name": "record", "type": "element", "fields": [ { "name": "json", "type": "string" } ] }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "cdc",
                      "module": "cdc",
                      "inputs": ["failures"],
                      "parameters": {
                        "format": "envelope",
                        "field": "record.json",
                        "accumulate": true
                      }
                    }
                  ]
                }
                """.formatted(GSON.toJson(envelopeJson));
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("cdc");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals("users", element.getAsString(ChangeRecord.FIELD_TABLE));
                Assertions.assertEquals(ChangeRecord.Op.UPDATE, ChangeRecord.getOp(element));
                Assertions.assertEquals("{\"id\":1}", element.getAsString(ChangeRecord.FIELD_KEYS));
                Assertions.assertEquals("ff/1", element.getAsString(ChangeRecord.FIELD_SEQUENCE));
                Assertions.assertEquals("postgres", element.getAsString(ChangeRecord.FIELD_SOURCE + "." + ChangeRecord.FIELD_SOURCE_PROVIDER));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEnvelopeFormatPassThrough() throws Exception {
        // envelope records (e.g. read back from an archive) normalized twice must be stable
        final String event = "{\"database\":\"d\",\"table\":\"users\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1723190400000,\"data\":[{\"id\":\"1\",\"name\":\"alice\"}]}";
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "payload": %s } ]
                      },
                      "schema": {
                        "fields": [ { "name": "payload", "type": "string" } ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "first",
                      "module": "cdc",
                      "inputs": ["events"],
                      "parameters": { "format": "ticdc" }
                    },
                    {
                      "name": "cdc",
                      "module": "cdc",
                      "inputs": ["first"],
                      "parameters": { "format": "envelope" }
                    }
                  ]
                }
                """.formatted(GSON.toJson(event));
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("cdc");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals(ChangeRecord.Op.INSERT, ChangeRecord.getOp(element));
                Assertions.assertEquals("{\"id\":\"1\"}", element.getAsString(ChangeRecord.FIELD_KEYS));
                Assertions.assertEquals("ticdc", element.getAsString(ChangeRecord.FIELD_SOURCE + "." + ChangeRecord.FIELD_SOURCE_PROVIDER));
            }
            Assertions.assertEquals(1, count);
            return null;
        });
        pipeline.run();
    }

}
