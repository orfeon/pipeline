package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.pipeline.Query2;
import com.mercari.solution.util.pipeline.lookup.source.BufferLookupSource;
import com.mercari.solution.util.pipeline.lookup.source.LookupSourceConfig;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryTransformBufferTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    // static: anonymous TupleTags declared inside a test method would capture
    // (and try to serialize) the non-serializable test class instance
    private static final TupleTag<MElement> OUTPUT_TAG = new TupleTag<>() {};
    private static final TupleTag<BadRecord> FAILURE_TAG = new TupleTag<>() {};

    /**
     * Batch: {@code @RequiresTimeSortedInput} makes per-key history cumulative
     * in event-time order; with includeCurrent (default) each element sees its
     * strict past plus itself.
     */
    @Test
    public void testBatchCumulativeHistory() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "amount": 10, "eventTime": "2025-05-01T00:00:01Z" },
                          { "userId": "u2", "amount": 5,  "eventTime": "2025-05-01T00:00:02Z" },
                          { "userId": "u1", "amount": 20, "eventTime": "2025-05-01T00:00:03Z" },
                          { "userId": "u2", "amount": 7,  "eventTime": "2025-05-01T00:00:04Z" },
                          { "userId": "u1", "amount": 30, "eventTime": "2025-05-01T00:00:05Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId, i.amount AS amount, s.cnt AS cnt, s.total AS total FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt, SUM(b.amount) AS total FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);
        Assertions.assertNotNull(output.getSchema().getField("cnt"));

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<Long, MElement> byAmount = new HashMap<>();
            for(final MElement element : elements) {
                byAmount.put(element.getAsLong("amount"), element);
            }
            Assertions.assertEquals(5, byAmount.size());
            // u1: 10 → 20 → 30, cumulative including the current element
            Assertions.assertEquals(1L, byAmount.get(10L).getAsLong("cnt"));
            Assertions.assertEquals(10L, byAmount.get(10L).getAsLong("total"));
            Assertions.assertEquals(2L, byAmount.get(20L).getAsLong("cnt"));
            Assertions.assertEquals(30L, byAmount.get(20L).getAsLong("total"));
            Assertions.assertEquals(3L, byAmount.get(30L).getAsLong("cnt"));
            Assertions.assertEquals(60L, byAmount.get(30L).getAsLong("total"));
            // u2 is an independent group
            Assertions.assertEquals(1L, byAmount.get(5L).getAsLong("cnt"));
            Assertions.assertEquals(2L, byAmount.get(7L).getAsLong("cnt"));
            Assertions.assertEquals(12L, byAmount.get(7L).getAsLong("total"));
            return null;
        });

        pipeline.run();
    }

    /**
     * includeCurrent=false exposes the strict past only; maxCount bounds both
     * the retained state and what an evaluation sees.
     */
    @Test
    public void testIncludeCurrentFalseAndMaxCount() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "amount": 1, "eventTime": "2025-05-01T00:00:01Z" },
                          { "userId": "u1", "amount": 2, "eventTime": "2025-05-01T00:00:02Z" },
                          { "userId": "u1", "amount": 3, "eventTime": "2025-05-01T00:00:03Z" },
                          { "userId": "u1", "amount": 4, "eventTime": "2025-05-01T00:00:04Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.amount AS amount, s.cnt AS cnt, s.total AS total FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt, SUM(b.amount) AS total FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "includeCurrent": false,
                            "maxCount": 2,
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<Long, MElement> byAmount = new HashMap<>();
            for(final MElement element : elements) {
                byAmount.put(element.getAsLong("amount"), element);
            }
            Assertions.assertEquals(4, byAmount.size());
            // strict past, trimmed to the newest 2 rows
            Assertions.assertEquals(0L, byAmount.get(1L).getAsLong("cnt"));
            Assertions.assertNull(byAmount.get(1L).getPrimitiveValue("total"));
            Assertions.assertEquals(1L, byAmount.get(2L).getAsLong("cnt"));
            Assertions.assertEquals(1L, byAmount.get(2L).getAsLong("total"));
            Assertions.assertEquals(2L, byAmount.get(3L).getAsLong("cnt"));
            Assertions.assertEquals(3L, byAmount.get(3L).getAsLong("total"));
            Assertions.assertEquals(2L, byAmount.get(4L).getAsLong("cnt"));
            Assertions.assertEquals(5L, byAmount.get(4L).getAsLong("total"));
            return null;
        });

        pipeline.run();
    }

    /** bufferFilter persists views only; triggerFilter evaluates purchases only. */
    @Test
    public void testBufferAndTriggerFilters() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "action": "view",     "amount": 1,   "eventTime": "2025-05-01T00:00:01Z" },
                          { "userId": "u1", "action": "view",     "amount": 2,   "eventTime": "2025-05-01T00:00:02Z" },
                          { "userId": "u1", "action": "purchase", "amount": 100, "eventTime": "2025-05-01T00:00:03Z" },
                          { "userId": "u1", "action": "view",     "amount": 3,   "eventTime": "2025-05-01T00:00:04Z" },
                          { "userId": "u1", "action": "purchase", "amount": 200, "eventTime": "2025-05-01T00:00:05Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "action", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.amount AS amount, s.viewCnt AS viewCnt FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS viewCnt FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "includeCurrent": false,
                            "bufferFilter": [
                              { "key": "action", "op": "=", "value": "view" }
                            ],
                            "triggerFilter": [
                              { "key": "action", "op": "=", "value": "purchase" }
                            ],
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<Long, MElement> byAmount = new HashMap<>();
            for(final MElement element : elements) {
                byAmount.put(element.getAsLong("amount"), element);
            }
            // only the two purchases produce output
            Assertions.assertEquals(2, byAmount.size());
            // the first purchase sees the two earlier views (purchases are not buffered)
            Assertions.assertEquals(2L, byAmount.get(100L).getAsLong("viewCnt"));
            // the second sees three views
            Assertions.assertEquals(3L, byAmount.get(200L).getAsLong("viewCnt"));
            return null;
        });

        pipeline.run();
    }

    /**
     * The module-level filter drops elements entirely — neither buffered nor
     * evaluated — before any state access.
     */
    @Test
    public void testCommonFilterSkipsBufferAndEvaluation() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "action": "view",   "amount": 1, "eventTime": "2025-05-01T00:00:01Z" },
                          { "userId": "u1", "action": "ignore", "amount": 2, "eventTime": "2025-05-01T00:00:02Z" },
                          { "userId": "u1", "action": "view",   "amount": 3, "eventTime": "2025-05-01T00:00:03Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "action", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "filter": [
                          { "key": "action", "op": "!=", "value": "ignore" }
                        ],
                        "sql": "SELECT i.amount AS amount, s.cnt AS cnt FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<Long, Long> countByAmount = new HashMap<>();
            for(final MElement element : elements) {
                countByAmount.put(element.getAsLong("amount"), element.getAsLong("cnt"));
            }
            // the "ignore" element neither evaluated nor buffered
            Assertions.assertEquals(2, countByAmount.size());
            Assertions.assertEquals(1L, countByAmount.get(1L));
            Assertions.assertEquals(2L, countByAmount.get(3L));
            return null;
        });

        pipeline.run();
    }

    /** Multiple inputs share one buffer; __input tells them apart. */
    @Test
    public void testMultipleInputsWithInputColumn() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "views",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "eventTime": "2025-05-01T00:00:01Z" },
                          { "userId": "u1", "eventTime": "2025-05-01T00:00:02Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      }
                    },
                    {
                      "name": "purchases",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "amount": 100, "eventTime": "2025-05-01T00:00:03Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["views", "purchases"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId, i.amount AS amount, s.viewCnt AS viewCnt FROM INPUT AS i JOIN LATERAL (SELECT SUM(CASE WHEN b.__input = 'views' THEN 1 ELSE 0 END) AS viewCnt FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            MElement purchase = null;
            int count = 0;
            for(final MElement element : elements) {
                count++;
                if(element.getPrimitiveValue("amount") != null) {
                    purchase = element;
                }
            }
            Assertions.assertEquals(3, count);
            Assertions.assertNotNull(purchase);
            // the purchase (latest event) sees both earlier views in the buffer
            Assertions.assertEquals(2L, purchase.getAsLong("viewCnt"));
            return null;
        });

        pipeline.run();
    }

    /**
     * Streaming: elements accumulate in arrival order, and the event-time TTL
     * timer clears an idle group's state. Drives the streaming DoFn directly
     * with a TestStream (TestStream is not expressible as a config source).
     */
    @Test
    public void testStreamingBufferWithStateTtl() {
        final Schema inputSchema = Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("amount", Schema.FieldType.INT64)));
        final BufferLookupSource bufferSource = BufferLookupSource.builder()
                .withName("buf")
                .withTable("history")
                .withGroupFields(List.of("userId"))
                .withRowSchema(inputSchema)
                .build();
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema)
                .withSource(bufferSource)
                .withSql("SELECT i.amount AS amount, s.cnt AS cnt FROM INPUT AS i"
                        + " JOIN LATERAL (SELECT COUNT(*) AS cnt FROM buf.history AS b"
                        + " WHERE b.userId = i.userId) AS s ON TRUE")
                .build();

        final LookupSourceConfig.BufferConfig bufferConfig = new LookupSourceConfig.BufferConfig(
                "buf", "history", List.of("userId"), null,
                null, null, true, 5L, null, null, null); // stateTtlSeconds = 5
        final QueryTransform.BufferQueryProcessor processor = new QueryTransform.BufferQueryProcessor(
                query, "INPUT", null, bufferConfig, List.of("userId", "amount"), List.of("events"),
                inputSchema, Map.of("", OUTPUT_TAG), Map.of(), List.of(), false, FAILURE_TAG);

        final List<Schema.Field> stateFields = new ArrayList<>(inputSchema.getFields());
        stateFields.add(Schema.Field.of(BufferLookupSource.TIMESTAMP_FIELD, Schema.FieldType.TIMESTAMP));
        stateFields.add(Schema.Field.of(BufferLookupSource.INPUT_FIELD, Schema.FieldType.STRING.withNullable(true)));
        final ElementCoder stateCoder = ElementCoder.of(Schema.of(stateFields));

        final Instant t1 = Instant.parse("2025-05-01T10:00:00Z");
        final Instant t2 = Instant.parse("2025-05-01T10:00:01Z");
        final Instant t3 = Instant.parse("2025-05-01T10:00:20Z");
        final TestStream<KV<String, MElement>> stream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(inputSchema)))
                .addElements(TimestampedValue.of(KV.of("u1", element("u1", 1L, t1)), t1))
                .addElements(TimestampedValue.of(KV.of("u1", element("u1", 2L, t2)), t2))
                // the TTL timer (t2 + 5s) fires before the next element arrives
                .advanceWatermarkTo(Instant.parse("2025-05-01T10:00:10Z"))
                .addElements(TimestampedValue.of(KV.of("u1", element("u1", 3L, t3)), t3))
                .advanceWatermarkToInfinity();

        final PCollectionTuple outputs = pipeline
                .apply("TestStream", stream)
                .apply("Query", ParDo
                        .of(new QueryTransform.StreamingBufferQueryDoFn(processor, stateCoder))
                        .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));
        final PCollection<MElement> output = outputs.get(OUTPUT_TAG)
                .setCoder(ElementCoder.of(query.getOutputSchema()));

        PAssert.that(output).satisfies(elements -> {
            final Map<Long, Long> countByAmount = new HashMap<>();
            for(final MElement element : elements) {
                countByAmount.put(element.getAsLong("amount"), element.getAsLong("cnt"));
            }
            Assertions.assertEquals(3, countByAmount.size());
            Assertions.assertEquals(1L, countByAmount.get(1L));
            Assertions.assertEquals(2L, countByAmount.get(2L));
            // the TTL cleared the buffer while the group was idle
            Assertions.assertEquals(1L, countByAmount.get(3L));
            return null;
        });

        pipeline.run();
    }

    /** Explicit fields must cover every buffer column the SQL references. */
    @Test
    public void testFieldsMissingReferencedColumnRejected() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "amount": 1, "category": "a" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "category", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId, s.total AS total FROM INPUT AS i JOIN LATERAL (SELECT SUM(b.amount) AS total FROM buf.history AS b WHERE b.userId = i.userId AND b.category = 'a') AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "tables": [
                              { "name": "history", "fields": ["amount"] }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final IllegalModuleException thrown = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(thrown.getMessage().contains("category"),
                "unexpected error: " + thrown.getMessage());
    }

    /** Correlating the buffer key to a different input column fails at construction. */
    @Test
    public void testWrongKeyCorrelationRejected() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "otherId": "x1", "amount": 1 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "otherId", "type": "string" },
                          { "name": "amount", "type": "int64" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId, s.cnt AS cnt FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt FROM buf.history AS b WHERE b.userId = i.otherId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

    /** At most one buffer source per transform. */
    @Test
    public void testMultipleBufferSourcesRejected() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "amount": 1 }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "amount", "type": "int64" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId FROM INPUT AS i",
                        "sources": [
                          {
                            "name": "buf1",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "tables": [ { "name": "history1" } ]
                          },
                          {
                            "name": "buf2",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "tables": [ { "name": "history2" } ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

    /**
     * insertSql defines the buffered rows by SQL: the WHERE selects what
     * persists (bufferFilter equivalent) and the select list transforms it.
     */
    @Test
    public void testInsertSqlSelectivePersistAndProjection() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "action": "view",     "amount": 1,   "eventTime": "2025-05-01T00:00:01Z" },
                          { "userId": "u1", "action": "purchase", "amount": 100, "eventTime": "2025-05-01T00:00:02Z" },
                          { "userId": "u1", "action": "view",     "amount": 3,   "eventTime": "2025-05-01T00:00:03Z" },
                          { "userId": "u1", "action": "purchase", "amount": 200, "eventTime": "2025-05-01T00:00:04Z" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "action", "type": "string" },
                          { "name": "amount", "type": "int64" },
                          { "name": "eventTime", "type": "timestamp" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.amount AS amount, s.total AS total FROM INPUT AS i JOIN LATERAL (SELECT SUM(b.doubled) AS total FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "includeCurrent": false,
                            "insertSql": "SELECT userId, amount * 2 AS doubled FROM INPUT WHERE action = 'view'",
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<Long, MElement> byAmount = new HashMap<>();
            for(final MElement element : elements) {
                byAmount.put(element.getAsLong("amount"), element);
            }
            Assertions.assertEquals(4, byAmount.size());
            // only views persist, transformed to amount*2; strict past visibility
            Assertions.assertNull(byAmount.get(1L).getPrimitiveValue("total"));
            Assertions.assertEquals(2L, byAmount.get(100L).getAsLong("total"));
            Assertions.assertEquals(2L, byAmount.get(3L).getAsLong("total"));
            Assertions.assertEquals(8L, byAmount.get(200L).getAsLong("total"));
            return null;
        });

        pipeline.run();
    }

    /** insertSql may fan out: one element persists 0..N buffer rows (UNNEST). */
    @Test
    public void testInsertSqlFanOutPersist() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "carts",
                      "module": "create",
                      "timestampAttribute": "eventTime",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "userId": "u1", "eventTime": "2025-05-01T00:00:01Z",
                            "items": [ { "category": "a", "qty": 1 }, { "category": "b", "qty": 2 } ] },
                          { "userId": "u1", "eventTime": "2025-05-01T00:00:02Z",
                            "items": [ { "category": "c", "qty": 3 } ] }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "eventTime", "type": "timestamp" },
                          { "name": "items", "type": "element", "mode": "repeated", "fields": [
                            { "name": "category", "type": "string" },
                            { "name": "qty", "type": "int64" }
                          ]}
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["carts"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId, s.cnt AS cnt FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "insertSql": "SELECT i.userId AS userId, e.category AS category FROM INPUT AS i, UNNEST(i.items) AS e",
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<Long> counts = new java.util.HashSet<>();
            for(final MElement element : elements) {
                counts.add(element.getAsLong("cnt"));
            }
            // first cart: its own 2 items visible (includeCurrent default);
            // second cart: 2 past + 1 current = 3
            Assertions.assertEquals(Set.of(2L, 3L), counts);
            return null;
        });

        pipeline.run();
    }

    /** The insert query cannot read the buffer itself (planned without it). */
    @Test
    public void testInsertSqlReferencingBufferRejected() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "userId": "u1", "amount": 1 } ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "userId", "type": "string" },
                          { "name": "amount", "type": "int64" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId FROM INPUT AS i",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            "insertSql": "SELECT b.userId AS userId FROM buf.history AS b",
                            "tables": [
                              { "name": "history" }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final IllegalModuleException thrown = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(thrown.getMessage().contains("insertSql"),
                "unexpected error: " + thrown.getMessage());
    }

    private static MElement element(String userId, long amount, Instant timestamp) {
        final Map<String, Object> values = new HashMap<>();
        values.put("userId", userId);
        values.put("amount", amount);
        return MElement.of(values, timestamp);
    }
}
