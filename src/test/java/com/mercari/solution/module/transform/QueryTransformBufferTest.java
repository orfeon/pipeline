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
                null, null, true, 5L, null, null, null, null, null, null); // stateTtlSeconds = 5
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

    /**
     * TTL clears the restored flag too: an idle key re-restores on its next
     * element, and the engine's retention trim keeps rows older than
     * maxDuration out regardless of the restore SQL — cache semantics.
     */
    @Test
    public void testTtlReRestoreRespectsRetention() throws Exception {
        final String url = "jdbc:h2:mem:bufferttlrestoretest;DB_CLOSE_DELAY=-1";
        try (final java.sql.Connection connection =
                     java.sql.DriverManager.getConnection(url, "sa", "")) {
            try (final java.sql.Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS HISTORY (
                          USER_ID VARCHAR(16) NOT NULL,
                          AMOUNT BIGINT NOT NULL,
                          EVENT_TIME TIMESTAMP,
                          PRIMARY KEY (USER_ID, AMOUNT)
                        )""");
                statement.execute("DELETE FROM HISTORY");
            }
            // write via setTimestamp so the stored instant is timezone-independent
            // (a TIMESTAMP literal would be interpreted as local wall-clock)
            try (final java.sql.PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO HISTORY VALUES (?, ?, ?)")) {
                insert.setString(1, "u1");
                insert.setLong(2, 1L);
                insert.setTimestamp(3, java.sql.Timestamp.from(
                        java.time.Instant.parse("2025-05-01T10:00:00Z")));
                insert.executeUpdate();
            }

            final Schema inputSchema = Schema.of(List.of(
                    Schema.Field.of("userId", Schema.FieldType.STRING),
                    Schema.Field.of("amount", Schema.FieldType.INT64)));
            final BufferLookupSource bufferSource = BufferLookupSource.builder()
                    .withName("buf")
                    .withTable("history")
                    .withGroupFields(List.of("userId"))
                    .withRowSchema(inputSchema)
                    .build();
            final com.mercari.solution.util.pipeline.lookup.source.JdbcLookupSource jdbc =
                    com.mercari.solution.util.pipeline.lookup.source.JdbcLookupSource.builder()
                            .withName("db")
                            .withDriver("org.h2.Driver")
                            .withUrl(url)
                            .withUser("sa")
                            .withPassword("")
                            .withTable("HISTORY", "HISTORY")
                            .build();
            final Query2 query = Query2.builder()
                    .withInput("INPUT", inputSchema)
                    .withSource(bufferSource)
                    .withSource(jdbc)
                    .withStateRestore("SELECT h.USER_ID AS userId, h.AMOUNT AS amount,"
                            + " h.EVENT_TIME AS __timestamp FROM INPUT AS i"
                            + " JOIN db.HISTORY AS h ON h.USER_ID = i.userId")
                    .withSql("SELECT i.amount AS amount, s.cnt AS cnt FROM INPUT AS i"
                            + " JOIN LATERAL (SELECT COUNT(*) AS cnt FROM buf.history AS b"
                            + " WHERE b.userId = i.userId) AS s ON TRUE")
                    .build();

            // maxDuration 30s, TTL 10s, restore enabled, dedup by userId+time
            final LookupSourceConfig.BufferConfig bufferConfig = new LookupSourceConfig.BufferConfig(
                    "buf", "history", List.of("userId"), null,
                    null, 30L, true, 10L, null, null,
                    null, "restore", List.of("userId", "__timestamp"), null);
            final QueryTransform.BufferQueryProcessor processor = new QueryTransform.BufferQueryProcessor(
                    query, "INPUT", null, bufferConfig, List.of("userId", "amount"), List.of("events"),
                    inputSchema, Map.of("", OUTPUT_TAG), Map.of(), List.of(), true, FAILURE_TAG);

            final List<Schema.Field> stateFields = new ArrayList<>(inputSchema.getFields());
            stateFields.add(Schema.Field.of(BufferLookupSource.TIMESTAMP_FIELD, Schema.FieldType.TIMESTAMP));
            stateFields.add(Schema.Field.of(BufferLookupSource.INPUT_FIELD, Schema.FieldType.STRING.withNullable(true)));
            final ElementCoder stateCoder = ElementCoder.of(Schema.of(stateFields));

            final Instant t2 = Instant.parse("2025-05-01T10:00:05Z");
            final Instant t3 = Instant.parse("2025-05-01T10:00:40Z");
            final TestStream<KV<String, MElement>> stream = TestStream
                    .create(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(inputSchema)))
                    .addElements(TimestampedValue.of(KV.of("u1", element("u1", 2L, t2)), t2))
                    // TTL (t2 + 10s) fires: buffer + restored flag cleared
                    .advanceWatermarkTo(Instant.parse("2025-05-01T10:00:20Z"))
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
                Assertions.assertEquals(2, countByAmount.size(), "map=" + countByAmount);
                // first touch restores the external row (10:00:00, within 30s) → 2
                Assertions.assertEquals(2L, countByAmount.get(2L));
                // after TTL: re-restore, but the external row is now outside the
                // retention window relative to 10:00:40 → only the current element
                Assertions.assertEquals(1L, countByAmount.get(3L));
                return null;
            });

            pipeline.run();
        }
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

    /**
     * The restart-recovery loop: state is empty (fresh pipeline), the external
     * store holds the pre-restart history, restoreSql seeds each key's state on
     * first touch, and dedupFields absorbs the overlap between restored rows
     * and replayed input.
     */
    @Test
    public void testRestoreFromExternalWithReplayDedup() throws Exception {
        final String url = "jdbc:h2:mem:bufferrestoretest;DB_CLOSE_DELAY=-1";
        try (final java.sql.Connection connection =
                     java.sql.DriverManager.getConnection(url, "sa", "")) {
            try (final java.sql.Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS HISTORY (
                          USER_ID VARCHAR(16) NOT NULL,
                          EVENT_ID VARCHAR(16) NOT NULL,
                          EVENT_TIME TIMESTAMP,
                          PRIMARY KEY (USER_ID, EVENT_ID)
                        )""");
                statement.execute("DELETE FROM HISTORY");
                statement.execute("""
                        INSERT INTO HISTORY VALUES
                          ('u1', 'e1', TIMESTAMP '2025-05-01 00:00:01'),
                          ('u1', 'e2', TIMESTAMP '2025-05-01 00:00:02')""");
            }

            // input replays e2 (already in the external store) and adds e3
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
                              { "userId": "u1", "eventId": "e2", "eventTime": "2025-05-01T00:00:02Z" },
                              { "userId": "u1", "eventId": "e3", "eventTime": "2025-05-01T00:00:03Z" }
                            ]
                          },
                          "schema": {
                            "fields": [
                              { "name": "userId", "type": "string" },
                              { "name": "eventId", "type": "string" },
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
                            "sql": "SELECT i.eventId AS eventId, s.cnt AS cnt FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                            "sources": [
                              {
                                "name": "ext",
                                "type": "jdbc",
                                "driver": "org.h2.Driver",
                                "url": "%s",
                                "user": "sa",
                                "password": "",
                                "tables": [ { "name": "HISTORY" } ]
                              },
                              {
                                "name": "buf",
                                "type": "buffer",
                                "groupFields": ["userId"],
                                "dedupFields": ["eventId"],
                                "restoreSql": "SELECT h.USER_ID AS userId, h.EVENT_ID AS eventId, h.EVENT_TIME AS __timestamp FROM INPUT AS i JOIN ext.HISTORY AS h ON h.USER_ID = i.userId",
                                "tables": [ { "name": "history" } ]
                              }
                            ]
                          }
                        }
                      ]
                    }
                    """.formatted(url);

            final Config config = Config.load(configJson);
            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

            PAssert.that(outputs.get("query").getCollection()).satisfies(elements -> {
                final Map<String, Long> countByEvent = new HashMap<>();
                for(final MElement element : elements) {
                    countByEvent.put(element.getAsString("eventId"), element.getAsLong("cnt"));
                }
                Assertions.assertEquals(2, countByEvent.size());
                // e2: restored e1+e2 plus the replayed live e2, deduplicated → 2
                Assertions.assertEquals(2L, countByEvent.get("e2"));
                // e3: e1, e2 (restored), e3 → 3 (the replayed e2 counted once)
                Assertions.assertEquals(3L, countByEvent.get("e3"));
                return null;
            });

            pipeline.run();
        }
    }

    /** insertOutput emits the persisted rows — including persist-only elements. */
    @Test
    public void testInsertOutputEmitsPersistedRows() throws Exception {
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
                          { "userId": "u1", "action": "purchase", "amount": 100, "eventTime": "2025-05-01T00:00:03Z" }
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
                            "bufferFilter": [ { "key": "action", "op": "=", "value": "view" } ],
                            "triggerFilter": [ { "key": "action", "op": "=", "value": "purchase" } ],
                            "insertSql": "SELECT userId, amount FROM INPUT WHERE action = 'view'",
                            "insertOutput": "persisted",
                            "tables": [ { "name": "history" } ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        // persist-only view elements still emit their buffer rows
        PAssert.that(outputs.get("query.persisted").getCollection()).satisfies(elements -> {
            final Set<Long> amounts = new java.util.HashSet<>();
            for(final MElement element : elements) {
                amounts.add(element.getAsLong("amount"));
                Assertions.assertNotNull(element.getPrimitiveValue("__timestamp"));
            }
            Assertions.assertEquals(Set.of(1L, 2L), amounts);
            return null;
        });
        // the trigger element evaluated against the two persisted views
        PAssert.that(outputs.get("query").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                count++;
                Assertions.assertEquals(2L, element.getAsLong("viewCnt"));
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    /** restoreSql demands dedupFields and the __timestamp column. */
    @Test
    public void testRestoreValidationErrors() throws Exception {
        final String base = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "userId": "u1" } ]
                      },
                      "schema": { "fields": [ { "name": "userId", "type": "string" } ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["events"],
                      "parameters": {
                        "sql": "SELECT i.userId AS userId, s.cnt AS cnt FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt FROM buf.history AS b WHERE b.userId = i.userId) AS s ON TRUE",
                        "sources": [
                          {
                            "name": "buf",
                            "type": "buffer",
                            "groupFields": ["userId"],
                            %s
                            "tables": [ { "name": "history" } ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """;

        // missing dedupFields
        final Config noDedup = Config.load(base.formatted(
                "\"restoreSql\": \"SELECT userId, CURRENT_TIMESTAMP AS __timestamp FROM INPUT\","));
        final IllegalModuleException thrown1 = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, noDedup));
        Assertions.assertTrue(thrown1.getMessage().contains("dedupFields"),
                "unexpected error: " + thrown1.getMessage());

        // missing __timestamp column in the restore select list
        final Config noTimestamp = Config.load(base.formatted(
                "\"restoreSql\": \"SELECT userId FROM INPUT\", \"dedupFields\": [\"userId\"],"));
        final IllegalModuleException thrown2 = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(pipeline, noTimestamp));
        Assertions.assertTrue(thrown2.getMessage().contains("__timestamp"),
                "unexpected error: " + thrown2.getMessage());
    }

    private static MElement element(String userId, long amount, Instant timestamp) {
        final Map<String, Object> values = new HashMap<>();
        values.put("userId", userId);
        values.put("amount", amount);
        return MElement.of(values, timestamp);
    }
}
