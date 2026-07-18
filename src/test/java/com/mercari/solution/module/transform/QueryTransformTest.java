package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class QueryTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String CREATE_USERS_SOURCE = """
            {
              "name": "users",
              "module": "create",
              "parameters": {
                "type": "element",
                "elements": [
                  {
                    "userId": "u1",
                    "events": [
                      { "category": "a", "amount": 1 },
                      { "category": "a", "amount": 2 },
                      { "category": "b", "amount": 5 }
                    ]
                  },
                  {
                    "userId": "u2",
                    "events": []
                  }
                ]
              },
              "schema": {
                "fields": [
                  { "name": "userId", "type": "string" },
                  { "name": "events", "type": "element", "mode": "repeated", "fields": [
                    { "name": "category", "type": "string" },
                    { "name": "amount", "type": "int64" }
                  ]}
                ]
              }
            }
            """;

    @Test
    public void testAggregateOverElementCollection() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    %s
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["users"],
                      "parameters": {
                        "sql": "SELECT userId, COUNT(*) AS cnt, SUM(e.amount) AS total FROM INPUT, UNNEST(events) AS e GROUP BY userId"
                      }
                    }
                  ]
                }
                """.formatted(CREATE_USERS_SOURCE);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);
        Assertions.assertNotNull(output.getSchema().getField("cnt"));
        Assertions.assertNotNull(output.getSchema().getField("total"));

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals("u1", element.getAsString("userId"));
                Assertions.assertEquals(3L, element.getAsLong("cnt"));
                Assertions.assertEquals(8L, element.getAsLong("total"));
                count++;
            }
            // u2 has an empty events array and must yield no output row
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testFanOutOverElementCollection() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    %s
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["users"],
                      "parameters": {
                        "table": "users",
                        "sql": "SELECT userId, e.category AS category, e.amount AS amount FROM users, UNNEST(events) AS e WHERE e.amount >= 2 ORDER BY e.amount DESC LIMIT 2"
                      }
                    }
                  ]
                }
                """.formatted(CREATE_USERS_SOURCE);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, Long> amounts = new HashMap<>();
            for(final MElement element : elements) {
                Assertions.assertEquals("u1", element.getAsString("userId"));
                amounts.put(element.getAsString("category") + "_" + element.getAsLong("amount"), element.getAsLong("amount"));
            }
            // ORDER BY amount DESC LIMIT 2 within the element: 5 (b) and 2 (a)
            Assertions.assertEquals(Set.of("b_5", "a_2"), amounts.keySet());
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testScalarQuery() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "numbers",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1, 2, 3]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["numbers"],
                      "parameters": {
                        "sql": "SELECT `value` AS id, `value` * 10 AS scaled FROM INPUT WHERE `value` > 1"
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
            final Set<Long> scaled = new HashSet<>();
            for(final MElement element : elements) {
                scaled.add(element.getAsLong("scaled"));
            }
            Assertions.assertEquals(Set.of(20L, 30L), scaled);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testCommonFilterSkipsEvaluation() throws Exception {
        // parameters.filter (filter DSL) drops elements before the SQL runs.
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "numbers",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1, 2, 3]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["numbers"],
                      "parameters": {
                        "filter": [
                          { "key": "value", "op": ">=", "value": 2 }
                        ],
                        "sql": "SELECT `value` AS id, `value` * 10 AS scaled FROM INPUT"
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
            final Set<Long> scaled = new HashSet<>();
            for(final MElement element : elements) {
                scaled.add(element.getAsLong("scaled"));
            }
            Assertions.assertEquals(Set.of(20L, 30L), scaled);
            return null;
        });

        pipeline.run();
    }

    /**
     * The queries form: named statements evaluated per element in one session;
     * intermediates (output: false) are referenceable by later statements, and
     * each output statement becomes a named module output consumable downstream
     * as {@code transformName.queryName}.
     */
    @Test
    public void testQueriesMultipleOutputs() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "numbers",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [1, 5, 20, 40]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["numbers"],
                      "parameters": {
                        "queries": [
                          { "name": "scaled", "sql": "SELECT `value` AS id, `value` * 10 AS score FROM INPUT", "output": false },
                          { "name": "high", "sql": "SELECT id, score FROM scaled WHERE score >= 100" },
                          { "name": "low", "sql": "SELECT id, score FROM scaled WHERE score < 100" }
                        ]
                      }
                    },
                    {
                      "name": "highOnly",
                      "module": "select",
                      "inputs": ["query.high"],
                      "parameters": {
                        "select": [
                          { "name": "id", "field": "id" },
                          { "name": "score", "field": "score" }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection high = outputs.get("query.high");
        final MCollection low = outputs.get("query.low");
        Assertions.assertNotNull(high);
        Assertions.assertNotNull(low);
        Assertions.assertNotNull(high.getSchema().getField("score"));

        PAssert.that(high.getCollection()).satisfies(elements -> {
            final Set<Long> scores = new HashSet<>();
            for(final MElement element : elements) {
                scores.add(element.getAsLong("score"));
            }
            Assertions.assertEquals(Set.of(200L, 400L), scores);
            return null;
        });
        PAssert.that(low.getCollection()).satisfies(elements -> {
            final Set<Long> scores = new HashSet<>();
            for(final MElement element : elements) {
                scores.add(element.getAsLong("score"));
            }
            Assertions.assertEquals(Set.of(10L, 50L), scores);
            return null;
        });
        // the downstream module consumed the named output
        PAssert.that(outputs.get("highOnly").getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement ignored : elements) {
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    /** exclusive: evaluation stops after the first output query that produced rows. */
    @Test
    public void testQueriesExclusiveRouting() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "numbers",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [1, 5, 20, 40]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["numbers"],
                      "parameters": {
                        "exclusive": true,
                        "queries": [
                          { "name": "high",   "sql": "SELECT `value` AS id FROM INPUT WHERE `value` >= 20" },
                          { "name": "medium", "sql": "SELECT `value` AS id FROM INPUT WHERE `value` >= 5" },
                          { "name": "rest",   "sql": "SELECT `value` AS id FROM INPUT" }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        // 20, 40 → high only; 5 → medium (high produced nothing); 1 → rest
        PAssert.that(outputs.get("query.high").getCollection()).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
            }
            Assertions.assertEquals(Set.of(20L, 40L), ids);
            return null;
        });
        PAssert.that(outputs.get("query.medium").getCollection()).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
            }
            Assertions.assertEquals(Set.of(5L), ids);
            return null;
        });
        PAssert.that(outputs.get("query.rest").getCollection()).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
            }
            Assertions.assertEquals(Set.of(1L), ids);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testSqlAndQueriesAreExclusive() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "numbers",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["numbers"],
                      "parameters": {
                        "sql": "SELECT `value` AS id FROM INPUT",
                        "queries": [
                          { "name": "a", "sql": "SELECT `value` AS id FROM INPUT" }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

    @Test
    public void testValidationError() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "numbers",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["numbers"],
                      "parameters": {
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

    @Test
    public void testAutoGeneratedColumnNameRejected() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "numbers",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["numbers"],
                      "parameters": {
                        "sql": "SELECT `value` + 1 FROM INPUT"
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

    @Test
    public void testJdbcLookupJoin() throws Exception {
        final String url = "jdbc:h2:mem:querytransformtest;DB_CLOSE_DELAY=-1";
        // Keep the named in-memory database alive for the duration of the test.
        try (final java.sql.Connection connection =
                     java.sql.DriverManager.getConnection(url, "sa", "")) {
            try (final java.sql.Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS ITEMS (
                          ITEM_ID VARCHAR(16) PRIMARY KEY,
                          TITLE VARCHAR(64),
                          PRICE BIGINT
                        )""");
                statement.execute("DELETE FROM ITEMS");
                statement.execute("""
                        INSERT INTO ITEMS VALUES
                          ('i1', 'apple', 100),
                          ('i2', 'banana', 200)""");
            }

            final String configJson = """
                    {
                      "sources": [
                        {
                          "name": "carts",
                          "module": "create",
                          "parameters": {
                            "type": "element",
                            "elements": [
                              { "itemId": "i1", "qty": 2 },
                              { "itemId": "i2", "qty": 3 },
                              { "itemId": "i9", "qty": 1 }
                            ]
                          },
                          "schema": {
                            "fields": [
                              { "name": "itemId", "type": "string" },
                              { "name": "qty", "type": "int64" }
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
                            "sql": "SELECT i.itemId AS itemId, m.TITLE AS title, i.qty * m.PRICE AS total FROM INPUT AS i LEFT JOIN db.ITEMS AS m ON m.ITEM_ID = i.itemId",
                            "sources": [
                              {
                                "name": "db",
                                "type": "jdbc",
                                "driver": "org.h2.Driver",
                                "url": "%s",
                                "user": "sa",
                                "password": "",
                                "tables": [
                                  { "name": "ITEMS" }
                                ]
                              }
                            ]
                          }
                        }
                      ]
                    }
                    """.formatted(url);

            final Config config = Config.load(configJson);
            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

            final MCollection output = outputs.get("query");
            Assertions.assertNotNull(output);
            Assertions.assertNotNull(output.getSchema().getField("title"));
            Assertions.assertNotNull(output.getSchema().getField("total"));

            PAssert.that(output.getCollection()).satisfies(elements -> {
                final Map<String, MElement> byItem = new HashMap<>();
                for(final MElement element : elements) {
                    byItem.put(element.getAsString("itemId"), element);
                }
                Assertions.assertEquals(3, byItem.size());
                Assertions.assertEquals("apple", byItem.get("i1").getAsString("title"));
                Assertions.assertEquals(200L, byItem.get("i1").getAsLong("total"));
                Assertions.assertEquals("banana", byItem.get("i2").getAsString("title"));
                Assertions.assertEquals(600L, byItem.get("i2").getAsLong("total"));
                // unknown item: LEFT JOIN keeps the input row with nulls
                Assertions.assertNull(byItem.get("i9").getPrimitiveValue("title"));
                return null;
            });

            pipeline.run();
        }
    }

    @Test
    public void testJdbcLookupJoinWithCache() throws Exception {
        final String url = "jdbc:h2:mem:querytransformcachetest;DB_CLOSE_DELAY=-1";
        // Keep the named in-memory database alive for the duration of the test.
        try (final java.sql.Connection connection =
                     java.sql.DriverManager.getConnection(url, "sa", "")) {
            try (final java.sql.Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS ITEMS (
                          ITEM_ID VARCHAR(16) PRIMARY KEY,
                          TITLE VARCHAR(64),
                          PRICE BIGINT
                        )""");
                statement.execute("DELETE FROM ITEMS");
                statement.execute("""
                        INSERT INTO ITEMS VALUES
                          ('i1', 'apple', 100),
                          ('i2', 'banana', 200)""");
            }

            // Repeated keys (i1 twice, i9 twice) exercise cache hits and the
            // negative cache; results must be identical to the uncached join.
            final String configJson = """
                    {
                      "sources": [
                        {
                          "name": "carts",
                          "module": "create",
                          "parameters": {
                            "type": "element",
                            "elements": [
                              { "cartId": "c1", "itemId": "i1", "qty": 2 },
                              { "cartId": "c2", "itemId": "i1", "qty": 5 },
                              { "cartId": "c3", "itemId": "i2", "qty": 3 },
                              { "cartId": "c4", "itemId": "i9", "qty": 1 },
                              { "cartId": "c5", "itemId": "i9", "qty": 4 }
                            ]
                          },
                          "schema": {
                            "fields": [
                              { "name": "cartId", "type": "string" },
                              { "name": "itemId", "type": "string" },
                              { "name": "qty", "type": "int64" }
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
                            "sql": "SELECT i.cartId AS cartId, m.TITLE AS title, i.qty * m.PRICE AS total FROM INPUT AS i LEFT JOIN db.ITEMS AS m ON m.ITEM_ID = i.itemId",
                            "sources": [
                              {
                                "name": "db",
                                "type": "jdbc",
                                "driver": "org.h2.Driver",
                                "url": "%s",
                                "user": "sa",
                                "password": "",
                                "cache": {
                                  "maxSize": 100,
                                  "expireAfterSeconds": 60
                                },
                                "tables": [
                                  { "name": "ITEMS" }
                                ]
                              }
                            ]
                          }
                        }
                      ]
                    }
                    """.formatted(url);

            final Config config = Config.load(configJson);
            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

            final MCollection output = outputs.get("query");
            Assertions.assertNotNull(output);

            PAssert.that(output.getCollection()).satisfies(elements -> {
                final Map<String, MElement> byCart = new HashMap<>();
                for(final MElement element : elements) {
                    byCart.put(element.getAsString("cartId"), element);
                }
                Assertions.assertEquals(5, byCart.size());
                Assertions.assertEquals("apple", byCart.get("c1").getAsString("title"));
                Assertions.assertEquals(200L, byCart.get("c1").getAsLong("total"));
                Assertions.assertEquals("apple", byCart.get("c2").getAsString("title"));
                Assertions.assertEquals(500L, byCart.get("c2").getAsLong("total"));
                Assertions.assertEquals("banana", byCart.get("c3").getAsString("title"));
                Assertions.assertEquals(600L, byCart.get("c3").getAsLong("total"));
                // unknown item: LEFT JOIN keeps the input rows with nulls
                Assertions.assertNull(byCart.get("c4").getPrimitiveValue("title"));
                Assertions.assertNull(byCart.get("c5").getPrimitiveValue("title"));
                return null;
            });

            pipeline.run();
        }
    }

    @Test
    public void testJdbcLateralLookupJoin() throws Exception {
        final String url = "jdbc:h2:mem:querytransformlateraltest;DB_CLOSE_DELAY=-1";
        // Keep the named in-memory database alive for the duration of the test.
        try (final java.sql.Connection connection =
                     java.sql.DriverManager.getConnection(url, "sa", "")) {
            try (final java.sql.Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS EVENTS (
                          USER_ID VARCHAR(16) NOT NULL,
                          SEQ BIGINT NOT NULL,
                          AMOUNT BIGINT,
                          PRIMARY KEY (USER_ID, SEQ)
                        )""");
                statement.execute("DELETE FROM EVENTS");
                statement.execute("""
                        INSERT INTO EVENTS VALUES
                          ('u1', 1, 10), ('u1', 2, 20), ('u1', 3, 30),
                          ('u2', 1, 5)""");
            }

            // The correlated LATERAL block: the key + range correlations become
            // the batched fetch; COUNT/SUM run per key inside the DoFn. This
            // exercises the full module path: Gson config parsing, Query2
            // serialization into the DoFn, and worker-side setup of the lateral
            // inner-plan evaluator.
            final String configJson = """
                    {
                      "sources": [
                        {
                          "name": "users",
                          "module": "create",
                          "parameters": {
                            "type": "element",
                            "elements": [
                              { "userId": "u1", "maxSeq": 2 },
                              { "userId": "u2", "maxSeq": 9 },
                              { "userId": "u9", "maxSeq": 9 }
                            ]
                          },
                          "schema": {
                            "fields": [
                              { "name": "userId", "type": "string" },
                              { "name": "maxSeq", "type": "int64" }
                            ]
                          }
                        }
                      ],
                      "transforms": [
                        {
                          "name": "query",
                          "module": "query",
                          "inputs": ["users"],
                          "parameters": {
                            "sql": "SELECT i.userId AS userId, s.cnt AS cnt, s.total AS total FROM INPUT AS i JOIN LATERAL (SELECT COUNT(*) AS cnt, SUM(e.AMOUNT) AS total FROM db.EVENTS AS e WHERE e.USER_ID = i.userId AND e.SEQ >= 1 AND e.SEQ <= i.maxSeq) AS s ON TRUE",
                            "sources": [
                              {
                                "name": "db",
                                "type": "jdbc",
                                "driver": "org.h2.Driver",
                                "url": "%s",
                                "user": "sa",
                                "password": "",
                                "tables": [
                                  { "name": "EVENTS" }
                                ]
                              }
                            ]
                          }
                        }
                      ]
                    }
                    """.formatted(url);

            final Config config = Config.load(configJson);
            final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

            final MCollection output = outputs.get("query");
            Assertions.assertNotNull(output);
            Assertions.assertNotNull(output.getSchema().getField("cnt"));
            Assertions.assertNotNull(output.getSchema().getField("total"));

            PAssert.that(output.getCollection()).satisfies(elements -> {
                final Map<String, MElement> byUser = new HashMap<>();
                for(final MElement element : elements) {
                    byUser.put(element.getAsString("userId"), element);
                }
                Assertions.assertEquals(3, byUser.size());
                // u1: seq 1..2 → 2 rows, 10+20
                Assertions.assertEquals(2L, byUser.get("u1").getAsLong("cnt"));
                Assertions.assertEquals(30L, byUser.get("u1").getAsLong("total"));
                // u2: seq 1..9 → 1 row, 5
                Assertions.assertEquals(1L, byUser.get("u2").getAsLong("cnt"));
                Assertions.assertEquals(5L, byUser.get("u2").getAsLong("total"));
                // u9: no match → the aggregate over the empty set keeps the row
                Assertions.assertEquals(0L, byUser.get("u9").getAsLong("cnt"));
                Assertions.assertNull(byUser.get("u9").getPrimitiveValue("total"));
                return null;
            });

            pipeline.run();
        }
    }

    private static final String CREATE_ORDERS_SOURCE = """
            {
              "name": "orders",
              "module": "create",
              "parameters": {
                "type": "element",
                "elements": [
                  { "userId": "u1", "amount": 100 },
                  { "userId": "u2", "amount": 200 },
                  { "userId": "u9", "amount": 300 }
                ]
              },
              "schema": {
                "fields": [
                  { "name": "userId", "type": "string" },
                  { "name": "amount", "type": "int64" }
                ]
              }
            }
            """;

    private static final String CREATE_MEMBERS_SOURCE = """
            {
              "name": "members",
              "module": "create",
              "parameters": {
                "type": "element",
                "elements": [
                  { "userId": "u1", "userName": "alice" },
                  { "userId": "u2", "userName": "bob" }
                ]
              },
              "schema": {
                "fields": [
                  { "name": "userId", "type": "string" },
                  { "name": "userName", "type": "string" }
                ]
              }
            }
            """;

    @Test
    public void testSideInputLookupJoin() throws Exception {
        // Another MCollection delivered as a Beam side input joined as a lookup
        // table: no external store, indexed once per window on the worker.
        final String configJson = """
                {
                  "sources": [
                    %s,
                    %s
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["orders"],
                      "sideInputs": ["members"],
                      "parameters": {
                        "sql": "SELECT o.userId AS userId, o.amount AS amount, m.userName AS userName FROM INPUT AS o LEFT JOIN side.members AS m ON m.userId = o.userId",
                        "sources": [
                          {
                            "name": "side",
                            "type": "sideinput",
                            "tables": [
                              { "name": "members", "keyFields": ["userId"] }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """.formatted(CREATE_ORDERS_SOURCE, CREATE_MEMBERS_SOURCE);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);
        Assertions.assertNotNull(output.getSchema().getField("userName"));

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, MElement> byUser = new HashMap<>();
            for(final MElement element : elements) {
                byUser.put(element.getAsString("userId"), element);
            }
            Assertions.assertEquals(3, byUser.size());
            Assertions.assertEquals("alice", byUser.get("u1").getAsString("userName"));
            Assertions.assertEquals(100L, byUser.get("u1").getAsLong("amount"));
            Assertions.assertEquals("bob", byUser.get("u2").getAsString("userName"));
            // unknown user: LEFT JOIN keeps the input row with nulls
            Assertions.assertNull(byUser.get("u9").getPrimitiveValue("userName"));
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testMultipleSideInputLookupJoin() throws Exception {
        final String createCategories = """
                {
                  "name": "categories",
                  "module": "create",
                  "parameters": {
                    "type": "element",
                    "elements": [
                      { "categoryId": "c1", "label": "food" }
                    ]
                  },
                  "schema": {
                    "fields": [
                      { "name": "categoryId", "type": "string" },
                      { "name": "label", "type": "string" }
                    ]
                  }
                }
                """;
        final String createPurchases = """
                {
                  "name": "purchases",
                  "module": "create",
                  "parameters": {
                    "type": "element",
                    "elements": [
                      { "userId": "u1", "categoryId": "c1" },
                      { "userId": "u2", "categoryId": "c9" }
                    ]
                  },
                  "schema": {
                    "fields": [
                      { "name": "userId", "type": "string" },
                      { "name": "categoryId", "type": "string" }
                    ]
                  }
                }
                """;
        final String configJson = """
                {
                  "sources": [
                    %s,
                    %s,
                    %s
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["purchases"],
                      "sideInputs": ["members", "categories"],
                      "parameters": {
                        "sql": "SELECT p.userId AS userId, m.userName AS userName, c.label AS label FROM INPUT AS p JOIN side.members AS m ON m.userId = p.userId LEFT JOIN side.categories AS c ON c.categoryId = p.categoryId",
                        "sources": [
                          {
                            "name": "side",
                            "type": "sideinput",
                            "tables": [
                              { "name": "members", "keyFields": ["userId"] },
                              { "name": "categories", "keyFields": ["categoryId"] }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """.formatted(createPurchases, CREATE_MEMBERS_SOURCE, createCategories);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("query");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, MElement> byUser = new HashMap<>();
            for(final MElement element : elements) {
                byUser.put(element.getAsString("userId"), element);
            }
            Assertions.assertEquals(2, byUser.size());
            Assertions.assertEquals("alice", byUser.get("u1").getAsString("userName"));
            Assertions.assertEquals("food", byUser.get("u1").getAsString("label"));
            Assertions.assertEquals("bob", byUser.get("u2").getAsString("userName"));
            Assertions.assertNull(byUser.get("u2").getPrimitiveValue("label"));
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testSideInputSourceRequiresDeclaredSideInput() throws Exception {
        // The sideinput source references a collection missing from the module's
        // sideInputs list: rejected at pipeline construction.
        final String configJson = """
                {
                  "sources": [
                    %s,
                    %s
                  ],
                  "transforms": [
                    {
                      "name": "query",
                      "module": "query",
                      "inputs": ["orders"],
                      "parameters": {
                        "sql": "SELECT o.userId AS userId, m.userName AS userName FROM INPUT AS o JOIN side.members AS m ON m.userId = o.userId",
                        "sources": [
                          {
                            "name": "side",
                            "type": "sideinput",
                            "tables": [
                              { "name": "members", "keyFields": ["userId"] }
                            ]
                          }
                        ]
                      }
                    }
                  ]
                }
                """.formatted(CREATE_ORDERS_SOURCE, CREATE_MEMBERS_SOURCE);

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

}
