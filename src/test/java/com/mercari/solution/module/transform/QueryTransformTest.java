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

}
