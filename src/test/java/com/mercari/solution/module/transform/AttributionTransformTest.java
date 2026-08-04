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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AttributionTransformTest {

    private static final double DELTA = 1e-9;

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testExternalTwoInputs() throws Exception {
        // 3x2 grid; every region=a leaf triples (100 -> 300)
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "target",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "region": "a", "category": "x", "sales": 300 },
                          { "region": "a", "category": "y", "sales": 300 },
                          { "region": "b", "category": "x", "sales": 100 },
                          { "region": "b", "category": "y", "sales": 100 },
                          { "region": "c", "category": "x", "sales": 100 },
                          { "region": "c", "category": "y", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    },
                    {
                      "name": "baseline",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "region": "a", "category": "x", "sales": 100 },
                          { "region": "a", "category": "y", "sales": 100 },
                          { "region": "b", "category": "x", "sales": 100 },
                          { "region": "b", "category": "y", "sales": 100 },
                          { "region": "c", "category": "x", "sales": 100 },
                          { "region": "c", "category": "y", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["target", "baseline"],
                      "parameters": {
                        "measures": [
                          { "name": "sales" }
                        ],
                        "vocabulary": {
                          "dimensions": [
                            { "name": "region" },
                            { "name": "category" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("attribution");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            final MElement row = rows.getFirst();
            Assertions.assertEquals("sales", row.getAsString("measure"));
            Assertions.assertEquals("riskloc", row.getAsString("algorithm"));
            Assertions.assertEquals("netDelta", row.getAsString("epBasis"));
            Assertions.assertEquals(1L, row.getAsLong("rank"));
            Assertions.assertEquals(false, row.getPrimitiveValue("noFinding"));
            Assertions.assertEquals(1L, row.getAsLong("layer"));
            assertElements(row, "region=a");
            Assertions.assertEquals(200.0, row.getAsDouble("baseline"), DELTA);
            Assertions.assertEquals(600.0, row.getAsDouble("target"), DELTA);
            Assertions.assertEquals(400.0, row.getAsDouble("delta"), DELTA);
            Assertions.assertEquals(600.0, row.getAsDouble("totalBaseline"), DELTA);
            Assertions.assertEquals(1000.0, row.getAsDouble("totalTarget"), DELTA);
            Assertions.assertEquals(1.0, row.getAsDouble("explanatoryPower"), DELTA);
            Assertions.assertEquals(2.0 / 3.0, row.getAsDouble("riskScore"), DELTA);
            Assertions.assertEquals(2L, row.getAsLong("leafCount"));
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testExternalLabelField() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "metrics",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "window_type": "current",  "region": "a", "sales": 300 },
                          { "window_type": "current",  "region": "a", "sales": 300 },
                          { "window_type": "current",  "region": "b", "sales": 100 },
                          { "window_type": "current",  "region": "c", "sales": 100 },
                          { "window_type": "previous", "region": "a", "sales": 100 },
                          { "window_type": "previous", "region": "a", "sales": 100 },
                          { "window_type": "previous", "region": "b", "sales": 100 },
                          { "window_type": "previous", "region": "c", "sales": 100 },
                          { "window_type": "ignored",  "region": "a", "sales": 9999 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "window_type", "type": "string" },
                        { "name": "region", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["metrics"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "comparison": {
                          "reference": {
                            "strategy": "external",
                            "labelField": "window_type",
                            "targetLabel": "current",
                            "baselineLabel": "previous"
                          }
                        },
                        "vocabulary": {
                          "dimensions": [ { "name": "region" } ]
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            final MElement row = rows.getFirst();
            assertElements(row, "region=a");
            // Two "current" rows of (a) summed to 600 vs two "previous" rows summed to 200;
            // the "ignored" label must be dropped
            Assertions.assertEquals(200.0, row.getAsDouble("baseline"), DELTA);
            Assertions.assertEquals(600.0, row.getAsDouble("target"), DELTA);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testSplit() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "metrics",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "anomalous": true,  "region": "a", "category": "x", "sales": 300 },
                          { "anomalous": true,  "region": "a", "category": "y", "sales": 300 },
                          { "anomalous": true,  "region": "b", "category": "x", "sales": 100 },
                          { "anomalous": true,  "region": "b", "category": "y", "sales": 100 },
                          { "anomalous": false, "region": "a", "category": "x", "sales": 100 },
                          { "anomalous": false, "region": "a", "category": "y", "sales": 100 },
                          { "anomalous": false, "region": "b", "category": "x", "sales": 100 },
                          { "anomalous": false, "region": "b", "category": "y", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "anomalous", "type": "boolean" },
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["metrics"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "comparison": {
                          "reference": {
                            "strategy": "split",
                            "split": { "by": { "field": "anomalous", "baseline": false, "target": true } }
                          }
                        },
                        "vocabulary": {
                          "dimensions": [ { "name": "region" }, { "name": "category" } ]
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            assertElements(rows.getFirst(), "region=a");
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testTimeShift() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "daily",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "ts": "2024-01-01T00:00:00Z", "region": "a", "sales": 100 },
                          { "ts": "2024-01-01T00:00:00Z", "region": "b", "sales": 100 },
                          { "ts": "2024-01-02T00:00:00Z", "region": "a", "sales": 100 },
                          { "ts": "2024-01-02T00:00:00Z", "region": "b", "sales": 100 },
                          { "ts": "2024-01-03T00:00:00Z", "region": "a", "sales": 100 },
                          { "ts": "2024-01-03T00:00:00Z", "region": "b", "sales": 100 },
                          { "ts": "2024-01-04T00:00:00Z", "region": "a", "sales": 100 },
                          { "ts": "2024-01-04T00:00:00Z", "region": "b", "sales": 100 },
                          { "ts": "2024-01-08T00:00:00Z", "region": "a", "sales": 300 },
                          { "ts": "2024-01-08T00:00:00Z", "region": "b", "sales": 100 },
                          { "ts": "2024-01-09T00:00:00Z", "region": "a", "sales": 300 },
                          { "ts": "2024-01-09T00:00:00Z", "region": "b", "sales": 100 },
                          { "ts": "2024-01-10T00:00:00Z", "region": "a", "sales": 300 },
                          { "ts": "2024-01-10T00:00:00Z", "region": "b", "sales": 100 },
                          { "ts": "2024-01-11T00:00:00Z", "region": "a", "sales": 300 },
                          { "ts": "2024-01-11T00:00:00Z", "region": "b", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "ts", "type": "timestamp" },
                        { "name": "region", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["daily"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "comparison": {
                          "reference": {
                            "strategy": "timeShift",
                            "timeShift": { "offset": "P7D", "timeField": "ts" }
                          }
                        },
                        "vocabulary": {
                          "dimensions": [ { "name": "region" } ]
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            final MElement row = rows.getFirst();
            assertElements(row, "region=a");
            // tmax = Jan 11: target window (Jan 4, Jan 11] = Jan 8-11 rows,
            // baseline window (Dec 28, Jan 4] = Jan 1-4 rows
            Assertions.assertEquals(400.0, row.getAsDouble("baseline"), DELTA);
            Assertions.assertEquals(1200.0, row.getAsDouble("target"), DELTA);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testSyntheticMarginal() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "metrics",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "d1": "a", "d2": "x", "cnt": 90 },
                          { "d1": "a", "d2": "y", "cnt": 10 },
                          { "d1": "b", "d2": "x", "cnt": 10 },
                          { "d1": "b", "d2": "y", "cnt": 90 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "d1", "type": "string" },
                        { "name": "d2", "type": "string" },
                        { "name": "cnt", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["metrics"],
                      "parameters": {
                        "measures": [ { "name": "cnt" } ],
                        "comparison": {
                          "reference": { "strategy": "synthetic", "synthetic": { "method": "marginal" } }
                        },
                        "vocabulary": {
                          "dimensions": [ { "name": "d1" }, { "name": "d2" } ]
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            // The two under-performing interaction cells localized at layer 2
            Assertions.assertEquals(2, rows.size());
            final List<String> slices = rows.stream()
                    .map(AttributionTransformTest::describeElements)
                    .sorted()
                    .toList();
            Assertions.assertEquals(List.of("d1=a,d2=y", "d1=b,d2=x"), slices);
            for(final MElement row : rows) {
                Assertions.assertEquals(2L, row.getAsLong("layer"));
                // auto resolves to absoluteDelta: the marginal baseline has zero net delta
                Assertions.assertEquals("absoluteDelta", row.getAsString("epBasis"));
                Assertions.assertEquals(50.0, row.getAsDouble("baseline"), DELTA);
                Assertions.assertEquals(10.0, row.getAsDouble("target"), DELTA);
            }
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testDerivedMeasure() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "target",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "d": "A", "g": "p", "orders": 30, "sessions": 100 },
                          { "d": "A", "g": "q", "orders": 30, "sessions": 100 },
                          { "d": "B", "g": "p", "orders": 10, "sessions": 100 },
                          { "d": "B", "g": "q", "orders": 10, "sessions": 100 },
                          { "d": "C", "g": "p", "orders": 10, "sessions": 100 },
                          { "d": "C", "g": "q", "orders": 10, "sessions": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "d", "type": "string" },
                        { "name": "g", "type": "string" },
                        { "name": "orders", "type": "float64" },
                        { "name": "sessions", "type": "float64" }
                      ] }
                    },
                    {
                      "name": "baseline",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "d": "A", "g": "p", "orders": 10, "sessions": 100 },
                          { "d": "A", "g": "q", "orders": 10, "sessions": 100 },
                          { "d": "B", "g": "p", "orders": 10, "sessions": 100 },
                          { "d": "B", "g": "q", "orders": 10, "sessions": 100 },
                          { "d": "C", "g": "p", "orders": 10, "sessions": 100 },
                          { "d": "C", "g": "q", "orders": 10, "sessions": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "d", "type": "string" },
                        { "name": "g", "type": "string" },
                        { "name": "orders", "type": "float64" },
                        { "name": "sessions", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["target", "baseline"],
                      "parameters": {
                        "measures": [
                          { "name": "cvr", "type": "derived", "expression": "orders / sessions" }
                        ],
                        "vocabulary": {
                          "dimensions": [ { "name": "d" }, { "name": "g" } ]
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            final MElement row = rows.getFirst();
            Assertions.assertEquals("cvr", row.getAsString("measure"));
            assertElements(row, "d=A");
            // Slice and total values are the actual ratios, not pseudo-column sums
            Assertions.assertEquals(0.1, row.getAsDouble("baseline"), DELTA);
            Assertions.assertEquals(0.3, row.getAsDouble("target"), DELTA);
            Assertions.assertEquals(60.0 / 600.0, row.getAsDouble("totalBaseline"), DELTA);
            Assertions.assertEquals(100.0 / 600.0, row.getAsDouble("totalTarget"), DELTA);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testGuardsMaxCardinality() throws Exception {
        // The anomalous tail values are bucketed into "other" which then carries the anomaly
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "target",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "v1", "sales": 100 },
                          { "id": "v2", "sales": 100 },
                          { "id": "v3", "sales": 100 },
                          { "id": "v4", "sales": 80 },
                          { "id": "v5", "sales": 80 },
                          { "id": "v6", "sales": 80 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "id", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    },
                    {
                      "name": "baseline",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "v1", "sales": 100 },
                          { "id": "v2", "sales": 100 },
                          { "id": "v3", "sales": 100 },
                          { "id": "v4", "sales": 20 },
                          { "id": "v5", "sales": 20 },
                          { "id": "v6", "sales": 20 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "id", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["target", "baseline"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": {
                          "dimensions": [ { "name": "id" } ]
                        },
                        "engine": {
                          "guards": { "maxCardinality": 3 }
                        }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            final MElement row = rows.getFirst();
            assertElements(row, "id=other");
            Assertions.assertEquals(60.0, row.getAsDouble("baseline"), DELTA);
            Assertions.assertEquals(240.0, row.getAsDouble("target"), DELTA);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testEmitNoFinding() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "target",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "region": "a", "sales": 100 },
                          { "region": "b", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    },
                    {
                      "name": "baseline",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "region": "a", "sales": 100 },
                          { "region": "b", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["target", "baseline"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                      }
                    },
                    {
                      "name": "attributionSilent",
                      "module": "attribution",
                      "inputs": ["target", "baseline"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] },
                        "output": { "emitNoFinding": false }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            final MElement row = rows.getFirst();
            Assertions.assertEquals(true, row.getPrimitiveValue("noFinding"));
            Assertions.assertEquals(0L, row.getAsLong("rank"));
            Assertions.assertEquals(200.0, row.getAsDouble("totalBaseline"), DELTA);
            Assertions.assertEquals(200.0, row.getAsDouble("totalTarget"), DELTA);
            return null;
        });
        PAssert.that(outputs.get("attributionSilent").getCollection()).empty();

        pipeline.run();
    }

    @Test
    public void testAdtributorAlgorithm() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "target",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "region": "a", "category": "x", "sales": 150 },
                          { "region": "a", "category": "y", "sales": 150 },
                          { "region": "a", "category": "z", "sales": 150 },
                          { "region": "b", "category": "x", "sales": 100 },
                          { "region": "b", "category": "y", "sales": 100 },
                          { "region": "b", "category": "z", "sales": 100 },
                          { "region": "c", "category": "x", "sales": 100 },
                          { "region": "c", "category": "y", "sales": 100 },
                          { "region": "c", "category": "z", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    },
                    {
                      "name": "baseline",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "region": "a", "category": "x", "sales": 100 },
                          { "region": "a", "category": "y", "sales": 100 },
                          { "region": "a", "category": "z", "sales": 100 },
                          { "region": "b", "category": "x", "sales": 100 },
                          { "region": "b", "category": "y", "sales": 100 },
                          { "region": "b", "category": "z", "sales": 100 },
                          { "region": "c", "category": "x", "sales": 100 },
                          { "region": "c", "category": "y", "sales": 100 },
                          { "region": "c", "category": "z", "sales": 100 }
                        ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["target", "baseline"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": {
                          "dimensions": [ { "name": "region" }, { "name": "category" } ]
                        },
                        "engine": { "algorithm": "adtributor" }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertFalse(rows.isEmpty());
            final MElement top = rows.stream()
                    .filter(row -> row.getAsLong("rank") == 1L)
                    .findAny().orElseThrow();
            Assertions.assertEquals("adtributor", top.getAsString("algorithm"));
            assertElements(top, "region=a");
            Assertions.assertNull(top.getPrimitiveValue("riskScore"));
            Assertions.assertNotNull(top.getPrimitiveValue("surprise"));
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testDistributionMeasure() throws Exception {
        // Event-level latency rows labeled base/cur; the region=a tail (top sample) jumps to 500
        // while the median stays put — localized at p99, mirroring the core engine fixture
        final StringBuilder elements = new StringBuilder();
        for(final String region : List.of("a", "b", "c")) {
            for(final String category : List.of("x", "y")) {
                for(int i = 1; i <= 10; i++) {
                    final double base = i * 10;
                    final double cur = switch (region) {
                        case "a" -> i == 10 ? 500 : i * 10;
                        case "b" -> i == 10 ? 101 : i * 10;
                        default -> i == 10 ? 99 : i * 10;
                    };
                    if(!elements.isEmpty()) {
                        elements.append(",");
                    }
                    elements.append(String.format(
                            "{ \"region\": \"%s\", \"category\": \"%s\", \"window\": \"base\", \"latency\": %f },",
                            region, category, base));
                    elements.append(String.format(
                            "{ \"region\": \"%s\", \"category\": \"%s\", \"window\": \"cur\", \"latency\": %f }",
                            region, category, cur));
                }
            }
        }
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ %s ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "window", "type": "string" },
                        { "name": "latency", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["events"],
                      "parameters": {
                        "measures": [
                          { "name": "latency", "type": "distribution", "quantiles": [0.99] }
                        ],
                        "comparison": {
                          "reference": {
                            "strategy": "external",
                            "labelField": "window",
                            "baselineLabel": "base",
                            "targetLabel": "cur"
                          }
                        },
                        "vocabulary": {
                          "dimensions": [
                            { "name": "region" },
                            { "name": "category" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """.formatted(elements);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("attribution");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(rows -> {
            final List<MElement> list = toList(rows);
            Assertions.assertEquals(1, list.size());
            final MElement row = list.getFirst();
            Assertions.assertEquals("latency", row.getAsString("measure"));
            Assertions.assertEquals(0.99, row.getAsDouble("quantile"), DELTA);
            Assertions.assertEquals("absoluteDelta", row.getAsString("epBasis"));
            Assertions.assertEquals(false, row.getPrimitiveValue("noFinding"));
            assertElements(row, "region=a");
            // Values are quantiles of the merged slice sketches, not sums
            Assertions.assertEquals(100.0, row.getAsDouble("baseline"), DELTA);
            Assertions.assertEquals(500.0, row.getAsDouble("target"), DELTA);
            Assertions.assertEquals(100.0, row.getAsDouble("totalBaseline"), DELTA);
            Assertions.assertEquals(500.0, row.getAsDouble("totalTarget"), DELTA);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testDistinctMeasure() throws Exception {
        // Event-level rows with a user id; region=a loses most of its distinct users
        // (10 -> 3) while b gains one (asymmetric noise) and c stays flat
        final StringBuilder elements = new StringBuilder();
        for(final String region : List.of("a", "b", "c")) {
            for(final String category : List.of("x", "y")) {
                final int targetUsers = switch (region) {
                    case "a" -> 3;
                    case "b" -> 11;
                    default -> 10;
                };
                for(int i = 1; i <= Math.max(10, targetUsers); i++) {
                    final String user = region + "_" + category + "_u" + i;
                    if(i <= 10) {
                        if(!elements.isEmpty()) {
                            elements.append(",");
                        }
                        elements.append(String.format(
                                "{ \"region\": \"%s\", \"category\": \"%s\", \"window\": \"base\", \"user_id\": \"%s\" }",
                                region, category, user));
                    }
                    if(i <= targetUsers) {
                        elements.append(String.format(
                                ",{ \"region\": \"%s\", \"category\": \"%s\", \"window\": \"cur\", \"user_id\": \"%s\" }",
                                region, category, user));
                    }
                }
            }
        }
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "events",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ %s ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "window", "type": "string" },
                        { "name": "user_id", "type": "string" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["events"],
                      "parameters": {
                        "measures": [
                          { "name": "user_id", "type": "distinct" }
                        ],
                        "comparison": {
                          "reference": {
                            "strategy": "external",
                            "labelField": "window",
                            "baselineLabel": "base",
                            "targetLabel": "cur"
                          }
                        },
                        "vocabulary": {
                          "dimensions": [
                            { "name": "region" },
                            { "name": "category" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """.formatted(elements);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("attribution");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(rows -> {
            final List<MElement> list = toList(rows);
            Assertions.assertEquals(1, list.size());
            final MElement row = list.getFirst();
            Assertions.assertEquals("user_id", row.getAsString("measure"));
            Assertions.assertNull(row.getAsDouble("quantile"));
            Assertions.assertEquals("absoluteDelta", row.getAsString("epBasis"));
            Assertions.assertEquals(false, row.getPrimitiveValue("noFinding"));
            assertElements(row, "region=a");
            // Values are union distinct estimates, not sums
            Assertions.assertEquals(20.0, row.getAsDouble("baseline"), DELTA);
            Assertions.assertEquals(6.0, row.getAsDouble("target"), DELTA);
            Assertions.assertEquals(60.0, row.getAsDouble("totalBaseline"), DELTA);
            Assertions.assertEquals(48.0, row.getAsDouble("totalTarget"), DELTA);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testSketchMeasures() throws Exception {
        // Pre-aggregated sketch input (e.g. BigQuery-side aggregation): one row per leaf and
        // role carrying base64 KLL and Theta sketch bytes. The (a, x) baseline arrives as two
        // partial sketches to prove wholesale merging. Fixture values mirror the event-level
        // distribution / distinct tests: region=a tail jumps to 500 and loses 7 of 10 users.
        final StringBuilder elements = new StringBuilder();
        for(final String region : List.of("a", "b", "c")) {
            for(final String category : List.of("x", "y")) {
                final String prefix = region + "_" + category + "_u";
                if("a".equals(region) && "x".equals(category)) {
                    // baseline split into two partial aggregates
                    appendSketchRow(elements, region, category, "base",
                            kllBase64(10, 50), thetaBase64(prefix, 1, 5));
                    appendSketchRow(elements, region, category, "base",
                            kllBase64(60, 100), thetaBase64(prefix, 6, 10));
                } else {
                    appendSketchRow(elements, region, category, "base",
                            kllBase64(10, 100), thetaBase64(prefix, 1, 10));
                }
                final double curTop = switch (region) {
                    case "a" -> 500;
                    case "b" -> 101;
                    default -> 99;
                };
                final int curUsers = switch (region) {
                    case "a" -> 3;
                    case "b" -> 11;
                    default -> 10;
                };
                appendSketchRow(elements, region, category, "cur",
                        kllBase64WithTop(curTop), thetaBase64(prefix, 1, curUsers));
            }
        }
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "aggregates",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ %s ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "category", "type": "string" },
                        { "name": "window", "type": "string" },
                        { "name": "lat_sk", "type": "string" },
                        { "name": "users_sk", "type": "string" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["aggregates"],
                      "parameters": {
                        "measures": [
                          { "name": "lat_sk", "type": "sketch", "format": "kll", "quantiles": [0.99] },
                          { "name": "users_sk", "type": "sketch", "format": "theta" }
                        ],
                        "comparison": {
                          "reference": {
                            "strategy": "external",
                            "labelField": "window",
                            "baselineLabel": "base",
                            "targetLabel": "cur"
                          }
                        },
                        "vocabulary": {
                          "dimensions": [
                            { "name": "region" },
                            { "name": "category" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """.formatted(elements);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        final MCollection output = outputs.get("attribution");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(rows -> {
            final List<MElement> list = toList(rows);
            Assertions.assertEquals(2, list.size());
            for(final MElement row : list) {
                Assertions.assertEquals("absoluteDelta", row.getAsString("epBasis"));
                Assertions.assertEquals(false, row.getPrimitiveValue("noFinding"));
                assertElements(row, "region=a");
                if("lat_sk".equals(row.getAsString("measure"))) {
                    Assertions.assertEquals(0.99, row.getAsDouble("quantile"), DELTA);
                    Assertions.assertEquals(100.0, row.getAsDouble("baseline"), DELTA);
                    Assertions.assertEquals(500.0, row.getAsDouble("target"), DELTA);
                } else {
                    Assertions.assertEquals("users_sk", row.getAsString("measure"));
                    Assertions.assertNull(row.getAsDouble("quantile"));
                    Assertions.assertEquals(20.0, row.getAsDouble("baseline"), DELTA);
                    Assertions.assertEquals(6.0, row.getAsDouble("target"), DELTA);
                    Assertions.assertEquals(60.0, row.getAsDouble("totalBaseline"), DELTA);
                    Assertions.assertEquals(48.0, row.getAsDouble("totalTarget"), DELTA);
                }
            }
            return null;
        });

        pipeline.run();
    }

    private static void appendSketchRow(
            final StringBuilder elements, final String region, final String category,
            final String window, final String latSketch, final String usersSketch) {
        if(!elements.isEmpty()) {
            elements.append(",");
        }
        elements.append(String.format(
                "{ \"region\": \"%s\", \"category\": \"%s\", \"window\": \"%s\", \"lat_sk\": \"%s\", \"users_sk\": \"%s\" }",
                region, category, window, latSketch, usersSketch));
    }

    /** KLL sketch of values from..to in steps of 10, base64-encoded. */
    private static String kllBase64(final double from, final double to) {
        final org.apache.datasketches.kll.KllDoublesSketch sketch =
                org.apache.datasketches.kll.KllDoublesSketch.newHeapInstance(200);
        for(double v = from; v <= to; v += 10) {
            sketch.update(v);
        }
        return java.util.Base64.getEncoder().encodeToString(sketch.toByteArray());
    }

    /** KLL sketch of 10..90 plus the given top value, base64-encoded. */
    private static String kllBase64WithTop(final double top) {
        final org.apache.datasketches.kll.KllDoublesSketch sketch =
                org.apache.datasketches.kll.KllDoublesSketch.newHeapInstance(200);
        for(double v = 10; v <= 90; v += 10) {
            sketch.update(v);
        }
        sketch.update(top);
        return java.util.Base64.getEncoder().encodeToString(sketch.toByteArray());
    }

    /** Theta sketch of identities prefix+from .. prefix+to, base64-encoded. */
    private static String thetaBase64(final String prefix, final int from, final int to) {
        final org.apache.datasketches.theta.UpdateSketch sketch =
                org.apache.datasketches.theta.UpdateSketch.builder().build();
        for(int i = from; i <= to; i++) {
            sketch.update(prefix + i);
        }
        return java.util.Base64.getEncoder().encodeToString(sketch.compact().toByteArray());
    }

    @Test
    public void testLeafCombineFnMergeAndAccumulatorRoundTrip() {
        // The accumulator must survive Java serialization (Beam fusion boundaries) with its
        // KLL and Theta sketches intact, and merging must be equivalent to direct accumulation
        final AttributionTransform.LeafCombineFn fn = new AttributionTransform.LeafCombineFn(1, 1, 1);

        AttributionTransform.LeafCombineFn.Accumulator first = fn.createAccumulator();
        first = fn.addInput(first, new AttributionTransform.LeafContribution(
                new double[]{2.0}, new double[]{10.0}, new String[]{"u1"}));
        first = fn.addInput(first, new AttributionTransform.LeafContribution(
                new double[]{3.0}, new double[]{20.0}, new String[]{"u2"}));
        final AttributionTransform.LeafCombineFn.Accumulator restored =
                org.apache.beam.sdk.util.SerializableUtils.clone(first);

        AttributionTransform.LeafCombineFn.Accumulator second = fn.createAccumulator();
        second = fn.addInput(second, new AttributionTransform.LeafContribution(
                new double[]{5.0}, new double[]{30.0}, new String[]{"u2"}));

        final AttributionTransform.LeafAggregate aggregate =
                fn.extractOutput(fn.mergeAccumulators(List.of(restored, second)));

        Assertions.assertEquals(3L, aggregate.rows);
        Assertions.assertEquals(10.0, aggregate.sums[0], DELTA);
        final org.apache.datasketches.kll.KllDoublesSketch kll = org.apache.datasketches.kll.KllDoublesSketch
                .heapify(org.apache.datasketches.memory.Memory.wrap(aggregate.kll[0]));
        Assertions.assertEquals(3L, kll.getN());
        Assertions.assertEquals(30.0, kll.getQuantile(0.99), DELTA);
        // u2 appears twice: the distinct estimate must be 2
        Assertions.assertEquals(2.0, org.apache.datasketches.theta.Sketches
                .heapifySketch(org.apache.datasketches.memory.Memory.wrap(aggregate.theta[0]))
                .getEstimate(), DELTA);
    }

    @Test
    public void testLeafCombineFnMergesPreSerializedSketchesWithRawInputs() {
        // A column can receive raw samples/identities and pre-serialized sketches in any mix
        final AttributionTransform.LeafCombineFn fn = new AttributionTransform.LeafCombineFn(0, 1, 1);

        final org.apache.datasketches.kll.KllDoublesSketch kll =
                org.apache.datasketches.kll.KllDoublesSketch.newHeapInstance(200);
        kll.update(20);
        kll.update(30);
        final org.apache.datasketches.theta.UpdateSketch theta =
                org.apache.datasketches.theta.UpdateSketch.builder().build();
        theta.update("u2");
        theta.update("u3");

        AttributionTransform.LeafCombineFn.Accumulator acc = fn.createAccumulator();
        acc = fn.addInput(acc, new AttributionTransform.LeafContribution(
                new double[0], new double[]{10.0}, new String[]{"u1"}));
        acc = fn.addInput(acc, new AttributionTransform.LeafContribution(
                new double[0], new double[]{Double.NaN}, new String[]{null},
                new byte[][]{kll.toByteArray()}, new byte[][]{theta.compact().toByteArray()}));

        final AttributionTransform.LeafAggregate aggregate = fn.extractOutput(acc);
        Assertions.assertEquals(3L, org.apache.datasketches.kll.KllDoublesSketch
                .heapify(org.apache.datasketches.memory.Memory.wrap(aggregate.kll[0])).getN());
        Assertions.assertEquals(3.0, org.apache.datasketches.theta.Sketches
                .heapifySketch(org.apache.datasketches.memory.Memory.wrap(aggregate.theta[0]))
                .getEstimate(), DELTA);
    }

    @Test
    public void testValidationErrors() throws Exception {
        // [config, expected message fragment]
        final String[][] cases = {
                {
                        // distribution measures take no expression
                        transform("""
                        "measures": [ { "name": "sales", "type": "distribution", "expression": "a / b" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "expression must not be set for type: distribution"
                },
                {
                        // quantiles out of range
                        transform("""
                        "measures": [ { "name": "sales", "type": "distribution", "quantiles": [1.5] } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "quantiles must be in (0, 1)"
                },
                {
                        // spec constraint 6: distribution measures cannot use shapley allocation
                        transform("""
                        "measures": [ { "name": "sales", "type": "distribution" } ],
                        "semantics": { "derivedAllocation": "shapley" },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "cannot be used with derivedAllocation: shapley"
                },
                {
                        // quantiles are not additive: no netDelta basis for distribution measures
                        transform("""
                        "measures": [ { "name": "sales", "type": "distribution" } ],
                        "semantics": { "epBasis": "netDelta" },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "always uses epBasis: absoluteDelta"
                },
                {
                        // no independence model for distributions
                        transform("""
                        "measures": [ { "name": "sales", "type": "distribution" } ],
                        "comparison": { "reference": { "strategy": "synthetic" } },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 1),
                        "cannot be used with the synthetic reference"
                },
                {
                        // quantiles are a distribution-only parameter
                        transform("""
                        "measures": [ { "name": "sales", "type": "distinct", "quantiles": [0.5] } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "quantiles must not be set for type: distinct"
                },
                {
                        // distinct estimates are not additive: no netDelta basis
                        transform("""
                        "measures": [ { "name": "sales", "type": "distinct" } ],
                        "semantics": { "epBasis": "netDelta" },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "type: distinct always uses epBasis: absoluteDelta"
                },
                {
                        // no independence model for identity sets
                        transform("""
                        "measures": [ { "name": "sales", "type": "distinct" } ],
                        "comparison": { "reference": { "strategy": "synthetic" } },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 1),
                        "type: distinct cannot be used with the synthetic reference"
                },
                {
                        // sketch measures require a format
                        transform("""
                        "measures": [ { "name": "region", "type": "sketch" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "format parameter is required for type: sketch"
                },
                {
                        // quantiles are meaningless for theta sketches
                        transform("""
                        "measures": [ { "name": "region", "type": "sketch", "format": "theta", "quantiles": [0.5] } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "quantiles must not be set for format: theta"
                },
                {
                        // sketch fields must carry serialized bytes (or base64 strings)
                        transform("""
                        "measures": [ { "name": "sales", "type": "sketch", "format": "kll" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "must be a bytes (serialized sketch) or string (base64) type"
                },
                {
                        // reserved comparison mode
                        transform("""
                        "measures": [ { "name": "sales" } ],
                        "comparison": { "mode": "series" },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "comparison.mode: series is reserved"
                },
                {
                        // external without labelField requires 2 inputs
                        transform("""
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 1),
                        "requires exactly 2 inputs"
                },
                {
                        // labelField without baselineLabel/targetLabel
                        transform("""
                        "measures": [ { "name": "sales" } ],
                        "comparison": { "reference": { "strategy": "external", "labelField": "region" } },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 1),
                        "baselineLabel and reference.targetLabel"
                },
                {
                        // unknown dimension field
                        transform("""
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": { "dimensions": [ { "name": "unknown_field" } ] }
                        """, 2),
                        "does not exist"
                },
                {
                        // reserved expressiveness
                        transform("""
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ], "expressiveness": "predicate" }
                        """, 2),
                        "expressiveness: predicate is reserved"
                },
                {
                        // calendar-ambiguous timeShift offset
                        transform("""
                        "measures": [ { "name": "sales" } ],
                        "comparison": { "reference": { "strategy": "timeShift", "timeShift": { "offset": "P1M" } } },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 1),
                        "timeShift.offset is invalid"
                },
                {
                        // netDelta explanatory power is undefined against a marginal baseline
                        transform("""
                        "measures": [ { "name": "sales" } ],
                        "comparison": { "reference": { "strategy": "synthetic" } },
                        "semantics": { "epBasis": "netDelta" },
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 1),
                        "epBasis: netDelta cannot be used"
                }
        };

        for(final String[] testCase : cases) {
            final Config config = Config.load(testCase[0]);
            final TestPipeline errorPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
            final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                    () -> MPipeline.apply(errorPipeline, config));
            Assertions.assertTrue(e.getMessage().contains(testCase[1]),
                    "expected message to contain [" + testCase[1] + "] but was: " + e.getMessage());
        }
    }

    private static String transform(final String parameters, final int inputCount) {
        final String source = """
            {
              "name": "%s",
              "module": "create",
              "parameters": {
                "type": "element",
                "elements": [ { "region": "a", "sales": 100 } ]
              },
              "schema": { "fields": [
                { "name": "region", "type": "string" },
                { "name": "sales", "type": "float64" }
              ] }
            }
            """;
        final StringBuilder sources = new StringBuilder();
        final StringBuilder inputs = new StringBuilder();
        for(int i = 0; i < inputCount; i++) {
            if(i > 0) {
                sources.append(",");
                inputs.append(",");
            }
            sources.append(String.format(source, "input" + i));
            inputs.append("\"input").append(i).append("\"");
        }
        return "{ \"sources\": [" + sources + "], \"transforms\": [ { "
                + "\"name\": \"attribution\", \"module\": \"attribution\", "
                + "\"inputs\": [" + inputs + "], \"parameters\": {" + parameters + "} } ] }";
    }

    private static List<MElement> toList(final Iterable<MElement> elements) {
        final List<MElement> rows = new ArrayList<>();
        for(final MElement element : elements) {
            rows.add(element);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static String describeElements(final MElement row) {
        final Object value = row.getPrimitiveValue("elements");
        final List<String> parts = new ArrayList<>();
        for(final Object entry : (List<Object>) value) {
            final Map<String, Object> map = (Map<String, Object>) entry;
            parts.add(asString(map.get("dimension")) + "=" + asString(map.get("value")));
        }
        return String.join(",", parts);
    }

    private static String asString(final Object value) {
        return value == null ? null : value.toString();
    }

    private static void assertElements(final MElement row, final String expected) {
        Assertions.assertEquals(expected, describeElements(row));
    }

    @Test
    public void testMinimalParametersUseDefaults() throws Exception {
        // Spec §4.3: measures + dimensions alone must run with
        // external + contribution + riskloc + top-3 report defaults
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "target",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "region": "a", "sales": 300 }, { "region": "b", "sales": 100 } ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    },
                    {
                      "name": "baseline",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "region": "a", "sales": 100 }, { "region": "b", "sales": 100 } ]
                      },
                      "schema": { "fields": [
                        { "name": "region", "type": "string" },
                        { "name": "sales", "type": "float64" }
                      ] }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "attribution",
                      "module": "attribution",
                      "inputs": ["target", "baseline"],
                      "parameters": {
                        "measures": [ { "name": "sales" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("attribution").getCollection()).satisfies(elements -> {
            final List<MElement> rows = toList(elements);
            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals("riskloc", rows.getFirst().getAsString("algorithm"));
            assertElements(rows.getFirst(), "region=a");
            return null;
        });

        pipeline.run();
    }
}
