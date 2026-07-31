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
    public void testValidationErrors() throws Exception {
        // [config, expected message fragment]
        final String[][] cases = {
                {
                        // reserved measure type
                        transform("""
                        "measures": [ { "name": "sales", "type": "distribution" } ],
                        "vocabulary": { "dimensions": [ { "name": "region" } ] }
                        """, 2),
                        "measures.type: distribution is reserved"
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
