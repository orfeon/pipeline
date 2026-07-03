package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.IllegalModuleException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FilterTransformTest {

    private static final double DELTA = 1e-15;

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void test() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1, 2, 3],
                        "select": [
                          { "name": "sequence" },
                          { "name": "data", "type": "json", "value": '[{ "fieldA": "value1", "fieldB": "value2" },{ "fieldA": "value3", "fieldB": "value4" }]' },
                          { "name": "message", "func": "struct", "mode": "repeated", "fields": [
                            { "name": "field1", "type": "string", "value": "str1" },
                            { "name": "field2", "type": "string", "value": "str2" }
                          ] }
                        ]
                      },
                      "timestampAttribute": "sequence"
                    }
                  ],
                  "transforms": [
                    {
                      "name": "filter",
                      "module": "select",
                      "inputs": ["create"],
                      "parameters": {
                        "select": [
                          { "name": "events", "func": "struct", "mode": "repeated", "fields": [
                            { "name": "id", "func": "json_path", "field": "data", "path": "$.fieldA" },
                            { "name": "description", "func": "json_path", "field": "data", "path": "$.fieldB" }
                          ], "each": "data" }
                        ],
                        "flattenField": "events"
                      }
                    },
                    {
                      "name": "filter2",
                      "module": "select",
                      "inputs": ["filter"],
                      "parameters": {
                        "select": [
                            { "name": "constantValue", "type": "string", "value": "1234567890" },
                            { "name": "id", "type": "string", "field": "events.id" },
                            { "name": "events", "func": "struct", "mode": "repeated", "fields": [
                              { "name": "name", "type": "string", "value": "events.description" },
                              { "name": "uid", "func": "hash", "text": "myevent#" },
                              { "name": "properties", "func": "struct", "fields": [
                                { "name": "key1", "func": "struct", "fields": [
                                  { "name": "name", "field": "events.id" }
                                ]},
                                { "name": "key2", "func": "struct", "fields": [
                                  { "name": "description", "field": "events.description" }
                                ]}
                              ]}
                            ]}
                          ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("filter2");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                System.out.println(row);
                count++;
            }
            System.out.println(count);
            //Assertions.assertEquals(3, count);
            return null;
        });

        pipeline.run();

    }

    @Test
    public void testFilterModuleFilterOnly() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "filterOnlyCreate",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1, 2, 3, 4, 5]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "filterOnly",
                      "module": "filter",
                      "inputs": ["filterOnlyCreate"],
                      "parameters": {
                        "filter": [
                          { "key": "value", "op": ">", "value": 2 }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("filterOnly");
        Assertions.assertNotNull(output);

        // filter only keeps input schema as-is
        Assertions.assertNotNull(output.getSchema().getField("value"));
        Assertions.assertNotNull(output.getSchema().getField("sequence"));

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<Long> values = new HashSet<>();
            for(final MElement element : elements) {
                values.add((Long) element.getPrimitiveValue("value"));
            }
            Assertions.assertEquals(Set.of(3L, 4L, 5L), values);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testFilterModuleFilterAndSelect() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "filterSelectCreate",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1, 2, 3, 4, 5]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "filterSelect",
                      "module": "filter",
                      "inputs": ["filterSelectCreate"],
                      "parameters": {
                        "filter": [
                          { "key": "value", "op": "<", "value": 2 }
                        ],
                        "select": [
                          { "name": "id", "field": "value" },
                          { "name": "doubled", "expression": "value * 2", "type": "float64" },
                          { "name": "constantValue", "type": "string", "value": "fixed" }
                        ]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("filterSelect");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                final long id = (Long) element.getPrimitiveValue("id");
                Assertions.assertTrue(id < 2, "id: " + id);
                Assertions.assertEquals(id * 2.0D, (Double) element.getPrimitiveValue("doubled"), DELTA);
                Assertions.assertEquals("fixed", element.getAsString("constantValue"));
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testFilterModuleValidationError() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "filterInvalidCreate",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [0, 1]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "filterInvalid",
                      "module": "filter",
                      "inputs": ["filterInvalidCreate"],
                      "parameters": {}
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

}
