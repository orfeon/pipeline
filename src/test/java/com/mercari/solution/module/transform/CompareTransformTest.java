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
import java.util.Map;

public class CompareTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testCompare() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "compareCreate1",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "k1", "category": "A" },
                          { "id": "k2", "category": "B" },
                          { "id": "k3", "category": "C" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "category", "type": "string" }
                        ]
                      }
                    },
                    {
                      "name": "compareCreate2",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [
                          { "id": "k1", "category": "A" },
                          { "id": "k2", "category": "X" },
                          { "id": "k4", "category": "D" }
                        ]
                      },
                      "schema": {
                        "fields": [
                          { "name": "id", "type": "string" },
                          { "name": "category", "type": "string" }
                        ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "compare",
                      "module": "compare",
                      "inputs": ["compareCreate1", "compareCreate2"],
                      "parameters": {
                        "primaryKeyFields": ["id"]
                      }
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("compare");
        Assertions.assertNotNull(output);

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Map<String, MElement> results = new HashMap<>();
            for(final MElement element : elements) {
                results.put(element.getAsString("keys"), element);
            }

            // k1 is identical in both inputs and must not be output
            Assertions.assertEquals(3, results.size());
            Assertions.assertFalse(results.containsKey("k1"));

            // k2 exists in both inputs but differs in the category field
            final MElement k2 = results.get("k2");
            Assertions.assertNotNull(k2);
            Assertions.assertEquals("compare", k2.getAsString("table"));
            final String k2Missing = String.valueOf(k2.getPrimitiveValue("missingInputs"));
            Assertions.assertFalse(k2Missing.contains("compareCreate1"));
            Assertions.assertFalse(k2Missing.contains("compareCreate2"));
            final String k2Differences = String.valueOf(k2.getPrimitiveValue("differences"));
            Assertions.assertTrue(k2Differences.contains("category"), "differences: " + k2Differences);

            // k3 exists only in input1
            final MElement k3 = results.get("k3");
            Assertions.assertNotNull(k3);
            final String k3Missing = String.valueOf(k3.getPrimitiveValue("missingInputs"));
            Assertions.assertTrue(k3Missing.contains("compareCreate2"), "missingInputs: " + k3Missing);

            // k4 exists only in input2
            final MElement k4 = results.get("k4");
            Assertions.assertNotNull(k4);
            final String k4Missing = String.valueOf(k4.getPrimitiveValue("missingInputs"));
            Assertions.assertTrue(k4Missing.contains("compareCreate1"), "missingInputs: " + k4Missing);

            return null;
        });

        pipeline.run();
    }

    @Test
    public void testCompareMissingPrimaryKeyFields() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "compareInvalidCreate",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [1, 2, 3]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "compare",
                      "module": "compare",
                      "inputs": ["compareInvalidCreate"],
                      "parameters": {}
                    }
                  ]
                }
                """;

        final Config config = Config.load(configJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> MPipeline.apply(pipeline, config));
    }

}
