package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PartitionTransformTest {

    private static final double DELTA = 1e-15;

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void test1() throws Exception {

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "create1",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [3, 0, 2, 1]
                      },
                      "timestampAttribute": "sequence"
                    },
                    {
                      "name": "create2",
                      "module": "create",
                      "parameters": {
                        "type": "int64",
                        "elements": [5, 1, 4, 3]
                      },
                      "timestampAttribute": "sequence"
                    }
                  ],
                  "transforms": [
                    {
                      "name": "partition",
                      "module": "partition",
                      "inputs": ["create1", "create2"],
                      "parameters": {
                        "exclusive": true,
                        "partitions": [
                          {
                            "name": "output1",
                            "filter": [
                              { "key": "value", "op": "in", "value": [1, 2, 3] }
                            ]
                          },
                          {
                            "name": "output2",
                            "filter": [
                              { "key": "value", "op": "in", "value": [4, 5, 6] }
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

        final MCollection output1 = outputs.get("partition.output1");
        PAssert.that(output1.getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                Assertions.assertTrue((long)element.getPrimitiveValue("value") > 0 && (long)element.getPrimitiveValue("value") < 4);
                count++;
            }
            Assertions.assertEquals(5, count);
            return null;
        });

        final MCollection output2 = outputs.get("partition.output2");
        PAssert.that(output2.getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                Assertions.assertTrue((long)element.getPrimitiveValue("value") > 3 && (long)element.getPrimitiveValue("value") < 7);
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    // Regression test: a partition without `select` must pass the input element through unchanged.
    // The select transform upstream produces AVRO-backed elements; rebuilding them as primitive maps
    // used to break the partition output coder (`Illegal data type: ELEMENT for AvroCoder`).
    @Test
    public void testPartitionWithoutSelectAfterSelectTransform() throws Exception {

        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - category: a
                          id: 1
                        - category: b
                          id: 2
                        - category: c
                          id: 3
                        - category: d
                          id: 4
                    schema:
                      fields:
                        - name: category
                          type: string
                        - name: id
                          type: int64
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: category
                          field: category
                        - name: id
                          field: id
                  - name: partition
                    module: partition
                    inputs:
                      - select
                    parameters:
                      exclusive: true
                      partitions:
                        - name: group1
                          filter:
                            - { key: category, op: in, value: [a, b] }
                        - name: group2
                          filter:
                            - { key: category, op: "=", value: c }
                """;

        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("partition.group1").getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                final String category = element.getPrimitiveValue("category").toString();
                Assertions.assertTrue("a".equals(category) || "b".equals(category));
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        PAssert.that(outputs.get("partition.group2").getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                Assertions.assertEquals("c", element.getPrimitiveValue("category").toString());
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputs.get("partition.excluded").getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                Assertions.assertEquals("d", element.getPrimitiveValue("category").toString());
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }

    // Multi-input case: the per-partition and excluded coders are built over the rebuilt
    // (ELEMENT-typed) union schema, and elements from the second input carry index 1 —
    // both the matched and the excluded path must adapt AVRO-backed elements to that coder.
    @Test
    public void testPartitionWithoutSelectMultiInput() throws Exception {

        final String configYaml = """
                sources:
                  - name: create1
                    module: create
                    parameters:
                      type: element
                      elements:
                        - category: a
                        - category: b
                    schema:
                      fields:
                        - name: category
                          type: string
                  - name: create2
                    module: create
                    parameters:
                      type: element
                      elements:
                        - category: c
                        - category: d
                    schema:
                      fields:
                        - name: category
                          type: string
                transforms:
                  - name: select1
                    module: select
                    inputs:
                      - create1
                    parameters:
                      select:
                        - name: category
                          field: category
                  - name: select2
                    module: select
                    inputs:
                      - create2
                    parameters:
                      select:
                        - name: category
                          field: category
                  - name: partition
                    module: partition
                    inputs:
                      - select1
                      - select2
                    parameters:
                      partitions:
                        - name: matched
                          filter:
                            - { key: category, op: in, value: [a, c] }
                """;

        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("partition.matched").getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                final String category = element.getPrimitiveValue("category").toString();
                Assertions.assertTrue("a".equals(category) || "c".equals(category));
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        PAssert.that(outputs.get("partition.excluded").getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                final String category = element.getPrimitiveValue("category").toString();
                Assertions.assertTrue("b".equals(category) || "d".equals(category));
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    // union: true merges matched outputs into the default output, whose coder is built over
    // the rebuilt (ELEMENT-typed) union of the partition output schemas — a select-less
    // partition must adapt AVRO-backed elements to it even with a single input.
    @Test
    public void testPartitionWithoutSelectUnion() throws Exception {

        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - category: a
                        - category: b
                    schema:
                      fields:
                        - name: category
                          type: string
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: category
                          field: category
                  - name: partition
                    module: partition
                    inputs:
                      - select
                    parameters:
                      union: true
                      partitions:
                        - name: matched
                          filter:
                            - { key: category, op: "=", value: a }
                """;

        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("partition").getCollection()).satisfies(elements -> {
            int count = 0;
            for (final MElement element : elements) {
                Assertions.assertEquals("a", element.getPrimitiveValue("category").toString());
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run();
    }
}
