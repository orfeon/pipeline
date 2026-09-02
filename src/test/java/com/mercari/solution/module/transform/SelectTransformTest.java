package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class SelectTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testStatelessSelect() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    timestampAttribute: field_ts
                    parameters:
                      type: element
                      elements:
                        - field_string: string_value1
                          field_long: 10
                          field_ts: "2025-01-01T00:00:00Z"
                          field_enum: a
                        - field_string: string_value2
                          field_long: 20
                          field_ts: "2025-01-01T00:00:01Z"
                          field_enum: b
                        - field_string: string_value3
                          field_long: 30
                          field_ts: "2025-01-01T00:00:02Z"
                          field_enum: c
                        - field_string: string_value4
                          field_long: 40
                          field_ts: "2025-01-01T00:00:03Z"
                          field_enum: a
                        - field_string: string_value5
                          field_long: 50
                          field_ts: "2025-01-01T00:00:04Z"
                          field_enum: e
                    schema:
                      fields:
                        - name: field_string
                          type: string
                        - name: field_long
                          type: int64
                        - name: field_ts
                          type: timestamp
                        - name: field_enum
                          type: enumeration
                          symbols:
                            - a
                            - b
                            - c
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: field_long_renamed
                          field: field_long
                        - name: field_long_casted_string
                          type: string
                          field: field_long
                        - name: field_enum_replaced
                          func: replace
                          field: field_enum
                          type: string
                          mapping:
                            a: A
                            b: B
                          default: C
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("select");

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
    public void testStatefulSelect() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    timestampAttribute: field_ts
                    parameters:
                      type: element
                      elements:
                        - field_string: string_value1
                          field_long: 10
                          field_ts: "2025-01-01T00:00:00Z"
                          field_nested:
                            field_string: nested_string_value1
                            field_long: -10
                        - field_string: string_value2
                          field_long: 20
                          field_ts: "2025-01-01T00:00:01Z"
                          field_nested:
                            field_string: nested_string_value2
                            field_long: -20
                        - field_string: string_value3
                          field_long: 30
                          field_ts: "2025-01-01T00:00:02Z"
                          field_nested:
                            field_string: nested_string_value3
                            field_long: -30
                        - field_string: string_value4
                          field_long: 40
                          field_ts: "2025-01-01T00:00:03Z"
                          field_nested:
                            field_string: nested_string_value4
                            field_long: -40
                        - field_string: string_value5
                          field_long: 50
                          field_ts: "2025-01-01T00:00:04Z"
                          field_nested:
                            field_string: nested_string_value5
                            field_long: -50
                    schema:
                      fields:
                        - name: field_string
                          type: string
                        - name: field_long
                          type: int64
                        - name: field_ts
                          type: timestamp
                        - name: field_nested
                          type: record
                          fields:
                            - name: field_string
                              type: string
                            - name: field_long
                              type: int64
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: field_long_sum_count3
                          func: sum
                          field: field_long
                          range:
                            count: 3
                        - name: field_long_sum_duration3
                          func: sum
                          field: field_long
                          range:
                            duration: 2
                        - name: field_long_avg_count3
                          func: avg
                          field: field_long
                          range:
                            count: 3
                        - name: field_long_min_count3
                          func: min
                          field: field_long
                          range:
                            count: 3
                        - name: field_long_argmin_count3
                          func: arg_min
                          field: field_string
                          comparingField: field_long
                          range:
                            count: 3
                        - name: field_long_string_array_agg_count3
                          func: array_agg
                          fields:
                            - field_long
                            - field_string
                          range:
                            count: 3
                        - name: field_nested_long_sum_count3
                          func: sum
                          field: field_nested.field_long
                          range:
                            count: 3
                        - name: field_sum_long_sum_count3
                          expression: "field_long_sum_count3 * 2"
                        - name: lag_long
                          func: lag
                          expression: "(field_long[2] - field_long[0]) / (2 * field_long[0])"
                        - name: field_long_array_agg_count3
                          func: array_agg
                          field: field_long
                          range:
                            count: 3
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("select");

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

    // outputFilter evaluates conditions on the computed output fields (the filter parameter
    // only sees input fields), with and/or nesting.
    @Test
    public void testOutputFilter() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - path: datasets/dim_users/daily/a.parquet
                        - path: datasets/events/weekly/b.parquet
                        - path: datasets/other/daily/c.parquet
                        - path: datasets/events/hourly/d.parquet
                        - path: malformed
                    schema:
                      fields:
                        - name: path
                          type: string
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      filter:
                        - key: path
                          op: match
                          value: "^datasets/[^/]+/[^/]+/.+$"
                      select:
                        - name: table
                          text: '${path?split("/")[1]}'
                        - name: frequency
                          text: '${path?split("/")[2]}'
                      outputFilter:
                        and:
                          - key: frequency
                            op: in
                            value: [daily, weekly, monthly]
                          - or:
                              - key: table
                                op: match
                                value: "^dim_.*"
                              - key: table
                                op: in
                                value: [events, users]
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("select").getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                final String table = row.getPrimitiveValue("table").toString();
                final String frequency = row.getPrimitiveValue("frequency").toString();
                org.junit.jupiter.api.Assertions.assertTrue(
                        ("dim_users".equals(table) && "daily".equals(frequency))
                                || ("events".equals(table) && "weekly".equals(frequency)));
                count++;
            }
            org.junit.jupiter.api.Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    // outputFilter applies after flattenField: individual flattened records are filtered.
    @Test
    public void testOutputFilterWithFlatten() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - body: '{"items":["a","b"]}'
                        - body: '{"items":["c"]}'
                    schema:
                      fields:
                        - name: body
                          type: string
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: item
                          func: json_path
                          field: body
                          path: "$.items"
                          mode: repeated
                      flattenField: item
                      outputFilter:
                        - key: item
                          op: "!="
                          value: b
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("select").getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                final String item = row.getPrimitiveValue("item").toString();
                org.junit.jupiter.api.Assertions.assertTrue("a".equals(item) || "c".equals(item));
                count++;
            }
            org.junit.jupiter.api.Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    // outputFilter also applies on the stateful select path.
    @Test
    public void testOutputFilterStateful() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    timestampAttribute: field_ts
                    parameters:
                      type: element
                      elements:
                        - field_long: 10
                          field_ts: "2025-01-01T00:00:00Z"
                        - field_long: 20
                          field_ts: "2025-01-01T00:00:01Z"
                        - field_long: 30
                          field_ts: "2025-01-01T00:00:02Z"
                        - field_long: 40
                          field_ts: "2025-01-01T00:00:03Z"
                    schema:
                      fields:
                        - name: field_long
                          type: int64
                        - name: field_ts
                          type: timestamp
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: field_long
                          field: field_long
                        - name: field_long_sum
                          func: sum
                          field: field_long
                          range:
                            count: 3
                      outputFilter:
                        - key: field_long
                          op: ">="
                          value: 30
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("select").getCollection()).satisfies(rows -> {
            // pins the documented contract: the state is updated before the output filter applies,
            // so the filtered-out records (10, 20) still contribute to the windowed sums
            final Map<Long, Long> expectedSums = Map.of(30L, 60L, 40L, 90L);
            int count = 0;
            for (final MElement row : rows) {
                final long fieldLong = (long) row.getPrimitiveValue("field_long");
                org.junit.jupiter.api.Assertions.assertTrue(fieldLong >= 30L);
                org.junit.jupiter.api.Assertions.assertEquals(
                        expectedSums.get(fieldLong), row.getPrimitiveValue("field_long_sum"));
                count++;
            }
            org.junit.jupiter.api.Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    // outputFilter compares timestamp fields with the typed representation (ISO string literals),
    // as documented for filter conditions.
    @Test
    public void testOutputFilterTimestamp() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - id: a
                          field_ts: "2025-01-01T00:00:00Z"
                        - id: b
                          field_ts: "2025-01-02T00:00:00Z"
                        - id: c
                          field_ts: "2025-01-03T00:00:00Z"
                    schema:
                      fields:
                        - name: id
                          type: string
                        - name: field_ts
                          type: timestamp
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: id
                          field: id
                        - name: field_ts
                          field: field_ts
                      outputFilter:
                        - key: field_ts
                          op: ">="
                          value: "2025-01-02T00:00:00Z"
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        PAssert.that(outputs.get("select").getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                final String id = row.getPrimitiveValue("id").toString();
                org.junit.jupiter.api.Assertions.assertTrue("b".equals(id) || "c".equals(id));
                count++;
            }
            org.junit.jupiter.api.Assertions.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    // outputFilter field names are validated against the output schema at assembly time.
    @Test
    public void testOutputFilterValidation() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - value: a
                    schema:
                      fields:
                        - name: value
                          type: string
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: renamed
                          field: value
                      outputFilter:
                        - key: value
                          op: "="
                          value: a
                """;
        final Config config = Config.load(configYaml);
        final com.mercari.solution.module.IllegalModuleException e = org.junit.jupiter.api.Assertions.assertThrows(
                com.mercari.solution.module.IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("outputFilter"), e.getMessage());
    }

    // The SQL-like text form is validated against the output schema too, and an empty condition
    // (which would match no record) is rejected at assembly time.
    @Test
    public void testOutputFilterValidationTextAndEmpty() throws IOException {
        final String base = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - value: a
                    schema:
                      fields:
                        - name: value
                          type: string
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: renamed
                          field: value
                      outputFilter: %s
                """;

        final Config textConfig = Config.load(base.formatted("\"renamd = 'a'\""));   // typo'd field in SQL text form
        final com.mercari.solution.module.IllegalModuleException e1 = org.junit.jupiter.api.Assertions.assertThrows(
                com.mercari.solution.module.IllegalModuleException.class,
                () -> MPipeline.apply(TestPipeline.create().enableAbandonedNodeEnforcement(false), textConfig));
        org.junit.jupiter.api.Assertions.assertTrue(e1.getMessage().contains("outputFilter"), e1.getMessage());

        final Config emptyConfig = Config.load(base.formatted("[]"));
        final com.mercari.solution.module.IllegalModuleException e2 = org.junit.jupiter.api.Assertions.assertThrows(
                com.mercari.solution.module.IllegalModuleException.class,
                () -> MPipeline.apply(TestPipeline.create().enableAbandonedNodeEnforcement(false), emptyConfig));
        org.junit.jupiter.api.Assertions.assertTrue(e2.getMessage().contains("empty"), e2.getMessage());
    }

    // Regression test: json_path `mode: repeated` must yield an array type also when `type` is
    // omitted (default string) — it used to be ignored without an explicit `type`, which broke
    // a subsequent flattenField with "is not array type".
    @Test
    public void testJsonPathRepeatedModeWithoutType() throws IOException {
        final String configYaml = """
                sources:
                  - name: create
                    module: create
                    parameters:
                      type: element
                      elements:
                        - body: '{"items":["a","b"]}'
                        - body: '{"items":["c"]}'
                    schema:
                      fields:
                        - name: body
                          type: string
                transforms:
                  - name: select
                    module: select
                    inputs:
                      - create
                    parameters:
                      select:
                        - name: item
                          func: json_path
                          field: body
                          path: "$.items"
                          mode: repeated
                      flattenField: item
                """;
        final Config config = Config.load(configYaml);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("select");

        PAssert.that(output.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final MElement row : rows) {
                final String item = row.getPrimitiveValue("item").toString();
                org.junit.jupiter.api.Assertions.assertTrue(
                        "a".equals(item) || "b".equals(item) || "c".equals(item));
                count++;
            }
            org.junit.jupiter.api.Assertions.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }
}
