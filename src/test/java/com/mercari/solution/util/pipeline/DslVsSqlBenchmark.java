package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Ad-hoc microbenchmark: per-element cost of the Filter/Select DSLs vs the
 * Query2 (Calcite SQL) engine. Not a regression test — run explicitly with
 * {@code mvn test -Dtest=DslVsSqlBenchmark}.
 */
public class DslVsSqlBenchmark {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static Schema schema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.INT64),
                Schema.Field.of("category", Schema.FieldType.STRING),
                Schema.Field.of("qty", Schema.FieldType.INT64),
                Schema.Field.of("price", Schema.FieldType.FLOAT64)));
    }

    private static MElement element(long i) {
        return MElement.of(Map.of(
                "userId", i % 100,
                "category", (i % 2 == 0) ? "A" : "B",
                "qty", i % 50,
                "price", 12.5d), TIMESTAMP);
    }

    @Test
    public void benchmark() {
        final Schema schema = schema();

        // --- Filter DSL ---
        final Filter.ConditionNode condition = Filter.parse("""
                [
                  { "key": "qty", "op": ">", "value": 10 },
                  { "key": "category", "op": "=", "value": "A" }
                ]
                """);

        // --- Select DSL ---
        final JsonArray selects = new Gson().fromJson("""
                [
                  { "name": "userId" },
                  { "name": "category" },
                  { "name": "amount", "expression": "qty * price", "type": "float64" }
                ]
                """, JsonArray.class);
        final List<SelectFunction> selectFunctions = SelectFunction.of(selects, schema.getFields());
        for (final SelectFunction f : selectFunctions) {
            f.setup();
        }

        // --- Query2: equivalent filter + projection ---
        final Query2 query = Query2.builder()
                .withInput("INPUT", schema)
                .withSql("SELECT userId, category, qty * price AS amount FROM INPUT WHERE qty > 10 AND category = 'A'")
                .build();
        query.setup();

        // --- Query2: aggregation over a 100-row in-memory batch (buffer-like) ---
        final Query2 aggQuery = Query2.builder()
                .withInput("INPUT", schema)
                .withSql("SELECT category, SUM(qty * price) AS total, COUNT(*) AS cnt FROM INPUT GROUP BY category")
                .build();
        aggQuery.setup();
        final List<MElement> batch100 = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            batch100.add(element(i));
        }

        // --- Query2: a 3-statement session (intermediate + 2 routed outputs) ---
        final Query2 session = Query2.builder()
                .withInput("INPUT", schema)
                .withQuery("enriched", "SELECT userId, category, qty * price AS amount FROM INPUT", false)
                .withQuery("high", "SELECT userId, amount FROM enriched WHERE amount >= 300", true)
                .withQuery("low", "SELECT userId, amount FROM enriched WHERE amount < 300", true)
                .build();
        session.setup();

        try {
            final List<MElement> elements = new ArrayList<>();
            for (long i = 0; i < 1024; i++) {
                elements.add(element(i));
            }

            long sink = 0;

            // warmup
            for (int i = 0; i < 200_000; i++) {
                sink += Filter.filter(condition, schema, elements.get(i % 1024)) ? 1 : 0;
                sink += SelectFunction.apply(selectFunctions, elements.get(i % 1024), TIMESTAMP).size();
            }
            for (int i = 0; i < 5_000; i++) {
                sink += query.execute(elements.get(i % 1024), TIMESTAMP).size();
                sink += aggQuery.execute(batch100, TIMESTAMP).size();
            }

            // filter DSL
            long start = System.nanoTime();
            final int filterIters = 2_000_000;
            for (int i = 0; i < filterIters; i++) {
                sink += Filter.filter(condition, schema, elements.get(i % 1024)) ? 1 : 0;
            }
            report("Filter DSL (2 conditions)", start, filterIters);

            // select DSL
            start = System.nanoTime();
            final int selectIters = 1_000_000;
            for (int i = 0; i < selectIters; i++) {
                sink += SelectFunction.apply(selectFunctions, elements.get(i % 1024), TIMESTAMP).size();
            }
            report("Select DSL (pass x2 + expression)", start, selectIters);

            // Query2 per element
            start = System.nanoTime();
            final int queryIters = 20_000;
            for (int i = 0; i < queryIters; i++) {
                sink += query.execute(elements.get(i % 1024), TIMESTAMP).size();
            }
            report("Query2 SQL filter+project (1 row)", start, queryIters);

            // Query2 aggregation over 100 rows
            start = System.nanoTime();
            final int aggIters = 20_000;
            for (int i = 0; i < aggIters; i++) {
                sink += aggQuery.execute(batch100, TIMESTAMP).size();
            }
            report("Query2 SQL GROUP BY over 100 rows", start, aggIters);

            // Query2 session: 3 statements per element
            for (int i = 0; i < 5_000; i++) {
                sink += session.executeAll(
                        Map.of("INPUT", List.of(elements.get(i % 1024))), TIMESTAMP).outputs().size();
            }
            start = System.nanoTime();
            final int sessionIters = 20_000;
            for (int i = 0; i < sessionIters; i++) {
                sink += session.executeAll(
                        Map.of("INPUT", List.of(elements.get(i % 1024))), TIMESTAMP).outputs().size();
            }
            report("Query2 session (3 statements)", start, sessionIters);

            System.out.println("(sink=" + sink + ")");
        } finally {
            query.teardown();
            aggQuery.teardown();
            session.teardown();
        }
    }

    private static void report(final String label, final long startNanos, final int iters) {
        final long elapsed = System.nanoTime() - startNanos;
        System.out.printf("%-40s %,12.0f ns/op  (%,d iters, %,d ms total)%n",
                label, (double) elapsed / iters, iters, elapsed / 1_000_000);
    }

}
