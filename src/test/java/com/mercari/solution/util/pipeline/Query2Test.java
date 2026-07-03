package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Query2Test {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    /**
     * In-memory lookup source for tests: full rows are held in schema column
     * order and filtered by the requested key prefix (point) or prefix + range.
     * Records the number of lookup invocations and the last batch size.
     */
    private static class MemoryLookupSource extends LookupSource {

        private final LinkedHashMap<String, Schema> schemas;
        private final HashMap<String, List<LookupKey>> keys;
        private final HashMap<String, List<Object[]>> rows;

        int lookupCount;
        int lastBatchSize;

        MemoryLookupSource(final String name) {
            super(name);
            this.schemas = new LinkedHashMap<>();
            this.keys = new HashMap<>();
            this.rows = new HashMap<>();
        }

        MemoryLookupSource withTable(final String table, final Schema schema,
                final List<LookupKey> candidates, final List<Object[]> tableRows) {
            schemas.put(table, schema);
            keys.put(table, candidates);
            rows.put(table, tableRows);
            return this;
        }

        @Override
        protected void setupInternal() {
        }

        @Override
        protected void closeInternal() {
        }

        @Override
        public Map<String, Schema> tableSchemas() {
            return schemas;
        }

        @Override
        public List<LookupKey> keyCandidates(final String table) {
            return keys.getOrDefault(table, List.of());
        }

        @Override
        public Iterable<Object[]> lookup(final String table, final String indexName,
                final LookupBatch batch, final int[] projects) {
            lookupCount++;
            lastBatchSize = batch.requests().size();

            final Schema schema = schemas.get(table);
            final List<String> keyColumns = keyColumns(table, indexName);
            final int[] keyIndexes = new int[keyColumns.size()];
            for (int i = 0; i < keyColumns.size(); i++) {
                keyIndexes[i] = indexOfField(schema, keyColumns.get(i));
            }

            final List<Object[]> result = new ArrayList<>();
            for (final Object[] row : rows.get(table)) {
                if (matches(row, keyIndexes, batch)) {
                    result.add(project(row, projects, schema.countFields()));
                }
            }
            return result;
        }

        private List<String> keyColumns(final String table, final String indexName) {
            for (final LookupKey candidate : keys.get(table)) {
                if (indexName == null ? candidate.isPrimaryKey()
                        : indexName.equals(candidate.indexName())) {
                    return candidate.columns();
                }
            }
            throw new IllegalStateException("unknown index: " + indexName);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private boolean matches(final Object[] row, final int[] keyIndexes,
                final LookupBatch batch) {
            for (final LookupRequest request : batch.requests()) {
                boolean prefixEquals = true;
                for (int i = 0; i < request.prefix().size(); i++) {
                    if (!request.prefix().get(i).equals(row[keyIndexes[i]])) {
                        prefixEquals = false;
                        break;
                    }
                }
                if (!prefixEquals) {
                    continue;
                }
                if (!request.isRange()) {
                    return true;
                }
                final Comparable value = (Comparable) row[keyIndexes[request.prefix().size()]];
                final int cl = value.compareTo(request.lower());
                final int cu = value.compareTo(request.upper());
                if ((cl > 0 || (cl == 0 && request.lowerInclusive()))
                        && (cu < 0 || (cu == 0 && request.upperInclusive()))) {
                    return true;
                }
            }
            return false;
        }

        private static Object[] project(final Object[] row, final int[] projects, final int all) {
            if (projects == null) {
                return row.clone();
            }
            final Object[] out = new Object[projects.length];
            for (int i = 0; i < projects.length; i++) {
                out[i] = row[projects[i]];
            }
            return out;
        }

        private static int indexOfField(final Schema schema, final String name) {
            for (int i = 0; i < schema.countFields(); i++) {
                if (schema.getField(i).getName().equals(name)) {
                    return i;
                }
            }
            throw new IllegalStateException("unknown field: " + name);
        }
    }

    private static Schema usersSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.INT64),
                Schema.Field.of("name", Schema.FieldType.STRING),
                Schema.Field.of("price", Schema.FieldType.FLOAT64)));
    }

    private static MemoryLookupSource usersSource() {
        return new MemoryLookupSource("mem")
                .withTable("users", usersSchema(),
                        List.of(LookupKey.primaryKey(List.of("userId"))),
                        List.of(
                                new Object[]{1L, "alice", 2.0d},
                                new Object[]{2L, "bob", 3.0d}));
    }

    private static Schema inputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.INT64),
                Schema.Field.of("qty", Schema.FieldType.INT64)));
    }

    @Test
    public void testPointLookupJoin() {
        final MemoryLookupSource source = usersSource();
        final String sql = """
                SELECT
                  i.userId AS userId,
                  u.name AS name,
                  i.qty * u.price AS total
                FROM
                  INPUT AS i
                JOIN
                  mem.users AS u
                ON
                  u.userId = i.userId
                """;
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build();

        final Schema outputSchema = query.getOutputSchema();
        Assertions.assertEquals(3, outputSchema.countFields());

        query.setup();
        try {
            final List<MElement> inputs = List.of(
                    MElement.of(Map.of("userId", 1L, "qty", 2L), TIMESTAMP),
                    MElement.of(Map.of("userId", 3L, "qty", 5L), TIMESTAMP));
            final List<MElement> outputs = query.execute(inputs, TIMESTAMP);

            // userId=3 has no match (INNER join drops it)
            Assertions.assertEquals(1, outputs.size());
            final MElement output = outputs.getFirst();
            Assertions.assertEquals(1L, output.getAsLong("userId"));
            Assertions.assertEquals("alice", output.getAsString("name"));
            Assertions.assertEquals(4.0d, output.getAsDouble("total"), 1e-9);

            // One batched lookup call for the whole input list, two key requests
            Assertions.assertEquals(1, source.lookupCount);
            Assertions.assertEquals(2, source.lastBatchSize);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLeftLookupJoin() {
        final MemoryLookupSource source = usersSource();
        final String sql = """
                SELECT
                  i.userId AS userId,
                  u.name AS name
                FROM
                  INPUT AS i
                LEFT JOIN
                  mem.users AS u
                ON
                  u.userId = i.userId
                """;
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql(sql)
                .build();

        query.setup();
        try {
            final List<MElement> inputs = List.of(
                    MElement.of(Map.of("userId", 1L, "qty", 2L), TIMESTAMP),
                    MElement.of(Map.of("userId", 3L, "qty", 5L), TIMESTAMP));
            final List<MElement> outputs = query.execute(inputs, TIMESTAMP);

            Assertions.assertEquals(2, outputs.size());
            final Map<Long, MElement> byUser = new HashMap<>();
            for (final MElement output : outputs) {
                byUser.put(output.getAsLong("userId"), output);
            }
            Assertions.assertEquals("alice", byUser.get(1L).getAsString("name"));
            Assertions.assertNull(byUser.get(3L).getPrimitiveValue("name"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testStandaloneScanIsRejected() {
        final MemoryLookupSource source = usersSource();
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSource(source)
                .withSql("SELECT userId, name FROM mem.users")
                .build();

        query.setup();
        try {
            final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                    () -> query.execute(List.of(
                            MElement.of(Map.of("userId", 1L, "qty", 1L), TIMESTAMP)), TIMESTAMP));
            Assertions.assertTrue(rootMessage(e).contains("standalone scans are not supported"),
                    "unexpected message: " + rootMessage(e));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testRepeatedExecutionWithoutSources() {
        // Query2 must cover the plain per-element SQL of Query (no sources).
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("events", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("category", Schema.FieldType.STRING),
                        Schema.Field.of("amount", Schema.FieldType.INT64)))))));
        final String sql = """
                SELECT userId, COUNT(*) AS cnt, SUM(e.amount) AS total
                FROM MyTable, UNNEST(events) AS e
                GROUP BY userId
                """;
        final Query2 query = Query2.of("MyTable", schema, sql);
        query.setup();
        try {
            for (long i = 1; i <= 3; i++) {
                final MElement input = MElement.of(Map.of(
                        "userId", "u" + i,
                        "events", List.of(
                                Map.of("category", "a", "amount", i),
                                Map.of("category", "b", "amount", i * 10))), TIMESTAMP);
                final List<MElement> outputs =
                        query.execute(Map.of("MyTable", List.of(input)), TIMESTAMP);
                Assertions.assertEquals(1, outputs.size());
                Assertions.assertEquals(2L, outputs.getFirst().getAsLong("cnt"));
                Assertions.assertEquals(i + i * 10, outputs.getFirst().getAsLong("total"));
            }
        } finally {
            query.teardown();
        }
    }

    private static String rootMessage(final Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return String.valueOf(t.getMessage());
    }

    /** Host of a scalar UDF (public static overloads become SQL overloads). */
    public static class TestUdfs {

        public static long scale(long value, long factor) {
            return value * factor;
        }

        public static double scale(double value, long factor) {
            return value * factor;
        }
    }

    /** Aggregate UDF following Calcite's accumulator convention. */
    public static class SumOfSquares {

        public long init() {
            return 0L;
        }

        public long add(long accumulator, long value) {
            return accumulator + value * value;
        }

        public long merge(long a, long b) {
            return a + b;
        }

        public long result(long accumulator) {
            return accumulator;
        }
    }

    @Test
    public void testScalarUdf() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withScalarFunction("SCALE_", TestUdfs.class, "scale")
                .withSql("SELECT userId, SCALE_(qty, 10) AS scaled FROM INPUT")
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(MElement.of(Map.of("userId", 1L, "qty", 3L), TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(30L, outputs.getFirst().getAsLong("scaled"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testAggregateUdf() {
        final Schema schema = Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.STRING),
                Schema.Field.of("events", Schema.FieldType.array(Schema.FieldType.element(List.of(
                        Schema.Field.of("category", Schema.FieldType.STRING),
                        Schema.Field.of("amount", Schema.FieldType.INT64)))))));
        final Query2 query = Query2.builder()
                .withInput("MyTable", schema)
                .withAggregateFunction("SUM_OF_SQUARES", SumOfSquares.class)
                .withSql("""
                        SELECT userId, SUM_OF_SQUARES(e.amount) AS ss
                        FROM MyTable, UNNEST(events) AS e
                        GROUP BY userId
                        """)
                .build();
        query.setup();
        try {
            final MElement input = MElement.of(Map.of(
                    "userId", "u1",
                    "events", List.of(
                            Map.of("category", "a", "amount", 2L),
                            Map.of("category", "b", "amount", 3L))), TIMESTAMP);
            final List<MElement> outputs =
                    query.execute(Map.of("MyTable", List.of(input)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(13L, outputs.getFirst().getAsLong("ss")); // 4 + 9
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testStandardFunctionSurface() {
        // Smoke test over representative functions from each documented
        // category (string / numeric / datetime / conditional / array / cast).
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSql("""
                        SELECT
                          SUBSTR('hello', 2, 3) AS s1,
                          REGEXP_REPLACE('a1b2', '[0-9]', '') AS s2,
                          STARTS_WITH('abc', 'ab') AS b1,
                          POWER(2, 10) AS n1,
                          MOD(10, 3) AS n2,
                          SAFE_DIVIDE(1.0, 0.0) AS n3,
                          IF(1 > 0, 'y', 'n') AS c1,
                          IFNULL(CAST(NULL AS VARCHAR), 'x') AS c2,
                          GREATEST(1, 5, 3) AS c3,
                          UNIX_MILLIS(TIMESTAMP_MILLIS(1234)) AS t1,
                          ARRAY_LENGTH(ARRAY[1, 2, 3]) AS a1,
                          ARRAY_TO_STRING(ARRAY['a', 'b'], '-') AS a2,
                          SAFE_CAST('x' AS INTEGER) AS v1
                        FROM INPUT
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(MElement.of(Map.of("userId", 1L, "qty", 1L), TIMESTAMP)), TIMESTAMP);
            final MElement output = outputs.getFirst();
            Assertions.assertEquals("ell", output.getAsString("s1"));
            Assertions.assertEquals("ab", output.getAsString("s2"));
            Assertions.assertEquals(true, output.getPrimitiveValue("b1"));
            Assertions.assertEquals(1024.0d, output.getAsDouble("n1"), 1e-9);
            Assertions.assertEquals(1L, output.getAsLong("n2"));
            Assertions.assertNull(output.getPrimitiveValue("n3"));
            Assertions.assertEquals("y", output.getAsString("c1"));
            Assertions.assertEquals("x", output.getAsString("c2"));
            Assertions.assertEquals(5L, output.getAsLong("c3"));
            Assertions.assertEquals(1234L, output.getAsLong("t1"));
            Assertions.assertEquals(3, ((Number) output.getPrimitiveValue("a1")).intValue());
            Assertions.assertEquals("a-b", output.getAsString("a2"));
            Assertions.assertNull(output.getPrimitiveValue("v1"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testBuiltinCurrentDateFunction() {
        // Parity with the old Query core: CURRENT_DATE_ is registered by default.
        final Query2 query = Query2.builder()
                .withInput("INPUT", inputSchema())
                .withSql("SELECT userId, CURRENT_DATE_('Asia/Tokyo') AS today FROM INPUT")
                .build();
        Assertions.assertEquals(Schema.Type.date,
                query.getOutputSchema().getField("today").getFieldType().getType());
        query.setup();
        try {
            final long before = java.time.LocalDate.now(
                    java.time.ZoneId.of("Asia/Tokyo")).toEpochDay();
            final List<MElement> outputs = query.execute(
                    List.of(MElement.of(Map.of("userId", 1L, "qty", 1L), TIMESTAMP)), TIMESTAMP);
            final long after = java.time.LocalDate.now(
                    java.time.ZoneId.of("Asia/Tokyo")).toEpochDay();
            final int today = (Integer) outputs.getFirst().getPrimitiveValue("today");
            Assertions.assertTrue(today >= before && today <= after,
                    "unexpected epoch day: " + today);
        } finally {
            query.teardown();
        }
    }
}
