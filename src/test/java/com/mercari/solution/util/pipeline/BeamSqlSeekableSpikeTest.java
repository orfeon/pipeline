package com.mercari.solution.util.pipeline;

import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupRequest;
import com.mercari.solution.util.pipeline.lookup.source.JdbcLookupSource;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Feasibility spike (kept as a regression pin): the query module's
 * LookupSource adapters can serve Beam SQL joins via the native
 * {@code BeamSqlSeekableTable} mechanism — a thin adapter converts
 * {@code seekRow} into a single-point {@code lookup()}. Findings this test
 * pins down:
 *
 * <ul>
 *   <li>{@code BeamSideInputLookupJoinRule} routes the join through
 *       {@code seekRow}; the sub-row's schema carries the seekable table's
 *       join-column NAMES, so candidate-key matching works by name.</li>
 *   <li>{@code seekRow} is called once per left row — no batching or key
 *       dedup (unlike the query module's 512-row batches).</li>
 *   <li><b>Beam quirk</b>: the equi-condition must put the main-input column
 *       on the LEFT ({@code c.userId = u.USER_ID}); the reverse order crashes
 *       inside Beam's {@code JoinAsLookup.joinFieldsMapping}
 *       (ArrayIndexOutOfBoundsException).</li>
 * </ul>
 */
public class BeamSqlSeekableSpikeTest {

    private static final String URL = "jdbc:h2:mem:seekablespike;DB_CLOSE_DELAY=-1";

    private final transient TestPipeline pipeline =
            TestPipeline.create().enableAbandonedNodeEnforcement(false);

    /** Seekable-table adapter over a LookupSource (point equi-join keys only). */
    public static class LookupSeekableTable extends SchemaBaseBeamTable
            implements BeamSqlSeekableTable {

        static final ConcurrentLinkedQueue<String> SEEK_ROW_SHAPES =
                new ConcurrentLinkedQueue<>();

        private final JdbcLookupSource source;
        private final String table;

        LookupSeekableTable(Schema schema, JdbcLookupSource source, String table) {
            super(schema);
            this.source = source;
            this.table = table;
        }

        @Override
        public void setUp(Schema joinSubsetType) {
            source.setup();
        }

        @Override
        public void tearDown() {
            source.close();
        }

        @Override
        public List<Row> seekRow(Row keyRow) {
            SEEK_ROW_SHAPES.add(String.valueOf(keyRow.getSchema().getFieldNames()));
            // Match the seek columns against a candidate key (name-insensitive
            // ordering handled by requiring exact list equality here).
            final List<String> seekColumns = keyRow.getSchema().getFieldNames();
            String indexName = null;
            boolean matched = false;
            for (final LookupKey candidate : source.keyCandidates(table)) {
                if (candidate.columns().equals(seekColumns)) {
                    indexName = candidate.indexName();
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw new IllegalStateException("join columns " + seekColumns
                        + " do not match a candidate key of " + table);
            }
            final List<Object> keyValues = new ArrayList<>();
            for (int i = 0; i < seekColumns.size(); i++) {
                keyValues.add(keyRow.getValue(i));
            }
            final Iterable<Object[]> rows = source.lookup(table, indexName,
                    LookupBatch.of(List.of(LookupRequest.point(keyValues))), null);
            final List<Row> result = new ArrayList<>();
            for (final Object[] values : rows) {
                final Row.Builder builder = Row.withSchema(getSchema());
                for (final Object value : values) {
                    builder.addValue(value);
                }
                result.add(builder.build());
            }
            return result;
        }

        @Override
        public PCollection<Row> buildIOReader(PBegin begin) {
            throw new UnsupportedOperationException(
                    "lookup table '" + table + "' cannot be scanned");
        }

        @Override
        public POutput buildIOWriter(PCollection<Row> input) {
            throw new UnsupportedOperationException("read-only");
        }

        @Override
        public PCollection.IsBounded isBounded() {
            return PCollection.IsBounded.BOUNDED;
        }
    }

    @Test
    public void spike() throws Exception {
        try (Connection connection = DriverManager.getConnection(URL, "sa", "")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS USERS (
                          USER_ID BIGINT PRIMARY KEY,
                          NAME VARCHAR(64),
                          SCORE DOUBLE
                        )""");
                statement.execute("DELETE FROM USERS");
                statement.execute(
                        "INSERT INTO USERS VALUES (1, 'alice', 1.5), (2, 'bob', 2.5)");
            }

            final JdbcLookupSource source = JdbcLookupSource.builder()
                    .withName("db")
                    .withDriver("org.h2.Driver")
                    .withUrl(URL)
                    .withUser("sa")
                    .withPassword("")
                    .withTable("USERS")
                    .build();

            final Schema usersSchema = Schema.builder()
                    .addInt64Field("USER_ID")
                    .addNullableField("NAME", Schema.FieldType.STRING)
                    .addNullableField("SCORE", Schema.FieldType.DOUBLE)
                    .build();

            final Schema inputSchema = Schema.builder()
                    .addInt64Field("userId")
                    .addInt64Field("qty")
                    .build();
            final PCollection<Row> input = pipeline
                    .apply(Create.of(
                            Row.withSchema(inputSchema).addValues(1L, 2L).build(),
                            Row.withSchema(inputSchema).addValues(2L, 3L).build(),
                            Row.withSchema(inputSchema).addValues(9L, 1L).build())
                            .withRowSchema(inputSchema));

            final PCollection<Row> result = input.apply(SqlTransform
                    .query("""
                            SELECT c.userId, u.NAME AS name, c.qty
                            FROM PCOLLECTION AS c
                            JOIN db.USERS AS u ON c.userId = u.USER_ID
                            """)
                    .withTableProvider("db", new ReadOnlyTableProvider("lookup",
                            Map.of("USERS", new LookupSeekableTable(
                                    usersSchema, source, "USERS")))));

            PAssert.that(result).satisfies(rows -> {
                int count = 0;
                for (final Row row : rows) {
                    count++;
                    if (row.getInt64("userId") == 1L) {
                        Assertions.assertEquals("alice", row.getString("name"));
                        Assertions.assertEquals(2L, row.getInt64("qty"));
                    } else {
                        Assertions.assertEquals(2L, row.getInt64("userId"));
                        Assertions.assertEquals("bob", row.getString("name"));
                    }
                }
                Assertions.assertEquals(2, count); // userId=9 dropped (INNER)
                return null;
            });

            pipeline.run();
            // One seekRow per left row, sub-row schema = the table's key column names.
            Assertions.assertEquals(3, LookupSeekableTable.SEEK_ROW_SHAPES.size());
            for (final String shape : LookupSeekableTable.SEEK_ROW_SHAPES) {
                Assertions.assertEquals("[USER_ID]", shape);
            }
        }
    }
}
