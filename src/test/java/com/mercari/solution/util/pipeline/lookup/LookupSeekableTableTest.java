package com.mercari.solution.util.pipeline.lookup;

import com.mercari.solution.util.pipeline.lookup.source.JdbcLookupSource;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Map;

/**
 * The query module's LookupSources exposed to Beam SQL via
 * {@link LookupSeekableTable} (Beam's native seekable-table lookup): single and
 * composite candidate keys matched by join-column names, value conversion for
 * date/timestamp columns, and scan rejection. Note the Beam quirk pinned here:
 * the equi-condition must put the main-input column on the LEFT
 * ({@code c.userId = u.USER_ID}).
 */
public class LookupSeekableTableTest {

    private static final String URL = "jdbc:h2:mem:seekabletest;DB_CLOSE_DELAY=-1";

    private static Connection keepAlive;

    private final transient TestPipeline pipeline =
            TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @BeforeAll
    public static void createTables() throws Exception {
        keepAlive = DriverManager.getConnection(URL, "sa", "");
        try (Statement statement = keepAlive.createStatement()) {
            statement.execute("""
                    CREATE TABLE USERS (
                      USER_ID BIGINT PRIMARY KEY,
                      NAME VARCHAR(64),
                      BIRTH DATE,
                      CREATED TIMESTAMP
                    )""");
            statement.execute("""
                    INSERT INTO USERS VALUES
                      (1, 'alice', DATE '1990-01-15', TIMESTAMP '2020-01-02 03:04:05'),
                      (2, 'bob', DATE '1985-06-30', TIMESTAMP '2021-11-22 10:20:30')""");
            statement.execute("""
                    CREATE TABLE EVENTS (
                      USER_ID BIGINT NOT NULL,
                      SEQ BIGINT NOT NULL,
                      AMOUNT BIGINT,
                      PRIMARY KEY (USER_ID, SEQ)
                    )""");
            statement.execute("""
                    INSERT INTO EVENTS VALUES
                      (1, 1, 10), (1, 2, 20), (2, 1, 5)""");
        }
    }

    @AfterAll
    public static void dropDatabase() throws Exception {
        try (Statement statement = keepAlive.createStatement()) {
            statement.execute("DROP ALL OBJECTS");
        }
        keepAlive.close();
    }

    private static JdbcLookupSource source(String... tables) {
        final JdbcLookupSource.Builder builder = JdbcLookupSource.builder()
                .withName("db")
                .withDriver("org.h2.Driver")
                .withUrl(URL)
                .withUser("sa")
                .withPassword("");
        for (final String table : tables) {
            builder.withTable(table);
        }
        return builder.build();
    }

    private static SqlTransform withLookupTables(String sql, JdbcLookupSource source) {
        try {
            source.setup();
            return SqlTransform.query(sql).withTableProvider("db",
                    new ReadOnlyTableProvider("db", LookupSeekableTable.tablesOf(source)));
        } finally {
            source.close();
        }
    }

    @Test
    public void testPointLookupWithTypedColumns() {
        final Schema inputSchema = Schema.builder()
                .addInt64Field("userId")
                .build();
        final PCollection<Row> input = pipeline.apply(Create.of(
                        Row.withSchema(inputSchema).addValues(1L).build(),
                        Row.withSchema(inputSchema).addValues(9L).build())
                .withRowSchema(inputSchema));

        final PCollection<Row> result = input.apply(withLookupTables("""
                SELECT c.userId, u.NAME AS name, u.BIRTH AS birth, u.CREATED AS created
                FROM PCOLLECTION AS c
                JOIN db.USERS AS u ON c.userId = u.USER_ID
                """, source("USERS")));

        final long expectedCreated =
                java.sql.Timestamp.valueOf("2020-01-02 03:04:05").getTime();
        PAssert.that(result).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                Assertions.assertEquals(1L, row.getInt64("userId"));
                Assertions.assertEquals("alice", row.getString("name"));
                // DATE/TIME columns surface as ISO strings at the seekable-join
                // boundary (Beam's join output cannot carry java.time logical
                // types); TIMESTAMP is Beam's primitive DATETIME.
                Assertions.assertEquals("1990-01-15", row.getString("birth"));
                Assertions.assertEquals(expectedCreated, toEpochMillis(row.getValue("created")));
            }
            Assertions.assertEquals(1, count); // userId=9 dropped (INNER)
            return null;
        });
        pipeline.run();
    }

    private static long toEpochMillis(final Object value) {
        return value instanceof org.joda.time.base.AbstractInstant instant
                ? instant.getMillis() : ((Number) value).longValue();
    }

    @Test
    public void testCompositeKeyLookup() {
        final Schema inputSchema = Schema.builder()
                .addInt64Field("userId")
                .addInt64Field("seq")
                .build();
        final PCollection<Row> input = pipeline.apply(Create.of(
                        Row.withSchema(inputSchema).addValues(1L, 2L).build(),
                        Row.withSchema(inputSchema).addValues(2L, 1L).build(),
                        Row.withSchema(inputSchema).addValues(1L, 9L).build())
                .withRowSchema(inputSchema));

        final PCollection<Row> result = input.apply(withLookupTables("""
                SELECT c.userId, c.seq, e.AMOUNT AS amount
                FROM PCOLLECTION AS c
                JOIN db.EVENTS AS e
                  ON c.userId = e.USER_ID AND c.seq = e.SEQ
                """, source("EVENTS")));

        PAssert.that(result).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                final long amount = row.getInt64("amount");
                if (row.getInt64("userId") == 1L) {
                    Assertions.assertEquals(2L, row.getInt64("seq"));
                    Assertions.assertEquals(20L, amount);
                } else {
                    Assertions.assertEquals(5L, amount);
                }
            }
            Assertions.assertEquals(2, count); // (1,9) has no match
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testStandaloneScanIsRejected() {
        final Schema inputSchema = Schema.builder().addInt64Field("userId").build();
        final PCollection<Row> input = pipeline.apply(Create.of(
                Row.withSchema(inputSchema).addValues(1L).build()).withRowSchema(inputSchema));

        final RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> {
            input.apply(withLookupTables(
                    "SELECT USER_ID FROM db.USERS", source("USERS")));
            pipeline.run();
        });
        Assertions.assertTrue(rootMessage(e).contains("standalone scans are not supported"),
                "unexpected message: " + rootMessage(e));
    }

    private static String rootMessage(final Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return String.valueOf(t.getMessage());
    }
}
