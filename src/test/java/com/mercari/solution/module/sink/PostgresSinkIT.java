package com.mercari.solution.module.sink;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=PostgresSinkIT})
 * for the postgres sink module against a real PostgreSQL container managed by Testcontainers:
 * COPY BINARY insert, staged upsert / do-nothing / delete, row-level ops (MERGE), table
 * preparation and the cdc apply mode fed by the postgres source's logical replication.
 */
@Testcontainers
public class PostgresSinkIT {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withCommand("postgres", "-c", "wal_level=logical");

    private static Connection connect() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private static TestPipeline createPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    private static String connectionJson() {
        return """
                "url": "%s",
                "user": "%s",
                "password": "%s"
                """.formatted(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private static String createSourceJson(final String elementsJson) {
        return """
                {
                  "name": "create",
                  "module": "create",
                  "parameters": {
                    "type": "element",
                    "elements": [%s]
                  },
                  "schema": {
                    "fields": [
                      { "name": "id", "type": "int64" },
                      { "name": "name", "type": "string" },
                      { "name": "ver", "type": "int64" },
                      { "name": "op", "type": "string" }
                    ]
                  }
                }
                """.formatted(elementsJson);
    }

    private static String sinkConfigJson(final String sourceJson, final String sinkParametersJson) {
        return """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "pg",
                      "module": "postgres",
                      "inputs": ["create"],
                      "parameters": {
                        %s,
                        %s
                      }
                    }
                  ]
                }
                """.formatted(sourceJson, connectionJson(), sinkParametersJson);
    }

    private static void run(final String configJson) throws Exception {
        final TestPipeline pipeline = createPipeline();
        MPipeline.apply(pipeline, Config.load(configJson));
        pipeline.run().waitUntilFinish();
    }

    private static List<String> query(final String sql) throws Exception {
        final List<String> rows = new ArrayList<>();
        try(final Connection connection = connect();
            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql)) {
            final int columns = resultSet.getMetaData().getColumnCount();
            while(resultSet.next()) {
                final StringBuilder sb = new StringBuilder();
                for(int i = 1; i <= columns; i++) {
                    if(i > 1) {
                        sb.append('|');
                    }
                    sb.append(resultSet.getString(i));
                }
                rows.add(sb.toString());
            }
        }
        return rows;
    }

    private static void execute(final String... sqls) throws Exception {
        try(final Connection connection = connect();
            final Statement statement = connection.createStatement()) {
            for(final String sql : sqls) {
                statement.execute(sql);
            }
        }
    }

    /** All supported types: postgres source (COPY out) → postgres sink (COPY in) into a same-shaped table. */
    @Test
    public void testInsertAllTypes() throws Exception {
        execute("CREATE TYPE sink_mood AS ENUM ('sad','ok','happy')");
        final String ddl = """
                CREATE TABLE %s (
                  id integer PRIMARY KEY,
                  boolfield boolean,
                  shortfield smallint,
                  longfield bigint,
                  floatfield real,
                  doublefield double precision,
                  decimalfield numeric(20, 2),
                  textfield text,
                  varcharfield varchar(50),
                  charfield char(3),
                  bytesfield bytea,
                  datefield date,
                  timefield time,
                  timetzfield timetz,
                  timestampfield timestamp,
                  timestamptzfield timestamptz,
                  uuidfield uuid,
                  jsonfield json,
                  jsonbfield jsonb,
                  xmlfield xml,
                  inetfield inet,
                  cidrfield cidr,
                  macaddrfield macaddr,
                  macaddr8field macaddr8,
                  moodfield sink_mood,
                  intarrayfield integer[],
                  textarrayfield text[],
                  moodarrayfield sink_mood[],
                  uuidarrayfield uuid[],
                  numericarrayfield numeric[],
                  timestamptzarrayfield timestamptz[],
                  generatedfield integer GENERATED ALWAYS AS (id * 2) STORED
                )""";
        execute(ddl.formatted("sink_alltypes_src"), ddl.formatted("sink_alltypes_dst"));
        execute("""
                INSERT INTO sink_alltypes_src VALUES (
                  1, true, 12, 1234567890123, 1.25, -2.5, 12345.67,
                  'hello', 'varchar value', 'abc', decode('62696e617279', 'hex'),
                  '2024-01-15', '12:34:56.789', '12:34:56+09',
                  '2023-11-14 22:13:20', '2023-11-14 22:13:20+00',
                  '123e4567-e89b-12d3-a456-426614174000',
                  '{"a":1}', '{"b":[1,2]}', '<a>1</a>',
                  '192.168.0.1', '192.168.100.0/24',
                  '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05',
                  'happy',
                  '{1,2,3}', '{"x","y z"}', '{sad,happy}',
                  '{123e4567-e89b-12d3-a456-426614174000}',
                  '{12345.67,-0.01}',
                  '{"2023-11-14 22:13:20+00","2023-11-14 22:13:21+00"}'
                )""",
                "INSERT INTO sink_alltypes_src (id) VALUES (2)",
                "INSERT INTO sink_alltypes_src (id, intarrayfield, moodarrayfield) VALUES (3, '{}', '{ok,sad}')");

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "src",
                      "module": "postgres",
                      "parameters": { %s, "table": "sink_alltypes_src" }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "pg",
                      "module": "postgres",
                      "inputs": ["src"],
                      "parameters": { %s, "table": "sink_alltypes_dst", "settings": { "synchronous_commit": "off" } }
                    }
                  ]
                }
                """.formatted(connectionJson(), connectionJson());

        final TestPipeline pipeline = createPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        PAssert.that(outputs.get("pg").getCollection()).satisfies(elements -> {
            long rows = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals("public.sink_alltypes_dst", element.getAsString("table"));
                Assertions.assertEquals("INSERT", element.getAsString("op"));
                Assertions.assertEquals(element.getAsLong("rows"), element.getAsLong("affectedRows"));
                rows += element.getAsLong("rows");
            }
            Assertions.assertEquals(3L, rows);
            return null;
        });
        pipeline.run().waitUntilFinish();

        // json has no equality operator: compare its text form
        final String select = "SELECT id, boolfield, shortfield, longfield, floatfield, doublefield, decimalfield, textfield, varcharfield, charfield,"
                + " bytesfield, datefield, timefield, timestampfield, timestamptzfield, uuidfield, jsonfield::text, jsonbfield, xmlfield::text,"
                + " inetfield, cidrfield, macaddrfield, macaddr8field, moodfield, intarrayfield, textarrayfield, moodarrayfield, uuidarrayfield,"
                + " numericarrayfield, timestamptzarrayfield, generatedfield FROM ";
        Assertions.assertEquals(List.of(), query("(" + select + "sink_alltypes_src) EXCEPT (" + select + "sink_alltypes_dst)"));
        Assertions.assertEquals(List.of(), query("(" + select + "sink_alltypes_dst) EXCEPT (" + select + "sink_alltypes_src)"));
        Assertions.assertEquals(List.of("3"), query("SELECT count(*) FROM sink_alltypes_dst"));
        // timetz values are normalized to UTC on the way (timetz equality keeps the zone, so compare the text form)
        Assertions.assertEquals(List.of("03:34:56+00"), query("SELECT timetzfield::text FROM sink_alltypes_dst WHERE id = 1"));
    }

    @Test
    public void testInsertOrUpdateLastWinsAndUpdateCondition() throws Exception {
        execute("CREATE TABLE sink_upsert (id bigint PRIMARY KEY, name text, ver bigint)",
                "INSERT INTO sink_upsert VALUES (1, 'old', 1), (2, 'keep', 5)");

        // duplicate keys in one batch: the last row wins; op column is not a table column -> ignoreUnknownFields
        run(sinkConfigJson(createSourceJson("""
                { "id": 1, "name": "first", "ver": 2, "op": "" },
                { "id": 1, "name": "last", "ver": 3, "op": "" },
                { "id": 3, "name": "new", "ver": 1, "op": "" }
                """), """
                "table": "sink_upsert",
                "op": "INSERT_OR_UPDATE",
                "ignoreUnknownFields": true,
                "batchSize": 0
                """));
        Assertions.assertEquals(
                List.of("1|last|3", "2|keep|5", "3|new|1"),
                query("SELECT id, name, ver FROM sink_upsert ORDER BY id"));

        // updateCondition: a lower version must not overwrite, a higher one must; keyFields default to the primary key
        run(sinkConfigJson(createSourceJson("""
                { "id": 1, "name": "stale", "ver": 2, "op": "" },
                { "id": 2, "name": "fresh", "ver": 6, "op": "" }
                """), """
                "table": "sink_upsert",
                "op": "INSERT_OR_UPDATE",
                "updateFields": ["name", "ver"],
                "updateCondition": "target.ver < excluded.ver",
                "ignoreUnknownFields": true
                """));
        Assertions.assertEquals(
                List.of("1|last|3", "2|fresh|6", "3|new|1"),
                query("SELECT id, name, ver FROM sink_upsert ORDER BY id"));
    }

    @Test
    public void testInsertOrDoNothingAndDelete() throws Exception {
        execute("CREATE TABLE sink_donothing (id bigint, name text, ver bigint, CONSTRAINT sink_donothing_uk UNIQUE (id))",
                "INSERT INTO sink_donothing VALUES (1, 'existing', 1)");

        run(sinkConfigJson(createSourceJson("""
                { "id": 1, "name": "ignored", "ver": 9, "op": "" },
                { "id": 2, "name": "first", "ver": 1, "op": "" },
                { "id": 2, "name": "second", "ver": 2, "op": "" }
                """), """
                "table": "sink_donothing",
                "op": "INSERT_OR_DONOTHING",
                "keyFields": ["id"],
                "ignoreUnknownFields": true
                """));
        Assertions.assertEquals(
                List.of("1|existing|1", "2|first|1"),
                query("SELECT id, name, ver FROM sink_donothing ORDER BY id"));

        // DELETE stages the keys only (the other input fields are ignored)
        run(sinkConfigJson(createSourceJson("""
                { "id": 1, "name": "x", "ver": 0, "op": "" },
                { "id": 3, "name": "x", "ver": 0, "op": "" }
                """), """
                "table": "sink_donothing",
                "op": "DELETE",
                "keyFields": ["id"],
                "ignoreUnknownFields": true
                """));
        Assertions.assertEquals(List.of("2|first|1"), query("SELECT id, name, ver FROM sink_donothing ORDER BY id"));
    }

    @Test
    public void testOpFieldMerge() throws Exception {
        execute("CREATE TABLE sink_merge (id bigint PRIMARY KEY, name text, ver bigint)",
                "INSERT INTO sink_merge VALUES (1, 'one', 1), (2, 'two', 1), (3, 'three', 1)");

        run(sinkConfigJson(createSourceJson("""
                { "id": 1, "name": "ONE", "ver": 2, "op": "UPDATE" },
                { "id": 2, "name": "x", "ver": 0, "op": "DELETE" },
                { "id": 4, "name": "four", "ver": 1, "op": "INSERT" },
                { "id": 5, "name": "x", "ver": 0, "op": "DELETE" },
                { "id": 4, "name": "FOUR", "ver": 2, "op": "UPDATE" }
                """), """
                "table": "sink_merge",
                "opField": "op"
                """));
        Assertions.assertEquals(
                List.of("1|ONE|2", "3|three|1", "4|FOUR|2"),
                query("SELECT id, name, ver FROM sink_merge ORDER BY id"));
    }

    @Test
    public void testCreateTableAndEmptyTable() throws Exception {
        run(sinkConfigJson(createSourceJson("""
                { "id": 1, "name": "a", "ver": 1, "op": "I" },
                { "id": 2, "name": "b", "ver": 1, "op": "I" }
                """), """
                "table": "sink_created",
                "createTable": true,
                "keyFields": ["id"]
                """));
        Assertions.assertEquals(List.of("1|a|1|I", "2|b|1|I"), query("SELECT id, name, ver, op FROM sink_created ORDER BY id"));
        Assertions.assertEquals(List.of("id"), query("""
                SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'sink_created'::regclass AND i.indisprimary"""));

        // TRUNCATE (emptyTable: true) then DELETE FROM (emptyTable: delete) before writing
        run(sinkConfigJson(createSourceJson("""
                { "id": 3, "name": "c", "ver": 1, "op": "I" }
                """), """
                "table": "sink_created",
                "emptyTable": true
                """));
        Assertions.assertEquals(List.of("3|c"), query("SELECT id, name FROM sink_created ORDER BY id"));
        run(sinkConfigJson(createSourceJson("""
                { "id": 4, "name": "d", "ver": 1, "op": "I" }
                """), """
                "table": "sink_created",
                "emptyTable": "delete"
                """));
        Assertions.assertEquals(List.of("4|d"), query("SELECT id, name FROM sink_created ORDER BY id"));
    }

    @Test
    public void testLaunchValidationAgainstDestination() throws Exception {
        execute("CREATE TABLE sink_nokey (id bigint, name text, ver bigint, op text)");

        // upsert without a unique index on the keys
        final Config noIndex = Config.load(sinkConfigJson(createSourceJson("{}"), """
                "table": "sink_nokey",
                "op": "INSERT_OR_UPDATE",
                "keyFields": ["id"]
                """));
        final IllegalModuleException e1 = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(createPipeline(), noIndex));
        Assertions.assertTrue(e1.getMessage().contains("has no unique index on the key columns"), "unexpected message: " + e1.getMessage());

        // upsert without keys and without a primary key
        final Config noKey = Config.load(sinkConfigJson(createSourceJson("{}"), """
                "table": "sink_nokey",
                "op": "INSERT_OR_UPDATE"
                """));
        final IllegalModuleException e2 = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(createPipeline(), noKey));
        Assertions.assertTrue(e2.getMessage().contains("has no primary key"), "unexpected message: " + e2.getMessage());

        // an input field without a column
        execute("CREATE TABLE sink_narrow (id bigint PRIMARY KEY, name text)");
        final Config unknownField = Config.load(sinkConfigJson(createSourceJson("{}"), """
                "table": "sink_narrow"
                """));
        final IllegalModuleException e3 = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(createPipeline(), unknownField));
        Assertions.assertTrue(e3.getMessage().contains("has no column for input field: ver"), "unexpected message: " + e3.getMessage());

        // a missing table
        final Config missing = Config.load(sinkConfigJson(createSourceJson("{}"), """
                "table": "sink_missing"
                """));
        final IllegalModuleException e4 = Assertions.assertThrows(
                IllegalModuleException.class, () -> MPipeline.apply(createPipeline(), missing));
        Assertions.assertTrue(e4.getMessage().contains("does not exist"), "unexpected message: " + e4.getMessage());
    }

    /** An encoding failure (value out of the column's range) is routed per element; the other rows are written. */
    @Test
    public void testEncodeFailureIsRoutedPerElement() throws Exception {
        execute("CREATE TABLE sink_failure (id bigint PRIMARY KEY, name text, ver smallint, op text)");
        final String configJson = """
                {
                  "sources": [%s],
                  "sinks": [
                    {
                      "name": "pg",
                      "module": "postgres",
                      "inputs": ["create"],
                      "failFast": false,
                      "parameters": { %s, "table": "sink_failure" }
                    }
                  ]
                }
                """.formatted(createSourceJson("""
                { "id": 1, "name": "ok", "ver": 1, "op": "" },
                { "id": 2, "name": "overflow", "ver": 70000, "op": "" },
                { "id": 3, "name": "ok", "ver": 3, "op": "" }
                """), connectionJson());
        run(configJson);
        Assertions.assertEquals(List.of("1|1", "3|3"), query("SELECT id, ver FROM sink_failure ORDER BY id"));
    }

    /**
     * cdc apply: changes of a source table captured by the postgres source (logical
     * replication) are normalized by the cdc transform, collapsed per key ({@code accumulate},
     * because batches are applied in no particular order) and applied to a destination table
     * with a sequence guard column, so the destination converges to the source state.
     */
    @Test
    public void testCdcApply() throws Exception {
        execute("CREATE TABLE sink_cdc_src (id integer PRIMARY KEY, name text, price numeric(10,2), tags text[], updated timestamptz)",
                "CREATE TABLE sink_cdc_dst (id integer PRIMARY KEY, name text, price numeric(10,2), tags text[], updated timestamptz, seq text)",
                "CREATE PUBLICATION sink_cdc_pub FOR TABLE sink_cdc_src",
                "SELECT pg_create_logical_replication_slot('sink_cdc_slot', 'pgoutput')",
                // 6 change records retained in the slot before the pipeline starts
                "INSERT INTO sink_cdc_src VALUES (1, 'one', 10.50, '{\"a\",\"b\"}', '2023-11-14 22:13:20+00')",
                "INSERT INTO sink_cdc_src VALUES (2, 'two', NULL, NULL, NULL)",
                "INSERT INTO sink_cdc_src VALUES (3, 'three', 3.00, '{}', NULL)",
                "UPDATE sink_cdc_src SET name = 'ONE', price = 11.00 WHERE id = 1",
                "DELETE FROM sink_cdc_src WHERE id = 2",
                "UPDATE sink_cdc_src SET tags = '{\"c\"}' WHERE id = 3");
        // a stale destination row must be replaced regardless of the guard (its seq is null)
        execute("INSERT INTO sink_cdc_dst (id, name) VALUES (1, 'stale')");

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "postgresCdc",
                      "module": "postgres",
                      "mode": "changeDataCapture",
                      "parameters": {
                        %s,
                        "cdc": {
                          "slot": "sink_cdc_slot",
                          "publication": "sink_cdc_pub",
                          "maxNumRecords": 6,
                          "maxReadTimeSeconds": 120
                        }
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "envelope",
                      "module": "cdc",
                      "inputs": ["postgresCdc"],
                      "parameters": { "format": "postgres", "accumulate": true }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "pg",
                      "module": "postgres",
                      "inputs": ["envelope"],
                      "parameters": {
                        %s,
                        "table": "sink_cdc_dst",
                        "cdc": true,
                        "sequenceField": "seq",
                        "batchSize": 2
                      }
                    }
                  ]
                }
                """.formatted(connectionJson(), connectionJson());
        run(configJson);

        final String select = "SELECT id, name, price, tags, updated FROM ";
        Assertions.assertEquals(List.of(), query("(" + select + "sink_cdc_src) EXCEPT (" + select + "sink_cdc_dst)"));
        Assertions.assertEquals(List.of(), query("(" + select + "sink_cdc_dst) EXCEPT (" + select + "sink_cdc_src)"));
        Assertions.assertEquals(List.of("2"), query("SELECT count(*) FROM sink_cdc_dst WHERE seq IS NOT NULL"));

        // replaying an older change must not overwrite the newer state (sequence guard)
        final List<String> seq = query("SELECT seq FROM sink_cdc_dst WHERE id = 1");
        final String staleEnvelope = "{\"table\":\"sink_cdc_src\",\"op\":\"UPDATE\",\"keys\":\"{\\\"id\\\":1}\",\"after\":\"{\\\"id\\\":1,\\\"name\\\":\\\"older\\\"}\",\"commitTimestamp\":1672531200000000,\"sequence\":\"0/1\",\"source\":{\"provider\":\"postgres\"}}";
        run("""
                {
                  "sources": [
                    {
                      "name": "archive",
                      "module": "create",
                      "parameters": {
                        "type": "element",
                        "elements": [ { "payload": %s } ]
                      },
                      "schema": {
                        "fields": [ { "name": "payload", "type": "string" } ]
                      }
                    }
                  ],
                  "transforms": [
                    {
                      "name": "envelope",
                      "module": "cdc",
                      "inputs": ["archive"],
                      "parameters": { "format": "envelope", "field": "payload" }
                    }
                  ],
                  "sinks": [
                    {
                      "name": "pg",
                      "module": "postgres",
                      "inputs": ["envelope"],
                      "parameters": { %s, "table": "sink_cdc_dst", "cdc": true, "sequenceField": "seq" }
                    }
                  ]
                }
                """.formatted(new com.google.gson.Gson().toJson(staleEnvelope), connectionJson()));
        Assertions.assertEquals(List.of("1|ONE"), query("SELECT id, name FROM sink_cdc_dst WHERE id = 1"));
        Assertions.assertEquals(seq, query("SELECT seq FROM sink_cdc_dst WHERE id = 1"));

        execute("SELECT pg_drop_replication_slot('sink_cdc_slot')", "DROP PUBLICATION sink_cdc_pub");
    }

}
