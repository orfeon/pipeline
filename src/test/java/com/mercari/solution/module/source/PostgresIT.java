package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.db.PostgresUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Integration test (run via maven-failsafe: {@code mvn verify -DskipITs=false -Dit.test=PostgresIT})
 * for the postgres source module and the PostgresUtil COPY BINARY reader/writer against a real
 * PostgreSQL container managed by Testcontainers.
 *
 * Unlike the round-trip unit tests in PostgresUtilTest, these tests verify the binary format
 * assumptions (array header layout, timetz zone offset sign, inet address family codes,
 * enum label transfer, postgres epoch offsets) against PostgreSQL itself.
 */
@Testcontainers
public class PostgresIT {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            // logical replication (changeDataCapture mode tests); a superset of the default level
            .withCommand("postgres", "-c", "wal_level=logical");

    private static Connection connect() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private static TestPipeline createPipeline() {
        final DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
        return TestPipeline.fromOptions(options).enableAbandonedNodeEnforcement(false);
    }

    @Test
    public void testSourceModuleAllTypes() throws Exception {

        try(final Connection connection = connect();
            final Statement statement = connection.createStatement()) {

            statement.execute("CREATE TYPE mood AS ENUM ('sad','ok','happy')");
            statement.execute("""
                    CREATE TABLE alltypes (
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
                      moodfield mood,
                      intarrayfield integer[],
                      textarrayfield text[],
                      moodarrayfield mood[],
                      uuidarrayfield uuid[],
                      numericarrayfield numeric[],
                      timestamptzarrayfield timestamptz[]
                    )""");
            statement.execute("""
                    INSERT INTO alltypes VALUES (
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
                    )""");
            statement.execute("INSERT INTO alltypes (id) VALUES (2)");
            statement.execute("INSERT INTO alltypes (id, intarrayfield, moodarrayfield) VALUES (3, '{}', '{ok,NULL,sad}')");
        }

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "postgresSource",
                      "module": "postgres",
                      "parameters": {
                        "url": "%s",
                        "user": "%s",
                        "password": "%s",
                        "table": "alltypes"
                      }
                    }
                  ]
                }
                """.formatted(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        final TestPipeline pipeline = createPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));
        final MCollection output = outputs.get("postgresSource");

        PAssert.that(output.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                switch (element.getAsLong("id").intValue()) {
                    case 1 -> {
                        Assertions.assertEquals(Boolean.TRUE, element.getPrimitiveValue("boolfield"));
                        Assertions.assertEquals(12L, element.getAsLong("shortfield"));
                        Assertions.assertEquals(1234567890123L, element.getAsLong("longfield"));
                        Assertions.assertEquals(1.25D, element.getAsDouble("floatfield"), 1e-9);
                        Assertions.assertEquals(-2.5D, element.getAsDouble("doublefield"), 1e-9);
                        Assertions.assertEquals(
                                toDecimalBytes(new BigDecimal("12345.67")),
                                element.getAsBytes("decimalfield"));
                        Assertions.assertEquals("hello", element.getAsString("textfield"));
                        Assertions.assertEquals("varchar value", element.getAsString("varcharfield"));
                        Assertions.assertEquals("abc", element.getAsString("charfield"));
                        Assertions.assertEquals(
                                ByteBuffer.wrap("binary".getBytes(StandardCharsets.UTF_8)),
                                element.getAsBytes("bytesfield"));
                        Assertions.assertEquals(
                                (int) LocalDate.of(2024, 1, 15).toEpochDay(),
                                ((Number) element.getPrimitiveValue("datefield")).intValue());
                        Assertions.assertEquals(
                                LocalTime.of(12, 34, 56, 789000000).toNanoOfDay() / 1000L,
                                element.getAsLong("timefield"));
                        // '12:34:56+09' is 03:34:56 UTC
                        Assertions.assertEquals(
                                LocalTime.of(3, 34, 56).toNanoOfDay() / 1000L,
                                element.getAsLong("timetzfield"));
                        Assertions.assertEquals(1700000000000000L, element.getAsLong("timestampfield"));
                        Assertions.assertEquals(1700000000000000L, element.getAsLong("timestamptzfield"));
                        Assertions.assertEquals("123e4567-e89b-12d3-a456-426614174000", element.getAsString("uuidfield"));
                        Assertions.assertEquals("{\"a\":1}", element.getAsString("jsonfield"));
                        // jsonb output is normalized by postgres
                        Assertions.assertEquals("{\"b\": [1, 2]}", element.getAsString("jsonbfield"));
                        Assertions.assertEquals("<a>1</a>", element.getAsString("xmlfield"));
                        Assertions.assertEquals("192.168.0.1/32", element.getAsString("inetfield"));
                        Assertions.assertEquals("192.168.100.0/24", element.getAsString("cidrfield"));
                        Assertions.assertEquals("08:00:2b:01:02:03", element.getAsString("macaddrfield"));
                        Assertions.assertEquals("08:00:2b:01:02:03:04:05", element.getAsString("macaddr8field"));
                        Assertions.assertEquals("happy", element.getAsString("moodfield"));
                        Assertions.assertEquals(List.of(1, 2, 3), intList(element.getPrimitiveValue("intarrayfield")));
                        Assertions.assertEquals(List.of("x", "y z"), stringList(element.getPrimitiveValue("textarrayfield")));
                        Assertions.assertEquals(List.of("sad", "happy"), stringList(element.getPrimitiveValue("moodarrayfield")));
                        Assertions.assertEquals(
                                List.of("123e4567-e89b-12d3-a456-426614174000"),
                                stringList(element.getPrimitiveValue("uuidarrayfield")));
                        Assertions.assertEquals(
                                List.of(toDecimalBytes(new BigDecimal("12345.67")), toDecimalBytes(new BigDecimal("-0.01"))),
                                element.getPrimitiveValue("numericarrayfield"));
                        Assertions.assertEquals(
                                List.of(1700000000000000L, 1700000001000000L),
                                longList(element.getPrimitiveValue("timestamptzarrayfield")));
                    }
                    case 2 -> {
                        Assertions.assertNull(element.getPrimitiveValue("boolfield"));
                        Assertions.assertNull(element.getPrimitiveValue("decimalfield"));
                        Assertions.assertNull(element.getPrimitiveValue("textfield"));
                        Assertions.assertNull(element.getPrimitiveValue("timetzfield"));
                        Assertions.assertNull(element.getPrimitiveValue("moodfield"));
                        Assertions.assertNull(element.getPrimitiveValue("intarrayfield"));
                        Assertions.assertNull(element.getPrimitiveValue("moodarrayfield"));
                    }
                    case 3 -> {
                        Assertions.assertEquals(List.of(), intList(element.getPrimitiveValue("intarrayfield")));
                        // null array elements are skipped
                        Assertions.assertEquals(List.of("ok", "sad"), stringList(element.getPrimitiveValue("moodarrayfield")));
                    }
                    default -> Assertions.fail("unexpected id: " + element.getAsLong("id"));
                }
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testCopyBinaryWrite() throws Exception {

        try(final Connection connection = connect()) {
            try(final Statement statement = connection.createStatement()) {
                statement.execute("CREATE TYPE color AS ENUM ('red','green','blue')");
                statement.execute("""
                        CREATE TABLE writetypes (
                          id integer,
                          textfield text,
                          colorfield color,
                          colorarrayfield color[],
                          intarrayfield integer[],
                          textarrayfield text[],
                          numericarrayfield numeric[],
                          timestamptzarrayfield timestamptz[],
                          inetfield inet,
                          cidrfield cidr,
                          macaddrfield macaddr,
                          macaddr8field macaddr8,
                          xmlfield xml,
                          timetzfield timetz
                        )""");
            }

            final String query = "SELECT * FROM writetypes";
            // exercises pg_type catalog resolution for the enum scalar and enum array columns
            final List<PostgresUtil.Column> columns = PostgresUtil.getColumnsFromQuery(connection, query);
            final Schema schema;
            try(final PreparedStatement statement = connection.prepareStatement(query)) {
                schema = ResultSetToRecordConverter.convertSchema(statement.getMetaData());
            }

            final GenericData.Record record = new GenericData.Record(schema);
            record.put("id", 1);
            record.put("textfield", "hello");
            record.put("colorfield", "green");
            record.put("colorarrayfield", Arrays.asList("red", "blue"));
            record.put("intarrayfield", Arrays.asList(1, 2, 3));
            record.put("textarrayfield", Arrays.asList("a", "b c"));
            record.put("numericarrayfield", List.of(toDecimalBytes(new BigDecimal("12345.67"))));
            record.put("timestamptzarrayfield", List.of(1700000000000000L));
            record.put("inetfield", "10.1.2.3");
            record.put("cidrfield", "10.0.0.0/8");
            record.put("macaddrfield", "08:00:2b:01:02:03");
            record.put("macaddr8field", "08:00:2b:01:02:03:04:05");
            record.put("xmlfield", "<x>1</x>");
            record.put("timetzfield", LocalTime.of(3, 34, 56).toNanoOfDay() / 1000L);

            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try(final DataOutputStream output = new DataOutputStream(bytes)) {
                PostgresUtil.writeHeader(output);
                PostgresUtil.write(output, columns, schema.getFields(), record);
                PostgresUtil.writeTrailer(output);
            }
            final List<String> columnNames = columns.stream().map(c -> c.name).toList();
            connection.unwrap(PGConnection.class).getCopyAPI().copyIn(
                    PostgresUtil.createCopyInStatement("writetypes", columnNames),
                    new ByteArrayInputStream(bytes.toByteArray()));

            try(final Statement statement = connection.createStatement()) {
                // pgjdbc sets the session timezone to the JVM default; fix it for stable text output
                statement.execute("SET TIME ZONE 'UTC'");
                try(final ResultSet resultSet = statement.executeQuery("""
                        SELECT
                          textfield,
                          colorfield::text,
                          array_to_string(colorarrayfield, ','),
                          array_to_string(intarrayfield, ','),
                          array_to_string(textarrayfield, ','),
                          array_to_string(numericarrayfield, ','),
                          array_to_string(timestamptzarrayfield, ','),
                          inetfield::text,
                          cidrfield::text,
                          macaddrfield::text,
                          macaddr8field::text,
                          xmlfield::text,
                          timetzfield::text
                        FROM writetypes""")) {

                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals("hello", resultSet.getString(1));
                    Assertions.assertEquals("green", resultSet.getString(2));
                    Assertions.assertEquals("red,blue", resultSet.getString(3));
                    Assertions.assertEquals("1,2,3", resultSet.getString(4));
                    Assertions.assertEquals("a,b c", resultSet.getString(5));
                    Assertions.assertEquals("12345.670000000", resultSet.getString(6));
                    Assertions.assertEquals("2023-11-14 22:13:20+00", resultSet.getString(7));
                    Assertions.assertEquals("10.1.2.3/32", resultSet.getString(8));
                    Assertions.assertEquals("10.0.0.0/8", resultSet.getString(9));
                    Assertions.assertEquals("08:00:2b:01:02:03", resultSet.getString(10));
                    Assertions.assertEquals("08:00:2b:01:02:03:04:05", resultSet.getString(11));
                    Assertions.assertEquals("<x>1</x>", resultSet.getString(12));
                    Assertions.assertEquals("03:34:56+00", resultSet.getString(13));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    /**
     * The all-tables (tables parameter) mode: the source lists matching base tables at assembly
     * time and outputs one tagged collection per table with per-table schema and assembly-time
     * attributes. Bare patterns match public-schema tables only; dotted patterns match the
     * schema-qualified name.
     */
    @Test
    public void testSourceModuleAllTables() throws Exception {

        try(final Connection connection = connect();
            final Statement statement = connection.createStatement()) {

            statement.execute("CREATE TABLE multi_a (id integer PRIMARY KEY, name text)");
            statement.execute("INSERT INTO multi_a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3')");
            statement.execute("CREATE TABLE multi_b (id integer PRIMARY KEY)");
            statement.execute("INSERT INTO multi_b VALUES (1), (2)");
            statement.execute("CREATE TABLE multi_skip (id integer PRIMARY KEY)");
            statement.execute("INSERT INTO multi_skip VALUES (1)");
            statement.execute("CREATE SCHEMA mschema");
            statement.execute("CREATE TABLE mschema.multi_c (id integer PRIMARY KEY, label text)");
            statement.execute("INSERT INTO mschema.multi_c VALUES (1, 'c1')");
        }

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "postgresMulti",
                      "module": "postgres",
                      "parameters": {
                        "url": "%s",
                        "user": "%s",
                        "password": "%s",
                        "tables": {
                          "includes": ["multi_*", "mschema.*"],
                          "excludes": ["multi_skip"]
                        }
                      }
                    }
                  ]
                }
                """.formatted(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        final TestPipeline pipeline = createPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));

        Assertions.assertNull(outputs.get("postgresMulti.multi_skip"), "excluded table must not be output: " + outputs.keySet());

        final MCollection outputA = outputs.get("postgresMulti.multi_a");
        Assertions.assertNotNull(outputA, "postgresMulti.multi_a not found in: " + outputs.keySet());
        Assertions.assertEquals("multi_a", outputA.getAttributes().get("table"));
        Assertions.assertEquals("public", outputA.getAttributes().get("schema"));
        Assertions.assertEquals("multi_a", outputA.getAttributes().get("name"));
        Assertions.assertNotNull(outputA.getSchema().getField("name"));

        final MCollection outputB = outputs.get("postgresMulti.multi_b");
        Assertions.assertNotNull(outputB, "postgresMulti.multi_b not found in: " + outputs.keySet());

        // non-public schema tables keep the schema-qualified tag
        final MCollection outputC = outputs.get("postgresMulti.mschema.multi_c");
        Assertions.assertNotNull(outputC, "postgresMulti.mschema.multi_c not found in: " + outputs.keySet());
        Assertions.assertEquals("mschema.multi_c", outputC.getAttributes().get("table"));
        Assertions.assertEquals("mschema", outputC.getAttributes().get("schema"));
        Assertions.assertEquals("multi_c", outputC.getAttributes().get("name"));
        Assertions.assertNotNull(outputC.getSchema().getField("label"));

        PAssert.that(outputA.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals("a" + element.getAsLong("id"), element.getAsString("name"));
                count++;
            }
            Assertions.assertEquals(3, count);
            return null;
        });
        PAssert.that(outputB.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement ignored : elements) {
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        PAssert.that(outputC.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals("c1", element.getAsString("label"));
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    /**
     * The tables.select / tables.where templates: common per-table SELECT and WHERE clause
     * fragments applied to every matched table, with ${table} resolved per table at assembly
     * time. The output schema comes from the rendered per-table query.
     */
    @Test
    public void testSourceModuleAllTablesSelectWhereTemplate() throws Exception {

        try(final Connection connection = connect();
            final Statement statement = connection.createStatement()) {

            statement.execute("CREATE TABLE tmpl_x (id integer PRIMARY KEY, value text)");
            statement.execute("INSERT INTO tmpl_x VALUES (1, 'x1'), (2, 'x2'), (3, 'x3')");
            statement.execute("CREATE TABLE tmpl_y (id integer PRIMARY KEY, value text)");
            statement.execute("INSERT INTO tmpl_y VALUES (1, 'y1'), (2, 'y2')");
        }

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "postgresTmpl",
                      "module": "postgres",
                      "parameters": {
                        "url": "%s",
                        "user": "%s",
                        "password": "%s",
                        "tables": {
                          "includes": ["tmpl_*"],
                          "select": "id, '${table}'::text AS source_table",
                          "where": "id >= 2"
                        }
                      }
                    }
                  ]
                }
                """.formatted(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        final TestPipeline pipeline = createPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));

        final MCollection outputX = outputs.get("postgresTmpl.tmpl_x");
        Assertions.assertNotNull(outputX, "postgresTmpl.tmpl_x not found in: " + outputs.keySet());
        // the schema comes from the rendered query, so the computed column must be present
        Assertions.assertNotNull(outputX.getSchema().getField("source_table"));
        Assertions.assertNull(outputX.getSchema().getField("value"));
        final MCollection outputY = outputs.get("postgresTmpl.tmpl_y");
        Assertions.assertNotNull(outputY, "postgresTmpl.tmpl_y not found in: " + outputs.keySet());

        PAssert.that(outputX.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                Assertions.assertTrue(element.getAsLong("id") >= 2, "where must filter id 1");
                Assertions.assertEquals("tmpl_x", element.getAsString("source_table"));
                count++;
            }
            Assertions.assertEquals(2, count);
            return null;
        });
        PAssert.that(outputY.getCollection()).satisfies(elements -> {
            int count = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals(2L, element.getAsLong("id"));
                Assertions.assertEquals("tmpl_y", element.getAsString("source_table"));
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    /**
     * The changeDataCapture mode: reads pending changes from a logical replication slot with
     * the pgoutput plugin (binary tuple mode), outputs provider-native change records, and the
     * cdc transform (format: postgres) normalizes them into the unified envelope.
     */
    @Test
    public void testSourceModuleChangeDataCapture() throws Exception {

        try(final Connection connection = connect();
            final Statement statement = connection.createStatement()) {

            statement.execute("CREATE TABLE cdc_items (id integer PRIMARY KEY, name text, price numeric(10,2), tags text[], updated timestamptz)");
            // full before-images on update/delete
            statement.execute("ALTER TABLE cdc_items REPLICA IDENTITY FULL");
            statement.execute("CREATE PUBLICATION cdc_pub FOR TABLE cdc_items");
            statement.execute("SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput')");

            // 4 change records retained in the slot before the pipeline starts
            statement.execute("INSERT INTO cdc_items VALUES (1, 'one', 10.50, '{\"a\",\"b\"}', '2023-11-14 22:13:20+00')");
            statement.execute("INSERT INTO cdc_items VALUES (2, 'two', NULL, NULL, NULL)");
            statement.execute("UPDATE cdc_items SET name = 'ONE', price = 11.00 WHERE id = 1");
            statement.execute("DELETE FROM cdc_items WHERE id = 2");
        }

        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "postgresCdc",
                      "module": "postgres",
                      "mode": "changeDataCapture",
                      "parameters": {
                        "url": "%s",
                        "user": "%s",
                        "password": "%s",
                        "cdc": {
                          "slot": "cdc_slot",
                          "publication": "cdc_pub",
                          "maxNumRecords": 4,
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
                      "parameters": {
                        "format": "postgres"
                      }
                    }
                  ]
                }
                """.formatted(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        final TestPipeline pipeline = createPipeline();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load(configJson));

        final MCollection rawOutput = outputs.get("postgresCdc");
        Assertions.assertNotNull(rawOutput.getSchema().getField("lsn"));
        Assertions.assertNotNull(rawOutput.getSchema().getField("keysJson"));

        PAssert.that(rawOutput.getCollection()).satisfies(elements -> {
            int inserts = 0, updates = 0, deletes = 0;
            for(final MElement element : elements) {
                Assertions.assertEquals("public", element.getAsString("schema"));
                Assertions.assertEquals("cdc_items", element.getAsString("table"));
                Assertions.assertTrue(element.getAsLong("commitLsn") > 0);
                switch (element.getAsString("op")) {
                    case "INSERT" -> {
                        inserts++;
                        final String newValues = element.getAsString("newValuesJson");
                        if(newValues.contains("\"id\":1")) {
                            Assertions.assertTrue(newValues.contains("\"name\":\"one\""), "unexpected: " + newValues);
                            Assertions.assertTrue(newValues.contains("\"price\":\"10.5\""), "unexpected: " + newValues);
                            Assertions.assertTrue(newValues.contains("\"tags\":[\"a\",\"b\"]"), "unexpected: " + newValues);
                            Assertions.assertTrue(newValues.contains("\"updated\":\"2023-11-14T22:13:20Z\""), "unexpected: " + newValues);
                        } else {
                            Assertions.assertTrue(newValues.contains("\"name\":\"two\""), "unexpected: " + newValues);
                            Assertions.assertTrue(newValues.contains("\"price\":null"), "unexpected: " + newValues);
                        }
                    }
                    case "UPDATE" -> {
                        updates++;
                        Assertions.assertEquals("{\"id\":1}", element.getAsString("keysJson"));
                        // REPLICA IDENTITY FULL: the whole before-image arrives
                        final String oldValues = element.getAsString("oldValuesJson");
                        Assertions.assertTrue(oldValues.contains("\"name\":\"one\""), "unexpected: " + oldValues);
                        final String newValues = element.getAsString("newValuesJson");
                        Assertions.assertTrue(newValues.contains("\"name\":\"ONE\""), "unexpected: " + newValues);
                        Assertions.assertTrue(newValues.contains("\"price\":\"11\""), "unexpected: " + newValues);
                    }
                    case "DELETE" -> {
                        deletes++;
                        Assertions.assertEquals("{\"id\":2}", element.getAsString("keysJson"));
                        Assertions.assertNull(element.getAsString("newValuesJson"));
                        final String oldValues = element.getAsString("oldValuesJson");
                        Assertions.assertTrue(oldValues.contains("\"name\":\"two\""), "unexpected: " + oldValues);
                    }
                    default -> Assertions.fail("unexpected op: " + element.getAsString("op"));
                }
            }
            Assertions.assertEquals(2, inserts);
            Assertions.assertEquals(1, updates);
            Assertions.assertEquals(1, deletes);
            return null;
        });

        final MCollection envelopeOutput = outputs.get("envelope");
        PAssert.that(envelopeOutput.getCollection()).satisfies(elements -> {
            int count = 0;
            String insertSequence1 = null, updateSequence = null;
            for(final MElement element : elements) {
                Assertions.assertEquals("cdc_items", element.getAsString("table"));
                final String sequence = element.getAsString("sequence");
                Assertions.assertTrue(com.mercari.solution.util.pipeline.cdc.ChangeRecord.isValidSequence(sequence),
                        "illegal sequence: " + sequence);
                final String keys = element.getAsString("keys");
                if("{\"id\":1}".equals(keys)) {
                    final String after = element.getAsString("after");
                    if(after != null && after.contains("\"name\":\"one\"")) {
                        insertSequence1 = sequence;
                    } else if(after != null && after.contains("\"name\":\"ONE\"")) {
                        updateSequence = sequence;
                    }
                }
                count++;
            }
            Assertions.assertEquals(4, count);
            // the update commits after the insert: its (commitLsn, seq) sequence must sort higher
            Assertions.assertNotNull(insertSequence1);
            Assertions.assertNotNull(updateSequence);
            Assertions.assertTrue(com.mercari.solution.util.pipeline.cdc.ChangeRecord
                    .compareSequence(updateSequence, insertSequence1) > 0);
            return null;
        });

        pipeline.run().waitUntilFinish();

        try(final Connection connection = connect();
            final Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_drop_replication_slot('cdc_slot')");
            statement.execute("DROP PUBLICATION cdc_pub");
        }
    }

    private static ByteBuffer toDecimalBytes(final BigDecimal decimal) {
        return ByteBuffer.wrap(decimal.setScale(9).unscaledValue().toByteArray());
    }

    private static List<String> stringList(final Object value) {
        return ((List<?>) value).stream().map(String::valueOf).toList();
    }

    private static List<Integer> intList(final Object value) {
        return ((List<?>) value).stream().map(v -> ((Number) v).intValue()).toList();
    }

    private static List<Long> longList(final Object value) {
        return ((List<?>) value).stream().map(v -> ((Number) v).longValue()).toList();
    }

}
