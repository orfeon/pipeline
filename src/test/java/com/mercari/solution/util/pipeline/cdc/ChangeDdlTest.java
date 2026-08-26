package com.mercari.solution.util.pipeline.cdc;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ChangeDdlTest {

    @Test
    public void testDiff() {
        final List<ChangeSchema.Column> previous = List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_STRING, false),
                new ChangeSchema.Column("old", ChangeSchema.TYPE_STRING, false));
        final List<ChangeSchema.Column> current = List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, true),
                new ChangeSchema.Column("name", ChangeSchema.TYPE_JSON, false),
                new ChangeSchema.Column("age", ChangeSchema.TYPE_INT64, false),
                new ChangeSchema.Column("tenant", ChangeSchema.TYPE_STRING, true));
        final ChangeDdl.Diff diff = ChangeDdl.diff(previous, current);
        Assertions.assertEquals(List.of(new ChangeSchema.Column("age", ChangeSchema.TYPE_INT64, false)), diff.added());
        Assertions.assertEquals(List.of(new ChangeSchema.Column("tenant", ChangeSchema.TYPE_STRING, true)), diff.addedKeys());
        Assertions.assertEquals(List.of(new ChangeSchema.Column("name", ChangeSchema.TYPE_JSON, false)), diff.typeChanged());
        Assertions.assertEquals(List.of(new ChangeSchema.Column("old", ChangeSchema.TYPE_STRING, false)), diff.dropped());
        Assertions.assertTrue(ChangeDdl.diff(current, current).isEmpty());
        Assertions.assertEquals(2, ChangeDdl.diff(null, current).added().size());
        Assertions.assertEquals(2, ChangeDdl.diff(null, current).addedKeys().size());
    }

    @Test
    public void testAddColumns() {
        final String ddl = ChangeDdl.addColumns(ChangeDdl.Dialect.bigquery, "p.d.t", List.of(
                new ChangeSchema.Column("age", ChangeSchema.TYPE_INT64, false),
                new ChangeSchema.Column("tags", "ARRAY<STRING>", false),
                new ChangeSchema.Column("at", ChangeSchema.TYPE_DATETIME, false)));
        Assertions.assertEquals(
                "ALTER TABLE `p.d.t` ADD COLUMN IF NOT EXISTS `age` INT64;\n"
                        + "ALTER TABLE `p.d.t` ADD COLUMN IF NOT EXISTS `tags` ARRAY<STRING>;\n"
                        + "ALTER TABLE `p.d.t` ADD COLUMN IF NOT EXISTS `at` DATETIME;",
                ddl);
        Assertions.assertNull(ChangeDdl.addColumns(ChangeDdl.Dialect.bigquery, "p.d.t", List.of()));
        Assertions.assertEquals("TRUNCATE TABLE `p.d.t`;", ChangeDdl.truncate(ChangeDdl.Dialect.bigquery, "p:d.t"));
    }

    @Test
    public void testToBigQueryType() {
        Assertions.assertEquals("FLOAT64", ChangeDdl.toBigQueryType(ChangeSchema.TYPE_FLOAT32));
        Assertions.assertEquals("STRING", ChangeDdl.toBigQueryType(ChangeSchema.TYPE_UUID));
        Assertions.assertEquals("ARRAY<INT64>", ChangeDdl.toBigQueryType("ARRAY<INT64>"));
        Assertions.assertEquals("JSON", ChangeDdl.toBigQueryType("STRUCT<a INT64>"));
        Assertions.assertEquals("STRING", ChangeDdl.toBigQueryType("WHATEVER"));
    }

    @Test
    public void testFromBigQuerySchema() {
        final TableSchema schema = new TableSchema().setFields(List.of(
                new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("tags").setType("STRING").setMode("REPEATED"),
                new TableFieldSchema().setName("at").setType("DATETIME"),
                new TableFieldSchema().setName("addr").setType("RECORD")));
        Assertions.assertEquals(List.of(
                new ChangeSchema.Column("id", ChangeSchema.TYPE_INT64, false),
                new ChangeSchema.Column("tags", "ARRAY<STRING>", false),
                new ChangeSchema.Column("at", ChangeSchema.TYPE_DATETIME, false),
                new ChangeSchema.Column("addr", "STRUCT<>", false)), ChangeDdl.fromBigQuerySchema(schema));
        Assertions.assertNull(ChangeDdl.fromBigQuerySchema(null));
    }

}
