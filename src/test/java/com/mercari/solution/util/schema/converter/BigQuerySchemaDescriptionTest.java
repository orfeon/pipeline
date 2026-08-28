package com.mercari.solution.util.schema.converter;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * BigQuery field descriptions: table metadata → Avro doc / Schema.Field.description (source side)
 * and Schema.Field.description → TableFieldSchema.description (sink side, CREATE_IF_NEEDED).
 */
public class BigQuerySchemaDescriptionTest {

    private static TableSchema tableSchema() {
        return new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED").setDescription("member id"),
                new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("empty").setType("STRING").setMode("NULLABLE").setDescription(""),
                new TableFieldSchema().setName("tags").setType("STRING").setMode("REPEATED").setDescription("tag list"),
                new TableFieldSchema().setName("address").setType("RECORD").setMode("NULLABLE").setDescription("address record")
                        .setFields(Arrays.asList(
                                new TableFieldSchema().setName("zip").setType("STRING").setMode("NULLABLE").setDescription("zip code"),
                                new TableFieldSchema().setName("city").setType("STRING").setMode("NULLABLE")))));
    }

    @Test
    public void testTableSchemaToAvroDoc() {
        final org.apache.avro.Schema avro = AvroSchemaUtil.convertSchema(tableSchema());

        Assertions.assertEquals("member id", avro.getField("id").doc());
        Assertions.assertNull(avro.getField("name").doc());
        Assertions.assertNull(avro.getField("empty").doc());
        Assertions.assertEquals("tag list", avro.getField("tags").doc());
        Assertions.assertEquals("address record", avro.getField("address").doc());
        final org.apache.avro.Schema address = AvroSchemaUtil.unnestUnion(avro.getField("address").schema());
        Assertions.assertEquals("zip code", address.getField("zip").doc());
        Assertions.assertNull(address.getField("city").doc());

        // the element schema (what the dry-run reports) carries the doc as description
        final Schema schema = Schema.of(avro);
        Assertions.assertEquals("member id", schema.getField("id").getDescription());
        Assertions.assertEquals("zip code",
                schema.getField("address").getFieldType().getElementSchema().getField("zip").getDescription());
        Assertions.assertEquals("member id",
                schema.toJsonObject().getAsJsonArray("fields").get(0).getAsJsonObject().get("description").getAsString());
    }

    @Test
    public void testTableSchemaToElementSchema() {
        final Schema schema = Schema.of(tableSchema());
        Assertions.assertEquals("member id", schema.getField("id").getDescription());
        Assertions.assertNull(schema.getField("name").getDescription());
        Assertions.assertNull(schema.getField("empty").getDescription());
        Assertions.assertEquals("address record", schema.getField("address").getDescription());
        Assertions.assertEquals("zip code",
                schema.getField("address").getFieldType().getElementSchema().getField("zip").getDescription());
    }

    @Test
    public void testMergeDescriptionsIntoStorageReadSchema() {
        // the Storage read session schema has no doc; simulate it by stripping descriptions
        final TableSchema noDescription = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("tags").setType("STRING").setMode("REPEATED"),
                new TableFieldSchema().setName("address").setType("RECORD").setMode("NULLABLE")
                        .setFields(Arrays.asList(
                                new TableFieldSchema().setName("zip").setType("STRING").setMode("NULLABLE")))));
        final org.apache.avro.Schema sessionSchema = AvroSchemaUtil.convertSchema(noDescription);
        Assertions.assertNull(sessionSchema.getField("id").doc());

        final org.apache.avro.Schema merged = AvroSchemaUtil.mergeDescriptions(sessionSchema, tableSchema());

        Assertions.assertEquals("member id", merged.getField("id").doc());
        Assertions.assertNull(merged.getField("name").doc());
        Assertions.assertEquals("tag list", merged.getField("tags").doc());
        Assertions.assertEquals("address record", merged.getField("address").doc());
        Assertions.assertEquals("zip code",
                AvroSchemaUtil.unnestUnion(merged.getField("address").schema()).getField("zip").doc());
        // types are untouched
        Assertions.assertEquals(sessionSchema.getField("id").schema(), merged.getField("id").schema());
        Assertions.assertEquals(sessionSchema.getField("tags").schema().getType(), merged.getField("tags").schema().getType());
        // null-safe
        Assertions.assertSame(sessionSchema, AvroSchemaUtil.mergeDescriptions(sessionSchema, (TableSchema) null));
    }

    @Test
    public void testTableDescriptionBecomesRecordDoc() {
        final Table table = new Table().setSchema(tableSchema()).setDescription("members table");

        // direct runner path
        final org.apache.avro.Schema direct = AvroSchemaUtil.withDoc(AvroSchemaUtil.convertSchema(table.getSchema()), table.getDescription());
        Assertions.assertEquals("members table", direct.getDoc());
        Assertions.assertEquals("member id", direct.getField("id").doc());
        Assertions.assertEquals("members table", Schema.of(direct).getDescription());

        // dataflow (storage read) path
        final org.apache.avro.Schema session = AvroSchemaUtil.convertSchema(new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"))));
        final org.apache.avro.Schema merged = AvroSchemaUtil.mergeDescriptions(session, table);
        Assertions.assertEquals("members table", merged.getDoc());
        Assertions.assertEquals("member id", merged.getField("id").doc());

        // no description: schema untouched
        Assertions.assertSame(session, AvroSchemaUtil.withDoc(session, null));
        Assertions.assertSame(session, AvroSchemaUtil.withDoc(session, ""));
        Assertions.assertNull(Schema.of(session).getDescription());
    }

    @Test
    public void testElementSchemaToTableFieldSchemaDescription() {
        final Schema schema = Schema.parse("""
                {
                  "fields": [
                    { "name": "id", "type": "long", "mode": "required", "description": "member id" },
                    { "name": "name", "type": "string" },
                    { "name": "tags", "type": "string", "mode": "repeated", "description": "tag list" },
                    { "name": "address", "type": "element", "description": "address record", "fields": [
                      { "name": "zip", "type": "string", "description": "zip code" }
                    ] }
                  ]
                }
                """);

        final TableSchema tableSchema = ElementToTableRowConverter.convertSchema(schema);
        final TableFieldSchema id = tableSchema.getFields().get(0);
        final TableFieldSchema name = tableSchema.getFields().get(1);
        final TableFieldSchema tags = tableSchema.getFields().get(2);
        final TableFieldSchema address = tableSchema.getFields().get(3);

        Assertions.assertEquals("member id", id.getDescription());
        Assertions.assertNull(name.getDescription());
        Assertions.assertEquals("tag list", tags.getDescription());
        Assertions.assertEquals("REPEATED", tags.getMode());
        Assertions.assertEquals("address record", address.getDescription());
        Assertions.assertEquals("zip code", address.getFields().get(0).getDescription());
    }

}
