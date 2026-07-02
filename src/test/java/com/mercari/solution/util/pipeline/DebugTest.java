package com.mercari.solution.util.pipeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.apache.avro.LogicalTypes;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class DebugTest {

    private static Schema createSchema() {
        return Schema.of(List.of(
                Schema.Field.of("stringField", Schema.FieldType.STRING),
                Schema.Field.of("longField", Schema.FieldType.INT64)));
    }

    @Test
    public void testCreateOutputAvroSchema() {
        final Schema schema = createSchema();
        final org.apache.avro.Schema avroSchema = Debug.createOutputAvroSchema(schema);

        Assertions.assertEquals("DebugRecord", avroSchema.getName());
        Assertions.assertNotNull(avroSchema.getField("timestamp"));
        Assertions.assertNotNull(avroSchema.getField("data"));
        Assertions.assertEquals(
                LogicalTypes.timestampMicros(),
                avroSchema.getField("timestamp").schema().getLogicalType());
        final org.apache.avro.Schema dataSchema = avroSchema.getField("data").schema();
        Assertions.assertNotNull(dataSchema.getField("stringField"));
        Assertions.assertNotNull(dataSchema.getField("longField"));
    }

    @Test
    public void testTempDirectoryDryRun() throws Exception {
        try(final Debug.TempDirectory tempDirectory = Debug.createTempDirectory(true)) {
            Assertions.assertNull(tempDirectory.getPath());
            // no outputs for dry run temp directory
            final JsonArray outputs = Debug.readDebugOutputs(tempDirectory);
            Assertions.assertEquals(0, outputs.size());
        }
        Assertions.assertEquals(0, Debug.readDebugOutputs(null).size());
    }

    @Test
    public void testTempDirectoryCreateAndClose() throws Exception {
        final Debug.TempDirectory tempDirectory = Debug.createTempDirectory(false);
        final Path path = tempDirectory.getPath();
        Assertions.assertNotNull(path);
        Assertions.assertTrue(Files.exists(path));

        // no outputs directory yet
        final JsonArray outputs = Debug.readDebugOutputs(tempDirectory);
        Assertions.assertEquals(0, outputs.size());

        // create some content and ensure close() removes everything recursively
        Files.createDirectories(path.resolve("outputs/mymodule"));
        Files.writeString(path.resolve("outputs/mymodule/file.txt"), "content");

        tempDirectory.close();
        Assertions.assertFalse(Files.exists(path));
    }

    @Test
    public void testReadDebugOutputs() throws Exception {
        final Schema schema = createSchema();
        final org.apache.avro.Schema debugSchema = Debug.createOutputAvroSchema(schema);
        final org.apache.avro.Schema dataSchema = debugSchema.getField("data").schema();

        try(final Debug.TempDirectory tempDirectory = Debug.createTempDirectory(false)) {
            final Path moduleDir = tempDirectory.getPath().resolve("outputs").resolve("mymodule");
            Files.createDirectories(moduleDir);

            // schema file (avro container file with no records)
            try(final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(debugSchema))) {
                writer.create(debugSchema, moduleDir.resolve("schema.avro").toFile());
            }

            // records file
            final GenericRecord data1 = new GenericRecordBuilder(dataSchema)
                    .set("stringField", "a")
                    .set("longField", 1L)
                    .build();
            final GenericRecord record1 = new GenericRecordBuilder(debugSchema)
                    .set("timestamp", 1000000L)
                    .set("data", data1)
                    .build();
            final GenericRecord data2 = new GenericRecordBuilder(dataSchema)
                    .set("stringField", "b")
                    .set("longField", 2L)
                    .build();
            final GenericRecord record2 = new GenericRecordBuilder(debugSchema)
                    .set("timestamp", 2000000L)
                    .set("data", data2)
                    .build();
            try(final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(debugSchema))) {
                writer.create(debugSchema, moduleDir.resolve("records-00000.avro").toFile());
                writer.append(record1);
                writer.append(record2);
            }

            final JsonArray outputs = Debug.readDebugOutputs(tempDirectory);
            Assertions.assertEquals(1, outputs.size());

            final JsonObject output = outputs.get(0).getAsJsonObject();
            Assertions.assertEquals("mymodule", output.get("name").getAsString());
            Assertions.assertTrue(output.has("schema"));

            final JsonArray records = output.getAsJsonArray("records");
            Assertions.assertEquals(2, records.size());
            // records are sorted by timestamp descending
            final JsonObject first = records.get(0).getAsJsonObject();
            final JsonObject second = records.get(1).getAsJsonObject();
            Assertions.assertEquals("b", first.getAsJsonObject("data").get("stringField").getAsString());
            Assertions.assertEquals("a", second.getAsJsonObject("data").get("stringField").getAsString());
        }
    }

}
