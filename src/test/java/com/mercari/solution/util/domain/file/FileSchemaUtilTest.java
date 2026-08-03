package com.mercari.solution.util.domain.file;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSchemaUtilTest {

    @TempDir
    Path tempDir;

    private static final Schema WRITER_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "root",
              "fields": [
                { "name": "id", "type": "long" },
                { "name": "name", "type": "string" },
                { "name": "category", "type": "string" }
              ]
            }
            """);

    // Beam FileSystems misreads a Windows drive letter ("C:/...") as a URI scheme,
    // so local test files are addressed relative to the working directory
    private static String relativize(final Path path) {
        final Path workingDir = Path.of("").toAbsolutePath();
        return workingDir.relativize(path.toAbsolutePath()).toString().replace('\\', '/');
    }

    private void writeParquetFile(final Path path) throws Exception {
        try(final ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(path))
                .withConf(new PlainParquetConfiguration())
                .withDataModel(GenericData.get())
                .withSchema(WRITER_SCHEMA)
                .build()) {
            for(int i = 1; i <= 3; i++) {
                final GenericRecord record = new GenericData.Record(WRITER_SCHEMA);
                record.put("id", (long) i);
                record.put("name", "name" + i);
                record.put("category", "category" + i);
                writer.write(record);
            }
        }
    }

    private void writeAvroFile(final Path path) throws Exception {
        try(final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(WRITER_SCHEMA))) {
            writer.create(WRITER_SCHEMA, path.toFile());
            for(int i = 1; i <= 3; i++) {
                final GenericRecord record = new GenericData.Record(WRITER_SCHEMA);
                record.put("id", (long) i);
                record.put("name", "name" + i);
                record.put("category", "category" + i);
                writer.append(record);
            }
        }
    }

    @Test
    public void testGetParquetSchemaFromExactPath() throws Exception {
        final Path file = tempDir.resolve("test.parquet");
        writeParquetFile(file);

        final Schema schema = FileSchemaUtil.getParquetSchema(relativize(file));

        Assertions.assertNotNull(schema.getField("id"));
        Assertions.assertNotNull(schema.getField("name"));
        Assertions.assertNotNull(schema.getField("category"));
        Assertions.assertEquals(Schema.Type.LONG, schema.getField("id").schema().getType());
    }

    @Test
    public void testGetParquetSchemaFromGlobSkipsEmptyAndBrokenFiles() throws Exception {
        // glob resolution matches the runtime IO; zero-length placeholders (_SUCCESS) are
        // filtered by size and unreadable files are skipped with a warning
        Files.write(tempDir.resolve("_SUCCESS"), new byte[0]);
        Files.write(tempDir.resolve("aaa-broken.parquet"), "not a parquet file".getBytes(StandardCharsets.UTF_8));
        writeParquetFile(tempDir.resolve("part-00000.parquet"));

        final Schema schema = FileSchemaUtil.getParquetSchema(relativize(tempDir) + "/*");

        Assertions.assertNotNull(schema.getField("id"));
        Assertions.assertNotNull(schema.getField("name"));
        Assertions.assertNotNull(schema.getField("category"));
    }

    @Test
    public void testGetAvroSchemaFromGlob() throws Exception {
        Files.write(tempDir.resolve("_SUCCESS"), new byte[0]);
        writeAvroFile(tempDir.resolve("part-00000.avro"));

        final Schema schema = FileSchemaUtil.getAvroSchema(relativize(tempDir) + "/*.avro");

        Assertions.assertEquals(WRITER_SCHEMA.getFields().size(), schema.getFields().size());
        Assertions.assertNotNull(schema.getField("id"));
    }

    @Test
    public void testNoFilesFoundThrows() {
        final String pattern = relativize(tempDir) + "/*.parquet";
        final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> FileSchemaUtil.getParquetSchema(pattern));
        Assertions.assertTrue(e.getMessage().contains(pattern), "unexpected message: " + e.getMessage());
    }

    @Test
    public void testOnlyBrokenFilesThrows() throws Exception {
        Files.write(tempDir.resolve("broken.parquet"), "not a parquet file".getBytes(StandardCharsets.UTF_8));
        final String pattern = relativize(tempDir) + "/*.parquet";
        final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> FileSchemaUtil.getParquetSchema(pattern));
        Assertions.assertTrue(e.getMessage().contains("could not be read"), "unexpected message: " + e.getMessage());
    }

}
