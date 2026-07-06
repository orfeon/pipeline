package com.mercari.solution.module.source;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for the storage source's fields projection semantics (schema-redesign.md Phase 4).
 *
 * Reading local Avro files needs no schema sampling as long as a schema is declared
 * (sampling itself is gs://・s3:// only), so these run on the DirectRunner without GCS.
 * The Avro record name of the declared schema is "root" (ElementToAvroConverter), so the
 * test files are written with a writer schema named "root" to satisfy Avro name resolution.
 */
public class StorageSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @TempDir
    Path tempDir;

    private static final org.apache.avro.Schema WRITER_SCHEMA = new org.apache.avro.Schema.Parser().parse("""
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

    private String writeAvroFile() throws Exception {
        final File file = tempDir.resolve("test.avro").toFile();
        try(final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(WRITER_SCHEMA))) {
            writer.create(WRITER_SCHEMA, file);
            for(int i = 1; i <= 3; i++) {
                final GenericRecord record = new GenericData.Record(WRITER_SCHEMA);
                record.put("id", (long) i);
                record.put("name", "name" + i);
                record.put("category", "category" + i);
                writer.append(record);
            }
        }
        // Beam FileSystems misreads a Windows drive letter ("C:/...") as a URI scheme,
        // so local test files are addressed relative to the working directory
        final Path workingDir = Path.of("").toAbsolutePath();
        return workingDir.relativize(file.toPath().toAbsolutePath()).toString().replace('\\', '/');
    }

    @Test
    public void testDeclaredSchemaSubsetProjectsColumns() throws Exception {
        // schema.fields is the output logical shape: a subset of the writer schema acts as the
        // Avro reader schema, so unlisted writer fields are skipped during decode
        final String path = writeAvroFile();
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "storageinput",
                      "module": "storage",
                      "parameters": {
                        "input": "%s",
                        "format": "avro",
                        "schema": {
                          "fields": [
                            { "name": "id", "type": "long" },
                            { "name": "name", "type": "string" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """.formatted(path);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("storageinput");
        Assertions.assertTrue(output.getSchema().hasField("id"));
        Assertions.assertTrue(output.getSchema().hasField("name"));
        Assertions.assertFalse(output.getSchema().hasField("category"));

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
                Assertions.assertNotNull(element.getAsString("name"));
                final GenericRecord record = (GenericRecord) element.getValue();
                Assertions.assertNull(record.getSchema().getField("category"));
            }
            Assertions.assertEquals(Set.of(1L, 2L, 3L), ids);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testParametersFieldsProjectsAvro() throws Exception {
        // parameters.fields now projects for avro too (it used to be silently ignored)
        final String path = writeAvroFile();
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "storageinput",
                      "module": "storage",
                      "parameters": {
                        "input": "%s",
                        "format": "avro",
                        "fields": ["id"],
                        "schema": {
                          "fields": [
                            { "name": "id", "type": "long" },
                            { "name": "name", "type": "string" },
                            { "name": "category", "type": "string" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """.formatted(path);

        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);

        final MCollection output = outputs.get("storageinput");
        Assertions.assertTrue(output.getSchema().hasField("id"));
        Assertions.assertFalse(output.getSchema().hasField("name"));
        Assertions.assertFalse(output.getSchema().hasField("category"));

        PAssert.that(output.getCollection()).satisfies(elements -> {
            final Set<Long> ids = new HashSet<>();
            for(final MElement element : elements) {
                ids.add(element.getAsLong("id"));
            }
            Assertions.assertEquals(Set.of(1L, 2L, 3L), ids);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnknownProjectionFieldThrows() throws Exception {
        // a projection name missing from the input schema is an assembly-time error,
        // not a silent drop
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "storageinput",
                      "module": "storage",
                      "parameters": {
                        "input": "/tmp/input/*.avro",
                        "format": "avro",
                        "fields": ["id", "noSuchField"],
                        "schema": {
                          "fields": [ { "name": "id", "type": "long" } ]
                        }
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("noSuchField"),
                "unexpected message: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("are not present in the input schema"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testUnknownProjectionFieldThrowsForParquet() throws Exception {
        // the same validation guards the parquet projection (previously a typo produced an
        // all-nullable schema with no error)
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "storageinput",
                      "module": "storage",
                      "parameters": {
                        "input": "/tmp/input/*.parquet",
                        "format": "parquet",
                        "fields": ["noSuchField"],
                        "schema": {
                          "fields": [ { "name": "id", "type": "long" } ]
                        }
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("noSuchField"),
                "unexpected message: " + e.getMessage());
    }

}
