package com.mercari.solution.util.pipeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.AvroToElementConverter;
import com.mercari.solution.util.schema.converter.AvroToJsonConverter;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class Debug {

    private static final Logger LOG = LoggerFactory.getLogger(Debug.class);

    public static DebugTransform create(
            final Schema inputSchema,
            final String workDirPath,
            final String name,
            final MErrorHandler errorHandler) {

        return new DebugTransform(name, workDirPath, inputSchema, errorHandler);
    }

    public static class DebugTransform extends PTransform<PCollection<MElement>, WriteFilesResult<Void>> {

        private final String name;
        private final String workDirPath;
        private final Schema inputSchema;
        final MErrorHandler errorHandler;

        private DebugTransform(
                final String name,
                final String workDirPath,
                final Schema inputSchema,
                final MErrorHandler errorHandler) {

            this.name = name;
            this.workDirPath = workDirPath;
            this.inputSchema = inputSchema;
            this.errorHandler = errorHandler;
        }

        @Override
        public WriteFilesResult<Void> expand(PCollection<MElement> input) {

            final String outputSinkDirPath = addDirPath(workDirPath, "outputs/" + name);
            final String tempSinkDirPath = addDirPath(workDirPath, "temp/" + name);
            final org.apache.avro.Schema outputAvroSchema = createOutputAvroSchema(inputSchema);

            writeAvroSchemaFile(outputSinkDirPath + "/schema.avro", outputAvroSchema);

            FileIO.Write<Void, GenericRecord> write = FileIO
                    .<GenericRecord>write()
                    .to(outputSinkDirPath)
                    .via(AvroIO.sink(outputAvroSchema))
                    .withTempDirectory(tempSinkDirPath);

            PCollection<GenericRecord> records = input
                    .apply("Format", ParDo.of(new FormatDoFn(inputSchema)))
                    .setCoder(AvroCoder.of(outputAvroSchema));

            if(OptionUtil.isUnbounded(input)) {
                // The default windowed file naming embeds the window boundary ISO timestamps,
                // which contain ':' characters that are illegal in Windows file names and make
                // the write fail at the finalize (rename) step. Use a portable naming instead.
                write = write.withNaming(new PortableWindowedNaming());
                records = records
                        .apply("Window", Window
                                .<GenericRecord>into(FixedWindows.of(Duration.standardSeconds(10L)))
                                .triggering(Repeatedly
                                        .forever(AfterFirst.of(
                                                AfterProcessingTime
                                                        .pastFirstElementInPane()
                                                        .plusDelayOf(Duration.standardSeconds(5)),
                                                AfterPane
                                                        .elementCountAtLeast(10))))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes());
            }
            errorHandler.apply(write);
            return records.apply("Write", write);
        }

        /**
         * File naming for windowed (unbounded input) writes that avoids characters illegal on
         * some filesystems (the default windowed naming embeds ':' from ISO timestamps, which
         * fails on Windows).
         */
        private static class PortableWindowedNaming implements FileIO.Write.FileNaming {
            @Override
            public String getFilename(
                    final BoundedWindow window,
                    final PaneInfo pane,
                    final int numShards,
                    final int shardIndex,
                    final Compression compression) {
                final String windowString;
                if(window instanceof GlobalWindow) {
                    windowString = "global";
                } else {
                    windowString = Long.toString(window.maxTimestamp().getMillis());
                }
                return String.format("output-%s-%d-%05d-of-%05d.avro",
                        windowString, pane.getIndex(), shardIndex, numShards);
            }
        }

        private static class FormatDoFn extends DoFn<MElement, GenericRecord> {

            private final Schema schema;

            private transient org.apache.avro.Schema avroSchema;

            FormatDoFn(final Schema schema) {
                this.schema = schema;
            }

            @Setup
            public void setup() {
                this.schema.setup(DataType.AVRO);
                this.avroSchema = Debug.createOutputAvroSchema(schema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement input = c.element();
                if(input == null) {
                    return;
                }

                try {
                    long epochMillis = c.timestamp().getMillis();
                    if ((Long.MIN_VALUE / 1000) == epochMillis) {
                        epochMillis = 0;
                    }
                    final GenericRecord record = ElementToAvroConverter.convert(schema, input);
                    final GenericRecord output = new GenericRecordBuilder(avroSchema)
                            .set("timestamp", epochMillis * 1000L)
                            .set("data", record)
                            .build();
                    c.output(output);
                } catch (Throwable e) {
                    LOG.error("debug error: {}", e.getMessage());
                }
            }

        }
    }

    public static org.apache.avro.Schema createOutputAvroSchema(final Schema inputSchema) {
        return SchemaBuilder
                .record("DebugRecord")
                .fields()
                .name("timestamp").type(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("data").type(inputSchema.getAvroSchema()).noDefault()
                .endRecord();
    }

    public static JsonArray readDebugOutputs(final TempDirectory tempDirectory) {
        final JsonArray outputsArray = new JsonArray();
        if(tempDirectory == null || tempDirectory.getPath() == null) {
            return outputsArray;
        }
        final Path outputDirPath = Path.of(addDirPath(tempDirectory.getPath().toAbsolutePath().toString(),  "outputs/"));
        if(!outputDirPath.toFile().exists()) {
            LOG.info("no debug records");
            return outputsArray;
        }
        try (final Stream<Path> subDirs = Files.list(outputDirPath)) {
            subDirs.filter(Files::isDirectory)
                    .map(Debug::readWorkOutputs)
                    .forEach(outputsArray::add);
        } catch (IOException e) {
            LOG.error("Failed to workDir: {}", tempDirectory, e);
        }
        return outputsArray;
    }

    private static JsonObject readWorkOutputs(Path subDir) {
        final JsonObject outputObject = new JsonObject();
        outputObject.addProperty("name", subDir.getFileName().toString());
        outputObject.add("records", readRecordsFromDir(subDir));
        final Path schemaPath = Path.of(addDirPath(subDir.toAbsolutePath().toString(), "schema.avro"));
        if(schemaPath.toFile().exists()) {
            final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try(final InputStream is = new FileInputStream(schemaPath.toFile());
                final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(is, datumReader)) {
                final org.apache.avro.Schema schema = dataFileReader.getSchema();
                final org.apache.avro.Schema.Field field = schema.getField("data");
                final List<Schema.Field> fields = AvroToElementConverter.convertFields(field.schema().getFields());
                outputObject.add("schema", Schema.of(fields).toJsonObject());
            } catch (Throwable e) {
                LOG.error("Failed to read output avro file: {}", schemaPath, e);
            }
        }
        return outputObject;
    }

    private static JsonArray readRecordsFromDir(Path dir) {
        final JsonArray dataArray = new JsonArray();
        final List<GenericRecord> records = new ArrayList<>();
        try (final Stream<Path> files = Files.list(dir).filter(Files::isRegularFile)) {
            files.flatMap(Debug::readRecordsFromFile).forEach(records::add);
        } catch (IOException e) {
            LOG.error("Failed to read directory: {}", dir, e);
        }
        records.stream()
                .sorted(Comparator.comparing((GenericRecord r) -> (Long) r.get("timestamp")).reversed())
                .map(AvroToJsonConverter::convertObject)
                .forEach(dataArray::add);
        return dataArray;
    }

    private static Stream<GenericRecord> readRecordsFromFile(Path file) {
        try {
            final byte[] bytes = Files.readAllBytes(file.toAbsolutePath());
            return AvroSchemaUtil.decodeFile(bytes).stream();
        } catch (IOException e) {
            LOG.error("Failed to read file: {}", file.getFileName(), e);
            return Stream.of();
        }
    }

    private static void writeAvroSchemaFile(
            final String path,
            final org.apache.avro.Schema schema) {
        final File file = new File(path);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, file);
        } catch (IOException e) {
            LOG.error("Failed to write avro schema file: {}", file, e);
        }
    }

    public static TempDirectory createTempDirectory(boolean dryRun) throws IOException {
        return new TempDirectory(dryRun);
    }

    public static class TempDirectory implements AutoCloseable {

        private final Path path;

        public TempDirectory(boolean dryRun) throws IOException {
            if(dryRun) {
                this.path = null;
            } else {
                this.path = Files.createTempDirectory("mercari-pipeline-");
            }
        }

        public Path getPath() {
            return path;
        }

        @Override
        public void close() {
            if(path == null) {
                return;
            }
            try(final Stream<Path> stream = Files.walk(path)) {
                final long count = stream
                        .sorted(Comparator.reverseOrder()) // DO NOT REMOVE. To ensure the order is maintained so that the parent file is not deleted first
                        .filter(p -> {
                            try {
                                Files.delete(p);
                                return true;
                            } catch (final IOException e) {
                                LOG.warn("Failed to delete file: {}", p, e);
                                return false;
                            }
                        })
                        .count();
                LOG.info("delete work files: {}", count);
            } catch (IOException e) {
                LOG.warn("Failed to walk directory for deletion: {}", path, e);
            }
        }
    }

    private static String addDirPath(String parent, String child) {
        if(parent == null) {
            return null;
        }
        if(child == null) {
            return parent;
        }
        if(parent.endsWith("/")) {
            return parent + child;
        } else {
            return parent + "/" + child;
        }
    }

}
