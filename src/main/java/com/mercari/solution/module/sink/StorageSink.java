package com.mercari.solution.module.sink;

import com.mercari.solution.module.*;
import com.mercari.solution.module.sink.fileio.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.FileUtil;
import com.mercari.solution.util.pipeline.Union;
import freemarker.template.Template;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

@Sink.Module(name="storage", schema=true)
public class StorageSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(StorageSink.class);

    public static class Parameters implements Serializable {

        private String output;
        private FileUtil.Format format;
        private String suffix;
        private String tempDirectory;
        private Integer numShards;
        private Compression compression;
        private FileUtil.CodecName codec;
        private Boolean noSpilling;
        private Integer maxNumWritersPerBundle;

        // csv
        private Boolean header;
        private Boolean bom;

        // inner use
        private List<String> outputTemplateArgs;

        public Parameters validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("parameters.output must not be null");
            } else {
                final String prefix = getPrefix(this.output);
                final String[] paths = this.output.replaceFirst(prefix, "").split("/", 2);
                if(paths.length < 2 || paths[1].isEmpty()) {
                    errorMessages.add("parameters.output[" + this.output + "] must contain an object path after the bucket, e.g. gs://bucket/path/to/object");
                }
                if(this.output.startsWith("gs://")) {
                    /*
                    final String bucketName = StorageUtil.getBucketName(this.output);
                    if(!TemplateUtil.isTemplateText(bucketName) && !StorageUtil.existsBucket(this.output)) {
                        errorMessages.add("parameters.output[" + this.output + "] bucket does not exist or is not accessible.");
                    }
                    if(!StorageUtil.checkBucketPermissions(bucketName, Set.of("storage.objects.create"))) {
                        errorMessages.add("parameters.output[" + this.output + "] bucket requires write permission for worker service account");
                    }

                     */
                }
            }
            if(this.format == null) {
                errorMessages.add("parameters.format must not be null");
            }
            if(Boolean.TRUE.equals(this.noSpilling) && this.maxNumWritersPerBundle != null) {
                errorMessages.add("parameters.noSpilling and parameters.maxNumWritersPerBundle must not be set together");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
            return this;
        }

        public Parameters setDefaults() {
            if(this.suffix == null) {
                this.suffix = "";
            }
            if(this.codec == null) {
                this.codec = FileUtil.CodecName.SNAPPY;
            }
            if(this.noSpilling == null) {
                this.noSpilling = false;
            }

            // For CSV format
            if(this.header == null) {
                this.header = false;
            }
            if(this.bom == null) {
                this.bom = false;
            }
            return this;
        }
    }

    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class)
                .validate()
                .setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);
        final Schema outputSchema = Optional.ofNullable(getSchema()).orElse(inputSchema);

        final PCollection<MElement> outputFiles = expand(
                getName(), parameters, input, inputSchema, outputSchema, errorHandler);

        // The written-files records are the sink's result (control records): downstream steps
        // wait on them, and action modules may consume them as inputs (e.g. load each written
        // file with a bigquery action, or keep a history file with a storage action).
        return MCollectionTuple
                .of(outputFiles, createFileSchema());
    }

    public static Schema createFileSchema() {
        return OutputDoFn.schema;
    }

    public static PCollection<MElement> expand(
            final String name,
            final Parameters parameters,
            final PCollection<MElement> input,
            final Schema inputSchema,
            final Schema outputSchema,
            final MErrorHandler errorHandler) {

        final String object = getObject(parameters.output);

        final FileIO.Sink<MElement> sink = switch (parameters.format) {
            case csv -> {
                final List<String> fields = outputSchema.getFields().stream()
                        .map(Schema.Field::getName)
                        .toList();
                yield CsvSink.of(outputSchema, fields, parameters.header, null, parameters.bom);
            }
            case json -> JsonSink.of(outputSchema, false, parameters.bom);
            case avro, parquet -> {
                final boolean fitSchema = false;// Optional.ofNullable(schema.getUseDestinationSchema()).orElse(false);
                yield switch (parameters.format) {
                    case avro -> AvroSink.of(outputSchema, parameters.codec, fitSchema);
                    case parquet -> ParquetSink.of(outputSchema, parameters.codec, fitSchema);
                    default -> throw new IllegalArgumentException();
                };
            }
        };

        final String label = switch (parameters.format) {
            case csv -> "WriteCSV";
            case json -> "WriteJSON";
            case avro -> "WriteAvro";
            case parquet -> "WriteParquet";
        };

        final WriteFilesResult writeFilesResult;
        if(TemplateUtil.isTemplateText(object)) {
            final List<String> outputTemplateArgs = Optional
                    .ofNullable(parameters.outputTemplateArgs)
                    .orElseGet(() -> TemplateUtil.extractTemplateArgs(object, inputSchema));
            final FileIO.Write<String, MElement> write = createDynamicWrite(
                    name, parameters, inputSchema, outputTemplateArgs, errorHandler);
            writeFilesResult = input.apply(label + "Dynamic", write.via(sink));
        } else {
            final FileIO.Write<Void, MElement> write = createWrite(parameters, errorHandler);
            writeFilesResult = input.apply(label, write.via(sink));
        }

        final PCollection<KV> rows = writeFilesResult.getPerDestinationOutputFilenames();
        return rows
                .apply("Output", ParDo.of(new OutputDoFn(name)))
                .setCoder(ElementCoder.of(OutputDoFn.schema));
    }

    private static FileIO.Write<Void, MElement> createWrite(
            final Parameters parameters,
            final MErrorHandler errorHandler) {

        final String bucket = getBucket(parameters.output);
        final String object = getObject(parameters.output);
        final String suffix = parameters.suffix;
        final Integer numShards = parameters.numShards;

        final FileIO.Write<Void, MElement> write = FileIO
                .<MElement>write()
                .to(bucket)
                .withNaming(getFileNaming(object, suffix, numShards));

        return applyWriteOptions(write, parameters, errorHandler);
    }

    private static FileIO.Write<String, MElement> createDynamicWrite(
            final String name,
            final Parameters parameters,
            final Schema inputSchema,
            final List<String> outputTemplateArgs,
            final MErrorHandler errorHandler) {

        final String bucket = getBucket(parameters.output);
        final String object = getObject(parameters.output);
        final String suffix = parameters.suffix;
        final Integer numShards = parameters.numShards;

        final FileIO.Write<String, MElement> write = FileIO
                .<String, MElement>writeDynamic()
                .to(bucket)
                .by(new DestinationFn(name, object, inputSchema, outputTemplateArgs))
                .withDestinationCoder(StringUtf8Coder.of())
                .withNaming(key -> getFileNaming(key, suffix, numShards));

        return applyWriteOptions(write, parameters, errorHandler);
    }

    private static <DestinationT> FileIO.Write<DestinationT, MElement> applyWriteOptions(
            FileIO.Write<DestinationT, MElement> write,
            final Parameters parameters,
            final MErrorHandler errorHandler) {

        if(parameters.numShards == null || parameters.numShards < 1) {
            write = write.withAutoSharding();
        } else {
            write = write.withNumShards(parameters.numShards);
        }

        if(parameters.tempDirectory != null) {
            write = write.withTempDirectory(parameters.tempDirectory);
        }
        if(parameters.compression != null) {
            write = write.withCompression(parameters.compression);
        }
        if(parameters.noSpilling) {
            write = write.withNoSpilling();
        }
        if(parameters.maxNumWritersPerBundle != null) {
            write = write.withMaxNumWritersPerBundle(parameters.maxNumWritersPerBundle);
        }

        return errorHandler.apply(write);
    }

    // Destinations are evaluated inside FileIO's dynamic write instead of being pre-computed
    // into a KV key, so the destination string is not duplicated into every shuffled element.
    private static class DestinationFn implements SerializableFunction<MElement, String> {

        private final String name;
        private final String path;
        private final Schema inputSchema;
        private final List<String> templateArgs;

        private transient volatile Template pathTemplate;

        DestinationFn(
                final String name,
                final String path,
                final Schema inputSchema,
                final List<String> templateArgs) {

            this.name = name;
            this.path = path;
            this.inputSchema = inputSchema;
            this.templateArgs = templateArgs;
        }

        @Override
        public String apply(final MElement element) {
            Template template = pathTemplate;
            if(template == null) {
                synchronized (this) {
                    if(pathTemplate == null) {
                        pathTemplate = TemplateUtil.createSafeTemplate("pathTemplate" + name, path);
                    }
                    template = pathTemplate;
                }
            }
            final Map<String, Object> values = element.asStandardMap(inputSchema, templateArgs);
            TemplateUtil.setFunctions(values);
            values.put("__timestamp", Instant.ofEpochMilli(element.getEpochMillis()));
            return TemplateUtil.executeStrictTemplate(template, values);
        }

    }

    private static class OutputDoFn extends DoFn<KV, MElement> {

        private static final Schema schema = Schema.builder()
                .withField(Schema.Field.of("sink", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("path", Schema.FieldType.STRING.withNullable(false)))
                .withField(Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP.withNullable(false)))
                .build();

        private final String name;

        OutputDoFn(String name) {
            this.name = name;
        }


        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement row = MElement.builder()
                    .withString("sink", this.name)
                    .withString("path", c.element().getValue().toString())
                    .withTimestamp("timestamp", c.timestamp())
                    .build();
            c.output(row);
        }

    }

    private static FileIO.Write.FileNaming getFileNaming(
            final String key,
            final String suffix,
            final Integer numShards) {

        if(TemplateUtil.isTemplateText(suffix)) {
            return new TemplateFileNaming(key, suffix);
        } else if(numShards != null && numShards == 1) {
            return new SingleFileNaming(key, suffix);
        }

        return FileIO.Write.defaultNaming(key, suffix);
    }


    private static class TemplateFileNaming implements FileIO.Write.FileNaming {

        private final String path;
        private final String suffix;
        private transient volatile Template suffixTemplate;

        private TemplateFileNaming(final String path, final String suffix) {
            this.path = path;
            this.suffix = suffix;
        }

        public String getFilename(final BoundedWindow window,
                                  final PaneInfo pane,
                                  final int numShards,
                                  final int shardIndex,
                                  final Compression compression) {

            Template template = suffixTemplate;
            if(template == null) {
                synchronized (this) {
                    if(suffixTemplate == null) {
                        suffixTemplate = TemplateUtil.createStrictTemplate("TemplateFileNaming", suffix);
                    }
                    template = suffixTemplate;
                }
            }

            final Map<String,Object> values = new HashMap<>();
            if (window != GlobalWindow.INSTANCE) {
                final IntervalWindow iw = (IntervalWindow)window;
                final Instant start = Instant.ofEpochMilli(iw.start().getMillis());
                final Instant end   = Instant.ofEpochMilli(iw.end().getMillis());
                values.put("windowStart", start);
                values.put("windowEnd",   end);
            }

            values.put("paneIndex", pane.getIndex());
            values.put("paneIsFirst", pane.isFirst());
            values.put("paneIsLast", pane.isLast());
            values.put("paneTiming", pane.getTiming().name());
            values.put("paneIsOnlyFiring", pane.isFirst() && pane.isLast());
            values.put("numShards", numShards);
            values.put("shardIndex", shardIndex);
            values.put("suggestedSuffix", compression.getSuggestedSuffix());

            TemplateUtil.setFunctions(values);

            final String filename = TemplateUtil.executeStrictTemplate(template, values);
            final String fullPath = this.path + filename;
            LOG.debug("templateFilename: {}", fullPath);
            return fullPath;
        }

    }

    private static class SingleFileNaming implements FileIO.Write.FileNaming {

        private final String path;
        private final String suffix;

        private SingleFileNaming(final String path, final String suffix) {
            this.path = path;
            this.suffix = suffix;
        }

        public String getFilename(final BoundedWindow window,
                                  final PaneInfo pane,
                                  final int numShards,
                                  final int shardIndex,
                                  final Compression compression) {

            final String fullPath = this.path + this.suffix;
            return fullPath;
        }

    }

    private static String getBucket(String output) {
        final String prefix = getPrefix(output);
        final String[] paths = output.replaceFirst(prefix, "").split("/", -1);
        return prefix + paths[0] + "/";
    }

    private static String getObject(String output) {
        final String prefix = getPrefix(output);
        final String[] paths = output.replaceFirst(prefix, "").split("/", 2);
        return paths[1];
    }

    private static String getPrefix(final String output) {
        if(output.startsWith("gs://")) {
            return "gs://";
        } else if(output.startsWith("s3://")) {
            return "s3://";
        } else {
            return "";
        }
    }

}
