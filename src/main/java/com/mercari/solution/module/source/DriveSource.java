package com.mercari.solution.module.source;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.Sheets;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.cloud.google.workspace.DriveUtil;
import com.mercari.solution.util.cloud.google.workspace.DocsUtil;
import com.mercari.solution.util.cloud.google.workspace.FormsUtil;
import com.mercari.solution.util.cloud.google.workspace.SheetsUtil;
import com.mercari.solution.util.cloud.google.workspace.SlidesUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.pipeline.Unnest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Source.Module(name="drive")
public class DriveSource extends Source {

    private static final String CLIENT_NAME = "DriveSource";

    private static final Logger LOG = LoggerFactory.getLogger(DriveSource.class);

    private static class Parameters {

        private List<FileParameters> files;
        private List<QueryParameters> queries;

        private String user;
        private String fields;

        private String export;
        private Boolean content;

        private JsonElement filter;
        private String flatten;

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if((queries == null || queries.isEmpty()) && (files == null || files.isEmpty())) {
                errorMessages.add("parameters.queries or files must not be empty");
            }

            if(queries != null) {
                for(final QueryParameters queryParameters : queries) {
                    errorMessages.addAll(queryParameters.validate());
                }
            }
            if(files != null) {
                for(final FileParameters fileParameters : files) {
                    errorMessages.addAll(fileParameters.validate());
                }
            }

            if(fields != null) {
                if(fields.equals("*")) {
                    errorMessages.add("parameters.fields does not support '*' value");
                } else if(!fields.contains("files(")) {
                    errorMessages.add("parameters.fields must be format such as 'files(id,kind,...)'.");
                }
            }

            if(flatten != null) {
                if(!"spreadsheet.sheets".equals(flatten)) {
                    errorMessages.add("parameters.flatten: " + flatten + " must be array type");
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults(final PipelineOptions options) {
            if(user == null) {
                user = DataflowOptions.getServiceAccount(options);
            }
            if(queries == null) {
                queries = new ArrayList<>();
            }
            queries.forEach(QueryParameters::setDefaults);

            if(files == null) {
                files = new ArrayList<>();
            }
            files.forEach(FileParameters::setDefaults);

            if(fields == null) {
                fields = DriveUtil.DEFAULT_FIELDS;
            } else if(!fields.contains("nextPageToken")) {
                fields = fields + ",nextPageToken";
            }

            if(content == null) {
                content = false;
            }
        }

        public List<String> getQueries() {
            final List<String> jsons = new ArrayList<>();
            for(final QueryParameters parameters : queries) {
                jsons.add(parameters.toJsonString());
            }
            return jsons;
        }

        public List<String> getFiles() {
            final List<String> jsons = new ArrayList<>();
            for(final FileParameters parameters : files) {
                jsons.add(parameters.toJsonString());
            }
            return jsons;
        }
    }

    private static class FileParameters implements Serializable {

        private String id;
        private List<String> ranges;

        public String getId() {
            return id;
        }

        private List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(id == null) {
                errorMessages.add("parameters.files[].id is required");
            }
            return errorMessages;
        }

        private FileParameters setDefaults() {
            if(ranges == null) {
                ranges = new ArrayList<>();
            }
            return this;
        }

        private String toJsonString() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id", id);
            final JsonArray jsonArray = new JsonArray();
            for(String range : ranges) {
                jsonArray.add(range);
            }
            jsonObject.add("ranges", jsonArray);
            return jsonObject.toString();
        }

    }

    private static class QueryParameters implements Serializable {

        private String query;
        private String driveId;
        private String folderId;
        private Boolean recursive;

        private List<String> validate() {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(query == null) {
                errorMessages.add("parameters.query must not be null");
            }
            return errorMessages;
        }

        private void setDefaults() {
            if(recursive == null) {
                recursive = true;
            }
        }

        private String toJsonString() {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("query", query);
            jsonObject.addProperty("driveId", driveId);
            jsonObject.addProperty("folderId", folderId);
            jsonObject.addProperty("recursive", recursive);
            return jsonObject.toString();
        }
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults(begin.getPipeline().getOptions());

        final Schema fileSchema = DriveUtil.createFileSchema2(parameters.fields);

        final TupleTag<KV<String, KV<String, MElement>>> filesOutputTag = new TupleTag<>() {};
        final TupleTag<KV<String, KV<String, MElement>>> queriesOutputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> filesFailuresTag = new TupleTag<>() {};
        final TupleTag<BadRecord> queriesFailuresTag = new TupleTag<>() {};

        final PCollectionTuple filesResult = begin
                .apply("Files", Create
                        .of(parameters.getFiles())
                        .withCoder(StringUtf8Coder.of()))
                .apply("WithKey", WithKeys.of(""))
                .apply("GroupIntoBatches", GroupIntoBatches.ofSize(100L))
                .apply("ReadFile", ParDo
                        .of(new FileDoFn(parameters, fileSchema, getLoggings(), getFailFast(), filesFailuresTag))
                        .withOutputTags(filesOutputTag, TupleTagList.of(filesFailuresTag)));
        final PCollectionTuple queryResult = begin
                .apply("Queries", Create
                        .of(parameters.getQueries())
                        .withCoder(StringUtf8Coder.of()))
                .apply("ExecuteQuery", ParDo
                        .of(new QueryDoFn(parameters, fileSchema, getFailFast(), filesFailuresTag))
                        .withOutputTags(queriesOutputTag, TupleTagList.of(queriesFailuresTag)));

        errorHandler.addError(filesResult.get(filesFailuresTag));
        errorHandler.addError(queryResult.get(queriesFailuresTag));

        final Coder<KV<String,KV<String,MElement>>> filesCoder = KvCoder
                .of(StringUtf8Coder.of(), KvCoder.of(
                        StringUtf8Coder.of(), ElementCoder.of(fileSchema)));

        final PCollection<KV<String, KV<String, MElement>>> files = PCollectionList
                .of(filesResult.get(filesOutputTag).setCoder(filesCoder))
                .and(queryResult.get(queriesOutputTag).setCoder(filesCoder))
                .apply(Flatten.pCollections());

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};

        final Schema outputSchema = createOutputSchema(parameters, fileSchema);
        final PCollectionTuple outputs = files
                .apply("ReadContent", ParDo
                        .of(new ReadDoFn(parameters, outputSchema, getLoggings(), getFailFast(), filesFailuresTag))
                        .withOutputTags(outputTag, TupleTagList.of(failuresTag)));

        errorHandler.addError(outputs.get(failuresTag));

        return MCollectionTuple.of(outputs.get(outputTag), outputSchema);
    }

    private static Schema createOutputSchema(final Parameters parameters, final Schema fileSchema) {
        final Schema outputSchema = Schema.builder(fileSchema)
                .withType(DataType.AVRO)
                .withField("export", Schema.FieldType.element(Schema.builder()
                        .withField("mimeType", Schema.FieldType.STRING)
                        .withField("body", Schema.FieldType.BYTES)
                        .build()))
                .withField("spreadsheet", Schema.FieldType.element(SheetsUtil.createSchema()))
                .withField("document", Schema.FieldType.element(DocsUtil.createSchema()))
                .withField("presentation", Schema.FieldType.element(SlidesUtil.createSchema()))
                .withField("form", Schema.FieldType.element(FormsUtil.createSchema()))
                .build();
        if(parameters.flatten != null) {
            return Unnest
                    .createSchema(outputSchema.getFields(), parameters.flatten)
                    .withType(DataType.AVRO);
        } else {
            return outputSchema;
        }
    }

    private static class FileDoFn extends DoFn<KV<String, Iterable<String>>, KV<String, KV<String, MElement>>> {

        private final String user;
        private final String fields;

        private final Schema fileSchema;

        private final Map<String, Logging> logging;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        FileDoFn(final Parameters parameters,
                 final Schema fileSchema,
                 final List<Logging> logging,
                 final boolean failFast,
                 final TupleTag<BadRecord> failuresTag) {

            this.user = parameters.user;
            this.fields = DriveUtil.extractFields(parameters.fields);

            this.fileSchema = fileSchema;

            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            getOrCreateDrive();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final KV<String, Iterable<String>> element = c.element();
            if(element == null || element.getValue() == null) {
                return;
            }

            try {
                final List<FileParameters> elementValues = Lists
                        .newArrayList(element.getValue())
                        .stream()
                        .map(s -> JsonUtil.fromJson(s, FileParameters.class))
                        .map(FileParameters::setDefaults)
                        .toList();
                final Map<String, FileParameters> fileIdAndParams = elementValues.stream()
                        .collect(Collectors.toMap(FileParameters::getId, p -> p));
                final List<File> files = DriveUtil.get(getOrCreateDrive(), fileIdAndParams.keySet(), fields);
                for(final File file : files) {
                    final Map<String, Object> values = DriveUtil.convertPrimitives(fileSchema, file);
                    final MElement output = MElement.of(values, c.timestamp());
                    final FileParameters parameters = fileIdAndParams.get(file.getId());
                    c.output(KV.of(file.getMimeType(), KV.of(parameters.toJsonString(), output)));
                }
            } catch (final Throwable e) {
                final Map<String, Object> map = new HashMap<>();
                map.put("file", element);
                final BadRecord badRecord = processError("Failed to get drive file", map, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        private Drive getOrCreateDrive() {
            return DriveUtil.getOrCreateDrive(CLIENT_NAME, user);
        }

    }

    private static class QueryDoFn extends DoFn<String, KV<String, KV<String, MElement>>> {

        private final String user;
        private final String fields;
        private final Schema fileSchema;

        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        QueryDoFn(
                final Parameters parameters,
                final Schema fileSchema,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.user = parameters.user;
            this.fields = parameters.fields;
            this.fileSchema = fileSchema;

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        public void setup() {
            this.fileSchema.setup();
            getOrCreateDrive();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final String element = c.element();
            if(element == null) {
                return;
            }

            try {
                final QueryParameters parameters = JsonUtil.fromJson(element, QueryParameters.class);
                parameters.setDefaults();

                final List<String> path = new ArrayList<>();
                final Drive drive = getOrCreateDrive();
                final List<File> files = DriveUtil.query(
                        drive, parameters.query, parameters.driveId, parameters.folderId, path, fields, parameters.recursive);

                for (final File file : files) {
                    final Map<String, Object> values = DriveUtil.convertPrimitives(fileSchema, file);
                    final MElement output = MElement.of(values, c.timestamp());
                    c.output(KV.of(file.getMimeType(), KV.of("", output)));
                }
            } catch (Throwable e) {
                final Map<String, Object> map = new HashMap<>();
                map.put("query", element);
                final BadRecord badRecord = processError("Failed to query drive file", map, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        private Drive getOrCreateDrive() {
            return DriveUtil.getOrCreateDrive(CLIENT_NAME, user);
        }

    }

    private static class ReadDoFn extends DoFn<KV<String, KV<String, MElement>>, MElement> {

        private final String user;
        private final String export;
        private final boolean content;

        private final Schema outputSchema;

        private final Filter filter;
        private final Unnest unnest;

        private final Map<String, Logging> logging;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        ReadDoFn(
                final Parameters parameters,
                final Schema outputSchema,
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.user = parameters.user;
            this.export = parameters.export;
            this.content = parameters.content;
            this.outputSchema = outputSchema;

            this.filter = Filter.of(parameters.filter);
            this.unnest = Unnest.of(parameters.flatten);

            this.logging = Logging.map(logging);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            getOrCreateSheets();
            filter.setup();
            unnest.setup();
            this.outputSchema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final KV<String, KV<String, MElement>> element = c.element();
            if(element == null) {
                return;
            }
            final KV<String, MElement> elementValue = element.getValue();
            if(elementValue == null) {
                return;
            }
            final MElement input = elementValue.getValue();
            if(input == null) {
                return;
            }

            if(!filter.filter(input)) {
                Logging.log(LOG, logging, "not_matched", input);
                return;
            }

            if(!content && export == null) {
                output(c, input);
                return;
            }

            try {
                final String mimeType = Optional.ofNullable(element.getKey()).orElse("");
                final String contentCondition = elementValue.getKey();
                final FileParameters parameters;
                if (contentCondition != null && !contentCondition.isEmpty()) {
                    parameters = JsonUtil.fromJson(contentCondition, FileParameters.class);
                    parameters.setDefaults();
                } else {
                    parameters = null;
                }

                final MElement file = elementValue.getValue();
                final String fileId = file.getAsString("id");
                final Map<String, Object> fileValues = file.asPrimitiveMap();

                final KV<String, Map<String, Object>> contentValues = switch (mimeType) {
                    case DriveUtil.MIMETYPE_APPS_SHEETS -> {
                        final Map<String, Object> spreadsheetValues;
                        if(content) {
                            final List<String> ranges = Optional.ofNullable(parameters).map(fp -> fp.ranges).orElseGet(ArrayList::new);
                            spreadsheetValues = SheetsUtil.get(getOrCreateSheets(), fileId, ranges);
                        } else {
                            spreadsheetValues = new HashMap<>();
                        }
                        yield KV.of("spreadsheet", spreadsheetValues);
                    }
                    default -> null;
                };

                if(contentValues != null) {
                    fileValues.put(contentValues.getKey(), contentValues.getValue());
                }

                if(export != null) {
                    final byte[] bytes = DriveUtil.export(getOrCreateDrive(), fileId, export);
                    final Map<String, Object> exportValues = new HashMap<>();
                    exportValues.put("mimeType", export);
                    exportValues.put("body", ByteBuffer.wrap(bytes));
                    fileValues.put("export", exportValues);
                }

                final MElement output = MElement.of(fileValues, c.timestamp());
                output(c, output);
            } catch (Throwable e) {
                final BadRecord badRecord = processError("Failed to read file content", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

        private void output(ProcessContext c, MElement element) {
            if(unnest.useUnnest()) {
                final List<Map<String, Object>> list = unnest.unnest(element);
                for(final MElement output : MElement.ofList(list, c.timestamp())) {
                    c.output(output.convert(outputSchema));
                    Logging.log(LOG, logging, "output", output);
                }
            } else {
                c.output(element.convert(outputSchema));
                Logging.log(LOG, logging, "output", element);
            }
        }

        private Drive getOrCreateDrive() {
            return DriveUtil.getOrCreateDrive(CLIENT_NAME, user);
        }

        private Sheets getOrCreateSheets() {
            return SheetsUtil.getOrCreateSheets(CLIENT_NAME, user);
        }

    }
}
