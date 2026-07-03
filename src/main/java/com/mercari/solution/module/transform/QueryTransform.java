package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.ParameterManagerUtil;
import com.mercari.solution.util.cloud.google.StorageUtil;
import com.mercari.solution.util.pipeline.Query2;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.pipeline.lookup.source.BigtableLookupSource;
import com.mercari.solution.util.pipeline.lookup.source.JdbcLookupSource;
import com.mercari.solution.util.pipeline.lookup.source.RestLookupSource;
import com.mercari.solution.util.pipeline.lookup.source.SpannerLookupSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Runs a Calcite SQL statement over each input element, inside a DoFn, optionally
 * joined against external lookup sources (JDBC / Spanner / Bigtable / REST).
 *
 * <p>Unlike the {@code beamsql} module (which plans SQL over whole PCollections and may
 * shuffle), this module evaluates the SQL per element as a bounded in-memory query:
 * the element is registered as a one-row table, and any array-of-struct fields can be
 * expanded with {@code UNNEST}/{@code LATERAL} so that aggregation, ORDER BY / LIMIT and
 * set operations run over the per-element collection. The evaluation never shuffles and
 * preserves the input windowing/timestamps, so batch and streaming behave identically.
 * One input element yields zero or more output rows (fan-out or fold, decided by the SQL).
 *
 * <p>With {@code sources}, external tables are referenced as {@code sourceName.tableName}
 * and joined on their key (primary key / unique index / row key / request parameter):
 * a join whose condition constrains a contiguous prefix of the key — point equality on
 * the full key, or leading-column equality plus a bounded range on the next column —
 * becomes a batched key-driven read (never a scan of the external table). INNER and
 * LEFT joins are supported; any other use of a lookup table fails with an explanatory
 * error.
 */
@Transform.Module(name="query")
public class QueryTransform extends Transform {

    private static class Parameters implements Serializable {

        private String sql;
        private String table;
        private List<SourceParameters> sources;

        private void validate() {
            if(this.sql == null) {
                throw new IllegalModuleException("parameters.sql must not be null");
            }
            if(this.sources != null) {
                for(int i=0; i<sources.size(); i++) {
                    sources.get(i).validate(i);
                }
            }
        }

        private void setDefaults(Map<String, String> templateArgs) {
            sql = loadQuery(sql, templateArgs);
            if(table == null) {
                table = "INPUT";
            }
            if(sources == null) {
                sources = new ArrayList<>();
            }
        }

        private String loadQuery(String sql, Map<String, String> templateArgs) {
            if(sql == null) {
                return null;
            }
            String query;
            if(sql.startsWith("gs://")) {
                LOG.info("sql parameter is GCS path: {}", sql);
                final String rawQuery = StorageUtil.readString(sql);
                query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            } else if(ParameterManagerUtil.isParameterVersionResource(sql)) {
                LOG.info("sql parameter is Parameter Manager resource: {}", sql);
                final ParameterManagerUtil.Version version = ParameterManagerUtil.getParameterVersion(sql);
                if(version.payload == null) {
                    throw new IllegalArgumentException("sql resource does not exists for: " + sql);
                }
                query = new String(version.payload, StandardCharsets.UTF_8);
            } else if(sql.startsWith("data:")) {
                LOG.info("sql parameter is base64 encoded");
                query = new String(Base64.getDecoder().decode(sql.substring("data:".length())), StandardCharsets.UTF_8);
            } else {
                // Query text is not a valid file path on some platforms (e.g. newlines or ':' on Windows)
                Path path;
                try {
                    path = Paths.get(sql);
                } catch (final Throwable e) {
                    path = null;
                }
                if(path != null && Files.exists(path) && !Files.isDirectory(path)) {
                    try {
                        final String rawQuery = Files.readString(path, StandardCharsets.UTF_8);
                        query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
                    } catch (IOException e) {
                        query = sql;
                    }
                } else {
                    query = sql;
                }
            }
            return query;
        }
    }

    /** One external lookup source (its tables are referenced as {@code name.table} in SQL). */
    private static class SourceParameters implements Serializable {

        private String name;
        private String type;

        // jdbc
        private String driver;
        private String url;
        private String user;
        private String password;

        // spanner / bigtable
        private String projectId;
        private String instanceId;
        private String databaseId;
        private Boolean emulator;
        private Long maxStalenessSeconds;
        private String appProfileId;

        // rest
        private String baseUrl;
        private Map<String, String> headers;
        private List<String> allowedHosts;
        private Long timeoutMillis;

        private List<TableParameters> tables;

        private void validate(int index) {
            if(name == null) {
                throw new IllegalModuleException("parameters.sources[" + index + "].name must not be null");
            }
            if(type == null) {
                throw new IllegalModuleException("parameters.sources[" + index + "].type must not be null");
            }
            if(tables == null || tables.isEmpty()) {
                throw new IllegalModuleException("parameters.sources[" + index + "].tables must not be empty");
            }
            switch (type) {
                case "jdbc" -> {
                    if(driver == null || url == null) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "] (jdbc) requires driver and url");
                    }
                }
                case "spanner" -> {
                    if(projectId == null || instanceId == null || databaseId == null) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "] (spanner) requires projectId, instanceId and databaseId");
                    }
                }
                case "bigtable" -> {
                    if(projectId == null || instanceId == null) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "] (bigtable) requires projectId and instanceId");
                    }
                }
                case "rest" -> {
                }
                default -> throw new IllegalModuleException(
                        "parameters.sources[" + index + "].type must be one of jdbc, spanner, bigtable, rest but was: " + type);
            }
        }
    }

    private static class TableParameters implements Serializable {

        private String name;
        private String table;

        // spanner query table (parameterized GoogleSQL/GQL: graph traversal, full-text search, ...)
        private String sql;
        private String keyField;
        private String paramName;
        private String bindMode;

        // bigtable
        private String tableId;
        private String rowKeyField;
        private String rowKeyType;
        private List<ColumnParameters> columns;

        // rest
        private String endpoint;
        private String method;
        private Map<String, String> headers;
        private Map<String, String> params;
        private String body;
        private List<String> keyFields;
        private String rowsFrom;
        private JsonElement fields;
    }

    private static class ColumnParameters implements Serializable {

        private String name;
        private String family;
        private String qualifier;
        private String type;
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults(getTemplateArgs());

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final Query2 query;
        try {
            query = Query2.builder()
                    .withInput(parameters.table, inputSchema)
                    .withSources(createSources(parameters.sources))
                    .withSql(parameters.sql)
                    .build();
        } catch (final Throwable e) {
            throw new IllegalModuleException(
                    "query transform module[" + getName() + "] failed to plan sql: " + parameters.sql + ", cause: " + e.getMessage());
        }

        final Schema outputSchema = query.getOutputSchema();
        validateOutputSchema(outputSchema);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Query", ParDo
                        .of(new QueryDoFn(query, parameters.table, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
    }

    private static List<LookupSource> createSources(final List<SourceParameters> sources) {
        final List<LookupSource> lookupSources = new ArrayList<>();
        for(final SourceParameters source : sources) {
            lookupSources.add(createSource(source));
        }
        return lookupSources;
    }

    private static LookupSource createSource(final SourceParameters source) {
        return switch (source.type) {
            case "jdbc" -> {
                final JdbcLookupSource.Builder builder = JdbcLookupSource.builder()
                        .withName(source.name)
                        .withDriver(source.driver)
                        .withUrl(source.url)
                        .withUser(source.user)
                        .withPassword(source.password);
                for(final TableParameters table : source.tables) {
                    builder.withTable(table.name, table.table == null ? table.name : table.table);
                }
                yield builder.build();
            }
            case "spanner" -> {
                final SpannerLookupSource.Builder builder = SpannerLookupSource.builder()
                        .withName(source.name)
                        .withProjectId(source.projectId)
                        .withInstanceId(source.instanceId)
                        .withDatabaseId(source.databaseId)
                        .withEmulator(Boolean.TRUE.equals(source.emulator))
                        .withMaxStalenessSeconds(source.maxStalenessSeconds == null ? 0L : source.maxStalenessSeconds);
                for(final TableParameters table : source.tables) {
                    if(table.sql != null) {
                        // Parameterized GoogleSQL/GQL statement as a key-driven table
                        // (Spanner Graph traversal, full-text search, arbitrary query)
                        final SpannerLookupSource.QueryTableBuilder queryBuilder =
                                SpannerLookupSource.queryTable()
                                        .withName(table.name)
                                        .withSql(table.sql)
                                        .withKeyField(table.keyField);
                        if(table.paramName != null) {
                            queryBuilder.withParamName(table.paramName);
                        }
                        if(table.bindMode != null) {
                            queryBuilder.withBindMode(SpannerLookupSource.BindMode
                                    .valueOf(table.bindMode.trim().toUpperCase()));
                        }
                        if(table.fields != null && !table.fields.isJsonNull()) {
                            queryBuilder.withFields(parseFields(table.fields));
                        }
                        builder.withQueryTable(queryBuilder.build());
                    } else {
                        builder.withTable(table.name, table.table == null ? table.name : table.table);
                    }
                }
                yield builder.build();
            }
            case "bigtable" -> {
                final BigtableLookupSource.Builder builder = BigtableLookupSource.builder()
                        .withName(source.name)
                        .withProjectId(source.projectId)
                        .withInstanceId(source.instanceId)
                        .withAppProfileId(source.appProfileId);
                for(final TableParameters table : source.tables) {
                    final BigtableLookupSource.TableBuilder tableBuilder = BigtableLookupSource.table()
                            .withName(table.name)
                            .withTableId(table.tableId == null ? table.table : table.tableId);
                    if(table.rowKeyField != null) {
                        tableBuilder.withRowKeyField(table.rowKeyField);
                    }
                    if(table.rowKeyType != null) {
                        tableBuilder.withRowKeyType(Schema.Type.of(table.rowKeyType));
                    }
                    if(table.columns != null) {
                        for(final ColumnParameters column : table.columns) {
                            tableBuilder.withColumn(column.name, column.family, column.qualifier,
                                    Schema.Type.of(column.type == null ? "string" : column.type));
                        }
                    }
                    builder.withTable(tableBuilder.build());
                }
                yield builder.build();
            }
            case "rest" -> {
                final RestLookupSource.Builder builder = RestLookupSource.builder()
                        .withName(source.name)
                        .withBaseUrl(source.baseUrl);
                if(source.headers != null) {
                    for(final Map.Entry<String, String> header : source.headers.entrySet()) {
                        builder.withDefaultHeader(header.getKey(), header.getValue());
                    }
                }
                if(source.allowedHosts != null) {
                    builder.withAllowedHosts(source.allowedHosts);
                }
                if(source.timeoutMillis != null) {
                    builder.withTimeoutMillis(source.timeoutMillis);
                }
                for(final TableParameters table : source.tables) {
                    final RestLookupSource.TableBuilder tableBuilder = RestLookupSource.TableConfig.builder()
                            .withName(table.name)
                            .withEndpoint(table.endpoint);
                    if(table.method != null) {
                        tableBuilder.withMethod(table.method);
                    }
                    if(table.headers != null) {
                        for(final Map.Entry<String, String> header : table.headers.entrySet()) {
                            tableBuilder.withHeader(header.getKey(), header.getValue());
                        }
                    }
                    if(table.params != null) {
                        for(final Map.Entry<String, String> param : table.params.entrySet()) {
                            tableBuilder.withParam(param.getKey(), param.getValue());
                        }
                    }
                    if(table.body != null) {
                        tableBuilder.withBody(table.body);
                    }
                    if(table.keyFields != null && !table.keyFields.isEmpty()) {
                        tableBuilder.withKeyFields(table.keyFields);
                    }
                    if(table.rowsFrom != null) {
                        tableBuilder.withRowsFrom(table.rowsFrom);
                    }
                    if(table.fields != null && !table.fields.isJsonNull()) {
                        tableBuilder.withFields(parseFields(table.fields));
                    }
                    builder.withTable(tableBuilder.build());
                }
                yield builder.build();
            }
            default -> throw new IllegalModuleException("unsupported lookup source type: " + source.type);
        };
    }

    /** Accepts a plain fields array (the standard schema fields JSON). */
    private static List<Schema.Field> parseFields(final JsonElement fields) {
        final JsonObject schemaJson = new JsonObject();
        if(fields.isJsonArray()) {
            schemaJson.add("fields", fields);
        } else if(fields.isJsonObject() && fields.getAsJsonObject().has("fields")) {
            schemaJson.add("fields", fields.getAsJsonObject().get("fields"));
        } else {
            final JsonArray array = new JsonArray();
            array.add(fields);
            schemaJson.add("fields", array);
        }
        return Schema.parse(schemaJson).getFields();
    }

    // Auto-generated column names such as EXPR$0 are not legal field names for
    // downstream schema conversions (e.g. Avro). Require an explicit alias instead.
    private void validateOutputSchema(final Schema outputSchema) {
        for(final Schema.Field field : outputSchema.getFields()) {
            if(field.getName().contains("$")) {
                throw new IllegalModuleException(
                        "query transform module[" + getName() + "] output column '" + field.getName()
                                + "' has an auto-generated name. Add an explicit alias (e.g. `AS my_column`) in the select list");
            }
        }
    }

    private static class QueryDoFn extends DoFn<MElement, MElement> {

        private final Query2 query;
        private final String table;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        QueryDoFn(
                final Query2 query,
                final String table,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.query = query;
            this.table = table;
            this.logs = Logging.map(logs);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            query.setup();
        }

        @Teardown
        public void teardown() {
            query.teardown();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                Logging.log(LOG, logs, "input", input);
                final List<MElement> results = query.execute(Map.of(table, List.of(input)), c.timestamp());
                for(final MElement output : results) {
                    Logging.log(LOG, logs, "output", output);
                    c.output(output);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to execute query", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

    }

}
