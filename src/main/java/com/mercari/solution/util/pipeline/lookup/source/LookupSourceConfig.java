package com.mercari.solution.util.pipeline.lookup.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.LookupSource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The shared {@code sources} configuration surface for modules that expose
 * external lookup tables to SQL ({@code query}, {@code beamsql}): Gson-mapped
 * parameter DTOs plus the factory turning them into {@link LookupSource}s.
 * See the query module docs for the per-type parameters.
 */
public final class LookupSourceConfig {

    private LookupSourceConfig() {
    }

    /** One external lookup source (its tables are referenced as {@code name.table} in SQL). */
    public static class SourceParameters implements Serializable {

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

        // grpc
        private String target;
        private Boolean plaintext;
        private String descriptorSetPath;
        private Integer maxInboundMessageBytes;

        // buffer (state-backed history of the transform's own input)
        private List<String> groupFields;
        private Integer maxCount;
        private Long maxDurationSeconds;
        private Boolean includeCurrent;
        private Long stateTtlSeconds;
        private JsonElement bufferFilter;
        private JsonElement triggerFilter;

        private CacheParameters cache;

        private List<TableParameters> tables;

        public void validate(int index) {
            if(cache != null) {
                cache.validate(index);
            }
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
                case "sideinput" -> {
                    for(int t = 0; t < tables.size(); t++) {
                        final TableParameters table = tables.get(t);
                        if(table.name == null && table.input == null) {
                            throw new IllegalModuleException(
                                    "parameters.sources[" + index + "].tables[" + t
                                            + "] (sideinput) requires name or input");
                        }
                        if(table.keyFields == null || table.keyFields.isEmpty()) {
                            throw new IllegalModuleException(
                                    "parameters.sources[" + index + "].tables[" + t
                                            + "] (sideinput) requires keyFields");
                        }
                    }
                }
                case "grpc" -> {
                    if(target == null || descriptorSetPath == null) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "] (grpc) requires target and descriptorSetPath");
                    }
                    for(int t = 0; t < tables.size(); t++) {
                        final TableParameters table = tables.get(t);
                        if(table.method == null || table.keyField == null) {
                            throw new IllegalModuleException(
                                    "parameters.sources[" + index + "].tables[" + t
                                            + "] (grpc) requires method and keyField");
                        }
                    }
                }
                case "buffer" -> {
                    if(groupFields == null || groupFields.isEmpty()) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "] (buffer) requires groupFields");
                    }
                    if(tables.size() != 1) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "] (buffer) requires exactly one table");
                    }
                    if(tables.get(0).name == null) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "].tables[0] (buffer) requires name");
                    }
                    if(cache != null) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "] (buffer) does not support cache"
                                        + " (the buffer changes with every element)");
                    }
                    if(maxCount != null && maxCount <= 0) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "].maxCount must be positive");
                    }
                    if(maxDurationSeconds != null && maxDurationSeconds <= 0) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "].maxDurationSeconds must be positive");
                    }
                    if(stateTtlSeconds != null && stateTtlSeconds <= 0) {
                        throw new IllegalModuleException(
                                "parameters.sources[" + index + "].stateTtlSeconds must be positive");
                    }
                }
                default -> throw new IllegalModuleException(
                        "parameters.sources[" + index + "].type must be one of jdbc, spanner, bigtable, rest, grpc, sideinput, buffer but was: " + type);
            }
        }
    }

    /**
     * On-memory cache over the source's key-driven lookups. Presence of the
     * block enables the cache (set {@code enabled: false} to keep it off);
     * point / prefix-equality lookups are then served from a bounded per-worker
     * cache — range lookups always go to the backend. Cached rows are stale up
     * to {@code expireAfterSeconds}, so enable it only for read-mostly tables.
     */
    public static class CacheParameters implements Serializable {

        private Boolean enabled;
        private Long maxSize;
        private Long expireAfterSeconds;
        private Boolean cacheEmptyResults;

        private void validate(int index) {
            if(maxSize != null && maxSize <= 0) {
                throw new IllegalModuleException(
                        "parameters.sources[" + index + "].cache.maxSize must be positive");
            }
            if(expireAfterSeconds != null && expireAfterSeconds < 0) {
                throw new IllegalModuleException(
                        "parameters.sources[" + index + "].cache.expireAfterSeconds must not be negative");
            }
        }

        private boolean isEnabled() {
            return !Boolean.FALSE.equals(enabled);
        }

        private LookupSource.CacheSpec toSpec() {
            return new LookupSource.CacheSpec(
                    maxSize == null ? LookupSource.CacheSpec.DEFAULT_MAX_SIZE : maxSize,
                    expireAfterSeconds == null ? 0L : expireAfterSeconds,
                    cacheEmptyResults == null || cacheEmptyResults);
        }
    }

    public static class TableParameters implements Serializable {

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

        // grpc (method = "package.Service/Method"; keyField / rowsFrom / fields shared)
        private Boolean serverStreaming;
        private String requestKeyField;
        private String requestTemplate;

        // sideinput (keyFields shared with rest)
        private String input;
    }

    public static class ColumnParameters implements Serializable {

        private String name;
        private String family;
        private String qualifier;
        private String type;
    }

    public static void validate(final List<SourceParameters> sources) {
        if(sources == null) {
            return;
        }
        for(int i = 0; i < sources.size(); i++) {
            sources.get(i).validate(i);
        }
    }

    /**
     * The distinct side input collection names referenced by {@code sideinput}
     * sources — what the hosting module must resolve and pass to
     * {@link #createSources(List, Map)}.
     */
    public static List<String> sideInputNames(final List<SourceParameters> sources) {
        final List<String> names = new ArrayList<>();
        if(sources == null) {
            return names;
        }
        for(final SourceParameters source : sources) {
            if(!"sideinput".equals(source.type)) {
                continue;
            }
            for(final TableParameters table : source.tables) {
                final String input = table.input != null ? table.input : table.name;
                if(!names.contains(input)) {
                    names.add(input);
                }
            }
        }
        return names;
    }

    /**
     * The {@code buffer} source's parameters in one serializable value, for the
     * hosting transform (which owns the stateful DoFn the buffer lives in).
     * {@code fields} is the explicit stored-fields list ({@code null} = derive
     * from the planned SQL); the filter JSONs are the filter DSL conditions as
     * text ({@code null} = match everything).
     */
    public record BufferConfig(
            String sourceName,
            String tableName,
            List<String> groupFields,
            List<String> fields,
            Integer maxCount,
            Long maxDurationSeconds,
            boolean includeCurrent,
            Long stateTtlSeconds,
            String bufferFilterJson,
            String triggerFilterJson) implements Serializable {
    }

    /**
     * The single {@code buffer} source's parameters, or {@code null} when none
     * is configured. At most one buffer source is allowed per module (Beam
     * state is keyed per {@code DoFn}, so one transform has one buffer key).
     */
    public static BufferConfig bufferConfig(final List<SourceParameters> sources) {
        if(sources == null) {
            return null;
        }
        BufferConfig config = null;
        for(final SourceParameters source : sources) {
            if(!"buffer".equals(source.type)) {
                continue;
            }
            if(config != null) {
                throw new IllegalModuleException(
                        "parameters.sources may contain at most one buffer source"
                                + " (Beam state allows one buffer key per transform)");
            }
            config = new BufferConfig(
                    source.name,
                    source.tables.get(0).name,
                    List.copyOf(source.groupFields),
                    parseFieldNames(source.tables.get(0).fields),
                    source.maxCount,
                    source.maxDurationSeconds,
                    !Boolean.FALSE.equals(source.includeCurrent),
                    source.stateTtlSeconds != null ? source.stateTtlSeconds : source.maxDurationSeconds,
                    source.bufferFilter == null || source.bufferFilter.isJsonNull()
                            ? null : source.bufferFilter.toString(),
                    source.triggerFilter == null || source.triggerFilter.isJsonNull()
                            ? null : source.triggerFilter.toString());
        }
        return config;
    }

    /** A plain string array ({@code ["field1", ...]}), or {@code null}. */
    private static List<String> parseFieldNames(final JsonElement fields) {
        if(fields == null || fields.isJsonNull()) {
            return null;
        }
        if(!fields.isJsonArray()) {
            throw new IllegalModuleException(
                    "buffer table fields must be an array of field names but was: " + fields);
        }
        final List<String> names = new ArrayList<>();
        for(final JsonElement field : fields.getAsJsonArray()) {
            if(!field.isJsonPrimitive()) {
                throw new IllegalModuleException(
                        "buffer table fields must be an array of field names but was: " + fields);
            }
            names.add(field.getAsString());
        }
        return names;
    }

    public static List<LookupSource> createSources(final List<SourceParameters> sources) {
        return createSources(sources, null);
    }

    public static List<LookupSource> createSources(
            final List<SourceParameters> sources,
            final Map<String, Schema> sideInputSchemas) {
        return createSources(sources, sideInputSchemas, null);
    }

    /**
     * @param sideInputSchemas side input name → schema, for {@code sideinput}
     *        sources ({@code null} when the module does not support them)
     * @param bufferInputSchema the module's (union) input schema, for {@code buffer}
     *        sources ({@code null} when the module does not support them)
     */
    public static List<LookupSource> createSources(
            final List<SourceParameters> sources,
            final Map<String, Schema> sideInputSchemas,
            final Schema bufferInputSchema) {

        final List<LookupSource> lookupSources = new ArrayList<>();
        if(sources == null) {
            return lookupSources;
        }
        for(final SourceParameters source : sources) {
            lookupSources.add(createSource(source, sideInputSchemas, bufferInputSchema));
        }
        return lookupSources;
    }

    private static LookupSource createSource(
            final SourceParameters source,
            final Map<String, Schema> sideInputSchemas,
            final Schema bufferInputSchema) {

        final LookupSource lookupSource = createSourceInternal(source, sideInputSchemas, bufferInputSchema);
        if(source.cache != null && source.cache.isEnabled()) {
            lookupSource.setCacheSpec(source.cache.toSpec());
        }
        return lookupSource;
    }

    private static LookupSource createSourceInternal(
            final SourceParameters source,
            final Map<String, Schema> sideInputSchemas,
            final Schema bufferInputSchema) {
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
            case "grpc" -> {
                final GrpcLookupSource.Builder builder = GrpcLookupSource.builder()
                        .withName(source.name)
                        .withTarget(source.target)
                        .withPlaintext(Boolean.TRUE.equals(source.plaintext))
                        .withDescriptorSetPath(source.descriptorSetPath);
                if(source.headers != null) {
                    for(final Map.Entry<String, String> header : source.headers.entrySet()) {
                        builder.withHeader(header.getKey(), header.getValue());
                    }
                }
                if(source.timeoutMillis != null) {
                    builder.withDeadlineMillis(source.timeoutMillis);
                }
                if(source.maxInboundMessageBytes != null) {
                    builder.withMaxInboundMessageBytes(source.maxInboundMessageBytes);
                }
                for(final TableParameters table : source.tables) {
                    final GrpcLookupSource.TableBuilder tableBuilder = GrpcLookupSource.TableConfig.builder()
                            .withName(table.name)
                            .withMethod(table.method)
                            .withKeyField(table.keyField)
                            .withServerStreaming(Boolean.TRUE.equals(table.serverStreaming));
                    if(table.requestKeyField != null) {
                        tableBuilder.withRequestKeyField(table.requestKeyField);
                    }
                    if(table.rowsFrom != null) {
                        tableBuilder.withRowsFrom(table.rowsFrom);
                    }
                    if(table.requestTemplate != null) {
                        tableBuilder.withRequestTemplate(table.requestTemplate);
                    }
                    if(table.fields != null && !table.fields.isJsonNull()) {
                        tableBuilder.withFields(parseFields(table.fields));
                    }
                    builder.withTable(tableBuilder.build());
                }
                yield builder.build();
            }
            case "buffer" -> {
                if(bufferInputSchema == null) {
                    throw new IllegalModuleException(
                            "buffer lookup sources are only supported by the query transform module");
                }
                yield BufferLookupSource.builder()
                        .withName(source.name)
                        .withTable(source.tables.get(0).name)
                        .withGroupFields(source.groupFields)
                        .withInputSchema(bufferInputSchema)
                        .build();
            }
            case "sideinput" -> {
                if(sideInputSchemas == null) {
                    throw new IllegalModuleException(
                            "sideinput lookup sources are only supported by the query transform module");
                }
                final SideInputLookupSource.Builder builder = SideInputLookupSource.builder()
                        .withName(source.name);
                for(final TableParameters table : source.tables) {
                    final String input = table.input != null ? table.input : table.name;
                    final Schema schema = sideInputSchemas.get(input);
                    if(schema == null) {
                        throw new IllegalModuleException(
                                "sideinput source '" + source.name + "' table references side input '"
                                        + input + "' which is not listed in the module's sideInputs: "
                                        + sideInputSchemas.keySet());
                    }
                    builder.withTable(SideInputLookupSource.table()
                            .withName(table.name)
                            .withInput(input)
                            .withKeyFields(table.keyFields)
                            .withSchema(schema)
                            .build());
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
}
