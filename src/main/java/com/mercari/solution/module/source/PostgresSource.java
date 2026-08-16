package com.mercari.solution.module.source;

import com.google.gson.JsonElement;
import com.mercari.solution.util.cloud.SecretProviders;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.domain.db.PostgresUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Source module for reading records from PostgreSQL (or compatible) databases
 * using {@code COPY (SELECT ...) TO STDOUT (FORMAT BINARY)} for high throughput.
 * Each table is split into physical block ({@code ctid}) ranges, and the ranges are
 * read in parallel with efficient TID range scans.
 *
 * Supports a single table ({@code table}) or every base table matching the
 * {@code tables} include/exclude patterns, with one tagged output per table.
 */
@Source.Module(name="postgres")
public class PostgresSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSource.class);

    private static class Parameters implements Serializable {

        private String url;
        private String user;
        private String password;

        private String query;
        private String table;

        private String select;
        private String where;
        private Long splitSize;

        // for all-tables parameters
        private JsonElement tables;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            } else if(!url.startsWith("jdbc:postgresql:")) {
                errorMessages.add("parameters.url must be jdbc:postgresql: url");
            }
            if(table == null && tables == null) {
                errorMessages.add("parameters.table or parameters.tables must not be null");
            }
            if(tables != null) {
                if(table != null) {
                    errorMessages.add("parameters.table must not be set together with parameters.tables");
                }
                if(select != null || where != null) {
                    errorMessages.add("parameters.select and parameters.where must not be set together with parameters.tables (use tables.select / tables.where)");
                }
                try {
                    TablesParameter.of(tables);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(e.getMessage());
                }
            }
            if(user != null && password == null) {
                errorMessages.add("parameters.password must not be null");
            }
            if(splitSize != null && splitSize < 1) {
                errorMessages.add("parameters.splitSize must be positive");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(select == null) {
                select = "*";
            }
            if(splitSize == null) {
                splitSize = 1_000_000L;
            }
        }

        public void replaceParameters(final Pipeline pipeline) {
            if(user == null) {
                final String serviceAccount = DataflowOptions.getServiceAccount(pipeline.getOptions());
                LOG.info("Using worker service account: '{}' for database user", serviceAccount);
                user = serviceAccount.replace(".gserviceaccount.com", "");
                password = "dummy";
                if(!url.contains("enableIamAuth")) {
                    url = url + "&enableIamAuth=true";
                }
            } else if(SecretProviders.isSecretReference(user) || SecretProviders.isSecretReference(password)) {
                LOG.info("parameters.user|password is secret resource.");
                user = SecretProviders.resolveIfSecret(user);
                password = SecretProviders.resolveIfSecret(password);
            }
        }

    }

    /**
     * The {@code tables} parameter. Accepts either a pattern list shorthand
     * {@code tables: ["users", "item_*"]} or the full form
     * {@code tables: {includes: [...], excludes: [...], select: "...", where: "..."}}.
     * Missing includes defaults to all public-schema tables; {@code *} matches any sequence.
     */
    static class TablesParameter implements Serializable {

        final List<String> includes;
        final List<String> excludes;
        // common per-table SELECT clause template (default "*") and WHERE clause template;
        // ${table} (output tag), ${schema} and ${name} are available as template variables
        final String select;
        final String where;

        private TablesParameter(
                final List<String> includes,
                final List<String> excludes,
                final String select,
                final String where) {

            this.includes = includes;
            this.excludes = excludes;
            this.select = select;
            this.where = where;
        }

        static TablesParameter of(final JsonElement json) {
            final List<String> includes;
            final List<String> excludes;
            final String select;
            final String where;
            if(json.isJsonArray()) {
                includes = toStringList(json);
                excludes = new ArrayList<>();
                select = null;
                where = null;
            } else if(json.isJsonObject()) {
                includes = json.getAsJsonObject().has("includes")
                        ? toStringList(json.getAsJsonObject().get("includes"))
                        : new ArrayList<>();
                excludes = json.getAsJsonObject().has("excludes")
                        ? toStringList(json.getAsJsonObject().get("excludes"))
                        : new ArrayList<>();
                select = getStringMember(json, "select");
                where = getStringMember(json, "where");
            } else {
                throw new IllegalArgumentException(
                        "'tables' must be a pattern array or an object with 'includes', 'excludes', 'select' and 'where': " + json);
            }
            if(includes.isEmpty()) {
                includes.add("*");
            }
            return new TablesParameter(includes, excludes, select, where);
        }

        private static String getStringMember(final JsonElement json, final String member) {
            if(!json.getAsJsonObject().has(member)) {
                return null;
            }
            final JsonElement element = json.getAsJsonObject().get(member);
            if(!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
                throw new IllegalArgumentException("'tables." + member + "' must be a string: " + json);
            }
            return element.getAsString();
        }

        private static List<String> toStringList(final JsonElement json) {
            if(!json.isJsonArray()) {
                throw new IllegalArgumentException("'tables' patterns must be a string array: " + json);
            }
            final List<String> list = new ArrayList<>();
            for(final JsonElement element : json.getAsJsonArray()) {
                if(!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
                    throw new IllegalArgumentException("'tables' patterns must be a string array: " + json);
                }
                list.add(element.getAsString());
            }
            return list;
        }

        /**
         * A pattern without a dot matches the table name only in the {@code public} schema;
         * a pattern with a dot matches the schema-qualified {@code schema.name}.
         */
        boolean matches(final String schema, final String name) {
            return includes.stream().anyMatch(p -> matchesPattern(p, schema, name))
                    && excludes.stream().noneMatch(p -> matchesPattern(p, schema, name));
        }

        private static boolean matchesPattern(final String pattern, final String schema, final String name) {
            if(pattern.contains(".")) {
                return matchesGlob(pattern, schema + "." + name);
            }
            return "public".equals(schema) && matchesGlob(pattern, name);
        }

        private static boolean matchesGlob(final String pattern, final String value) {
            final String[] literals = pattern.split("\\*", -1);
            final StringBuilder regex = new StringBuilder();
            for(int i = 0; i < literals.length; i++) {
                if(i > 0) {
                    regex.append(".*");
                }
                regex.append(java.util.regex.Pattern.quote(literals[i]));
            }
            return value.matches(regex.toString());
        }

    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();
        parameters.replaceParameters(begin.getPipeline());

        if(parameters.tables != null) {
            return expandAllTables(begin, parameters);
        }

        final org.apache.avro.Schema outputAvroSchema;
        final List<PostgresUtil.Column> columns;
        final List<PostgresUtil.Range> blockRanges;
        try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                .createDataSource(PostgresUtil.DRIVER, parameters.url, parameters.user, parameters.password, true)) {

            try(final Connection connection = dataSource.getConnection()) {
                final String query = PostgresUtil.createQuery(
                        parameters.table, parameters.select, parameters.where, null);
                try(final PreparedStatement statement = connection
                        .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    final ResultSetMetaData meta = statement.getMetaData();
                    if(meta == null) {
                        throw new IllegalModuleException("Failed to get schema for query: " + query);
                    }
                    outputAvroSchema = ResultSetToRecordConverter.convertSchema(meta);
                    columns = PostgresUtil.getColumns(connection, meta);
                }

                final long blockCount = PostgresUtil.getBlockCount(connection, parameters.table);
                final double estimatedRows = PostgresUtil.getEstimatedRowCount(connection, parameters.table);
                blockRanges = PostgresUtil.createBlockRanges(blockCount, estimatedRows, parameters.splitSize);
                connection.commit();
                LOG.info("{} table: {} blockCount: {}, estimatedRows: {}, split into {} ctid ranges",
                        getName(), parameters.table, blockCount, estimatedRows, blockRanges.size());
            }
        } catch (final IOException | SQLException e) {
            throw new IllegalModuleException("Failed to connect database. url: " + parameters.url, e);
        }
        LOG.info("{} outputSchema: {}", getName(), outputAvroSchema);

        final PCollection<MElement> elements = applyRead(
                begin, "", parameters,
                parameters.table, parameters.select, parameters.where,
                columns, outputAvroSchema, blockRanges);

        return MCollectionTuple
                .of(elements, Schema.of(outputAvroSchema));
    }

    // Assembly-time resolved read of one matched table (only used while building the graph).
    private record TableRead(
            String tag,
            String select,
            String where,
            PostgresUtil.TableId tableId,
            List<PostgresUtil.Column> columns,
            org.apache.avro.Schema avroSchema,
            List<PostgresUtil.Range> ranges) { }

    /**
     * The all-tables ({@code tables}) mode: enumerates the matching base tables at assembly
     * time, resolves each table's schema and ctid ranges, and appends one read branch per
     * table as a tagged output with assembly-time attributes (table/schema/name).
     *
     * Unlike the spanner tables mode there is no shared snapshot: each ctid range is read
     * on its own connection, so consistency across ranges and tables is not guaranteed.
     */
    private MCollectionTuple expandAllTables(final PBegin begin, final Parameters parameters) {

        final TablesParameter tablesParameter = TablesParameter.of(parameters.tables);
        // JsonElement is not java-serializable; drop the already-parsed raw JSON before
        // building the graph
        parameters.tables = null;

        final List<TableRead> tableReads = new ArrayList<>();
        try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                .createDataSource(PostgresUtil.DRIVER, parameters.url, parameters.user, parameters.password, true)) {

            try(final Connection connection = dataSource.getConnection()) {
                final List<PostgresUtil.TableId> allTables = PostgresUtil.getBaseTables(connection);
                final List<PostgresUtil.TableId> matched = allTables.stream()
                        .filter(t -> tablesParameter.matches(t.schema(), t.name()))
                        .toList();
                if(matched.isEmpty()) {
                    throw new IllegalModuleException(
                            "postgres source module[" + getName() + "].tables matched no table. database tables: "
                                    + allTables.stream().map(PostgresUtil.TableId::qualifiedName).toList());
                }
                LOG.info("postgres source module[{}] reads {} tables: {}", getName(), matched.size(),
                        matched.stream().map(PostgresUtil.TableId::qualifiedName).toList());

                for(final PostgresUtil.TableId tableId : matched) {
                    final String tag = createTag(tableId);
                    final String select = renderTableTemplate(tablesParameter.select, "*", tag, tableId);
                    final String where = renderTableTemplate(tablesParameter.where, null, tag, tableId);

                    final String query = PostgresUtil.createQuery(tableId.quotedName(), select, where, null);
                    final org.apache.avro.Schema avroSchema;
                    final List<PostgresUtil.Column> columns;
                    try(final PreparedStatement statement = connection
                            .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                        final ResultSetMetaData meta = statement.getMetaData();
                        if(meta == null) {
                            throw new IllegalModuleException("Failed to get schema for query: " + query);
                        }
                        avroSchema = ResultSetToRecordConverter.convertSchema(meta);
                        columns = PostgresUtil.getColumns(connection, meta);
                    }

                    // a partitioned-table parent has no storage (blockCount 0) and falls back
                    // to a single unsplit COPY of all partitions
                    final long blockCount = PostgresUtil.getBlockCount(connection, tableId.quotedName());
                    final double estimatedRows = PostgresUtil.getEstimatedRowCount(connection, tableId.quotedName());
                    final List<PostgresUtil.Range> ranges = PostgresUtil
                            .createBlockRanges(blockCount, estimatedRows, parameters.splitSize);
                    LOG.info("{} table: {} blockCount: {}, estimatedRows: {}, split into {} ctid ranges",
                            getName(), tableId.qualifiedName(), blockCount, estimatedRows, ranges.size());

                    tableReads.add(new TableRead(tag, select, where, tableId, columns, avroSchema, ranges));
                }
                connection.commit();
            }
        } catch (final IOException | SQLException e) {
            throw new IllegalModuleException("Failed to connect database. url: " + parameters.url, e);
        }

        MCollectionTuple tuple = MCollectionTuple.empty(begin.getPipeline());
        for(final TableRead tableRead : tableReads) {
            final PCollection<MElement> elements = applyRead(
                    begin, "." + tableRead.tag(), parameters,
                    tableRead.tableId().quotedName(), tableRead.select(), tableRead.where(),
                    tableRead.columns(), tableRead.avroSchema(), tableRead.ranges());
            tuple = tuple.and(tableRead.tag(), elements, Schema.of(tableRead.avroSchema()), Map.of(
                    "table", tableRead.tag(),
                    "schema", tableRead.tableId().schema(),
                    "name", tableRead.tableId().name()));
        }
        return tuple;
    }

    // public-schema tables keep their bare name as the output tag; others are schema-qualified
    private static String createTag(final PostgresUtil.TableId tableId) {
        return "public".equals(tableId.schema()) ? tableId.name() : tableId.qualifiedName();
    }

    private String renderTableTemplate(
            final String template,
            final String defaultValue,
            final String tag,
            final PostgresUtil.TableId tableId) {

        if(template == null) {
            return defaultValue;
        }
        final Map<String, Object> model = new HashMap<>();
        if(getTemplateArgs() != null) {
            model.putAll(getTemplateArgs());
        }
        model.put("table", tag);
        model.put("schema", tableId.schema());
        model.put("name", tableId.name());
        return TemplateUtil.executeStrictTemplate(template, model);
    }

    // Shared by the single-table (table) and all-tables (tables) paths.
    private PCollection<MElement> applyRead(
            final PBegin begin,
            final String nameSuffix,
            final Parameters parameters,
            final String table,
            final String select,
            final String where,
            final List<PostgresUtil.Column> columns,
            final org.apache.avro.Schema avroSchema,
            final List<PostgresUtil.Range> blockRanges) {

        return begin
                .apply("CreateRanges" + nameSuffix, Create
                        .of(blockRanges).withCoder(SerializableCoder.of(PostgresUtil.Range.class)))
                .apply("Reshuffle" + nameSuffix, Reshuffle.viaRandomKey())
                .apply("ReadCopy" + nameSuffix, ParDo.of(new ReadDoFn(
                        parameters.url, parameters.user, parameters.password,
                        table, select, where, columns, avroSchema.toString())))
                .setCoder(AvroCoder.of(avroSchema))
                .apply("Convert" + nameSuffix, ParDo.of(new ConvertDoFn(
                        getTimestampAttribute(), getTimestampDefault())));
    }

    private static class ReadDoFn extends DoFn<PostgresUtil.Range, GenericRecord> {

        private final String url;
        private final String user;
        private final String password;
        private final String table;
        private final String select;
        private final String where;
        private final List<PostgresUtil.Column> columns;
        private final String schemaString;

        private transient org.apache.avro.Schema schema;
        private transient HikariDataSource dataSource;

        // Worker-shared connection pools, reference-counted per url+user so that the teardown
        // of one read branch's DoFn does not close a pool other branches still use.
        private static final Map<String, PoolEntry> POOLS = new HashMap<>();

        private static final class PoolEntry {

            private final HikariDataSource dataSource;
            private int refCount;

            private PoolEntry(final HikariDataSource dataSource) {
                this.dataSource = dataSource;
            }
        }

        ReadDoFn(
                final String url,
                final String user,
                final String password,
                final String table,
                final String select,
                final String where,
                final List<PostgresUtil.Column> columns,
                final String schemaString) {

            this.url = url;
            this.user = user;
            this.password = password;
            this.table = table;
            this.select = select;
            this.where = where;
            this.columns = columns;
            this.schemaString = schemaString;
        }

        private static HikariDataSource acquire(final String url, final String user, final String password) {
            final String key = url + "|" + user;
            synchronized (POOLS) {
                PoolEntry entry = POOLS.get(key);
                if (entry == null) {
                    final HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(url);
                    config.setUsername(user);
                    config.setPassword(password);
                    config.setDriverClassName(PostgresUtil.DRIVER);
                    config.setMaximumPoolSize(10);
                    config.setReadOnly(true);
                    config.addDataSourceProperty("ApplicationName", "mercari-pipeline");
                    entry = new PoolEntry(new HikariDataSource(config));
                    POOLS.put(key, entry);
                }
                entry.refCount++;
                return entry.dataSource;
            }
        }

        private static void release(final String url, final String user) {
            final String key = url + "|" + user;
            synchronized (POOLS) {
                final PoolEntry entry = POOLS.get(key);
                if (entry == null) {
                    return;
                }
                entry.refCount--;
                if (entry.refCount <= 0) {
                    POOLS.remove(key);
                    entry.dataSource.close();
                }
            }
        }

        @Setup
        public void setup() {
            this.schema = AvroSchemaUtil.convertSchema(schemaString);
            this.dataSource = acquire(url, user, password);
        }

        @Teardown
        public void teardown() {
            if (dataSource != null) {
                dataSource = null;
                release(url, user);
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws SQLException, IOException {
            final PostgresUtil.Range range = c.element();
            final String query = PostgresUtil.createQuery(table, select, where, range.createCondition());
            final String copySql = PostgresUtil.createCopyOutStatement(query);
            LOG.info("Start copy out [{}]", copySql);

            long count = 0;
            final Instant start = Instant.now();
            try (final Connection connection = dataSource.getConnection()) {
                final PGConnection pgConnection = connection.unwrap(PGConnection.class);
                try (final PGCopyInputStream pgCopyInputStream = new PGCopyInputStream(pgConnection, copySql);
                     final BufferedInputStream bufferedInputStream = new BufferedInputStream(pgCopyInputStream, 524288);
                     final DataInputStream input = new DataInputStream(bufferedInputStream)) {

                    PostgresUtil.readHeader(input);
                    GenericRecord record;
                    while((record = PostgresUtil.read(input, schema, columns)) != null) {
                        c.output(record);
                        count++;
                    }
                }
            }
            final long time = Instant.now().getMillis() - start.getMillis();
            LOG.info("Finished copy out [{}], total count: [{}], took [{}] millisec", copySql, count, time);
        }
    }

    private static class ConvertDoFn extends DoFn<GenericRecord, MElement> {

        private final String timestampAttribute;
        private final String timestampDefault;

        ConvertDoFn(
                final String timestampAttribute,
                final String timestampDefault) {

            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final GenericRecord record = c.element();
            final MElement element = MElement.of(record, c.timestamp());
            if(timestampAttribute == null) {
                c.output(element);
            } else {
                final Instant timestamp = Optional
                        .ofNullable(element.getAsJodaInstant(timestampAttribute))
                        .orElseGet(() -> DateTimeUtil.toJodaInstant(timestampDefault));
                c.outputWithTimestamp(element, timestamp);
            }
        }
    }

}
