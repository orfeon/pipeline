package com.mercari.solution.module.source;

import com.google.gson.JsonElement;
import com.mercari.solution.util.cloud.SecretProviders;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.domain.db.PgOutput;
import com.mercari.solution.util.domain.db.PostgresReplicationSource;
import com.mercari.solution.util.domain.db.PostgresUtil;
import com.mercari.solution.util.pipeline.cdc.PostgresChangeCapture;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
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

        // for changeDataCapture mode parameters
        private CdcParameter cdc;

        private static class CdcParameter implements Serializable {

            private String slot;
            private String publication;
            private Boolean createSlot;
            private Integer statusIntervalSeconds;
            // bound the read (mainly for tests and one-shot drains); absent = endless stream
            private Long maxNumRecords;
            private Long maxReadTimeSeconds;

            private List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                if(slot == null) {
                    errorMessages.add("parameters.cdc.slot must not be null");
                }
                if(publication == null) {
                    errorMessages.add("parameters.cdc.publication must not be null");
                }
                if(statusIntervalSeconds != null && statusIntervalSeconds < 1) {
                    errorMessages.add("parameters.cdc.statusIntervalSeconds must be positive");
                }
                return errorMessages;
            }

            private void setDefaults() {
                if(createSlot == null) {
                    createSlot = false;
                }
                if(statusIntervalSeconds == null) {
                    statusIntervalSeconds = 10;
                }
            }
        }

        public void validate(final Mode mode) {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            } else if(!url.startsWith("jdbc:postgresql:")) {
                errorMessages.add("parameters.url must be jdbc:postgresql: url");
            }
            if(Mode.changeDataCapture.equals(mode)) {
                if(cdc == null) {
                    errorMessages.add("parameters.cdc must not be null if mode is changeDataCapture");
                } else {
                    errorMessages.addAll(cdc.validate());
                }
                if(table != null || tables != null || select != null || where != null) {
                    errorMessages.add("parameters.table, tables, select and where are not applicable if mode is changeDataCapture");
                }
            } else {
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
                if(cdc != null) {
                    errorMessages.add("parameters.cdc is only applicable if mode is changeDataCapture");
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
            if(cdc != null) {
                cdc.setDefaults();
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
        parameters.validate(getMode());
        parameters.setDefaults();
        parameters.replaceParameters(begin.getPipeline());

        if(Mode.changeDataCapture.equals(getMode())) {
            return expandChangeDataCapture(begin, parameters);
        }
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

    /**
     * The changeDataCapture mode: reads the logical replication slot with the pgoutput
     * plugin (binary tuple mode) and outputs provider-native change records
     * ({@code postgres_cdc.avsc}); the {@code cdc} transform (format: postgres) normalizes
     * them into the unified envelope.
     *
     * Server requirements ({@code wal_level=logical}, PostgreSQL 14+, publication, slot)
     * are verified at assembly time; the slot is created here when {@code cdc.createSlot}.
     */
    private MCollectionTuple expandChangeDataCapture(final PBegin begin, final Parameters parameters) {

        final Parameters.CdcParameter cdc = parameters.cdc;
        final String database;
        final Map<String, List<String>> keyColumns = new HashMap<>();
        try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                .createDataSource(PostgresUtil.DRIVER, parameters.url, parameters.user, parameters.password)) {

            try(final Connection connection = dataSource.getConnection()) {
                final int serverVersion = Integer.parseInt(queryFirstString(connection, "SHOW server_version_num"));
                if(serverVersion < 140000) {
                    throw new IllegalModuleException("postgres source module[" + getName()
                            + "] changeDataCapture mode requires PostgreSQL 14 or later (pgoutput binary mode). server_version_num: " + serverVersion);
                }
                final String walLevel = queryFirstString(connection, "SHOW wal_level");
                if(!"logical".equals(walLevel)) {
                    throw new IllegalModuleException("postgres source module[" + getName()
                            + "] changeDataCapture mode requires wal_level=logical. current wal_level: " + walLevel);
                }
                if(!exists(connection, "SELECT pubname FROM pg_publication WHERE pubname = ?", cdc.publication)) {
                    throw new IllegalModuleException("postgres source module[" + getName()
                            + "].cdc.publication: " + cdc.publication + " does not exist. create it with: CREATE PUBLICATION " + cdc.publication + " FOR TABLE ...");
                }
                if(!exists(connection, "SELECT slot_name FROM pg_replication_slots WHERE slot_name = ?", cdc.slot)) {
                    if(!cdc.createSlot) {
                        throw new IllegalModuleException("postgres source module[" + getName()
                                + "].cdc.slot: " + cdc.slot + " does not exist. create it with: SELECT pg_create_logical_replication_slot('" + cdc.slot + "', 'pgoutput') or set cdc.createSlot: true");
                    }
                    try(final PreparedStatement statement = connection
                            .prepareStatement("SELECT pg_create_logical_replication_slot(?, 'pgoutput')")) {
                        statement.setString(1, cdc.slot);
                        statement.execute();
                    }
                    LOG.info("postgres source module[{}] created logical replication slot: {}", getName(), cdc.slot);
                }
                // primary key columns of the published tables: pins the change records'
                // keysJson to the primary key even under REPLICA IDENTITY FULL (where the
                // protocol's key flags cover every column)
                final String keysSql = """
                        SELECT pt.schemaname, pt.tablename, a.attname
                        FROM pg_publication_tables pt
                        JOIN pg_class c ON c.relname = pt.tablename
                        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = pt.schemaname
                        JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
                        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                        WHERE pt.pubname = ANY(string_to_array(?, ','))
                        ORDER BY pt.schemaname, pt.tablename, array_position(i.indkey, a.attnum)
                        """;
                try(final PreparedStatement statement = connection.prepareStatement(keysSql)) {
                    statement.setString(1, cdc.publication);
                    try(final ResultSet resultSet = statement.executeQuery()) {
                        while(resultSet.next()) {
                            keyColumns
                                    .computeIfAbsent(resultSet.getString(1) + "." + resultSet.getString(2), k -> new ArrayList<>())
                                    .add(resultSet.getString(3));
                        }
                    }
                }
                LOG.info("postgres source module[{}] publication: {} key columns: {}", getName(), cdc.publication, keyColumns);

                database = connection.getCatalog();
                connection.commit();
            }
        } catch (final IOException | SQLException e) {
            throw new IllegalModuleException("Failed to connect database. url: " + parameters.url, e);
        }

        final PostgresReplicationSource source = new PostgresReplicationSource(
                parameters.url, parameters.user, parameters.password,
                cdc.slot, cdc.publication, cdc.statusIntervalSeconds, keyColumns);

        final PCollection<PgOutput.ChangeEvent> events;
        final Read.Unbounded<PgOutput.ChangeEvent> read = Read.from(source);
        if(cdc.maxNumRecords != null || cdc.maxReadTimeSeconds != null) {
            var bounded = read.withMaxNumRecords(Optional.ofNullable(cdc.maxNumRecords).orElse(Long.MAX_VALUE));
            if(cdc.maxReadTimeSeconds != null) {
                bounded = bounded.withMaxReadTime(Duration.standardSeconds(cdc.maxReadTimeSeconds));
            }
            events = begin.apply("ReadReplicationStream", bounded);
        } else {
            events = begin.apply("ReadReplicationStream", read);
        }

        final Schema outputSchema = PostgresChangeCapture.schema();
        final PCollection<MElement> elements = events
                .apply("ConvertToElement", ParDo.of(new ChangeEventToElementDoFn(outputSchema, database)))
                .setCoder(ElementCoder.of(outputSchema))
                // the replication slot is read by a single reader: break fusion so that
                // downstream stages distribute across workers
                .apply("Redistribute", Reshuffle.viaRandomKey());

        return MCollectionTuple
                .of(elements, outputSchema);
    }

    private static String queryFirstString(final Connection connection, final String sql) throws SQLException {
        try(final PreparedStatement statement = connection.prepareStatement(sql);
            final ResultSet resultSet = statement.executeQuery()) {
            if(!resultSet.next()) {
                throw new SQLException("Empty result for query: " + sql);
            }
            return resultSet.getString(1);
        }
    }

    private static boolean exists(final Connection connection, final String sql, final String value) throws SQLException {
        try(final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, value);
            try(final ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private static class ChangeEventToElementDoFn extends DoFn<PgOutput.ChangeEvent, MElement> {

        private final Schema outputSchema;
        private final String database;

        ChangeEventToElementDoFn(final Schema outputSchema, final String database) {
            this.outputSchema = outputSchema;
            this.database = database;
        }

        @Setup
        public void setup() {
            outputSchema.setup(DataType.AVRO);
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = PostgresChangeCapture.convert(c.element(), database, c.timestamp());
            c.output(element.convert(outputSchema, DataType.AVRO));
        }
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
