package com.mercari.solution.module.sink;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.mercari.solution.module.*;
import com.mercari.solution.util.cloud.google.SecretManagerUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.domain.db.TiDBUtil;
import com.mercari.solution.util.domain.db.stmt.PreparedStatementTemplate;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.converter.ToStatementConverter;
import com.mercari.solution.module.sink.fileio.ParquetSink;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink module for writing records to TiDB (or MySQL compatible) databases.
 *
 * <p>The write method is selectable through the {@code mode} parameter, mirroring the
 * options analysed for applying TiDB Lightning techniques from Apache Beam:
 * <ul>
 *   <li>{@code jdbc} (default): JDBC batch INSERT (optionally UPSERT). Lowest latency, online table safe.</li>
 *   <li>{@code loaddata}: {@code LOAD DATA LOCAL INFILE} streaming. Higher throughput for medium batches.</li>
 *   <li>{@code importinto}: write Parquet files to GCS and run {@code IMPORT INTO ... FROM 'gs://...'},
 *       which internally uses the Lightning physical import for the highest bulk throughput.</li>
 * </ul>
 * Records that fail to be written are emitted as a PCollection of failure records.
 */
@Sink.Module(name="tidb")
public class TiDBSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBSink.class);

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public enum Mode implements Serializable {
        jdbc,
        loaddata,
        importinto
    }

    private static class Parameters implements Serializable {

        private String url;
        private String user;
        private String password;
        private String table;

        private Mode mode;
        private Boolean autoConfigureUrl;

        private String op;
        private Integer batchSize;
        private Boolean createTable;
        private Boolean emptyTable;
        private List<String> keyFields;

        // for mode importinto
        private String tempDirectory;
        private Integer importThread;

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            } else if(!url.startsWith("jdbc:mysql:")) {
                errorMessages.add("parameters.url must be jdbc:mysql: url");
            }
            if(table == null) {
                errorMessages.add("parameters.table must not be null");
            }
            if(user == null) {
                errorMessages.add("parameters.user must not be null");
            }
            if(batchSize != null && batchSize < 1) {
                errorMessages.add("parameters.batchSize must be positive");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(mode == null) {
                mode = Mode.jdbc;
            }
            if(autoConfigureUrl == null) {
                autoConfigureUrl = true;
            }
            if(op == null) {
                op = JdbcUtil.OP.INSERT.name();
            }
            if(batchSize == null) {
                batchSize = 1000;
            }
            if(createTable == null) {
                createTable = false;
            }
            if(emptyTable == null) {
                emptyTable = false;
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
        }

        private void replaceParameters() {
            if(password == null) {
                password = "";
            }
            if(SecretManagerUtil.isSecretName(user) || SecretManagerUtil.isSecretName(password)) {
                try(final SecretManagerServiceClient secretClient = SecretManagerUtil.createClient()) {
                    if(SecretManagerUtil.isSecretName(user)) {
                        user = SecretManagerUtil.getSecret(secretClient, user).toStringUtf8();
                    }
                    if(SecretManagerUtil.isSecretName(password)) {
                        password = SecretManagerUtil.getSecret(secretClient, password).toStringUtf8();
                    }
                }
            }
            if(autoConfigureUrl) {
                url = TiDBUtil.appendQueryParameters(url, recommendedUrlParameters(mode));
            }
        }
    }

    private static Map<String, String> recommendedUrlParameters(final Mode mode) {
        final Map<String, String> params = new LinkedHashMap<>();
        switch (mode) {
            case jdbc -> {
                params.put("rewriteBatchedStatements", "true");
                params.put("cachePrepStmts", "true");
                params.put("useServerPrepStmts", "true");
                params.put("useConfigs", "maxPerformance");
            }
            case loaddata -> params.put("allowLoadLocalInfile", "true");
            case importinto -> {
                // IMPORT INTO is a single SQL statement; no batch tuning parameters are needed
            }
        }
        return params;
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();
        parameters.replaceParameters();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final PCollection<MElement> tableReady = prepareTable(input, inputSchema, parameters);

        final PCollection<MElement> failures = switch (parameters.mode) {
            case jdbc -> writeJdbc(tableReady, inputSchema, parameters);
            case loaddata -> writeLoadData(tableReady, inputSchema, parameters);
            case importinto -> writeImportInto(tableReady, inputSchema, parameters);
        };

        return MCollectionTuple.of(failures, MFailure.schema());
    }

    private PCollection<MElement> prepareTable(
            final PCollection<MElement> input,
            final Schema inputSchema,
            final Parameters parameters) {

        final List<List<String>> ddls = new ArrayList<>();
        if(parameters.createTable) {
            final String ddl = JdbcUtil.buildCreateTableSQL(
                    inputSchema.getAvroSchema(), parameters.table, JdbcUtil.DB.MYSQL, parameters.keyFields);
            ddls.add(Arrays.asList(ddl));
        }
        if(parameters.emptyTable) {
            ddls.add(Arrays.asList("DELETE FROM " + parameters.table));
        }
        if(ddls.isEmpty()) {
            return input;
        }
        final PCollection<String> wait = input.getPipeline()
                .apply("SupplyDDL", Create.of(ddls).withCoder(ListCoder.of(StringUtf8Coder.of())))
                .apply("PrepareTable", ParDo.of(new TablePrepareDoFn(
                        parameters.url, parameters.user, parameters.password)));
        return input
                .apply("WaitToTableReady", Wait.on(wait))
                .setCoder(input.getCoder());
    }

    private PCollection<MElement> writeJdbc(
            final PCollection<MElement> input,
            final Schema inputSchema,
            final Parameters parameters) {

        final PreparedStatementTemplate statementTemplate = JdbcUtil.createStatement(
                parameters.table, inputSchema.getAvroSchema(),
                JdbcUtil.OP.valueOf(parameters.op), JdbcUtil.DB.MYSQL, parameters.keyFields);

        return input
                .apply("WriteJdbcBatch", ParDo.of(new WriteJdbcDoFn(
                        getJobName(), getName(), parameters, statementTemplate)))
                .setCoder(ElementCoder.of(MFailure.schema()));
    }

    private PCollection<MElement> writeLoadData(
            final PCollection<MElement> input,
            final Schema inputSchema,
            final Parameters parameters) {

        return input
                .apply("WriteLoadData", ParDo.of(new WriteLoadDataDoFn(
                        getJobName(), getName(), parameters, inputSchema)))
                .setCoder(ElementCoder.of(MFailure.schema()));
    }

    private PCollection<MElement> writeImportInto(
            final PCollection<MElement> input,
            final Schema inputSchema,
            final Parameters parameters) {

        final String tempDirectory = resolveTempDirectory(input, parameters);
        final String outputDir = stripTrailingSlash(tempDirectory)
                + "/tidb_import/" + getJobName() + "/" + getName() + "/";
        final String glob = outputDir + "*.parquet";
        LOG.info("{} mode importinto writes parquet to {} then IMPORT INTO from {}", getName(), outputDir, glob);

        final PCollection<KV<String, MElement>> withKey = input
                .apply("WithKey", ParDo.of(new WithKeyDoFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder()));

        final WriteFilesResult<Void> writeResult = withKey
                .apply("WriteParquet", FileIO.<KV<String, MElement>>write()
                        .via(ParquetSink.of(inputSchema, StorageSink.CodecName.SNAPPY, false))
                        .to(outputDir)
                        .withNaming(FileIO.Write.defaultNaming("part", ".parquet")));

        final PCollection<String> writtenFiles = writeResult
                .getPerDestinationOutputFilenames()
                .apply("ExtractFileName", ParDo.of(new ExtractFileNameDoFn()))
                .setCoder(StringUtf8Coder.of());

        return input.getPipeline()
                .apply("SeedImport", Create.of(glob).withCoder(StringUtf8Coder.of()))
                .apply("WaitFilesWritten", Wait.on(writtenFiles))
                .setCoder(StringUtf8Coder.of())
                .apply("ImportInto", ParDo.of(new ImportIntoDoFn(getJobName(), getName(), parameters)))
                .setCoder(ElementCoder.of(MFailure.schema()));
    }

    private String resolveTempDirectory(final PCollection<MElement> input, final Parameters parameters) {
        if(parameters.tempDirectory != null) {
            return parameters.tempDirectory;
        }
        final String tempLocation = input.getPipeline().getOptions().getTempLocation();
        if(tempLocation == null) {
            throw new IllegalModuleException(
                    "tidb sink mode importinto requires parameters.tempDirectory or pipeline tempLocation");
        }
        return tempLocation;
    }

    private static String stripTrailingSlash(final String path) {
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }


    private static class WriteJdbcDoFn extends DoFn<MElement, MElement> {

        private final String job;
        private final String module;
        private final String url;
        private final String user;
        private final String password;
        private final PreparedStatementTemplate statementTemplate;
        private final int batchSize;

        private transient JdbcUtil.CloseableDataSource dataSource;
        private transient Connection connection;
        private transient PreparedStatement preparedStatement;
        private transient List<MElement> buffer;

        WriteJdbcDoFn(
                final String job,
                final String module,
                final Parameters parameters,
                final PreparedStatementTemplate statementTemplate) {

            this.job = job;
            this.module = module;
            this.url = parameters.url;
            this.user = parameters.user;
            this.password = parameters.password;
            this.statementTemplate = statementTemplate;
            this.batchSize = parameters.batchSize;
        }

        @Setup
        public void setup() {
            this.dataSource = JdbcUtil.createDataSource(TiDBUtil.DRIVER, url, user, password);
        }

        @Teardown
        public void teardown() throws Exception {
            cleanUp();
            if(dataSource != null) {
                try {
                    dataSource.close();
                } catch (final IOException e) {
                    LOG.warn("tidb sink failed to close dataSource: {}", e.getMessage());
                } finally {
                    dataSource = null;
                }
            }
        }

        @StartBundle
        public void startBundle() throws SQLException {
            if(connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(statementTemplate.getStatementString());
            }
            buffer = new ArrayList<>();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws SQLException {
            final MElement element = c.element();
            preparedStatement.clearParameters();
            ToStatementConverter.convertElement(element, statementTemplate.createPlaceholderSetterProxy(preparedStatement));
            preparedStatement.addBatch();
            buffer.add(element);
            if(buffer.size() >= batchSize) {
                flush(c);
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) throws SQLException {
            try {
                if(buffer != null && !buffer.isEmpty()) {
                    preparedStatement.executeBatch();
                    connection.commit();
                }
            } catch (final SQLException e) {
                safeRollback();
                emitFailures(buffer, e, (element, failure) ->
                        c.output(failure, element.getTimestamp(), org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE));
            } finally {
                buffer = null;
            }
        }

        private void flush(final ProcessContext c) throws SQLException {
            try {
                preparedStatement.executeBatch();
                connection.commit();
            } catch (final SQLException e) {
                safeRollback();
                emitFailures(buffer, e, (element, failure) -> c.output(failure));
            } finally {
                buffer = new ArrayList<>();
            }
        }

        private void emitFailures(
                final List<MElement> elements,
                final SQLException e,
                final FailureConsumer consumer) {

            for(final MElement element : elements) {
                final MElement failure = MFailure
                        .of(job, module, element.toString(), e, element.getTimestamp())
                        .toElement(element.getTimestamp());
                consumer.accept(element, failure);
            }
        }

        private void safeRollback() {
            try {
                preparedStatement.clearBatch();
                connection.rollback();
            } catch (final SQLException re) {
                LOG.warn("tidb sink failed to rollback: {}", re.getMessage());
            }
        }

        private void cleanUp() {
            if(preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (final SQLException e) {
                    // ignore
                } finally {
                    preparedStatement = null;
                }
            }
            if(connection != null) {
                try {
                    connection.close();
                } catch (final SQLException e) {
                    // ignore
                } finally {
                    connection = null;
                }
            }
        }

        @FunctionalInterface
        private interface FailureConsumer {
            void accept(MElement element, MElement failure);
        }
    }

    private static class WriteLoadDataDoFn extends DoFn<MElement, MElement> {

        private final String job;
        private final String module;
        private final String url;
        private final String user;
        private final String password;
        private final String table;
        private final String op;
        private final int batchSize;
        private final Schema schema;
        private final List<String> fields;

        private transient JdbcUtil.CloseableDataSource dataSource;
        private transient Connection connection;
        private transient List<MElement> buffer;
        private transient String loadStatement;

        WriteLoadDataDoFn(
                final String job,
                final String module,
                final Parameters parameters,
                final Schema schema) {

            this.job = job;
            this.module = module;
            this.url = parameters.url;
            this.user = parameters.user;
            this.password = parameters.password;
            this.table = parameters.table;
            this.op = parameters.op;
            this.batchSize = parameters.batchSize;
            this.schema = schema;
            this.fields = schema.getFields().stream().map(Schema.Field::getName).toList();
        }

        @Setup
        public void setup() {
            this.schema.setup();
            this.dataSource = JdbcUtil.createDataSource(TiDBUtil.DRIVER, url, user, password);
            this.loadStatement = createLoadStatement();
        }

        @Teardown
        public void teardown() {
            if(connection != null) {
                try {
                    connection.close();
                } catch (final SQLException e) {
                    // ignore
                } finally {
                    connection = null;
                }
            }
            if(dataSource != null) {
                try {
                    dataSource.close();
                } catch (final IOException e) {
                    LOG.warn("tidb sink failed to close dataSource: {}", e.getMessage());
                } finally {
                    dataSource = null;
                }
            }
        }

        @StartBundle
        public void startBundle() throws SQLException {
            if(connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(true);
            }
            buffer = new ArrayList<>();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            buffer.add(c.element());
            if(buffer.size() >= batchSize) {
                flush(c::output);
            }
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) {
            if(buffer != null && !buffer.isEmpty()) {
                flush(failure -> c.output(failure, failure.getTimestamp(),
                        org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE));
            }
            buffer = null;
        }

        private void flush(final java.util.function.Consumer<MElement> failureConsumer) {
            final List<MElement> flushing = buffer;
            buffer = new ArrayList<>();
            final byte[] csv = toCsv(flushing);
            try(final InputStream stream = new ByteArrayInputStream(csv);
                final Statement statement = connection.createStatement()) {

                statement.unwrap(com.mysql.cj.jdbc.JdbcStatement.class).setLocalInfileInputStream(stream);
                statement.execute(loadStatement);
            } catch (final SQLException | IOException e) {
                LOG.error("tidb sink LOAD DATA failed: {}", e.getMessage());
                for(final MElement element : flushing) {
                    failureConsumer.accept(MFailure
                            .of(job, module, element.toString(), e, element.getTimestamp())
                            .toElement(element.getTimestamp()));
                }
            }
        }

        private byte[] toCsv(final List<MElement> elements) {
            final StringBuilder sb = new StringBuilder();
            for(final MElement element : elements) {
                for(int i = 0; i < fields.size(); i++) {
                    if(i > 0) {
                        sb.append(',');
                    }
                    sb.append(toLoadDataField(schema.getField(fields.get(i)), element));
                }
                sb.append('\n');
            }
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        private String toLoadDataField(final Schema.Field field, final MElement element) {
            final Object value = element.getAsStandardValue(field);
            if(value == null) {
                return "\\N";
            }
            final String text;
            if(value instanceof Instant instant) {
                text = TIMESTAMP_FORMAT.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
            } else {
                text = value.toString();
            }
            final String escaped = text
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r");
            return "\"" + escaped + "\"";
        }

        private String createLoadStatement() {
            final String prefix = switch (JdbcUtil.OP.valueOf(op)) {
                case INSERT_OR_UPDATE -> "REPLACE ";
                case INSERT_OR_DONOTHING -> "IGNORE ";
                default -> "";
            };
            final String columns = fields.stream().map(f -> "`" + f + "`").collect(java.util.stream.Collectors.joining(","));
            return "LOAD DATA LOCAL INFILE 'stream' " + prefix + "INTO TABLE " + table
                    + " CHARACTER SET utf8mb4"
                    + " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\'"
                    + " LINES TERMINATED BY '\\n'"
                    + " (" + columns + ")";
        }
    }

    private static class ImportIntoDoFn extends DoFn<String, MElement> {

        private final String job;
        private final String module;
        private final String url;
        private final String user;
        private final String password;
        private final String table;
        private final Integer importThread;

        ImportIntoDoFn(final String job, final String module, final Parameters parameters) {
            this.job = job;
            this.module = module;
            this.url = parameters.url;
            this.user = parameters.user;
            this.password = parameters.password;
            this.table = parameters.table;
            this.importThread = parameters.importThread;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final String glob = c.element();
            final StringBuilder sb = new StringBuilder("IMPORT INTO ").append(table)
                    .append(" FROM '").append(glob).append("' FORMAT 'parquet'");
            if(importThread != null) {
                sb.append(" WITH thread=").append(importThread);
            }
            final String importSql = sb.toString();
            LOG.info("tidb sink execute: {}", importSql);

            try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                    .createDataSource(TiDBUtil.DRIVER, url, user, password);
                final Connection connection = dataSource.getConnection()) {

                connection.setAutoCommit(true);
                try(final Statement statement = connection.createStatement()) {
                    statement.execute(importSql);
                }
                LOG.info("tidb sink finished: {}", importSql);
            } catch (final SQLException | IOException e) {
                LOG.error("tidb sink IMPORT INTO failed: {}", e.getMessage());
                c.output(MFailure.of(job, module, importSql, e, c.timestamp()).toElement(c.timestamp()));
            }
        }
    }

    private static class WithKeyDoFn extends DoFn<MElement, KV<String, MElement>> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(KV.of("", c.element()));
        }
    }

    private static class ExtractFileNameDoFn extends DoFn<KV<Void, String>, String> {
        @ProcessElement
        public void processElement(final ProcessContext c) {
            c.output(c.element().getValue());
        }
    }

    private static class TablePrepareDoFn extends DoFn<List<String>, String> {

        private final String url;
        private final String user;
        private final String password;

        TablePrepareDoFn(final String url, final String user, final String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            final List<String> ddl = c.element();
            if(ddl == null) {
                return;
            }
            if(ddl.isEmpty()) {
                c.output("ok");
                return;
            }
            try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                    .createDataSource(TiDBUtil.DRIVER, url, user, password);
                final Connection connection = dataSource.getConnection()) {
                for(final String sql : ddl) {
                    LOG.info("ExecuteDDL: " + sql);
                    connection.createStatement().executeUpdate(sql);
                    connection.commit();
                    LOG.info("ExecutedDDL: " + sql);
                }
            }
            c.output("ok");
        }
    }

}
