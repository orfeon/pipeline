package com.mercari.solution.module.sink;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.mercari.solution.module.*;
import com.mercari.solution.util.cloud.google.SecretManagerUtil;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.domain.db.PostgresUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Sink module for writing records to PostgreSQL (or compatible) databases
 * using {@code COPY ... FROM STDIN (FORMAT BINARY)} for high throughput.
 */
@Sink.Module(name="postgres")
public class PostgresSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSink.class);

    private static class Parameters implements Serializable {

        private String url;
        private String user;
        private String password;
        private String table;
        private Boolean createTable;
        private Boolean emptyTable;
        private List<String> keyFields;
        private Integer batchSize;
        private Integer bufferSize;

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            } else if(!url.startsWith("jdbc:postgresql:")) {
                errorMessages.add("parameters.url must be jdbc:postgresql: url");
            }
            if(table == null) {
                errorMessages.add("parameters.table must not be null");
            }
            if(user == null) {
                errorMessages.add("parameters.user must not be null");
            }
            if(password == null) {
                errorMessages.add("parameters.password must not be null");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(createTable == null) {
                createTable = false;
            }
            if(emptyTable == null) {
                emptyTable = false;
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(batchSize == null) {
                batchSize = 100_000;
            }
            if(bufferSize == null) {
                bufferSize = 65536;
            }
        }

        public void replaceParameters() {
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
        }
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

        final List<List<String>> ddls = new ArrayList<>();
        if(parameters.createTable) {
            final String ddl = JdbcUtil.buildCreateTableSQL(
                    inputSchema.getAvroSchema(), parameters.table, JdbcUtil.DB.POSTGRESQL, parameters.keyFields);
            ddls.add(Arrays.asList(ddl));
        }
        if(parameters.emptyTable) {
            ddls.add(Arrays.asList("DELETE FROM " + parameters.table));
        }

        final PCollection<MElement> tableReady;
        if(ddls.isEmpty()) {
            tableReady = input;
        } else {
            final PCollection<String> wait = input.getPipeline()
                    .apply("SupplyDDL", Create.of(ddls).withCoder(ListCoder.of(StringUtf8Coder.of())))
                    .apply("PrepareTable", ParDo.of(new TablePrepareDoFn(
                            parameters.url, parameters.user, parameters.password)));
            tableReady = input
                    .apply("WaitToTableCreation", Wait.on(wait))
                    .setCoder(input.getCoder());
        }

        final PCollection<MElement> results = tableReady
                .apply("WriteCopy", ParDo.of(new WriteDoFn(parameters, inputSchema.getAvroSchema().toString())));

        return MCollectionTuple
                .of(results, Schema.builder().withField("dummy", Schema.FieldType.STRING).build());
    }

    private static class WriteDoFn extends DoFn<MElement, MElement> {

        private final String url;
        private final String user;
        private final String password;
        private final String table;
        private final Integer batchSize;
        private final Integer bufferSize;
        private final String schemaString;

        private transient org.apache.avro.Schema schema;
        private transient JdbcUtil.CloseableDataSource dataSource;
        private transient Connection connection;
        private transient List<PostgresUtil.Column> columns;
        private transient List<org.apache.avro.Schema.Field> fields;
        private transient String copyStatement;

        private transient DataOutputStream output;
        private transient int count;

        WriteDoFn(final Parameters parameters, final String schemaString) {
            this.url = parameters.url;
            this.user = parameters.user;
            this.password = parameters.password;
            this.table = parameters.table;
            this.batchSize = parameters.batchSize;
            this.bufferSize = parameters.bufferSize;
            this.schemaString = schemaString;
        }

        @Setup
        public void setup() {
            this.schema = new org.apache.avro.Schema.Parser().parse(schemaString);
            this.dataSource = JdbcUtil.createDataSource(PostgresUtil.DRIVER, url, user, password);
        }

        @Teardown
        public void teardown() throws Exception {
            cleanUpDataSource();
        }

        @StartBundle
        public void startBundle(final StartBundleContext c) throws SQLException {
            if(connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
            }
            if(columns == null) {
                resolveColumns();
            }
            count = 0;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws SQLException, IOException {
            try {
                if(output == null) {
                    openCopy();
                }
                final GenericRecord record = ElementToAvroConverter.convert(schema, c.element());
                PostgresUtil.write(output, columns, fields, record);
                count += 1;

                if(count >= batchSize) {
                    closeCopy();
                    connection.commit();
                    count = 0;
                }
            } catch (final SQLException | IOException | RuntimeException e) {
                rollback();
                throw new RuntimeException(e);
            }
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            try {
                closeCopy();
                connection.commit();
                cleanUpConnection();
            } catch (final SQLException | IOException | RuntimeException e) {
                rollback();
                cleanUpConnection();
                throw new RuntimeException(e);
            }
        }

        private void resolveColumns() throws SQLException {
            final List<PostgresUtil.Column> tableColumns = PostgresUtil
                    .getColumnsFromQuery(connection, "SELECT * FROM " + table);
            this.columns = new ArrayList<>();
            this.fields = new ArrayList<>();
            final List<String> columnNames = new ArrayList<>();
            for(final PostgresUtil.Column column : tableColumns) {
                org.apache.avro.Schema.Field field = schema.getField(column.name);
                if(field == null) {
                    field = schema.getFields().stream()
                            .filter(f -> f.name().equalsIgnoreCase(column.name))
                            .findAny()
                            .orElse(null);
                }
                if(field == null) {
                    LOG.info("postgres sink table: {} column: {} is missing in input. skipped", table, column.name);
                    continue;
                }
                this.columns.add(column);
                this.fields.add(field);
                columnNames.add(column.name);
            }
            if(this.columns.isEmpty()) {
                throw new IllegalStateException(
                        "postgres sink table: " + table + " has no columns matching input fields: " + schema.getFields());
            }
            this.copyStatement = PostgresUtil.createCopyInStatement(table, columnNames);
            LOG.info("postgres sink copy statement: {}", copyStatement);
        }

        private void openCopy() throws SQLException, IOException {
            final PGConnection pgConnection = PostgresUtil.getPGConnection(connection);
            this.output = new DataOutputStream(new PGCopyOutputStream(pgConnection, copyStatement, bufferSize));
            PostgresUtil.writeHeader(output);
        }

        private void closeCopy() throws SQLException, IOException {
            if(output != null) {
                try {
                    PostgresUtil.writeTrailer(output);
                    output.close();
                } finally {
                    output = null;
                }
            }
        }

        private void rollback() {
            try {
                if(output != null) {
                    try {
                        output.close();
                    } catch (final Exception e) {
                        LOG.warn("postgres sink failed to close copy stream: {}", e.getMessage());
                    } finally {
                        output = null;
                    }
                }
                if(connection != null) {
                    connection.rollback();
                }
            } catch (final SQLException e) {
                LOG.warn("postgres sink failed to rollback: {}", e.getMessage());
            }
        }

        private void cleanUpConnection() throws SQLException {
            if(connection != null) {
                try {
                    connection.close();
                } finally {
                    connection = null;
                    columns = null;
                    fields = null;
                }
            }
        }

        private void cleanUpDataSource() throws Exception {
            try {
                closeCopy();
            } catch (final Exception e) {
                LOG.warn("postgres sink failed to close copy stream: {}", e.getMessage());
            }
            cleanUpConnection();
            if(dataSource != null) {
                try {
                    dataSource.close();
                } catch (final IOException e) {
                    LOG.warn("postgres sink failed to close dataSource: {}", e.getMessage());
                } finally {
                    dataSource = null;
                }
            }
        }
    }

    private static class TablePrepareDoFn extends DoFn<List<String>, String> {

        private static final Logger LOG = LoggerFactory.getLogger(TablePrepareDoFn.class);

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
                    .createDataSource(PostgresUtil.DRIVER, url, user, password)) {
                try(final Connection connection = dataSource.getConnection()) {
                    for(final String sql : ddl) {
                        LOG.info("ExecuteDDL: " + sql);
                        connection.createStatement().executeUpdate(sql);
                        connection.commit();
                        LOG.info("ExecutedDDL: " + sql);
                    }
                }
            }
            c.output("ok");
        }
    }

}
