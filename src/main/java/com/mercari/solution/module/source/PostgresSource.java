package com.mercari.solution.module.source;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.cloud.google.SecretManagerUtil;
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
import java.util.List;
import java.util.Optional;

/**
 * Source module for reading records from PostgreSQL (or compatible) databases
 * using {@code COPY (SELECT ...) TO STDOUT (FORMAT BINARY)} for high throughput.
 * The table is split into physical block ({@code ctid}) ranges, and the ranges are
 * read in parallel with efficient TID range scans.
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

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            } else if(!url.startsWith("jdbc:postgresql:")) {
                errorMessages.add("parameters.url must be jdbc:postgresql: url");
            }
            if(table == null) {
                errorMessages.add("parameters.table must not be null");
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
            } else if(SecretManagerUtil.isSecretName(user) || SecretManagerUtil.isSecretName(password)) {
                LOG.info("parameters.user|password is secret resource.");
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

        public static class TableParameter implements Serializable {

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
                    columns = PostgresUtil.getColumns(meta);
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

        final PCollection<PostgresUtil.Range> ranges = begin
                .apply("CreateRanges", Create
                        .of(blockRanges).withCoder(SerializableCoder.of(PostgresUtil.Range.class)))
                .apply("Reshuffle", Reshuffle.viaRandomKey());

        final PCollection<GenericRecord> records = ranges
                .apply("ReadCopy", ParDo.of(new ReadDoFn(parameters, columns, outputAvroSchema.toString())))
                .setCoder(AvroCoder.of(outputAvroSchema));

        final PCollection<MElement> elements = records
                .apply("Convert", ParDo.of(new ConvertDoFn(getTimestampAttribute(), getTimestampDefault())));

        return MCollectionTuple
                .of(elements, Schema.of(outputAvroSchema));
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

        private static volatile HikariDataSource dataSource;

        ReadDoFn(
                final Parameters parameters,
                final List<PostgresUtil.Column> columns,
                final String schemaString) {

            this.url = parameters.url;
            this.user = parameters.user;
            this.password = parameters.password;
            this.table = parameters.table;
            this.select = parameters.select;
            this.where = parameters.where;
            this.columns = columns;
            this.schemaString = schemaString;
        }

        private static HikariDataSource getDataSource(String url, String user, String password) {
            if (dataSource == null) {
                synchronized (ReadDoFn.class) {
                    if (dataSource == null) {
                        final HikariConfig config = new HikariConfig();
                        config.setJdbcUrl(url);
                        config.setUsername(user);
                        config.setPassword(password);
                        config.setDriverClassName(PostgresUtil.DRIVER);
                        config.setMaximumPoolSize(10);
                        config.setReadOnly(true);
                        config.addDataSourceProperty("ApplicationName", "mercari-pipeline");
                        dataSource = new HikariDataSource(config);
                    }
                }
            }
            return dataSource;
        }

        @Setup
        public void setup() {
            this.schema = AvroSchemaUtil.convertSchema(schemaString);
            getDataSource(url, user, password);
        }

        @Teardown
        public void teardown() throws SQLException, IOException {
            if (dataSource != null && !dataSource.isClosed()) {
                synchronized (ReadDoFn.class) {
                    if (dataSource != null && !dataSource.isClosed()) {
                        dataSource.close();
                        dataSource = null;
                    }
                }
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
            try (final Connection connection = getDataSource(url, user, password).getConnection()) {
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