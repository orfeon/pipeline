package com.mercari.solution.module.source;

import com.mercari.solution.util.cloud.SecretProviders;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.domain.db.TiDBUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Source module for reading records from TiDB (or MySQL compatible) databases.
 *
 * <p>The table is split into chunks and read in parallel by distributed workers.
 * TiDB specific features are leveraged for high throughput:
 * <ul>
 *   <li>{@code TABLESAMPLE REGIONS()} splits the table along TiKV region boundaries so
 *       chunks are distributed across TiKV nodes (with numeric MIN/MAX split as fallback).</li>
 *   <li>{@code tidb_snapshot} gives every worker a lock-free, consistent MVCC snapshot.</li>
 *   <li>{@code _tidb_rowid} is used as the split key for tables without an explicit primary key.</li>
 * </ul>
 */
@Source.Module(name="tidb")
public class TiDBSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBSource.class);

    private static class Parameters implements Serializable {

        private String url;
        private String user;
        private String password;

        private String table;
        private String select;
        private String where;
        private String splitField;
        private Long splitSize;
        private Boolean useSnapshot;
        private Integer fetchSize;

        public void validate() {
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
            if(useSnapshot == null) {
                useSnapshot = true;
            }
            if(fetchSize == null) {
                // MySQL connector row-by-row streaming mode (avoids buffering the whole result on the client)
                fetchSize = Integer.MIN_VALUE;
            }
        }

        public void replaceParameters() {
            if(password == null) {
                password = "";
            }
            if(SecretProviders.isSecretReference(user) || SecretProviders.isSecretReference(password)) {
                LOG.info("parameters.user|password is secret resource.");
                user = SecretProviders.resolveIfSecret(user);
                password = SecretProviders.resolveIfSecret(password);
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();
        parameters.replaceParameters();

        final org.apache.avro.Schema outputAvroSchema;
        final List<TiDBUtil.Range> ranges;
        final String keyField;
        final String snapshotTSO;
        try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil
                .createDataSource(TiDBUtil.DRIVER, parameters.url, parameters.user, parameters.password, true)) {

            try(final Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(true);

                final String query = TiDBUtil.createQuery(
                        parameters.table, parameters.select, parameters.where, null);
                try(final PreparedStatement statement = connection
                        .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    final ResultSetMetaData meta = statement.getMetaData();
                    if(meta == null) {
                        throw new IllegalModuleException("Failed to get schema for query: " + query);
                    }
                    outputAvroSchema = ResultSetToRecordConverter.convertSchema(meta);
                }

                snapshotTSO = parameters.useSnapshot ? TiDBUtil.getSnapshotTSO(connection) : null;

                final TiDBUtil.SplitKey splitKey = TiDBUtil.resolveSplitKey(
                        connection, parameters.table, parameters.splitField);
                keyField = splitKey == null ? null : splitKey.field();
                ranges = createRanges(connection, parameters, splitKey);
            }
        } catch (final IOException | SQLException e) {
            throw new IllegalModuleException("Failed to connect database. url: " + parameters.url, e);
        }
        LOG.info("{} outputSchema: {}, splitKey: {}, snapshotTSO: {}, ranges: {}",
                getName(), outputAvroSchema, keyField, snapshotTSO, ranges.size());

        final PCollection<TiDBUtil.Range> rangeCollection = begin
                .apply("CreateRanges", Create
                        .of(ranges).withCoder(SerializableCoder.of(TiDBUtil.Range.class)))
                .apply("Reshuffle", Reshuffle.viaRandomKey());

        final PCollection<GenericRecord> records = rangeCollection
                .apply("ReadTable", ParDo.of(new ReadDoFn(parameters, keyField, snapshotTSO, outputAvroSchema.toString())))
                .setCoder(AvroCoder.of(outputAvroSchema));

        final PCollection<MElement> elements = records
                .apply("Convert", ParDo.of(new ConvertDoFn(getTimestampAttribute(), getTimestampDefault())));

        return MCollectionTuple
                .of(elements, Schema.of(outputAvroSchema));
    }

    private static List<TiDBUtil.Range> createRanges(
            final Connection connection,
            final Parameters parameters,
            final TiDBUtil.SplitKey splitKey) throws SQLException {

        if(splitKey == null) {
            return List.of(TiDBUtil.Range.full());
        }

        // Strategy A: split along TiKV region boundaries
        final List<TiDBUtil.Range> regionRanges = TiDBUtil
                .createRegionRanges(connection, parameters.table, splitKey.field());
        if(regionRanges != null) {
            LOG.info("TiDB table: {} split into {} region ranges (key: {})",
                    parameters.table, regionRanges.size(), splitKey.field());
            return regionRanges;
        }

        // Strategy B: numeric MIN/MAX even split
        if(splitKey.integer()) {
            final long estimatedRows = TiDBUtil.getEstimatedRowCount(connection, parameters.table);
            final List<TiDBUtil.Range> minMaxRanges = TiDBUtil.createMinMaxRanges(
                    connection, parameters.table, splitKey.field(),
                    parameters.where, estimatedRows, parameters.splitSize);
            LOG.info("TiDB table: {} split into {} min/max ranges (key: {}, estimatedRows: {})",
                    parameters.table, minMaxRanges.size(), splitKey.field(), estimatedRows);
            return minMaxRanges;
        }

        // Strategy C: whole table
        LOG.info("TiDB table: {} read as a whole (no splittable strategy available)", parameters.table);
        return List.of(TiDBUtil.Range.full());
    }

    private static class ReadDoFn extends DoFn<TiDBUtil.Range, GenericRecord> {

        private final String url;
        private final String user;
        private final String password;
        private final String table;
        private final String select;
        private final String where;
        private final String keyField;
        private final String snapshotTSO;
        private final int fetchSize;
        private final String schemaString;

        private transient JdbcUtil.CloseableDataSource dataSource;
        private transient Connection connection;
        private transient org.apache.avro.Schema schema;

        ReadDoFn(
                final Parameters parameters,
                final String keyField,
                final String snapshotTSO,
                final String schemaString) {

            this.url = parameters.url;
            this.user = parameters.user;
            this.password = parameters.password;
            this.table = parameters.table;
            this.select = parameters.select;
            this.where = parameters.where;
            this.keyField = keyField;
            this.snapshotTSO = snapshotTSO;
            this.fetchSize = parameters.fetchSize;
            this.schemaString = schemaString;
        }

        @Setup
        public void setup() throws SQLException {
            this.dataSource = JdbcUtil.createDataSource(TiDBUtil.DRIVER, url, user, password, true);
            this.connection = dataSource.getConnection();
            this.connection.setAutoCommit(true);
            if(snapshotTSO != null) {
                // every worker reads the same MVCC version without taking a lock
                TiDBUtil.setSnapshot(connection, snapshotTSO);
            }
            TiDBUtil.enablePaging(connection);
            this.schema = new org.apache.avro.Schema.Parser().parse(schemaString);
        }

        @Teardown
        public void teardown() throws SQLException, IOException {
            if(this.connection != null) {
                try {
                    this.connection.close();
                } finally {
                    this.connection = null;
                }
            }
            if(this.dataSource != null) {
                try {
                    this.dataSource.close();
                } finally {
                    this.dataSource = null;
                }
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws SQLException, IOException {
            final TiDBUtil.Range range = c.element();
            final String condition = range.createCondition(keyField);
            final String query = TiDBUtil.createQuery(table, select, where, condition);
            LOG.info("Start read [{}]", query);

            long count = 0;
            final Instant start = Instant.now();
            try(final Statement statement = connection
                    .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                statement.setFetchSize(fetchSize);
                try(final ResultSet resultSet = statement.executeQuery(query)) {
                    while(resultSet.next()) {
                        c.output(ResultSetToRecordConverter.convert(schema, resultSet));
                        count++;
                    }
                }
            }
            final long time = Instant.now().getMillis() - start.getMillis();
            LOG.info("Finished read [{}], total count: [{}], took [{}] millisec", query, count, time);
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