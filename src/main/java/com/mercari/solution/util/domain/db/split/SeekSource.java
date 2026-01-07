package com.mercari.solution.util.domain.db.split;

import com.mercari.solution.module.Logging;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Module;
import com.mercari.solution.util.domain.db.CharCollation;
import com.mercari.solution.util.domain.db.Properties;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SeekSource extends PTransform<PBegin, PCollectionTuple> {

    private static final Logger LOG = LoggerFactory.getLogger(SeekSource.class);
    private static final Counter splitCounter = Metrics.counter("seek_source", "split_count");

    private final Properties properties;
    private final String outputSchemaJson;
    private final Boolean deduplicate;

    private final Boolean enableInitialSplit;
    private final Integer initialSplitSize;

    private final List<Logging> logs;
    private final Boolean failFast;

    public final TupleTag<GenericRecord> outputTag;
    public final TupleTag<RestrictionRecord> restrictionTag;
    public final TupleTag<BadRecord> failureTag;

    SeekSource(
            final Properties properties,
            final Schema outputSchema,
            final List<Logging> logs,
            final Boolean failFast,
            final Boolean enableInitialSplit,
            final Integer initialSplitSize) {

        this.properties = properties;
        this.outputSchemaJson = outputSchema.toString();
        this.deduplicate = false;
        this.logs = logs;
        this.failFast = failFast;
        this.enableInitialSplit = enableInitialSplit;
        this.initialSplitSize = initialSplitSize;

        this.outputTag = new TupleTag<>() {};
        this.restrictionTag = new TupleTag<>() {};
        this.failureTag = new TupleTag<>() {};
    }

    public static SeekSource of(
            final String table,
            final String select,
            List<String> seekFields,
            final Integer fetchSize,
            final String driver,
            final String url,
            final String user,
            final String password,
            final Boolean enableSplit,
            final Boolean enableInitialSplit,
            final Integer initialSplitSize,
            final Map<String,String> defaultCollations,
            final Map<String, String> dataSourceProperties,
            final List<Logging> logs,
            final Boolean failFast) {

        final Schema outputSchema;
        final CharCollation charCollation;
        try(final HikariDataSource source = Properties.createDataSource(url, user, password, dataSourceProperties);
            final Connection connection = source.getConnection()) {

            outputSchema = createOutputSchema(connection, table, select);
            if(seekFields == null || seekFields.isEmpty()) {
                seekFields = JdbcUtil.getPrimaryKeyNames(connection, null, null, table);
            }
            charCollation = CharCollation.of(connection, table, seekFields, defaultCollations);
        } catch (final SQLException e) {
            throw new IllegalArgumentException("Failed to initialize jdbc seek source with SQLException: " + e.getMessage(), e);
        }

        /*
        try {
            Class.forName(driver);
            try (final Connection connection = DriverManager
                    .getConnection(url, user, password)) {
                connection.setReadOnly(true);
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                outputSchema = createOutputSchema(connection, table, select);
                if(seekFields == null || seekFields.isEmpty()) {
                    seekFields = JdbcUtil.getPrimaryKeyNames(connection, null, null, table);
                }
                charCollation = CharCollation.of(connection, table, seekFields, defaultCollations);
            } catch (final SQLException e) {
                throw new IllegalArgumentException("Failed to initialize jdbc seek source with SQLException: " + e.getMessage(), e);
            }
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to initialize jdbc seek source with ClassNotFoundException: " + e.getMessage(), e);
        }

         */

        LOG.info("seek char collations: {}", charCollation);

        final Properties properties = new Properties(
                table, select, seekFields, fetchSize, enableSplit,
                driver, url, user, password, charCollation, dataSourceProperties);
        return new SeekSource(properties, outputSchema, logs, failFast, enableInitialSplit, initialSplitSize);
    }

    public static Schema createOutputSchema(
            final Connection connection,
            final String table,
            final String select) throws SQLException {

        final String query = String.format("""
                SELECT %s
                FROM %s
                LIMIT 1
                """, select, table);
        return JdbcUtil.createAvroSchemaFromQuery(connection, query, List.of());
    }

    public Schema getOutputAvroSchema() {
        return AvroSchemaUtil.convertSchema(outputSchemaJson);
    }

    @Override
    public PCollectionTuple expand(final PBegin begin) {
        final List<TupleTag<?>> tags = List.of(restrictionTag, failureTag);
        final PCollectionTuple results = begin
                .apply("SupplyTable", Create
                        .of(properties.table)
                        .withCoder(StringUtf8Coder.of()))
                .apply("SeekTable", ParDo
                        .of(new SeekDoFn(
                                properties, outputSchemaJson, enableInitialSplit, initialSplitSize,
                                logs, failFast, outputTag, restrictionTag, failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(tags)));
        final Schema outputSchema = AvroSchemaUtil.convertSchema(outputSchemaJson);
        PCollection<GenericRecord> output = results
                .get(outputTag)
                .setCoder(AvroCoder.of(outputSchema));

        if(deduplicate) {
            output = output
                    .apply("WithKey", ParDo.of(new WithKeyDoFn(properties.seekFields)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), output.getCoder()))
                    .apply("Deduplicate", Combine.perKey(new DeduplicateCombineFn()))
                    .apply(Values.create());
        }

        return PCollectionTuple
                .of(outputTag, output)
                .and(restrictionTag, results.get(restrictionTag))
                .and(failureTag, results.get(failureTag));
    }

    @DoFn.BoundedPerElement
    public static class SeekDoFn extends DoFn<String, GenericRecord> {

        private static final Logger LOG = LoggerFactory.getLogger(SeekDoFn.class);

        private final Properties properties;
        private final String outputSchemaString;
        private final Boolean enableInitialSplit;
        private final Integer initialSplitSize;

        private final TupleTag<GenericRecord> outputTag;
        private final TupleTag<RestrictionRecord> restrictionTag;
        private final TupleTag<BadRecord> failureTag;

        private final Map<String, Logging> logs;
        private final boolean failFast;

        private transient Seeker seeker;
        private transient Schema seekSchema;

        private transient HikariDataSource dataSource;
        private transient String processName;

        public SeekDoFn(
                final Properties properties,
                final String outputSchemaString,
                final Boolean enableInitialSplit,
                final Integer initialSplitSize,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<GenericRecord> outputTag,
                final TupleTag<RestrictionRecord> restrictionTag,
                final TupleTag<BadRecord> failureTag) {

            this.properties = properties;
            this.outputSchemaString = outputSchemaString;
            this.enableInitialSplit = enableInitialSplit;
            this.initialSplitSize = initialSplitSize;

            this.logs = Logging.map(logs);
            this.failFast = failFast;

            this.outputTag = outputTag;
            this.restrictionTag = restrictionTag;
            this.failureTag = failureTag;
        }

        @Setup
        public void setup() throws SQLException {
            this.dataSource = Properties.createDataSource(properties);
            this.seeker = Seeker.create(properties, outputSchemaString, failFast);
            this.seekSchema = seeker.convertSeekSchema();
            this.processName = createProcessName();
            LOG.info("setup process {}", processName);
        }

        @Teardown
        public void teardown() throws SQLException {

        }

        @ProcessElement
        public void processElement(
                final RestrictionTracker<IndexRange, IndexPosition> tracker,
                final MultiOutputReceiver outputs) throws SQLException, IOException, ClassNotFoundException {

            LOG.info("start process {} for restriction: {}", processName, tracker.currentRestriction());
            try(final Connection connection = dataSource.getConnection()) {
                final RestrictionRecord record = seeker.seek(connection,
                        tracker,
                        outputs.get(outputTag),
                        outputs.get(failureTag));
                outputs.get(restrictionTag).output(record);
                LOG.info("finished process {} for restriction: {}", processName, tracker.currentRestriction());
            } catch (final Throwable e) {
                final IndexRange restriction = tracker.currentRestriction();
                if(restriction == null) {
                    return;
                }
                final BadRecord badRecord = Module.processError("Failed to seeking for restriction", restriction.toMap(), e, failFast);
                outputs.get(failureTag).output(badRecord);
            }
        }

        @GetInitialRestriction
        public IndexRange getInitialRestriction(@Element String input) throws SQLException, IOException {
            try(final Connection connection = dataSource.getConnection()) {
                final IndexRange initialIndexRange = Splitter
                        .createInitialIndexRange(connection, properties.table, properties.seekFields);
                LOG.info("Initial restriction: {}", initialIndexRange);
                return initialIndexRange;
            }
        }

        @GetRestrictionCoder
        public Coder<IndexRange> getRestrictionCoder() {
            return AvroCoder.of(IndexRange.class);
        }

        @SplitRestriction
        public void splitRestriction(
                @Element String input,
                @Restriction IndexRange restriction,
                OutputReceiver<IndexRange> out) {

            if(enableInitialSplit) {
                try(final Connection connection = dataSource.getConnection()) {
                    final List<IndexRange> ranges = Splitter.split(
                            connection, properties.table, properties.seekFields, restriction,
                            seekSchema, initialSplitSize, properties.charCollation);
                    LOG.info("Batch split restriction: {}. size: {} for batch mode", restriction, ranges.size());
                    if(ranges.size() < 2) {
                        out.output(restriction);
                    } else {
                        int i=0;
                        for(final IndexRange range : ranges) {
                            final double ratio = restriction.getRatio() / ranges.size();
                            range.setRatio(ratio);
                            out.output(range);
                            LOG.info("Batch initial restriction {}: {}", i, range);
                            i++;
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (Throwable e) {
                    throw new RuntimeException("failed to schema: " + outputSchemaString, e);
                }
            } else {
                LOG.info("Not split restriction: {} for batch mode", restriction);
                out.output(restriction);
            }
        }

        @NewTracker
        public RestrictionTracker<IndexRange, IndexPosition> newTracker(
                @Element String element,
                @Restriction IndexRange restriction) {

            LOG.info("newTracker from restriction: {}", restriction);
            return IndexRangeTracker.tracker(
                    properties.enableSplit, restriction, properties, outputSchemaString);
        }

        //@GetSize
        public double getSize(
                @Restriction IndexRange restriction) throws Exception {
            return 0.5D;//getRecordCountAndSize(file, restriction).getSize();
        }

        private static String createProcessName() {
            return ProcessHandle.current().pid() + "/" + Thread.currentThread().getName();
        }

    }

    public static class FormatRestrictionDoFn extends DoFn<RestrictionRecord, MElement> {

        private transient Schema schema;

        @Setup
        public void setup() {
            this.schema = RestrictionRecord.createAvroSchema();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final RestrictionRecord record = c.element();
            if(record == null) {
                return;
            }
            MElement element = null;
            try {
                element = record.toElement(schema, Instant.now());
                c.output(element);
            } catch (final Throwable e) {
                LOG.error("failed to element: {}, for schema: {}", element, schema);
            }
        }

    }

    public static class WithKeyDoFn extends DoFn<GenericRecord,KV<String,GenericRecord>> {

        private final List<String> seekFields;

        WithKeyDoFn(final List<String> seekFields) {
            this.seekFields = seekFields;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final GenericRecord input = c.element();
            if(input == null) {
                return;
            }

            final List<String> keys = new ArrayList<>();
            for(final String seekField : seekFields) {
                final String key = AvroSchemaUtil.getAsString(input, seekField);
                keys.add(key);
            }
            final String output = String.join("#", keys);
            c.output(KV.of(output, input));
        }

    }

    public static class DeduplicateCombineFn extends Combine.CombineFn<GenericRecord, List<GenericRecord>, GenericRecord> {

        @Override
        public List<GenericRecord> createAccumulator() {
            return List.of();
        }

        @Override
        public List<GenericRecord> addInput(List<GenericRecord> accumulator, GenericRecord input) {
            return List.of(input);
        }

        @Override
        public List<GenericRecord> mergeAccumulators(
                @UnknownKeyFor @NonNull @Initialized Iterable<List<GenericRecord>> accumulators) {

            for(final List<GenericRecord> accumulator : accumulators) {
                if(accumulator == null || accumulator.isEmpty()) {
                    continue;
                }
                return List.of(accumulator.getFirst());
            }
            return List.of();
        }

        @Override
        public GenericRecord extractOutput(List<GenericRecord> accumulator) {
            if(accumulator == null || accumulator.isEmpty()) {
                return null;
            }
            return accumulator.getFirst();
        }
    }

}
