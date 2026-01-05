package com.mercari.solution.util.domain.db.split;

import com.mercari.solution.module.Module;
import com.mercari.solution.util.domain.db.Properties;
import com.mercari.solution.util.gcp.JdbcUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Seeker {

    private static final Logger LOG = LoggerFactory.getLogger(Seeker.class);

    private final String table;
    private final List<String> seekFields;
    private final String select;
    private final Integer fetchSize;
    private final Schema outputSchema;
    private final Boolean failFast;

    public Seeker(
            final Properties properties,
            final String outputSchemaJson,
            final Boolean failFast) {
        this.table = properties.table;
        this.seekFields = properties.seekFields;
        this.select = properties.select;
        this.fetchSize = properties.fetchSize;
        this.outputSchema = AvroSchemaUtil.convertSchema(outputSchemaJson);
        this.failFast = failFast;
    }

    public static Seeker create(
            final Properties properties,
            final String outputSchemaJson,
            final Boolean failFast) {
        return new Seeker(properties, outputSchemaJson, failFast);
    }

    public RestrictionRecord seek(
            final Connection connection,
            final RestrictionTracker<IndexRange, IndexPosition> tracker,
            final DoFn.OutputReceiver<GenericRecord> output,
            final DoFn.OutputReceiver<BadRecord> badRecordOutput) throws SQLException {

        final Instant begin = Instant.now();

        final IndexRange restriction = tracker.currentRestriction();
        if(restriction == null) {
            throw new IllegalStateException("current restriction is null");
        }

        final IndexPosition originalTo = tracker.currentRestriction().getTo().copy();

        final String name = createName();
        final IndexPosition startPosition = restriction.getFrom().copy();

        long proceededCount = 0L;
        long queryCount = 0;
        int lastFetchCount = fetchSize;
        while(lastFetchCount == fetchSize) {
            if (!tracker.tryClaim(startPosition)) {
                final long durationMillis = Instant.now().getMillis() - begin.getMillis();
                LOG.info(String.format("[%s] tryClaim is false. next startPos: [%s]",  name, startPosition));
                final IndexRange proceeded = IndexRange.of(restriction.getFrom().copy(), startPosition.copy(false), restriction.getPrevFrom());
                return RestrictionRecord.of(proceeded, proceededCount, durationMillis);
            }

            final String preparedStatementQuery = createStatementQuery(
                    startPosition,
                    tracker.currentRestriction().getTo());

            try (final PreparedStatement statement = connection.prepareStatement(
                    preparedStatementQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                statement.setFetchSize(fetchSize);
                setStatementParameters(statement, outputSchema, startPosition, tracker.currentRestriction().getTo());
                final String rangeString = toString(startPosition, tracker.currentRestriction().getTo());

                int count = 0;
                final Instant start = Instant.now();
                try(final ResultSet resultSet = statement.executeQuery()) {
                    while(resultSet.next()) {
                        final GenericRecord record = ResultSetToRecordConverter.convert(outputSchema, resultSet);
                        output.output(record);
                        count++;
                        if(resultSet.isLast()) {
                            final List<IndexOffset> latestOffsets = new ArrayList<>();
                            for (final String seekFieldName : seekFields) {
                                final Schema.Field seekField = outputSchema.getField(seekFieldName);
                                final Schema fieldSchema = AvroSchemaUtil.unnestUnion(seekField.schema());
                                final Object fieldValue = record.get(seekField.name());
                                final boolean isCaseSensitive = Boolean.parseBoolean(seekField.getProp("isCaseSensitive"));
                                latestOffsets.add(IndexOffset.of(seekField.name(), fieldSchema.getType(), true, fieldValue, isCaseSensitive));
                            }
                            startPosition.setIsOpen(true);
                            startPosition.setOffsets(latestOffsets);
                        }
                    }
                } catch (final Throwable e) {
                    final BadRecord badRecord = Module.processError("Failed to seeking for restriction", restriction.toMap(), e, failFast);
                    badRecordOutput.output(badRecord);
                }
                lastFetchCount = count;
                proceededCount += count;
                queryCount += 1;
                final long time = Instant.now().getMillis() - start.getMillis();
                LOG.info(String.format(
                        "[%s] Finished to read range [%s], total count: [%d], [%d] millisec. next startPos: [%s]",
                        name, rangeString, count, time, startPosition));
            } catch (final Throwable e) {
                final BadRecord badRecord = Module.processError("Failed to seeking for restriction", restriction.toMap(), e, failFast);
                badRecordOutput.output(badRecord);
            }
        }

        final long durationMillis = Instant.now().getMillis() - begin.getMillis();

        final IndexRange proceeded = IndexRange.of(restriction.getFrom().copy(), startPosition.copy(false), restriction.getPrevFrom());
        final RestrictionRecord record = RestrictionRecord.of(proceeded, proceededCount, durationMillis);
        if(!proceeded.getTo().isSame(originalTo)) {
            record.setPrevTo(originalTo);
        }
        return record;
    }

    public Schema convertSeekSchema() {
        final SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder
                .record("root")
                .prop("table", table)
                .prop("primaryKeys", String.join(",", seekFields))
                .fields();
        for(final String seekField : seekFields) {
            final Schema seekFieldSchema = outputSchema.getField(seekField).schema();
            schemaBuilder.name(seekField).type(seekFieldSchema).withDefault(null);
        }
        return schemaBuilder.endRecord();
    }

    private String createStatementQuery(
            final IndexPosition startPosition,
            final IndexPosition stopPosition) {

        final List<String> startFields = seekFields.subList(0, startPosition.getOffsets().size());
        final List<String> stopFields = seekFields.subList(0, stopPosition.getOffsets().size());
        final String t1 = String.join(",", startFields);
        final String t2 = String.join(",", stopFields);
        final String v1 = String.join(",", startFields.stream().map(a -> "?").toList());
        final String v2 = String.join(",", stopFields.stream().map(a -> "?").toList());
        final String o1 = ">" + (startPosition.getIsOpen() ? "" : "=");
        final String o2 = "<" + (stopPosition.getIsOpen() ? "" : "=");

        return String.format(
                """
                SELECT %s FROM %s
                WHERE
                  (%s) %s (%s)
                  AND (%s) %S (%s)
                ORDER BY %s LIMIT %d
                """,
                select, table,
                t1, o1, v1,
                t2, o2, v2,
                String.join(", ", seekFields), fetchSize);
    }

    private static void setStatementParameters(
            final PreparedStatement statement,
            final Schema outputSchema,
            final IndexPosition from,
            final IndexPosition to) throws SQLException {

        int paramIndex = setStatementParameters(statement, outputSchema, from.getOffsets(), 1);
        setStatementParameters(statement, outputSchema, to.getOffsets(), paramIndex);
    }

    private static int setStatementParameters(
            final PreparedStatement statement,
            final Schema outputSchema,
            final List<IndexOffset> offsets,
            final int paramIndexOffset) throws SQLException {

        int paramIndex = paramIndexOffset;
        for(final IndexOffset offset : offsets) {
            final Object value = offset.getValue();
            final Schema.Field field = Optional
                    .ofNullable(outputSchema.getField(offset.getFieldName()))
                    .orElseGet(() -> outputSchema.getField(offset.getFieldName().toLowerCase())); // For PostgreSQL
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            JdbcUtil.setStatement(statement, paramIndex, fieldSchema, value);
            if(value != null) {
                paramIndex = paramIndex + 1;
            }
        }
        return paramIndex;
    }

    private static String toString(
            final IndexPosition from,
            final IndexPosition to) {

        var f = from.getOffsets().stream().map(IndexOffset::getAsString).toList();
        var t = to.getOffsets().stream().map(IndexOffset::getAsString).toList();
        String s1 = String.join(",", f);
        String s2 = String.join(",", t);
        return "from: [" + s1 + "], to: [" + s2 + "]";
    }

    private static String createName() {
        return ProcessHandle.current().pid() + "/" + Thread.currentThread().getName();
    }

}
