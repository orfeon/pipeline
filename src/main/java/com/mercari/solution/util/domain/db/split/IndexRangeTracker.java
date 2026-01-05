package com.mercari.solution.util.domain.db.split;

import com.mercari.solution.util.domain.db.Properties;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class IndexRangeTracker
        extends RestrictionTracker<IndexRange, IndexPosition>
        implements RestrictionTracker.HasProgress {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRangeTracker.class);

    private final boolean enableSplit;
    protected IndexRange range;
    protected IndexPosition lastClaimedPosition = null;
    protected IndexPosition lastAttemptedPosition = null;

    private final Properties properties;
    protected final String outputSchemaJson;

    protected boolean completed;

    public IndexRangeTracker(
            final boolean enableSplit,
            final Properties properties,
            final IndexRange range,
            final String outputSchemaJson) {

        this.enableSplit = enableSplit;
        this.range = range;
        this.properties = properties;
        this.outputSchemaJson = outputSchemaJson;
    }

    public static IndexRangeTracker tracker(
            final boolean enableSplit,
            final IndexRange range,
            final Properties properties,
            final String outputSchemaJson) {

        return new IndexRangeTracker(
                enableSplit, properties, range, outputSchemaJson);
    }

    @Override
    public boolean tryClaim(final IndexPosition position) {
        if(position == null) {
            throw new IllegalStateException("tryClaim position is null");
        }
        if(lastAttemptedPosition != null && lastAttemptedPosition.isOverTo(position, properties.charCollation)) {
            LOG.error("lastAttemptedPosition: {} is over position: {}", lastAttemptedPosition, position);
        }
        this.lastAttemptedPosition = position.copy();
        if(position.isOverTo(this.range.getTo(), properties.charCollation)) {
            LOG.info("Position: {} is over to end: {}", position, this.range.getTo());
            return false;
        }
        this.lastClaimedPosition = position.copy();
        return true;
    }

    @Override
    public IndexRange currentRestriction() {
        return range;
    }

    @Override
    public SplitResult<IndexRange> trySplit(double fractionOfRemainder) {
        if(!enableSplit) {
            LOG.info("Not split restriction: {}", this.range);
            return null;
        }
        if(fractionOfRemainder == 0) {
            LOG.info("fractionOfRemainder is zero");
            return null;
        }

        final IndexRange currentRange = IndexRange.of(
                Optional.ofNullable(lastAttemptedPosition).orElse(range.getFrom()).copy(),
                range.getTo().copy(),
                range.getPrevFrom());
        LOG.info("Try split restriction: {}, with fraction: {}, base restriction: {}",
                currentRange, fractionOfRemainder, range);
        try {
            final Schema outputSchema = AvroSchemaUtil.convertSchema(outputSchemaJson);
            final Schema seekSchema = convertSeekSchema(outputSchema);

            List<IndexRange> newRanges;
            Class.forName(properties.driver);
            try (final Connection connection = DriverManager
                    .getConnection(properties.url, properties.user, properties.password)) {
                connection.setReadOnly(true);
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                if (!isSplittable(currentRange)) {
                    newRanges = Splitter.divide(connection, properties.table, seekSchema, currentRange,2, properties.charCollation);
                } else {
                    newRanges = Splitter.split(connection, properties.table, properties.seekFields, currentRange, seekSchema);
                    if(newRanges.size() == 1) {
                        newRanges = Splitter.divide(connection, properties.table, seekSchema, newRanges.getFirst(),2, properties.charCollation);
                    }
                }

                if(newRanges.isEmpty()) {
                    LOG.info("Failed to split restriction: {}", currentRange);
                    return null;
                }
            }

            if(newRanges.size() == 2) {
                LOG.info("Succeeded to split restriction: {}. new restriction: {}", currentRange, newRanges);
                final IndexRange firstRange = newRanges.getFirst().copy();
                final IndexRange lastRange = newRanges.getLast().copy();
                firstRange.setFrom(this.range.getFrom().copy());
                lastRange.setPrevFrom(Optional
                        .ofNullable(this.range.getPrevFrom())
                        .map(IndexPosition::copy)
                        .orElse(null));
                this.range = firstRange;
                return SplitResult.of(this.range, lastRange);
            }
            LOG.info("Failed to divide restriction: {}", currentRange);
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Not found driver class: " + properties.driver, e);
        }
    }

    @Override
    public void checkDone() throws IllegalStateException {
        if(completed) {
            LOG.info("Finished splittable function for range: {}", this.range);
            return;
        }
        if(lastAttemptedPosition == null) {
            throw new IllegalStateException("Last attempted index offset should not be null. No work was claimed in range.");
        }
    }

    @Override
    public IsBounded isBounded() {
        return IsBounded.BOUNDED;
    }

    @Override
    public Progress getProgress() {
        return Progress.from(0.8, 0.2);
    }

    public Schema convertSeekSchema(final Schema tableSchema) {
        final SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder
                .record("root")
                .prop("table", properties.table)
                .prop("primaryKeys", String.join(",", properties.seekFields))
                .fields();
        for(final String seekField : properties.seekFields) {
            final Schema seekFieldSchema = tableSchema.getField(seekField).schema();
            schemaBuilder.name(seekField).type(seekFieldSchema).withDefault(null);
        }
        return schemaBuilder.endRecord();
    }

    public boolean isSplittable(final IndexRange range) {
        return isSplittable(properties.seekFields, range.getFrom(), range.getTo());
    }

    private static boolean isSplittable(
            final List<String> seekFields,
            final IndexPosition pos1,
            final IndexPosition pos2) {

        if(seekFields == null || seekFields.isEmpty()) {
            throw new IllegalArgumentException("");
        }
        if(pos1 == null || pos2 == null) {
            throw new IllegalArgumentException("");
        }
        if(seekFields.size() == 1) {
            return false;
        }
        if(Math.abs(pos1.getOffsets().size() - pos2.getOffsets().size()) > 1) {
            return false;
        }
        if(seekFields.size() != Math.max(pos1.getOffsets().size(), pos2.getOffsets().size())) {
            return false;
        }
        for(int i=0; i<seekFields.size() - 1; i++) {
            final Object val1 = pos1.getOffsets().get(i).getValue();
            final Object val2 = pos2.getOffsets().get(i).getValue();
            if((val1 == null && val2 != null) || (val1 != null && val2 == null)) {
                return true;
            } else if(val1 != null && !val1.equals(val2)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDividable(
            final List<String> seekFields,
            final IndexPosition pos1,
            final IndexPosition pos2) {

        if(seekFields == null || seekFields.isEmpty()) {
            throw new IllegalArgumentException("");
        }
        if(pos1 == null || pos2 == null) {
            throw new IllegalArgumentException("");
        }
        if(seekFields.size() == 1) {
            return true;
        }
        if(Math.abs(pos1.getOffsets().size() - pos2.getOffsets().size()) > 1) {
            return false;
        }
        if(seekFields.size() != Math.max(pos1.getOffsets().size(), pos2.getOffsets().size())) {
            return false;
        }
        for(int i=0; i<seekFields.size() - 1; i++) {
            final Object val1 = pos1.getOffsets().get(i).getValue();
            final Object val2 = pos2.getOffsets().get(i).getValue();
            if((val1 == null && val2 != null) || (val1 != null && val2 == null)) {
                return true;
            } else if(val1 != null && !val1.equals(val2)) {
                return true;
            }
        }
        return false;
    }

}
