package com.mercari.solution.util.domain.db.split;

import com.mercari.solution.util.domain.db.CharCollation;
import com.mercari.solution.util.gcp.JdbcUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Splitter {

    private static final Logger LOG = LoggerFactory.getLogger(Splitter.class);

    public enum Operator implements Serializable {
        GREATER(">"),
        LESSER("<"),
        EQUAL("=");

        private final String name;

        public String getName() {
            return this.name;
        }

        public String getName(final boolean ascending) {
            if(ascending) {
                return this.getName();
            } else {
                return this.reverse().getName();
            }
        }

        Operator(String name) {
            this.name = name;
        }

        public Operator reverse() {
            if(GREATER.equals(this)) {
                return LESSER;
            } else if(LESSER.equals(this)) {
                return GREATER;
            } else {
                return this;
            }
        }
    }

    public static List<IndexRange> split(
            final Connection connection,
            final String table,
            final List<String> seekFields,
            final IndexRange range,
            final Schema seekSchema,
            final int size,
            final CharCollation charCollation) {

        final List<IndexRange> splitRanges = split(connection, table, seekFields, range, seekSchema);
        final List<IndexRange> results = new ArrayList<>();
        for (final IndexRange splitRange : splitRanges) {
            if (!isSplittable(splitRange, seekFields)) {
                final List<IndexRange> result = divide(connection, table, seekSchema, splitRange, size, charCollation);
                results.addAll(result);
            } else {
                final List<IndexRange> result = split(connection, table, seekFields, splitRange, seekSchema);
                results.addAll(result);
            }
        }
        return results;
    }

    public static List<IndexRange> divide(
            final Connection connection,
            final String table,
            final Schema seekSchema,
            final IndexRange range,
            final int splitNum,
            final CharCollation charCollation) {

        final IndexPosition from;
        if(range.getFrom().getOffsets().size() < seekSchema.getFields().size()) {
            from = nextPosition(connection, range.getFrom(), table, seekSchema).copy(false);
        } else {
            from = range.getFrom().copy();
        }

        final IndexPosition to;
        if(range.getTo().getOffsets().size() < seekSchema.getFields().size()) {
            to = prevPosition(connection, range.getTo(), table, seekSchema).copy(false);
        } else {
            to = range.getTo().copy();
        }

        final IndexRange sourceRange = IndexRange.of(from, to);

        List<IndexRange> ranges;
        try {
            ranges = splitIndexRange(sourceRange, splitNum, charCollation);
        } catch (Throwable e) {
            LOG.error("error. failed range: {}, source range: {} message: {}", range, sourceRange, e.getMessage());
            ranges = splitIndexRange(range, splitNum, charCollation);
        }

        if(ranges.size() > 1) {
            ranges.getFirst().getFrom().setIsOpen(sourceRange.getFrom().getIsOpen());
            ranges.getLast().getTo().setIsOpen(sourceRange.getTo().getIsOpen());
        }
        return ranges;
    }

    public static List<IndexRange> divide(
            final IndexRange range,
            final int splitNum,
            final CharCollation charCollation) {

        final List<IndexRange> ranges = splitIndexRange(range, splitNum, charCollation);
        if(ranges.size() > 1) {
            ranges.getFirst().getFrom().setIsOpen(range.getFrom().getIsOpen());
            ranges.getLast().getTo().setIsOpen(range.getTo().getIsOpen());
        }
        return ranges;
    }

    public static List<IndexRange> split(
            final Connection connection,
            final String table,
            final List<String> seekFields,
            final IndexRange range,
            final Schema tableSchema) {

        if(!isSplittable(range, seekFields)) {
            return List.of(range.copy());
        }
        final int ignoreCount = countIgnore(range, seekFields);

        final List<IndexRange> ranges = new ArrayList<>();
        final IndexPosition p1 = fill(connection, table, seekFields, tableSchema, range.getFrom(), false, false, ignoreCount);
        final IndexPosition prevPosition;
        if(p1 != null) {
            final IndexPosition f = range.getFrom().copy(range.getFrom().getIsOpen());
            final IndexPosition t = p1.copy();
            ranges.add(IndexRange.of(f, t));
            prevPosition = p1;
        } else {
            prevPosition = range.getFrom().copy();
        }

        final IndexPosition p2 = fill(connection, table, seekFields, tableSchema, prevPosition, true, true, ignoreCount);
        if(p2 != null) {
            final IndexPosition f = p2.copy(p2.getIsOpen());
            final IndexPosition t = range.getTo().copy();
            ranges.add(IndexRange.of(f, t));
        } else {
            LOG.error("error position: {}", prevPosition);
        }

        return ranges;
    }

    protected static IndexPosition fill(
            final Connection connection,
            final String table,
            final List<String> seekFields,
            final Schema tableSchema,
            final IndexPosition position,
            final boolean isStart,
            final boolean isOpen,
            final int ignoreLast) {

        final String toQuery = createPreparedStatementQuery(table, seekFields, position, isStart, isOpen, ignoreLast);
        try (final PreparedStatement statement = connection.prepareStatement(
                toQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            setStatementParameters(statement, position, seekFields, tableSchema, 1, ignoreLast);
            try(final ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()) {
                    final GenericRecord record = ResultSetToRecordConverter.convert(tableSchema, resultSet);
                    final List<IndexOffset> latestOffsets = new ArrayList<>();
                    for (final String fieldName : seekFields) {
                        final Schema.Field field = tableSchema.getField(fieldName);
                        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                        final Object fieldValue = record.get(field.name());
                        final boolean isCaseSensitive = Boolean.parseBoolean(field.getProp("isCaseSensitive"));
                        latestOffsets.add(IndexOffset.of(field.name(), fieldSchema.getType(), true, fieldValue, isCaseSensitive));
                    }
                    return IndexPosition.of(latestOffsets, false);
                }
            }
            return null;
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String createPreparedStatementQuery(
            final String table,
            final List<String> fields,
            final IndexPosition position,
            final boolean isStart,
            final boolean isOpen,
            final int ignoreLast) {

        final String select = String.join(", ", fields);
        final String fromWhere = createWhere(position, isStart, isOpen, ignoreLast);
        final String fromOrders = isStart ? select : String.join(" DESC, ", fields) + " DESC";
        return String.format("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1", select, table, fromWhere, fromOrders);
    }

    private static String createWhere(
            final IndexPosition position,
            final boolean isStart, final boolean isOpen, final int ignoreLast) {

        final int toIndex = Math.max(position.getOffsets().size() - ignoreLast, 1);
        final List<IndexOffset> offsets_ = position.getOffsets().subList(0, toIndex);
        final List<String> conditions = new ArrayList<>();
        for(int i=0; i<offsets_.size()-1; i++) {
            final IndexOffset offset = offsets_.get(i);
            if(offset.getValue() == null) {
                conditions.add(offset.getFieldName() + " IS NULL");
            } else {
                conditions.add(offset.getFieldName() + " = ?");
            }
        }

        final IndexOffset lastOffset = offsets_.getLast();
        if(lastOffset.getValue() == null) {
            conditions.add(lastOffset.getFieldName() + (isOpen ? " IS NOT NULL" : " IS NULL"));
        } else {
            final String operation = (isStart ? Operator.GREATER : Operator.LESSER).getName(lastOffset.getAscending()) + (isOpen ? "" : "=");
            conditions.add(lastOffset.getFieldName() + " " + operation + " ?");
        }

        return "(" + String.join(" AND ", conditions) + ")";
    }

    private static int setStatementParameters(
            final PreparedStatement statement,
            final IndexPosition position,
            final List<String> seekFields,
            final Schema tableSchema,
            int paramIndex,
            final int ignoreLast) throws SQLException {

        int max = seekFields.size() - ignoreLast;
        for(int index=0; index<max; index++) {
            final IndexOffset offset = position.getOffsets().get(index);
            final Object value = offset.getValue();
            final Schema.Field field = Optional
                    .ofNullable(tableSchema.getField(offset.getFieldName()))
                    .orElseGet(() -> tableSchema.getField(offset.getFieldName().toLowerCase())); // For PostgreSQL
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            JdbcUtil.setStatement(statement, paramIndex, fieldSchema, value);

            if(value != null) {
                paramIndex = paramIndex + 1;
            }
        }
        return paramIndex;
    }

    protected static IndexPosition nextPosition(
            final Connection connection,
            final IndexPosition position,
            final String table,
            final Schema seekSchema) {
        return nextPosition(connection, position, table, seekSchema, false);
    }

    protected static IndexPosition prevPosition(
            final Connection connection,
            final IndexPosition position,
            final String table,
            final Schema seekSchema) {
        return nextPosition(connection, position, table, seekSchema, true);
    }

    protected static IndexPosition nextPosition(
            final Connection connection,
            final IndexPosition position,
            final String table,
            final Schema seekSchema,
            boolean prev) {

        final String seekFieldStr = seekSchema.getFields()
                .stream()
                .map(Schema.Field::name)
                .collect(Collectors.joining(","));

        final List<String> offsetFields = new ArrayList<>();
        for(int i=0; i<position.getOffsets().size(); i++) {
            offsetFields.add(seekSchema.getFields().get(i).name());
        }

        final String offsetFieldsStr = String.join(",", offsetFields);
        final String offsetParamsStr = offsetFields.stream().map(f -> "?").collect(Collectors.joining(","));
        final String opStr = (prev ? "<" : ">") + (seekSchema.getFields().size() == position.getOffsets().size() && position.getIsOpen() ? "" : "=");
        final String orderStr = seekSchema.getFields()
                .stream()
                .map(f -> f.name() + (prev ? " DESC": ""))
                .collect(Collectors.joining(", "));

        final String query = String.format("""
            SELECT
              %s
            FROM
              %s
            WHERE
              (%s) %s (%s)
            ORDER BY
              %s
            LIMIT
              1
            """, seekFieldStr, table, offsetFieldsStr, opStr, offsetParamsStr, orderStr);

        try (final PreparedStatement statement = connection.prepareStatement(
                query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            for(int i=0; i<position.getOffsets().size(); i++) {
                final String seekFieldName = offsetFields.get(i);
                final Object value = position.getOffsets().get(i).getValue();
                JdbcUtil.setStatement(statement, i+1, seekSchema.getField(seekFieldName).schema(), value);
            }

            try(final ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()) {
                    final GenericRecord record = ResultSetToRecordConverter.convert(seekSchema, resultSet);
                    final List<IndexOffset> latestOffsets = new ArrayList<>();
                    for (final Schema.Field field : seekSchema.getFields()) {
                        final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                        final Object fieldValue = record.get(field.name());
                        final boolean isCaseSensitive = Boolean.parseBoolean(field.getProp("isCaseSensitive"));
                        latestOffsets.add(IndexOffset.of(field.name(), fieldSchema.getType(), true, fieldValue, isCaseSensitive));
                    }
                    return IndexPosition.of(latestOffsets, false);
                }
            }
            return null;
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int countIgnore(
            final IndexRange range,
            final List<String> seekFields) {
        int count = 1;
        for(int i=0; i<seekFields.size(); i++) {
            if(i >= Math.min(range.getFrom().getOffsets().size(), range.getTo().getOffsets().size())) {
                break;
            }
            final IndexOffset o1 = range.getFrom().getOffsets().get(i);
            final IndexOffset o2 = range.getTo().getOffsets().get(i);
            if(!o1.getValue().equals(o2)) {
                break;
            }
            count++;
        }

        return seekFields.size() - count;
    }

    public static IndexRange createInitialIndexRange(
            final Connection connection,
            final String table,
            final List<String> parameterFieldNames) throws SQLException, IOException {

        final String selectFieldNames = String.join(",", parameterFieldNames);
        final String firstFieldMinQuery = String.format(
                "SELECT %s FROM %s ORDER BY %s LIMIT 1",
                selectFieldNames, table, selectFieldNames);
        final String firstFieldMaxQuery = String.format(
                "SELECT %s FROM %s ORDER BY %s LIMIT 1",
                selectFieldNames, table, String.join(" DESC,", parameterFieldNames) + " DESC");

        // Set startOffset
        final List<IndexOffset> indexStartOffsets = new ArrayList<>();
        try(final PreparedStatement statement = connection
                .prepareStatement(firstFieldMinQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            if(statement.getMetaData() == null) {
                throw new IllegalArgumentException("Failed to get schema for query: " + firstFieldMinQuery);
            }

            try (final ResultSet resultSet = statement.executeQuery()) {
                if(!resultSet.next()) {
                    throw new IllegalStateException("");
                }
                final GenericRecord record = ResultSetToRecordConverter.convert(resultSet);
                for(final String fieldName : parameterFieldNames) {
                    Schema.Field field = record.getSchema().getField(fieldName);
                    if(field == null) {
                        // For PostgreSQL
                        field = record.getSchema().getField(fieldName.toLowerCase());
                    }

                    final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                    final Object value = record.get(field.name());
                    final String logicalType = Optional.ofNullable(fieldSchema.getLogicalType()).map(s -> s.getName().toLowerCase()).orElse(null);
                    final boolean isCaseSensitive = Boolean.parseBoolean(field.getProp("isCaseSensitive"));
                    indexStartOffsets.add(IndexOffset.of(field.name(), fieldSchema.getType(), true, value, logicalType, isCaseSensitive));
                }
            }
        }

        // Set stopOffset
        final List<IndexOffset> indexStopOffsets = new ArrayList<>();
        try(final PreparedStatement statement = connection
                .prepareStatement(firstFieldMaxQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            if(statement.getMetaData() == null) {
                throw new IllegalArgumentException("Failed to get schema for query: " + firstFieldMinQuery);
            }

            try (final ResultSet resultSet = statement.executeQuery()) {
                if(!resultSet.next()) {
                    throw new IllegalStateException();
                }
                final GenericRecord record = ResultSetToRecordConverter.convert(resultSet);

                for(final String fieldName : parameterFieldNames) {
                    Schema.Field field = record.getSchema().getField(fieldName);
                    if(field == null) {
                        // For PostgreSQL
                        field = record.getSchema().getField(fieldName.toLowerCase());
                    }

                    final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                    final Object value = record.get(field.name());
                    final boolean isCaseSensitive = Boolean.parseBoolean(field.getProp("isCaseSensitive"));
                    indexStopOffsets.add(IndexOffset.of(field.name(), fieldSchema.getType(), true, value, isCaseSensitive));
                }
            }
        }

        return IndexRange.of(
                IndexPosition.of(indexStartOffsets, false),
                IndexPosition.of(indexStopOffsets, false));
    }

    public static List<IndexRange> splitIndexRange(
            final IndexRange range,
            final int splitNum,
            final CharCollation charCollation) {

        return splitIndexRange(null, range.getFrom().getOffsets(), range.getTo().getOffsets(), splitNum, charCollation);
    }

    private static List<IndexRange> splitIndexRange(
            final List<IndexOffset> parents,
            final List<IndexOffset> from,
            final List<IndexOffset> to,
            final int splitNum,
            final CharCollation charCollation) {

        final List<IndexOffset> parentOffsets = new ArrayList<>();
        if(parents != null && !parents.isEmpty()) {
            parentOffsets.addAll(parents);
        }

        final IndexOffset firstFromOffset = from.getFirst();
        final IndexOffset firstToOffset = to.isEmpty() ? IndexOffset.empty(firstFromOffset, charCollation) : to.getFirst();

        if(firstFromOffset.compareTo(firstToOffset, charCollation) == 0) {
            if(from.size() > 1 || to.size() > 1) {
                final List<IndexOffset> f = from.subList(1, from.size());
                final List<IndexOffset> t = to.size() > 1 ? to.subList(1, to.size()) : List.of();
                parentOffsets.add(from.getFirst());
                return splitIndexRange(parentOffsets, f, t, splitNum, charCollation);
            } else {
                final List<IndexOffset> from_ = new ArrayList<>(parentOffsets);
                final List<IndexOffset> to_   = new ArrayList<>(parentOffsets);
                from_.addAll(from);
                to_.addAll(to);
                final List<IndexRange> results = new ArrayList<>();
                results.add(IndexRange.of(
                        IndexPosition.of(from_, true),
                        IndexPosition.of(to_, false)));
                return results;
            }
        }

        final List<IndexOffset> splitOffsets = splitOffset(firstFromOffset, firstToOffset, splitNum, charCollation);

        if(splitOffsets.size() > 1) {
            final List<IndexRange> results = new ArrayList<>();
            List<IndexOffset> nextFrom = new ArrayList<>(parentOffsets);
            nextFrom.addAll(from);
            for(final IndexOffset offset : splitOffsets) {
                final List<IndexOffset> nextTo = new ArrayList<>(parentOffsets);
                nextTo.add(offset);
                final IndexRange range = IndexRange.of(
                        IndexPosition.of(nextFrom, true),
                        IndexPosition.of(nextTo, false));
                results.add(range);
                List<IndexOffset> offsets = new ArrayList<>(parentOffsets);
                offsets.add(offset);
                nextFrom = offsets;
            }
            return results;
        } else {
            if(from.size() > 1) {
                final List<IndexOffset> f = from.subList(1, from.size());
                final List<IndexOffset> t = to.size() > 1 ? to.subList(1, to.size()) : List.of();
                parentOffsets.add(from.getFirst());
                return splitIndexRange(parentOffsets, f, t, splitNum, charCollation);
            } else {
                final List<IndexOffset> from_ = new ArrayList<>(parentOffsets);
                final List<IndexOffset> to_   = new ArrayList<>(parentOffsets);
                from_.addAll(from);
                to_.addAll(to);
                final List<IndexRange> results = new ArrayList<>();
                results.add(IndexRange.of(
                        IndexPosition.of(from_, true),
                        IndexPosition.of(to_, false)));
                return results;
            }
        }
    }

    private static List<IndexOffset> splitOffset(
            final IndexOffset fromOffset,
            final IndexOffset firstToOffset,
            final int splitNum,
            final CharCollation charCollation) {

        return switch (fromOffset.getFieldType()) {
            case BOOLEAN -> splitBoolean(fromOffset.getFieldName(), firstToOffset.getAscending());
            case INT -> splitInteger(fromOffset.getFieldName(),
                    fromOffset.getIntValue(), firstToOffset.getIntValue(),
                    fromOffset.getAscending(), splitNum);
            case LONG -> splitLong(fromOffset.getFieldName(),
                    fromOffset.getLongValue(), firstToOffset.getLongValue(),
                    fromOffset.getAscending(), splitNum);
            case FLOAT -> splitFloat(fromOffset.getFieldName(),
                    fromOffset.getFloatValue(), firstToOffset.getFloatValue(),
                    fromOffset.getAscending(), splitNum);
            case DOUBLE -> splitDouble(fromOffset.getFieldName(),
                    fromOffset.getDoubleValue(), firstToOffset.getDoubleValue(),
                    fromOffset.getAscending(), splitNum);
            case ENUM, STRING -> splitString(fromOffset.getFieldName(),
                    fromOffset.getStringValue(), firstToOffset.getStringValue(),
                    fromOffset.getAscending(), splitNum,
                    charCollation, 0);
            case FIXED, BYTES -> splitBytes(fromOffset.getFieldName(),
                    fromOffset.getBytesValue(), firstToOffset.getBytesValue(),
                    fromOffset.getAscending(), splitNum);
            default -> throw new IllegalArgumentException("Not supported range type: " + fromOffset.getFieldType());
        };
    }

    private static List<IndexOffset> splitBoolean(final String name, final boolean ascending) {
        final List<IndexOffset> results = new ArrayList<>();
        results.add(IndexOffset.of(name, Schema.Type.BOOLEAN, ascending, Boolean.FALSE));
        results.add(IndexOffset.of(name, Schema.Type.BOOLEAN, ascending, Boolean.TRUE));
        return results;
    }

    private static List<IndexOffset> splitInteger(final String name, Integer min, Integer max, final boolean ascending, final int splitNum) {
        final double boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, null));
            min = Integer.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, null));
            max = Integer.MAX_VALUE;
        }
        boundSize = Math.abs((max.doubleValue() / splitNum) - (min.doubleValue() / splitNum));

        int prev = min;
        for(int i=1; i<splitNum; i++) {
            int next = (int)Math.round(min + boundSize * i);
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.INT, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.INT, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitLong(final String name, Long min, Long max, final boolean ascending, final int splitNum) {
        final double boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, null));
            min = Long.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, null));
            max = Long.MAX_VALUE;
        }
        boundSize = Math.abs((max.doubleValue() / splitNum) - (min.doubleValue() / splitNum));

        long prev = min;
        for(int i=1; i<splitNum; i++) {
            long next = Math.round(min + boundSize * i);
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.LONG, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitFloat(final String name, Float min, Float max, final boolean ascending, final int splitNum) {
        final float boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, null));
            min = Float.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, null));
            max = Float.MAX_VALUE;
        }
        boundSize = Math.abs((max / splitNum) - (min / splitNum));

        float prev = min;
        for(int i=1; i<splitNum; i++) {
            float next = min + boundSize * i;
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.FLOAT, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitDouble(final String name, Double min, Double max, final boolean ascending, final int splitNum) {
        final double boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, null));
            min = Double.MIN_VALUE;
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, null));
            max = Double.MAX_VALUE;
        }
        boundSize = Math.abs((max / splitNum) - (min / splitNum));

        double prev = min;
        for(int i=1; i<splitNum; i++) {
            double next = (min + boundSize * i);
            if(prev == next || next >= max) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.DOUBLE, ascending, max));
        return results;
    }

    private static List<IndexOffset> splitNumeric(final String name, BigDecimal min, BigDecimal max, final boolean ascending, final int splitNum) {
        final BigDecimal boundSize;
        final List<IndexOffset> results = new ArrayList<>();
        if(min == null && max == null) {
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
            return results;
        } else if(min == null) {
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
            min = BigDecimal.valueOf(Double.MIN_VALUE);
        } else if(max == null) {
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
            max = BigDecimal.valueOf(Double.MAX_VALUE);
        }
        boundSize = max.subtract(min).divide(BigDecimal.valueOf(splitNum)).abs();

        BigDecimal prev = min;
        for(int i=1; i<splitNum; i++) {
            BigDecimal next = min.add((boundSize.multiply(BigDecimal.valueOf(i))));
            if(prev == next || next.subtract(max).doubleValue() >= 0D) {
                continue;
            }
            results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, next));
            prev = next;
        }
        results.add(IndexOffset.of(name, Schema.Type.BYTES, ascending, max));
        return results;
    }

    public static List<IndexOffset> splitBytes(final String name, ByteBuffer min, ByteBuffer max, final boolean ascending, final int splitNum) {
        if(min == null && max == null) {
            return Arrays.asList(IndexOffset.of(name, Schema.Type.BYTES, ascending, null));
        } else if(min == null) {
            byte[] bytes = new byte[32];
            for(int i=0; i<32; i++) {
                bytes[i] = -128;
            }
            min = ByteBuffer.wrap(bytes);
        } else if(max == null) {
            byte[] bytes = new byte[32];
            for(int i=0; i<32; i++) {
                bytes[i] = 127;
            }
            max = ByteBuffer.wrap(bytes);
        }

        final byte[] mins = min.array();
        final byte[] maxs = max.array();

        final List<byte[]> bytes = splitByte(mins, maxs, 0, splitNum);
        return bytes.stream()
                .map(ByteBuffer::wrap)
                .map(s -> IndexOffset.of(name, Schema.Type.BYTES, ascending, s))
                .collect(Collectors.toList());
    }

    static List<byte[]> splitByte(byte[] min, byte[] max, int index, int splitNum) {
        if(index >= min.length || index >= max.length) {
            return new ArrayList<>();
        }
        final byte cmin = min[index];
        final byte cmax = max[index];
        final int diff = cmax - cmin;
        if(diff < 0) {
            throw new IllegalStateException("Illegal byte at index: " + index + ", min: " + cmin + ", max: " + cmax);
        } else if(diff == 0) {
            return splitByte(
                    min,
                    max,
                    index + 1,
                    splitNum);
        } else {
            final List<Byte> results = new ArrayList<>();
            final double boundSize = (double)diff / (double)splitNum;
            byte prev = cmin;
            for(int i=1; i<splitNum; i++) {
                byte next = (byte)Math.round(cmin + boundSize * i);
                if(prev == next || next >= cmax) {
                    continue;
                }
                results.add(next);
                prev = next;
            }
            results.add(cmax);

            byte[] prefix = new byte[index + 1];
            for(int i=0; i<index; i++) {
                prefix[i] = min[i];
            }

            final List<byte[]> strs = new ArrayList<>();
            for(int i=0; i<results.size() - 1; i++) {
                prefix[index] = results.get(i);
                strs.add(Arrays.copyOf(prefix, prefix.length));
            }
            strs.add(Arrays.copyOf(max, max.length));
            return strs;
        }
    }

    public static List<IndexOffset> splitString(
            final String field,
            String min,
            String max,
            final boolean ascending,
            final int splitNum,
            final CharCollation charCollation,
            final int depth) {

        if(min == null || min.isEmpty()) {
            min = String.valueOf(charCollation.firstCollationAscii(field));
        }
        if(max == null || max.isEmpty()) {
            if(charCollation.getIsCaseSensitive()) {
                max = String.valueOf(charCollation.lastCollationAscii(field));
            } else {
                max = String.valueOf(charCollation.lastCollationAscii(field));
            }
        }

        List<String> strs = new ArrayList<>();
        final char[] minCharArray = min.toCharArray();
        final char[] maxCharArray = max.toCharArray();
        for (int i = 0; i < Math.min(minCharArray.length, maxCharArray.length); i++) {
            strs = splitChar(field, minCharArray, maxCharArray, i, splitNum, charCollation);
            if (strs.size() > 1) {
                break;
            }
        }
        List<IndexOffset> offsets = strs.stream()
                .map(s -> IndexOffset.of(field, Schema.Type.STRING, ascending, s))
                .collect(Collectors.toList());

        if(offsets.size() < 2) {
            int size = Math.max(minCharArray.length, maxCharArray.length);
            if(offsets.isEmpty() && minCharArray.length == maxCharArray.length) {
                size = size + 1;
            }
            final char[] newMinCharArray = new char[size];
            final char[] newMaxCharArray = new char[size];

            for(int i=0; i<size; i++) {
                if(i >= minCharArray.length) {
                    newMinCharArray[i] = charCollation.firstCollationAscii(field);
                } else {
                    newMinCharArray[i] = minCharArray[i];
                }
                if(i >= maxCharArray.length) {
                    newMaxCharArray[i] = charCollation.lastCollationAscii(field);
                } else {
                    newMaxCharArray[i] = maxCharArray[i];
                }
            }
            final String newMin = String.valueOf(newMinCharArray);
            final String newMax = String.valueOf(newMaxCharArray);
            offsets = splitString(
                    field, newMin, newMax,
                    ascending, splitNum, charCollation, depth + 1);
        }

        final IndexOffset lastOffset = IndexOffset
                .of(field, Schema.Type.STRING, ascending, max);
        offsets.set(offsets.size() - 1, lastOffset);
        return offsets;
    }

    private static List<String> splitChar(
            final String field,
            final char[] min,
            final char[] max,
            final int index,
            final int splitNum,
            final CharCollation charCollation) {

        if(index >= min.length || index >= max.length) {
            return new ArrayList<>();
        }

        final char cmin = min[index];
        final char cmax = max[index];
        final int diff = charCollation.distance(field, cmin, cmax);
        if(diff < 0) {
            throw new IllegalStateException("Illegal string min: " + String.valueOf(min) + ", max: " + String.valueOf(max));
        } else if(diff == 0) {
            return splitChar(
                    field,
                    min,
                    max,
                    index + 1,
                    splitNum,
                    charCollation);
        } else if(diff == 1) {
            return splitChar(
                    field,
                    min,
                    max,
                    index + 1,
                    splitNum,
                    charCollation);
        } else {
            final List<Integer> indexes = new ArrayList<>();
            final double boundSize = (double)diff / (double)splitNum;
            final int cminIndex = charCollation.index(field, cmin);
            final int cmaxIndex = charCollation.index(field, cmax);
            int prevIndex = cminIndex;
            for(int i=1; i<splitNum; i++) {
                int nextIndex = Long.valueOf(Math.round(cminIndex + boundSize * i)).intValue();
                if(prevIndex == nextIndex || nextIndex >= cmaxIndex) {
                    continue;
                }
                indexes.add(nextIndex);
                prevIndex = nextIndex;
            }
            indexes.add(cmaxIndex);

            char[] prefix = new char[index + 1];
            System.arraycopy(min, 0, prefix, 0, index);

            final List<String> strs = new ArrayList<>();
            if(indexes.size() > 1) {
                for (int i = 0; i < indexes.size() - 1; i++) {
                    int c = indexes.get(i);
                    prefix[index] = charCollation.convertCollationChar(field, c);
                    strs.add(String.valueOf(prefix));
                }
            }
            strs.add(String.valueOf(max));
            return strs;
        }
    }

    public static boolean isSplittable(
            final IndexRange range,
            final List<String> seekFields) {
        return isSplittable(seekFields, range.getFrom(), range.getTo());
    }

    private static boolean isSplittable(
            final List<String> fields,
            final IndexPosition pos1,
            final IndexPosition pos2) {

        if(fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("");
        }
        if(pos1 == null || pos2 == null) {
            throw new IllegalArgumentException("");
        }
        if(fields.size() == 1) {
            return false;
        }
        if(Math.abs(pos1.getOffsets().size() - pos2.getOffsets().size()) > 1) {
            return false;
        }
        if(fields.size() != Math.max(pos1.getOffsets().size(), pos2.getOffsets().size())) {
            return false;
        }
        for(int i=0; i<fields.size() - 1; i++) {
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
