package com.mercari.solution.util.domain.db;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.postgresql.PGConnection;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Utility for transferring data with PostgreSQL (or compatible) databases
 * using {@code COPY ... WITH (FORMAT BINARY)} via the JDBC driver's CopyManager API.
 */
public class PostgresUtil {

    public static final String DRIVER = "org.postgresql.Driver";

    private static final byte[] COPY_BINARY_SIGNATURE = {
            'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0
    };

    // 2000-01-01T00:00:00Z (PostgreSQL epoch) relative to unix epoch
    private static final long POSTGRES_EPOCH_MICROS = 946684800000000L;
    private static final int POSTGRES_EPOCH_DAYS = 10957;

    private static final int NUMERIC_POSITIVE = 0x0000;
    private static final int NUMERIC_NEGATIVE = 0x4000;
    private static final int NUMERIC_NAN = 0xC000;

    // Fallback row density used to size splits when the table has never been analyzed.
    private static final double DEFAULT_ROWS_PER_BLOCK = 100d;

    public enum ColumnType implements Serializable {
        BOOL,
        INT2,
        INT4,
        INT8,
        FLOAT4,
        FLOAT8,
        NUMERIC,
        TEXT,
        VARCHAR,
        BPCHAR,
        BYTEA,
        DATE,
        TIME,
        TIMESTAMP,
        TIMESTAMPTZ,
        UUID,
        JSON,
        JSONB;

        public static ColumnType of(final String typeName) {
            return switch (typeName.toLowerCase()) {
                case "bool", "boolean" -> BOOL;
                case "int2", "smallint", "smallserial" -> INT2;
                case "int4", "int", "integer", "serial", "oid" -> INT4;
                case "int8", "bigint", "bigserial" -> INT8;
                case "float4", "real" -> FLOAT4;
                case "float8", "double precision" -> FLOAT8;
                case "numeric", "decimal" -> NUMERIC;
                case "text", "name", "citext" -> TEXT;
                case "varchar", "character varying" -> VARCHAR;
                case "bpchar", "char", "character" -> BPCHAR;
                case "bytea" -> BYTEA;
                case "date" -> DATE;
                case "time" -> TIME;
                case "timestamp" -> TIMESTAMP;
                case "timestamptz" -> TIMESTAMPTZ;
                case "uuid" -> UUID;
                case "json" -> JSON;
                case "jsonb" -> JSONB;
                default -> throw new IllegalArgumentException("postgres module does not support column type: " + typeName);
            };
        }
    }

    public static class Column implements Serializable {

        public final String name;
        public final ColumnType type;

        public Column(final String name, final ColumnType type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            return name + ":" + type;
        }
    }

    /**
     * A physical block ({@code ctid}) range of a table.
     * Used to split a table into chunks that can be read in parallel with
     * efficient TID range scans ({@code ctid >= '(start,0)' AND ctid < '(end,0)'}).
     * {@code startBlock} is inclusive and {@code endBlock} is exclusive;
     * a null bound means unbounded on that side.
     */
    public static class Range implements Serializable {

        public final Long startBlock;
        public final Long endBlock;

        private Range(final Long startBlock, final Long endBlock) {
            this.startBlock = startBlock;
            this.endBlock = endBlock;
        }

        public static Range of(final long startBlock, final long endBlock) {
            return new Range(startBlock, endBlock);
        }

        public static Range from(final long startBlock) {
            return new Range(startBlock, null);
        }

        public static Range full() {
            return new Range(null, null);
        }

        public boolean isFull() {
            return startBlock == null && endBlock == null;
        }

        public String createCondition() {
            if(isFull()) {
                return null;
            }
            final StringBuilder sb = new StringBuilder();
            if(startBlock != null) {
                sb.append("ctid >= '(").append(startBlock).append(",0)'::tid");
            }
            if(endBlock != null) {
                if(!sb.isEmpty()) {
                    sb.append(" AND ");
                }
                sb.append("ctid < '(").append(endBlock).append(",0)'::tid");
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            if(isFull()) {
                return "Range[full]";
            }
            return "Range[" + startBlock + ", " + (endBlock == null ? "" : endBlock) + ")";
        }
    }

    /**
     * Number of physical blocks (8KB pages) the table's main fork currently occupies.
     * Derived from {@code pg_relation_size}, so it reflects the real on-disk size
     * (not the possibly stale {@code pg_class.relpages} estimate) and costs only a stat call.
     * Valid block numbers are {@code 0 .. blockCount - 1}.
     */
    public static long getBlockCount(final Connection connection, final String table) throws SQLException {
        final String sql = "SELECT pg_relation_size(?::regclass) / current_setting('block_size')::bigint";
        try(final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, table);
            try(final ResultSet resultSet = statement.executeQuery()) {
                if(!resultSet.next()) {
                    return 0L;
                }
                return resultSet.getLong(1);
            }
        }
    }

    /**
     * Estimated number of live rows from {@code pg_class.reltuples}.
     * Returns a value &lt;= 0 when the table has never been analyzed.
     */
    public static double getEstimatedRowCount(final Connection connection, final String table) throws SQLException {
        final String sql = "SELECT reltuples FROM pg_class WHERE oid = ?::regclass";
        try(final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, table);
            try(final ResultSet resultSet = statement.executeQuery()) {
                if(!resultSet.next()) {
                    return -1d;
                }
                return resultSet.getDouble(1);
            }
        }
    }

    /**
     * Mechanically split the table block range {@code [0, blockCount)} into
     * {@link Range}s, each covering approximately {@code splitSizeRows} rows.
     * The number of blocks per split is derived from the estimated row density
     * ({@code estimatedRows / blockCount}); when the density is unknown a
     * conservative default is used. The last range is left open-ended so that
     * any rows appended after the split point are still read.
     */
    public static List<Range> createBlockRanges(
            final long blockCount,
            final double estimatedRows,
            final long splitSizeRows) {

        final List<Range> ranges = new ArrayList<>();
        if(blockCount <= 0) {
            // empty (or sub-page) table: read everything with a single query
            ranges.add(Range.full());
            return ranges;
        }

        final double rowsPerBlock = estimatedRows > 0 ? (estimatedRows / blockCount) : DEFAULT_ROWS_PER_BLOCK;
        final long blocksPerSplit = Math.max(1L, Math.round(splitSizeRows / Math.max(rowsPerBlock, 1d)));
        for(long start = 0; start < blockCount; start += blocksPerSplit) {
            final long end = start + blocksPerSplit;
            if(end >= blockCount) {
                ranges.add(Range.from(start));
            } else {
                ranges.add(Range.of(start, end));
            }
        }
        return ranges;
    }

    public static PGConnection getPGConnection(final Connection connection) throws SQLException {
        return connection.unwrap(PGConnection.class);
    }

    public static List<Column> getColumnsFromQuery(
            final Connection connection,
            final String query) throws SQLException {

        try(final PreparedStatement statement = connection
                .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            final ResultSetMetaData meta = statement.getMetaData();
            if(meta == null) {
                throw new IllegalArgumentException("Failed to get result schema for query: " + query);
            }
            return getColumns(meta);
        }
    }

    public static List<Column> getColumns(final ResultSetMetaData meta) throws SQLException {
        final List<Column> columns = new ArrayList<>();
        for(int column = 1; column <= meta.getColumnCount(); column++) {
            columns.add(new Column(
                    meta.getColumnName(column),
                    ColumnType.of(meta.getColumnTypeName(column))));
        }
        return columns;
    }

    public static String createQuery(
            final String table,
            final String select,
            final String where,
            final String rangeCondition) {

        final List<String> conditions = new ArrayList<>();
        if(rangeCondition != null && !rangeCondition.isEmpty()) {
            conditions.add(rangeCondition);
        }
        if(where != null && !where.isEmpty()) {
            conditions.add("(" + where + ")");
        }
        final StringBuilder sb = new StringBuilder("SELECT ").append(select).append(" FROM ").append(table);
        if(!conditions.isEmpty()) {
            sb.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        return sb.toString();
    }

    public static String createCopyOutStatement(final String query) {
        return "COPY (" + query + ") TO STDOUT (FORMAT BINARY)";
    }

    public static String createCopyInStatement(final String table, final List<String> columnNames) {
        return "COPY " + table + " (" + String.join(",", columnNames) + ") FROM STDIN (FORMAT BINARY)";
    }


    // Reader methods for COPY ... TO STDOUT (FORMAT BINARY)

    public static void readHeader(final DataInputStream input) throws IOException {
        final byte[] signature = new byte[COPY_BINARY_SIGNATURE.length];
        input.readFully(signature);
        for(int i = 0; i < COPY_BINARY_SIGNATURE.length; i++) {
            if(signature[i] != COPY_BINARY_SIGNATURE[i]) {
                throw new IllegalStateException("Illegal COPY BINARY signature");
            }
        }
        input.readInt(); // flags field
        final int extensionLength = input.readInt();
        if(extensionLength > 0) {
            input.skipBytes(extensionLength);
        }
    }

    /**
     * Reads the next tuple. Returns null when the file trailer is reached.
     */
    public static GenericRecord read(
            final DataInputStream input,
            final Schema schema,
            final List<Column> columns) throws IOException {

        final short fieldCount = input.readShort();
        if(fieldCount == -1) {
            return null;
        }
        if(fieldCount != columns.size()) {
            throw new IllegalStateException(
                    "Illegal COPY BINARY field count: " + fieldCount + ", expected: " + columns.size());
        }
        final GenericData.Record record = new GenericData.Record(schema);
        for(int i = 0; i < fieldCount; i++) {
            final int length = input.readInt();
            if(length == -1) {
                record.put(i, null);
                continue;
            }
            final byte[] bytes = new byte[length];
            input.readFully(bytes);
            record.put(i, decodeValue(columns.get(i).type, bytes));
        }
        return record;
    }

    private static Object decodeValue(final ColumnType type, final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return switch (type) {
            case BOOL -> bytes[0] != 0;
            case INT2 -> (int) buffer.getShort();
            case INT4 -> buffer.getInt();
            case INT8 -> buffer.getLong();
            case FLOAT4 -> buffer.getFloat();
            case FLOAT8 -> buffer.getDouble();
            case NUMERIC -> toAvroDecimalBytes(decodeNumeric(buffer));
            case TEXT, VARCHAR, BPCHAR, JSON -> new String(bytes, StandardCharsets.UTF_8);
            case JSONB -> {
                final int version = buffer.get();
                if(version != 1) {
                    throw new IllegalStateException("Illegal jsonb binary version: " + version);
                }
                yield new String(bytes, 1, bytes.length - 1, StandardCharsets.UTF_8);
            }
            case BYTEA -> ByteBuffer.wrap(bytes);
            case DATE -> buffer.getInt() + POSTGRES_EPOCH_DAYS;
            case TIME -> buffer.getLong();
            case TIMESTAMP, TIMESTAMPTZ -> buffer.getLong() + POSTGRES_EPOCH_MICROS;
            case UUID -> new UUID(buffer.getLong(), buffer.getLong()).toString();
        };
    }

    private static BigDecimal decodeNumeric(final ByteBuffer buffer) {
        final short ndigits = buffer.getShort();
        final short weight = buffer.getShort();
        final int sign = buffer.getShort() & 0xFFFF;
        final short dscale = buffer.getShort();
        if(sign == NUMERIC_NAN) {
            return null;
        }
        BigDecimal value = BigDecimal.ZERO;
        for(int i = 0; i < ndigits; i++) {
            final short digit = buffer.getShort();
            if(digit != 0) {
                value = value.add(BigDecimal.valueOf(digit).scaleByPowerOfTen(4 * (weight - i)));
            }
        }
        // digit groups are base-10000 so the computed scale may exceed dscale with trailing zeros
        value = value.setScale(dscale, RoundingMode.UNNECESSARY);
        if(sign == NUMERIC_NEGATIVE) {
            value = value.negate();
        }
        return value;
    }

    // Convert to the same representation as ResultSetToRecordConverter (Avro decimal(38,9) bytes)
    private static ByteBuffer toAvroDecimalBytes(final BigDecimal decimal) {
        if(decimal == null) {
            return null;
        }
        final BigDecimal newDecimal;
        if(decimal.scale() > 9) {
            newDecimal = decimal.setScale(9, RoundingMode.HALF_UP).scaleByPowerOfTen(9);
        } else {
            newDecimal = decimal.scaleByPowerOfTen(9);
        }
        return ByteBuffer.wrap(newDecimal.toBigInteger().toByteArray());
    }


    // Writer methods for COPY ... FROM STDIN (FORMAT BINARY)

    public static void writeHeader(final DataOutputStream output) throws IOException {
        output.write(COPY_BINARY_SIGNATURE);
        output.writeInt(0); // flags field
        output.writeInt(0); // header extension area length
    }

    public static void writeTrailer(final DataOutputStream output) throws IOException {
        output.writeShort(-1);
    }

    /**
     * Writes one tuple. {@code fields} must be aligned with {@code columns} by index
     * (an entry may be null when the input has no value for the column).
     */
    public static void write(
            final DataOutputStream output,
            final List<Column> columns,
            final List<Schema.Field> fields,
            final GenericRecord record) throws IOException {

        output.writeShort(columns.size());
        for(int i = 0; i < columns.size(); i++) {
            final Schema.Field field = fields.get(i);
            final Object value = field == null ? null : record.get(field.pos());
            if(value == null) {
                output.writeInt(-1);
                continue;
            }
            encodeValue(output, columns.get(i).type, unnestUnion(field.schema()), value);
        }
    }

    private static void encodeValue(
            final DataOutputStream output,
            final ColumnType type,
            final Schema fieldSchema,
            final Object value) throws IOException {

        switch (type) {
            case BOOL -> {
                output.writeInt(1);
                output.writeBoolean(toBoolean(value));
            }
            case INT2 -> {
                output.writeInt(2);
                output.writeShort((short) toLong(value));
            }
            case INT4 -> {
                output.writeInt(4);
                output.writeInt((int) toLong(value));
            }
            case INT8 -> {
                output.writeInt(8);
                output.writeLong(toLong(value));
            }
            case FLOAT4 -> {
                output.writeInt(4);
                output.writeFloat(((Number) value).floatValue());
            }
            case FLOAT8 -> {
                output.writeInt(8);
                output.writeDouble(((Number) value).doubleValue());
            }
            case NUMERIC -> {
                final byte[] bytes = encodeNumeric(toBigDecimal(fieldSchema, value));
                output.writeInt(bytes.length);
                output.write(bytes);
            }
            case TEXT, VARCHAR, BPCHAR, JSON -> {
                final byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                output.writeInt(bytes.length);
                output.write(bytes);
            }
            case JSONB -> {
                final byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
                output.writeInt(bytes.length + 1);
                output.write(1); // jsonb binary format version
                output.write(bytes);
            }
            case BYTEA -> {
                final byte[] bytes = toBytes(value);
                output.writeInt(bytes.length);
                output.write(bytes);
            }
            case DATE -> {
                output.writeInt(4);
                output.writeInt(toEpochDays(value) - POSTGRES_EPOCH_DAYS);
            }
            case TIME -> {
                output.writeInt(8);
                output.writeLong(toMicroOfDay(fieldSchema, value));
            }
            case TIMESTAMP, TIMESTAMPTZ -> {
                output.writeInt(8);
                output.writeLong(toEpochMicros(fieldSchema, value) - POSTGRES_EPOCH_MICROS);
            }
            case UUID -> {
                final UUID uuid = java.util.UUID.fromString(value.toString());
                output.writeInt(16);
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());
            }
        }
    }

    private static byte[] encodeNumeric(final BigDecimal decimal) {
        final int dscale = Math.max(decimal.scale(), 0);
        if(decimal.signum() == 0) {
            final ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putShort((short) 0); // ndigits
            buffer.putShort((short) 0); // weight
            buffer.putShort((short) NUMERIC_POSITIVE);
            buffer.putShort((short) dscale);
            return buffer.array();
        }

        BigDecimal abs = decimal.abs();
        if(abs.scale() < 0) {
            abs = abs.setScale(0);
        }
        final int scaleGroups = (dscale + 3) / 4;
        BigInteger unscaled = abs.movePointRight(scaleGroups * 4).toBigIntegerExact();
        final BigInteger base = BigInteger.valueOf(10000);
        final List<Short> digits = new ArrayList<>();
        while(unscaled.signum() != 0) {
            final BigInteger[] quotientAndRemainder = unscaled.divideAndRemainder(base);
            digits.add(quotientAndRemainder[1].shortValueExact());
            unscaled = quotientAndRemainder[0];
        }
        final int ndigits = digits.size();
        final int weight = ndigits - scaleGroups - 1;

        final ByteBuffer buffer = ByteBuffer.allocate(8 + 2 * ndigits);
        buffer.putShort((short) ndigits);
        buffer.putShort((short) weight);
        buffer.putShort((short) (decimal.signum() < 0 ? NUMERIC_NEGATIVE : NUMERIC_POSITIVE));
        buffer.putShort((short) dscale);
        for(int i = ndigits - 1; i >= 0; i--) {
            buffer.putShort(digits.get(i));
        }
        return buffer.array();
    }

    private static boolean toBoolean(final Object value) {
        return switch (value) {
            case Boolean b -> b;
            case Number n -> n.longValue() != 0;
            default -> Boolean.parseBoolean(value.toString());
        };
    }

    private static long toLong(final Object value) {
        return switch (value) {
            case Number n -> n.longValue();
            default -> Long.parseLong(value.toString());
        };
    }

    private static byte[] toBytes(final Object value) {
        return switch (value) {
            case ByteBuffer byteBuffer -> {
                final ByteBuffer duplicated = byteBuffer.duplicate();
                final byte[] bytes = new byte[duplicated.remaining()];
                duplicated.get(bytes);
                yield bytes;
            }
            case byte[] bytes -> bytes;
            default -> value.toString().getBytes(StandardCharsets.UTF_8);
        };
    }

    private static BigDecimal toBigDecimal(final Schema fieldSchema, final Object value) {
        if(value instanceof ByteBuffer || value instanceof byte[]) {
            final byte[] bytes = toBytes(value);
            final int scale;
            if(fieldSchema != null && fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal decimalType) {
                scale = decimalType.getScale();
            } else {
                scale = 9;
            }
            return new BigDecimal(new BigInteger(bytes), scale);
        }
        return switch (value) {
            case BigDecimal bd -> bd;
            case Float f -> BigDecimal.valueOf(f);
            case Double d -> BigDecimal.valueOf(d);
            case Number n -> BigDecimal.valueOf(n.longValue());
            default -> new BigDecimal(value.toString());
        };
    }

    private static int toEpochDays(final Object value) {
        return switch (value) {
            case Number n -> n.intValue();
            case Utf8 u -> (int) LocalDate.parse(u.toString()).toEpochDay();
            case String s -> (int) LocalDate.parse(s).toEpochDay();
            default -> throw new IllegalArgumentException("Failed to convert value: " + value + " to date");
        };
    }

    private static long toMicroOfDay(final Schema fieldSchema, final Object value) {
        if(value instanceof Number n) {
            if(fieldSchema != null && fieldSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
                return n.longValue() * 1000L;
            }
            return n.longValue();
        }
        return LocalTime.parse(value.toString()).toNanoOfDay() / 1000L;
    }

    private static long toEpochMicros(final Schema fieldSchema, final Object value) {
        if(value instanceof Number n) {
            if(fieldSchema != null && fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
                return n.longValue() * 1000L;
            }
            return n.longValue();
        }
        final Instant instant = Instant.parse(value.toString());
        return instant.getEpochSecond() * 1000_000L + instant.getNano() / 1000L;
    }

    private static Schema unnestUnion(final Schema schema) {
        if(schema == null) {
            return null;
        }
        if(Schema.Type.UNION.equals(schema.getType())) {
            return schema.getTypes().stream()
                    .filter(s -> !Schema.Type.NULL.equals(s.getType()))
                    .findAny()
                    .orElse(schema);
        }
        return schema;
    }

}