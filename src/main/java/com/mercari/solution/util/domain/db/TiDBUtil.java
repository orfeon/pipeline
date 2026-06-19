package com.mercari.solution.util.domain.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility for reading data from TiDB (or MySQL compatible) databases in parallel.
 *
 * <p>It leverages TiDB specific features inspired by the dumpling tool:
 * <ul>
 *   <li>{@code TABLESAMPLE REGIONS()} to split a table along TiKV region boundaries
 *       so each chunk maps to a different region/node (strategy A).</li>
 *   <li>numeric primary key MIN/MAX even split as a fallback (strategy B).</li>
 *   <li>whole table read as the final fallback (strategy C).</li>
 *   <li>{@code tidb_snapshot} (MVCC snapshot read) for lock-free, consistent reads.</li>
 *   <li>{@code _tidb_rowid} as the split key for tables without an explicit primary key.</li>
 * </ul>
 */
public class TiDBUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBUtil.class);

    public static final String DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String IMPLICIT_ROWID = "_tidb_rowid";

    /**
     * A value range of the split key column.
     * {@code lower} is inclusive and {@code upper} is exclusive; a null bound means
     * unbounded on that side. Bounds are pre-formatted SQL literals.
     */
    public static class Range implements Serializable {

        public final String lower;
        public final String upper;

        private Range(final String lower, final String upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public static Range of(final String lower, final String upper) {
            return new Range(lower, upper);
        }

        public static Range full() {
            return new Range(null, null);
        }

        public boolean isFull() {
            return lower == null && upper == null;
        }

        public String createCondition(final String keyField) {
            if(isFull()) {
                return null;
            }
            final String key = "`" + keyField + "`";
            final StringBuilder sb = new StringBuilder();
            if(lower != null) {
                sb.append(key).append(" >= ").append(lower);
            }
            if(upper != null) {
                if(!sb.isEmpty()) {
                    sb.append(" AND ");
                }
                sb.append(key).append(" < ").append(upper);
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            if(isFull()) {
                return "Range[full]";
            }
            return "Range[" + (lower == null ? "" : lower) + ", " + (upper == null ? "" : upper) + ")";
        }
    }

    public record SplitKey(String field, boolean integer) implements Serializable {}

    /**
     * Appends the given query parameters to a JDBC url, skipping any key that is
     * already present. Used to auto-apply the throughput related parameters
     * recommended for each write mode.
     */
    public static String appendQueryParameters(final String url, final Map<String, String> params) {
        boolean hasQuery = url.contains("?");
        final StringBuilder sb = new StringBuilder(url);
        for(final Map.Entry<String, String> entry : params.entrySet()) {
            if(url.contains(entry.getKey() + "=")) {
                continue;
            }
            sb.append(hasQuery ? "&" : "?").append(entry.getKey()).append("=").append(entry.getValue());
            hasQuery = true;
        }
        return sb.toString();
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

    /**
     * Acquires a consistent snapshot TSO (Timestamp Oracle) of the cluster.
     * Reader sessions then set {@code tidb_snapshot} to this value so that every
     * worker reads the same MVCC version without taking any lock.
     */
    public static String getSnapshotTSO(final Connection connection) throws SQLException {
        final boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try(final Statement statement = connection.createStatement()) {
            statement.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");
            try(final ResultSet resultSet = statement.executeQuery("SELECT @@tidb_current_ts")) {
                final String tso = resultSet.next() ? resultSet.getString(1) : null;
                statement.execute("ROLLBACK");
                if(tso == null || "0".equals(tso)) {
                    return null;
                }
                return tso;
            }
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    public static void setSnapshot(final Connection connection, final String tso) throws SQLException {
        try(final Statement statement = connection.createStatement()) {
            statement.execute("SET @@tidb_snapshot = '" + tso + "'");
        }
    }

    /**
     * Enables TiDB coprocessor paging for the session to bound server side memory
     * usage during large scans. Default on since TiDB v6.2.0; ignored on databases
     * that do not know the variable.
     */
    public static void enablePaging(final Connection connection) {
        try(final Statement statement = connection.createStatement()) {
            statement.execute("SET @@session.tidb_enable_paging = ON");
        } catch (final SQLException e) {
            LOG.info("tidb_enable_paging is not available ({}). skipped", e.getMessage());
        }
    }

    /**
     * Decides the column used to split the table.
     * <ol>
     *   <li>the user specified {@code userKey} when present,</li>
     *   <li>else a single column primary key,</li>
     *   <li>else the implicit {@code _tidb_rowid} (for tables without a clustered key).</li>
     * </ol>
     * Returns null when no usable split key exists (the table is read as a whole).
     */
    public static SplitKey resolveSplitKey(
            final Connection connection,
            final String table,
            final String userKey) throws SQLException {

        if(userKey != null) {
            final Integer type = getColumnSqlType(connection, table, userKey);
            if(type == null) {
                throw new IllegalArgumentException("splitField: " + userKey + " not found in table: " + table);
            }
            return new SplitKey(userKey, isIntegerType(type));
        }

        final List<String> primaryKeys = getPrimaryKeys(connection, table);
        if(primaryKeys.size() == 1) {
            final Integer type = getColumnSqlType(connection, table, primaryKeys.getFirst());
            if(type != null) {
                return new SplitKey(primaryKeys.getFirst(), isIntegerType(type));
            }
        }

        final Integer rowIdType = getColumnSqlType(connection, table, IMPLICIT_ROWID);
        if(rowIdType != null) {
            return new SplitKey(IMPLICIT_ROWID, true);
        }

        LOG.warn("TiDB table: {} has no usable split key. read whole table with single query", table);
        return null;
    }

    /**
     * Splits the table along TiKV region boundaries with {@code TABLESAMPLE REGIONS()}.
     * Returns null when the syntax is unavailable (non TiDB, or TiDB older than v5.0).
     */
    public static List<Range> createRegionRanges(
            final Connection connection,
            final String table,
            final String keyField) {

        final String query = "SELECT `" + keyField + "` FROM " + table
                + " TABLESAMPLE REGIONS() ORDER BY `" + keyField + "`";
        try(final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(query)) {

            final int sqlType = resultSet.getMetaData().getColumnType(1);
            final List<String> boundaries = new ArrayList<>();
            while(resultSet.next()) {
                final String literal = toLiteral(sqlType, resultSet.getString(1));
                if(literal != null) {
                    boundaries.add(literal);
                }
            }
            return rangesFromBoundaries(boundaries);
        } catch (final SQLException e) {
            LOG.info("TABLESAMPLE REGIONS() is not available ({}). fallback to another split strategy", e.getMessage());
            return null;
        }
    }

    /**
     * Splits an integer split key evenly between its MIN and MAX, sizing the number
     * of chunks from the estimated row count and {@code splitSize}.
     */
    public static List<Range> createMinMaxRanges(
            final Connection connection,
            final String table,
            final String keyField,
            final String where,
            final long estimatedRows,
            final long splitSize) throws SQLException {

        final StringBuilder sb = new StringBuilder("SELECT MIN(`")
                .append(keyField).append("`), MAX(`").append(keyField).append("`) FROM ").append(table);
        if(where != null && !where.isEmpty()) {
            sb.append(" WHERE (").append(where).append(")");
        }
        try(final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sb.toString())) {

            if(!resultSet.next()) {
                return List.of(Range.full());
            }
            final BigDecimal min = resultSet.getBigDecimal(1);
            final BigDecimal max = resultSet.getBigDecimal(2);
            if(min == null || max == null) {
                return List.of(Range.full());
            }
            return createNumericRanges(min.toBigInteger(), max.toBigInteger(), estimatedRows, splitSize);
        }
    }

    /**
     * Estimated number of rows from {@code information_schema.TABLES.TABLE_ROWS}.
     * Returns a value &lt;= 0 when statistics are unavailable.
     */
    public static long getEstimatedRowCount(final Connection connection, final String table) throws SQLException {
        final String[] schemaAndName = splitTableName(connection, table);
        final String query = "SELECT TABLE_ROWS FROM information_schema.TABLES"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        try(final PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, schemaAndName[0]);
            statement.setString(2, schemaAndName[1]);
            try(final ResultSet resultSet = statement.executeQuery()) {
                if(!resultSet.next()) {
                    return -1L;
                }
                final long rows = resultSet.getLong(1);
                return resultSet.wasNull() ? -1L : rows;
            }
        }
    }


    // pure helpers (unit testable without a database)

    static List<Range> rangesFromBoundaries(final List<String> boundaries) {
        final List<Range> ranges = new ArrayList<>();
        if(boundaries.size() <= 1) {
            // empty or a single region: nothing to split on
            ranges.add(Range.full());
            return ranges;
        }
        // boundaries[i] is the minimum key of region i; boundaries[0] is the table min.
        // Leave the first range open below and the last open above to cover everything.
        ranges.add(Range.of(null, boundaries.get(1)));
        for(int i = 1; i < boundaries.size() - 1; i++) {
            ranges.add(Range.of(boundaries.get(i), boundaries.get(i + 1)));
        }
        ranges.add(Range.of(boundaries.getLast(), null));
        return ranges;
    }

    static List<Range> createNumericRanges(
            final BigInteger min,
            final BigInteger max,
            final long estimatedRows,
            final long splitSize) {

        final List<Range> ranges = new ArrayList<>();
        if(min == null || max == null || max.compareTo(min) <= 0 || estimatedRows <= 0) {
            ranges.add(Range.full());
            return ranges;
        }
        final long chunks = Math.max(1L, (long) Math.ceil((double) estimatedRows / splitSize));
        if(chunks <= 1) {
            ranges.add(Range.full());
            return ranges;
        }
        final BigInteger step = max.subtract(min).divide(BigInteger.valueOf(chunks)).add(BigInteger.ONE);
        BigInteger cutoff = min;
        while(cutoff.compareTo(max) < 0) {
            final BigInteger next = cutoff.add(step);
            final String lower = cutoff.equals(min) ? null : cutoff.toString();
            final String upper = next.compareTo(max) >= 0 ? null : next.toString();
            ranges.add(Range.of(lower, upper));
            cutoff = next;
        }
        return ranges;
    }

    static String toLiteral(final int sqlType, final String text) {
        if(text == null) {
            return null;
        }
        if(isNumericType(sqlType)) {
            return text;
        }
        return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'";
    }

    static boolean isIntegerType(final int sqlType) {
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.BIT -> true;
            default -> false;
        };
    }

    static boolean isNumericType(final int sqlType) {
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.BIT,
                 Types.DECIMAL, Types.NUMERIC, Types.REAL, Types.FLOAT, Types.DOUBLE -> true;
            default -> false;
        };
    }

    private static Integer getColumnSqlType(
            final Connection connection,
            final String table,
            final String column) {

        final String query = "SELECT `" + column + "` FROM " + table + " WHERE 1 = 0";
        try(final PreparedStatement statement = connection
                .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            final ResultSetMetaData meta = statement.getMetaData();
            if(meta == null || meta.getColumnCount() < 1) {
                return null;
            }
            return meta.getColumnType(1);
        } catch (final SQLException e) {
            // the column (e.g. _tidb_rowid on a clustered table) does not exist
            return null;
        }
    }

    private static List<String> getPrimaryKeys(final Connection connection, final String table) throws SQLException {
        final String[] schemaAndName = splitTableName(connection, table);
        final DatabaseMetaData metaData = connection.getMetaData();
        final Map<Short, String> primaryKeys = new HashMap<>();
        try(final ResultSet resultSet = metaData.getPrimaryKeys(schemaAndName[0], null, schemaAndName[1])) {
            while(resultSet.next()) {
                primaryKeys.put(resultSet.getShort("KEY_SEQ"), resultSet.getString("COLUMN_NAME"));
            }
        }
        return primaryKeys.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList();
    }

    private static String[] splitTableName(final Connection connection, final String table) throws SQLException {
        final String cleaned = table.replace("`", "");
        final int index = cleaned.indexOf('.');
        if(index >= 0) {
            return new String[]{ cleaned.substring(0, index), cleaned.substring(index + 1) };
        }
        return new String[]{ Optional.ofNullable(connection.getCatalog()).orElse(connection.getSchema()), cleaned };
    }

}